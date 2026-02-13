#!/usr/bin/env python3
"""
Analyze GeoGuessr Team Duel Stats

Provides comprehensive analysis of player performance including:
- Accuracy, speed, and efficiency metrics
- Per-country and per-region performance
- Country confusion patterns
- Win/loss splits, team/round win rates
- Move vs no-move comparison
- Trend export for LLM analysis
"""

import json
import math
import pandas as pd
import argparse
from pathlib import Path
from typing import Optional
from datetime import datetime
import sys

from country_codes import LARGE_COUNTRIES, normalize_country_name


# Approximate land area per region/continent in km² (for normalizing distance metrics)
REGION_AREAS_KM2 = {
    'Africa': 30370000,
    'Asia': 44580000,
    'Europe': 10180000,
    'North America': 24710000,
    'Oceania': 8526000,
    'South America': 17840000,
}


# ===================================================================
# Data loading
# ===================================================================

def detect_my_team(df: pd.DataFrame) -> set:
    """Detect which players are on 'my team' using the most common team_key.

    The team_key format is '{player_id_1}_{player_id_2}' where each player_id
    is a 24-character hex string and they are sorted alphabetically.
    We parse the player IDs directly from the team_key rather than collecting
    all player_ids from matching rows (which would include opponents).

    Returns set of player_ids belonging to the primary team.
    """
    if 'team_key' not in df.columns:
        return set(df['player_id'].unique())

    team_keys = df['team_key'].value_counts()
    if team_keys.empty:
        return set(df['player_id'].unique())

    my_team_key = team_keys.index[0]

    # Parse player IDs from team_key (format: "id1_id2", each id is 24 hex chars)
    if len(my_team_key) == 49 and my_team_key[24] == '_':
        return {my_team_key[:24], my_team_key[25:]}

    # Fallback: split on underscore (works if IDs don't contain underscores)
    parts = my_team_key.split('_')
    if len(parts) == 2:
        return set(parts)

    # Last resort: return all player_ids from rows with this team_key
    return set(df[df['team_key'] == my_team_key]['player_id'].unique())


def load_data(csv_file: str) -> pd.DataFrame:
    """Load and validate CSV data"""
    df = pd.read_csv(csv_file)

    required_cols = ['game_id', 'round', 'player_id', 'player_name',
                     'distance_km', 'time_seconds']
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        print(f"\u274c Missing columns: {missing}")
        sys.exit(1)

    # Type conversions
    if 'game_won' in df.columns:
        df['game_won_bool'] = df['game_won'].apply(
            lambda x: True if str(x).strip() == 'True' else
                      (False if str(x).strip() == 'False' else None))

    if 'game_date' in df.columns:
        df['game_date_parsed'] = pd.to_datetime(df['game_date'], errors='coerce', utc=True)

    for col in ['won_team', 'won_round', 'is_team_best_guess', 'correct_country_flag']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: True if str(x).strip() == 'True' else
                                    (False if str(x).strip() == 'False' else None))

    for col in ['health_before', 'health_after', 'damage_dealt', 'multiplier', 'score']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Normalize country names (Czechia → Czech Republic, etc.)
    for col in ['correct_country', 'guessed_country']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: normalize_country_name(x) if pd.notna(x) else x)

    # Recompute correct_country_flag after normalization
    if 'correct_country' in df.columns and 'guessed_country' in df.columns and 'correct_country_flag' in df.columns:
        mask = (df['correct_country'].notna() & df['guessed_country'].notna() &
                ~df['correct_country'].isin(['Unknown', 'Lost at Sea']) &
                ~df['guessed_country'].isin(['Unknown', 'Lost at Sea', '']))
        df.loc[mask, 'correct_country_flag'] = df.loc[mask, 'correct_country'] == df.loc[mask, 'guessed_country']

    return df


# ===================================================================
# Analysis functions
# ===================================================================

def _filter_guess_clicked(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to rows where the player actively clicked guess (not auto pin-drop).

    In competitive team duels, one player clicks "Guess" which starts a 15-second
    countdown. Other players can click before it expires. If they don't, their pin
    is auto-submitted at round end (time_remaining ≈ 0).

    time_remaining_sec (v0.3.0+): round_end_time - guess_created_time.
      Higher = clicked earlier. Values < 1 = timer expired, auto-submitted.
    time_seconds (all versions): guess_created_time - round_start_time.
      Per-player elapsed time. When all players in a round have identical values,
      they were all auto-submitted at round end. However, without time_remaining_sec
      we can't reliably distinguish individual clicks, so we include all rows.
    """
    if 'time_remaining_sec' in df.columns:
        tr = pd.to_numeric(df['time_remaining_sec'], errors='coerce')
        mask = tr >= 1
        if mask.any():
            return df[mask]
    # Without time_remaining_sec, we can't reliably distinguish — return all rows
    return df


def player_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Overall player statistics.

    avg_time_sec and median_time_sec only count rounds where the player actively
    clicked guess (time_remaining_sec >= 1), not auto-submitted pin drops.
    Falls back to all rows when time_remaining_sec is unavailable.
    """
    # Distance stats use all rows
    dist_stats = df.groupby(['player_id', 'player_name']).agg({
        'distance_km': ['mean', 'median', 'min', 'max', 'std'],
        'game_id': 'count'
    }).round(2)
    dist_stats.columns = ['avg_dist_km', 'median_dist_km', 'best_dist_km',
                          'worst_dist_km', 'std_dist_km', 'total_guesses']

    # Time stats only count guess-clicked rounds
    df_clicked = _filter_guess_clicked(df)
    if len(df_clicked) > 0:
        time_stats = df_clicked.groupby(['player_id', 'player_name']).agg({
            'time_seconds': ['mean', 'median', 'count']
        }).round(2)
        time_stats.columns = ['avg_time_sec', 'median_time_sec', 'times_guess_clicked']
    else:
        time_stats = pd.DataFrame(columns=['avg_time_sec', 'median_time_sec', 'times_guess_clicked'])

    summary = dist_stats.join(time_stats, how='left')

    # Add no-pin count: rounds where the player's row is missing entirely
    # (they didn't place a pin at all — not even a pin drop)
    all_rounds = df.groupby('game_id')['round'].unique()
    nopin_counts = {}
    for pid in dist_stats.index.get_level_values('player_id').unique():
        player_games = df[df['player_id'] == pid]['game_id'].unique()
        player_existing = set(
            zip(df[df['player_id'] == pid]['game_id'],
                df[df['player_id'] == pid]['round'])
        )
        missing = sum(
            1 for g in player_games
            for r in all_rounds.get(g, [])
            if (g, r) not in player_existing
        )
        # Also count status='no_pin' rows if the column exists
        if 'status' in df.columns:
            missing += len(df[(df['player_id'] == pid) & (df['status'] == 'no_pin')])
        nopin_counts[pid] = missing
    nopin = pd.Series(nopin_counts, name='no_pin_count')
    # Index is MultiIndex (player_id, player_name); join on player_id level
    nopin_df = nopin.to_frame()
    nopin_df.index.name = 'player_id'
    summary = summary.join(nopin_df, on='player_id', how='left')
    summary['no_pin_count'] = summary['no_pin_count'].fillna(0).astype(int)

    # Add correct country % if available
    if 'correct_country_flag' in df.columns:
        pct = df[df['correct_country_flag'].notna()].groupby('player_id')['correct_country_flag'].mean() * 100
        summary = summary.join(pct.rename('correct_country_pct').round(1))

    summary = summary.sort_values('avg_dist_km').reset_index()
    return summary


def accuracy_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Rank players by accuracy"""
    ranking = df.groupby('player_name').agg({
        'distance_km': 'mean', 'game_id': 'count'
    }).round(2)
    ranking.columns = ['avg_distance_km', 'num_guesses']
    ranking = ranking.sort_values('avg_distance_km').reset_index()
    ranking.insert(0, 'rank', range(1, len(ranking) + 1))
    return ranking


def speed_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Rank players by speed.

    Only includes rounds where the player actively clicked guess
    (time_remaining_sec >= 1 when available). Also excludes score == 0
    and distance > 10,000 km.
    """
    df_timed = _filter_guess_clicked(df)

    # Exclude score == 0 (timeout / no pin) if score column exists
    if 'score' in df_timed.columns:
        df_timed = df_timed[df_timed['score'] > 0]

    # Exclude extreme distances (> 10,000 km suggests no meaningful guess)
    if 'distance_km' in df_timed.columns:
        df_timed = df_timed[df_timed['distance_km'] <= 10000]

    if len(df_timed) == 0:
        return pd.DataFrame()

    ranking = df_timed.groupby('player_name').agg({
        'time_seconds': 'mean', 'game_id': 'count'
    }).round(2)
    ranking.columns = ['avg_time_sec', 'times_guess_clicked']
    ranking = ranking.sort_values('avg_time_sec').reset_index()
    ranking.insert(0, 'rank', range(1, len(ranking) + 1))
    return ranking


def speed_vs_accuracy(df: pd.DataFrame) -> pd.DataFrame:
    """Compare speed vs accuracy.

    Only counts rounds where the player actively clicked guess
    (time_remaining_sec >= 1 when available).
    """
    df_timed = _filter_guess_clicked(df)
    if 'score' in df_timed.columns:
        df_timed = df_timed[df_timed['score'] > 0]
    if 'distance_km' in df_timed.columns:
        df_timed = df_timed[df_timed['distance_km'] <= 10000]
    if len(df_timed) == 0:
        return pd.DataFrame()

    analysis = df_timed.groupby('player_name').agg({
        'time_seconds': 'mean', 'distance_km': 'mean', 'game_id': 'count'
    }).round(2)
    analysis.columns = ['avg_time_sec', 'avg_distance_km', 'times_guess_clicked']

    # Efficiency: lower combined rank = better. Weight time and distance equally.
    # Use rank-based scoring so it works well with any number of players.
    analysis['time_rank'] = analysis['avg_time_sec'].rank()
    analysis['dist_rank'] = analysis['avg_distance_km'].rank()
    analysis['combined_rank'] = (analysis['time_rank'] + analysis['dist_rank']).round(1)
    analysis = analysis.drop(columns=['time_rank', 'dist_rank'])

    return analysis.sort_values('combined_rank').reset_index()


def team_stats_summary(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Team-level stats split by win/loss, with total + per-move-mode rows."""
    if 'game_won_bool' not in df.columns or df['game_won_bool'].isna().all():
        return None

    def _make_rows(subset, mode_label='total'):
        rows = []
        for outcome, label in [(True, 'Win'), (False, 'Loss')]:
            s = subset[subset['game_won_bool'] == outcome]
            if len(s) == 0:
                continue
            rows.append({
                'mode': mode_label,
                'outcome': label,
                'games': s['game_id'].nunique(),
                'avg_dist_km': round(s['distance_km'].mean(), 2),
                'worst_dist_km': round(s['distance_km'].max(), 2),
                'avg_time_sec': round(s['time_seconds'].mean(), 2),
            })
        return rows

    rows = _make_rows(df, 'total')

    # Per move mode breakdown
    if 'move_mode' in df.columns and df['move_mode'].nunique() >= 2:
        for mode in sorted(df['move_mode'].dropna().unique()):
            mode_df = df[df['move_mode'] == mode]
            rows.extend(_make_rows(mode_df, mode))

    return pd.DataFrame(rows) if rows else None


def player_win_loss_split(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Per-player average distance in wins vs losses, with country accuracy breakdown."""
    if 'game_won_bool' not in df.columns or df['game_won_bool'].isna().all():
        return None

    valid = df[df['game_won_bool'].notna()]
    rows = []
    for player_name in sorted(valid['player_name'].unique()):
        pdf = valid[valid['player_name'] == player_name]
        row = {'player_name': player_name}

        wins = pdf[pdf['game_won_bool'] == True]
        losses = pdf[pdf['game_won_bool'] == False]
        row['avg_dist_win'] = round(wins['distance_km'].mean(), 2) if len(wins) else 0
        row['avg_dist_loss'] = round(losses['distance_km'].mean(), 2) if len(losses) else 0

        if 'correct_country_flag' in pdf.columns:
            correct = pdf[pdf['correct_country_flag'] == True]
            incorrect = pdf[pdf['correct_country_flag'] == False]
            row['wins_correct_country'] = len(correct[correct['game_won_bool'] == True])
            row['wins_wrong_country'] = len(incorrect[incorrect['game_won_bool'] == True])
            row['losses_correct_country'] = len(correct[correct['game_won_bool'] == False])
            row['losses_wrong_country'] = len(incorrect[incorrect['game_won_bool'] == False])

        rows.append(row)

    return pd.DataFrame(rows) if rows else None


def won_team_stats(df: pd.DataFrame, by_move_mode: bool = False, team_name: str = 'Team') -> Optional[pd.DataFrame]:
    """How often each player beat their teammate, with team aggregate row first.

    If by_move_mode=True, returns a separate breakdown per move mode.
    """
    if 'won_team' not in df.columns or df['won_team'].isna().all():
        return None

    valid = df[df['won_team'].notna()]
    group_cols = ['player_name']
    if by_move_mode and 'move_mode' in valid.columns:
        group_cols.append('move_mode')

    # Team aggregate row
    if not by_move_mode:
        team_won = int(valid['won_team'].sum())
        team_total = len(valid)
        team_row = pd.DataFrame([{
            'player_name': team_name,
            'rounds_won': team_won,
            'total_rounds': team_total,
            'win_pct': round(team_won / max(team_total, 1) * 100, 1),
        }])
    else:
        team_row = pd.DataFrame()

    # Per-player rows
    result = valid.groupby(group_cols).agg(
        rounds_won=('won_team', 'sum'),
        total_rounds=('won_team', 'count')
    )
    result['win_pct'] = (result['rounds_won'] / result['total_rounds'] * 100).round(1)
    player_rows = result.reset_index().sort_values(group_cols + ['win_pct'], ascending=[True] * len(group_cols) + [False])

    if not team_row.empty:
        return pd.concat([team_row, player_rows], ignore_index=True)
    return player_rows


def won_round_stats(df: pd.DataFrame, by_move_mode: bool = False) -> Optional[pd.DataFrame]:
    """How often each player had the best guess across all teams.

    If by_move_mode=True, returns a separate breakdown per move mode.
    """
    if 'won_round' not in df.columns or df['won_round'].isna().all():
        return None

    valid = df[df['won_round'].notna()]
    group_cols = ['player_name']
    if by_move_mode and 'move_mode' in valid.columns:
        group_cols.append('move_mode')

    result = valid.groupby(group_cols).agg(
        rounds_won=('won_round', 'sum'),
        total_rounds=('won_round', 'count')
    )
    result['win_pct'] = (result['rounds_won'] / result['total_rounds'] * 100).round(1)
    return result.reset_index().sort_values(group_cols + ['win_pct'], ascending=[True] * len(group_cols) + [False])


def region_performance(df: pd.DataFrame, team_name: str = 'Team') -> pd.DataFrame:
    """Per-region performance by player, with team aggregate row first.

    Team row shows distance as a ratio (km per 1000 km² of region area) so
    regions of different sizes can be compared. Player rows show both km and
    the ratio, since per-player km values are directly comparable.
    """
    if 'region' not in df.columns:
        return pd.DataFrame()

    df_valid = df[df['region'].notna() & (df['region'] != 'Unknown') & (df['region'] != 'Other')]
    if len(df_valid) == 0:
        return pd.DataFrame()

    # Team aggregate row: normalize by region area
    team_agg = df_valid.groupby('region')['distance_km'].mean().round(2)

    # Build team row: express avg distance as % of region span (sqrt of area).
    # e.g. if region span ~3000km and avg_dist=600km, that's 20% — intuitive.
    team_norm = {}
    for region, avg_km in team_agg.items():
        area = REGION_AREAS_KM2.get(region)
        if area:
            span = math.sqrt(area)
            pct = round(avg_km / span * 100, 1)
            team_norm[region] = f'{pct}%'
        else:
            team_norm[region] = f'{avg_km:.0f}km'

    team_row = pd.DataFrame([team_norm], index=[f'{team_name} (% of span)'])

    # Per-player rows: show km values (directly comparable between players)
    perf = df_valid.groupby(['player_name', 'region']).agg({
        'distance_km': 'mean'
    }).round(1)
    perf.columns = ['avg_distance_km']
    perf = perf.reset_index()
    player_pivot = perf.pivot(index='player_name', columns='region', values='avg_distance_km')

    return pd.concat([team_row, player_pivot])


def best_worst_countries(df: pd.DataFrame, n: int = 10, team_name: str = 'Team') -> tuple:
    """Best and worst countries per player, with team aggregate rows first."""
    if 'correct_country' not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame(), pd.DataFrame()

    # Team aggregate
    team_perf = df_valid.groupby('correct_country').agg({
        'distance_km': ['mean', 'count']
    }).round(2)
    team_perf.columns = ['avg_dist_km', 'num_guesses']
    team_perf = team_perf[team_perf['num_guesses'] >= 3].reset_index()
    team_perf.insert(0, 'player_name', team_name)

    team_best = team_perf.sort_values('avg_dist_km').head(n)
    team_worst = team_perf.sort_values('avg_dist_km', ascending=False).head(n)

    # Per-player
    perf = df_valid.groupby(['player_name', 'correct_country']).agg({
        'distance_km': ['mean', 'count']
    }).round(2)
    perf.columns = ['avg_dist_km', 'num_guesses']
    perf = perf[perf['num_guesses'] >= 3].reset_index()

    best = perf.sort_values('avg_dist_km').groupby('player_name').head(n)
    best = best.sort_values(['player_name', 'avg_dist_km'])
    worst = perf.sort_values('avg_dist_km', ascending=False).groupby('player_name').head(n)
    worst = worst.sort_values(['player_name', 'avg_dist_km'], ascending=[True, False])

    best = pd.concat([team_best, best], ignore_index=True)
    worst = pd.concat([team_worst, worst], ignore_index=True)
    return best, worst


def best_worst_in_country(df: pd.DataFrame, n: int = 10, team_name: str = 'Team') -> tuple:
    """Countries where we're closest/furthest when we guess the correct country.

    Only considers rounds where correct_country_flag is True. This shows
    which countries we know well enough to get the right country but are
    still far from the actual location (worst-in-country) vs pinpointing (best).
    """
    if 'correct_country' not in df.columns or 'correct_country_flag' not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    df_correct = df[df['correct_country_flag'] == True]
    df_valid = df_correct[df_correct['correct_country'].notna() &
                          ~df_correct['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame(), pd.DataFrame()

    # Team aggregate
    team_perf = df_valid.groupby('correct_country').agg(
        avg_dist_km=('distance_km', 'mean'),
        num_correct=('distance_km', 'count')
    ).round(2).reset_index()
    team_perf = team_perf[team_perf['num_correct'] >= 3]
    team_perf.insert(0, 'player_name', team_name)

    team_best = team_perf.sort_values('avg_dist_km').head(n)
    team_worst = team_perf.sort_values('avg_dist_km', ascending=False).head(n)

    # Per-player
    perf = df_valid.groupby(['player_name', 'correct_country']).agg(
        avg_dist_km=('distance_km', 'mean'),
        num_correct=('distance_km', 'count')
    ).round(2).reset_index()
    perf = perf[perf['num_correct'] >= 3]

    best = perf.sort_values('avg_dist_km').groupby('player_name').head(n)
    best = best.sort_values(['player_name', 'avg_dist_km'])
    worst = perf.sort_values('avg_dist_km', ascending=False).groupby('player_name').head(n)
    worst = worst.sort_values(['player_name', 'avg_dist_km'], ascending=[True, False])

    best = pd.concat([team_best, best], ignore_index=True)
    worst = pd.concat([team_worst, worst], ignore_index=True)
    return best, worst


def countries_i_confuse(df: pd.DataFrame, team_name: str = 'Team') -> pd.DataFrame:
    """Countries I keep thinking are other countries, with team aggregate rows first."""
    if 'correct_country' not in df.columns or 'guessed_country' not in df.columns:
        return pd.DataFrame()

    df_valid = df[
        df['correct_country'].notna() &
        df['guessed_country'].notna() &
        ~df['correct_country'].isin(['Unknown', 'Lost at Sea']) &
        ~df['guessed_country'].isin(['Unknown', 'Lost at Sea', '']) &
        (df['correct_country_flag'] == False)
    ]
    if len(df_valid) == 0:
        return pd.DataFrame()

    # Team aggregate
    team_confusion = df_valid.groupby(
        ['correct_country', 'guessed_country']
    ).size().reset_index(name='times')
    team_confusion.insert(0, 'player_name', team_name)
    team_confusion = team_confusion.sort_values('times', ascending=False)

    # Per-player
    confusion = df_valid.groupby(
        ['player_name', 'correct_country', 'guessed_country']
    ).size().reset_index(name='times')
    confusion = confusion.sort_values(['player_name', 'times'], ascending=[True, False])

    return pd.concat([team_confusion, confusion], ignore_index=True)


def countries_worth_studying(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """Worst countries that are also geographically large.

    importance = (avg_dist_km / area_km2) * log(1 + num_guesses)

    The area normalization ensures that being 500km off in Monaco matters more
    than 500km off in Russia. The log(1+n) factor weights countries we see
    frequently higher than one-offs, without letting frequency dominate.
    """
    if 'correct_country' not in df.columns:
        return pd.DataFrame()

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame()

    perf = df_valid.groupby('correct_country').agg({
        'distance_km': ['mean', 'count']
    }).round(2)
    perf.columns = ['avg_dist_km', 'num_guesses']
    perf = perf[perf['num_guesses'] >= 3].reset_index()

    perf['area_km2'] = perf['correct_country'].map(LARGE_COUNTRIES)
    perf = perf.dropna(subset=['area_km2'])
    perf['area_km2'] = perf['area_km2'].astype(int)

    # importance = (avg_dist / area) * log(1 + num_guesses)
    # Normalized to 0-100 scale
    raw = (perf['avg_dist_km'] / perf['area_km2']) * perf['num_guesses'].apply(math.log1p)
    if raw.max() > 0:
        perf['importance'] = (raw / raw.max() * 100).round(0).astype(int)
    else:
        perf['importance'] = 0

    perf = perf.sort_values('importance', ascending=False).head(n)
    return perf


def move_vs_nomove(df: pd.DataFrame, team_name: str = 'Team') -> Optional[pd.DataFrame]:
    """Compare performance across move modes, with team aggregate row first."""
    if 'move_mode' not in df.columns or df['move_mode'].nunique() < 2:
        return None

    aggs = {'distance_km': 'mean', 'time_seconds': 'mean', 'game_id': 'count'}

    # Team aggregate row
    team_agg = df.groupby('move_mode').agg(aggs).round(2)
    team_agg.columns = ['avg_dist_km', 'avg_time_sec', 'num_guesses']
    if 'correct_country_flag' in df.columns:
        team_pct = df[df['correct_country_flag'].notna()].groupby(
            'move_mode'
        )['correct_country_flag'].mean().round(3) * 100
        team_agg = team_agg.join(team_pct.rename('correct_country_pct'))
    team_agg.insert(0, 'player_name', team_name)
    team_agg = team_agg.reset_index()

    # Per-player rows
    comparison = df.groupby(['player_name', 'move_mode']).agg(aggs).round(2)
    comparison.columns = ['avg_dist_km', 'avg_time_sec', 'num_guesses']

    if 'correct_country_flag' in df.columns:
        pct = df[df['correct_country_flag'].notna()].groupby(
            ['player_name', 'move_mode']
        )['correct_country_flag'].mean().round(3) * 100
        comparison = comparison.join(pct.rename('correct_country_pct'))

    player_rows = comparison.reset_index()
    # Sort players by move_mode then name
    player_rows = player_rows.sort_values(['move_mode', 'player_name']).reset_index(drop=True)
    # Team rows first, then player rows (both sorted by move_mode)
    team_agg = team_agg.sort_values('move_mode').reset_index(drop=True)
    return pd.concat([team_agg, player_rows], ignore_index=True)


def rounds_played_trend(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """How many rounds per game, and which round games typically end on"""
    if 'total_rounds' not in df.columns:
        return None

    game_df = df.groupby('game_id').first().reset_index()
    stats = {
        'avg_rounds_per_game': round(game_df['total_rounds'].mean(), 1),
        'min_rounds': int(game_df['total_rounds'].min()),
        'max_rounds': int(game_df['total_rounds'].max()),
        'total_games': len(game_df),
    }

    if 'game_won_bool' in game_df.columns:
        wins = game_df[game_df['game_won_bool'] == True]
        losses = game_df[game_df['game_won_bool'] == False]
        if len(wins) > 0:
            stats['avg_rounds_in_wins'] = round(wins['total_rounds'].mean(), 1)
            stats['win_count'] = len(wins)
        if len(losses) > 0:
            stats['avg_rounds_in_losses'] = round(losses['total_rounds'].mean(), 1)
            stats['loss_count'] = len(losses)

    return pd.DataFrame([stats])


def recent_vs_alltime(df: pd.DataFrame, recent_n: int = 10) -> Optional[pd.DataFrame]:
    """Compare recent performance (last N games) vs all-time per player.

    Shows avg distance, country accuracy %, win rate %, and avg time for
    both 'all-time' and 'recent' windows, plus a delta column with arrows.
    """
    if 'game_date_parsed' not in df.columns or df['game_date_parsed'].isna().all():
        return None

    # Get chronologically ordered unique games
    game_dates = df.groupby('game_id')['game_date_parsed'].min().sort_values()
    if len(game_dates) < 2:
        return None

    recent_game_ids = set(game_dates.tail(recent_n).index)

    rows = []
    for player_name in sorted(df['player_name'].unique()):
        pdf = df[df['player_name'] == player_name]
        pdf_recent = pdf[pdf['game_id'].isin(recent_game_ids)]

        if len(pdf_recent) == 0:
            continue

        def calc_stats(subset):
            stats = {
                'games': subset['game_id'].nunique(),
                'avg_dist_km': round(float(subset['distance_km'].mean()), 1),
                'avg_time_sec': round(float(subset['time_seconds'].mean()), 1),
            }
            if 'correct_country_flag' in subset.columns and subset['correct_country_flag'].notna().any():
                stats['country_acc_%'] = round(float(subset['correct_country_flag'].mean() * 100), 1)
            else:
                stats['country_acc_%'] = None
            if 'game_won_bool' in subset.columns and subset['game_won_bool'].notna().any():
                game_outcomes = subset.groupby('game_id')['game_won_bool'].first()
                stats['win_rate_%'] = round(float(game_outcomes.mean() * 100), 1)
            else:
                stats['win_rate_%'] = None
            return stats

        all_stats = calc_stats(pdf)
        rec_stats = calc_stats(pdf_recent)

        def delta_str(recent_val, all_val, lower_is_better=True):
            if recent_val is None or all_val is None:
                return ''
            diff = recent_val - all_val
            if abs(diff) < 0.05:
                return '  -'
            if lower_is_better:
                improving = diff < 0
            else:
                improving = diff > 0
            # Green circle for improvement, red circle for decline
            if improving:
                icon = '\033[32m\u25cf\033[0m'  # green ●
            else:
                icon = '\033[31m\u25cf\033[0m'  # red ●
            return f'{icon}{abs(diff):.1f}'

        rows.append({
            'player': player_name,
            'period': 'all-time',
            'games': all_stats['games'],
            'avg_dist_km': all_stats['avg_dist_km'],
            'avg_time_sec': all_stats['avg_time_sec'],
            'country_acc_%': all_stats['country_acc_%'],
            'win_rate_%': all_stats['win_rate_%'],
        })
        rows.append({
            'player': player_name,
            'period': f'last {rec_stats["games"]}',
            'games': rec_stats['games'],
            'avg_dist_km': rec_stats['avg_dist_km'],
            'avg_time_sec': rec_stats['avg_time_sec'],
            'country_acc_%': rec_stats['country_acc_%'],
            'win_rate_%': rec_stats['win_rate_%'],
        })
        rows.append({
            'player': player_name,
            'period': 'delta',
            'games': '',
            'avg_dist_km': delta_str(rec_stats['avg_dist_km'], all_stats['avg_dist_km'], lower_is_better=True),
            'avg_time_sec': delta_str(rec_stats['avg_time_sec'], all_stats['avg_time_sec'], lower_is_better=True),
            'country_acc_%': delta_str(rec_stats['country_acc_%'], all_stats['country_acc_%'], lower_is_better=False),
            'win_rate_%': delta_str(rec_stats['win_rate_%'], all_stats['win_rate_%'], lower_is_better=False),
        })

    if not rows:
        return None

    result = pd.DataFrame(rows)
    return result


def competitive_advantage(df: pd.DataFrame, min_guesses: int = 3) -> Optional[pd.DataFrame]:
    """Identify opponent country weaknesses vs your team's strengths.

    Compares per-country average distance between your team and opponents.
    Positive 'advantage_km' means you outperform opponents on that country.
    Requires opponent data in the CSV (i.e. fetched without --my-team-only).
    """
    if 'correct_country' not in df.columns or 'is_team_best_guess' not in df.columns:
        return None

    # Determine which players are on the user's team vs opponents
    # We use team_key or is_team_best_guess to distinguish.
    # If team_key is present, identify the team. Otherwise, use a heuristic:
    # players appearing in is_team_best_guess True/False analysis.
    # The simplest approach: look at all unique player_ids and group by team_key if available.

    # Use detect_my_team to identify team members
    my_team_players = detect_my_team(df)

    if not my_team_players:
        return None

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return None

    df_my_team = df_valid[df_valid['player_id'].isin(my_team_players)]
    df_opponents = df_valid[~df_valid['player_id'].isin(my_team_players)]

    if len(df_opponents) == 0:
        return None

    # Per-country stats for my team
    my_stats = df_my_team.groupby('correct_country').agg(
        my_avg_dist=('distance_km', 'mean'),
        my_guesses=('distance_km', 'count')
    ).round(1)

    # Per-country stats for opponents
    opp_stats = df_opponents.groupby('correct_country').agg(
        opp_avg_dist=('distance_km', 'mean'),
        opp_guesses=('distance_km', 'count')
    ).round(1)

    # Join and filter
    combined = my_stats.join(opp_stats, how='inner')
    combined = combined[(combined['my_guesses'] >= min_guesses) & (combined['opp_guesses'] >= min_guesses)]

    if len(combined) == 0:
        return None

    combined['advantage_km'] = (combined['opp_avg_dist'] - combined['my_avg_dist']).round(1)
    combined = combined.sort_values('advantage_km', ascending=False).reset_index()

    return combined


def country_performance(df: pd.DataFrame, player_id: str = None) -> pd.DataFrame:
    """Per-country performance breakdown"""
    if player_id:
        df = df[df['player_id'] == player_id]

    if 'correct_country' not in df.columns:
        return pd.DataFrame()

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame()

    perf = df_valid.groupby('correct_country').agg({
        'distance_km': ['mean', 'count'],
    }).round(2)
    perf.columns = ['avg_distance_km', 'num_guesses']
    perf = perf[perf['num_guesses'] >= 3]
    return perf.sort_values('avg_distance_km').reset_index()


# ===================================================================
# Initiative & timing analysis (requires v0.3.0 columns)
# ===================================================================

def initiative_summary(df: pd.DataFrame, team_name: str = 'Team') -> Optional[pd.DataFrame]:
    """Per-player initiative metrics: who clicks first, who doesn't guess.

    Works with both new CSVs (status/clicked_first columns) and old CSVs
    (infers no_pin from missing player rows per round).

    initiative_rate = clicked_first / rounds_guessed
    participation_rate = rounds_guessed / total_round_slots
    """
    has_status = 'status' in df.columns and df['status'].notna().any()
    has_clicked = 'clicked_first' in df.columns and df['clicked_first'].notna().any()

    df = df.copy()

    # Build a full round roster: every (game_id, round) x every player should have a row
    # Rounds where a player has no row = no_pin
    all_rounds = df.groupby('game_id')['round'].unique()
    players = df['player_name'].unique()

    # Count total round-slots per player (rounds they should have participated in)
    round_slots = {}
    for player in players:
        player_games = df[df['player_name'] == player]['game_id'].unique()
        total_slots = sum(len(all_rounds.get(g, [])) for g in player_games)
        round_slots[player] = total_slots

    # Detect no_pin: rounds in games where the player has a row for some rounds but not this one
    if has_status:
        df['_is_guessed'] = df['status'] == 'guessed'
        df['_is_nopin'] = df['status'] == 'no_pin'
    else:
        # All existing rows are guesses (no_pin means the row is absent)
        df['_is_guessed'] = True
        df['_is_nopin'] = False

    if has_clicked:
        df['_clicked_first'] = df['clicked_first'].apply(
            lambda x: True if str(x).strip() == 'True' else False)
    else:
        # Derive clicked_first from time_seconds: in each round, the player
        # with the lowest time_seconds clicked first (submitted earliest).
        # If all players have the same time (within 0.5s), nobody clicked.
        df['_clicked_first'] = False
        if 'time_seconds' in df.columns:
            for (gid, rnd), rdf in df.groupby(['game_id', 'round']):
                if len(rdf) < 2:
                    continue
                spread = rdf['time_seconds'].max() - rdf['time_seconds'].min()
                if spread < 0.5:
                    continue  # All timed out, nobody clicked
                min_time = rdf['time_seconds'].min()
                first_idx = rdf[rdf['time_seconds'] == min_time].index[0]
                df.loc[first_idx, '_clicked_first'] = True
        has_clicked = True  # We now have derived values

    # Count missing rows per player (= no_pin from absent rows)
    nopin_from_missing = {}
    for player in players:
        player_games = df[df['player_name'] == player]['game_id'].unique()
        rows_present = len(df[df['player_name'] == player])
        nopin_from_missing[player] = round_slots[player] - rows_present

    rows = []

    # Team aggregate first
    team_guessed_count = int(df['_is_guessed'].sum())
    team_nopin_from_status = int(df['_is_nopin'].sum())
    team_nopin_missing = sum(nopin_from_missing.values())
    team_total_nopin = team_nopin_from_status + team_nopin_missing
    team_clicked = int(df[df['_is_guessed'] & df['_clicked_first']].shape[0])
    team_total_slots = sum(round_slots.values())

    rows.append({
        'player_name': team_name,
        'clicked_first': team_clicked if has_clicked else '-',
        'guessed_not_first': (team_guessed_count - team_clicked) if has_clicked else '-',
        'no_pin': team_total_nopin,
        'rounds_guessed': team_guessed_count,
        'total_round_slots': team_total_slots,
        'initiative_rate': round(team_clicked / max(team_guessed_count, 1) * 100, 1) if has_clicked else '-',
        'participation_rate': round(team_guessed_count / max(team_total_slots, 1) * 100, 1),
    })

    # Per player
    for player_name in sorted(players):
        pdf = df[df['player_name'] == player_name]
        guessed_count = int(pdf['_is_guessed'].sum())
        nopin_status = int(pdf['_is_nopin'].sum())
        nopin_missing = nopin_from_missing.get(player_name, 0)
        total_nopin = nopin_status + nopin_missing
        clicked = int(pdf[pdf['_is_guessed'] & pdf['_clicked_first']].shape[0])
        total_slots = round_slots.get(player_name, len(pdf))

        rows.append({
            'player_name': player_name,
            'clicked_first': clicked if has_clicked else '-',
            'guessed_not_first': (guessed_count - clicked) if has_clicked else '-',
            'no_pin': total_nopin,
            'rounds_guessed': guessed_count,
            'total_round_slots': total_slots,
            'initiative_rate': round(clicked / max(guessed_count, 1) * 100, 1) if has_clicked else '-',
            'participation_rate': round(guessed_count / max(total_slots, 1) * 100, 1),
        })

    return pd.DataFrame(rows)


def guess_time_by_region(df: pd.DataFrame, team_name: str = 'Team') -> Optional[pd.DataFrame]:
    """Average time_remaining_sec per player per region."""
    if 'time_remaining_sec' not in df.columns or 'region' not in df.columns:
        return None

    df_guessed = df[df['status'] == 'guessed'] if 'status' in df.columns else df
    df_valid = df_guessed[(df_guessed['region'].notna()) &
                          (~df_guessed['region'].isin(['Unknown', 'Other']))].copy()
    df_valid['time_remaining_sec'] = pd.to_numeric(df_valid['time_remaining_sec'], errors='coerce')
    df_valid = df_valid.dropna(subset=['time_remaining_sec'])

    if df_valid.empty:
        return None

    # Team aggregate
    team_agg = df_valid.groupby('region')['time_remaining_sec'].mean().round(1)
    team_row = team_agg.to_frame().T
    team_row.index = [team_name]

    # Per player
    player_agg = df_valid.groupby(['player_name', 'region'])['time_remaining_sec'].mean().round(1)
    player_pivot = player_agg.reset_index().pivot(
        index='player_name', columns='region', values='time_remaining_sec')

    return pd.concat([team_row, player_pivot])


def guess_time_by_country(df: pd.DataFrame, n: int = 15) -> Optional[pd.DataFrame]:
    """Average time_remaining_sec per player per country (top N by frequency)."""
    if 'time_remaining_sec' not in df.columns or 'correct_country' not in df.columns:
        return None

    df_guessed = df[df['status'] == 'guessed'] if 'status' in df.columns else df
    df_valid = df_guessed[(df_guessed['correct_country'].notna()) &
                          (~df_guessed['correct_country'].isin(['Unknown', 'Lost at Sea']))].copy()
    df_valid['time_remaining_sec'] = pd.to_numeric(df_valid['time_remaining_sec'], errors='coerce')
    df_valid = df_valid.dropna(subset=['time_remaining_sec'])

    if df_valid.empty:
        return None

    # Get top N countries by frequency
    top_countries = df_valid['correct_country'].value_counts().head(n).index.tolist()
    df_top = df_valid[df_valid['correct_country'].isin(top_countries)]

    result = df_top.groupby(['player_name', 'correct_country']).agg(
        avg_time_remaining=('time_remaining_sec', 'mean'),
        guesses=('time_remaining_sec', 'count')
    ).round(1).reset_index()
    result = result.sort_values(['player_name', 'avg_time_remaining'], ascending=[True, False])
    return result


def fastest_slowest_guesses(df: pd.DataFrame, n: int = 10, team_name: str = 'Team') -> tuple:
    """Top N fastest and slowest guesses by team and per player."""
    if 'time_remaining_sec' not in df.columns:
        return None, None

    df_guessed = df[df['status'] == 'guessed'] if 'status' in df.columns else df
    df_valid = df_guessed.copy()
    df_valid['time_remaining_sec'] = pd.to_numeric(df_valid['time_remaining_sec'], errors='coerce')
    df_valid = df_valid.dropna(subset=['time_remaining_sec'])

    if df_valid.empty:
        return None, None

    cols = ['player_name', 'correct_country', 'distance_km', 'time_remaining_sec', 'game_id', 'round']

    # Team level: fastest = highest time_remaining
    fastest = df_valid.nlargest(n, 'time_remaining_sec')[cols]
    slowest = df_valid.nsmallest(n, 'time_remaining_sec')[cols]

    return fastest, slowest


def no_pin_analysis(df: pd.DataFrame, team_name: str = 'Team') -> Optional[pd.DataFrame]:
    """No-pin round analysis: frequency, loss rate, avg round duration.

    Detects no-pin from both 'status' column (if present) and missing player
    rows per round (player participated in a game but has no row for a round).
    """
    has_status = 'status' in df.columns and df['status'].notna().any()

    # Build expected round slots per player
    all_rounds = df.groupby('game_id')['round'].unique()
    players = sorted(df['player_name'].unique())

    # Find missing rounds per player (= no_pin from absent rows)
    nopin_rounds = {}  # player_name -> list of (game_id, round) tuples
    for player in players:
        player_games = df[df['player_name'] == player]['game_id'].unique()
        player_existing = set(
            zip(df[df['player_name'] == player]['game_id'],
                df[df['player_name'] == player]['round'])
        )
        missing = []
        for game_id in player_games:
            for rnd in all_rounds.get(game_id, []):
                if (game_id, rnd) not in player_existing:
                    missing.append((game_id, rnd))
        nopin_rounds[player] = missing

    # Also count status-based no_pin rows
    nopin_from_status = {}
    if has_status:
        for player in players:
            nopin_from_status[player] = len(
                df[(df['player_name'] == player) & (df['status'] == 'no_pin')]
            )
    else:
        nopin_from_status = {p: 0 for p in players}

    # Total no_pin per player
    total_nopin = {p: len(nopin_rounds[p]) + nopin_from_status[p] for p in players}

    if sum(total_nopin.values()) == 0:
        return None

    # Build lookup: (game_id, round) -> did our team lose that round?
    # A round is "lost" if game_won_bool is False for that game OR won_round is False
    has_won_round = 'won_round' in df.columns
    round_lost = {}
    if has_won_round:
        for (gid, rnd), rdf in df.groupby(['game_id', 'round']):
            # If any team member lost the round, the team lost the round
            round_lost[(gid, rnd)] = not rdf['won_round'].any()

    rows = []

    def _round_loss_pct(nopin_round_list):
        if not has_won_round or not nopin_round_list:
            return '-'
        lost = sum(1 for gr in nopin_round_list if round_lost.get(gr, False))
        return round(lost / len(nopin_round_list) * 100, 1)

    # Team aggregate
    team_total_nopin = sum(total_nopin.values())
    team_total_slots = sum(
        sum(len(all_rounds.get(g, [])) for g in df[df['player_name'] == p]['game_id'].unique())
        for p in players
    )
    all_nopin_rounds = [gr for p in players for gr in nopin_rounds[p]]
    rows.append({
        'player_name': team_name,
        'no_pin_count': team_total_nopin,
        'total_round_slots': team_total_slots,
        'no_pin_pct': round(team_total_nopin / max(team_total_slots, 1) * 100, 1),
        'round_loss_pct': _round_loss_pct(all_nopin_rounds),
    })

    # Per player
    for player in players:
        player_games = df[df['player_name'] == player]['game_id'].unique()
        player_slots = sum(len(all_rounds.get(g, [])) for g in player_games)
        nopin_count = total_nopin[player]
        if nopin_count == 0:
            continue
        rows.append({
            'player_name': player,
            'no_pin_count': nopin_count,
            'total_round_slots': player_slots,
            'no_pin_pct': round(nopin_count / max(player_slots, 1) * 100, 1),
            'round_loss_pct': _round_loss_pct(nopin_rounds[player]),
        })

    return pd.DataFrame(rows)


def no_pin_by_region(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """No-pin breakdown by region per player."""
    if 'region' not in df.columns:
        return None

    if 'status' in df.columns:
        df_nopin = df[(df['status'] == 'no_pin') &
                      (df['region'].notna()) &
                      (~df['region'].isin(['Unknown', 'Other']))]
        if df_nopin.empty:
            return None
        result = df_nopin.groupby(['player_name', 'region']).size().reset_index(name='no_pin_count')
        result = result.sort_values(['player_name', 'no_pin_count'], ascending=[True, False])
        return result

    # Without status column, we can't identify no-pin by region
    # (missing rows don't have region data)
    return None


def hesitation_index(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Team coordination latency: time between first and last guess per round."""
    if 'time_remaining_sec' not in df.columns:
        return None

    df_guessed = df[df['status'] == 'guessed'] if 'status' in df.columns else df
    df_valid = df_guessed.copy()
    df_valid['time_remaining_sec'] = pd.to_numeric(df_valid['time_remaining_sec'], errors='coerce')
    df_valid = df_valid.dropna(subset=['time_remaining_sec'])

    if df_valid.empty:
        return None

    # Per round: max(time_remaining) - min(time_remaining) = gap between first and last click
    round_gap = df_valid.groupby(['game_id', 'round']).agg(
        first_click=('time_remaining_sec', 'max'),
        last_click=('time_remaining_sec', 'min'),
        num_guessers=('time_remaining_sec', 'count')
    )
    round_gap['hesitation_sec'] = (round_gap['first_click'] - round_gap['last_click']).round(2)
    round_gap = round_gap[round_gap['num_guessers'] > 1]  # Only rounds with 2+ guessers

    if round_gap.empty:
        return None

    result = round_gap.reset_index()
    result = result[['game_id', 'round', 'hesitation_sec', 'first_click', 'last_click']]
    return result


def pressure_response(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Performance after winning vs losing the previous round."""
    if 'won_round' not in df.columns or 'game_date_parsed' not in df.columns:
        return None

    df_guessed = df[df['status'] == 'guessed'] if 'status' in df.columns else df
    df_sorted = df_guessed.sort_values(['game_id', 'round']).copy()
    if df_sorted.empty:
        return None

    rows = []
    for player_name, pdf in df_sorted.groupby('player_name'):
        for game_id, gdf in pdf.groupby('game_id'):
            gdf = gdf.sort_values('round')
            rounds_list = gdf['round'].unique()
            for i in range(1, len(rounds_list)):
                prev_round = gdf[gdf['round'] == rounds_list[i - 1]]
                curr_round = gdf[gdf['round'] == rounds_list[i]]
                if prev_round.empty or curr_round.empty:
                    continue
                prev_won = prev_round.iloc[0].get('won_round', None)
                curr_dist = curr_round.iloc[0].get('distance_km', None)
                if prev_won is not None and curr_dist is not None:
                    rows.append({
                        'player_name': player_name,
                        'prev_outcome': 'Won' if prev_won else 'Lost',
                        'distance_km': curr_dist,
                    })

    if not rows:
        return None

    result_df = pd.DataFrame(rows)
    summary = result_df.groupby(['player_name', 'prev_outcome']).agg(
        avg_dist_km=('distance_km', 'mean'),
        rounds=('distance_km', 'count')
    ).round(1).reset_index()
    return summary


# ===================================================================
# Trend export
# ===================================================================

def generate_trend_data(df: pd.DataFrame) -> dict:
    """Generate chronological trend data for LLM analysis"""
    if 'game_date_parsed' not in df.columns or df['game_date_parsed'].isna().all():
        print("  No game_date data available for trend export.")
        return {}

    df_sorted = df.sort_values('game_date_parsed')

    trend = {
        'generated_at': datetime.now().isoformat(),
        'total_games': int(df['game_id'].nunique()),
        'total_guesses': len(df),
        'date_range': {
            'first_game': str(df_sorted['game_date_parsed'].min()),
            'last_game': str(df_sorted['game_date_parsed'].max()),
        },
        'players': {},
        'team': {},
    }

    for player_name in df['player_name'].unique():
        pdf = df_sorted[df_sorted['player_name'] == player_name]
        games = []
        for game_id, gdf in pdf.groupby('game_id', sort=False):
            entry = {
                'game_id': game_id,
                'date': str(gdf['game_date_parsed'].iloc[0]) if gdf['game_date_parsed'].notna().any() else None,
                'rounds': len(gdf),
                'avg_distance_km': round(float(gdf['distance_km'].mean()), 2),
                'avg_time_sec': round(float(gdf['time_seconds'].mean()), 2),
            }
            if 'correct_country_flag' in gdf.columns and gdf['correct_country_flag'].notna().any():
                entry['correct_country_pct'] = round(float(gdf['correct_country_flag'].mean() * 100), 1)
            if 'game_won_bool' in gdf.columns and gdf['game_won_bool'].notna().any():
                entry['won'] = bool(gdf['game_won_bool'].iloc[0])
            games.append(entry)

        player_data = {'games': games, 'total_guesses': len(pdf)}

        # Rolling averages (5-game window)
        if len(games) >= 5:
            distances = [g['avg_distance_km'] for g in games]
            rolling = []
            for i in range(4, len(distances)):
                window = distances[i - 4:i + 1]
                rolling.append(round(sum(window) / 5, 2))
            player_data['rolling_avg_distance_5game'] = rolling

        trend['players'][player_name] = player_data

    # Team-level trends
    team_games = []
    for game_id, gdf in df_sorted.groupby('game_id', sort=False):
        entry = {
            'game_id': game_id,
            'date': str(gdf['game_date_parsed'].iloc[0]) if gdf['game_date_parsed'].notna().any() else None,
            'team_avg_distance_km': round(float(gdf['distance_km'].mean()), 2),
            'team_avg_time_sec': round(float(gdf['time_seconds'].mean()), 2),
        }
        if 'game_won_bool' in gdf.columns and gdf['game_won_bool'].notna().any():
            entry['won'] = bool(gdf['game_won_bool'].iloc[0])
        team_games.append(entry)

    trend['team'] = {'games': team_games}

    # Recent vs all-time comparison
    rva = recent_vs_alltime(df)
    if rva is not None:
        trend['recent_vs_alltime'] = rva.to_dict(orient='records')

    # Competitive advantage
    comp_adv = competitive_advantage(df)
    if comp_adv is not None:
        trend['competitive_advantage'] = comp_adv.to_dict(orient='records')

    return trend


# ===================================================================
# Main
# ===================================================================

def print_section(title: str, subtitle: str = ''):
    print(f"\n{'=' * 60}")
    print(f"{title}")
    if subtitle:
        print(f"({subtitle})")
    print("=" * 60)


def _team_first_order(names, team_name: str = 'Team') -> list:
    """Return names with team_name first, then remaining sorted alphabetically."""
    others = sorted([n for n in names if n != team_name])
    if team_name in names:
        return [team_name] + others
    return others


def main():
    parser = argparse.ArgumentParser(description='Analyze GeoGuessr team duel stats')
    parser.add_argument('csv_file', help='CSV file with stats')
    parser.add_argument('--player', help='Player ID for player-specific analysis')
    parser.add_argument('--export', help='Export analysis to CSV files in this directory')
    parser.add_argument('--trend-export', type=str, default=None,
                        help='Export chronological trend data to JSON for LLM analysis')
    args = parser.parse_args()

    print("\U0001f4ca GeoGuessr Team Duel Stats Analysis")
    print("=" * 60)

    df_all = load_data(args.csv_file)
    print(f"\n\u2705 Loaded {len(df_all)} guesses from {df_all['game_id'].nunique()} games")

    # Filter to my team only (opponents only used for competitive_advantage)
    my_team_pids = detect_my_team(df_all)
    df = df_all[df_all['player_id'].isin(my_team_pids)].copy()

    team_player_names = sorted(df['player_name'].unique())
    print(f"   Team players: {', '.join(team_player_names)}")
    if len(df) < len(df_all):
        n_opponents = df_all['player_id'].nunique() - len(my_team_pids)
        print(f"   ({n_opponents} opponent player(s) filtered from summaries)")

    # ---- Player Summary ----
    summary = player_summary(df)
    dist_cols = ['player_id', 'player_name', 'avg_dist_km', 'median_dist_km',
                 'best_dist_km', 'worst_dist_km', 'std_dist_km', 'total_guesses',
                 'correct_country_pct']
    dist_cols = [c for c in dist_cols if c in summary.columns]
    print_section("\U0001f4c8 PLAYER SUMMARY — DISTANCE")
    print(summary[dist_cols].to_string(index=False))

    other_cols = ['player_name', 'avg_time_sec', 'median_time_sec',
                  'times_guess_clicked', 'no_pin_count']
    other_cols = [c for c in other_cols if c in summary.columns]
    print_section("\U0001f4c8 PLAYER SUMMARY — TIMING")
    print(summary[other_cols].to_string(index=False))

    # ---- Accuracy Ranking ----
    print_section("\U0001f3af ACCURACY RANKING", "by average distance, lower is better")
    acc_rank = accuracy_ranking(df)
    print(acc_rank.to_string(index=False))

    # ---- Speed Ranking ----
    print_section("\u26a1 SPEED RANKING", "by average time in seconds, lower is faster")
    speed_rank = speed_ranking(df)
    if not speed_rank.empty:
        print(speed_rank.to_string(index=False))

    # ---- Speed vs Accuracy ----
    print_section("\u2696\ufe0f  SPEED VS ACCURACY")
    efficiency = speed_vs_accuracy(df)
    if not efficiency.empty:
        print(efficiency.to_string(index=False))

    # ---- Recent vs All-Time ----
    rva = recent_vs_alltime(df)
    if rva is not None:
        print_section("\U0001f4c5 RECENT VS ALL-TIME",
                      "last 10 games vs all-time. \033[32m\u25cf\033[0m=improving, \033[31m\u25cf\033[0m=declining")
        for player in rva['player'].unique():
            pdata = rva[rva['player'] == player]
            # Manual formatting to handle ANSI codes breaking alignment
            cols = ['period', 'games', 'avg_dist_km', 'avg_time_sec', 'country_acc_%', 'win_rate_%']
            header = f"  {'period':>8} {'games':>5} {'avg_dist_km':>13} {'avg_time_sec':>14} {'country_acc_%':>15} {'win_rate_%':>12}"
            print(f"\n  {player}:")
            print(header)
            for _, row in pdata.iterrows():
                vals = []
                for c in cols:
                    v = row.get(c, '')
                    vals.append(str(v) if v != '' and pd.notna(v) else '')
                # period is fixed width, rest are right-aligned
                line = f"  {vals[0]:>8} {vals[1]:>5} {vals[2]:>13} {vals[3]:>14} {vals[4]:>15} {vals[5]:>12}"
                print(line)

    # ---- Team Stats Summary ----
    team_stats = team_stats_summary(df)
    if team_stats is not None:
        print_section("\U0001f3c6 TEAM STATS SUMMARY", "avg/worst distance and avg time, split by win vs loss")
        print(team_stats.to_string(index=False))

    # ---- Player Win/Loss Split ----
    wl_split = player_win_loss_split(df)
    if wl_split is not None:
        print_section("\U0001f4ca PLAYER WIN/LOSS SPLIT", "avg distance in wins vs losses")
        print(wl_split.to_string(index=False))

    # ---- Won Team (within-team) ----
    wt = won_team_stats(df)
    if wt is not None:
        print_section("\U0001f91d WON TEAM (WITHIN-TEAM)", "how often each player beat their teammate")
        print(wt.to_string(index=False))

        wt_move = won_team_stats(df, by_move_mode=True)
        if wt_move is not None and 'move_mode' in wt_move.columns:
            print(f"\n  By move mode:")
            print(wt_move.to_string(index=False))

    # ---- Won Round (cross-team) ----
    wr = won_round_stats(df)
    if wr is not None:
        print_section("\U0001f451 WON ROUND (CROSS-TEAM)", "how often each player had the best guess in the round")
        print(wr.to_string(index=False))

        wr_move = won_round_stats(df, by_move_mode=True)
        if wr_move is not None and 'move_mode' in wr_move.columns:
            print(f"\n  By move mode:")
            print(wr_move.to_string(index=False))

    # ---- Region Performance ----
    print_section("\U0001f30d PERFORMANCE BY REGION",
                  "Team: avg dist as % of region span. Players: km. Lower is better.")
    region_perf = region_performance(df)
    if not region_perf.empty:
        print(region_perf.round(1).to_string())

    # ---- Move vs No-Move ----
    mvm = move_vs_nomove(df)
    if mvm is not None:
        print_section("\U0001f3ae MOVE VS NO-MOVE COMPARISON",
                      "distance, time, country accuracy across game modes")
        print(mvm.to_string(index=False))

    # ---- Countries I Confuse ----
    print_section("\U0001f500 COUNTRIES I CONFUSE",
                  "When it was X, I guessed Y — top 10 per player")
    confusion = countries_i_confuse(df)
    if not confusion.empty:
        top_per_player = confusion.groupby('player_name').head(10)
        # Team first, then players sorted by name
        for player in _team_first_order(top_per_player['player_name'].unique(), 'Team'):
            pdata = top_per_player[top_per_player['player_name'] == player]
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'guessed_country', 'times']].to_string(index=False))

    # ---- Closest/Furthest Countries (all guesses) ----
    best, worst = best_worst_countries(df, n=10)
    if not best.empty:
        print_section("\u2b50 CLOSEST COUNTRIES", "lowest avg distance (all guesses), min 3 guesses")
        for player in _team_first_order(best['player_name'].unique(), 'Team'):
            pdata = best[best['player_name'] == player]
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_guesses']].to_string(index=False))
    if not worst.empty:
        print_section("\U0001f4a9 FURTHEST COUNTRIES", "highest avg distance (all guesses), min 3 guesses")
        for player in _team_first_order(worst['player_name'].unique(), 'Team'):
            pdata = worst[worst['player_name'] == player]
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_guesses']].to_string(index=False))

    # ---- Best/Worst In-Country (correct country only) ----
    bic, wic = best_worst_in_country(df, n=10)
    if not bic.empty:
        print_section("\U0001f3af BEST IN-COUNTRY", "closest to location when we got the right country, min 3")
        for player in _team_first_order(bic['player_name'].unique(), 'Team'):
            pdata = bic[bic['player_name'] == player]
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_correct']].to_string(index=False))
    if not wic.empty:
        print_section("\U0001f4cd WORST IN-COUNTRY", "furthest from location when we got the right country, min 3")
        for player in _team_first_order(wic['player_name'].unique(), 'Team'):
            pdata = wic[wic['player_name'] == player]
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_correct']].to_string(index=False))

    # ---- Countries Worth Studying ----
    worth = countries_worth_studying(df)
    if not worth.empty:
        print_section("\U0001f4d6 COUNTRIES WORTH STUDYING",
                      "importance = (avg_dist_km / area_km2) * log(1 + num_guesses)")
        print(worth.to_string(index=False))

    # ---- Competitive Advantage ----
    comp_adv = competitive_advantage(df_all)
    if comp_adv is not None:
        print_section("\u2694\ufe0f  COMPETITIVE ADVANTAGE",
                      "positive advantage_km = we're closer. Sorted by opponent distance.")
        # Countries we WIN: advantage_km > 0, sorted by opp distance desc (they struggled most)
        our_wins = comp_adv[comp_adv['advantage_km'] > 0].sort_values('opp_avg_dist', ascending=False)
        if len(our_wins) > 0:
            print(f"\n  Countries we dominate (top {min(5, len(our_wins))}, by opponent distance):")
            print(our_wins.head(5).to_string(index=False))
        else:
            print("\n  No countries where we outperform opponents (need more opponent data)")
        # Countries we LOSE: advantage_km < 0, sorted by opp distance asc (they were closest)
        our_losses = comp_adv[comp_adv['advantage_km'] < 0].sort_values('opp_avg_dist', ascending=True)
        if len(our_losses) > 0:
            print(f"\n  Countries opponents dominate (top {min(5, len(our_losses))}, by opponent distance):")
            print(our_losses.head(5).to_string(index=False))

    # ---- Rounds Played Trend ----
    rpt = rounds_played_trend(df)
    if rpt is not None:
        print_section("\U0001f4c9 ROUNDS PLAYED TREND",
                      "how many rounds per game, win/loss round counts")
        print(rpt.to_string(index=False))

    # ---- Initiative Summary ----
    init_summary = initiative_summary(df)
    if init_summary is not None:
        print_section("\U0001f3af INITIATIVE SUMMARY",
                      "who clicks first, participation rates")
        print(init_summary.to_string(index=False))

    # ---- No-Pin Analysis ----
    nopin = no_pin_analysis(df)
    if nopin is not None:
        print_section("\u274c NO-PIN ANALYSIS",
                      "rounds where player did not drop a pin")
        print(nopin.to_string(index=False))

        nopin_region = no_pin_by_region(df)
        if nopin_region is not None:
            print(f"\n  By region:")
            print(nopin_region.to_string(index=False))

    # ---- Guess Speed by Region ----
    speed_region = guess_time_by_region(df)
    if speed_region is not None:
        print_section("\u23f1\ufe0f  GUESS SPEED BY REGION",
                      "avg time remaining (sec) when guess submitted. Higher = faster.")
        print(speed_region.round(1).to_string())

    # ---- Fastest/Slowest Guesses ----
    fastest, slowest = fastest_slowest_guesses(df) if 'time_remaining_sec' in df.columns else (None, None)
    if fastest is not None:
        print_section("\u26a1 FASTEST GUESSES", "highest time remaining = clicked earliest")
        print(fastest.to_string(index=False))
    if slowest is not None:
        print(f"\n  Slowest guesses (lowest time remaining):")
        print(slowest.to_string(index=False))

    # ---- Hesitation Index ----
    hes = hesitation_index(df)
    if hes is not None:
        print_section("\U0001f914 HESITATION INDEX",
                      "gap between first and last guess per round (team coordination)")
        avg_hes = hes['hesitation_sec'].mean()
        print(f"  Average hesitation: {avg_hes:.1f}s across {len(hes)} rounds")
        print(f"\n  Most hesitation:")
        print(hes.nlargest(5, 'hesitation_sec').to_string(index=False))

    # ---- Pressure Response ----
    pressure = pressure_response(df)
    if pressure is not None:
        print_section("\U0001f4aa PRESSURE RESPONSE",
                      "avg distance after winning vs losing the previous round")
        print(pressure.to_string(index=False))

    # ---- Player-specific analysis ----
    if args.player:
        print_section(f"\U0001f464 PLAYER ANALYSIS: {args.player}")
        player_df = df[df['player_id'] == args.player]
        if len(player_df) == 0:
            print(f"  No data for player {args.player}")
        else:
            print(f"\nTotal guesses: {len(player_df)}")
            print(f"Average distance: {player_df['distance_km'].mean():.2f} km")
            print(f"Median distance: {player_df['distance_km'].median():.2f} km")

            print("\n\U0001f4cd Best Countries:")
            cp = country_performance(df, args.player)
            if not cp.empty:
                print(cp.head(10).to_string(index=False))

    # ---- Export CSVs ----
    if args.export:
        export_dir = Path(args.export)
        export_dir.mkdir(exist_ok=True)

        summary.to_csv(export_dir / 'player_summary.csv', index=False)
        acc_rank.to_csv(export_dir / 'accuracy_ranking.csv', index=False)
        if not speed_rank.empty:
            speed_rank.to_csv(export_dir / 'speed_ranking.csv', index=False)
        if not efficiency.empty:
            efficiency.to_csv(export_dir / 'speed_vs_accuracy.csv', index=False)
        if not region_perf.empty:
            region_perf.to_csv(export_dir / 'region_performance.csv')
        if not confusion.empty:
            confusion.to_csv(export_dir / 'countries_i_confuse.csv', index=False)
        if team_stats is not None:
            team_stats.to_csv(export_dir / 'team_stats_summary.csv', index=False)
        if wl_split is not None:
            wl_split.to_csv(export_dir / 'player_win_loss_split.csv', index=False)
        if wt is not None:
            wt.to_csv(export_dir / 'won_team_stats.csv', index=False)
        if wr is not None:
            wr.to_csv(export_dir / 'won_round_stats.csv', index=False)
        if not worst.empty:
            worst.to_csv(export_dir / 'worst_countries.csv', index=False)
        if not best.empty:
            best.to_csv(export_dir / 'best_countries.csv', index=False)
        if not worth.empty:
            worth.to_csv(export_dir / 'countries_worth_studying.csv', index=False)
        if rva is not None:
            rva.to_csv(export_dir / 'recent_vs_alltime.csv', index=False)
        if comp_adv is not None:
            comp_adv.to_csv(export_dir / 'competitive_advantage.csv', index=False)

        print(f"\n\U0001f4be Analysis exported to {export_dir}/")

    # ---- Trend Export ----
    if args.trend_export:
        print(f"\n\U0001f4c8 Generating trend data...")
        trend = generate_trend_data(df)
        if trend:
            with open(args.trend_export, 'w') as f:
                json.dump(trend, f, indent=2, default=str)
            print(f"\u2705 Trend data exported to {args.trend_export}")


if __name__ == '__main__':
    main()
