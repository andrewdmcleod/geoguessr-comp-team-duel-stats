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

from country_codes import LARGE_COUNTRIES


# ===================================================================
# Data loading
# ===================================================================

def detect_my_team(df: pd.DataFrame) -> set:
    """Detect which players are on 'my team' using the most common team_key.

    Returns set of player_ids belonging to the primary team.
    """
    if 'team_key' not in df.columns:
        return set(df['player_id'].unique())

    team_keys = df['team_key'].value_counts()
    if team_keys.empty:
        return set(df['player_id'].unique())

    my_team_key = team_keys.index[0]
    my_team_pids = set(df[df['team_key'] == my_team_key]['player_id'].unique())
    return my_team_pids


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

    return df


# ===================================================================
# Analysis functions
# ===================================================================

def player_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Overall player statistics"""
    summary = df.groupby(['player_id', 'player_name']).agg({
        'distance_km': ['mean', 'median', 'min', 'max', 'std'],
        'time_seconds': ['mean', 'median'],
        'game_id': 'count'
    }).round(2)

    summary.columns = ['avg_dist_km', 'median_dist_km', 'best_dist_km',
                       'worst_dist_km', 'std_dist_km', 'avg_time_sec',
                       'median_time_sec', 'total_guesses']

    # Add correct country % if available
    if 'correct_country_flag' in df.columns:
        pct = df[df['correct_country_flag'].notna()].groupby('player_id')['correct_country_flag'].mean() * 100
        summary = summary.join(pct.rename('correct_country_pct').round(1))

    summary = summary.sort_values('avg_dist_km').reset_index()
    return summary


def accuracy_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Rank players by accuracy"""
    ranking = df.groupby(['player_id', 'player_name']).agg({
        'distance_km': 'mean', 'game_id': 'count'
    }).round(2)
    ranking.columns = ['avg_distance_km', 'num_guesses']
    ranking = ranking.sort_values('avg_distance_km').reset_index()
    ranking.insert(0, 'rank', range(1, len(ranking) + 1))
    return ranking


def speed_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Rank players by speed.

    Excludes likely timeout/no-pin guesses:
    - time_seconds == 0 (no data)
    - score == 0 (no meaningful guess placed)
    - distance > 10,000 km (effectively random / no pin)
    """
    df_timed = df[df['time_seconds'] > 0]

    # Exclude score == 0 (timeout / no pin) if score column exists
    if 'score' in df_timed.columns:
        df_timed = df_timed[df_timed['score'] > 0]

    # Exclude extreme distances (> 10,000 km suggests no meaningful guess)
    if 'distance_km' in df_timed.columns:
        df_timed = df_timed[df_timed['distance_km'] <= 10000]

    if len(df_timed) == 0:
        return pd.DataFrame()

    ranking = df_timed.groupby(['player_id', 'player_name']).agg({
        'time_seconds': 'mean', 'game_id': 'count'
    }).round(2)
    ranking.columns = ['avg_time_sec', 'num_guesses']
    ranking = ranking.sort_values('avg_time_sec').reset_index()
    ranking.insert(0, 'rank', range(1, len(ranking) + 1))
    return ranking


def speed_vs_accuracy(df: pd.DataFrame) -> pd.DataFrame:
    """Compare speed vs accuracy (excludes timeouts/no-pin guesses)"""
    df_timed = df[df['time_seconds'] > 0]
    if 'score' in df_timed.columns:
        df_timed = df_timed[df_timed['score'] > 0]
    if 'distance_km' in df_timed.columns:
        df_timed = df_timed[df_timed['distance_km'] <= 10000]
    if len(df_timed) == 0:
        return pd.DataFrame()

    analysis = df_timed.groupby(['player_id', 'player_name']).agg({
        'time_seconds': 'mean', 'distance_km': 'mean', 'game_id': 'count'
    }).round(2)
    analysis.columns = ['avg_time_sec', 'avg_distance_km', 'num_guesses']

    if len(analysis) > 1:
        time_norm = (analysis['avg_time_sec'] - analysis['avg_time_sec'].min()) / \
                    max((analysis['avg_time_sec'].max() - analysis['avg_time_sec'].min()), 1)
        dist_norm = (analysis['avg_distance_km'] - analysis['avg_distance_km'].min()) / \
                    max((analysis['avg_distance_km'].max() - analysis['avg_distance_km'].min()), 1)
        analysis['efficiency_score'] = ((time_norm + dist_norm) / 2 * 100).round(2)
    else:
        analysis['efficiency_score'] = 50.0

    return analysis.sort_values('efficiency_score').reset_index()


def team_stats_summary(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Team-level stats split by win/loss"""
    if 'game_won_bool' not in df.columns or df['game_won_bool'].isna().all():
        return None

    rows = []
    for outcome, label in [(True, 'Win'), (False, 'Loss')]:
        subset = df[df['game_won_bool'] == outcome]
        if len(subset) == 0:
            continue
        rows.append({
            'outcome': label,
            'games': subset['game_id'].nunique(),
            'avg_dist_km': round(subset['distance_km'].mean(), 2),
            'worst_dist_km': round(subset['distance_km'].max(), 2),
            'avg_time_sec': round(subset['time_seconds'].mean(), 2),
        })

    return pd.DataFrame(rows) if rows else None


def player_win_loss_split(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Per-player average distance in wins vs losses"""
    if 'game_won_bool' not in df.columns or df['game_won_bool'].isna().all():
        return None

    result = df[df['game_won_bool'].notna()].groupby(
        ['player_name', 'game_won_bool']
    )['distance_km'].mean().round(2).unstack(fill_value=0)

    cols = {}
    if False in result.columns:
        cols['avg_dist_loss'] = result[False]
    if True in result.columns:
        cols['avg_dist_win'] = result[True]

    if not cols:
        return None

    out = pd.DataFrame(cols)
    return out.reset_index()


def won_team_stats(df: pd.DataFrame, by_move_mode: bool = False) -> Optional[pd.DataFrame]:
    """How often each player beat their teammate.

    If by_move_mode=True, returns a separate breakdown per move mode.
    """
    if 'won_team' not in df.columns or df['won_team'].isna().all():
        return None

    valid = df[df['won_team'].notna()]
    group_cols = ['player_name']
    if by_move_mode and 'move_mode' in valid.columns:
        group_cols.append('move_mode')

    result = valid.groupby(group_cols).agg(
        rounds_won=('won_team', 'sum'),
        total_rounds=('won_team', 'count')
    )
    result['win_pct'] = (result['rounds_won'] / result['total_rounds'] * 100).round(1)
    return result.reset_index().sort_values(group_cols + ['win_pct'], ascending=[True] * len(group_cols) + [False])


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


def region_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Per-region performance by player"""
    if 'region' not in df.columns:
        return pd.DataFrame()

    df_valid = df[df['region'].notna() & (df['region'] != 'Unknown') & (df['region'] != 'Other')]
    if len(df_valid) == 0:
        return pd.DataFrame()

    perf = df_valid.groupby(['player_name', 'region']).agg({
        'distance_km': 'mean', 'game_id': 'count'
    }).round(2)
    perf.columns = ['avg_distance_km', 'num_guesses']
    perf = perf.reset_index()
    return perf.pivot(index='player_name', columns='region', values='avg_distance_km')


def best_worst_countries(df: pd.DataFrame, n: int = 10) -> tuple:
    """Best and worst countries per player"""
    if 'correct_country' not in df.columns:
        return pd.DataFrame(), pd.DataFrame()

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame(), pd.DataFrame()

    perf = df_valid.groupby(['player_name', 'correct_country']).agg({
        'distance_km': ['mean', 'count']
    }).round(2)
    perf.columns = ['avg_dist_km', 'num_guesses']
    perf = perf[perf['num_guesses'] >= 2].reset_index()

    best = perf.sort_values('avg_dist_km').groupby('player_name').head(n)
    best = best.sort_values(['player_name', 'avg_dist_km'])
    worst = perf.sort_values('avg_dist_km', ascending=False).groupby('player_name').head(n)
    worst = worst.sort_values(['player_name', 'avg_dist_km'], ascending=[True, False])
    return best, worst


def countries_i_confuse(df: pd.DataFrame) -> pd.DataFrame:
    """Countries I keep thinking are other countries"""
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

    confusion = df_valid.groupby(
        ['player_name', 'correct_country', 'guessed_country']
    ).size().reset_index(name='times')
    confusion = confusion.sort_values(['player_name', 'times'], ascending=[True, False])
    return confusion


def countries_worth_studying(df: pd.DataFrame, n: int = 15) -> pd.DataFrame:
    """Worst countries that are also geographically large"""
    if 'correct_country' not in df.columns:
        return pd.DataFrame()

    df_valid = df[df['correct_country'].notna() & ~df['correct_country'].isin(['Unknown', 'Lost at Sea'])]
    if len(df_valid) == 0:
        return pd.DataFrame()

    perf = df_valid.groupby('correct_country').agg({
        'distance_km': ['mean', 'count']
    }).round(2)
    perf.columns = ['avg_dist_km', 'num_guesses']
    perf = perf[perf['num_guesses'] >= 2].reset_index()

    perf['area_km2'] = perf['correct_country'].map(LARGE_COUNTRIES)
    perf = perf.dropna(subset=['area_km2'])
    perf['area_km2'] = perf['area_km2'].astype(int)

    # Importance weight: combines how badly you perform with how often the country appears.
    # Higher = more worth studying. Uses log(num_guesses) so a country appearing 100x
    # isn't weighted 50x more than one appearing 2x, but still significantly more.
    perf['importance'] = (perf['avg_dist_km'] * perf['num_guesses'].apply(math.log1p)).round(1)

    perf = perf.sort_values('importance', ascending=False).head(n)
    return perf


def move_vs_nomove(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Compare performance across move modes"""
    if 'move_mode' not in df.columns or df['move_mode'].nunique() < 2:
        return None

    aggs = {'distance_km': 'mean', 'time_seconds': 'mean', 'game_id': 'count'}

    comparison = df.groupby(['player_name', 'move_mode']).agg(aggs).round(2)
    comparison.columns = ['avg_dist_km', 'avg_time_sec', 'num_guesses']

    if 'correct_country_flag' in df.columns:
        pct = df[df['correct_country_flag'].notna()].groupby(
            ['player_name', 'move_mode']
        )['correct_country_flag'].mean().round(3) * 100
        comparison = comparison.join(pct.rename('correct_country_pct'))

    return comparison.reset_index()


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
                return '  \u2796'  # ➖ stable
            # Determine if the change is an improvement
            if lower_is_better:
                improving = diff < 0
            else:
                improving = diff > 0
            icon = '\U0001f4c8' if improving else '\U0001f4c9'  # 📈 improving, 📉 declining
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

    if 'team_key' in df.columns:
        # Use the most common team_key as "my team"
        team_keys = df['team_key'].value_counts()
        if len(team_keys) == 0:
            return None
        my_team_key = team_keys.index[0]
        my_team_players = set(df[df['team_key'] == my_team_key]['player_id'].unique())
    else:
        # Fallback: if won_team is present, players with won_team data are on my team
        if 'won_team' not in df.columns:
            return None
        my_team_players = set(df[df['won_team'].notna()]['player_id'].unique())

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
    print_section("\U0001f4c8 PLAYER SUMMARY")
    summary = player_summary(df)
    print(summary.to_string(index=False))

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
                      "last 10 games vs all-time. \U0001f4c8=improving, \U0001f4c9=declining")
        for player in rva['player'].unique():
            pdata = rva[rva['player'] == player]
            print(f"\n  {player}:")
            print(pdata.drop(columns=['player']).to_string(index=False))

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
                  "Average distance in km per player per continent. Lower is better.")
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
        for player, pdata in top_per_player.groupby('player_name', sort=True):
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'guessed_country', 'times']].to_string(index=False))

    # ---- Best/Worst Countries ----
    best, worst = best_worst_countries(df, n=10)
    if not best.empty:
        print_section("\u2b50 BEST COUNTRIES", "lowest avg distance per player, min 2 guesses")
        for player, pdata in best.groupby('player_name', sort=True):
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_guesses']].to_string(index=False))
    if not worst.empty:
        print_section("\U0001f4a9 WORST COUNTRIES", "highest avg distance per player, min 2 guesses")
        for player, pdata in worst.groupby('player_name', sort=True):
            print(f"\n  {player}:")
            print(pdata[['correct_country', 'avg_dist_km', 'num_guesses']].to_string(index=False))

    # ---- Countries Worth Studying ----
    worth = countries_worth_studying(df)
    if not worth.empty:
        print_section("\U0001f4d6 COUNTRIES WORTH STUDYING",
                      "Worst performance + geographically large = worth learning regional clues")
        print(worth.to_string(index=False))

    # ---- Competitive Advantage ----
    comp_adv = competitive_advantage(df_all)
    if comp_adv is not None:
        print_section("\u2694\ufe0f  COMPETITIVE ADVANTAGE",
                      "countries where you outperform opponents (positive = your advantage)")
        n_show = min(10, len(comp_adv))
        print(f"\n  Top {n_show} advantages (you're better):")
        print(comp_adv.head(n_show).to_string(index=False))
        n_bottom = min(10, len(comp_adv))
        print(f"\n  Top {n_bottom} disadvantages (opponents are better):")
        print(comp_adv.tail(n_bottom).to_string(index=False))

    # ---- Rounds Played Trend ----
    rpt = rounds_played_trend(df)
    if rpt is not None:
        print_section("\U0001f4c9 ROUNDS PLAYED TREND",
                      "how many rounds per game, win/loss round counts")
        print(rpt.to_string(index=False))

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
