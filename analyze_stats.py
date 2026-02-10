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
    """Rank players by speed"""
    df_timed = df[df['time_seconds'] > 0]
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
    """Compare speed vs accuracy"""
    df_timed = df[df['time_seconds'] > 0]
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


def won_team_stats(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """How often each player beat their teammate"""
    if 'won_team' not in df.columns or df['won_team'].isna().all():
        return None

    valid = df[df['won_team'].notna()]
    result = valid.groupby('player_name').agg(
        rounds_won=('won_team', 'sum'),
        total_rounds=('won_team', 'count')
    )
    result['win_pct'] = (result['rounds_won'] / result['total_rounds'] * 100).round(1)
    return result.reset_index().sort_values('win_pct', ascending=False)


def won_round_stats(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """How often each player had the best guess across all teams"""
    if 'won_round' not in df.columns or df['won_round'].isna().all():
        return None

    valid = df[df['won_round'].notna()]
    result = valid.groupby('player_name').agg(
        rounds_won=('won_round', 'sum'),
        total_rounds=('won_round', 'count')
    )
    result['win_pct'] = (result['rounds_won'] / result['total_rounds'] * 100).round(1)
    return result.reset_index().sort_values('win_pct', ascending=False)


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
    worst = perf.sort_values('avg_dist_km', ascending=False).groupby('player_name').head(n)
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
    confusion = confusion.sort_values('times', ascending=False)
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
    perf = perf.sort_values('avg_dist_km', ascending=False).head(n)
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

    df = load_data(args.csv_file)
    print(f"\n\u2705 Loaded {len(df)} guesses from {df['game_id'].nunique()} games")
    print(f"   Players: {', '.join(df['player_name'].unique())}")

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

    # ---- Won Round (cross-team) ----
    wr = won_round_stats(df)
    if wr is not None:
        print_section("\U0001f451 WON ROUND (CROSS-TEAM)", "how often each player had the best guess in the round")
        print(wr.to_string(index=False))

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
                  "When it was X, I guessed Y — top 20")
    confusion = countries_i_confuse(df)
    if not confusion.empty:
        print(confusion.head(20).to_string(index=False))

    # ---- Best/Worst Countries ----
    best, worst = best_worst_countries(df, n=10)
    if not best.empty:
        print_section("\u2b50 BEST COUNTRIES", "lowest avg distance per player, min 2 guesses")
        print(best.to_string(index=False))
    if not worst.empty:
        print_section("\U0001f4a9 WORST COUNTRIES", "highest avg distance per player, min 2 guesses")
        print(worst.to_string(index=False))

    # ---- Countries Worth Studying ----
    worth = countries_worth_studying(df)
    if not worth.empty:
        print_section("\U0001f4d6 COUNTRIES WORTH STUDYING",
                      "Worst performance + geographically large = worth learning regional clues")
        print(worth.to_string(index=False))

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
