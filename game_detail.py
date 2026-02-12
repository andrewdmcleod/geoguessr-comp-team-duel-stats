#!/usr/bin/env python3
"""
GeoGuessr Game Detail Drilldown

Per-game analysis: round-by-round performance, initiative, timing,
health progression, and player comparison.

Usage:
    python game_detail.py team_duels.csv GAME_ID
    python game_detail.py team_duels.csv GAME_ID --json
    python game_detail.py team_duels.csv --list         # list all games
    python game_detail.py team_duels.csv --last          # show last game
"""

import argparse
import json
import sys
import pandas as pd
from pathlib import Path
from typing import Optional


def load_data(csv_file: str) -> pd.DataFrame:
    """Load CSV with type conversions."""
    df = pd.read_csv(csv_file)

    for col in ['won_team', 'won_round', 'is_team_best_guess', 'correct_country_flag', 'game_won']:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: True if str(x).strip() == 'True' else
                          (False if str(x).strip() == 'False' else None))

    for col in ['health_before', 'health_after', 'damage_dealt',
                'multiplier', 'score', 'distance_km', 'time_seconds',
                'time_remaining_sec', 'round_duration_sec']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'game_date' in df.columns:
        df['game_date_parsed'] = pd.to_datetime(df['game_date'], errors='coerce', utc=True)

    if 'clicked_first' in df.columns:
        df['clicked_first'] = df['clicked_first'].apply(
            lambda x: True if str(x).strip() == 'True' else False)

    return df


def list_games(df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """List recent games with summary info."""
    games = []
    for game_id, gdf in df.groupby('game_id'):
        first = gdf.iloc[0]
        game_date = first.get('game_date', '')
        won = first.get('game_won', None)
        total_rounds = first.get('total_rounds', gdf['round'].nunique())
        mode = first.get('move_mode', '')
        players = sorted(gdf['player_name'].unique())

        games.append({
            'game_id': game_id,
            'date': str(game_date)[:19] if game_date else '',
            'rounds': int(total_rounds) if pd.notna(total_rounds) else gdf['round'].nunique(),
            'won': won,
            'mode': mode,
            'players': ', '.join(players),
            'avg_dist_km': round(gdf['distance_km'].mean(), 1),
        })

    result = pd.DataFrame(games)
    if 'date' in result.columns:
        result = result.sort_values('date', ascending=False)
    return result.head(n)


def game_overview(gdf: pd.DataFrame) -> dict:
    """High-level game summary."""
    first = gdf.iloc[0]

    overview = {
        'game_id': first['game_id'],
        'date': str(first.get('game_date', '')),
        'total_rounds': int(first.get('total_rounds', gdf['round'].nunique())),
        'move_mode': first.get('move_mode', ''),
        'competitive_mode': first.get('competitive_mode', ''),
        'result': 'Won' if first.get('game_won') is True else
                  ('Lost' if first.get('game_won') is False else 'Unknown'),
    }

    # Team stats
    overview['team_avg_dist_km'] = round(gdf['distance_km'].mean(), 1)
    overview['team_avg_time_sec'] = round(gdf['time_seconds'].mean(), 1)

    if 'correct_country_flag' in gdf.columns and gdf['correct_country_flag'].notna().any():
        overview['team_country_accuracy'] = round(
            gdf['correct_country_flag'].mean() * 100, 1)

    if 'score' in gdf.columns:
        overview['team_total_score'] = int(gdf['score'].sum())

    return overview


def round_breakdown(gdf: pd.DataFrame) -> list:
    """Per-round breakdown with all players."""
    rounds = []
    for rnd_num in sorted(gdf['round'].unique()):
        rdf = gdf[gdf['round'] == rnd_num]
        first = rdf.iloc[0]

        rnd = {
            'round': int(rnd_num),
            'country': first.get('correct_country', ''),
            'region': first.get('region', ''),
        }

        # Round-level info
        if 'health_before' in rdf.columns and rdf['health_before'].notna().any():
            rnd['health_before'] = int(rdf['health_before'].iloc[0])
        if 'health_after' in rdf.columns and rdf['health_after'].notna().any():
            rnd['health_after'] = int(rdf['health_after'].iloc[0])
        if 'won_round' in rdf.columns and rdf['won_round'].notna().any():
            rnd['won_round'] = bool(rdf['won_round'].iloc[0])
        if 'round_duration_sec' in rdf.columns and rdf['round_duration_sec'].notna().any():
            rnd['round_duration_sec'] = round(float(rdf['round_duration_sec'].iloc[0]), 1)

        # Per-player guesses
        players = []
        for _, row in rdf.iterrows():
            player = {
                'player_name': row['player_name'],
                'distance_km': round(float(row['distance_km']), 2),
                'time_seconds': round(float(row['time_seconds']), 1),
                'score': int(row['score']) if pd.notna(row.get('score')) else None,
            }

            status = row.get('status', '')
            if status == 'no_pin':
                player['status'] = 'no_pin'
            else:
                player['status'] = 'guessed'

            if pd.notna(row.get('guessed_country')):
                player['guessed_country'] = row['guessed_country']
            if pd.notna(row.get('correct_country_flag')):
                player['correct_country'] = bool(row['correct_country_flag'])
            if pd.notna(row.get('won_team')):
                player['won_team'] = bool(row['won_team'])
            if pd.notna(row.get('is_team_best_guess')):
                player['is_team_best'] = bool(row['is_team_best_guess'])
            if pd.notna(row.get('time_remaining_sec')):
                player['time_remaining_sec'] = round(float(row['time_remaining_sec']), 1)
            if pd.notna(row.get('clicked_first')):
                player['clicked_first'] = bool(row['clicked_first'])
            if pd.notna(row.get('damage_dealt')):
                player['damage_dealt'] = int(row['damage_dealt'])
            if pd.notna(row.get('multiplier')) and row['multiplier'] != 1:
                player['multiplier'] = float(row['multiplier'])

            players.append(player)

        rnd['players'] = players
        rounds.append(rnd)

    return rounds


def player_game_summary(gdf: pd.DataFrame) -> list:
    """Per-player summary for this game."""
    summaries = []
    for player_name in sorted(gdf['player_name'].unique()):
        pdf = gdf[gdf['player_name'] == player_name]
        if 'status' in pdf.columns:
            pdf_guessed = pdf[pdf['status'] != 'no_pin']
        else:
            pdf_guessed = pdf

        summary = {
            'player_name': player_name,
            'rounds_played': len(pdf),
            'avg_dist_km': round(pdf_guessed['distance_km'].mean(), 1) if len(pdf_guessed) > 0 else None,
            'avg_time_sec': round(pdf_guessed['time_seconds'].mean(), 1) if len(pdf_guessed) > 0 else None,
            'total_score': int(pdf['score'].sum()) if 'score' in pdf.columns else None,
        }

        if 'correct_country_flag' in pdf_guessed.columns and pdf_guessed['correct_country_flag'].notna().any():
            summary['country_accuracy'] = round(
                pdf_guessed['correct_country_flag'].mean() * 100, 1)

        if 'won_team' in pdf.columns and pdf['won_team'].notna().any():
            summary['won_team_count'] = int(pdf['won_team'].sum())

        if 'status' in pdf.columns:
            summary['no_pin_count'] = int((pdf['status'] == 'no_pin').sum())

        if 'clicked_first' in pdf.columns and 'status' in pdf.columns:
            guessed = pdf[pdf['status'] != 'no_pin']
            summary['clicked_first_count'] = int(guessed['clicked_first'].sum())

        summaries.append(summary)

    return summaries


def health_progression(gdf: pd.DataFrame) -> Optional[list]:
    """Track health across rounds."""
    if 'health_before' not in gdf.columns or gdf['health_before'].isna().all():
        return None

    progression = []
    for rnd_num in sorted(gdf['round'].unique()):
        rdf = gdf[gdf['round'] == rnd_num]
        first = rdf.iloc[0]
        entry = {'round': int(rnd_num)}
        if pd.notna(first.get('health_before')):
            entry['health_before'] = int(first['health_before'])
        if pd.notna(first.get('health_after')):
            entry['health_after'] = int(first['health_after'])
        if pd.notna(first.get('damage_dealt')):
            entry['damage_dealt'] = int(rdf['damage_dealt'].max())
        if 'won_round' in rdf.columns and rdf['won_round'].notna().any():
            entry['won'] = bool(first['won_round'])
        progression.append(entry)

    return progression


def format_game_detail(gdf: pd.DataFrame) -> dict:
    """Full game detail as a structured dict."""
    detail = {
        'overview': game_overview(gdf),
        'player_summary': player_game_summary(gdf),
        'rounds': round_breakdown(gdf),
    }

    hp = health_progression(gdf)
    if hp:
        detail['health_progression'] = hp

    return detail


def print_game_detail(gdf: pd.DataFrame):
    """Pretty-print game detail to terminal."""
    overview = game_overview(gdf)
    rounds = round_breakdown(gdf)
    player_summaries = player_game_summary(gdf)

    # Header
    print(f"\n{'=' * 60}")
    print(f"\U0001f3ae GAME DETAIL: {overview['game_id']}")
    print(f"{'=' * 60}")
    print(f"  Date:        {overview['date'][:19]}")
    print(f"  Mode:        {overview.get('move_mode', 'N/A')} / {overview.get('competitive_mode', 'N/A')}")
    print(f"  Rounds:      {overview['total_rounds']}")
    print(f"  Result:      {overview['result']}")
    print(f"  Team avg:    {overview['team_avg_dist_km']} km, {overview['team_avg_time_sec']}s")
    if 'team_country_accuracy' in overview:
        print(f"  Country acc: {overview['team_country_accuracy']}%")
    if 'team_total_score' in overview:
        print(f"  Total score: {overview['team_total_score']}")

    # Player summaries
    print(f"\n{'-' * 60}")
    print(f"\U0001f464 PLAYER SUMMARY")
    print(f"{'-' * 60}")
    for ps in player_summaries:
        parts = [f"  {ps['player_name']}:"]
        if ps.get('avg_dist_km') is not None:
            parts.append(f"avg {ps['avg_dist_km']} km")
        if ps.get('avg_time_sec') is not None:
            parts.append(f"{ps['avg_time_sec']}s")
        if ps.get('country_accuracy') is not None:
            parts.append(f"{ps['country_accuracy']}% country")
        if ps.get('won_team_count') is not None:
            parts.append(f"won team {ps['won_team_count']}x")
        if ps.get('no_pin_count', 0) > 0:
            parts.append(f"\u274c{ps['no_pin_count']} no-pin")
        if ps.get('clicked_first_count', 0) > 0:
            parts.append(f"\U0001f947{ps['clicked_first_count']} first")
        print('  '.join(parts))

    # Health progression
    hp = health_progression(gdf)
    if hp:
        print(f"\n{'-' * 60}")
        print(f"\u2764\ufe0f  HEALTH PROGRESSION")
        print(f"{'-' * 60}")
        for h in hp:
            won_marker = '\u2705' if h.get('won') else '\u274c' if h.get('won') is False else '  '
            damage = h.get('damage_dealt', 0)
            print(f"  R{h['round']:2d}: {h.get('health_before', '?'):>5} \u2192 {h.get('health_after', '?'):>5}"
                  f"  {won_marker}  dmg={damage}")

    # Round-by-round
    print(f"\n{'-' * 60}")
    print(f"\U0001f30d ROUND-BY-ROUND")
    print(f"{'-' * 60}")
    for rnd in rounds:
        won_marker = '\u2705' if rnd.get('won_round') else '\u274c' if rnd.get('won_round') is False else ''
        print(f"\n  Round {rnd['round']}: {rnd.get('country', '?')} ({rnd.get('region', '?')}) {won_marker}")
        if rnd.get('round_duration_sec'):
            print(f"    Duration: {rnd['round_duration_sec']}s")

        for p in rnd['players']:
            status_icon = '\u274c' if p.get('status') == 'no_pin' else ''
            first_icon = '\U0001f947' if p.get('clicked_first') else '  '
            team_best = '\u2b50' if p.get('is_team_best') else '  '
            country_icon = '\u2705' if p.get('correct_country') else ('\u274c' if p.get('correct_country') is False else '  ')

            parts = [f"    {first_icon}{team_best} {p['player_name']:<12}"]
            if p.get('status') == 'no_pin':
                parts.append('NO PIN')
            else:
                parts.append(f"{p['distance_km']:>8.1f} km")
                parts.append(f"{p['time_seconds']:>5.1f}s")
                if p.get('time_remaining_sec') is not None:
                    parts.append(f"(rem {p['time_remaining_sec']:.0f}s)")
                parts.append(country_icon)
                if p.get('guessed_country'):
                    parts.append(f"guessed: {p['guessed_country']}")
                if p.get('score') is not None:
                    parts.append(f"score={p['score']}")
                if p.get('damage_dealt'):
                    parts.append(f"dmg={p['damage_dealt']}")

            print('  '.join(parts))


def main():
    parser = argparse.ArgumentParser(
        description='GeoGuessr game detail drilldown',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  python game_detail.py team_duels.csv --list
  python game_detail.py team_duels.csv --last
  python game_detail.py team_duels.csv GAME_ID
  python game_detail.py team_duels.csv GAME_ID --json
  python game_detail.py team_duels.csv GAME_ID --csv out/
""")
    parser.add_argument('csv_file', help='CSV file with stats')
    parser.add_argument('game_id', nargs='?', help='Game ID to drill into')
    parser.add_argument('--list', action='store_true',
                        help='List recent games')
    parser.add_argument('--list-n', type=int, default=20,
                        help='Number of games to list (default: 20)')
    parser.add_argument('--last', action='store_true',
                        help='Show the most recent game')
    parser.add_argument('--json', action='store_true',
                        help='Output as JSON')
    parser.add_argument('--csv', type=str, default=None,
                        help='Export round data to CSV in this directory')
    args = parser.parse_args()

    df = load_data(args.csv_file)

    # List mode
    if args.list:
        games = list_games(df, n=args.list_n)
        if args.json:
            print(json.dumps(games.to_dict(orient='records'), indent=2, default=str))
        else:
            print(f"\n\U0001f4cb Recent Games ({len(games)}):")
            print(games.to_string(index=False))
        return

    # Determine game ID
    game_id = args.game_id
    if args.last:
        if 'game_date' in df.columns:
            df['_sort_date'] = pd.to_datetime(df['game_date'], errors='coerce', utc=True)
            game_id = df.sort_values('_sort_date').iloc[-1]['game_id']
        else:
            game_id = df['game_id'].iloc[-1]
        print(f"  (Latest game: {game_id})")

    if not game_id:
        parser.print_help()
        print("\n\u274c Please provide a game_id, --list, or --last")
        sys.exit(1)

    # Filter to this game
    gdf = df[df['game_id'] == game_id]
    if len(gdf) == 0:
        print(f"\u274c No data found for game {game_id}")
        print(f"   Available games: {df['game_id'].nunique()}")
        sys.exit(1)

    # Output
    if args.json:
        detail = format_game_detail(gdf)
        print(json.dumps(detail, indent=2, default=str))
    elif args.csv:
        export_dir = Path(args.csv)
        export_dir.mkdir(exist_ok=True)
        # Export round-level data
        export_cols = [c for c in gdf.columns if c not in ['game_date_parsed', '_sort_date']]
        gdf[export_cols].to_csv(export_dir / f'game_{game_id}.csv', index=False)
        print(f"\u2705 Exported to {export_dir}/game_{game_id}.csv")
    else:
        print_game_detail(gdf)


if __name__ == '__main__':
    main()
