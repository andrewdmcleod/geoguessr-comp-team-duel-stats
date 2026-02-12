"""Shared test fixtures for Phase 4 tests."""

import csv
import json
import os
import tempfile
from pathlib import Path

import pytest


# ===================================================================
# Synthetic test data
# ===================================================================

SAMPLE_ROWS = [
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game001',
        'game_date': '2025-01-15T10:30:00Z',
        'round': '1',
        'total_rounds': '5',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'move',
        'player_id': 'player_a',
        'player_name': 'Alice',
        'time_seconds': '22.5',
        'distance_meters': '150000',
        'distance_km': '150.0',
        'score': '3500',
        'correct_lat': '48.8584',
        'correct_lng': '2.2945',
        'guess_lat': '47.0',
        'guess_lng': '3.0',
        'correct_country_code': 'FR',
        'correct_country': 'France',
        'guessed_country': 'France',
        'correct_country_flag': 'True',
        'region': 'Europe',
        'is_team_best_guess': 'True',
        'won_team': 'True',
        'won_round': 'True',
        'game_won': 'True',
        'health_before': '6000',
        'health_after': '5500',
        'damage_dealt': '500',
        'multiplier': '1.0',
    },
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game001',
        'game_date': '2025-01-15T10:30:00Z',
        'round': '1',
        'total_rounds': '5',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'move',
        'player_id': 'player_b',
        'player_name': 'Bob',
        'time_seconds': '35.2',
        'distance_meters': '500000',
        'distance_km': '500.0',
        'score': '2000',
        'correct_lat': '48.8584',
        'correct_lng': '2.2945',
        'guess_lat': '52.0',
        'guess_lng': '5.0',
        'correct_country_code': 'FR',
        'correct_country': 'France',
        'guessed_country': 'Netherlands',
        'correct_country_flag': 'False',
        'region': 'Europe',
        'is_team_best_guess': 'False',
        'won_team': 'False',
        'won_round': 'False',
        'game_won': 'True',
        'health_before': '6000',
        'health_after': '5500',
        'damage_dealt': '500',
        'multiplier': '1.0',
    },
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game001',
        'game_date': '2025-01-15T10:30:00Z',
        'round': '2',
        'total_rounds': '5',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'move',
        'player_id': 'player_a',
        'player_name': 'Alice',
        'time_seconds': '18.0',
        'distance_meters': '80000',
        'distance_km': '80.0',
        'score': '4200',
        'correct_lat': '-33.8688',
        'correct_lng': '151.2093',
        'guess_lat': '-34.0',
        'guess_lng': '150.5',
        'correct_country_code': 'AU',
        'correct_country': 'Australia',
        'guessed_country': 'Australia',
        'correct_country_flag': 'True',
        'region': 'Oceania',
        'is_team_best_guess': 'True',
        'won_team': 'True',
        'won_round': 'False',
        'game_won': 'True',
        'health_before': '5500',
        'health_after': '5000',
        'damage_dealt': '500',
        'multiplier': '1.5',
    },
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game001',
        'game_date': '2025-01-15T10:30:00Z',
        'round': '2',
        'total_rounds': '5',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'move',
        'player_id': 'player_b',
        'player_name': 'Bob',
        'time_seconds': '45.1',
        'distance_meters': '2000000',
        'distance_km': '2000.0',
        'score': '800',
        'correct_lat': '-33.8688',
        'correct_lng': '151.2093',
        'guess_lat': '-20.0',
        'guess_lng': '140.0',
        'correct_country_code': 'AU',
        'correct_country': 'Australia',
        'guessed_country': 'Australia',
        'correct_country_flag': 'True',
        'region': 'Oceania',
        'is_team_best_guess': 'False',
        'won_team': 'False',
        'won_round': 'False',
        'game_won': 'True',
        'health_before': '5500',
        'health_after': '5000',
        'damage_dealt': '500',
        'multiplier': '1.5',
    },
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game002',
        'game_date': '2025-01-16T14:00:00Z',
        'round': '1',
        'total_rounds': '3',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'no-move',
        'player_id': 'player_a',
        'player_name': 'Alice',
        'time_seconds': '60.0',
        'distance_meters': '3000000',
        'distance_km': '3000.0',
        'score': '500',
        'correct_lat': '35.6762',
        'correct_lng': '139.6503',
        'guess_lat': '40.0',
        'guess_lng': '116.0',
        'correct_country_code': 'JP',
        'correct_country': 'Japan',
        'guessed_country': 'China',
        'correct_country_flag': 'False',
        'region': 'Asia',
        'is_team_best_guess': 'False',
        'won_team': 'False',
        'won_round': 'False',
        'game_won': 'False',
        'health_before': '6000',
        'health_after': '4000',
        'damage_dealt': '0',
        'multiplier': '1.0',
    },
    {
        'team_key': 'player_a_player_b',
        'game_id': 'game002',
        'game_date': '2025-01-16T14:00:00Z',
        'round': '1',
        'total_rounds': '3',
        'competitive_mode': 'TeamDuels',
        'move_mode': 'no-move',
        'player_id': 'player_b',
        'player_name': 'Bob',
        'time_seconds': '55.0',
        'distance_meters': '1500000',
        'distance_km': '1500.0',
        'score': '1200',
        'correct_lat': '35.6762',
        'correct_lng': '139.6503',
        'guess_lat': '37.0',
        'guess_lng': '127.0',
        'correct_country_code': 'JP',
        'correct_country': 'Japan',
        'guessed_country': 'South Korea',
        'correct_country_flag': 'False',
        'region': 'Asia',
        'is_team_best_guess': 'True',
        'won_team': 'True',
        'won_round': 'False',
        'game_won': 'False',
        'health_before': '6000',
        'health_after': '4000',
        'damage_dealt': '0',
        'multiplier': '1.0',
    },
]

CSV_COLUMNS = [
    'team_key', 'game_id', 'game_date', 'round', 'total_rounds',
    'competitive_mode', 'move_mode', 'player_id', 'player_name',
    'time_seconds', 'distance_meters', 'distance_km', 'score',
    'correct_lat', 'correct_lng', 'guess_lat', 'guess_lng',
    'correct_country_code', 'correct_country', 'guessed_country',
    'correct_country_flag', 'region',
    'is_team_best_guess', 'won_team', 'won_round', 'game_won',
    'health_before', 'health_after', 'damage_dealt', 'multiplier',
]


@pytest.fixture
def sample_rows():
    """Return the synthetic test dataset as a list of dicts."""
    return [dict(r) for r in SAMPLE_ROWS]


@pytest.fixture
def sample_csv(tmp_path):
    """Write sample data to a CSV file and return the path."""
    csv_path = tmp_path / 'test_data.csv'
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(SAMPLE_ROWS)
    return str(csv_path)


@pytest.fixture
def outdir_with_export(tmp_path, sample_csv):
    """Create an outdir structure with a single export and latest.json."""
    outdir = tmp_path / 'out'
    export_dir = outdir / 'exports' / '2025-01-16_140000'
    export_dir.mkdir(parents=True)

    # Copy CSV to export dir
    import shutil
    export_csv = export_dir / 'team_duels.csv'
    shutil.copy2(sample_csv, export_csv)

    # Write latest.json
    latest = {
        'latest_export_dir': str(export_dir),
        'export_id': '2025-01-16_140000',
        'created_at': '2025-01-16T14:00:00Z',
        'csv_file': str(export_csv),
        'total_rows': len(SAMPLE_ROWS),
        'total_games': 2,
    }
    with open(outdir / 'latest.json', 'w') as f:
        json.dump(latest, f)

    return str(outdir)
