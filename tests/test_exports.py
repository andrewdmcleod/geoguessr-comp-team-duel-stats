"""Unit tests for export directory structure and latest.json resolution.

Tests the export directory structure, latest.json pointer file,
and the resolve_export() function from geoguessr_dashboard.py.
These do NOT require Docker.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from geoguessr_dashboard import resolve_export


# ===================================================================
# resolve_export() tests
# ===================================================================

class TestResolveExport:
    def test_resolve_latest(self, outdir_with_export):
        """Resolving 'latest' should return the CSV from latest.json."""
        csv_path = resolve_export('latest', outdir_with_export)
        assert os.path.isfile(csv_path)
        assert csv_path.endswith('team_duels.csv')

    def test_resolve_export_id(self, outdir_with_export):
        """Resolving by export_id should find the right directory."""
        csv_path = resolve_export('2025-01-16_140000', outdir_with_export)
        assert os.path.isfile(csv_path)
        assert '2025-01-16_140000' in csv_path

    def test_resolve_direct_path(self, sample_csv):
        """Resolving a direct CSV path should return it."""
        csv_path = resolve_export(sample_csv, '/nonexistent')
        assert csv_path == sample_csv

    def test_resolve_latest_missing(self, tmp_path):
        """Resolving 'latest' when no latest.json exists should raise."""
        outdir = str(tmp_path / 'empty_out')
        os.makedirs(outdir, exist_ok=True)
        with pytest.raises(FileNotFoundError, match='latest.json'):
            resolve_export('latest', outdir)

    def test_resolve_bad_export_id(self, outdir_with_export):
        """Resolving a nonexistent export_id should raise."""
        with pytest.raises(FileNotFoundError):
            resolve_export('nonexistent_id', outdir_with_export)

    def test_resolve_bad_path(self, outdir_with_export):
        """Resolving a nonexistent file path should raise."""
        with pytest.raises(FileNotFoundError):
            resolve_export('/no/such/file.csv', outdir_with_export)


# ===================================================================
# latest.json structure tests
# ===================================================================

class TestLatestJson:
    def test_latest_json_exists(self, outdir_with_export):
        latest_path = os.path.join(outdir_with_export, 'latest.json')
        assert os.path.isfile(latest_path)

    def test_latest_json_structure(self, outdir_with_export):
        with open(os.path.join(outdir_with_export, 'latest.json')) as f:
            data = json.load(f)

        assert 'latest_export_dir' in data
        assert 'export_id' in data
        assert 'created_at' in data
        assert 'csv_file' in data
        assert 'total_rows' in data
        assert 'total_games' in data

    def test_latest_json_export_id_matches(self, outdir_with_export):
        with open(os.path.join(outdir_with_export, 'latest.json')) as f:
            data = json.load(f)
        assert data['export_id'] == '2025-01-16_140000'

    def test_latest_json_csv_file_exists(self, outdir_with_export):
        with open(os.path.join(outdir_with_export, 'latest.json')) as f:
            data = json.load(f)
        assert os.path.isfile(data['csv_file'])

    def test_latest_json_counts(self, outdir_with_export):
        with open(os.path.join(outdir_with_export, 'latest.json')) as f:
            data = json.load(f)
        assert data['total_rows'] == 6
        assert data['total_games'] == 2


# ===================================================================
# Export directory structure tests
# ===================================================================

class TestExportDirStructure:
    def test_exports_dir_exists(self, outdir_with_export):
        exports_dir = os.path.join(outdir_with_export, 'exports')
        assert os.path.isdir(exports_dir)

    def test_export_id_dir_exists(self, outdir_with_export):
        export_dir = os.path.join(outdir_with_export, 'exports', '2025-01-16_140000')
        assert os.path.isdir(export_dir)

    def test_csv_in_export_dir(self, outdir_with_export):
        csv_path = os.path.join(
            outdir_with_export, 'exports', '2025-01-16_140000', 'team_duels.csv'
        )
        assert os.path.isfile(csv_path)

    def test_csv_has_correct_row_count(self, outdir_with_export):
        import csv as csv_mod
        csv_path = os.path.join(
            outdir_with_export, 'exports', '2025-01-16_140000', 'team_duels.csv'
        )
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            rows = list(reader)
        assert len(rows) == 6

    def test_csv_has_correct_columns(self, outdir_with_export):
        import csv as csv_mod
        csv_path = os.path.join(
            outdir_with_export, 'exports', '2025-01-16_140000', 'team_duels.csv'
        )
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            row = next(reader)
        assert 'team_key' in row
        assert 'game_id' in row
        assert 'player_name' in row
        assert 'distance_km' in row
