"""Unit tests for pg_push module.

Tests data transformation (rows_to_tables), parsers, and DDL generation.
These do NOT require Docker or PostgreSQL.
"""

import pytest
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pg_push import (
    rows_to_tables,
    get_ddl,
    get_indexes,
    _parse_bool,
    _parse_float,
    _parse_ts,
)


# ===================================================================
# Parser unit tests
# ===================================================================

class TestParseBool:
    def test_true_string(self):
        assert _parse_bool('True') is True

    def test_false_string(self):
        assert _parse_bool('False') is False

    def test_none(self):
        assert _parse_bool(None) is None

    def test_empty_string(self):
        assert _parse_bool('') is None

    def test_garbage(self):
        assert _parse_bool('maybe') is None

    def test_whitespace(self):
        assert _parse_bool(' True ') is True
        assert _parse_bool(' False ') is False


class TestParseFloat:
    def test_valid_float(self):
        assert _parse_float('3.14') == 3.14

    def test_integer_string(self):
        assert _parse_float('42') == 42.0

    def test_none(self):
        assert _parse_float(None) is None

    def test_empty_string(self):
        assert _parse_float('') is None

    def test_garbage(self):
        assert _parse_float('abc') is None

    def test_negative(self):
        assert _parse_float('-33.8688') == -33.8688

    def test_zero(self):
        assert _parse_float('0') == 0.0


class TestParseTs:
    def test_valid_timestamp(self):
        assert _parse_ts('2025-01-15T10:30:00Z') == '2025-01-15T10:30:00Z'

    def test_none(self):
        assert _parse_ts(None) is None

    def test_empty(self):
        assert _parse_ts('') is None

    def test_strips_whitespace(self):
        assert _parse_ts('  2025-01-15T10:30:00Z  ') == '2025-01-15T10:30:00Z'


# ===================================================================
# rows_to_tables tests
# ===================================================================

class TestRowsToTables:
    def test_basic_transformation(self, sample_rows):
        games, rounds, guesses = rows_to_tables(sample_rows)

        # 2 unique games
        assert len(games) == 2

        # 3 unique rounds (game001 has 2 rounds, game002 has 1)
        assert len(rounds) == 3

        # 6 guesses (one per row)
        assert len(guesses) == 6

    def test_games_deduplicated(self, sample_rows):
        games, _, _ = rows_to_tables(sample_rows)
        game_ids = [g[0] for g in games]
        assert sorted(game_ids) == ['game001', 'game002']

    def test_rounds_deduplicated(self, sample_rows):
        _, rounds, _ = rows_to_tables(sample_rows)
        round_keys = [(r[0], r[1]) for r in rounds]
        assert ('game001', 1) in round_keys
        assert ('game001', 2) in round_keys
        assert ('game002', 1) in round_keys

    def test_game_fields(self, sample_rows):
        games, _, _ = rows_to_tables(sample_rows)
        # Find game001
        game001 = [g for g in games if g[0] == 'game001'][0]
        assert game001[0] == 'game001'                   # game_id
        assert game001[1] == '2025-01-15T10:30:00Z'     # played_at
        assert game001[2] == 'player_a_player_b'        # team_key
        assert game001[3] == 'TeamDuels'                 # competitive_mode
        assert game001[4] == 'move'                      # move_mode
        assert game001[5] == 5                           # total_rounds
        assert game001[6] is True                        # game_won

    def test_round_fields(self, sample_rows):
        _, rounds, _ = rows_to_tables(sample_rows)
        # Find game001 round 1
        r = [r for r in rounds if r[0] == 'game001' and r[1] == 1][0]
        assert r[2] == 48.8584        # pano_lat
        assert r[3] == 2.2945         # pano_lng
        assert r[4] == 'FR'           # pano_country_code
        assert r[5] == 'France'       # pano_country_name
        assert r[6] == 'Europe'       # region

    def test_guess_fields(self, sample_rows):
        _, _, guesses = rows_to_tables(sample_rows)
        # Find Alice's guess in game001 round 1
        alice_g1r1 = [g for g in guesses
                      if g[0] == 'game001' and g[1] == 1 and g[2] == 'player_a'][0]
        assert alice_g1r1[3] == 'Alice'         # player_name
        assert alice_g1r1[4] == 'player_a_player_b'  # team_key
        assert alice_g1r1[5] == 47.0             # guess_lat
        assert alice_g1r1[6] == 3.0              # guess_lng
        assert alice_g1r1[7] == 'France'         # guessed_country
        assert alice_g1r1[8] == 150000.0         # distance_m
        assert alice_g1r1[9] == 150.0            # distance_km
        assert alice_g1r1[10] == 3500.0          # score
        assert alice_g1r1[11] == 22.5            # time_seconds
        assert alice_g1r1[12] is True            # is_team_best_guess
        assert alice_g1r1[13] is True            # won_team
        assert alice_g1r1[14] is True            # won_round
        assert alice_g1r1[15] is True            # correct_country_flag

    def test_empty_rows(self):
        games, rounds, guesses = rows_to_tables([])
        assert games == []
        assert rounds == []
        assert guesses == []

    def test_game_won_false(self, sample_rows):
        games, _, _ = rows_to_tables(sample_rows)
        game002 = [g for g in games if g[0] == 'game002'][0]
        assert game002[6] is False  # game_won


# ===================================================================
# DDL tests
# ===================================================================

class TestDDL:
    def test_ddl_returns_list(self):
        ddl = get_ddl('geoguessr')
        assert isinstance(ddl, list)
        assert len(ddl) == 4  # schema + 3 tables

    def test_ddl_creates_schema(self):
        ddl = get_ddl('geoguessr')
        assert 'CREATE SCHEMA IF NOT EXISTS geoguessr' in ddl[0]

    def test_ddl_creates_games_table(self):
        ddl = get_ddl('myschema')
        assert any('myschema.games' in d for d in ddl)

    def test_ddl_creates_rounds_table(self):
        ddl = get_ddl('geoguessr')
        assert any('geoguessr.rounds' in d for d in ddl)

    def test_ddl_creates_guesses_table(self):
        ddl = get_ddl('geoguessr')
        assert any('geoguessr.guesses' in d for d in ddl)

    def test_ddl_has_foreign_keys(self):
        ddl = get_ddl('geoguessr')
        ddl_text = ' '.join(ddl)
        assert 'FOREIGN KEY' in ddl_text

    def test_indexes_returns_list(self):
        indexes = get_indexes('geoguessr')
        assert isinstance(indexes, list)
        assert len(indexes) == 5

    def test_indexes_reference_schema(self):
        indexes = get_indexes('custom_schema')
        for idx in indexes:
            assert 'custom_schema.' in idx
