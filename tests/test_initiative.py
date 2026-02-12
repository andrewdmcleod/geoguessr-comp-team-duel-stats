"""Unit tests for initiative/timing analysis functions in analyze_stats.py."""

import sys
import os

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyze_stats import (
    initiative_summary,
    guess_time_by_region,
    no_pin_analysis,
    hesitation_index,
    pressure_response,
    fastest_slowest_guesses,
    detect_my_team,
)


@pytest.fixture
def df_initiative():
    """DataFrame with initiative/timing columns for testing."""
    data = [
        # Game 1, Round 1 — Alice clicks first
        {'game_id': 'g1', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 100.0, 'time_seconds': 20.0,
         'status': 'guessed', 'clicked_first': True, 'time_remaining_sec': 40.0,
         'region': 'Europe', 'correct_country': 'France', 'won_round': True,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},
        {'game_id': 'g1', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 30.0,
         'status': 'guessed', 'clicked_first': False, 'time_remaining_sec': 30.0,
         'region': 'Europe', 'correct_country': 'France', 'won_round': True,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},

        # Game 1, Round 2 — Bob clicks first, Alice no-pin
        {'game_id': 'g1', 'round': 2, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 0.0, 'time_seconds': 0.0,
         'status': 'no_pin', 'clicked_first': False, 'time_remaining_sec': None,
         'region': 'Asia', 'correct_country': 'Japan', 'won_round': False,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},
        {'game_id': 'g1', 'round': 2, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 500.0, 'time_seconds': 45.0,
         'status': 'guessed', 'clicked_first': True, 'time_remaining_sec': 15.0,
         'region': 'Asia', 'correct_country': 'Japan', 'won_round': False,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},

        # Game 1, Round 3 — Both guess, Alice first
        {'game_id': 'g1', 'round': 3, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 50.0, 'time_seconds': 15.0,
         'status': 'guessed', 'clicked_first': True, 'time_remaining_sec': 45.0,
         'region': 'Europe', 'correct_country': 'Germany', 'won_round': True,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},
        {'game_id': 'g1', 'round': 3, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 300.0, 'time_seconds': 50.0,
         'status': 'guessed', 'clicked_first': False, 'time_remaining_sec': 10.0,
         'region': 'Europe', 'correct_country': 'Germany', 'won_round': True,
         'game_date': '2025-01-15T10:00:00Z', 'game_won': True,
         'round_duration_sec': 60.0},
    ]
    df = pd.DataFrame(data)
    df['game_date_parsed'] = pd.to_datetime(df['game_date'], errors='coerce', utc=True)
    return df


class TestDetectMyTeam:
    def test_detects_team_from_short_team_key(self, df_initiative):
        pids = detect_my_team(df_initiative)
        assert pids == {'pa', 'pb'}

    def test_excludes_opponents_with_same_team_key(self):
        """Opponent rows share the same team_key but should not be included."""
        pid_a = '5f705c262172ba000196ddbd'
        pid_b = '695c3de0900ad6bdf176304f'
        pid_opp = 'aaaaaaaaaaaaaaaaaaaaaa00'
        team_key = f'{pid_a}_{pid_b}'
        data = [
            {'game_id': 'g1', 'round': 1, 'player_id': pid_a,
             'player_name': 'Alice', 'team_key': team_key},
            {'game_id': 'g1', 'round': 1, 'player_id': pid_b,
             'player_name': 'Bob', 'team_key': team_key},
            {'game_id': 'g1', 'round': 1, 'player_id': pid_opp,
             'player_name': 'Opponent', 'team_key': team_key},
        ]
        df = pd.DataFrame(data)
        pids = detect_my_team(df)
        assert pids == {pid_a, pid_b}
        assert pid_opp not in pids

    def test_fallback_without_team_key(self, df_initiative):
        df = df_initiative.drop(columns=['team_key'])
        pids = detect_my_team(df)
        assert len(pids) == 2


class TestInitiativeSummary:
    def test_returns_dataframe(self, df_initiative):
        result = initiative_summary(df_initiative)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_has_team_row(self, df_initiative):
        result = initiative_summary(df_initiative)
        assert result.iloc[0]['player_name'] == 'Team'

    def test_alice_initiative_rate(self, df_initiative):
        result = initiative_summary(df_initiative)
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        # Alice: guessed in rounds 1,3. Clicked first in both.
        # initiative_rate = 2/2 * 100 = 100.0
        assert alice['clicked_first'] == 2
        assert alice['initiative_rate'] == 100.0

    def test_alice_has_no_pin(self, df_initiative):
        result = initiative_summary(df_initiative)
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        assert alice['no_pin'] == 1

    def test_bob_initiative_rate(self, df_initiative):
        result = initiative_summary(df_initiative)
        bob = result[result['player_name'] == 'Bob'].iloc[0]
        # Bob: guessed in all 3 rounds. Clicked first in round 2 only.
        # initiative_rate = 1/3 * 100 = 33.3
        assert bob['clicked_first'] == 1
        assert bob['initiative_rate'] == 33.3

    def test_works_without_status_column(self, df_initiative):
        """Without status column, initiative_summary still works by inferring from rows."""
        df = df_initiative.drop(columns=['status'])
        result = initiative_summary(df)
        assert result is not None
        assert len(result) == 3  # Team + 2 players
        # clicked_first should still work (column is present)
        assert result.iloc[0]['player_name'] == 'Team'


class TestGuessTimeByRegion:
    def test_returns_dataframe(self, df_initiative):
        result = guess_time_by_region(df_initiative)
        assert result is not None

    def test_has_team_row(self, df_initiative):
        result = guess_time_by_region(df_initiative)
        assert 'Team' in result.index

    def test_europe_values(self, df_initiative):
        result = guess_time_by_region(df_initiative)
        # Europe: Alice (40, 45), Bob (30, 10) -> averages
        assert 'Europe' in result.columns

    def test_returns_none_without_time_remaining(self, df_initiative):
        df = df_initiative.drop(columns=['time_remaining_sec'])
        result = guess_time_by_region(df)
        assert result is None


class TestNoPinAnalysis:
    def test_returns_dataframe(self, df_initiative):
        result = no_pin_analysis(df_initiative)
        assert result is not None

    def test_has_team_row(self, df_initiative):
        result = no_pin_analysis(df_initiative)
        assert result.iloc[0]['player_name'] == 'Team'

    def test_alice_has_one_nopin(self, df_initiative):
        result = no_pin_analysis(df_initiative)
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        assert alice['no_pin_count'] == 1

    def test_bob_has_no_nopin(self, df_initiative):
        result = no_pin_analysis(df_initiative)
        # Bob guessed in all 3 rounds — no no-pin rows for Bob
        bob_rows = result[result['player_name'] == 'Bob']
        assert len(bob_rows) == 0  # Bob doesn't appear in no-pin analysis

    def test_returns_none_without_status(self, df_initiative):
        df = df_initiative.drop(columns=['status'])
        result = no_pin_analysis(df)
        assert result is None


class TestHesitationIndex:
    def test_returns_dataframe(self, df_initiative):
        result = hesitation_index(df_initiative)
        assert result is not None

    def test_round_1_hesitation(self, df_initiative):
        result = hesitation_index(df_initiative)
        r1 = result[(result['game_id'] == 'g1') & (result['round'] == 1)]
        assert len(r1) == 1
        # Round 1: Alice=40, Bob=30 -> hesitation = 10
        assert r1.iloc[0]['hesitation_sec'] == 10.0

    def test_round_3_hesitation(self, df_initiative):
        result = hesitation_index(df_initiative)
        r3 = result[(result['game_id'] == 'g1') & (result['round'] == 3)]
        assert len(r3) == 1
        # Round 3: Alice=45, Bob=10 -> hesitation = 35
        assert r3.iloc[0]['hesitation_sec'] == 35.0

    def test_returns_none_without_time_remaining(self, df_initiative):
        df = df_initiative.drop(columns=['time_remaining_sec'])
        result = hesitation_index(df)
        assert result is None


class TestPressureResponse:
    def test_returns_dataframe(self, df_initiative):
        result = pressure_response(df_initiative)
        assert result is not None

    def test_has_player_rows(self, df_initiative):
        result = pressure_response(df_initiative)
        assert 'Alice' in result['player_name'].values

    def test_returns_none_without_won_round(self, df_initiative):
        df = df_initiative.drop(columns=['won_round'])
        result = pressure_response(df)
        assert result is None


class TestFastestSlowestGuesses:
    def test_returns_tuples(self, df_initiative):
        fastest, slowest = fastest_slowest_guesses(df_initiative)
        assert fastest is not None
        assert slowest is not None

    def test_fastest_is_highest_time_remaining(self, df_initiative):
        fastest, _ = fastest_slowest_guesses(df_initiative, n=1)
        # Highest time_remaining: Alice round 3 = 45s
        assert fastest.iloc[0]['time_remaining_sec'] == 45.0

    def test_slowest_is_lowest_time_remaining(self, df_initiative):
        _, slowest = fastest_slowest_guesses(df_initiative, n=1)
        # Lowest time_remaining: Bob round 3 = 10s
        assert slowest.iloc[0]['time_remaining_sec'] == 10.0
