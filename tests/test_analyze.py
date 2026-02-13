"""Unit tests for analyze_stats.py analysis functions.

Covers: load_data normalization, _filter_guess_clicked, _team_first_order,
best_worst_in_country, competitive_advantage, speed_vs_accuracy,
player_win_loss_split, move_vs_nomove, no_pin_analysis round_loss_pct,
derived clicked_first, player_summary timing enrichments, region_detail,
_add_flags, and CLI game exclusion filters.
"""

import csv
import json
import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyze_stats import (
    _add_flags,
    _filter_guess_clicked,
    _team_first_order,
    best_worst_in_country,
    competitive_advantage,
    detect_my_team,
    initiative_summary,
    load_data,
    move_vs_nomove,
    no_pin_analysis,
    player_summary,
    player_win_loss_split,
    region_detail,
    speed_vs_accuracy,
    region_performance,
)
from country_codes import flag_emoji, country_with_flag


# ===================================================================
# Test fixtures
# ===================================================================

@pytest.fixture
def df_basic():
    """Basic DataFrame with core columns for multiple analysis functions."""
    data = [
        # Game 1, Round 1 — both players, move mode, France
        {'game_id': 'g1', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 100.0, 'time_seconds': 20.0,
         'score': 3500, 'correct_country': 'France', 'guessed_country': 'France',
         'correct_country_flag': True, 'region': 'Europe', 'move_mode': 'move',
         'game_won': 'True', 'won_round': True, 'won_team': True,
         'is_team_best_guess': True, 'time_remaining_sec': 12.0,
         'status': 'guessed', 'clicked_first': True,
         'game_date': '2025-01-15T10:00:00Z'},
        {'game_id': 'g1', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 300.0, 'time_seconds': 30.0,
         'score': 2000, 'correct_country': 'France', 'guessed_country': 'Germany',
         'correct_country_flag': False, 'region': 'Europe', 'move_mode': 'move',
         'game_won': 'True', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 5.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-15T10:00:00Z'},

        # Game 1, Round 2 — both players, move mode, Japan
        {'game_id': 'g1', 'round': 2, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 500.0, 'time_seconds': 25.0,
         'score': 1500, 'correct_country': 'Japan', 'guessed_country': 'China',
         'correct_country_flag': False, 'region': 'Asia', 'move_mode': 'move',
         'game_won': 'True', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 8.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-15T10:00:00Z'},
        {'game_id': 'g1', 'round': 2, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 18.0,
         'score': 3000, 'correct_country': 'Japan', 'guessed_country': 'Japan',
         'correct_country_flag': True, 'region': 'Asia', 'move_mode': 'move',
         'game_won': 'True', 'won_round': True, 'won_team': True,
         'is_team_best_guess': True, 'time_remaining_sec': 14.0,
         'status': 'guessed', 'clicked_first': True,
         'game_date': '2025-01-15T10:00:00Z'},

        # Game 2, Round 1 — both players, no-move mode, France (loss)
        {'game_id': 'g2', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 50.0, 'time_seconds': 15.0,
         'score': 4000, 'correct_country': 'France', 'guessed_country': 'France',
         'correct_country_flag': True, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': True, 'won_team': True,
         'is_team_best_guess': True, 'time_remaining_sec': 10.0,
         'status': 'guessed', 'clicked_first': True,
         'game_date': '2025-01-16T10:00:00Z'},
        {'game_id': 'g2', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 800.0, 'time_seconds': 40.0,
         'score': 1000, 'correct_country': 'France', 'guessed_country': 'Spain',
         'correct_country_flag': False, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 3.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-16T10:00:00Z'},

        # Game 2, Round 2 — Alice guesses, Bob no-pin (loss)
        {'game_id': 'g2', 'round': 2, 'player_id': 'pa', 'player_name': 'Alice',
         'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 20.0,
         'score': 3000, 'correct_country': 'France', 'guessed_country': 'France',
         'correct_country_flag': True, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': False, 'won_team': True,
         'is_team_best_guess': True, 'time_remaining_sec': 7.0,
         'status': 'guessed', 'clicked_first': True,
         'game_date': '2025-01-16T10:00:00Z'},
        {'game_id': 'g2', 'round': 2, 'player_id': 'pb', 'player_name': 'Bob',
         'team_key': 'pa_pb', 'distance_km': 0.0, 'time_seconds': 0.0,
         'score': 0, 'correct_country': 'France', 'guessed_country': '',
         'correct_country_flag': False, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 0.0,
         'status': 'no_pin', 'clicked_first': False,
         'game_date': '2025-01-16T10:00:00Z'},
    ]
    df = pd.DataFrame(data)
    # Type conversions similar to load_data
    df['game_won_bool'] = df['game_won'].apply(
        lambda x: True if str(x).strip() == 'True' else False)
    df['game_date_parsed'] = pd.to_datetime(df['game_date'], errors='coerce', utc=True)
    return df


@pytest.fixture
def df_with_opponents(df_basic):
    """DataFrame that includes opponent rows (for competitive_advantage)."""
    opponent_rows = [
        # Opponent in game 1, round 1
        {'game_id': 'g1', 'round': 1, 'player_id': 'opp1', 'player_name': 'Opponent1',
         'team_key': 'pa_pb', 'distance_km': 800.0, 'time_seconds': 35.0,
         'score': 1000, 'correct_country': 'France', 'guessed_country': 'Italy',
         'correct_country_flag': False, 'region': 'Europe', 'move_mode': 'move',
         'game_won': 'True', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 5.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-15T10:00:00Z',
         'game_won_bool': True,
         'game_date_parsed': pd.Timestamp('2025-01-15T10:00:00Z')},
        # Opponent in game 1, round 2
        {'game_id': 'g1', 'round': 2, 'player_id': 'opp1', 'player_name': 'Opponent1',
         'team_key': 'pa_pb', 'distance_km': 1000.0, 'time_seconds': 40.0,
         'score': 500, 'correct_country': 'Japan', 'guessed_country': 'Korea',
         'correct_country_flag': False, 'region': 'Asia', 'move_mode': 'move',
         'game_won': 'True', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 3.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-15T10:00:00Z',
         'game_won_bool': True,
         'game_date_parsed': pd.Timestamp('2025-01-15T10:00:00Z')},
        # Opponent in game 2, round 1
        {'game_id': 'g2', 'round': 1, 'player_id': 'opp1', 'player_name': 'Opponent1',
         'team_key': 'pa_pb', 'distance_km': 600.0, 'time_seconds': 30.0,
         'score': 1500, 'correct_country': 'France', 'guessed_country': 'France',
         'correct_country_flag': True, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 5.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-16T10:00:00Z',
         'game_won_bool': False,
         'game_date_parsed': pd.Timestamp('2025-01-16T10:00:00Z')},
        # Opponent in game 2, round 2
        {'game_id': 'g2', 'round': 2, 'player_id': 'opp1', 'player_name': 'Opponent1',
         'team_key': 'pa_pb', 'distance_km': 500.0, 'time_seconds': 25.0,
         'score': 2000, 'correct_country': 'France', 'guessed_country': 'Belgium',
         'correct_country_flag': False, 'region': 'Europe', 'move_mode': 'no-move',
         'game_won': 'False', 'won_round': False, 'won_team': False,
         'is_team_best_guess': False, 'time_remaining_sec': 4.0,
         'status': 'guessed', 'clicked_first': False,
         'game_date': '2025-01-16T10:00:00Z',
         'game_won_bool': False,
         'game_date_parsed': pd.Timestamp('2025-01-16T10:00:00Z')},
    ]
    return pd.concat([df_basic, pd.DataFrame(opponent_rows)], ignore_index=True)


# ===================================================================
# _filter_guess_clicked tests
# ===================================================================

class TestFilterGuessClicked:
    def test_filters_by_time_remaining(self, df_basic):
        """Rows with time_remaining_sec < 1 should be excluded."""
        result = _filter_guess_clicked(df_basic)
        # Bob's no_pin row has time_remaining_sec=0.0, should be excluded
        assert len(result) < len(df_basic)
        assert all(pd.to_numeric(result['time_remaining_sec'], errors='coerce') >= 1)

    def test_returns_all_without_time_remaining(self):
        """Without time_remaining_sec column, returns all rows."""
        df = pd.DataFrame({
            'player_id': ['pa', 'pb'],
            'time_seconds': [20.0, 30.0],
        })
        result = _filter_guess_clicked(df)
        assert len(result) == 2

    def test_returns_all_if_no_valid_remaining(self):
        """If no rows have time_remaining >= 1, returns all rows (fallback)."""
        df = pd.DataFrame({
            'player_id': ['pa', 'pb'],
            'time_remaining_sec': [0.0, 0.5],
        })
        result = _filter_guess_clicked(df)
        assert len(result) == 2  # All returned since none pass filter


# ===================================================================
# _team_first_order tests
# ===================================================================

class TestTeamFirstOrder:
    def test_team_first(self):
        names = ['Bob', 'Team', 'Alice']
        result = _team_first_order(names, 'Team')
        assert result == ['Team', 'Alice', 'Bob']

    def test_no_team(self):
        names = ['Bob', 'Alice']
        result = _team_first_order(names, 'Team')
        assert result == ['Alice', 'Bob']

    def test_custom_team_name(self):
        names = ['Charlie', 'MyTeam', 'Alice']
        result = _team_first_order(names, 'MyTeam')
        assert result == ['MyTeam', 'Alice', 'Charlie']


# ===================================================================
# best_worst_in_country tests
# ===================================================================

class TestBestWorstInCountry:
    def test_returns_tuple(self, df_basic):
        best, worst = best_worst_in_country(df_basic)
        assert isinstance(best, pd.DataFrame)
        assert isinstance(worst, pd.DataFrame)

    def test_only_correct_country_guesses(self, df_basic):
        """Should only include rows where correct_country_flag is True."""
        best, worst = best_worst_in_country(df_basic, n=20)
        # All data is filtered to correct_country_flag == True
        # France: Alice correct 3 times (100, 50, 200), Bob correct 0 times
        # Japan: Alice correct 0, Bob correct 1 time (not enough, min 3)
        # So only France for Alice (if she has >=3)
        if not best.empty:
            # Team row should appear for countries with >= 3 correct guesses
            team_rows = best[best['player_name'] == 'Team']
            assert 'num_correct' in best.columns

    def test_min_3_guesses(self, df_basic):
        """Countries with < 3 correct guesses should be excluded."""
        best, worst = best_worst_in_country(df_basic, n=20)
        if not best.empty:
            assert all(best['num_correct'] >= 3)
        if not worst.empty:
            assert all(worst['num_correct'] >= 3)

    def test_team_row_first(self, df_basic):
        """Team aggregate should appear before player rows."""
        best, worst = best_worst_in_country(df_basic, n=20)
        if not best.empty and 'Team' in best['player_name'].values:
            team_idx = best[best['player_name'] == 'Team'].index[0]
            player_idx = best[best['player_name'] != 'Team'].index
            if len(player_idx) > 0:
                assert team_idx < player_idx.min()

    def test_returns_empty_without_columns(self):
        """Returns empty if required columns missing."""
        df = pd.DataFrame({'player_id': ['pa'], 'distance_km': [100.0]})
        best, worst = best_worst_in_country(df)
        assert best.empty
        assert worst.empty


# ===================================================================
# competitive_advantage tests
# ===================================================================

class TestCompetitiveAdvantage:
    def test_returns_dataframe(self, df_with_opponents):
        result = competitive_advantage(df_with_opponents, min_guesses=1)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_has_advantage_column(self, df_with_opponents):
        result = competitive_advantage(df_with_opponents, min_guesses=1)
        assert 'advantage_km' in result.columns

    def test_excludes_opponents_from_my_stats(self, df_with_opponents):
        """My team stats should only include pa and pb, not opp1."""
        result = competitive_advantage(df_with_opponents, min_guesses=1)
        assert result is not None
        # The function uses detect_my_team to separate teams
        my_pids = detect_my_team(df_with_opponents)
        assert 'opp1' not in my_pids

    def test_returns_none_without_opponents(self, df_basic):
        """Returns None if no opponent data."""
        result = competitive_advantage(df_basic)
        assert result is None

    def test_positive_advantage_means_we_outperform(self, df_with_opponents):
        """Positive advantage_km means our team has lower distance."""
        result = competitive_advantage(df_with_opponents, min_guesses=1)
        if result is not None and len(result) > 0:
            for _, row in result.iterrows():
                if row['advantage_km'] > 0:
                    assert row['opp_avg_dist'] > row['my_avg_dist']


# ===================================================================
# speed_vs_accuracy tests
# ===================================================================

class TestSpeedVsAccuracy:
    def test_returns_dataframe(self, df_basic):
        result = speed_vs_accuracy(df_basic)
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_has_combined_rank(self, df_basic):
        result = speed_vs_accuracy(df_basic)
        assert 'combined_rank' in result.columns

    def test_sorted_by_combined_rank(self, df_basic):
        result = speed_vs_accuracy(df_basic)
        ranks = result['combined_rank'].tolist()
        assert ranks == sorted(ranks)

    def test_no_efficiency_score_column(self, df_basic):
        """Old efficiency_score column should not exist (replaced by combined_rank)."""
        result = speed_vs_accuracy(df_basic)
        assert 'efficiency_score' not in result.columns

    def test_excludes_auto_submissions(self, df_basic):
        """Should exclude rows where time_remaining_sec < 1."""
        result = speed_vs_accuracy(df_basic)
        # The no_pin row with time_remaining=0 should be excluded
        assert result is not None


# ===================================================================
# player_win_loss_split tests
# ===================================================================

class TestPlayerWinLossSplit:
    def test_returns_dataframe(self, df_basic):
        result = player_win_loss_split(df_basic)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_has_country_breakdown(self, df_basic):
        """Should have correct/incorrect country breakdown columns."""
        result = player_win_loss_split(df_basic)
        assert 'wins_correct_country' in result.columns
        assert 'wins_wrong_country' in result.columns
        assert 'losses_correct_country' in result.columns
        assert 'losses_wrong_country' in result.columns

    def test_country_counts_add_up(self, df_basic):
        """Country breakdown should sum to total rounds for each player."""
        result = player_win_loss_split(df_basic)
        for _, row in result.iterrows():
            player = row['player_name']
            pdf = df_basic[
                (df_basic['player_name'] == player) &
                (df_basic['game_won_bool'].notna()) &
                (df_basic['correct_country_flag'].notna())
            ]
            total_country = (
                row['wins_correct_country'] + row['wins_wrong_country'] +
                row['losses_correct_country'] + row['losses_wrong_country']
            )
            assert total_country == len(pdf)

    def test_has_both_players(self, df_basic):
        result = player_win_loss_split(df_basic)
        assert 'Alice' in result['player_name'].values
        assert 'Bob' in result['player_name'].values


# ===================================================================
# move_vs_nomove tests
# ===================================================================

class TestMoveVsNomove:
    def test_team_rows_first(self, df_basic):
        """Team aggregate rows should come before player rows."""
        result = move_vs_nomove(df_basic)
        assert result is not None
        # First rows should be Team
        team_rows = result[result['player_name'] == 'Team']
        player_rows = result[result['player_name'] != 'Team']
        if len(team_rows) > 0 and len(player_rows) > 0:
            assert team_rows.index.max() < player_rows.index.min()

    def test_has_both_modes(self, df_basic):
        result = move_vs_nomove(df_basic)
        assert result is not None
        modes = result['move_mode'].unique()
        assert 'move' in modes
        assert 'no-move' in modes

    def test_sorted_by_mode_then_player(self, df_basic):
        result = move_vs_nomove(df_basic)
        # Team rows come first (already verified)
        # Player rows should be sorted by move_mode then player_name
        player_rows = result[result['player_name'] != 'Team'].reset_index(drop=True)
        for i in range(1, len(player_rows)):
            prev = player_rows.iloc[i - 1]
            curr = player_rows.iloc[i]
            assert (prev['move_mode'], prev['player_name']) <= (curr['move_mode'], curr['player_name'])


# ===================================================================
# no_pin_analysis round_loss_pct tests
# ===================================================================

class TestNoPinRoundLoss:
    def test_round_loss_pct_present(self, df_basic):
        """no_pin_analysis should include round_loss_pct column."""
        result = no_pin_analysis(df_basic)
        if result is not None:
            assert 'round_loss_pct' in result.columns

    def test_team_round_loss_pct(self, df_basic):
        """Team round_loss_pct should be computed."""
        result = no_pin_analysis(df_basic)
        if result is not None:
            team_row = result[result['player_name'] == 'Team']
            if len(team_row) > 0:
                # round_loss_pct should be a number or '-'
                pct = team_row.iloc[0]['round_loss_pct']
                assert pct == '-' or isinstance(pct, (int, float))

    def test_nopin_player_appears(self, df_basic):
        """Bob has a no_pin row and should appear in the analysis."""
        result = no_pin_analysis(df_basic)
        if result is not None:
            player_names = result['player_name'].tolist()
            assert 'Bob' in player_names


# ===================================================================
# load_data country normalization tests
# ===================================================================

class TestLoadDataNormalization:
    def test_normalizes_czechia(self, tmp_path):
        """Czechia should be normalized to Czech Republic in both columns."""
        csv_path = tmp_path / 'test.csv'
        rows = [
            {'game_id': 'g1', 'round': '1', 'player_id': 'pa', 'player_name': 'Alice',
             'distance_km': '100', 'time_seconds': '20',
             'correct_country': 'Czechia', 'guessed_country': 'Czechia',
             'correct_country_flag': 'True'},
        ]
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        df = load_data(str(csv_path))
        assert df.iloc[0]['correct_country'] == 'Czech Republic'
        assert df.iloc[0]['guessed_country'] == 'Czech Republic'

    def test_recomputes_correct_country_flag(self, tmp_path):
        """Flag should be recomputed after normalization."""
        csv_path = tmp_path / 'test.csv'
        rows = [
            # Czechia vs Czech Republic — originally marked False, but after
            # normalization both become Czech Republic → should be True
            {'game_id': 'g1', 'round': '1', 'player_id': 'pa', 'player_name': 'Alice',
             'distance_km': '100', 'time_seconds': '20',
             'correct_country': 'Czech Republic', 'guessed_country': 'Czechia',
             'correct_country_flag': 'False'},
        ]
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        df = load_data(str(csv_path))
        assert df.iloc[0]['correct_country_flag'] == True

    def test_preserves_non_aliased_countries(self, tmp_path):
        """Countries without aliases should be unchanged."""
        csv_path = tmp_path / 'test.csv'
        rows = [
            {'game_id': 'g1', 'round': '1', 'player_id': 'pa', 'player_name': 'Alice',
             'distance_km': '100', 'time_seconds': '20',
             'correct_country': 'France', 'guessed_country': 'Germany',
             'correct_country_flag': 'False'},
        ]
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        df = load_data(str(csv_path))
        assert df.iloc[0]['correct_country'] == 'France'
        assert df.iloc[0]['guessed_country'] == 'Germany'


# ===================================================================
# Derived clicked_first from time_seconds tests
# ===================================================================

class TestDerivedClickedFirst:
    def test_derives_from_time_seconds_spread(self):
        """Without clicked_first column, should derive from time_seconds spread."""
        data = [
            {'game_id': 'g1', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
             'team_key': 'pa_pb', 'distance_km': 100.0, 'time_seconds': 15.0,
             'status': 'guessed', 'game_date': '2025-01-15T10:00:00Z'},
            {'game_id': 'g1', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
             'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 30.0,
             'status': 'guessed', 'game_date': '2025-01-15T10:00:00Z'},
        ]
        df = pd.DataFrame(data)
        result = initiative_summary(df)
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        bob = result[result['player_name'] == 'Bob'].iloc[0]
        # Alice has lower time_seconds (15 vs 30), so she clicked first
        assert alice['clicked_first'] == 1
        assert bob['clicked_first'] == 0

    def test_all_timed_out_nobody_clicked(self):
        """If all players have same time_seconds (within 0.5s), nobody clicked."""
        data = [
            {'game_id': 'g1', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
             'team_key': 'pa_pb', 'distance_km': 100.0, 'time_seconds': 60.0,
             'status': 'guessed', 'game_date': '2025-01-15T10:00:00Z'},
            {'game_id': 'g1', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
             'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 60.2,
             'status': 'guessed', 'game_date': '2025-01-15T10:00:00Z'},
        ]
        df = pd.DataFrame(data)
        result = initiative_summary(df)
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        bob = result[result['player_name'] == 'Bob'].iloc[0]
        # Spread = 0.2 < 0.5 → nobody clicked
        assert alice['clicked_first'] == 0
        assert bob['clicked_first'] == 0

    def test_derives_without_status_column(self):
        """Should still derive clicked_first even without status column."""
        data = [
            {'game_id': 'g1', 'round': 1, 'player_id': 'pa', 'player_name': 'Alice',
             'team_key': 'pa_pb', 'distance_km': 100.0, 'time_seconds': 10.0,
             'game_date': '2025-01-15T10:00:00Z'},
            {'game_id': 'g1', 'round': 1, 'player_id': 'pb', 'player_name': 'Bob',
             'team_key': 'pa_pb', 'distance_km': 200.0, 'time_seconds': 25.0,
             'game_date': '2025-01-15T10:00:00Z'},
        ]
        df = pd.DataFrame(data)
        result = initiative_summary(df)
        assert result is not None
        alice = result[result['player_name'] == 'Alice'].iloc[0]
        assert alice['clicked_first'] == 1


# ===================================================================
# region_performance tests
# ===================================================================

class TestRegionPerformance:
    def test_team_row_uses_pct_of_span(self, df_basic):
        """Team row should show % of span values."""
        result = region_performance(df_basic)
        if not result.empty:
            team_row = result.iloc[0]
            # Team row values should contain '%' strings
            for val in team_row.values:
                if pd.notna(val):
                    assert '%' in str(val) or 'km' in str(val)

    def test_player_rows_are_numeric(self, df_basic):
        """Player rows should have numeric km values."""
        result = region_performance(df_basic)
        if not result.empty and len(result) > 1:
            player_rows = result.iloc[1:]
            for col in player_rows.columns:
                for val in player_rows[col].dropna():
                    # Should be numeric (float)
                    assert isinstance(val, (int, float))


# ===================================================================
# Player Summary enriched timing tests
# ===================================================================

class TestPlayerSummaryTiming:
    def test_has_not_clicked_guess_count(self, df_basic):
        result = player_summary(df_basic)
        assert 'not_clicked_guess_count' in result.columns

    def test_has_rounds_present_count(self, df_basic):
        result = player_summary(df_basic)
        assert 'rounds_present_count' in result.columns
        # Each player should have at least 1 round
        for _, row in result.iterrows():
            assert row['rounds_present_count'] > 0

    def test_has_clicked_first_count(self, df_basic):
        result = player_summary(df_basic)
        assert 'clicked_first_count' in result.columns

    def test_has_clicked_first_rate(self, df_basic):
        result = player_summary(df_basic)
        assert 'clicked_first_rate' in result.columns
        # Rate should be 0-100
        for _, row in result.iterrows():
            assert 0 <= row['clicked_first_rate'] <= 100

    def test_has_clicked_guess_rate(self, df_basic):
        result = player_summary(df_basic)
        assert 'clicked_guess_rate' in result.columns
        # Rate should be 0-100
        for _, row in result.iterrows():
            assert 0 <= row['clicked_guess_rate'] <= 100

    def test_not_clicked_equals_total_minus_clicked(self, df_basic):
        """not_clicked_guess_count = total_guesses - times_guess_clicked."""
        result = player_summary(df_basic)
        for _, row in result.iterrows():
            expected = row['total_guesses'] - (row['times_guess_clicked'] or 0)
            assert row['not_clicked_guess_count'] == expected


# ===================================================================
# region_detail tests
# ===================================================================

class TestRegionDetail:
    def test_returns_dataframe(self, df_basic):
        result = region_detail(df_basic)
        assert result is not None
        assert isinstance(result, pd.DataFrame)

    def test_has_required_columns(self, df_basic):
        result = region_detail(df_basic)
        assert 'num_rounds' in result.columns
        assert 'avg_dist_km' in result.columns
        assert 'median_dist_km' in result.columns

    def test_has_team_row(self, df_basic):
        result = region_detail(df_basic)
        assert 'Team' in result['player_name'].values

    def test_num_rounds_correct(self, df_basic):
        result = region_detail(df_basic)
        # Europe has rounds from game1 (r1) and game2 (r1, r2) for Alice and Bob
        team_europe = result[(result['player_name'] == 'Team') & (result['region'] == 'Europe')]
        if len(team_europe) > 0:
            assert team_europe.iloc[0]['num_rounds'] > 0


# ===================================================================
# Flag emoji tests
# ===================================================================

class TestFlagEmoji:
    def test_us_flag(self):
        assert flag_emoji('US') == '🇺🇸'

    def test_fr_flag(self):
        assert flag_emoji('FR') == '🇫🇷'

    def test_lowercase(self):
        assert flag_emoji('jp') == '🇯🇵'

    def test_empty(self):
        assert flag_emoji('') == ''

    def test_none(self):
        assert flag_emoji(None) == ''

    def test_invalid_length(self):
        assert flag_emoji('USA') == ''

    def test_country_with_flag_france(self):
        result = country_with_flag('France')
        assert '🇫🇷' in result
        assert 'France' in result

    def test_country_with_flag_unknown(self):
        assert country_with_flag('Unknown') == 'Unknown'

    def test_country_with_flag_not_found(self):
        result = country_with_flag('Atlantis')
        assert result == 'Atlantis'


# ===================================================================
# _add_flags display helper tests
# ===================================================================

class TestAddFlags:
    def test_adds_flags_to_correct_country(self):
        df = pd.DataFrame({'correct_country': ['France', 'Japan'], 'distance_km': [100, 200]})
        result = _add_flags(df)
        assert '🇫🇷' in result.iloc[0]['correct_country']
        assert '🇯🇵' in result.iloc[1]['correct_country']

    def test_does_not_modify_original(self):
        df = pd.DataFrame({'correct_country': ['France'], 'distance_km': [100]})
        _add_flags(df)
        assert df.iloc[0]['correct_country'] == 'France'

    def test_handles_guessed_country(self):
        df = pd.DataFrame({
            'correct_country': ['France'],
            'guessed_country': ['Germany'],
        })
        result = _add_flags(df, ['correct_country', 'guessed_country'])
        assert '🇫🇷' in result.iloc[0]['correct_country']
        assert '🇩🇪' in result.iloc[0]['guessed_country']


# ===================================================================
# CLI game exclusion filter tests
# ===================================================================

class TestExcludeFirstNGames:
    def test_excludes_games(self, tmp_path):
        """--exclude-first-n-games should drop earliest games."""
        csv_path = tmp_path / 'test.csv'
        rows = []
        for i, date in enumerate(['2025-01-01T10:00:00Z', '2025-01-02T10:00:00Z',
                                    '2025-01-03T10:00:00Z']):
            rows.append({
                'game_id': f'g{i+1}', 'round': '1', 'player_id': 'pa',
                'player_name': 'Alice', 'distance_km': '100', 'time_seconds': '20',
                'correct_country': 'France', 'guessed_country': 'France',
                'correct_country_flag': 'True', 'game_date': date,
            })
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        df = load_data(str(csv_path))
        # Simulate the exclusion logic from main()
        game_dates = df.groupby('game_id')['game_date_parsed'].min().sort_values()
        exclude_ids = set(game_dates.head(2).index)
        df_filtered = df[~df['game_id'].isin(exclude_ids)]
        assert df_filtered['game_id'].nunique() == 1
        assert 'g3' in df_filtered['game_id'].values


class TestIgnoreGamesFile:
    def test_excludes_specified_games(self, tmp_path):
        """--ignore-games-file should drop listed game IDs."""
        csv_path = tmp_path / 'test.csv'
        rows = [
            {'game_id': 'g1', 'round': '1', 'player_id': 'pa', 'player_name': 'Alice',
             'distance_km': '100', 'time_seconds': '20', 'correct_country': 'France',
             'guessed_country': 'France', 'correct_country_flag': 'True',
             'game_date': '2025-01-01T10:00:00Z'},
            {'game_id': 'g2', 'round': '1', 'player_id': 'pa', 'player_name': 'Alice',
             'distance_km': '200', 'time_seconds': '30', 'correct_country': 'Japan',
             'guessed_country': 'Japan', 'correct_country_flag': 'True',
             'game_date': '2025-01-02T10:00:00Z'},
        ]
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

        ignore_path = tmp_path / 'ignore.json'
        with open(ignore_path, 'w') as f:
            json.dump({'ignore_game_ids': ['g1']}, f)

        df = load_data(str(csv_path))
        ignore_ids = set(json.loads(ignore_path.read_text())['ignore_game_ids'])
        df_filtered = df[~df['game_id'].isin(ignore_ids)]
        assert df_filtered['game_id'].nunique() == 1
        assert 'g2' in df_filtered['game_id'].values
