#!/usr/bin/env python3
"""
PostgreSQL push module for GeoGuessr Team Duel Stats.

Loads CSV-style row data into a structured PostgreSQL schema with
games, rounds, and guesses tables. Supports replace/append/skip modes.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional

try:
    import psycopg2
    import psycopg2.extras
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


# ===================================================================
# Schema DDL
# ===================================================================

def get_ddl(schema: str) -> list:
    """Return ordered DDL statements for the geoguessr schema."""
    return [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",

        f"""CREATE TABLE IF NOT EXISTS {schema}.games (
            game_id          TEXT PRIMARY KEY,
            played_at        TIMESTAMPTZ,
            team_key         TEXT,
            competitive_mode TEXT,
            move_mode        TEXT,
            total_rounds     INTEGER,
            game_won         BOOLEAN,
            created_at       TIMESTAMPTZ DEFAULT NOW()
        )""",

        f"""CREATE TABLE IF NOT EXISTS {schema}.rounds (
            game_id              TEXT NOT NULL,
            round_number         INTEGER NOT NULL,
            pano_lat             DOUBLE PRECISION,
            pano_lng             DOUBLE PRECISION,
            pano_country_code    TEXT,
            pano_country_name    TEXT,
            region               TEXT,
            PRIMARY KEY (game_id, round_number),
            FOREIGN KEY (game_id) REFERENCES {schema}.games(game_id) ON DELETE CASCADE
        )""",

        f"""CREATE TABLE IF NOT EXISTS {schema}.guesses (
            game_id              TEXT NOT NULL,
            round_number         INTEGER NOT NULL,
            player_id            TEXT NOT NULL,
            player_name          TEXT,
            team_key             TEXT,
            guess_lat            DOUBLE PRECISION,
            guess_lng            DOUBLE PRECISION,
            guessed_country      TEXT,
            distance_m           DOUBLE PRECISION,
            distance_km          DOUBLE PRECISION,
            score                DOUBLE PRECISION,
            time_seconds         DOUBLE PRECISION,
            is_team_best_guess   BOOLEAN,
            won_team             BOOLEAN,
            won_round            BOOLEAN,
            correct_country_flag BOOLEAN,
            health_before        DOUBLE PRECISION,
            health_after         DOUBLE PRECISION,
            damage_dealt         DOUBLE PRECISION,
            multiplier           DOUBLE PRECISION,
            PRIMARY KEY (game_id, round_number, player_id),
            FOREIGN KEY (game_id, round_number) REFERENCES {schema}.rounds(game_id, round_number) ON DELETE CASCADE
        )""",
    ]


def get_indexes(schema: str) -> list:
    """Return CREATE INDEX statements."""
    return [
        f"CREATE INDEX IF NOT EXISTS idx_games_team_key ON {schema}.games(team_key)",
        f"CREATE INDEX IF NOT EXISTS idx_games_played_at ON {schema}.games(played_at)",
        f"CREATE INDEX IF NOT EXISTS idx_rounds_country ON {schema}.rounds(pano_country_code)",
        f"CREATE INDEX IF NOT EXISTS idx_guesses_player ON {schema}.guesses(player_id)",
        f"CREATE INDEX IF NOT EXISTS idx_guesses_team ON {schema}.guesses(team_key)",
    ]


# ===================================================================
# Data transformation: flat CSV rows -> normalized tables
# ===================================================================

def _parse_bool(val) -> Optional[bool]:
    """Parse a boolean value from CSV string."""
    if val is None:
        return None
    s = str(val).strip()
    if s == 'True':
        return True
    if s == 'False':
        return False
    return None


def _parse_float(val) -> Optional[float]:
    """Parse a float, returning None on failure."""
    if val is None or str(val).strip() == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_ts(val) -> Optional[str]:
    """Parse a timestamp string, returning ISO format or None."""
    if val is None or str(val).strip() == '':
        return None
    return str(val).strip()


def rows_to_tables(rows: List[Dict]) -> tuple:
    """Convert flat CSV rows into (games, rounds, guesses) tuples.

    Returns:
        games: list of tuples for INSERT
        rounds: list of tuples for INSERT
        guesses: list of tuples for INSERT
    """
    games_seen = {}
    rounds_seen = {}
    guesses = []

    for row in rows:
        game_id = row.get('game_id', '')
        round_num = int(row.get('round', 0))

        # Games table (one row per game)
        if game_id and game_id not in games_seen:
            games_seen[game_id] = (
                game_id,
                _parse_ts(row.get('game_date')),
                row.get('team_key', ''),
                row.get('competitive_mode', ''),
                row.get('move_mode', ''),
                int(row.get('total_rounds', 0)) if row.get('total_rounds') else None,
                _parse_bool(row.get('game_won')),
            )

        # Rounds table (one row per game+round)
        round_key = (game_id, round_num)
        if round_key not in rounds_seen:
            rounds_seen[round_key] = (
                game_id,
                round_num,
                _parse_float(row.get('correct_lat')),
                _parse_float(row.get('correct_lng')),
                row.get('correct_country_code', ''),
                row.get('correct_country', ''),
                row.get('region', ''),
            )

        # Guesses table (one row per game+round+player)
        guesses.append((
            game_id,
            round_num,
            row.get('player_id', ''),
            row.get('player_name', ''),
            row.get('team_key', ''),
            _parse_float(row.get('guess_lat')),
            _parse_float(row.get('guess_lng')),
            row.get('guessed_country', ''),
            _parse_float(row.get('distance_meters')),
            _parse_float(row.get('distance_km')),
            _parse_float(row.get('score')),
            _parse_float(row.get('time_seconds')),
            _parse_bool(row.get('is_team_best_guess')),
            _parse_bool(row.get('won_team')),
            _parse_bool(row.get('won_round')),
            _parse_bool(row.get('correct_country_flag')),
            _parse_float(row.get('health_before')),
            _parse_float(row.get('health_after')),
            _parse_float(row.get('damage_dealt')),
            _parse_float(row.get('multiplier')),
        ))

    return (
        list(games_seen.values()),
        list(rounds_seen.values()),
        guesses,
    )


# ===================================================================
# Push to Postgres
# ===================================================================

def push_to_postgres(
    rows: List[Dict],
    csv_columns: List[str],
    dsn: str,
    schema: str = 'geoguessr',
    if_exists: str = 'replace',
    batch_size: int = 5000,
):
    """Push flat CSV rows into PostgreSQL normalized tables.

    Args:
        rows: List of dicts (CSV rows)
        csv_columns: Column names (for reference)
        dsn: PostgreSQL connection string
        schema: Schema name
        if_exists: 'replace' (drop+recreate), 'append', or 'skip'
        batch_size: Insert batch size
    """
    if not HAS_PSYCOPG2:
        raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary")

    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # Handle if_exists strategy
        if if_exists == 'skip':
            # Check if tables exist and have data
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = 'games'
                )
            """, (schema,))
            if cur.fetchone()[0]:
                cur.execute(f"SELECT COUNT(*) FROM {schema}.games")
                count = cur.fetchone()[0]
                if count > 0:
                    print(f"  Skip mode: {schema}.games has {count} rows, skipping load.")
                    return

        if if_exists == 'replace':
            print(f"  Dropping existing tables in schema {schema}...")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.guesses CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.rounds CASCADE")
            cur.execute(f"DROP TABLE IF EXISTS {schema}.games CASCADE")
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

        # Create schema + tables
        print(f"  Creating schema and tables...")
        for ddl in get_ddl(schema):
            cur.execute(ddl)

        # Transform rows
        print(f"  Transforming {len(rows)} rows...")
        games, rounds, guesses = rows_to_tables(rows)
        print(f"  {len(games)} games, {len(rounds)} rounds, {len(guesses)} guesses")

        # Insert games
        print(f"  Inserting games...")
        games_sql = f"""
            INSERT INTO {schema}.games
            (game_id, played_at, team_key, competitive_mode, move_mode, total_rounds, game_won)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO NOTHING
        """
        for i in range(0, len(games), batch_size):
            batch = games[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, games_sql, batch)

        # Insert rounds
        print(f"  Inserting rounds...")
        rounds_sql = f"""
            INSERT INTO {schema}.rounds
            (game_id, round_number, pano_lat, pano_lng, pano_country_code, pano_country_name, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id, round_number) DO NOTHING
        """
        for i in range(0, len(rounds), batch_size):
            batch = rounds[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, rounds_sql, batch)

        # Insert guesses
        print(f"  Inserting guesses...")
        guesses_sql = f"""
            INSERT INTO {schema}.guesses
            (game_id, round_number, player_id, player_name, team_key,
             guess_lat, guess_lng, guessed_country,
             distance_m, distance_km, score, time_seconds,
             is_team_best_guess, won_team, won_round, correct_country_flag,
             health_before, health_after, damage_dealt, multiplier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id, round_number, player_id) DO NOTHING
        """
        for i in range(0, len(guesses), batch_size):
            batch = guesses[i:i + batch_size]
            psycopg2.extras.execute_batch(cur, guesses_sql, batch)

        # Create indexes
        print(f"  Creating indexes...")
        for idx_sql in get_indexes(schema):
            cur.execute(idx_sql)

        conn.commit()
        print(f"  ✅ Loaded into PostgreSQL: {len(games)} games, {len(rounds)} rounds, {len(guesses)} guesses")

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
