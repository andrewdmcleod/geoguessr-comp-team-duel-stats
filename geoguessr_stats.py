#!/usr/bin/env python3
"""
GeoGuessr Team Duel Stats Fetcher

Fetches competitive team duel stats from GeoGuessr and exports to CSV.

NOTE: The GeoGuessr API only provides each player's FINAL guess position.
There is no way to distinguish between clicking "Guess" vs timer expiry,
or first pin drop vs final pin position. Every guess entry is the final
state when the round ended.
"""

import json
import csv
import argparse
import time
import os
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from country_codes import country_name_from_code, CODE_TO_REGION

# ---------------------------------------------------------------------------
# CSV column order (30 columns)
# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    'game_id', 'game_date', 'round', 'total_rounds',
    'competitive_mode', 'move_mode',
    'player_id', 'player_name',
    'time_seconds', 'distance_meters', 'distance_km', 'score',
    'correct_lat', 'correct_lng', 'guess_lat', 'guess_lng',
    'correct_country_code', 'correct_country', 'guessed_country',
    'correct_country_flag', 'region',
    'is_team_best_guess', 'won_team', 'won_round', 'game_won',
    'health_before', 'health_after', 'damage_dealt', 'multiplier',
]

# ---------------------------------------------------------------------------
# Raw data cache directory
# ---------------------------------------------------------------------------
RAW_DATA_DIR = Path('raw_data')
RAW_GAMES_DIR = RAW_DATA_DIR / 'games'
RAW_FEED_DIR = RAW_DATA_DIR / 'feed'


def ensure_raw_dirs():
    """Create raw data cache directories if they don't exist."""
    RAW_GAMES_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FEED_DIR.mkdir(parents=True, exist_ok=True)


# ===================================================================
# GeoGuessr API
# ===================================================================

class GeoGuessrAPI:
    """Interface to GeoGuessr API"""

    BASE_URL = "https://www.geoguessr.com/api"
    GAME_SERVER_URL = "https://game-server.geoguessr.com/api"

    def __init__(self, cookie: str):
        self.session = requests.Session()
        self.session.headers.update({
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_user_profile(self) -> Dict:
        """Get current user profile"""
        url = f"{self.BASE_URL}/v3/profiles"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def get_user_activity(self, count: int = 100, pagination_token: Optional[str] = None) -> Dict:
        """Get user activity feed with cursor-based pagination"""
        url = f"{self.BASE_URL}/v4/feed/private"
        params = {'count': count}
        if pagination_token:
            params['paginationToken'] = pagination_token
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_team_duel_game_ids(self, limit: Optional[int] = None) -> List[str]:
        """Extract team duel game IDs from activity feed"""
        game_ids = []
        seen_ids = set()
        page = 0
        pagination_token = None

        while True:
            print(f"  Fetching activity feed page {page + 1}...")
            feed_data = self.get_user_activity(count=100, pagination_token=pagination_token)

            # Save raw feed page
            feed_file = RAW_FEED_DIR / f"page_{page}.json"
            with open(feed_file, 'w') as f:
                json.dump(feed_data, f)

            activities = feed_data.get('entries', [])
            if not activities:
                break

            new_on_page = 0

            for activity in activities:
                if not isinstance(activity, dict):
                    continue

                if activity.get('type') == 7:
                    payload_str = activity.get('payload', '')
                    if not payload_str:
                        continue

                    try:
                        games_list = json.loads(payload_str)
                        for game_entry in games_list:
                            if game_entry.get('type') == 6:
                                game_payload = game_entry.get('payload', {})
                                game_id = game_payload.get('gameId')
                                competitive_mode = game_payload.get('competitiveGameMode')

                                if game_id and competitive_mode and competitive_mode != 'None':
                                    if game_id not in seen_ids:
                                        seen_ids.add(game_id)
                                        game_ids.append(game_id)
                                        new_on_page += 1
                                        print(f"    Found {competitive_mode}: {game_id}")

                                        if limit and len(game_ids) >= limit:
                                            return game_ids
                    except json.JSONDecodeError:
                        continue

            pagination_token = feed_data.get('paginationToken')
            if not pagination_token:
                print(f"  No more pages (no pagination token).")
                break

            if new_on_page == 0:
                print(f"  No new games on page {page + 1}, stopping.")
                break

            page += 1
            time.sleep(0.3)

            if page >= 50:
                break

        return game_ids

    def get_game_details(self, game_id: str) -> Dict:
        """Get game data — from local cache or API"""
        # Check local cache first
        cache_file = RAW_GAMES_DIR / f"{game_id}.json"
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)

        # Fetch from API
        url = f"{self.GAME_SERVER_URL}/duels/{game_id}"
        response = self.session.get(url)

        if response.status_code == 404:
            url = f"{self.BASE_URL}/v3/games/{game_id}"
            response = self.session.get(url)

        response.raise_for_status()
        data = response.json()

        # Save to local cache
        with open(cache_file, 'w') as f:
            json.dump(data, f)

        return data

    def get_player_profile(self, player_id: str) -> Dict:
        """Get player profile to retrieve nickname"""
        url = f"{self.BASE_URL}/v3/users/{player_id}"
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                return response.json()
        except Exception:
            pass
        return {}


# ===================================================================
# Geocoding providers
# ===================================================================

class GeocodingProvider:
    """Base class for geocoding providers"""

    def reverse_geocode(self, lat: float, lng: float) -> Optional[str]:
        raise NotImplementedError


class NominatimProvider(GeocodingProvider):
    """OpenStreetMap Nominatim (free, no API key, strict rate limits)"""

    def __init__(self):
        self.geocoder = Nominatim(user_agent="geoguessr_stats_tool", timeout=10)

    def reverse_geocode(self, lat: float, lng: float) -> Optional[str]:
        location = self.geocoder.reverse(f"{lat}, {lng}", language='en')
        if location and 'address' in location.raw:
            return (location.raw['address'].get('country')
                    or location.raw['address'].get('sea')
                    or location.raw['address'].get('ocean'))
        return None


class OpenCageProvider(GeocodingProvider):
    """OpenCage Geocoding API (free tier: 2500 req/day)"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('OPENCAGE_API_KEY')
        if not self.api_key:
            raise ValueError(
                "OpenCage requires an API key. Set OPENCAGE_API_KEY env var "
                "or add 'opencage_api_key' to config.json. "
                "Get a free key at https://opencagedata.com/users/sign_up"
            )
        self.url = "https://api.opencagedata.com/geocode/v1/json"

    def reverse_geocode(self, lat: float, lng: float) -> Optional[str]:
        params = {
            'q': f"{lat},{lng}",
            'key': self.api_key,
            'no_annotations': 1,
            'language': 'en',
        }
        resp = requests.get(self.url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get('results', [])
        if results:
            components = results[0].get('components', {})
            return (components.get('country')
                    or components.get('body_of_water')
                    or components.get('ocean'))
        return None


class GoogleProvider(GeocodingProvider):
    """Google Maps Geocoding API (requires billing-enabled API key)"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get('GOOGLE_MAPS_API_KEY')
        if not self.api_key:
            raise ValueError(
                "Google geocoding requires an API key. Set GOOGLE_MAPS_API_KEY env var "
                "or add 'google_maps_api_key' to config.json. "
                "See https://developers.google.com/maps/documentation/geocoding/get-api-key"
            )
        self.url = "https://maps.googleapis.com/maps/api/geocode/json"

    def reverse_geocode(self, lat: float, lng: float) -> Optional[str]:
        # Try country-specific first
        params = {
            'latlng': f"{lat},{lng}",
            'key': self.api_key,
            'result_type': 'country',
            'language': 'en',
        }
        resp = requests.get(self.url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get('results', [])
        for result in results:
            for component in result.get('address_components', []):
                if 'country' in component.get('types', []):
                    return component.get('long_name')

        # No country found — try without filter for ocean/water features
        params_broad = {
            'latlng': f"{lat},{lng}",
            'key': self.api_key,
            'language': 'en',
        }
        resp2 = requests.get(self.url, params=params_broad, timeout=10)
        resp2.raise_for_status()
        data2 = resp2.json()
        results2 = data2.get('results', [])
        for result in results2:
            types = result.get('types', [])
            if 'natural_feature' in types or 'establishment' in types:
                return result.get('formatted_address', 'Unknown body of water')
        return None


GEOCODING_PROVIDERS = {
    'nominatim': NominatimProvider,
    'opencage': OpenCageProvider,
    'google': GoogleProvider,
}

# Default delays (seconds) per provider based on free-tier rate limits
PROVIDER_DEFAULT_DELAYS = {
    'nominatim': 1.5,
    'opencage': 1.0,
    'google': 0.05,
}

PROVIDER_FALLBACK_ORDER = ['google', 'opencage', 'nominatim']


def _try_init_provider(name: str, config: Dict) -> Optional[GeocodingProvider]:
    """Try to initialise a provider, return None if missing API key or error."""
    try:
        if name == 'opencage':
            return OpenCageProvider(api_key=config.get('opencage_api_key'))
        elif name == 'google':
            return GoogleProvider(api_key=config.get('google_maps_api_key'))
        elif name == 'nominatim':
            return NominatimProvider()
        return None
    except ValueError:
        return None


class ReverseGeocoder:
    """Handles reverse geocoding with rate limiting and fallback provider support"""

    def __init__(self, enable_geocoding: bool = True, delay: Optional[float] = None,
                 provider_name: str = 'google', config: Optional[Dict] = None):
        self.enable = enable_geocoding
        self.cache = {}
        self.provider = None
        self.fallbacks = []
        self.provider_name = provider_name
        self.delay = delay if delay is not None else PROVIDER_DEFAULT_DELAYS.get(provider_name, 1.0)
        self.api_calls = 0

        if not enable_geocoding:
            return

        config = config or {}

        self.provider = _try_init_provider(provider_name, config)
        if not self.provider:
            raise ValueError(
                f"Cannot initialise provider '{provider_name}'. "
                f"Check API key in config.json or env vars."
            )
        print(f"  Primary geocoding provider: {provider_name} (delay: {self.delay}s)")

        for fb_name in PROVIDER_FALLBACK_ORDER:
            if fb_name == provider_name:
                continue
            fb_provider = _try_init_provider(fb_name, config)
            if fb_provider:
                fb_delay = PROVIDER_DEFAULT_DELAYS.get(fb_name, 1.5)
                self.fallbacks.append((fb_name, fb_provider, fb_delay))
                print(f"  Fallback geocoding provider: {fb_name} (delay: {fb_delay}s)")

    def _try_provider(self, provider: GeocodingProvider, lat: float, lng: float,
                      delay: float, name: str) -> Optional[str]:
        for attempt in range(2):
            try:
                time.sleep(delay if attempt == 0 else delay * 2)
                self.api_calls += 1
                country = provider.reverse_geocode(lat, lng)
                if country:
                    return country
                return None
            except (GeocoderTimedOut, GeocoderServiceError, requests.RequestException) as e:
                if attempt < 1:
                    print(f"  \u26a0\ufe0f  {name} retry: {e}")
                else:
                    print(f"  \u26a0\ufe0f  {name} failed: {e}")
        return None

    def get_country(self, lat: float, lng: float) -> Optional[str]:
        """Get country from coordinates with caching and fallback.
        Returns 'Lost at Sea' for ocean coordinates, None only when disabled."""
        if not self.enable or not self.provider:
            return None

        cache_key = f"{lat:.3f},{lng:.3f}"
        if cache_key in self.cache:
            return self.cache[cache_key]

        print(f"  \U0001f30d Reverse geocoding ({lat:.4f}, {lng:.4f})...", end='\r')

        country = self._try_provider(self.provider, lat, lng, self.delay, self.provider_name)

        if country is None:
            for fb_name, fb_provider, fb_delay in self.fallbacks:
                print(f"  \U0001f504 Falling back to {fb_name}...")
                country = self._try_provider(fb_provider, lat, lng, fb_delay, fb_name)
                if country is not None:
                    break

        if country is None:
            country = "Lost at Sea"

        self.cache[cache_key] = country
        return country


# ===================================================================
# Game data processing
# ===================================================================

def process_game_data(game: Dict, team_members: Dict[str, str],
                      geocoder: ReverseGeocoder, user_id: str,
                      my_team_only: bool = False) -> List[Dict]:
    """Process a single game and extract player stats.

    When my_team_only is True, only geocode/output rows for the user's team
    but still read opponent distances for won_round computation.
    """
    game_id = game.get('gameId') or game.get('token')
    options = game.get('options', {})
    movement_options = options.get('movementOptions', {})
    forbidden_moving = movement_options.get('forbidMoving', False)
    move_mode = 'no_move' if forbidden_moving else 'move'
    competitive_mode = options.get('competitiveGameMode', 'Unknown')

    rounds = game.get('rounds', [])
    teams = game.get('teams', [])
    total_rounds = game.get('currentRoundNumber', len(rounds))

    # Game date from first round
    game_date = ''
    if rounds:
        first_start = rounds[0].get('startTime')
        if first_start:
            game_date = first_start

    # Game result
    result = game.get('result', {})
    is_draw = result.get('isDraw', False)
    winning_team_id = result.get('winningTeamId')

    # Build lookups
    player_to_team_id = {}
    team_id_lookup = {}
    for team in teams:
        tid = team.get('id') or team.get('teamId', '')
        team_id_lookup[tid] = team
        for player in team.get('players', []):
            player_to_team_id[player.get('playerId')] = tid

    # Find the user's team for this game
    user_team_id = player_to_team_id.get(user_id, '')
    my_team_pids = set()
    if user_team_id:
        team_data = team_id_lookup.get(user_team_id, {})
        my_team_pids = {p.get('playerId') for p in team_data.get('players', [])}

    # Round results lookup: team_id -> round_num -> {healthBefore, ...}
    round_results_lookup = {}
    for team in teams:
        tid = team.get('id') or team.get('teamId', '')
        round_results_lookup[tid] = {}
        for rr in team.get('roundResults', []):
            rn = rr.get('roundNumber')
            round_results_lookup[tid][rn] = rr

    # Build round start times lookup
    round_start_times = {}
    for rd in rounds:
        rn = rd.get('roundNumber')
        st = rd.get('startTime')
        if rn and st:
            round_start_times[rn] = st

    # Count geocode lookups needed
    geocode_count = 0
    for team in teams:
        for player in team.get('players', []):
            pid = player.get('playerId')
            if my_team_only and pid not in my_team_pids:
                continue
            geocode_count += len([g for g in player.get('guesses', [])
                                  if g.get('lat') is not None])

    print(f"    {len(rounds)} rounds, ~{geocode_count} geocode lookups needed")

    # ---- Collect all raw guess rows (all players, for cross-team stats) ----
    raw_rows = []

    for round_data in rounds:
        round_num = round_data.get('roundNumber')
        panorama = round_data.get('panorama', {})
        correct_lat = panorama.get('lat')
        correct_lng = panorama.get('lng')

        if correct_lat is None or correct_lng is None:
            continue

        # Correct country from panorama.countryCode (no geocoding!)
        correct_country_code = panorama.get('countryCode', '')
        if correct_country_code:
            correct_country = country_name_from_code(correct_country_code)
        else:
            # Fallback for old games missing countryCode
            correct_country = geocoder.get_country(correct_lat, correct_lng) or 'Unknown'
            correct_country_code = ''

        correct_region = CODE_TO_REGION.get(correct_country_code.upper(), 'Other') if correct_country_code else 'Other'

        round_start = round_start_times.get(round_num)

        for team in teams:
            tid = team.get('id') or team.get('teamId', '')
            rr = round_results_lookup.get(tid, {}).get(round_num, {})

            for player in team.get('players', []):
                player_id = player.get('playerId')
                player_name = team_members.get(player_id, player_id)
                is_opponent = player_id not in my_team_pids

                guesses = player.get('guesses', [])
                guess_for_round = None
                for guess in guesses:
                    if guess.get('roundNumber') == round_num:
                        guess_for_round = guess
                        break

                if not guess_for_round:
                    continue

                guess_lat = guess_for_round.get('lat')
                guess_lng = guess_for_round.get('lng')
                distance = guess_for_round.get('distance', 0)
                guess_score = guess_for_round.get('score', 0)
                is_team_best = guess_for_round.get('isTeamsBestGuessOnRound', False)

                if guess_lat is None or guess_lng is None:
                    continue

                # Time calculation
                time_to_guess = 0.0
                guess_created = guess_for_round.get('created')
                if round_start and guess_created:
                    try:
                        start_dt = datetime.fromisoformat(round_start.replace('Z', '+00:00'))
                        guess_dt = datetime.fromisoformat(guess_created.replace('Z', '+00:00'))
                        time_to_guess = (guess_dt - start_dt).total_seconds()
                    except Exception:
                        time_to_guess = 0.0

                # Geocode guessed country (skip for opponents in my-team-only mode)
                if my_team_only and is_opponent:
                    guessed_country = ''
                else:
                    guessed_country = geocoder.get_country(guess_lat, guess_lng) or 'Unknown'

                # Correct country flag
                if guessed_country and guessed_country not in ('', 'Unknown', 'Lost at Sea') and correct_country not in ('Unknown',):
                    correct_flag = (guessed_country == correct_country)
                else:
                    correct_flag = ''

                # Game won
                if is_draw:
                    game_won = 'draw'
                elif winning_team_id:
                    game_won = (tid == winning_team_id)
                else:
                    team_obj = team_id_lookup.get(tid, {})
                    game_won = team_obj.get('health', 0) > 0

                raw_rows.append({
                    'game_id': game_id,
                    'game_date': game_date,
                    'round': round_num,
                    'total_rounds': total_rounds,
                    'competitive_mode': competitive_mode,
                    'move_mode': move_mode,
                    'player_id': player_id,
                    'player_name': player_name,
                    'time_seconds': round(time_to_guess, 2),
                    'distance_meters': round(distance, 2),
                    'distance_km': round(distance / 1000, 2),
                    'score': guess_score,
                    'correct_lat': correct_lat,
                    'correct_lng': correct_lng,
                    'guess_lat': guess_lat,
                    'guess_lng': guess_lng,
                    'correct_country_code': correct_country_code,
                    'correct_country': correct_country,
                    'guessed_country': guessed_country,
                    'correct_country_flag': correct_flag,
                    'region': correct_region,
                    'is_team_best_guess': is_team_best,
                    'won_team': False,   # computed below
                    'won_round': False,  # computed below
                    'game_won': game_won,
                    'health_before': rr.get('healthBefore', ''),
                    'health_after': rr.get('healthAfter', ''),
                    'damage_dealt': rr.get('damageDealt', ''),
                    'multiplier': rr.get('multiplier', ''),
                    '_team_id': tid,  # internal, stripped before output
                })

    # ---- Compute won_team and won_round ----
    by_round = defaultdict(list)
    for row in raw_rows:
        by_round[row['round']].append(row)

    for round_num, round_rows in by_round.items():
        if not round_rows:
            continue

        # won_round: best distance across ALL players
        best_overall = min(r['distance_meters'] for r in round_rows)
        for r in round_rows:
            if r['distance_meters'] <= best_overall:
                r['won_round'] = True

        # won_team: best distance within each team
        by_team = defaultdict(list)
        for r in round_rows:
            by_team[r['_team_id']].append(r)

        for tid, team_rows in by_team.items():
            best_team = min(r['distance_meters'] for r in team_rows)
            for r in team_rows:
                if r['distance_meters'] <= best_team:
                    r['won_team'] = True

    # ---- Filter & clean ----
    output_rows = []
    for row in raw_rows:
        if my_team_only and row['player_id'] not in my_team_pids:
            continue
        del row['_team_id']
        output_rows.append(row)

    return output_rows


# ===================================================================
# Incremental CSV helpers
# ===================================================================

def load_existing_csv(csv_path: str):
    """Load existing CSV. Returns (set of game_ids, list of row dicts, list of column names)."""
    path = Path(csv_path)
    if not path.exists():
        return set(), [], []

    existing_rows = []
    existing_game_ids = set()
    with open(path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        for row in reader:
            existing_rows.append(row)
            existing_game_ids.add(row.get('game_id'))

    return existing_game_ids, existing_rows, columns


def migrate_old_rows(old_rows: list, old_columns: list) -> list:
    """Add missing columns to old CSV rows with empty/default values.
    Preserves existing geocoding data."""
    if not old_rows:
        return []
    if set(old_columns) >= set(CSV_COLUMNS):
        return old_rows  # No migration needed

    print(f"  Migrating {len(old_rows)} existing rows to new format ({len(old_columns)} -> {len(CSV_COLUMNS)} columns)")
    migrated = []
    for row in old_rows:
        new_row = {col: row.get(col, '') for col in CSV_COLUMNS}
        migrated.append(new_row)
    return migrated


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(description='Fetch GeoGuessr team duel stats')
    parser.add_argument('--limit', type=int,
                        help='Limit number of games to fetch (for testing)')
    parser.add_argument('--csv', type=str, default=None,
                        help='Path to persistent CSV file. New games will be appended. '
                             'If not specified, a timestamped file is created.')
    parser.add_argument('--no-geocode', action='store_true',
                        help='Disable reverse geocoding (faster but no country data)')
    parser.add_argument('--geo-provider', type=str, default='google',
                        choices=list(GEOCODING_PROVIDERS.keys()),
                        help='Primary geocoding provider (default: google, falls back to others)')
    parser.add_argument('--geocode-delay', type=float, default=None,
                        help='Delay between geocoding requests in seconds (default: auto per provider)')
    parser.add_argument('--my-team-only', action='store_true',
                        help='Only include guesses from you and your teammate')
    args = parser.parse_args()

    # Ensure raw data dirs exist
    ensure_raw_dirs()

    # Load config
    config_path = Path('config.json')
    if not config_path.exists():
        print("\u274c config.json not found!")
        return

    with open(config_path) as f:
        config = json.load(f)

    # Load cookie
    cookie_file = Path(config.get('cookie_file', 'cookie.txt'))
    if not cookie_file.exists():
        print(f"\u274c {cookie_file} not found!")
        return

    with open(cookie_file) as f:
        cookie = f.read().strip()

    # Initialize API
    api = GeoGuessrAPI(cookie)
    try:
        geocoder = ReverseGeocoder(
            enable_geocoding=not args.no_geocode,
            delay=args.geocode_delay,
            provider_name=args.geo_provider,
            config=config
        )
    except ValueError as e:
        print(f"\u274c {e}")
        return

    print("\U0001f3ae GeoGuessr Team Duel Stats Fetcher")
    print("=" * 50)

    # Load existing CSV for incremental mode
    existing_game_ids = set()
    existing_rows = []
    existing_columns = []
    if args.csv:
        existing_game_ids, existing_rows, existing_columns = load_existing_csv(args.csv)
        if existing_game_ids:
            print(f"\n\U0001f4c2 Found {len(existing_game_ids)} games already in {args.csv}")

    # Collect all team members
    all_team_members = {}

    # Get user profile
    print(f"\n\U0001f4cb Fetching user profile...")
    try:
        profile = api.get_user_profile()
        user_data = profile.get('user', profile)
        username = user_data.get('nick', 'Unknown')
        user_id = user_data.get('id', config.get('player_id'))
        print(f"\u2705 Logged in as: {username} (ID: {user_id})")
        all_team_members[user_id] = username
    except Exception as e:
        print(f"\u274c Error fetching profile: {e}")
        print("   Make sure your cookie is valid!")
        return

    # Fetch team duel game IDs
    print(f"\n\U0001f3af Fetching team duel games from activity feed...")
    if args.limit:
        print(f"   (Limited to {args.limit} games for testing)")

    game_ids = api.get_team_duel_game_ids(limit=args.limit)

    if not game_ids:
        print("\u274c No team duel games found in activity feed!")
        print("   Make sure you've played some competitive team duels.")
        return

    print(f"\u2705 Found {len(game_ids)} team duel game(s) in feed")

    # Filter to only new games
    new_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
    if existing_game_ids:
        print(f"   {len(new_game_ids)} new game(s) to process ({len(existing_game_ids)} already in CSV)")

    if not new_game_ids and existing_rows:
        print("\n\u2705 No new games to fetch! CSV is up to date.")
        # Still migrate if needed
        if existing_columns and set(existing_columns) < set(CSV_COLUMNS):
            migrated = migrate_old_rows(existing_rows, existing_columns)
            output_file = args.csv
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(migrated)
            print(f"   (Migrated CSV format to {len(CSV_COLUMNS)} columns)")
        print(f"\n\U0001f389 Done!")
        return

    # Process games
    print(f"\n\u2699\ufe0f  Processing game data...")
    if not args.no_geocode:
        print(f"   (Reverse geocoding enabled - {args.geo_provider}, {geocoder.delay}s delay)")
    else:
        print(f"   (Geocoding disabled)")

    new_rows = []

    for idx, game_id in enumerate(new_game_ids, 1):
        cached = (RAW_GAMES_DIR / f"{game_id}.json").exists()
        src = "cached" if cached else "API"
        print(f"\n  [{idx}/{len(new_game_ids)}] Fetching game {game_id} ({src})...")

        try:
            game = api.get_game_details(game_id)

            # Collect player IDs
            for team in game.get('teams', []):
                for player in team.get('players', []):
                    pid = player.get('playerId')
                    if pid and pid not in all_team_members:
                        all_team_members[pid] = pid

            rows = process_game_data(
                game, all_team_members, geocoder, user_id,
                my_team_only=args.my_team_only
            )
            new_rows.extend(rows)

            n_rounds = game.get('currentRoundNumber', len(game.get('rounds', [])))
            print(f"  \u2705 Extracted {len(rows)} guesses from {n_rounds} rounds")

        except Exception as e:
            print(f"  \u26a0\ufe0f  Error processing game: {e}")
            continue

    # Fetch nicknames for players we don't have yet
    if args.my_team_only:
        # Only fetch names for our team
        players_without_names = [
            pid for pid, name in all_team_members.items()
            if pid == name
        ]
    else:
        players_without_names = [pid for pid, name in all_team_members.items() if pid == name]

    if players_without_names:
        print(f"\n\U0001f465 Fetching nicknames for {len(players_without_names)} players...")
        for player_id in players_without_names:
            try:
                profile = api.get_player_profile(player_id)
                nickname = profile.get('nick', player_id)
                all_team_members[player_id] = nickname
                time.sleep(0.2)
            except Exception:
                pass

    # Update names in new rows
    for row in new_rows:
        row['player_name'] = all_team_members.get(row['player_id'], row['player_id'])

    # Print player list
    relevant_pids = {r['player_id'] for r in new_rows}
    if relevant_pids:
        print(f"\n\U0001f465 Players in new games:")
        for pid in relevant_pids:
            print(f"  - {all_team_members.get(pid, pid)} ({pid})")

    # Merge with existing data
    if existing_rows:
        migrated = migrate_old_rows(existing_rows, existing_columns)
        all_rows = migrated + new_rows
    else:
        all_rows = new_rows

    # Export to CSV
    output_file = args.csv or f"team_duel_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    print(f"\n\U0001f4be Exporting to {output_file}...")

    if all_rows:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"\u2705 Exported {len(all_rows)} rows to {output_file}")
        if existing_rows:
            print(f"   ({len(existing_rows)} existing + {len(new_rows)} new)")
    else:
        print("\u26a0\ufe0f  No data to export!")

    if geocoder.api_calls > 0:
        print(f"\n\U0001f4ca Geocoding stats: {geocoder.api_calls} API calls, {len(geocoder.cache)} cached results")

    print(f"\n\U0001f389 Done!")


if __name__ == '__main__':
    main()
