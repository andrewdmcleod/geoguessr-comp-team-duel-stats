#!/usr/bin/env python3
"""
GeoGuessr Team Duel Stats Fetcher

Fetches competitive team duel stats from GeoGuessr and exports to CSV.

Timing model: In competitive team duels, one player clicks "Guess" which
starts a 15-second countdown. Other players can click within that window
or their pin is auto-submitted at round end.

Key derived columns:
- time_seconds: guess_created - round_start (elapsed time per player)
- time_remaining_sec: round_end - guess_created (higher = clicked earlier,
  near 0 = auto-submitted pin drop, ~15 = triggered the countdown)
- clicked_first: True for the player who triggered the countdown
- status: 'guessed' (has a guess row) or 'no_pin' (no guess at all)
"""

import json
import csv
import argparse
import time
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from datetime import datetime, timezone

import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

try:
    import questionary
    HAS_QUESTIONARY = True
except ImportError:
    HAS_QUESTIONARY = False

from country_codes import country_name_from_code, CODE_TO_REGION, normalize_country_name

# ---------------------------------------------------------------------------
# CSV column order (30 columns)
# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    'team_key',
    'game_id', 'game_date', 'round', 'total_rounds',
    'competitive_mode', 'move_mode',
    'player_id', 'player_name',
    'time_seconds', 'distance_meters', 'distance_km', 'score',
    'correct_lat', 'correct_lng', 'guess_lat', 'guess_lng',
    'correct_country_code', 'correct_country', 'guessed_country',
    'correct_country_flag', 'region',
    'is_team_best_guess', 'won_team', 'won_round', 'game_won',
    'health_before', 'health_after', 'damage_dealt', 'multiplier',
    # Initiative & timing columns (v0.3.0)
    'guess_created', 'round_start_time', 'round_end_time',
    'timer_start_time', 'round_duration_sec', 'time_remaining_sec',
    'clicked_first', 'status',
]

# ---------------------------------------------------------------------------
# Raw data cache directory
# ---------------------------------------------------------------------------
RAW_DATA_DIR = Path('raw_data')
RAW_GAMES_DIR = RAW_DATA_DIR / 'games'
RAW_FEED_DIR = RAW_DATA_DIR / 'feed'
NICKNAMES_CACHE = RAW_DATA_DIR / 'nicknames.json'


def ensure_raw_dirs():
    """Create raw data cache directories if they don't exist."""
    RAW_GAMES_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FEED_DIR.mkdir(parents=True, exist_ok=True)


def load_nickname_cache() -> Dict[str, str]:
    """Load cached player nicknames from disk."""
    if NICKNAMES_CACHE.is_file():
        try:
            with open(NICKNAMES_CACHE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_nickname_cache(cache: Dict[str, str]):
    """Save player nickname cache to disk."""
    NICKNAMES_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(NICKNAMES_CACHE, 'w') as f:
        json.dump(cache, f, indent=2)


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

    def get_team_duel_game_ids(self, limit: Optional[int] = None,
                               max_pages: int = 50) -> List[str]:
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

            if page >= max_pages:
                print(f"  Reached max pages limit ({max_pages}). Stopping pagination.")
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
                      my_team_only: bool = False,
                      team_key: str = '') -> List[Dict]:
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

    # Build round timing lookups
    round_start_times = {}
    round_end_times = {}
    round_timer_start_times = {}
    for rd in rounds:
        rn = rd.get('roundNumber')
        if rn:
            st = rd.get('startTime')
            et = rd.get('endTime')
            tst = rd.get('timerStartTime')
            if st:
                round_start_times[rn] = st
            if et:
                round_end_times[rn] = et
            if tst:
                round_timer_start_times[rn] = tst

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
        round_end = round_end_times.get(round_num)
        timer_start = round_timer_start_times.get(round_num)

        # Compute round duration
        round_duration_sec = ''
        if round_start and round_end:
            try:
                rs_dt = datetime.fromisoformat(round_start.replace('Z', '+00:00'))
                re_dt = datetime.fromisoformat(round_end.replace('Z', '+00:00'))
                round_duration_sec = round((re_dt - rs_dt).total_seconds(), 2)
            except Exception:
                round_duration_sec = ''

        # Game won (shared for all players in this round)
        def compute_game_won(tid):
            if is_draw:
                return 'draw'
            elif winning_team_id:
                return (tid == winning_team_id)
            else:
                team_obj = team_id_lookup.get(tid, {})
                return team_obj.get('health', 0) > 0

        for team in teams:
            tid = team.get('id') or team.get('teamId', '')
            rr = round_results_lookup.get(tid, {}).get(round_num, {})
            game_won = compute_game_won(tid)

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

                if guess_for_round:
                    # ---- GUESSED: player dropped a pin ----
                    status = 'guessed'
                    guess_lat = guess_for_round.get('lat')
                    guess_lng = guess_for_round.get('lng')
                    distance = guess_for_round.get('distance', 0)
                    guess_score = guess_for_round.get('score', 0)
                    is_team_best = guess_for_round.get('isTeamsBestGuessOnRound', False)
                    guess_created_ts = guess_for_round.get('created', '')

                    if guess_lat is None or guess_lng is None:
                        continue

                    # Time from round start to guess
                    time_to_guess = 0.0
                    if round_start and guess_created_ts:
                        try:
                            start_dt = datetime.fromisoformat(round_start.replace('Z', '+00:00'))
                            guess_dt = datetime.fromisoformat(guess_created_ts.replace('Z', '+00:00'))
                            time_to_guess = (guess_dt - start_dt).total_seconds()
                        except Exception:
                            time_to_guess = 0.0

                    # Time remaining when guess submitted (higher = clicked earlier)
                    time_remaining = 0.0
                    if round_end and guess_created_ts:
                        try:
                            end_dt = datetime.fromisoformat(round_end.replace('Z', '+00:00'))
                            guess_dt = datetime.fromisoformat(guess_created_ts.replace('Z', '+00:00'))
                            time_remaining = (end_dt - guess_dt).total_seconds()
                        except Exception:
                            time_remaining = 0.0

                    # Geocode guessed country
                    if my_team_only and is_opponent:
                        guessed_country = ''
                    else:
                        guessed_country = normalize_country_name(
                            geocoder.get_country(guess_lat, guess_lng) or 'Unknown'
                        )

                    # Correct country flag
                    if guessed_country and guessed_country not in ('', 'Unknown', 'Lost at Sea') and correct_country not in ('Unknown',):
                        correct_flag = (guessed_country == correct_country)
                    else:
                        correct_flag = ''

                else:
                    # ---- NO PIN: player did not guess ----
                    status = 'no_pin'
                    guess_lat = ''
                    guess_lng = ''
                    distance = ''
                    guess_score = 0
                    is_team_best = False
                    guess_created_ts = ''
                    time_to_guess = ''
                    time_remaining = 0.0
                    guessed_country = ''
                    correct_flag = ''

                raw_rows.append({
                    'team_key': team_key,
                    'game_id': game_id,
                    'game_date': game_date,
                    'round': round_num,
                    'total_rounds': total_rounds,
                    'competitive_mode': competitive_mode,
                    'move_mode': move_mode,
                    'player_id': player_id,
                    'player_name': player_name,
                    'time_seconds': round(time_to_guess, 2) if isinstance(time_to_guess, float) else '',
                    'distance_meters': round(distance, 2) if isinstance(distance, (int, float)) else '',
                    'distance_km': round(distance / 1000, 2) if isinstance(distance, (int, float)) else '',
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
                    # Initiative & timing columns
                    'guess_created': guess_created_ts,
                    'round_start_time': round_start or '',
                    'round_end_time': round_end or '',
                    'timer_start_time': timer_start or '',
                    'round_duration_sec': round_duration_sec,
                    'time_remaining_sec': round(time_remaining, 2) if isinstance(time_remaining, float) else '',
                    'clicked_first': False,  # computed below
                    'status': status,
                    '_team_id': tid,  # internal, stripped before output
                })

    # ---- Compute won_team, won_round, and clicked_first ----
    by_game_round = defaultdict(list)
    for row in raw_rows:
        by_game_round[(row['game_id'], row['round'])].append(row)

    for (gid, round_num), round_rows in by_game_round.items():
        if not round_rows:
            continue

        # won_round: best distance across ALL players who guessed
        guessed_rows = [r for r in round_rows if r['status'] == 'guessed' and r['distance_meters'] != '']
        if guessed_rows:
            best_overall = min(r['distance_meters'] for r in guessed_rows)
            for r in guessed_rows:
                if r['distance_meters'] <= best_overall:
                    r['won_round'] = True

        # won_team: best distance within each team (guessed only)
        by_team = defaultdict(list)
        for r in round_rows:
            by_team[r['_team_id']].append(r)

        for tid, team_rows in by_team.items():
            team_guessed = [r for r in team_rows if r['status'] == 'guessed' and r['distance_meters'] != '']
            if team_guessed:
                best_team = min(r['distance_meters'] for r in team_guessed)
                for r in team_guessed:
                    if r['distance_meters'] <= best_team:
                        r['won_team'] = True

            # clicked_first: within each team, who had highest time_remaining?
            team_with_time = [r for r in team_rows
                              if r['status'] == 'guessed' and r['time_remaining_sec'] != ''
                              and isinstance(r['time_remaining_sec'], (int, float))]
            if team_with_time:
                max_remaining = max(r['time_remaining_sec'] for r in team_with_time)
                for r in team_with_time:
                    if r['time_remaining_sec'] >= max_remaining:
                        r['clicked_first'] = True

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
# Multi-team detection & configuration
# ===================================================================

def make_team_key(player_ids) -> str:
    """Create a stable team key from a collection of player IDs."""
    return '_'.join(sorted(player_ids))


def discover_teams(game_details: Dict[str, Dict], user_id: str) -> Dict[str, Dict]:
    """Scan all games to find unique team compositions for the user.

    Returns {team_key: {"player_ids": set, "game_ids": list}}.
    """
    teams = {}
    for game_id, game in game_details.items():
        for team in game.get('teams', []):
            pids = {p.get('playerId') for p in team.get('players', [])}
            if user_id in pids:
                key = make_team_key(pids)
                if key not in teams:
                    teams[key] = {"player_ids": pids, "game_ids": []}
                teams[key]["game_ids"].append(game_id)
                break
    return teams


def load_teams_config(path: str) -> Optional[Dict]:
    """Load teams_config.json, return None if not found."""
    p = Path(path)
    if not p.exists():
        return None
    with open(p) as f:
        return json.load(f)


def save_teams_config(config: Dict, path: str):
    """Save teams configuration to JSON."""
    with open(path, 'w') as f:
        json.dump(config, f, indent=2)
    print(f"  \U0001f4be Teams config saved to {path}")


def reconcile_teams(discovered: Dict, saved_config: Optional[Dict]) -> Tuple[Optional[Dict], List[str], List[str]]:
    """Compare discovered teams against saved config.

    Returns (saved_config, new_team_keys, deleted_team_keys).
    """
    discovered_keys = set(discovered.keys())

    if saved_config is None:
        return None, sorted(discovered_keys), []

    saved_keys = {t["team_key"] for t in saved_config.get("teams", [])}
    new_keys = sorted(discovered_keys - saved_keys)
    deleted_keys = sorted(saved_keys - discovered_keys)

    return saved_config, new_keys, deleted_keys


def show_teams_menu(discovered_teams: Dict, player_names: Dict[str, str],
                    existing_config: Optional[Dict],
                    default_my_team_only: bool = False,
                    default_reverse_geocode: bool = True) -> Dict:
    """Interactive menu for configuring team settings using questionary."""
    if not HAS_QUESTIONARY:
        print("\u274c 'questionary' package required for team menu.")
        print("   Install with: pip install questionary")
        print("   Or use --no-teams-menu for non-interactive mode.")
        sys.exit(1)

    print(f"\n{'=' * 50}")
    print("\U0001f3af TEAM CONFIGURATION")
    print(f"{'=' * 50}")

    # Build team info list
    teams_list = []
    for team_key, team_data in sorted(discovered_teams.items()):
        pids = sorted(team_data["player_ids"])
        names = [player_names.get(pid, pid[:8] + '...') for pid in pids]
        label = " + ".join(names)
        game_count = len(team_data["game_ids"])

        # Check for existing saved settings
        existing_entry = None
        if existing_config:
            for t in existing_config.get("teams", []):
                if t["team_key"] == team_key:
                    existing_entry = t
                    break

        teams_list.append({
            "team_key": team_key,
            "player_ids": pids,
            "names": names,
            "label": label,
            "game_count": game_count,
            "existing": existing_entry,
        })

    # Display teams
    print(f"\nFound {len(teams_list)} team composition(s):\n")
    for i, t in enumerate(teams_list, 1):
        status = ""
        if t["existing"]:
            status = " [configured]"
        elif existing_config:
            status = " [NEW]"
        print(f"  {i}. {t['label']} ({t['game_count']} games){status}")

    # Step 1: Select which teams to enable
    choices = []
    for t in teams_list:
        checked = t["existing"]["enabled"] if t["existing"] else True
        choices.append(questionary.Choice(
            title=f"{t['label']} ({t['game_count']} games)",
            value=t["team_key"],
            checked=checked,
        ))

    enabled_keys = questionary.checkbox(
        "\nSelect teams to enable (space to toggle, enter to confirm):",
        choices=choices,
    ).ask()

    if enabled_keys is None:
        print("\nCancelled.")
        sys.exit(0)

    # Step 2: Per-team settings for enabled teams
    result_teams = []
    for t in teams_list:
        enabled = t["team_key"] in enabled_keys

        if enabled and not t["existing"]:
            # New team — ask for settings
            print(f"\n  Configuring: {t['label']}")

            my_team_only = questionary.confirm(
                f"    Only include your team's guesses (my_team_only)?",
                default=default_my_team_only,
            ).ask()
            if my_team_only is None:
                sys.exit(0)

            reverse_geocode = questionary.confirm(
                f"    Enable reverse geocoding?",
                default=default_reverse_geocode,
            ).ask()
            if reverse_geocode is None:
                sys.exit(0)

        elif t["existing"]:
            # Keep existing settings
            my_team_only = t["existing"].get("my_team_only", default_my_team_only)
            reverse_geocode = t["existing"].get("reverse_geocode", default_reverse_geocode)
        else:
            # Disabled team — use defaults
            my_team_only = default_my_team_only
            reverse_geocode = default_reverse_geocode

        result_teams.append({
            "team_key": t["team_key"],
            "player_ids": t["player_ids"],
            "player_names": {pid: player_names.get(pid, pid) for pid in t["player_ids"]},
            "enabled": enabled,
            "my_team_only": my_team_only,
            "reverse_geocode": reverse_geocode,
        })

    return {"teams": result_teams, "version": 1}


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
                        help='(Deprecated) Use teams_config.json per-team settings instead. '
                             'Sets default my_team_only for first-run team config.')
    parser.add_argument('--no-teams-menu', action='store_true',
                        help='Non-interactive mode. Errors if new teams are detected '
                             'that require configuration.')
    parser.add_argument('--teams-config', type=str, default='teams_config.json',
                        help='Path to teams configuration file (default: teams_config.json)')
    parser.add_argument('--outdir', type=str, default='out',
                        help='Base output directory (default: out)')
    parser.add_argument('--export-id', type=str, default=None,
                        help='Export ID (default: auto-generated timestamp)')
    parser.add_argument('--export-dir', type=str, default=None,
                        help='Override export directory path (ignores --outdir/--export-id)')
    parser.add_argument('--no-latest-json', action='store_true',
                        help='Do not write latest.json pointer file')
    parser.add_argument('--list-exports', action='store_true',
                        help='List existing exports and exit')
    parser.add_argument('--to-postgres', type=str, default=None,
                        help='Push data to PostgreSQL (DSN, e.g. postgresql://user:pass@host:5432/db)')
    parser.add_argument('--pg-schema', type=str, default='geoguessr',
                        help='PostgreSQL schema name (default: geoguessr)')
    parser.add_argument('--pg-if-exists', type=str, default='replace',
                        choices=['replace', 'append', 'skip'],
                        help='Postgres table conflict strategy (default: replace)')
    parser.add_argument('--pg-batch-size', type=int, default=5000,
                        help='Postgres insert batch size (default: 5000)')
    parser.add_argument('--max-pages', type=int, default=50,
                        help='Maximum activity feed pages to fetch (default: 50)')
    args = parser.parse_args()

    # Handle --list-exports
    if args.list_exports:
        exports_dir = Path(args.outdir) / 'exports'
        if not exports_dir.exists():
            print(f"No exports found in {exports_dir}")
            return
        exports = sorted(exports_dir.iterdir(), reverse=True)
        if not exports:
            print(f"No exports found in {exports_dir}")
            return
        print(f"📦 Exports in {exports_dir} (newest first):")
        for d in exports:
            if d.is_dir():
                latest_json = Path(args.outdir) / 'latest.json'
                is_latest = ''
                if latest_json.exists():
                    try:
                        with open(latest_json) as f:
                            lj = json.load(f)
                        if lj.get('export_id') == d.name:
                            is_latest = ' ← latest'
                    except Exception:
                        pass
                print(f"  {d.name}{is_latest}")
        return

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

    game_ids = api.get_team_duel_game_ids(limit=args.limit, max_pages=args.max_pages)

    if not game_ids:
        print("\u274c No team duel games found in activity feed!")
        print("   Make sure you've played some competitive team duels.")
        return

    print(f"\u2705 Found {len(game_ids)} team duel game(s) in feed")

    # Filter to only new games
    new_game_ids = [gid for gid in game_ids if gid not in existing_game_ids]
    if existing_game_ids:
        print(f"   {len(new_game_ids)} new game(s) to process ({len(existing_game_ids)} already in CSV)")

    # ================================================================
    # Phase 1: Load ALL game details (cached + new) for team discovery
    # ================================================================
    print(f"\n\u2699\ufe0f  Loading game data for team discovery...")
    all_game_details = {}

    # Load cached game data for existing game IDs (needed for team discovery)
    if existing_game_ids:
        loaded_cached = 0
        for gid in existing_game_ids:
            cache_file = RAW_GAMES_DIR / f"{gid}.json"
            if cache_file.exists():
                with open(cache_file) as f:
                    all_game_details[gid] = json.load(f)
                    loaded_cached += 1
        if loaded_cached:
            print(f"   Loaded {loaded_cached} cached games")

    # Fetch new games from API (or cache)
    for idx, game_id in enumerate(new_game_ids, 1):
        cached = (RAW_GAMES_DIR / f"{game_id}.json").exists()
        src = "cached" if cached else "API"
        print(f"  [{idx}/{len(new_game_ids)}] Loading game {game_id} ({src})...")
        try:
            game = api.get_game_details(game_id)
            all_game_details[game_id] = game
        except Exception as e:
            print(f"  \u26a0\ufe0f  Error loading game: {e}")
            continue

    # Collect all player IDs from all games
    for game in all_game_details.values():
        for team in game.get('teams', []):
            for player in team.get('players', []):
                pid = player.get('playerId')
                if pid and pid not in all_team_members:
                    all_team_members[pid] = pid

    # ================================================================
    # Phase 2: Fetch player nicknames (with disk cache)
    # ================================================================
    nickname_cache = load_nickname_cache()

    # Apply cached nicknames first
    cached_count = 0
    for pid in list(all_team_members.keys()):
        if pid in nickname_cache:
            all_team_members[pid] = nickname_cache[pid]
            cached_count += 1

    # Only fetch nicknames we don't have cached
    players_without_names = [pid for pid, name in all_team_members.items()
                             if pid == name and pid not in nickname_cache]
    if players_without_names:
        print(f"\n\U0001f465 Fetching nicknames for {len(players_without_names)} new players"
              f" ({cached_count} cached)...")
        new_fetched = 0
        for player_id in players_without_names:
            try:
                pprofile = api.get_player_profile(player_id)
                nickname = pprofile.get('nick', player_id)
                all_team_members[player_id] = nickname
                nickname_cache[player_id] = nickname
                new_fetched += 1
                time.sleep(0.2)
            except Exception:
                pass
        # Save updated cache
        save_nickname_cache(nickname_cache)
        print(f"   Cached {new_fetched} new nicknames (total: {len(nickname_cache)})")
    elif cached_count > 0:
        print(f"\n\U0001f465 Loaded {cached_count} player nicknames from cache")

    # ================================================================
    # Phase 3: Discover teams
    # ================================================================
    discovered_teams = discover_teams(all_game_details, user_id)
    print(f"\n\U0001f50d Discovered {len(discovered_teams)} unique team composition(s)")
    for tk, td in sorted(discovered_teams.items()):
        names = [all_team_members.get(pid, pid[:8] + '...') for pid in sorted(td["player_ids"])]
        print(f"   {' + '.join(names)} ({len(td['game_ids'])} games)")

    # ================================================================
    # Phase 4: Resolve team config
    # ================================================================
    teams_config_path = args.teams_config
    saved_config = load_teams_config(teams_config_path)
    saved_config, new_team_keys, deleted_team_keys = reconcile_teams(
        discovered_teams, saved_config
    )

    if len(discovered_teams) == 1 and saved_config is None:
        # Single team shortcut — auto-generate config without menu
        team_key = list(discovered_teams.keys())[0]
        team_data = discovered_teams[team_key]
        pids = sorted(team_data["player_ids"])
        names = [all_team_members.get(pid, pid) for pid in pids]
        print(f"\n\u2705 Single team detected: {' + '.join(names)}")

        teams_config = {
            "teams": [{
                "team_key": team_key,
                "player_ids": pids,
                "player_names": {pid: all_team_members.get(pid, pid) for pid in pids},
                "enabled": True,
                "my_team_only": args.my_team_only,
                "reverse_geocode": not args.no_geocode,
            }],
            "version": 1,
        }
        save_teams_config(teams_config, teams_config_path)

    elif new_team_keys or deleted_team_keys or saved_config is None:
        # Changes detected or first run with multiple teams — need config update

        if deleted_team_keys:
            print(f"\n\u26a0\ufe0f  {len(deleted_team_keys)} team(s) no longer found in games:")
            for key in deleted_team_keys:
                print(f"   REMOVED: {key}")
            # Auto-remove deleted teams from saved config
            if saved_config:
                saved_config["teams"] = [
                    t for t in saved_config["teams"]
                    if t["team_key"] not in deleted_team_keys
                ]

        if new_team_keys:
            print(f"\n\u2728 {len(new_team_keys)} new team(s) discovered:")
            for key in new_team_keys:
                pids = sorted(discovered_teams[key]["player_ids"])
                names = [all_team_members.get(pid, pid) for pid in pids]
                print(f"   NEW: {' + '.join(names)}")

        if args.no_teams_menu:
            if new_team_keys:
                print(f"\n\u274c Cannot proceed in --no-teams-menu mode with new teams.")
                print(f"   Run without --no-teams-menu to configure, or edit {teams_config_path} manually.")
                return
            # Only deletions — save updated config and continue
            if saved_config and deleted_team_keys:
                save_teams_config(saved_config, teams_config_path)
            teams_config = saved_config
        else:
            # Show interactive menu
            teams_config = show_teams_menu(
                discovered_teams,
                all_team_members,
                saved_config,
                default_my_team_only=args.my_team_only,
                default_reverse_geocode=not args.no_geocode,
            )
            save_teams_config(teams_config, teams_config_path)
    else:
        # No changes — use saved config
        teams_config = saved_config
        print(f"\n\u2705 Using team config from {teams_config_path}")

    # ================================================================
    # Phase 5: Per-team processing
    # ================================================================

    # No new games early exit (after team config is resolved)
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

    # Build team_key -> set of game_ids mapping
    team_game_ids = {}
    for game_id, game in all_game_details.items():
        for team in game.get('teams', []):
            pids = {p.get('playerId') for p in team.get('players', [])}
            if user_id in pids:
                key = make_team_key(pids)
                if key not in team_game_ids:
                    team_game_ids[key] = set()
                team_game_ids[key].add(game_id)
                break

    enabled_teams = [t for t in teams_config["teams"] if t["enabled"]]

    if not enabled_teams:
        print("\n\u26a0\ufe0f  No teams enabled! Enable at least one team in teams_config.json.")
        return

    print(f"\n\u2699\ufe0f  Processing {len(enabled_teams)} enabled team(s)...")

    new_rows = []

    for team_cfg in enabled_teams:
        team_key = team_cfg["team_key"]
        team_pids = set(team_cfg["player_ids"])
        my_team_only = team_cfg.get("my_team_only", False)
        reverse_geocode = team_cfg.get("reverse_geocode", True)

        team_gids = team_game_ids.get(team_key, set())
        new_team_gids = team_gids & set(new_game_ids)

        if not new_team_gids:
            continue

        names = [all_team_members.get(pid, pid) for pid in sorted(team_pids)]
        print(f"\n  \U0001f465 Team: {' + '.join(names)} ({len(new_team_gids)} new games)")
        print(f"     my_team_only={my_team_only}, reverse_geocode={reverse_geocode}")

        # Create team-specific geocoder if reverse_geocode differs from global
        if reverse_geocode and args.no_geocode:
            # Team wants geocoding but global flag disabled it — create a new geocoder
            try:
                team_geocoder = ReverseGeocoder(
                    enable_geocoding=True,
                    delay=args.geocode_delay,
                    provider_name=args.geo_provider,
                    config=config
                )
            except ValueError as e:
                print(f"  \u26a0\ufe0f  Cannot enable geocoding for this team: {e}")
                team_geocoder = geocoder
        elif not reverse_geocode:
            # Team doesn't want geocoding — use disabled geocoder
            team_geocoder = ReverseGeocoder(enable_geocoding=False)
        else:
            team_geocoder = geocoder

        for game_id in sorted(new_team_gids):
            game = all_game_details.get(game_id)
            if not game:
                continue

            cached = True  # Already loaded into all_game_details
            print(f"\n    [{game_id}]")

            try:
                rows = process_game_data(
                    game, all_team_members, team_geocoder, user_id,
                    my_team_only=my_team_only,
                    team_key=team_key,
                )
                new_rows.extend(rows)

                n_rounds = game.get('currentRoundNumber', len(game.get('rounds', [])))
                print(f"    \u2705 Extracted {len(rows)} guesses from {n_rounds} rounds")

            except Exception as e:
                print(f"    \u26a0\ufe0f  Error processing game: {e}")
                continue

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

    # ================================================================
    # Export to CSV (+ structured export directory)
    # ================================================================

    # Resolve export directory
    export_id = args.export_id or datetime.now().strftime('%Y-%m-%d_%H%M%S')
    if args.export_dir:
        export_dir = Path(args.export_dir)
    else:
        export_dir = Path(args.outdir) / 'exports' / export_id
    export_dir.mkdir(parents=True, exist_ok=True)

    # Write CSV to export dir
    export_csv = export_dir / 'team_duels.csv'
    # Also write to --csv path if specified (backward compat)
    output_file = args.csv or str(export_csv)

    print(f"\n\U0001f4be Exporting to {output_file}...")

    if all_rows:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"\u2705 Exported {len(all_rows)} rows to {output_file}")
        if existing_rows:
            print(f"   ({len(existing_rows)} existing + {len(new_rows)} new)")

        # Also write to export dir if --csv pointed elsewhere
        if str(Path(output_file).resolve()) != str(export_csv.resolve()):
            with open(export_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
                writer.writeheader()
                writer.writerows(all_rows)
            print(f"   Also exported to {export_csv}")
    else:
        print("\u26a0\ufe0f  No data to export!")

    # Write latest.json pointer
    if not args.no_latest_json and all_rows:
        latest_path = Path(args.outdir) / 'latest.json'
        latest_path.parent.mkdir(parents=True, exist_ok=True)
        latest_data = {
            "latest_export_dir": str(export_dir),
            "export_id": export_id,
            "created_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "csv_file": str(export_csv),
            "total_rows": len(all_rows),
            "total_games": len(set(r['game_id'] for r in all_rows)),
        }
        with open(latest_path, 'w') as f:
            json.dump(latest_data, f, indent=2)
        print(f"   Updated {latest_path}")

    # Push to Postgres if requested
    if args.to_postgres and all_rows:
        print(f"\n🐘 Pushing to PostgreSQL...")
        try:
            from pg_push import push_to_postgres
            push_to_postgres(
                rows=all_rows,
                csv_columns=CSV_COLUMNS,
                dsn=args.to_postgres,
                schema=args.pg_schema,
                if_exists=args.pg_if_exists,
                batch_size=args.pg_batch_size,
            )
        except ImportError:
            print("  ❌ pg_push module not found. Install psycopg2: pip install psycopg2-binary")
        except Exception as e:
            print(f"  ❌ Postgres push failed: {e}")

    if geocoder.api_calls > 0:
        print(f"\n\U0001f4ca Geocoding stats: {geocoder.api_calls} API calls, {len(geocoder.cache)} cached results")

    print(f"\n\U0001f389 Done!")


if __name__ == '__main__':
    main()
