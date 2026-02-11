# GeoGuessr Team Duel Stats

Fetch and analyze your competitive team duel games from GeoGuessr. Exports detailed per-round stats to CSV and provides in-depth performance analysis.

## Features

- **Fetch game data** from GeoGuessr's API with cursor-based pagination
- **Reverse geocode** guess locations using Google Maps, OpenCage, or Nominatim (with automatic fallback)
- **Country detection** for correct locations using `panorama.countryCode` (no geocoding needed)
- **Local caching** of raw API responses in `raw_data/` so re-runs never re-fetch game data
- **Incremental mode** with `--csv` flag to only fetch new games and append to an existing CSV
- **30 CSV columns** including win/loss, health, damage, scores, team/round winners, and more
- **Comprehensive analysis** with per-player, per-country, per-region breakdowns
- **Trend export** to JSON for feeding into LLMs for deeper trend analysis

## Prerequisites

- **Python 3.8+**
- **GeoGuessr Pro account** (team duels is a Pro feature; the API requires an active session)
- **Google Maps API key** (recommended for reverse geocoding guess locations)

## Quick Start

```bash
# 1. Clone and set up
git clone https://github.com/andrewdmcleod/geoguessr-comp-team-duel-stats.git
cd geoguessr-comp-team-duel-stats
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure (see Setup section below)
cp config.json.example config.json
cp cookie.txt.example cookie.txt
# Edit config.json and cookie.txt with your details

# 3. Verify your API keys work
python test_geocoding.py

# 4. Fetch your games
python geoguessr_stats.py --csv team_duels.csv --my-team-only

# 5. Analyze your stats
python analyze_stats.py team_duels.csv
```

## Setup

### 1. Install dependencies

```bash
git clone https://github.com/andrewdmcleod/geoguessr-comp-team-duel-stats.git
cd geoguessr-comp-team-duel-stats
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure

Copy the example files and fill in your details:

```bash
cp config.json.example config.json
cp cookie.txt.example cookie.txt
```

**`config.json`** — set your player ID and (optionally) geocoding API keys:

```json
{
  "player_id": "YOUR_GEOGUESSR_USER_ID",
  "cookie_file": "cookie.txt",
  "opencage_api_key": "",
  "google_maps_api_key": ""
}
```

- **`player_id`**: Your GeoGuessr user ID. Go to your profile on [geoguessr.com](https://www.geoguessr.com), click on your avatar/name, and look at the URL — it will be something like `geoguessr.com/user/abc123def456...`. Copy that hex string. This is used as a fallback if the API can't determine your identity from the cookie; in most cases the cookie is sufficient.
- **`google_maps_api_key`** (recommended): Required for reverse geocoding your guess locations. See [Google Maps API setup](#google-maps-api-setup) below.
- **`opencage_api_key`** (optional fallback): Get one from [OpenCage](https://opencagedata.com/). Free tier allows 2,500 requests/day.

**`cookie.txt`** — your GeoGuessr session cookie:

1. Log in to [geoguessr.com](https://www.geoguessr.com) in your browser
2. Open Developer Tools (F12) → Application → Cookies → `https://www.geoguessr.com`
3. Find the cookie named `_ncfa`
4. Copy its value and paste it into `cookie.txt`

> **Note:** The cookie expires periodically. If you get authentication errors, grab a fresh cookie.

### 3. Verify your setup

Run the geocoding test to confirm your API keys are working:

```bash
python test_geocoding.py
```

This tests all configured providers against a known location (Eiffel Tower) and reports which ones are active.

### Google Maps API setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Go to **APIs & Services** → **Library**
4. Search for **"Geocoding API"** and click **Enable**
5. Go to **APIs & Services** → **Credentials** → **Create Credentials** → **API Key**
6. Copy the key into `config.json` as `google_maps_api_key`

> Google gives you $200/month of free credit, which covers ~40,000 geocoding requests. You won't be charged unless you exceed this.

## Usage

### Fetching stats

Remember to activate the virtual environment first: `source .venv/bin/activate`

```bash
# Fetch all team duel games (with geocoding)
python geoguessr_stats.py

# Fetch only your team's guesses (recommended — saves geocoding API calls)
python geoguessr_stats.py --my-team-only

# Use a persistent CSV file (incremental — only fetches new games)
python geoguessr_stats.py --csv team_duels.csv --my-team-only

# Limit to N games (useful for testing)
python geoguessr_stats.py --limit 5

# Skip geocoding (faster — correct_country still works via panorama data,
# but guessed_country will be empty)
python geoguessr_stats.py --no-geocode

# Choose geocoding provider (default: google)
python geoguessr_stats.py --geo-provider opencage

# Custom geocoding delay (seconds between requests)
python geoguessr_stats.py --geocode-delay 2.0
```

### Analyzing stats

```bash
# Full analysis of a CSV file
python analyze_stats.py team_duels.csv

# Filter analysis to a specific player
python analyze_stats.py team_duels.csv --player YOUR_PLAYER_ID

# Export analysis tables as CSV files
python analyze_stats.py team_duels.csv --export analysis_output/

# Export chronological trend data as JSON (for LLM analysis)
python analyze_stats.py team_duels.csv --trend-export trends.json
```

### Typical workflow

```bash
# First run — fetches all games
python geoguessr_stats.py --csv team_duels.csv --my-team-only

# After playing more games — only fetches new ones
python geoguessr_stats.py --csv team_duels.csv --my-team-only

# Analyze whenever you want
python analyze_stats.py team_duels.csv
```

### Analysis sections

The analysis script outputs:

| Section | Description |
|---------|-------------|
| **Player Summary** | Games, rounds, avg distance/time, country accuracy per player |
| **Accuracy Ranking** | Players ranked by average distance |
| **Speed Ranking** | Players ranked by average time (timed rounds only) |
| **Speed vs Accuracy** | Scatter of avg time vs avg distance per player |
| **Team Stats Summary** | Avg/worst distance and avg time, split by win vs loss |
| **Player Win/Loss Split** | Avg distance in wins vs losses per player |
| **Won Team** | % of rounds each player had the best guess on their team |
| **Won Round** | % of rounds each player had the best guess overall |
| **Region Performance** | Average distance per player per continent |
| **Best/Worst Countries** | Per player, countries with best/worst average distance (min 2 guesses) |
| **Countries I Confuse** | "When it was X, I guessed Y" patterns |
| **Countries Worth Studying** | Worst-performing large countries (geographically significant) |
| **Move vs No-Move** | Distance, time, and accuracy comparison across game modes |
| **Rounds Played Trend** | Avg rounds per game, avg rounds in wins vs losses |

## CSV Columns

The exported CSV contains 30 columns:

| Column | Description |
|--------|-------------|
| `game_id` | Unique game identifier |
| `game_date` | ISO timestamp of the game |
| `round` | Round number within the game |
| `total_rounds` | Total rounds in the game |
| `competitive_mode` | Game mode (e.g. "TeamDuels") |
| `move_mode` | Movement mode (move, no-move, NMPZ) |
| `player_id` | Player's GeoGuessr ID |
| `player_name` | Player's display name |
| `time_seconds` | Time taken for the guess |
| `distance_meters` | Distance from correct location (meters) |
| `distance_km` | Distance from correct location (km) |
| `score` | Score points for this guess |
| `correct_lat` / `correct_lng` | Correct location coordinates |
| `guess_lat` / `guess_lng` | Player's guess coordinates |
| `correct_country_code` | ISO 3166-1 alpha-2 code of correct country |
| `correct_country` | Full name of the correct country |
| `guessed_country` | Full name of the guessed country (requires geocoding) |
| `correct_country_flag` | Whether the player guessed the correct country |
| `region` | Continent/region of the correct location |
| `is_team_best_guess` | Whether this was the team's best guess for the round |
| `won_team` | Whether this player beat their teammate on this round |
| `won_round` | Whether this player had the best guess across all players |
| `game_won` | Whether the player's team won the game |
| `health_before` / `health_after` | Team health before/after the round |
| `damage_dealt` | Damage dealt by the team this round |
| `multiplier` | Round damage multiplier |

## How it works

- **Your team is detected automatically.** The script logs in with your cookie, fetches your profile, and uses your user ID to identify which team you're on in each game. Your teammates are discovered from the game data — you don't need to configure them.
- **Correct country** comes from `panorama.countryCode` in the game data (no API call needed).
- **Guessed country** requires reverse geocoding your guess coordinates via Google Maps (or another provider). This is the only part that uses geocoding API calls. Use `--no-geocode` to skip this.
- **`--my-team-only`** skips geocoding opponent guesses (saving API calls) but still reads their distances to compute `won_round`.

## Caching

Game details are cached locally in `raw_data/games/` as JSON files. This means:

- Re-running the script never re-fetches game data from the API
- If you change processing logic, you can re-process from cached data without API calls
- The activity feed is always re-fetched (it's small and changes as you play new games)

## Geocoding Providers

| Provider | Default Delay | Free Tier | Notes |
|----------|--------------|-----------|-------|
| Google Maps | 0.05s | ~40,000/month ($200 credit) | Recommended. Most accurate. |
| OpenCage | 1.0s | 2,500/day | Good fallback option. |
| Nominatim | 1.5s | 1 req/sec | Free but strict rate limits. May block your IP. |

The script uses Google as the primary provider and automatically falls back to OpenCage, then Nominatim if the primary fails.

## Known Limitations

- The GeoGuessr API only provides each player's **final guess position**. There is no way to distinguish between clicking "Guess" vs timer expiry, or first pin drop vs final pin position.
- Geocoding accuracy depends on the provider. Ocean/water guesses may show as "Lost at Sea".
- The session cookie expires periodically and needs to be refreshed manually.
- Only **team duel** games are fetched. Other game modes (battle royale, classic, etc.) are not supported.

## License

MIT
