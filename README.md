# GeoGuessr Team Duel Stats

Fetch and analyze your competitive team duel games from GeoGuessr. Exports detailed per-round stats to CSV and provides in-depth performance analysis.

## Features

- **Fetch game data** from GeoGuessr's API with cursor-based pagination
- **Reverse geocode** guess locations using Google Maps, OpenCage, or Nominatim (with automatic fallback)
- **Country detection** for correct locations using `panorama.countryCode` (no geocoding needed)
- **Local caching** of raw API responses in `raw_data/` so re-runs never re-fetch game data
- **Incremental mode** with `--csv` flag to only fetch new games and append to an existing CSV
- **Multi-team detection** — automatically discovers all teams you've played on, with interactive menu or saved config
- **38 CSV columns** including team_key, win/loss, health, damage, scores, initiative/timing, and more
- **Nickname caching** to `raw_data/nicknames.json` — avoids re-fetching player names every run
- **Country name normalization** — handles Czechia/Czech Republic, Türkiye/Turkey, and other API mismatches
- **Initiative/timing metrics** — who clicks first, no-pin tracking, guess speed analysis, hesitation index
- **Game drilldown** — per-game round-by-round analysis with `game_detail.py`
- **Comprehensive analysis** with per-player, per-country, per-region breakdowns and team aggregate rows
- **Trend export** to JSON for feeding into LLMs for deeper trend analysis
- **Grafana dashboard** via Docker Compose with auto-provisioned PostgreSQL, configurable via `.env`

## Prerequisites

- **Python 3.8+**
- **GeoGuessr Pro account** (team duels is a Pro feature; the API requires an active session)
- **Google Maps API key** (recommended for reverse geocoding guess locations)
- **Docker with compose plugin** (required for the Grafana dashboard)

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

# 4. Fetch your games (interactive team menu on first run)
python geoguessr_stats.py --csv team_duels.csv

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
# Fetch all team duel games (interactive team menu on first run)
python geoguessr_stats.py --csv team_duels.csv

# Skip the interactive team menu (use saved teams_config.json)
python geoguessr_stats.py --csv team_duels.csv --no-teams-menu

# Use a specific teams config file
python geoguessr_stats.py --csv team_duels.csv --teams-config my_teams.json

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

> **Multi-team support:** On first run, the script discovers all teams you've played on and shows an interactive menu (via `questionary`) where you can select which teams to process and configure per-team settings (my-team-only, reverse geocoding). Your choices are saved to `teams_config.json` so subsequent runs skip the menu. If only one team is found, it's auto-selected.

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

### Game drilldown

Drill into individual games with `game_detail.py`:

```bash
# List recent games
python game_detail.py team_duels.csv --list

# Show the most recent game
python game_detail.py team_duels.csv --last

# Show a specific game
python game_detail.py team_duels.csv GAME_ID

# Export as JSON (for programmatic use)
python game_detail.py team_duels.csv --last --json

# Export as CSV
python game_detail.py team_duels.csv GAME_ID --csv output_dir/
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
| **Player Summary — Distance** | Avg/median/best/worst distance, std dev, country accuracy per player |
| **Player Summary — Timing** | Avg/median time (active clicks only), no-pin count per player |
| **Accuracy Ranking** | Players ranked by average distance |
| **Speed Ranking** | Players ranked by average time (excludes auto-submissions and no-pin) |
| **Speed vs Accuracy** | Combined rank score (time rank + distance rank) per player |
| **Recent vs All-Time** | Last 10 games vs all-time stats with colored ● trend indicators |
| **Team Stats Summary** | Avg/worst distance and avg time, split by win vs loss and move mode |
| **Player Win/Loss Split** | Avg distance in wins vs losses, with correct/incorrect country breakdown |
| **Won Team** | % of rounds each player beat their teammate (+ team aggregate row) |
| **Won Round** | % of rounds each player had the best guess overall (+ by move mode) |
| **Region Performance** | Team: distance as % of region span. Players: avg km per continent |
| **Move vs No-Move** | Distance, time, and accuracy across game modes (team rows first) |
| **Countries I Confuse** | "When it was X, I guessed Y" — top 10 per player (team first) |
| **Closest/Furthest Countries** | Best/worst avg distance per country (min 3 guesses, team first) |
| **Best/Worst In-Country** | Closest/furthest when correct country was guessed (team first) |
| **Countries Worth Studying** | Importance score 0–100: (avg_dist / area) × log(1 + frequency) |
| **Competitive Advantage** | Countries you dominate vs opponents dominate, sorted by opponent distance |
| **Rounds Played Trend** | Avg rounds per game, avg rounds in wins vs losses |
| **Initiative Summary** | Who clicks first (derived from timing), participation rates, no-pin counts |
| **No-Pin Analysis** | No-pin frequency and round loss % when no pin dropped |
| **Guess Speed by Region** | Avg time remaining when guess submitted, per region per player |
| **Fastest/Slowest Guesses** | Top N quickest and slowest individual guesses |
| **Hesitation Index** | Gap between first and last guess per round (team coordination metric) |
| **Pressure Response** | Avg distance after winning vs losing the previous round |

> **Backward compatibility:** The analysis engine works with both 30-column (pre-v0.3.0) and 38-column CSVs. For older CSVs, initiative metrics are derived from `time_seconds` spread, and no-pin rounds are detected from missing player rows. Timing-only sections (guess speed by region, hesitation index) require 38-column data.

> **Timing model:** In competitive team duels, one player clicks "Guess" which starts a 15-second countdown. Other players can click within that window or their pin is auto-submitted at round end. `time_remaining_sec` = round_end − guess_created (higher = clicked earlier, ≈0 = auto-submitted). Speed/timing stats exclude auto-submissions (time_remaining < 1s).

## CSV Columns

The exported CSV contains 38 columns:

| Column | Description |
|--------|-------------|
| `team_key` | Stable team identifier (sorted player IDs joined with `_`) |
| `game_id` | Unique game identifier |
| `game_date` | ISO timestamp of the game |
| `round` | Round number within the game |
| `total_rounds` | Total rounds in the game |
| `competitive_mode` | Game mode (e.g. "TeamDuels") |
| `move_mode` | Movement mode (move, no-move, NMPZ) |
| `player_id` | Player's GeoGuessr ID |
| `player_name` | Player's display name |
| `time_seconds` | Elapsed time from round start to guess submission (per-player) |
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
| `guess_created` | ISO timestamp when the guess was submitted |
| `round_start_time` | ISO timestamp of round start |
| `round_end_time` | ISO timestamp of round end |
| `timer_start_time` | ISO timestamp when the guess timer started |
| `round_duration_sec` | Round duration in seconds |
| `time_remaining_sec` | Seconds remaining when guess submitted (higher = clicked faster) |
| `clicked_first` | Whether this player clicked first within their team |
| `status` | `guessed` or `no_pin` (player did not drop a pin) |

## How it works

- **Your team is detected automatically.** The script logs in with your cookie, fetches your profile, and uses your user ID to identify which team you're on in each game. Your teammates are discovered from the game data — you don't need to configure them.
- **Correct country** comes from `panorama.countryCode` in the game data (no API call needed).
- **Guessed country** requires reverse geocoding your guess coordinates via Google Maps (or another provider). This is the only part that uses geocoding API calls. Use `--no-geocode` to skip this.
- **`--my-team-only`** (deprecated) skips geocoding opponent guesses. Now configured per-team in the interactive menu / `teams_config.json`.
- **Multi-team processing**: Each team gets its own CSV and geocoder instance. Teams are identified by a stable key (sorted player IDs).

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

## Grafana Dashboard

Visualize your stats in a Grafana dashboard backed by PostgreSQL. Uses Docker Compose to manage the Postgres and Grafana containers.

### Quick start

```bash
# 1. Make sure you have data (run the fetcher first if needed)
python geoguessr_stats.py --csv team_duels.csv

# 2. (Optional) Customize ports/credentials
cp .env.example .env
# Edit .env to change PG_PORT, GRAFANA_PORT, passwords, etc.

# 3. Launch the dashboard (starts Postgres + Grafana via docker compose)
python geoguessr_dashboard.py --config config.json

# 4. Open http://localhost:3000 (admin / geoguessr)
```

### Configuration

The dashboard is configured via `docker-compose.yml` and `.env`:

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Service definitions (images, healthchecks, volumes) |
| `.env` | Ports, credentials, image versions (copy from `.env.example`) |
| `grafana/provisioning/` | Datasource and dashboard auto-provisioning |
| `grafana/dashboards/` | Dashboard panel JSON definitions |

Default credentials (override in `.env`):

| Service | Port | Username | Password |
|---------|------|----------|----------|
| Grafana | 3000 | admin | geoguessr |
| PostgreSQL | 5432 | geoguessr | geoguessr |

### Dashboard commands

```bash
# Launch with latest export data
python geoguessr_dashboard.py --config config.json

# Refresh data before launching
python geoguessr_dashboard.py --config config.json --refresh

# Use a specific export
python geoguessr_dashboard.py --config config.json --export 2025-02-10_143000

# List available exports
python geoguessr_dashboard.py --list-exports

# Auto-refresh: scan for new games every 5 minutes while dashboard runs
python geoguessr_dashboard.py --config config.json --watch

# Auto-refresh with custom interval (10 minutes)
python geoguessr_dashboard.py --config config.json --watch --watch-interval 600

# Custom ports (edit .env, or override inline)
GRAFANA_PORT=3001 PG_PORT=5433 python geoguessr_dashboard.py --config config.json

# Stop the dashboard
python geoguessr_dashboard.py --stop

# Or use docker compose directly
docker compose up -d      # start services
docker compose down        # stop and remove containers
docker compose logs -f     # tail logs
docker compose ps          # check status
```

### Dashboard panels

| Panel | Description |
|-------|-------------|
| **Team Rolling Avg Distance** | 5-game rolling average distance over time |
| **Player Rolling Avg Distance** | Per-player rolling average (one series each) |
| **Guess Time vs Distance** | Scatter plot of speed vs accuracy (excludes timeouts) |
| **Win Rate Over Time** | 10-game rolling win rate |
| **Correct Country Rate** | 10-game rolling country accuracy per player |
| **Countries Worth Studying** | Worst countries weighted by frequency |
| **Countries I Confuse** | Confusion pairs: actual vs guessed country |
| **Best Countries** | Lowest avg distance per country per player |
| **Move vs No-Move** | Distance comparison by movement mode |
| **Speed Ranking** | Avg guess time + click rate % |
| **Summary Stats** | Total games, guesses, players, avg distance, accuracy |
| **Won Team %** | How often each player beat their teammate, by move mode |
| **Won Round %** | How often each player had the best guess overall, by move mode |
| **Worst Countries** | Highest avg distance per country per player |
| **Region Performance** | Average distance per player per continent |
| **Player Win/Loss Split** | Average distance in wins vs losses per player |
| **Competitive Advantage** | Countries where your team outperforms opponents |
| **Recent vs All-Time** | Last 10 games vs all-time per-player comparison |
| **Initiative Rate by Player** | How often each player clicks first + participation rate |
| **Initiative Rate Over Time** | 5-game rolling initiative rate per player |
| **Guess Speed by Region** | Avg time remaining when guess submitted, per region |
| **No-Pin Analysis** | No-pin frequency, loss rate when no pin dropped |
| **Hesitation Index** | Rolling avg gap between first and last guess (team coordination) |
| **Pressure Response** | Avg distance after winning vs losing previous round |

### Export directory structure

When using `--outdir` (default: `out/`), exports are organized as:

```
out/
  latest.json              # Pointer to the most recent export
  exports/
    2025-02-10_143000/     # Timestamped export directory
      team_duels.csv       # CSV data
    2025-02-11_091500/
      team_duels.csv
```

### Direct Postgres push

You can also push data directly to any PostgreSQL database:

```bash
python geoguessr_stats.py --csv team_duels.csv \
  --to-postgres postgresql://user:pass@localhost:5432/mydb \
  --pg-schema geoguessr
```

This creates three normalized tables: `games`, `rounds`, and `guesses` with proper foreign keys and indexes.

## Known Limitations

- The GeoGuessr API provides each player's **final guess position** and timing data. The `time_remaining_sec` field (round_end - guess_created) lets us distinguish active clicks (≥1s remaining) from auto-submissions (~0s remaining), but we can't distinguish first pin drop vs final pin position.
- Geocoding accuracy depends on the provider. Ocean/water guesses may show as "Lost at Sea".
- The session cookie expires periodically and needs to be refreshed manually.
- Only **team duel** games are fetched. Other game modes (battle royale, classic, etc.) are not supported.
- For older CSV data (30-column format without initiative columns), the analysis engine derives `clicked_first` from `time_seconds` spread and detects no-pin rounds from missing rows. Full timing metrics require re-exporting with v0.3.0.

## License

MIT
