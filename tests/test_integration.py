"""Integration tests for Phase 4: Postgres + Grafana via docker compose.

These tests require Docker with the compose plugin to be installed and running.
They use a test-specific docker-compose.test.yml with non-standard ports (15432/13000)
to avoid conflicts with any running dashboard instance.

Run with: pytest tests/test_integration.py -v
Skip if no Docker: automatically skipped via @requires_docker

All containers are cleaned up after tests via docker compose down.
"""

import csv as csv_mod
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pg_push import push_to_postgres, rows_to_tables
from tests.conftest import SAMPLE_ROWS, CSV_COLUMNS

# Test ports (non-standard to avoid conflicts with real dashboard)
TEST_PG_PORT = 15432
TEST_GRAFANA_PORT = 13000
TEST_PG_DSN = f'postgresql://geoguessr:geoguessr@localhost:{TEST_PG_PORT}/geoguessr'

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ===================================================================
# Docker availability check
# ===================================================================

def docker_compose_available() -> bool:
    try:
        result = subprocess.run(
            ['docker', 'compose', 'version'],
            capture_output=True, text=True, timeout=10
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_docker = pytest.mark.skipif(
    not docker_compose_available(),
    reason='docker compose not available'
)


# ===================================================================
# Test compose file generation
# ===================================================================

def create_test_compose_dir():
    """Create a temp directory with a test docker-compose.yml that uses
    non-standard ports so it doesn't collide with the real dashboard.

    Also writes a test-specific datasource.yml for Grafana pointing
    at the compose service name 'postgres'.
    """
    tmp = tempfile.mkdtemp(prefix='geoguessr_test_')

    # Write docker-compose.test.yml
    compose = f"""
services:
  postgres:
    image: postgres:16-alpine
    container_name: geoguessr-test-postgres
    environment:
      POSTGRES_USER: geoguessr
      POSTGRES_PASSWORD: geoguessr
      POSTGRES_DB: geoguessr
    ports:
      - "{TEST_PG_PORT}:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U geoguessr"]
      interval: 3s
      timeout: 3s
      retries: 15

  grafana:
    image: grafana/grafana:11.4.0
    container_name: geoguessr-test-grafana
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: testpass
      GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH: /var/lib/grafana/dashboards/overview.json
    ports:
      - "{TEST_GRAFANA_PORT}:3000"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning:ro
      - ./grafana/dashboards:/var/lib/grafana/dashboards:ro
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:3000/api/health || exit 1"]
      interval: 3s
      timeout: 3s
      retries: 15
"""
    with open(os.path.join(tmp, 'docker-compose.yml'), 'w') as f:
        f.write(compose)

    # Copy grafana dashboards from the real project
    shutil.copytree(
        os.path.join(PROJECT_DIR, 'grafana', 'dashboards'),
        os.path.join(tmp, 'grafana', 'dashboards'),
    )

    # Create test provisioning with datasource pointing at compose service 'postgres'
    prov_ds_dir = os.path.join(tmp, 'grafana', 'provisioning', 'datasources')
    prov_dash_dir = os.path.join(tmp, 'grafana', 'provisioning', 'dashboards')
    os.makedirs(prov_ds_dir)
    os.makedirs(prov_dash_dir)

    shutil.copy2(
        os.path.join(PROJECT_DIR, 'grafana', 'provisioning', 'dashboards', 'dashboards.yml'),
        prov_dash_dir,
    )

    ds_yaml = """apiVersion: 1
datasources:
  - name: GeoGuessr PostgreSQL
    type: postgres
    uid: geoguessr-postgres
    access: proxy
    url: postgres:5432
    user: geoguessr
    jsonData:
      database: geoguessr
      sslmode: disable
      postgresVersion: 1600
    secureJsonData:
      password: geoguessr
    isDefault: true
    editable: true
"""
    with open(os.path.join(prov_ds_dir, 'datasource.yml'), 'w') as f:
        f.write(ds_yaml)

    return tmp


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture(scope='module')
def compose_env():
    """Start the test docker compose stack and yield (compose_dir, dsn).

    Cleans up everything after all tests in this module.
    """
    if not docker_compose_available():
        pytest.skip('docker compose not available')

    compose_dir = create_test_compose_dir()

    try:
        # docker compose up (wait for healthchecks)
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d', '--wait', '--wait-timeout', '60'],
            capture_output=True, text=True, cwd=compose_dir, timeout=90
        )
        if result.returncode != 0:
            print(f"compose up stderr: {result.stderr}")
            pytest.fail(f'docker compose up failed: {result.stderr}')

        yield compose_dir, TEST_PG_DSN

    finally:
        # Always tear down
        subprocess.run(
            ['docker', 'compose', 'down', '--remove-orphans', '-v'],
            capture_output=True, cwd=compose_dir, timeout=30
        )
        shutil.rmtree(compose_dir, ignore_errors=True)


@pytest.fixture(scope='module')
def loaded_pg(compose_env):
    """Load test data into Postgres and return the DSN."""
    compose_dir, dsn = compose_env

    push_to_postgres(
        rows=[dict(r) for r in SAMPLE_ROWS],
        csv_columns=CSV_COLUMNS,
        dsn=dsn,
        schema='geoguessr',
        if_exists='replace',
    )
    return dsn


@pytest.fixture(scope='module')
def grafana_url(compose_env):
    """Return the Grafana base URL (Grafana is already healthy from compose --wait)."""
    return f'http://localhost:{TEST_GRAFANA_PORT}'


# ===================================================================
# Postgres tests
# ===================================================================

@requires_docker
class TestPostgresContainer:
    def test_postgres_is_running(self, compose_env):
        """Postgres container should be healthy via compose healthcheck."""
        compose_dir, _ = compose_env
        result = subprocess.run(
            ['docker', 'compose', 'ps', '--status', 'running', '--format', 'json'],
            capture_output=True, text=True, cwd=compose_dir
        )
        assert 'postgres' in result.stdout

    def test_postgres_accepts_query(self, compose_env):
        """Should be able to run a simple SQL query."""
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-c', 'SELECT 1 AS test;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'test' in result.stdout


@requires_docker
class TestPostgresDataLoad:
    def test_data_loaded(self, loaded_pg):
        """push_to_postgres should complete without error."""
        assert loaded_pg == TEST_PG_DSN

    def test_games_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.games;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 2

    def test_rounds_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.rounds;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 3

    def test_guesses_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.guesses;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 6

    def test_game_data_correct(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT game_won, competitive_mode FROM geoguessr.games "
             "WHERE game_id = 'game001';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 't' in result.stdout
        assert 'TeamDuels' in result.stdout

    def test_round_country_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT pano_country_code, pano_country_name FROM geoguessr.rounds "
             "WHERE game_id = 'game001' AND round_number = 1;"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'FR' in result.stdout
        assert 'France' in result.stdout

    def test_guess_player_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT player_name, distance_km, score FROM geoguessr.guesses "
             "WHERE game_id = 'game001' AND round_number = 1 "
             "AND player_id = 'player_a';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'Alice' in result.stdout
        assert '150' in result.stdout
        assert '3500' in result.stdout

    def test_indexes_created(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT indexname FROM pg_indexes WHERE schemaname = 'geoguessr';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'idx_games_team_key' in result.stdout
        assert 'idx_guesses_player' in result.stdout

    def test_foreign_key_integrity(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT COUNT(*) FROM geoguessr.guesses gu "
             "LEFT JOIN geoguessr.games g ON gu.game_id = g.game_id "
             "WHERE g.game_id IS NULL;"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 0

    def test_replace_mode_idempotent(self, loaded_pg):
        push_to_postgres(
            rows=[dict(r) for r in SAMPLE_ROWS],
            csv_columns=CSV_COLUMNS,
            dsn=loaded_pg,
            schema='geoguessr',
            if_exists='replace',
        )
        result = subprocess.run(
            ['docker', 'exec', 'geoguessr-test-postgres',
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.games;'],
            capture_output=True, text=True
        )
        count = int(result.stdout.strip())
        assert count == 2


# ===================================================================
# Grafana tests
# ===================================================================

@requires_docker
class TestGrafanaContainer:
    def test_grafana_health(self, grafana_url, loaded_pg):
        """Grafana health endpoint should respond."""
        import urllib.request
        url = f'{grafana_url}/api/health'
        with urllib.request.urlopen(url, timeout=5) as resp:
            data = json.loads(resp.read())
        assert data.get('database') == 'ok'

    def test_grafana_datasource_provisioned(self, grafana_url, loaded_pg):
        """The PostgreSQL datasource should be provisioned."""
        import urllib.request
        import base64
        url = f'{grafana_url}/api/datasources'
        req = urllib.request.Request(url)
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        with urllib.request.urlopen(req, timeout=5) as resp:
            datasources = json.loads(resp.read())
        ds_names = [ds['name'] for ds in datasources]
        assert 'GeoGuessr PostgreSQL' in ds_names

    def test_grafana_dashboard_provisioned(self, grafana_url, loaded_pg):
        """The overview dashboard should be provisioned."""
        import urllib.request
        import base64
        url = f'{grafana_url}/api/dashboards/uid/geoguessr-overview'
        req = urllib.request.Request(url)
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        dashboard = data.get('dashboard', {})
        assert dashboard.get('uid') == 'geoguessr-overview'
        assert dashboard.get('title') == 'GeoGuessr Team Duel Stats'

    def test_grafana_dashboard_has_panels(self, grafana_url, loaded_pg):
        """The overview dashboard should have all expected panels."""
        import urllib.request
        import base64
        url = f'{grafana_url}/api/dashboards/uid/geoguessr-overview'
        req = urllib.request.Request(url)
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        panels = data.get('dashboard', {}).get('panels', [])
        assert len(panels) == 24
        titles = {p['title'] for p in panels}
        assert 'Team Rolling Avg Distance (km) Over Time' in titles
        assert 'Win Rate Over Time' in titles
        assert 'Countries Worth Studying' in titles
        assert 'Speed Ranking (Avg Guess Time)' in titles
        assert 'Summary Stats' in titles
        # v0.2.0 panels
        assert 'Won Team % (Beat Teammate)' in titles
        assert 'Won Round % (Best Overall Guess)' in titles
        assert 'Worst Countries (Highest Avg Distance)' in titles
        assert 'Region Performance (Avg Distance by Continent)' in titles
        assert 'Player Win/Loss Distance Split' in titles
        assert 'Competitive Advantage by Country' in titles
        assert 'Recent vs All-Time (Last 10 Games)' in titles
        # v0.3.0 initiative panels
        assert 'Initiative Rate by Player' in titles
        assert 'Initiative Rate Over Time' in titles
        assert 'Guess Speed by Region' in titles
        assert 'No-Pin Analysis' in titles
        assert 'Hesitation Index (Team Coordination)' in titles
        assert 'Pressure Response' in titles

    def test_grafana_query_postgres_via_api(self, grafana_url, loaded_pg):
        """Query Postgres through Grafana's datasource proxy API."""
        import urllib.request
        import base64
        url = f'{grafana_url}/api/ds/query'
        req = urllib.request.Request(url, method='POST')
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        req.add_header('Content-Type', 'application/json')

        query_body = json.dumps({
            "queries": [{
                "refId": "A",
                "datasource": {"uid": "geoguessr-postgres"},
                "rawSql": "SELECT COUNT(*) AS cnt FROM geoguessr.games",
                "format": "table"
            }],
            "from": "now-1y",
            "to": "now"
        }).encode('utf-8')

        with urllib.request.urlopen(req, data=query_body, timeout=10) as resp:
            data = json.loads(resp.read())
        results = data.get('results', {})
        assert 'A' in results
        frames = results['A'].get('frames', [])
        assert len(frames) > 0
