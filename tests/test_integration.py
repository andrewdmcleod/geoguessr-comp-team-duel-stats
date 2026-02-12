"""Integration tests for Phase 4: Postgres + Grafana Docker workflow.

These tests require Docker to be installed and running.
They spin up ephemeral containers, load test data, and verify everything works.

Run with: pytest tests/test_integration.py -v
Skip if no Docker: pytest tests/test_integration.py -v -k "not docker"

The tests use non-standard ports (15432/13000) to avoid conflicts.
All containers are cleaned up after tests.
"""

import csv as csv_mod
import json
import os
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pg_push import push_to_postgres, rows_to_tables
from tests.conftest import SAMPLE_ROWS, CSV_COLUMNS

# Test ports (non-standard to avoid conflicts)
TEST_PG_PORT = 15432
TEST_GRAFANA_PORT = 13000
TEST_PG_CONTAINER = 'geoguessr-test-postgres'
TEST_GRAFANA_CONTAINER = 'geoguessr-test-grafana'
TEST_NETWORK = 'geoguessr-test-net'
TEST_PG_DSN = f'postgresql://geoguessr:geoguessr@localhost:{TEST_PG_PORT}/geoguessr'


# ===================================================================
# Docker availability check
# ===================================================================

def docker_available() -> bool:
    try:
        result = subprocess.run(
            ['docker', 'info'], capture_output=True, text=True, timeout=10
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_docker = pytest.mark.skipif(
    not docker_available(),
    reason='Docker not available or not running'
)


# ===================================================================
# Docker helper functions
# ===================================================================

def run_docker(cmd, timeout=60):
    return subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=timeout)


def container_exists(name):
    result = subprocess.run(
        ['docker', 'inspect', name], capture_output=True, text=True
    )
    return result.returncode == 0


def stop_container(name):
    if container_exists(name):
        subprocess.run(['docker', 'rm', '-f', name], capture_output=True)


def network_exists(name):
    result = subprocess.run(
        ['docker', 'network', 'inspect', name], capture_output=True, text=True
    )
    return result.returncode == 0


def cleanup_all():
    """Remove all test containers and network."""
    stop_container(TEST_GRAFANA_CONTAINER)
    stop_container(TEST_PG_CONTAINER)
    if network_exists(TEST_NETWORK):
        subprocess.run(['docker', 'network', 'rm', TEST_NETWORK], capture_output=True)


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture(scope='module')
def docker_env():
    """Set up Docker network + Postgres container for the test module.

    Yields the Postgres DSN. Cleans up everything after.
    """
    if not docker_available():
        pytest.skip('Docker not available')

    cleanup_all()

    try:
        # Create network
        if not network_exists(TEST_NETWORK):
            run_docker(['docker', 'network', 'create', TEST_NETWORK])

        # Start Postgres
        run_docker([
            'docker', 'run', '-d',
            '--name', TEST_PG_CONTAINER,
            '--network', TEST_NETWORK,
            '-e', 'POSTGRES_USER=geoguessr',
            '-e', 'POSTGRES_PASSWORD=geoguessr',
            '-e', 'POSTGRES_DB=geoguessr',
            '-p', f'{TEST_PG_PORT}:5432',
            'postgres:16-alpine',
        ])

        # Wait for Postgres to be ready
        start = time.time()
        while time.time() - start < 30:
            result = subprocess.run(
                ['docker', 'exec', TEST_PG_CONTAINER, 'pg_isready', '-U', 'geoguessr'],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail('Postgres failed to start within 30s')

        yield TEST_PG_DSN

    finally:
        cleanup_all()


@pytest.fixture(scope='module')
def loaded_pg(docker_env):
    """Load test data into Postgres and return the DSN."""
    push_to_postgres(
        rows=[dict(r) for r in SAMPLE_ROWS],
        csv_columns=CSV_COLUMNS,
        dsn=docker_env,
        schema='geoguessr',
        if_exists='replace',
    )
    return docker_env


@pytest.fixture(scope='module')
def grafana_env(loaded_pg):
    """Start Grafana container connected to the test Postgres.

    Yields the Grafana base URL. Cleans up after.
    """
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    prov_dir = os.path.join(project_dir, 'grafana', 'provisioning')
    dash_dir = os.path.join(project_dir, 'grafana', 'dashboards')

    if not os.path.isdir(prov_dir):
        pytest.skip('Grafana provisioning directory not found')

    # We need to create a test-specific datasource that points at the test container
    import tempfile
    import shutil

    tmp_prov = tempfile.mkdtemp(prefix='grafana_test_prov_')
    tmp_ds_dir = os.path.join(tmp_prov, 'datasources')
    tmp_dash_prov_dir = os.path.join(tmp_prov, 'dashboards')
    os.makedirs(tmp_ds_dir)
    os.makedirs(tmp_dash_prov_dir)

    # Copy the dashboards provisioning file
    shutil.copy2(
        os.path.join(prov_dir, 'dashboards', 'dashboards.yml'),
        tmp_dash_prov_dir
    )

    # Write a test datasource pointing at the test Postgres container
    ds_yaml = f"""apiVersion: 1
datasources:
  - name: GeoGuessr PostgreSQL
    type: postgres
    uid: geoguessr-postgres
    access: proxy
    url: {TEST_PG_CONTAINER}:5432
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
    with open(os.path.join(tmp_ds_dir, 'datasource.yml'), 'w') as f:
        f.write(ds_yaml)

    try:
        stop_container(TEST_GRAFANA_CONTAINER)
        run_docker([
            'docker', 'run', '-d',
            '--name', TEST_GRAFANA_CONTAINER,
            '--network', TEST_NETWORK,
            '-e', 'GF_SECURITY_ADMIN_USER=admin',
            '-e', 'GF_SECURITY_ADMIN_PASSWORD=testpass',
            '-v', f'{os.path.abspath(tmp_prov)}:/etc/grafana/provisioning:ro',
            '-v', f'{os.path.abspath(dash_dir)}:/var/lib/grafana/dashboards:ro',
            '-p', f'{TEST_GRAFANA_PORT}:3000',
            'grafana/grafana:11.4.0',
        ])

        # Wait for Grafana
        start = time.time()
        while time.time() - start < 30:
            result = subprocess.run(
                ['docker', 'exec', TEST_GRAFANA_CONTAINER,
                 'curl', '-sf', 'http://localhost:3000/api/health'],
                capture_output=True, text=True
            )
            if result.returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail('Grafana failed to start within 30s')

        yield f'http://localhost:{TEST_GRAFANA_PORT}'

    finally:
        stop_container(TEST_GRAFANA_CONTAINER)
        shutil.rmtree(tmp_prov, ignore_errors=True)


# ===================================================================
# Postgres tests
# ===================================================================

@requires_docker
class TestPostgresContainer:
    def test_postgres_is_running(self, docker_env):
        """Postgres container should be running and accepting connections."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER, 'pg_isready', '-U', 'geoguessr'],
            capture_output=True, text=True
        )
        assert result.returncode == 0

    def test_postgres_accepts_query(self, docker_env):
        """Should be able to run a simple SQL query."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-c', 'SELECT 1 AS test;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'test' in result.stdout


@requires_docker
class TestPostgresDataLoad:
    def test_data_loaded(self, loaded_pg):
        """push_to_postgres should complete without error."""
        # If we reach here, the loaded_pg fixture succeeded
        assert loaded_pg == TEST_PG_DSN

    def test_games_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.games;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 2  # 2 games in sample data

    def test_rounds_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.rounds;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 3  # 3 rounds in sample data

    def test_guesses_table_has_data(self, loaded_pg):
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.guesses;'],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 6  # 6 guesses in sample data

    def test_game_data_correct(self, loaded_pg):
        """Verify game001 was loaded with correct fields."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT game_won, competitive_mode FROM geoguessr.games WHERE game_id = 'game001';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 't' in result.stdout       # game_won = true
        assert 'TeamDuels' in result.stdout

    def test_round_country_data(self, loaded_pg):
        """Verify round panorama data is correct."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT pano_country_code, pano_country_name FROM geoguessr.rounds "
             "WHERE game_id = 'game001' AND round_number = 1;"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'FR' in result.stdout
        assert 'France' in result.stdout

    def test_guess_player_data(self, loaded_pg):
        """Verify guess data for a specific player."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT player_name, distance_km, score FROM geoguessr.guesses "
             "WHERE game_id = 'game001' AND round_number = 1 AND player_id = 'player_a';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'Alice' in result.stdout
        assert '150' in result.stdout
        assert '3500' in result.stdout

    def test_indexes_created(self, loaded_pg):
        """Verify indexes exist."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT indexname FROM pg_indexes WHERE schemaname = 'geoguessr';"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        assert 'idx_games_team_key' in result.stdout
        assert 'idx_guesses_player' in result.stdout

    def test_foreign_key_integrity(self, loaded_pg):
        """Verify FK relationships: all guess game_ids exist in games table."""
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             "SELECT COUNT(*) FROM geoguessr.guesses gu "
             "LEFT JOIN geoguessr.games g ON gu.game_id = g.game_id "
             "WHERE g.game_id IS NULL;"],
            capture_output=True, text=True
        )
        assert result.returncode == 0
        count = int(result.stdout.strip())
        assert count == 0  # No orphaned guesses

    def test_replace_mode_idempotent(self, loaded_pg):
        """Loading the same data twice with replace mode should work."""
        push_to_postgres(
            rows=[dict(r) for r in SAMPLE_ROWS],
            csv_columns=CSV_COLUMNS,
            dsn=loaded_pg,
            schema='geoguessr',
            if_exists='replace',
        )
        result = subprocess.run(
            ['docker', 'exec', TEST_PG_CONTAINER,
             'psql', '-U', 'geoguessr', '-t', '-c',
             'SELECT COUNT(*) FROM geoguessr.games;'],
            capture_output=True, text=True
        )
        count = int(result.stdout.strip())
        assert count == 2  # Still 2 games


# ===================================================================
# Grafana tests
# ===================================================================

@requires_docker
class TestGrafanaContainer:
    def test_grafana_health(self, grafana_env):
        """Grafana health endpoint should respond."""
        import urllib.request
        url = f'{grafana_env}/api/health'
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
            assert data.get('database') == 'ok'
        except Exception as e:
            pytest.fail(f'Grafana health check failed: {e}')

    def test_grafana_datasource_provisioned(self, grafana_env):
        """The PostgreSQL datasource should be provisioned."""
        import urllib.request
        url = f'{grafana_env}/api/datasources'
        req = urllib.request.Request(url)
        # Basic auth: admin:testpass
        import base64
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                datasources = json.loads(resp.read())
            ds_names = [ds['name'] for ds in datasources]
            assert 'GeoGuessr PostgreSQL' in ds_names
        except Exception as e:
            pytest.fail(f'Datasource check failed: {e}')

    def test_grafana_dashboard_provisioned(self, grafana_env):
        """The overview dashboard should be provisioned."""
        import urllib.request
        import base64
        url = f'{grafana_env}/api/dashboards/uid/geoguessr-overview'
        req = urllib.request.Request(url)
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            dashboard = data.get('dashboard', {})
            assert dashboard.get('uid') == 'geoguessr-overview'
            assert dashboard.get('title') == 'GeoGuessr Team Duel Stats'
        except Exception as e:
            pytest.fail(f'Dashboard check failed: {e}')

    def test_grafana_dashboard_has_panels(self, grafana_env):
        """The overview dashboard should have all expected panels."""
        import urllib.request
        import base64
        url = f'{grafana_env}/api/dashboards/uid/geoguessr-overview'
        req = urllib.request.Request(url)
        credentials = base64.b64encode(b'admin:testpass').decode('ascii')
        req.add_header('Authorization', f'Basic {credentials}')
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            panels = data.get('dashboard', {}).get('panels', [])
            assert len(panels) == 11  # 11 panels in overview.json
            titles = {p['title'] for p in panels}
            assert 'Team Rolling Avg Distance (km) Over Time' in titles
            assert 'Win Rate Over Time' in titles
            assert 'Countries Worth Studying' in titles
            assert 'Speed Ranking (Avg Guess Time)' in titles
            assert 'Summary Stats' in titles
        except Exception as e:
            pytest.fail(f'Panel check failed: {e}')

    def test_grafana_datasource_query_works(self, grafana_env):
        """Should be able to query Postgres through Grafana's query API."""
        import urllib.request
        import base64
        url = f'{grafana_env}/api/ds/query'
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

        try:
            with urllib.request.urlopen(req, data=query_body, timeout=10) as resp:
                data = json.loads(resp.read())
            # Check the query returned results
            results = data.get('results', {})
            assert 'A' in results
            frames = results['A'].get('frames', [])
            assert len(frames) > 0
        except Exception as e:
            pytest.fail(f'Datasource query failed: {e}')
