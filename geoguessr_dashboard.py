#!/usr/bin/env python3
"""
GeoGuessr Dashboard Launcher

One-command Grafana dashboard workflow:
1. Starts ephemeral Postgres + Grafana via Docker
2. Loads the latest (or selected) export into Postgres
3. Provisions Grafana automatically (datasource + dashboards)
4. Prints a clickable Grafana URL when ready

Requires: Docker (docker compose or docker CLI)
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

# ===================================================================
# Configuration
# ===================================================================

DOCKER_NETWORK = 'geoguessr-stats'
PG_CONTAINER = 'geoguessr-postgres'
GRAFANA_CONTAINER = 'geoguessr-grafana'
PG_USER = 'geoguessr'
PG_PASS = 'geoguessr'
PG_DB = 'geoguessr'
PG_SCHEMA = 'geoguessr'
GRAFANA_ADMIN_USER = 'admin'
GRAFANA_ADMIN_PASS = 'geoguessr'


# ===================================================================
# Docker helpers
# ===================================================================

def run_cmd(cmd: list, capture=False, check=True, timeout=60) -> subprocess.CompletedProcess:
    """Run a shell command."""
    try:
        return subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            check=check,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        print(f"  ❌ Command timed out after {timeout}s: {' '.join(cmd)}")
        raise
    except subprocess.CalledProcessError as e:
        if capture:
            print(f"  ❌ Command failed: {' '.join(cmd)}")
            if e.stderr:
                print(f"     {e.stderr.strip()}")
        raise


def docker_available() -> bool:
    """Check if Docker is available and running."""
    try:
        result = run_cmd(['docker', 'info'], capture=True, check=False)
        return result.returncode == 0
    except FileNotFoundError:
        return False


def container_running(name: str) -> bool:
    """Check if a Docker container is running."""
    try:
        result = run_cmd(
            ['docker', 'inspect', '--format', '{{.State.Running}}', name],
            capture=True, check=False
        )
        return result.stdout.strip() == 'true'
    except Exception:
        return False


def container_exists(name: str) -> bool:
    """Check if a Docker container exists (running or stopped)."""
    try:
        result = run_cmd(
            ['docker', 'inspect', name],
            capture=True, check=False
        )
        return result.returncode == 0
    except Exception:
        return False


def stop_container(name: str):
    """Stop and remove a container if it exists."""
    if container_exists(name):
        run_cmd(['docker', 'rm', '-f', name], capture=True, check=False)


def network_exists(name: str) -> bool:
    """Check if a Docker network exists."""
    try:
        result = run_cmd(
            ['docker', 'network', 'inspect', name],
            capture=True, check=False
        )
        return result.returncode == 0
    except Exception:
        return False


def ensure_network():
    """Create the Docker network if it doesn't exist."""
    if not network_exists(DOCKER_NETWORK):
        print(f"  Creating Docker network: {DOCKER_NETWORK}")
        run_cmd(['docker', 'network', 'create', DOCKER_NETWORK], capture=True)


# ===================================================================
# Postgres
# ===================================================================

def start_postgres(port: int = 5432, timeout: int = 60):
    """Start an ephemeral Postgres container."""
    stop_container(PG_CONTAINER)

    print(f"  Starting PostgreSQL on port {port}...")
    run_cmd([
        'docker', 'run', '-d',
        '--name', PG_CONTAINER,
        '--network', DOCKER_NETWORK,
        '-e', f'POSTGRES_USER={PG_USER}',
        '-e', f'POSTGRES_PASSWORD={PG_PASS}',
        '-e', f'POSTGRES_DB={PG_DB}',
        '-p', f'{port}:5432',
        'postgres:16-alpine',
    ], capture=True)

    # Poll until ready
    print(f"  Waiting for PostgreSQL to be ready...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = run_cmd(
                ['docker', 'exec', PG_CONTAINER, 'pg_isready', '-U', PG_USER],
                capture=True, check=False, timeout=5
            )
            if result.returncode == 0:
                print(f"  ✅ PostgreSQL ready ({time.time() - start:.1f}s)")
                return
        except Exception:
            pass
        time.sleep(1)

    raise TimeoutError(f"PostgreSQL failed to start within {timeout}s")


def get_pg_dsn(port: int = 5432) -> str:
    """Get the PostgreSQL DSN for local connection."""
    return f'postgresql://{PG_USER}:{PG_PASS}@localhost:{port}/{PG_DB}'


def get_pg_dsn_internal() -> str:
    """Get the PostgreSQL DSN for container-to-container connection."""
    return f'postgresql://{PG_USER}:{PG_PASS}@{PG_CONTAINER}:5432/{PG_DB}'


# ===================================================================
# Grafana
# ===================================================================

def start_grafana(port: int = 3000, project_dir: str = '.', timeout: int = 60):
    """Start Grafana container with provisioning."""
    stop_container(GRAFANA_CONTAINER)

    prov_dir = os.path.join(project_dir, 'grafana', 'provisioning')
    dash_dir = os.path.join(project_dir, 'grafana', 'dashboards')

    if not os.path.isdir(prov_dir):
        raise FileNotFoundError(f"Grafana provisioning directory not found: {prov_dir}")

    print(f"  Starting Grafana on port {port}...")
    run_cmd([
        'docker', 'run', '-d',
        '--name', GRAFANA_CONTAINER,
        '--network', DOCKER_NETWORK,
        '-e', f'GF_SECURITY_ADMIN_USER={GRAFANA_ADMIN_USER}',
        '-e', f'GF_SECURITY_ADMIN_PASSWORD={GRAFANA_ADMIN_PASS}',
        '-e', 'GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/overview.json',
        '-v', f'{os.path.abspath(prov_dir)}:/etc/grafana/provisioning:ro',
        '-v', f'{os.path.abspath(dash_dir)}:/var/lib/grafana/dashboards:ro',
        '-p', f'{port}:3000',
        'grafana/grafana:11.4.0',
    ], capture=True)

    # Poll until ready
    print(f"  Waiting for Grafana to be ready...")
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = run_cmd(
                ['docker', 'exec', GRAFANA_CONTAINER,
                 'curl', '-sf', 'http://localhost:3000/api/health'],
                capture=True, check=False, timeout=5
            )
            if result.returncode == 0:
                print(f"  ✅ Grafana ready ({time.time() - start:.1f}s)")
                return
        except Exception:
            pass
        time.sleep(1)

    raise TimeoutError(f"Grafana failed to start within {timeout}s")


# ===================================================================
# Export resolution
# ===================================================================

def resolve_export(export_arg: str, outdir: str) -> str:
    """Resolve the export directory from the --export argument.

    Args:
        export_arg: 'latest', an export_id, or a path
        outdir: base output directory

    Returns:
        Path to the resolved CSV file
    """
    if export_arg == 'latest':
        latest_path = Path(outdir) / 'latest.json'
        if not latest_path.exists():
            raise FileNotFoundError(
                f"No latest.json found at {latest_path}. "
                f"Run geoguessr_stats.py first or use --export <path>."
            )
        with open(latest_path) as f:
            latest = json.load(f)
        csv_file = latest.get('csv_file')
        if csv_file and os.path.isfile(csv_file):
            return csv_file
        export_dir = latest.get('latest_export_dir', '')
        csv_candidate = os.path.join(export_dir, 'team_duels.csv')
        if os.path.isfile(csv_candidate):
            return csv_candidate
        raise FileNotFoundError(f"CSV file not found in latest export: {export_dir}")

    # Check if it's an export ID
    export_dir = Path(outdir) / 'exports' / export_arg
    if export_dir.is_dir():
        csv_candidate = export_dir / 'team_duels.csv'
        if csv_candidate.exists():
            return str(csv_candidate)
        raise FileNotFoundError(f"No team_duels.csv in {export_dir}")

    # Check if it's a direct path
    if os.path.isfile(export_arg):
        return export_arg

    raise FileNotFoundError(f"Cannot resolve export: {export_arg}")


# ===================================================================
# Data loading
# ===================================================================

def load_data_to_postgres(csv_file: str, dsn: str, schema: str = PG_SCHEMA):
    """Load a CSV file into Postgres using pg_push."""
    import csv as csv_module
    from pg_push import push_to_postgres

    print(f"  Loading {csv_file}...")
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        rows = list(reader)

    if not rows:
        print("  ⚠️  No data in CSV file!")
        return

    print(f"  {len(rows)} rows to load")
    push_to_postgres(
        rows=rows,
        csv_columns=list(rows[0].keys()),
        dsn=dsn,
        schema=schema,
        if_exists='replace',
    )


# ===================================================================
# Cleanup
# ===================================================================

def cleanup():
    """Stop and remove all containers."""
    print("\n🧹 Cleaning up Docker containers...")
    stop_container(GRAFANA_CONTAINER)
    stop_container(PG_CONTAINER)
    if network_exists(DOCKER_NETWORK):
        run_cmd(['docker', 'network', 'rm', DOCKER_NETWORK], capture=True, check=False)
    print("  ✅ Cleanup complete")


# ===================================================================
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Launch GeoGuessr Grafana Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Launch with latest export data
  python geoguessr_dashboard.py --config config.json

  # Launch with a specific export
  python geoguessr_dashboard.py --config config.json --export 2025-02-10_143000

  # Launch and refresh data first
  python geoguessr_dashboard.py --config config.json --refresh

  # List available exports
  python geoguessr_dashboard.py --config config.json --list-exports

  # Custom ports
  python geoguessr_dashboard.py --config config.json --pg-port 5433 --grafana-port 3001

  # Stop the dashboard
  python geoguessr_dashboard.py --stop
        """
    )
    parser.add_argument('--config', type=str, default='config.json',
                        help='Path to config.json')
    parser.add_argument('--outdir', type=str, default='out',
                        help='Base output directory (default: out)')
    parser.add_argument('--export', type=str, default='latest',
                        help='Export to load: "latest", an export_id, or a CSV path (default: latest)')
    parser.add_argument('--list-exports', action='store_true',
                        help='List existing exports and exit')
    parser.add_argument('--team', type=str, default=None,
                        help='Team ID filter (not yet implemented)')
    parser.add_argument('--refresh', action='store_true',
                        help='Run the fetcher/analyzer first to create a fresh export')
    parser.add_argument('--rebuild-db', action='store_true',
                        help='Force replace mode even if containers already running')
    parser.add_argument('--no-grafana', action='store_true',
                        help='Only start Postgres and load data (debug mode)')
    parser.add_argument('--pg-port', type=int, default=5432,
                        help='PostgreSQL host port (default: 5432)')
    parser.add_argument('--grafana-port', type=int, default=3000,
                        help='Grafana host port (default: 3000)')
    parser.add_argument('--docker-timeout', type=int, default=60,
                        help='Timeout in seconds for Docker containers to start')
    parser.add_argument('--stop', action='store_true',
                        help='Stop the dashboard (remove containers) and exit')
    args = parser.parse_args()

    # Handle --stop
    if args.stop:
        cleanup()
        return

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
                print(f"  {d.name}")
        return

    # Pre-flight checks
    print("🎮 GeoGuessr Dashboard Launcher")
    print("=" * 50)

    if not docker_available():
        print("❌ Docker is not available or not running.")
        print("   Install Docker: https://docs.docker.com/get-docker/")
        sys.exit(1)

    project_dir = os.path.dirname(os.path.abspath(__file__))

    # Check Grafana provisioning files exist
    prov_dir = os.path.join(project_dir, 'grafana', 'provisioning')
    if not os.path.isdir(prov_dir) and not args.no_grafana:
        print(f"❌ Grafana provisioning not found at: {prov_dir}")
        print("   Make sure the grafana/ directory exists in the project root.")
        sys.exit(1)

    # Handle --refresh
    if args.refresh:
        print("\n🔄 Refreshing data...")
        refresh_cmd = [
            sys.executable, os.path.join(project_dir, 'geoguessr_stats.py'),
            '--config' if '--config' in sys.argv else '', args.config,
            '--outdir', args.outdir,
        ]
        # Filter empty strings
        refresh_cmd = [c for c in refresh_cmd if c]
        try:
            run_cmd(refresh_cmd, timeout=300)
        except Exception as e:
            print(f"  ⚠️  Refresh failed: {e}")
            print("  Continuing with existing data...")

    # Resolve export
    print(f"\n📂 Resolving export: {args.export}")
    try:
        csv_file = resolve_export(args.export, args.outdir)
        print(f"  Using: {csv_file}")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        sys.exit(1)

    # Step 1: Create Docker network
    print(f"\n🐳 Setting up Docker environment...")
    ensure_network()

    # Step 2: Start Postgres
    print(f"\n🐘 Starting PostgreSQL...")
    try:
        start_postgres(port=args.pg_port, timeout=args.docker_timeout)
    except Exception as e:
        print(f"❌ Failed to start PostgreSQL: {e}")
        cleanup()
        sys.exit(1)

    # Step 3: Load data
    print(f"\n📊 Loading data into PostgreSQL...")
    dsn = get_pg_dsn(args.pg_port)
    try:
        load_data_to_postgres(csv_file, dsn)
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        cleanup()
        sys.exit(1)

    # Step 4: Start Grafana
    if not args.no_grafana:
        print(f"\n📈 Starting Grafana...")
        try:
            start_grafana(
                port=args.grafana_port,
                project_dir=project_dir,
                timeout=args.docker_timeout,
            )
        except Exception as e:
            print(f"❌ Failed to start Grafana: {e}")
            cleanup()
            sys.exit(1)

    # Done!
    print(f"\n{'=' * 50}")
    print(f"✅ Dashboard is ready!")
    print(f"{'=' * 50}")

    if not args.no_grafana:
        url = f"http://localhost:{args.grafana_port}"
        print(f"\n  🌐 Grafana URL: {url}")
        print(f"  👤 Username:    {GRAFANA_ADMIN_USER}")
        print(f"  🔑 Password:    {GRAFANA_ADMIN_PASS}")

    print(f"\n  🐘 PostgreSQL:  {dsn}")
    print(f"\n  To stop: python geoguessr_dashboard.py --stop")

    # Wait for Ctrl+C
    print(f"\n  Press Ctrl+C to stop the dashboard...")

    def signal_handler(sig, frame):
        cleanup()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup()


if __name__ == '__main__':
    main()
