#!/usr/bin/env python3
"""
GeoGuessr Dashboard Launcher

One-command Grafana dashboard workflow:
1. Starts ephemeral Postgres + Grafana via docker compose
2. Loads the latest (or selected) export into Postgres
3. Grafana is auto-provisioned (datasource + dashboards via mounted files)
4. Prints a clickable Grafana URL when ready

Configuration:
  docker-compose.yml  — service definitions (images, healthchecks, volumes)
  .env                — ports, credentials, image versions (copy from .env.example)
  grafana/            — provisioning and dashboard JSON files

Requires: Docker with compose plugin (docker compose)
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


# ===================================================================
# Load settings from .env (or defaults matching docker-compose.yml)
# ===================================================================

def load_env(project_dir: str) -> dict:
    """Load configuration from .env file, falling back to defaults.

    These defaults match docker-compose.yml's ${VAR:-default} syntax.
    """
    defaults = {
        'PG_CONTAINER': 'geoguessr-postgres',
        'PG_USER': 'geoguessr',
        'PG_PASSWORD': 'geoguessr',
        'PG_DB': 'geoguessr',
        'PG_PORT': '5432',
        'PG_SCHEMA': 'geoguessr',
        'GRAFANA_CONTAINER': 'geoguessr-grafana',
        'GRAFANA_ADMIN_USER': 'admin',
        'GRAFANA_ADMIN_PASSWORD': 'geoguessr',
        'GRAFANA_PORT': '3000',
    }

    env_file = os.path.join(project_dir, '.env')
    if os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, _, value = line.partition('=')
                defaults[key.strip()] = value.strip()

    return defaults


# ===================================================================
# Shell helpers
# ===================================================================

def run_cmd(cmd: list, capture=False, check=True, timeout=60,
            cwd=None) -> subprocess.CompletedProcess:
    """Run a shell command."""
    try:
        return subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            check=check,
            timeout=timeout,
            cwd=cwd,
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


def docker_compose_available(project_dir: str) -> bool:
    """Check if docker compose is available."""
    try:
        result = run_cmd(
            ['docker', 'compose', 'version'],
            capture=True, check=False, cwd=project_dir
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


# ===================================================================
# Docker Compose wrappers
# ===================================================================

def compose_up(project_dir: str, services: list = None, timeout: int = 60):
    """Run docker compose up -d [services]."""
    cmd = ['docker', 'compose', 'up', '-d', '--wait',
           '--wait-timeout', str(timeout)]
    if services:
        cmd.extend(services)
    print(f"  Running: {' '.join(cmd)}")
    run_cmd(cmd, capture=False, cwd=project_dir, timeout=timeout + 30)


def compose_down(project_dir: str):
    """Run docker compose down."""
    run_cmd(
        ['docker', 'compose', 'down', '--remove-orphans'],
        capture=True, check=False, cwd=project_dir, timeout=30
    )


def compose_ps(project_dir: str) -> str:
    """Run docker compose ps and return output."""
    result = run_cmd(
        ['docker', 'compose', 'ps', '--format', 'table'],
        capture=True, check=False, cwd=project_dir
    )
    return result.stdout


def wait_for_healthy(container_name: str, timeout: int = 60):
    """Poll until a container's healthcheck passes."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = run_cmd(
                ['docker', 'inspect', '--format',
                 '{{.State.Health.Status}}', container_name],
                capture=True, check=False, timeout=5
            )
            status = result.stdout.strip()
            if status == 'healthy':
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"{container_name} not healthy within {timeout}s")


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

def load_data_to_postgres(csv_file: str, dsn: str, schema: str):
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
# Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Launch GeoGuessr Grafana Dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Configuration files:
  docker-compose.yml  — Docker service definitions
  .env                — Ports, credentials, image versions (cp .env.example .env)
  grafana/            — Provisioning and dashboard JSON files

Examples:
  # Launch with latest export data
  python geoguessr_dashboard.py --config config.json

  # Launch with a specific export
  python geoguessr_dashboard.py --config config.json --export 2025-02-10_143000

  # Launch and refresh data first
  python geoguessr_dashboard.py --config config.json --refresh

  # List available exports
  python geoguessr_dashboard.py --list-exports

  # Custom ports (edit .env or pass overrides)
  GRAFANA_PORT=3001 PG_PORT=5433 python geoguessr_dashboard.py --config config.json

  # Stop the dashboard
  python geoguessr_dashboard.py --stop
        """
    )
    parser.add_argument('--config', type=str, default='config.json',
                        help='Path to config.json')
    parser.add_argument('--outdir', type=str, default='out',
                        help='Base output directory (default: out)')
    parser.add_argument('--export', type=str, default='latest',
                        help='Export to load: "latest", an export_id, or a CSV path '
                             '(default: latest)')
    parser.add_argument('--list-exports', action='store_true',
                        help='List existing exports and exit')
    parser.add_argument('--refresh', action='store_true',
                        help='Run the fetcher first to create a fresh export')
    parser.add_argument('--rebuild-db', action='store_true',
                        help='Force drop + recreate Postgres tables')
    parser.add_argument('--no-grafana', action='store_true',
                        help='Only start Postgres and load data (debug mode)')
    parser.add_argument('--docker-timeout', type=int, default=60,
                        help='Timeout in seconds for containers to become healthy')
    parser.add_argument('--stop', action='store_true',
                        help='Stop the dashboard (docker compose down) and exit')
    args = parser.parse_args()

    project_dir = os.path.dirname(os.path.abspath(__file__))
    env = load_env(project_dir)

    # Handle --stop
    if args.stop:
        print("🧹 Stopping dashboard...")
        compose_down(project_dir)
        print("  ✅ Done")
        return

    # Handle --list-exports
    if args.list_exports:
        exports_dir = Path(args.outdir) / 'exports'
        if not exports_dir.exists():
            print(f"No exports found in {exports_dir}")
            return
        exports = sorted(exports_dir.iterdir(), reverse=True)
        dirs = [d for d in exports if d.is_dir()]
        if not dirs:
            print(f"No exports found in {exports_dir}")
            return
        print(f"📦 Exports in {exports_dir} (newest first):")
        for d in dirs:
            print(f"  {d.name}")
        return

    # Pre-flight checks
    print("🎮 GeoGuessr Dashboard Launcher")
    print("=" * 50)

    compose_file = os.path.join(project_dir, 'docker-compose.yml')
    if not os.path.isfile(compose_file):
        print(f"❌ docker-compose.yml not found at: {compose_file}")
        sys.exit(1)

    if not docker_compose_available(project_dir):
        print("❌ 'docker compose' is not available.")
        print("   Install Docker Desktop or the compose plugin:")
        print("   https://docs.docker.com/compose/install/")
        sys.exit(1)

    prov_dir = os.path.join(project_dir, 'grafana', 'provisioning')
    if not os.path.isdir(prov_dir) and not args.no_grafana:
        print(f"❌ Grafana provisioning not found at: {prov_dir}")
        sys.exit(1)

    # Handle --refresh
    if args.refresh:
        print("\n🔄 Refreshing data...")
        refresh_cmd = [
            sys.executable, os.path.join(project_dir, 'geoguessr_stats.py'),
            '--outdir', args.outdir,
        ]
        try:
            run_cmd(refresh_cmd, timeout=300, cwd=project_dir)
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

    # Determine which services to start
    services = ['postgres']
    if not args.no_grafana:
        services.append('grafana')

    # Step 1: docker compose up
    print(f"\n🐳 Starting services: {', '.join(services)}")
    print(f"   (images and config defined in docker-compose.yml + .env)")
    try:
        compose_up(project_dir, services=services, timeout=args.docker_timeout)
    except Exception as e:
        print(f"❌ Failed to start services: {e}")
        compose_down(project_dir)
        sys.exit(1)

    # Step 2: Load data into Postgres
    pg_port = int(env.get('PG_PORT', '5432'))
    pg_user = env.get('PG_USER', 'geoguessr')
    pg_pass = env.get('PG_PASSWORD', 'geoguessr')
    pg_db = env.get('PG_DB', 'geoguessr')
    pg_schema = env.get('PG_SCHEMA', 'geoguessr')
    dsn = f'postgresql://{pg_user}:{pg_pass}@localhost:{pg_port}/{pg_db}'

    print(f"\n📊 Loading data into PostgreSQL...")
    try:
        load_data_to_postgres(csv_file, dsn, schema=pg_schema)
    except Exception as e:
        print(f"❌ Failed to load data: {e}")
        compose_down(project_dir)
        sys.exit(1)

    # Done!
    grafana_port = int(env.get('GRAFANA_PORT', '3000'))
    grafana_user = env.get('GRAFANA_ADMIN_USER', 'admin')
    grafana_pass = env.get('GRAFANA_ADMIN_PASSWORD', 'geoguessr')

    print(f"\n{'=' * 50}")
    print(f"✅ Dashboard is ready!")
    print(f"{'=' * 50}")

    if not args.no_grafana:
        print(f"\n  🌐 Grafana URL: http://localhost:{grafana_port}")
        print(f"  👤 Username:    {grafana_user}")
        print(f"  🔑 Password:    {grafana_pass}")

    print(f"\n  🐘 PostgreSQL:  {dsn}")
    print(f"\n  Configuration:  docker-compose.yml + .env")
    print(f"  To stop:        python geoguessr_dashboard.py --stop")
    print(f"                  (or: docker compose down)")

    # Wait for Ctrl+C
    print(f"\n  Press Ctrl+C to stop the dashboard...")

    def signal_handler(sig, frame):
        print()
        compose_down(project_dir)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        compose_down(project_dir)


if __name__ == '__main__':
    main()
