#!/usr/bin/env python3
"""Docker-compatible supervisor for RouteSpark backend services.

Manages all order/forecast services in a container environment.
Unlike the macOS supervisor, this doesn't use caffeinate.

Services managed:
- Order Sync Listener
- Archive Listener
- Archive Export Worker
- Archive Purge Worker
- Retrain Daemon
- Delivery Manifest Listener
- Low-Qty Notification Daemon
- Config Sync Listener
- Promo Sync Listener

Note: DB Manager service deprecated - all clients use direct PostgreSQL.

Usage:
    python supervisor_docker.py start
    python supervisor_docker.py start --detach
    python supervisor_docker.py status
    python supervisor_docker.py stop
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# Paths (container-relative)
BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / 'scripts'
LOG_DIR = Path(os.environ.get('LOG_DIR', '/app/logs'))
STATUS_FILE = LOG_DIR / 'service_status.json'
STATUS_INTERVAL_SECONDS = int(os.environ.get('SERVICE_STATUS_INTERVAL_SECONDS', '30'))

# Config from environment
SA_PATH = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', '/app/config/serviceAccountKey.json')

# Global shutdown flag
_shutdown_requested = False


class Service:
    """Represents a managed background service."""
    
    def __init__(self, name: str, cmd: List[str], log_file: Path, cwd: Optional[Path] = None):
        self.name = name
        self.cmd = cmd
        self.log_file = log_file
        self.cwd = cwd or BASE_DIR
        self.process: Optional[subprocess.Popen] = None
    
    def start(self) -> bool:
        """Start the service."""
        if self.is_running():
            log(f"  {self.name}: Already running (PID {self.process.pid})")
            return True
        
        # Ensure log directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            log_handle = open(self.log_file, 'a')
            log_handle.write(f"\n{'='*60}\n")
            log_handle.write(f"Service started at {datetime.now()}\n")
            log_handle.write(f"{'='*60}\n")
            log_handle.flush()
            
            self.process = subprocess.Popen(
                self.cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.cwd),
            )
            
            # Give it a moment to start
            time.sleep(0.5)
            
            if self.process.poll() is None:
                log(f"  ✅ {self.name}: Started (PID {self.process.pid})")
                return True
            else:
                log(f"  ❌ {self.name}: Failed to start (exit code {self.process.returncode})")
                return False
                
        except Exception as e:
            log(f"  ❌ {self.name}: Error starting - {e}")
            return False
    
    def stop(self) -> bool:
        """Stop the service."""
        if not self.is_running():
            log(f"  {self.name}: Not running")
            return True
        
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
            log(f"  ✅ {self.name}: Stopped")
            return True
        except subprocess.TimeoutExpired:
            self.process.kill()
            log(f"  ⚠️  {self.name}: Killed (didn't stop gracefully)")
            return True
        except Exception as e:
            log(f"  ❌ {self.name}: Error stopping - {e}")
            return False
    
    def is_running(self) -> bool:
        """Check if service is running."""
        if self.process is None:
            return False
        return self.process.poll() is None
    
    def status(self) -> str:
        """Get service status string."""
        if self.is_running():
            return f"🟢 Running (PID {self.process.pid})"
        return "🔴 Stopped"


def log(msg: str):
    """Print timestamped log message."""
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def write_service_status(services: List[Service]) -> None:
    """Write a small status file used by external health checks."""
    try:
        now = datetime.utcnow().isoformat() + "Z"
        payload = {
            "timestamp": now,
            "services": [
                {
                    "name": svc.name,
                    "running": svc.is_running(),
                }
                for svc in services
            ],
        }
        tmp_path = STATUS_FILE.with_suffix('.tmp')
        tmp_path.write_text(json.dumps(payload, indent=2))
        tmp_path.replace(STATUS_FILE)
    except Exception as e:
        log(f"⚠️  Failed to write service status: {e}")


def create_services() -> List[Service]:
    """Create service definitions."""
    python = sys.executable

    # PostgreSQL connection is read from environment by all services.
    # Do NOT pass password via CLI args (visible in process list).
    # Environment variables: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD

    services = [
        # NOTE: DB Manager service DEPRECATED - all clients now use direct PostgreSQL.
        # The db_manager_pg.py module is kept for utility functions (handle_sync_order, etc.)
        # but no longer runs as a service watching dbRequests.

        # Watches all orders, syncs to PostgreSQL directly
        Service(
            name="Order Sync Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'order_sync_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'order_sync.log',
        ),
        # Handles archive requests from app via direct PostgreSQL
        Service(
            name="Archive Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'order_archive_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'archive_listener.log',
        ),
        # Archive export worker (owner-requested PCF export zips)
        Service(
            name="Archive Export Worker",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'archive_export_worker.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'archive_export_worker.log',
        ),
        # Archive purge + artifact cleanup worker (guarded by ARCHIVE_PURGE_ENABLED)
        Service(
            name="Archive Purge Worker",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'archive_purge_worker.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'archive_purge_worker.log',
        ),
        # Periodic retraining check
        Service(
            name="Retrain Daemon",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'retrain_daemon.py'),
                '--service-account', SA_PATH,
                '--interval', '86400',  # Once per day
            ],
            log_file=LOG_DIR / 'retrain_daemon.log',
        ),
        # Delivery manifest requests from app
        Service(
            name="Delivery Manifest Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'delivery_manifest_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'delivery_listener.log',
        ),
        # NOTE: Promo Email Listener disabled - feature not in use.
        # File kept at order_forecast/scripts/promo_email_listener.py for future use.
        #
        # NOTE: catalog_upload_listener depends on pcf_pipeline (macOS Vision)
        # It must run on the Mac, not in the server container.
        # See supervisor.py on Mac for that service.
        #
        # Low-quantity notification daemon
        Service(
            name="Low-Qty Notification Daemon",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'low_qty_notification_daemon.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'low_qty_notifications.log',
        ),
        # Config sync listener - real-time sync for stores/products/schedules
        Service(
            name="Config Sync Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'config_sync_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'config_sync.log',
        ),
        # Promo sync listener - real-time sync for promo data
        Service(
            name="Promo Sync Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'promo_sync_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'promo_sync.log',
        ),
        # Route transfer sync listener - cross-route case splitting ledger
        Service(
            name="Route Transfer Sync",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'route_transfer_sync_listener.py'),
                '--serviceAccount', SA_PATH,
            ],
            log_file=LOG_DIR / 'route_transfers.log',
        ),
    ]

    return services


def handle_shutdown(signum, frame):
    """Handle shutdown signals."""
    global _shutdown_requested
    _shutdown_requested = True
    log("\n🛑 Shutdown signal received...")


def cmd_start(args):
    """Start all services."""
    global _shutdown_requested
    
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)
    
    pg_host = os.environ.get('POSTGRES_HOST', 'localhost')
    pg_db = os.environ.get('POSTGRES_DB', 'routespark')
    
    log("🚀 Starting RouteSpark Backend Services (Docker)")
    log("=" * 50)
    log(f"   Service Account: {SA_PATH}")
    log(f"   PostgreSQL: {pg_host}/{pg_db}")
    log(f"   Logs: {LOG_DIR}")
    log("")
    
    services = create_services()
    
    all_started = True
    for i, service in enumerate(services):
        if not service.start():
            all_started = False
        # Add delay between services to prevent conflicts
        if i < len(services) - 1:
            time.sleep(2)
    
    if all_started:
        log("")
        log("✅ All services started!")
    else:
        log("")
        log("⚠️  Some services failed to start")
        if not args.detach:
            return 1
    
    # Keep running and monitor (unless detached)
    if not args.detach:
        log("")
        log("Monitoring services (Ctrl+C or SIGTERM to stop)...")
        try:
            last_status_write = 0.0
            while not _shutdown_requested:
                time.sleep(10)
                if time.time() - last_status_write >= STATUS_INTERVAL_SECONDS:
                    write_service_status(services)
                    last_status_write = time.time()
                for service in services:
                    if not service.is_running() and not _shutdown_requested:
                        log(f"⚠️  {service.name} died, restarting...")
                        service.start()
        except KeyboardInterrupt:
            pass
        
        log("\n🛑 Stopping all services...")
        for service in services:
            service.stop()
        write_service_status(services)
    
    return 0


def cmd_stop(args):
    """Stop all services by pattern matching."""
    log("🛑 Stopping RouteSpark Backend Services")
    log("=" * 50)
    
    patterns = [
        # NOTE: db_manager_pg.py removed - service deprecated
        'order_sync_listener.py',
        'order_archive_listener.py',
        'retrain_daemon.py',
        'delivery_manifest_listener.py',
        'promo_email_listener.py',
        # catalog_upload_listener runs on Mac
        'low_qty_notification_daemon.py',
        'config_sync_listener.py',
        'promo_sync_listener.py',
        'route_transfer_sync_listener.py',
    ]

    for pattern in patterns:
        try:
            result = subprocess.run(
                ['pgrep', '-f', pattern],
                capture_output=True,
                text=True,
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        log(f"  ✅ Stopped {pattern} (PID {pid})")
                    except:
                        pass
            else:
                log(f"  {pattern}: Not running")
        except Exception as e:
            log(f"  ⚠️  Error checking {pattern}: {e}")
    
    time.sleep(1)
    log("")
    log("✅ All services stopped")
    return 0


def cmd_status(args):
    """Show status of all services."""
    log("📊 RouteSpark Backend Services Status")
    log("=" * 50)
    
    patterns = [
        # NOTE: DB Manager removed - service deprecated
        ('Order Sync Listener', 'order_sync_listener.py'),
        ('Archive Listener', 'order_archive_listener.py'),
        ('Retrain Daemon', 'retrain_daemon.py'),
        ('Delivery Manifest', 'delivery_manifest_listener.py'),
        ('Promo Email', 'promo_email_listener.py'),
        # Catalog Upload runs on Mac (depends on pcf_pipeline)
        ('Low-Qty Notifications', 'low_qty_notification_daemon.py'),
        ('Config Sync', 'config_sync_listener.py'),
        ('Promo Sync', 'promo_sync_listener.py'),
        ('Route Transfers', 'route_transfer_sync_listener.py'),
    ]

    for name, pattern in patterns:
        try:
            result = subprocess.run(
                ['pgrep', '-f', pattern],
                capture_output=True,
                text=True,
            )
            if result.stdout.strip():
                pids = result.stdout.strip().split('\n')
                log(f"  🟢 {name}: Running (PID {', '.join(pids)})")
            else:
                log(f"  🔴 {name}: Stopped")
        except Exception as e:
            log(f"  ⚠️  {name}: Unknown ({e})")
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='RouteSpark Backend Supervisor (Docker)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start all services')
    start_parser.add_argument('--detach', '-d', action='store_true',
                              help='Exit after starting (let Docker manage)')
    
    # Other commands
    subparsers.add_parser('stop', help='Stop all services')
    subparsers.add_parser('status', help='Show status')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 1
    
    commands = {
        'start': cmd_start,
        'stop': cmd_stop,
        'status': cmd_status,
    }
    
    return commands[args.command](args)


if __name__ == '__main__':
    sys.exit(main())
