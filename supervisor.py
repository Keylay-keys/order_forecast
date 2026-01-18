#!/usr/bin/env python3
"""Unified supervisor for all RouteSpark backend services.

Manages:
- Order Sync Listener (watches /orders, auto-syncs new routes)
- Order Archive Listener (handles app requests for old orders)
- Retrain Daemon (auto-retrains after order cycles complete)
- PCF OCR Listener (processes PCF images)

Usage:
    # Start all services
    python supervisor.py start

    # Start with specific route
    python supervisor.py start --route 989262

    # Check status
    python supervisor.py status

    # Stop all services
    python supervisor.py stop

    # Restart all
    python supervisor.py restart
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Paths
BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / 'scripts'
VENV_PYTHON = BASE_DIR / 'venv' / 'bin' / 'python'
PID_FILE = BASE_DIR / '.supervisor.pid'
LOG_DIR = BASE_DIR / 'logs'

# PCF Pipeline paths (separate project)
PCF_DIR = Path('/Users/kylemacmini/Desktop/projects/pcf_pipeline')
PCF_VENV_PYTHON = PCF_DIR / '.venv' / 'bin' / 'python'
PCF_SA_PATH = PCF_DIR / 'config' / 'serviceAccountKey.json'

# Default config
DEFAULT_ROUTE = '989262'
DEFAULT_SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'
DEFAULT_DB_PATH = BASE_DIR / 'data' / 'analytics.duckdb'


class Service:
    """Represents a managed background service."""
    
    def __init__(self, name: str, cmd: List[str], log_file: Path, cwd: Optional[Path] = None, use_caffeinate: bool = True):
        self.name = name
        self.cmd = cmd
        self.log_file = log_file
        self.cwd = cwd or BASE_DIR
        self.use_caffeinate = use_caffeinate
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
            
            # Wrap in caffeinate to prevent Mac sleep
            # -i: prevent idle sleep, -s: prevent system sleep
            actual_cmd = self.cmd
            if self.use_caffeinate:
                actual_cmd = ['caffeinate', '-is'] + self.cmd
            
            self.process = subprocess.Popen(
                actual_cmd,
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


def create_services(route: str, sa_path: str, db_path: str) -> List[Service]:
    """Create service definitions."""
    python = str(VENV_PYTHON)
    
    services = [
        # CORE: DB Manager owns the single DuckDB connection
        # Must start FIRST - other services communicate via Firebase
        Service(
            name="DB Manager",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'db_manager.py'),
                '--serviceAccount', sa_path,
                '--duckdb', str(db_path),
            ],
            log_file=LOG_DIR / 'db_manager.log',
        ),
        # Watches all orders, syncs new routes via DB Manager
        Service(
            name="Order Sync Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'order_sync_listener.py'),
                '--serviceAccount', sa_path,
            ],
            log_file=LOG_DIR / 'order_sync.log',
        ),
        # Handles archive requests from app via DB Manager
        Service(
            name="Archive Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'order_archive_listener.py'),
                '--serviceAccount', sa_path,
            ],
            log_file=LOG_DIR / 'archive_listener.log',
        ),
        # Periodic retraining check (uses DBClient)
        Service(
            name="Retrain Daemon",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'retrain_daemon.py'),
                '--service-account', sa_path,
                '--interval', '86400',  # Once per day (24 hours)
            ],
            log_file=LOG_DIR / 'retrain_daemon.log',
        ),
        # Delivery manifest requests from app
        Service(
            name="Delivery Manifest Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'delivery_manifest_listener.py'),
                '--serviceAccount', sa_path,
            ],
            log_file=LOG_DIR / 'delivery_listener.log',
        ),
    ]

    # Promo email listener - multi-user mode (reads settings from Firebase)
    # Users configure their IMAP settings in app: Settings → Promos
    services.append(
        Service(
            name="Promo Email Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'promo_email_listener.py'),
                '--service-account', sa_path,
                '--multi-user',  # Reads settings from Firebase for all users
            ],
            log_file=LOG_DIR / 'promo_email.log',
        )
    )
    
    # Catalog upload listener - watches for order guide uploads from app
    services.append(
        Service(
            name="Catalog Upload Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'catalog_upload_listener.py'),
                '--serviceAccount', sa_path,
            ],
            log_file=LOG_DIR / 'catalog_upload.log',
        )
    )
    
    # Low-quantity notification daemon
    services.append(
        Service(
            name="Low-Qty Notification Daemon",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'low_qty_notification_daemon.py'),
                '--serviceAccount', sa_path,
            ],
            log_file=LOG_DIR / 'low_qty_notifications.log',
        )
    )
    
    # Web Portal API (FastAPI)
    # Runs on 127.0.0.1:8000 - use Cloudflare Tunnel for external access
    services.append(
        Service(
            name="Web Portal API",
            cmd=[
                python,
                '-m', 'uvicorn',
                'api.main:app',
                '--host', '127.0.0.1',
                '--port', '8000',
            ],
            log_file=LOG_DIR / 'web_api.log',
            cwd=BASE_DIR,  # Run from order_forecast directory
        )
    )
    
    # Add PCF OCR Listener if the project exists
    if PCF_DIR.exists() and PCF_VENV_PYTHON.exists():
        services.append(
            Service(
                name="PCF OCR Listener",
                cmd=[
                    str(PCF_VENV_PYTHON),
                    '-m', 'pcf_core.runner',
                    '--watch-all',
                    '--credential', str(PCF_SA_PATH),
                    '--bucket', 'routespark-1f47d.firebasestorage.app',
                ],
                log_file=LOG_DIR / 'pcf_listener.log',
                cwd=PCF_DIR,  # Run from PCF project directory
            )
        )
    
    return services


def cmd_start(args):
    """Start all services."""
    log("🚀 Starting RouteSpark Backend Services")
    log("=" * 50)
    
    services = create_services(args.route, args.service_account, args.db)
    
    all_started = True
    for i, service in enumerate(services):
        if not service.start():
            all_started = False
        # Add delay between services to prevent DuckDB lock conflicts
        if i < len(services) - 1:
            time.sleep(2)
    
    if all_started:
        log("")
        log("✅ All services started!")
        log(f"   Logs: {LOG_DIR}/")
        log("")
        log("To stop: python supervisor.py stop")
        
        # Keep running and monitor
        if not args.detach:
            log("")
            log("Monitoring services (Ctrl+C to stop)...")
            try:
                while True:
                    time.sleep(10)
                    for service in services:
                        if not service.is_running():
                            log(f"⚠️  {service.name} died, restarting...")
                            service.start()
            except KeyboardInterrupt:
                log("\n🛑 Stopping all services...")
                for service in services:
                    service.stop()
    else:
        log("")
        log("⚠️  Some services failed to start")
        return 1
    
    return 0


def cmd_stop(args):
    """Stop all services."""
    log("🛑 Stopping RouteSpark Backend Services")
    log("=" * 50)
    
    # Find and kill processes by name
    import subprocess
    
    # First, kill the supervisor monitoring loop if running
    # (to prevent auto-restart of services)
    my_pid = os.getpid()
    try:
        result = subprocess.run(
            ['pgrep', '-f', 'supervisor.py'],
            capture_output=True,
            text=True,
        )
        if result.stdout.strip():
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                pid_int = int(pid)
                if pid_int != my_pid:  # Don't kill ourselves
                    try:
                        os.kill(pid_int, signal.SIGTERM)
                        log(f"  ✅ Stopped supervisor monitoring loop (PID {pid})")
                    except:
                        pass
    except Exception as e:
        log(f"  ⚠️  Error stopping supervisor: {e}")
    
    # Give supervisor a moment to stop
    time.sleep(0.5)
    
    # Now stop individual services
    patterns = ['db_manager.py', 'order_sync_listener.py', 'order_archive_listener.py', 'retrain_daemon.py', 'delivery_manifest_listener.py', 'promo_email_listener.py', 'catalog_upload_listener.py', 'low_qty_notification_daemon.py', 'api.main:app', 'pcf_core.runner']
    
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
    
    # Wait a bit for graceful shutdown
    time.sleep(1)
    log("")
    log("✅ All services stopped")
    
    return 0


def cmd_status(args):
    """Show status of all services."""
    log("📊 RouteSpark Backend Services Status")
    log("=" * 50)
    
    patterns = [
        ('DB Manager', 'db_manager.py'),
        ('Order Sync Listener', 'order_sync_listener.py'),
        ('Archive Listener', 'order_archive_listener.py'),
        ('Retrain Daemon', 'retrain_daemon.py'),
        ('Delivery Manifest', 'delivery_manifest_listener.py'),
        ('Promo Email', 'promo_email_listener.py'),
        ('Catalog Upload', 'catalog_upload_listener.py'),
        ('Low-Qty Notifications', 'low_qty_notification_daemon.py'),
        ('Web Portal API', 'api.main:app'),
        ('PCF OCR Listener', 'pcf_core.runner'),
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


def cmd_restart(args):
    """Restart all services."""
    cmd_stop(args)
    time.sleep(1)
    # Set default args for start command
    args.route = getattr(args, 'route', DEFAULT_ROUTE)
    args.service_account = getattr(args, 'service_account', DEFAULT_SA_PATH)
    args.db = getattr(args, 'db', str(DEFAULT_DB_PATH))
    args.detach = True  # Always detach on restart
    return cmd_start(args)


def cmd_logs(args):
    """Tail logs from all services."""
    log_files = list(LOG_DIR.glob('*.log'))
    if not log_files:
        log("No logs found")
        return 1
    
    log(f"📜 Tailing logs from: {', '.join(f.name for f in log_files)}")
    log("=" * 50)
    
    try:
        subprocess.run(['tail', '-f'] + [str(f) for f in log_files])
    except KeyboardInterrupt:
        pass
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        description='RouteSpark Backend Supervisor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  start     Start all services
  stop      Stop all services  
  restart   Restart all services
  status    Show service status
  logs      Tail all logs
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start all services')
    start_parser.add_argument('--route', default=DEFAULT_ROUTE, help='Route number')
    start_parser.add_argument('--service-account', default=DEFAULT_SA_PATH, help='Firebase SA')
    start_parser.add_argument('--db', default=str(DEFAULT_DB_PATH), help='DuckDB path')
    start_parser.add_argument('--detach', '-d', action='store_true', help='Detach after starting')
    
    # Other commands
    subparsers.add_parser('stop', help='Stop all services')
    subparsers.add_parser('restart', help='Restart all services')
    subparsers.add_parser('status', help='Show status')
    subparsers.add_parser('logs', help='Tail logs')
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 1
    
    commands = {
        'start': cmd_start,
        'stop': cmd_stop,
        'restart': cmd_restart,
        'status': cmd_status,
        'logs': cmd_logs,
    }
    
    return commands[args.command](args)


if __name__ == '__main__':
    sys.exit(main())
