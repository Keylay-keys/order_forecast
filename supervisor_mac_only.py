#!/usr/bin/env python3
"""Mac-only supervisor for RouteSpark services.

Use this AFTER the server is running (PostgreSQL and Docker services).
This script only runs services that require macOS:
- PCF OCR Listener (uses Apple Vision framework)
- Catalog Upload Listener (depends on pcf_pipeline)

All database operations use direct PostgreSQL connections to the server.

Usage:
    # After server is live:
    python supervisor_mac_only.py start

    # Check status
    python supervisor_mac_only.py status

    # Stop
    python supervisor_mac_only.py stop
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
from typing import List, Optional

# Paths
BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / 'scripts'
VENV_PYTHON = BASE_DIR / 'venv' / 'bin' / 'python'
LOG_DIR = BASE_DIR / 'logs'

# PCF Pipeline paths
PCF_DIR = Path('/Users/kylemacmini/projects/pcf_pipeline')
PCF_VENV_PYTHON = PCF_DIR / '.venv' / 'bin' / 'python'
PCF_SA_PATH = PCF_DIR / 'config' / 'serviceAccountKey.json'

# Service account
DEFAULT_SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'

# PCF Archive settings - keep local during day, sync to server at night
PCF_ARCHIVE_REMOTE = 'keylay@routespark:/mnt/archive/pcf/pcf_archive'
PCF_ARCHIVE_SYNC_ON_WRITE = '0'  # Don't sync on each OCR run, use nightly sync


class Service:
    """Represents a managed background service."""
    
    def __init__(self, name: str, cmd: List[str], log_file: Path, cwd: Optional[Path] = None, env: Optional[dict] = None):
        self.name = name
        self.cmd = cmd
        self.log_file = log_file
        self.cwd = cwd or BASE_DIR
        self.env = env  # Additional environment variables
        self.process: Optional[subprocess.Popen] = None
    
    def start(self) -> bool:
        """Start the service with caffeinate wrapper."""
        if self.is_running():
            log(f"  {self.name}: Already running (PID {self.process.pid})")
            return True
        
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            log_handle = open(self.log_file, 'a')
            log_handle.write(f"\n{'='*60}\n")
            log_handle.write(f"Service started at {datetime.now()}\n")
            log_handle.write(f"{'='*60}\n")
            log_handle.flush()
            
            # Wrap in caffeinate to prevent Mac sleep
            actual_cmd = ['caffeinate', '-is'] + self.cmd
            
            # Merge environment variables
            proc_env = os.environ.copy()
            if self.env:
                proc_env.update(self.env)
            
            self.process = subprocess.Popen(
                actual_cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.cwd),
                env=proc_env,
            )
            
            time.sleep(0.5)
            
            if self.process.poll() is None:
                log(f"  ✅ {self.name}: Started (PID {self.process.pid})")
                return True
            else:
                log(f"  ❌ {self.name}: Failed (exit {self.process.returncode})")
                return False
                
        except Exception as e:
            log(f"  ❌ {self.name}: Error - {e}")
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
            log(f"  ⚠️  {self.name}: Killed")
            return True
        except Exception as e:
            log(f"  ❌ {self.name}: Error stopping - {e}")
            return False
    
    def is_running(self) -> bool:
        if self.process is None:
            return False
        return self.process.poll() is None


def log(msg: str):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def create_services() -> List[Service]:
    """Create Mac-only service definitions."""
    python = str(VENV_PYTHON)
    services = []
    
    # Catalog Upload Listener (depends on pcf_pipeline)
    services.append(
        Service(
            name="Catalog Upload Listener",
            cmd=[
                python,
                str(SCRIPTS_DIR / 'catalog_upload_listener.py'),
                '--serviceAccount', DEFAULT_SA_PATH,
            ],
            log_file=LOG_DIR / 'catalog_upload.log',
        )
    )
    
    # PCF OCR Listener (macOS Vision)
    if PCF_DIR.exists() and PCF_VENV_PYTHON.exists():
        # Environment for PCF archive - keep local during day, sync at night
        pcf_env = {
            'PCF_ARCHIVE_REMOTE': PCF_ARCHIVE_REMOTE,
            'PCF_ARCHIVE_SYNC_ON_WRITE': PCF_ARCHIVE_SYNC_ON_WRITE,
        }
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
                cwd=PCF_DIR,
                env=pcf_env,
            )
        )
    else:
        log(f"⚠️  PCF pipeline not found at {PCF_DIR}")
    
    return services


def cmd_start(args):
    """Start Mac-only services."""
    log("🍎 Starting Mac-Only RouteSpark Services")
    log("=" * 50)
    log("")
    log("These services require macOS and cannot run on the server:")
    log("  - Catalog Upload Listener (depends on pcf_pipeline)")
    log("  - PCF OCR Listener (Apple Vision framework)")
    log("")
    log("⚠️  Make sure the SERVER is running (PostgreSQL + Docker services)!")
    log("")
    
    services = create_services()
    
    for i, service in enumerate(services):
        service.start()
        if i < len(services) - 1:
            time.sleep(2)
    
    log("")
    log("✅ Mac-only services started!")
    log(f"   Logs: {LOG_DIR}/")
    
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
            log("\n🛑 Stopping Mac services...")
            for service in services:
                service.stop()
    
    return 0


def cmd_stop(args):
    """Stop Mac-only services."""
    log("🛑 Stopping Mac-Only Services")
    log("=" * 50)
    
    patterns = [
        ('Catalog Upload', 'catalog_upload_listener.py'),
        ('PCF OCR', 'pcf_core.runner'),
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
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        log(f"  ✅ Stopped {name} (PID {pid})")
                    except:
                        pass
            else:
                log(f"  {name}: Not running")
        except Exception as e:
            log(f"  ⚠️  Error: {e}")
    
    return 0


def cmd_status(args):
    """Show status of Mac-only services."""
    log("📊 Mac-Only Services Status")
    log("=" * 50)
    
    patterns = [
        ('Catalog Upload Listener', 'catalog_upload_listener.py'),
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


def main():
    parser = argparse.ArgumentParser(
        description='Mac-Only RouteSpark Services',
        epilog='Run this AFTER the server is live with db_manager running.',
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    start_parser = subparsers.add_parser('start', help='Start Mac-only services')
    start_parser.add_argument('--detach', '-d', action='store_true',
                              help='Exit after starting')
    
    subparsers.add_parser('stop', help='Stop Mac-only services')
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
