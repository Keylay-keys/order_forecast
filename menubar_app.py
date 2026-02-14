#!/usr/bin/env python3
"""RouteSpark Backend Status - macOS Menu Bar App

Shows service status in the menu bar with controls to start/stop services.

Usage:
    python menubar_app.py

Features:
    - 🟢 Green icon when all services running
    - 🟡 Yellow icon when some services running
    - 🔴 Red icon when no services running
    - Start/Stop all services
    - View individual service status
    - Quick access to logs
"""

import os
import subprocess
import sys
from pathlib import Path
from threading import Thread
from typing import Dict, List, Tuple

import rumps

# Paths
APP_DIR = Path(__file__).parent
SUPERVISOR = APP_DIR / 'supervisor.py'
LOGS_DIR = APP_DIR / 'logs'
VENV_PYTHON = APP_DIR / 'venv' / 'bin' / 'python'

# Services to monitor
SERVICES = [
    ('DB Manager', 'db_manager.py'),
    ('Order Sync', 'order_sync_listener.py'),
    ('Archive', 'order_archive_listener.py'),
    ('Retrain', 'retrain_daemon.py'),
    ('PCF OCR', 'pcf_core.runner'),
]


def check_process_running(pattern: str) -> Tuple[bool, int]:
    """Check if a process matching pattern is running. Returns (running, pid)."""
    try:
        result = subprocess.run(
            ['pgrep', '-f', pattern],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            pids = result.stdout.strip().split('\n')
            return True, int(pids[0])
        return False, 0
    except Exception:
        return False, 0


def get_all_status() -> Dict[str, Tuple[bool, int]]:
    """Get status of all services."""
    status = {}
    for name, pattern in SERVICES:
        running, pid = check_process_running(pattern)
        status[name] = (running, pid)
    return status


def run_supervisor_command(cmd: str) -> str:
    """Run a supervisor command and return output."""
    try:
        result = subprocess.run(
            [str(VENV_PYTHON), str(SUPERVISOR), cmd, '-d'],
            capture_output=True,
            text=True,
            cwd=str(APP_DIR),
            timeout=60,
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Error: {e}"


class RouteSparkStatusApp(rumps.App):
    def __init__(self):
        super(RouteSparkStatusApp, self).__init__(
            name="RouteSpark",
            title="RS",  # Start with text title
            icon=None,
            template=None,
        )
        
        self.status = {}
        self.update_menu()
        
        # Start background status checker
        self.timer = rumps.Timer(self.refresh_status, 10)  # Check every 10 seconds
        self.timer.start()
    
    def get_title_for_status(self) -> str:
        """Get title based on service status."""
        running = sum(1 for _, (r, _) in self.status.items() if r)
        total = len(self.status)
        
        if running == 0:
            return "RS ⛔"  # Red stop sign
        elif running == total:
            return "RS ✓"  # Check mark
        else:
            return f"RS {running}/{total}"
    
    def update_menu(self):
        """Update menu with current status."""
        self.status = get_all_status()
        self.title = self.get_title_for_status()
        
        running = sum(1 for _, (r, _) in self.status.items() if r)
        total = len(self.status)
        
        # Build menu items
        self.menu.clear()
        
        # Header
        self.menu.add(rumps.MenuItem(
            f"RouteSpark Backend: {running}/{total} running",
            callback=None,
        ))
        self.menu.add(rumps.separator)
        
        # Service status items
        for name, pattern in SERVICES:
            is_running, pid = self.status.get(name, (False, 0))
            icon = "🟢" if is_running else "🔴"
            text = f"{icon} {name}"
            if is_running:
                text += f" (PID {pid})"
            
            item = rumps.MenuItem(text, callback=None)
            self.menu.add(item)
        
        self.menu.add(rumps.separator)
        
        # Control buttons
        if running == total:
            self.menu.add(rumps.MenuItem("⏹ Stop All", callback=self.stop_all))
            self.menu.add(rumps.MenuItem("🔄 Restart All", callback=self.restart_all))
        elif running == 0:
            self.menu.add(rumps.MenuItem("▶️ Start All", callback=self.start_all))
        else:
            self.menu.add(rumps.MenuItem("▶️ Start All", callback=self.start_all))
            self.menu.add(rumps.MenuItem("⏹ Stop All", callback=self.stop_all))
            self.menu.add(rumps.MenuItem("🔄 Restart All", callback=self.restart_all))
        
        self.menu.add(rumps.separator)
        
        # Logs submenu
        logs_menu = rumps.MenuItem("📋 Logs")
        logs_menu.add(rumps.MenuItem("Open Logs Folder", callback=self.open_logs_folder))
        logs_menu.add(rumps.separator)
        logs_menu.add(rumps.MenuItem("Tail All Logs", callback=self.tail_all_logs))
        logs_menu.add(rumps.separator)
        
        for name, pattern in SERVICES:
            log_name = pattern.replace('.py', '.log').replace('.runner', '_listener.log')
            if 'db_manager' in pattern:
                log_name = 'db_manager.log'
            elif 'order_sync' in pattern:
                log_name = 'order_sync.log'
            elif 'order_archive' in pattern:
                log_name = 'archive_listener.log'
            elif 'retrain' in pattern:
                log_name = 'retrain_daemon.log'
            elif 'pcf' in pattern:
                log_name = 'pcf_listener.log'
            
            logs_menu.add(rumps.MenuItem(
                f"📄 {name}",
                callback=lambda _, ln=log_name: self.open_log(ln)
            ))
        
        self.menu.add(logs_menu)
        
        self.menu.add(rumps.separator)
        self.menu.add(rumps.MenuItem("🔄 Refresh", callback=self.manual_refresh))
    
    def refresh_status(self, _=None):
        """Background status refresh."""
        self.update_menu()
    
    @rumps.clicked("🔄 Refresh")
    def manual_refresh(self, _):
        """Manual refresh triggered by user."""
        self.update_menu()
        rumps.notification(
            "RouteSpark",
            "Status Refreshed",
            f"{sum(1 for _, (r, _) in self.status.items() if r)}/{len(self.status)} services running"
        )
    
    def start_all(self, _):
        """Start all services."""
        rumps.notification("RouteSpark", "Starting Services", "Please wait...")
        
        def do_start():
            output = run_supervisor_command('start')
            self.update_menu()
            running = sum(1 for _, (r, _) in self.status.items() if r)
            rumps.notification(
                "RouteSpark",
                "Services Started",
                f"{running}/{len(self.status)} services now running"
            )
        
        Thread(target=do_start).start()
    
    def stop_all(self, _):
        """Stop all services."""
        rumps.notification("RouteSpark", "Stopping Services", "Please wait...")
        
        def do_stop():
            output = run_supervisor_command('stop')
            self.update_menu()
            rumps.notification("RouteSpark", "Services Stopped", "All services have been stopped")
        
        Thread(target=do_stop).start()
    
    def restart_all(self, _):
        """Restart all services."""
        rumps.notification("RouteSpark", "Restarting Services", "Please wait...")
        
        def do_restart():
            run_supervisor_command('stop')
            run_supervisor_command('start')
            self.update_menu()
            running = sum(1 for _, (r, _) in self.status.items() if r)
            rumps.notification(
                "RouteSpark",
                "Services Restarted",
                f"{running}/{len(self.status)} services now running"
            )
        
        Thread(target=do_restart).start()
    
    def open_logs_folder(self, _):
        """Open logs folder in Finder."""
        subprocess.run(['open', str(LOGS_DIR)])
    
    def tail_all_logs(self, _):
        """Open Terminal with all logs tailing."""
        script = f'''
        tell application "Terminal"
            activate
            do script "cd {APP_DIR} && source venv/bin/activate && python supervisor.py logs"
        end tell
        '''
        subprocess.run(['osascript', '-e', script])
    
    def open_log(self, log_name: str):
        """Open a specific log file in Console.app."""
        log_path = LOGS_DIR / log_name
        if log_path.exists():
            subprocess.run(['open', '-a', 'Console', str(log_path)])
        else:
            rumps.notification("RouteSpark", "Log Not Found", f"{log_name} doesn't exist yet")


if __name__ == "__main__":
    RouteSparkStatusApp().run()

