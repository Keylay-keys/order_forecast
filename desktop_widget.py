#!/usr/bin/env python3
"""RouteSpark Desktop Widget - Floating status panel

Shows status of:
- Server services (via API health checks)
- Mac-only services (PCF OCR, Catalog Upload)

Usage:
    python desktop_widget.py
"""

import subprocess
import sys
import json
import urllib.request
import urllib.error
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
import time
import logging
from logging.handlers import RotatingFileHandler

from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QFont, QMouseEvent
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QFrame, QGraphicsDropShadowEffect,
    QCheckBox
)

# Paths
APP_DIR = Path(__file__).parent
SUPERVISOR_MAC = APP_DIR / 'supervisor_mac_only.py'
LOGS_DIR = APP_DIR / 'logs'
VENV_PYTHON = APP_DIR / 'venv' / 'bin' / 'python'
SETTINGS_FILE = APP_DIR / '.widget_settings.json'
WIDGET_LOG_FILE = Path.home() / 'Library' / 'Logs' / 'routespark-widget.log'
BACKUP_LOG_FILE = Path.home() / 'Library' / 'Logs' / 'routespark-critical-backup.log'
ARCHIVE_SYNC_LOG_FILE = Path.home() / 'Library' / 'Logs' / 'routespark-pcf-archive-sync.log'
PCF_RECONCILER_HEARTBEAT_FILE = Path.home() / 'projects' / 'pcf_pipeline' / 'logs' / 'reconciler' / 'archive_selector_heartbeat.json'
ARCHIVE_SSH_HOST = "keylay@100.64.201.120"
ARCHIVE_PURGE_SERVICE_NAME = "Archive Purge Worker"
logger = logging.getLogger("routespark_widget")

# Server API URL (default; can be overridden in .widget_settings.json)
SERVER_API_URL = 'https://api.routespark.pro'

# Default widget settings (merged with .widget_settings.json)
DEFAULT_SETTINGS = {
    'macos_notifications': True,
    'ntfy': {
        'enabled': True,
        'topic': 'routespark-down-detection',
    },
    # Require N consecutive failures before marking server down
    'server_failure_threshold': 3,
    # Require N consecutive successes before marking server up
    'server_recovery_threshold': 2,
    # Suppress repeat notifications for the same service within this window
    'notification_cooldown_minutes': 30,
    # Server health check timeout in seconds
    'server_check_timeout_seconds': 5,
    # Refresh interval in milliseconds
    'refresh_interval_ms': 5000,
    # Optional: notify when a service recovers
    'notify_recovery': False,
    # Optional override for server API base URL (e.g., http://100.64.201.120:8000)
    'server_api_url': SERVER_API_URL,
    # Log level (INFO, WARNING, ERROR)
    'log_level': 'INFO',
}

# Mac-only services to monitor locally
MAC_SERVICES = [
    ('PCF OCR', 'pcf_core.runner'),
    ('Catalog Upload', 'catalog_upload_listener.py'),
]

# Server services (checked via API heartbeat)
SERVER_SERVICE_MAP = {
    "Order Sync": "Order Sync Listener",
    "Archive": "Archive Listener",
    "Retrain": "Retrain Daemon",
    "Delivery": "Delivery Manifest Listener",
    "Low-Qty Notif": "Low-Qty Notification Daemon",
    "Config Sync": "Config Sync Listener",
    "Promo Sync": "Promo Sync Listener",
}


def send_macos_notification(title: str, message: str, sound: bool = True):
    """Send macOS desktop notification."""
    try:
        sound_cmd = 'with sound "Blow"' if sound else ''
        script = f'''
        display notification "{message}" with title "{title}" {sound_cmd}
        '''
        subprocess.run(['osascript', '-e', script], capture_output=True)
    except Exception as e:
        logger.warning("macOS notification error: %s", e)


def send_ntfy_notification(title: str, message: str, settings: dict):
    """Send push notification via ntfy.sh"""
    ntfy_config = settings.get('ntfy', {})
    if not ntfy_config.get('enabled'):
        return
    
    topic = ntfy_config.get('topic', '')
    if not topic:
        return
    
    try:
        url = f"https://ntfy.sh/{topic}"
        data = message.encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        req.add_header('Title', title)
        req.add_header('Priority', 'high')
        req.add_header('Tags', 'warning,rotating_light')
        
        with urllib.request.urlopen(req, timeout=10) as response:
            pass
    except Exception as e:
        logger.warning("ntfy notification error: %s", e)


def send_notification(title: str, message: str, settings: dict):
    """Send notifications via all enabled channels."""
    if settings.get('macos_notifications', True):
        send_macos_notification(title, message)
    if settings.get('ntfy', {}).get('enabled'):
        send_ntfy_notification(title, message, settings)


def load_settings() -> dict:
    """Load widget settings (merge with defaults)."""
    settings = json.loads(json.dumps(DEFAULT_SETTINGS))
    try:
        if SETTINGS_FILE.exists():
            saved = json.loads(SETTINGS_FILE.read_text())
            if isinstance(saved, dict):
                settings.update(saved)
                if isinstance(saved.get('ntfy'), dict):
                    settings['ntfy'].update(saved['ntfy'])
    except Exception:
        pass
    return settings


def save_settings(settings: dict):
    """Save widget settings."""
    try:
        SETTINGS_FILE.write_text(json.dumps(settings, indent=2))
    except Exception as e:
        logger.warning("Settings save error: %s", e)


def init_logger(settings: dict) -> logging.Logger:
    """Initialize rotating file logger."""
    logger = logging.getLogger("routespark_widget")
    if logger.handlers:
        return logger

    level_str = settings.get('log_level', 'INFO').upper()
    level = getattr(logging, level_str, logging.INFO)
    logger.setLevel(level)

    handler = RotatingFileHandler(
        WIDGET_LOG_FILE,
        maxBytes=256 * 1024,
        backupCount=3,
    )
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


# Colors
COLORS = {
    'bg': '#1a1a2e',
    'header': '#16213e',
    'accent': '#0f3460',
    'green': '#00d26a',
    'red': '#ff4757',
    'yellow': '#ffa502',
    'blue': '#3498db',
    'text': '#eaeaea',
    'text_dim': '#888888',
}


@dataclass
class ProbeResult:
    service: str
    ok: bool
    checked_at: float
    latency_ms: int | None
    status: str
    error: str | None = None
    details: dict | None = None


@dataclass
class MonitorState:
    service: str
    status: str = "up"
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_result: ProbeResult | None = None
    last_transition_at: float | None = None
    last_notified_at: float | None = None
    last_error_logged: str | None = None


@dataclass
class TransitionInfo:
    old_status: str
    new_status: str
    did_transition: bool
    should_notify_down: bool = False
    should_notify_recovery: bool = False


def check_process_running(pattern: str) -> tuple[bool, int]:
    """Check if a process matching pattern is running."""
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


def check_server_health(server_api_url: str, timeout_seconds: int) -> dict:
    """Check server health via API."""
    health = {
        'connected': False,
        'status': 'offline',
        'detailsProtected': True,
        'firebaseHealth': {'status': 'protected'},
        'serviceHealth': {'status': 'protected', 'services': []},
    }
    try:
        req = urllib.request.Request(
            f"{server_api_url}/api/health",
            headers={'User-Agent': 'RouteSpark-Widget/1.0'}
        )
        with urllib.request.urlopen(req, timeout=timeout_seconds) as response:
            if response.status == 200:
                data = json.loads(response.read().decode('utf-8'))
                health.update({
                    'connected': True,
                    'status': data.get('status', 'unknown'),
                    'apiHealth': data,
                })
    except Exception as e:
        health.update({'connected': False, 'status': 'offline', 'error': str(e)})
        return health

    return health


def get_last_backup_success(log_path: Path) -> str | None:
    """Return the last successful backup timestamp string."""
    try:
        if not log_path.exists():
            return None
        lines = log_path.read_text(errors='ignore').splitlines()
        for line in reversed(lines):
            if "Backup complete:" in line:
                return line.split(": Backup complete:", 1)[0].strip()
    except Exception:
        return None
    return None


def get_archive_sync_status() -> dict | None:
    """Return local archive sync status from the Mac launchd-managed log."""
    try:
        if not ARCHIVE_SYNC_LOG_FILE.exists():
            return None
        last_start = None
        last_complete = None
        for line in ARCHIVE_SYNC_LOG_FILE.read_text(errors='ignore').splitlines():
            if "Starting PCF archive sync" in line:
                last_start = line.strip()
            elif "PCF archive sync complete" in line:
                last_complete = line.strip()
        if not last_complete:
            return None
        return {
            "status": "success",
            "lastStart": last_start,
            "lastComplete": last_complete,
        }
    except Exception as e:
        logger.debug("Archive sync status check failed: %s", e)
    return None


def get_archive_purge_status() -> dict | None:
    """Fetch live archive purge worker status from the server."""
    try:
        result = subprocess.run(
            ['ssh', '-o', 'ConnectTimeout=3', '-o', 'BatchMode=yes',
             ARCHIVE_SSH_HOST,
             (
                 "docker exec routespark-order-forecast sh -lc "
                 "\"cat /app/logs/service_status.json 2>/dev/null; "
                 "printf '\\n---ROUTESPARK-SEP---\\n'; "
                 "tail -n 40 /app/logs/archive_purge_worker.log 2>/dev/null\""
             )],
            capture_output=True, text=True, timeout=8,
        )
        if result.returncode == 0 and result.stdout.strip():
            raw = result.stdout
            if "\n---ROUTESPARK-SEP---\n" not in raw:
                return None
            status_raw, log_raw = raw.split("\n---ROUTESPARK-SEP---\n", 1)
            service_status = json.loads(status_raw)
            services = service_status.get("services", [])
            purge_service = next(
                (svc for svc in services if svc.get("name") == ARCHIVE_PURGE_SERVICE_NAME),
                None,
            )
            last_log_line = ""
            for line in reversed(log_raw.splitlines()):
                if "[archive_purge_worker]" in line:
                    last_log_line = line.strip()
                    break
            state = "unknown"
            if "ARCHIVE_PURGE_ENABLED=false; purge cycle skipped" in last_log_line:
                state = "disabled"
            elif "purge cycle skipped" in last_log_line:
                state = "skipped"
            elif last_log_line:
                state = "active"
            return {
                "serviceTimestamp": service_status.get("timestamp"),
                "running": bool(purge_service and purge_service.get("running")),
                "state": state,
                "lastLogLine": last_log_line,
            }
    except Exception as e:
        logger.debug("Archive purge status check failed: %s", e)
    return None


def get_pcf_reconciler_status() -> dict | None:
    """Return local archive-triggered reconciler heartbeat state."""
    try:
        if not PCF_RECONCILER_HEARTBEAT_FILE.exists():
            return None
        data = json.loads(PCF_RECONCILER_HEARTBEAT_FILE.read_text())
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        logger.debug("PCF reconciler status check failed: %s", e)
    return None


def format_archive_timestamp(ts: str) -> str:
    """Format ISO timestamp for compact display."""
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        local_dt = parsed.astimezone()
        return local_dt.strftime("%b %d %H:%M")
    except Exception:
        return ts[:16] if ts else "?"


def extract_log_timestamp(line: str) -> str:
    """Extract timestamp prefix from local or server log lines."""
    if not line:
        return ""
    patterns = [
        (r"^[A-Z][a-z]{2} [A-Z][a-z]{2}\s+\d{1,2} \d\d:\d\d:\d\d [A-Z]{2,4} \d{4}", "%a %b %d %H:%M:%S %Z %Y"),
        (r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", "%Y-%m-%d %H:%M:%S"),
    ]
    for pattern, fmt in patterns:
        match = re.match(pattern, line)
        if not match:
            continue
        raw = match.group(0)
        try:
            parsed = datetime.strptime(raw, fmt)
            return parsed.strftime("%b %d %H:%M")
        except Exception:
            return raw
    return ""


def format_backup_timestamp(ts: str) -> str:
    """Format backup timestamp for compact display."""
    try:
        parsed = datetime.strptime(ts, "%a %b %d %H:%M:%S %Z %Y")
        return parsed.strftime("%b %d %H:%M")
    except Exception:
        parts = ts.split()
        if len(parts) >= 4:
            return " ".join(parts[1:4])
    return ts


def run_supervisor_command(cmd: str) -> str:
    """Run a supervisor command."""
    try:
        args = [str(VENV_PYTHON), str(SUPERVISOR_MAC), cmd]
        if cmd == 'start':
            args.append('-d')
        
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            cwd=str(APP_DIR),
            timeout=60,
        )
        return result.stdout + result.stderr
    except Exception as e:
        return f"Error: {e}"


class ServiceRow(QFrame):
    """A single service status row."""
    
    service_down = pyqtSignal(str)
    
    def __init__(self, name: str, pattern: str = None, is_server: bool = False, info_width: int = 50):
        super().__init__()
        self.name = name
        self.pattern = pattern
        self.is_server = is_server
        self.info_width = info_width
        self.is_running = False
        self.was_running = False
        self.first_check = True
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 3, 8, 3)
        layout.setSpacing(8)
        
        # Status indicator
        self.indicator = QLabel("●")
        self.indicator.setFixedWidth(14)
        self.indicator.setFont(QFont("Arial", 10))
        layout.addWidget(self.indicator)
        
        # Service name
        self.label = QLabel(name)
        self.label.setFont(QFont("SF Pro Display", 10))
        self.label.setStyleSheet(f"color: {COLORS['text']};")
        layout.addWidget(self.label, 1)
        
        # Info label (PID for Mac, status for server)
        self.info_label = QLabel("")
        self.info_label.setFont(QFont("SF Mono", 8))
        self.info_label.setStyleSheet(f"color: {COLORS['text_dim']};")
        self.info_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.info_label.setFixedWidth(self.info_width)
        layout.addWidget(self.info_label)
    
    def update_status(self, running: bool = None, info: str = "") -> bool:
        """Update the status display. Returns True if service went down."""
        self.was_running = self.is_running
        
        if running is not None:
            self.is_running = running
        elif self.pattern:
            self.is_running, pid = check_process_running(self.pattern)
            info = str(pid) if self.is_running else ""
        
        if self.is_running:
            self.indicator.setStyleSheet(f"color: {COLORS['green']};")
        else:
            self.indicator.setStyleSheet(f"color: {COLORS['red']};")
        
        self.info_label.setText(info)
        
        # Check if service went down
        went_down = self.was_running and not self.is_running and not self.first_check
        self.first_check = False
        
        if went_down:
            self.service_down.emit(self.name)
        
        return went_down


class RouteSparkWidget(QWidget):
    """Main desktop widget."""
    
    operation_complete = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        self.drag_position = None
        self.settings = load_settings()
        global logger
        logger = init_logger(self.settings)
        self.server_health = {'connected': False}
        self.server_monitor = MonitorState(service="server")
        self._server_probe_lock = Lock()
        self._server_pending_result = None
        self._server_probe_inflight = False
        self.last_notify_at = {}
        self.init_ui()
        logger.info(
            "Widget started (server_api_url=%s, refresh_ms=%s)",
            self.settings.get('server_api_url', SERVER_API_URL),
            self.settings.get('refresh_interval_ms', 5000),
        )
        
        self.operation_complete.connect(self._on_operation_complete)
        
        # Auto-refresh timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_status)
        refresh_ms = int(self.settings.get('refresh_interval_ms', 5000))
        self.timer.start(refresh_ms)
    
    def _on_operation_complete(self, operation: str):
        """Handle operation completion."""
        if operation == "start":
            self.start_btn.setText("▶ Start Mac")
            self.start_btn.setEnabled(True)
        elif operation == "stop":
            self.stop_btn.setText("⏹ Stop Mac")
            self.stop_btn.setEnabled(True)
        self.refresh_status()
    
    def _on_service_down(self, service_name: str):
        """Handle service going down."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        title = "RouteSpark Service Down"
        message = f"{service_name} stopped at {timestamp}"
        logger.warning("%s", message)
        if self._notification_allowed(service_name):
            send_notification(title, message, self.settings)
        else:
            logger.info("Suppressed %s alert (cooldown)", service_name)

    def _notification_allowed(self, service_name: str) -> bool:
        """Return True when a notification is outside the configured cooldown."""
        cooldown_minutes = int(self.settings.get('notification_cooldown_minutes', 30))
        last_ts = self.last_notify_at.get(service_name, 0)
        now_ts = time.time()
        if now_ts - last_ts >= cooldown_minutes * 60:
            self.last_notify_at[service_name] = now_ts
            return True
        return False

    def _probe_server_health(self) -> ProbeResult:
        """Return a timestamped server health probe result."""
        timeout_seconds = int(self.settings.get('server_check_timeout_seconds', 5))
        server_api_url = self.settings.get('server_api_url', SERVER_API_URL)
        started_at = time.time()
        details = check_server_health(server_api_url, timeout_seconds)
        latency_ms = int((time.time() - started_at) * 1000)
        return ProbeResult(
            service="server",
            ok=bool(details.get('connected', False)),
            checked_at=time.time(),
            latency_ms=latency_ms,
            status=details.get('status', 'offline'),
            error=details.get('error'),
            details=details,
        )

    def _launch_server_probe(self):
        """Start a server probe if one is not already in flight."""
        with self._server_probe_lock:
            if self._server_probe_inflight:
                return
            self._server_probe_inflight = True

        def check_server():
            result = self._probe_server_health()
            with self._server_probe_lock:
                self._server_pending_result = result
                self._server_probe_inflight = False

        Thread(target=check_server, daemon=True).start()

    def _consume_server_probe_result(self) -> ProbeResult | None:
        """Return the latest completed server probe result, if any."""
        with self._server_probe_lock:
            result = self._server_pending_result
            self._server_pending_result = None
            return result

    def _apply_server_probe_result(self, probe_result: ProbeResult) -> TransitionInfo:
        """Apply a fresh probe result to the server monitor state."""
        monitor = self.server_monitor
        old_status = monitor.status
        failure_threshold = int(self.settings.get('server_failure_threshold', 3))
        recovery_threshold = int(self.settings.get('server_recovery_threshold', 2))

        if probe_result.ok:
            monitor.consecutive_failures = 0
            if old_status == "down":
                monitor.consecutive_successes += 1
            else:
                monitor.consecutive_successes = 1
            if old_status == "down" and monitor.consecutive_successes >= recovery_threshold:
                monitor.status = "up"
            elif old_status == "retrying":
                monitor.status = "up"
            else:
                monitor.status = "up"
            monitor.last_error_logged = None
        else:
            monitor.consecutive_successes = 0
            monitor.consecutive_failures += 1
            if old_status == "up":
                monitor.status = "retrying"
            elif old_status == "retrying" and monitor.consecutive_failures >= failure_threshold:
                monitor.status = "down"
            else:
                monitor.status = old_status

        monitor.last_result = probe_result
        did_transition = monitor.status != old_status
        if did_transition:
            monitor.last_transition_at = probe_result.checked_at

        return TransitionInfo(
            old_status=old_status,
            new_status=monitor.status,
            did_transition=did_transition,
            should_notify_down=did_transition and old_status == "retrying" and monitor.status == "down",
            should_notify_recovery=(
                did_transition
                and old_status == "down"
                and monitor.status == "up"
                and bool(self.settings.get('notify_recovery', False))
            ),
        )

    def _handle_server_transition(self, transition: TransitionInfo):
        """Log and notify on server status transitions."""
        if not transition.did_transition:
            return

        logger.info("Server transition %s -> %s", transition.old_status, transition.new_status)

        if transition.should_notify_down:
            service_name = self.server_row.name
            timestamp = datetime.now().strftime('%H:%M:%S')
            title = "RouteSpark Service Down"
            message = f"{service_name} stopped at {timestamp}"
            logger.warning("%s", message)
            if self._notification_allowed(service_name):
                send_notification(title, message, self.settings)
            else:
                logger.info("Suppressed %s alert (cooldown)", service_name)

        if transition.should_notify_recovery:
            service_name = self.server_row.name
            if self._notification_allowed(service_name):
                send_notification("RouteSpark Service Up", "Server recovered", self.settings)
            else:
                logger.info("Suppressed %s recovery alert (cooldown)", service_name)

    def _render_server_row(self):
        """Render the server row from the current monitor state."""
        monitor = self.server_monitor
        result = monitor.last_result

        if result is None:
            self.server_row.update_status(running=True, info="...")
            self.server_row.indicator.setStyleSheet(f"color: {COLORS['yellow']};")
            self.server_status.setText("Checking...")
            self.server_status.setStyleSheet(f"color: {COLORS['yellow']};")
            self.server_stats.setText("")
            return

        if monitor.status == "down":
            self.server_row.update_status(running=False, info="DOWN")
            self.server_status.setText("Offline")
            self.server_status.setStyleSheet(f"color: {COLORS['red']};")
            self.server_stats.setText("")
            return

        if monitor.status == "retrying":
            self.server_row.update_status(running=True, info="RETRY")
            self.server_row.indicator.setStyleSheet(f"color: {COLORS['yellow']};")
            self.server_status.setText("Retrying...")
            self.server_status.setStyleSheet(f"color: {COLORS['yellow']};")
            self.server_stats.setText("")
            return

        self.server_row.update_status(running=True, info="OK")
        self.server_status.setText("Connected")
        self.server_status.setStyleSheet(f"color: {COLORS['green']};")
        self.server_stats.setText("Public health OK")

    def init_ui(self):
        """Initialize the UI."""
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Main container
        container = QFrame(self)
        container.setObjectName("container")
        container.setStyleSheet(f"""
            #container {{
                background-color: {COLORS['bg']};
                border-radius: 12px;
                border: 1px solid {COLORS['accent']};
            }}
        """)
        
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(20)
        shadow.setOffset(0, 4)
        shadow.setColor(Qt.GlobalColor.black)
        container.setGraphicsEffect(shadow)
        
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.addWidget(container)
        
        layout = QVBoxLayout(container)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(6)
        
        # Header
        header = QHBoxLayout()
        
        title = QLabel("⚡ RouteSpark")
        title.setFont(QFont("SF Pro Display", 13, QFont.Weight.Bold))
        title.setStyleSheet(f"color: {COLORS['text']};")
        header.addWidget(title)
        
        header.addStretch()
        
        # Close button
        close_btn = QPushButton("×")
        close_btn.setFixedSize(20, 20)
        close_btn.setFont(QFont("Arial", 14))
        close_btn.setStyleSheet(f"""
            QPushButton {{
                background: transparent;
                color: {COLORS['text_dim']};
                border: none;
            }}
            QPushButton:hover {{
                color: {COLORS['red']};
            }}
        """)
        close_btn.clicked.connect(self.close)
        header.addWidget(close_btn)
        
        layout.addLayout(header)
        
        # Server section header
        server_header = QHBoxLayout()
        server_label = QLabel("🖥️ Server")
        server_label.setFont(QFont("SF Pro Display", 10, QFont.Weight.Bold))
        server_label.setStyleSheet(f"color: {COLORS['blue']};")
        server_header.addWidget(server_label)
        
        self.server_status = QLabel("...")
        self.server_status.setFont(QFont("SF Mono", 9))
        self.server_status.setStyleSheet(f"color: {COLORS['text_dim']};")
        server_header.addWidget(self.server_status)
        server_header.addStretch()
        
        layout.addLayout(server_header)
        
        # Server info rows
        self.server_row = ServiceRow("API + PostgreSQL", is_server=True)
        layout.addWidget(self.server_row)

        self.firebase_row = ServiceRow("Firebase", is_server=True)
        layout.addWidget(self.firebase_row)

        self.backup_row = ServiceRow("Backup", is_server=True, info_width=120)
        layout.addWidget(self.backup_row)

        self.archive_sync_row = ServiceRow("Archive Sync", is_server=False, info_width=120)
        layout.addWidget(self.archive_sync_row)
        self.archive_purge_row = ServiceRow("Archive Purge", is_server=True, info_width=120)
        layout.addWidget(self.archive_purge_row)
        self._archive_sync_cache = None
        self._archive_sync_last_check = 0
        self._reconciler_cache = None
        self._reconciler_last_check = 0
        self._archive_purge_cache = None
        self._archive_purge_last_check = 0

        self.server_service_rows = {}
        for label in SERVER_SERVICE_MAP.keys():
            row = ServiceRow(label, is_server=True, info_width=90)
            self.server_service_rows[label] = row
            layout.addWidget(row)
        
        # Server stats
        self.server_stats = QLabel("")
        self.server_stats.setFont(QFont("SF Mono", 8))
        self.server_stats.setStyleSheet(f"color: {COLORS['text_dim']}; padding-left: 22px;")
        layout.addWidget(self.server_stats)

        
        # Divider
        divider = QFrame()
        divider.setFrameShape(QFrame.Shape.HLine)
        divider.setStyleSheet(f"background-color: {COLORS['accent']};")
        divider.setFixedHeight(1)
        layout.addWidget(divider)
        
        # Mac section header
        mac_header = QHBoxLayout()
        mac_label = QLabel("🍎 Mac-Only")
        mac_label.setFont(QFont("SF Pro Display", 10, QFont.Weight.Bold))
        mac_label.setStyleSheet(f"color: {COLORS['green']};")
        mac_header.addWidget(mac_label)
        
        self.mac_status = QLabel("")
        self.mac_status.setFont(QFont("SF Mono", 9))
        self.mac_status.setStyleSheet(f"color: {COLORS['text_dim']};")
        mac_header.addWidget(self.mac_status)
        mac_header.addStretch()
        
        layout.addLayout(mac_header)
        
        # Mac service rows
        self.mac_rows = []
        for name, pattern in MAC_SERVICES:
            row = ServiceRow(name, pattern)
            row.service_down.connect(self._on_service_down)
            self.mac_rows.append(row)
            layout.addWidget(row)

        self.reconciler_row = ServiceRow("PCF Reconciler", is_server=False, info_width=120)
        layout.addWidget(self.reconciler_row)
        
        # Divider
        divider2 = QFrame()
        divider2.setFrameShape(QFrame.Shape.HLine)
        divider2.setStyleSheet(f"background-color: {COLORS['accent']};")
        divider2.setFixedHeight(1)
        layout.addWidget(divider2)
        
        # Notification toggle
        notify_layout = QHBoxLayout()
        notify_layout.setContentsMargins(4, 4, 4, 0)
        
        self.notify_checkbox = QCheckBox("🔔 Notify on down")
        self.notify_checkbox.setFont(QFont("SF Pro Display", 9))
        self.notify_checkbox.setStyleSheet(f"color: {COLORS['text_dim']};")
        self.notify_checkbox.setChecked(self.settings.get('macos_notifications', True))
        self.notify_checkbox.stateChanged.connect(self._toggle_notifications)
        notify_layout.addWidget(self.notify_checkbox)
        
        layout.addLayout(notify_layout)
        
        # Control buttons
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(8)
        btn_layout.setContentsMargins(4, 4, 4, 4)
        
        self.start_btn = QPushButton("▶ Start Mac")
        self.start_btn.setFont(QFont("Arial", 10))
        self.start_btn.setStyleSheet(self._button_style(COLORS['green']))
        self.start_btn.clicked.connect(self.start_mac)
        btn_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton("⏹ Stop Mac")
        self.stop_btn.setFont(QFont("Arial", 10))
        self.stop_btn.setStyleSheet(self._button_style(COLORS['red']))
        self.stop_btn.clicked.connect(self.stop_mac)
        btn_layout.addWidget(self.stop_btn)
        
        self.logs_btn = QPushButton("📋 Logs")
        self.logs_btn.setFont(QFont("Arial", 10))
        self.logs_btn.setStyleSheet(self._button_style(COLORS['text']))
        self.logs_btn.clicked.connect(self.open_logs)
        btn_layout.addWidget(self.logs_btn)
        
        layout.addLayout(btn_layout)
        
        # Set size
        self.setFixedSize(320, 590)
        
        # Position in bottom-right corner
        screen = QApplication.primaryScreen().geometry()
        self.move(screen.width() - 340, screen.height() - 610)
        
        # Initial refresh
        self.refresh_status()
    
    def _button_style(self, color: str) -> str:
        return f"""
            QPushButton {{
                background-color: transparent;
                color: {color};
                border: 1px solid {color}66;
                border-radius: 6px;
                padding: 8px 12px;
            }}
            QPushButton:hover {{
                background-color: {color}22;
            }}
            QPushButton:pressed {{
                background-color: {color}33;
            }}
        """
    
    def refresh_status(self):
        """Refresh all service statuses."""
        latest_probe = self._consume_server_probe_result()
        if latest_probe is not None:
            self.server_health = latest_probe.details or {'connected': False}
            transition = self._apply_server_probe_result(latest_probe)
            if latest_probe.error and latest_probe.error != self.server_monitor.last_error_logged:
                logger.warning("Server health error: %s", latest_probe.error)
                self.server_monitor.last_error_logged = latest_probe.error
            self._handle_server_transition(transition)

        self._launch_server_probe()
        self._render_server_row()

        backup_ts = get_last_backup_success(BACKUP_LOG_FILE)
        if backup_ts:
            backup_info = format_backup_timestamp(backup_ts)
            self.backup_row.update_status(running=True, info=backup_info)
        else:
            self.backup_row.update_status(running=False, info="NONE")

        # Archive sync status (local log, check once per minute)
        now = time.time()
        if now - self._archive_sync_last_check >= 60:
            self._archive_sync_last_check = now
            self._archive_sync_cache = get_archive_sync_status()

        if self._archive_sync_cache:
            complete_line = self._archive_sync_cache.get("lastComplete", "")
            sync_info = extract_log_timestamp(complete_line)
            self.archive_sync_row.update_status(running=True, info=sync_info or "OK")
        else:
            self.archive_sync_row.update_status(running=False, info="NO DATA")

        # PCF fragmented-delivery reconciler heartbeat (local file, check once per minute)
        if now - self._reconciler_last_check >= 60:
            self._reconciler_last_check = now
            self._reconciler_cache = get_pcf_reconciler_status()

        if self._reconciler_cache:
            watcher = bool(self._reconciler_cache.get("watcher"))
            observer_alive = bool(self._reconciler_cache.get("observerAlive"))
            pending = int(self._reconciler_cache.get("pendingDeliveries", 0) or 0)
            last_seen = format_archive_timestamp(self._reconciler_cache.get("lastSeenAt", ""))
            if watcher and observer_alive:
                info = f"{last_seen} · {pending}" if last_seen else f"P{pending}"
                self.reconciler_row.update_status(running=True, info=info)
            else:
                self.reconciler_row.update_status(running=False, info="STALE")
        else:
            self.reconciler_row.update_status(running=False, info="NO DATA")

        # Archive purge status (SSH check once per hour, cached between)
        if now - self._archive_purge_last_check >= 3600:
            self._archive_purge_last_check = now
            def fetch_archive_purge():
                self._archive_purge_cache = get_archive_purge_status()
            Thread(target=fetch_archive_purge, daemon=True).start()

        if self._archive_purge_cache:
            purge_state = self._archive_purge_cache.get("state", "unknown")
            purge_running = bool(self._archive_purge_cache.get("running"))
            purge_line = self._archive_purge_cache.get("lastLogLine", "")
            purge_info = extract_log_timestamp(purge_line)
            if purge_state == "disabled":
                self.archive_purge_row.update_status(running=True, info=f"OFF {purge_info}".strip())
            elif purge_state == "skipped":
                self.archive_purge_row.update_status(running=True, info=f"SKIP {purge_info}".strip())
            elif purge_running:
                self.archive_purge_row.update_status(running=True, info=purge_info or "OK")
            else:
                self.archive_purge_row.update_status(running=False, info=purge_info or "DOWN")
        else:
            self.archive_purge_row.update_status(running=False, info="NO DATA")

        # Firebase health
        firebase_health = self.server_health.get('firebaseHealth', {})
        firebase_status = firebase_health.get('status', 'unknown')
        if firebase_status == 'healthy':
            self.firebase_row.update_status(running=True, info="OK")
        elif firebase_status == 'protected':
            self.firebase_row.update_status(running=True, info="AUTH")
        elif firebase_status == 'unhealthy':
            self.firebase_row.update_status(running=False, info="DOWN")
        else:
            self.firebase_row.update_status(running=False, info="UNKNOWN")

        # Server service heartbeats
        service_health = self.server_health.get('serviceHealth', {})
        service_status = service_health.get('status', 'unavailable')
        services = service_health.get('services', []) if isinstance(service_health, dict) else []
        services_by_name = {s.get('name'): s for s in services if isinstance(s, dict)}
        is_stale = service_status == 'stale'

        for label, service_name in SERVER_SERVICE_MAP.items():
            row = self.server_service_rows.get(label)
            svc = services_by_name.get(service_name) if services_by_name else None
            if service_status == 'protected':
                row.update_status(running=True, info="AUTH")
                continue
            if is_stale:
                row.update_status(running=False, info="STALE")
                continue
            if not svc:
                row.update_status(running=False, info="NONE")
                continue
            running = bool(svc.get('running', False))
            row.update_status(running=running, info="OK" if running else "DOWN")
        
        # Check Mac services
        mac_running = 0
        for row in self.mac_rows:
            was_first_check = row.first_check
            prev_running = row.is_running
            row.update_status()
            if row.is_running:
                mac_running += 1
            if prev_running and not row.is_running:
                logger.warning("Mac service down: %s", row.name)
            elif not prev_running and row.is_running and not was_first_check:
                logger.info("Mac service up: %s", row.name)
        
        self.mac_status.setText(f"{mac_running}/{len(self.mac_rows)}")
        if mac_running == len(self.mac_rows):
            self.mac_status.setStyleSheet(f"color: {COLORS['green']};")
        elif mac_running == 0:
            self.mac_status.setStyleSheet(f"color: {COLORS['red']};")
        else:
            self.mac_status.setStyleSheet(f"color: {COLORS['yellow']};")
    
    def start_mac(self):
        """Start Mac-only services."""
        self.start_btn.setText("Starting...")
        self.start_btn.setEnabled(False)
        
        def do_start():
            run_supervisor_command('start')
            self.operation_complete.emit("start")
        
        Thread(target=do_start, daemon=True).start()
    
    def stop_mac(self):
        """Stop Mac-only services."""
        self.stop_btn.setText("Stopping...")
        self.stop_btn.setEnabled(False)
        
        def do_stop():
            run_supervisor_command('stop')
            self.operation_complete.emit("stop")
        
        Thread(target=do_stop, daemon=True).start()
    
    def open_logs(self):
        """Open logs folder."""
        subprocess.run(['open', str(LOGS_DIR)])
    
    def _toggle_notifications(self, state: int):
        """Toggle notification setting."""
        self.settings['macos_notifications'] = state == 2
        save_settings(self.settings)
        
        if state == 2:
            send_macos_notification(
                "RouteSpark Notifications",
                "Notifications enabled"
            )
    
    def mousePressEvent(self, event: QMouseEvent):
        if event.button() == Qt.MouseButton.LeftButton:
            self.drag_position = event.globalPosition().toPoint() - self.frameGeometry().topLeft()
            event.accept()
    
    def mouseMoveEvent(self, event: QMouseEvent):
        if event.buttons() == Qt.MouseButton.LeftButton and self.drag_position:
            self.move(event.globalPosition().toPoint() - self.drag_position)
            event.accept()


def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(True)
    
    widget = RouteSparkWidget()
    widget.show()
    
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
