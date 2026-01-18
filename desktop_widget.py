#!/usr/bin/env python3
"""RouteSpark Desktop Widget - Floating status panel

A small, always-on-top widget showing service status with controls.
Now includes notifications when services go down.

Usage:
    python desktop_widget.py
"""

import subprocess
import sys
import json
from datetime import datetime
from pathlib import Path
from threading import Thread

from PyQt6.QtCore import Qt, QTimer, QPoint, pyqtSignal
from PyQt6.QtGui import QFont, QMouseEvent
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QPushButton, QFrame, QGraphicsDropShadowEffect,
    QCheckBox
)

# Paths
APP_DIR = Path(__file__).parent
SUPERVISOR = APP_DIR / 'supervisor.py'
LOGS_DIR = APP_DIR / 'logs'
VENV_PYTHON = APP_DIR / 'venv' / 'bin' / 'python'
SETTINGS_FILE = APP_DIR / '.widget_settings.json'

# Services to monitor (updated to match supervisor)
SERVICES = [
    ('DB Manager', 'db_manager.py'),
    ('Order Sync', 'order_sync_listener.py'),
    ('Archive', 'order_archive_listener.py'),
    ('Retrain', 'retrain_daemon.py'),
    ('Delivery', 'delivery_manifest_listener.py'),
    ('Promo Email', 'promo_email_listener.py'),
    ('Catalog Upload', 'catalog_upload_listener.py'),
    ('Low-Qty Notif', 'low_qty_notification_daemon.py'),
    ('Web Portal API', 'uvicorn api.main:app'),
    ('PCF OCR', 'pcf_core.runner'),
]


def send_macos_notification(title: str, message: str, sound: bool = True):
    """Send macOS desktop notification (appears in notification center)."""
    try:
        sound_cmd = 'with sound "Blow"' if sound else ''
        script = f'''
        display notification "{message}" with title "{title}" {sound_cmd}
        '''
        subprocess.run(['osascript', '-e', script], capture_output=True)
    except Exception as e:
        print(f"macOS notification error: {e}")


def send_ntfy_notification(title: str, message: str, settings: dict):
    """Send push notification via ntfy.sh"""
    import urllib.request
    import urllib.error
    
    ntfy_config = settings.get('ntfy', {})
    if not ntfy_config.get('enabled'):
        return
    
    topic = ntfy_config.get('topic', '')
    if not topic:
        print("ntfy topic not configured - skipping")
        return
    
    try:
        url = f"https://ntfy.sh/{topic}"
        data = message.encode('utf-8')
        req = urllib.request.Request(url, data=data, method='POST')
        req.add_header('Title', title)
        req.add_header('Priority', 'high')
        req.add_header('Tags', 'warning,rotating_light')
        
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status == 200:
                print(f"ntfy notification sent to {topic}")
            else:
                print(f"ntfy error: {response.status}")
    except urllib.error.URLError as e:
        print(f"ntfy notification error: {e}")
    except Exception as e:
        print(f"ntfy notification error: {e}")


def send_notification(title: str, message: str, settings: dict):
    """Send notifications via all enabled channels."""
    # Always send macOS notification if enabled
    if settings.get('macos_notifications', True):
        send_macos_notification(title, message)
    
    # Send ntfy push notification if configured
    if settings.get('ntfy', {}).get('enabled'):
        send_ntfy_notification(title, message, settings)


def load_settings() -> dict:
    """Load widget settings."""
    try:
        if SETTINGS_FILE.exists():
            return json.loads(SETTINGS_FILE.read_text())
    except Exception:
        pass
    return {
        'macos_notifications': True,
        'ntfy': {
            'enabled': True,
            'topic': 'routespark-down-detection',
        }
    }


def save_settings(settings: dict):
    """Save widget settings."""
    try:
        SETTINGS_FILE.write_text(json.dumps(settings, indent=2))
    except Exception as e:
        print(f"Settings save error: {e}")

# Colors
COLORS = {
    'bg': '#1a1a2e',
    'header': '#16213e',
    'accent': '#0f3460',
    'green': '#00d26a',
    'red': '#ff4757',
    'yellow': '#ffa502',
    'text': '#eaeaea',
    'text_dim': '#888888',
}


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


def run_supervisor_command(cmd: str) -> str:
    """Run a supervisor command."""
    try:
        # Only add -d flag for start command (daemonize)
        args = [str(VENV_PYTHON), str(SUPERVISOR), cmd]
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
    
    # Signal emitted when service goes down: (service_name, was_running)
    service_down = pyqtSignal(str)
    
    def __init__(self, name: str, pattern: str):
        super().__init__()
        self.name = name
        self.pattern = pattern
        self.is_running = False
        self.was_running = False  # Track previous state for notifications
        self.first_check = True   # Don't notify on initial load
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 3, 8, 3)
        layout.setSpacing(8)
        
        # Status indicator (colored dot)
        self.indicator = QLabel("●")
        self.indicator.setFixedWidth(14)
        self.indicator.setFont(QFont("Arial", 10))
        layout.addWidget(self.indicator)
        
        # Service name
        self.label = QLabel(name)
        self.label.setFont(QFont("SF Pro Display", 10))
        self.label.setStyleSheet(f"color: {COLORS['text']};")
        layout.addWidget(self.label, 1)
        
        # PID label
        self.pid_label = QLabel("")
        self.pid_label.setFont(QFont("SF Mono", 8))
        self.pid_label.setStyleSheet(f"color: {COLORS['text_dim']};")
        self.pid_label.setFixedWidth(45)
        layout.addWidget(self.pid_label)
        
        self.update_status()
    
    def update_status(self) -> bool:
        """Update the status display. Returns True if service went down."""
        self.was_running = self.is_running
        self.is_running, pid = check_process_running(self.pattern)
        
        if self.is_running:
            self.indicator.setStyleSheet(f"color: {COLORS['green']};")
            self.pid_label.setText(str(pid))
        else:
            self.indicator.setStyleSheet(f"color: {COLORS['red']};")
            self.pid_label.setText("")
        
        # Check if service went down (was running, now not)
        went_down = self.was_running and not self.is_running and not self.first_check
        self.first_check = False
        
        if went_down:
            self.service_down.emit(self.name)
        
        return went_down


class RouteSparkWidget(QWidget):
    """Main desktop widget."""
    
    # Signals for thread-safe UI updates
    operation_complete = pyqtSignal(str)  # Signal when start/stop completes
    
    def __init__(self):
        super().__init__()
        self.drag_position = None
        self.settings = load_settings()
        self.init_ui()
        
        # Connect signal to handler
        self.operation_complete.connect(self._on_operation_complete)
        
        # Auto-refresh timer
        self.timer = QTimer()
        self.timer.timeout.connect(self.refresh_status)
        self.timer.start(5000)  # Refresh every 5 seconds
    
    def _on_operation_complete(self, operation: str):
        """Handle operation completion on main thread."""
        if operation == "start":
            self.start_btn.setText("▶ Start")
            self.start_btn.setEnabled(True)
        elif operation == "stop":
            self.stop_btn.setText("⏹ Stop")
            self.stop_btn.setEnabled(True)
        self.refresh_status()
    
    def _on_service_down(self, service_name: str):
        """Handle service going down - send notifications."""
        timestamp = datetime.now().strftime('%H:%M:%S')
        title = "RouteSpark Service Down"
        message = f"{service_name} stopped at {timestamp}"
        
        print(f"🚨 {message}")
        send_notification(title, message, self.settings)
    
    def init_ui(self):
        """Initialize the UI."""
        # Window flags: frameless (no longer always on top - can go behind other apps)
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Main container with rounded corners
        container = QFrame(self)
        container.setObjectName("container")
        container.setStyleSheet(f"""
            #container {{
                background-color: {COLORS['bg']};
                border-radius: 12px;
                border: 1px solid {COLORS['accent']};
            }}
        """)
        
        # Add drop shadow
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
        layout.setSpacing(8)
        
        # Header
        header = QHBoxLayout()
        
        title = QLabel("⚡ RouteSpark")
        title.setFont(QFont("SF Pro Display", 13, QFont.Weight.Bold))
        title.setStyleSheet(f"color: {COLORS['text']};")
        header.addWidget(title)
        
        header.addStretch()
        
        # Status summary
        self.status_label = QLabel(f"0/{len(SERVICES)}")
        self.status_label.setFont(QFont("SF Mono", 10))
        self.status_label.setStyleSheet(f"color: {COLORS['green']};")
        header.addWidget(self.status_label)
        
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
        
        # Divider
        divider = QFrame()
        divider.setFrameShape(QFrame.Shape.HLine)
        divider.setStyleSheet(f"background-color: {COLORS['accent']};")
        divider.setFixedHeight(1)
        layout.addWidget(divider)
        
        # Service rows
        self.service_rows = []
        for name, pattern in SERVICES:
            row = ServiceRow(name, pattern)
            row.service_down.connect(self._on_service_down)  # Connect notification
            self.service_rows.append(row)
            layout.addWidget(row)
        
        # Divider
        divider2 = QFrame()
        divider2.setFrameShape(QFrame.Shape.HLine)
        divider2.setStyleSheet(f"background-color: {COLORS['accent']};")
        divider2.setFixedHeight(1)
        layout.addWidget(divider2)
        
        # Notification toggle
        notify_layout = QHBoxLayout()
        notify_layout.setContentsMargins(4, 4, 4, 0)
        
        self.notify_checkbox = QCheckBox("🔔 Notify on service down")
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
        
        self.start_btn = QPushButton("▶ Start")
        self.start_btn.setFont(QFont("Arial", 10))
        self.start_btn.setStyleSheet(self._button_style(COLORS['green']))
        self.start_btn.clicked.connect(self.start_all)
        btn_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton("⏹ Stop")
        self.stop_btn.setFont(QFont("Arial", 10))
        self.stop_btn.setStyleSheet(self._button_style(COLORS['red']))
        self.stop_btn.clicked.connect(self.stop_all)
        btn_layout.addWidget(self.stop_btn)
        
        self.logs_btn = QPushButton("📋 Logs")
        self.logs_btn.setFont(QFont("Arial", 10))
        self.logs_btn.setStyleSheet(self._button_style(COLORS['text']))
        self.logs_btn.clicked.connect(self.open_logs)
        btn_layout.addWidget(self.logs_btn)
        
        layout.addLayout(btn_layout)
        
        # Set fixed size (taller for more services)
        self.setFixedSize(280, 455)
        
        # Position in bottom-right corner
        screen = QApplication.primaryScreen().geometry()
        self.move(screen.width() - 300, screen.height() - 515)
        
        # Initial status update
        self.refresh_status()
    
    def _button_style(self, color: str) -> str:
        """Generate button stylesheet."""
        return f"""
            QPushButton {{
                background-color: transparent;
                color: {color};
                border: 1px solid {color}66;
                border-radius: 6px;
                padding: 10px 14px;
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
        running = 0
        for row in self.service_rows:
            row.update_status()
            if row.is_running:
                running += 1
        
        total = len(self.service_rows)
        self.status_label.setText(f"{running}/{total}")
        
        if running == total:
            self.status_label.setStyleSheet(f"color: {COLORS['green']};")
        elif running == 0:
            self.status_label.setStyleSheet(f"color: {COLORS['red']};")
        else:
            self.status_label.setStyleSheet(f"color: {COLORS['yellow']};")
    
    def start_all(self):
        """Start all services."""
        self.start_btn.setText("Starting...")
        self.start_btn.setEnabled(False)
        
        def do_start():
            run_supervisor_command('start')
            self.operation_complete.emit("start")
        
        Thread(target=do_start, daemon=True).start()
    
    def stop_all(self):
        """Stop all services."""
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
        self.settings['macos_notifications'] = state == 2  # Qt.CheckState.Checked = 2
        save_settings(self.settings)
        
        # Send test notification when enabled
        if state == 2:
            send_macos_notification(
                "RouteSpark Notifications",
                "Notifications enabled - you'll be alerted if services go down"
            )
    
    # Dragging support
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

