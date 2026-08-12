#!/usr/bin/env python3
"""Runtime-specific supervisor for split RouteSpark Python runtimes."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


BASE_DIR = Path(__file__).parent
SCRIPTS_DIR = BASE_DIR / "scripts"
WORKERS_DIR = BASE_DIR.parent / "workers"
LOG_DIR = Path(os.environ.get("LOG_DIR", "/app/logs"))
STATUS_INTERVAL_SECONDS = int(os.environ.get("SERVICE_STATUS_INTERVAL_SECONDS", "30"))
SA_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/app/config/serviceAccountKey.json")
_shutdown_requested = False


@dataclass
class ServiceSpec:
    key: str
    name: str
    cmd: List[str]
    log_name: str


class Service:
    def __init__(self, spec: ServiceSpec, cwd: Path):
        self.spec = spec
        self.cwd = cwd
        self.log_file = LOG_DIR / spec.log_name
        self.process: Optional[subprocess.Popen] = None

    def is_running(self) -> bool:
        return self.process is not None and self.process.poll() is None

    def start(self) -> bool:
        if self.is_running():
            log(f"{self.spec.name}: already running (PID {self.process.pid})")
            return True

        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            log_handle = open(self.log_file, "a")
            log_handle.write(f"\n{'=' * 60}\n")
            log_handle.write(f"Service started at {datetime.now()}\n")
            log_handle.write(f"{'=' * 60}\n")
            log_handle.flush()
            self.process = subprocess.Popen(
                self.spec.cmd,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                cwd=str(self.cwd),
            )
            time.sleep(0.5)
            if self.process.poll() is None:
                log(f"{self.spec.name}: started (PID {self.process.pid})")
                return True
            log(f"{self.spec.name}: failed to start (exit code {self.process.returncode})")
            return False
        except Exception as exc:
            log(f"{self.spec.name}: error starting - {exc}")
            return False

    def stop(self) -> None:
        if not self.is_running():
            return
        try:
            self.process.terminate()
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def make_specs(python: str) -> Dict[str, List[ServiceSpec]]:
    return {
        "order_forecast": [
            ServiceSpec(
                key="retrain",
                name="Retrain Daemon",
                cmd=[
                    python,
                    str(SCRIPTS_DIR / "retrain_daemon.py"),
                    "--service-account",
                    SA_PATH,
                    "--interval",
                    os.environ.get("RETRAIN_INTERVAL_SECONDS", "86400"),
                ],
                log_name="retrain_daemon.log",
            ),
            ServiceSpec(
                key="forecast_generation",
                name="Forecast Generation Worker",
                cmd=[
                    python,
                    str(SCRIPTS_DIR / "forecast_generation_worker.py"),
                    "--serviceAccount",
                    SA_PATH,
                ],
                log_name="forecast_generation_worker.log",
            ),
        ],
        "listeners": [
            ServiceSpec(
                key="order_sync",
                name="Order Sync Listener",
                cmd=[python, str(SCRIPTS_DIR / "order_sync_listener.py"), "--serviceAccount", SA_PATH],
                log_name="order_sync.log",
            ),
            ServiceSpec(
                key="archive_listener",
                name="Archive Listener",
                cmd=[python, str(SCRIPTS_DIR / "order_archive_listener.py"), "--serviceAccount", SA_PATH],
                log_name="archive_listener.log",
            ),
            ServiceSpec(
                key="delivery_manifest",
                name="Delivery Manifest Listener",
                cmd=[python, str(SCRIPTS_DIR / "delivery_manifest_listener.py"), "--serviceAccount", SA_PATH],
                log_name="delivery_listener.log",
            ),
            ServiceSpec(
                key="config_sync",
                name="Config Sync Listener",
                cmd=[python, str(SCRIPTS_DIR / "config_sync_listener.py"), "--serviceAccount", SA_PATH],
                log_name="config_sync.log",
            ),
            ServiceSpec(
                key="promo_sync",
                name="Promo Sync Listener",
                cmd=[python, str(SCRIPTS_DIR / "promo_sync_listener.py"), "--serviceAccount", SA_PATH],
                log_name="promo_sync.log",
            ),
            ServiceSpec(
                key="route_transfer_sync",
                name="Route Transfer Sync Listener",
                cmd=[python, str(SCRIPTS_DIR / "route_transfer_sync_listener.py"), "--serviceAccount", SA_PATH],
                log_name="route_transfers.log",
            ),
        ],
        "workers": [
            ServiceSpec(
                key="archive_export",
                name="Archive Export Worker",
                cmd=[python, str(SCRIPTS_DIR / "archive_export_worker.py"), "--serviceAccount", SA_PATH],
                log_name="archive_export_worker.log",
            ),
            ServiceSpec(
                key="archive_purge",
                name="Archive Purge Worker",
                cmd=[python, str(SCRIPTS_DIR / "archive_purge_worker.py"), "--serviceAccount", SA_PATH],
                log_name="archive_purge_worker.log",
            ),
            ServiceSpec(
                key="pos_archive",
                name="POS Archive Worker",
                cmd=[python, str(WORKERS_DIR / "archive" / "pos_archive_worker.py"), "--serviceAccount", SA_PATH],
                log_name="pos_archive_worker.log",
            ),
            ServiceSpec(
                key="low_qty_notifications",
                name="Low-Qty Notification Daemon",
                cmd=[python, str(SCRIPTS_DIR / "low_qty_notification_daemon.py"), "--serviceAccount", SA_PATH],
                log_name="low_qty_notifications.log",
            ),
        ],
    }


def _enabled_keys_for_runtime(runtime: str) -> Optional[set[str]]:
    env_name = f"ROUTESPARK_ENABLED_{runtime.upper()}"
    raw = os.environ.get(env_name, "")
    if not raw.strip():
        return None

    values = {
        item.strip().lower()
        for item in raw.split(",")
        if item.strip()
    }
    return values or set()


def write_status(runtime: str, services: List[Service]) -> None:
    payload = {
        "runtime": runtime,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "services": [{"name": s.spec.name, "running": s.is_running()} for s in services],
    }
    tmp_path = LOG_DIR / f"{runtime}_status.tmp"
    out_path = LOG_DIR / f"{runtime}_status.json"
    tmp_path.write_text(json.dumps(payload, indent=2))
    tmp_path.replace(out_path)


def handle_shutdown(signum, frame) -> None:
    del signum, frame
    global _shutdown_requested
    _shutdown_requested = True
    log("shutdown requested")


def run_runtime(runtime: str) -> int:
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    specs = make_specs(sys.executable).get(runtime)
    if not specs:
        log(f"unknown runtime group: {runtime}")
        return 2

    enabled_keys = _enabled_keys_for_runtime(runtime)
    if enabled_keys is not None:
        specs = [spec for spec in specs if spec.key in enabled_keys]
        log(f"{runtime}: filtered services -> {[spec.key for spec in specs]}")
        if not specs:
            log(f"{runtime}: no services enabled after filtering")
            return 2

    services = [Service(spec, BASE_DIR) for spec in specs]
    all_started = True
    for idx, service in enumerate(services):
        all_started = service.start() and all_started
        if idx < len(services) - 1:
            time.sleep(2)

    last_status_write = 0.0
    while not _shutdown_requested:
        time.sleep(10)
        if time.time() - last_status_write >= STATUS_INTERVAL_SECONDS:
            write_status(runtime, services)
            last_status_write = time.time()
        for service in services:
            if not service.is_running() and not _shutdown_requested:
                log(f"{service.spec.name}: died, restarting")
                service.start()

    for service in services:
        service.stop()
    write_status(runtime, services)
    return 0 if all_started else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Supervisor for split RouteSpark runtimes")
    parser.add_argument("runtime", choices=["order_forecast", "listeners", "workers"])
    args = parser.parse_args()
    return run_runtime(args.runtime)


if __name__ == "__main__":
    raise SystemExit(main())
