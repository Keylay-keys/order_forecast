#!/usr/bin/env python3
"""Cluster-targeted RouteSpark desktop widget.

This is a sibling to the Dell-targeted widget. It exists so the Mac can talk to
the mini cluster without mutating the current production monitor path.

The cluster widget uses two sources of truth:
- public API health from the live cluster edge
- live deployment/CronJob state from `k3s kubectl` over SSH

It is intentionally not activated by default.
"""

from pathlib import Path
import json
import socket
import subprocess
import time

import desktop_widget as base


CLUSTER_EDGE_LAN_SSH_HOST = "keylay@192.168.1.39"
CLUSTER_EDGE_TAILSCALE_SSH_HOST = "keylay@100.66.239.73"
CLUSTER_DATA_LAN_SSH_HOST = "keylay@192.168.1.40"
CLUSTER_DATA_TAILSCALE_SSH_HOST = "keylay@100.72.199.115"
CLUSTER_PUBLIC_API_URL = "https://api.routespark.pro"
LEGACY_CLUSTER_API_URLS = {
    "http://192.168.1.39",
    "http://100.66.239.73",
}

CLUSTER_SNAPSHOT_TTL_SECONDS = 10
BACKUP_TIMESTAMP_TTL_SECONDS = 60
ARCHIVE_FRESHNESS_TTL_SECONDS = 60

_original_check_server_health = base.check_server_health
_cluster_snapshot_cache: dict | None = None
_cluster_snapshot_checked_at = 0.0
_backup_timestamp_cache: str | None = None
_backup_timestamp_checked_at = 0.0
_archive_freshness_cache: str | None = None
_archive_freshness_checked_at = 0.0


base.SETTINGS_FILE = base.APP_DIR / ".widget_settings.cluster.json"
base.WIDGET_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-widget-cluster.log"
base.BACKUP_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-critical-backup-cluster.log"
base.ARCHIVE_SYNC_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-pcf-archive-sync-cluster.log"
base.ARCHIVE_SSH_HOST = CLUSTER_EDGE_LAN_SSH_HOST
base.SERVER_API_URL = CLUSTER_PUBLIC_API_URL
base.SERVER_SERVICE_MAP = {
    "Order Sync": "Order Sync Listener",
    "Forecast": "Order Forecast",
    "Delivery": "Delivery Manifest Listener",
    "Config Sync": "Config Sync Listener",
    "Promo Sync": "Promo Sync Listener",
    "Route Xfer": "Route Transfer Sync Listener",
    "Export": "Archive Export Worker",
}
base.DEFAULT_SETTINGS = {
    **base.DEFAULT_SETTINGS,
    "server_api_url": CLUSTER_PUBLIC_API_URL,
}


def _host_port_reachable(host: str, port: int, timeout_seconds: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _ordered_cluster_ssh_hosts() -> list[str]:
    if _host_port_reachable("192.168.1.39", 22):
        return [CLUSTER_EDGE_LAN_SSH_HOST, CLUSTER_EDGE_TAILSCALE_SSH_HOST]
    return [CLUSTER_EDGE_TAILSCALE_SSH_HOST, CLUSTER_EDGE_LAN_SSH_HOST]


def _ordered_cluster_data_ssh_hosts() -> list[str]:
    if _host_port_reachable("192.168.1.40", 22):
        return [CLUSTER_DATA_LAN_SSH_HOST, CLUSTER_DATA_TAILSCALE_SSH_HOST]
    return [CLUSTER_DATA_TAILSCALE_SSH_HOST, CLUSTER_DATA_LAN_SSH_HOST]


def _fetch_cluster_snapshot(force: bool = False) -> dict | None:
    global _cluster_snapshot_cache, _cluster_snapshot_checked_at

    now = time.time()
    if (
        not force
        and _cluster_snapshot_cache is not None
        and (now - _cluster_snapshot_checked_at) < CLUSTER_SNAPSHOT_TTL_SECONDS
    ):
        return _cluster_snapshot_cache

    ssh_hosts = _ordered_cluster_ssh_hosts()
    remote_script = r"""
python3 - <<'PY'
import json
import subprocess

def run_json(args):
    return json.loads(subprocess.check_output(args).decode())

snapshot = {
    "deployments": run_json(["k3s", "kubectl", "get", "deploy", "-A", "-o", "json"]),
    "cronjobs": run_json(["k3s", "kubectl", "get", "cronjob", "-A", "-o", "json"]),
}
print(json.dumps(snapshot))
PY
""".strip()

    for ssh_host in ssh_hosts:
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "ConnectTimeout=3",
                    "-o", "BatchMode=yes",
                    ssh_host,
                    remote_script,
                ],
                capture_output=True,
                text=True,
                timeout=12,
            )
            if result.returncode != 0 or not result.stdout.strip():
                continue
            snapshot = json.loads(result.stdout)
            snapshot["_ssh_host"] = ssh_host
            snapshot["_checked_at"] = now
            _cluster_snapshot_cache = snapshot
            _cluster_snapshot_checked_at = now
            return snapshot
        except Exception as exc:
            base.logger.debug("Cluster snapshot fetch failed via %s: %s", ssh_host, exc)
    return _cluster_snapshot_cache


def _find_deployment(snapshot: dict | None, namespace: str, name: str) -> dict | None:
    if not snapshot:
        return None
    for item in snapshot.get("deployments", {}).get("items", []):
        metadata = item.get("metadata", {})
        if metadata.get("namespace") == namespace and metadata.get("name") == name:
            return item
    return None


def _find_cronjob(snapshot: dict | None, namespace: str, name: str) -> dict | None:
    if not snapshot:
        return None
    for item in snapshot.get("cronjobs", {}).get("items", []):
        metadata = item.get("metadata", {})
        if metadata.get("namespace") == namespace and metadata.get("name") == name:
            return item
    return None


def _deployment_running(snapshot: dict | None, namespace: str, name: str) -> tuple[bool, str]:
    deploy = _find_deployment(snapshot, namespace, name)
    if not deploy:
        return False, "NONE"

    spec = deploy.get("spec", {})
    status = deploy.get("status", {})
    desired = int(spec.get("replicas", 0) or 0)
    available = int(status.get("availableReplicas", 0) or 0)

    if desired <= 0:
        return False, "0/0"
    return available >= desired, f"{available}/{desired}"


def _cronjob_state(snapshot: dict | None, namespace: str, name: str) -> tuple[bool, str]:
    cronjob = _find_cronjob(snapshot, namespace, name)
    if not cronjob:
        return False, "NONE"

    spec = cronjob.get("spec", {})
    status = cronjob.get("status", {})
    suspended = bool(spec.get("suspend", False))
    active = len(status.get("active", []))
    last_successful = status.get("lastSuccessfulTime", "")
    last_schedule = status.get("lastScheduleTime", "")

    if suspended:
        return False, "OFF"
    if active > 0:
        return True, "RUN"
    if last_successful or last_schedule:
        return True, "OK"
    return False, "IDLE"


def _get_cluster_backup_success() -> str | None:
    global _backup_timestamp_cache, _backup_timestamp_checked_at

    now = time.time()
    if (now - _backup_timestamp_checked_at) < BACKUP_TIMESTAMP_TTL_SECONDS:
        return _backup_timestamp_cache

    remote_script = (
        "python3 - <<'PY'\n"
        "from pathlib import Path\n"
        "from datetime import datetime, timezone\n"
        "latest = Path('/srv/routespark/backups/postgres/routespark_latest.dump')\n"
        "if not latest.exists():\n"
        "    raise SystemExit(1)\n"
        "ts = datetime.fromtimestamp(latest.stat().st_mtime, tz=timezone.utc)\n"
        "print(ts.strftime('%a %b %d %H:%M:%S UTC %Y'))\n"
        "PY"
    )

    for ssh_host in _ordered_cluster_data_ssh_hosts():
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "ConnectTimeout=3",
                    "-o", "BatchMode=yes",
                    ssh_host,
                    remote_script,
                ],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if result.returncode != 0 or not result.stdout.strip():
                continue
            _backup_timestamp_cache = result.stdout.strip()
            _backup_timestamp_checked_at = now
            return _backup_timestamp_cache
        except Exception as exc:
            base.logger.debug("Cluster backup timestamp check failed via %s: %s", ssh_host, exc)
    return _backup_timestamp_cache


def _get_cluster_archive_freshness() -> str | None:
    global _archive_freshness_cache, _archive_freshness_checked_at

    now = time.time()
    if (now - _archive_freshness_checked_at) < ARCHIVE_FRESHNESS_TTL_SECONDS:
        return _archive_freshness_cache

    remote_script = (
        "python3 - <<'PY'\n"
        "from datetime import datetime, timezone\n"
        "from pathlib import Path\n"
        "root = Path('/mnt/archive/pcf/pcf_archive')\n"
        "if not root.exists():\n"
        "    raise SystemExit(1)\n"
        "latest_mtime = root.stat().st_mtime\n"
        "for path in root.rglob('*'):\n"
        "    try:\n"
        "        latest_mtime = max(latest_mtime, path.stat().st_mtime)\n"
        "    except OSError:\n"
        "        pass\n"
        "ts = datetime.fromtimestamp(latest_mtime, tz=timezone.utc)\n"
        "print(ts.isoformat().replace('+00:00', 'Z'))\n"
        "PY"
    )

    for ssh_host in _ordered_cluster_data_ssh_hosts():
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o", "ConnectTimeout=3",
                    "-o", "BatchMode=yes",
                    ssh_host,
                    remote_script,
                ],
                capture_output=True,
                text=True,
                timeout=20,
            )
            if result.returncode != 0 or not result.stdout.strip():
                continue
            _archive_freshness_cache = result.stdout.strip()
            _archive_freshness_checked_at = now
            return _archive_freshness_cache
        except Exception as exc:
            base.logger.debug("Cluster archive freshness check failed via %s: %s", ssh_host, exc)
    return _archive_freshness_cache


def _cronjob_info(snapshot: dict | None, namespace: str, name: str) -> tuple[bool, str]:
    cronjob = _find_cronjob(snapshot, namespace, name)
    if not cronjob:
        return False, "NONE"

    spec = cronjob.get("spec", {})
    status = cronjob.get("status", {})
    if bool(spec.get("suspend", False)):
        return False, "OFF"

    active = len(status.get("active", []))
    if active > 0:
        return True, "RUN"

    last_successful = status.get("lastSuccessfulTime", "")
    if last_successful:
        return True, base.format_archive_timestamp(last_successful)

    last_schedule = status.get("lastScheduleTime", "")
    if last_schedule:
        return True, f"SCH {base.format_archive_timestamp(last_schedule)}"

    return False, "IDLE"


def check_cluster_server_health(server_api_url: str, timeout_seconds: int) -> dict:
    api_url = server_api_url or CLUSTER_PUBLIC_API_URL
    health = _original_check_server_health(api_url, timeout_seconds)
    health["_sourceUrl"] = api_url

    snapshot = _fetch_cluster_snapshot()
    health["_clusterSnapshot"] = snapshot

    firebase_running, firebase_info = _deployment_running(snapshot, "routespark-admin", "firebase-tools")
    health["firebaseHealth"] = {
        "status": "healthy" if firebase_running else "unhealthy",
        "info": firebase_info,
    }

    service_specs = [
        ("Order Sync Listener", "routespark-events", "order-sync-listener"),
        ("Order Forecast", "routespark-forecast", "order-forecast"),
        ("Delivery Manifest Listener", "routespark-events", "delivery-manifest-listener"),
        ("Config Sync Listener", "routespark-events", "config-sync-listener"),
        ("Promo Sync Listener", "routespark-events", "promo-sync-listener"),
        ("Route Transfer Sync Listener", "routespark-events", "route-transfer-sync-listener"),
        ("Archive Export Worker", "routespark-events", "archive-export-worker"),
    ]
    services = []
    for display_name, namespace, deploy_name in service_specs:
        running, info = _deployment_running(snapshot, namespace, deploy_name)
        services.append({
            "name": display_name,
            "running": running,
            "info": info,
        })

    health["serviceHealth"] = {
        "status": "healthy" if snapshot else "unavailable",
        "services": services,
    }
    return health


def get_cluster_archive_purge_status() -> dict | None:
    """Return cluster archive-purge CronJob state."""
    snapshot = _fetch_cluster_snapshot()
    cj = _find_cronjob(snapshot, "routespark-events", "archive-purge")
    if cj:
        spec = cj.get("spec", {})
        cj_status = cj.get("status", {})
        suspended = bool(spec.get("suspend", False))
        active_jobs = len(cj_status.get("active", []))
        last_schedule = cj_status.get("lastScheduleTime", "")
        last_successful = cj_status.get("lastSuccessfulTime", "")

        if suspended:
            state = "disabled"
        elif active_jobs > 0:
            state = "active"
        elif last_successful:
            state = "active"
        else:
            state = "skipped"

        ts = last_successful or last_schedule or ""

        return {
            "serviceTimestamp": ts,
            "running": not suspended,
            "state": state,
            "lastLogLine": (
                f"suspended={suspended} active={active_jobs} "
                f"lastSuccess={last_successful or 'none'}"
            ),
        }
    for ssh_host in _ordered_cluster_ssh_hosts():
        base.logger.debug("Cluster archive purge status not found via cached snapshot; last host=%s", ssh_host)
        break
    return None


base.check_server_health = check_cluster_server_health
base.get_archive_purge_status = get_cluster_archive_purge_status
base.get_last_backup_success = lambda _log_path: _get_cluster_backup_success()


class ClusterWidget(base.RouteSparkWidget):
    """RouteSpark widget bound to live cluster state."""

    def __init__(self):
        super().__init__()
        if self.settings.get("server_api_url") in LEGACY_CLUSTER_API_URLS:
            self.settings["server_api_url"] = CLUSTER_PUBLIC_API_URL
            base.save_settings(self.settings)
        self.archive_sync_row.name = "Archive Fresh"
        self.archive_sync_row.label.setText("Archive Fresh")
        self.reconciler_row.name = "Low-Qty"
        self.reconciler_row.label.setText("Low-Qty")

    def refresh_status(self):
        super().refresh_status()

        snapshot = self.server_health.get("_clusterSnapshot") or _fetch_cluster_snapshot()
        if snapshot:
            snapshot_host = snapshot.get("_ssh_host", "").split("@")[-1] if snapshot.get("_ssh_host") else "cluster"
            self.server_stats.setText(
                f"Cluster OK via {self.server_health.get('_sourceUrl', CLUSTER_PUBLIC_API_URL)} · {snapshot_host}"
            )
        else:
            self.server_stats.setText("Cluster snapshot unavailable")

        firebase_running, firebase_info = _deployment_running(snapshot, "routespark-admin", "firebase-tools")
        self.firebase_row.update_status(running=firebase_running, info=firebase_info)

        backup_ts = _get_cluster_backup_success()
        if backup_ts:
            self.backup_row.update_status(running=True, info=base.format_backup_timestamp(backup_ts))
        else:
            self.backup_row.update_status(running=False, info="NONE")

        archive_ts = _get_cluster_archive_freshness()
        if archive_ts:
            self.archive_sync_row.update_status(running=True, info=base.format_archive_timestamp(archive_ts))
        else:
            self.archive_sync_row.update_status(running=False, info="NONE")

        purge_running, purge_info = _cronjob_info(snapshot, "routespark-events", "archive-purge")
        self.archive_purge_row.update_status(running=purge_running, info=purge_info)

        low_qty_running, low_qty_info = _cronjob_info(snapshot, "routespark-events", "low-qty-notifications")
        self.reconciler_row.update_status(running=low_qty_running, info=low_qty_info)

        service_specs = {
            "Order Sync": ("routespark-events", "order-sync-listener"),
            "Forecast": ("routespark-forecast", "order-forecast"),
            "Delivery": ("routespark-events", "delivery-manifest-listener"),
            "Config Sync": ("routespark-events", "config-sync-listener"),
            "Promo Sync": ("routespark-events", "promo-sync-listener"),
            "Route Xfer": ("routespark-events", "route-transfer-sync-listener"),
            "Export": ("routespark-events", "archive-export-worker"),
        }
        for label, row in self.server_service_rows.items():
            namespace, deploy_name = service_specs[label]
            running, info = _deployment_running(snapshot, namespace, deploy_name)
            row.update_status(running=running, info=info)


base.RouteSparkWidget = ClusterWidget


if __name__ == "__main__":
    base.main()
