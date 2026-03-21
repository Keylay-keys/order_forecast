#!/usr/bin/env python3
"""Cluster-targeted RouteSpark desktop widget.

This is a sibling to the Dell-targeted widget. It exists so the Mac can talk to
the mini cluster without mutating the current production monitor path.

It prefers raw LAN for local speed and falls back to Tailscale when the LAN
path is unavailable.

It is intentionally not activated by default.
"""

from pathlib import Path
import json
import socket
import subprocess

import desktop_widget as base


CLUSTER_EDGE_LAN_SSH_HOST = "keylay@192.168.1.39"
CLUSTER_EDGE_TAILSCALE_SSH_HOST = "keylay@100.66.239.73"
CLUSTER_EDGE_LAN_API_URL = "http://192.168.1.39"
CLUSTER_EDGE_TAILSCALE_API_URL = "http://100.66.239.73"


base.SETTINGS_FILE = base.APP_DIR / ".widget_settings.cluster.json"
base.WIDGET_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-widget-cluster.log"
base.BACKUP_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-critical-backup-cluster.log"
base.ARCHIVE_SYNC_LOG_FILE = Path.home() / "Library" / "Logs" / "routespark-pcf-archive-sync-cluster.log"
base.ARCHIVE_SSH_HOST = CLUSTER_EDGE_LAN_SSH_HOST
base.SERVER_API_URL = CLUSTER_EDGE_LAN_API_URL
base.DEFAULT_SETTINGS = {
    **base.DEFAULT_SETTINGS,
    "server_api_url": CLUSTER_EDGE_LAN_API_URL,
}


def _host_port_reachable(host: str, port: int, timeout_seconds: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def _ordered_cluster_api_urls(primary_url: str) -> list[str]:
    if primary_url == CLUSTER_EDGE_TAILSCALE_API_URL:
        return [CLUSTER_EDGE_TAILSCALE_API_URL, CLUSTER_EDGE_LAN_API_URL]
    return [primary_url, CLUSTER_EDGE_TAILSCALE_API_URL]


def _ordered_cluster_ssh_hosts() -> list[str]:
    if _host_port_reachable("192.168.1.39", 22):
        return [CLUSTER_EDGE_LAN_SSH_HOST, CLUSTER_EDGE_TAILSCALE_SSH_HOST]
    return [CLUSTER_EDGE_TAILSCALE_SSH_HOST, CLUSTER_EDGE_LAN_SSH_HOST]


def check_cluster_server_health(server_api_url: str, timeout_seconds: int) -> dict:
    last_health = None
    for api_url in _ordered_cluster_api_urls(server_api_url):
        health = base.check_server_health(api_url, timeout_seconds)
        health["_sourceUrl"] = api_url
        if health.get("connected"):
            return health
        last_health = health
    return last_health or {
        "connected": False,
        "status": "offline",
        "error": "No cluster API targets available",
    }


def get_cluster_archive_purge_status() -> dict | None:
    """Return cluster worker deployment state in place of the Dell Docker check."""
    for ssh_host in _ordered_cluster_ssh_hosts():
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o",
                    "ConnectTimeout=3",
                    "-o",
                    "BatchMode=yes",
                    ssh_host,
                    (
                        "sudo k3s kubectl -n routespark-events "
                        "get deployment workers -o json"
                    ),
                ],
                capture_output=True,
                text=True,
                timeout=8,
            )
            if result.returncode != 0 or not result.stdout.strip():
                continue

            deployment = json.loads(result.stdout)
            spec = deployment.get("spec", {})
            status = deployment.get("status", {})
            desired = int(spec.get("replicas", 0) or 0)
            ready = int(status.get("readyReplicas", 0) or 0)
            available = int(status.get("availableReplicas", 0) or 0)
            updated = int(status.get("updatedReplicas", 0) or 0)

            if desired == 0:
                state = "disabled"
            elif ready >= 1 and available >= 1:
                state = "active"
            else:
                state = "starting"

            condition_ts = ""
            for condition in status.get("conditions", []):
                last_update = condition.get("lastUpdateTime") or condition.get("lastTransitionTime")
                if last_update:
                    condition_ts = last_update

            return {
                "serviceTimestamp": condition_ts,
                "running": ready >= 1,
                "state": state,
                "lastLogLine": (
                    f"{ssh_host} desired={desired} ready={ready} "
                    f"available={available} updated={updated}"
                ),
            }
        except Exception as exc:
            base.logger.debug("Cluster archive purge status check failed via %s: %s", ssh_host, exc)
    return None


base.check_server_health = check_cluster_server_health
base.get_archive_purge_status = get_cluster_archive_purge_status


if __name__ == "__main__":
    base.main()
