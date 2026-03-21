#!/usr/bin/env python3
"""Cluster-targeted wrapper for Mac-only RouteSpark services.

This keeps the current Dell-targeted supervisor untouched while giving the Mac
an alternate archive destination for cluster validation.
"""

import socket

import supervisor_mac_only as base


LAN_ARCHIVE_REMOTE = "keylay@192.168.1.40:/mnt/archive/pcf/pcf_archive"
TAILSCALE_ARCHIVE_REMOTE = "keylay@100.72.199.115:/mnt/archive/pcf/pcf_archive"


def _host_port_reachable(host: str, port: int, timeout_seconds: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


base.PCF_ARCHIVE_REMOTE = (
    LAN_ARCHIVE_REMOTE
    if _host_port_reachable("192.168.1.40", 22)
    else TAILSCALE_ARCHIVE_REMOTE
)


if __name__ == "__main__":
    raise SystemExit(base.main())
