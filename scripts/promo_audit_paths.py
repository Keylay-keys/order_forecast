"""Path helpers for promo-audit artifacts.

Prefers the external drive when mounted so large audit outputs do not consume
internal disk space. Falls back to the repo workspace when the drive is not
available.
"""

from __future__ import annotations

from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_OUTPUT_ROOT = WORKSPACE_ROOT / "output"
EXTERNAL_AUDIT_ROOT = Path("/Volumes/Extreme SSD/routespark_promo_audit")


def _workspace_tag() -> str:
    return WORKSPACE_ROOT.name


def preferred_output_root() -> Path:
    if EXTERNAL_AUDIT_ROOT.exists():
        return EXTERNAL_AUDIT_ROOT / _workspace_tag()
    return WORKSPACE_OUTPUT_ROOT


def default_audit_dir(route: str) -> Path:
    return preferred_output_root() / f"promo_audit_{route}"


def default_tracking_dir(route: str) -> Path:
    return preferred_output_root() / f"promo_tracking_{route}"


def default_warehouse_db(route: str) -> Path:
    return default_tracking_dir(route) / "promo_audit.duckdb"
