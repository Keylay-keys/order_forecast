"""Guard the PostgreSQL-only runtime boundary.

DuckDB is retained only for isolated offline audit warehouses. This test keeps
it from silently returning to order, forecast, API, listener, or supervisor
runtime code.
"""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

ALLOWED_DUCKDB_IMPORTS = {
    Path("scripts/build_mailbox_archive_warehouse.py"),
    Path("scripts/build_promo_audit_warehouse.py"),
    Path("scripts/deep_promo_anomaly_report.py"),
}

RETIRED_RUNTIME_FILES = {
    Path("check_db.py"),
    Path("fix_store_ids.py"),
    Path("scripts/calendar_features.py"),
    Path("scripts/compute_shares.py"),
    Path("scripts/db_client.py"),
    Path("scripts/db_manager.py"),
    Path("scripts/db_schema.py"),
    Path("scripts/db_sync.py"),
    Path("scripts/feedback_collector.py"),
    Path("scripts/load_json_orders.py"),
    Path("scripts/migrate_duckdb_to_postgres.py"),
    Path("scripts/retrain_scheduler.py"),
    Path("scripts/test_db_queries.py"),
}

ACTIVE_ENTRYPOINTS = {
    Path("supervisor.py"),
    Path("supervisor_docker.py"),
    Path("supervisor_mac_only.py"),
    Path("menubar_app.py"),
    Path("start_archive_listener.sh"),
    Path("scripts/order_archive_listener.py"),
    Path("scripts/order_sync_listener.py"),
    Path("api/dependencies.py"),
    Path("api/main.py"),
}


def _duckdb_imports(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(alias.name == "duckdb" for alias in node.names):
                return True
        elif isinstance(node, ast.ImportFrom) and node.module == "duckdb":
            return True
    return False


def test_duckdb_imports_are_limited_to_offline_audit_tools() -> None:
    actual = {
        path.relative_to(ROOT)
        for path in ROOT.rglob("*.py")
        if not any(part in {".git", ".venv", "venv", "node_modules"} for part in path.parts)
        and _duckdb_imports(path)
    }

    assert actual == ALLOWED_DUCKDB_IMPORTS


def test_retired_duckdb_runtime_files_stay_removed() -> None:
    existing = sorted(str(path) for path in RETIRED_RUNTIME_FILES if (ROOT / path).exists())

    assert existing == []


def test_active_entrypoints_expose_no_duckdb_wiring() -> None:
    forbidden = ("--duckdb", "db_manager.py", "get_duckdb")
    violations: list[str] = []

    for relative_path in ACTIVE_ENTRYPOINTS:
        text = (ROOT / relative_path).read_text(encoding="utf-8").lower()
        for token in forbidden:
            if token in text:
                violations.append(f"{relative_path}: {token}")

    assert violations == []


def test_runtime_requirements_do_not_install_duckdb() -> None:
    installed_packages = {
        line.split("#", 1)[0].strip().lower()
        for line in (ROOT / "requirements_docker.txt").read_text(encoding="utf-8").splitlines()
        if line.split("#", 1)[0].strip()
    }

    assert not any(package.startswith("duckdb") for package in installed_packages)
