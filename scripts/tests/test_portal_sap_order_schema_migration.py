import os
from unittest.mock import patch

import pytest

from order_forecast.scripts import portal_sap_order_schema_migration as migration


def test_checksum_is_pinned_to_current_schema():
    assert migration.verify_checksum() == migration.expected_sha256()


def test_target_preflight_accepts_only_the_declared_web_api_database():
    environment = {
        "POSTGRES_HOST": "192.168.1.40",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "routespark",
        "WEB_API_POSTGRES_HOST": "192.168.1.40",
        "WEB_API_POSTGRES_PORT": "5432",
        "WEB_API_POSTGRES_DB": "routespark",
    }
    with patch.dict(os.environ, environment, clear=False):
        assert migration.verify_target() == {
            "host": "192.168.1.40",
            "port": "5432",
            "database": "routespark",
        }

    environment["WEB_API_POSTGRES_DB"] = "wrong-database"
    with patch.dict(os.environ, environment, clear=False):
        with pytest.raises(RuntimeError, match="does not match"):
            migration.verify_target()


def test_schema_contains_required_constraints_and_no_delete_grant():
    sql = migration.MIGRATION_PATH.read_text(encoding="utf-8")
    for constraint in migration.CONSTRAINTS - {"portal_sap_order_preferences_pkey"}:
        assert constraint in sql
    assert "PRIMARY KEY (firebase_uid, route_number)" in sql
    assert "REVOKE ALL ON TABLE portal_sap_order_preferences FROM PUBLIC" in sql
    assert "DELETE" not in sql
