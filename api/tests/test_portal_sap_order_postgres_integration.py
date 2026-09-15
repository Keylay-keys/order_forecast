"""Opt-in PostgreSQL integration coverage for Portal SAP-order CAS writes."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest

from order_forecast.api.portal_sap_order_store import (
    PortalSapOrderConflict,
    read_portal_sap_order,
    save_portal_sap_order,
)


TEST_DSN = os.environ.get("TEST_PORTAL_SAP_ORDER_POSTGRES_DSN", "").strip()
TEST_UID = "portal-sap-order-postgres-integration"
TEST_ROUTE = "988200"


pytestmark = pytest.mark.skipif(
    not TEST_DSN,
    reason="TEST_PORTAL_SAP_ORDER_POSTGRES_DSN is not set",
)


def connect():
    return psycopg2.connect(
        TEST_DSN,
        options="-c statement_timeout=5000 -c lock_timeout=2000",
    )


def acquire():
    return connect()


def release(conn):
    conn.close()


@pytest.fixture(autouse=True)
def exact_row_cleanup():
    def cleanup():
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM portal_sap_order_preferences WHERE firebase_uid = %s AND route_number = %s",
                    (TEST_UID, TEST_ROUTE),
                )
            conn.commit()
        finally:
            conn.close()

    cleanup()
    yield
    cleanup()


def test_committed_readback_reset_and_concurrent_cas_conflict():
    created = save_portal_sap_order(
        TEST_UID,
        TEST_ROUTE,
        ["200", "100"],
        0,
        acquire=acquire,
        release=release,
    )
    assert created.revision == 1
    assert read_portal_sap_order(
        TEST_UID,
        TEST_ROUTE,
        acquire=acquire,
        release=release,
    ).preference == created

    def competing_write(sap: str):
        try:
            return save_portal_sap_order(
                TEST_UID,
                TEST_ROUTE,
                [sap],
                1,
                acquire=acquire,
                release=release,
            )
        except PortalSapOrderConflict as conflict:
            return conflict

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(competing_write, ("300", "400")))

    successes = [result for result in results if not isinstance(result, PortalSapOrderConflict)]
    conflicts = [result for result in results if isinstance(result, PortalSapOrderConflict)]
    assert len(successes) == 1
    assert len(conflicts) == 1
    assert successes[0].revision == 2
    assert conflicts[0].current.revision == 2

    reset = save_portal_sap_order(
        TEST_UID,
        TEST_ROUTE,
        [],
        2,
        acquire=acquire,
        release=release,
    )
    assert reset.revision == 3
    assert reset.sap_order == []
