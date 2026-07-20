#!/usr/bin/env python3
"""Probe Firestore -> Postgres product catalog sync for config-sync-listener.

Intended to run from the config-sync-listener pod or an equivalent environment
with Firebase Admin credentials and POSTGRES_* env vars.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Optional

import firebase_admin
import psycopg2
from firebase_admin import credentials, firestore


DEFAULT_SERVICE_ACCOUNT = "/app/config/serviceAccountKey.json"
DEFAULT_ROUTE = "900000"
DEFAULT_SAP = "999998"
DEFAULT_UPC = "12345-67890"


def _init_firestore(service_account: str) -> firestore.Client:
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account)
        firebase_admin.initialize_app(cred)
    return firestore.client()


def _pg_connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def _fetch_pg_product(route: str, sap: str) -> Optional[tuple[Any, ...]]:
    with _pg_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sap, route_number, full_name, upc, is_active
                FROM product_catalog
                WHERE route_number = %s AND sap = %s
                """,
                (route, sap),
            )
            return cur.fetchone()


def _wait_for_pg_state(route: str, sap: str, expected_upc: str, expected_active: bool, timeout: int) -> tuple[Any, ...]:
    deadline = time.monotonic() + timeout
    last_row: Optional[tuple[Any, ...]] = None

    while time.monotonic() < deadline:
        row = _fetch_pg_product(route, sap)
        last_row = row
        if row and row[3] == expected_upc and row[4] is expected_active:
            return row
        time.sleep(1)

    raise TimeoutError(
        f"Timed out waiting for route={route} sap={sap} upc={expected_upc!r} "
        f"active={expected_active}; last_row={last_row!r}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--route", default=DEFAULT_ROUTE)
    parser.add_argument("--sap", default=DEFAULT_SAP)
    parser.add_argument("--upc", default=DEFAULT_UPC)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--service-account", default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", DEFAULT_SERVICE_ACCOUNT))
    args = parser.parse_args()

    db = _init_firestore(args.service_account)
    product_ref = db.collection("masterCatalog").document(args.route).collection("products").document(args.sap)

    payload = {
        "sap": args.sap,
        "fullName": "Phase 0 Sync Probe",
        "brand": "RouteSpark",
        "category": "Test",
        "casePack": 1,
        "displayOrder": 999998,
        "upc": args.upc,
        "active": False,
    }

    print(f"Writing probe product masterCatalog/{args.route}/products/{args.sap}")
    product_ref.set(payload, merge=True)
    row = _wait_for_pg_state(args.route, args.sap, args.upc, False, args.timeout)
    print(f"Postgres write verified: {row}")

    print("Deleting probe product from Firestore")
    product_ref.delete()
    row = _wait_for_pg_state(args.route, args.sap, args.upc, False, args.timeout)
    print(f"Postgres cleanup verified inactive: {row}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
