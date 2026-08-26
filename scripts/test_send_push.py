#!/usr/bin/env python3
"""Explicit operator-only Expo push check.

This module is safe to import. A real push requires both ``--send-real-push``
and ``ROUTESPARK_ALLOW_REAL_PUSH_TEST=1``. It must never be collected or used
as part of an automated test suite.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any


__test__ = False
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
REAL_PUSH_ENV = "ROUTESPARK_ALLOW_REAL_PUSH_TEST"


def _payload(token: str, route: str) -> dict[str, Any]:
    return {
        "to": token,
        "title": "🧪 Test: Low Stock Alert",
        "body": "This is a test notification from the daemon",
        "data": {
            "type": "low_quantity",
            "routeNumber": route,
            "orderDate": "2026-01-16",
            "saps": ["TEST-SAP-001"],
        },
        "sound": "default",
        "priority": "high",
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send one explicitly authorized real Expo test push"
    )
    parser.add_argument("--service-account", required=True)
    parser.add_argument("--route", required=True)
    parser.add_argument(
        "--send-real-push",
        action="store_true",
        help="Required acknowledgement that this contacts a real device",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if not args.send_real_push or os.environ.get(REAL_PUSH_ENV) != "1":
        raise RuntimeError(
            "real push denied: require --send-real-push and "
            f"{REAL_PUSH_ENV}=1"
        )
    if not str(args.route).isdigit() or len(str(args.route)) > 10:
        raise RuntimeError("invalid route")

    service_account = Path(args.service_account).expanduser().resolve(strict=True)

    # Network-capable imports and clients are deliberately inside the gated
    # main path so pytest/import discovery cannot contact Firebase or Expo.
    import requests
    from google.cloud import firestore

    db = firestore.Client.from_service_account_json(str(service_account))
    route_doc = db.collection("routes").document(str(args.route)).get()
    if not route_doc.exists:
        raise RuntimeError("route not found")
    owner_id = str((route_doc.to_dict() or {}).get("ownerUid") or "").strip()
    if not owner_id:
        raise RuntimeError("route owner not found")

    user_doc = db.collection("users").document(owner_id).get()
    tokens = list((user_doc.to_dict() or {}).get("fcmTokens") or [])
    if not tokens:
        raise RuntimeError("route owner has no push token")

    response = requests.post(
        EXPO_PUSH_URL,
        json=[_payload(str(tokens[-1]), str(args.route))],
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    result = response.json()
    receipt = result.get("data") if isinstance(result, dict) else None
    if not isinstance(receipt, list) or not receipt or receipt[0].get("status") != "ok":
        raise RuntimeError("Expo did not accept the test push")
    print("Real test push accepted by Expo")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
