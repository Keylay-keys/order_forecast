import inspect
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from order_forecast.api.routers import transfers
from order_forecast.api.route_ownership import extract_owned_routes_for_owner
from order_forecast.api.tests.route_transfer_fakes import FakeFirestore


FIXTURE = json.loads(
    (
        Path(__file__).parents[2]
        / "contracts"
        / "route-transfers"
        / "legacy-owner.json"
    ).read_text()
)


def _request(path):
    return SimpleNamespace(
        state=SimpleNamespace(),
        url=SimpleNamespace(path=path),
        method="GET",
        client=SimpleNamespace(host="127.0.0.1"),
        headers={},
    )


def _transfer_path(key=None):
    base = f"routeTransfers/{FIXTURE['routeGroupId']}/transfers"
    return f"{base}/{key}" if key else base


class LegacyRouteTransferContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.owner = FIXTURE["ownerData"]
        self.token = {"uid": FIXTURE["ownerUid"]}
        self.access_patch = patch.object(
            transfers,
            "require_route_access",
            new=AsyncMock(return_value=self.owner),
        )
        self.access_patch.start()
        self.addCleanup(self.access_patch.stop)

    def _db(self, transfer=None):
        docs = {}
        if transfer is not None:
            docs[_transfer_path(FIXTURE["createRequest"]["transferKey"])] = transfer
        return FakeFirestore(docs, delete_field=transfers.firestore.DELETE_FIELD)

    async def _call(self, endpoint, **kwargs):
        with patch.object(transfers.firestore, "transactional", side_effect=lambda fn: fn):
            return await inspect.unwrap(endpoint)(**kwargs)

    def test_owner_route_extraction_preserves_legacy_contract(self):
        self.assertEqual(
            extract_owned_routes_for_owner(self.owner),
            sorted([FIXTURE["routeGroupId"], FIXTURE["secondaryRoute"]], key=int),
        )
        with self.assertRaises(HTTPException) as context:
            transfers._require_owner_master_route({"profile": {"role": "team_member"}})
        self.assertEqual(context.exception.status_code, 403)

    async def test_ledger_response_preserves_owner_fields_and_totals(self):
        db = self._db(FIXTURE["transfer"])
        result = await self._call(
            transfers.get_transfer_ledger,
            request=_request("/api/transfers/ledger"),
            route_group_id=FIXTURE["routeGroupId"],
            limit=200,
            decoded_token=self.token,
            db=db,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(result["transfers"]), 1)
        row = result["transfers"][0]
        self.assertEqual(row["reservedBy"], {"consumer-order": 4})
        self.assertEqual(row["reservedTotal"], 4)
        self.assertEqual(row["availableUnits"], 8)

    async def test_history_reads_firestore_and_reports_truncation(self):
        today = datetime.now(timezone.utc).date()
        newer_date = today.isoformat()
        older_date = (today - timedelta(days=1)).isoformat()
        old_outside_range = (today - timedelta(days=31)).isoformat()
        db = FakeFirestore({
            _transfer_path("newer"): {
                **FIXTURE["transfer"],
                "transferDate": newer_date,
                "createdAt": 2000,
            },
            _transfer_path("probe"): {
                **FIXTURE["transfer"],
                "transferDate": older_date,
                "createdAt": 1000,
            },
            _transfer_path("too-old"): {
                **FIXTURE["transfer"],
                "transferDate": old_outside_range,
                "createdAt": 3000,
            },
        })

        result = await self._call(
            transfers.list_transfers,
            request=_request("/api/transfers"),
            route_group_id=FIXTURE["routeGroupId"],
            route=None,
            days=30,
            limit=1,
            decoded_token=self.token,
            db=db,
        )

        self.assertEqual(len(result["transfers"]), 1)
        self.assertEqual(result["transfers"][0]["transfer_id"], "newer")
        self.assertEqual(result["transfers"][0]["transfer_date"], newer_date)
        self.assertEqual(result["transfers"][0]["route_group_id"], FIXTURE["routeGroupId"])
        self.assertEqual(result["transfers"][0]["from_route_number"], FIXTURE["routeGroupId"])
        self.assertTrue(result["hasMore"])

    async def test_history_route_filter_keeps_only_matching_transfers(self):
        transfer_date = datetime.now(timezone.utc).date().isoformat()
        db = FakeFirestore({
            _transfer_path("matching"): {
                **FIXTURE["transfer"],
                "transferDate": transfer_date,
            },
            _transfer_path("other-route"): {
                **FIXTURE["transfer"],
                "toRouteNumber": "777777",
                "transferDate": transfer_date,
            },
        })

        result = await self._call(
            transfers.list_transfers,
            request=_request("/api/transfers"),
            route_group_id=FIXTURE["routeGroupId"],
            route=FIXTURE["secondaryRoute"],
            days=30,
            limit=500,
            decoded_token=self.token,
            db=db,
        )

        self.assertEqual(
            [row["transfer_id"] for row in result["transfers"]],
            ["matching"],
        )
        self.assertFalse(result["hasMore"])

    async def test_history_applies_30_90_and_180_day_cutoffs(self):
        today = datetime.now(timezone.utc).date()
        docs = {}
        for age in (10, 45, 100, 170, 181):
            docs[_transfer_path(f"age-{age}")] = {
                **FIXTURE["transfer"],
                "transferDate": (today - timedelta(days=age)).isoformat(),
            }
        db = FakeFirestore(docs)

        expected_counts = {30: 1, 90: 2, 180: 4}
        for days, expected in expected_counts.items():
            with self.subTest(days=days):
                result = await self._call(
                    transfers.list_transfers,
                    request=_request("/api/transfers"),
                    route_group_id=FIXTURE["routeGroupId"],
                    route=None,
                    days=days,
                    limit=500,
                    decoded_token=self.token,
                    db=db,
                )
                self.assertEqual(len(result["transfers"]), expected)

    async def test_history_remains_owner_only_and_primary_group_scoped(self):
        with patch.object(
            transfers,
            "require_route_access",
            new=AsyncMock(return_value={"profile": {"role": "team_member"}}),
        ):
            with self.assertRaises(HTTPException) as team_member:
                await self._call(
                    transfers.list_transfers,
                    request=_request("/api/transfers"),
                    route_group_id=FIXTURE["routeGroupId"],
                    route=None,
                    days=30,
                    limit=500,
                    decoded_token=self.token,
                    db=self._db(),
                )
        self.assertEqual(team_member.exception.status_code, 403)

        with self.assertRaises(HTTPException) as secondary_group:
            await self._call(
                transfers.list_transfers,
                request=_request("/api/transfers"),
                route_group_id=FIXTURE["secondaryRoute"],
                route=None,
                days=30,
                limit=500,
                decoded_token=self.token,
                db=self._db(),
            )
        self.assertEqual(secondary_group.exception.status_code, 403)

    async def test_create_update_and_delete_keep_legacy_shapes(self):
        db = self._db()
        payload = transfers.TransferCreateRequest(**FIXTURE["createRequest"])
        common = {
            "request": _request("/api/transfers/create"),
            "decoded_token": self.token,
            "db": db,
        }

        created = await self._call(transfers.create_transfer, payload=payload, **common)
        self.assertEqual(
            created,
            {"ok": True, "transferId": payload.transferKey, "created": True},
        )

        updated = await self._call(
            transfers.create_transfer,
            payload=payload.model_copy(update={"units": 10}),
            **common,
        )
        self.assertEqual(updated["created"], False)
        self.assertEqual(db.get_document(_transfer_path(payload.transferKey))["units"], 10)

        deleted = await self._call(
            transfers.create_transfer,
            payload=payload.model_copy(update={"units": 0}),
            **common,
        )
        self.assertEqual(deleted["deleted"], True)
        self.assertIsNone(db.get_document(_transfer_path(payload.transferKey)))

    async def test_create_rejects_unowned_routes_and_reserved_reductions(self):
        payload = transfers.TransferCreateRequest(**FIXTURE["createRequest"])
        common = {
            "request": _request("/api/transfers/create"),
            "decoded_token": self.token,
        }

        with self.assertRaises(HTTPException) as unowned:
            await self._call(
                transfers.create_transfer,
                payload=payload.model_copy(update={"toRouteNumber": "777777"}),
                db=self._db(),
                **common,
            )
        self.assertEqual(unowned.exception.status_code, 403)

        existing = {**FIXTURE["transfer"], "reservedBy": {"consumer-order": 9}}
        with self.assertRaises(HTTPException) as conflict:
            await self._call(
                transfers.create_transfer,
                payload=payload.model_copy(update={"units": 8}),
                db=self._db(existing),
                **common,
            )
        self.assertEqual(conflict.exception.status_code, 409)

    async def test_reserve_release_and_conflict_keep_legacy_shapes(self):
        existing = {**FIXTURE["transfer"], "reservedBy": {"consumer-order": 4}}
        db = self._db(existing)
        payload = transfers.TransferReserveRequest(**FIXTURE["reserveRequest"])
        common = {
            "request": _request("/api/transfers/reserve"),
            "decoded_token": self.token,
            "db": db,
        }

        reserved = await self._call(transfers.reserve_transfer, payload=payload, **common)
        self.assertEqual(reserved, {"ok": True, "reservedTotal": 7, "availableUnits": 5})

        released = await self._call(
            transfers.reserve_transfer,
            payload=payload.model_copy(update={"units": 0}),
            **common,
        )
        self.assertEqual(released, {"ok": True, "reservedTotal": 4, "availableUnits": 8})

        with self.assertRaises(HTTPException) as conflict:
            await self._call(
                transfers.reserve_transfer,
                payload=payload.model_copy(update={"units": 9}),
                **common,
            )
        self.assertEqual(conflict.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
