import inspect
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

from order_forecast.api.routers import transfers
from order_forecast.api.route_ownership import extract_owned_routes_for_owner
from order_forecast.api.tests.route_transfer_fakes import FakeFirestore


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "route-transfer-legacy-owner.json").read_text()
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
