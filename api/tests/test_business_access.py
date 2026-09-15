import inspect
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api.business_access import require_business_access
from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.routers import business, metrics
from order_forecast.api.tests.route_transfer_fakes import FakeFirestore


def fixture():
    return FakeFirestore({
        "accounting_pilots/owner": {"schemaVersion": 1, "enabled": True, "routes": ["989262"]},
        "users/owner": {"profile": {"role": "owner", "routeNumber": "989262"}},
        "routes/989262": {"ownerUid": "owner"},
        "routeNumbers/989262": {"userId": "owner"},
    })


class BusinessAccessTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_invitation_and_both_canonical_anchors_required(self):
        db = fixture()
        self.assertIsNotNone(await require_business_access("989262", {"uid": "owner"}, db))
        for path, data in [
            ("accounting_pilots/owner", {"schemaVersion": 1, "enabled": False, "routes": ["989262"]}),
            ("users/owner", {"profile": {"role": "teamMember", "routeNumber": "989262"}}),
            ("users/owner", {"profile": {"role": "owner", "routeNumber": "123456"}}),
            ("routes/989262", {"ownerUid": "new-owner"}),
            ("routeNumbers/989262", {"userId": "new-owner"}),
            ("routes/989262", {}),
            ("routeNumbers/989262", {}),
        ]:
            with self.subTest(path=path, data=data):
                db = fixture()
                db.set_document(path, data)
                with self.assertRaises(HTTPException) as error:
                    await require_business_access("989262", {"uid": "owner"}, db)
                self.assertEqual(error.exception.status_code, 403)

    async def test_other_invited_owner_cannot_read_kyles_route(self):
        db = fixture()
        db.set_document("users/tester", {"profile": {"role": "owner", "routeNumber": "123456"}})
        db.set_document("accounting_pilots/tester", {"schemaVersion": 1, "enabled": True, "routes": ["123456", "989262"]})
        db.set_document("routes/123456", {"ownerUid": "tester"})
        db.set_document("routeNumbers/123456", {"userId": "tester"})
        await require_business_access("123456", {"uid": "tester"}, db)
        with self.assertRaises(HTTPException) as error:
            await require_business_access("989262", {"uid": "tester"}, db)
        self.assertEqual(error.exception.status_code, 403)

    async def test_malformed_missing_and_client_forged_grants_fail_closed(self):
        for data in [{}, {"enabled": True, "routes": ["989262"]},
                     {"schemaVersion": True, "enabled": True, "routes": ["989262"]},
                     {"schemaVersion": 1, "enabled": "true", "routes": ["989262"]},
                     {"schemaVersion": 1, "enabled": True, "routes": "989262"},
                     {"schemaVersion": 1, "enabled": True, "routes": [989262]},
                     {"schemaVersion": 1, "enabled": True, "routes": ["*"]}]:
            with self.subTest(data=data):
                db = fixture()
                db.set_document("accounting_pilots/owner", data)
                with self.assertRaises(HTTPException) as error:
                    await require_business_access("989262", {"uid": "owner", "accountingBeta": True}, db)
                self.assertEqual(error.exception.status_code, 403)

    async def test_revocation_applies_to_next_request(self):
        db = fixture()
        await require_business_access("989262", {"uid": "owner"}, db)
        db.update_document("accounting_pilots/owner", {"enabled": False})
        with self.assertRaises(HTTPException) as error:
            await require_business_access("989262", {"uid": "owner"}, db)
        self.assertEqual(error.exception.status_code, 403)

    async def test_outage_does_not_become_empty_success_or_leak_details(self):
        db = fixture()
        with patch.object(db, "collection", side_effect=RuntimeError("private database details")):
            with self.assertRaises(HTTPException) as error:
                await require_business_access("989262", {"uid": "owner"}, db)
        self.assertEqual(error.exception.status_code, 503)
        self.assertNotIn("private database", error.exception.detail)

    async def test_invalid_identity_and_route(self):
        for route, token, expected in [("../989262", {"uid": "owner"}, 400), ("989262", {}, 401), ("989262", {"uid": "a/b"}, 401)]:
            with self.assertRaises(HTTPException) as error:
                await require_business_access(route, token, fixture())
            self.assertEqual(error.exception.status_code, expected)

    def test_direct_http_requires_auth_and_denies_uninvited_owner(self):
        app = FastAPI()
        app.include_router(business.router, prefix="/api")
        app.include_router(metrics.router, prefix="/api")
        db = fixture()
        app.dependency_overrides[get_firestore] = lambda: db
        with TestClient(app) as client:
            self.assertEqual(client.get("/api/business/access?route=989262").status_code, 401)
            app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "owner"}
            response = client.get("/api/business/access?route=989262")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), {"uid": "owner", "routeNumber": "989262", "enabled": True})
            db.set_document("accounting_pilots/owner", {})
            for path in ["business/access", "metrics/settlements", "metrics/expenses", "metrics/payroll"]:
                self.assertEqual(client.get(f"/api/{path}?route=989262").status_code, 403)
            self.assertEqual(client.delete("/api/metrics/expenses/example?route=989262").status_code, 403)
            self.assertEqual(client.post("/api/metrics/expenses", json={"routeNumber": "989262", "date": "2026-09-08", "category": "fuel", "amount": "1.00"}).status_code, 403)

    def test_every_legacy_metrics_handler_retains_gate(self):
        # Guard the legacy API source too, although it is not deployed in this slice.
        for route in metrics.router.routes:
            source = inspect.getsource(inspect.unwrap(route.endpoint))
            self.assertTrue("require_business_access(" in source or "_require_owner(" in source)
