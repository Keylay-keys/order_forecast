"""Canonical Accountant handoff facts, placement and evidence boundaries."""
from copy import deepcopy
from contextlib import contextmanager
import logging
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api.accountant_handoff import build_handoff_preview
from order_forecast.api.accounting_domain import Accounting
from order_forecast.api import accounting_evidence as evidence
from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.errors import install_api_error_handlers
from order_forecast.api.routers import accounting
from order_forecast.api.tests.test_business_access import fixture
from order_forecast.api.tests.test_accounting import MemoryStore, analysis, expense


YEAR = 2026


class AccountantHandoffBuilderTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.accounting = Accounting(self.store, "123456", "2027-11-06")

    def statement(self, name, facts=None):
        content = b"%PDF-1.7\n" + name.encode() + b"\n%%EOF"
        manifest = evidence.upload(self.store, "123456", "statement", [(name + ".pdf", "application/pdf", content)])
        reading = evidence.analyze_statement(
            self.store, "123456", manifest["id"], manifest["files"][0]["id"],
            reader=lambda *_: deepcopy(facts or analysis()),
        )
        return manifest, reading

    def confirm(self, reading, revision, version="final", **extra):
        return evidence.confirm_statement(self.accounting, {
            "revision": revision,
            "uploadId": reading["uploadId"],
            "fileId": reading["fileId"],
            "attemptId": reading["id"],
            "version": version,
            **extra,
        })

    def paid_expense(self, **changes):
        revision = self.accounting.load("expenses")["revision"]
        return self.accounting.expenses("POST", "", {
            "revision": revision,
            "expense": expense(**changes),
        })["items"][0]

    def receipt(self, name, linked=None, included=True, archived=False, created_at="2026-09-12T18:00:00Z"):
        content = b"\xff\xd8\xff\xc0\x00\x02\xff\xda\x00\x02" + name.encode() + b"\xff\xd9"
        manifest = evidence.upload(self.store, "123456", "receipt", [(name + ".jpg", "image/jpeg", content)])
        manifest["createdAt"] = created_at
        self.store.put("receipt-upload:" + manifest["id"], manifest)
        review = evidence.receipt_review(self.store, "123456", manifest)
        review.update(includeInAccountantHandoff=included)
        if linked:
            review.update(status="recorded", linkedExpenseId=linked)
        if archived:
            review["archivedAt"] = "2026-11-06T18:00:00Z"
        self.store.put("receipt-review:" + manifest["id"], review)
        return manifest

    def preview(self, year=YEAR):
        return build_handoff_preview(
            self.store, "123456", year,
            generated_at="2026-11-06T22:30:00Z",
        )

    def test_final_replaces_preliminary_once_and_exact_confirmed_source_wins(self):
        first_manifest, preliminary = self.statement("monday")
        first = self.confirm(preliminary, 0, "preliminary")
        final_facts = analysis(marginCents=110000, newSettlementCents=110000,
                               account=dict(previousBalance=0, payment=0, totalNewSettlement=110000, amountToPay=110000))
        final_manifest, final = self.statement("friday", final_facts)
        self.confirm(final, 1, "final", replaceSource=first["settlements"][0]["source"])

        result = self.preview()

        self.assertEqual(len(result["statementPeriods"]), 1)
        row = result["statementPeriods"][0]
        self.assertEqual(row["version"], "final")
        self.assertEqual(row["source"]["uploadId"], final_manifest["id"])
        self.assertNotEqual(row["source"]["uploadId"], first_manifest["id"])
        self.assertEqual(row["facts"]["marginCents"], 110000)
        self.assertEqual(result["export"]["sourceFileCount"], 1)

    def test_unknown_gross_sales_is_null_counted_and_preliminary_is_a_gap(self):
        _, reading = self.statement("preliminary")
        self.confirm(reading, 0, "preliminary")

        result = self.preview()

        row = result["statementPeriods"][0]
        self.assertIsNone(row["facts"]["grossSalesCents"])
        self.assertEqual(row["unknownFields"], ["grossSalesCents"])
        total = result["summaries"]["statementFacts"]["grossSalesCents"]
        self.assertEqual(total, {"valueCents": 0, "knownCount": 0, "unknownCount": 1})
        self.assertEqual(
            {gap["code"] for gap in result["gaps"]},
            {"STATEMENT_FACTS_UNKNOWN", "PRELIMINARY_STATEMENT_ONLY"},
        )

    def test_each_fact_uses_its_approved_year_date_and_cross_year_is_flagged(self):
        cross_year = analysis(periodStart="2025-12-25", periodEnd="2026-01-01")
        _, reading = self.statement("cross-year", cross_year)
        self.confirm(reading, 0)
        self.accounting.remittances("POST", "", {"revision": 1, "remittance": {
            "periodEnd": "2026-01-01", "receivedDate": "2027-01-02", "amountCents": 100000,
            "method": "bank", "note": "",
        }})
        self.accounting.route_stores("POST", {"action": "refresh"}, lambda: [{"id": "store1", "name": "Store One"}])
        self.accounting.route_stores("POST", {"action": "select", "revision": 1,
                                               "selectedStoreIds": ["store1"], "handheldStoreIds": ["store1"]})
        self.accounting.cash("POST", "", {"revision": 0, "action": "save", "duplicateConfirmed": False, "payment": {
            "periodEnd": "2026-01-01", "receivedDate": "2027-01-03", "amountCents": 5000,
            "customer": "Store One", "storeId": "store1", "method": "cash", "note": "",
        }})
        self.paid_expense(date="2025-12-31")
        self.paid_expense(date="2026-01-02", vendor="January Fuel")

        result_2026 = self.preview(2026)
        result_2027 = self.preview(2027)

        self.assertEqual(len(result_2026["statementPeriods"]), 1)
        self.assertTrue(result_2026["statementPeriods"][0]["crossesCalendarYear"])
        self.assertEqual(result_2026["settlementPayments"], [])
        self.assertEqual(result_2026["cashAccountCollections"], [])
        self.assertEqual([row["vendor"] for row in result_2026["paidExpenses"]], ["January Fuel"])
        self.assertEqual(len(result_2027["settlementPayments"]), 1)
        self.assertEqual(len(result_2027["cashAccountCollections"]), 1)
        self.assertIn("BILLING_PERIOD_CROSSES_CALENDAR_YEAR", {gap["code"] for gap in result_2026["gaps"]})

    def test_supporting_documents_require_active_marked_link_and_gaps_are_explicit(self):
        supported = self.paid_expense(vendor="Supported")
        missing = self.paid_expense(vendor="Missing")
        included = self.receipt("included", linked=supported["id"])
        self.receipt("unlinked", linked=None)
        self.receipt("deleted", linked=missing["id"], archived=True)
        self.receipt("not-marked", linked=missing["id"], included=False)
        outside = self.paid_expense(date="2025-09-12", vendor="Prior year")
        self.receipt("linked-prior-year", linked=outside["id"])

        result = self.preview()

        self.assertEqual([row["purchaseId"] for row in result["supportingPurchases"]], [included["id"]])
        paid = {row["vendor"]: row for row in result["paidExpenses"]}
        self.assertEqual(paid["Supported"]["supportingPurchaseIds"], [included["id"]])
        self.assertEqual(paid["Missing"]["supportingPurchaseIds"], [])
        codes = [gap["code"] for gap in result["gaps"]]
        self.assertEqual(codes.count("MARKED_PURCHASE_WITHOUT_PAID_EXPENSE"), 1)
        self.assertIn("PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS", codes)
        self.assertEqual(result["export"]["sourceFileCount"], 1)

    def test_confirmed_pointer_manifest_and_original_mismatches_fail_closed(self):
        manifest, reading = self.statement("verified")
        self.confirm(reading, 0)
        cases = []

        attempt_key = ("statement", reading["uploadId"], reading["fileId"], reading["id"])
        bad_attempt = deepcopy(self.store.attempts[attempt_key])
        bad_attempt["sha256"] = "f" * 64
        cases.append(("attempt", attempt_key, bad_attempt))

        original_key = ("statement", manifest["id"], manifest["files"][0]["id"])
        bad_original = deepcopy(self.store.originals[original_key])
        bad_original["sha256"] = "e" * 64
        cases.append(("original", original_key, bad_original))

        for kind, key, bad in cases:
            with self.subTest(kind=kind):
                saved = deepcopy(self.store.attempts if kind == "attempt" else self.store.originals)
                target = self.store.attempts if kind == "attempt" else self.store.originals
                target[key] = bad
                with self.assertRaises(HTTPException) as raised:
                    self.preview()
                self.assertEqual(raised.exception.status_code, 503)
                if kind == "attempt":
                    self.store.attempts = saved
                else:
                    self.store.originals = saved

    def test_snapshot_is_deterministic_and_changes_with_included_facts(self):
        self.paid_expense(vendor="Stable")
        first = self.preview()
        second = self.preview()
        self.assertEqual(first["snapshotId"], second["snapshotId"])

        self.paid_expense(vendor="New")
        changed = self.preview()
        self.assertNotEqual(first["snapshotId"], changed["snapshotId"])


class AccountantHandoffHttpTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        install_api_error_handlers(self.app, debug_mode=False, app_logger=logging.getLogger("test.handoff"))
        self.app.include_router(accounting.router, prefix="/api")
        self.db = fixture()
        self.app.dependency_overrides[get_firestore] = lambda: self.db
        self.app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "owner"}
        self.store = MemoryStore()

        @contextmanager
        def transaction(uid, route):
            if uid != "owner" or route != "989262":
                raise AssertionError("unexpected scope")
            before = deepcopy((self.store.docs, self.store.history, self.store.attempts, self.store.originals))
            yield self.store
            after = (self.store.docs, self.store.history, self.store.attempts, self.store.originals)
            if before != after:
                raise AssertionError("preview wrote accounting state")

        self.patcher = patch.object(accounting, "accounting_transaction", transaction)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def get(self, year="2026"):
        return self.client.get(f"/api/business/accounting/handoff/preview?route=989262&year={year}")

    def test_owner_preview_is_read_only_no_store_and_versioned(self):
        response = self.get()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["schemaVersion"], 1)
        self.assertEqual(response.json()["route"], "989262")
        self.assertEqual(response.headers["cache-control"], "private, no-store")

    def test_invalid_year_has_stable_non_sensitive_error(self):
        response = self.get("2026-01")
        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(response.json()["code"], "HANDOFF_YEAR_INVALID")

    def test_team_member_and_transferred_owner_cannot_preview(self):
        self.app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "team-member"}
        self.db.set_document("users/team-member", {"profile": {"role": "teamMember", "routeNumber": "989262"}})
        self.assertEqual(self.get().status_code, 403)
        self.app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "owner"}
        self.db.set_document("routes/989262", {"ownerUid": "someone-else"})
        self.assertEqual(self.get().status_code, 403)

    def test_database_error_does_not_leak_internal_details(self):
        with patch.object(accounting, "accounting_transaction", side_effect=RuntimeError("secret database host")):
            response = self.get()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["code"], "HANDOFF_PREVIEW_UNAVAILABLE")
        self.assertNotIn("secret", response.text)


if __name__ == "__main__":
    unittest.main()
