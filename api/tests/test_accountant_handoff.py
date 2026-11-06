"""Canonical Accountant handoff facts, placement and evidence boundaries."""
from copy import deepcopy
from contextlib import contextmanager
import hashlib
import io
import json
import logging
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from order_forecast.api.accountant_handoff import build_handoff_preview
from order_forecast.api.accountant_handoff_export import create_handoff_export, DEFAULT_EXPORT_COPY
from order_forecast.api.accounting_store import AccountingExportBusy, accounting_export_scope
from order_forecast.api.accounting_domain import Accounting
from order_forecast.api import accounting_evidence as evidence
from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.errors import install_api_error_handlers
from order_forecast.api.middleware.rate_limit import limiter
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

    def test_cluster_timestamp_format_is_accepted_and_malformed_dates_fail_closed(self):
        paid = self.paid_expense(vendor="Timestamp format")
        self.receipt("offset-time", linked=paid["id"], created_at="2026-09-12T18:00:00+00:00")
        self.assertEqual(len(self.preview()["supportingPurchases"]), 1)

        expenses = self.accounting.load("expenses")
        expenses["items"][0]["date"] = "2026-02-31"
        self.store.put("expenses", expenses)
        with self.assertRaises(HTTPException) as raised:
            self.preview()
        self.assertEqual(raised.exception.status_code, 503)

    def test_zip_uses_same_snapshot_hashes_entries_and_escapes_spreadsheet_cells(self):
        _, reading = self.statement("final")
        self.confirm(reading, 0)
        paid = self.paid_expense(vendor="=FORMULA", note="@hidden", amountCents=17500)
        self.receipt("support", linked=paid["id"])
        self.receipt("support-two", linked=paid["id"])
        snapshot = self.preview()

        with tempfile.TemporaryDirectory() as directory:
            artifact = create_handoff_export(
                self.store, "123456", snapshot, DEFAULT_EXPORT_COPY,
                temp_directory=directory,
            )
            self.assertTrue(Path(artifact.path).is_file())
            with zipfile.ZipFile(artifact.path) as bundle:
                names = set(bundle.namelist())
                self.assertTrue({"summary.pdf", "income-by-billing-period.csv", "paid-expenses.csv",
                                 "data-gaps.csv", "manifest.json"}.issubset(names))
                self.assertTrue(any(name.startswith("settlement-statements/") for name in names))
                supporting = [name for name in names if name.startswith("supporting-documents/")]
                self.assertEqual(len(supporting), 2)
                self.assertEqual(len(set(supporting)), 2)
                self.assertTrue(bundle.read("summary.pdf").startswith(b"%PDF-1.4"))
                self.assertTrue(bundle.read("summary.pdf").endswith(b"%%EOF\n"))
                expenses_csv = bundle.read("paid-expenses.csv").decode("utf-8-sig")
                self.assertIn("'=FORMULA", expenses_csv)
                self.assertIn("'@hidden", expenses_csv)
                manifest = json.loads(bundle.read("manifest.json"))
                self.assertEqual(manifest["snapshot"]["snapshotId"], snapshot["snapshotId"])
                self.assertNotIn("manifest.json", {entry["path"] for entry in manifest["entries"]})
                for entry in manifest["entries"]:
                    content = bundle.read(entry["path"])
                    self.assertEqual(entry["bytes"], len(content))
                    self.assertEqual(entry["sha256"], hashlib.sha256(content).hexdigest())
                self.assertEqual(artifact.manifest_sha256,
                                 hashlib.sha256(bundle.read("manifest.json")).hexdigest())

    def test_export_refuses_source_changed_after_preview_and_removes_partial_file(self):
        manifest, reading = self.statement("final")
        self.confirm(reading, 0)
        snapshot = self.preview()
        key = ("statement", manifest["id"], manifest["files"][0]["id"])
        self.store.originals[key]["content"] += b"tampered"

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(HTTPException) as raised:
                create_handoff_export(self.store, "123456", snapshot, DEFAULT_EXPORT_COPY,
                                      temp_directory=directory)
            self.assertEqual(raised.exception.status_code, 503)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_export_refuses_preview_over_hard_limit_without_reading_sources(self):
        snapshot = self.preview()
        snapshot["export"].update(available=False, reasonCode="HANDOFF_EXPORT_TOO_LARGE")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(HTTPException) as raised:
                create_handoff_export(self.store, "123456", snapshot, DEFAULT_EXPORT_COPY,
                                      temp_directory=directory)
            self.assertEqual(raised.exception.status_code, 413)
            self.assertEqual(list(Path(directory).iterdir()), [])


class AccountantHandoffExportScopeTests(unittest.TestCase):
    class Cursor:
        def __init__(self, try_lock=True, unlock=True):
            self.try_lock, self.unlock, self.last = try_lock, unlock, ""
            self.queries = []

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, query, params=None):
            self.last = query
            self.queries.append((query, params))

        def fetchone(self):
            if "pg_try_advisory_lock" in self.last:
                return (self.try_lock,)
            if "pg_advisory_unlock" in self.last:
                return (self.unlock,)
            raise AssertionError(self.last)

    class Connection:
        def __init__(self, cursor):
            self.saved_cursor = cursor
            self.rollbacks = 0

        def cursor(self):
            return self.saved_cursor

        def rollback(self):
            self.rollbacks += 1

    def test_cluster_lock_is_acquired_and_released_even_when_export_fails(self):
        cursor = self.Cursor()
        connection = self.Connection(cursor)
        released, discarded = [], []
        with self.assertRaisesRegex(RuntimeError, "export failed"):
            with accounting_export_scope("owner", "989262", acquire=lambda: connection,
                                         release=released.append, discard=discarded.append):
                raise RuntimeError("export failed")
        self.assertTrue(any("pg_try_advisory_lock" in query for query, _ in cursor.queries))
        self.assertTrue(any("pg_advisory_unlock" in query for query, _ in cursor.queries))
        self.assertEqual(released, [connection])
        self.assertEqual(discarded, [])

    def test_busy_scope_returns_connection_without_yielding(self):
        cursor = self.Cursor(try_lock=False)
        connection = self.Connection(cursor)
        released = []
        with self.assertRaises(AccountingExportBusy):
            with accounting_export_scope("owner", "989262", acquire=lambda: connection,
                                         release=released.append, discard=lambda _: None):
                self.fail("busy scope yielded")
        self.assertFalse(any("pg_advisory_unlock" in query for query, _ in cursor.queries))
        self.assertEqual(released, [connection])

    def test_failed_unlock_discards_pooled_connection(self):
        cursor = self.Cursor(unlock=False)
        connection = self.Connection(cursor)
        released, discarded = [], []
        with accounting_export_scope("owner", "989262", acquire=lambda: connection,
                                     release=released.append, discard=discarded.append):
            pass
        self.assertEqual(released, [])
        self.assertEqual(discarded, [connection])

class AccountantHandoffHttpTests(unittest.TestCase):
    def setUp(self):
        limiter.reset()
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
        self.export_patcher = patch.object(accounting, "accounting_export_scope", transaction)
        self.export_patcher.start()
        self.addCleanup(self.export_patcher.stop)
        self.client = TestClient(self.app, raise_server_exceptions=False)

    def get(self, year="2026"):
        return self.client.get(f"/api/business/accounting/handoff/preview?route=989262&year={year}")

    def export(self, year="2026"):
        return self.client.get(f"/api/business/accounting/handoff/export?route=989262&year={year}")

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

    def test_owner_export_is_zip_with_manifest_hash_and_temporary_file_is_removed(self):
        before = set(Path(tempfile.gettempdir()).glob("routespark-handoff-*.zip"))
        response = self.export()
        after = set(Path(tempfile.gettempdir()).glob("routespark-handoff-*.zip"))
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["content-type"], "application/zip")
        self.assertIn("routespark-accountant-handoff-2026.zip", response.headers["content-disposition"])
        with zipfile.ZipFile(io.BytesIO(response.content)) as bundle:
            manifest = bundle.read("manifest.json")
            self.assertEqual(response.headers["x-routespark-manifest-sha256"], hashlib.sha256(manifest).hexdigest())
        self.assertEqual(after, before)

    def test_export_errors_are_stable_and_do_not_leave_files(self):
        with patch.object(accounting, "execute_handoff_export", side_effect=HTTPException(413, "private")):
            too_large = self.export()
        self.assertEqual(too_large.status_code, 413)
        self.assertEqual(too_large.json()["code"], "HANDOFF_EXPORT_TOO_LARGE")
        self.assertNotIn("private", too_large.text)

        with patch.object(accounting, "execute_handoff_export", side_effect=accounting.AccountingExportBusy()):
            busy = self.export()
        self.assertEqual(busy.status_code, 409)
        self.assertEqual(busy.json()["code"], "HANDOFF_EXPORT_BUSY")

    def test_team_member_and_transferred_owner_cannot_preview(self):
        self.app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "team-member"}
        self.db.set_document("users/team-member", {"profile": {"role": "teamMember", "routeNumber": "989262"}})
        self.assertEqual(self.get().status_code, 403)
        self.assertEqual(self.export().status_code, 403)
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
