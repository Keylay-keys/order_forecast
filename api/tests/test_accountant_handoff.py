"""Canonical Accountant handoff facts, placement and evidence boundaries."""
from copy import deepcopy
from contextlib import contextmanager
import asyncio
import csv
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
from order_forecast.api.accountant_handoff import build_handoff_preview, HandoffEvidenceUnverified
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

    def saved_bill(self, **changes):
        revision = self.accounting.load("bills")["revision"]
        value = dict(vendor="Storage Co", category="storage", amountCents=28400,
                     dueDate="2026-09-20", frequency="monthly", note="")
        value.update(changes)
        return self.accounting.bills("POST", "", {"revision": revision, "bill": value})["items"][0]

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
        unlinked = self.receipt("unlinked", linked=None)
        self.receipt("deleted", linked=missing["id"], archived=True)
        self.receipt("not-marked", linked=missing["id"], included=False)
        outside = self.paid_expense(date="2025-09-12", vendor="Prior year")
        self.receipt("linked-prior-year", linked=outside["id"])

        result = self.preview()

        self.assertEqual([row["purchaseId"] for row in result["supportingPurchases"]], [included["id"]])
        self.assertEqual([row["purchaseId"] for row in result["receiptsNotRecorded"]], [unlinked["id"]])
        paid = {row["vendor"]: row for row in result["paidExpenses"]}
        self.assertEqual(paid["Supported"]["supportingPurchaseIds"], [included["id"]])
        self.assertEqual(paid["Missing"]["supportingPurchaseIds"], [])
        codes = [gap["code"] for gap in result["gaps"]]
        self.assertNotIn("MARKED_PURCHASE_WITHOUT_PAID_EXPENSE", codes)
        self.assertIn("LINKED_PURCHASE_NOT_MARKED", codes)
        self.assertNotIn("PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS", codes)
        self.assertEqual(result["export"]["sourceFileCount"], 2)

    def test_unverified_statement_is_omitted_and_becomes_a_dated_gap(self):
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
                result = self.preview()
                self.assertEqual(result["statementPeriods"], [])
                gap = next(item for item in result["gaps"] if item["code"] == "STATEMENT_EVIDENCE_UNVERIFIED")
                self.assertEqual(gap["dates"], {"periodStart": "2026-09-03", "periodEnd": "2026-09-09"})
                self.assertEqual(result["export"]["sourceFileCount"], 0)
                if kind == "attempt":
                    with tempfile.TemporaryDirectory() as directory:
                        artifact = create_handoff_export(self.store, "123456", result, DEFAULT_EXPORT_COPY,
                                                         temp_directory=directory)
                        with zipfile.ZipFile(artifact.path) as bundle:
                            gaps = bundle.read("items-to-check.csv").decode("utf-8-sig")
                            self.assertIn("Saved settlement statement could not be verified and is not included", gaps)
                if kind == "attempt":
                    self.store.attempts = saved
                else:
                    self.store.originals = saved

    def test_unverified_receipt_is_omitted_and_becomes_a_dated_gap(self):
        paid = self.paid_expense(vendor="Damaged receipt")
        manifest = self.receipt("damaged", linked=paid["id"], created_at="2026-09-12T18:00:00Z")
        original_key = ("receipt", manifest["id"], manifest["files"][0]["id"])
        self.store.originals[original_key]["content"] += b"tampered"

        result = self.preview()

        self.assertEqual(result["supportingPurchases"], [])
        receipt_gap = next(item for item in result["gaps"] if item["code"] == "SUPPORTING_DOCUMENT_EVIDENCE_UNVERIFIED")
        self.assertEqual(receipt_gap["dates"], {"createdAt": "2026-09-12"})
        self.assertNotIn("PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS", {item["code"] for item in result["gaps"]})
        self.assertEqual(result["export"]["sourceFileCount"], 0)

    def test_snapshot_is_deterministic_and_changes_with_included_facts(self):
        self.paid_expense(vendor="Stable")
        first = self.preview()
        second = self.preview()
        self.assertEqual(first["snapshotId"], second["snapshotId"])

        self.paid_expense(vendor="New")
        changed = self.preview()
        self.assertNotEqual(first["snapshotId"], changed["snapshotId"])

    def test_money_out_only_snapshot_has_current_bills_unrecorded_receipts_and_split_totals(self):
        bill = self.saved_bill()
        self.paid_expense(vendor="Split payment", billId=bill["id"], category="fuel", amountCents=17500,
                          splits=[{"category": "fuel", "amountCents": 15000},
                                  {"category": "route_supplies", "amountCents": 2500}])
        included = self.receipt("scan-only")
        self.receipt("left-out", included=False)

        result = self.preview()

        self.assertEqual(result["moneyIn"], {
            "verifiedStatementCount": 0,
            "unverifiedStatementCount": 0,
            "paymentCount": 0,
            "collectionCount": 0,
        })
        self.assertEqual(result["receiptsLeftOutCount"], 1)
        self.assertEqual([item["purchaseId"] for item in result["receiptsNotRecorded"]], [included["id"]])
        self.assertNotIn(included["id"], result["receiptsNotRecorded"][0]["folder"])
        self.assertEqual(result["summaries"]["paidExpensesByCategory"], [
            {"category": "fuel", "valueCents": 15000},
            {"category": "route_supplies", "valueCents": 2500},
        ])
        self.assertEqual(len(result["bills"]), 1)
        self.assertEqual(result["bills"][0]["paymentCount"], 1)
        self.assertEqual(result["bills"][0]["paymentTotalCents"], 17500)
        self.assertEqual(result["revisions"]["bills"], 1)
        self.assertEqual(result["export"]["unrecordedReceiptFileCount"], 1)

        with tempfile.TemporaryDirectory() as directory:
            artifact = create_handoff_export(self.store, "123456", result, DEFAULT_EXPORT_COPY,
                                             temp_directory=directory)
            with zipfile.ZipFile(artifact.path) as bundle:
                names = set(bundle.namelist())
                summary = bundle.read("summary.pdf")
                self.assertIn(b"Fuel: $150.00", summary)
                self.assertIn(b"Route supplies: $25.00", summary)
                self.assertNotIn("income-by-billing-period.csv", names)
                self.assertIn("bills.csv", names)
                self.assertIn("receipts-not-recorded.csv", names)
                receipt_files = [name for name in names if name.startswith("receipts-not-recorded/")]
                self.assertEqual(len(receipt_files), 1)
                self.assertNotIn(included["id"], receipt_files[0])
                bills = list(csv.DictReader(io.StringIO(bundle.read("bills.csv").decode("utf-8-sig"))))
                self.assertEqual(bills[0]["Payments recorded in year"], "1")
                self.assertEqual(bills[0]["Total recorded in year"], "175.00")
                receipts = list(csv.DictReader(io.StringIO(bundle.read("receipts-not-recorded.csv").decode("utf-8-sig"))))
                self.assertEqual(receipts[0]["Documents"], "1")
                self.assertEqual(receipts[0]["File status"], "Included")

    def test_payment_only_year_explains_that_no_statements_are_included(self):
        self.store.put("money-in", {
            "schemaVersion": 1,
            "route": "123456",
            "revision": 1,
            "settlements": [],
            "remittances": [{
                "id": "payment-one",
                "periodEnd": "2026-09-09",
                "receivedDate": "2026-09-14",
                "amountCents": 100000,
                "method": "bank",
                "note": "",
            }],
        })
        snapshot = self.preview()
        with tempfile.TemporaryDirectory() as directory:
            artifact = create_handoff_export(self.store, "123456", snapshot, DEFAULT_EXPORT_COPY,
                                             temp_directory=directory)
            with zipfile.ZipFile(artifact.path) as bundle:
                summary = bundle.read("summary.pdf")
                self.assertIn(b"No settlement statements are included for this year.", summary)
                self.assertNotIn(b"None recorded in RouteSpark for this year.", summary)

    def test_current_bill_list_handles_stopped_one_time_archived_and_prior_year_payments(self):
        stopped = self.saved_bill(vendor="Stopped monthly")
        bills = self.accounting.load("bills")
        self.accounting.bills("PATCH", stopped["id"], {"revision": bills["revision"], "action": "stop"})
        one_time = self.saved_bill(vendor="One time", frequency="once", dueDate="2026-10-15")
        archived = self.paid_expense(vendor="Archived", billId=one_time["id"])
        expenses = self.accounting.load("expenses")
        self.accounting.expenses("PATCH", archived["id"], {"revision": expenses["revision"], "action": "archive"})

        current = {item["vendor"]: item for item in self.preview(2026)["bills"]}
        self.assertEqual(current["Stopped monthly"]["status"], "stopped")
        self.assertEqual(current["One time"]["frequency"], "once")
        self.assertEqual(current["One time"]["paymentCount"], 0)

        late_without_payment = self.saved_bill(vendor="Added later")
        late_with_payment = self.saved_bill(vendor="Added later but paid")
        self.paid_expense(vendor="Prior payment", billId=late_with_payment["id"], date="2025-12-31")
        prior = {item["vendor"]: item for item in self.preview(2025)["bills"]}
        self.assertNotIn(late_without_payment["vendor"], prior)
        self.assertEqual(prior[late_with_payment["vendor"]]["paymentCount"], 1)

    def test_unrecorded_receipt_moves_from_upload_year_to_paid_expense_year(self):
        receipt = self.receipt("january-upload", created_at="2027-01-02T12:00:00Z")
        self.assertEqual(self.preview(2026)["receiptsNotRecorded"], [])
        self.assertEqual([item["purchaseId"] for item in self.preview(2027)["receiptsNotRecorded"]],
                         [receipt["id"]])

        paid = self.paid_expense(date="2026-12-30", vendor="Late December")
        evidence.change_receipt_review(self.accounting, receipt["id"], {
            "revision": 0,
            "action": "record",
            "expenseId": paid["id"],
        })
        self.assertEqual([item["purchaseId"] for item in self.preview(2026)["supportingPurchases"]],
                         [receipt["id"]])
        self.assertEqual(self.preview(2027)["receiptsNotRecorded"], [])

    def test_known_damage_is_a_gap_for_linked_and_unlinked_receipts_without_blocking_export(self):
        paid = self.paid_expense(vendor="Linked damage")
        linked = self.receipt("linked-damage", linked=paid["id"])
        unlinked = self.receipt("unlinked-damage")
        integrity = {
            "schemaVersion": 1,
            "status": "needs_rescan",
            "reason": "stored_original_hash_mismatch",
            "detectedAt": "2026-11-07T12:00:00Z",
        }
        self.store.put("receipt-integrity:" + linked["id"], integrity)
        self.store.put("receipt-integrity:" + unlinked["id"], integrity)

        result = self.preview()

        self.assertEqual(result["supportingPurchases"], [])
        self.assertEqual(len(result["receiptsNotRecorded"]), 1)
        self.assertEqual(result["receiptsNotRecorded"][0]["fileStatus"], "unverified")
        self.assertEqual(result["receiptsNotRecorded"][0]["documentCount"], 1)
        self.assertEqual(result["receiptsNotRecorded"][0]["files"], [])
        codes = [item["code"] for item in result["gaps"]]
        self.assertEqual(codes.count("SUPPORTING_DOCUMENT_EVIDENCE_UNVERIFIED"), 2)
        self.assertNotIn("PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS", codes)
        with tempfile.TemporaryDirectory() as directory:
            artifact = create_handoff_export(self.store, "123456", result, DEFAULT_EXPORT_COPY,
                                             temp_directory=directory)
            with zipfile.ZipFile(artifact.path) as bundle:
                self.assertFalse(any(name.startswith("receipts-not-recorded/") for name in bundle.namelist()))
                csv_text = bundle.read("receipts-not-recorded.csv").decode("utf-8-sig")
                self.assertIn("Could not be verified, file not included", csv_text)
                rows = list(csv.DictReader(io.StringIO(csv_text)))
                self.assertEqual(rows[0]["Documents"], "1")
                self.assertEqual(rows[0]["Folder"], "")

    def test_real_store_style_hash_failure_returns_safe_receipt_and_statement_dates(self):
        manifest, reading = self.statement("final")
        self.confirm(reading, 0)
        paid = self.paid_expense(vendor="Hash receipt")
        receipt = self.receipt("hash-receipt", linked=paid["id"])
        snapshot = self.preview()
        original = self.store.original

        def hashing_original(kind, upload, file):
            value = original(kind, upload, file)
            if value is not None and hashlib.sha256(value["content"]).hexdigest() != value["sha256"]:
                raise RuntimeError("Original integrity mismatch with private storage details")
            return value

        cases = [
            (("receipt", receipt["id"], receipt["files"][0]["id"]),
             {"recordType": "receipt", "createdDate": "2026-09-12"}),
            (("statement", manifest["id"], manifest["files"][0]["id"]),
             {"recordType": "statement", "periodStart": "2026-09-03", "periodEnd": "2026-09-09"}),
        ]
        for key, expected in cases:
            with self.subTest(kind=key[0]):
                saved = self.store.originals[key]["content"]
                self.store.originals[key]["content"] += b"tampered"
                with patch.object(self.store, "original", side_effect=hashing_original):
                    with tempfile.TemporaryDirectory() as directory:
                        with self.assertRaises(HandoffEvidenceUnverified) as raised:
                            create_handoff_export(self.store, "123456", snapshot, DEFAULT_EXPORT_COPY,
                                                  temp_directory=directory)
                        self.assertEqual(raised.exception.details, expected)
                        self.assertEqual(list(Path(directory).iterdir()), [])
                self.store.originals[key]["content"] = saved

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
        money = self.confirm(reading, 0)
        self.accounting.remittances("POST", "", {"revision": money["revision"], "remittance": {
            "periodEnd": "2026-09-09", "receivedDate": "2026-09-14", "amountCents": 100000,
            "method": "bank", "note": "=private formula",
        }})
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
                                 "items-to-check.csv", "manifest.json"}.issubset(names))
                self.assertNotIn("data-gaps.csv", names)
                self.assertTrue(any(name.startswith("settlement-statements/") for name in names))
                supporting = [name for name in names if name.startswith("supporting-documents/")]
                self.assertEqual(len(supporting), 2)
                self.assertEqual(len(set(supporting)), 2)
                summary = bundle.read("summary.pdf")
                self.assertTrue(summary.startswith(b"%PDF-1.4"))
                self.assertTrue(summary.endswith(b"%%EOF\n"))
                self.assertIn(b"Generated: November 6, 2026, 10:30 PM UTC", summary)
                self.assertIn(b"Gross sales: Not available for 1 billing period", summary)
                self.assertIn(b"Total Margin Discount: $1,000.00 from 1 billing period", summary)
                self.assertIn(b"$1,000.00 from 1 payment", summary)

                income_csv = bundle.read("income-by-billing-period.csv").decode("utf-8-sig")
                income = list(csv.DictReader(io.StringIO(income_csv)))
                self.assertEqual(list(income[0]), DEFAULT_EXPORT_COPY["incomeHeaders"])
                self.assertNotIn("Record ID", income[0])
                statement = next(row for row in income if row["Record type"] == "Settlement statement")
                self.assertEqual(statement["Statement status"], "Final")
                self.assertEqual(statement["Gross sales"], "")
                self.assertEqual(statement["Total Margin Discount"], "1000.00")
                self.assertEqual(statement["Cash-account sales"], "50.00")
                self.assertTrue(statement["Statement file"].startswith("settlement-statements/"))
                negative = next(row for row in income if row["Adjustment"] == "Net Cash Sales")
                self.assertEqual(negative["Amount"], "-50.00")
                payment = next(row for row in income if row["Record type"] == "Settlement payment")
                self.assertEqual(payment["Payment method"], "Bank transfer")
                self.assertEqual(payment["Note"], "'=private formula")

                expenses_csv = bundle.read("paid-expenses.csv").decode("utf-8-sig")
                self.assertIn("'=FORMULA", expenses_csv)
                self.assertIn("'@hidden", expenses_csv)
                expenses = list(csv.DictReader(io.StringIO(expenses_csv)))
                self.assertEqual(list(expenses[0]), DEFAULT_EXPORT_COPY["expenseHeaders"])
                self.assertNotIn("Expense ID", expenses[0])
                self.assertEqual(expenses[0]["Category amount"], "175.00")
                self.assertEqual(expenses[0]["Expense total"], "175.00")
                self.assertEqual(expenses[0]["Payment method"], "Bank transfer")
                self.assertIn("supporting-documents/", expenses[0]["Supporting documents folder"])

                gaps_csv = bundle.read("items-to-check.csv").decode("utf-8-sig")
                gaps = list(csv.DictReader(io.StringIO(gaps_csv)))
                self.assertEqual(list(gaps[0]), DEFAULT_EXPORT_COPY["gapHeaders"])
                self.assertNotIn("Gap code", gaps[0])
                self.assertEqual(gaps[0]["Record type"], "Settlement statement")
                self.assertEqual(gaps[0]["Missing details"], "Gross sales")
                manifest = json.loads(bundle.read("manifest.json"))
                self.assertEqual(manifest["snapshot"]["snapshotId"], snapshot["snapshotId"])
                self.assertEqual(manifest["copyVersion"], "labels-20261107-3")
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
            self.assertEqual(raised.exception.details, {
                "recordType": "statement", "periodStart": "2026-09-03", "periodEnd": "2026-09-09",
            })
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

    def test_export_file_is_removed_when_client_disconnects_during_send(self):
        with tempfile.NamedTemporaryFile(prefix="routespark-handoff-test-", suffix=".zip", delete=False) as file:
            file.write(b"temporary export")
            path = file.name

        response = accounting.HandoffFileResponse(path, media_type="application/zip")

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.body":
                raise ConnectionError("client disconnected")

        scope = {"type": "http", "method": "GET", "headers": []}
        with self.assertRaisesRegex(ConnectionError, "client disconnected"):
            asyncio.run(response(scope, receive, send))
        self.assertFalse(Path(path).exists())

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

        limiter.reset()
        safe_details = {"recordType": "receipt", "createdDate": "2026-09-12"}
        with patch.object(accounting, "execute_handoff_export", side_effect=HandoffEvidenceUnverified(safe_details)):
            unverified = self.export()
        self.assertEqual(unverified.status_code, 503)
        self.assertEqual(unverified.json()["code"], "HANDOFF_EVIDENCE_UNVERIFIED")
        self.assertEqual(unverified.json()["details"], safe_details)

        limiter.reset()
        with patch.object(accounting, "execute_handoff_export", side_effect=HTTPException(503, "private")):
            unavailable = self.export()
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(unavailable.json()["code"], "HANDOFF_EXPORT_UNAVAILABLE")
        self.assertNotIn("private", unavailable.text)

    def test_preview_distinguishes_unverified_evidence_from_unavailable_storage(self):
        safe_details = {"recordType": "statement", "periodStart": "2026-09-03", "periodEnd": "2026-09-09"}
        with patch.object(accounting, "execute_handoff_preview", side_effect=HandoffEvidenceUnverified(safe_details)):
            unverified = self.get()
        self.assertEqual(unverified.status_code, 503)
        self.assertEqual(unverified.json()["code"], "HANDOFF_EVIDENCE_UNVERIFIED")
        self.assertEqual(unverified.json()["details"], safe_details)

        with patch.object(accounting, "execute_handoff_preview", side_effect=HTTPException(503, "private")):
            unavailable = self.get()
        self.assertEqual(unavailable.status_code, 503)
        self.assertEqual(unavailable.json()["code"], "HANDOFF_PREVIEW_UNAVAILABLE")
        self.assertNotIn("private", unavailable.text)

    def test_unverified_details_reject_ids_names_paths_hashes_and_invalid_dates(self):
        unsafe = [
            {"recordType": "statement", "periodStart": "2026-09-03", "periodEnd": "2026-09-09", "fileId": "private"},
            {"recordType": "receipt", "createdDate": "2026-09-12", "path": "/private/source.jpg"},
            {"recordType": "receipt", "createdDate": "2026-02-31"},
            {"recordType": "statement", "periodStart": "2026-09-03", "periodEnd": "not-a-date"},
        ]
        for details in unsafe:
            with self.subTest(details=details):
                limiter.reset()
                with patch.object(accounting, "execute_handoff_preview",
                                  side_effect=HandoffEvidenceUnverified(details)):
                    response = self.get()
                self.assertEqual(response.status_code, 503)
                self.assertNotIn("details", response.json())

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
