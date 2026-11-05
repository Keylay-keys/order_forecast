"""Financial behavior and authenticated HTTP contracts using isolated memory data."""
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import unittest
from unittest.mock import patch
from uuid import UUID, uuid4, uuid5

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api.accounting_domain import Accounting
from order_forecast.api import accounting_evidence as evidence
from order_forecast.api.dependencies import get_firestore, verify_firebase_token
from order_forecast.api.routers import accounting
from order_forecast.api.tests.test_business_access import fixture


class MemoryStore:
    def __init__(self):
        self.docs, self.originals, self.attempts, self.history, self.agent_jobs = {}, {}, {}, [], {}

    def get(self, key):
        return deepcopy(self.docs.get(key))

    def put(self, key, value):
        self.docs[key] = deepcopy(value)
        self.history.append((key, deepcopy(value)))

    def documents(self, prefix):
        return deepcopy([v for k, v in self.docs.items() if k.startswith(prefix)])

    def queue_payment_advice_cleanup(self, document_number):
        self.queued_advice_cleanup = [*getattr(self, 'queued_advice_cleanup', []), document_number]

    def has_original(self, kind, digest):
        return any(k[0] == kind and v["sha256"] == digest for k, v in self.originals.items())

    def add_original(self, kind, upload, file, content):
        self.originals[kind, upload, file["id"]] = dict(content=content, sha256=file["sha256"], mediaType=file.get("mediaType", "application/pdf"), originalName=file["originalName"])

    def original(self, kind, upload, file):
        return deepcopy(self.originals.get((kind, upload, file)))

    def add_attempt(self, kind, upload, file, attempt, payload):
        assert (kind, upload, file, attempt) not in self.attempts
        self.attempts[kind, upload, file, attempt] = deepcopy(payload)

    def attempt(self, kind, upload, file, attempt):
        return deepcopy(self.attempts.get((kind, upload, file, attempt)))

    def queue_receipt(self, upload, retry=False):
        pass

    def receipt_jobs(self):
        return {}

    def queue_receipt_agent(self, upload, ocr_attempt):
        self.agent_jobs.setdefault((upload, ocr_attempt), {"status": "queued", "review": None})

    def receipt_agent_review(self, upload, ocr_attempt):
        return deepcopy(self.agent_jobs.get((upload, ocr_attempt)))

    def receipt_agent_reviews(self):
        return deepcopy(self.agent_jobs)


def expense(**changes):
    return dict(dict(date="2026-09-14", vendor="Storage Co", category="storage", amountCents=28400, note="", paymentMethod="bank"), **changes)


def bill(**changes):
    return dict(dict(vendor="Storage Co", category="storage", amountCents=28400, dueDate="2026-09-20", frequency="monthly", note=""), **changes)


def analysis(route="123456", **changes):
    return dict(dict(schemaVersion=1, route=route, periodStart="2026-09-03", periodEnd="2026-09-09", pageCount=1,
        adjustments=[dict(key="netCashSales", label="Net Cash Sales", cents=-5000), dict(key="other", label="Other", cents=5000)],
        marginCents=100000, inventoryCents=0, totalAdjustmentsCents=0, newSettlementCents=100000,
        account=dict(previousBalance=0, payment=0, totalNewSettlement=100000, amountToPay=100000),
        checks=[dict(label="Check " + str(i), calculatedCents=0, printedCents=0) for i in range(8)],
        cashState="present", cashSalesCents=5000, hhcRentalFeeCents=0, issues=[]), **changes)


class AccountingBehaviorTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.domain = Accounting(self.store, "123456", "2026-09-15")

    def test_create_bill_is_unpaid_and_identity_survives_rename(self):
        created = self.domain.bills("POST", "", {"revision": 0, "bill": bill()})
        self.assertEqual(self.domain.load("expenses")["items"], [])
        self.assertEqual(self.domain.load("money-in")["remittances"], [])
        bid = created["items"][0]["id"]
        changed = self.domain.bills("PATCH", bid, {"revision": 1, "action": "edit", "bill": bill(vendor="Renamed", category="other")})
        self.assertEqual(changed["items"][0]["id"], bid)
        self.assertEqual(self.domain.load("expenses")["revision"], 0)

    def test_linked_payment_newest_amount_and_stop_one_bill(self):
        b = self.domain.bills("POST", "", {"revision": 0, "bill": bill()})["items"][0]
        first = self.domain.expenses("POST", "", {"revision": 0, "expense": expense(billId=b["id"], date="2026-08-14")})
        second = self.domain.expenses("POST", "", {"revision": 1, "expense": expense(billId=b["id"], amountCents=30000)})
        self.assertEqual({p["billId"] for p in second["items"]}, {b["id"]})
        self.assertIn("recurrence", second["items"][0])
        self.domain.expenses("PATCH", second["items"][0]["id"], {"revision": 2, "action": "edit", "expense": expense(amountCents=30000), "monthly": False, "confirmRecurrenceOff": True})
        self.assertEqual(self.domain.load("bills")["items"][0]["status"], "stopped")
        self.assertIn("recurrence", self.domain.load("expenses")["items"][1])
        self.assertEqual(first["items"][0]["amountCents"], 28400)
        self.domain.expenses("PATCH", first["items"][0]["id"], {"revision": 3, "action": "edit", "expense": expense(date="2026-08-14", note="Corrected memo"), "monthly": True})
        self.assertEqual(self.domain.load("bills")["items"][0]["status"], "stopped")
        self.assertNotIn("recurrence", self.domain.load("expenses")["items"][1])

    def test_stale_revision_invalid_amount_splits_and_foreign_bill_rejected(self):
        self.domain.expenses("POST", "", {"revision": 0, "expense": expense()})
        for body in ({"revision": 0, "expense": expense()}, {"revision": 1, "expense": expense(amountCents=True)},
                     {"revision": 1, "expense": expense(billId=str(uuid4()))},
                     {"revision": 1, "expense": expense(splits=[dict(category="storage", amountCents=10000), dict(category="other", amountCents=1)])}):
            with self.assertRaises(HTTPException):
                self.domain.expenses("POST", "", body)
        self.assertEqual(len(self.domain.load("expenses")["items"]), 1)

    def test_archive_restore_preserves_history(self):
        paid = self.domain.expenses("POST", "", {"revision": 0, "expense": expense()})["items"][0]
        self.domain.expenses("PATCH", paid["id"], {"revision": 1, "action": "archive"})
        self.assertIn("archivedAt", self.domain.load("expenses")["items"][0])
        self.domain.expenses("PATCH", paid["id"], {"revision": 2, "action": "restore"})
        self.assertNotIn("archivedAt", self.domain.load("expenses")["items"][0])
        self.assertEqual(len(self.store.history), 3)

    def add_statement(self, suffix, reader=None):
        manifest = evidence.upload(self.store, "123456", "statement", [(suffix + ".pdf", "application/pdf", b"%PDF-1.7\n" + suffix.encode() + b"\n%%EOF")])
        attempt = evidence.analyze_statement(self.store, "123456", manifest["id"], manifest["files"][0]["id"], reader=reader or (lambda *args: analysis()))
        return manifest, attempt

    def confirm(self, attempt, revision, version="preliminary", **extra):
        return evidence.confirm_statement(self.domain, dict(revision=revision, uploadId=attempt["uploadId"], fileId=attempt["fileId"], attemptId=attempt["id"], version=version, **extra))

    def test_performance_projection_does_not_expose_evidence_or_change_stored_attempt(self):
        from order_forecast.api.tests.test_accounting_performance import attempt as metric_fixture
        def reader(*_):
            source = metric_fixture()
            source['_evidence']['extracted'].update(routeNumber='123456', periodStart='2026-09-03', periodEnd='2026-09-09')
            value = analysis(_evidence=source['_evidence'])
            value['adjustments'].append(dict(key='salesReturns', label='Returns', cents=-500))
            value['adjustments'][1]['cents'] += 500
            value['checks'][7] = dict(label='Net sales', printedCents=9000, calculatedCents=9000)
            return value
        _, reading = self.add_statement('projection', reader)
        # Bind the fixture to the actual original, as the real extractor does.
        stored = next(iter(self.store.attempts.values()))
        stored['_evidence']['extracted']['source']['documentId'] = 'sha256:' + reading['sha256']
        before = deepcopy(self.store.attempts)
        history = deepcopy(self.store.history)
        public = evidence.latest_statement(self.store, reading['uploadId'], reading['fileId'])
        self.assertEqual(public['analysis']['grossSalesCents'], 10000)
        self.assertNotIn('_evidence', public)
        self.assertNotIn('PRIVATE PDF TEXT', str(public))
        self.assertEqual(self.store.attempts, before)
        self.assertEqual(self.store.history, history)
        self.confirm(reading, 0)  # additive projection retains the existing review contract

    def test_printed_time_projects_old_readings_without_changing_evidence_or_ledger(self):
        _, reading = self.add_statement('printed-time', lambda *_: analysis(_evidence={'sourceText': 'Gruma Corporation 09/14/26 19:25\nPrivate source text'}))
        before, history = deepcopy(self.store.attempts), deepcopy(self.store.history)
        public = evidence.latest_statement(self.store, reading['uploadId'], reading['fileId'])
        self.assertEqual(public['analysis']['printedIssuedAt'], '2026-09-14T19:25')
        self.assertNotIn('_evidence', public)
        self.assertNotIn('Private source text', str(public))
        self.assertEqual(self.store.attempts, before)
        self.assertEqual(self.store.history, history)

    def test_preliminary_final_is_one_period_and_does_not_record_income(self):
        _, preliminary = self.add_statement("monday")
        first = self.confirm(preliminary, 0)
        _, final = self.add_statement("holiday-shifted-final")
        with self.assertRaises(HTTPException):
            self.confirm(final, 1, "final")
        result = self.confirm(final, 1, "final", replaceSource=first["settlements"][0]["source"])
        self.assertEqual(len(result["settlements"]), 1)
        self.assertEqual(result["remittances"], [])
        self.assertEqual(result["settlements"][0]["revision"], "final")
        self.assertEqual(len(self.store.originals), 2)  # no unapproved preliminary deletion
        self.assertEqual(len(self.store.attempts), 2)
        self.assertEqual(self.store.history[2][1]["settlements"][0]["revision"], "preliminary")
        with self.assertRaises(HTTPException):
            self.confirm(preliminary, 2, "preliminary")

    def test_failed_reconciliation_and_stale_attempt_never_post(self):
        manifest, bad = self.add_statement("bad", lambda *args: analysis(totalAdjustmentsCents=1))
        with self.assertRaises(HTTPException):
            self.confirm(bad, 0)
        evidence.analyze_statement(self.store, "123456", manifest["id"], manifest["files"][0]["id"], reader=lambda *args: analysis())
        with self.assertRaises(HTTPException):
            self.confirm(bad, 0)
        self.assertEqual(self.domain.load("money-in")["settlements"], [])

    def test_cash_flow_selected_store_required_duplicate_and_corrections(self):
        _, a = self.add_statement("cash")
        self.confirm(a, 0)
        self.domain.route_stores("POST", {"action": "refresh"}, lambda: [dict(id="store1", name="Store One")])
        self.domain.route_stores("POST", dict(action="select", revision=1, selectedStoreIds=["store1"], handheldStoreIds=["store1"]))
        collection = dict(periodEnd="2026-09-09", receivedDate="2026-09-14", amountCents=4000, customer="Store One", storeId="store1", method="cash", note="")
        data = self.domain.cash("POST", "", dict(revision=0, action="save", payment=collection, duplicateConfirmed=False))
        with self.assertRaises(HTTPException):
            self.domain.cash("POST", "", dict(revision=1, action="save", payment=collection, duplicateConfirmed=False))
        self.domain.cash("PATCH", data["payments"][0]["id"], dict(revision=1, action="archive"))
        self.domain.cash("PATCH", data["payments"][0]["id"], dict(revision=2, action="restore"))
        self.domain.cash("POST", "store-amounts", dict(revision=3, storeAmount=dict(periodEnd="2026-09-09", customer="Store One", storeId="store1", amountCents=5000)))
        self.assertEqual(len(self.domain.load("cash-accounts")["payments"]), 1)
        self.assertEqual(self.domain.load("money-in")["remittances"], [])

    def test_remittance_requires_confirmed_period_and_edits_without_duplicate_income(self):
        payment = dict(periodEnd="2026-09-09", receivedDate="2026-09-14", amountCents=100000, method="bank", note="Deposit")
        with self.assertRaises(HTTPException):
            self.domain.remittances("POST", "", dict(revision=0, remittance=payment))
        _, reading = self.add_statement("remittance")
        self.confirm(reading, 0, "final")
        saved = self.domain.remittances("POST", "", dict(revision=1, remittance=payment))
        with self.assertRaises(HTTPException):
            self.domain.remittances("POST", "", dict(revision=2, remittance=payment))
        corrected = self.domain.remittances("PATCH", saved["remittances"][0]["id"], dict(revision=2, remittance=dict(payment, amountCents=99000)))
        self.assertEqual(len(corrected["remittances"]), 1)
        self.assertEqual(corrected["remittances"][0]["amountCents"], 99000)
        self.assertEqual(self.domain.load("expenses")["items"], [])

    def test_payee_reuse_edit_and_stale_tab_protection(self):
        self.domain.payees("POST", "", {"payee":dict(name=" Storage Co ", defaultCategory="storage")})
        reused = self.domain.payees("POST", "", {"payee":dict(name="storage co", defaultCategory="storage")})
        self.assertEqual(len(reused["items"]), 1)
        self.assertEqual(reused["items"][0]["useCount"], 2)
        pid = reused["items"][0]["id"]
        edited = self.domain.payees("PATCH", pid, dict(revision=2, action="edit", payee=dict(name="Renamed Storage", defaultCategory="other")))
        self.assertEqual(edited["items"][0]["id"], pid)
        with self.assertRaises(HTTPException):
            self.domain.payees("PATCH", pid, dict(revision=2, action="remove"))

    def test_104_pdf_backfill_duplicate_guard_and_no_money(self):
        files = [(f"statement-{i}.pdf", "application/pdf", f"%PDF-1.7\n{i}\n%%EOF".encode()) for i in range(104)]
        result = evidence.upload(self.store, "123456", "statement", files)
        self.assertEqual(len(result["files"]), 104)
        before = deepcopy(self.store.docs)
        duplicate = evidence.upload(self.store, "123456", "statement", files[:1])
        self.assertEqual(duplicate['duplicateCount'], 1)
        self.assertEqual(duplicate['files'], [])
        self.assertEqual(self.store.docs, before)
        mixed = evidence.upload(self.store, "123456", "statement", files[:1] + [('new.pdf', 'application/pdf', b'%PDF-1.7\nNew version\n%%EOF')])
        self.assertEqual(mixed['duplicateCount'], 1)
        self.assertEqual(len(mixed['files']), 1)
        self.assertEqual(self.domain.load("money-in")["settlements"], [])

    def test_receipt_review_links_only_existing_expense_and_retains_original(self):
        pdf = b"%PDF-1.7\nreceipt\n%%EOF"
        packet = evidence.upload(self.store, "123456", "receipt", [("receipt.pdf", "application/pdf", pdf)])
        with self.assertRaises(HTTPException):
            evidence.change_receipt_review(self.domain, packet["id"], {"revision": 0, "action": "record", "expenseId": str(uuid4())})
        paid = self.domain.expenses("POST", "", {"revision": 0, "expense": expense()})["items"][0]
        linked = evidence.change_receipt_review(self.domain, packet["id"], {"revision": 0, "action": "record", "expenseId": paid["id"]})
        self.assertEqual(linked["items"][0]["review"]["linkedExpenseId"], paid["id"])
        evidence.change_receipt_review(self.domain, packet["id"], {"revision": 1, "action": "unlink"})
        self.assertEqual(evidence.original(self.store, "receipt", packet["id"], packet["files"][0]["id"])["content"], pdf)
        self.assertEqual(len(self.domain.load("expenses")["items"]), 1)

    def test_receipts_invoices_accumulate_past_104_across_uploads(self):
        for batch in range(22):
            files = [(f"invoice-{batch}-{i}.pdf", "application/pdf", f"%PDF-1.7\nInvoice {batch}-{i}\n%%EOF".encode()) for i in range(5)]
            evidence.upload(self.store, "123456", "receipt", files)
        self.assertEqual(len(self.store.originals), 110)
        self.assertEqual(len(self.store.documents("receipt-upload:")), 22)
        self.assertEqual(self.domain.load("expenses")["items"], [])

    def test_worker_report_requires_complete_hash_bound_evidence(self):
        packet = evidence.upload(self.store, "123456", "receipt", [("receipt.pdf", "application/pdf", b"%PDF-1.7\nreceipt\n%%EOF")])
        file = packet["files"][0]
        report = dict(schemaVersion=1, route="123456", packetId=packet["id"], attemptId=str(uuid4()), createdAt="2026-09-15T12:00:00Z", pipelineVersion="fixture", status="needs_attention",
                      documents=[dict(fileId=file["id"], sha256="0" * 64, status="unsupported", extraction={})], proposal=None)
        with self.assertRaises(HTTPException):
            evidence.accept_receipt_attempt(self.store, "123456", packet["id"], report)
        self.assertEqual(self.store.attempts, {})
        report["documents"][0]["sha256"] = file["sha256"]
        inbox = evidence.accept_receipt_attempt(self.store, "123456", packet["id"], report)
        self.assertEqual(inbox["items"][0]["ocrStatus"], "needs_attention")
        self.assertEqual(self.domain.load("expenses")["items"], [])

    def test_app_scan_uuid5_file_identity_is_valid_only_as_evidence(self):
        packet_id = str(uuid4())
        file_id = str(uuid5(UUID(packet_id), "page_0.jpg"))
        content = b"app scan evidence"
        file = dict(id=file_id, originalName="Scan page 1.jpg", storedName=file_id + ".jpg",
                    mediaType="image/jpeg", bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
        manifest = dict(schemaVersion=1, id=packet_id, route="123456", source="app_scan",
                        status="received", createdAt="2026-11-04T20:00:00Z", files=[file])
        self.store.add_original("receipt", packet_id, file, content)
        self.store.put("receipt-upload:" + packet_id, manifest)
        report = dict(schemaVersion=1, route="123456", packetId=packet_id,
                      attemptId=str(uuid4()), createdAt="2026-11-04T20:01:00Z",
                      pipelineVersion="fixture", status="needs_attention",
                      documents=[dict(fileId=file_id, sha256=file["sha256"], status="unsupported",
                                      extraction={"documentType": "unknown", "fields": {}})], proposal=None)
        inbox = evidence.accept_receipt_attempt(self.store, "123456", packet_id, report)
        self.assertEqual(inbox["items"][0]["files"][0]["id"], file_id)

    def test_receipt_agent_review_requires_exact_file_coverage_and_stays_advisory(self):
        packet = evidence.upload(self.store, "123456", "receipt", [
            ("receipt.pdf", "application/pdf", b"%PDF-1.7\nreceipt\n%%EOF")])
        file = packet["files"][0]
        ocr_id = str(uuid4())
        report = dict(schemaVersion=1, route="123456", packetId=packet["id"], attemptId=ocr_id,
                      createdAt="2026-11-04T20:01:00Z", pipelineVersion="fixture", status="needs_attention",
                      documents=[dict(fileId=file["id"], sha256=file["sha256"], status="unsupported",
                                      extraction={"documentType": "unknown", "fields": {}})], proposal=None)
        evidence.accept_receipt_attempt(self.store, "123456", packet["id"], report)
        review = dict(schemaVersion=1, reviewId=str(uuid4()), route="123456", packetId=packet["id"],
                      ocrAttemptId=ocr_id, createdAt="2026-11-04T20:02:00Z", model="fixture-model",
                      confidence=80, confidenceTier="medium", confidenceReasons=["The paid date is not visible."],
                      fileReviews=[dict(fileId=file["id"], sha256=file["sha256"], documentType="invoice",
                                        evidence=["Invoice heading is visible."], issues=["No payment proof is visible."])],
                      proposal=dict(status="needs_review", purchaseCount=1, relationship="invoice_only",
                                    vendor="Example Vendor", amountCents=12500, currency="USD", paymentDate=None,
                                    category="other", paymentMethod=None, invoiceNumber="INV-1", checks={},
                                    issues=["An invoice alone does not prove payment."], agentReview="complete"))
        bad = deepcopy(review)
        bad["fileReviews"][0]["sha256"] = "0" * 64
        with self.assertRaises(HTTPException):
            evidence.accept_receipt_agent_attempt(self.store, "123456", packet["id"], ocr_id, bad)
        evidence.accept_receipt_agent_attempt(self.store, "123456", packet["id"], ocr_id, review)
        self.store.agent_jobs[packet["id"], ocr_id] = {"status": "complete", "review": review}
        inbox = evidence.receipt_inbox(self.store, "123456")
        self.assertEqual(inbox["items"][0]["proposal"]["agentReview"], "complete")
        self.assertEqual(inbox["items"][0]["files"][0]["documentType"], "invoice")
        self.assertEqual(self.domain.load("expenses")["items"], [])


class AccountingHttpTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(accounting.router, prefix="/api")
        self.db = fixture()
        self.app.dependency_overrides[get_firestore] = lambda: self.db
        self.app.dependency_overrides[verify_firebase_token] = lambda: {"uid": "owner"}
        self.client = TestClient(self.app)
        self.stores = {}
        @contextmanager
        def transaction(uid, route):
            original = self.stores.setdefault((uid, route), MemoryStore())
            working = deepcopy(original)
            yield working
            self.stores[uid, route] = working
        self.patcher = patch.object(accounting, "accounting_transaction", transaction)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def request(self, method, path, body=None, rid=None):
        return self.client.request(method, "/api/business/accounting/" + path + "?route=989262", json=body,
            headers={"Idempotency-Key": rid or str(uuid4()), "X-Accounting-Timezone": "America/Denver"})

    def test_legacy_system_payment_notes_are_neutral_on_read_without_writes(self):
        self.request("GET", "money-in")
        store = self.stores[("owner", "989262")]
        legacy = "Confirmed by Gruma payment advice 2600012345. Date is the company payment date."
        payment = dict(id="saved", periodEnd="2026-09-02", amountCents=125025, receivedDate="2026-09-15", method="other", note=legacy, paymentAdvice={"documentNumber": "2600012345"})
        custom = {**payment, "id": "custom", "note": "My Gruma payment note"}
        manual = {k: v for k, v in payment.items() if k != "paymentAdvice"}
        store.put("money-in", dict(schemaVersion=1, route="989262", revision=4, settlements=[], remittances=[payment, custom, manual]))
        before = deepcopy(store.docs)
        history = deepcopy(store.history)
        response = self.request("GET", "money-in")
        self.assertEqual(response.status_code, 200, response.text)
        payments = response.json()["remittances"]
        self.assertEqual(payments[0], {**payment, "note": legacy.replace("Gruma", "company")})
        self.assertEqual(payments[1:], [custom, manual])
        self.assertEqual(self.stores[("owner", "989262")].docs, before)
        self.assertEqual(self.stores[("owner", "989262")].history, history)

    def test_owner_create_replay_and_changed_request_conflict(self):
        rid = str(uuid4())
        body = dict(revision=0, bill=bill())
        first = self.request("POST", "bills", body, rid)
        self.assertEqual(first.status_code, 201, first.text)
        replay = self.request("POST", "bills", body, rid)
        self.assertEqual(first.json(), replay.json())
        self.assertEqual(self.request("POST", "bills", dict(revision=1, bill=bill(vendor="Other")), rid).status_code, 409)
        self.assertEqual(self.request("GET", "expenses").json()["items"], [])

    def test_revocation_and_transfer_deny_reads_writes_and_originals(self):
        self.db.set_document("routes/989262", {"ownerUid": "someone-else"})
        for method, path in [("GET", "expenses"), ("POST", "bills"), ("GET", f"statement-reviews/{uuid4()}/{uuid4()}/original")]:
            self.assertEqual(self.request(method, path, dict(revision=0, bill=bill()) if method == "POST" else None).status_code, 403)
        self.assertEqual(self.stores, {})

    def test_database_failure_is_not_empty_success_or_private_error(self):
        with patch.object(accounting, "accounting_transaction", side_effect=RuntimeError("secret host and password")):
            result = self.request("GET", "expenses")
        self.assertEqual(result.status_code, 503)
        self.assertNotIn("secret", result.text)

    def test_json_size_shape_boolean_revision_and_unknown_fields(self):
        for body in [[], {"revision": True, "bill": bill()}, {"revision": 0, "bill": dict(bill(), owner="other")}, {"revision": 0, "bill": bill(note="x" * 9000)}]:
            self.assertIn(self.request("POST", "bills", body).status_code, (400, 413))
        self.assertEqual(self.request("GET", "bills").json()["items"], [])

    def test_multipart_upload_original_auth_and_duplicate(self):
        pdf = b"%PDF-1.7\nprivate statement\n%%EOF"
        url = "/api/business/accounting/statements?route=989262"
        response = self.client.post(url, files={"files": ("statement.pdf", pdf, "application/pdf")}, headers={"Idempotency-Key": str(uuid4())})
        self.assertEqual(response.status_code, 201, response.text)
        manifest = response.json()
        path = f'statement-reviews/{manifest["id"]}/{manifest["files"][0]["id"]}/original'
        original = self.request("GET", path)
        self.assertEqual(original.content, pdf)
        self.assertEqual(original.headers["cache-control"], "private, no-store")
        duplicate = self.client.post(url, files={"files": ("renamed.pdf", pdf, "application/pdf")}, headers={"Idempotency-Key": str(uuid4())})
        self.assertEqual(duplicate.status_code, 201)
        self.assertEqual(duplicate.json()['duplicateCount'], 1)
        self.assertEqual(self.request("GET", "money-in").json()["remittances"], [])


if __name__ == "__main__":
    unittest.main()
