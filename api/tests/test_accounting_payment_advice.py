"""Synthetic payment-advice facts only; no live financial data or writes."""
from unittest import TestCase

from order_forecast.api import accounting_payment_advice as advice
from order_forecast.api.accounting_domain import Accounting
from order_forecast.api.tests.test_accounting import MemoryStore, analysis
from order_forecast.api import accounting_evidence as evidence
from order_forecast.api.routers.accounting import dispatch

TEXT = """Mission Foods
Payment advice
Document / Date
2600012345 / 09/15/2026
Your account with us
123456
PAYMENT DETAIL
We have cleared the items listed below with document 2600012345.
_________________________________________________________________________
Invoice number    Date      Description           Deductions    Gross
                                                               amount
_________________________________________________________________________
20260902/123456- 09/02/2026 Net Settlement/20260902 0.00 1,250.25
_________________________________________________________________________
SUM TOTAL 0.00 1,250.25
Payment document Date Currency Payment amount
2600012345 09/15/2026 USD *********1,250.25*
"""


class AdviceTests(TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.accounting = Accounting(self.store, "123456", "2026-09-21")
        self.parsed = advice.parse_text(TEXT, "123456")
        self.period = dict(periodStart="2026-08-27", periodEnd="2026-09-02", revision="final",
            source="sha256:" + "f" * 64, amountToPayCents=125025, statementReview={"fileId": "verified"})
        self.store.put("money-in", dict(schemaVersion=1, route="123456", revision=0, settlements=[self.period], remittances=[]))

    def save(self, changes=None, content=b"synthetic pdf"):
        return advice.save(self.store, {**self.parsed, **(changes or {})}, "synthetic-message", content)

    def test_legacy_arrival_copy_is_neutral_without_rewriting_evidence(self):
        from copy import deepcopy
        self.save()
        ledger = advice.reconcile(self.accounting)
        self.assertNotIn("Gruma", ledger["remittances"][0]["note"])
        record = self.store.get(advice.PREFIX + "2600012345")
        self.assertNotIn("Gruma", record["detail"])
        record["detail"] = "Payment recorded automatically from a matching Gruma payment advice."
        records = {record["documentNumber"]: record}
        before = deepcopy(records)
        result = {"paymentAdviceDocuments": [record["documentNumber"]], "paymentAdviceRemovedDocuments": [record["documentNumber"]]}
        status, projected = advice.arrival_status(result, records)
        self.assertEqual(status, "ready")
        self.assertNotIn("Gruma", projected["detail"])
        self.assertIn("Matched payment-advice PDFs were removed", projected["detail"])
        self.assertEqual(records, before)
        record["detail"] = "Amounts do not match. Review the payment."
        record["status"] = "needs_attention"
        self.assertIn(record["detail"], advice.arrival_status(result, records)[1]["detail"])

    def test_reads_payment_date_separately_from_settlement_reference(self):
        self.assertEqual(self.parsed["paymentDate"], "2026-09-15")
        self.assertEqual(self.parsed["periodEnd"], "2026-09-02")
        self.assertEqual(self.parsed["amountCents"], 125025)

    def test_confirm_endpoint_reconciles_pending_advice_and_get_remains_read_only(self):
        self.store.put("money-in", dict(schemaVersion=1, route="123456", revision=0, settlements=[], remittances=[]))
        self.save()
        manifest = evidence.upload(self.store, "123456", "statement", [("fixture.pdf", "application/pdf", b"%PDF-1.7\nfixture\n%%EOF")])
        facts = analysis(periodStart="2026-08-27", periodEnd="2026-09-02", marginCents=125025, newSettlementCents=125025,
            account=dict(previousBalance=0, payment=0, totalNewSettlement=125025, amountToPay=125025))
        attempt = evidence.analyze_statement(self.store, "123456", manifest["id"], manifest["files"][0]["id"], reader=lambda *_: facts)
        writes = len(self.store.history)
        self.assertEqual(dispatch(self.store, "123456", "2026-09-21", "GET", "money-in", {}, [], None)["remittances"], [])
        self.assertEqual(len(self.store.history), writes)
        body = dict(revision=0, uploadId=attempt["uploadId"], fileId=attempt["fileId"], attemptId=attempt["id"], version="final")
        ledger = dispatch(self.store, "123456", "2026-09-21", "POST", "money-in/confirm-statement", body, [], None)
        self.assertEqual(ledger["revision"], 2)
        self.assertEqual(len(ledger["remittances"]), 1)
        self.assertEqual(ledger["remittances"][0]["amountCents"], 125025)
        self.assertEqual(dispatch(self.store, "123456", "2026-09-21", "POST", "money-in/sync-statements", {}, [], None), ledger)

    def test_mismatches_unsupported_rows_currency_dates_and_math_fail_closed(self):
        for old, new in [("Your account with us\n123456", "Your account with us\n999999"),
                         ("/123456-", "/999999-"), ("Net Settlement/20260902", "Net Settlement/20260909"),
                         ("USD", "CAD"), ("09/15/2026", "02/30/2026"),
                         ("SUM TOTAL 0.00 1,250.25", "SUM TOTAL 0.00 1,250.24"),
                         ("SUM TOTAL 0.00", "SUM TOTAL 1.00"),
                         ("*********1,250.25*", "*********1,250.24*"),
                         ("document 2600012345.", "document 2600012346."),
                         ("Net Settlement/20260902 0.00", "Net Settlement/20260902 1.00"),
                         ("SUM TOTAL", "20260902/123456- 09/02/2026 Net Settlement/20260902 0.00 1,250.25\nSUM TOTAL")]:
            with self.subTest(old=old), self.assertRaises(ValueError):
                advice.parse_text(TEXT.replace(old, new), "123456")

    def test_exact_match_records_once_with_company_date_and_evidence(self):
        self.save()
        ledger = advice.reconcile(self.accounting)
        self.assertEqual(len(ledger["remittances"]), 1)
        payment = ledger["remittances"][0]
        self.assertEqual((payment["receivedDate"], payment["amountCents"], payment["method"]), ("2026-09-15", 125025, "other"))
        self.assertEqual(payment["paymentAdvice"]["documentNumber"], "2600012345")
        self.save(content=b"a re-export with the same verified facts")
        self.assertEqual(advice.reconcile(self.accounting), ledger)
        self.assertEqual(ledger["revision"], 1)

    def test_existing_payment_is_never_changed_or_duplicated(self):
        for amount in (125025, 100000):
            payment = dict(id="manual", periodEnd="2026-09-02", receivedDate="2026-09-16", amountCents=amount, method="bank", note="User note")
            ledger = self.accounting.load("money-in")
            ledger["remittances"] = [payment]
            self.store.put("money-in", ledger)
            self.store.docs.pop(advice.PREFIX + "2600012345", None)
            self.save()
            self.assertEqual(advice.reconcile(self.accounting), ledger)

    def test_waits_for_final_and_matches_when_statement_is_confirmed_later(self):
        ledger = self.accounting.load("money-in")
        ledger["settlements"][0]["revision"] = "preliminary"
        self.store.put("money-in", ledger)
        self.save()
        self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])
        ledger["settlements"][0]["revision"] = "final"
        self.store.put("money-in", ledger)
        self.assertEqual(len(advice.reconcile(self.accounting)["remittances"]), 1)

    def test_mismatch_missing_evidence_future_date_and_wrong_route_do_not_record(self):
        for change in ({"amountCents": 125026}, {"paymentDate": "2026-09-22"}, {"route": "999999"}):
            self.store.docs.pop(advice.PREFIX + "2600012345", None)
            self.save(change)
            self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])
        self.store.docs.pop(advice.PREFIX + "2600012345", None)
        self.save()
        ledger = self.accounting.load("money-in")
        ledger["settlements"][0].pop("statementReview")
        self.store.put("money-in", ledger)
        self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])

    def test_conflicting_document_and_two_documents_for_one_period_require_review(self):
        self.save()
        with self.assertRaises(ValueError):
            self.save({"amountCents": 125026})
        self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])
        self.assertEqual(self.store.get(advice.PREFIX + "2600012345")["status"], "conflict")
        self.save({"documentNumber": "2600012346"})
        self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])

    def test_replay_does_not_recreate_a_payment_the_user_removed(self):
        self.save()
        ledger = advice.reconcile(self.accounting)
        ledger["remittances"] = []
        self.store.put("money-in", ledger)
        self.save()
        self.assertEqual(advice.reconcile(self.accounting)["remittances"], [])

    def test_corrected_payment_no_longer_claims_an_exact_advice_match(self):
        self.save()
        ledger = advice.reconcile(self.accounting)
        payment = ledger["remittances"][0]
        value = {k: payment[k] for k in ("periodEnd", "receivedDate", "amountCents", "method", "note")}
        value["amountCents"] += 1
        corrected = self.accounting.remittances("PATCH", payment["id"], {"revision": ledger["revision"], "remittance": value})
        self.assertNotIn("paymentAdvice", corrected["remittances"][0])

    def test_arrival_projection_changes_from_waiting_to_ready_after_later_matching(self):
        self.save()
        result = {"paymentAdviceDocuments": ["2600012345"], "paymentAdviceErrors": []}
        records = {"2600012345": self.store.get(advice.PREFIX + "2600012345")}
        self.assertEqual(advice.arrival_status(result, records)[0], "needs_attention")
        advice.reconcile(self.accounting)
        records["2600012345"] = self.store.get(advice.PREFIX + "2600012345")
        status, projected = advice.arrival_status(result, records)
        self.assertEqual(status, "ready")
        self.assertEqual(projected["paymentAdviceRecorded"], 1)
        self.assertEqual(advice.arrival_status({**result, "statementReadErrors": 1}, records)[0], "needs_attention")
