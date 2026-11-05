"""New issue selection is date-based, scoped, reconciled, and payment-preserving."""
from copy import deepcopy
from unittest import TestCase
from fastapi import HTTPException
from order_forecast.api import accounting_evidence as evidence
from order_forecast.api.accounting_domain import Accounting
from order_forecast.api.tests.test_accounting import MemoryStore, analysis
from order_forecast.api.routers.accounting import dispatch

class AutomaticStatementTests(TestCase):
    def setUp(self):
        self.store=MemoryStore();self.accounting=Accounting(self.store,'123456','2026-09-19');self.counter=0
    def add(self, stamp='09/14/26 19:25', **changes):
        self.counter+=1
        manifest=evidence.upload(self.store,'123456','statement',[(f'{self.counter}.pdf','application/pdf',f'%PDF-1.4\n{self.counter}\n%%EOF'.encode())])
        value=analysis(**changes)
        value['_evidence']={'sourceText':f'Gruma Corporation       {stamp}\nPrinted statement' if stamp else 'Header not read'}
        return evidence.analyze_statement(self.store,'123456',manifest['id'],manifest['files'][0]['id'],reader=lambda *_:deepcopy(value))
    def confirm(self, attempt, version='preliminary'):
        return evidence.confirm_statement(self.accounting,dict(revision=self.accounting.load('money-in')['revision'],uploadId=attempt['uploadId'],fileId=attempt['fileId'],attemptId=attempt['id'],version=version))
    def test_first_statement_requires_review_then_newer_updates_and_retry_is_inert(self):
        first=self.add();self.assertEqual(evidence.sync_statement_updates(self.accounting)['settlements'],[])
        self.confirm(first)
        before=deepcopy(self.store.originals)
        second=self.add('09/18/26 19:10')
        result=evidence.sync_statement_updates(self.accounting)
        self.assertEqual(result['settlements'][0]['source'],'sha256:'+second['sha256'])
        self.assertEqual(result['settlements'][0]['revision'],'final')
        self.assertEqual(result['settlements'][0]['automaticUpdate']['previousIssuedAt'],'2026-09-14T19:25')
        revision=result['revision'];history=len(self.store.history)
        self.assertEqual(evidence.sync_statement_updates(self.accounting)['revision'],revision)
        self.assertEqual(len(self.store.history),history)
        for key,value in before.items():self.assertEqual(self.store.originals[key],value)
        self.assertFalse(result['remittances'])
    def test_changed_amount_updates_statement_and_preserves_actual_payment(self):
        self.confirm(self.add())
        ledger=self.accounting.load('money-in');ledger['remittances']=[{'id':'payment','amountCents':100000,'method':'bank'}];self.store.put('money-in',ledger)
        second=self.add('09/18/26 19:10',marginCents=110000,newSettlementCents=110000,account=dict(previousBalance=0,payment=0,totalNewSettlement=110000,amountToPay=110000))
        result=evidence.sync_statement_updates(self.accounting)
        self.assertEqual(result['remittances'],ledger['remittances'])
        self.assertEqual(result['settlements'][0]['amountToPayCents'],110000)
        self.assertEqual(result['settlements'][0]['automaticUpdate']['previousAmountToPayCents'],100000)
    def test_older_arrivals_never_downgrade_and_later_corrections_can_advance_final(self):
        first=self.add();self.confirm(first);second=self.add('09/18/26 19:10');evidence.sync_statement_updates(self.accounting)
        self.add('09/13/26 10:00');self.assertEqual(evidence.sync_statement_updates(self.accounting)['settlements'][0]['source'],'sha256:'+second['sha256'])
        third=self.add('09/19/26 09:00');self.assertEqual(evidence.sync_statement_updates(self.accounting)['settlements'][0]['source'],'sha256:'+third['sha256'])
    def test_missing_tied_and_unreconciled_issues_stay_for_review(self):
        for scenario in ('missing','tie','math'):
            with self.subTest(scenario=scenario):
                self.setUp();first=self.add();self.confirm(first)
                if scenario=='missing':self.add('')
                elif scenario=='tie':self.add('09/18/26 19:10');self.add('09/18/26 19:10')
                else:self.add('09/18/26 19:10',newSettlementCents=99999)
                result=evidence.sync_statement_updates(self.accounting)
                self.assertEqual(result['settlements'][0]['source'],'sha256:'+first['sha256'])
                self.assertEqual(result['revision'],1)
    def test_same_end_different_start_and_other_route_cannot_replace(self):
        first=self.add();self.confirm(first)
        self.add('09/18/26 19:10',periodStart='2026-09-04')
        self.add('09/19/26 19:10',route='654321')
        self.assertEqual(evidence.sync_statement_updates(self.accounting)['settlements'][0]['source'],'sha256:'+first['sha256'])
    def test_bulk_upload_order_does_not_determine_latest_after_first_review(self):
        final=self.add('09/18/26 19:10');first=self.add('09/14/26 19:25')
        result=self.confirm(first)
        self.assertEqual(result['settlements'][0]['source'],'sha256:'+final['sha256'])
    def test_sync_rejects_supplied_identity_or_facts(self):
        for body in ({'route':'654321'},{'amountToPayCents':1},{'automatic_update':True}):
            with self.assertRaises(HTTPException):dispatch(self.store,'123456','2026-09-19','POST','money-in/sync-statements',body,[],None)
