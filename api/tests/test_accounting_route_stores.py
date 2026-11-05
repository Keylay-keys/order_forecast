"""Current route directory is a read-only projection; selections remain explicit."""
from copy import deepcopy
from unittest import TestCase
from fastapi import HTTPException
from order_forecast.api.accounting_domain import Accounting
from order_forecast.api.tests.test_accounting import MemoryStore


class CurrentRouteStoresTests(TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.domain = Accounting(self.store, "123456", "2026-09-19")
        self.stores = [dict(id="one", name="Store One")]

    def read(self):
        return self.domain.route_stores("GET", {}, lambda: deepcopy(self.stores))

    def select(self, revision, selected, handheld):
        return self.domain.route_stores("POST", dict(action="select", revision=revision,
            selectedStoreIds=selected, handheldStoreIds=handheld), lambda: deepcopy(self.stores))

    def test_reads_new_and_renamed_stores_without_saving_or_changing_revision(self):
        self.assertEqual(self.read()["stores"], self.stores)
        self.select(0, ["one"], ["one"])
        before = deepcopy(self.store.docs)
        history = deepcopy(self.store.history)
        self.stores = [dict(id="two", name="Added on phone"), dict(id="one", name="Renamed on phone")]
        result = self.read()
        self.assertEqual(result["stores"], self.stores)
        self.assertEqual(result["revision"], 1)
        self.assertEqual(result["selectedStoreIds"], ["one"])
        self.assertEqual(result["handheldStoreIds"], ["one"])
        self.assertEqual(self.store.docs, before)
        self.assertEqual(self.store.history, history)

    def test_can_select_new_current_store_without_manual_refresh(self):
        self.stores.append(dict(id="new", name="New Store"))
        result = self.select(0, ["new"], ["new"])
        self.assertEqual(result["selectedStoreIds"], ["new"])
        self.assertEqual(result["revision"], 1)
        self.assertEqual(self.domain.load("route-stores")["stores"], result["stores"])

    def test_removed_selected_store_keeps_identity_and_financial_history(self):
        self.select(0, ["one"], ["one"])
        self.store.put("cash-accounts", {"payments": [{"storeId": "one", "amountCents": 12345}]})
        financial = deepcopy(self.store.docs["cash-accounts"])
        self.stores = []
        result = self.read()
        self.assertEqual(result["stores"], [])
        self.assertEqual(result["selectedStoreIds"], ["one"])
        self.assertEqual(self.select(1, ["one"], ["one"])["handheldStoreIds"], ["one"])
        self.assertEqual(self.store.docs["cash-accounts"], financial)

    def test_unselected_removed_or_foreign_store_cannot_be_selected(self):
        self.domain.route_stores("POST", {"action": "refresh"}, lambda: deepcopy(self.stores))
        self.stores = []
        for store_id in ("one", "foreign"):
            with self.assertRaises(HTTPException):
                self.select(1, [store_id], [])
        self.assertEqual(self.domain.load("route-stores")["selectedStoreIds"], [])

    def test_read_never_advances_revision_and_stale_selection_is_rejected(self):
        self.read()
        self.select(0, ["one"], [])
        self.read()
        with self.assertRaises(HTTPException) as error:
            self.select(0, [], [])
        self.assertEqual(error.exception.status_code, 409)

    def test_failed_directory_read_cannot_overwrite_snapshot_or_selections(self):
        self.select(0, ["one"], ["one"])
        before = deepcopy(self.store.docs)
        def unavailable():
            raise RuntimeError("unavailable")
        with self.assertRaises(RuntimeError):
            self.domain.route_stores("GET", {}, unavailable)
        self.stores = [dict(id="duplicate", name="First"), dict(id="duplicate", name="Second")]
        with self.assertRaises(HTTPException):
            self.read()
        self.assertEqual(self.store.docs, before)
