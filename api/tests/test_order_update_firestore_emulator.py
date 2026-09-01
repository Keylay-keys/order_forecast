"""Real Firestore-emulator contention tests for web order patch transactions."""

from __future__ import annotations

import os
import threading
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from fastapi import HTTPException
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore

from order_forecast.api.models import OrderItem, OrderUpdateRequest, StoreOrder
from order_forecast.api.routers import orders


ROUTE = "988200"
ORDER_ID = "order-988200-web-save-contention-test"
PROJECT = "routespark-web-save-contention-test"


def _emulator_is_loopback() -> bool:
    value = os.environ.get("FIRESTORE_EMULATOR_HOST", "")
    host, separator, port = value.rpartition(":")
    return bool(separator and port.isdigit() and host in {"127.0.0.1", "localhost", "::1"})


@unittest.skipUnless(_emulator_is_loopback(), "requires a loopback Firestore emulator")
class OrderUpdateFirestoreEmulatorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = firestore.Client(project=PROJECT, credentials=AnonymousCredentials())
        cls.ref = cls.db.collection("routes").document(ROUTE).collection("orders").document(ORDER_ID)

    def setUp(self):
        self.ref.set({
            "id": ORDER_ID,
            "routeNumber": ROUTE,
            "expectedDeliveryDate": "2026-09-03",
            "scheduleKey": "thursday",
            "status": "draft",
            "createdAt": datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc),
            "updatedAt": datetime(2026, 8, 31, 12, 0, tzinfo=timezone.utc),
            "submittedAt": None,
            "stores": [{
                "storeId": "store-1",
                "storeName": "Store 1",
                "items": [{"sap": "base", "quantity": 1, "source": "existing"}],
            }],
            "sapOrder": ["base", "100", "200", "moved"],
            "orderRevision": 1,
        })

    def tearDown(self):
        self.ref.delete()

    def _update(self, sap: str, quantity: int):
        return orders._update_order_document(
            self.db.transaction(),
            order_ref=self.ref,
            route_number=ROUTE,
            payload=OrderUpdateRequest(stores=[StoreOrder(
                storeId="store-1",
                storeName="Store 1",
                items=[OrderItem(sap=sap, quantity=quantity)],
            )]),
            now=datetime(2026, 8, 31, 20, 0, tzinfo=timezone.utc),
        )

    @staticmethod
    def _items_by_sap(document):
        store = next(store for store in document["stores"] if store["storeId"] == "store-1")
        return {item["sap"]: item for item in store["items"]}

    def test_disjoint_concurrent_patches_both_survive_retry(self):
        first_has_read = threading.Event()
        errors = []
        original_merge = orders.merge_order_store_patches
        first_merge = True

        def delayed_first_merge(existing, incoming):
            nonlocal first_merge
            if first_merge:
                first_merge = False
                first_has_read.set()
                time.sleep(0.25)
            return original_merge(existing, incoming)

        def worker(sap, quantity):
            try:
                self._update(sap, quantity)
            except Exception as error:  # pragma: no cover - asserted below
                errors.append(error)

        with patch.object(orders, "merge_order_store_patches", delayed_first_merge):
            first = threading.Thread(target=worker, args=("100", 4), daemon=True)
            second = threading.Thread(target=worker, args=("200", 7), daemon=True)
            first.start()
            self.assertTrue(first_has_read.wait(10))
            second.start()
            first.join(15)
            second.join(15)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(errors, [])
        items = self._items_by_sap(self.ref.get().to_dict())
        self.assertEqual(items["base"]["source"], "existing")
        self.assertEqual(items["100"]["quantity"], 4)
        self.assertEqual(items["200"]["quantity"], 7)

    def test_delayed_autosave_cannot_overwrite_finalization(self):
        finalizer_has_read = threading.Event()
        autosave_errors = []
        finalize_errors = []

        @firestore.transactional
        def finalize(transaction):
            snapshot = self.ref.get(transaction=transaction)
            current = snapshot.to_dict()
            finalizer_has_read.set()
            time.sleep(0.25)
            transaction.update(self.ref, {
                "status": "finalized",
                "submittedAt": datetime(2026, 8, 31, 20, 1, tzinfo=timezone.utc),
                "orderRevision": max(2, int(current.get("orderRevision") or 1) + 1),
            })

        def autosave_worker():
            try:
                self._update("100", 4)
            except Exception as error:
                autosave_errors.append(error)

        def finalize_worker():
            try:
                finalize(self.db.transaction())
            except Exception as error:  # pragma: no cover - asserted below
                finalize_errors.append(error)

        finalizer = threading.Thread(target=finalize_worker, daemon=True)
        finalizer.start()
        self.assertTrue(finalizer_has_read.wait(10))
        autosave = threading.Thread(target=autosave_worker, daemon=True)
        autosave.start()
        autosave.join(15)
        finalizer.join(15)

        self.assertFalse(autosave.is_alive())
        self.assertFalse(finalizer.is_alive())
        self.assertEqual(finalize_errors, [])
        self.assertEqual(len(autosave_errors), 1)
        self.assertIsInstance(autosave_errors[0], HTTPException)
        self.assertEqual(autosave_errors[0].status_code, 400)
        document = self.ref.get().to_dict()
        self.assertEqual(document["status"], "finalized")
        self.assertGreaterEqual(document["orderRevision"], 2)

    def test_delayed_autosave_retries_over_latest_reallocation(self):
        reallocation_has_read = threading.Event()
        errors = []

        @firestore.transactional
        def reallocate(transaction):
            snapshot = self.ref.get(transaction=transaction)
            current = snapshot.to_dict()
            reallocation_has_read.set()
            time.sleep(0.25)
            stores = current.get("stores") or []
            store = next(item for item in stores if item.get("storeId") == "store-1")
            items = [item for item in store.get("items") or [] if item.get("sap") != "moved"]
            items.append({"sap": "moved", "quantity": 9, "source": "reallocation"})
            next_stores = [
                {**item, "items": items} if item.get("storeId") == "store-1" else item
                for item in stores
            ]
            transaction.update(self.ref, {
                "stores": next_stores,
                "orderRevision": max(2, int(current.get("orderRevision") or 1) + 1),
            })

        def autosave_worker():
            try:
                self._update("100", 4)
            except Exception as error:  # pragma: no cover - asserted below
                errors.append(error)

        def reallocation_worker():
            try:
                reallocate(self.db.transaction())
            except Exception as error:  # pragma: no cover - asserted below
                errors.append(error)

        reallocation = threading.Thread(target=reallocation_worker, daemon=True)
        reallocation.start()
        self.assertTrue(reallocation_has_read.wait(10))
        autosave = threading.Thread(target=autosave_worker, daemon=True)
        autosave.start()
        autosave.join(15)
        reallocation.join(15)

        self.assertFalse(autosave.is_alive())
        self.assertFalse(reallocation.is_alive())
        self.assertEqual(errors, [])
        document = self.ref.get().to_dict()
        items = self._items_by_sap(document)
        self.assertEqual(document["orderRevision"], 2)
        self.assertEqual(items["moved"], {
            "sap": "moved",
            "quantity": 9,
            "source": "reallocation",
        })
        self.assertEqual(items["100"]["quantity"], 4)


if __name__ == "__main__":
    unittest.main()
