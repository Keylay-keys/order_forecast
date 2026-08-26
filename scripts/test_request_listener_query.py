from __future__ import annotations

import unittest

try:
    from .request_listener_query import pending_request_query
except ImportError:
    from request_listener_query import pending_request_query


class _Query:
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.filters = []

    def where(self, *, filter):
        self.filters.append(filter)
        return self


class _Client:
    def __init__(self):
        self.collections = []

    def collection(self, collection_name: str):
        self.collections.append(collection_name)
        return _Query(collection_name)


class PendingRequestQueryTests(unittest.TestCase):
    def test_delivery_requests_are_filtered_to_pending(self):
        client = _Client()
        query = pending_request_query(client, "deliveryRequests")

        self.assertEqual(client.collections, ["deliveryRequests"])
        self.assertEqual(query.collection_name, "deliveryRequests")
        self.assertEqual(len(query.filters), 1)
        self.assertEqual(query.filters[0].field_path, "status")
        self.assertEqual(query.filters[0].op_string, "==")
        self.assertEqual(query.filters[0].value, "pending")

    def test_order_requests_are_filtered_to_pending(self):
        client = _Client()
        query = pending_request_query(client, "orderRequests")

        self.assertEqual(client.collections, ["orderRequests"])
        self.assertEqual(query.filters[0].value, "pending")

    def test_unknown_collection_fails_before_firestore_access(self):
        client = _Client()
        with self.assertRaisesRegex(ValueError, "unsupported request collection"):
            pending_request_query(client, "users")
        self.assertEqual(client.collections, [])


if __name__ == "__main__":
    unittest.main()
