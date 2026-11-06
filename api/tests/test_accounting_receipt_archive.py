"""Receipt archive worker fences: deleted reviews never start paid processing."""
from contextlib import contextmanager
import unittest
from unittest.mock import patch

from order_forecast.api.routers import accounting_worker


class EmptyCursor:
    def __init__(self):
        self.queries = []

    def execute(self, query, params=None):
        self.queries.append((query, params))

    def fetchone(self):
        return None


class ReceiptArchiveWorkerTests(unittest.TestCase):
    def test_ocr_and_agent_claims_exclude_archived_reviews_in_postgres(self):
        for claim in (accounting_worker.claim, accounting_worker.claim_agent):
            with self.subTest(claim=claim.__name__):
                cursor = EmptyCursor()

                @contextmanager
                def transaction():
                    yield cursor

                with patch.object(accounting_worker, "transaction", transaction):
                    self.assertIsNone(claim())
                query = cursor.queries[0][0]
                self.assertIn("accounting_documents", query)
                self.assertIn("receipt-review:", query)
                self.assertIn("archivedAt", query)
                self.assertIn("NOT EXISTS", query)


if __name__ == "__main__":
    unittest.main()
