"""Hermetic exact Firestore projection transaction tests."""

from __future__ import annotations

import copy
import unittest

from order_forecast.api.pcf_effect_firebase import ExactFirestoreProjectionStore
from order_forecast.api.pcf_effect_projection import (
    ProjectionPlanError,
    projection_document_paths,
)
from order_forecast.api.tests.test_pcf_effect_projection import (
    NOW,
    _effect,
    _proofs,
    _snapshot,
)


class _Snapshot:
    def __init__(self, data, *, update_time=NOW):
        self.exists = data is not None
        self._data = copy.deepcopy(data)
        self.update_time = update_time if self.exists else None

    def to_dict(self):
        return copy.deepcopy(self._data)


class _Ref:
    def __init__(self, db, path):
        self.db = db
        self.path = path

    def get(self, transaction=None):
        if transaction is not None:
            return transaction.read(self.path)
        self.db.direct_reads.append(self.path)
        return _Snapshot(self.db.docs.get(self.path))


class _Transaction:
    def __init__(self, db):
        self.db = db
        self.wrote = False

    def read(self, path):
        if self.wrote:
            raise AssertionError("Firestore transaction read occurred after a write")
        self.db.transaction_reads.append(path)
        return _Snapshot(self.db.docs.get(path))

    def set(self, ref, document):
        self.wrote = True
        self.db.writes.append(ref.path)
        self.db.docs[ref.path] = copy.deepcopy(document)


class _Db:
    def __init__(self, docs):
        self.docs = copy.deepcopy(docs)
        self.writes = []
        self.transaction_reads = []
        self.direct_reads = []

    def document(self, path):
        return _Ref(self, path)

    def transaction(self):
        return _Transaction(self)


def _docs(effect, *, existing=False):
    snapshot = _snapshot(effect, existing=existing)
    parent_paths, container_paths = projection_document_paths(effect)
    values = {
        f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}": snapshot.queue,
        f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}/server/admission": snapshot.admission,
    }
    values.update({path: snapshot.parents.get(path) for path in parent_paths})
    values.update({path: snapshot.containers.get(path) for path in container_paths})
    return {path: value for path, value in values.items() if value}


class PcfEffectFirebaseTests(unittest.TestCase):
    def store(self, db):
        return ExactFirestoreProjectionStore(
            db,
            transactional=lambda function: function,
            clock=lambda: NOW,
        )

    def test_transaction_reads_complete_exact_set_before_writing(self):
        effect = _effect()
        db = _Db(_docs(effect, existing=True))
        receipt = self.store(db).apply(effect, _proofs(effect))
        self.assertEqual(receipt.disposition, "applied")
        self.assertEqual(len(db.transaction_reads), 4)
        self.assertEqual(len(db.writes), 3)
        self.assertEqual(
            db.writes[-1],
            f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}",
        )
        self.assertNotIn("pcfQueueExpiresAt", db.docs[db.writes[-1]])

    def test_crash_replay_performs_no_second_write(self):
        effect = _effect()
        db = _Db(_docs(effect))
        first = self.store(db).apply(effect, _proofs(effect))
        self.assertEqual(first.disposition, "applied")
        db.writes.clear()
        second = self.store(db).apply(effect, _proofs(effect))
        self.assertEqual(second.disposition, "already_correct")
        self.assertEqual(db.writes, [])
        self.assertEqual(second.write_set.evidence_sha256, first.write_set.evidence_sha256)

    def test_missing_or_drifted_admission_writes_nothing(self):
        effect = _effect()
        docs = _docs(effect)
        admission_path = (
            f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}/server/admission"
        )
        docs.pop(admission_path)
        db = _Db(docs)
        with self.assertRaisesRegex(ProjectionPlanError, "source_drift"):
            self.store(db).apply(effect, _proofs(effect))
        self.assertEqual(db.writes, [])

    def test_independent_store_reloads_every_exact_written_path(self):
        effect = _effect()
        writer_db = _Db(_docs(effect))
        receipt = self.store(writer_db).apply(effect, _proofs(effect))
        reader_db = _Db(writer_db.docs)
        digest = self.store(reader_db).verify_exact(receipt.write_set, effect)
        self.assertRegex(digest, r"^[0-9a-f]{64}$")
        self.assertEqual(len(reader_db.direct_reads), 4)
        self.assertEqual(reader_db.writes, [])

    def test_independent_readback_rejects_post_write_tamper(self):
        effect = _effect()
        writer_db = _Db(_docs(effect))
        receipt = self.store(writer_db).apply(effect, _proofs(effect))
        container_path = next(iter(receipt.write_set.containers))
        writer_db.docs[container_path]["pages"][0]["items"][0]["product"] = "99999"
        with self.assertRaisesRegex(ProjectionPlanError, "readback_mismatch"):
            self.store(_Db(writer_db.docs)).verify_exact(receipt.write_set, effect)


if __name__ == "__main__":
    unittest.main()
