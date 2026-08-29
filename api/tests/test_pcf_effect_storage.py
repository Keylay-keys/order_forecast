"""Hermetic exact-generation PCF Storage effect tests."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from google.api_core.exceptions import PreconditionFailed

from order_forecast.api.pcf_effect_storage import (
    ExactPcfObjectStore,
    StorageEffectError,
    canonical_path,
    source_object,
)
from order_forecast.api.pcf_work_runtime import EXPECTED_BUCKET
from order_forecast.api.tests.test_pcf_worker_contract import (
    DELIVERY,
    ROUTE,
    manifest,
)


NOW = datetime(2026, 8, 29, 18, 0, tzinfo=timezone.utc)


class _Blob:
    def __init__(self, bucket, name, generation, size, md5_hash):
        self.bucket = bucket
        self.name = name
        self.generation = generation
        self.size = size
        self.md5_hash = md5_hash

    def delete(self, *, if_generation_match):
        self.bucket.delete_calls.append((self.name, if_generation_match))
        if self.bucket.delete_race is not None:
            replacement = self.bucket.delete_race
            self.bucket.objects[self.name] = replacement
            self.bucket.versions[(self.name, replacement.generation)] = replacement
            raise PreconditionFailed("simulated delete race")
        current = self.bucket.objects.get(self.name)
        if current is None or current.generation != if_generation_match:
            raise PreconditionFailed("generation changed")
        self.bucket.objects.pop(self.name)
        self.bucket.versions.pop((self.name, self.generation), None)


class _Bucket:
    name = EXPECTED_BUCKET

    def __init__(self):
        self.objects = {}
        self.versions = {}
        self.copy_calls = []
        self.delete_calls = []
        self.copy_race = None
        self.delete_race = None
        self.next_generation = 1000

    def add(self, name, generation, size=1200, md5_hash="YWJjZGVmMDEyMzQ1Njc4OQ=="):
        blob = _Blob(self, name, generation, size, md5_hash)
        self.objects[name] = blob
        self.versions[(name, generation)] = blob
        return blob

    def get_blob(self, name, *, generation=None):
        if generation is not None:
            return self.versions.get((name, generation))
        return self.objects.get(name)

    def copy_blob(
        self,
        source,
        destination_bucket,
        destination,
        *,
        source_generation,
        if_source_generation_match,
        if_generation_match,
    ):
        self.copy_calls.append(
            {
                "source": source.name,
                "destination": destination,
                "sourceGeneration": source_generation,
                "sourceMatch": if_source_generation_match,
                "destinationMatch": if_generation_match,
            }
        )
        if self.copy_race is not None:
            raced = self.copy_race
            self.objects[destination] = raced
            self.versions[(destination, raced.generation)] = raced
            raise PreconditionFailed("simulated copy race")
        current = self.objects.get(destination)
        observed_generation = 0 if current is None else current.generation
        if (
            destination_bucket is not self
            or source.generation != source_generation
            or source.generation != if_source_generation_match
            or observed_generation != if_generation_match
        ):
            raise PreconditionFailed("copy precondition")
        self.next_generation += 1
        return self.add(
            destination,
            self.next_generation,
            size=source.size,
            md5_hash=source.md5_hash,
        )


def _source():
    raw = manifest()["objects"][0]
    normalized = {
        "containerCode": raw["containerCode"],
        "pageNumber": raw["uploadPageNumber"],
        "storagePath": raw["objectPath"],
        "storageGeneration": raw["generation"],
        "size": raw["size"],
        "md5Hash": raw["md5Hash"],
    }
    return source_object(normalized, route=ROUTE, delivery=DELIVERY)


class PcfEffectStorageTests(unittest.TestCase):
    def store(self, bucket):
        return ExactPcfObjectStore(bucket, clock=lambda: NOW)

    def test_manifest_source_and_destination_are_identity_bound(self):
        source = _source()
        expected = f"routes/{ROUTE}/pcfs/{DELIVERY}/{source.container}/page_1.jpg"
        self.assertEqual(canonical_path(source, target_delivery=DELIVERY), expected)
        raw = manifest()["objects"][0]
        normalized = {
            "containerCode": raw["containerCode"],
            "pageNumber": raw["uploadPageNumber"],
            "storagePath": raw["objectPath"],
            "storageGeneration": raw["generation"],
            "size": raw["size"],
            "md5Hash": raw["md5Hash"],
        }
        with self.assertRaisesRegex(StorageEffectError, "source_invalid"):
            source_object(normalized, route="989262", delivery=DELIVERY)
        with self.assertRaisesRegex(StorageEffectError, "destination_invalid"):
            canonical_path(source, target_delivery="not-a-delivery")

    def test_copy_uses_both_generation_preconditions_and_reads_back(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        proof = self.store(bucket).ensure_canonical_copy(
            source, target_delivery=DELIVERY
        )
        self.assertEqual(proof.disposition, "applied")
        self.assertEqual(len(bucket.copy_calls), 1)
        self.assertEqual(bucket.copy_calls[0]["sourceMatch"], source.generation)
        self.assertEqual(bucket.copy_calls[0]["destinationMatch"], 0)
        self.assertRegex(proof.evidence_sha256, r"^[0-9a-f]{64}$")

    def test_retry_accepts_only_an_already_correct_destination(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        destination = canonical_path(source, target_delivery=DELIVERY)
        bucket.add(
            destination, 800, size=source.size_bytes, md5_hash=source.md5_hash
        )
        proof = self.store(bucket).ensure_canonical_copy(
            source, target_delivery=DELIVERY
        )
        self.assertEqual(proof.disposition, "already_correct")
        self.assertEqual(bucket.copy_calls, [])

    def test_wrong_destination_is_overwritten_with_observed_generation_cas(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        destination = canonical_path(source, target_delivery=DELIVERY)
        bucket.add(destination, 700, size=11, md5_hash="eHh4eHh4eHh4eHh4eHh4eA==")
        proof = self.store(bucket).ensure_canonical_copy(
            source, target_delivery=DELIVERY
        )
        self.assertEqual(proof.disposition, "applied")
        self.assertEqual(bucket.copy_calls[0]["destinationMatch"], 700)

    def test_copy_race_accepts_matching_bytes_but_rejects_other_bytes(self):
        for correct in (True, False):
            with self.subTest(correct=correct):
                bucket = _Bucket()
                source = _source()
                bucket.add(
                    source.object_path,
                    source.generation,
                    size=source.size_bytes,
                    md5_hash=source.md5_hash,
                )
                destination = canonical_path(source, target_delivery=DELIVERY)
                bucket.copy_race = _Blob(
                    bucket,
                    destination,
                    901,
                    source.size_bytes if correct else 99,
                    source.md5_hash if correct else "eHh4eHh4eHh4eHh4eHh4eA==",
                )
                if correct:
                    result = self.store(bucket).ensure_canonical_copy(
                        source, target_delivery=DELIVERY
                    )
                    self.assertEqual(result.disposition, "already_correct")
                else:
                    with self.assertRaisesRegex(
                        StorageEffectError, "destination_conflict"
                    ):
                        self.store(bucket).ensure_canonical_copy(
                            source, target_delivery=DELIVERY
                        )

    def test_cleanup_deletes_only_the_frozen_current_generation(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        proof = self.store(bucket).cleanup_source(source)
        self.assertEqual(proof.disposition, "applied")
        self.assertEqual(bucket.delete_calls, [(source.object_path, source.generation)])
        self.assertIsNone(bucket.get_blob(source.object_path))

    def test_cleanup_never_deletes_a_newer_generation(self):
        bucket = _Bucket()
        source = _source()
        newer = source.generation + 1
        bucket.add(
            source.object_path,
            newer,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        proof = self.store(bucket).cleanup_source(source)
        self.assertEqual(proof.disposition, "superseded_input")
        self.assertEqual(proof.current_generation, newer)
        self.assertEqual(bucket.delete_calls, [])

    def test_cleanup_delete_race_leaves_successor_and_skips(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        bucket.delete_race = _Blob(
            bucket,
            source.object_path,
            source.generation + 2,
            source.size_bytes,
            source.md5_hash,
        )
        proof = self.store(bucket).cleanup_source(source)
        self.assertEqual(proof.disposition, "superseded_input")
        self.assertEqual(proof.current_generation, source.generation + 2)

    def test_missing_source_is_idempotently_already_absent(self):
        proof = self.store(_Bucket()).cleanup_source(_source())
        self.assertEqual(proof.disposition, "already_absent")

    def test_independent_canonical_recheck_detects_tampering(self):
        bucket = _Bucket()
        source = _source()
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        store = self.store(bucket)
        proof = store.ensure_canonical_copy(source, target_delivery=DELIVERY)
        canonical = bucket.get_blob(
            proof.canonical_path, generation=proof.canonical_generation
        )
        canonical.md5_hash = "eHh4eHh4eHh4eHh4eHh4eA=="
        with self.assertRaisesRegex(StorageEffectError, "canonical_unverified"):
            store.verify_canonical(proof)

    def test_read_only_proofs_do_not_copy_or_delete(self):
        bucket = _Bucket()
        source = _source()
        destination = canonical_path(source, target_delivery=DELIVERY)
        bucket.add(
            source.object_path,
            source.generation,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        bucket.add(
            destination,
            777,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        store = self.store(bucket)
        self.assertEqual(
            store.prove_canonical(source, target_delivery=DELIVERY).canonical_generation,
            777,
        )
        self.assertEqual(store.preflight_source(source).disposition, "current")
        self.assertEqual(bucket.copy_calls, [])
        self.assertEqual(bucket.delete_calls, [])

    def test_preflight_classifies_newer_or_absent_before_any_delete(self):
        source = _source()
        bucket = _Bucket()
        self.assertEqual(
            self.store(bucket).preflight_source(source).disposition,
            "already_absent",
        )
        bucket.add(
            source.object_path,
            source.generation + 1,
            size=source.size_bytes,
            md5_hash=source.md5_hash,
        )
        proof = self.store(bucket).preflight_source(source)
        self.assertEqual(proof.disposition, "superseded_input")
        self.assertEqual(proof.current_generation, source.generation + 1)
        self.assertEqual(bucket.delete_calls, [])

    def test_bucket_contract_fails_closed(self):
        bucket = _Bucket()
        bucket.name = "another-project.appspot.com"
        with self.assertRaises(ValueError):
            ExactPcfObjectStore(bucket)


if __name__ == "__main__":
    unittest.main()
