"""Pure PCF Firebase projection plan and read-back tests."""

from __future__ import annotations

import copy
import unittest
from datetime import timedelta

from order_forecast.api.pcf_effect_inputs import load_effect_input
from order_forecast.api.pcf_effect_projection import (
    ProjectionPlanError,
    ProjectionSnapshot,
    admitted_manifest_hash,
    build_projection_write_set,
    verify_projection_documents,
)
from order_forecast.api.pcf_effect_storage import (
    CanonicalObjectProof,
    canonical_path,
    source_object,
)
from order_forecast.api.tests.test_pcf_effect_inputs import NOW, effect_claim
from order_forecast.api.tests.test_pcf_worker_contract import CONTAINER, DELIVERY, ROUTE


def _effect():
    return load_effect_input(effect_claim())


def _proofs(effect):
    result = []
    for page in effect.pages:
        source = source_object(dict(page), route=effect.route, delivery=effect.delivery)
        destination = canonical_path(source, target_delivery=DELIVERY)
        result.append(
            CanonicalObjectProof(
                source.object_path,
                source.generation,
                destination,
                900 + source.page,
                source.size_bytes,
                source.md5_hash,
                "applied",
                f"{source.page:064x}",
                NOW,
            )
        )
    return tuple(result)


def _admission(effect):
    manifest = [
        {
            "logicalPageKey": str(page["logicalPageKey"]),
            "containerCode": str(page["containerCode"]),
            "pageNumber": int(page["pageNumber"]),
            "storageBucket": str(page["storageBucket"]),
            "storagePath": str(page["storagePath"]),
            "storageGeneration": str(page["storageGeneration"]),
        }
        for page in sorted(effect.pages, key=lambda row: str(row["logicalPageKey"]))
    ]
    return {
        "admissionStatus": "ready",
        "admissionVersion": 1,
        "admissionEpoch": 8,
        "admittedManifest": manifest,
        "admittedManifestHash": admitted_manifest_hash(effect.pages),
    }


def _snapshot(effect, *, existing=False):
    parent_path = f"routes/{ROUTE}/pcfs/{DELIVERY}"
    container_path = f"{parent_path}/containers/{CONTAINER}"
    parent = {
        "containerCount": 1,
        "totalItems": 2,
        "expiredItems": 0,
        "expiringItems": 1,
        "lastItemExpiryDate": "2026-08-31",
        "unrelatedParentField": "keep-parent",
    }
    container = {
        "containerCode": CONTAINER,
        "totalItems": 2,
        "expiredItems": 0,
        "expiringItems": 1,
        "unrelatedContainerField": "keep-container",
        "pages": [
            {
                "pageNumber": 1,
                "items": [
                    {
                        "product": "12345",
                        "visibleOnList": False,
                        "isLowQuantity": True,
                        "guaranteed": {"isGuaranteed": True},
                    }
                ],
            }
        ],
    }
    return ProjectionSnapshot(
        queue={
            "pendingBackendProcessing": True,
            "backendProcessingStatus": "pending",
            "processingStatus": "pending",
            "status": "unprocessed",
            "pcfQueueExpiresAt": NOW + timedelta(days=7),
            "unrelatedQueueField": "keep-queue",
        },
        admission=_admission(effect),
        parents={parent_path: parent} if existing else {},
        containers={container_path: container} if existing else {},
    )


class PcfEffectProjectionTests(unittest.TestCase):
    def test_projection_preserves_known_user_controls_and_unrelated_fields(self):
        effect = _effect()
        write_set = build_projection_write_set(
            effect,
            _snapshot(effect, existing=True),
            _proofs(effect),
            now=NOW + timedelta(hours=1),
        )
        parent_path = f"routes/{ROUTE}/pcfs/{DELIVERY}"
        container_path = f"{parent_path}/containers/{CONTAINER}"
        parent = write_set.parents[parent_path]
        container = write_set.containers[container_path]
        first_item = container["pages"][0]["items"][0]
        self.assertEqual(parent["unrelatedParentField"], "keep-parent")
        self.assertEqual(container["unrelatedContainerField"], "keep-container")
        self.assertIs(first_item["visibleOnList"], False)
        self.assertIs(first_item["isLowQuantity"], True)
        self.assertEqual(first_item["guaranteed"], {"isGuaranteed": True})
        self.assertEqual(write_set.queue["unrelatedQueueField"], "keep-queue")
        self.assertNotIn("pcfQueueExpiresAt", write_set.queue)

    def test_projection_has_expected_app_shape_and_denver_metrics(self):
        effect = _effect()
        write_set = build_projection_write_set(
            effect, _snapshot(effect), _proofs(effect), now=NOW
        )
        container = next(iter(write_set.containers.values()))
        page = container["pages"][0]
        self.assertEqual(container["deliveryNumber"], DELIVERY)
        self.assertEqual(container["loadingDate"], "08/01/2026")
        self.assertEqual(container["totalItems"], 2)
        self.assertEqual(container["expiredItems"], 0)
        self.assertEqual(container["expiringItems"], 1)
        self.assertEqual(container["lastItemExpiryDate"], "2026-08-31")
        self.assertEqual(page["canonicalStoragePath"], page["imageUrl"])
        self.assertEqual(write_set.queue["pageStats"]["processed"], 1)
        self.assertEqual(write_set.queue["backendProcessingStatus"], "done")
        self.assertEqual(write_set.queue["processingStatus"], "processed")

    def test_crash_after_write_replay_is_byte_for_byte_deterministic(self):
        effect = _effect()
        first = build_projection_write_set(
            effect, _snapshot(effect), _proofs(effect), now=NOW
        )
        replay_snapshot = ProjectionSnapshot(
            queue=copy.deepcopy(first.queue),
            admission=_admission(effect),
            parents=copy.deepcopy(first.parents),
            containers=copy.deepcopy(first.containers),
        )
        replay = build_projection_write_set(
            effect,
            replay_snapshot,
            _proofs(effect),
            now=NOW + timedelta(days=2),
        )
        self.assertEqual(replay.queue, first.queue)
        self.assertEqual(replay.parents, first.parents)
        self.assertEqual(replay.containers, first.containers)
        self.assertEqual(replay.evidence_sha256, first.evidence_sha256)

    def test_foreign_terminal_projection_cannot_be_overwritten(self):
        effect = _effect()
        snapshot = _snapshot(effect)
        queue = dict(snapshot.queue)
        queue.update(
            {
                "pendingBackendProcessing": False,
                "backendProcessingStatus": "done",
                "processingStatus": "processed",
                "status": "processed",
                "pcfClusterProjection": {
                    "workId": "11111111-1111-4111-8111-111111111111",
                    "workAttempt": 1,
                    "inputHash": "b" * 64,
                },
            }
        )
        queue.pop("pcfQueueExpiresAt", None)
        with self.assertRaisesRegex(ProjectionPlanError, "source_drift"):
            build_projection_write_set(
                effect,
                ProjectionSnapshot(queue, snapshot.admission, {}, {}),
                _proofs(effect),
                now=NOW,
            )

    def test_admission_or_canonical_drift_fails_before_writes(self):
        effect = _effect()
        for kind in ("admission", "canonical"):
            with self.subTest(kind=kind):
                snapshot = _snapshot(effect)
                proofs = list(_proofs(effect))
                if kind == "admission":
                    admission = copy.deepcopy(snapshot.admission)
                    admission["admittedManifest"][0]["storageGeneration"] = "99"
                    snapshot = ProjectionSnapshot(snapshot.queue, admission, {}, {})
                else:
                    proof = proofs[0]
                    proofs[0] = CanonicalObjectProof(
                        proof.source_path,
                        proof.source_generation,
                        proof.canonical_path + "-wrong",
                        proof.canonical_generation,
                        proof.size_bytes,
                        proof.md5_hash,
                        proof.disposition,
                        proof.evidence_sha256,
                        proof.verified_at,
                    )
                with self.assertRaises(ProjectionPlanError):
                    build_projection_write_set(
                        effect, snapshot, tuple(proofs), now=NOW
                    )

    def test_independent_readback_compares_owned_fields_and_ttl_absence(self):
        effect = _effect()
        expected = build_projection_write_set(
            effect, _snapshot(effect), _proofs(effect), now=NOW
        )
        actual = ProjectionSnapshot(
            queue={**copy.deepcopy(expected.queue), "postWriteUnrelated": True},
            admission=_admission(effect),
            parents=copy.deepcopy(expected.parents),
            containers=copy.deepcopy(expected.containers),
        )
        self.assertRegex(verify_projection_documents(expected, actual), r"^[0-9a-f]{64}$")
        actual.containers[next(iter(actual.containers))]["pages"][0]["items"][0][
            "days"
        ] = "999"
        with self.assertRaisesRegex(ProjectionPlanError, "readback_mismatch"):
            verify_projection_documents(expected, actual)
        actual = ProjectionSnapshot(
            queue={**copy.deepcopy(expected.queue), "pcfQueueExpiresAt": NOW},
            admission=_admission(effect),
            parents=copy.deepcopy(expected.parents),
            containers=copy.deepcopy(expected.containers),
        )
        with self.assertRaisesRegex(ProjectionPlanError, "readback_mismatch"):
            verify_projection_documents(expected, actual)


if __name__ == "__main__":
    unittest.main()
