"""Immutable-result and external-evidence tests for PCF effects."""

from __future__ import annotations

import copy
import hashlib
import tempfile
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from order_forecast.api.pcf_effect_inputs import (
    EffectInputError,
    load_effect_input,
    verify_external_evidence,
)
from order_forecast.api.pcf_effect_runtime import EffectClaim
from order_forecast.api.pcf_worker_contract import canonical_result_identity
from order_forecast.api.tests.test_pcf_worker_contract import (
    DELIVERY,
    ROUTE,
    manifest,
    ocr_context,
    result_payload,
)


NOW = datetime(2026, 8, 29, 23, 30, tzinfo=timezone.utc)
WORK_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")


def stored_result(raw: dict) -> dict:
    digest, retained = canonical_result_identity(raw)
    return {
        "schemaVersion": 1,
        "resultHash": digest,
        "acceptanceEventId": str(
            uuid.UUID("22222222-3333-4444-8555-666666666666")
        ),
        "acceptedAt": NOW.isoformat(),
        "claimTokenHash": "f" * 64,
        "request": retained,
    }


def effect_claim(*, result: dict | None = None) -> EffectClaim:
    return EffectClaim(
        work_id=WORK_ID,
        route=ROUTE,
        delivery=DELIVERY,
        source_queue_path=f"pcf_processing_queue/{ROUTE}/queue/{DELIVERY}",
        source_revision="fsut1|2026-08-29T20:00:00.000000000Z|0",
        input_hash_version="pcf-work-input-v1",
        input_hash="a" * 64,
        step="projecting",
        effect_attempt=1,
        work_attempt=1,
        confidence=96,
        manifest=manifest(),
        ocr_context=ocr_context(),
        result=result or stored_result(result_payload(work_id=WORK_ID)),
        claimed_at=NOW,
        visible_again_at=NOW + timedelta(minutes=2),
    )


def materialized_claim(root: Path) -> EffectClaim:
    raw = result_payload(work_id=WORK_ID)
    run = root / f"{WORK_ID}-attempt-1"
    for index, artifact in enumerate(raw["evidencePackage"]["artifacts"]):
        path = run.joinpath(*artifact["relativePath"].split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        content = f"evidence-{index}-{artifact['role']}".encode()
        path.write_bytes(content)
        artifact["sizeBytes"] = len(content)
        artifact["sha256"] = hashlib.sha256(content).hexdigest()
        if artifact["role"] == "package_manifest":
            raw["evidencePackage"]["manifestSha256"] = artifact["sha256"]
    return effect_claim(result=stored_result(raw))


class PcfEffectInputTests(unittest.TestCase):
    def test_stored_result_revalidates_complete_work_and_evidence_identity(self) -> None:
        value = load_effect_input(effect_claim())
        self.assertEqual(value.work_id, WORK_ID)
        self.assertEqual(value.route, ROUTE)
        self.assertEqual(value.delivery, DELIVERY)
        self.assertEqual(value.loading_date, "08/01/2026")
        self.assertEqual(value.confidence, 96)
        self.assertEqual(len(value.pages), 1)
        self.assertEqual(value.evidence_run_relative_path, f"{WORK_ID}-attempt-1")

    def test_any_stored_identity_or_hash_drift_fails_before_effect_io(self) -> None:
        cases = []
        wrong_work = copy.deepcopy(effect_claim().result)
        wrong_work["request"]["workId"] = str(uuid.uuid4())
        cases.append(wrong_work)
        wrong_hash = copy.deepcopy(effect_claim().result)
        wrong_hash["resultHash"] = "0" * 64
        cases.append(wrong_hash)
        secret_reappeared = copy.deepcopy(effect_claim().result)
        secret_reappeared["request"]["claimToken"] = "secret"
        cases.append(secret_reappeared)
        wrong_attempt = copy.deepcopy(effect_claim().result)
        wrong_attempt["request"]["attemptNumber"] = 2
        cases.append(wrong_attempt)
        for value in cases:
            with self.subTest(keys=value["request"].keys()):
                with self.assertRaisesRegex(EffectInputError, "result_invalid"):
                    load_effect_input(effect_claim(result=value))

    def test_every_exact_artifact_is_rehashed_from_authoritative_mount(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            claim = materialized_claim(root)
            effect_input = load_effect_input(claim)
            proof = verify_external_evidence(
                effect_input,
                mount_root=root,
                now=NOW + timedelta(seconds=5),
            )
            self.assertEqual(proof.work_id, WORK_ID)
            self.assertEqual(proof.artifact_count, 4)
            self.assertRegex(proof.evidence_sha256, r"^[0-9a-f]{64}$")

    def test_missing_tampered_or_symlinked_artifact_fails_closed(self) -> None:
        for mode in ("missing", "tampered", "symlink"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as raw:
                root = Path(raw)
                value = load_effect_input(materialized_claim(root))
                artifact = value.artifacts[0]
                path = root / value.evidence_run_relative_path / artifact.relative_path
                if mode == "missing":
                    path.unlink()
                elif mode == "tampered":
                    path.write_bytes(b"changed")
                else:
                    target = root / "outside-image"
                    target.write_bytes(b"outside")
                    path.unlink()
                    path.symlink_to(target)
                with self.assertRaises(EffectInputError):
                    verify_external_evidence(value, mount_root=root, now=NOW)

    def test_mount_or_run_path_must_exist_as_a_direct_nonsymlink_child(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            missing = base / "missing"
            with self.assertRaisesRegex(EffectInputError, "evidence_unavailable"):
                verify_external_evidence(
                    load_effect_input(effect_claim()),
                    mount_root=missing,
                    now=NOW,
                )


if __name__ == "__main__":
    unittest.main()
