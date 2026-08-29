from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from order_forecast.api.pcf_runtime_control import (
    BOOTSTRAP_REVISION,
    MAX_CONTROL_BYTES,
    RuntimeControlError,
    control_revision,
    load_runtime_control,
    parse_runtime_control,
)


EVIDENCE = (
    "/Volumes/Extreme SSD/routespark_offload/pcf-migration/"
    "firestore-read-redesign/P2/global/p2-20260829T180000Z/phase-bundle.json"
)


def control_payload(*, stage="off", discovery="legacy", authority="legacy_mac"):
    payload = {
        "contractVersion": "pcf-runtime-control-document-v1",
        "revision": BOOTSTRAP_REVISION,
        "previousRevision": BOOTSTRAP_REVISION,
        "updatedAt": "2026-08-29T18:00:00Z",
        "approvedBy": "release-owner",
        "goDecisionEvidence": EVIDENCE,
        "controls": {
            "PCF_FIRESTORE_DISCOVERY_MODE": discovery,
            "PCF_CLUSTER_STAGE": stage,
            "PCF_FIREBASE_WRITE_AUTHORITY": authority,
        },
    }
    payload["revision"] = control_revision(payload)
    return payload


def test_valid_control_hashes_canonical_document_and_exposes_exact_authority():
    payload = control_payload(stage="projection", discovery="disabled", authority="cluster")
    control = parse_runtime_control(payload)
    assert control.updated_at == datetime(2026, 8, 29, 18, tzinfo=timezone.utc)
    assert control.allows_stage("ingress")
    assert control.allows_stage("projection")
    assert control.allows_ttl_repair


def test_mutation_unknown_keys_and_illegal_projection_combinations_fail_closed():
    changed = control_payload()
    changed["approvedBy"] = "different-owner"
    with pytest.raises(RuntimeControlError, match="invalid"):
        parse_runtime_control(changed)

    extra = control_payload()
    extra["controls"]["PCF_EFFECTS_ENABLED"] = True
    extra["revision"] = control_revision(extra)
    with pytest.raises(RuntimeControlError, match="invalid"):
        parse_runtime_control(extra)

    for discovery, authority in (("bounded", "cluster"), ("disabled", "legacy_mac")):
        with pytest.raises(RuntimeControlError, match="invalid"):
            parse_runtime_control(
                control_payload(
                    stage="projection",
                    discovery=discovery,
                    authority=authority,
                )
            )


def test_nonprojection_states_never_grant_ttl_repair():
    for stage in ("off", "ingress", "shadow", "claims"):
        control = parse_runtime_control(
            control_payload(stage=stage, discovery="disabled", authority="cluster")
        )
        assert not control.allows_ttl_repair


def test_file_reader_rejects_duplicate_keys_invalid_utf8_and_oversize(tmp_path: Path):
    path = tmp_path / "control.json"
    payload = control_payload()
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert load_runtime_control(path).revision == payload["revision"]

    path.write_text('{"revision":"a","revision":"b"}', encoding="utf-8")
    with pytest.raises(RuntimeControlError, match="unavailable"):
        load_runtime_control(path)

    path.write_bytes(b"\xff")
    with pytest.raises(RuntimeControlError, match="unavailable"):
        load_runtime_control(path)

    path.write_bytes(b"x" * (MAX_CONTROL_BYTES + 1))
    with pytest.raises(RuntimeControlError, match="unavailable"):
        load_runtime_control(path)


def test_evidence_reference_and_utc_timestamp_are_strict():
    for field, value in (
        ("goDecisionEvidence", "/tmp/unsigned.json"),
        ("goDecisionEvidence", EVIDENCE.replace("phase-bundle.json", "../escape.json")),
        ("updatedAt", "2026-08-29T18:00:00-06:00"),
        ("approvedBy", " release-owner"),
    ):
        payload = control_payload()
        payload[field] = value
        payload["revision"] = control_revision(payload)
        with pytest.raises(RuntimeControlError, match="invalid"):
            parse_runtime_control(payload)


def test_governing_contract_points_to_the_exact_document_schema():
    contracts = Path(__file__).resolve().parents[2] / "contracts/pcf"
    contract = json.loads((contracts / "runtime-controls-v1.json").read_text(encoding="utf-8"))
    schema = json.loads(
        (contracts / "runtime-control-document-v1.schema.json").read_text(encoding="utf-8")
    )
    assert contract["revisionContract"]["documentSchema"] == (
        "runtime-control-document-v1.schema.json"
    )
    assert schema["properties"]["contractVersion"]["const"] == (
        "pcf-runtime-control-document-v1"
    )
    assert set(schema["required"]) == {
        "contractVersion",
        "revision",
        "previousRevision",
        "updatedAt",
        "approvedBy",
        "goDecisionEvidence",
        "controls",
    }
