from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from order_forecast.api.pcf_effect_control import parse_effect_control
from order_forecast.api.pcf_effect_controller import EffectControllerReport
from order_forecast.api.pcf_effect_service import (
    EffectServiceError,
    PostgresSuccessorGuard,
    run_controlled_effect_cycle,
    validate_service_account_separation,
)
from order_forecast.api.pcf_effect_worker import EffectCycleReport
from order_forecast.api.pcf_runtime_control import control_revision
from order_forecast.api.tests.test_pcf_effect_control import payload as effect_payload


NOW = datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)
EVIDENCE = (
    "/Volumes/Extreme SSD/routespark_offload/pcf-migration/"
    "firestore-read-redesign/P2/988200/test/phase-bundle.json"
)


def runtime_payload(*, stage="projection", discovery="disabled", authority="cluster"):
    payload = {
        "contractVersion": "pcf-runtime-control-document-v1",
        "revision": "0" * 64,
        "previousRevision": "0" * 64,
        "updatedAt": "2026-08-29T23:00:00Z",
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


def write_json(path: Path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_runtime_authority_is_checked_before_effect_control_or_controller(tmp_path: Path):
    runtime = tmp_path / "runtime.json"
    effect = tmp_path / "missing-effect.json"
    write_json(
        runtime,
        runtime_payload(stage="claims", discovery="disabled", authority="cluster"),
    )
    called = []
    with pytest.raises(EffectServiceError, match="authority_denied"):
        run_controlled_effect_cycle(
            object(),
            object(),
            {},
            actor_id="pcf-effect-worker-test",
            runtime_control_path=runtime,
            effect_control_path=effect,
            now_fn=lambda: NOW,
            controller_fn=lambda *_args, **_kwargs: called.append(True),
        )
    assert called == []


def test_controlled_cycle_passes_only_exact_live_ids_and_guarded_claim(tmp_path: Path):
    runtime = tmp_path / "runtime.json"
    effect = tmp_path / "effect.json"
    write_json(runtime, runtime_payload())
    write_json(effect, effect_payload())
    observed = {}
    idle = EffectCycleReport("idle", None, None, None, None, None, None)

    def controller(*_args, **kwargs):
        observed.update(kwargs)
        return EffectControllerReport(idle, "not_terminal", None)

    report = run_controlled_effect_cycle(
        object(),
        object(),
        {},
        actor_id="pcf-effect-worker-test",
        runtime_control_path=runtime,
        effect_control_path=effect,
        now_fn=lambda: NOW,
        controller_fn=controller,
    )
    expected = parse_effect_control(effect_payload()).admitted_work_ids(now=NOW)
    assert observed["admitted_work_ids"] == expected
    assert callable(observed["claim_fn"])
    assert report.admitted_work_count == 1


def test_three_service_accounts_must_be_different_and_same_project(tmp_path: Path):
    paths = tuple(tmp_path / f"{name}.json" for name in ("writer", "verify", "clean"))
    for index, path in enumerate(paths):
        write_json(
            path,
            {
                "project_id": "routespark-test",
                "client_email": f"pcf-{index}@routespark-test.iam.gserviceaccount.com",
            },
        )
    identities = validate_service_account_separation(
        paths, expected_project="routespark-test"
    )
    assert len({item.client_email for item in identities}) == 3

    write_json(
        paths[2],
        {
            "project_id": "routespark-test",
            "client_email": identities[0].client_email,
        },
    )
    with pytest.raises(EffectServiceError, match="not_separate"):
        validate_service_account_separation(paths, expected_project="routespark-test")


class Cursor:
    def __init__(self, row):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params):
        assert "source_revision" in sql
        assert params[0] == "988200"

    def fetchone(self):
        return self.row


class Connection:
    def __init__(self, row):
        self.row = row
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs):
        return Cursor(self.row)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def test_successor_guard_closes_its_transaction_before_external_cleanup():
    from order_forecast.api.tests.test_pcf_effect_worker import claim

    conn = Connection({"successor_exists": False})
    assert PostgresSuccessorGuard(conn)(claim()) is False
    assert (conn.commits, conn.rollbacks) == (1, 0)
