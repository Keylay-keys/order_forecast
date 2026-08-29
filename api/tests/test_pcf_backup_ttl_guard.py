from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

from order_forecast.api.pcf_queue_ttl_guard import QueueTtlGuardReport
from order_forecast.api.pcf_queue_ttl_arming import QueueTtlArmSweepReport
from order_forecast.api.pcf_work_runtime import BackupAcceptance
from order_forecast.api.routers import pcf_internal
from order_forecast.api.tests.test_pcf_runtime_control import control_payload
from order_forecast.api.pcf_runtime_control import parse_runtime_control


RAW_RECEIPT = {
    "backupId": "routespark-nightly-20260829",
    "dumpName": "routespark_nightly_20260829T021500Z.dump",
    "sha256": "f" * 64,
    "sizeBytes": 1436256,
    "coversThrough": "2026-08-29T02:14:00Z",
    "verifiedAt": "2026-08-29T03:00:00Z",
    "offBoxTargets": [
        "dell:/srv/routespark/backups/postgres",
        "dell:/mnt/archive/backups/postgres",
    ],
    "restoreInventoryVerified": True,
}


def _request(*, stage: str, discovery: str, authority: str):
    control = parse_runtime_control(
        control_payload(stage=stage, discovery=discovery, authority=authority)
    )
    return SimpleNamespace(state=SimpleNamespace(pcf_runtime_control=control))


def _acceptance(*, replay=False):
    return BackupAcceptance(
        event_id=uuid.UUID("123e4567-e89b-42d3-a456-426614174000"),
        accepted_at=datetime(2026, 8, 29, 3, 1, tzinfo=timezone.utc),
        idempotent_replay=replay,
    )


@pytest.mark.anyio
async def test_ingress_stage_commits_receipt_without_firebase_ttl_work():
    conn = Mock()
    request = _request(stage="ingress", discovery="legacy", authority="legacy_mac")
    with patch.object(
        pcf_internal, "_worker_principal", return_value=SimpleNamespace(principal="backup")
    ), patch.object(
        pcf_internal, "_read_strict_json", new=AsyncMock(return_value=RAW_RECEIPT)
    ), patch.object(
        pcf_internal, "get_pcf_pg_connection", return_value=conn
    ), patch.object(
        pcf_internal, "return_pcf_pg_connection"
    ), patch.object(
        pcf_internal, "accept_backup_receipt", return_value=_acceptance()
    ) as accept, patch.object(
        pcf_internal, "run_queue_ttl_guard_cycle"
    ) as guard, patch.object(
        pcf_internal, "run_queue_ttl_arm_sweep"
    ) as arming, patch.object(
        pcf_internal, "get_firestore"
    ) as firestore:
        response = await pcf_internal.verify_offbox_backup(request)

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body["queueTtlGuard"] == {"executed": False}
    assert body["queueTtlArming"] == {"executed": False}
    accept.assert_called_once()
    conn.commit.assert_called_once()
    guard.assert_not_called()
    arming.assert_not_called()
    firestore.assert_not_called()


@pytest.mark.anyio
@pytest.mark.parametrize("replay", [False, True])
async def test_projection_stage_runs_guard_after_committed_new_or_replayed_receipt(replay):
    conn = Mock()
    db = object()
    request = _request(stage="projection", discovery="disabled", authority="cluster")
    report = QueueTtlGuardReport(3, 1, 1, 3, 3)
    arm_report = QueueTtlArmSweepReport(4, 2, 1, 1, 1)
    order = []
    conn.commit.side_effect = lambda: order.append("receipt_commit")

    def run_guard(observed_conn, observed_db):
        assert observed_conn is conn
        assert observed_db is db
        order.append("ttl_guard")
        return report

    def run_arming(observed_conn, observed_db, **kwargs):
        assert observed_conn is conn
        assert observed_db is db
        assert kwargs["actor_id"] == "backup"
        order.append("ttl_arming")
        return arm_report

    with patch.object(
        pcf_internal, "_worker_principal", return_value=SimpleNamespace(principal="backup")
    ), patch.object(
        pcf_internal, "_read_strict_json", new=AsyncMock(return_value=RAW_RECEIPT)
    ), patch.object(
        pcf_internal, "get_pcf_pg_connection", return_value=conn
    ), patch.object(
        pcf_internal, "return_pcf_pg_connection"
    ), patch.object(
        pcf_internal, "accept_backup_receipt", return_value=_acceptance(replay=replay)
    ), patch.object(
        pcf_internal, "get_firestore", return_value=db
    ), patch.object(
        pcf_internal, "run_queue_ttl_guard_cycle", side_effect=run_guard
    ), patch.object(
        pcf_internal, "run_queue_ttl_arm_sweep", side_effect=run_arming
    ):
        response = await pcf_internal.verify_offbox_backup(request)

    assert response.status_code == 200
    body = json.loads(response.body)
    assert body["idempotentReplay"] is replay
    assert body["queueTtlGuard"] == {
        "executed": True,
        "candidateCount": 3,
        "verifiedDisarms": 3,
    }
    assert body["queueTtlArming"] == {
        "executed": True,
        "candidateCount": 4,
        "armedCount": 2,
        "replayCount": 1,
        "heldCount": 1,
        "deferredCount": 1,
    }
    assert order == ["receipt_commit", "ttl_guard", "ttl_arming"]
    conn.rollback.assert_called_once()


@pytest.mark.anyio
async def test_guard_failure_preserves_receipt_and_returns_retryable_error():
    conn = Mock()
    request = _request(stage="projection", discovery="disabled", authority="cluster")
    with patch.object(
        pcf_internal, "_worker_principal", return_value=SimpleNamespace(principal="backup")
    ), patch.object(
        pcf_internal, "_read_strict_json", new=AsyncMock(return_value=RAW_RECEIPT)
    ), patch.object(
        pcf_internal, "get_pcf_pg_connection", return_value=conn
    ), patch.object(
        pcf_internal, "return_pcf_pg_connection"
    ), patch.object(
        pcf_internal, "accept_backup_receipt", return_value=_acceptance()
    ), patch.object(
        pcf_internal, "get_firestore", return_value=object()
    ), patch.object(
        pcf_internal,
        "run_queue_ttl_guard_cycle",
        side_effect=RuntimeError("index unavailable"),
    ), patch.object(
        pcf_internal, "run_queue_ttl_arm_sweep"
    ):
        response = await pcf_internal.verify_offbox_backup(request)

    assert response.status_code == 503
    assert json.loads(response.body) == {
        "error": {"code": "pcf_queue_ttl_guard_unavailable", "retryAfterSeconds": 10}
    }
    conn.commit.assert_called_once()
    conn.rollback.assert_called_once()
