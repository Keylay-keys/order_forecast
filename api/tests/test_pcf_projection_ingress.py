import copy
import hashlib
import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
from fastapi import FastAPI
from fastapi import HTTPException

from order_forecast.api.machine_auth import MachinePrincipal
from order_forecast.api.middleware.internal_machine_auth import (
    InternalMachineAuthMiddleware,
)
from order_forecast.api.pcf_projection_ingress import (
    ProjectionEventNotImplemented,
    ProjectionIngressConflict,
    commit_projection_event,
    record_projection_rejection,
)
from order_forecast.api.routers import pcf_internal


def container_event(event_id="event-container-0001"):
    return {
        "schemaVersion": 1,
        "sourceEventId": event_id,
        "eventType": "projection.pcf_container.changed",
        "mutationKind": "upsert",
        "occurredAt": "2026-08-25T10:00:00.000000000Z",
        "sourcePath": "routes/989262/pcfs/1806242572/containers/C01",
        "sourceRevision": "fsut1|2026-08-25T10:00:00.000000000Z|0",
        "entity": {
            "route": "989262",
            "delivery": "1806242572",
            "container": "C01",
        },
        "automationExcluded": False,
        "fixture": None,
        "payload": {
            "active": True,
            "loadingDate": "2026-08-25",
            "pageNumbers": [],
            "items": [],
        },
    }


def delivery_event():
    event = container_event("event-delivery-00001")
    event.update({
        "eventType": "projection.pcf_delivery.changed",
        "sourcePath": "routes/989262/pcfs/1806242572",
        "entity": {"route": "989262", "delivery": "1806242572"},
        "payload": {"active": True, "loadingDate": "2026-08-25", "status": "approved"},
    })
    return event


class _ReceiptCursor:
    def __init__(self, conn):
        self.conn = conn
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split()).lower()
        self.result = None
        if normalized.startswith("select pg_advisory_xact_lock"):
            return
        if normalized.startswith("select payload from pcf_work_events"):
            payload = self.conn.receipts.get(params[0])
            self.result = {"payload": payload} if payload is not None else None
            return
        if normalized.startswith("insert into pcf_projection_routes"):
            self.conn.route_updates.append(tuple(params))
            return
        if normalized.startswith("insert into pcf_work_events"):
            payload = getattr(params[-1], "adapted", params[-1])
            source_event_id = params[1] if len(params) == 5 else None
            if source_event_id is not None:
                self.conn.receipts[source_event_id] = payload
            self.conn.inserted.append(payload)
            return
        raise AssertionError(f"unexpected SQL: {normalized}")

    def fetchone(self):
        return self.result


class _ReceiptConnection:
    def __init__(self):
        self.receipts = {}
        self.inserted = []
        self.route_updates = []

    def cursor(self, *args, **kwargs):
        return _ReceiptCursor(self)


class ProjectionIngressCommitTests(unittest.TestCase):
    def test_commit_records_projection_and_transport_duplicate_is_noop(self):
        conn = _ReceiptConnection()
        event = container_event()
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_container_projection",
            return_value="apply",
        ) as apply_projection:
            first = commit_projection_event(conn, event, actor_id="dispatcher@example.com")
            second = commit_projection_event(conn, event, actor_id="dispatcher@example.com")

        self.assertEqual(first.disposition, "apply")
        self.assertFalse(first.duplicate_transport_event)
        self.assertEqual(second.disposition, "apply")
        self.assertTrue(second.duplicate_transport_event)
        apply_projection.assert_called_once()
        receipt = conn.receipts[event["sourceEventId"]]
        self.assertEqual(receipt["disposition"], "apply")
        self.assertEqual(receipt["sourceRevision"], event["sourceRevision"])
        self.assertRegex(receipt["eventHash"], r"^[0-9a-f]{64}$")
        self.assertEqual(conn.route_updates, [("989262", event["sourceRevision"])])

    def test_fixture_suppression_does_not_create_route_progress(self):
        conn = _ReceiptConnection()
        event = container_event("event-fixture-0001")
        event["automationExcluded"] = True
        event["fixture"] = {"identity": "fixture-one", "generation": "1"}
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_container_projection",
            return_value="fixture_suppressed",
        ):
            result = commit_projection_event(
                conn, event, actor_id="dispatcher@example.com"
            )

        self.assertEqual(result.disposition, "fixture_suppressed")
        self.assertEqual(conn.route_updates, [])

    def test_source_event_id_reuse_with_different_content_is_rejected(self):
        conn = _ReceiptConnection()
        original = container_event()
        changed = copy.deepcopy(original)
        changed["payload"]["loadingDate"] = "2026-08-26"
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_container_projection",
            return_value="apply",
        ):
            commit_projection_event(conn, original, actor_id="dispatcher@example.com")
            with self.assertRaises(ProjectionIngressConflict):
                commit_projection_event(conn, changed, actor_id="dispatcher@example.com")

    def test_valid_unimplemented_event_remains_retryable_without_receipt(self):
        conn = _ReceiptConnection()
        with self.assertRaises(ProjectionEventNotImplemented):
            commit_projection_event(
                conn,
                delivery_event(),
                actor_id="dispatcher@example.com",
            )
        self.assertEqual(conn.inserted, [])

    def test_rejection_receipt_retains_hash_and_reason_not_body(self):
        conn = _ReceiptConnection()
        body = b'{"secret-looking-field":"not retained"}'
        digest = hashlib.sha256(body).hexdigest()
        record_projection_rejection(
            conn,
            body_sha256=digest,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
        )
        self.assertEqual(conn.inserted, [{
            "bodySha256": digest,
            "contractVersion": "pcf-projection-receipt-v1",
            "reason": "contract_validation_failed",
        }])
        self.assertNotIn("secret-looking-field", json.dumps(conn.inserted))

    def test_rejection_with_source_event_id_is_idempotent(self):
        conn = _ReceiptConnection()
        body_hash = "b" * 64
        first = record_projection_rejection(
            conn,
            body_sha256=body_hash,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
            source_event_id="event-rejected-0001",
        )
        second = record_projection_rejection(
            conn,
            body_sha256=body_hash,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
            source_event_id="event-rejected-0001",
        )

        self.assertFalse(first)
        self.assertTrue(second)
        self.assertEqual(len(conn.inserted), 1)

    def test_reused_rejected_source_id_records_bounded_conflict(self):
        conn = _ReceiptConnection()
        record_projection_rejection(
            conn,
            body_sha256="b" * 64,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
            source_event_id="event-rejected-0002",
        )
        duplicate = record_projection_rejection(
            conn,
            body_sha256="c" * 64,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
            source_event_id="event-rejected-0002",
        )

        self.assertFalse(duplicate)
        self.assertEqual(len(conn.inserted), 2)
        self.assertEqual(conn.inserted[-1]["reason"], "source_event_conflict")


class _EndpointConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _Request:
    def __init__(self, body, *, principal=None, content_type="application/json"):
        self._body = body
        self.state = SimpleNamespace(machine_principal=principal)
        self.headers = {"content-type": content_type}

    async def stream(self):
        yield self._body


TASK_PRINCIPAL = MachinePrincipal(
    principal="pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com",
    provider="google_cloud_tasks_oidc",
    capability="pcf.event.ingest",
    key_id="google-subject",
)


class ProjectionIngressEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_middleware_principal_fails_before_body_or_database(self):
        request = _Request(b"{}", principal=None)
        with patch.object(pcf_internal, "get_pcf_pg_connection") as get_conn:
            with self.assertRaises(HTTPException) as raised:
                await pcf_internal.ingest_projection_event(request)
        self.assertEqual(raised.exception.status_code, 401)
        get_conn.assert_not_called()

    async def test_valid_event_commits_and_returns_bounded_result(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        result = SimpleNamespace(disposition="apply", duplicate_transport_event=False)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ) as returned, patch.object(
            pcf_internal, "commit_projection_event", return_value=result
        ) as commit_event:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.body), {"status": "accepted", "disposition": "apply"})
        self.assertEqual(conn.commits, 1)
        commit_event.assert_called_once()
        returned.assert_called_once_with(conn)

    async def test_duplicate_json_key_is_durably_rejected_and_acknowledged(self):
        conn = _EndpointConnection()
        request = _Request(b'{"schemaVersion":1,"schemaVersion":1}', principal=TASK_PRINCIPAL)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(pcf_internal, "record_projection_rejection") as record:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(record.call_args.kwargs["reason"], "json_invalid")

    async def test_oversized_body_is_hashed_without_retaining_it(self):
        conn = _EndpointConnection()
        body = b"x" * (pcf_internal.MAX_EVENT_BODY_BYTES + 200_000)
        request = _Request(body, principal=TASK_PRINCIPAL)
        with patch.object(
            pcf_internal, "get_pcf_pg_connection", return_value=conn
        ), patch.object(pcf_internal, "return_pcf_pg_connection"), patch.object(
            pcf_internal, "record_projection_rejection"
        ) as record:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(conn.commits, 1)
        self.assertEqual(record.call_args.kwargs["reason"], "body_too_large")
        self.assertEqual(
            record.call_args.kwargs["body_sha256"], hashlib.sha256(body).hexdigest()
        )

    async def test_valid_unimplemented_event_returns_retryable_503(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(delivery_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=ProjectionEventNotImplemented("delivery"),
        ):
            with self.assertRaises(HTTPException) as raised:
                await pcf_internal.ingest_projection_event(request)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertGreaterEqual(conn.rollbacks, 1)

    async def test_contract_failure_is_acknowledged_only_after_rejection_commit(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=pcf_internal.EventContractError("invalid"),
        ), patch.object(pcf_internal, "record_projection_rejection") as rejection:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(conn.commits, 1)
        self.assertGreaterEqual(conn.rollbacks, 1)
        rejection.assert_called_once()
        self.assertEqual(
            rejection.call_args.kwargs["source_event_id"],
            "event-container-0001",
        )


class ProjectionIngressAsgiTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _app(*, enabled):
        app = FastAPI()
        app.include_router(pcf_internal.router)
        app.add_middleware(InternalMachineAuthMiddleware, enabled=enabled)
        return app

    async def test_disabled_middleware_hides_real_route_before_database(self):
        transport = httpx.ASGITransport(app=self._app(enabled=False))
        with patch.object(pcf_internal, "get_pcf_pg_connection") as get_conn:
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                response = await client.post(
                    "/internal/v1/pcf/events",
                    content=json.dumps(container_event()),
                    headers={"content-type": "application/json"},
                )

        self.assertEqual(response.status_code, 404)
        get_conn.assert_not_called()

    async def test_authenticated_middleware_supplies_real_route_principal(self):
        conn = _EndpointConnection()
        result = SimpleNamespace(disposition="apply", duplicate_transport_event=False)
        transport = httpx.ASGITransport(app=self._app(enabled=True))
        with patch(
            "order_forecast.api.middleware.internal_machine_auth.authenticate_internal_headers",
            return_value=TASK_PRINCIPAL,
        ), patch.object(
            pcf_internal, "get_pcf_pg_connection", return_value=conn
        ), patch.object(pcf_internal, "return_pcf_pg_connection"), patch.object(
            pcf_internal, "commit_projection_event", return_value=result
        ):
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                response = await client.post(
                    "/internal/v1/pcf/events",
                    content=json.dumps(container_event()),
                    headers={
                        "authorization": "Bearer verified-by-test",
                        "content-type": "application/json",
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "accepted")
        self.assertEqual(conn.commits, 1)


if __name__ == "__main__":
    unittest.main()
