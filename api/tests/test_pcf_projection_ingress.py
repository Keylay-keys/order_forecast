import copy
import hashlib
import json
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

import httpx
from fastapi import FastAPI
from fastapi import HTTPException
from google.api_core.exceptions import AlreadyExists

from order_forecast.api.machine_auth import MachinePrincipal
from order_forecast.api.middleware.internal_machine_auth import (
    InternalMachineAuthMiddleware,
)
from order_forecast.api.pcf_projection_ingress import (
    ProjectionEventNotImplemented,
    ProjectionIngressConflict,
    commit_projection_event,
    projection_rejection_receipt_source_id,
    record_projection_rejection,
)
from order_forecast.api.pcf_projection_alerts import (
    build_projection_rejection_alert,
    publish_projection_rejection_alert,
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


def retention_event(event_id="event-retention-0001", *, mutation_kind="upsert"):
    event = container_event(event_id)
    event.update({
        "eventType": "projection.pcf_retention.changed",
        "mutationKind": mutation_kind,
        "sourcePath": "pcfProjectionRoutePolicies/989262",
        "sourceRevision": (
            "fsut1|2026-08-25T10:00:00.000000000Z|1"
            if mutation_kind == "delete"
            else "fsut1|2026-08-25T10:00:00.000000000Z|0"
        ),
        "entity": {"route": "989262"},
        "payload": (
            None
            if mutation_kind == "delete"
            else {"retentionDays": 4, "ownerUserId": "owner-989262"}
        ),
    })
    return event


def upload_event():
    return {
        "eventId": "firestore-upload-event-01",
        "eventType": "pcf.upload.settled",
        "schemaVersion": 1,
        "route": "989262",
        "delivery": "1806242572",
        "occurredAt": "2026-08-29T18:00:01Z",
        "sourceRevision": "fsut1|2026-08-29T18:00:00.000000000Z|0",
        "quietPeriodElapsed": True,
        "coverageScope": "uploaded_manifest",
        "manifest": {
            "containerCount": 1,
            "pageCount": 1,
            "objects": [{
                "containerCode": "003400001234567890",
                "uploadPageNumber": 1,
                "objectPath": (
                    "routes/989262/unprocessed/"
                    "1806242572_003400001234567890_page_1.jpg"
                ),
                "generation": "123456789",
                "size": 1200,
                "md5Hash": "YWJjZGVmMDEyMzQ1Njc4OQ==",
            }],
        },
        "sourceQueuePath": "pcf_processing_queue/989262/queue/1806242572",
        "automationExcluded": False,
        "fixture": None,
    }


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
        ) as apply_projection, patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_delivery_from_current_policy",
            return_value=SimpleNamespace(reason="scheduled"),
        ) as plan_delivery:
            first = commit_projection_event(conn, event, actor_id="dispatcher@example.com")
            second = commit_projection_event(conn, event, actor_id="dispatcher@example.com")

        self.assertEqual(first.disposition, "apply")
        self.assertFalse(first.duplicate_transport_event)
        self.assertEqual(second.disposition, "apply")
        self.assertTrue(second.duplicate_transport_event)
        apply_projection.assert_called_once()
        plan_delivery.assert_called_once()
        receipt = conn.receipts[event["sourceEventId"]]
        self.assertEqual(receipt["disposition"], "apply")
        self.assertEqual(receipt["sourceRevision"], event["sourceRevision"])
        self.assertEqual(receipt["planningDisposition"], "scheduled")
        self.assertEqual(receipt["plannedDeliveryCount"], 1)
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

    def test_retention_event_replans_only_its_route_without_advancing_item_watermark(self):
        conn = _ReceiptConnection()
        event = retention_event()
        plans = [("delivery-a", SimpleNamespace()), ("delivery-b", SimpleNamespace())]
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_route_retention_policy",
            return_value="inserted",
        ) as apply_policy, patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_route_from_current_policy",
            return_value=plans,
        ) as plan_route:
            first = commit_projection_event(conn, event, actor_id="dispatcher@example.com")
            second = commit_projection_event(conn, event, actor_id="dispatcher@example.com")

        self.assertEqual(first.disposition, "inserted")
        self.assertEqual(first.planning_disposition, "route_replanned")
        self.assertEqual(first.planned_delivery_count, 2)
        self.assertTrue(second.duplicate_transport_event)
        apply_policy.assert_called_once_with(
            conn,
            route_number="989262",
            retention_days=4,
            policy_revision=event["sourceRevision"],
        )
        plan_route.assert_called_once()
        self.assertEqual(conn.route_updates, [])

    def test_retention_tombstone_clears_policy_and_cancels_route_plans(self):
        conn = _ReceiptConnection()
        event = retention_event("event-retention-delete", mutation_kind="delete")
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_route_retention_policy",
            return_value="updated",
        ) as apply_policy, patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_route_from_current_policy",
            return_value=[],
        ) as plan_route:
            result = commit_projection_event(conn, event, actor_id="dispatcher@example.com")

        self.assertEqual(result.planning_disposition, "policy_missing")
        apply_policy.assert_called_once_with(
            conn,
            route_number="989262",
            retention_days=None,
            policy_revision=event["sourceRevision"],
        )
        plan_route.assert_called_once()

    def test_retention_watermark_only_does_not_replan(self):
        conn = _ReceiptConnection()
        event = retention_event("event-retention-watermark")
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_route_retention_policy",
            return_value="advance_watermark_only",
        ), patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_route_from_current_policy",
        ) as plan_route:
            result = commit_projection_event(conn, event, actor_id="dispatcher@example.com")

        self.assertEqual(result.planning_disposition, "policy_unchanged")
        self.assertEqual(result.planned_delivery_count, 0)
        plan_route.assert_not_called()

    def test_source_event_id_reuse_with_different_content_is_rejected(self):
        conn = _ReceiptConnection()
        original = container_event()
        changed = copy.deepcopy(original)
        changed["payload"]["loadingDate"] = "2026-08-26"
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_container_projection",
            return_value="apply",
        ), patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_delivery_from_current_policy",
            return_value=SimpleNamespace(reason="scheduled"),
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
        self.assertTrue(record_projection_rejection(
            conn,
            body_sha256=digest,
            reason="contract_validation_failed",
            actor_id="dispatcher@example.com",
        ))
        self.assertEqual(len(conn.inserted), 1)

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

    def test_rejection_receipt_and_alert_share_body_free_deterministic_identity(self):
        source_event_id = "event-rejected-sensitive-name"
        receipt_id = projection_rejection_receipt_source_id(
            body_sha256="d" * 64,
            reason="revision_conflict",
            source_event_id=source_event_id,
        )
        alert_id, fields = build_projection_rejection_alert(
            receipt_source_event_id=receipt_id,
            reason="revision_conflict",
            actor_id="dispatcher@example.com",
            observed_at=pcf_internal.datetime(2026, 8, 29, tzinfo=pcf_internal.timezone.utc),
        )
        serialized = json.dumps({"id": alert_id, "fields": fields}, default=str)
        self.assertNotIn(source_event_id, serialized)
        self.assertNotIn("dispatcher@example.com", serialized)
        self.assertEqual(fields["reason"], "revision_conflict")
        self.assertRegex(alert_id, r"^pcf-projection-rejection-[0-9a-f]{64}$")

    def test_projection_alert_create_is_idempotent_without_read_or_merge(self):
        class Reference:
            def __init__(self):
                self.created = []

            def create(self, fields):
                self.created.append(fields)

        class Collection:
            def __init__(self, reference):
                self.reference = reference
                self.document_ids = []

            def document(self, document_id):
                self.document_ids.append(document_id)
                return self.reference

        class Database:
            def __init__(self):
                self.reference = Reference()
                self.collection_name = None
                self.collection_value = Collection(self.reference)

            def collection(self, name):
                self.collection_name = name
                return self.collection_value

        database = Database()
        receipt_id = projection_rejection_receipt_source_id(
            body_sha256="e" * 64,
            reason="unknown_schema_or_event_type",
            source_event_id="event-unknown-0001",
        )
        alert_id = publish_projection_rejection_alert(
            database,
            receipt_source_event_id=receipt_id,
            reason="unknown_schema_or_event_type",
            actor_id="dispatcher@example.com",
            observed_at=pcf_internal.datetime(2026, 8, 29, tzinfo=pcf_internal.timezone.utc),
        )
        self.assertEqual(database.collection_name, "admin_alerts")
        self.assertEqual(database.collection_value.document_ids, [alert_id])
        self.assertEqual(len(database.reference.created), 1)
        self.assertNotIn("body", json.dumps(database.reference.created, default=str).lower())

        class ExistingReference(Reference):
            def create(self, fields):
                raise AlreadyExists("already exists")

        database.reference = ExistingReference()
        database.collection_value.reference = database.reference
        self.assertEqual(
            publish_projection_rejection_alert(
                database,
                receipt_source_event_id=receipt_id,
                reason="unknown_schema_or_event_type",
                actor_id="dispatcher@example.com",
                observed_at=pcf_internal.datetime(
                    2026, 8, 29, tzinfo=pcf_internal.timezone.utc
                ),
            ),
            alert_id,
        )

    def test_reused_rejected_source_id_is_bounded_without_poisoning_real_id(self):
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

        self.assertTrue(duplicate)
        self.assertEqual(len(conn.inserted), 1)
        self.assertNotIn("event-rejected-0002", conn.receipts)

        event = container_event("event-rejected-0002")
        with patch(
            "order_forecast.api.pcf_projection_ingress.apply_container_projection",
            return_value="apply",
        ), patch(
            "order_forecast.api.pcf_projection_ingress.plan_projected_delivery_from_current_policy",
            return_value=SimpleNamespace(reason="scheduled"),
        ):
            accepted = commit_projection_event(
                conn,
                event,
                actor_id="dispatcher@example.com",
            )
        self.assertFalse(accepted.duplicate_transport_event)
        self.assertIn("event-rejected-0002", conn.receipts)


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
        result = SimpleNamespace(
            disposition="apply",
            duplicate_transport_event=False,
            planning_disposition="scheduled",
            planned_delivery_count=1,
        )
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ) as returned, patch.object(
            pcf_internal, "commit_projection_event", return_value=result
        ) as commit_event:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json.loads(response.body),
            {
                "status": "accepted",
                "disposition": "apply",
                "planningDisposition": "scheduled",
                "plannedDeliveryCount": 1,
            },
        )
        self.assertEqual(conn.commits, 1)
        commit_event.assert_called_once()
        returned.assert_called_once_with(conn)

    async def test_settled_upload_dispatches_to_work_ingress_and_returns_identity(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(upload_event()).encode(), principal=TASK_PRINCIPAL)
        work_id = uuid.uuid4()
        result = SimpleNamespace(
            disposition="queued",
            duplicate_transport_event=False,
            work_id=work_id,
            input_hash_version="pcf-work-input-v1",
            input_hash="a" * 64,
        )
        with patch.object(
            pcf_internal, "get_pcf_pg_connection", return_value=conn
        ), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal, "commit_pcf_upload_event", return_value=result
        ) as commit_work, patch.object(
            pcf_internal, "commit_projection_event"
        ) as commit_projection:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            json.loads(response.body),
            {
                "status": "accepted",
                "disposition": "queued",
                "workId": str(work_id),
                "inputHashVersion": "pcf-work-input-v1",
                "inputHash": "a" * 64,
            },
        )
        self.assertEqual(conn.commits, 1)
        commit_work.assert_called_once()
        commit_projection.assert_not_called()

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

    async def test_unknown_schema_uses_explicit_bounded_taxonomy(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=pcf_internal.EventContractError("unsupported schemaVersion"),
        ), patch.object(
            pcf_internal, "record_projection_rejection"
        ) as rejection, patch.object(
            pcf_internal, "_publish_severe_rejection_alert"
        ) as publish_alert:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(
            rejection.call_args.kwargs["reason"],
            "unknown_schema_or_event_type",
        )
        publish_alert.assert_called_once_with(
            body_sha256=hashlib.sha256(request._body).hexdigest(),
            reason="unknown_schema_or_event_type",
            actor_id=TASK_PRINCIPAL.principal,
            source_event_id="event-container-0001",
        )

    async def test_equal_revision_conflict_uses_revision_taxonomy(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(pcf_internal, "get_pcf_pg_connection", return_value=conn), patch.object(
            pcf_internal, "return_pcf_pg_connection"
        ), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=pcf_internal.LifecycleConflictError("equal revision"),
        ), patch.object(
            pcf_internal, "record_projection_rejection"
        ) as rejection, patch.object(
            pcf_internal, "_publish_severe_rejection_alert"
        ) as publish_alert:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(rejection.call_args.kwargs["reason"], "revision_conflict")
        self.assertEqual(
            rejection.call_args.kwargs["source_event_id"],
            "event-container-0001",
        )
        publish_alert.assert_called_once()

    async def test_source_event_conflict_reaches_the_same_severe_alert_sink(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(
            pcf_internal, "get_pcf_pg_connection", return_value=conn
        ), patch.object(pcf_internal, "return_pcf_pg_connection"), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=ProjectionIngressConflict("source event reused"),
        ), patch.object(
            pcf_internal, "record_projection_rejection"
        ) as rejection, patch.object(
            pcf_internal, "_publish_severe_rejection_alert"
        ) as publish_alert:
            response = await pcf_internal.ingest_projection_event(request)

        self.assertEqual(response.status_code, 204)
        self.assertEqual(rejection.call_args.kwargs["reason"], "source_event_conflict")
        self.assertEqual(
            publish_alert.call_args.kwargs["reason"],
            "source_event_conflict",
        )

    async def test_severe_rejection_is_retryable_until_alert_projection_commits(self):
        conn = _EndpointConnection()
        request = _Request(json.dumps(container_event()).encode(), principal=TASK_PRINCIPAL)
        with patch.object(
            pcf_internal, "get_pcf_pg_connection", return_value=conn
        ), patch.object(pcf_internal, "return_pcf_pg_connection"), patch.object(
            pcf_internal,
            "commit_projection_event",
            side_effect=pcf_internal.LifecycleConflictError("equal revision"),
        ), patch.object(
            pcf_internal, "record_projection_rejection"
        ), patch.object(
            pcf_internal,
            "_publish_severe_rejection_alert",
            side_effect=RuntimeError("alert unavailable"),
        ):
            with self.assertRaises(HTTPException) as raised:
                await pcf_internal.ingest_projection_event(request)

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(conn.commits, 1)


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
        result = SimpleNamespace(
            disposition="apply",
            duplicate_transport_event=False,
            planning_disposition="scheduled",
            planned_delivery_count=1,
        )
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
