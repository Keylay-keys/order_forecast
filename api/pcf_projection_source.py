"""Bounded Firestore source adapter for one-route PCF reconciliation.

This module is deliberately not wired to a scheduler or API. It reads exactly
one eligible route in one read-only transaction and emits the same normalized
container event contract as the owning Firestore trigger.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Callable, Mapping

from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath

from .fixture_identity import classify_fixture_document
from .pcf_event_contract import (
    EventContractError,
    canonical_date,
    canonical_json_bytes,
    firestore_revision,
    normalize_google_update_time,
    validate_projection_event,
)
from .pcf_projection_reconciliation import (
    ExactFirestoreAbsenceProof,
    MAX_ROUTE_ENTITIES,
    ProjectionReconciliationError,
    RouteEntityKey,
    RouteChecksum,
    summarize_source_route,
)


MAX_ROUTE_DELIVERIES = 500
_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_ENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True)
class FirestoreRouteSnapshot:
    source: RouteChecksum
    route_reads: int
    delivery_reads: int
    container_reads: int
    query_count: int
    minimum_billed_document_reads: int = 0


@dataclass(frozen=True)
class ExactFixtureIdentity:
    identity: str
    generation: str
    parent_classification: str
    container_classification: str


def load_production_fixture_registry(
    contract_path: Path,
    *,
    production_project: str,
) -> Mapping[tuple[str, str, str], ExactFixtureIdentity]:
    try:
        contract = json.loads(contract_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProjectionReconciliationError("fixture_registry_unreadable") from exc
    if (
        not isinstance(contract, Mapping)
        or contract.get("contractVersion") != "pcf-fixture-v1"
        or contract.get("productionProject") != production_project
    ):
        raise ProjectionReconciliationError("fixture_registry_contract_invalid")
    rows = contract.get("productionLegacyFixtures")
    if not isinstance(rows, list) or len(rows) > 100:
        raise ProjectionReconciliationError("fixture_registry_rows_invalid")
    registry: dict[tuple[str, str, str], ExactFixtureIdentity] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ProjectionReconciliationError("fixture_registry_row_invalid")
        required = {
            "identity",
            "generation",
            "route",
            "delivery",
            "container",
            "parentClassification",
            "containerClassification",
            "disposition",
            "sourceEvidence",
            "liveEvidence",
        }
        if set(row) != required:
            raise ProjectionReconciliationError("fixture_registry_row_invalid")
        route = str(row["route"])
        key = RouteEntityKey(str(row["delivery"]), str(row["container"]))
        if (
            _ROUTE_RE.fullmatch(route) is None
            or _ENTITY_RE.fullmatch(key.delivery_id) is None
            or _ENTITY_RE.fullmatch(key.container_id) is None
            or (route, key.delivery_id, key.container_id) in registry
        ):
            raise ProjectionReconciliationError("fixture_registry_identity_invalid")
        identity = str(row["identity"])
        generation = str(row["generation"])
        parent_classification = str(row["parentClassification"])
        container_classification = str(row["containerClassification"])
        if (
            _ENTITY_RE.fullmatch(identity) is None
            or not generation
            or parent_classification
            not in {"ordinary_data", "valid_fixture", "invalid_fixture_marker"}
            or container_classification
            not in {"ordinary_data", "valid_fixture", "invalid_fixture_marker"}
        ):
            raise ProjectionReconciliationError("fixture_registry_identity_invalid")
        registry[(route, key.delivery_id, key.container_id)] = ExactFixtureIdentity(
            identity=identity,
            generation=generation,
            parent_classification=parent_classification,
            container_classification=container_classification,
        )
    return registry


def _google_time(value: Any, name: str) -> str:
    rfc3339 = getattr(value, "rfc3339", None)
    if callable(rfc3339):
        try:
            return normalize_google_update_time(rfc3339())
        except EventContractError as exc:
            raise ProjectionReconciliationError(f"{name}_invalid") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ProjectionReconciliationError(f"{name}_invalid")
    normalized = value.astimezone(timezone.utc)
    return normalize_google_update_time(
        normalized.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )


def _fixture_marker(document: Mapping[str, Any]) -> tuple[str, str] | None:
    metadata = document.get("metadata")
    nested = metadata if isinstance(metadata, Mapping) else {}
    marker = document.get("fixture", nested.get("fixture"))
    if marker is None:
        return None
    if not isinstance(marker, Mapping):
        raise ProjectionReconciliationError("fixture_marker_invalid")
    return str(marker.get("identity") or ""), str(marker.get("generation") or "")


def _fixture_envelope(
    container: Mapping[str, Any],
    parent: Mapping[str, Any],
    exact_fixture: ExactFixtureIdentity | None = None,
) -> tuple[bool, Mapping[str, str] | None]:
    classifications = [
        classify_fixture_document(container),
        classify_fixture_document(parent),
    ]
    if exact_fixture is not None:
        expected = [
            exact_fixture.container_classification,
            exact_fixture.parent_classification,
        ]
        if classifications != expected:
            raise ProjectionReconciliationError("fixture_registry_state_mismatch")
        return True, {
            "identity": exact_fixture.identity,
            "generation": exact_fixture.generation,
        }
    if "invalid_fixture_marker" in classifications:
        raise ProjectionReconciliationError("fixture_marker_invalid")
    markers = [
        _fixture_marker(document)
        for document, classification in zip(
            (container, parent), classifications, strict=True
        )
        if classification == "valid_fixture"
    ]
    identities = {marker for marker in markers if marker is not None}
    if len(identities) > 1:
        raise ProjectionReconciliationError("fixture_marker_conflict")
    if not identities:
        return False, None
    identity, generation = identities.pop()
    return True, {"identity": identity, "generation": generation}


def _canonical_loading_date(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        raise ProjectionReconciliationError("loading_date_invalid")
    try:
        return canonical_date(value, "loadingDate")
    except EventContractError as exc:
        raise ProjectionReconciliationError("loading_date_invalid") from exc


def _expiry_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return None
    try:
        return canonical_date(value, "expiryDate")
    except EventContractError:
        return None


def _guarantee_or_none(value: Any) -> Mapping[str, Any] | None:
    if not isinstance(value, Mapping) or value.get("isGuaranteed") is not True:
        return None
    expiry = value.get("guaranteeExpiresAt")
    if isinstance(expiry, str):
        try:
            expiry_value = normalize_google_update_time(expiry)
        except EventContractError as exc:
            raise ProjectionReconciliationError("guarantee_expiry_invalid") from exc
    else:
        expiry_value = _google_time(expiry, "guarantee_expiry")
    return {"isGuaranteed": True, "guaranteeExpiresAt": expiry_value}


def _container_payload(
    container: Mapping[str, Any],
    parent: Mapping[str, Any],
) -> Mapping[str, Any]:
    pages = container.get("pages")
    if not isinstance(pages, list) or len(pages) > 1000:
        raise ProjectionReconciliationError("container_pages_invalid")
    page_numbers: list[int] = []
    items: list[dict[str, Any]] = []
    seen_pages: set[int] = set()
    for page_index, page in enumerate(pages):
        if not isinstance(page, Mapping):
            raise ProjectionReconciliationError("container_page_invalid")
        page_number = page.get("pageNumber")
        if (
            isinstance(page_number, bool)
            or not isinstance(page_number, int)
            or not 1 <= page_number <= 1000
        ):
            page_number = page_index + 1
        if page_number in seen_pages:
            raise ProjectionReconciliationError("container_page_duplicate")
        seen_pages.add(page_number)
        page_numbers.append(page_number)
        raw_items = page.get("items")
        if not isinstance(raw_items, list) or len(raw_items) > 5000:
            raise ProjectionReconciliationError("container_items_invalid")
        for item_position, item in enumerate(raw_items):
            if len(items) >= 5000:
                raise ProjectionReconciliationError("container_item_total_exceeded")
            if not isinstance(item, Mapping):
                raise ProjectionReconciliationError("container_item_invalid")
            product = item.get("product")
            if not isinstance(product, str) or not product.strip() or len(product.strip()) > 128:
                raise ProjectionReconciliationError("container_item_product_invalid")
            description = item.get("description")
            if description is None:
                description = ""
            if not isinstance(description, str) or len(description) > 512:
                raise ProjectionReconciliationError("container_item_description_invalid")
            raw_days = item.get("days")
            items.append(
                {
                    "pageNumber": page_number,
                    "itemPosition": item_position,
                    "product": product.strip(),
                    "description": description.strip(),
                    "expiryDate": _expiry_or_none(item.get("bestBefore")),
                    "visibleOnList": item.get("visibleOnList") is not False,
                    "isLowQuantity": item.get("isLowQuantity") is True,
                    "isShortCoded": item.get("isShortCoded") is True
                    or (isinstance(raw_days, str) and "*" in raw_days),
                    "guaranteed": _guarantee_or_none(item.get("guaranteed")),
                }
            )
    loading_source = container.get("loadingDate")
    if loading_source is None:
        loading_source = parent.get("loadingDate")
    return {
        "active": container.get("active") is not False
        and parent.get("active") is not False,
        "loadingDate": _canonical_loading_date(loading_source),
        "pageNumbers": sorted(page_numbers),
        "items": sorted(
            items,
            key=lambda item: (item["pageNumber"], item["itemPosition"]),
        ),
    }


def build_container_projection_payload(
    container: Mapping[str, Any],
    parent: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Normalize one exact Firestore container for the shared projection.

    Result finalization and route reconciliation deliberately share this one
    normalization contract so an OCR write cannot produce a different
    ``items_current`` business hash from the listener/reconciler path.
    """
    if not isinstance(container, Mapping) or not isinstance(parent, Mapping):
        raise ProjectionReconciliationError("container_projection_input_invalid")
    return _container_payload(container, parent)


def build_reconciliation_source_event(
    *,
    route_number: str,
    delivery_id: str,
    container_id: str,
    parent_data: Mapping[str, Any],
    container_data: Mapping[str, Any],
    container_update_time: Any,
    snapshot_read_time: Any,
    exact_fixture: ExactFixtureIdentity | None = None,
) -> Mapping[str, Any]:
    update_time = _google_time(container_update_time, "container_update_time")
    occurred_at = _google_time(snapshot_read_time, "snapshot_read_time")
    source_path = (
        f"routes/{route_number}/pcfs/{delivery_id}/containers/{container_id}"
    )
    identity = hashlib.sha256(
        canonical_json_bytes(
            {
                "purpose": "pcf-reconciliation-source-v1",
                "sourcePath": source_path,
                "sourceRevision": firestore_revision(update_time, "upsert"),
            }
        )
    ).hexdigest()
    excluded, fixture = _fixture_envelope(
        container_data,
        parent_data,
        exact_fixture,
    )
    event = {
        "schemaVersion": 1,
        "sourceEventId": f"reconcile-source:{identity}",
        "eventType": "projection.pcf_container.changed",
        "mutationKind": "upsert",
        "occurredAt": occurred_at,
        "sourcePath": source_path,
        "sourceRevision": firestore_revision(update_time, "upsert"),
        "entity": {
            "route": str(route_number),
            "delivery": str(delivery_id),
            "container": str(container_id),
        },
        "automationExcluded": excluded,
        "fixture": fixture,
        "payload": _container_payload(container_data, parent_data),
    }
    try:
        return validate_projection_event(event)
    except EventContractError as exc:
        raise ProjectionReconciliationError("reconciliation_source_event_invalid") from exc


class FirestoreProjectionSourceAdapter:
    def __init__(
        self,
        firestore_client: Any,
        transaction_runner: (
            Callable[[Callable[[Any], FirestoreRouteSnapshot]], FirestoreRouteSnapshot]
            | None
        ) = None,
        fixture_registry: Mapping[tuple[str, str, str], ExactFixtureIdentity] | None = None,
    ) -> None:
        self._db = firestore_client
        self._transaction_runner = transaction_runner or self._run_read_only_transaction
        self._fixture_registry = dict(fixture_registry or {})

    def _run_read_only_transaction(
        self,
        read: Callable[[Any], FirestoreRouteSnapshot],
    ) -> FirestoreRouteSnapshot:
        transaction = self._db.transaction(read_only=True, max_attempts=1)

        @firestore.transactional
        def execute(current_transaction: Any) -> FirestoreRouteSnapshot:
            return read(current_transaction)

        return execute(transaction)

    def read_route(self, route_number: str) -> FirestoreRouteSnapshot:
        route = str(route_number)
        if _ROUTE_RE.fullmatch(route) is None:
            raise ProjectionReconciliationError("route_invalid")

        def read(transaction: Any) -> FirestoreRouteSnapshot:
            route_ref = self._db.document(f"routes/{route}")
            route_snapshot = route_ref.get(transaction=transaction, timeout=10)
            if not route_snapshot.exists:
                raise ProjectionReconciliationError("source_route_missing")
            boundary = _google_time(route_snapshot.read_time, "source_snapshot_read_time")
            parent_query = (
                route_ref.collection("pcfs")
                .order_by(FieldPath.document_id())
                .limit(MAX_ROUTE_DELIVERIES + 1)
            )
            parents = list(parent_query.stream(transaction=transaction, timeout=20))
            if len(parents) > MAX_ROUTE_DELIVERIES:
                raise ProjectionReconciliationError("source_route_delivery_limit_exceeded")
            events: list[Mapping[str, Any]] = []
            container_query_minimum_reads = 0
            for parent in parents:
                parent_read_time = _google_time(parent.read_time, "parent_read_time")
                if parent_read_time != boundary:
                    raise ProjectionReconciliationError("source_snapshot_read_time_changed")
                parent_data = parent.to_dict() or {}
                remaining = MAX_ROUTE_ENTITIES + 1 - len(events)
                containers = list(
                    parent.reference.collection("containers")
                    .order_by(FieldPath.document_id())
                    .limit(remaining)
                    .stream(transaction=transaction, timeout=20)
                )
                container_query_minimum_reads += max(1, len(containers))
                for container in containers:
                    if len(events) >= MAX_ROUTE_ENTITIES:
                        raise ProjectionReconciliationError(
                            "source_route_entity_limit_exceeded"
                        )
                    if _google_time(container.read_time, "container_read_time") != boundary:
                        raise ProjectionReconciliationError(
                            "source_snapshot_read_time_changed"
                        )
                    if container.update_time is None:
                        raise ProjectionReconciliationError(
                            "source_container_revision_missing"
                        )
                    events.append(
                        build_reconciliation_source_event(
                            route_number=route,
                            delivery_id=str(parent.id),
                            container_id=str(container.id),
                            parent_data=parent_data,
                            container_data=container.to_dict() or {},
                            container_update_time=container.update_time,
                            snapshot_read_time=container.read_time,
                            exact_fixture=self._fixture_registry.get(
                                (route, str(parent.id), str(container.id))
                            ),
                        )
                    )
            source = summarize_source_route(
                route,
                firestore_revision(boundary, "upsert"),
                events,
            )
            return FirestoreRouteSnapshot(
                source=source,
                route_reads=1,
                delivery_reads=len(parents),
                container_reads=len(events),
                query_count=1 + len(parents),
                minimum_billed_document_reads=(
                    1
                    + max(1, len(parents))
                    + container_query_minimum_reads
                ),
            )

        return self._transaction_runner(read)

    def read_exact_absence(
        self,
        route_number: str,
        key: RouteEntityKey,
    ) -> ExactFirestoreAbsenceProof:
        route = str(route_number)
        if (
            _ROUTE_RE.fullmatch(route) is None
            or _ENTITY_RE.fullmatch(key.delivery_id) is None
            or _ENTITY_RE.fullmatch(key.container_id) is None
        ):
            raise ProjectionReconciliationError("absence_identity_invalid")
        source_path = (
            f"routes/{route}/pcfs/{key.delivery_id}/containers/{key.container_id}"
        )
        snapshot = self._db.document(source_path).get(timeout=10)
        return ExactFirestoreAbsenceProof(
            source_path=source_path,
            read_time=_google_time(snapshot.read_time, "absence_read_time"),
            exists=bool(snapshot.exists),
        )
