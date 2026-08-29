"""Pure, field-scoped Firebase projection plan for one accepted PCF result."""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from .pcf_effect_inputs import EffectInput
from .pcf_effect_storage import CanonicalObjectProof, canonical_path, source_object
_DENVER = ZoneInfo("America/Denver")
_PRESERVED_ITEM_FIELDS = (
    "visibleOnList",
    "isLowQuantity",
    "guaranteed",
    "editedAt",
    "originalProduct",
)
_PARENT_OWNED_FIELDS = (
    "deliveryNumber",
    "routeNumber",
    "status",
    "containerCount",
    "totalItems",
    "expiredItems",
    "expiringItems",
    "allItemsExpired",
    "lastItemExpiryDate",
    "updatedAt",
    "pcfClusterProjection",
)
_CONTAINER_OWNED_FIELDS = (
    "containerCode",
    "deliveryNumber",
    "loadingDate",
    "routeNumber",
    "pages",
    "totalItems",
    "expiredItems",
    "expiringItems",
    "allItemsExpired",
    "lastItemExpiryDate",
    "updatedAt",
    "active",
    "pcfClusterProjection",
)
_QUEUE_OWNED_FIELDS = (
    "containers",
    "pendingBackendProcessing",
    "processingStatus",
    "status",
    "backendProcessingStatus",
    "completedAt",
    "finishedAt",
    "updatedAt",
    "updatedAtMs",
    "pageStats",
    "progress",
    "pcfClusterProjection",
)


class ProjectionPlanError(RuntimeError):
    """The live Firebase state cannot safely receive this frozen result."""


@dataclass(frozen=True)
class ProjectionSnapshot:
    queue: Mapping[str, Any]
    admission: Mapping[str, Any]
    parents: Mapping[str, Mapping[str, Any]]
    containers: Mapping[str, Mapping[str, Any]]
    revisions: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectionWriteSet:
    work_id: uuid.UUID
    queue_path: str
    parents: Mapping[str, dict[str, Any]]
    containers: Mapping[str, dict[str, Any]]
    queue: dict[str, Any]
    canonical_proofs: tuple[CanonicalObjectProof, ...]
    evidence_sha256: str


def _digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: (
            item.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            if isinstance(item, datetime)
            else str(item)
        ),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _date(value: str) -> date:
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except (TypeError, ValueError) as exc:
        raise ProjectionPlanError("pcf_projection_date_invalid") from exc


def _manifest_identity(pages: tuple[dict[str, str | int], ...]) -> list[dict[str, Any]]:
    return [
        {
            "logicalPageKey": str(page["logicalPageKey"]),
            "containerCode": str(page["containerCode"]),
            "pageNumber": int(page["pageNumber"]),
            "storageBucket": str(page["storageBucket"]),
            "storagePath": str(page["storagePath"]),
            "storageGeneration": str(page["storageGeneration"]),
        }
        for page in sorted(pages, key=lambda row: str(row["logicalPageKey"]))
    ]


def admitted_manifest_hash(pages: tuple[dict[str, str | int], ...]) -> str:
    payload = [
        {
            "logicalPageKey": item["logicalPageKey"],
            "storageBucket": item["storageBucket"],
            "storagePath": item["storagePath"],
            "storageGeneration": item["storageGeneration"],
        }
        for item in _manifest_identity(pages)
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_live_source(effect: EffectInput, snapshot: ProjectionSnapshot) -> None:
    if not isinstance(snapshot.queue, Mapping) or not isinstance(snapshot.admission, Mapping):
        raise ProjectionPlanError("pcf_projection_source_missing")
    queue = snapshot.queue
    admission = snapshot.admission
    expected_manifest = _manifest_identity(effect.pages)
    raw_manifest = admission.get("admittedManifest")
    stamp = queue.get("pcfClusterProjection")
    exact_replay = (
        isinstance(stamp, Mapping)
        and stamp == _stamp(effect)
        and queue.get("pendingBackendProcessing") is False
        and str(queue.get("backendProcessingStatus") or "").lower() == "done"
        and str(queue.get("processingStatus") or "").lower() == "processed"
        and str(queue.get("status") or "").lower() == "processed"
        and "pcfQueueExpiresAt" not in queue
    )
    open_source = (
        queue.get("pendingBackendProcessing") is True
        and str(queue.get("backendProcessingStatus") or "").lower()
        in {"pending", "waiting_images"}
        and str(queue.get("processingStatus") or "").lower()
        in {"pending", "unprocessed"}
        and queue.get("maintenanceOperation") is None
    )
    if (
        admission.get("admissionStatus") != "ready"
        or admission.get("admissionVersion") != 1
        or isinstance(admission.get("admissionEpoch"), bool)
        or not isinstance(admission.get("admissionEpoch"), int)
        or admission["admissionEpoch"] < 1
        or raw_manifest != expected_manifest
        or admission.get("admittedManifestHash") != admitted_manifest_hash(effect.pages)
        or not (open_source or exact_replay)
    ):
        raise ProjectionPlanError("pcf_projection_source_drift")


def validate_projection_source(
    effect: EffectInput,
    snapshot: ProjectionSnapshot,
) -> None:
    """Public read-only preflight used before any canonical Storage copy."""
    if not isinstance(effect, EffectInput) or not isinstance(snapshot, ProjectionSnapshot):
        raise ValueError("invalid PCF projection source proof")
    _validate_live_source(effect, snapshot)


def _assignment_maps(proposal: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    deliveries: dict[str, str] = {}
    loading_dates: dict[str, str] = {}
    for assignment in proposal["deliveryAssignments"]:
        target = str(assignment["deliveryNumber"])
        loading = str(assignment["loadingDate"])
        for container in assignment["containerCodes"]:
            code = str(container)
            if code in deliveries:
                raise ProjectionPlanError("pcf_projection_assignment_duplicate")
            deliveries[code] = target
            loading_dates[code] = loading
    return deliveries, loading_dates


def projection_document_paths(effect: EffectInput) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Return the complete exact parent/container write set without discovery."""
    if not isinstance(effect, EffectInput):
        raise ValueError("invalid PCF effect input")
    targets, _ = _assignment_maps(effect.proposal)
    parent_paths = tuple(
        sorted({f"routes/{effect.route}/pcfs/{delivery}" for delivery in targets.values()})
    )
    container_paths = tuple(
        sorted(
            f"routes/{effect.route}/pcfs/{targets[str(container['containerCode'])]}"
            f"/containers/{container['containerCode']}"
            for container in effect.proposal["containers"]
        )
    )
    if not parent_paths or not container_paths or len(container_paths) > 100:
        raise ProjectionPlanError("pcf_projection_path_set_invalid")
    return parent_paths, container_paths


def _existing_item_map(existing: Mapping[str, Any]) -> dict[tuple[int, str], dict[str, Any]]:
    result: dict[tuple[int, str], dict[str, Any]] = {}
    pages = existing.get("pages")
    if not isinstance(pages, list):
        return result
    for raw_page in pages:
        if not isinstance(raw_page, Mapping):
            continue
        page_number = raw_page.get("pageNumber")
        if isinstance(page_number, bool) or not isinstance(page_number, int):
            continue
        items = raw_page.get("items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, Mapping):
                continue
            product = item.get("product")
            if isinstance(product, str):
                result.setdefault((page_number, product), dict(item))
    return result


def _metrics(pages: list[dict[str, Any]], *, business_date: date) -> dict[str, Any]:
    items = [item for page in pages for item in page["items"]]
    parsed = [_date(item["bestBefore"]) for item in items if item["bestBefore"]]
    expired = sum(value <= business_date for value in parsed)
    expiring = sum(1 <= (value - business_date).days <= 7 for value in parsed)
    latest = max(parsed).isoformat() if parsed else None
    return {
        "totalItems": len(items),
        "expiredItems": expired,
        "expiringItems": expiring,
        "allItemsExpired": bool(items) and expired == len(items),
        "lastItemExpiryDate": latest,
    }


def _stamp(effect: EffectInput) -> dict[str, Any]:
    return {
        "inputHash": effect.input_hash,
        "workAttempt": effect.work_attempt,
        "workId": str(effect.work_id),
    }


def _owned(document: Mapping[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {key: document.get(key) for key in fields}


def build_projection_write_set(
    effect: EffectInput,
    snapshot: ProjectionSnapshot,
    canonical_proofs: tuple[CanonicalObjectProof, ...],
    *,
    now: datetime,
) -> ProjectionWriteSet:
    """Build full merged documents after validating exact live source state."""
    if not isinstance(effect, EffectInput) or not isinstance(snapshot, ProjectionSnapshot):
        raise ValueError("invalid PCF projection input")
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("PCF projection clock must be timezone-aware")
    _validate_live_source(effect, snapshot)
    if len(canonical_proofs) != len(effect.pages):
        raise ProjectionPlanError("pcf_projection_canonical_coverage_mismatch")
    proof_by_source: dict[tuple[str, int], CanonicalObjectProof] = {}
    for proof in canonical_proofs:
        if not isinstance(proof, CanonicalObjectProof):
            raise ProjectionPlanError("pcf_projection_canonical_proof_invalid")
        key = (proof.source_path, proof.source_generation)
        if key in proof_by_source:
            raise ProjectionPlanError("pcf_projection_canonical_proof_duplicate")
        proof_by_source[key] = proof

    targets, loading_dates = _assignment_maps(effect.proposal)
    manifest_by_page = {
        (str(page["containerCode"]), int(page["pageNumber"])): page
        for page in effect.pages
    }
    proposal_by_container = {
        str(container["containerCode"]): container
        for container in effect.proposal["containers"]
    }
    now_utc = effect.accepted_at.astimezone(timezone.utc)
    business_date = effect.accepted_at.astimezone(_DENVER).date()
    stamp = _stamp(effect)
    containers: dict[str, dict[str, Any]] = {}
    parent_deltas: dict[str, dict[str, int]] = {}

    for code, proposal_container in proposal_by_container.items():
        target_delivery = targets[code]
        path = f"routes/{effect.route}/pcfs/{target_delivery}/containers/{code}"
        existing = copy.deepcopy(dict(snapshot.containers.get(path) or {}))
        prior_metrics = {
            key: int(existing.get(key) or 0)
            for key in ("totalItems", "expiredItems", "expiringItems")
        }
        existing_items = _existing_item_map(existing)
        pages: list[dict[str, Any]] = []
        for proposal_page in sorted(
            proposal_container["pages"], key=lambda row: int(row["pageNumber"])
        ):
            page_number = int(proposal_page["pageNumber"])
            manifest_page = manifest_by_page[(code, page_number)]
            source = source_object(
                dict(manifest_page), route=effect.route, delivery=effect.delivery
            )
            proof = proof_by_source.get((source.object_path, source.generation))
            expected_canonical = canonical_path(
                source, target_delivery=target_delivery, target_page=page_number
            )
            if (
                proof is None
                or proof.canonical_path != expected_canonical
                or proof.size_bytes != source.size_bytes
                or proof.md5_hash != source.md5_hash
            ):
                raise ProjectionPlanError("pcf_projection_canonical_proof_mismatch")
            items: list[dict[str, Any]] = []
            for proposal_item in proposal_page["items"]:
                item = {
                    "product": str(proposal_item["product"]),
                    "description": str(proposal_item["description"]),
                    "bestBefore": str(proposal_item["bestBefore"]),
                    "days": str(proposal_item["days"]),
                    "isShortCoded": proposal_item["isShortCoded"] is True,
                    "visibleOnList": True,
                }
                prior = existing_items.get((page_number, item["product"]), {})
                for field in _PRESERVED_ITEM_FIELDS:
                    if field in prior:
                        item[field] = copy.deepcopy(prior[field])
                items.append(item)
            pages.append(
                {
                    "pageNumber": page_number,
                    "resolvedPageNumber": page_number,
                    "imageUrl": expected_canonical,
                    "canonicalImageUrl": expected_canonical,
                    "canonicalStoragePath": expected_canonical,
                    "repairSourcePath": source.object_path,
                    "items": items,
                    "confidence": effect.confidence,
                    "processingStatus": "processed",
                    "pageNumberSource": "upload_order",
                    "pageNumberConfidence": "high",
                    "metadata": {
                        "lineItemCount": len(items),
                        "starredItemCount": sum(item["isShortCoded"] for item in items),
                        "pageNumberSource": "upload_order",
                        "pageNumberConfidence": "high",
                    },
                }
            )
        metrics = _metrics(pages, business_date=business_date)
        merged = existing
        merged.update(
            {
                "containerCode": code,
                "deliveryNumber": target_delivery,
                "loadingDate": loading_dates[code],
                "routeNumber": effect.route,
                "pages": pages,
                **metrics,
                "updatedAt": now_utc,
                "active": True,
                "pcfClusterProjection": stamp,
            }
        )
        merged.setdefault("createdAt", now_utc)
        merged.setdefault(
            "metadata", {"deviceInfo": "Cluster Processor", "appVersion": "cluster-v1"}
        )
        containers[path] = merged
        delta = parent_deltas.setdefault(
            target_delivery,
            {"new": 0, "totalItems": 0, "expiredItems": 0, "expiringItems": 0},
        )
        delta["new"] += 0 if snapshot.containers.get(path) else 1
        for field in ("totalItems", "expiredItems", "expiringItems"):
            delta[field] += int(metrics[field]) - prior_metrics[field]

    parents: dict[str, dict[str, Any]] = {}
    for delivery, delta in parent_deltas.items():
        path = f"routes/{effect.route}/pcfs/{delivery}"
        existing = copy.deepcopy(dict(snapshot.parents.get(path) or {}))
        parent = existing
        parent.update(
            {
                "deliveryNumber": delivery,
                "routeNumber": effect.route,
                "status": "completed",
                "containerCount": int(existing.get("containerCount") or 0) + delta["new"],
                "totalItems": max(0, int(existing.get("totalItems") or 0) + delta["totalItems"]),
                "expiredItems": max(0, int(existing.get("expiredItems") or 0) + delta["expiredItems"]),
                "expiringItems": max(0, int(existing.get("expiringItems") or 0) + delta["expiringItems"]),
                "updatedAt": now_utc,
                "pcfClusterProjection": stamp,
            }
        )
        parent["allItemsExpired"] = (
            parent["totalItems"] > 0 and parent["expiredItems"] == parent["totalItems"]
        )
        latest = [
            value
            for value in (
                existing.get("lastItemExpiryDate"),
                *(
                    container.get("lastItemExpiryDate")
                    for container in containers.values()
                    if container["deliveryNumber"] == delivery
                ),
            )
            if isinstance(value, str) and value
        ]
        parent["lastItemExpiryDate"] = max(latest) if latest else None
        parent.setdefault("createdAt", snapshot.queue.get("createdAt") or now_utc)
        parent.setdefault(
            "metadata",
            copy.deepcopy(snapshot.queue.get("metadata"))
            if isinstance(snapshot.queue.get("metadata"), Mapping)
            else {"deviceInfo": "Cluster Processor", "appVersion": "cluster-v1"},
        )
        parents[path] = parent

    queue = copy.deepcopy(dict(snapshot.queue))
    queue_containers = []
    for code in sorted(proposal_by_container):
        target = targets[code]
        document = containers[
            f"routes/{effect.route}/pcfs/{target}/containers/{code}"
        ]
        queue_containers.append(
            {
                "containerCode": code,
                "loadingDate": loading_dates[code],
                "pages": [
                    {
                        "pageNumber": page["pageNumber"],
                        "resolvedPageNumber": page["resolvedPageNumber"],
                        "imageUrl": page["imageUrl"],
                        "canonicalStoragePath": page["canonicalStoragePath"],
                        "repairSourcePath": page["repairSourcePath"],
                        "processingStatus": "processed",
                        "pageNumberSource": "upload_order",
                        "pageNumberConfidence": "high",
                    }
                    for page in document["pages"]
                ],
            }
        )
    page_count = len(effect.pages)
    queue.update(
        {
            "containers": queue_containers,
            "pendingBackendProcessing": False,
            "processingStatus": "processed",
            "status": "processed",
            "backendProcessingStatus": "done",
            "completedAt": now_utc.isoformat().replace("+00:00", "Z"),
            "finishedAt": now_utc.isoformat().replace("+00:00", "Z"),
            "updatedAt": now_utc,
            "updatedAtMs": int(now_utc.timestamp() * 1000),
            "pageStats": {
                "total": page_count,
                "processed": page_count,
                "failed": 0,
                "pending": 0,
            },
            "progress": 100,
            "pcfClusterProjection": stamp,
        }
    )
    queue.pop("pcfQueueExpiresAt", None)
    evidence = {
        "canonical": sorted(
            proof.evidence_sha256 for proof in canonical_proofs
        ),
        "containers": {
            path: _owned(value, _CONTAINER_OWNED_FIELDS)
            for path, value in sorted(containers.items())
        },
        "parents": {
            path: _owned(value, _PARENT_OWNED_FIELDS)
            for path, value in sorted(parents.items())
        },
        "queue": _owned(queue, _QUEUE_OWNED_FIELDS),
        "queuePath": f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}",
        "workId": str(effect.work_id),
    }
    return ProjectionWriteSet(
        effect.work_id,
        f"pcf_processing_queue/{effect.route}/queue/{effect.delivery}",
        parents,
        containers,
        queue,
        canonical_proofs,
        _digest(evidence),
    )


def verify_projection_documents(
    expected: ProjectionWriteSet,
    actual: ProjectionSnapshot,
) -> str:
    """Compare only cluster-owned fields after an independent exact read."""
    if not isinstance(expected, ProjectionWriteSet) or not isinstance(actual, ProjectionSnapshot):
        raise ValueError("invalid PCF projection verification input")
    mismatches: list[str] = []
    for path, document in expected.parents.items():
        observed = actual.parents.get(path)
        if not isinstance(observed, Mapping) or _owned(observed, _PARENT_OWNED_FIELDS) != _owned(
            document, _PARENT_OWNED_FIELDS
        ):
            mismatches.append(path)
    for path, document in expected.containers.items():
        observed = actual.containers.get(path)
        if not isinstance(observed, Mapping) or _owned(
            observed, _CONTAINER_OWNED_FIELDS
        ) != _owned(document, _CONTAINER_OWNED_FIELDS):
            mismatches.append(path)
    if _owned(actual.queue, _QUEUE_OWNED_FIELDS) != _owned(
        expected.queue, _QUEUE_OWNED_FIELDS
    ) or "pcfQueueExpiresAt" in actual.queue:
        mismatches.append(expected.queue_path)
    if mismatches:
        raise ProjectionPlanError("pcf_projection_readback_mismatch")
    return _digest(
        {
            "projectionEvidenceSha256": expected.evidence_sha256,
            "verifiedPaths": sorted(
                [expected.queue_path, *expected.parents, *expected.containers]
            ),
            "workId": str(expected.work_id),
        }
    )
