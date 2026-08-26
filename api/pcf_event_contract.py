"""Pure helpers for the frozen PCF/shared-projection event contract.

This module deliberately performs no Firebase, PostgreSQL, Storage, Expo, or
network I/O.  P2 ingress can call these helpers before opening a transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


EVENT_SCHEMA_VERSION = 1
BUSINESS_HASH_VERSION = "items-current-business-v2"
WORK_INPUT_HASH_VERSION = "pcf-work-input-v1"

EVENT_TYPES = frozenset(
    {
        "projection.pcf_container.changed",
        "projection.pcf_delivery.changed",
        "projection.notification_recipient.changed",
        "projection.route_entitlement.changed",
    }
)

_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_NUMERIC_ID_RE = re.compile(r"^[0-9]{1,20}$")
_ENTITY_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_EVENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{7,255}$")
_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_BASE64_RE = re.compile(r"^[A-Za-z0-9+/]{22}==$")
_PCF_OBJECT_PATH_RE = re.compile(
    r"^routes/(?P<route>[0-9]{1,10})/unprocessed/"
    r"(?P<delivery>[0-9]{1,20})_(?P<container>[^/]{1,128})_page_"
    r"(?P<page>[0-9]{1,4})\.(?:jpg|jpeg|png)$",
    re.IGNORECASE,
)
_FIRESTORE_REVISION_RE = re.compile(
    r"^fsut1\|"
    r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})T"
    r"(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})\."
    r"(?P<fraction>[0-9]{9})Z\|(?P<rank>[01])$"
)
_GOOGLE_TIMESTAMP_RE = re.compile(
    r"^(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})T"
    r"(?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{3}|[0-9]{6}|[0-9]{9}))?Z$"
)


class EventContractError(ValueError):
    """A deterministic, non-retryable contract validation failure."""


def _require_string(value: Any, name: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value:
        raise EventContractError(f"{name} must be a non-empty string")
    if pattern is not None and pattern.fullmatch(value) is None:
        raise EventContractError(f"{name} has invalid format")
    return value


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], name: str) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise EventContractError(f"{name} keys mismatch: missing={missing}, extra={extra}")


def validate_source_event_id(value: Any) -> str:
    """Validate the transport identity independently of the full envelope."""
    return _require_string(value, "sourceEventId", _EVENT_ID_RE)


def _canonical_json_value(value: Any, path: str = "$") -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        raise EventContractError(f"{path} floats are forbidden in canonical contract JSON")
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key in value:
            if not isinstance(key, str) or not key:
                raise EventContractError(f"{path} object keys must be non-empty strings")
            normalized[key] = _canonical_json_value(value[key], f"{path}.{key}")
        return normalized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_json_value(item, f"{path}[{index}]") for index, item in enumerate(value)]
    raise EventContractError(f"{path} contains unsupported type {type(value).__name__}")


def canonical_json_bytes(value: Any) -> bytes:
    """Return deterministic UTF-8 JSON; arrays retain caller-defined order."""
    normalized = _canonical_json_value(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _domain_hash(version: str, payload: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes({"payload": payload, "version": version})
    ).hexdigest()


def business_hash(payload: Any, version: str = BUSINESS_HASH_VERSION) -> str:
    if version != BUSINESS_HASH_VERSION:
        raise EventContractError("unsupported business hash version")
    return _domain_hash(version, payload)


def canonical_date(value: str, name: str = "date") -> str:
    raw = _require_string(value, name).strip()
    try:
        if "/" in raw:
            month_text, day_text, year_text = raw.split("/")
            parsed = date(int(year_text), int(month_text), int(day_text))
        else:
            parsed = date.fromisoformat(raw)
    except (TypeError, ValueError) as exc:
        raise EventContractError(f"{name} must be M/D/YYYY, MM/DD/YYYY, or YYYY-MM-DD") from exc
    return parsed.isoformat()


def normalize_google_update_time(value: str) -> str:
    """Normalize Google's Z timestamp to fixed nanosecond precision without loss."""
    raw = _require_string(value, "updateTime")
    match = _GOOGLE_TIMESTAMP_RE.fullmatch(raw)
    if match is None:
        raise EventContractError("updateTime must be a Google Z-normalized RFC3339 timestamp")
    try:
        datetime.fromisoformat(f"{match.group('date')}T{match.group('time')}+00:00")
    except ValueError as exc:
        raise EventContractError("updateTime is not a valid calendar instant") from exc
    fraction = (match.group("fraction") or "").ljust(9, "0")
    return f"{match.group('date')}T{match.group('time')}.{fraction}Z"


def firestore_revision(update_time: str, mutation_kind: str) -> str:
    """Build the sortable per-document revision used by projection fencing.

    A delete event uses oldValue.updateTime.  Rank 1 makes that tombstone newer
    than the upsert carrying the same document updateTime.  A later recreation
    still wins because Firestore create/update time increases after recreation.
    """
    if mutation_kind not in {"upsert", "delete"}:
        raise EventContractError("mutationKind must be upsert or delete")
    rank = 1 if mutation_kind == "delete" else 0
    return f"fsut1|{normalize_google_update_time(update_time)}|{rank}"


def _revision_tuple(value: str) -> tuple[str, int]:
    raw = _require_string(value, "sourceRevision")
    match = _FIRESTORE_REVISION_RE.fullmatch(raw)
    if match is None:
        raise EventContractError("sourceRevision has unsupported format")
    normalized = normalize_google_update_time(
        f"{match.group('date')}T{match.group('time')}.{match.group('fraction')}Z"
    )
    return normalized, int(match.group("rank"))


def compare_firestore_revisions(left: str, right: str) -> int:
    left_value = _revision_tuple(left)
    right_value = _revision_tuple(right)
    return (left_value > right_value) - (left_value < right_value)


def projection_decision(
    *,
    current_revision: str | None,
    current_business_hash: str | None,
    current_mutation_kind: str | None,
    incoming_revision: str,
    incoming_business_hash: str,
    incoming_mutation_kind: str,
) -> str:
    """Classify an incoming entity event without performing an effect."""
    _revision_tuple(incoming_revision)
    _require_string(incoming_business_hash, "incomingBusinessHash", _HASH_RE)
    if incoming_mutation_kind not in {"upsert", "delete"}:
        raise EventContractError("incomingMutationKind must be upsert or delete")
    if current_revision is None:
        if current_business_hash is not None or current_mutation_kind is not None:
            raise EventContractError("current projection state is incomplete")
        return "apply"

    _revision_tuple(current_revision)
    _require_string(current_business_hash, "currentBusinessHash", _HASH_RE)
    if current_mutation_kind not in {"upsert", "delete"}:
        raise EventContractError("currentMutationKind must be upsert or delete")

    ordering = compare_firestore_revisions(incoming_revision, current_revision)
    if ordering < 0:
        return "stale_noop"
    if ordering == 0:
        if (
            incoming_business_hash == current_business_hash
            and incoming_mutation_kind == current_mutation_kind
        ):
            return "duplicate_noop"
        return "revision_conflict_reject"
    if (
        incoming_mutation_kind == "upsert"
        and current_mutation_kind == "upsert"
        and incoming_business_hash == current_business_hash
    ):
        return "advance_watermark_only"
    return "apply"


def expiring_semantic_key(user_id: str, expiry_date: str, alert_days: int) -> str:
    user = _require_string(user_id, "userId", _ENTITY_ID_RE)
    if isinstance(alert_days, bool) or not isinstance(alert_days, int) or not 0 <= alert_days <= 365:
        raise EventContractError("alertDays must be an integer from 0 through 365")
    return canonical_json_bytes(
        {
            "alertDays": alert_days,
            "expiryDate": canonical_date(expiry_date, "expiryDate"),
            "userId": user,
        }
    ).decode("utf-8")


def lifecycle_semantic_key(
    *,
    route: str,
    delivery: str,
    container: str,
    transition_kind: str,
    effective_business_date: str,
    source_revision: str,
) -> str:
    route_value = _require_string(route, "route", _ROUTE_RE)
    delivery_value = _require_string(delivery, "delivery", _NUMERIC_ID_RE)
    container_value = _require_string(container, "container", _ENTITY_ID_RE)
    transition = _require_string(transition_kind, "transitionKind")
    if re.fullmatch(r"^[a-z][a-z0-9_]{2,63}$", transition) is None:
        raise EventContractError("transitionKind has invalid format")
    _revision_tuple(source_revision)
    return canonical_json_bytes(
        {
            "container": container_value,
            "delivery": delivery_value,
            "effectiveBusinessDate": canonical_date(
                effective_business_date, "effectiveBusinessDate"
            ),
            "route": route_value,
            "sourceRevision": source_revision,
            "transitionKind": transition,
        }
    ).decode("utf-8")


def pcf_work_input_hash(
    *,
    route: str,
    delivery: str,
    coverage_scope: str,
    objects: Sequence[Mapping[str, Any]],
    version: str = WORK_INPUT_HASH_VERSION,
) -> str:
    if version != WORK_INPUT_HASH_VERSION:
        raise EventContractError("unsupported PCF work-input hash version")
    route_value = _require_string(route, "route", _ROUTE_RE)
    delivery_value = _require_string(delivery, "delivery", _ENTITY_ID_RE)
    if coverage_scope != "uploaded_manifest":
        raise EventContractError("coverageScope must be uploaded_manifest")
    if not isinstance(objects, Sequence) or isinstance(objects, (str, bytes, bytearray)):
        raise EventContractError("objects must be an array")
    if not 1 <= len(objects) <= 100:
        raise EventContractError("objects must contain 1 through 100 entries")

    normalized: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    seen_pages: set[tuple[str, int]] = set()
    for index, raw in enumerate(objects):
        if not isinstance(raw, Mapping):
            raise EventContractError(f"objects[{index}] must be an object")
        _require_exact_keys(
            raw,
            {
                "containerCode",
                "uploadPageNumber",
                "objectPath",
                "generation",
                "size",
                "md5Hash",
            },
            f"objects[{index}]",
        )
        container = _require_string(raw["containerCode"], "containerCode", _ENTITY_ID_RE)
        page = raw["uploadPageNumber"]
        size = raw["size"]
        if isinstance(page, bool) or not isinstance(page, int) or not 1 <= page <= 1000:
            raise EventContractError("uploadPageNumber must be an integer from 1 through 1000")
        if isinstance(size, bool) or not isinstance(size, int) or not 1 <= size <= 20_000_000:
            raise EventContractError("size must be an integer from 1 through 20000000")
        path = _require_string(raw["objectPath"], "objectPath")
        path_match = _PCF_OBJECT_PATH_RE.fullmatch(path)
        if (
            path_match is None
            or path_match.group("route") != route_value
            or path_match.group("delivery") != delivery_value
            or path_match.group("container") != container
            or int(path_match.group("page")) != page
            or len(path.encode("utf-8")) > 1024
        ):
            raise EventContractError("objectPath does not match route/delivery/container/page")
        generation = _require_string(raw["generation"], "generation")
        if re.fullmatch(r"[1-9][0-9]{0,19}", generation) is None:
            raise EventContractError("generation must be a positive decimal string")
        md5_hash = _require_string(raw["md5Hash"], "md5Hash")
        if _MD5_BASE64_RE.fullmatch(md5_hash) is None:
            raise EventContractError("md5Hash must be a base64-encoded MD5 digest")
        logical_page = (container, page)
        if path in seen_paths:
            raise EventContractError("duplicate objectPath")
        if logical_page in seen_pages:
            raise EventContractError("duplicate containerCode/uploadPageNumber")
        seen_paths.add(path)
        seen_pages.add(logical_page)
        normalized.append(
            {
                "containerCode": container,
                "generation": generation,
                "md5Hash": md5_hash,
                "objectPath": path,
                "size": size,
                "uploadPageNumber": page,
            }
        )

    normalized.sort(key=lambda item: (item["objectPath"], int(item["generation"])))
    payload = {
        "coverageScope": coverage_scope,
        "delivery": delivery_value,
        "objects": normalized,
        "route": route_value,
    }
    # The architecture contract hashes this payload directly, without an extra
    # wrapper; input_hash_version is persisted beside the digest.
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def validate_pcf_upload_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the frozen top-level ``pcf.upload.settled`` event envelope."""
    if not isinstance(event, Mapping):
        raise EventContractError("event must be an object")
    expected = {
        "eventId",
        "eventType",
        "schemaVersion",
        "route",
        "delivery",
        "occurredAt",
        "sourceRevision",
        "quietPeriodElapsed",
        "coverageScope",
        "manifest",
        "sourceQueuePath",
        "automationExcluded",
        "fixture",
    }
    _require_exact_keys(event, expected, "event")
    if len(canonical_json_bytes(event)) > 65_536:
        raise EventContractError("event exceeds the 65536-byte transport body ceiling")
    _require_string(event["eventId"], "eventId", _EVENT_ID_RE)
    if event["eventType"] != "pcf.upload.settled":
        raise EventContractError("unsupported eventType")
    if event["schemaVersion"] != EVENT_SCHEMA_VERSION:
        raise EventContractError("unsupported schemaVersion")
    route = _require_string(event["route"], "route", _ROUTE_RE)
    delivery = _require_string(event["delivery"], "delivery", _NUMERIC_ID_RE)
    normalize_google_update_time(event["occurredAt"])
    _, revision_rank = _revision_tuple(event["sourceRevision"])
    if revision_rank != 0:
        raise EventContractError("pcf.upload.settled sourceRevision must be an upsert revision")
    if event["quietPeriodElapsed"] is not True:
        raise EventContractError("quietPeriodElapsed must be true")
    if event["coverageScope"] != "uploaded_manifest":
        raise EventContractError("coverageScope must be uploaded_manifest")
    if event["sourceQueuePath"] != f"pcf_processing_queue/{route}/queue/{delivery}":
        raise EventContractError("sourceQueuePath does not match route/delivery")
    if not isinstance(event["automationExcluded"], bool):
        raise EventContractError("automationExcluded must be boolean")
    fixture = event["fixture"]
    if event["automationExcluded"]:
        if not isinstance(fixture, Mapping):
            raise EventContractError("excluded events require exact fixture identity")
        _require_exact_keys(fixture, {"identity", "generation"}, "fixture")
        _require_string(fixture["identity"], "fixture.identity", _ENTITY_ID_RE)
        _require_string(fixture["generation"], "fixture.generation")
    elif fixture is not None:
        raise EventContractError("non-excluded events must not carry fixture identity")

    manifest = event["manifest"]
    if not isinstance(manifest, Mapping):
        raise EventContractError("manifest must be an object")
    _require_exact_keys(manifest, {"containerCount", "pageCount", "objects"}, "manifest")
    objects = manifest["objects"]
    if not isinstance(objects, Sequence) or isinstance(objects, (str, bytes, bytearray)):
        raise EventContractError("manifest.objects must be an array")
    page_count = manifest["pageCount"]
    container_count = manifest["containerCount"]
    for value, name in ((page_count, "pageCount"), (container_count, "containerCount")):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise EventContractError(f"{name} must be a positive integer")
    if page_count != len(objects):
        raise EventContractError("pageCount does not match manifest.objects")
    actual_containers = {
        item.get("containerCode") for item in objects if isinstance(item, Mapping)
    }
    if container_count != len(actual_containers):
        raise EventContractError("containerCount does not match manifest.objects")
    input_hash = pcf_work_input_hash(
        route=route,
        delivery=delivery,
        coverage_scope=event["coverageScope"],
        objects=objects,
    )
    return {
        "event": dict(event),
        "inputHash": input_hash,
        "inputHashVersion": WORK_INPUT_HASH_VERSION,
    }


def validate_projection_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Validate the common projection envelope and exact identity/path binding."""
    if not isinstance(event, Mapping):
        raise EventContractError("event must be an object")
    if len(canonical_json_bytes(event)) > 65_536:
        raise EventContractError("event exceeds the 65536-byte transport body ceiling")
    expected = {
        "schemaVersion",
        "sourceEventId",
        "eventType",
        "mutationKind",
        "occurredAt",
        "sourcePath",
        "sourceRevision",
        "entity",
        "automationExcluded",
        "fixture",
        "payload",
    }
    _require_exact_keys(event, expected, "event")
    if event["schemaVersion"] != EVENT_SCHEMA_VERSION:
        raise EventContractError("unsupported schemaVersion")
    validate_source_event_id(event["sourceEventId"])
    event_type = _require_string(event["eventType"], "eventType")
    if event_type not in EVENT_TYPES:
        raise EventContractError("unsupported eventType")
    mutation_kind = event["mutationKind"]
    if mutation_kind not in {"upsert", "delete"}:
        raise EventContractError("mutationKind must be upsert or delete")
    normalize_google_update_time(event["occurredAt"])
    source_revision = _require_string(event["sourceRevision"], "sourceRevision")
    _, rank = _revision_tuple(source_revision)
    if rank != (1 if mutation_kind == "delete" else 0):
        raise EventContractError("sourceRevision operation rank does not match mutationKind")
    if not isinstance(event["automationExcluded"], bool):
        raise EventContractError("automationExcluded must be boolean")
    fixture = event["fixture"]
    if event["automationExcluded"]:
        if not isinstance(fixture, Mapping):
            raise EventContractError("excluded events require exact fixture identity")
        _require_exact_keys(fixture, {"identity", "generation"}, "fixture")
        _require_string(fixture["identity"], "fixture.identity", _ENTITY_ID_RE)
        _require_string(fixture["generation"], "fixture.generation")
    elif fixture is not None:
        raise EventContractError("non-excluded events must not carry fixture identity")

    entity = event["entity"]
    if not isinstance(entity, Mapping):
        raise EventContractError("entity must be an object")
    path = _require_string(event["sourcePath"], "sourcePath")
    if event_type == "projection.pcf_container.changed":
        _require_exact_keys(entity, {"route", "delivery", "container"}, "entity")
        route = _require_string(entity["route"], "entity.route", _ROUTE_RE)
        delivery = _require_string(entity["delivery"], "entity.delivery", _ENTITY_ID_RE)
        container = _require_string(entity["container"], "entity.container", _ENTITY_ID_RE)
        expected_path = f"routes/{route}/pcfs/{delivery}/containers/{container}"
    elif event_type == "projection.pcf_delivery.changed":
        _require_exact_keys(entity, {"route", "delivery"}, "entity")
        route = _require_string(entity["route"], "entity.route", _ROUTE_RE)
        delivery = _require_string(entity["delivery"], "entity.delivery", _ENTITY_ID_RE)
        expected_path = f"routes/{route}/pcfs/{delivery}"
    elif event_type == "projection.notification_recipient.changed":
        _require_exact_keys(entity, {"userId"}, "entity")
        user_id = _require_string(entity["userId"], "entity.userId", _ENTITY_ID_RE)
        expected_path = f"users/{user_id}"
    else:
        _require_exact_keys(entity, {"route"}, "entity")
        route = _require_string(entity["route"], "entity.route", _ROUTE_RE)
        expected_path = f"routeEntitlements/{route}"
    if path != expected_path:
        raise EventContractError("sourcePath does not match event entity")
    if mutation_kind == "delete" and event["payload"] is not None:
        raise EventContractError("delete payload must be null")
    if mutation_kind == "upsert" and not isinstance(event["payload"], Mapping):
        raise EventContractError("upsert payload must be an object")
    normalized = dict(event)
    normalized["entity"] = dict(entity)
    normalized["payload"] = _normalize_projection_payload(
        event_type=event_type,
        mutation_kind=mutation_kind,
        payload=event["payload"],
    )
    return normalized


def _require_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise EventContractError(f"{name} must be boolean")
    return value


def _nullable_date(value: Any, name: str) -> str | None:
    return None if value is None else canonical_date(value, name)


def _nullable_guarantee(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EventContractError("guaranteed must be null or an object")
    _require_exact_keys(value, {"isGuaranteed", "guaranteeExpiresAt"}, "guaranteed")
    if value["isGuaranteed"] is not True:
        raise EventContractError("guaranteed.isGuaranteed must be true when present")
    return {
        "isGuaranteed": True,
        "guaranteeExpiresAt": normalize_google_update_time(
            value["guaranteeExpiresAt"]
        ),
    }


def _normalize_projection_payload(
    *, event_type: str, mutation_kind: str, payload: Any
) -> dict[str, Any] | None:
    if mutation_kind == "delete":
        return None
    assert isinstance(payload, Mapping)
    if event_type == "projection.pcf_container.changed":
        _require_exact_keys(
            payload, {"active", "loadingDate", "pageNumbers", "items"}, "payload"
        )
        raw_page_numbers = payload["pageNumbers"]
        if not isinstance(raw_page_numbers, Sequence) or isinstance(
            raw_page_numbers, (str, bytes, bytearray)
        ):
            raise EventContractError("payload.pageNumbers must be an array")
        page_numbers: list[int] = []
        for page_number in raw_page_numbers:
            if (
                isinstance(page_number, bool)
                or not isinstance(page_number, int)
                or not 1 <= page_number <= 1000
            ):
                raise EventContractError(
                    "pageNumbers entries must be integers from 1 through 1000"
                )
            page_numbers.append(page_number)
        if len(page_numbers) != len(set(page_numbers)):
            raise EventContractError("pageNumbers must be unique")
        page_numbers.sort()
        page_number_set = set(page_numbers)
        items = payload["items"]
        if not isinstance(items, Sequence) or isinstance(items, (str, bytes, bytearray)):
            raise EventContractError("payload.items must be an array")
        if len(items) > 5000:
            raise EventContractError("payload.items exceeds 5000 entries")
        normalized_items: list[dict[str, Any]] = []
        seen_positions: set[tuple[int, int]] = set()
        item_keys = {
            "pageNumber",
            "itemPosition",
            "product",
            "description",
            "expiryDate",
            "visibleOnList",
            "isLowQuantity",
            "isShortCoded",
            "guaranteed",
        }
        for index, item in enumerate(items):
            if not isinstance(item, Mapping):
                raise EventContractError(f"payload.items[{index}] must be an object")
            _require_exact_keys(item, item_keys, f"payload.items[{index}]")
            page_number = item["pageNumber"]
            item_position = item["itemPosition"]
            if (
                isinstance(page_number, bool)
                or not isinstance(page_number, int)
                or not 1 <= page_number <= 1000
            ):
                raise EventContractError("pageNumber must be an integer from 1 through 1000")
            if (
                isinstance(item_position, bool)
                or not isinstance(item_position, int)
                or not 0 <= item_position <= 9999
            ):
                raise EventContractError("itemPosition must be an integer from 0 through 9999")
            position = (page_number, item_position)
            if position in seen_positions:
                raise EventContractError("duplicate pageNumber/itemPosition")
            if page_number not in page_number_set:
                raise EventContractError("item pageNumber is absent from pageNumbers")
            seen_positions.add(position)
            product = _require_string(item["product"], "product").strip()
            if len(product) > 128:
                raise EventContractError("product is too long")
            description = item["description"]
            if not isinstance(description, str) or len(description) > 512:
                raise EventContractError("description must be a string of at most 512 characters")
            normalized_items.append(
                {
                    "description": description.strip(),
                    "expiryDate": _nullable_date(item["expiryDate"], "expiryDate"),
                    "guaranteed": _nullable_guarantee(item["guaranteed"]),
                    "isLowQuantity": _require_bool(item["isLowQuantity"], "isLowQuantity"),
                    "isShortCoded": _require_bool(item["isShortCoded"], "isShortCoded"),
                    "itemPosition": item_position,
                    "pageNumber": page_number,
                    "product": product,
                    "visibleOnList": _require_bool(item["visibleOnList"], "visibleOnList"),
                }
            )
        normalized_items.sort(key=lambda item: (item["pageNumber"], item["itemPosition"]))
        return {
            "active": _require_bool(payload["active"], "active"),
            "items": normalized_items,
            "loadingDate": _nullable_date(payload["loadingDate"], "loadingDate"),
            "pageNumbers": page_numbers,
        }

    if event_type == "projection.pcf_delivery.changed":
        _require_exact_keys(payload, {"active", "loadingDate", "status"}, "payload")
        status = payload["status"]
        if status is not None:
            status = _require_string(status, "status").strip().lower()
            if len(status) > 64:
                raise EventContractError("status is too long")
        return {
            "active": _require_bool(payload["active"], "active"),
            "loadingDate": _nullable_date(payload["loadingDate"], "loadingDate"),
            "status": status,
        }

    if event_type == "projection.notification_recipient.changed":
        expected = {
            "enabled",
            "expirationAlertDays",
            "deliveryMinuteLocal",
            "timezone",
            "routeNumbers",
            "hasPushToken",
            "tokenSetHash",
        }
        _require_exact_keys(payload, expected, "payload")
        alert_days = payload["expirationAlertDays"]
        if alert_days is not None and (
            isinstance(alert_days, bool)
            or not isinstance(alert_days, int)
            or not 0 <= alert_days <= 365
        ):
            raise EventContractError("expirationAlertDays must be null or 0 through 365")
        minute = payload["deliveryMinuteLocal"]
        if minute is not None and (
            isinstance(minute, bool) or not isinstance(minute, int) or not 0 <= minute <= 1439
        ):
            raise EventContractError("deliveryMinuteLocal must be null or 0 through 1439")
        timezone_name = _require_string(payload["timezone"], "timezone").strip()
        try:
            ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError as exc:
            raise EventContractError("timezone must be a known IANA zone") from exc
        routes = payload["routeNumbers"]
        if not isinstance(routes, Sequence) or isinstance(routes, (str, bytes, bytearray)):
            raise EventContractError("routeNumbers must be an array")
        normalized_routes = sorted({_require_string(route, "routeNumber", _ROUTE_RE) for route in routes})
        if len(normalized_routes) != len(routes):
            raise EventContractError("routeNumbers must be unique")
        has_token = _require_bool(payload["hasPushToken"], "hasPushToken")
        token_hash = payload["tokenSetHash"]
        if has_token:
            token_hash = _require_string(token_hash, "tokenSetHash", _HASH_RE)
        elif token_hash is not None:
            raise EventContractError("tokenSetHash must be null when hasPushToken is false")
        return {
            "deliveryMinuteLocal": minute,
            "enabled": _require_bool(payload["enabled"], "enabled"),
            "expirationAlertDays": alert_days,
            "hasPushToken": has_token,
            "routeNumbers": normalized_routes,
            "timezone": timezone_name,
            "tokenSetHash": token_hash,
        }

    _require_exact_keys(
        payload,
        {"active", "ownerUserId", "entitlementFingerprint"},
        "payload",
    )
    owner = payload["ownerUserId"]
    if owner is not None:
        owner = _require_string(owner, "ownerUserId", _ENTITY_ID_RE)
    fingerprint = _require_string(
        payload["entitlementFingerprint"], "entitlementFingerprint", _HASH_RE
    )
    return {
        "active": _require_bool(payload["active"], "active"),
        "entitlementFingerprint": fingerprint,
        "ownerUserId": owner,
    }


def projection_event_business_hash(event: Mapping[str, Any]) -> str:
    normalized = validate_projection_event(event)
    if normalized["mutationKind"] == "delete":
        payload: Any = {"entity": normalized["entity"], "tombstone": True}
    else:
        payload = {"entity": normalized["entity"], "payload": normalized["payload"]}
    return business_hash(payload)


def utc_now_string() -> str:
    """Used only by future callers for audit fields, never as a source revision."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
