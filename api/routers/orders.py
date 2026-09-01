"""Orders router - create, update, finalize, and audit orders."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from google.cloud import firestore
from schedule_cycle import resolve_order_schedule_for_delivery_date

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    require_route_feature_access,
    get_firestore,
    get_route_timezone,
)
from ..errors import StructuredApiError
from ..middleware.rate_limit import (
    rate_limit_order_read,
    rate_limit_order_update,
    rate_limit_write,
)
from ..order_attention import build_core_item_issues, build_next_order_store_updates
from ..order_adjustment_schedule_v2_release import stamp_write_allowed_for_headers
from ..order_update import merge_order_store_patches
from ..models import (
    AdjustmentReminderReconcileRequest,
    AdjustmentReminderReconcileResponse,
    Order,
    OrderCreateRequest,
    OrderUpdateRequest,
    StoreReallocationRequest,
    StoreReallocationResponse,
    FullOrderAdjustmentConfirmRequest,
    FullOrderAdjustmentConfirmResponse,
    ErrorResponse,
)
from ..firestore_timestamp import timestamp_ms
from ..order_full_adjustment import validate_and_merge_full_adjustment
from ..order_reallocation import apply_store_reallocation, moves_signature
try:
    from ...scripts.order_adjustment_schedule import (
        build_canonical_reminder_fields,
        meaningful_lines,
        protected_existing_fields,
        reminder_fields_equal,
        trusted_adjustment_settings,
    )
except ImportError:
    from scripts.order_adjustment_schedule import (
        build_canonical_reminder_fields,
        meaningful_lines,
        protected_existing_fields,
        reminder_fields_equal,
        trusted_adjustment_settings,
    )

try:
    from ...scripts.finalize_rollout import api_finalize_rollout_enabled_for_route
except ImportError:
    from scripts.finalize_rollout import api_finalize_rollout_enabled_for_route

from .schedule import (
    get_order_schedule_context_from_firestore,
    get_schedule_timezone_from_firestore,
)


router = APIRouter()


@firestore.transactional
def _reconcile_adjustment_reminder_document(
    transaction,
    *,
    adjustment_ref,
    source_ref,
    settings_ref,
    route_number: str,
    order_id: str,
    adjustment_id: str,
    authenticated_uid: str,
    payload: AdjustmentReminderReconcileRequest,
    now_ms: int,
) -> Dict[str, Any]:
    adjustment_snap = adjustment_ref.get(transaction=transaction)
    source_snap = source_ref.get(transaction=transaction)
    settings_snap = settings_ref.get(transaction=transaction)

    if not adjustment_snap.exists:
        raise StructuredApiError(404, "Adjustment missing", "ADJUSTMENT_NOT_FOUND")
    if not source_snap.exists:
        raise StructuredApiError(404, "Adjustment source missing", "MISSING_SOURCE_ORDER")
    if not settings_snap.exists:
        raise StructuredApiError(422, "Adjustment settings missing", "MISSING_ADJUSTMENT_SETTINGS")

    adjustment = adjustment_snap.to_dict() or {}
    source = source_snap.to_dict() or {}
    requester = settings_snap.to_dict() or {}
    if adjustment.get("routeNumber") != route_number:
        raise StructuredApiError(409, "Adjustment route changed", "ADJUSTMENT_ROUTE_CHANGED")
    if source.get("routeNumber") != route_number:
        raise StructuredApiError(404, "Adjustment source missing", "MISSING_SOURCE_ORDER")
    if adjustment.get("sourceOrderId") != order_id:
        raise StructuredApiError(409, "Adjustment source changed", "ADJUSTMENT_SOURCE_CHANGED")

    user_settings = requester.get("userSettings") or {}
    if not isinstance(user_settings, Mapping):
        raise StructuredApiError(422, "Adjustment settings missing", "MISSING_ADJUSTMENT_SETTINGS")
    notifications = user_settings.get("notifications") or {}
    if not isinstance(notifications, Mapping):
        raise StructuredApiError(422, "Adjustment settings missing", "MISSING_ADJUSTMENT_SETTINGS")
    raw_adjustment_settings = notifications.get("orderAdjustmentSettings") or {}
    if not isinstance(raw_adjustment_settings, Mapping):
        raise StructuredApiError(422, "Adjustment settings missing", "MISSING_ADJUSTMENT_SETTINGS")
    settings = trusted_adjustment_settings(
        raw_adjustment_settings
    )
    timestamp_container = user_settings.get("timestamps") or {}
    if not isinstance(timestamp_container, Mapping):
        raise StructuredApiError(422, "Invalid settings revision", "INVALID_SETTINGS_REVISION")
    settings_updated_at_ms = timestamp_ms(
        timestamp_container.get("updatedAt")
    )
    if (
        payload.expectedSettingsUpdatedAtMs is not None
        and payload.expectedSettingsUpdatedAtMs != settings_updated_at_ms
    ):
        raise StructuredApiError(409, "Settings changed", "SETTINGS_REVISION_CHANGED")

    if (
        str(adjustment.get("status") or "draft") != "draft"
        or str(adjustment.get("reminderStatus") or "none") in {"sending", "sent"}
    ):
        return {
            "orderId": order_id,
            "adjustmentId": adjustment_id,
            "changed": False,
            "fields": protected_existing_fields(adjustment),
        }

    fields = build_canonical_reminder_fields(
        source,
        settings=settings,
        has_meaningful_lines=meaningful_lines(adjustment),
        status=str(adjustment.get("status") or "draft"),
        now_ms=now_ms,
    )
    existing_skipped_at_ms = adjustment.get("reminderSkippedAtMs")
    if (
        fields.get("reminderStatus") == "skipped"
        and adjustment.get("reminderStatus") == "skipped"
        and fields.get("reminderSkipReason") == adjustment.get("reminderSkipReason")
        and isinstance(existing_skipped_at_ms, int)
        and not isinstance(existing_skipped_at_ms, bool)
        and existing_skipped_at_ms >= 0
    ):
        # reminderSkippedAtMs records the transition into this skip state. It
        # must not become a fresh "now" value on every trusted reconcile.
        fields["reminderSkippedAtMs"] = existing_skipped_at_ms
    if reminder_fields_equal(adjustment, fields):
        return {
            "orderId": order_id,
            "adjustmentId": adjustment_id,
            "changed": False,
            "fields": fields,
        }

    transaction.update(
        adjustment_ref,
        {
            **fields,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
    )
    return {
        "orderId": order_id,
        "adjustmentId": adjustment_id,
        "changed": True,
        "fields": fields,
    }
API_FORECAST_WORKER_ID = "api-finalize"
CORE_ITEM_ENFORCEMENT_ENABLED = (
    os.environ.get("CORE_ITEM_ENFORCEMENT_ENABLED", "false").lower() == "true"
)
logger = logging.getLogger("api.orders")

def _normalize_route_number(value: Any) -> str:
    v = str(value or "").strip()
    return v if v.isdigit() and len(v) <= 10 else ""


def _resolve_route_group_id(
    db: firestore.Client,
    *,
    requester_user_data: Dict[str, Any],
    order_route_number: str,
) -> str:
    """Resolve the master routeGroupId for transfer ledger operations.

    Transfers are grouped under the *owner's* primary route number (routeGroupId).
    For owners, this is `users/{uid}.profile.routeNumber`.
    For team members, resolve via `routes/{route}.ownerUid` then owner's profile.routeNumber.
    """
    profile = (requester_user_data or {}).get("profile", {}) or {}
    role = str(profile.get("role") or "").strip()
    if role == "owner":
        master = _normalize_route_number(profile.get("routeNumber"))
        if master:
            return master

    # Team member path: route doc -> owner uid -> owner's primary route number.
    route = _normalize_route_number(order_route_number)
    if not route:
        return ""

    try:
        route_doc = db.collection("routes").document(route).get()
        if not route_doc.exists:
            return ""
        route_data = route_doc.to_dict() or {}
        owner_uid = str(route_data.get("ownerUid") or route_data.get("userId") or "").strip()
        if not owner_uid:
            return ""
        owner_doc = db.collection("users").document(owner_uid).get()
        if not owner_doc.exists:
            return ""
        owner_data = owner_doc.to_dict() or {}
        owner_profile = owner_data.get("profile", {}) or {}
        return _normalize_route_number(owner_profile.get("routeNumber"))
    except Exception:
        return ""


def _valid_iana_timezone(value: Any) -> Optional[str]:
    timezone_name = str(value or "").strip()
    if not timezone_name:
        return None
    try:
        ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return timezone_name


def _require_route_owner_timezone(db: firestore.Client, route_number: str) -> str:
    timezone_name, status = get_schedule_timezone_from_firestore(db, route_number)
    if timezone_name:
        return timezone_name
    errors = {
        "missing_owner": ("Route owner is not configured.", "ROUTE_OWNER_MISSING"),
        "owner_conflict": ("Route owner records disagree.", "ROUTE_OWNER_CONFLICT"),
        "missing_owner_user": ("Route owner profile is missing.", "ROUTE_OWNER_PROFILE_MISSING"),
        "missing_timezone": ("Select a business timezone.", "ROUTE_TIMEZONE_MISSING"),
        "invalid_timezone": ("Select a valid business timezone.", "ROUTE_TIMEZONE_INVALID"),
    }
    message, code = errors.get(status, errors["missing_owner"])
    raise StructuredApiError(422, message, code)


def _build_scheduled_order(
    resolution: Mapping[str, Any],
    timezone_name: str,
    *,
    provenance: str = "cycle",
) -> Dict[str, Any]:
    cycle = resolution["cycle"]
    return {
        "version": 2,
        "date": resolution["orderDate"],
        "timezone": timezone_name,
        "scheduleKey": resolution["scheduleKey"],
        "orderDay": cycle["orderDay"],
        "loadOffsetDays": cycle["loadOffsetDays"],
        "deliveryOffsetDays": cycle["deliveryOffsetDays"],
        "provenance": provenance,
    }


def _resolve_create_schedule(
    db: firestore.Client,
    payload: OrderCreateRequest,
) -> tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    try:
        schedule_context = get_order_schedule_context_from_firestore(db, payload.routeNumber)
        cycles = list(schedule_context["cycles"])
    except Exception as exc:
        logger.warning("Order schedule lookup failed route=%s: %s", payload.routeNumber, exc)
        raise StructuredApiError(
            503,
            "Could not load the route order schedule. Try again.",
            "ORDER_SCHEDULE_UNAVAILABLE",
        ) from exc
    resolution = resolve_order_schedule_for_delivery_date(
        payload.deliveryDate,
        cycles,
        payload.scheduleKey,
    )
    if resolution["status"] == "unresolved":
        if not payload.isHolidaySchedule:
            code = {
                "missing_schedule": "ORDER_SCHEDULE_MISSING",
                "ambiguous_schedule": "ORDER_SCHEDULE_AMBIGUOUS",
                "off_cycle": "ORDER_SCHEDULE_OFF_CYCLE",
            }[resolution["reason"]]
            raise StructuredApiError(
                400,
                "Delivery date does not match one configured order cycle.",
                code,
            )
        if not payload.allowUnresolvedHolidaySchedule:
            raise StructuredApiError(
                409,
                "Acknowledge that this holiday order will have no Order Adjustment window.",
                "HOLIDAY_SCHEDULE_ACK_REQUIRED",
            )
        return None, None

    timezone_name = schedule_context.get("timezone")
    if not timezone_name:
        errors = {
            "missing_owner": ("Route owner is not configured.", "ROUTE_OWNER_MISSING"),
            "owner_conflict": ("Route owner records disagree.", "ROUTE_OWNER_CONFLICT"),
            "missing_owner_user": ("Route owner profile is missing.", "ROUTE_OWNER_PROFILE_MISSING"),
            "missing_timezone": ("Select a business timezone.", "ROUTE_TIMEZONE_MISSING"),
            "invalid_timezone": ("Select a valid business timezone.", "ROUTE_TIMEZONE_INVALID"),
        }
        message, code = errors.get(str(schedule_context.get("status")), errors["missing_owner"])
        raise StructuredApiError(422, message, code)
    return resolution, _build_scheduled_order(resolution, timezone_name)


def _get_local_order_date(db: firestore.Client, route_number: str) -> str:
    """Return local date (YYYY-MM-DD) using route owner's timezone when possible."""
    tz_name = get_route_timezone(db, route_number)
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            return datetime.now(ZoneInfo(tz_name)).date().isoformat()
        except Exception:
            pass
    return datetime.utcnow().date().isoformat()


def _resolve_finalize_stamp(
    db: firestore.Client,
    *,
    route_number: str,
    order_data: Mapping[str, Any],
) -> tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    delivery_date = str(order_data.get("expectedDeliveryDate") or "").strip()
    schedule_key = str(order_data.get("scheduleKey") or "").strip().lower()
    if not delivery_date or not schedule_key:
        return None, None, None
    try:
        schedule_context = get_order_schedule_context_from_firestore(db, route_number)
        cycles = list(schedule_context["cycles"])
        resolution = resolve_order_schedule_for_delivery_date(
            delivery_date,
            cycles,
            schedule_key,
        )
        timezone_name = schedule_context.get("timezone")
    except Exception as exc:
        logger.warning("Finalize stamp resolution failed route=%s: %s", route_number, exc)
        return None, delivery_date, schedule_key
    if resolution["status"] != "resolved" or timezone_name is None:
        return None, delivery_date, schedule_key
    return _build_scheduled_order(resolution, timezone_name), delivery_date, schedule_key


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    return None


def _order_ref(db: firestore.Client, route_number: str, order_id: str):
    """Build a route-scoped order document reference."""
    return (
        db.collection("routes")
        .document(route_number)
        .collection("orders")
        .document(order_id)
    )


def _requester_is_route_owner(user_data: Dict[str, Any], route_number: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if (
        str(profile.get("role") or "").strip().lower() == "owner"
        and _normalize_route_number(profile.get("routeNumber")) == route_number
    ):
        return True
    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return (
        isinstance(assignment, dict)
        and str(assignment.get("role") or "").strip().lower() == "owner"
    )


def _order_revision(order_data: Dict[str, Any]) -> int:
    value = order_data.get("orderRevision")
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _batch_base_order_revision(batch: Dict[str, Any]) -> int:
    explicit = batch.get("baseOrderRevision")
    if isinstance(explicit, int) and not isinstance(explicit, bool) and explicit >= 0:
        return explicit
    working = batch.get("workingCopySnapshot") or {}
    return _order_revision(working)


def _prove_intervening_reallocations(
    transaction,
    *,
    order_id: str,
    base_revision: int,
    current_revision: int,
    order_audit_ref,
    route_adjustments_ref,
) -> set[str]:
    """Return affected SAPs only when every intervening revision is a verified reallocation."""
    if current_revision == base_revision:
        return set()
    if current_revision < base_revision:
        raise StructuredApiError(
            409,
            "The sent adjustment revision is newer than the finalized order.",
            "ADJUSTMENT_REBASE_UNSAFE",
            {"baseOrderRevision": base_revision, "currentOrderRevision": current_revision},
        )

    by_applied_revision: Dict[int, List[Dict[str, Any]]] = {}
    for audit_doc in order_audit_ref.stream(transaction=transaction):
        audit = audit_doc.to_dict() or {}
        meta = audit.get("meta") or {}
        applied_revision = meta.get("appliedOrderRevision")
        if (
            audit.get("action") == "order_store_reallocated"
            and isinstance(applied_revision, int)
            and not isinstance(applied_revision, bool)
            and base_revision < applied_revision <= current_revision
        ):
            by_applied_revision.setdefault(applied_revision, []).append(audit)

    affected_saps: set[str] = set()
    for applied_revision in range(base_revision + 1, current_revision + 1):
        candidates = by_applied_revision.get(applied_revision) or []
        if len(candidates) != 1:
            raise StructuredApiError(
                409,
                "The finalized order history cannot be safely rebased.",
                "ADJUSTMENT_REBASE_UNSAFE",
                {"baseOrderRevision": base_revision, "currentOrderRevision": current_revision},
            )
        meta = candidates[0].get("meta") or {}
        reallocation_id = str(meta.get("reallocationId") or "")
        if meta.get("baseOrderRevision") != applied_revision - 1 or not reallocation_id:
            raise StructuredApiError(
                409,
                "The finalized order history cannot be safely rebased.",
                "ADJUSTMENT_REBASE_UNSAFE",
                {"baseOrderRevision": base_revision, "currentOrderRevision": current_revision},
            )

        mutation_doc = route_adjustments_ref.document(reallocation_id).get(transaction=transaction)
        mutation = mutation_doc.to_dict() if mutation_doc.exists else {}
        receipt = (mutation or {}).get("storeReallocation") or {}
        moves = receipt.get("moves") or []
        if (
            (mutation or {}).get("mode") != "store_reallocation"
            or str((mutation or {}).get("sourceOrderId") or "") != order_id
            or receipt.get("baseOrderRevision") != applied_revision - 1
            or receipt.get("appliedOrderRevision") != applied_revision
            or not moves
        ):
            raise StructuredApiError(
                409,
                "The finalized order history cannot be safely rebased.",
                "ADJUSTMENT_REBASE_UNSAFE",
                {"baseOrderRevision": base_revision, "currentOrderRevision": current_revision},
            )
        move_saps = {str(move.get("sap") or "").strip() for move in moves}
        if "" in move_saps:
            raise StructuredApiError(
                409,
                "The finalized order history cannot be safely rebased.",
                "ADJUSTMENT_REBASE_UNSAFE",
                {"baseOrderRevision": base_revision, "currentOrderRevision": current_revision},
            )
        affected_saps.update(move_saps)
    return affected_saps


def _validate_adjusted_store_allocations(
    order_data: Dict[str, Any],
    route_stores: Dict[str, Dict[str, Any]],
    *,
    prior_order_data: Dict[str, Any],
) -> None:
    """Validate merged truth without re-litigating untouched catalog history."""
    def allocation_quantities(data: Dict[str, Any]) -> Dict[tuple[str, str], int]:
        result: Dict[tuple[str, str], int] = {}
        for raw_store in data.get("stores") or []:
            store_id = str(raw_store.get("storeId") or raw_store.get("id") or "").strip()
            for raw_item in raw_store.get("items") or []:
                sap = str(raw_item.get("sap") or "").strip()
                quantity = raw_item.get("quantity")
                if (
                    store_id
                    and sap
                    and isinstance(quantity, int)
                    and not isinstance(quantity, bool)
                    and quantity > 0
                ):
                    key = (store_id, sap)
                    result[key] = result.get(key, 0) + quantity
        return result

    prior_quantities = allocation_quantities(prior_order_data)
    merged_quantities = allocation_quantities(order_data)
    changed_pairs = {
        key
        for key in set(prior_quantities) | set(merged_quantities)
        if prior_quantities.get(key, 0) != merged_quantities.get(key, 0)
    }
    increased_pairs = {
        key
        for key in changed_pairs
        if merged_quantities.get(key, 0) > prior_quantities.get(key, 0)
    }
    touched_store_ids = {store_id for store_id, _sap in changed_pairs}

    quantities: Dict[tuple[str, str], int] = {}
    seen_store_ids: set[str] = set()
    not_carried: list[Dict[str, str]] = []
    for store in order_data.get("stores") or []:
        store_id = str(store.get("storeId") or store.get("id") or "").strip()
        route_store = route_stores.get(store_id)
        if not store_id or store_id in seen_store_ids:
            raise StructuredApiError(422, "The adjusted order contains an invalid store.", "ADJUSTMENT_STORE_INVALID")
        if store_id in touched_store_ids and route_store is None:
            raise StructuredApiError(
                422,
                "The adjustment changes a store that is no longer available.",
                "ADJUSTMENT_STORE_INVALID",
                {"stores": [{"storeId": store_id, "storeName": store.get("storeName") or ""}]},
            )
        if (
            store_id in touched_store_ids
            and route_store is not None
            and route_store.get("isActive", True) is False
        ):
            raise StructuredApiError(
                409,
                "The adjustment changes an inactive store.",
                "ADJUSTMENT_STORE_INACTIVE",
                {
                    "stores": [{
                        "storeId": store_id,
                        "storeName": route_store.get("name") or store.get("storeName") or "",
                    }],
                },
            )
        seen_store_ids.add(store_id)
        carried = route_store.get("items") if route_store is not None else None
        carried_saps = (
            {str(sap).strip() for sap in carried if str(sap).strip()}
            if isinstance(carried, list)
            else None
        )
        seen_saps: set[str] = set()
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            quantity = item.get("quantity")
            if (
                not sap
                or sap in seen_saps
                or isinstance(quantity, bool)
                or not isinstance(quantity, int)
                or quantity <= 0
            ):
                raise StructuredApiError(422, "The adjusted order contains an invalid item.", "ADJUSTMENT_ITEM_INVALID")
            if (
                carried_saps is not None
                and (store_id, sap) in increased_pairs
                and sap not in carried_saps
            ):
                not_carried.append({
                    "sap": sap,
                    "storeId": store_id,
                    "storeName": str(
                        route_store.get("name")
                        or store.get("storeName")
                        or store_id
                    ),
                })
            seen_saps.add(sap)
            quantities[(store_id, sap)] = quantity

    if not_carried:
        raise StructuredApiError(
            409,
            "A store does not carry an adjusted item.",
            "ADJUSTMENT_ITEM_NOT_CARRIED",
            {"items": sorted(not_carried, key=lambda item: (item["storeName"], item["sap"]))},
        )

    inbound_floors: Dict[tuple[str, str], int] = {}
    for allocation in order_data.get("inboundTransferStoreAllocations") or []:
        key = (str(allocation.get("storeId") or ""), str(allocation.get("sap") or ""))
        units = allocation.get("units")
        if isinstance(units, int) and not isinstance(units, bool) and units > 0:
            inbound_floors[key] = inbound_floors.get(key, 0) + units
    for key, required_units in inbound_floors.items():
        if quantities.get(key, 0) < required_units:
            raise StructuredApiError(
                409,
                "The adjusted order conflicts with an inbound transfer allocation.",
                "ADJUSTMENT_INBOUND_ALLOCATION_CONFLICT",
            )


@firestore.transactional
def _confirm_full_order_adjustment_document(
    transaction,
    *,
    order_ref,
    adjustment_ref,
    batch_ref,
    audit_ref,
    order_audit_ref,
    route_adjustments_ref,
    stores_ref,
    route_number: str,
    payload: FullOrderAdjustmentConfirmRequest,
    actor_user_id: str,
    requester_is_owner: bool,
    now: datetime,
) -> Dict[str, Any]:
    order_doc = order_ref.get(transaction=transaction)
    adjustment_doc = adjustment_ref.get(transaction=transaction)
    batch_doc = batch_ref.get(transaction=transaction)
    if not order_doc.exists:
        raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")
    if not adjustment_doc.exists or not batch_doc.exists:
        raise StructuredApiError(404, "The sent adjustment was not found.", "ADJUSTMENT_BATCH_NOT_FOUND")

    order_data = order_doc.to_dict() or {}
    adjustment = adjustment_doc.to_dict() or {}
    batch = batch_doc.to_dict() or {}
    if str(order_data.get("routeNumber") or "") != route_number:
        raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")
    if str(adjustment.get("sourceOrderId") or "") != str(order_ref.id):
        raise StructuredApiError(409, "The adjustment points to another order.", "ADJUSTMENT_ORDER_MISMATCH")
    adjustment_actor = str(adjustment.get("userId") or "")
    if adjustment_actor != actor_user_id and not requester_is_owner:
        raise StructuredApiError(404, "The sent adjustment was not found.", "ADJUSTMENT_BATCH_NOT_FOUND")

    prior_confirmation = adjustment.get("confirmation") or {}
    if adjustment.get("status") == "confirmed":
        if (
            prior_confirmation.get("sentBatchId") == payload.sentBatchId
            and sorted(prior_confirmation.get("acceptedLineSaps") or []) == sorted(payload.acceptedSaps)
        ):
            projection = adjustment.get("projection") or {}
            return {
                "orderId": str(order_ref.id),
                "adjustmentId": payload.adjustmentId,
                "orderRevision": int(prior_confirmation.get("appliedOrderRevision") or _order_revision(order_data)),
                "changed": bool(prior_confirmation.get("changed")),
                "idempotent": True,
                "projectionStatus": projection.get("status"),
            }
        raise StructuredApiError(409, "This adjustment was already confirmed differently.", "ADJUSTMENT_ALREADY_CONFIRMED")

    last_sent = adjustment.get("lastSent") or {}
    if (
        adjustment.get("status") != "sent"
        or adjustment.get("mode") != "full_order"
        or last_sent.get("batchId") != payload.sentBatchId
        or last_sent.get("cumulativeSignature") != batch.get("cumulativeSignature")
        or last_sent.get("workingCopySignature") != batch.get("workingCopySignature")
        or str(batch.get("adjustmentId") or "") != payload.adjustmentId
        or str(batch.get("sourceOrderId") or "") != str(order_ref.id)
        or str(batch.get("routeNumber") or "") != route_number
    ):
        raise StructuredApiError(409, "The sent adjustment could not be verified.", "ADJUSTMENT_BATCH_MISMATCH")
    if order_data.get("status") != "finalized":
        raise StructuredApiError(409, "Only finalized orders can be adjusted.", "ADJUSTMENT_ORDER_NOT_FINALIZED")

    current_revision = _order_revision(order_data)
    batch_base_revision = _batch_base_order_revision(batch)
    intervening_reallocation_saps = set()
    if batch.get("schemaVersion") != 2:
        intervening_reallocation_saps = _prove_intervening_reallocations(
            transaction,
            order_id=str(order_ref.id),
            base_revision=batch_base_revision,
            current_revision=current_revision,
            order_audit_ref=order_audit_ref,
            route_adjustments_ref=route_adjustments_ref,
        )
    merged = validate_and_merge_full_adjustment(
        current_order=order_data,
        batch=batch,
        accepted_saps=payload.acceptedSaps,
        intervening_reallocation_saps=intervening_reallocation_saps,
    )
    store_docs = list(stores_ref.stream(transaction=transaction))
    route_stores = {str(doc.id): (doc.to_dict() or {}) for doc in store_docs}
    _validate_adjusted_store_allocations(
        merged,
        route_stores,
        prior_order_data=order_data,
    )
    if CORE_ITEM_ENFORCEMENT_ENABLED and payload.acceptedSaps:
        issues = build_core_item_issues(
            order_data=merged,
            stores=[{"id": doc.id, **(doc.to_dict() or {})} for doc in store_docs],
        )
        if issues:
            raise StructuredApiError(
                409,
                "Core item requirements must be resolved before confirming.",
                "CORE_ITEMS_REQUIRED",
                {"items": issues},
            )

    applied_at_ms = int(now.timestamp() * 1000)
    changed = (merged.get("stores") or []) != (order_data.get("stores") or [])
    applied_revision = current_revision + 1 if changed else current_revision
    if changed:
        transaction.update(order_ref, {
            "stores": merged.get("stores") or [],
            "routeTransfers": merged.get("routeTransfers") or [],
            "routeSplittingEnabled": bool(merged.get("routeSplittingEnabled")),
            "inboundTransfersUsed": merged.get("inboundTransfersUsed") or [],
            "inboundTransferStoreAllocations": merged.get("inboundTransferStoreAllocations") or [],
            "updatedAt": now,
            "orderRevision": applied_revision,
            "orderAdjustmentAppliedAtMs": applied_at_ms,
            "lastMutation": {
                "kind": "full_adjustment",
                "mutationId": payload.adjustmentId,
                "atMs": applied_at_ms,
            },
        })

    emailed_lines = batch.get("cumulativeLines") or []
    accepted = set(payload.acceptedSaps)
    confirmation = {
        "confirmedAtMs": applied_at_ms,
        "confirmedByUserId": actor_user_id,
        "emailedLines": emailed_lines,
        "acceptedLines": [line for line in emailed_lines if str(line.get("sap") or "") in accepted],
        "acceptedLineSaps": payload.acceptedSaps,
        "rejectedLineSaps": sorted({str(line.get("sap") or "") for line in emailed_lines} - accepted),
        "sentBatchId": payload.sentBatchId,
        "sentBatchSignature": batch.get("cumulativeSignature"),
        "notes": payload.notes,
        "appliedOrderRevision": applied_revision,
        "changed": changed,
        "baseOrderRevision": batch_base_revision,
        "rebasedFromOrderRevision": batch_base_revision if batch_base_revision != current_revision else None,
        "interveningReallocationSaps": sorted(intervening_reallocation_saps),
    }
    adjustment_update: Dict[str, Any] = {
        "status": "confirmed",
        "confirmation": confirmation,
        "reminderEnabled": False,
        "reminderStatus": "skipped",
        "reminderSkippedAtMs": applied_at_ms,
        "reminderSkipReason": "adjustment_confirmed",
        "updatedAt": now,
    }
    if changed:
        adjustment_update["projection"] = {
            "status": "pending",
            "targetOrderRevision": applied_revision,
            "attemptCount": 0,
        }
    transaction.update(adjustment_ref, adjustment_update)
    transaction.set(audit_ref, {
        "orderId": str(order_ref.id),
        "routeNumber": route_number,
        "userId": actor_user_id,
        "action": "order_full_adjustment_confirmed",
        "source": "api",
        "meta": {
            "adjustmentId": payload.adjustmentId,
            "sentBatchId": payload.sentBatchId,
            "acceptedSaps": payload.acceptedSaps,
            "baseOrderRevision": current_revision,
            "sentBatchBaseOrderRevision": batch_base_revision,
            "appliedOrderRevision": applied_revision,
            "changed": changed,
            "interveningReallocationSaps": sorted(intervening_reallocation_saps),
        },
        "createdAt": now,
    })
    return {
        "orderId": str(order_ref.id),
        "adjustmentId": payload.adjustmentId,
        "orderRevision": applied_revision,
        "changed": changed,
        "idempotent": False,
        "projectionStatus": "pending" if changed else None,
    }


@firestore.transactional
def _finalize_order_document(
    transaction,
    *,
    order_ref,
    stores_ref,
    route_number: str,
    now: datetime,
    resolved_stamp: Optional[Dict[str, Any]] = None,
    resolved_delivery_date: Optional[str] = None,
    resolved_schedule_key: Optional[str] = None,
) -> Dict[str, Any]:
    order_doc = order_ref.get(transaction=transaction)
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    if str(order_data.get("routeNumber", "")) != route_number:
        raise HTTPException(403, "Route mismatch")
    if order_data.get("status") != "draft":
        raise HTTPException(409, "Only draft orders can be finalized")

    store_docs = []
    stores = []
    if order_data.get("coreItemPolicyVersion") == 1:
        store_docs = list(stores_ref.stream(transaction=transaction))
        stores = [
            {"id": store_doc.id, **(store_doc.to_dict() or {})}
            for store_doc in store_docs
        ]

        if CORE_ITEM_ENFORCEMENT_ENABLED:
            issues = build_core_item_issues(order_data=order_data, stores=stores)
            if issues:
                raise StructuredApiError(
                    status_code=409,
                    error="Core items require a quantity or explicit override.",
                    code="CORE_ITEMS_REQUIRED",
                    details={"items": issues},
                )

    next_order_updates = build_next_order_store_updates(
        order_data=order_data,
        stores=stores,
    )

    next_revision = max(1, _order_revision(order_data) + 1)
    updated_at_ms = int(now.timestamp() * 1000)
    finalize_update: Dict[str, Any] = {
        "status": "finalized",
        "submittedAt": now,
        "updatedAt": now,
        "orderRevision": next_revision,
        "lastMutation": {
            "kind": "finalization",
            "mutationId": str(order_data.get("id") or order_ref.id),
            "atMs": updated_at_ms,
        },
    }
    if (
        "scheduledOrder" not in order_data
        and resolved_stamp is not None
        and order_data.get("expectedDeliveryDate") == resolved_delivery_date
        and str(order_data.get("scheduleKey") or "").strip().lower() == resolved_schedule_key
    ):
        finalize_update["scheduledOrder"] = resolved_stamp
    transaction.update(order_ref, finalize_update)
    for store_doc in store_docs:
        if store_doc.id not in next_order_updates:
            continue
        transaction.update(store_doc.reference, {
            "nextOrderItems": next_order_updates[store_doc.id],
            "updatedAt": updated_at_ms,
        })
    return {**order_data, **finalize_update}


@firestore.transactional
def _apply_store_reallocation_document(
    transaction,
    *,
    order_ref,
    adjustment_ref,
    audit_ref,
    stores_ref,
    products_ref,
    route_number: str,
    payload: StoreReallocationRequest,
    actor_user_id: str,
    requester_is_owner: bool,
    local_date: str,
    timezone_name: str,
    now: datetime,
) -> Dict[str, Any]:
    """Atomically materialize a store reallocation into the finalized order."""
    order_doc = order_ref.get(transaction=transaction)
    if not order_doc.exists:
        raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")
    order_data = order_doc.to_dict() or {}
    if str(order_data.get("routeNumber") or "") != route_number:
        raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")

    signature = moves_signature(payload.moves)
    adjustment_doc = adjustment_ref.get(transaction=transaction)
    draft_created_at = None
    if adjustment_doc.exists:
        existing = adjustment_doc.to_dict() or {}
        existing_reallocation = existing.get("storeReallocation") or {}
        order_id = str(order_data.get("id") or order_ref.id)
        same_operation = (
            str(existing.get("sourceOrderId") or "") == order_id
            and str(existing_reallocation.get("movesSignature") or "") == signature
            and str(existing.get("status") or "") == "applied"
        )
        if same_operation:
            existing_actor = str(existing_reallocation.get("appliedByUserId") or existing.get("userId") or "")
            if existing_actor != actor_user_id and not requester_is_owner:
                raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")
            projection = existing.get("projection") or {}
            return {
                "orderId": order_id,
                "reallocationId": str(payload.reallocationId),
                "orderRevision": int(existing_reallocation.get("appliedOrderRevision") or 1),
                "appliedAtMs": int(existing_reallocation.get("appliedAtMs") or 1),
                "reallocationCount": int(existing_reallocation.get("reallocationCount") or 1),
                "idempotent": True,
                "projectionStatus": str(projection.get("status") or "pending"),
            }

        existing_actor = str(existing.get("userId") or "")
        if existing_actor != actor_user_id:
            raise StructuredApiError(404, "Order not found", "ORDER_NOT_FOUND")
        promotable_draft = (
            str(existing.get("routeNumber") or "") == route_number
            and str(existing.get("sourceOrderId") or "") == order_id
            and str(existing.get("status") or "") == "draft"
            and str(existing.get("mode") or "") == "store_reallocation"
            and not existing.get("storeReallocation")
            and not existing.get("projection")
        )
        if not promotable_draft:
            raise StructuredApiError(
                409,
                "This reallocation ID has already been used.",
                "REALLOCATION_ID_CONFLICT",
            )
        draft_created_at = existing.get("createdAt")

    if order_data.get("status") != "finalized":
        raise StructuredApiError(
            409,
            "Only finalized orders can be reallocated.",
            "REALLOCATION_ORDER_NOT_FINALIZED",
        )
    delivery_date = str(order_data.get("expectedDeliveryDate") or "").strip()
    if not delivery_date or delivery_date < local_date:
        raise StructuredApiError(
            409,
            "This delivery is closed for reallocation.",
            "REALLOCATION_DELIVERY_CLOSED",
        )

    current_revision = _order_revision(order_data)
    if current_revision != payload.baseOrderRevision:
        raise StructuredApiError(
            409,
            "The order changed. Refresh allocations and review the move again.",
            "REALLOCATION_STALE_ORDER",
            {"currentOrderRevision": current_revision},
        )

    store_docs = list(stores_ref.stream(transaction=transaction))
    route_stores = {
        str(store_doc.id): (store_doc.to_dict() or {})
        for store_doc in store_docs
    }
    products: Dict[str, Dict[str, Any]] = {}
    for sap in sorted({move.sap for move in payload.moves}):
        product_doc = products_ref.document(sap).get(transaction=transaction)
        if product_doc.exists:
            products[sap] = product_doc.to_dict() or {}

    mutation = apply_store_reallocation(
        order_data=order_data,
        moves=payload.moves,
        route_stores=route_stores,
        products=products,
        enforce_core_items=CORE_ITEM_ENFORCEMENT_ENABLED,
    )

    applied_at_ms = int(now.timestamp() * 1000)
    applied_revision = current_revision + 1
    prior_summary = order_data.get("storeReallocationSummary") or {}
    prior_count = prior_summary.get("count")
    reallocation_count = (
        prior_count if isinstance(prior_count, int) and not isinstance(prior_count, bool) and prior_count >= 0 else 0
    ) + 1
    reallocation_id = str(payload.reallocationId)
    order_id = str(order_data.get("id") or order_ref.id)

    transaction.update(order_ref, {
        "stores": mutation["stores"],
        "updatedAt": now,
        "orderRevision": applied_revision,
        "lastMutation": {
            "kind": "store_reallocation",
            "mutationId": reallocation_id,
            "atMs": applied_at_ms,
        },
        "storeReallocationSummary": {
            "count": reallocation_count,
            "lastAppliedAtMs": applied_at_ms,
            "lastAdjustmentId": reallocation_id,
        },
    })
    transaction.set(adjustment_ref, {
        "id": reallocation_id,
        "routeNumber": route_number,
        "userId": actor_user_id,
        "status": "applied",
        "mode": "store_reallocation",
        "sourceOrderId": order_id,
        "sourceOrderDate": order_data.get("orderDate"),
        "sourceOrderExpectedDeliveryDate": delivery_date,
        "sourceOrderSubmittedAtMs": int(
            (_to_datetime(order_data.get("submittedAt")) or now).timestamp() * 1000
        ),
        "adjustmentDate": local_date,
        "targetDeliveryDate": delivery_date,
        "timezone": timezone_name,
        "lines": [],
        "email": {},
        "storeReallocation": {
            "sourceOrderId": order_id,
            "moves": mutation["auditMoves"],
            "movesSignature": signature,
            "baseOrderRevision": current_revision,
            "appliedOrderRevision": applied_revision,
            "reallocationCount": reallocation_count,
            "appliedAtMs": applied_at_ms,
            "appliedByUserId": actor_user_id,
        },
        "projection": {
            "status": "pending",
            "targetOrderRevision": applied_revision,
            "attemptCount": 0,
        },
        "reminderEnabled": False,
        "reminderStatus": "skipped",
        "reminderSkippedAtMs": applied_at_ms,
        "reminderSkipReason": "store_reallocation_applied",
        "createdAt": draft_created_at or now,
        "updatedAt": now,
    })
    transaction.set(audit_ref, {
        "orderId": order_id,
        "routeNumber": route_number,
        "userId": actor_user_id,
        "action": "order_store_reallocated",
        "source": "api",
        "meta": {
            "reallocationId": reallocation_id,
            "movesSignature": signature,
            "moveCount": len(payload.moves),
            "baseOrderRevision": current_revision,
            "appliedOrderRevision": applied_revision,
            "beforeTotals": mutation["beforeTotals"],
            "afterTotals": mutation["afterTotals"],
        },
        "createdAt": now,
    })

    return {
        "orderId": order_id,
        "reallocationId": reallocation_id,
        "orderRevision": applied_revision,
        "appliedAtMs": applied_at_ms,
        "reallocationCount": reallocation_count,
        "idempotent": False,
        "projectionStatus": "pending",
    }


def _log_order_audit(
    db: firestore.Client,
    order_id: str,
    route_number: str,
    user_id: str,
    action: str,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    """Write a lightweight audit entry for order changes."""
    _order_ref(db, route_number, order_id).collection("audit").add({
        "orderId": order_id,
        "routeNumber": route_number,
        "userId": user_id,
        "action": action,
        "meta": meta or {},
        "source": "web_portal",
        "createdAt": firestore.SERVER_TIMESTAMP,
    })


@router.get(
    "/orders/active",
    response_model=Optional[Order],
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_order_read
async def get_active_order(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Optional[Order]:
    """Get the active (draft) order for a route."""
    await require_route_access(route, decoded_token, db)

    orders_ref = db.collection("routes").document(route).collection("orders")
    q = (
        orders_ref.where("status", "==", "draft")
        .order_by("createdAt", direction=firestore.Query.DESCENDING)
        .limit(1)
    )
    docs = list(q.stream())
    if not docs:
        return None
    return Order(**docs[0].to_dict())


@router.get(
    "/orders/{order_id}",
    response_model=Order,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
async def get_order(
    request: Request,
    order_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Get an order by ID."""
    await require_route_access(route, decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()

    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    # Verify doc route matches path route
    if str(order_data.get("routeNumber", "")) != route:
        raise HTTPException(403, "Route mismatch")

    return Order(**order_data)


@router.post(
    "/orders",
    response_model=Order,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_order(
    request: Request,
    payload: OrderCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Create a new draft order."""
    await require_route_feature_access(payload.routeNumber, "ordering", decoded_token, db)
    schedule_resolution, resolved_scheduled_order = _resolve_create_schedule(db, payload)
    request_headers = getattr(request, "headers", {})
    stamp_allowed = stamp_write_allowed_for_headers(request_headers)
    scheduled_order = resolved_scheduled_order if stamp_allowed else None
    if resolved_scheduled_order is not None and not stamp_allowed:
        logger.info(
            "order_adjustment_v2_stamp_withheld route=%s endpoint=create client=%s",
            payload.routeNumber,
            request_headers.get("x-routespark-app-version", "unknown"),
        )
    expected_load_date = (
        str(schedule_resolution["loadDate"])
        if schedule_resolution is not None
        else None
    )

    now = datetime.now(timezone.utc)
    order_id = f"order-{payload.routeNumber}-{int(now.timestamp() * 1000)}"

    order_doc = {
        "id": order_id,
        "routeNumber": payload.routeNumber,
        "userId": decoded_token["uid"],
        "orderDate": _get_local_order_date(db, payload.routeNumber),
        "expectedDeliveryDate": payload.deliveryDate.isoformat(),
        **({"expectedLoadDate": expected_load_date} if expected_load_date else {}),
        "scheduleKey": payload.scheduleKey,
        **({"scheduledOrder": scheduled_order} if scheduled_order is not None else {}),
        "status": "draft",
        "stores": [],
        "createdAt": now,
        "updatedAt": now,
        "submittedAt": None,
        "orderCycleId": None,
        "notes": payload.notes,
        "isHolidaySchedule": payload.isHolidaySchedule,
        **(
            {
                "coreItemPolicyVersion": payload.coreItemPolicyVersion,
                "coreItemOverrides": [],
            }
            if payload.coreItemPolicyVersion == 1
            else {}
        ),
    }

    _order_ref(db, payload.routeNumber, order_id).set(order_doc)
    _log_order_audit(
        db,
        order_id,
        payload.routeNumber,
        decoded_token["uid"],
        "order_created",
        {
            "deliveryDate": payload.deliveryDate.isoformat(),
            "scheduleKey": payload.scheduleKey,
            "scheduledOrderVersion": scheduled_order.get("version") if scheduled_order else None,
        },
    )

    return Order(**order_doc)


@firestore.transactional
def _update_order_document(
    transaction,
    *,
    order_ref,
    route_number: str,
    payload: OrderUpdateRequest,
    now: datetime,
) -> Dict[str, Any]:
    """Atomically merge one web draft patch into the latest order."""

    order_doc = order_ref.get(transaction=transaction)
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    if str(order_data.get("routeNumber") or "") != route_number:
        raise HTTPException(403, "Route mismatch")
    if order_data.get("status") != "draft":
        raise HTTPException(400, "Order is not editable")

    update_data: Dict[str, Any] = {
        "stores": merge_order_store_patches(
            order_data.get("stores") or [],
            payload.stores,
        ),
        "updatedAt": now,
    }
    if payload.notes is not None:
        update_data["notes"] = payload.notes
    if payload.inboundTransfersUsed is not None:
        update_data["inboundTransfersUsed"] = [
            transfer.model_dump() for transfer in payload.inboundTransfersUsed
        ]
    if payload.routeTransfers is not None:
        update_data["routeTransfers"] = [
            transfer.model_dump() for transfer in payload.routeTransfers
        ]
    if payload.routeSplittingEnabled is not None:
        update_data["routeSplittingEnabled"] = payload.routeSplittingEnabled
    if payload.sapOrder is not None:
        update_data["sapOrder"] = list(payload.sapOrder)

    transaction.update(order_ref, update_data)
    return {**order_data, **update_data}


@router.put(
    "/orders/{order_id}",
    response_model=Order,
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_order_update
async def update_order(
    request: Request,
    order_id: str,
    payload: OrderUpdateRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Merge draft order store/item patches into the current Firestore order."""
    await require_route_feature_access(route, "ordering", decoded_token, db)
    order_ref = _order_ref(db, route, order_id)
    now = datetime.now(timezone.utc)
    transaction = db.transaction()
    updated = _update_order_document(
        transaction,
        order_ref=order_ref,
        route_number=route,
        payload=payload,
        now=now,
    )

    _log_order_audit(
        db,
        order_id,
        route,
        decoded_token["uid"],
        "order_updated",
        {
            "storeCount": len(payload.stores),
            "sapOrderCount": len(payload.sapOrder) if payload.sapOrder is not None else None,
        },
    )
    return Order(**updated)


@router.delete(
    "/orders/{order_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def delete_order(
    request: Request,
    order_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Delete a draft order."""
    requester_user_data = await require_route_feature_access(route, "ordering", decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    route_number = str(order_data.get("routeNumber", ""))
    if route_number != route:
        raise HTTPException(403, "Route mismatch")

    if order_data.get("status") != "draft":
        raise HTTPException(409, "Only draft orders can be deleted")

    # Release transfer reservations before deleting (best-effort cleanup)
    route_group_id = _resolve_route_group_id(
        db,
        requester_user_data=requester_user_data,
        order_route_number=route_number,
    ) or route_number

    # 1. Release inbound transfer reservations
    inbound_transfers = order_data.get("inboundTransfersUsed") or []
    for transfer_use in inbound_transfers:
        transfer_key = transfer_use.get("transferKey")
        if not transfer_key:
            continue
        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )
            transfer_ref.update({
                f"reservedBy.{order_id}": firestore.DELETE_FIELD,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            })
        except Exception:
            pass  # Best-effort — continue even if cleanup fails

    # 2. Delete planned outbound transfers (only if no other orders have reserved from them)
    route_transfers = order_data.get("routeTransfers") or []
    for transfer_alloc in route_transfers:
        to_route = transfer_alloc.get("toRouteNumber")
        sap = transfer_alloc.get("sap")
        if not to_route or not sap:
            continue

        # Transfer key format: {orderId}:{fromRoute}:{toRoute}:{sap}
        transfer_key = f"{order_id}:{route_number}:{to_route}:{sap}".replace("/", "_")
        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )
            transfer_snap = transfer_ref.get()
            if transfer_snap.exists:
                transfer_data = transfer_snap.to_dict()
                status = transfer_data.get("status")
                reserved_by = transfer_data.get("reservedBy", {})
                reserved_total = sum(v for v in reserved_by.values() if isinstance(v, (int, float)))

                # Only delete if it's planned and has no reservations
                if status == "planned" and reserved_total == 0:
                    transfer_ref.delete()
        except Exception:
            pass  # Best-effort cleanup

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_deleted",
    )

    order_ref.delete()

    return {"orderId": order_id, "status": "deleted"}


@router.post(
    "/orders/{order_id}/finalize",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def finalize_order(
    request: Request,
    order_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Finalize an order and trigger PostgreSQL sync."""
    requester_user_data = await require_route_feature_access(route, "ordering", decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    pre_transaction_order = order_ref.get()
    pre_transaction_data = pre_transaction_order.to_dict() or {} if pre_transaction_order.exists else {}
    if "scheduledOrder" in pre_transaction_data:
        resolved_stamp, resolved_delivery_date, resolved_schedule_key = None, None, None
    elif not stamp_write_allowed_for_headers(getattr(request, "headers", {})):
        resolved_stamp, resolved_delivery_date, resolved_schedule_key = None, None, None
        logger.info(
            "order_adjustment_v2_stamp_withheld route=%s endpoint=finalize client=%s",
            route,
            getattr(request, "headers", {}).get("x-routespark-app-version", "unknown"),
        )
    else:
        resolved_stamp, resolved_delivery_date, resolved_schedule_key = _resolve_finalize_stamp(
            db,
            route_number=route,
            order_data=pre_transaction_data,
        )
    now = datetime.now(timezone.utc)
    order_data = _finalize_order_document(
        db.transaction(),
        order_ref=order_ref,
        stores_ref=db.collection("routes").document(route).collection("stores"),
        route_number=route,
        now=now,
        resolved_stamp=resolved_stamp,
        resolved_delivery_date=resolved_delivery_date,
        resolved_schedule_key=resolved_schedule_key,
    )
    route_number = str(order_data.get("routeNumber", ""))

    # Commit outbound transfers (change status from 'planned' to 'committed')
    route_group_id = _resolve_route_group_id(
        db,
        requester_user_data=requester_user_data,
        order_route_number=route_number,
    ) or route_number
    route_transfers = order_data.get("routeTransfers") or []
    uid = decoded_token["uid"]

    for transfer_alloc in route_transfers:
        to_route = transfer_alloc.get("toRouteNumber")
        sap = transfer_alloc.get("sap")
        units = transfer_alloc.get("units", 0)

        if not to_route or not sap or units <= 0:
            continue

        # Transfer key format: {orderId}:{fromRoute}:{toRoute}:{sap}
        transfer_key = f"{order_id}:{route_number}:{to_route}:{sap}".replace("/", "_")

        try:
            transfer_ref = (
                db.collection("routeTransfers")
                .document(route_group_id)
                .collection("transfers")
                .document(transfer_key)
            )

            # Upsert with status='committed'
            transfer_data = {
                "routeGroupId": route_group_id,
                "purchaseRouteNumber": route_number,
                "fromRouteNumber": route_number,
                "toRouteNumber": to_route,
                "sap": sap,
                "units": units,
                "casePack": transfer_alloc.get("casePack", 1),
                "transferDate": transfer_alloc.get("transferDate") or order_data.get("expectedDeliveryDate") or order_data.get("orderDate"),
                "status": "committed",
                "reason": "pooled_order",
                "sourceOrderId": order_id,
                "createdByUid": uid,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            }

            # Set createdAt if new doc
            transfer_snap = transfer_ref.get()
            if not transfer_snap.exists:
                transfer_data["createdAt"] = firestore.SERVER_TIMESTAMP

            # Merge to preserve reservedBy map
            transfer_ref.set(transfer_data, merge=True)
        except Exception as exc:
            # Best-effort — log but don't fail the finalize
            print(f"[finalize_order] Failed to commit transfer {transfer_key}: {exc}")

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_finalized",
    )

    # Trigger direct PostgreSQL sync (best-effort)
    sync_result: Dict[str, Any] = {"synced": False}
    forecast_queue_result: Dict[str, Any] = {"enqueued": False}
    try:
        from ..dependencies import get_pg_connection, return_pg_connection
        from db_manager_pg import handle_sync_order
        try:
            from ...scripts.forecast_generation_queue import (
                JOB_TYPE_FORECAST_ONLY,
                JOB_TYPE_RETRAIN_THEN_FORECAST,
                append_finalize_event_job_keys,
                derive_finalize_targets,
                enqueue_generation_job,
                mark_finalize_event_error,
                register_finalize_event,
            )
            from ...scripts.retrain_readiness import evaluate_retrain_readiness
        except ImportError:
            from scripts.forecast_generation_queue import (
                JOB_TYPE_FORECAST_ONLY,
                JOB_TYPE_RETRAIN_THEN_FORECAST,
                append_finalize_event_job_keys,
                derive_finalize_targets,
                enqueue_generation_job,
                mark_finalize_event_error,
                register_finalize_event,
            )
            from scripts.retrain_readiness import evaluate_retrain_readiness

        conn = get_pg_connection()
        try:
            result = handle_sync_order(conn, db, {
                'orderId': order_id,
                'routeNumber': route_number,
            })
            if 'error' in result:
                sync_result = {"synced": False, "error": result['error']}
            else:
                sync_result = {"synced": True, **result}
                finalize_key = ""
                if not api_finalize_rollout_enabled_for_route(route_number):
                    forecast_queue_result = {
                        "enqueued": False,
                        "status": "disabled",
                    }
                else:
                    try:
                        readiness = evaluate_retrain_readiness(route_number, conn=conn)
                        job_type = (
                            JOB_TYPE_RETRAIN_THEN_FORECAST
                            if readiness.get("ready_for_retrain")
                            else JOB_TYPE_FORECAST_ONLY
                        )
                        event_row = register_finalize_event(
                            route_number=route_number,
                            order_id=order_id,
                            schedule_key=order_data.get("scheduleKey"),
                            finalized_at_raw=now,
                            worker_id=API_FORECAST_WORKER_ID,
                        )
                        finalize_key = str(event_row.get("finalize_key") or "")
                        if event_row.get("status") == "processed":
                            forecast_queue_result = {
                                "enqueued": False,
                                "status": "already_processed",
                                "finalizeKey": finalize_key,
                            }
                        else:
                            targets = derive_finalize_targets(route_number, order_data.get("scheduleKey"))
                            if not targets:
                                if finalize_key:
                                    mark_finalize_event_error(finalize_key, "no_targets")
                                forecast_queue_result = {
                                    "enqueued": False,
                                    "status": "no_targets",
                                    "finalizeKey": finalize_key,
                                    "jobType": job_type,
                                }
                            else:
                                job_keys: List[str] = []
                                for target in targets:
                                    row = enqueue_generation_job(
                                        route_number=route_number,
                                        schedule_key=str(target.get("schedule_key") or ""),
                                        delivery_date=str(target.get("delivery_date") or ""),
                                        source="api_finalize",
                                        job_type=job_type,
                                        finalize_key=finalize_key or None,
                                    )
                                    if row and row.get("job_key"):
                                        job_keys.append(str(row.get("job_key")))

                                if finalize_key and job_keys:
                                    append_finalize_event_job_keys(finalize_key, job_keys)
                                elif finalize_key and not job_keys:
                                    mark_finalize_event_error(finalize_key, "enqueue_failed")

                                forecast_queue_result = {
                                    "enqueued": bool(job_keys),
                                    "status": "queued" if job_keys else "enqueue_failed",
                                    "finalizeKey": finalize_key,
                                    "jobKeys": job_keys,
                                    "jobType": job_type,
                                    "readiness": {
                                        "readyForRetrain": bool(readiness.get("ready_for_retrain")),
                                        "hasEnoughData": bool(readiness.get("has_enough_data")),
                                        "cycleStatus": str((readiness.get("cycle") or {}).get("status") or ""),
                                        "minNonHolidayOrdersForRetrain": int(
                                            readiness.get("min_non_holiday_orders_for_retrain") or 0
                                        ),
                                    },
                                }
                    except Exception as exc:
                        if finalize_key:
                            try:
                                mark_finalize_event_error(finalize_key, f"api_enqueue_error:{exc}")
                            except Exception:
                                pass
                        print(f"[finalize_order] Forecast queue enqueue failed for {route_number}/{order_id}: {exc}")
                        forecast_queue_result = {
                            "enqueued": False,
                            "status": "error",
                        }
        finally:
            return_pg_connection(conn)
    except Exception as exc:
        sync_result = {"synced": False, "error": str(exc)}

    return {
        "orderId": order_id,
        "status": "finalized",
        "scheduledOrder": order_data.get("scheduledOrder"),
        "sync": sync_result,
        "forecastQueue": forecast_queue_result,
    }


@router.post(
    "/orders/{order_id}/reallocations",
    response_model=StoreReallocationResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_store_reallocation(
    request: Request,
    order_id: str,
    payload: StoreReallocationRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> StoreReallocationResponse:
    """Atomically reallocate finalized units between stores without forecasting."""
    requester_user_data = await require_route_feature_access(
        route,
        "ordering",
        decoded_token,
        db,
    )
    order_ref = _order_ref(db, route, order_id)
    reallocation_id = str(payload.reallocationId)
    adjustment_ref = (
        db.collection("routes")
        .document(route)
        .collection("orderAdjustments")
        .document(reallocation_id)
    )
    audit_ref = order_ref.collection("audit").document(
        f"store-reallocation-{reallocation_id}"
    )
    route_ref = db.collection("routes").document(route)
    now = datetime.now(timezone.utc)
    result = _apply_store_reallocation_document(
        db.transaction(),
        order_ref=order_ref,
        adjustment_ref=adjustment_ref,
        audit_ref=audit_ref,
        stores_ref=route_ref.collection("stores"),
        products_ref=(
            db.collection("masterCatalog")
            .document(route)
            .collection("products")
        ),
        route_number=route,
        payload=payload,
        actor_user_id=str(decoded_token["uid"]),
        requester_is_owner=_requester_is_route_owner(requester_user_data, route),
        local_date=_get_local_order_date(db, route),
        timezone_name=get_route_timezone(db, route) or "UTC",
        now=now,
    )
    logger.info(
        "store_reallocation_applied route=%s order_id=%s reallocation_id=%s revision=%s idempotent=%s",
        route,
        order_id,
        reallocation_id,
        result["orderRevision"],
        result["idempotent"],
    )
    return StoreReallocationResponse(**result)


@router.post(
    "/orders/{order_id}/adjustments/{adjustment_id}/reminder/reconcile",
    response_model=AdjustmentReminderReconcileResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def reconcile_order_adjustment_reminder(
    request: Request,
    payload: AdjustmentReminderReconcileRequest,
    order_id: str = Path(..., min_length=1, max_length=160, pattern=r"^[A-Za-z0-9_-]+$"),
    adjustment_id: str = Path(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$"),
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> AdjustmentReminderReconcileResponse:
    """Reconcile one draft from the live source stamp and persisted requester settings."""
    requester_uid = str(decoded_token.get("uid") or "").strip()
    if not requester_uid:
        raise StructuredApiError(401, "Authentication required", "AUTH_REQUIRED")
    await require_route_feature_access(route, "ordering", decoded_token, db)

    route_ref = db.collection("routes").document(route)
    source_ref = route_ref.collection("orders").document(order_id)
    adjustment_ref = route_ref.collection("orderAdjustments").document(adjustment_id)
    settings_ref = db.collection("users").document(requester_uid)
    now = datetime.now(timezone.utc)
    now_ms = int(now.timestamp()) * 1000 + now.microsecond // 1000
    result = _reconcile_adjustment_reminder_document(
        db.transaction(),
        adjustment_ref=adjustment_ref,
        source_ref=source_ref,
        settings_ref=settings_ref,
        route_number=route,
        order_id=order_id,
        adjustment_id=adjustment_id,
        authenticated_uid=requester_uid,
        payload=payload,
        now_ms=now_ms,
    )
    logger.info(
        "order_adjustment_reminder_reconciled route=%s order_id=%s adjustment_id=%s changed=%s requester=%s",
        route,
        order_id,
        adjustment_id,
        result["changed"],
        requester_uid,
    )
    return AdjustmentReminderReconcileResponse(**result)


@router.post(
    "/orders/{order_id}/adjustments/{adjustment_id}/confirm",
    response_model=FullOrderAdjustmentConfirmResponse,
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def confirm_full_order_adjustment(
    request: Request,
    order_id: str,
    adjustment_id: str,
    payload: FullOrderAdjustmentConfirmRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> FullOrderAdjustmentConfirmResponse:
    """Verify a sent batch and atomically revise the canonical finalized order."""
    requester_user_data = await require_route_feature_access(
        route,
        "ordering",
        decoded_token,
        db,
    )
    if payload.adjustmentId != adjustment_id:
        raise StructuredApiError(422, "Adjustment ID mismatch.", "ADJUSTMENT_ID_MISMATCH")

    route_ref = db.collection("routes").document(route)
    order_ref = route_ref.collection("orders").document(order_id)
    adjustment_ref = route_ref.collection("orderAdjustments").document(adjustment_id)
    batch_ref = adjustment_ref.collection("emailBatches").document(payload.sentBatchId)
    audit_ref = order_ref.collection("audit").document(f"full-adjustment-{adjustment_id}")
    result = _confirm_full_order_adjustment_document(
        db.transaction(),
        order_ref=order_ref,
        adjustment_ref=adjustment_ref,
        batch_ref=batch_ref,
        audit_ref=audit_ref,
        order_audit_ref=order_ref.collection("audit"),
        route_adjustments_ref=route_ref.collection("orderAdjustments"),
        stores_ref=route_ref.collection("stores"),
        route_number=route,
        payload=payload,
        actor_user_id=str(decoded_token["uid"]),
        requester_is_owner=_requester_is_route_owner(requester_user_data, route),
        now=datetime.now(timezone.utc),
    )
    logger.info(
        "full_order_adjustment_confirmed route=%s order_id=%s adjustment_id=%s revision=%s changed=%s idempotent=%s",
        route,
        order_id,
        adjustment_id,
        result["orderRevision"],
        result["changed"],
        result["idempotent"],
    )
    return FullOrderAdjustmentConfirmResponse(**result)


@router.get(
    "/orders/{order_id}/audit",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_order_read
async def get_order_audit(
    request: Request,
    order_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return audit log entries for an order."""
    await require_route_access(route, decoded_token, db)

    order_ref = _order_ref(db, route, order_id)
    order_doc = order_ref.get()
    if not order_doc.exists:
        raise HTTPException(404, "Order not found")

    order_data = order_doc.to_dict() or {}
    if str(order_data.get("routeNumber", "")) != route:
        raise HTTPException(403, "Route mismatch")

    audit_ref = order_ref.collection("audit").order_by(
        "createdAt", direction=firestore.Query.DESCENDING
    ).limit(200)
    entries = []
    for doc in audit_ref.stream():
        data = doc.to_dict() or {}
        data["id"] = doc.id
        entries.append(data)

    return {
        "orderId": order_id,
        "entries": entries,
    }
