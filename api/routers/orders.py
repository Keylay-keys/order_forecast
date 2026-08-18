"""Orders router - create, update, finalize, and audit orders."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from schedule_cycle import get_schedule_key_for_delivery_date

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    require_route_feature_access,
    get_firestore,
    get_route_timezone,
)
from ..errors import StructuredApiError
from ..middleware.rate_limit import rate_limit_write
from ..order_attention import build_core_item_issues, build_next_order_store_updates
from ..models import (
    Order,
    OrderCreateRequest,
    OrderUpdateRequest,
    StoreReallocationRequest,
    StoreReallocationResponse,
    FullOrderAdjustmentConfirmRequest,
    FullOrderAdjustmentConfirmResponse,
    ErrorResponse,
)
from ..order_full_adjustment import validate_and_merge_full_adjustment
from ..order_reallocation import apply_store_reallocation, moves_signature

try:
    from ...scripts.finalize_rollout import api_finalize_rollout_enabled_for_route
except ImportError:
    from scripts.finalize_rollout import api_finalize_rollout_enabled_for_route

from .schedule import get_order_cycles_from_firestore


router = APIRouter()
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


def _validate_non_holiday_schedule_key(
    db: firestore.Client,
    payload: OrderCreateRequest,
) -> Optional[Dict[str, Any]]:
    """Reject normal orders whose scheduleKey disagrees with the route schedule.

    Holiday/off-schedule orders are allowed to bypass this because their normal
    route cycle may not apply.
    """
    if payload.isHolidaySchedule:
        return None

    try:
        cycles = get_order_cycles_from_firestore(db, payload.routeNumber)
    except Exception as exc:
        logger.warning(
            "Schedule key validation skipped for route %s: failed to load cycles: %s",
            payload.routeNumber,
            exc,
        )
        return None

    if not cycles:
        logger.info(
            "Schedule key validation skipped for route %s: no configured cycles",
            payload.routeNumber,
        )
        return None

    resolution = get_schedule_key_for_delivery_date(payload.deliveryDate, cycles)
    if not resolution:
        logger.info(
            "Schedule key validation skipped for route %s delivery %s: no matching cycle",
            payload.routeNumber,
            payload.deliveryDate,
        )
        return None

    valid_keys = {
        str(match.get("scheduleKey", "")).lower()
        for match in resolution.get("matches", [])
        if match.get("scheduleKey")
    }
    if not valid_keys:
        valid_keys = {str(resolution.get("scheduleKey", "")).lower()}

    if payload.scheduleKey.lower() not in valid_keys:
        raise HTTPException(
            400,
            (
                "Schedule key does not match the selected delivery date. "
                "Refresh the page and try again."
            ),
        )

    return resolution


def _derive_expected_load_date(
    payload: OrderCreateRequest,
    resolution: Optional[Dict[str, Any]],
) -> Optional[str]:
    if not resolution:
        return None

    schedule_key = payload.scheduleKey.lower()
    matches = [
        match
        for match in resolution.get("matches", [])
        if match.get("matchedBy") == "delivery"
        and str(match.get("scheduleKey") or "").lower() == schedule_key
    ]
    if len(matches) != 1:
        return None

    cycle = matches[0].get("cycle") or {}
    order_day = cycle.get("orderDay")
    load_offset = cycle.get("loadOffsetDays")
    delivery_offset = cycle.get("deliveryOffsetDays")
    if not all(isinstance(value, int) for value in (order_day, load_offset, delivery_offset)):
        return None

    order_date = payload.deliveryDate - timedelta(days=delivery_offset)
    if order_date.isoweekday() != order_day:
        return None

    return (order_date + timedelta(days=load_offset)).isoformat()


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
) -> None:
    """Validate the complete server-merged allocation before committing it."""
    quantities: Dict[tuple[str, str], int] = {}
    seen_store_ids: set[str] = set()
    for store in order_data.get("stores") or []:
        store_id = str(store.get("storeId") or store.get("id") or "").strip()
        route_store = route_stores.get(store_id)
        if not store_id or store_id in seen_store_ids or route_store is None:
            raise StructuredApiError(422, "The adjusted order contains an invalid store.", "ADJUSTMENT_STORE_INVALID")
        if route_store.get("isActive", True) is False:
            raise StructuredApiError(409, "The adjusted order contains an inactive store.", "ADJUSTMENT_STORE_INACTIVE")
        seen_store_ids.add(store_id)
        carried = route_store.get("items")
        carried_saps = {str(sap) for sap in carried} if isinstance(carried, list) else None
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
            if carried_saps is not None and sap not in carried_saps:
                raise StructuredApiError(409, "A store does not carry an adjusted item.", "ADJUSTMENT_ITEM_NOT_CARRIED")
            seen_saps.add(sap)
            quantities[(store_id, sap)] = quantity

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
    _validate_adjusted_store_allocations(merged, route_stores)
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
    transaction.update(order_ref, {
        "status": "finalized",
        "submittedAt": now,
        "updatedAt": now,
        "orderRevision": next_revision,
        "lastMutation": {
            "kind": "finalization",
            "mutationId": str(order_data.get("id") or order_ref.id),
            "atMs": updated_at_ms,
        },
    })
    for store_doc in store_docs:
        if store_doc.id not in next_order_updates:
            continue
        transaction.update(store_doc.reference, {
            "nextOrderItems": next_order_updates[store_doc.id],
            "updatedAt": updated_at_ms,
        })
    return order_data


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
@rate_limit_write
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
    schedule_resolution = _validate_non_holiday_schedule_key(db, payload)
    expected_load_date = _derive_expected_load_date(payload, schedule_resolution)

    order_id = f"order-{payload.routeNumber}-{int(datetime.utcnow().timestamp() * 1000)}"
    now = datetime.now(timezone.utc)

    order_doc = {
        "id": order_id,
        "routeNumber": payload.routeNumber,
        "userId": decoded_token["uid"],
        "orderDate": _get_local_order_date(db, payload.routeNumber),
        "expectedDeliveryDate": payload.deliveryDate.isoformat(),
        **({"expectedLoadDate": expected_load_date} if expected_load_date else {}),
        "scheduleKey": payload.scheduleKey,
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
        {"deliveryDate": payload.deliveryDate.isoformat(), "scheduleKey": payload.scheduleKey},
    )

    return Order(**order_doc)


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
@rate_limit_write
async def update_order(
    request: Request,
    order_id: str,
    payload: OrderUpdateRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Order:
    """Merge draft order store/item patches into the current Firestore order."""
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
        raise HTTPException(400, "Order is not editable")

    now = datetime.now(timezone.utc)

    # Clients send quantity patches. Because `stores` is stored as an ARRAY of
    # MAPs in Firestore, merge into the current document instead of replacing the
    # array; otherwise a web draft save can erase mobile edits from Firebase.
    existing_stores: List[Dict[str, Any]] = order_data.get("stores") or []
    existing_store_by_id: Dict[str, Dict[str, Any]] = {
        str(s.get("storeId")): s for s in existing_stores if s.get("storeId") is not None
    }
    merged_store_by_id: Dict[str, Dict[str, Any]] = {
        store_id: {
            **store,
            "items": list(store.get("items") or []),
        }
        for store_id, store in existing_store_by_id.items()
    }

    for store in payload.stores:
        incoming_store = store.dict(exclude_unset=True)
        store_id = str(incoming_store.get("storeId", ""))
        if not store_id:
            continue
        existing_store = merged_store_by_id.get(store_id) or existing_store_by_id.get(store_id, {})

        existing_items = existing_store.get("items") or []
        existing_item_by_sap: Dict[str, Dict[str, Any]] = {
            str(it.get("sap")): it for it in existing_items if it.get("sap") is not None
        }

        for item in store.items:
            incoming_item = item.dict(exclude_unset=True, by_alias=False)
            sap = str(incoming_item.get("sap", "")).strip()
            if not sap:
                continue
            quantity = int(incoming_item.get("quantity") or 0)
            if quantity <= 0:
                existing_item_by_sap.pop(sap, None)
                continue
            base = existing_item_by_sap.get(sap, {})
            existing_item_by_sap[sap] = {**base, **incoming_item}

        merged_store_by_id[store_id] = {
            **existing_store,
            **{k: v for k, v in incoming_store.items() if k != "items"},
            "items": list(existing_item_by_sap.values()),
        }

    existing_order = [str(s.get("storeId")) for s in existing_stores if s.get("storeId") is not None]
    incoming_order = [str(s.storeId) for s in payload.stores if s.storeId]
    ordered_store_ids = list(dict.fromkeys([*existing_order, *incoming_order]))
    merged_stores = [
        merged_store_by_id[store_id]
        for store_id in ordered_store_ids
        if merged_store_by_id.get(store_id, {}).get("items")
    ]

    update_data = {
        "stores": merged_stores,
        "updatedAt": now,
    }
    if payload.notes is not None:
        update_data["notes"] = payload.notes

    # Transfer metadata (web portal parity)
    if payload.inboundTransfersUsed is not None:
        update_data["inboundTransfersUsed"] = [t.dict() for t in payload.inboundTransfersUsed]
    if payload.routeTransfers is not None:
        update_data["routeTransfers"] = [t.dict() for t in payload.routeTransfers]
    if payload.routeSplittingEnabled is not None:
        update_data["routeSplittingEnabled"] = payload.routeSplittingEnabled
    if payload.sapOrder is not None:
        update_data["sapOrder"] = payload.sapOrder

    order_ref.update(update_data)

    _log_order_audit(
        db,
        order_id,
        route_number,
        decoded_token["uid"],
        "order_updated",
        {
            "storeCount": len(payload.stores),
            "sapOrderCount": len(payload.sapOrder) if payload.sapOrder is not None else None,
        },
    )

    updated = order_ref.get().to_dict() or {}
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
    now = datetime.now(timezone.utc)
    order_data = _finalize_order_document(
        db.transaction(),
        order_ref=order_ref,
        stores_ref=db.collection("routes").document(route).collection("stores"),
        route_number=route,
        now=now,
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
@rate_limit_write
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
