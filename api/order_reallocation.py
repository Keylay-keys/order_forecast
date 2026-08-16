"""Pure validation and mutation helpers for canonical store reallocations."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping, Tuple

from .errors import StructuredApiError
from .models import StoreReallocationMoveRequest
from .order_attention import build_core_item_issues


StoreSapKey = Tuple[str, str]


def canonical_move_payload(moves: Iterable[StoreReallocationMoveRequest]) -> List[Dict[str, Any]]:
    """Return the stable semantic representation used for idempotency."""
    return sorted(
        (
            {
                "sap": move.sap,
                "fromStoreId": move.fromStoreId,
                "toStoreId": move.toStoreId,
                "unitQuantity": int(move.unitQuantity),
            }
            for move in moves
        ),
        key=lambda move: (
            move["sap"],
            move["fromStoreId"],
            move["toStoreId"],
        ),
    )


def moves_signature(moves: Iterable[StoreReallocationMoveRequest]) -> str:
    encoded = json.dumps(
        canonical_move_payload(moves),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _conflict(message: str, code: str, details: Dict[str, Any] | None = None) -> None:
    raise StructuredApiError(
        status_code=409,
        error=message,
        code=code,
        details=details,
    )


def _store_id(store: Mapping[str, Any]) -> str:
    return str(store.get("storeId") or store.get("id") or "").strip()


def _store_name(store: Mapping[str, Any], fallback: str) -> str:
    return str(store.get("storeName") or store.get("name") or fallback).strip() or fallback


def _item_quantity_by_key(stores: Iterable[Mapping[str, Any]]) -> Dict[StoreSapKey, int]:
    quantities: Dict[StoreSapKey, int] = {}
    for store in stores:
        store_id = _store_id(store)
        if not store_id:
            continue
        for item in store.get("items") or []:
            if not isinstance(item, Mapping):
                continue
            sap = str(item.get("sap") or "").strip()
            if not sap:
                continue
            raw_quantity = item.get("quantity")
            if isinstance(raw_quantity, bool) or not isinstance(raw_quantity, int) or raw_quantity < 0:
                _conflict(
                    "The finalized order contains an invalid quantity.",
                    "REALLOCATION_ORDER_INVALID",
                    {"storeId": store_id, "sap": sap},
                )
            key = (store_id, sap)
            quantities[key] = quantities.get(key, 0) + raw_quantity
    return quantities


def _route_totals(quantities: Mapping[StoreSapKey, int]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for (_store_id_value, sap), quantity in quantities.items():
        totals[sap] = totals.get(sap, 0) + quantity
    return totals


def _replace_store_sap_quantity(store: Dict[str, Any], sap: str, quantity: int) -> None:
    items = [deepcopy(item) for item in (store.get("items") or []) if isinstance(item, dict)]
    matching = [index for index, item in enumerate(items) if str(item.get("sap") or "").strip() == sap]
    if quantity <= 0:
        store["items"] = [item for index, item in enumerate(items) if index not in matching]
        return

    if matching:
        first = matching[0]
        items[first] = {**items[first], "sap": sap, "quantity": quantity}
        store["items"] = [
            item
            for index, item in enumerate(items)
            if index == first or index not in matching
        ]
        return

    store["items"] = [*items, {"sap": sap, "quantity": quantity}]


def _validate_inbound_floor(order_data: Mapping[str, Any], quantities: Mapping[StoreSapKey, int]) -> None:
    inbound: Dict[StoreSapKey, int] = {}
    for allocation in order_data.get("inboundTransferStoreAllocations") or []:
        if not isinstance(allocation, Mapping):
            continue
        store_id = str(allocation.get("storeId") or "").strip()
        sap = str(allocation.get("sap") or "").strip()
        raw_units = allocation.get("units")
        if not store_id or not sap or isinstance(raw_units, bool) or not isinstance(raw_units, int):
            continue
        inbound[(store_id, sap)] = inbound.get((store_id, sap), 0) + max(0, raw_units)

    for (store_id, sap), inbound_units in inbound.items():
        store_units = quantities.get((store_id, sap), 0)
        if store_units < inbound_units:
            _conflict(
                "This move would conflict with an inbound transfer allocation.",
                "REALLOCATION_INBOUND_CONFLICT",
                {
                    "storeId": store_id,
                    "sap": sap,
                    "storeUnits": store_units,
                    "inboundUnits": inbound_units,
                },
            )


def apply_store_reallocation(
    *,
    order_data: Mapping[str, Any],
    moves: List[StoreReallocationMoveRequest],
    route_stores: Mapping[str, Mapping[str, Any]],
    products: Mapping[str, Mapping[str, Any]],
    enforce_core_items: bool,
) -> Dict[str, Any]:
    """Validate a move set and return canonical stores plus server-derived audit data."""
    proposed_stores = deepcopy(list(order_data.get("stores") or []))
    order_store_by_id = {
        _store_id(store): store
        for store in proposed_stores
        if isinstance(store, dict) and _store_id(store)
    }
    before = _item_quantity_by_key(proposed_stores)
    after = dict(before)

    outgoing: Dict[StoreSapKey, int] = {}
    deltas: Dict[StoreSapKey, int] = {}

    for move in moves:
        if move.fromStoreId == move.toStoreId:
            _conflict(
                "Move source and destination must be different stores.",
                "REALLOCATION_SAME_STORE",
                {"sap": move.sap, "storeId": move.fromStoreId},
            )

        for store_id, role in (
            (move.fromStoreId, "source"),
            (move.toStoreId, "destination"),
        ):
            if store_id not in order_store_by_id:
                _conflict(
                    f"The {role} store is not part of this finalized order.",
                    "REALLOCATION_STORE_NOT_IN_ORDER",
                    {"storeId": store_id, "role": role, "sap": move.sap},
                )
            route_store = route_stores.get(store_id)
            if route_store is None or route_store.get("isActive", True) is False:
                _conflict(
                    f"The {role} store is not active on this route.",
                    "REALLOCATION_STORE_INACTIVE",
                    {"storeId": store_id, "role": role},
                )
            carried_saps = route_store.get("items")
            if isinstance(carried_saps, list) and move.sap not in {str(sap) for sap in carried_saps}:
                _conflict(
                    f"The {role} store does not carry this item.",
                    "REALLOCATION_ITEM_NOT_CARRIED",
                    {"storeId": store_id, "role": role, "sap": move.sap},
                )

        source_key = (move.fromStoreId, move.sap)
        destination_key = (move.toStoreId, move.sap)
        outgoing[source_key] = outgoing.get(source_key, 0) + move.unitQuantity
        deltas[source_key] = deltas.get(source_key, 0) - move.unitQuantity
        deltas[destination_key] = deltas.get(destination_key, 0) + move.unitQuantity

    for key, outgoing_units in outgoing.items():
        available = before.get(key, 0)
        if outgoing_units > available:
            _conflict(
                "The source store no longer has enough units for this move.",
                "REALLOCATION_SOURCE_OVERDRAW",
                {
                    "storeId": key[0],
                    "sap": key[1],
                    "availableUnits": available,
                    "requestedUnits": outgoing_units,
                },
            )

    for key, delta in deltas.items():
        after[key] = after.get(key, 0) + delta
        if after[key] < 0:
            _conflict(
                "The source store no longer has enough units for this move.",
                "REALLOCATION_SOURCE_OVERDRAW",
                {"storeId": key[0], "sap": key[1]},
            )

    before_totals = _route_totals(before)
    after_totals = _route_totals(after)
    if before_totals != after_totals:
        _conflict(
            "Store reallocation cannot change route purchase totals.",
            "REALLOCATION_ROUTE_TOTAL_CHANGED",
        )

    _validate_inbound_floor(order_data, after)

    for (store_id, sap), delta in deltas.items():
        if delta == 0:
            continue
        _replace_store_sap_quantity(order_store_by_id[store_id], sap, after[(store_id, sap)])

    proposed_order = {**deepcopy(dict(order_data)), "stores": proposed_stores}
    if enforce_core_items and order_data.get("coreItemPolicyVersion") == 1:
        core_issues = build_core_item_issues(
            order_data=proposed_order,
            stores=[{"id": store_id, **dict(store)} for store_id, store in route_stores.items()],
        )
        if core_issues:
            _conflict(
                "Core items require a quantity or explicit override.",
                "CORE_ITEMS_REQUIRED",
                {"items": core_issues},
            )

    audit_moves: List[Dict[str, Any]] = []
    for move in moves:
        source_store = route_stores[move.fromStoreId]
        destination_store = route_stores[move.toStoreId]
        product = products.get(move.sap) or {}
        raw_case_pack = product.get("casePack") or product.get("tray") or 1
        case_pack = raw_case_pack if isinstance(raw_case_pack, int) and raw_case_pack > 0 else 1
        full_name = str(
            product.get("fullName")
            or product.get("name")
            or product.get("product")
            or f"SAP {move.sap}"
        )
        audit_moves.append({
            "sap": move.sap,
            "fullName": full_name,
            "casePack": case_pack,
            "unitQuantity": move.unitQuantity,
            "fromStoreId": move.fromStoreId,
            "fromStoreName": _store_name(source_store, move.fromStoreId),
            "toStoreId": move.toStoreId,
            "toStoreName": _store_name(destination_store, move.toStoreId),
        })

    return {
        "stores": proposed_stores,
        "auditMoves": audit_moves,
        "beforeTotals": before_totals,
        "afterTotals": after_totals,
    }

