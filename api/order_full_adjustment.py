"""Pure validation and merge logic for server-authoritative full-order confirmation."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Set

from .errors import StructuredApiError
from .models import SAP_PATTERN


def _positive_int(value: Any) -> int:
    try:
        parsed = int(float(value or 0))
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _require_nonnegative_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StructuredApiError(422, f"{field} must be a whole number.", "ADJUSTMENT_WORKING_COPY_INVALID")
    if not math.isfinite(float(value)) or float(value) < 0 or not float(value).is_integer():
        raise StructuredApiError(422, f"{field} must be a whole number.", "ADJUSTMENT_WORKING_COPY_INVALID")
    return int(value)


def _validate_working_copy_shape(order: Dict[str, Any]) -> None:
    seen_store_ids: Set[str] = set()
    for store in order.get("stores") or []:
        store_id = str(store.get("storeId") or store.get("id") or "").strip()
        if not store_id or store_id in seen_store_ids:
            raise StructuredApiError(422, "The working copy contains an invalid store.", "ADJUSTMENT_WORKING_COPY_INVALID")
        seen_store_ids.add(store_id)
        seen_saps: Set[str] = set()
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            if not sap or sap in seen_saps:
                raise StructuredApiError(422, "The working copy contains an invalid item.", "ADJUSTMENT_WORKING_COPY_INVALID")
            seen_saps.add(sap)
            _require_nonnegative_integer(item.get("quantity"), field="Item quantity")

    for key in ("routeTransfers", "inboundTransfersUsed", "inboundTransferStoreAllocations"):
        for row in order.get(key) or []:
            _require_nonnegative_integer(row.get("units"), field="Transfer units")
            if key != "inboundTransferStoreAllocations":
                _require_nonnegative_integer(row.get("casePack"), field="Transfer case pack")


def cumulative_lines_signature(lines: Iterable[Dict[str, Any]]) -> str:
    grouped: Dict[str, Dict[str, Any]] = {}
    for source in lines:
        sap = str(source.get("sap") or "").strip()
        case_quantity = _positive_int(source.get("caseQuantity"))
        case_pack = _positive_int(source.get("casePack"))
        if not sap or not case_quantity or not case_pack:
            continue
        entry = grouped.setdefault(sap, {"signed": 0, "casePacks": []})
        if case_pack not in entry["casePacks"]:
            entry["casePacks"].append(case_pack)
        entry["signed"] += case_quantity if source.get("direction") == "add" else -case_quantity

    canonical = []
    for sap in sorted(grouped):
        entry = grouped[sap]
        signed = int(entry["signed"])
        if signed:
            canonical.append({
                "sap": sap,
                "direction": "add" if signed > 0 else "remove",
                "casePack": entry["casePacks"][0],
                "caseQuantity": abs(signed),
            })
    return _compact_json(canonical)


def _validate_cumulative_lines_shape(lines: Iterable[Dict[str, Any]]) -> None:
    case_packs: Dict[str, int] = {}
    for line in lines:
        sap = str(line.get("sap") or "").strip()
        if not SAP_PATTERN.fullmatch(sap) or line.get("direction") not in {"add", "remove"}:
            raise StructuredApiError(422, "The sent adjustment contains an invalid item.", "ADJUSTMENT_BATCH_INVALID")
        case_pack = _require_nonnegative_integer(line.get("casePack"), field="Case pack")
        case_quantity = _require_nonnegative_integer(line.get("caseQuantity"), field="Case quantity")
        if case_pack < 1 or case_quantity < 1:
            raise StructuredApiError(422, "The sent adjustment contains an invalid quantity.", "ADJUSTMENT_BATCH_INVALID")
        if sap in case_packs and case_packs[sap] != case_pack:
            raise StructuredApiError(422, "The sent adjustment contains conflicting case packs.", "ADJUSTMENT_BATCH_INVALID")
        case_packs[sap] = case_pack


def _sorted_rows(rows: Iterable[Dict[str, Any]], key) -> List[Dict[str, Any]]:
    return sorted((dict(row) for row in rows), key=key)


def _canonical_transfer_state(order: Dict[str, Any]) -> Dict[str, Any]:
    route_transfers = [{
        "sap": row.get("sap"),
        "toRouteNumber": row.get("toRouteNumber"),
        "units": _positive_int(row.get("units")),
        "casePack": _positive_int(row.get("casePack")),
        "transferDate": row.get("transferDate") if row.get("transferDate") is not None else None,
        "sourceOrderId": row.get("sourceOrderId") if row.get("sourceOrderId") is not None else None,
    } for row in (order.get("routeTransfers") or [])]
    inbound = [{
        "transferKey": row.get("transferKey"),
        "fromRouteNumber": row.get("fromRouteNumber"),
        "sap": row.get("sap"),
        "units": _positive_int(row.get("units")),
        "casePack": _positive_int(row.get("casePack")),
        "transferDate": row.get("transferDate") if row.get("transferDate") is not None else None,
        "sourceOrderId": row.get("sourceOrderId") if row.get("sourceOrderId") is not None else None,
    } for row in (order.get("inboundTransfersUsed") or [])]
    allocations = [{
        "transferKey": row.get("transferKey"),
        "storeId": row.get("storeId"),
        "sap": row.get("sap"),
        "units": _positive_int(row.get("units")),
    } for row in (order.get("inboundTransferStoreAllocations") or [])]

    return {
        "routeSplittingEnabled": bool(order.get("routeSplittingEnabled")),
        "routeTransfers": _sorted_rows(
            route_transfers,
            lambda row: f"{row.get('sap')}:{row.get('toRouteNumber')}:{row.get('sourceOrderId') or ''}:{row.get('transferDate') or ''}",
        ),
        "inboundTransfersUsed": _sorted_rows(
            inbound, lambda row: f"{row.get('transferKey')}:{row.get('sap')}"
        ),
        "inboundTransferStoreAllocations": _sorted_rows(
            allocations,
            lambda row: f"{row.get('transferKey')}:{row.get('storeId')}:{row.get('sap')}",
        ),
    }


def working_copy_signature(order: Dict[str, Any]) -> str:
    stores = []
    for store in order.get("stores") or []:
        items = [
            {"sap": str(item.get("sap")), "quantity": _positive_int(item.get("quantity"))}
            for item in (store.get("items") or [])
            if item.get("sap") and _positive_int(item.get("quantity")) > 0
        ]
        stores.append({
            "storeId": str(store.get("storeId") or store.get("id") or ""),
            "items": sorted(items, key=lambda item: item["sap"]),
        })
    stores.sort(key=lambda store: store["storeId"])
    return _compact_json({"stores": stores, **_canonical_transfer_state(order)})


def _purchase_totals(order: Dict[str, Any]) -> Dict[str, int]:
    totals: Dict[str, int] = {}
    for store in order.get("stores") or []:
        for item in store.get("items") or []:
            sap = str(item.get("sap") or "").strip()
            if sap:
                totals[sap] = totals.get(sap, 0) + _positive_int(item.get("quantity"))
    if order.get("routeSplittingEnabled"):
        for row in order.get("routeTransfers") or []:
            sap = str(row.get("sap") or "").strip()
            if sap:
                totals[sap] = totals.get(sap, 0) + _positive_int(row.get("units"))
    inbound_totals: Dict[str, int] = {}
    for row in order.get("inboundTransfersUsed") or []:
        sap = str(row.get("sap") or "").strip()
        if sap:
            inbound_totals[sap] = inbound_totals.get(sap, 0) + _positive_int(row.get("units"))
    for sap, inbound_units in inbound_totals.items():
        totals[sap] = max(0, totals.get(sap, 0) - inbound_units)
    return totals


def _line_deltas(lines: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    deltas: Dict[str, int] = {}
    for line in lines:
        sap = str(line.get("sap") or "").strip()
        units = _positive_int(line.get("casePack")) * _positive_int(line.get("caseQuantity"))
        if sap and units:
            deltas[sap] = deltas.get(sap, 0) + (units if line.get("direction") == "add" else -units)
    return {sap: delta for sap, delta in deltas.items() if delta}


def _store_sap_map(order: Dict[str, Any], sap: str) -> Dict[str, int]:
    return {
        str(store.get("storeId") or store.get("id") or ""): sum(
            _positive_int(item.get("quantity"))
            for item in (store.get("items") or [])
            if str(item.get("sap") or "").strip() == sap
        )
        for store in order.get("stores") or []
    }


def validate_and_merge_full_adjustment(
    *,
    current_order: Dict[str, Any],
    batch: Dict[str, Any],
    accepted_saps: List[str],
) -> Dict[str, Any]:
    lines = batch.get("cumulativeLines") or []
    working = batch.get("workingCopySnapshot") or {}
    _validate_cumulative_lines_shape(lines)
    if cumulative_lines_signature(lines) != batch.get("cumulativeSignature"):
        raise StructuredApiError(409, "The sent adjustment could not be verified.", "ADJUSTMENT_BATCH_MISMATCH")
    if working_copy_signature(working) != batch.get("workingCopySignature"):
        raise StructuredApiError(409, "The sent adjustment could not be verified.", "ADJUSTMENT_BATCH_MISMATCH")
    _validate_working_copy_shape(working)
    if _canonical_transfer_state(current_order) != _canonical_transfer_state(working):
        raise StructuredApiError(
            409,
            "Transfer details cannot be changed through an order adjustment.",
            "ADJUSTMENT_TRANSFER_STATE_CHANGED",
        )

    emailed_saps = {str(line.get("sap") or "").strip() for line in lines}
    accepted = set(accepted_saps)
    if "" in emailed_saps or not accepted.issubset(emailed_saps):
        raise StructuredApiError(422, "Accepted items must come from the sent adjustment.", "ADJUSTMENT_ACCEPTED_ITEMS_INVALID")

    expected_deltas = _line_deltas(lines)
    current_totals = _purchase_totals(current_order)
    working_totals = _purchase_totals(working)
    actual_deltas = {
        sap: working_totals.get(sap, 0) - current_totals.get(sap, 0)
        for sap in set(current_totals) | set(working_totals)
        if working_totals.get(sap, 0) != current_totals.get(sap, 0)
    }
    if actual_deltas != expected_deltas:
        raise StructuredApiError(409, "The finalized order changed. Rebuild the adjustment.", "ADJUSTMENT_BASELINE_CHANGED")
    for sap in (set(current_totals) | set(working_totals)) - emailed_saps:
        if _store_sap_map(current_order, sap) != _store_sap_map(working, sap):
            raise StructuredApiError(409, "The finalized order changed. Rebuild the adjustment.", "ADJUSTMENT_BASELINE_CHANGED")

    merged = deepcopy(current_order)
    if not accepted:
        return merged
    working_stores = {
        str(store.get("storeId") or store.get("id") or ""): store
        for store in working.get("stores") or []
    }
    merged_stores = {
        str(store.get("storeId") or store.get("id") or ""): store
        for store in merged.get("stores") or []
    }
    for store_id, working_store in working_stores.items():
        if store_id not in merged_stores:
            merged_store = {
                "storeId": store_id,
                "storeName": working_store.get("storeName") or "",
                "items": [],
            }
            merged["stores"].append(merged_store)
            merged_stores[store_id] = merged_store
    for store in merged.get("stores") or []:
        store_id = str(store.get("storeId") or store.get("id") or "")
        working_items = (working_stores.get(store_id) or {}).get("items") or []
        store["items"] = [
            *[deepcopy(item) for item in (store.get("items") or []) if str(item.get("sap") or "") not in accepted],
            *[deepcopy(item) for item in working_items if str(item.get("sap") or "") in accepted and _positive_int(item.get("quantity")) > 0],
        ]

    return merged
