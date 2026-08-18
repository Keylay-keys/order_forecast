"""Pure validation and merge logic for server-authoritative full-order confirmation."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional, Set

from .errors import StructuredApiError
from .models import SAP_PATTERN, STORE_ID_PATTERN


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


def _require_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StructuredApiError(422, f"{field} must be a whole number.", "ADJUSTMENT_BATCH_INVALID")
    if not math.isfinite(float(value)) or not float(value).is_integer():
        raise StructuredApiError(422, f"{field} must be a whole number.", "ADJUSTMENT_BATCH_INVALID")
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


def semantic_changes_signature(base_order_revision: int, changes: Iterable[Dict[str, Any]]) -> str:
    canonical_changes = []
    for source in sorted(changes, key=lambda change: str(change.get("sap") or "")):
        store_deltas = sorted(
            source.get("storeDeltas") or [],
            key=lambda delta: str(delta.get("storeId") or ""),
        )
        canonical_changes.append({
            "baselinePurchaseUnits": source.get("baselinePurchaseUnits"),
            "emailOnlyUnitDelta": source.get("emailOnlyUnitDelta"),
            "sap": source.get("sap"),
            "storeDeltas": [{
                "storeId": delta.get("storeId"),
                "unitDelta": delta.get("unitDelta"),
            } for delta in store_deltas],
        })
    return _compact_json({
        "baseOrderRevision": base_order_revision,
        "quantityChanges": canonical_changes,
    })


def _validate_semantic_changes(
    *,
    batch: Dict[str, Any],
    emailed_saps: Set[str],
    expected_deltas: Dict[str, int],
) -> List[Dict[str, Any]]:
    base_revision = _require_nonnegative_integer(
        batch.get("baseOrderRevision"),
        field="Base order revision",
    )
    raw_changes = batch.get("quantityChanges")
    if not isinstance(raw_changes, list) or len(raw_changes) > 250:
        raise StructuredApiError(422, "The semantic adjustment is invalid.", "ADJUSTMENT_BATCH_INVALID")

    changes: List[Dict[str, Any]] = []
    seen_saps: Set[str] = set()
    for source in raw_changes:
        if not isinstance(source, dict):
            raise StructuredApiError(422, "The semantic adjustment is invalid.", "ADJUSTMENT_BATCH_INVALID")
        sap = str(source.get("sap") or "").strip()
        if not SAP_PATTERN.fullmatch(sap) or sap in seen_saps:
            raise StructuredApiError(422, "The semantic adjustment contains an invalid item.", "ADJUSTMENT_BATCH_INVALID")
        seen_saps.add(sap)
        baseline = _require_nonnegative_integer(
            source.get("baselinePurchaseUnits"),
            field="Baseline purchase units",
        )
        email_only_delta = _require_integer(
            source.get("emailOnlyUnitDelta"),
            field="Email-only quantity",
        )
        raw_store_deltas = source.get("storeDeltas")
        if not isinstance(raw_store_deltas, list) or len(raw_store_deltas) > 500:
            raise StructuredApiError(422, "The semantic adjustment is invalid.", "ADJUSTMENT_BATCH_INVALID")
        store_deltas = []
        seen_store_ids: Set[str] = set()
        for raw_delta in raw_store_deltas:
            if not isinstance(raw_delta, dict):
                raise StructuredApiError(422, "The semantic adjustment is invalid.", "ADJUSTMENT_BATCH_INVALID")
            store_id = str(raw_delta.get("storeId") or "").strip()
            unit_delta = _require_integer(raw_delta.get("unitDelta"), field="Store quantity delta")
            if (
                not STORE_ID_PATTERN.fullmatch(store_id)
                or store_id in seen_store_ids
                or unit_delta == 0
                or abs(unit_delta) > 1_000_000
            ):
                raise StructuredApiError(422, "The semantic adjustment contains an invalid store delta.", "ADJUSTMENT_BATCH_INVALID")
            seen_store_ids.add(store_id)
            store_deltas.append({"storeId": store_id, "unitDelta": unit_delta})
        allocated_delta = sum(delta["unitDelta"] for delta in store_deltas)
        if allocated_delta + email_only_delta != expected_deltas.get(sap, 0):
            raise StructuredApiError(409, "The semantic adjustment does not match the emailed quantities.", "ADJUSTMENT_BATCH_MISMATCH")
        changes.append({
            "sap": sap,
            "baselinePurchaseUnits": baseline,
            "storeDeltas": sorted(store_deltas, key=lambda delta: delta["storeId"]),
            "emailOnlyUnitDelta": email_only_delta,
        })

    if seen_saps != emailed_saps:
        raise StructuredApiError(409, "The semantic adjustment does not match the emailed items.", "ADJUSTMENT_BATCH_MISMATCH")
    expected_signature = semantic_changes_signature(base_revision, changes)
    if expected_signature != batch.get("semanticSignature"):
        raise StructuredApiError(409, "The semantic adjustment could not be verified.", "ADJUSTMENT_BATCH_MISMATCH")
    return sorted(changes, key=lambda change: change["sap"])


def _apply_semantic_changes(
    *,
    current_order: Dict[str, Any],
    working_order: Dict[str, Any],
    changes: List[Dict[str, Any]],
    accepted_saps: Set[str],
) -> Dict[str, Any]:
    current_totals = _purchase_totals(current_order)
    quantity_conflict_saps = sorted(
        change["sap"]
        for change in changes
        if (
            change["sap"] in accepted_saps
            and current_totals.get(change["sap"], 0) != change["baselinePurchaseUnits"]
        )
    )
    if quantity_conflict_saps:
        raise StructuredApiError(
            409,
            "An emailed item quantity changed after this adjustment was sent.",
            "ADJUSTMENT_QUANTITY_CONFLICT",
            {"saps": quantity_conflict_saps},
        )

    merged = deepcopy(current_order)
    merged_stores = {
        str(store.get("storeId") or store.get("id") or ""): store
        for store in merged.get("stores") or []
    }
    working_stores = {
        str(store.get("storeId") or store.get("id") or ""): store
        for store in working_order.get("stores") or []
    }
    for change in changes:
        sap = change["sap"]
        if sap not in accepted_saps:
            continue
        for delta in change["storeDeltas"]:
            store_id = delta["storeId"]
            store = merged_stores.get(store_id)
            if store is None:
                raise StructuredApiError(
                    409,
                    "A store referenced by this adjustment is no longer in the finalized order.",
                    "ADJUSTMENT_STORE_CONFLICT",
                    {"sap": sap, "storeId": store_id},
                )
            current_items = store.get("items") or []
            current_quantity = sum(
                _positive_int(item.get("quantity"))
                for item in current_items
                if str(item.get("sap") or "").strip() == sap
            )
            next_quantity = current_quantity + delta["unitDelta"]
            if next_quantity < 0:
                raise StructuredApiError(
                    409,
                    "A store no longer has enough quantity for this adjustment.",
                    "ADJUSTMENT_ALLOCATION_CONFLICT",
                    {"saps": [sap], "storeId": store_id},
                )
            template = next(
                (item for item in current_items if str(item.get("sap") or "").strip() == sap),
                None,
            ) or next(
                (
                    item
                    for item in (working_stores.get(store_id) or {}).get("items") or []
                    if str(item.get("sap") or "").strip() == sap
                ),
                None,
            ) or {"sap": sap}
            store["items"] = [
                *[deepcopy(item) for item in current_items if str(item.get("sap") or "").strip() != sap],
                *([{**deepcopy(template), "sap": sap, "quantity": next_quantity}] if next_quantity > 0 else []),
            ]
    return merged


def validate_and_merge_full_adjustment(
    *,
    current_order: Dict[str, Any],
    batch: Dict[str, Any],
    accepted_saps: List[str],
    intervening_reallocation_saps: Optional[Set[str]] = None,
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
    if batch.get("schemaVersion") == 2:
        changes = _validate_semantic_changes(
            batch=batch,
            emailed_saps=emailed_saps,
            expected_deltas=expected_deltas,
        )
        return _apply_semantic_changes(
            current_order=current_order,
            working_order=working,
            changes=changes,
            accepted_saps=accepted,
        )

    current_totals = _purchase_totals(current_order)
    working_totals = _purchase_totals(working)
    quantity_conflict_saps = sorted(
        sap
        for sap in emailed_saps
        if working_totals.get(sap, 0) - current_totals.get(sap, 0) != expected_deltas.get(sap, 0)
    )
    if quantity_conflict_saps:
        raise StructuredApiError(
            409,
            "An emailed item quantity changed after this adjustment was sent.",
            "ADJUSTMENT_QUANTITY_CONFLICT",
            {"saps": quantity_conflict_saps},
        )

    reallocated_saps = intervening_reallocation_saps or set()
    allocation_conflict_saps = sorted(emailed_saps & reallocated_saps)
    if allocation_conflict_saps:
        raise StructuredApiError(
            409,
            "An emailed item was reallocated after this adjustment was sent.",
            "ADJUSTMENT_ALLOCATION_CONFLICT",
            {"saps": allocation_conflict_saps},
        )
    for sap in (set(current_totals) | set(working_totals)) - emailed_saps:
        if (
            sap not in reallocated_saps
            and _store_sap_map(current_order, sap) != _store_sap_map(working, sap)
        ):
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
