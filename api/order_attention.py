"""Pure order-attention rules shared by finalize enforcement and tests."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def _quantity(value: Any) -> int:
    try:
        quantity = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, quantity)


def _key(store_id: Any, sap: Any) -> Tuple[str, str]:
    return (str(store_id or "").strip(), str(sap or "").strip())


def build_core_item_issues(
    *,
    order_data: Dict[str, Any],
    stores: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return zero-quantity Core rows that lack an order-scoped override."""
    override_keys = {
        _key(override.get("storeId"), override.get("sap"))
        for override in order_data.get("coreItemOverrides") or []
        if isinstance(override, dict)
    }
    order_store_by_id = {
        str(store.get("storeId") or "").strip(): store
        for store in order_data.get("stores") or []
        if isinstance(store, dict)
    }
    inbound_by_key: Dict[Tuple[str, str], int] = {}
    for allocation in order_data.get("inboundTransferStoreAllocations") or []:
        if not isinstance(allocation, dict):
            continue
        key = _key(allocation.get("storeId"), allocation.get("sap"))
        if not all(key):
            continue
        inbound_by_key[key] = inbound_by_key.get(key, 0) + _quantity(allocation.get("units"))

    issues: List[Dict[str, Any]] = []
    for store in stores:
        if not isinstance(store, dict):
            continue
        store_id = str(store.get("id") or store.get("storeId") or "").strip()
        if not store_id:
            continue
        store_name = str(store.get("name") or "Unnamed Store").strip() or "Unnamed Store"
        core_saps = {
            str(sap).strip()
            for sap in store.get("coreItemSaps") or []
            if str(sap).strip()
        }
        order_store = order_store_by_id.get(store_id) or {}
        item_quantity_by_sap = {
            str(item.get("sap") or "").strip(): _quantity(item.get("quantity"))
            for item in order_store.get("items") or []
            if isinstance(item, dict) and str(item.get("sap") or "").strip()
        }

        for sap in sorted(core_saps):
            key = (store_id, sap)
            store_quantity = item_quantity_by_sap.get(sap, 0)
            if store_quantity > 0 or key in override_keys:
                continue
            assigned_inbound_quantity = inbound_by_key.get(key, 0)
            issues.append({
                "kind": "core",
                "storeId": store_id,
                "storeName": store_name,
                "sap": sap,
                "storeQuantity": store_quantity,
                "assignedInboundQuantity": assigned_inbound_quantity,
                "purchaseQuantity": max(0, store_quantity - assigned_inbound_quantity),
                "requiresOverride": True,
            })

    return issues
