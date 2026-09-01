"""Deterministic merge helpers for transactional draft-order updates."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Iterable, List, Mapping


def _model_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, Mapping):
        return deepcopy(dict(value))
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(exclude_unset=True, by_alias=False)
    legacy_dict = getattr(value, "dict", None)
    if callable(legacy_dict):
        return legacy_dict(exclude_unset=True, by_alias=False)
    raise TypeError("Order store patches must be mappings or Pydantic models")


def merge_order_store_patches(
    existing_stores: Iterable[Mapping[str, Any]],
    incoming_stores: Iterable[Any],
) -> List[Dict[str, Any]]:
    """Merge quantity patches without erasing unrelated store/item fields.

    A zero quantity removes that SAP line, not its parent store. There is no
    store-deletion signal in ``OrderUpdateRequest``, so existing itemless stores
    remain authoritative and incoming empty stores are retained.
    """

    existing = [deepcopy(dict(store)) for store in existing_stores]
    existing_by_id: Dict[str, Dict[str, Any]] = {
        str(store.get("storeId")): store
        for store in existing
        if store.get("storeId") is not None
    }
    merged_by_id: Dict[str, Dict[str, Any]] = {
        store_id: {**store, "items": deepcopy(list(store.get("items") or []))}
        for store_id, store in existing_by_id.items()
    }
    incoming_order: List[str] = []

    for incoming_value in incoming_stores:
        incoming = _model_dict(incoming_value)
        store_id = str(incoming.get("storeId") or "").strip()
        if not store_id:
            continue
        incoming_order.append(store_id)
        incoming_items = list(incoming.pop("items", []) or [])
        current_store = merged_by_id.get(store_id, {})
        current_items = list(current_store.get("items") or [])
        items_by_sap: Dict[str, Dict[str, Any]] = {
            str(item.get("sap")): deepcopy(dict(item))
            for item in current_items
            if item.get("sap") is not None
        }

        for incoming_item_value in incoming_items:
            incoming_item = _model_dict(incoming_item_value)
            sap = str(incoming_item.get("sap") or "").strip()
            if not sap:
                continue
            quantity = int(incoming_item.get("quantity") or 0)
            if quantity <= 0:
                items_by_sap.pop(sap, None)
                continue
            items_by_sap[sap] = {
                **items_by_sap.get(sap, {}),
                **incoming_item,
            }

        merged_by_id[store_id] = {
            **current_store,
            **incoming,
            "storeId": store_id,
            "items": list(items_by_sap.values()),
        }

    existing_order = [
        str(store.get("storeId"))
        for store in existing
        if store.get("storeId") is not None
    ]
    ordered_ids = list(dict.fromkeys([*existing_order, *incoming_order]))
    return [merged_by_id[store_id] for store_id in ordered_ids if store_id in merged_by_id]
