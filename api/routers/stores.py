"""Store management router for route-scoped stores."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field, field_validator

from ..dependencies import (
    get_firestore,
    require_route_access,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse, ROUTE_NUMBER_PATTERN, STORE_ID_PATTERN

router = APIRouter()
logger = logging.getLogger(__name__)

VALID_DAYS = {
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
}


class StorePayload(BaseModel):
    name: str = Field(..., min_length=1, max_length=120)
    number: Optional[str] = Field(None, max_length=60)
    address: Optional[str] = Field(None, max_length=240)
    deliveryDays: List[str] = Field(default_factory=list)

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        trimmed = value.strip()
        if not trimmed:
            raise ValueError("Store name is required")
        return trimmed

    @field_validator("number", "address", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        trimmed = str(value).strip()
        return trimmed or None

    @field_validator("deliveryDays")
    @classmethod
    def validate_delivery_days(cls, value: List[str]) -> List[str]:
        days: List[str] = []
        seen = set()
        for raw_day in value or []:
            day = str(raw_day).strip()
            if day not in VALID_DAYS:
                raise ValueError(f"Invalid delivery day: {day}")
            if day not in seen:
                days.append(day)
                seen.add(day)
        return days


class StoreItemsPayload(BaseModel):
    items: List[str] = Field(default_factory=list)

    @field_validator("items")
    @classmethod
    def validate_items(cls, value: List[str]) -> List[str]:
        items: List[str] = []
        seen = set()
        for raw_item in value or []:
            item = str(raw_item).strip()
            if not item or len(item) > 50:
                raise ValueError("Invalid SAP code")
            if item not in seen:
                items.append(item)
                seen.add(item)
        return items


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _to_millis(value: Any, fallback: int) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return fallback
    return fallback


def _clean_optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_store(raw: Dict[str, Any], fallback_id: str) -> Dict[str, Any]:
    fallback_now = _now_millis()
    store: Dict[str, Any] = {
        "id": str(raw.get("id") or fallback_id),
        "name": _clean_optional_text(raw.get("name")) or "Unnamed Store",
        "deliveryDays": [
            str(day).strip()
            for day in (raw.get("deliveryDays") if isinstance(raw.get("deliveryDays"), list) else [])
            if str(day).strip()
        ],
        "items": [
            str(item).strip()
            for item in (raw.get("items") if isinstance(raw.get("items"), list) else [])
            if str(item).strip()
        ],
        "createdAt": _to_millis(raw.get("createdAt"), fallback_now),
        "updatedAt": _to_millis(raw.get("updatedAt"), fallback_now),
    }
    number = _clean_optional_text(raw.get("number"))
    address = _clean_optional_text(raw.get("address"))
    if number:
        store["number"] = number
    if address:
        store["address"] = address
    return store


def _store_ref(db: firestore.Client, route: str, store_id: str):
    return db.collection("routes").document(route).collection("stores").document(store_id)


def _active_draft_references_store(db: firestore.Client, route: str, store_id: str) -> bool:
    orders_ref = db.collection("routes").document(route).collection("orders")
    docs = orders_ref.where("status", "==", "draft").stream()
    for order_doc in docs:
        order = order_doc.to_dict() or {}
        for store in order.get("stores") or []:
            if str((store or {}).get("storeId") or "") == store_id:
                return True
    return False


def _validate_store_id(store_id: str) -> str:
    if not STORE_ID_PATTERN.match(store_id):
        raise HTTPException(400, "Invalid store id")
    return store_id


@router.get("/stores", responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}})
@rate_limit_history
async def list_stores(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return normalized stores for a route."""
    await require_route_access(route, decoded_token, db)

    stores_ref = db.collection("routes").document(route).collection("stores")
    stores = [
        _normalize_store(doc.to_dict() or {}, doc.id)
        for doc in stores_ref.order_by("name").stream()
    ]
    return {"routeNumber": route, "stores": stores}


@router.post(
    "/stores",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_store(
    request: Request,
    payload: StorePayload,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Create a route-scoped store and return it."""
    await require_route_access(route, decoded_token, db)

    timestamp = _now_millis()
    store_id = f"store_{timestamp}"
    store_data: Dict[str, Any] = {
        "id": store_id,
        "name": payload.name,
        "deliveryDays": payload.deliveryDays,
        "items": [],
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }
    if payload.number:
        store_data["number"] = payload.number
    if payload.address:
        store_data["address"] = payload.address

    _store_ref(db, route, store_id).set(store_data)
    logger.info("store.created route=%s store=%s uid=%s", route, store_id, decoded_token.get("uid"))
    return {"store": _normalize_store(store_data, store_id)}


@router.get(
    "/stores/{store_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_store(
    request: Request,
    store_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return a single normalized store."""
    await require_route_access(route, decoded_token, db)
    store_id = _validate_store_id(store_id)

    doc = _store_ref(db, route, store_id).get()
    if not doc.exists:
        raise HTTPException(404, "Store not found")
    return {"store": _normalize_store(doc.to_dict() or {}, doc.id)}


@router.put(
    "/stores/{store_id}",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def update_store(
    request: Request,
    store_id: str,
    payload: StorePayload,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Update store metadata."""
    await require_route_access(route, decoded_token, db)
    store_id = _validate_store_id(store_id)

    ref = _store_ref(db, route, store_id)
    doc = ref.get()
    if not doc.exists:
        raise HTTPException(404, "Store not found")

    update_data: Dict[str, Any] = {
        "name": payload.name,
        "deliveryDays": payload.deliveryDays,
        "updatedAt": _now_millis(),
    }
    update_data["number"] = payload.number if payload.number else firestore.DELETE_FIELD
    update_data["address"] = payload.address if payload.address else firestore.DELETE_FIELD

    ref.update(update_data)
    updated = ref.get()
    logger.info("store.updated route=%s store=%s uid=%s", route, store_id, decoded_token.get("uid"))
    return {"store": _normalize_store(updated.to_dict() or {}, updated.id)}


@router.put(
    "/stores/{store_id}/items",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def update_store_items(
    request: Request,
    store_id: str,
    payload: StoreItemsPayload,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Replace store item assignments."""
    await require_route_access(route, decoded_token, db)
    store_id = _validate_store_id(store_id)

    ref = _store_ref(db, route, store_id)
    doc = ref.get()
    if not doc.exists:
        raise HTTPException(404, "Store not found")

    ref.update({"items": payload.items, "updatedAt": _now_millis()})
    updated = ref.get()
    logger.info(
        "store.items.updated route=%s store=%s count=%s uid=%s",
        route,
        store_id,
        len(payload.items),
        decoded_token.get("uid"),
    )
    return {"store": _normalize_store(updated.to_dict() or {}, updated.id)}


@router.delete(
    "/stores/{store_id}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def delete_store(
    request: Request,
    store_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Delete a store if no active draft order references it."""
    await require_route_access(route, decoded_token, db)
    store_id = _validate_store_id(store_id)

    ref = _store_ref(db, route, store_id)
    if not ref.get().exists:
        raise HTTPException(404, "Store not found")
    if _active_draft_references_store(db, route, store_id):
        raise HTTPException(409, "Store is used by an active draft order")

    ref.delete()
    logger.info("store.deleted route=%s store=%s uid=%s", route, store_id, decoded_token.get("uid"))
    return {"ok": True}
