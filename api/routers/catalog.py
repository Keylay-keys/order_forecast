"""Catalog management router for route-scoped product catalogs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field, validator

from ..dependencies import (
    get_firestore,
    has_access_to_route,
    require_route_access,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse, SAP_PATTERN

router = APIRouter()
logger = logging.getLogger(__name__)


class CatalogProductPayload(BaseModel):
    sap: str = Field(..., min_length=1, max_length=20)
    brand: str = Field(default="", max_length=120)
    category: str = Field(default="", max_length=120)
    fullName: str = Field(..., min_length=1, max_length=240)
    casePack: int = Field(default=0, ge=0, le=100000)
    displayOrder: int = Field(default=0, ge=0, le=1000000)
    active: bool = True
    pcfAbbreviation: Optional[str] = Field(None, max_length=120)
    upc: Optional[str] = Field(None, max_length=120)
    sku: Optional[str] = Field(None, max_length=120)
    notes: Optional[str] = Field(None, max_length=1000)

    @validator("sap")
    def validate_sap(cls, value: str) -> str:
        sap = value.strip()
        if not SAP_PATTERN.match(sap):
            raise ValueError("Invalid SAP code")
        return sap

    @validator("brand", "category", "fullName", pre=True)
    def normalize_required_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @validator("pcfAbbreviation", "upc", "sku", "notes", pre=True)
    def normalize_optional_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def _now_millis() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _clean_text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _clean_optional_text(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _clean_int(value: Any, fallback: int = 0) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return number if number >= 0 else fallback


def _product_ref(db: firestore.Client, route: str, sap: str):
    return db.collection("masterCatalog").document(route).collection("products").document(sap)


def _normalize_product(raw: Dict[str, Any], fallback_sap: str) -> Dict[str, Any]:
    sap = _clean_text(raw.get("sap"), fallback_sap)
    full_name = (
        _clean_text(raw.get("fullName"))
        or _clean_text(raw.get("name"))
        or _clean_text(raw.get("full_name"))
        or _clean_text(raw.get("description"))
        or "Unnamed Product"
    )
    upc = _clean_optional_text(raw.get("upc") or raw.get("sku"))
    return {
        "sap": sap,
        "brand": _clean_text(raw.get("brand")),
        "category": _clean_text(raw.get("category")),
        "fullName": full_name,
        "casePack": _clean_int(raw.get("casePack", raw.get("case_pack")), 0),
        "displayOrder": _clean_int(raw.get("displayOrder", raw.get("display_order")), 0),
        "active": bool(raw.get("active", raw.get("isActive", True))),
        "pcfAbbreviation": _clean_optional_text(raw.get("pcfAbbreviation") or raw.get("pcf_abbreviation")),
        "upc": upc,
        "sku": _clean_optional_text(raw.get("sku")),
        "notes": _clean_optional_text(raw.get("notes")),
    }


def _payload_to_doc(payload: CatalogProductPayload, *, created_at: Optional[int] = None) -> Dict[str, Any]:
    now = _now_millis()
    doc: Dict[str, Any] = {
        "sap": payload.sap,
        "brand": payload.brand,
        "category": payload.category,
        "fullName": payload.fullName,
        "casePack": payload.casePack,
        "displayOrder": payload.displayOrder,
        "active": payload.active,
        "updatedAt": now,
    }
    if created_at is not None:
        doc["createdAt"] = created_at
    if payload.pcfAbbreviation:
        doc["pcfAbbreviation"] = payload.pcfAbbreviation
    if payload.upc:
        doc["upc"] = payload.upc
    if payload.sku:
        doc["sku"] = payload.sku
    if payload.notes:
        doc["notes"] = payload.notes
    return doc


def _validate_sap_path(sap: str) -> str:
    value = sap.strip()
    if not SAP_PATTERN.match(value):
        raise HTTPException(400, "Invalid SAP code")
    return value


def _is_owner_for_route(user_data: Dict[str, Any], route: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if str(profile.get("role") or "").strip() == "owner" and has_access_to_route(user_data, route):
        return True

    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route) if isinstance(assignments, dict) else None
    return isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "owner"


async def _require_catalog_owner(
    route: str,
    decoded_token: dict,
    db: firestore.Client,
) -> Dict[str, Any]:
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_owner_for_route(user_data, route):
        raise HTTPException(403, "Catalog changes require route owner access")
    return user_data


@router.get(
    "/catalog/products",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def list_catalog_products(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return every product for route catalog management, including inactive products."""
    await require_route_access(route, decoded_token, db)

    products_ref = db.collection("masterCatalog").document(route).collection("products")
    products = [
        _normalize_product(doc.to_dict() or {}, doc.id)
        for doc in products_ref.stream()
    ]
    products.sort(key=lambda product: (product["displayOrder"], product["sap"]))
    return {"routeNumber": route, "products": products}


@router.get(
    "/catalog/products/{sap}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_catalog_product(
    request: Request,
    sap: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return one catalog product."""
    await require_route_access(route, decoded_token, db)
    sap = _validate_sap_path(sap)

    doc = _product_ref(db, route, sap).get()
    if not doc.exists:
        raise HTTPException(404, "Product not found")
    return {"product": _normalize_product(doc.to_dict() or {}, doc.id)}


@router.post(
    "/catalog/products",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def create_catalog_product(
    request: Request,
    payload: CatalogProductPayload,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Create a product in the route catalog."""
    await _require_catalog_owner(route, decoded_token, db)

    ref = _product_ref(db, route, payload.sap)
    if ref.get().exists:
        raise HTTPException(409, "Product already exists")

    doc_data = _payload_to_doc(payload, created_at=_now_millis())
    ref.set(doc_data)
    logger.info("catalog.product.created route=%s sap=%s uid=%s", route, payload.sap, decoded_token.get("uid"))
    return {"product": _normalize_product(doc_data, payload.sap)}


@router.put(
    "/catalog/products/{sap}",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def update_catalog_product(
    request: Request,
    sap: str,
    payload: CatalogProductPayload,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Update one product in the route catalog."""
    await _require_catalog_owner(route, decoded_token, db)
    sap = _validate_sap_path(sap)
    if payload.sap != sap:
        raise HTTPException(400, "SAP cannot be changed after product creation")

    ref = _product_ref(db, route, sap)
    doc = ref.get()
    if not doc.exists:
        raise HTTPException(404, "Product not found")

    raw = doc.to_dict() or {}
    doc_data = _payload_to_doc(payload)
    if raw.get("createdAt") is not None:
        doc_data["createdAt"] = raw.get("createdAt")
    ref.set(doc_data, merge=False)
    updated = ref.get()
    logger.info("catalog.product.updated route=%s sap=%s uid=%s", route, sap, decoded_token.get("uid"))
    return {"product": _normalize_product(updated.to_dict() or {}, updated.id)}


@router.delete(
    "/catalog/products/{sap}",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def delete_catalog_product(
    request: Request,
    sap: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Hard-delete a catalog product, matching mobile delete behavior."""
    await _require_catalog_owner(route, decoded_token, db)
    sap = _validate_sap_path(sap)

    ref = _product_ref(db, route, sap)
    if not ref.get().exists:
        raise HTTPException(404, "Product not found")
    ref.delete()
    logger.info("catalog.product.deleted route=%s sap=%s uid=%s", route, sap, decoded_token.get("uid"))
    return {"ok": True}
