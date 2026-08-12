"""Catalog management router for route-scoped product catalogs."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field, field_validator

from ..dependencies import (
    get_firestore,
    has_access_to_route,
    require_route_access,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse, SAP_PATTERN
from .reference import (
    DEFAULT_REFERENCE_CATALOG_ID,
    _fetch_reference_catalog_items,
    _fetch_reference_catalog_meta,
)

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_SAP_LIST_INPUT = 2000
MAX_ROUTE_CATALOG_BATCH_WRITES = 450
LARGE_HIDE_REVIEW_THRESHOLD = 50
REFERENCE_CATALOG_ORIGIN = "routespark-reference"


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

    @field_validator("sap")
    @classmethod
    def validate_sap(cls, value: str) -> str:
        sap = value.strip()
        if not SAP_PATTERN.match(sap):
            raise ValueError("Invalid SAP code")
        return sap

    @field_validator("brand", "category", "fullName", mode="before")
    @classmethod
    def normalize_required_text(cls, value: Any) -> str:
        return str(value or "").strip()

    @field_validator("pcfAbbreviation", "upc", "sku", "notes", mode="before")
    @classmethod
    def normalize_optional_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


class SapListActivationRequest(BaseModel):
    saps: List[str] = Field(default_factory=list, max_length=MAX_SAP_LIST_INPUT)
    hideMissingReferenceItems: bool = False

    @field_validator("saps")
    @classmethod
    def normalize_saps(cls, value: List[str]) -> List[str]:
        seen: set[str] = set()
        normalized: List[str] = []
        for raw in value:
            sap = str(raw or "").strip()
            if not sap:
                continue
            if not SAP_PATTERN.match(sap):
                raise ValueError(f"Invalid SAP code: {sap}")
            if sap not in seen:
                seen.add(sap)
                normalized.append(sap)
        return normalized


class SapListActivationResponse(BaseModel):
    ok: Literal[True] = True
    summary: Dict[str, Any]


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


def _clean_bool(value: Any, fallback: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "y"}:
            return True
        if text in {"false", "0", "no", "n"}:
            return False
    return bool(value)


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
        "tags": raw.get("tags") if isinstance(raw.get("tags"), list) else [],
        "notes": _clean_optional_text(raw.get("notes")),
    }


def _normalize_route_product_for_plan(raw: Dict[str, Any], fallback_sap: str) -> Dict[str, Any]:
    product = _normalize_product(raw, fallback_sap)
    product.update(
        {
            "catalogOrigin": _clean_optional_text(raw.get("catalogOrigin")),
            "referenceSap": _clean_optional_text(raw.get("referenceSap")),
            "referenceVersion": raw.get("referenceVersion"),
            "createdAt": raw.get("createdAt"),
            "updatedAt": raw.get("updatedAt"),
        }
    )
    return product


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


def fetch_reference_catalog_for_activation(
    *,
    catalog_id: str = DEFAULT_REFERENCE_CATALOG_ID,
) -> Dict[str, Any]:
    """Return the cluster-backed reference catalog used by SAP-list setup."""
    meta = _fetch_reference_catalog_meta(catalog_id=catalog_id)
    items = _fetch_reference_catalog_items(
        catalog_id=catalog_id,
        limit=MAX_SAP_LIST_INPUT,
        include_inactive=True,
    )
    return {"version": meta.get("version"), "items": items}


def list_route_catalog_products(
    *,
    db: firestore.Client,
    route: str,
    include_inactive: bool,
) -> List[Dict[str, Any]]:
    products_ref = db.collection("masterCatalog").document(route).collection("products")
    products = [
        _normalize_route_product_for_plan(doc.to_dict() or {}, doc.id)
        for doc in products_ref.stream()
    ]
    if not include_inactive:
        products = [product for product in products if product.get("active") is not False]
    products.sort(key=lambda product: (product["displayOrder"], product["sap"]))
    return products


def _sap_list_safety(
    *,
    uploaded_sap_count: int,
    matched_reference_count: int,
    hidden_reference_count: int,
    hide_missing_reference_items: bool,
) -> Dict[str, Any]:
    hide_missing_blocked = hide_missing_reference_items and matched_reference_count == 0
    reason: Optional[str] = None
    if hide_missing_blocked:
        reason = (
            "Upload at least one SAP code before hiding missing RouteSpark items."
            if uploaded_sap_count == 0
            else "No uploaded SAP codes matched the RouteSpark reference catalog."
        )
    return {
        "hideMissingBlocked": hide_missing_blocked,
        "hideMissingBlockReason": reason,
        "largeHideNeedsReview": hidden_reference_count > LARGE_HIDE_REVIEW_THRESHOLD,
    }


def build_sap_activation_plan(
    *,
    route_number: str,
    reference_version: Optional[int],
    reference_products: List[Dict[str, Any]],
    route_products: List[Dict[str, Any]],
    uploaded_saps: List[str],
    hide_missing_reference_items: bool,
) -> Dict[str, Any]:
    reference_by_sap = {product["sap"]: product for product in reference_products if product.get("sap")}
    route_by_sap = {product["sap"]: product for product in route_products if product.get("sap")}
    uploaded_sap_set = set(uploaded_saps)

    added_saps: List[str] = []
    activated_saps: List[str] = []
    already_active_saps: List[str] = []
    route_only_saps: List[str] = []
    orphaned_reference_saps: List[str] = []
    unknown_saps: List[str] = []
    inactive_reference_matched_saps: List[str] = []
    matched_reference_saps: List[str] = []

    for sap in uploaded_saps:
        reference_product = reference_by_sap.get(sap)
        route_product = route_by_sap.get(sap)

        if route_product and route_product.get("catalogOrigin") != REFERENCE_CATALOG_ORIGIN:
            route_only_saps.append(sap)
            continue

        if not reference_product:
            if route_product and route_product.get("catalogOrigin") == REFERENCE_CATALOG_ORIGIN:
                orphaned_reference_saps.append(sap)
            else:
                unknown_saps.append(sap)
            continue

        matched_reference_saps.append(sap)
        if reference_product.get("active") is False:
            inactive_reference_matched_saps.append(sap)
        if not route_product:
            added_saps.append(sap)
        elif route_product.get("active") is False:
            activated_saps.append(sap)
        else:
            already_active_saps.append(sap)

    hidden_reference_saps = (
        sorted(
            product["sap"]
            for product in route_products
            if product.get("active") is not False
            and product.get("catalogOrigin") == REFERENCE_CATALOG_ORIGIN
            and product.get("sap") not in uploaded_sap_set
        )
        if hide_missing_reference_items
        else []
    )
    preserved_user_saps = sorted(
        product["sap"]
        for product in route_products
        if product.get("active") is not False
        and product.get("catalogOrigin") != REFERENCE_CATALOG_ORIGIN
        and product.get("sap") not in uploaded_sap_set
    )
    safety = _sap_list_safety(
        uploaded_sap_count=len(uploaded_saps),
        matched_reference_count=len(matched_reference_saps),
        hidden_reference_count=len(hidden_reference_saps),
        hide_missing_reference_items=hide_missing_reference_items,
    )
    summary = {
        "routeNumber": route_number,
        "referenceVersion": reference_version,
        "uploadedSapCount": len(uploaded_saps),
        "matchedReferenceCount": len(matched_reference_saps),
        "alreadyActiveCount": len(already_active_saps),
        "preservedUserCount": len(preserved_user_saps) + len(route_only_saps),
        "activatedSaps": sorted(activated_saps),
        "addedSaps": sorted(added_saps),
        "hiddenReferenceSaps": hidden_reference_saps,
        "unknownSaps": sorted(unknown_saps),
        "routeOnlySaps": sorted(route_only_saps),
        "orphanedReferenceSaps": sorted(orphaned_reference_saps),
        "inactiveReferenceMatchedSaps": sorted(inactive_reference_matched_saps),
        "safety": safety,
    }
    return {
        "summary": summary,
        "referenceBySap": reference_by_sap,
        "routeBySap": route_by_sap,
        "plannedWriteCount": len(added_saps) + len(activated_saps) + len(hidden_reference_saps),
    }


def route_reference_doc_from_reference(
    *,
    reference: Dict[str, Any],
    reference_version: Optional[int],
    now: Any,
    existing: Optional[Dict[str, Any]] = None,
    active: Optional[bool] = None,
) -> Dict[str, Any]:
    sap = _clean_text(reference.get("sap"))
    doc: Dict[str, Any] = {
        "sap": sap,
        "brand": _clean_text(reference.get("brand"), "Unknown"),
        "category": _clean_text(reference.get("category"), "Other"),
        "fullName": _clean_text(reference.get("fullName") or reference.get("full_name"), "Unnamed Product"),
        "casePack": _clean_int(reference.get("casePack", reference.get("case_pack")), 0),
        "displayOrder": _clean_int(reference.get("displayOrder", reference.get("display_order")), 0),
        "active": _clean_bool(reference.get("active"), True) if active is None else active,
        "catalogOrigin": REFERENCE_CATALOG_ORIGIN,
        "referenceSap": sap,
        "updatedAt": now,
    }
    if existing and existing.get("createdAt") is not None:
        doc["createdAt"] = existing.get("createdAt")
    else:
        doc["createdAt"] = now
    if reference_version is not None:
        doc["referenceVersion"] = reference_version
    upc = _clean_optional_text(reference.get("upc"))
    if upc:
        doc["upc"] = upc
    sku = _clean_optional_text(reference.get("sku"))
    if sku:
        doc["sku"] = sku
    return doc


def ensure_route_catalog_mutation_ready(
    *,
    db: firestore.Client,
    route: str,
    owner_uid: Optional[str],
) -> Dict[str, Any]:
    route_ref = db.collection("routes").document(route)
    snapshot = route_ref.get()
    route_data = snapshot.to_dict() or {}
    existing_catalog = route_data.get("catalog") if isinstance(route_data.get("catalog"), dict) else {}
    catalog = {
        "mode": "source",
        "sourceCatalogId": route,
        "adoptedVersion": None,
        "shareEligible": True,
        "publishRequired": False,
        **(existing_catalog or {}),
    }
    if catalog.get("mode") != "adopted":
        return {"forked": False, "mode": catalog.get("mode") or "source"}

    now = _now_millis()
    source_catalog_id = _clean_optional_text(catalog.get("sourceCatalogId"))
    if source_catalog_id:
        adopters = db.collection("sharedCatalogs").document(source_catalog_id).collection("adopters")
        adopters.document(route).delete()
        if owner_uid:
            adopters.document(owner_uid).delete()

    catalog.update(
        {
            "mode": "forked",
            "shareEligible": True,
            "publishRequired": True,
            "updatedAt": now,
        }
    )
    route_ref.set({"catalog": catalog, "updatedAt": now}, merge=True)
    return {"forked": True, "mode": "forked"}


def _commit_catalog_operations(db: firestore.Client, operations: List[Any]) -> int:
    batch = db.batch()
    batch_write_count = 0
    committed_batches = 0

    def commit_batch() -> None:
        nonlocal batch, batch_write_count, committed_batches
        if batch_write_count == 0:
            return
        batch.commit()
        committed_batches += 1
        batch = db.batch()
        batch_write_count = 0

    for operation in operations:
        if batch_write_count >= MAX_ROUTE_CATALOG_BATCH_WRITES:
            commit_batch()
        operation(batch)
        batch_write_count += 1

    commit_batch()
    return committed_batches


def apply_sap_activation_plan(
    *,
    db: firestore.Client,
    route: str,
    plan: Dict[str, Any],
    reference_version: Optional[int],
) -> int:
    summary = plan["summary"]
    reference_by_sap = plan["referenceBySap"]
    route_by_sap = plan["routeBySap"]
    now = _now_millis()
    operations: List[Any] = []

    for sap in summary["addedSaps"]:
        reference_product = reference_by_sap.get(sap)
        if not reference_product:
            continue
        ref = _product_ref(db, route, sap)
        doc_data = route_reference_doc_from_reference(
            reference=reference_product,
            reference_version=reference_version,
            now=now,
        )
        operations.append(lambda batch, product_ref=ref, data=doc_data: batch.set(product_ref, data, merge=True))

    for sap in summary["activatedSaps"]:
        reference_product = reference_by_sap.get(sap)
        if reference_product:
            ref = _product_ref(db, route, sap)
            doc_data = route_reference_doc_from_reference(
                reference=reference_product,
                reference_version=reference_version,
                now=now,
                existing=route_by_sap.get(sap),
                active=True,
            )
            operations.append(lambda batch, product_ref=ref, data=doc_data: batch.set(product_ref, data, merge=True))
        else:
            ref = _product_ref(db, route, sap)
            operations.append(lambda batch, product_ref=ref: batch.update(product_ref, {"active": True, "updatedAt": now}))

    for sap in summary["hiddenReferenceSaps"]:
        ref = _product_ref(db, route, sap)
        operations.append(lambda batch, product_ref=ref: batch.update(product_ref, {"active": False, "updatedAt": now}))

    return _commit_catalog_operations(db, operations)


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


@router.post(
    "/catalog/sap-list/preview",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def preview_sap_list_activation(
    request: Request,
    payload: SapListActivationRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Preview SAP-list activation against the RouteSpark reference catalog."""
    await _require_catalog_owner(route, decoded_token, db)
    reference = fetch_reference_catalog_for_activation()
    route_products = list_route_catalog_products(db=db, route=route, include_inactive=True)
    plan = build_sap_activation_plan(
        route_number=route,
        reference_version=reference["version"],
        reference_products=reference["items"],
        route_products=route_products,
        uploaded_saps=payload.saps,
        hide_missing_reference_items=payload.hideMissingReferenceItems,
    )
    return {"ok": True, "summary": {**plan["summary"], "dryRun": True, "applied": False}}


@router.post(
    "/catalog/sap-list/activate",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def apply_sap_list_activation(
    request: Request,
    payload: SapListActivationRequest,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Apply SAP-list activation. The plan is always recomputed server-side."""
    await _require_catalog_owner(route, decoded_token, db)
    reference = fetch_reference_catalog_for_activation()
    route_products = list_route_catalog_products(db=db, route=route, include_inactive=True)
    plan = build_sap_activation_plan(
        route_number=route,
        reference_version=reference["version"],
        reference_products=reference["items"],
        route_products=route_products,
        uploaded_saps=payload.saps,
        hide_missing_reference_items=payload.hideMissingReferenceItems,
    )
    summary = plan["summary"]
    if summary["safety"]["hideMissingBlocked"]:
        raise HTTPException(
            status_code=400,
            detail={
                "error": summary["safety"]["hideMissingBlockReason"]
                or "Cannot hide missing RouteSpark items because no uploaded SAPs matched.",
                "code": "hide_missing_blocked",
                "details": {"summary": {**summary, "dryRun": False, "applied": False}},
            },
        )

    if plan["plannedWriteCount"] == 0:
        return {"ok": True, "summary": {**summary, "dryRun": False, "applied": True}}

    owner_uid = decoded_token.get("uid")
    fork_result = ensure_route_catalog_mutation_ready(db=db, route=route, owner_uid=owner_uid)
    try:
        committed_batches = apply_sap_activation_plan(
            db=db,
            route=route,
            plan=plan,
            reference_version=reference["version"],
        )
    except Exception as exc:
        logger.exception("catalog.sap_list.apply_failed route=%s uid=%s", route, owner_uid)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "Catalog setup did not finish. Retry to apply the remaining SAP codes.",
                "code": "sap_list_apply_failed",
                "details": {
                    "routeNumber": route,
                    "plannedWriteCount": plan["plannedWriteCount"],
                    "message": str(exc),
                },
            },
        ) from exc

    logger.info(
        "catalog.sap_list.applied route=%s uid=%s writes=%s batches=%s forked=%s",
        route,
        owner_uid,
        plan["plannedWriteCount"],
        committed_batches,
        fork_result.get("forked"),
    )
    return {
        "ok": True,
        "summary": {
            **summary,
            "dryRun": False,
            "applied": True,
            "committedBatches": committed_batches,
            "forked": fork_result.get("forked", False),
        },
    }


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
