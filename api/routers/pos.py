"""POS invoice router - list, detail, archive, delete."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional
import re

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write

router = APIRouter()

STATUS_VALUES = {"active", "archived"}


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value / 1000).isoformat() + "Z"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _normalize_invoice(doc: firestore.DocumentSnapshot) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    data["id"] = data.get("id") or doc.id
    for field in ("createdAt", "archivedAt"):
        if field in data:
            data[field] = _to_iso(data.get(field))
    return data


def _default_start_date(days: int) -> str:
    return (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")


def _validate_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return value
    except ValueError as exc:
        raise HTTPException(400, "Invalid date format (YYYY-MM-DD)") from exc


def _apply_filters(
    query: firestore.Query,
    status: Optional[str],
) -> firestore.Query:
    if status and status != "all":
        query = query.where("status", "==", status)
    return query


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _matches_filters(
    invoice: Dict[str, Any],
    store: Optional[str],
    invoice_number: Optional[str],
    start_date: Optional[str],
    end_date: Optional[str],
) -> bool:
    if store and _normalize_text(store) not in _normalize_text(invoice.get("store")):
        return False
    if invoice_number and (invoice.get("invoiceNumber") or "").lower().find(invoice_number.lower()) == -1:
        return False
    invoice_date = invoice.get("date")
    if invoice_date:
        if start_date and invoice_date < start_date:
            return False
        if end_date and invoice_date > end_date:
            return False
    elif start_date or end_date:
        return False
    return True


def _is_admin(user_data: Dict[str, Any]) -> bool:
    role = (user_data.get("profile") or {}).get("role")
    return role in {"owner", "admin"}


@router.get("/pos")
@rate_limit_history
async def list_pos_invoices(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    status: Optional[str] = Query(default="active", description="active|archived|all"),
    store: Optional[str] = Query(default=None),
    invoiceNumber: Optional[str] = Query(default=None),
    startDate: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    endDate: Optional[str] = Query(default=None, description="YYYY-MM-DD"),
    limit: int = Query(default=50, ge=1, le=200),
    cursor: Optional[str] = Query(default=None, description="Last document ID"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await require_route_access(route, decoded_token, db)

    if status and status not in STATUS_VALUES and status != "all":
        raise HTTPException(400, "Invalid status")

    start_date = _validate_date(startDate) or _default_start_date(90)
    end_date = _validate_date(endDate)

    invoices_ref = db.collection("routes").document(route).collection("pos_invoices")
    query = invoices_ref.order_by("createdAt", direction=firestore.Query.DESCENDING)
    query = _apply_filters(query, status)

    if cursor:
        cursor_doc = invoices_ref.document(cursor).get()
        if cursor_doc.exists:
            query = query.start_after(cursor_doc)

    docs = list(query.limit(limit).stream())
    items = []
    for doc in docs:
        data = _normalize_invoice(doc)
        if _matches_filters(data, store, invoiceNumber, start_date, end_date):
            items.append(data)
    next_cursor = docs[-1].id if len(docs) == limit else None

    return {
        "routeNumber": route,
        "items": items,
        "nextCursor": next_cursor,
    }


@router.get("/pos/{invoice_id}")
@rate_limit_history
async def get_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    invoices_group = db.collection_group("pos_invoices")
    doc = next(iter(invoices_group.where("id", "==", invoice_id).limit(1).stream()), None)
    if not doc:
        raise HTTPException(404, "Invoice not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)
    return _normalize_invoice(doc)


@router.put("/pos/{invoice_id}/archive")
@rate_limit_write
async def archive_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    invoices_group = db.collection_group("pos_invoices")
    doc = next(iter(invoices_group.where("id", "==", invoice_id).limit(1).stream()), None)
    if not doc:
        raise HTTPException(404, "Invoice not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    updates = {
        "status": "archived",
        "archivedAt": now_ms,
        "archivedBy": decoded_token.get("uid"),
    }
    doc.reference.update(updates)
    updated_doc = doc.reference.get()
    return {"ok": True, "invoice": _normalize_invoice(updated_doc)}


@router.delete("/pos/{invoice_id}")
@rate_limit_write
async def delete_pos_invoice(
    request: Request,
    invoice_id: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    invoices_group = db.collection_group("pos_invoices")
    doc = next(iter(invoices_group.where("id", "==", invoice_id).limit(1).stream()), None)
    if not doc:
        raise HTTPException(404, "Invoice not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    user_data = await require_route_access(route_number, decoded_token, db)

    if not _is_admin(user_data):
        raise HTTPException(403, "Admin access required")

    doc.reference.delete()
    return {"ok": True, "deletedId": invoice_id}
