"""Credits router - list, detail, status updates, archive."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
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

STATUS_VALUES = {"pending", "downloaded", "submitted"}


def _to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.utcfromtimestamp(value / 1000).isoformat() + "Z"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _get_credit_doc_by_route(
    db: firestore.Client,
    route: str,
    credit_id: str,
) -> Optional[firestore.DocumentSnapshot]:
    doc_ref = db.collection("routes").document(route).collection("credits").document(credit_id)
    doc = doc_ref.get()
    return doc if doc.exists else None


def _normalize_credit(doc: firestore.DocumentSnapshot) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    data["id"] = data.get("id") or doc.id
    for field in ("createdAt", "updatedAt", "downloadedAt", "submittedAt", "archivedAt"):
        if field in data:
            data[field] = _to_iso(data.get(field))
    return data


def _ms_from_date(date_str: str) -> int:
    try:
        return int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)
    except ValueError as exc:
        raise HTTPException(400, "Invalid date format (YYYY-MM-DD)") from exc


def _default_start_date(days: int) -> int:
    return int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)


def _apply_common_filters(
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
    credit: Dict[str, Any],
    store: Optional[str],
    item_number: Optional[str],
    team_member: Optional[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
) -> bool:
    if store and _normalize_text(store) not in _normalize_text(credit.get("store")):
        return False
    if item_number and (credit.get("itemNumber") or "").lower().find(item_number.lower()) == -1:
        return False
    if team_member and credit.get("teamMemberUid") != team_member:
        return False
    created_at = credit.get("createdAt")
    created_ms: Optional[int] = None
    if isinstance(created_at, str):
        try:
            created_ms = int(datetime.fromisoformat(created_at.replace("Z", "")).timestamp() * 1000)
        except Exception:
            created_ms = None
    elif isinstance(created_at, (int, float)):
        created_ms = int(created_at)
    if start_ms is not None and (created_ms is None or created_ms < start_ms):
        return False
    if end_ms is not None and (created_ms is None or created_ms > end_ms):
        return False
    return True


@router.get("/credits")
@rate_limit_history
async def list_credits(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    status: Optional[str] = Query(default=None, description="pending|downloaded|submitted|all"),
    store: Optional[str] = Query(default=None),
    itemNumber: Optional[str] = Query(default=None),
    teamMember: Optional[str] = Query(default=None),
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

    start_ms = _ms_from_date(startDate) if startDate else _default_start_date(30)
    end_ms = _ms_from_date(endDate) if endDate else None

    credits_ref = db.collection("routes").document(route).collection("credits")
    base_query = credits_ref.order_by("createdAt", direction=firestore.Query.DESCENDING)
    base_query = _apply_common_filters(base_query, status)

    if cursor:
        cursor_doc = credits_ref.document(cursor).get()
        if cursor_doc.exists:
            base_query = base_query.start_after(cursor_doc)

    docs = list(base_query.limit(limit).stream())
    items = []
    for doc in docs:
        data = _normalize_credit(doc)
        if data.get("isArchived"):
            continue
        if _matches_filters(data, store, itemNumber, teamMember, start_ms, end_ms):
            items.append(data)

    next_cursor = docs[-1].id if len(docs) == limit else None

    counts: Dict[str, int] = {}
    for st in STATUS_VALUES:
        count_query = credits_ref.order_by("createdAt", direction=firestore.Query.DESCENDING)
        count_query = _apply_common_filters(count_query, st)
        count = 0
        for doc in count_query.stream():
            data = _normalize_credit(doc)
            if data.get("isArchived"):
                continue
            if _matches_filters(data, store, itemNumber, teamMember, start_ms, end_ms):
                count += 1
        counts[st] = count

    return {
        "routeNumber": route,
        "items": items,
        "counts": counts,
        "nextCursor": next_cursor,
    }


@router.get("/credits/{credit_id}")
@rate_limit_history
async def get_credit(
    request: Request,
    credit_id: str,
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    if route:
        await require_route_access(route, decoded_token, db)
        doc = _get_credit_doc_by_route(db, route, credit_id)
        if not doc:
            raise HTTPException(404, "Credit not found")
        return _normalize_credit(doc)

    credits_group = db.collection_group("credits")
    doc = next(iter(credits_group.where("id", "==", credit_id).limit(1).stream()), None)
    if not doc:
        raise HTTPException(404, "Credit not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)

    return _normalize_credit(doc)


@router.put("/credits/{credit_id}/status")
@rate_limit_write
async def update_credit_status(
    request: Request,
    credit_id: str,
    status: str = Query(..., description="pending|downloaded|submitted"),
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    if status not in STATUS_VALUES:
        raise HTTPException(400, "Invalid status")

    doc = None
    if route:
        await require_route_access(route, decoded_token, db)
        doc = _get_credit_doc_by_route(db, route, credit_id)
        if not doc:
            raise HTTPException(404, "Credit not found")
    else:
        credits_group = db.collection_group("credits")
        doc = next(iter(credits_group.where("id", "==", credit_id).limit(1).stream()), None)
        if not doc:
            raise HTTPException(404, "Credit not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or route or "")
    user_data = await require_route_access(route_number, decoded_token, db)

    current_status = data.get("status")
    allowed_next = {
        "pending": {"pending", "downloaded"},
        "downloaded": {"downloaded", "submitted"},
        "submitted": {"submitted"},
    }
    if current_status in allowed_next and status not in allowed_next[current_status]:
        raise HTTPException(400, f"Invalid status transition: {current_status} -> {status}")

    updates: Dict[str, Any] = {
        "status": status,
        "updatedAt": int(datetime.utcnow().timestamp() * 1000),
    }

    uid = decoded_token.get("uid")
    if status == "downloaded":
        updates["downloadedAt"] = updates["updatedAt"]
        updates["downloadedBy"] = uid
    elif status == "submitted":
        updates["submittedAt"] = updates["updatedAt"]
        updates["submittedBy"] = uid

    doc.reference.update(updates)
    updated_doc = doc.reference.get()
    return {"ok": True, "credit": _normalize_credit(updated_doc)}


@router.put("/credits/{credit_id}/archive")
@rate_limit_write
async def archive_credit(
    request: Request,
    credit_id: str,
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    if route:
        await require_route_access(route, decoded_token, db)
        doc = _get_credit_doc_by_route(db, route, credit_id)
        if not doc:
            raise HTTPException(404, "Credit not found")
    else:
        credits_group = db.collection_group("credits")
        doc = next(iter(credits_group.where("id", "==", credit_id).limit(1).stream()), None)
        if not doc:
            raise HTTPException(404, "Credit not found")

    data = doc.to_dict() or {}
    route_number = str(data.get("routeNumber") or "")
    await require_route_access(route_number, decoded_token, db)

    now_ms = int(datetime.utcnow().timestamp() * 1000)
    updates = {
        "isArchived": True,
        "archivedAt": now_ms,
        "archivedBy": decoded_token.get("uid"),
        "updatedAt": now_ms,
    }
    doc.reference.update(updates)
    updated_doc = doc.reference.get()
    return {"ok": True, "credit": _normalize_credit(updated_doc)}
