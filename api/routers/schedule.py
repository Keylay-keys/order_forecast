"""Schedule router - get user's order schedule and calculate next delivery date."""

from __future__ import annotations

from datetime import datetime, date
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
)
from ..models import ScheduledOrder
from ..order_adjustment_schedule_v2_release import stamp_write_allowed_for_headers

from schedule_cycle import (
    get_cycle_dates,
    normalize_order_cycle,
    resolve_order_schedule_for_delivery_date,
)


router = APIRouter()


class OrderCycle(BaseModel):
    orderDay: int  # 1=Monday, 7=Sunday
    loadDay: int
    deliveryDay: int
    loadOffsetDays: Optional[int] = None
    deliveryOffsetDays: Optional[int] = None
    needsScheduleReview: Optional[bool] = None
    scheduleVersion: Optional[int] = None


class ScheduleResponse(BaseModel):
    cycles: List[OrderCycle]
    todayIsOrderDay: bool
    nextDelivery: Optional[Dict[str, Any]] = None  # {date, scheduleKey, cycleName}


class ScheduleKeyResponse(BaseModel):
    scheduleKey: Optional[str] = None
    deliveryDate: str
    orderDayName: str
    deliveryDayName: str
    orderDate: Optional[str] = None
    loadDate: Optional[str] = None
    timezone: Optional[str] = None
    matched: bool
    reason: Optional[str] = None
    scheduledOrder: Optional[ScheduledOrder] = None


DAY_NAMES = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def get_day_name(day_number: int) -> str:
    """Convert day number (1=Monday) to name."""
    return DAY_NAMES[day_number] if 0 < day_number <= 7 else 'Unknown'


def _validated_timezone_name(value: Optional[str]) -> Optional[str]:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        ZoneInfo(normalized)
    except (ZoneInfoNotFoundError, ValueError):
        return None
    return normalized


def _document_owner_values(data: Dict[str, Any]) -> set[str]:
    return {
        value
        for key in ("ownerUid", "userId", "userID")
        if (value := str(data.get(key) or "").strip())
    }


def resolve_schedule_owner_uid(
    db: firestore.Client,
    route_number: str,
) -> tuple[Optional[str], str]:
    """Resolve one owner across every route-authority document, or fail closed."""
    owner_values: set[str] = set()
    authority_document_exists = False
    for collection_name in ("routes", "routeEntitlements", "routeNumbers"):
        snap = db.collection(collection_name).document(route_number).get()
        if snap.exists:
            authority_document_exists = True
            owner_values.update(_document_owner_values(snap.to_dict() or {}))
    if len(owner_values) > 1:
        return None, "owner_conflict"
    if len(owner_values) == 1:
        return next(iter(owner_values)), "ready"
    if authority_document_exists:
        return None, "missing_owner"

    # Legacy fallback is accepted only when both owner-profile queries collapse
    # to exactly one user. Ambiguous profiles never become schedule authority.
    candidates: set[str] = set()
    for field in ("profile.routeNumber", "profile.currentRoute"):
        docs = list(
            db.collection("users")
            .where(filter=FieldFilter(field, "==", route_number))
            .where(filter=FieldFilter("profile.role", "==", "owner"))
            .limit(2)
            .stream()
        )
        candidates.update(doc.id for doc in docs)
    if len(candidates) > 1:
        return None, "owner_conflict"
    if len(candidates) == 1:
        return next(iter(candidates)), "ready"
    return None, "missing_owner"


def get_schedule_timezone_from_firestore(
    db: firestore.Client,
    route_number: str,
) -> tuple[Optional[str], str]:
    context = get_order_schedule_context_from_firestore(db, route_number)
    return context["timezone"], context["status"]


def get_order_schedule_context_from_firestore(
    db: firestore.Client,
    route_number: str,
) -> Dict[str, Any]:
    """Load one owner/user snapshot for both cycles and stamp timezone."""
    owner_uid, owner_status = resolve_schedule_owner_uid(db, route_number)
    if not owner_uid:
        return {
            "status": owner_status,
            "ownerUid": None,
            "cycles": [],
            "timezone": None,
        }
    user_doc = db.collection("users").document(owner_uid).get()
    if not user_doc.exists:
        return {
            "status": "missing_owner_user",
            "ownerUid": owner_uid,
            "cycles": [],
            "timezone": None,
        }
    user_data = user_doc.to_dict() or {}
    user_settings = user_data.get("userSettings") if isinstance(user_data.get("userSettings"), dict) else {}
    notifications = user_settings.get("notifications") if isinstance(user_settings.get("notifications"), dict) else {}
    scheduling = notifications.get("scheduling") if isinstance(notifications.get("scheduling"), dict) else {}
    raw_cycles = scheduling.get("orderCycles")
    if not isinstance(raw_cycles, list) or not raw_cycles:
        legacy_settings = user_data.get("settings") if isinstance(user_data.get("settings"), dict) else {}
        raw_cycles = legacy_settings.get("orderCycles")
    cycles = [normalize_order_cycle(cycle) for cycle in raw_cycles] if isinstance(raw_cycles, list) else []

    profile = user_data.get("profile") if isinstance(user_data.get("profile"), dict) else {}
    raw_timezone = str(profile.get("timezone") or "").strip()
    timezone_name = _validated_timezone_name(raw_timezone)
    status = "ready" if timezone_name else ("missing_timezone" if not raw_timezone else "invalid_timezone")
    return {
        "status": status,
        "ownerUid": owner_uid,
        "cycles": cycles,
        "timezone": timezone_name,
    }


def calculate_next_delivery(cycles: List[Dict], today: date) -> Optional[Dict[str, Any]]:
    """Calculate the next delivery date if today is an order day.
    
    Args:
        cycles: List of order cycles with orderDay, loadDay, deliveryDay
        today: Today's date
        
    Returns:
        Dict with deliveryDate, scheduleKey, cycleName or None if no match
    """
    if not cycles:
        return None
    
    # Get today's day of week (1=Monday, 7=Sunday)
    # Python: Monday=0, so we add 1
    today_dow = today.weekday() + 1  # Convert 0-6 to 1-7
    
    for cycle in cycles:
        normalized_cycle = normalize_order_cycle(cycle)
        order_day = normalized_cycle.get('orderDay')
        if order_day == today_dow:
            cycle_dates = get_cycle_dates(normalized_cycle, today)
            delivery_day = normalized_cycle.get('deliveryDay')
            
            return {
                'deliveryDate': cycle_dates['deliveryDateString'],
                'scheduleKey': get_day_name(order_day).lower(),
                'cycleName': f'{get_day_name(order_day)} → {get_day_name(delivery_day)}'
            }
    
    return None


def get_order_cycles_from_firestore(db: firestore.Client, route_number: str) -> List[Dict]:
    """Get user's order cycles from Firestore.
    
    Path: users/{uid}/userSettings.notifications.scheduling.orderCycles
    """
    try:
        context = get_order_schedule_context_from_firestore(db, route_number)
        if context["status"] in {"missing_owner", "owner_conflict", "missing_owner_user"}:
            print(f"[schedule] Owner authority unresolved for {route_number}: {context['status']}")
        return list(context["cycles"])
    except Exception as e:
        print(f"[schedule] Warning: Could not get order cycles from Firebase: {e}")
    
    return []


@router.get(
    "/schedule",
    response_model=ScheduleResponse,
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Access denied to route"},
    },
)
async def get_schedule(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> ScheduleResponse:
    """Get order schedule for a route.
    
    Returns the user's order cycles and calculates if today is an order day.
    If today is an order day, returns the next delivery date.
    """
    await require_route_access(route, decoded_token, db)
    
    # Get order cycles
    schedule_context = get_order_schedule_context_from_firestore(db, route)
    cycles = list(schedule_context["cycles"])
    
    # Calculate if today is an order day and get next delivery
    today = date.today()
    today_dow = today.weekday() + 1  # 1=Monday
    
    today_is_order_day = any(c.get('orderDay') == today_dow for c in cycles)
    next_delivery = calculate_next_delivery(cycles, today)
    
    return ScheduleResponse(
        cycles=[OrderCycle(**normalize_order_cycle(c)) for c in cycles],
        todayIsOrderDay=today_is_order_day,
        nextDelivery=next_delivery
    )


@router.get(
    "/schedule/delivery-date",
    response_model=ScheduleKeyResponse,
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Access denied to route"},
    },
)
async def get_schedule_key_for_date(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    deliveryDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="Delivery date (YYYY-MM-DD)"),
    scheduleKey: Optional[str] = Query(
        default=None,
        pattern=r"^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$",
        description="Optional selected Order Day used to disambiguate cycles",
    ),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> ScheduleKeyResponse:
    """Get the schedule key for a specific delivery date.
    
    Maps a delivery date back to the order day (e.g., Thursday delivery → 'monday').
    """
    await require_route_access(route, decoded_token, db)
    
    schedule_context = get_order_schedule_context_from_firestore(db, route)
    cycles = list(schedule_context["cycles"])
    
    # Parse delivery date
    try:
        delivery_date = datetime.strptime(deliveryDate, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")
    
    delivery_dow = delivery_date.weekday() + 1  # 1=Monday
    resolution = resolve_order_schedule_for_delivery_date(deliveryDate, cycles, scheduleKey)
    timezone_name = schedule_context["timezone"]
    timezone_status = schedule_context["status"]
    stamp_allowed = stamp_write_allowed_for_headers(getattr(request, "headers", {}))
    if resolution['status'] == 'resolved' and timezone_name:
        cycle = resolution['cycle']
        order_day = cycle['orderDay']
        scheduled_order = ScheduledOrder(
            version=2,
            date=resolution['orderDate'],
            timezone=timezone_name,
            scheduleKey=resolution['scheduleKey'],
            orderDay=order_day,
            loadOffsetDays=cycle['loadOffsetDays'],
            deliveryOffsetDays=cycle['deliveryOffsetDays'],
            provenance='cycle',
        )
        return ScheduleKeyResponse(
            scheduleKey=resolution['scheduleKey'],
            deliveryDate=deliveryDate,
            orderDayName=get_day_name(order_day),
            deliveryDayName=get_day_name(delivery_dow),
            orderDate=resolution['orderDate'],
            loadDate=resolution['loadDate'],
            timezone=timezone_name,
            matched=True,
            scheduledOrder=scheduled_order if stamp_allowed else None,
        )

    reason = resolution['reason'] if resolution['status'] == 'unresolved' else timezone_status
    return ScheduleKeyResponse(
        deliveryDate=deliveryDate,
        orderDayName=get_day_name(delivery_dow),
        deliveryDayName=get_day_name(delivery_dow),
        timezone=timezone_name,
        matched=False,
        reason=reason,
    )
