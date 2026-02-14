"""Schedule router - get user's order schedule and calculate next delivery date."""

from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_firestore,
)


router = APIRouter()


class OrderCycle(BaseModel):
    orderDay: int  # 1=Monday, 7=Sunday
    loadDay: int
    deliveryDay: int


class ScheduleResponse(BaseModel):
    cycles: List[OrderCycle]
    todayIsOrderDay: bool
    nextDelivery: Optional[Dict[str, Any]] = None  # {date, scheduleKey, cycleName}


DAY_NAMES = ['', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


def get_day_name(day_number: int) -> str:
    """Convert day number (1=Monday) to name."""
    return DAY_NAMES[day_number] if 0 < day_number <= 7 else 'Unknown'


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
        order_day = cycle.get('orderDay')
        if order_day == today_dow:
            delivery_day = cycle.get('deliveryDay')
            
            # Calculate days until delivery
            days_until = delivery_day - today_dow
            if days_until <= 0:
                days_until += 7
            
            delivery_date = today + timedelta(days=days_until)
            
            return {
                'deliveryDate': delivery_date.isoformat(),
                'scheduleKey': get_day_name(order_day).lower(),
                'cycleName': f'{get_day_name(order_day)} → {get_day_name(delivery_day)}'
            }
    
    return None


def get_order_cycles_from_firestore(db: firestore.Client, route_number: str) -> List[Dict]:
    """Get user's order cycles from Firestore.
    
    Path: users/{uid}/userSettings.notifications.scheduling.orderCycles
    """
    from google.cloud.firestore_v1.base_query import FieldFilter
    
    try:
        user_id = None
        
        # Try to find the user via route document
        route_doc = db.collection('routes').document(route_number).get()
        if route_doc.exists:
            route_data = route_doc.to_dict() or {}
            user_id = route_data.get('userId')
        
        # If no route doc, find the OWNER user with this route
        if not user_id:
            users_ref = db.collection('users')
            query = users_ref.where(
                filter=FieldFilter('profile.routeNumber', '==', route_number)
            ).where(
                filter=FieldFilter('profile.role', '==', 'owner')
            ).limit(1)
            docs = list(query.stream())
            if docs:
                user_id = docs[0].id
        
        # Try currentRoute field
        if not user_id:
            query = db.collection('users').where(
                filter=FieldFilter('profile.currentRoute', '==', route_number)
            ).where(
                filter=FieldFilter('profile.role', '==', 'owner')
            ).limit(1)
            docs = list(query.stream())
            if docs:
                user_id = docs[0].id
        
        if user_id:
            user_doc = db.collection('users').document(user_id).get()
            if user_doc.exists:
                user_data = user_doc.to_dict() or {}
                
                # Path: userSettings.notifications.scheduling.orderCycles
                user_settings = user_data.get('userSettings', {})
                notifications = user_settings.get('notifications', {})
                scheduling = notifications.get('scheduling', {})
                cycles = scheduling.get('orderCycles', [])
                
                if cycles:
                    return cycles
                
                # Fallback: check old path settings.orderCycles
                settings = user_data.get('settings', {})
                return settings.get('orderCycles', [])
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
    cycles = get_order_cycles_from_firestore(db, route)
    
    # Calculate if today is an order day and get next delivery
    today = date.today()
    today_dow = today.weekday() + 1  # 1=Monday
    
    today_is_order_day = any(c.get('orderDay') == today_dow for c in cycles)
    next_delivery = calculate_next_delivery(cycles, today)
    
    return ScheduleResponse(
        cycles=[OrderCycle(**c) for c in cycles],
        todayIsOrderDay=today_is_order_day,
        nextDelivery=next_delivery
    )


@router.get(
    "/schedule/delivery-date",
    responses={
        401: {"description": "Unauthorized"},
        403: {"description": "Access denied to route"},
    },
)
async def get_schedule_key_for_date(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    deliveryDate: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$", description="Delivery date (YYYY-MM-DD)"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, str]:
    """Get the schedule key for a specific delivery date.
    
    Maps a delivery date back to the order day (e.g., Thursday delivery → 'monday').
    """
    await require_route_access(route, decoded_token, db)
    
    cycles = get_order_cycles_from_firestore(db, route)
    
    # Parse delivery date
    try:
        delivery_date = datetime.strptime(deliveryDate, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")
    
    delivery_dow = delivery_date.weekday() + 1  # 1=Monday
    
    # Find matching cycle
    for cycle in cycles:
        if cycle.get('deliveryDay') == delivery_dow:
            order_day = cycle.get('orderDay', delivery_dow)
            return {
                'scheduleKey': get_day_name(order_day).lower(),
                'deliveryDate': deliveryDate,
                'orderDayName': get_day_name(order_day),
                'deliveryDayName': get_day_name(delivery_dow)
            }
    
    # No matching cycle, use delivery day as schedule key
    return {
        'scheduleKey': get_day_name(delivery_dow).lower(),
        'deliveryDate': deliveryDate,
        'orderDayName': get_day_name(delivery_dow),
        'deliveryDayName': get_day_name(delivery_dow)
    }
