"""Health check router - system status and sync info.

Endpoints:
    GET /api/health - Overall health status
    GET /api/health/database - PostgreSQL sync status
    GET /api/health/firebase - Firebase/Firestore connectivity
    POST /api/health/sync - Best-effort manual sync (Firebase -> PostgreSQL)
"""

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pathlib import Path

from google.cloud import firestore

from ..dependencies import (
    get_firestore,
    get_pg_connection,
    return_pg_connection,
    verify_firebase_token,
    require_route_access,
)
from ..middleware.rate_limit import rate_limit_write

router = APIRouter()
logger = logging.getLogger(__name__)

# Only expose detailed errors in debug mode
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"


@router.get("/health")
async def health_check() -> dict:
    """Basic health check - no auth required.
    
    Returns overall API status.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }


@router.get("/health/database")
async def database_health() -> dict:
    """PostgreSQL database health check with sync status.
    
    Returns:
    - healthy: Connected and has recent data
    - degraded: Connected but stale data
    - unhealthy: Connection failed
    """
    now = datetime.utcnow()
    
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        # Check order count
        cur.execute("SELECT COUNT(*) FROM orders_historical")
        order_count = cur.fetchone()[0]
        
        # Check most recent sync
        cur.execute("SELECT MAX(synced_at) FROM orders_historical")
        last_sync_result = cur.fetchone()
        last_sync = last_sync_result[0] if last_sync_result else None
        
        # Check routes synced count
        cur.execute("SELECT COUNT(*) FROM routes_synced")
        routes_count = cur.fetchone()[0]
        
        cur.close()
        return_pg_connection(conn)
        conn = None  # Mark as returned
        
        # Calculate sync age
        sync_age_minutes: Optional[int] = None
        if last_sync:
            # Handle timezone-aware datetime
            if last_sync.tzinfo is not None:
                from datetime import timezone
                now_tz = datetime.now(timezone.utc)
                sync_age_minutes = int((now_tz - last_sync).total_seconds() / 60)
            else:
                sync_age_minutes = int((now - last_sync).total_seconds() / 60)
        
        # Determine status based on data freshness
        # Green: < 12 hours, Yellow: 12-24 hours, Red: > 24 hours or no data
        if sync_age_minutes is None:
            status = "degraded" if order_count > 0 else "unhealthy"
        elif sync_age_minutes <= 720:  # 12 hours
            status = "healthy"
        elif sync_age_minutes <= 1440:  # 24 hours
            status = "degraded"
        else:
            status = "unhealthy"
        
        return {
            "status": status,
            "database": "postgresql",
            "lastSync": last_sync.isoformat() if last_sync else None,
            "syncAgeMinutes": sync_age_minutes,
            "orderCount": order_count,
            "routesCount": routes_count,
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        if conn is not None:
            return_pg_connection(conn)
        return {
            "status": "unhealthy",
            "database": "postgresql",
            "error": str(e) if DEBUG_MODE else "Database connection failed",
            "timestamp": now.isoformat()
        }


@router.get("/health/firebase")
async def firebase_health(
    db = Depends(get_firestore)
) -> dict:
    """Firebase/Firestore health check.
    
    Performs a simple read to verify connectivity.
    """
    try:
        # Try to read a known document
        doc = db.collection('_health').document('ping').get()
        # Even if doc doesn't exist, connection worked
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.post("/health/sync")
@rate_limit_write
async def trigger_sync(
    request: Request,
    route: Optional[str] = Query(default=None, pattern=r"^\d{1,10}$", description="Route number (optional)"),
    days: int = Query(default=60, ge=1, le=365, description="How far back to scan finalized orders"),
    limit: int = Query(default=300, ge=1, le=1000, description="Max finalized orders to scan"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Best-effort manual sync for support/debug.

    This exists primarily to back the web-portal "Sync Now" button.
    It scans recent finalized orders in Firestore and upserts them into PostgreSQL.
    """
    uid = decoded_token.get("uid")

    # If route not provided, derive from user doc (currentRoute, then primary routeNumber).
    if not route:
        user_doc = db.collection("users").document(uid).get()
        if not user_doc.exists:
            raise HTTPException(403, "Access denied")
        profile = (user_doc.to_dict() or {}).get("profile", {})
        derived = profile.get("currentRoute") or profile.get("routeNumber")
        derived = str(derived) if derived is not None else ""
        if not derived.isdigit():
            raise HTTPException(400, "No route selected")
        route = derived

    await require_route_access(route, decoded_token, db)

    cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()

    # Fetch a capped set of finalized orders and filter by delivery date in-process.
    # We avoid adding order_by clauses here to minimize index requirements.
    orders_ref = db.collection("routes").document(route).collection("orders")
    snaps = list(orders_ref.where("status", "==", "finalized").limit(limit).stream())

    order_ids: List[str] = []
    for s in snaps:
        data = s.to_dict() or {}
        delivery_date = (
            str(data.get("expectedDeliveryDate") or data.get("deliveryDate") or "")
        )
        if delivery_date and delivery_date < cutoff:
            continue
        order_ids.append(s.id)

    if not order_ids:
        return {"status": "ok", "message": f"No recent finalized orders found for route {route}."}

    # Run syncs (idempotent upserts).
    from db_manager_pg import handle_sync_order  # loaded via dependencies.py sys.path injection

    conn = None
    ok = 0
    failed = 0
    try:
        conn = get_pg_connection()
        for order_id in order_ids:
            result = handle_sync_order(conn, db, {"orderId": order_id, "routeNumber": route})
            if result.get("error"):
                failed += 1
            else:
                ok += 1
    finally:
        if conn is not None:
            return_pg_connection(conn)

    status = "ok" if failed == 0 else "partial"
    return {
        "status": status,
        "message": f"Synced {ok}/{len(order_ids)} orders for route {route} (failed: {failed}).",
    }


@router.get("/health/services")
async def services_health() -> dict:
    """Order-forecast service heartbeat status (from supervisor)."""
    # Allowed directories for status files (path traversal protection)
    ALLOWED_STATUS_DIRS = [
        Path("/app/logs"),
        Path("/srv/routespark/logs"),
    ]

    status_file = os.environ.get(
        "ORDER_FORECAST_STATUS_FILE",
        "/app/logs/order-forecast/service_status.json",
    )
    path = Path(status_file).resolve()

    # Validate path is within allowed directories
    if not any(str(path).startswith(str(allowed.resolve())) for allowed in ALLOWED_STATUS_DIRS if allowed.exists()):
        logger.warning(f"Blocked path traversal attempt: {status_file}")
        return {
            "status": "error",
            "error": "invalid_path",
            "timestamp": datetime.utcnow().isoformat(),
        }

    if not path.exists():
        return {
            "status": "unavailable",
            "error": "status_file_missing",
            "timestamp": datetime.utcnow().isoformat(),
        }
    try:
        data = json.loads(path.read_text())
        ts = data.get("timestamp")
        age_seconds = None
        if ts:
            try:
                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age_seconds = int((datetime.utcnow() - ts_dt.replace(tzinfo=None)).total_seconds())
            except Exception:
                age_seconds = None
        stale_after = int(os.environ.get("SERVICE_STATUS_STALE_SECONDS", "180"))
        stale = age_seconds is not None and age_seconds > stale_after
        return {
            "status": "stale" if stale else "healthy",
            "timestamp": ts,
            "ageSeconds": age_seconds,
            "services": data.get("services", []),
        }
    except Exception as e:
        logger.error(f"Services health check failed: {e}")
        return {
            "status": "unavailable",
            "error": str(e) if DEBUG_MODE else "Failed to read status",
            "timestamp": datetime.utcnow().isoformat(),
        }
