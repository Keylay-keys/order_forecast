"""Route transfers router - audit queries for cross-route transfers.

Endpoints:
  GET /api/transfers - list transfer entries for a route group (PostgreSQL-backed)
  GET /api/transfers/ledger - real-time ledger with reservations (Firestore-backed)
  POST /api/transfers/reserve - reserve units from a transfer (inbound)
  POST /api/transfers/create - create/upsert a transfer (outbound)

Notes:
- Phase 1 only: transfers are an accounting layer and do not mutate order totals.
- Data source for /transfers is PostgreSQL table route_transfers (synced by route_transfer_sync_listener.py).
- Ledger/reserve/create use Firestore directly for real-time reservation tracking.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Depends, Query, Request, HTTPException
from pydantic import BaseModel, Field
from google.cloud import firestore

from ..dependencies import (
    verify_firebase_token,
    require_route_access,
    get_pg_connection,
    return_pg_connection,
    get_firestore,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write

router = APIRouter()


def _row_to_dict(row: Any, columns: List[str]) -> Dict[str, Any]:
    if isinstance(row, dict):
        return row
    if isinstance(row, (list, tuple)):
        return {columns[i]: row[i] for i in range(len(columns))}
    return {"value": row}


@router.get("/transfers")
@rate_limit_history
async def list_transfers(
    request: Request,
    route_group_id: str = Query(..., alias="routeGroupId", pattern=r"^\d{1,10}$", description="Master route number (routeGroupId)"),
    route: Optional[str] = Query(None, pattern=r"^\d{1,10}$", description="Optional: filter by a specific route number (from/to)"),
    days: int = Query(default=30, ge=1, le=180, description="Days of history"),
    limit: int = Query(default=200, ge=1, le=500, description="Max rows"),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """List recent route transfers for a route group.

    Security:
    - Requires Firebase token
    - Requires route access to the master route (routeGroupId)
    """
    await require_route_access(route_group_id, decoded_token, db)

    cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()

    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()

        if route:
            cur.execute(
                """
                SELECT
                    fs_doc_path,
                    route_group_id,
                    transfer_id,
                    transfer_date,
                    purchase_route_number,
                    from_route_number,
                    to_route_number,
                    sap,
                    units,
                    case_pack,
                    status,
                    reason,
                    source_order_id,
                    created_by,
                    created_at,
                    updated_at,
                    synced_at
                FROM route_transfers
                WHERE route_group_id = %s
                  AND transfer_date >= %s
                  AND (from_route_number = %s OR to_route_number = %s)
                ORDER BY transfer_date DESC, created_at DESC NULLS LAST
                LIMIT %s
                """,
                [route_group_id, cutoff, route, route, limit],
            )
        else:
            cur.execute(
                """
                SELECT
                    fs_doc_path,
                    route_group_id,
                    transfer_id,
                    transfer_date,
                    purchase_route_number,
                    from_route_number,
                    to_route_number,
                    sap,
                    units,
                    case_pack,
                    status,
                    reason,
                    source_order_id,
                    created_by,
                    created_at,
                    updated_at,
                    synced_at
                FROM route_transfers
                WHERE route_group_id = %s
                  AND transfer_date >= %s
                ORDER BY transfer_date DESC, created_at DESC NULLS LAST
                LIMIT %s
                """,
                [route_group_id, cutoff, limit],
            )

        rows = cur.fetchall()
        columns = [d[0] for d in cur.description] if cur.description else []
        cur.close()
        return_pg_connection(conn)
        conn = None

        out = []
        for r in rows:
            d = _row_to_dict(r, columns)
            # Convert datetimes to ISO strings for JSON.
            for k in ("created_at", "updated_at", "synced_at"):
                v = d.get(k)
                if hasattr(v, "isoformat"):
                    d[k] = v.isoformat()
            out.append(d)

        return {"ok": True, "transfers": out}
    finally:
        if conn is not None:
            return_pg_connection(conn)


# =============================================================================
# REQUEST MODELS (Pydantic)
# =============================================================================

class TransferReserveRequest(BaseModel):
    """Request to reserve units from a transfer."""
    routeGroupId: str = Field(..., pattern=r"^\d{1,10}$")
    transferKey: str
    consumerOrderId: str
    units: int = Field(..., ge=0)


class TransferCreateRequest(BaseModel):
    """Request to create/upsert a transfer."""
    routeGroupId: str = Field(..., pattern=r"^\d{1,10}$")
    transferKey: str
    fromRouteNumber: str = Field(..., pattern=r"^\d{1,10}$")
    toRouteNumber: str = Field(..., pattern=r"^\d{1,10}$")
    sap: str
    units: int = Field(..., ge=0)
    casePack: int = Field(default=1, ge=1)
    transferDate: Optional[str] = None
    status: Optional[str] = Field(default="planned", pattern="^(planned|committed|canceled)$")
    reason: Optional[str] = None
    sourceOrderId: Optional[str] = None


# =============================================================================
# NEW ENDPOINTS (Real-time Firestore)
# =============================================================================

@router.get("/transfers/ledger")
@rate_limit_history
async def get_transfer_ledger(
    request: Request,
    route_group_id: str = Query(..., alias="routeGroupId", pattern=r"^\d{1,10}$"),
    limit: int = Query(default=200, ge=1, le=500),
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """Read transfer ledger directly from Firestore (real-time, includes reservations).

    Returns transfers with:
    - reservedBy map (which orders have reserved units)
    - reservedTotal (sum of all reservations)
    - availableUnits (units - reservedTotal)
    """
    await require_route_access(route_group_id, decoded_token, db)

    try:
        snap = (
            db.collection("routeTransfers")
            .document(route_group_id)
            .collection("transfers")
            .order_by("createdAt", direction=firestore.Query.DESCENDING)
            .limit(limit)
            .get()
        )

        transfers = []
        for doc in snap:
            data = doc.to_dict()
            if not data:
                continue

            # Calculate reservation totals
            reserved_by = data.get("reservedBy", {})
            reserved_total = sum(v for v in reserved_by.values() if isinstance(v, (int, float)))
            available_units = max(0, data.get("units", 0) - reserved_total)

            # Convert timestamps to millis for consistency with mobile app
            created_at = data.get("createdAt")
            updated_at = data.get("updatedAt")
            if hasattr(created_at, "timestamp"):
                data["createdAt"] = int(created_at.timestamp() * 1000)
            if hasattr(updated_at, "timestamp"):
                data["updatedAt"] = int(updated_at.timestamp() * 1000)

            transfers.append({
                "id": doc.id,
                **data,
                "reservedBy": reserved_by,
                "reservedTotal": reserved_total,
                "availableUnits": available_units,
            })

        return {"ok": True, "transfers": transfers}
    except Exception as exc:
        raise HTTPException(500, f"Failed to read transfer ledger: {str(exc)}")


@router.post("/transfers/reserve")
@rate_limit_write
async def reserve_transfer(
    request: Request,
    payload: TransferReserveRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """Reserve units from a transfer for a consumer order (inbound consumption).

    Mirrors Firebase callable reserveRouteTransfer (route-transfers.ts:190-262).
    """
    await require_route_access(payload.routeGroupId, decoded_token, db)

    transfer_ref = (
        db.collection("routeTransfers")
        .document(payload.routeGroupId)
        .collection("transfers")
        .document(payload.transferKey)
    )

    @firestore.transactional
    def reserve_in_transaction(transaction, ref):
        snap = ref.get(transaction=transaction)
        if not snap.exists:
            raise HTTPException(404, "Transfer not found")

        data = snap.to_dict()
        transfer_units = data.get("units", 0)
        status = data.get("status")
        if status == "canceled":
            raise HTTPException(409, "Transfer canceled")

        reserved_by = data.get("reservedBy", {})
        current = reserved_by.get(payload.consumerOrderId, 0)
        reserved_total = sum(v for v in reserved_by.values() if isinstance(v, (int, float)))

        max_allowed = transfer_units - (reserved_total - current)
        if payload.units > max_allowed:
            raise HTTPException(409, f"Insufficient available units (max: {max_allowed})")

        if payload.units <= 0:
            # Remove reservation
            transaction.update(ref, {
                f"reservedBy.{payload.consumerOrderId}": firestore.DELETE_FIELD,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            })
        else:
            # Set/update reservation
            transaction.update(ref, {
                f"reservedBy.{payload.consumerOrderId}": payload.units,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            })

        next_reserved = reserved_total - current + payload.units
        return {
            "reservedTotal": next_reserved,
            "availableUnits": max(0, transfer_units - next_reserved),
        }

    transaction = db.transaction()
    try:
        result = reserve_in_transaction(transaction, transfer_ref)
        return {"ok": True, **result}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Reservation failed: {str(exc)}")


@router.post("/transfers/create")
@rate_limit_write
async def create_transfer(
    request: Request,
    payload: TransferCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore),
):
    """Create or update a transfer entry (outbound route splitting).

    Mirrors Firebase callable createRouteTransfer (route-transfers.ts:94-188).
    """
    await require_route_access(payload.routeGroupId, decoded_token, db)

    # Validate both routes are owned by caller
    uid = decoded_token["uid"]
    user_ref = db.collection("users").document(uid)
    user_doc = user_ref.get()
    if not user_doc.exists:
        raise HTTPException(403, "User document not found")

    user_data = user_doc.to_dict()
    profile = user_data.get("profile", {})
    route_assignments = user_data.get("routeAssignments", {})

    owned_routes = set()
    if profile.get("routeNumber"):
        owned_routes.add(str(profile["routeNumber"]))
    for r in (profile.get("additionalRoutes") or []):
        owned_routes.add(str(r))
    for r in route_assignments.keys():
        owned_routes.add(str(r))

    if payload.fromRouteNumber not in owned_routes or payload.toRouteNumber not in owned_routes:
        raise HTTPException(403, "Must own both fromRoute and toRoute")

    if payload.fromRouteNumber == payload.toRouteNumber:
        raise HTTPException(400, "fromRoute and toRoute must differ")

    if not payload.sap or payload.sap.strip() == "":
        raise HTTPException(400, "SAP code is required")

    transfer_ref = (
        db.collection("routeTransfers")
        .document(payload.routeGroupId)
        .collection("transfers")
        .document(payload.transferKey)
    )

    @firestore.transactional
    def upsert_in_transaction(transaction, ref):
        snap = ref.get(transaction=transaction)
        existing = snap.to_dict() if snap.exists else None

        reserved_by = (existing.get("reservedBy", {}) if existing else {})
        reserved_total = sum(v for v in reserved_by.values() if isinstance(v, (int, float)))

        # units=0 means delete (only if no reservations)
        if payload.units == 0:
            if reserved_total > 0:
                raise HTTPException(409, f"Cannot delete transfer with {reserved_total} reserved units")
            transaction.delete(ref)
            return {"deleted": True}

        # Cannot shrink below existing reservations
        if existing and payload.units < reserved_total:
            raise HTTPException(409, f"Cannot reduce units below reserved total ({reserved_total})")

        # Build payload
        transfer_data = {
            "routeGroupId": payload.routeGroupId,
            "purchaseRouteNumber": payload.fromRouteNumber,  # Alias for compatibility
            "fromRouteNumber": payload.fromRouteNumber,
            "toRouteNumber": payload.toRouteNumber,
            "sap": payload.sap,
            "units": payload.units,
            "casePack": payload.casePack,
            "transferDate": payload.transferDate or datetime.utcnow().date().isoformat(),
            "status": payload.status or "planned",
            "createdByUid": uid,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }

        if payload.reason:
            transfer_data["reason"] = payload.reason
        if payload.sourceOrderId:
            transfer_data["sourceOrderId"] = payload.sourceOrderId

        if not existing:
            transfer_data["createdAt"] = firestore.SERVER_TIMESTAMP

        # Merge to preserve reservedBy
        transaction.set(ref, transfer_data, merge=True)
        return {"created": not snap.exists}

    transaction = db.transaction()
    try:
        result = upsert_in_transaction(transaction, transfer_ref)
        return {"ok": True, "transferId": payload.transferKey, **result}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Transfer create failed: {str(exc)}")

