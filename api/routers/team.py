"""Team/management router.

Provides owner/team-member endpoints for team management and scanner-access
requests.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field

from ..dependencies import verify_firebase_token, require_route_access, get_firestore
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse

router = APIRouter()
logger = logging.getLogger("api.team")

EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
SCANNER_REQUEST_NOTIFY_COOLDOWN_SECONDS = 10 * 60


class ScannerAccessRequestCreate(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")


class ScannerAccessResolveRequest(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")
    teamMemberUid: str = Field(..., min_length=1, max_length=128)
    action: str = Field(..., pattern=r"^(approve|deny)$")
    denyReason: Optional[str] = Field(default=None, max_length=300)


def _is_owner_for_route(user_data: Dict[str, Any], route_number: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if str(profile.get("role") or "").strip() == "owner" and str(profile.get("routeNumber") or "").strip() == route_number:
        return True
    assignments = user_data.get("routeAssignments", {}) or {}
    a = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return isinstance(a, dict) and str(a.get("role") or "").strip() == "owner"


def _is_team_member_for_route(user_data: Dict[str, Any], route_number: str) -> bool:
    assignments = user_data.get("routeAssignments", {}) or {}
    a = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return isinstance(a, dict) and str(a.get("role") or "").strip() == "team_member"


def _normalize_route_number(value: Any) -> str:
    v = str(value or "").strip()
    return v if v.isdigit() and len(v) <= 10 else ""


def _ts_to_millis(value: Any) -> Optional[int]:
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1000)
    return None


def _is_valid_expo_token(token: str) -> bool:
    if not token:
        return False
    return token.startswith("ExponentPushToken[") or token.startswith("ExpoPushToken[")


def _send_expo_push(tokens: List[str], *, title: str, body: str, data: Dict[str, Any]) -> Dict[str, int]:
    valid_tokens = [t for t in tokens if _is_valid_expo_token(t)]
    if not valid_tokens:
        return {"sent": 0, "failed": 0}

    messages = [
        {
            "to": token,
            "title": title,
            "body": body,
            "data": data,
            "sound": "default",
            "priority": "high",
        }
        for token in valid_tokens
    ]

    sent = 0
    failed = 0
    batch_size = 100
    headers = {"Content-Type": "application/json"}

    for i in range(0, len(messages), batch_size):
        batch = messages[i : i + batch_size]
        payload = json.dumps(batch).encode("utf-8")
        req = urllib_request.Request(EXPO_PUSH_URL, data=payload, headers=headers, method="POST")
        try:
            with urllib_request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8")
                decoded = json.loads(raw) if raw else {}
                result_rows = decoded.get("data")
                if isinstance(result_rows, list):
                    batch_sent = sum(1 for row in result_rows if isinstance(row, dict) and row.get("status") == "ok")
                    sent += batch_sent
                    failed += len(batch) - batch_sent
                else:
                    sent += len(batch)
        except (urllib_error.URLError, urllib_error.HTTPError, TimeoutError, ValueError) as exc:
            logger.warning("Scanner access push batch failed: %s", exc)
            failed += len(batch)

    return {"sent": sent, "failed": failed}


def _resolve_owner_uid_for_route(
    *,
    db: firestore.Client,
    route_number: str,
    requester_uid: str,
    requester_data: Dict[str, Any],
) -> str:
    route_doc = db.collection("routes").document(route_number).get()
    if route_doc.exists:
        route_data = route_doc.to_dict() or {}
        owner_uid = str(route_data.get("ownerUid") or route_data.get("userId") or "").strip()
        if owner_uid:
            return owner_uid

    assignments = requester_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    assigned_to = str(assignment.get("assignedTo") or "").strip() if isinstance(assignment, dict) else ""
    if assigned_to:
        return assigned_to

    # Last resort: if the requester is owner for this route, return requester.
    if _is_owner_for_route(requester_data, route_number):
        return requester_uid
    return ""


@router.get(
    "/team/members",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def list_team_members_for_route(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """List team members for a route (owner-only).

    Returns both approved and pending members.
    """
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_owner_for_route(user_data, route):
        raise HTTPException(403, "Owner access required")

    # NOTE: Same reason as above; scan and filter in memory.
    docs = db.collection("users").stream()

    members: List[Dict[str, Any]] = []
    for doc in docs:
        data = doc.to_dict() or {}
        assignment = (data.get("routeAssignments") or {}).get(route, {}) if isinstance(data.get("routeAssignments"), dict) else {}
        if not (isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "team_member"):
            continue
        needs_approval = bool(assignment.get("needsApproval"))
        verified = bool(assignment.get("verified"))
        assignments = data.get("routeAssignments") or {}
        assigned_routes: List[str] = []
        if isinstance(assignments, dict):
            for rn, a in assignments.items():
                if isinstance(a, dict) and str(a.get("role") or "").strip() == "team_member":
                    assigned_routes.append(str(rn))
        assigned_routes = sorted(set(assigned_routes), key=lambda x: int(x) if str(x).isdigit() else 10**12)
        added_at = assignment.get("addedAt")
        members.append({
            "uid": doc.id,
            "email": data.get("profile", {}).get("email"),
            "name": data.get("profile", {}).get("personalName"),
            "verified": verified,
            "needsApproval": needs_approval,
            "addedAt": int(added_at.timestamp() * 1000) if hasattr(added_at, "timestamp") else None,
            "scannerAccess": bool((data.get("trialStatus") or {}).get("features", {}).get("scanner")),
            "managementAccess": bool((data.get("trialStatus") or {}).get("features", {}).get("managementDashboard")),
            "role": "team_member",
            "assignedRoutes": assigned_routes,
        })

    pending = [m for m in members if m.get("needsApproval")]
    approved = [m for m in members if not m.get("needsApproval")]
    return {"ok": True, "approved": approved, "pending": pending}


@router.post(
    "/team/scanner-access/request",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def request_scanner_access(
    request: Request,
    payload: ScannerAccessRequestCreate,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Team member requests scanner access for a route.

    Idempotent behavior:
    - existing `pending` request returns success with `alreadyPending=true`
    - denied/approved requests are re-opened as pending
    """
    route = payload.route
    requester_uid = decoded_token["uid"]
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_team_member_for_route(user_data, route):
        raise HTTPException(403, "Team-member route access required")

    owner_uid = _resolve_owner_uid_for_route(
        db=db,
        route_number=route,
        requester_uid=requester_uid,
        requester_data=user_data,
    )
    if not owner_uid:
        raise HTTPException(409, "Route owner could not be resolved")
    if owner_uid == requester_uid:
        raise HTTPException(409, "Owner cannot request scanner access from self")

    profile = user_data.get("profile", {}) or {}
    team_member_email = str(profile.get("email") or "").strip()
    team_member_name = str(profile.get("personalName") or "").strip()

    req_ref = (
        db.collection("routes")
        .document(route)
        .collection("scannerAccessRequests")
        .document(requester_uid)
    )

    @firestore.transactional
    def upsert_request(transaction):
        snap = req_ref.get(transaction=transaction)
        existing = snap.to_dict() if snap.exists else {}
        existing_status = str(existing.get("status") or "").strip().lower()

        if existing_status == "pending":
            return {
                "alreadyPending": True,
                "requestId": requester_uid,
                "status": "pending",
                "shouldNotifyOwner": False,
            }

        now = datetime.now(timezone.utc)
        should_notify_owner = True
        last_notified = existing.get("lastNotifiedAt")
        if hasattr(last_notified, "timestamp"):
            elapsed = now.timestamp() - float(last_notified.timestamp())
            if elapsed < SCANNER_REQUEST_NOTIFY_COOLDOWN_SECONDS:
                should_notify_owner = False

        update_payload = {
            "routeNumber": route,
            "teamMemberUid": requester_uid,
            "teamMemberEmail": team_member_email,
            "teamMemberName": team_member_name,
            "ownerUid": owner_uid,
            "status": "pending",
            "requestedAt": firestore.SERVER_TIMESTAMP,
            "resolvedAt": firestore.DELETE_FIELD,
            "resolvedBy": firestore.DELETE_FIELD,
            "denyReason": firestore.DELETE_FIELD,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        if should_notify_owner:
            update_payload["lastNotifiedAt"] = firestore.SERVER_TIMESTAMP

        transaction.set(req_ref, update_payload, merge=True)

        return {
            "alreadyPending": False,
            "requestId": requester_uid,
            "status": "pending",
            "shouldNotifyOwner": should_notify_owner,
        }

    transaction = db.transaction()
    result = upsert_request(transaction)

    notification_sent = False
    notification_stats = {"sent": 0, "failed": 0}
    if result.get("shouldNotifyOwner"):
        owner_doc = db.collection("users").document(owner_uid).get()
        owner_data = owner_doc.to_dict() or {}
        owner_tokens = owner_data.get("fcmTokens") or []
        if isinstance(owner_tokens, list):
            notification_stats = _send_expo_push(
                owner_tokens,
                title="Scanner access request",
                body=f"{team_member_email or 'A team member'} requested scanner access",
                data={
                    "type": "scanner_access_request",
                    "routeNumber": route,
                    "teamMemberUid": requester_uid,
                },
            )
            notification_sent = notification_stats["sent"] > 0

    return {
        "ok": True,
        "requestId": result["requestId"],
        "status": result["status"],
        "alreadyPending": bool(result["alreadyPending"]),
        "notificationSent": notification_sent,
        "notification": notification_stats,
    }


@router.get(
    "/team/scanner-access/requests",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def list_scanner_access_requests(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    status: str = Query(default="pending", pattern=r"^(pending|approved|denied|all)$"),
    limit: int = Query(default=200, ge=1, le=500),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """List scanner-access requests for a route (owner-only)."""
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_owner_for_route(user_data, route):
        raise HTTPException(403, "Owner access required")

    collection_ref = (
        db.collection("routes")
        .document(route)
        .collection("scannerAccessRequests")
    )

    if status == "all":
        snap = collection_ref.limit(limit).get()
    else:
        snap = collection_ref.where("status", "==", status).limit(limit).get()

    requests_out: List[Dict[str, Any]] = []
    for doc in snap:
        data = doc.to_dict() or {}
        owner_uid = str(data.get("ownerUid") or "").strip()
        # Extra safety: only expose rows owned by this route owner.
        if owner_uid and owner_uid != decoded_token["uid"]:
            continue
        requests_out.append(
            {
                "id": doc.id,
                "routeNumber": route,
                "route": route,
                "teamMemberUid": data.get("teamMemberUid"),
                "teamMemberEmail": data.get("teamMemberEmail"),
                "teamMemberName": data.get("teamMemberName"),
                # Backward-compatible aliases for existing clients.
                "email": data.get("teamMemberEmail"),
                "name": data.get("teamMemberName"),
                "ownerUid": owner_uid,
                "status": data.get("status"),
                "requestedAt": _ts_to_millis(data.get("requestedAt")),
                "resolvedAt": _ts_to_millis(data.get("resolvedAt")),
                "resolvedBy": data.get("resolvedBy"),
                "denyReason": data.get("denyReason"),
                "lastNotifiedAt": _ts_to_millis(data.get("lastNotifiedAt")),
            }
        )

    requests_out.sort(key=lambda r: r.get("requestedAt") or 0, reverse=True)
    return {"ok": True, "requests": requests_out}


@router.post(
    "/team/scanner-access/resolve",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def resolve_scanner_access_request(
    request: Request,
    payload: ScannerAccessResolveRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Owner approves or denies scanner access request."""
    route = payload.route
    owner_uid = decoded_token["uid"]
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_owner_for_route(user_data, route):
        raise HTTPException(403, "Owner access required")

    request_ref = (
        db.collection("routes")
        .document(route)
        .collection("scannerAccessRequests")
        .document(payload.teamMemberUid)
    )
    team_member_ref = db.collection("users").document(payload.teamMemberUid)
    resolved_status = "approved" if payload.action == "approve" else "denied"

    @firestore.transactional
    def resolve_in_transaction(transaction):
        req_snap = request_ref.get(transaction=transaction)
        if not req_snap.exists:
            raise HTTPException(404, "Scanner access request not found")

        req_data = req_snap.to_dict() or {}
        req_owner_uid = str(req_data.get("ownerUid") or "").strip()
        if req_owner_uid and req_owner_uid != owner_uid:
            raise HTTPException(403, "Owner access required")

        status = str(req_data.get("status") or "").strip().lower()
        if status != "pending":
            raise HTTPException(409, f"Request is already {status or 'resolved'}")

        member_snap = team_member_ref.get(transaction=transaction)
        if not member_snap.exists:
            raise HTTPException(404, "Team member not found")
        member_data = member_snap.to_dict() or {}
        assignments = member_data.get("routeAssignments", {}) or {}
        assignment = assignments.get(route, {}) if isinstance(assignments, dict) else {}
        if not (isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "team_member"):
            raise HTTPException(409, "User is not a team member on this route")

        scanner_enabled = payload.action == "approve"
        transaction.update(
            team_member_ref,
            {
                "trialStatus.features.scanner": scanner_enabled,
                "timestamps.updatedAt": firestore.SERVER_TIMESTAMP,
            },
        )

        request_update = {
            "status": resolved_status,
            "resolvedAt": firestore.SERVER_TIMESTAMP,
            "resolvedBy": owner_uid,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        if payload.action == "deny" and payload.denyReason:
            request_update["denyReason"] = payload.denyReason.strip()
        elif payload.action == "approve":
            request_update["denyReason"] = firestore.DELETE_FIELD
        transaction.update(request_ref, request_update)

        team_member_email = str(member_data.get("profile", {}).get("email") or "").strip()
        return {"teamMemberEmail": team_member_email}

    transaction = db.transaction()
    result = resolve_in_transaction(transaction)

    member_doc = team_member_ref.get()
    member_tokens = (member_doc.to_dict() or {}).get("fcmTokens") or []
    notify_stats = {"sent": 0, "failed": 0}
    if isinstance(member_tokens, list):
        title = "Scanner access approved" if payload.action == "approve" else "Scanner access denied"
        body = (
            "Your route owner approved scanner access."
            if payload.action == "approve"
            else "Your route owner denied scanner access."
        )
        notify_stats = _send_expo_push(
            member_tokens,
            title=title,
            body=body,
            data={
                "type": "scanner_access_resolution",
                "routeNumber": route,
                "status": resolved_status,
            },
        )

    return {
        "ok": True,
        "route": route,
        "teamMemberUid": payload.teamMemberUid,
        "teamMemberEmail": result["teamMemberEmail"],
        "status": resolved_status,
        "notification": notify_stats,
    }
