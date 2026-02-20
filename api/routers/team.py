"""Team/management router.

Provides owner/team-member endpoints for team management and scanner-access
requests.
"""

from __future__ import annotations

import base64
import json
import logging
import os
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
MAILJET_API_KEY = str(os.environ.get("MAILJET_API_KEY") or "").strip()
MAILJET_SECRET_KEY = str(os.environ.get("MAILJET_SECRET_KEY") or "").strip()
MAILJET_FROM = str(os.environ.get("MAILJET_FROM") or "RouteSpark <no-reply@routespark.pro>").strip()
ISSUE_REPORT_ALERT_EMAIL = str(os.environ.get("ISSUE_REPORT_ALERT_EMAIL") or "").strip()


class ScannerAccessRequestCreate(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")


class ScannerAccessResolveRequest(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")
    teamMemberUid: str = Field(..., min_length=1, max_length=128)
    action: str = Field(..., pattern=r"^(approve|deny)$")
    denyReason: Optional[str] = Field(default=None, max_length=300)


class IssueReportCreate(BaseModel):
    route: str = Field(..., pattern=r"^\d{1,10}$")
    category: str = Field(
        default="other",
        pattern=r"^(account_access|billing_subscription|scanner_camera|team_permissions|data_issue|app_bug|performance_sync|other|harassment|hate|sexual_content|violence|illegal_activity|impersonation|privacy_doxxing|spam)$",
    )
    targetType: str = Field(default="other", pattern=r"^(user|route|content|other)$")
    targetRoute: Optional[str] = Field(default=None, pattern=r"^\d{1,10}$")
    targetContentRef: Optional[str] = Field(default=None, max_length=256)
    allowFollowUp: bool = Field(default=True)
    contactEmail: Optional[str] = Field(default=None, max_length=256)
    details: str = Field(default="", max_length=3000)
    targetUid: Optional[str] = Field(default=None, max_length=128)
    clientMeta: Dict[str, Any] = Field(default_factory=dict)


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


def _parse_mailjet_from() -> Dict[str, str]:
    from_email = MAILJET_FROM
    from_name = "RouteSpark"
    if "<" in MAILJET_FROM and ">" in MAILJET_FROM:
        before, after = MAILJET_FROM.split("<", 1)
        from_name = before.strip() or "RouteSpark"
        from_email = after.split(">", 1)[0].strip() or from_email
    return {"email": from_email, "name": from_name}


def _send_mailjet_email(to_email: str, subject: str, html_part: str) -> bool:
    if not to_email or not MAILJET_API_KEY or not MAILJET_SECRET_KEY:
        return False

    sender = _parse_mailjet_from()
    payload = {
        "Messages": [
            {
                "From": {"Email": sender["email"], "Name": sender["name"]},
                "To": [{"Email": to_email}],
                "Subject": subject,
                "HTMLPart": html_part,
            }
        ]
    }
    creds = f"{MAILJET_API_KEY}:{MAILJET_SECRET_KEY}".encode("utf-8")
    auth_header = "Basic " + base64.b64encode(creds).decode("utf-8")
    req = urllib_request.Request(
        "https://api.mailjet.com/v3.1/send",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": auth_header,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as resp:
            return 200 <= int(resp.status) < 300
    except Exception as exc:
        logger.warning("Issue report email send failed: %s", exc)
        return False


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


def _safe_text(value: Any, max_len: int) -> str:
    return str(value or "").strip()[:max_len]


def _normalize_report_category(payload: IssueReportCreate) -> str:
    return payload.category or "other"


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
    "/team/report-issue",
    responses={
        400: {"model": ErrorResponse},
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
    },
)
@rate_limit_write
async def submit_issue_report(
    request: Request,
    payload: IssueReportCreate,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Submit an in-app issue report for review.

    This endpoint is intentionally available to any authenticated user
    with access to the specified route.
    """
    route = payload.route
    reporter_uid = decoded_token["uid"]
    reporter_data = await require_route_access(route, decoded_token, db)
    details = _safe_text(payload.details, 3000)
    if len(details) < 10:
        raise HTTPException(400, "Please provide at least 10 characters in report details")
    category = _normalize_report_category(payload)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    duplicate_window_ms = 90 * 1000

    # Duplicate guard: block accidental rapid repeat submissions for same route/user/details.
    # Keep this query simple to avoid requiring additional Firestore composite indexes.
    prior_reports = (
        db.collection("issueReports")
        .where("reporterUid", "==", reporter_uid)
        .limit(25)
        .stream()
    )
    normalized_details = details.lower()
    for prior in prior_reports:
        prior_data = prior.to_dict() or {}
        if str(prior_data.get("routeNumber") or "") != route:
            continue
        prior_ms = prior_data.get("submittedAtMs")
        if not isinstance(prior_ms, int):
            continue
        if now_ms - prior_ms > duplicate_window_ms:
            continue
        prior_details = _safe_text(prior_data.get("details"), 3000).lower()
        if prior_details == normalized_details:
            raise HTTPException(409, "Duplicate report submitted too recently")

    reporter_profile = reporter_data.get("profile", {}) or {}
    client_meta = payload.clientMeta if isinstance(payload.clientMeta, dict) else {}
    client_meta_sanitized = {
        "platform": _safe_text(client_meta.get("platform"), 32),
        "appVersion": _safe_text(client_meta.get("appVersion"), 64),
        "buildNumber": _safe_text(client_meta.get("buildNumber"), 64),
    }
    report_doc = {
        "routeNumber": route,
        "reporterUid": reporter_uid,
        "reporterEmail": _safe_text(reporter_profile.get("email"), 256),
        "reporterRole": _safe_text(reporter_profile.get("role"), 64),
        "category": category,
        "targetType": _safe_text(payload.targetType, 32) or "other",
        "details": details,
        "targetUid": _safe_text(payload.targetUid, 128),
        "targetRoute": _safe_text(payload.targetRoute, 32),
        "targetContentRef": _safe_text(payload.targetContentRef, 256),
        "allowFollowUp": bool(payload.allowFollowUp),
        "contactEmail": _safe_text(payload.contactEmail, 256),
        "clientMeta": client_meta_sanitized,
        "status": "open",
        "source": "mobile_app",
        "submittedAtMs": now_ms,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }
    report_ref = db.collection("issueReports").document()
    report_ref.set(report_doc, merge=False)

    owner_uid = _resolve_owner_uid_for_route(
        db=db,
        route_number=route,
        requester_uid=reporter_uid,
        requester_data=reporter_data,
    )

    owner_email = ""
    owner_tokens: List[str] = []
    if owner_uid:
        owner_doc = db.collection("users").document(owner_uid).get()
        owner_data = owner_doc.to_dict() or {}
        owner_email = _safe_text((owner_data.get("profile") or {}).get("email"), 256)
        raw_tokens = owner_data.get("fcmTokens") or []
        if isinstance(raw_tokens, list):
            owner_tokens = [str(t) for t in raw_tokens if isinstance(t, str)]

    push_stats = {"sent": 0, "failed": 0}
    in_app_notification_written = False
    if owner_uid and owner_uid != reporter_uid and owner_tokens:
        push_stats = _send_expo_push(
            owner_tokens,
            title="New issue report submitted",
            body=f"{report_doc.get('category') or 'An issue'} was submitted for route {route}",
            data={
                "type": "issue_report_submitted",
                "routeNumber": route,
                "reportId": report_ref.id,
            },
        )
    if owner_uid and owner_uid != reporter_uid:
        db.collection("users").document(owner_uid).collection("notifications").add(
            {
                "title": "New issue report submitted",
                "body": f"{report_doc.get('category') or 'An issue'} was submitted for route {route}",
                "type": "issue_report_submitted",
                "read": False,
                "data": {
                    "routeNumber": route,
                    "reportId": report_ref.id,
                    "category": report_doc.get("category") or "other",
                },
                "createdAt": firestore.SERVER_TIMESTAMP,
            }
        )
        in_app_notification_written = True

    email_recipients: List[str] = []
    if ISSUE_REPORT_ALERT_EMAIL:
        email_recipients.append(ISSUE_REPORT_ALERT_EMAIL)
    if owner_email and owner_email not in email_recipients:
        email_recipients.append(owner_email)

    email_sent = False
    if email_recipients:
        subject = f"RouteSpark issue report · Route {route}"
        html = (
            "<div style='font-family:Arial,sans-serif'>"
            "<h3>New issue report submitted</h3>"
            f"<p><strong>Report ID:</strong> {report_ref.id}</p>"
            f"<p><strong>Route:</strong> {route}</p>"
            f"<p><strong>Reporter:</strong> {report_doc.get('reporterEmail') or reporter_uid}</p>"
            f"<p><strong>Category:</strong> {report_doc.get('category') or 'other'}</p>"
            f"<p><strong>Target Type:</strong> {report_doc.get('targetType') or 'other'}</p>"
            f"<p><strong>Target UID:</strong> {report_doc.get('targetUid') or '-'}</p>"
            f"<p><strong>Target Route:</strong> {report_doc.get('targetRoute') or '-'}</p>"
            f"<p><strong>Target Content Ref:</strong> {report_doc.get('targetContentRef') or '-'}</p>"
            f"<p><strong>Details:</strong><br/>{(report_doc.get('details') or '').replace('<', '&lt;').replace('>', '&gt;')}</p>"
            "</div>"
        )
        for recipient in email_recipients:
            email_sent = _send_mailjet_email(recipient, subject, html) or email_sent

    return {
        "ok": True,
        "reportId": report_ref.id,
        "status": "open",
        "submittedAtMs": now_ms,
        "ownerUid": owner_uid or None,
        "pushNotification": push_stats,
        "inAppNotificationWritten": in_app_notification_written,
        "emailSent": email_sent,
    }


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


@router.get(
    "/team/scanner-access/status",
    responses={
        401: {"model": ErrorResponse},
        403: {"model": ErrorResponse},
    },
)
@rate_limit_history
async def get_scanner_access_status(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Get scanner-access request status for the authenticated team member on a route."""
    requester_uid = decoded_token["uid"]
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_team_member_for_route(user_data, route):
        raise HTTPException(403, "Team-member route access required")

    req_ref = (
        db.collection("routes")
        .document(route)
        .collection("scannerAccessRequests")
        .document(requester_uid)
    )
    snap = req_ref.get()
    if not snap.exists:
        return {"ok": True, "route": route, "teamMemberUid": requester_uid, "status": "none"}

    data = snap.to_dict() or {}
    status = str(data.get("status") or "").strip().lower()
    if status not in ("pending", "approved", "denied"):
        status = "unknown"

    return {
        "ok": True,
        "route": route,
        "teamMemberUid": requester_uid,
        "status": status,
        "requestedAt": _ts_to_millis(data.get("requestedAt")),
        "resolvedAt": _ts_to_millis(data.get("resolvedAt")),
    }


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
