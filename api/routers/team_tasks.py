"""Route-scoped Team Tasks API.

Team Tasks are owner-assigned route work items for approved team members.
The first pass is API-backed only: clients do not write task documents
directly, and no route entitlement gate is applied to collaboration data.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field

from ..dependencies import (
    _resolve_owner_uid_for_route,
    get_firestore,
    require_route_access,
    verify_firebase_token,
)
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..models import ErrorResponse
from .team import _send_expo_push

router = APIRouter()
logger = logging.getLogger("api.team_tasks")

TEAM_TASK_LIMIT_DEFAULT = 100
TEAM_TASK_LIMIT_MAX = 100


class TeamTaskCreateRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    teamMemberUid: str = Field(..., min_length=1, max_length=128)
    task: str = Field(..., min_length=1, max_length=500)


class TeamTaskCompleteRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")


class TeamTaskResponse(BaseModel):
    id: str
    routeNumber: str
    ownerUid: str
    teamMemberUid: str
    teamMemberDisplay: str
    task: str
    status: Literal["open", "completed"]
    createdAtMs: Optional[int] = None
    updatedAtMs: Optional[int] = None
    completedAtMs: Optional[int] = None


def _is_owner_for_route(user_data: Dict[str, Any], route_number: str) -> bool:
    profile = user_data.get("profile", {}) or {}
    if (
        str(profile.get("role") or "").strip() == "owner"
        and str(profile.get("routeNumber") or "").strip() == route_number
    ):
        return True
    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return isinstance(assignment, dict) and str(assignment.get("role") or "").strip() == "owner"


def _assignment_for_route(user_data: Dict[str, Any], route_number: str) -> Dict[str, Any]:
    assignments = user_data.get("routeAssignments", {}) or {}
    assignment = assignments.get(route_number, {}) if isinstance(assignments, dict) else {}
    return assignment if isinstance(assignment, dict) else {}


def _is_approved_team_member(user_data: Dict[str, Any], route_number: str) -> bool:
    assignment = _assignment_for_route(user_data, route_number)
    if str(assignment.get("role") or "").strip() != "team_member":
        return False
    if bool(assignment.get("needsApproval")):
        return False
    if "verified" in assignment and not bool(assignment.get("verified")):
        return False
    return True


def _to_millis(value: Any) -> Optional[int]:
    if value is None:
        return None
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _safe_text(value: Any, max_len: int) -> str:
    return str(value or "").strip()[:max_len]


def _display_for_user(uid: str, user_data: Dict[str, Any]) -> str:
    profile = user_data.get("profile", {}) or {}
    return (
        _safe_text(profile.get("personalName"), 160)
        or _safe_text(profile.get("displayName"), 160)
        or _safe_text(profile.get("name"), 160)
        or _safe_text(profile.get("email"), 256)
        or uid
    )


def _tokens_for_user(user_data: Dict[str, Any]) -> List[str]:
    tokens = user_data.get("fcmTokens") or []
    if not isinstance(tokens, list):
        return []
    return [str(token) for token in tokens if isinstance(token, str) and token.strip()]


def _user_doc_data(db: firestore.Client, uid: str) -> Dict[str, Any]:
    if not uid:
        return {}
    doc = db.collection("users").document(uid).get()
    if not doc.exists:
        return {}
    return doc.to_dict() or {}


def _team_tasks_collection(db: firestore.Client, route_number: str):
    return db.collection("routes").document(route_number).collection("teamTasks")


def _team_task_ref(db: firestore.Client, route_number: str, task_id: str):
    return _team_tasks_collection(db, route_number).document(task_id)


def _serialize_task(route_number: str, doc: Any) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    return {
        "id": doc.id,
        "routeNumber": str(data.get("routeNumber") or route_number),
        "ownerUid": str(data.get("ownerUid") or ""),
        "teamMemberUid": str(data.get("teamMemberUid") or ""),
        "teamMemberDisplay": str(data.get("teamMemberDisplay") or ""),
        "task": str(data.get("task") or ""),
        "status": "completed" if str(data.get("status") or "").strip() == "completed" else "open",
        "createdAtMs": _to_millis(data.get("createdAt")),
        "updatedAtMs": _to_millis(data.get("updatedAt")),
        "completedAtMs": _to_millis(data.get("completedAt")),
    }


def _write_user_notification(
    db: firestore.Client,
    *,
    uid: str,
    title: str,
    body: str,
    notification_type: str,
    data: Dict[str, Any],
) -> None:
    db.collection("users").document(uid).collection("notifications").add(
        {
            "title": title,
            "body": body,
            "type": notification_type,
            "read": False,
            "data": data,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def _notify_user(
    db: firestore.Client,
    *,
    uid: str,
    title: str,
    body: str,
    notification_type: str,
    data: Dict[str, Any],
    user_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    _write_user_notification(
        db,
        uid=uid,
        title=title,
        body=body,
        notification_type=notification_type,
        data=data,
    )
    notify_data = user_data if user_data is not None else _user_doc_data(db, uid)
    tokens = _tokens_for_user(notify_data)
    return _send_expo_push(tokens, title=title, body=body, data={"type": notification_type, **data})


def _stream_tasks_for_route(db: firestore.Client, route_number: str, limit: int) -> List[Any]:
    collection_ref = _team_tasks_collection(db, route_number)
    try:
        return list(collection_ref.limit(limit).stream())
    except AttributeError:
        return list(collection_ref.limit(limit).get())


@router.get(
    "/team-tasks",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def list_team_tasks(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    limit: int = Query(default=TEAM_TASK_LIMIT_DEFAULT, ge=1, le=TEAM_TASK_LIMIT_MAX),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """List route Team Tasks.

    Owners see all route tasks. Team members see only their assigned tasks.
    """
    user_data = await require_route_access(route, decoded_token, db)
    requester_uid = decoded_token["uid"]
    is_owner = _is_owner_for_route(user_data, route)

    tasks: List[Dict[str, Any]] = []
    for doc in _stream_tasks_for_route(db, route, limit):
        task = _serialize_task(route, doc)
        if not is_owner and task["teamMemberUid"] != requester_uid:
            continue
        tasks.append(task)

    tasks.sort(key=lambda task: (task.get("status") == "completed", -(task.get("createdAtMs") or 0)))
    return {"ok": True, "tasks": tasks}


@router.get(
    "/team-tasks/capabilities",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}},
)
@rate_limit_history
async def get_team_task_capabilities(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Return lightweight Team Tasks availability for Quick Access defaulting."""
    user_data = await require_route_access(route, decoded_token, db)
    requester_uid = decoded_token["uid"]
    can_assign = _is_owner_for_route(user_data, route) and _owner_has_team_members(db, route)
    has_assigned = _has_open_assigned_task(db, route, requester_uid)
    return {
        "ok": True,
        "canAssign": can_assign,
        "hasAssignedTeamTasks": has_assigned,
        "defaultThirdSlot": "teamTasks" if can_assign or has_assigned else "stores",
    }


def _owner_has_team_members(db: firestore.Client, route_number: str) -> bool:
    # Team member data currently lives on user docs; scan is consistent with
    # /team/members and avoids a new index requirement for this low-frequency call.
    for doc in db.collection("users").stream():
        if _is_approved_team_member(doc.to_dict() or {}, route_number):
            return True
    return False


def _has_open_assigned_task(db: firestore.Client, route_number: str, uid: str) -> bool:
    for doc in _stream_tasks_for_route(db, route_number, TEAM_TASK_LIMIT_MAX):
        data = doc.to_dict() or {}
        if data.get("teamMemberUid") == uid and str(data.get("status") or "open") == "open":
            return True
    return False


@router.post(
    "/team-tasks",
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
@rate_limit_write
async def create_team_task(
    request: Request,
    payload: TeamTaskCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Create a Team Task for an approved team member. Owner-only."""
    route_number = payload.routeNumber
    owner_uid = decoded_token["uid"]
    owner_data = await require_route_access(route_number, decoded_token, db)
    if not _is_owner_for_route(owner_data, route_number):
        raise HTTPException(403, "Owner access required")

    task_text = _safe_text(payload.task, 500)
    if not task_text:
        raise HTTPException(400, "Task text is required")

    team_member_uid = payload.teamMemberUid.strip()
    if team_member_uid == owner_uid:
        raise HTTPException(409, "Assign the task to an approved team member")
    member_data = _user_doc_data(db, team_member_uid)
    if not member_data or not _is_approved_team_member(member_data, route_number):
        raise HTTPException(403, "Approved team member required")

    owner_display = _display_for_user(owner_uid, owner_data)
    member_display = _display_for_user(team_member_uid, member_data)
    task_ref = _team_tasks_collection(db, route_number).document()
    task_ref.set(
        {
            "routeNumber": route_number,
            "ownerUid": owner_uid,
            "teamMemberUid": team_member_uid,
            "teamMemberDisplay": member_display,
            "task": task_text,
            "status": "open",
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        },
        merge=False,
    )

    body = f"Route: {route_number}\nTask: {task_text}\nAssigned by: {owner_display}"
    notification = _notify_user(
        db,
        uid=team_member_uid,
        title="New team task assigned",
        body=body,
        notification_type="team_task_assigned",
        data={"routeNumber": route_number, "taskId": task_ref.id, "target": "teamTasks"},
        user_data=member_data,
    )
    return {"ok": True, "taskId": task_ref.id, "notification": notification}


@router.post(
    "/team-tasks/{task_id}/complete",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)
@rate_limit_write
async def complete_team_task(
    request: Request,
    task_id: str,
    payload: TeamTaskCompleteRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Mark the caller's assigned Team Task complete.

    The status transition is transactional so duplicate taps/retries do not
    duplicate owner notifications.
    """
    route_number = payload.routeNumber
    requester_uid = decoded_token["uid"]
    requester_data = await require_route_access(route_number, decoded_token, db)
    task_ref = _team_task_ref(db, route_number, task_id)

    @firestore.transactional
    def complete_in_transaction(transaction):
        snap = task_ref.get(transaction=transaction)
        if not snap.exists:
            raise HTTPException(404, "Team task not found")
        task_data = snap.to_dict() or {}
        if str(task_data.get("routeNumber") or route_number) != route_number:
            raise HTTPException(404, "Team task not found")
        if task_data.get("teamMemberUid") != requester_uid:
            raise HTTPException(403, "Assigned team member required")
        if str(task_data.get("status") or "open") == "completed":
            return {"changed": False, "task": task_data}
        transaction.update(
            task_ref,
            {
                "status": "completed",
                "completedAt": firestore.SERVER_TIMESTAMP,
                "completedBy": requester_uid,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
        )
        updated = dict(task_data)
        updated["status"] = "completed"
        updated["completedBy"] = requester_uid
        return {"changed": True, "task": updated}

    transaction = db.transaction()
    result = complete_in_transaction(transaction)
    if not result.get("changed"):
        return {"ok": True, "alreadyCompleted": True}

    task = result["task"]
    owner_uid = _resolve_owner_uid_for_route(
        db=db,
        route_number=route_number,
        requester_uid=requester_uid,
        requester_data=requester_data,
    ) or str(task.get("ownerUid") or "")
    if owner_uid and owner_uid != requester_uid:
        member_display = str(task.get("teamMemberDisplay") or "") or _display_for_user(requester_uid, requester_data)
        task_text = str(task.get("task") or "")
        body = (
            f"Route: {route_number}\n"
            f"Team Member: {member_display}\n"
            f"Task: {task_text}\n"
            "Marked Complete."
        )
        _notify_user(
            db,
            uid=owner_uid,
            title="Team task completed",
            body=body,
            notification_type="team_task_completed",
            data={
                "routeNumber": route_number,
                "taskId": task_id,
                "teamMemberUid": requester_uid,
                "target": "teamTasks",
            },
        )

    return {"ok": True}


@router.delete(
    "/team-tasks/{task_id}",
    responses={401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}},
)
@rate_limit_write
async def delete_team_task(
    request: Request,
    task_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Delete a Team Task. Owner-only."""
    user_data = await require_route_access(route, decoded_token, db)
    if not _is_owner_for_route(user_data, route):
        raise HTTPException(403, "Owner access required")

    task_ref = _team_task_ref(db, route, task_id)
    snap = task_ref.get()
    if not snap.exists:
        return {"ok": True, "alreadyDeleted": True}
    data = snap.to_dict() or {}
    if str(data.get("routeNumber") or route) != route:
        raise HTTPException(404, "Team task not found")
    task_ref.delete()
    return {"ok": True}
