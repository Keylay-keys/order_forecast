"""Route-scoped Team Tasks API.

Team Tasks are owner-assigned route work items for approved team members.
The first pass is API-backed only: clients do not write task documents
directly, and no route entitlement gate is applied to collaboration data.
"""

from __future__ import annotations

import logging
from datetime import datetime, time, timezone
from typing import Any, Dict, List, Literal, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field

from ..dependencies import (
    _resolve_owner_uid_for_route,
    get_firestore,
    get_route_timezone,
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
DEFAULT_ROUTE_TIMEZONE = "America/Denver"


class TeamTaskDueTime(BaseModel):
    hour: int = Field(..., ge=1, le=12)
    minute: int = Field(..., ge=0, le=59)
    period: Literal["AM", "PM"]


class TeamTaskScheduleFields(BaseModel):
    dueDate: Optional[str] = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    dueTime: Optional[TeamTaskDueTime] = None
    reminderEnabled: Optional[bool] = False
    reminderOffsetMinutes: Optional[int] = Field(default=None, ge=0, le=1440)


class TeamTaskCreateRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    teamMemberUid: str = Field(..., min_length=1, max_length=128)
    task: str = Field(..., min_length=1, max_length=500)
    dueDate: Optional[str] = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    dueTime: Optional[TeamTaskDueTime] = None
    reminderEnabled: Optional[bool] = False
    reminderOffsetMinutes: Optional[int] = Field(default=None, ge=0, le=1440)


class TeamTaskUpdateRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    teamMemberUid: Optional[str] = Field(default=None, min_length=1, max_length=128)
    task: Optional[str] = Field(default=None, min_length=1, max_length=500)
    dueDate: Optional[str] = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    dueTime: Optional[TeamTaskDueTime] = None
    reminderEnabled: Optional[bool] = False
    reminderOffsetMinutes: Optional[int] = Field(default=None, ge=0, le=1440)


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
    dueDate: Optional[str] = None
    dueTime: Optional[TeamTaskDueTime] = None
    timezone: Optional[str] = None
    dueAtMs: Optional[int] = None
    reminderEnabled: bool = False
    reminderOffsetMinutes: Optional[int] = None
    reminderAtMs: Optional[int] = None
    reminderStatus: Literal["none", "pending", "sending", "sent", "skipped", "failed"] = "none"
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


def _normalize_due_time(value: Any) -> Optional[Dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, TeamTaskDueTime):
        return value.model_dump()
    if not isinstance(value, dict):
        return None
    try:
        return TeamTaskDueTime(**value).model_dump()
    except Exception:
        return None


def _route_timezone(db: firestore.Client, route_number: str) -> str:
    tz_name = str(get_route_timezone(db, route_number) or "").strip() or DEFAULT_ROUTE_TIMEZONE
    try:
        ZoneInfo(tz_name)
        return tz_name
    except ZoneInfoNotFoundError:
        return DEFAULT_ROUTE_TIMEZONE


def _due_datetime_ms(due_date: str, due_time: Optional[TeamTaskDueTime], tz_name: str) -> int:
    try:
        parsed_date = datetime.strptime(due_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Invalid due date")

    if due_time:
        hour = due_time.hour % 12
        if due_time.period == "PM":
            hour += 12
        due_clock = time(hour=hour, minute=due_time.minute)
        due_dt = datetime.combine(parsed_date, due_clock, tzinfo=ZoneInfo(tz_name))
    else:
        due_dt = datetime.combine(parsed_date, time(23, 59, 59, 999000), tzinfo=ZoneInfo(tz_name))
    return int(due_dt.timestamp() * 1000)


def _schedule_fields(
    db: firestore.Client,
    route_number: str,
    payload: TeamTaskScheduleFields,
) -> Dict[str, Any]:
    due_date = str(payload.dueDate or "").strip() or None
    due_time = payload.dueTime
    reminder_enabled = bool(payload.reminderEnabled)
    reminder_offset = payload.reminderOffsetMinutes

    if due_time and not due_date:
        raise HTTPException(400, "Due time requires a due date")
    if reminder_enabled and not due_date:
        raise HTTPException(400, "Reminder requires a due date")
    if reminder_enabled and not due_time:
        raise HTTPException(400, "Reminder requires a due time")
    if reminder_enabled and reminder_offset is None:
        raise HTTPException(400, "Reminder timeframe is required")

    if not due_date:
        return {
            "dueDate": None,
            "dueTime": None,
            "timezone": None,
            "dueAtMs": None,
            "reminderEnabled": False,
            "reminderOffsetMinutes": None,
            "reminderAtMs": None,
            "reminderStatus": "none",
        }

    tz_name = _route_timezone(db, route_number)
    due_at_ms = _due_datetime_ms(due_date, due_time, tz_name)
    reminder_at_ms = None
    reminder_status = "none"
    if reminder_enabled and due_time:
        reminder_at_ms = due_at_ms - int(reminder_offset or 0) * 60 * 1000
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        reminder_status = "skipped" if reminder_at_ms <= now_ms or now_ms >= due_at_ms else "pending"

    return {
        "dueDate": due_date,
        "dueTime": due_time.model_dump() if due_time else None,
        "timezone": tz_name,
        "dueAtMs": due_at_ms,
        "reminderEnabled": bool(reminder_enabled and due_time),
        "reminderOffsetMinutes": reminder_offset if reminder_enabled and due_time else None,
        "reminderAtMs": reminder_at_ms,
        "reminderStatus": reminder_status,
    }


def _assignment_notification_body(route_number: str, task_text: str, owner_display: str, schedule: Dict[str, Any]) -> str:
    lines = [f"Route: {route_number}", f"Task: {task_text}"]
    if schedule.get("dueDate"):
        due_line = f"Due: {schedule['dueDate']}"
        due_time = _normalize_due_time(schedule.get("dueTime"))
        if due_time:
            due_line += f" {due_time['hour']}:{str(due_time['minute']).zfill(2)} {due_time['period']}"
        lines.append(due_line)
    lines.append(f"Assigned by: {owner_display}")
    return "\n".join(lines)


def _schedule_from_update_payload(
    db: firestore.Client,
    route_number: str,
    payload: TeamTaskUpdateRequest,
    current: Dict[str, Any],
) -> Dict[str, Any]:
    fields_set = payload.model_fields_set
    existing_due_time = _normalize_due_time(current.get("dueTime"))
    existing_offset = current.get("reminderOffsetMinutes")
    due_date = payload.dueDate if "dueDate" in fields_set else current.get("dueDate")
    due_time = (
        payload.dueTime
        if "dueTime" in fields_set
        else (TeamTaskDueTime(**existing_due_time) if existing_due_time else None)
    )
    reminder_enabled = payload.reminderEnabled if "reminderEnabled" in fields_set else bool(current.get("reminderEnabled"))
    reminder_offset = (
        payload.reminderOffsetMinutes
        if "reminderOffsetMinutes" in fields_set
        else (existing_offset if isinstance(existing_offset, int) else None)
    )

    if "dueDate" in fields_set and not payload.dueDate and "dueTime" not in fields_set:
        due_time = None
    if due_time is None and "reminderEnabled" not in fields_set:
        reminder_enabled = False
    if not due_date:
        reminder_enabled = False
        reminder_offset = None

    schedule_payload = TeamTaskScheduleFields(
        dueDate=due_date,
        dueTime=due_time,
        reminderEnabled=reminder_enabled,
        reminderOffsetMinutes=reminder_offset,
    )
    return _schedule_fields(db, route_number, schedule_payload)


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
        "dueDate": data.get("dueDate") if isinstance(data.get("dueDate"), str) else None,
        "dueTime": _normalize_due_time(data.get("dueTime")),
        "timezone": data.get("timezone") if isinstance(data.get("timezone"), str) else None,
        "dueAtMs": _to_millis(data.get("dueAtMs")),
        "reminderEnabled": bool(data.get("reminderEnabled")),
        "reminderOffsetMinutes": data.get("reminderOffsetMinutes") if isinstance(data.get("reminderOffsetMinutes"), int) else None,
        "reminderAtMs": _to_millis(data.get("reminderAtMs")),
        "reminderStatus": str(data.get("reminderStatus") or "none"),
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

    tasks.sort(key=_task_sort_key)
    return {"ok": True, "tasks": tasks}


def _task_sort_key(task: Dict[str, Any]):
    status_completed = task.get("status") == "completed"
    if status_completed:
        return (1, -(task.get("completedAtMs") or task.get("updatedAtMs") or task.get("createdAtMs") or 0))

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    due_at = task.get("dueAtMs")
    if isinstance(due_at, int):
        bucket = 0 if due_at < now_ms else 1
        return (0, bucket, due_at, -(task.get("createdAtMs") or 0))
    return (0, 2, 0, -(task.get("createdAtMs") or 0))


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
    schedule = _schedule_fields(db, route_number, payload)
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
            **schedule,
        },
        merge=False,
    )

    body = _assignment_notification_body(route_number, task_text, owner_display, schedule)
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


@router.patch(
    "/team-tasks/{task_id}",
    responses={400: {"model": ErrorResponse}, 401: {"model": ErrorResponse}, 403: {"model": ErrorResponse}, 404: {"model": ErrorResponse}, 409: {"model": ErrorResponse}},
)
@rate_limit_write
async def update_team_task(
    request: Request,
    task_id: str,
    payload: TeamTaskUpdateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    """Update a Team Task. Owner-only."""
    route_number = payload.routeNumber
    owner_uid = decoded_token["uid"]
    owner_data = await require_route_access(route_number, decoded_token, db)
    if not _is_owner_for_route(owner_data, route_number):
        raise HTTPException(403, "Owner access required")

    task_ref = _team_task_ref(db, route_number, task_id)
    snap = task_ref.get()
    if not snap.exists:
        raise HTTPException(404, "Team task not found")
    current = snap.to_dict() or {}
    if str(current.get("routeNumber") or route_number) != route_number:
        raise HTTPException(404, "Team task not found")

    task_text = _safe_text(payload.task if payload.task is not None else current.get("task"), 500)
    if not task_text:
        raise HTTPException(400, "Task text is required")

    previous_member_uid = str(current.get("teamMemberUid") or "")
    team_member_uid = payload.teamMemberUid.strip() if payload.teamMemberUid is not None else previous_member_uid
    if not team_member_uid or team_member_uid == owner_uid:
        raise HTTPException(409, "Assign the task to an approved team member")

    member_data = _user_doc_data(db, team_member_uid)
    if not member_data or not _is_approved_team_member(member_data, route_number):
        raise HTTPException(403, "Approved team member required")
    member_display = _display_for_user(team_member_uid, member_data)
    schedule = _schedule_from_update_payload(db, route_number, payload, current)

    update_payload = {
        "teamMemberUid": team_member_uid,
        "teamMemberDisplay": member_display,
        "task": task_text,
        "updatedAt": firestore.SERVER_TIMESTAMP,
        **schedule,
    }
    task_ref.set(update_payload, merge=True)

    reassigned = team_member_uid != previous_member_uid
    notification = None
    if reassigned:
        owner_display = _display_for_user(owner_uid, owner_data)
        body = _assignment_notification_body(route_number, task_text, owner_display, schedule)
        notification = _notify_user(
            db,
            uid=team_member_uid,
            title="New team task assigned",
            body=body,
            notification_type="team_task_assigned",
            data={"routeNumber": route_number, "taskId": task_id, "target": "teamTasks"},
            user_data=member_data,
        )

    return {"ok": True, "taskId": task_id, "reassigned": reassigned, "notification": notification}


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
