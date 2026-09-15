#!/usr/bin/env python3
"""Order Adjustment reminder worker.

Runs as a bounded --once CronJob. It claims due draft adjustment reminders
transactionally, sends one user notification, and records the result on the
adjustment document.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import socket
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib import request as urllib_request
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from .order_schedule_v3_release import v3_consumers_allowed
    from .order_send_reminder_refresh import run_pending as refresh_send_reminders
    from .order_adjustment_schedule import (
        WindowResolutionError,
        canonical_source_window,
        meaningful_lines,
    )
except ImportError:
    from order_schedule_v3_release import v3_consumers_allowed
    from order_send_reminder_refresh import run_pending as refresh_send_reminders
    from order_adjustment_schedule import (
        WindowResolutionError,
        canonical_source_window,
        meaningful_lines,
    )


EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
WORKER_ID = f"order-adjustment-reminder-{socket.gethostname()}-{os.getpid()}"
SCHEDULE_RULE_VERSION = 2
ORDER_ADJUSTMENT_REMINDERS_ENABLED = os.environ.get("ORDER_ADJUSTMENT_REMINDERS_ENABLED", "true").lower() == "true"
ORDER_ADJUSTMENT_REMINDER_DRY_RUN = os.environ.get("ORDER_ADJUSTMENT_REMINDER_DRY_RUN", "false").lower() == "true"
ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT", "100"))
ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS", str(10 * 60 * 1000)))
ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS", str(2 * 60 * 1000)))

logger = logging.getLogger("order_adjustment_reminder_worker")


def _send_helpers():
    try:
        from order_forecast.api.order_send_evidence import is_send_order_id, read_send_evidence, order_with_send_evidence
    except ModuleNotFoundError as error:
        if error.name != 'order_forecast':
            raise
        from api.order_send_evidence import is_send_order_id, read_send_evidence, order_with_send_evidence
    return is_send_order_id, read_send_evidence, order_with_send_evidence


def _send_inputs(db, data, source, transaction=None):
    """Only reserved new orders incur these reads. Never trust copied evidence."""
    if not re.fullmatch(r'order-[0-9]{1,10}-send-[a-f0-9]{64}', str(data.get('sourceOrderId') or '')):
        return data, None
    _, read_evidence, _ = _send_helpers()
    try:
        evidence = read_evidence(db, source or {'id': data['sourceOrderId']}, transaction)
    except ValueError:
        evidence = None
    uid = data.get('userId')
    if not isinstance(uid, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', uid):
        return {**data, 'reminderEnabled': False}, evidence
    try:
        from order_forecast.api.order_send_access import read_send_route_access
    except ModuleNotFoundError as error:
        if error.name != 'order_forecast':
            raise
        from api.order_send_access import read_send_route_access
    try:
        access = read_send_route_access(db, transaction, uid, data.get('routeNumber'))
    except Exception as error:
        if getattr(error, 'status_code', None) != 403:
            raise
        return {**data, 'reminderEnabled': False}, evidence
    try:
        raw = access.actor['userSettings']['notifications']['orderAdjustmentSettings']
        if not isinstance(raw, dict):
            raise ValueError('Invalid settings')
        lead = raw.get('reminderLeadMinutes')
        current = {'reminderEnabled': raw.get('reminderEnabled') is True,
                   'cutoffTimeLocal': raw.get('cutoffTimeLocal'),
                   'reminderLeadMinutes': lead if type(lead) is int and lead > 0 else None}
    except (TypeError, KeyError, ValueError):
        current = {'reminderEnabled': False, 'cutoffTimeLocal': None, 'reminderLeadMinutes': None}
    return {**data, **current}, evidence


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


def get_firestore_client(sa_path: str) -> firestore.Client:
    return firestore.Client.from_service_account_json(sa_path)


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


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


_meaningful_lines = meaningful_lines


def _local_date(value: Any, route_timezone: ZoneInfo) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    millis = _to_millis(value)
    if millis is None:
        return None
    return datetime.fromtimestamp(millis / 1000, timezone.utc).astimezone(route_timezone).date()


def _expected_v1_schedule(
    adjustment: Dict[str, Any],
    source_order: Optional[Dict[str, Any]],
) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    timezone_name = str(adjustment.get("timezone") or "UTC")
    try:
        route_timezone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return None, "invalid_timezone"

    source_order_id = str(adjustment.get("sourceOrderId") or "").strip()
    if source_order_id:
        # Source-backed adjustments require server-owned timing authority.
        # This legacy helper is intentionally standalone-only.
        return None, "window_unresolved"
    else:
        adjustment_day = _local_date(adjustment.get("adjustmentDate"), route_timezone)
        if adjustment_day is None:
            return None, "window_unresolved"

    cutoff_text = str(adjustment.get("cutoffTimeLocal") or "").strip()
    try:
        cutoff_hour, cutoff_minute = [int(part) for part in cutoff_text.split(":", 1)]
        if cutoff_hour not in range(24) or cutoff_minute not in range(60):
            raise ValueError
    except (TypeError, ValueError):
        return None, "invalid_cutoff"

    try:
        lead_minutes = int(adjustment.get("reminderLeadMinutes") or 0)
    except (TypeError, ValueError):
        lead_minutes = 0
    if lead_minutes <= 0:
        return None, "invalid_reminder_lead"

    cutoff_local = datetime(
        adjustment_day.year,
        adjustment_day.month,
        adjustment_day.day,
        cutoff_hour,
        cutoff_minute,
        tzinfo=route_timezone,
    )
    cutoff_at_ms = int(cutoff_local.timestamp() * 1000)
    reminder_at_ms = cutoff_at_ms - lead_minutes * 60_000
    return {
        "adjustmentDate": adjustment_day.isoformat(),
        "timezone": timezone_name,
        "cutoffAtMs": cutoff_at_ms,
        "reminderAtMs": reminder_at_ms,
        "reminderScheduledForMs": reminder_at_ms,
    }, None


def expected_schedule_from_fresh_source(
    adjustment: Dict[str, Any],
    source_order: Optional[Dict[str, Any]],
    *, now_ms: Optional[int] = None, send_evidence=None,
) -> tuple[Optional[Dict[str, Any]], str, Optional[str]]:
    source_order_id = str(adjustment.get("sourceOrderId") or "").strip()
    if source_order_id:
        if source_order is None:
            return None, "missing_source_order", "missing_source_order"
        additive = bool(re.fullmatch(r'order-[0-9]{1,10}-send-[a-f0-9]{64}', source_order_id))
        if additive:
            _, _, with_send = _send_helpers()
            try:
                source_order = with_send(source_order, send_evidence)
            except ValueError:
                return None, 'send', 'invalid_send_evidence'
        # Presence of either server authority field permanently opts the source
        # into the trusted path. A malformed/null adjustment anchor must fail
        # closed; it must never fall through to a weaker compatibility calculation.
        if "adjustmentAnchor" in source_order or "scheduledOrder" in source_order:
            stamp = source_order.get('scheduledOrder')
            is_v3 = isinstance(stamp, dict) and type(stamp.get('version')) is int and stamp['version'] == 3
            classification = 'send' if additive else 'v3' if is_v3 else 'v2'
            if is_v3 and (source_order.get('routeNumber') != adjustment.get('routeNumber')
                          or source_order.get('id') != source_order_id):
                return None, classification, 'source_identity_mismatch'
            try:
                lead_minutes = (adjustment.get('reminderLeadMinutes') if is_v3
                                else int(adjustment.get("reminderLeadMinutes") or 0))
                window = canonical_source_window(
                    source_order,
                    cutoff_time_local=str(adjustment.get("cutoffTimeLocal") or "").strip(),
                    reminder_lead_minutes=lead_minutes,
                    now_ms=now_ms,
                    allow_v3=additive or v3_consumers_allowed(adjustment.get('routeNumber')),
                )
            except (TypeError, ValueError, WindowResolutionError) as exc:
                reason = exc.code if isinstance(exc, WindowResolutionError) else "window_unresolved"
                return None, classification, reason
            return {
                "adjustmentDate": window.adjustment_date,
                "timezone": window.timezone,
                "cutoffTimeLocal": str(adjustment.get("cutoffTimeLocal") or "").strip(),
                "reminderLeadMinutes": lead_minutes,
                "cutoffAtMs": window.cutoff_at_ms,
                "reminderAtMs": window.reminder_at_ms,
                "reminderScheduledForMs": window.reminder_at_ms,
            }, classification, None
        return None, "v1_source", "window_unresolved"

    schedule, error = _expected_v1_schedule(adjustment, None)
    return schedule, "v1_standalone", error


def _is_valid_expo_token(token: str) -> bool:
    return bool(token) and (token.startswith("ExponentPushToken[") or token.startswith("ExpoPushToken["))


def _tokens_for_user(user_data: Dict[str, Any]) -> List[str]:
    tokens = user_data.get("fcmTokens") or []
    if not isinstance(tokens, list):
        return []
    return [str(token) for token in tokens if isinstance(token, str) and _is_valid_expo_token(token)]


def _send_expo_push(tokens: List[str], *, title: str, body: str, data: Dict[str, Any]) -> Dict[str, int]:
    valid_tokens = [token for token in tokens if _is_valid_expo_token(token)]
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
    for start in range(0, len(messages), 100):
        batch = messages[start : start + 100]
        payload = json.dumps(batch).encode("utf-8")
        req = urllib_request.Request(
            EXPO_PUSH_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=10) as resp:
                raw = resp.read().decode("utf-8")
                decoded = json.loads(raw) if raw else {}
            results = decoded.get("data") if isinstance(decoded, dict) else None
            if isinstance(results, list):
                success_count = sum(1 for result in results if isinstance(result, dict) and result.get("status") == "ok")
                sent += success_count
                failed += len(results) - success_count
            else:
                sent += len(batch)
        except Exception as exc:
            logger.warning("Expo push batch failed: %s", exc)
            failed += len(batch)
    return {"sent": sent, "failed": failed}


def _user_doc_data(db: firestore.Client, uid: str) -> Dict[str, Any]:
    if not uid:
        return {}
    snap = db.collection("users").document(uid).get()
    if not snap.exists:
        return {}
    return snap.to_dict() or {}


def _write_user_notification(
    db: firestore.Client,
    *,
    uid: str,
    title: str,
    body: str,
    notification_type: str,
    data: Dict[str, Any],
) -> None:
    if not uid:
        return
    db.collection("users").document(uid).collection("notifications").add(
        {
            "title": title,
            "body": body,
            "type": notification_type,
            "data": data,
            "read": False,
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )


def _reminder_body(data: Dict[str, Any]) -> str:
    lines = [
        f"Route: {data.get('routeNumber') or ''}",
    ]
    delivery_date = data.get("targetDeliveryDate") or data.get("sourceOrderExpectedDeliveryDate")
    if delivery_date:
        lines.append(f"Delivery: {delivery_date}")
    cutoff_time = data.get("cutoffTimeLocal")
    if cutoff_time:
        lines.append(f"Cutoff: {cutoff_time}")
    lines.append("Review and send your order adjustment.")
    return "\n".join(lines)


def _pending_query(db: firestore.Client, now_ms: int, limit: int) -> Iterable[Any]:
    return (
        db.collection_group("orderAdjustments")
        .where(filter=FieldFilter("status", "==", "draft"))
        .where(filter=FieldFilter("reminderStatus", "==", "pending"))
        .where(filter=FieldFilter("reminderAtMs", "<=", now_ms))
        .order_by("reminderAtMs")
        .limit(limit)
        .stream()
    )


def _stale_sending_query(db: firestore.Client, cutoff_ms: int, limit: int) -> Iterable[Any]:
    return (
        db.collection_group("orderAdjustments")
        .where(filter=FieldFilter("status", "==", "draft"))
        .where(filter=FieldFilter("reminderStatus", "==", "sending"))
        .where(filter=FieldFilter("reminderClaimedAtMs", "<=", cutoff_ms))
        .order_by("reminderClaimedAtMs")
        .limit(limit)
        .stream()
    )


def _adjustment_ref_from_snapshot(doc: Any) -> Any:
    ref = getattr(doc, "reference", None)
    if ref is None:
        raise RuntimeError("Order adjustment snapshot is missing reference")
    return ref


def _route_from_adjustment_ref(ref: Any) -> Optional[str]:
    raw_path = getattr(ref, "path", "")
    parts = list(raw_path) if isinstance(raw_path, tuple) else str(raw_path).split("/")
    if len(parts) != 4 or parts[0] != "routes" or parts[2] != "orderAdjustments":
        return None
    route = str(parts[1]).strip()
    return route if route.isdigit() and len(route) <= 10 else None


def claim_adjustment_reminder(db: firestore.Client, doc: Any, *, now_ms: int) -> Optional[Dict[str, Any]]:
    adjustment_ref = _adjustment_ref_from_snapshot(doc)
    path_route_number = _route_from_adjustment_ref(adjustment_ref)

    @firestore.transactional
    def _claim(transaction):
        snap = adjustment_ref.get(transaction=transaction)
        if not snap.exists:
            return None
        data = snap.to_dict() or {}
        source_order_id = str(data.get("sourceOrderId") or "").strip()
        route_number = str(data.get("routeNumber") or "").strip()
        if not path_route_number or route_number != path_route_number:
            logger.error(
                "Refusing reminder mutation for route mismatch path_route=%s field_route=%s adjustment=%s",
                path_route_number,
                route_number,
                getattr(snap, "id", ""),
            )
            return None
        source_order = None
        if source_order_id and route_number:
            source_ref = (
                db.collection("routes")
                .document(route_number)
                .collection("orders")
                .document(source_order_id)
            )
            source_snap = source_ref.get(transaction=transaction)
            source_order = source_snap.to_dict() or {} if source_snap.exists else None

        data, send_evidence = _send_inputs(db, data, source_order, transaction)

        status = str(data.get("status") or "draft")
        reminder_status = str(data.get("reminderStatus") or "none")
        claimed_at_ms = _to_millis(data.get("reminderClaimedAtMs")) or 0

        if status != "draft" or not bool(data.get("reminderEnabled")):
            return None
        if not _meaningful_lines(data):
            transaction.update(
                adjustment_ref,
                {
                    "reminderStatus": "skipped",
                    "reminderSkippedAtMs": now_ms,
                    "reminderSkipReason": "no_meaningful_lines",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            return None

        expected_schedule, classification, schedule_error = expected_schedule_from_fresh_source(data, source_order, now_ms=now_ms, send_evidence=send_evidence)
        if expected_schedule is None:
            transaction.update(
                adjustment_ref,
                {
                    "reminderStatus": "skipped",
                    "reminderSkippedAtMs": now_ms,
                    "reminderSkipReason": schedule_error or "window_unresolved",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            return None

        reminder_at_ms = int(expected_schedule["reminderAtMs"])
        cutoff_at_ms = int(expected_schedule["cutoffAtMs"])
        if reminder_at_ms > now_ms:
            transaction.update(
                adjustment_ref,
                {
                    **expected_schedule,
                    "reminderStatus": "pending",
                    "reminderSkippedAtMs": None,
                    "reminderSkipReason": None,
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            return None

        is_pending_due = reminder_status == "pending"
        is_stale_sending = reminder_status == "sending" and claimed_at_ms <= now_ms - ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS
        allowed_until = cutoff_at_ms + (
            ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS if is_stale_sending and classification not in ('v3', 'send') else 0
        )
        if cutoff_at_ms and now_ms >= allowed_until:
            transaction.update(
                adjustment_ref,
                {
                    **expected_schedule,
                    "reminderStatus": "skipped",
                    "reminderSkippedAtMs": now_ms,
                    "reminderSkipReason": "past_cutoff",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            return None
        if not (is_pending_due or is_stale_sending):
            return None
        if not data.get("userId"):
            transaction.update(
                adjustment_ref,
                {
                    "reminderStatus": "skipped",
                    "reminderSkippedAtMs": now_ms,
                    "reminderSkipReason": "missing_user",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
            )
            return None

        attempts = int(data.get("reminderAttemptCount") or 0) + 1
        claim_fields = {
            "reminderStatus": "sending",
            "reminderClaimedAtMs": now_ms,
            "reminderWorkerId": WORKER_ID,
            "reminderAttemptCount": attempts,
            "reminderSkippedAtMs": None,
            "reminderSkipReason": None,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }
        transaction.update(adjustment_ref, {**expected_schedule, **claim_fields})
        claimed = {**data, **expected_schedule, **claim_fields, "id": snap.id}
        claimed["sourceScheduleClassification"] = classification
        claimed["claimAgeMs"] = max(0, now_ms - reminder_at_ms)
        logger.info(
            "Claimed order adjustment reminder route=%s adjustment=%s classification=%s claimAgeMs=%s",
            route_number,
            snap.id,
            classification,
            claimed["claimAgeMs"],
        )
        return {"ref": adjustment_ref, "adjustment": claimed}

    return _claim(db.transaction())


def finish_adjustment_reminder(
    db: firestore.Client,
    adjustment_ref: Any,
    *,
    claim: Dict[str, Any],
    now_ms: int,
    sent: bool,
    push_stats: Dict[str, int],
    error: Optional[str] = None,
) -> bool:
    update = {
        "reminderStatus": "sent" if sent else "failed",
        "reminderSentAt": firestore.SERVER_TIMESTAMP if sent else None,
        "reminderSentAtMs": now_ms if sent else None,
        "reminderFailedAtMs": None if sent else now_ms,
        "reminderPushStats": push_stats,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    }
    if error:
        update["reminderError"] = error[:500]
    expected_claimed_at = _to_millis(claim.get("reminderClaimedAtMs"))
    expected_worker = str(claim.get("reminderWorkerId") or "")
    expected_attempt = int(claim.get("reminderAttemptCount") or 0)

    @firestore.transactional
    def _finish(transaction):
        snap = adjustment_ref.get(transaction=transaction)
        if not snap.exists:
            return False
        current = snap.to_dict() or {}
        if str(current.get("status") or "draft") != "draft":
            return False
        if str(current.get("reminderStatus") or "none") != "sending":
            return False
        if _to_millis(current.get("reminderClaimedAtMs")) != expected_claimed_at:
            return False
        if str(current.get("reminderWorkerId") or "") != expected_worker:
            return False
        if int(current.get("reminderAttemptCount") or 0) != expected_attempt:
            return False
        transaction.update(adjustment_ref, update)
        return True

    return bool(_finish(db.transaction()))


def send_claimed_reminder(db: firestore.Client, claimed: Dict[str, Any], *, now_ms: int) -> bool:
    adjustment_ref = claimed["ref"]
    adjustment = claimed["adjustment"]
    if adjustment.get('sourceScheduleClassification') in ('v3', 'send'):
        # A claim is not permission to send after cutoff or after gate revocation.
        route = adjustment.get('routeNumber')
        source_id = adjustment.get('sourceOrderId')
        if (adjustment.get('sourceScheduleClassification') == 'v3' and not v3_consumers_allowed(route)
                or _route_from_adjustment_ref(adjustment_ref) != route):
            return False
        source_snap = db.collection('routes').document(route).collection('orders').document(source_id).get()
        fresh = source_snap.to_dict() if source_snap.exists else None
        adjustment, evidence = _send_inputs(db, adjustment, fresh)
        schedule, classification, error = expected_schedule_from_fresh_source(adjustment, fresh, now_ms=now_ms, send_evidence=evidence)
        if (error or classification not in ('v3', 'send') or schedule is None or not adjustment.get('reminderEnabled')
                or now_ms < schedule['reminderAtMs'] or now_ms >= schedule['cutoffAtMs']):
            return False
        adjustment = {**adjustment, **schedule}
    uid = str(adjustment.get("userId") or "")
    user_data = _user_doc_data(db, uid)
    title = "Order adjustment reminder"
    body = _reminder_body(adjustment)
    payload = {
        "routeNumber": str(adjustment.get("routeNumber") or ""),
        "adjustmentId": str(adjustment.get("id") or ""),
        "target": "orderAdjustments",
    }

    if ORDER_ADJUSTMENT_REMINDER_DRY_RUN:
        raise RuntimeError("Dry-run must use audit_once and may not enter the send path")

    try:
        _write_user_notification(
            db,
            uid=uid,
            title=title,
            body=body,
            notification_type="order_adjustment_reminder",
            data=payload,
        )
        push_stats = _send_expo_push(
            _tokens_for_user(user_data),
            title=title,
            body=body,
            data={"type": "order_adjustment_reminder", **payload},
        )
        sent = push_stats.get("sent", 0) > 0
        finish_adjustment_reminder(db, adjustment_ref, claim=adjustment, now_ms=now_ms, sent=sent, push_stats=push_stats)
        return sent
    except Exception as exc:
        logger.exception("Failed sending order adjustment reminder")
        finish_adjustment_reminder(db, adjustment_ref, claim=adjustment, now_ms=now_ms, sent=False, push_stats={"sent": 0, "failed": 1}, error=str(exc))
        return False


def _doc_identity(doc: Any) -> str:
    ref = getattr(doc, "reference", None)
    return str(getattr(ref, "path", "") or ref or getattr(doc, "id", ""))


def classify_without_writes(db: firestore.Client, doc: Any, *, now_ms: int) -> Dict[str, Any]:
    data = doc.to_dict() or {}
    source_order_id = str(data.get("sourceOrderId") or "").strip()
    route_number = str(data.get("routeNumber") or "").strip()
    path_route_number = _route_from_adjustment_ref(getattr(doc, "reference", None))
    if not path_route_number or route_number != path_route_number:
        return {
            "id": str(getattr(doc, "id", "")),
            "path": _doc_identity(doc),
            "routeNumber": route_number or None,
            "pathRouteNumber": path_route_number,
            "sourceOrderId": source_order_id or None,
            "classification": "route_mismatch",
            "scheduleError": "route_mismatch",
            "action": "skip_route_mismatch",
            "schedule": None,
            "stamp": None,
        }
    source_order = None
    if source_order_id and route_number:
        source_snap = (
            db.collection("routes").document(route_number)
            .collection("orders").document(source_order_id).get()
        )
        source_order = source_snap.to_dict() or {} if source_snap.exists else None

    data, evidence = _send_inputs(db, data, source_order)
    schedule, classification, schedule_error = expected_schedule_from_fresh_source(data, source_order, now_ms=now_ms, send_evidence=evidence)
    status = str(data.get("status") or "draft")
    reminder_status = str(data.get("reminderStatus") or "none")
    eligible_for_reminder = (
        status == "draft"
        and bool(data.get("reminderEnabled"))
        and _meaningful_lines(data)
    )
    schedule_drift_fields = (
        [key for key, value in schedule.items() if data.get(key) != value]
        if schedule is not None
        else []
    )
    stored_reminder_at_ms = _to_millis(data.get("reminderAtMs"))
    stored_future_schedule_drift = bool(
        classification in {"v2", "v3", "send"}
        and eligible_for_reminder
        and stored_reminder_at_ms is not None
        and stored_reminder_at_ms > now_ms
        and schedule_drift_fields
    )
    action = "ignore"
    if status != "draft" or not bool(data.get("reminderEnabled")):
        action = "ignore"
    elif not _meaningful_lines(data):
        action = "skip_no_meaningful_lines"
    elif schedule is None:
        action = f"skip_{schedule_error or 'window_unresolved'}"
    else:
        reminder_at_ms = int(schedule["reminderAtMs"])
        cutoff_at_ms = int(schedule["cutoffAtMs"])
        claimed_at_ms = _to_millis(data.get("reminderClaimedAtMs")) or 0
        stale_sending = (
            reminder_status == "sending"
            and claimed_at_ms <= now_ms - ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS
        )
        if reminder_at_ms > now_ms:
            action = "patch_future" if any(data.get(key) != value for key, value in schedule.items()) else "wait_future"
        elif now_ms >= cutoff_at_ms + (ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS if stale_sending and classification != 'v3' else 0):
            action = "skip_past_cutoff"
        elif reminder_status == "pending" or stale_sending:
            action = "claim_and_send"
        else:
            action = "ignore_not_claimable"

    stamp = source_order.get("scheduledOrder") if isinstance(source_order, dict) else None
    return {
        "id": str(getattr(doc, "id", "")),
        "path": _doc_identity(doc),
        "routeNumber": route_number,
        "sourceOrderId": source_order_id or None,
        "classification": classification,
        "scheduleError": schedule_error,
        "action": action,
        "eligibleForReminder": eligible_for_reminder,
        "storedReminderAtMs": stored_reminder_at_ms,
        "scheduleDriftFields": schedule_drift_fields,
        "storedFutureScheduleDrift": stored_future_schedule_drift,
        "schedule": schedule,
        "stamp": (
            {"version": stamp.get("version"), "date": stamp.get("date"), "timezone": stamp.get("timezone")}
            if isinstance(stamp, dict) else None
        ),
    }


def audit_once(db: firestore.Client, *, route_number: str, now_ms: Optional[int] = None) -> Dict[str, Any]:
    route = str(route_number or "").strip()
    if not route.isdigit() or len(route) > 10:
        raise ValueError("A 1-10 digit route is required for dry-run audit")
    now = now_ms or _now_ms()
    docs = list(
        db.collection("routes").document(route)
        .collection("orderAdjustments").stream()
    )
    results = [classify_without_writes(db, doc, now_ms=now) for doc in docs]
    actions: Dict[str, int] = {}
    classifications: Dict[str, int] = {}
    for result in results:
        actions[result["action"]] = actions.get(result["action"], 0) + 1
        classifications[result["classification"]] = classifications.get(result["classification"], 0) + 1
        logger.info("[dry-run] %s", json.dumps(result, sort_keys=True, default=str))
    summary = {
        "event": "order_adjustment_reminder_audit",
        "routeNumber": route,
        "selected": len(results),
        "actions": actions,
        "classifications": classifications,
        "remainingDueIds": [result["id"] for result in results if result["action"] == "claim_and_send"],
        "workerId": WORKER_ID,
        "scheduleRuleVersion": SCHEDULE_RULE_VERSION,
        "dryRun": True,
        "results": results,
    }
    logger.info("%s", json.dumps(summary, sort_keys=True, default=str))
    return summary


def run_once(db: firestore.Client, *, now_ms: Optional[int] = None, limit: int = ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT) -> Dict[str, Any]:
    if ORDER_ADJUSTMENT_REMINDER_DRY_RUN:
        raise RuntimeError('Dry-run must use audit_once and may not mutate reminders')
    if not ORDER_ADJUSTMENT_REMINDERS_ENABLED:
        logger.info("ORDER_ADJUSTMENT_REMINDERS_ENABLED=false; skipping")
        return {"claimed": 0, "sent": 0, "failed": 0, "skipped": 0}

    now = now_ms or _now_ms()
    refresh_results = refresh_send_reminders(db, now_ms=now)
    cutoff = now - ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS
    docs = list(_pending_query(db, now, limit))
    remaining = max(0, limit - len(docs))
    if remaining:
        docs.extend(list(_stale_sending_query(db, cutoff, remaining)))

    stats: Dict[str, Any] = {
        "event": "order_adjustment_reminder_run",
        "settingsRefresh": refresh_results,
        "selected": len(docs),
        "claimed": 0,
        "sent": 0,
        "failed": 0,
        "skipped": 0,
        "remainingDueIds": [],
        "batchLimitReached": len(docs) >= limit,
        "workerId": WORKER_ID,
        "scheduleRuleVersion": SCHEDULE_RULE_VERSION,
    }
    seen: set[str] = set()
    for doc in docs:
        doc_key = _doc_identity(doc)
        if doc_key in seen:
            continue
        seen.add(doc_key)
        # Production samples the clock per stage. Explicit now_ms remains the
        # deterministic test clock, not a request-controlled runtime setting.
        claimed = claim_adjustment_reminder(db, doc, now_ms=now if now_ms is not None else _now_ms())
        if not claimed:
            stats["skipped"] += 1
            continue
        stats["claimed"] += 1
        if send_claimed_reminder(db, claimed, now_ms=now if now_ms is not None else _now_ms()):
            stats["sent"] += 1
        else:
            stats["failed"] += 1
    stats["remainingDueIds"] = [
        str(getattr(doc, "id", "")) for doc in _pending_query(db, now, limit)
    ]
    logger.info("%s", json.dumps(stats, sort_keys=True))
    return stats


def run_worker(sa_path: str, *, dry_run: bool = False, route_number: Optional[str] = None) -> Dict[str, Any]:
    db = get_firestore_client(sa_path)
    stats = audit_once(db, route_number=route_number or "") if dry_run else run_once(db)
    logger.info("Order adjustment reminder worker stats: %s", stats)
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Order Adjustment reminder worker")
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    parser.add_argument("--once", action="store_true", help="Run one reminder scan/send cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="Classify one route without writes or sends")
    parser.add_argument("--route", help="Route to audit; allowed only with --dry-run")
    args = parser.parse_args()
    if args.route and not args.dry_run:
        parser.error("--route is allowed only with --dry-run")
    if (args.dry_run or ORDER_ADJUSTMENT_REMINDER_DRY_RUN) and not args.route:
        parser.error("dry-run requires an explicit --route")
    return args


def main() -> int:
    configure_logging()
    args = parse_args()
    logger.info(json.dumps({
        "event": "order_adjustment_reminder_worker_start",
        "scheduleRuleVersion": SCHEDULE_RULE_VERSION,
        "dryRun": bool(args.dry_run or ORDER_ADJUSTMENT_REMINDER_DRY_RUN),
        "batchLimit": ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT,
        "claimTtlMs": ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS,
        "cutoffGraceMs": ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS,
    }, sort_keys=True))
    if not args.once:
        logger.error("This worker is CronJob-bounded and requires --once")
        return 2
    if not os.path.exists(args.serviceAccount):
        logger.error("Service account path does not exist: %s", args.serviceAccount)
        return 2
    run_worker(
        args.serviceAccount,
        dry_run=bool(args.dry_run or ORDER_ADJUSTMENT_REMINDER_DRY_RUN),
        route_number=args.route,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
