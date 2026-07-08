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
import socket
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib import request as urllib_request

from google.cloud import firestore


EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
WORKER_ID = f"order-adjustment-reminder-{socket.gethostname()}-{os.getpid()}"
ORDER_ADJUSTMENT_REMINDERS_ENABLED = os.environ.get("ORDER_ADJUSTMENT_REMINDERS_ENABLED", "true").lower() == "true"
ORDER_ADJUSTMENT_REMINDER_DRY_RUN = os.environ.get("ORDER_ADJUSTMENT_REMINDER_DRY_RUN", "false").lower() == "true"
ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT", "100"))
ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS", str(10 * 60 * 1000)))
ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS = int(os.environ.get("ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS", str(2 * 60 * 1000)))

logger = logging.getLogger("order_adjustment_reminder_worker")


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
        .where("status", "==", "draft")
        .where("reminderStatus", "==", "pending")
        .where("reminderAtMs", "<=", now_ms)
        .order_by("reminderAtMs")
        .limit(limit)
        .stream()
    )


def _stale_sending_query(db: firestore.Client, cutoff_ms: int, limit: int) -> Iterable[Any]:
    return (
        db.collection_group("orderAdjustments")
        .where("status", "==", "draft")
        .where("reminderStatus", "==", "sending")
        .where("reminderClaimedAtMs", "<=", cutoff_ms)
        .order_by("reminderClaimedAtMs")
        .limit(limit)
        .stream()
    )


def _adjustment_ref_from_snapshot(doc: Any) -> Any:
    ref = getattr(doc, "reference", None)
    if ref is None:
        raise RuntimeError("Order adjustment snapshot is missing reference")
    return ref


def claim_adjustment_reminder(db: firestore.Client, doc: Any, *, now_ms: int) -> Optional[Dict[str, Any]]:
    adjustment_ref = _adjustment_ref_from_snapshot(doc)

    @firestore.transactional
    def _claim(transaction):
        snap = adjustment_ref.get(transaction=transaction)
        if not snap.exists:
            return None
        data = snap.to_dict() or {}
        status = str(data.get("status") or "draft")
        reminder_status = str(data.get("reminderStatus") or "none")
        reminder_at_ms = _to_millis(data.get("reminderAtMs"))
        cutoff_at_ms = _to_millis(data.get("cutoffAtMs"))
        claimed_at_ms = _to_millis(data.get("reminderClaimedAtMs")) or 0

        is_pending_due = reminder_status == "pending" and reminder_at_ms is not None and reminder_at_ms <= now_ms
        is_stale_sending = reminder_status == "sending" and claimed_at_ms <= now_ms - ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS
        if status != "draft" or not bool(data.get("reminderEnabled")):
            return None
        if cutoff_at_ms and now_ms > cutoff_at_ms + ORDER_ADJUSTMENT_REMINDER_CUTOFF_GRACE_MS:
            transaction.update(
                adjustment_ref,
                {
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
        transaction.update(
            adjustment_ref,
            {
                "reminderStatus": "sending",
                "reminderClaimedAtMs": now_ms,
                "reminderWorkerId": WORKER_ID,
                "reminderAttemptCount": attempts,
                "updatedAt": firestore.SERVER_TIMESTAMP,
            },
        )
        claimed = dict(data)
        claimed["id"] = snap.id
        claimed["reminderAttemptCount"] = attempts
        return {"ref": adjustment_ref, "adjustment": claimed}

    return _claim(db.transaction())


def finish_adjustment_reminder(
    adjustment_ref: Any,
    *,
    now_ms: int,
    sent: bool,
    push_stats: Dict[str, int],
    error: Optional[str] = None,
) -> None:
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
    adjustment_ref.update(update)


def send_claimed_reminder(db: firestore.Client, claimed: Dict[str, Any], *, now_ms: int) -> bool:
    adjustment_ref = claimed["ref"]
    adjustment = claimed["adjustment"]
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
        logger.info("[dry-run] Would send order adjustment reminder route=%s adjustment=%s uid=%s", payload["routeNumber"], payload["adjustmentId"], uid)
        finish_adjustment_reminder(adjustment_ref, now_ms=now_ms, sent=True, push_stats={"sent": 0, "failed": 0, "dryRun": 1})
        return True

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
        finish_adjustment_reminder(adjustment_ref, now_ms=now_ms, sent=sent, push_stats=push_stats)
        return sent
    except Exception as exc:
        logger.exception("Failed sending order adjustment reminder")
        finish_adjustment_reminder(adjustment_ref, now_ms=now_ms, sent=False, push_stats={"sent": 0, "failed": 1}, error=str(exc))
        return False


def run_once(db: firestore.Client, *, now_ms: Optional[int] = None, limit: int = ORDER_ADJUSTMENT_REMINDER_BATCH_LIMIT) -> Dict[str, int]:
    if not ORDER_ADJUSTMENT_REMINDERS_ENABLED:
        logger.info("ORDER_ADJUSTMENT_REMINDERS_ENABLED=false; skipping")
        return {"claimed": 0, "sent": 0, "failed": 0, "skipped": 0}

    now = now_ms or _now_ms()
    cutoff = now - ORDER_ADJUSTMENT_REMINDER_CLAIM_TTL_MS
    docs = list(_pending_query(db, now, limit))
    remaining = max(0, limit - len(docs))
    if remaining:
        docs.extend(list(_stale_sending_query(db, cutoff, remaining)))

    stats = {"claimed": 0, "sent": 0, "failed": 0, "skipped": 0}
    seen: set[str] = set()
    for doc in docs:
        ref = getattr(doc, "reference", None)
        doc_key = str(getattr(ref, "path", "") or ref or getattr(doc, "id", ""))
        if doc_key in seen:
            continue
        seen.add(doc_key)
        claimed = claim_adjustment_reminder(db, doc, now_ms=now)
        if not claimed:
            stats["skipped"] += 1
            continue
        stats["claimed"] += 1
        if send_claimed_reminder(db, claimed, now_ms=now):
            stats["sent"] += 1
        else:
            stats["failed"] += 1
    return stats


def run_worker(sa_path: str) -> Dict[str, int]:
    db = get_firestore_client(sa_path)
    stats = run_once(db)
    logger.info("Order adjustment reminder worker stats: %s", stats)
    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Order Adjustment reminder worker")
    parser.add_argument("--serviceAccount", required=True, help="Path to Firebase service account JSON")
    parser.add_argument("--once", action="store_true", help="Run one reminder scan/send cycle and exit")
    return parser.parse_args()


def main() -> int:
    configure_logging()
    args = parse_args()
    if not args.once:
        logger.error("This worker is CronJob-bounded and requires --once")
        return 2
    if not os.path.exists(args.serviceAccount):
        logger.error("Service account path does not exist: %s", args.serviceAccount)
        return 2
    run_worker(args.serviceAccount)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
