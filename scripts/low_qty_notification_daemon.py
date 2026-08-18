"""One-shot low-quantity notification worker.

Claims due route slots from PostgreSQL before loading PCF inventory. Firestore
remains the due-time authority for route ownership, device tokens, and PCF data.

Usage:
    python scripts/low_qty_notification_daemon.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import json
import os
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Any, Tuple

import requests
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

try:
    from .low_qty_notification_store import (
        ClaimedExecution,
        EnabledPreference,
        begin_dispatch,
        claim_next_due,
        complete_claim,
        disable_claimed_preference,
        load_preference_run_counts,
        mark_retryable,
        mark_zero_ticket_retryable,
        reconcile_complete_enabled_snapshot,
        record_accepted_tickets,
        store_claim_payload,
        upsert_enabled_preference,
    )
    from .low_qty_schedule import next_scheduled_instant, parse_reminder_minute, validate_timezone
    from .low_quantity_loader import get_items_for_order_date
except ImportError:
    from low_qty_notification_store import (
        ClaimedExecution,
        EnabledPreference,
        begin_dispatch,
        claim_next_due,
        complete_claim,
        disable_claimed_preference,
        load_preference_run_counts,
        mark_retryable,
        mark_zero_ticket_retryable,
        reconcile_complete_enabled_snapshot,
        record_accepted_tickets,
        store_claim_payload,
        upsert_enabled_preference,
    )
    from low_qty_schedule import next_scheduled_instant, parse_reminder_minute, validate_timezone
    from low_quantity_loader import get_items_for_order_date

WORKER_ID = f"low-qty-notif-{socket.gethostname()}-{os.getpid()}"
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
EXPO_RECEIPTS_URL = "https://exp.host/--/api/v2/push/getReceipts"
LOW_QTY_NOTIFICATION_DRY_RUN = os.environ.get("LOW_QTY_NOTIFICATION_DRY_RUN", "false").lower() == "true"
DEFAULT_ONCE_LATE_TOLERANCE_MINUTES = int(
    os.environ.get("LOW_QTY_NOTIFICATION_ONCE_LATE_TOLERANCE_MINUTES", "20")
)

def _notifications_enabled() -> bool:
    """Read the opt-in at run time so absent configuration always fails closed."""
    return os.environ.get("LOW_QTY_NOTIFICATIONS_ENABLED", "false").strip().lower() == "true"


def _recipient_source() -> str:
    source = os.environ.get("LOW_QTY_RECIPIENT_SOURCE", "").strip().lower()
    if source not in {"firebase", "postgres"}:
        raise RuntimeError("LOW_QTY_RECIPIENT_SOURCE must be explicitly set to firebase or postgres")
    return source


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _order_reminder_settings(user_data: Dict[str, Any]) -> Dict[str, Any]:
    settings = user_data.get("userSettings")
    if not isinstance(settings, dict):
        return {}
    notifications = settings.get("notifications")
    if not isinstance(notifications, dict):
        return {}
    reminder = notifications.get("orderReminders")
    return reminder if isinstance(reminder, dict) else {}

def _allowed_routes() -> set[str] | None:
    raw = os.environ.get("ROUTESPARK_ALLOWED_ROUTES", "").strip()
    if not raw:
        return None
    values = {item.strip() for item in raw.split(",") if item.strip()}
    return values or None


def _route_allowed(route_number: str | None) -> bool:
    if not route_number:
        return False
    allowed = _allowed_routes()
    return True if allowed is None else str(route_number) in allowed


def get_firestore_client(sa_path: str) -> firestore.Client:
    """Initialize Firestore client."""
    return firestore.Client.from_service_account_json(sa_path)


def get_route_owner_strict(db: firestore.Client, route_number: str) -> Optional[str]:
    """Resolve the complete owner chain and propagate lookup failures."""
    lookups = (
        ("routes", ("ownerUid", "userId")),
        ("routeEntitlements", ("ownerUid",)),
        ("routeNumbers", ("userId", "userID")),
    )
    for collection_name, owner_fields in lookups:
        snapshot = db.collection(collection_name).document(str(route_number)).get()
        if not snapshot.exists:
            continue
        data = snapshot.to_dict() or {}
        for owner_field in owner_fields:
            owner_uid = data.get(owner_field)
            if isinstance(owner_uid, str) and owner_uid.strip():
                return owner_uid.strip()
    return None


def get_route_owner(db: firestore.Client, route_number: str) -> Optional[str]:
    """Compatibility wrapper that fails closed for legacy preview callers."""
    try:
        return get_route_owner_strict(db, route_number)
    except Exception as e:
        print(f"    [route] Error getting owner for route {route_number}: {e}")
    return None


def get_fcm_tokens(db: firestore.Client, user_id: str) -> List[str]:
    """Get FCM tokens for a user."""
    user_doc = db.collection("users").document(user_id).get()
    if not user_doc.exists:
        return []
    user_data = user_doc.to_dict() or {}
    tokens = user_data.get("fcmTokens")
    if not isinstance(tokens, list):
        return []
    return [token for token in tokens if isinstance(token, str)]


def is_valid_expo_token(token: str) -> bool:
    """Check if token is a valid Expo push token.
    
    Accepts both ExponentPushToken[...] and ExpoPushToken[...] formats.
    """
    if not isinstance(token, str):
        return False
    for prefix in ("ExponentPushToken[", "ExpoPushToken["):
        if token.startswith(prefix) and token.endswith("]"):
            value = token[len(prefix):-1]
            return bool(value and not any(character.isspace() for character in value))
    return False


@dataclass
class PushDeliveryResult:
    valid_token_count: int = 0
    delivered_count: int = 0
    pending_count: int = 0
    failed_count: int = 0
    invalid_tokens: List[str] = field(default_factory=list)
    accepted_ticket_ids: List[str] = field(default_factory=list)
    ambiguous: bool = False

    @property
    def successful(self) -> bool:
        # A receipt can remain pending beyond this short-lived CronJob. Keep
        # accepted pending tickets deduplicated to avoid duplicate pushes.
        return self.delivered_count > 0 or self.pending_count > 0

    @property
    def definitive_no_valid_token(self) -> bool:
        return bool(
            self.valid_token_count
            and not self.ambiguous
            and self.delivered_count == 0
            and self.pending_count == 0
            and len(set(self.invalid_tokens)) == self.valid_token_count
        )


@dataclass
class NotificationRunCounters:
    preferences_enabled: int = 0
    preferences_due: int = 0
    claims_acquired: int = 0
    claims_recovered: int = 0
    claims_skipped: int = 0
    pcf_evaluations: int = 0
    dispatches_started: int = 0
    accepted_tickets: int = 0
    sent: int = 0
    retryable: int = 0
    ownership_mismatches: int = 0
    closed: Dict[str, int] = field(default_factory=dict)

    def record_closed(self, reason: str) -> None:
        self.closed[reason] = self.closed.get(reason, 0) + 1

    def as_dict(self) -> Dict[str, Any]:
        return {
            "preferences_enabled": self.preferences_enabled,
            "preferences_due": self.preferences_due,
            "claims_acquired": self.claims_acquired,
            "claims_recovered": self.claims_recovered,
            "claims_skipped": self.claims_skipped,
            "pcf_evaluations": self.pcf_evaluations,
            "dispatches_started": self.dispatches_started,
            "accepted_tickets": self.accepted_tickets,
            "sent": self.sent,
            "retryable": self.retryable,
            "ownership_mismatches": self.ownership_mismatches,
            "closed": dict(sorted(self.closed.items())),
        }


def _receipt_invalidates_token(receipt: Dict[str, Any]) -> bool:
    details = receipt.get("details") or {}
    if details.get("error") == "DeviceNotRegistered":
        return True
    apns = details.get("apns") or {}
    return apns.get("reason") == "BadDeviceToken"


def _fetch_push_receipts(ticket_tokens: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
    if not ticket_tokens:
        return {}

    ticket_ids = [ticket_id for ticket_id, _token in ticket_tokens]
    response = requests.post(
        EXPO_RECEIPTS_URL,
        json={"ids": ticket_ids},
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    receipts = payload.get("data")
    return receipts if isinstance(receipts, dict) else {}


def remove_invalid_push_tokens(
    db: firestore.Client,
    user_id: str,
    invalid_tokens: List[str],
) -> None:
    unique_tokens = list(dict.fromkeys(invalid_tokens))
    if not unique_tokens:
        return
    db.collection("users").document(user_id).update({
        "fcmTokens": firestore.ArrayRemove(unique_tokens),
    })
    print(f"    [push] Removed {len(unique_tokens)} invalid token(s)")


def send_push_notification(
    fcm_tokens: List[str],
    title: str,
    body: str,
    data: Dict,
    *,
    accepted_ticket_callback: Optional[Callable[[List[str]], bool]] = None,
) -> PushDeliveryResult:
    """Send push notification via Expo Push API.
    
    Args:
        fcm_tokens: List of Expo push tokens
        title: Notification title
        body: Notification body
        data: Data payload for deep linking
    
    Returns:
        Delivery result based on Expo receipts when available.
    """
    delivery = PushDeliveryResult()
    if not fcm_tokens:
        return delivery
    
    messages = []
    for token in dict.fromkeys(fcm_tokens):
        if not is_valid_expo_token(token):
            continue
        messages.append({
            "to": token,
            "title": title,
            "body": body,
            "data": data,
            "sound": "default",
            "priority": "high",
        })
    
    if not messages:
        return delivery
    delivery.valid_token_count = len(messages)
    
    # Expo Push API limits batches to 100 messages
    BATCH_SIZE = 100
    ticket_tokens: List[Tuple[str, str]] = []
    
    for i in range(0, len(messages), BATCH_SIZE):
        batch = messages[i:i + BATCH_SIZE]
        try:
            response = requests.post(
                EXPO_PUSH_URL,
                json=batch,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            response.raise_for_status()
            response_payload = response.json()
            tickets = response_payload.get("data") if isinstance(response_payload, dict) else None
            if not isinstance(tickets, list) or len(tickets) != len(batch):
                delivery.ambiguous = True
                print("    [push] Ambiguous Expo response: ticket count did not match request")
                break
            for message, ticket in zip(batch, tickets):
                token = message["to"]
                if isinstance(ticket, dict) and ticket.get("status") == "ok" and ticket.get("id"):
                    ticket_tokens.append((ticket["id"], token))
                    continue
                delivery.failed_count += 1
                if isinstance(ticket, dict) and ticket.get("details", {}).get("error") == "DeviceNotRegistered":
                    delivery.invalid_tokens.append(token)
        except Exception as e:
            print(f"    [push] Ambiguous batch error: {e}")
            delivery.ambiguous = True
            break

    delivery.accepted_ticket_ids = [ticket_id for ticket_id, _token in ticket_tokens]
    if delivery.accepted_ticket_ids and accepted_ticket_callback is not None:
        try:
            if not accepted_ticket_callback(delivery.accepted_ticket_ids):
                delivery.ambiguous = True
                print("    [push] Could not persist accepted ticket IDs")
        except Exception as error:
            delivery.ambiguous = True
            print(f"    [push] Could not persist accepted ticket IDs: {error}")

    receipts: Dict[str, Dict[str, Any]] = {}
    if ticket_tokens and not delivery.ambiguous:
        try:
            receipts = _fetch_push_receipts(ticket_tokens)
        except Exception as error:
            # Ticket acceptance is authoritative. An unavailable receipt is
            # pending, not a reason to resend.
            print(f"    [push] Immediate receipt check unavailable: {error}")
    for ticket_id, token in ticket_tokens:
        receipt = receipts.get(ticket_id)
        if not receipt:
            delivery.pending_count += 1
        elif receipt.get("status") == "ok":
            delivery.delivered_count += 1
        else:
            delivery.failed_count += 1
            if _receipt_invalidates_token(receipt):
                delivery.invalid_tokens.append(token)

    delivery.invalid_tokens = list(dict.fromkeys(delivery.invalid_tokens))
    print(
        "    [push] "
        f"{delivery.delivered_count} delivered, "
        f"{delivery.pending_count} pending, "
        f"{delivery.failed_count} failed, "
        f"ambiguous={str(delivery.ambiguous).lower()}"
    )
    return delivery


def sync_firebase_recipient_snapshot(
    db: firestore.Client,
    *,
    now_utc: datetime,
) -> dict[str, int]:
    """Materialize the explicit Firebase rollback source into the same ledger authority."""
    query = db.collection("users").where(
        filter=FieldFilter(
            "userSettings.notifications.orderReminders.enabled",
            "==",
            True,
        )
    )
    snapshots = list(query.stream())
    preferences: List[EnabledPreference] = []
    for snapshot in snapshots:
        data = snapshot.to_dict() or {}
        profile = data.get("profile", {}) if isinstance(data.get("profile"), dict) else {}
        order_reminders = _order_reminder_settings(data)
        route_number = str(profile.get("currentRoute") or profile.get("routeNumber") or "").strip()
        if not route_number or order_reminders.get("enabled") is not True:
            continue

        owner_uid = get_route_owner_strict(db, route_number)
        if not owner_uid or owner_uid != snapshot.id:
            continue
        try:
            reminder_minute = parse_reminder_minute(order_reminders.get("time"))
            timezone_name = validate_timezone(profile.get("timezone"))
            _local_date, next_due_at = next_scheduled_instant(
                reminder_minute,
                timezone_name,
                after_utc=now_utc,
            )
            preferences.append(
                EnabledPreference(
                    route_number=route_number,
                    owner_uid=owner_uid,
                    reminder_minute_local=reminder_minute,
                    timezone_name=timezone_name,
                    next_due_at=next_due_at,
                )
            )
        except ValueError as exc:
            print(f"  [source:firebase] Invalid reminder for route {route_number}: {exc}")

    result = reconcile_complete_enabled_snapshot(preferences)
    print(
        "  [source:firebase] Preference snapshot committed: "
        f"enabled={result['enabled']} disabled={result['disabled']}"
    )
    return result


def _current_claim_owner(db: firestore.Client, claim: ClaimedExecution) -> Optional[str]:
    return get_route_owner_strict(db, claim.route_number)


def _reconcile_changed_claim_owner(
    db: firestore.Client,
    claim: ClaimedExecution,
    current_owner: Optional[str],
    *,
    now_utc: datetime,
) -> None:
    """Refresh one stale route preference without a collection reconciliation."""
    if not current_owner:
        disable_claimed_preference(claim, "unresolved_owner")
        return

    owner_snapshot = db.collection("users").document(current_owner).get()
    if not owner_snapshot.exists:
        disable_claimed_preference(claim, "owner_not_eligible")
        return
    owner_data = owner_snapshot.to_dict() or {}
    profile = owner_data.get("profile", {}) if isinstance(owner_data.get("profile"), dict) else {}
    owner_route = str(profile.get("currentRoute") or profile.get("routeNumber") or "").strip()
    reminder = _order_reminder_settings(owner_data)
    if owner_route != claim.route_number or reminder.get("enabled") is not True:
        disable_claimed_preference(claim, "owner_not_eligible")
        return

    try:
        reminder_minute = parse_reminder_minute(reminder.get("time"))
        timezone_name = validate_timezone(profile.get("timezone"))
        _local_date, next_due_at = next_scheduled_instant(
            reminder_minute,
            timezone_name,
            after_utc=now_utc,
        )
    except ValueError:
        disable_claimed_preference(claim, "invalid_owner_settings")
        return

    upsert_enabled_preference(
        EnabledPreference(
            route_number=claim.route_number,
            owner_uid=current_owner,
            reminder_minute_local=reminder_minute,
            timezone_name=timezone_name,
            next_due_at=next_due_at,
        )
    )


def _close_changed_owner(
    db: firestore.Client,
    claim: ClaimedExecution,
    current_owner: Optional[str],
    *,
    now_utc: datetime,
    counters: Optional[NotificationRunCounters] = None,
) -> int:
    try:
        _reconcile_changed_claim_owner(
            db,
            claim,
            current_owner,
            now_utc=now_utc,
        )
        if not complete_claim(
            claim,
            status="closed",
            reason="owner_changed",
            now_utc=now_utc,
        ):
            print(f"  [claim] Lost owner-change completion for route {claim.route_number}")
            return 1
    except Exception as exc:
        mark_retryable(claim, error=f"owner_reconcile_failed:{type(exc).__name__}")
        print(f"  [claim] Owner reconciliation failed for route {claim.route_number}: {exc}")
        return 1
    if counters is not None:
        counters.ownership_mismatches += 1
        counters.record_closed("owner_changed")
    print(f"  [claim] Closed stale owner for route {claim.route_number}")
    return 0


def _process_claim(
    db: firestore.Client,
    claim: ClaimedExecution,
    counters: Optional[NotificationRunCounters] = None,
) -> int:
    """Process one claimed slot; return one on a safely recorded retryable failure."""
    now_utc = datetime.now(timezone.utc)
    if not _route_allowed(claim.route_number):
        completed = complete_claim(
            claim,
            status="closed",
            reason="policy_excluded",
            now_utc=now_utc,
        )
        if not completed:
            print(f"  [claim] Lost policy completion for route {claim.route_number}")
            return 1
        if counters is not None:
            counters.record_closed("policy_excluded")
        print(f"  [claim] Closed policy-excluded route {claim.route_number}")
        return 0

    try:
        current_owner = _current_claim_owner(db, claim)
        if current_owner != claim.owner_uid:
            return _close_changed_owner(
                db,
                claim,
                current_owner,
                now_utc=now_utc,
                counters=counters,
            )
    except Exception as exc:
        mark_retryable(claim, error=f"owner_lookup_failed:{type(exc).__name__}")
        print(f"  [claim] Owner lookup failed for route {claim.route_number}: {exc}")
        return 1

    payload = claim.computed_payload
    saps = claim.computed_saps
    if payload is None or saps is None:
        if counters is not None:
            counters.pcf_evaluations += 1
        try:
            items = get_items_for_order_date(
                db,
                claim.route_number,
                claim.scheduled_local_date.isoformat(),
                resolved_timezone=claim.timezone_name,
            )
        except Exception as exc:
            mark_retryable(claim, error=f"pcf_failed:{type(exc).__name__}")
            if counters is not None:
                counters.retryable += 1
            print(f"  [claim] PCF evaluation failed for route {claim.route_number}: {exc}")
            return 1

        if not items:
            completed = complete_claim(
                claim,
                status="closed",
                reason="no_items",
                now_utc=datetime.now(timezone.utc),
            )
            if not completed:
                print(f"  [claim] Lost empty-result completion for route {claim.route_number}")
                return 1
            if counters is not None:
                counters.record_closed("no_items")
            print(f"  [claim] No low-quantity items for route {claim.route_number}")
            return 0

        saps = sorted({str(item.sap) for item in items})
        item_count = len(items)
        item_label = "item" if item_count == 1 else "items"
        verb = "needs" if item_count == 1 else "need"
        payload = {
            "title": "Low Stock Alert",
            "body": f"{item_count} {item_label} {verb} to be ordered today",
            "data": {
                "type": "low_quantity",
                "routeNumber": claim.route_number,
                "orderDate": claim.scheduled_local_date.isoformat(),
                "saps": saps,
            },
        }
        if not store_claim_payload(claim, payload=payload, saps=saps):
            print(f"  [claim] Lost payload ownership for route {claim.route_number}")
            return 1

    try:
        current_owner = _current_claim_owner(db, claim)
        if current_owner != claim.owner_uid:
            return _close_changed_owner(
                db,
                claim,
                current_owner,
                now_utc=datetime.now(timezone.utc),
                counters=counters,
            )
        tokens = get_fcm_tokens(db, claim.owner_uid)
    except Exception as exc:
        mark_retryable(claim, error=f"token_lookup_failed:{type(exc).__name__}")
        if counters is not None:
            counters.retryable += 1
        print(f"  [claim] Token lookup failed for route {claim.route_number}: {exc}")
        return 1

    valid_tokens = [token for token in dict.fromkeys(tokens) if is_valid_expo_token(token)]
    if not valid_tokens:
        completed = complete_claim(
            claim,
            status="closed",
            reason="no_valid_token",
            now_utc=datetime.now(timezone.utc),
        )
        if not completed:
            print(f"  [claim] Lost no-token completion for route {claim.route_number}")
            return 1
        if counters is not None:
            counters.record_closed("no_valid_token")
        print(f"  [claim] No valid notification token for route {claim.route_number}")
        return 0

    dispatch_at = datetime.now(timezone.utc)
    if not begin_dispatch(claim, now_utc=dispatch_at):
        print(f"  [claim] Lost dispatch ownership for route {claim.route_number}")
        return 1
    if counters is not None:
        counters.dispatches_started += 1

    delivery = send_push_notification(
        valid_tokens,
        str(payload["title"]),
        str(payload["body"]),
        dict(payload["data"]),
        accepted_ticket_callback=lambda ticket_ids: record_accepted_tickets(claim, ticket_ids),
    )
    if counters is not None:
        counters.accepted_tickets += len(delivery.accepted_ticket_ids)
    if delivery.invalid_tokens:
        try:
            remove_invalid_push_tokens(db, claim.owner_uid, delivery.invalid_tokens)
        except Exception as exc:
            print(f"  [claim] Could not remove invalid token(s): {exc}")

    completion_at = datetime.now(timezone.utc)
    if delivery.ambiguous:
        completed = complete_claim(
            claim,
            status="closed",
            reason="delivery_unknown",
            now_utc=completion_at,
            accepted_ticket_ids=delivery.accepted_ticket_ids,
            error="ambiguous_expo_dispatch",
        )
        if not completed:
            print(f"  [claim] Lost ambiguous completion for route {claim.route_number}")
        elif counters is not None:
            counters.record_closed("delivery_unknown")
        print(f"  [claim] Closed ambiguous dispatch for route {claim.route_number}")
        return 1
    if delivery.definitive_no_valid_token:
        completed = complete_claim(
            claim,
            status="closed",
            reason="no_valid_token",
            now_utc=completion_at,
            accepted_ticket_ids=delivery.accepted_ticket_ids,
        )
        if not completed:
            print(f"  [claim] Lost invalid-token completion for route {claim.route_number}")
            return 1
        if counters is not None:
            counters.record_closed("no_valid_token")
        print(f"  [claim] All tokens invalid for route {claim.route_number}")
        return 0
    if delivery.accepted_ticket_ids:
        completed = complete_claim(
            claim,
            status="sent",
            reason="accepted",
            now_utc=completion_at,
            accepted_ticket_ids=delivery.accepted_ticket_ids,
        )
        if not completed:
            print(f"  [claim] Lost sent completion for route {claim.route_number}")
            return 1
        if counters is not None:
            counters.sent += 1
        print(f"  [claim] Sent notification for route {claim.route_number}: {len(saps)} SAP(s)")
        return 0

    if not mark_zero_ticket_retryable(claim, error="expo_accepted_zero_tickets"):
        print(f"  [claim] Could not record zero-ticket retry for route {claim.route_number}")
        return 1
    if counters is not None:
        counters.retryable += 1
    print(f"  [claim] Expo accepted zero tickets for route {claim.route_number}; retry deferred")
    return 1


def check_and_notify_postgres(
    db: firestore.Client,
    *,
    late_tolerance_minutes: int = DEFAULT_ONCE_LATE_TOLERANCE_MINUTES,
) -> int:
    """Claim and process due PostgreSQL preferences in a bounded run."""
    failure_count = 0
    processed = 0
    counters = NotificationRunCounters()
    initial_counts = load_preference_run_counts(now_utc=datetime.now(timezone.utc))
    counters.preferences_enabled = initial_counts["enabled"]
    counters.preferences_due = initial_counts["due"]
    batch_limit = min(_positive_int_env("LOW_QTY_NOTIFICATION_BATCH_LIMIT", 100), 500)
    lease_seconds = _positive_int_env("LOW_QTY_CLAIM_LEASE_SECONDS", 300)
    max_attempts = _positive_int_env("LOW_QTY_MAX_ATTEMPTS", 3)
    for _ in range(batch_limit):
        claim = claim_next_due(
            now_utc=datetime.now(timezone.utc),
            lease_seconds=lease_seconds,
            late_tolerance_minutes=late_tolerance_minutes,
            max_attempts=max_attempts,
        )
        if claim is None:
            break
        processed += 1
        counters.claims_acquired += 1
        if claim.attempt_count > 1:
            counters.claims_recovered += 1
        try:
            failure_count += _process_claim(db, claim, counters)
        except Exception as exc:
            # Once dispatching, an unrecorded failure is recovered as
            # delivery_unknown after the lease. Before dispatch, try to retain
            # a bounded retry without hiding the failed CronJob run.
            try:
                if mark_retryable(
                    claim,
                    error=f"claim_processing_failed:{type(exc).__name__}",
                ):
                    counters.retryable += 1
            except Exception:
                pass
            print(f"  [claim] Unhandled failure for route {claim.route_number}: {exc}")
            failure_count += 1
    counters.claims_skipped = max(counters.preferences_due - counters.claims_acquired, 0)
    print(
        "  [metrics] "
        + json.dumps(
            {
                **counters.as_dict(),
                "failures": failure_count,
                "processed": processed,
            },
            sort_keys=True,
        )
    )
    return failure_count


def run_daemon(
    sa_path: str,
    *,
    run_once: bool = False,
    once_late_tolerance_minutes: int = DEFAULT_ONCE_LATE_TOLERANCE_MINUTES,
) -> None:
    """Run the sole supported one-shot CronJob execution path."""

    notifications_enabled = _notifications_enabled()

    print(f"\n📦 Low-Quantity Notification Daemon")
    print(f"   Worker ID: {WORKER_ID}")
    print(f"   Enabled: {notifications_enabled}")
    print(f"   Dry Run: {LOW_QTY_NOTIFICATION_DRY_RUN}")

    if not notifications_enabled:
        print("  [skip] LOW_QTY_NOTIFICATIONS_ENABLED is not explicitly true; exiting before Firebase initialization")
        return

    if not run_once:
        raise RuntimeError("Low-quantity notifications support only the --once CronJob path")
    if LOW_QTY_NOTIFICATION_DRY_RUN:
        raise RuntimeError(
            "The scheduled low-quantity worker cannot run in dry-run mode; "
            "use test_low_qty_send_now.py for preview"
        )
    if once_late_tolerance_minutes < 0 or once_late_tolerance_minutes > 60:
        raise ValueError("once_late_tolerance_minutes must be between 0 and 60")

    source = _recipient_source()
    print(f"   Recipient source: {source}")

    db = get_firestore_client(sa_path)
    if source == "firebase":
        # Explicit rollback mode only. It uses the bounded enabled-user query
        # and materializes into the same PostgreSQL claim authority.
        sync_firebase_recipient_snapshot(db, now_utc=datetime.now(timezone.utc))

    failure_count = check_and_notify_postgres(
        db,
        late_tolerance_minutes=once_late_tolerance_minutes,
    )
    if failure_count:
        raise RuntimeError(
            f"Low-quantity notification cycle failed for {failure_count} claim(s)"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Low-quantity notification daemon",
    )
    parser.add_argument(
        '--serviceAccount', 
        required=True, 
        help='Path to Firebase service account JSON'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run a single reminder scan/check cycle and exit'
    )
    parser.add_argument(
        '--once-late-tolerance-minutes',
        type=int,
        default=DEFAULT_ONCE_LATE_TOLERANCE_MINUTES,
        help='Late catch-up window for --once CronJob mode'
    )
    
    args = parser.parse_args()
    run_daemon(
        args.serviceAccount,
        run_once=bool(args.once),
        once_late_tolerance_minutes=args.once_late_tolerance_minutes,
    )
