"""Canonical source-backed Order Adjustment reminder scheduling.

This module is shared by the API and reminder worker. Legacy orders use the
server finalization date. Gated V3 orders use saved Send order + one day, with
actual finalization still required. Viewer timezone and client-owned dates are
never scheduling authority.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
import re
from typing import Any, Dict, Mapping
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


ANCHOR_KEYS = {"version", "finalizedAtMs", "finalizedDate", "timezone"}
STAMP_KEYS = {
    "version",
    "date",
    "timezone",
    "scheduleKey",
    "orderDay",
    "loadOffsetDays",
    "deliveryOffsetDays",
    "provenance",
}
CUTOFF_PATTERN = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
TERMINAL_STATUSES = {"sent", "confirmed", "applied", "archived", "canceled"}
DAY_KEYS = ("", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

REMINDER_SCHEDULE_FIELDS = (
    "adjustmentDate",
    "timezone",
    "cutoffTimeLocal",
    "cutoffAtMs",
    "reminderEnabled",
    "reminderLeadMinutes",
    "reminderAtMs",
    "reminderScheduledForMs",
    "reminderStatus",
    "reminderSkipReason",
    "reminderSkippedAtMs",
    "reminderClaimedAtMs",
    "reminderWorkerId",
    "reminderAttemptCount",
    "reminderNotificationId",
    "reminderSentAtMs",
    "reminderFailedAtMs",
    "reminderPushStats",
    "reminderError",
)


@dataclass(frozen=True)
class CanonicalReminderWindow:
    adjustment_date: str
    timezone: str
    cutoff_at_ms: int
    reminder_at_ms: int


class WindowResolutionError(ValueError):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def _strict_stamp(source_order: Mapping[str, Any]) -> Mapping[str, Any]:
    stamp = source_order.get("scheduledOrder")
    if not isinstance(stamp, Mapping) or set(stamp) != STAMP_KEYS:
        raise WindowResolutionError("window_unresolved")
    if stamp.get("version") != 2 or stamp.get("provenance") not in {"cycle", "backfill"}:
        raise WindowResolutionError("window_unresolved")
    order_day = stamp.get("orderDay")
    load_offset = stamp.get("loadOffsetDays")
    delivery_offset = stamp.get("deliveryOffsetDays")
    if (
        isinstance(order_day, bool)
        or not isinstance(order_day, int)
        or order_day not in range(1, 8)
        or isinstance(load_offset, bool)
        or not isinstance(load_offset, int)
        or isinstance(delivery_offset, bool)
        or not isinstance(delivery_offset, int)
        or load_offset < 0
        or delivery_offset < load_offset
        or delivery_offset > 60
    ):
        raise WindowResolutionError("window_unresolved")
    schedule_key = str(stamp.get("scheduleKey") or "").strip().lower()
    if (
        not schedule_key
        or str(stamp.get("scheduleKey")) != schedule_key
        or schedule_key != DAY_KEYS[order_day]
    ):
        raise WindowResolutionError("window_unresolved")
    try:
        scheduled_date = date.fromisoformat(str(stamp.get("date")))
        ZoneInfo(str(stamp.get("timezone")))
    except (TypeError, ValueError, ZoneInfoNotFoundError):
        raise WindowResolutionError("window_unresolved")
    if scheduled_date.isoweekday() != order_day:
        raise WindowResolutionError("window_unresolved")
    return stamp


def _timestamp_ms(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return None
        return int(value.timestamp()) * 1000 + value.microsecond // 1000
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
        return _timestamp_ms(parsed)
    if isinstance(value, Mapping):
        seconds = value.get("seconds", value.get("_seconds"))
        nanoseconds = value.get("nanoseconds", value.get("_nanoseconds", 0))
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, int)
            or isinstance(nanoseconds, bool)
            or not isinstance(nanoseconds, int)
            or nanoseconds < 0
            or nanoseconds >= 1_000_000_000
        ):
            return None
        return seconds * 1000 + nanoseconds // 1_000_000
    return None


def _source_finalization_authority(source_order: Mapping[str, Any]) -> tuple[date, str, ZoneInfo]:
    # Reader-first rollout: V3/unknown evidence must not enter Finalize+1 math,
    # even when a valid anchor exists. Send-date consumers are a separate slice.
    stamp = source_order.get('scheduledOrder')
    if 'scheduledOrder' in source_order and (not isinstance(stamp, Mapping) or stamp.get('version') != 2):
        raise WindowResolutionError('unsupported_schedule_version')
    if "adjustmentAnchor" in source_order:
        anchor = source_order.get("adjustmentAnchor")
        if not isinstance(anchor, Mapping) or set(anchor) != ANCHOR_KEYS:
            raise WindowResolutionError("invalid_adjustment_anchor")
        finalized_at_ms = anchor.get("finalizedAtMs")
        if (
            anchor.get("version") != 1
            or isinstance(finalized_at_ms, bool)
            or not isinstance(finalized_at_ms, int)
            or finalized_at_ms <= 0
        ):
            raise WindowResolutionError("invalid_adjustment_anchor")
        try:
            zone_name = str(anchor["timezone"])
            zone = ZoneInfo(zone_name)
            finalized_date = date.fromisoformat(str(anchor["finalizedDate"]))
            instant_date = datetime.fromtimestamp(
                finalized_at_ms / 1000, timezone.utc
            ).astimezone(zone).date()
        except (KeyError, TypeError, ValueError, OSError, ZoneInfoNotFoundError):
            raise WindowResolutionError("invalid_adjustment_anchor")
        if instant_date != finalized_date:
            raise WindowResolutionError("invalid_adjustment_anchor")
        return finalized_date, zone_name, zone

    # Compatibility for pre-anchor finalized orders. The schedule stamp supplies
    # only the server-captured route-owner timezone; its cycle date is not the
    # business finalization date.
    stamp = _strict_stamp(source_order)
    finalized_at_ms = _timestamp_ms(source_order.get("submittedAt"))
    if finalized_at_ms is None or finalized_at_ms <= 0:
        raise WindowResolutionError("window_unresolved")
    try:
        zone_name = str(stamp["timezone"])
        zone = ZoneInfo(zone_name)
        finalized_date = datetime.fromtimestamp(
            finalized_at_ms / 1000, timezone.utc
        ).astimezone(zone).date()
    except (KeyError, ValueError, OSError, ZoneInfoNotFoundError):
        raise WindowResolutionError("window_unresolved")
    return finalized_date, zone_name, zone


def canonical_source_window(
    source_order: Mapping[str, Any],
    *,
    cutoff_time_local: str,
    reminder_lead_minutes: int,
    now_ms: int | None = None,
    allow_v3: bool = False,
) -> CanonicalReminderWindow:
    if not CUTOFF_PATTERN.fullmatch(str(cutoff_time_local or "")):
        raise WindowResolutionError("invalid_cutoff")
    if (
        isinstance(reminder_lead_minutes, bool)
        or not isinstance(reminder_lead_minutes, int)
        or reminder_lead_minutes <= 0
    ):
        raise WindowResolutionError("invalid_reminder_lead")

    stamp = source_order.get('scheduledOrder')
    if isinstance(stamp, Mapping) and type(stamp.get('version')) is int and stamp['version'] == 3:
        if allow_v3 is not True:
            raise WindowResolutionError('unsupported_schedule_version')
        # Lazy import supports both repository and /app container layouts while
        # keeping the existing default-off worker independent of V3 activation.
        try:
            from order_forecast.api.order_schedule_window import ScheduleWindowError, resolve_scheduled_adjustment_window
        except ModuleNotFoundError as exc:
            if exc.name != 'order_forecast':
                raise
            from api.order_schedule_window import ScheduleWindowError, resolve_scheduled_adjustment_window
        try:
            resolved = resolve_scheduled_adjustment_window(source_order, cutoff_time_local, now_ms)
        except ScheduleWindowError as exc:
            raise WindowResolutionError(str(exc)) from None
        if resolved['finalizedAtMs'] is None:
            raise WindowResolutionError('past_cutoff' if resolved['status'] == 'closed' else 'needs_finalization')
        return CanonicalReminderWindow(
            adjustment_date=resolved['adjustmentDate'], timezone=resolved['timezone'],
            cutoff_at_ms=resolved['cutoffAtMs'],
            # A long lead or late Finalize cannot schedule before availability.
            reminder_at_ms=max(resolved['cutoffAtMs'] - reminder_lead_minutes * 60000,
                               resolved['opensAtMs'], resolved['finalizedAtMs']),
        )

    finalized_date, zone_name, zone = _source_finalization_authority(source_order)
    try:
        cutoff_hour, cutoff_minute = map(int, cutoff_time_local.split(":"))
        cutoff_clock = time(cutoff_hour, cutoff_minute)
    except (TypeError, ValueError):
        raise WindowResolutionError("invalid_schedule_or_cutoff")

    adjustment_day = finalized_date + timedelta(days=1)
    cutoff = datetime.combine(adjustment_day, cutoff_clock, tzinfo=zone)
    cutoff_ms = int(cutoff.timestamp()) * 1000 + cutoff.microsecond // 1000
    return CanonicalReminderWindow(
        adjustment_date=adjustment_day.isoformat(),
        timezone=zone_name,
        cutoff_at_ms=cutoff_ms,
        reminder_at_ms=cutoff_ms - reminder_lead_minutes * 60_000,
    )


# Temporary import compatibility for callers while the rollout is built from one
# reviewed commit. New code should use canonical_source_window.
canonical_source_v2_window = canonical_source_window


def meaningful_lines(data: Mapping[str, Any]) -> bool:
    raw_lines = data.get("lines") or (data.get("fullOrderWorkingCopy") or {}).get("generatedLines") or []
    if not isinstance(raw_lines, list):
        return False
    net_cases: Dict[str, float] = {}
    for raw_line in raw_lines:
        if not isinstance(raw_line, Mapping):
            continue
        sap = str(raw_line.get("sap") or "").strip()
        direction = str(raw_line.get("direction") or "").strip().lower()
        try:
            case_quantity = float(raw_line.get("caseQuantity") or 0)
        except (TypeError, ValueError):
            continue
        if not sap or case_quantity <= 0 or direction not in {"add", "remove"}:
            continue
        net_cases[sap] = net_cases.get(sap, 0) + (case_quantity if direction == "add" else -case_quantity)
    return any(abs(quantity) > 1e-9 for quantity in net_cases.values())


def trusted_adjustment_settings(raw: Mapping[str, Any]) -> Dict[str, Any]:
    lead = raw.get("reminderLeadMinutes")
    return {
        "reminderEnabled": raw.get("reminderEnabled") is True,
        "cutoffTimeLocal": str(raw.get("cutoffTimeLocal") or "").strip() or None,
        "reminderLeadMinutes": (
            lead if isinstance(lead, int) and not isinstance(lead, bool) and lead > 0 else None
        ),
    }


def _inactive_fields(settings: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "reminderEnabled": False,
        "cutoffTimeLocal": settings.get("cutoffTimeLocal"),
        "reminderLeadMinutes": settings.get("reminderLeadMinutes"),
        "cutoffAtMs": None,
        "reminderAtMs": None,
        "reminderStatus": "none",
        "reminderNotificationId": None,
        "reminderScheduledForMs": None,
    }


def build_canonical_reminder_fields(
    source_order: Mapping[str, Any],
    *,
    settings: Mapping[str, Any],
    has_meaningful_lines: bool,
    status: str,
    now_ms: int,
    allow_v3: bool = False,
) -> Dict[str, Any]:
    normalized = trusted_adjustment_settings(settings)
    if status in TERMINAL_STATUSES:
        return {
            "reminderEnabled": False,
            "reminderStatus": "skipped",
            "reminderSkippedAtMs": now_ms,
            "reminderSkipReason": f"adjustment_{status}",
            "reminderNotificationId": None,
            "reminderScheduledForMs": None,
            "cutoffAtMs": None,
            "reminderAtMs": None,
        }
    if status != "draft" or not has_meaningful_lines or not normalized["reminderEnabled"]:
        return _inactive_fields(normalized)

    try:
        window = canonical_source_window(
            source_order,
            cutoff_time_local=normalized.get("cutoffTimeLocal") or "",
            reminder_lead_minutes=normalized.get("reminderLeadMinutes") or 0,
            now_ms=now_ms,
            allow_v3=allow_v3,
        )
    except WindowResolutionError as exc:
        reason = exc.code if exc.code in {"invalid_cutoff", "invalid_reminder_lead", "needs_finalization", "past_cutoff"} else "window_unresolved"
        return {
            "reminderEnabled": True,
            "cutoffTimeLocal": normalized.get("cutoffTimeLocal"),
            "reminderLeadMinutes": normalized.get("reminderLeadMinutes"),
            "cutoffAtMs": None,
            "reminderAtMs": None,
            "reminderStatus": "skipped",
            "reminderSkippedAtMs": now_ms,
            "reminderSkipReason": reason,
            "reminderNotificationId": None,
            "reminderScheduledForMs": None,
        }

    fields = {
        "adjustmentDate": window.adjustment_date,
        "timezone": window.timezone,
        "cutoffTimeLocal": normalized["cutoffTimeLocal"],
        "reminderLeadMinutes": normalized["reminderLeadMinutes"],
        "cutoffAtMs": window.cutoff_at_ms,
        "reminderAtMs": window.reminder_at_ms,
        "reminderEnabled": True,
        "reminderNotificationId": None,
    }
    if now_ms >= window.cutoff_at_ms:
        return {
            **fields,
            "reminderStatus": "skipped",
            "reminderSkippedAtMs": now_ms,
            "reminderSkipReason": "past_cutoff",
            "reminderScheduledForMs": None,
        }
    return {
        **fields,
        "reminderStatus": "pending",
        "reminderScheduledForMs": window.reminder_at_ms,
        "reminderAttemptCount": 0,
        "reminderClaimedAtMs": None,
        "reminderWorkerId": None,
        "reminderSentAtMs": None,
        "reminderFailedAtMs": None,
        "reminderSkippedAtMs": None,
        "reminderSkipReason": None,
        "reminderPushStats": None,
        "reminderError": None,
    }


def reminder_fields_equal(current: Mapping[str, Any], fields: Mapping[str, Any]) -> bool:
    return all(current.get(key) == value for key, value in fields.items())


def protected_existing_fields(current: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: current.get(key) for key in REMINDER_SCHEDULE_FIELDS if key in current}


def window_as_dict(window: CanonicalReminderWindow) -> Dict[str, Any]:
    return asdict(window)
