"""Pure scheduling rules for recurring low-quantity reminders."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


UTC = timezone.utc


class InvalidReminderSetting(ValueError):
    """Raised when a reminder cannot be represented safely."""


def parse_reminder_minute(value: Any) -> int:
    """Strictly normalize an app reminder time to a minute of local day."""
    if not isinstance(value, dict):
        raise InvalidReminderSetting("reminder time must be an object")

    hour = value.get("hour")
    minute = value.get("minute")
    period = value.get("period")
    if isinstance(hour, bool) or not isinstance(hour, int) or hour not in range(1, 13):
        raise InvalidReminderSetting("reminder hour must be an integer from 1 through 12")
    if isinstance(minute, bool) or not isinstance(minute, int) or minute not in range(60):
        raise InvalidReminderSetting("reminder minute must be an integer from 0 through 59")
    if period not in {"AM", "PM"}:
        raise InvalidReminderSetting("reminder period must be AM or PM")

    hour_24 = hour % 12
    if period == "PM":
        hour_24 += 12
    return hour_24 * 60 + minute


def validate_timezone(timezone_name: Any) -> str:
    """Return a canonical usable IANA key or raise without a fallback."""
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        raise InvalidReminderSetting("timezone must be a non-empty IANA name")
    normalized = timezone_name.strip()
    try:
        ZoneInfo(normalized)
    except ZoneInfoNotFoundError as exc:
        raise InvalidReminderSetting(f"unknown timezone: {normalized}") from exc
    return normalized


def _valid_utc_candidates(local_naive: datetime, route_timezone: ZoneInfo) -> list[datetime]:
    candidates: set[datetime] = set()
    for fold in (0, 1):
        localized = local_naive.replace(tzinfo=route_timezone, fold=fold)
        candidate = localized.astimezone(UTC)
        round_trip = candidate.astimezone(route_timezone)
        if round_trip.replace(tzinfo=None) == local_naive:
            candidates.add(candidate)
    return sorted(candidates)


def scheduled_instant_for_local_date(
    scheduled_local_date: date,
    reminder_minute_local: int,
    timezone_name: str,
) -> datetime:
    """Resolve one local-date slot using the locked DST policy.

    Ambiguous fall-back times use their first occurrence. Nonexistent
    spring-forward times move to the first valid local minute after the gap.
    """
    if (
        isinstance(reminder_minute_local, bool)
        or not isinstance(reminder_minute_local, int)
        or reminder_minute_local not in range(24 * 60)
    ):
        raise InvalidReminderSetting("reminder minute must be in 0 through 1439")
    timezone_key = validate_timezone(timezone_name)
    route_timezone = ZoneInfo(timezone_key)
    local_midnight = datetime.combine(scheduled_local_date, time.min)
    configured_local = local_midnight + timedelta(minutes=reminder_minute_local)

    for minute_after_gap in range(181):
        candidates = _valid_utc_candidates(
            configured_local + timedelta(minutes=minute_after_gap),
            route_timezone,
        )
        if candidates:
            return candidates[0]
    raise InvalidReminderSetting("could not resolve reminder within three hours of local time")


def next_scheduled_instant(
    reminder_minute_local: int,
    timezone_name: str,
    *,
    after_utc: datetime,
) -> tuple[date, datetime]:
    """Return the first schedule slot strictly later than ``after_utc``."""
    if after_utc.tzinfo is None or after_utc.utcoffset() is None:
        raise ValueError("after_utc must be timezone-aware")

    timezone_key = validate_timezone(timezone_name)
    normalized_after = after_utc.astimezone(UTC)
    route_timezone = ZoneInfo(timezone_key)
    first_local_date = normalized_after.astimezone(route_timezone).date()

    for day_offset in range(3):
        local_date = first_local_date + timedelta(days=day_offset)
        candidate = scheduled_instant_for_local_date(
            local_date,
            reminder_minute_local,
            timezone_key,
        )
        if candidate > normalized_after:
            return local_date, candidate
    raise RuntimeError("could not resolve the next daily reminder slot")
