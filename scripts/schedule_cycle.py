"""Canonical order/load/delivery cycle helpers.

This module mirrors src/utils/scheduleCycle.ts. Keep the migration formulas and
reverse-mapping rules in lockstep so mobile, sync, and forecast code agree.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, TypedDict


MAX_SCHEDULE_OFFSET_DAYS = 60
DAY_NAMES = {
    1: "monday",
    2: "tuesday",
    3: "wednesday",
    4: "thursday",
    5: "friday",
    6: "saturday",
    7: "sunday",
}


class NormalizedOrderCycle(TypedDict):
    orderDay: int
    loadDay: int
    deliveryDay: int
    loadOffsetDays: int
    deliveryOffsetDays: int
    needsScheduleReview: bool
    scheduleVersion: int


class ScheduleMatch(TypedDict):
    scheduleKey: str
    orderDay: int
    cycle: NormalizedOrderCycle
    matchedBy: str
    offsetDays: int


class ScheduleKeyResolution(TypedDict):
    scheduleKey: str
    orderDay: int
    cycle: NormalizedOrderCycle
    matchedBy: str
    ambiguous: bool
    matches: List[ScheduleMatch]


def is_valid_weekday(value: Any) -> bool:
    return isinstance(value, int) and 1 <= value <= 7


def is_valid_offset(value: Any) -> bool:
    return isinstance(value, int) and 0 <= value <= MAX_SCHEDULE_OFFSET_DAYS


def schedule_key_for_day(day_number: int) -> str:
    return DAY_NAMES.get(day_number, "unknown")


def weekday_after(order_day: int, offset_days: int) -> int:
    return ((order_day - 1 + offset_days) % 7) + 1


def days_between_for_load_migration(order_day: int, load_day: int) -> int:
    offset = load_day - order_day
    if offset < 0:
        offset += 7
    return offset


def days_between_for_delivery_migration(order_day: int, delivery_day: int) -> int:
    offset = delivery_day - order_day
    if offset <= 0:
        offset += 7
    return offset


def derive_weekday_mirrors(
    order_day: int,
    load_offset_days: int,
    delivery_offset_days: int,
) -> Dict[str, int]:
    return {
        "loadDay": weekday_after(order_day, load_offset_days),
        "deliveryDay": weekday_after(order_day, delivery_offset_days),
    }


def normalize_order_cycle(cycle: Dict[str, Any]) -> NormalizedOrderCycle:
    order_day = cycle["orderDay"] if is_valid_weekday(cycle.get("orderDay")) else 1
    legacy_load_day = cycle["loadDay"] if is_valid_weekday(cycle.get("loadDay")) else order_day
    legacy_delivery_day = (
        cycle["deliveryDay"]
        if is_valid_weekday(cycle.get("deliveryDay"))
        else legacy_load_day
    )

    migrated_load_offset = days_between_for_load_migration(order_day, legacy_load_day)
    migrated_delivery_offset = days_between_for_delivery_migration(
        order_day,
        legacy_delivery_day,
    )
    while migrated_delivery_offset < migrated_load_offset:
        migrated_delivery_offset += 7

    has_valid_offsets = (
        is_valid_offset(cycle.get("loadOffsetDays"))
        and is_valid_offset(cycle.get("deliveryOffsetDays"))
        and cycle["deliveryOffsetDays"] >= cycle["loadOffsetDays"]
    )

    load_offset_days = (
        cycle["loadOffsetDays"] if has_valid_offsets else migrated_load_offset
    )
    delivery_offset_days = (
        cycle["deliveryOffsetDays"] if has_valid_offsets else migrated_delivery_offset
    )
    mirrors = derive_weekday_mirrors(
        order_day,
        load_offset_days,
        delivery_offset_days,
    )
    mirror_mismatch = (
        "loadDay" in cycle
        and "deliveryDay" in cycle
        and (
            cycle.get("loadDay") != mirrors["loadDay"]
            or cycle.get("deliveryDay") != mirrors["deliveryDay"]
        )
    )

    needs_schedule_review = bool(
        cycle.get("needsScheduleReview")
        or (
            not has_valid_offsets
            and (
                migrated_load_offset > 4
                or migrated_delivery_offset - migrated_load_offset > 3
            )
        )
        or mirror_mismatch
    )

    return {
        "orderDay": order_day,
        "loadDay": mirrors["loadDay"],
        "deliveryDay": mirrors["deliveryDay"],
        "loadOffsetDays": load_offset_days,
        "deliveryOffsetDays": delivery_offset_days,
        "needsScheduleReview": needs_schedule_review,
        "scheduleVersion": cycle.get("scheduleVersion", 2),
    }


def parse_civil_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def add_days(value: str | date, days: int) -> date:
    return parse_civil_date(value) + timedelta(days=days)


def weekday_for_civil_date(value: str | date) -> int:
    return parse_civil_date(value).isoweekday()


def next_date_for_weekday(
    weekday: int,
    from_date: str | date,
    include_today: bool = True,
) -> date:
    start = parse_civil_date(from_date)
    days_until = (weekday - start.isoweekday() + 7) % 7
    if not include_today and days_until == 0:
        days_until = 7
    return start + timedelta(days=days_until)


def get_cycle_dates(cycle: Dict[str, Any], from_date: str | date) -> Dict[str, Any]:
    normalized = normalize_order_cycle(cycle)
    order_date = next_date_for_weekday(normalized["orderDay"], from_date)
    load_date = add_days(order_date, normalized["loadOffsetDays"])
    delivery_date = add_days(order_date, normalized["deliveryOffsetDays"])

    return {
        "orderDate": order_date,
        "loadDate": load_date,
        "deliveryDate": delivery_date,
        "orderDateString": order_date.isoformat(),
        "loadDateString": load_date.isoformat(),
        "deliveryDateString": delivery_date.isoformat(),
        "scheduleKey": schedule_key_for_day(normalized["orderDay"]),
        "cycleName": (
            f"{schedule_key_for_day(normalized['orderDay']).title()} -> "
            f"{schedule_key_for_day(normalized['deliveryDay']).title()}"
        ),
        "cycle": normalized,
    }


def get_schedule_key_for_delivery_date(
    delivery_date: str | date,
    order_cycles: List[Dict[str, Any]],
) -> Optional[ScheduleKeyResolution]:
    target = parse_civil_date(delivery_date)
    matches: List[ScheduleMatch] = []

    for cycle in order_cycles:
        normalized = normalize_order_cycle(cycle)
        schedule_key = schedule_key_for_day(normalized["orderDay"])
        delivery_candidate_order_date = add_days(
            target,
            -normalized["deliveryOffsetDays"],
        )
        load_candidate_order_date = add_days(target, -normalized["loadOffsetDays"])

        if delivery_candidate_order_date.isoweekday() == normalized["orderDay"]:
            matches.append(
                {
                    "scheduleKey": schedule_key,
                    "orderDay": normalized["orderDay"],
                    "cycle": normalized,
                    "matchedBy": "delivery",
                    "offsetDays": normalized["deliveryOffsetDays"],
                }
            )

        if load_candidate_order_date.isoweekday() == normalized["orderDay"]:
            matches.append(
                {
                    "scheduleKey": schedule_key,
                    "orderDay": normalized["orderDay"],
                    "cycle": normalized,
                    "matchedBy": "load",
                    "offsetDays": normalized["loadOffsetDays"],
                }
            )

    if not matches:
        return None

    sorted_matches = sorted(
        matches,
        key=lambda match: (
            0 if match["matchedBy"] == "delivery" else 1,
            match["offsetDays"],
            match["orderDay"],
        ),
    )
    primary = sorted_matches[0]

    return {
        "scheduleKey": primary["scheduleKey"],
        "orderDay": primary["orderDay"],
        "cycle": primary["cycle"],
        "matchedBy": primary["matchedBy"],
        "ambiguous": len(sorted_matches) > 1,
        "matches": sorted_matches,
    }
