"""Dormant V3 adjustment consumer shared by future API/reminder integration.

No current endpoint or worker calls this module. The caller must supply an
authorized source order, trusted clock and cutoff. Availability is not authority
to send/edit. Legacy consumers and Low Quantity scheduling remain unchanged.
"""
from datetime import date, datetime, time, timedelta, timezone
import re
from zoneinfo import ZoneInfo

from .order_schedule_evidence import read_evidence

MAX_INSTANT = 253402300799999
ANCHOR_KEYS = {'version', 'finalizedAtMs', 'finalizedDate', 'timezone'}


class ScheduleWindowError(ValueError):
    pass


def require(value, code):
    if not value:
        raise ScheduleWindowError(code)


def integer(value, minimum, maximum):
    # Integral JSON numbers match JavaScript, but booleans never do.
    return (type(value) in (int, float) and minimum <= value <= maximum
            and value == int(value))


def _submitted_millis(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return None
        return int(value.timestamp()) * 1000 + value.microsecond // 1000
    if not isinstance(value, dict):
        return None
    seconds, nanos = value.get('seconds'), value.get('nanoseconds', 0)
    if not integer(seconds, 0, MAX_INSTANT // 1000) or not integer(nanos, 0, 999999999):
        return None
    return int(seconds) * 1000 + int(nanos) // 1000000


def _zoned_ms(day, clock, zone):
    """Existing adjustment semantics: earlier overlap, advance by DST gap.

    Deliberately does not change Low Quantity's first-valid-minute policy.
    """
    local = datetime.combine(day, time.fromisoformat(clock))
    candidates = [local.replace(tzinfo=zone, fold=fold).astimezone(timezone.utc) for fold in (0, 1)]
    exact = [item for item in candidates if item.astimezone(zone).replace(tzinfo=None) == local]
    selected = min(exact) if exact else min(
        (item for item in candidates if item.astimezone(zone).replace(tzinfo=None) > local),
        key=lambda item: (item.astimezone(zone).replace(tzinfo=None), item))
    return int(selected.timestamp()) * 1000


def resolve_scheduled_adjustment_window(order, cutoff, now):
    require(type(order) is dict, 'invalid_order')
    try:
        stamp = read_evidence(order.get('scheduledOrder'))
        if stamp['version'] == 3:
            start = date.fromisoformat(stamp['date'])
            require(order.get('routeNumber') == stamp['sourceRouteNumber']
                    and order.get('scheduleKey') == stamp['scheduleKey']
                    and order.get('expectedLoadDate') == (start + timedelta(days=stamp['loadOffsetDays'])).isoformat()
                    and order.get('expectedDeliveryDate') == (start + timedelta(days=stamp['deliveryOffsetDays'])).isoformat(),
                    'invalid_schedule_evidence')
    except (ValueError, TypeError, OverflowError):
        raise ScheduleWindowError('invalid_schedule_evidence') from None
    require(stamp['version'] == 3, 'unsupported_schedule_version')
    require(type(cutoff) is str and re.fullmatch(r'(?:[01]\d|2[0-3]):[0-5]\d', cutoff, re.ASCII), 'invalid_cutoff')
    require(integer(now, 1, MAX_INSTANT), 'invalid_now')
    require(order.get('status') in ('draft', 'finalized'), 'invalid_order_status')
    try:
        adjustment_day = date.fromisoformat(stamp['sendDate']) + timedelta(days=1)
        zone = ZoneInfo(stamp['timezone'])
        opens = _zoned_ms(adjustment_day, '00:00', zone)
        closes = _zoned_ms(adjustment_day, cutoff, zone)
    except (ValueError, OverflowError):
        raise ScheduleWindowError('invalid_schedule_evidence') from None
    finalized = None
    if order['status'] == 'draft':
        require(order.get('adjustmentAnchor') is None and order.get('submittedAt') is None, 'invalid_finalization')
    else:
        anchor = order.get('adjustmentAnchor')
        require(type(anchor) is dict and set(anchor) == ANCHOR_KEYS, 'invalid_finalization')
        require(integer(anchor['version'], 1, 1) and anchor['timezone'] == stamp['timezone']
                and integer(anchor['finalizedAtMs'], 1, now), 'invalid_finalization')
        finalized = int(anchor['finalizedAtMs'])
        try:
            local_date = datetime.fromtimestamp(finalized / 1000, timezone.utc).astimezone(zone).date().isoformat()
            require(_submitted_millis(order.get('submittedAt')) == finalized
                    and anchor['finalizedDate'] == local_date
                    and finalized >= _zoned_ms(date.fromisoformat(stamp['date']), '00:00', zone),
                    'invalid_finalization')
        except (ValueError, OverflowError, OSError):
            raise ScheduleWindowError('invalid_finalization') from None
    status = ('closed' if now >= closes else 'needs_finalization' if finalized is None
              else 'before_window' if now < opens else 'open')
    return {
        'adjustmentDate': adjustment_day.isoformat(), 'timezone': stamp['timezone'],
        'opensAtMs': opens, 'cutoffAtMs': closes, 'finalizedAtMs': finalized, 'status': status,
        'availableAtMs': None if finalized is None or max(opens, finalized) >= closes else max(opens, finalized),
    }
