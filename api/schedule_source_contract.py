"""Strict V3 source-save contract. No legacy normalization or date inference."""
from copy import deepcopy
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .errors import StructuredApiError

MAX_REVISION = 9007199254740991
CYCLE_KEYS = {'scheduleVersion', 'orderDay', 'sendOffsetDays', 'loadOffsetDays',
              'deliveryOffsetDays', 'loadDay', 'deliveryDay', 'needsScheduleReview'}


def invalid():
    return StructuredApiError(422, 'Schedule data is invalid.', 'SCHEDULE_INVALID')


def integer(value, low, high):
    # Match the shared JSON contract: integral numbers, never bool or strings.
    return type(value) in (int, float) and low <= value <= high and value == int(value)


def validate_scope(uid, route):
    if not (type(uid) is str and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', uid)
            and type(route) is str and re.fullmatch(r'[0-9]{1,10}', route)):
        raise StructuredApiError(403, 'Schedule access denied.', 'SCHEDULE_ACCESS_DENIED')


def validate_timezone(value):
    if type(value) is not str or len(value) > 64 or not re.fullmatch(r'(?:UTC|[A-Za-z_]+(?:/[A-Za-z0-9_+\-]+)+)', value):
        raise invalid()
    try:
        ZoneInfo(value)
    except (ValueError, ZoneInfoNotFoundError):
        raise invalid() from None
    return value


def validate_cycles(value):
    if type(value) is not list or len(value) > 7:
        raise invalid()
    cycles = []
    for cycle in value:
        if type(cycle) is not dict or set(cycle) != CYCLE_KEYS:
            raise invalid()
        if not integer(cycle['scheduleVersion'], 3, 3) or cycle['needsScheduleReview'] is not False:
            raise invalid()
        if not all(integer(cycle[key], 1, 7) for key in ('orderDay', 'loadDay', 'deliveryDay')):
            raise invalid()
        if not all(integer(cycle[key], 0, 60) for key in ('sendOffsetDays', 'loadOffsetDays', 'deliveryOffsetDays')):
            raise invalid()
        if not cycle['sendOffsetDays'] <= cycle['loadOffsetDays'] <= cycle['deliveryOffsetDays']:
            raise invalid()
        if any(cycle[day] != (cycle['orderDay'] - 1 + cycle[offset]) % 7 + 1
               for day, offset in [('loadDay', 'loadOffsetDays'), ('deliveryDay', 'deliveryOffsetDays')]):
            raise invalid()
        cycles.append({key: False if key == 'needsScheduleReview' else int(part) for key, part in cycle.items()})
    if len({cycle['orderDay'] for cycle in cycles}) != len(cycles):
        raise invalid()
    return cycles


def validate_write(value):
    if type(value) is not dict or set(value) != {'cycles', 'expectedRevision', 'requestId'}:
        raise invalid()
    if type(value['expectedRevision']) is not int or not 1 <= value['expectedRevision'] < MAX_REVISION:
        raise invalid()
    if type(value['requestId']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', value['requestId']):
        raise invalid()
    return {**deepcopy(value), 'cycles': validate_cycles(value['cycles'])}
