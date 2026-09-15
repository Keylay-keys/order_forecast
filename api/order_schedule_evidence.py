"""Versioned order evidence and a server-only transactional creation primitive.

No HTTP route calls create_v3_order yet. Existing creation remains gated until
dual-version adjustment/reminder consumers and release preflight are complete.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta
import re

from google.cloud import firestore

from .errors import StructuredApiError
from .models import AdjustmentAnchor, ScheduledOrder, ScheduledOrderV3
from .firestore_timestamp import timestamp_ms
from .schedule_source_contract import validate_cycles, validate_scope
from .schedule_source_store import _context

DAYS = ('', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')


def read_evidence(value):
    """Exact version dispatch. Unknown versions never fall back to legacy math."""
    if type(value) is not dict or type(value.get('version')) is not int:
        raise ValueError('Invalid schedule evidence')
    model = {2: ScheduledOrder, 3: ScheduledOrderV3}.get(value['version'])
    if model is None:
        raise ValueError('Unsupported schedule evidence')
    return model.model_validate(value).model_dump()


def validate_order_evidence(order):
    stamp = read_evidence(order.get('scheduledOrder'))
    if stamp['version'] == 3:
        start = date.fromisoformat(stamp['date'])
        if (order.get('routeNumber') != stamp['sourceRouteNumber']
                or order.get('scheduleKey') != stamp['scheduleKey']
                or order.get('expectedLoadDate') != (start + timedelta(days=stamp['loadOffsetDays'])).isoformat()
                or order.get('expectedDeliveryDate') != (start + timedelta(days=stamp['deliveryOffsetDays'])).isoformat()):
            raise ValueError('Order does not match saved schedule evidence')
        if order.get('status') == 'finalized':
            anchor = AdjustmentAnchor.model_validate(order.get('adjustmentAnchor')).model_dump()
            submitted = order.get('submittedAt')
            if (not isinstance(submitted, datetime) or submitted.tzinfo is None
                    or timestamp_ms(submitted) != anchor['finalizedAtMs']
                    or anchor['timezone'] != stamp['timezone'] or anchor['finalizedDate'] < stamp['date']):
                raise ValueError('Finalization does not match saved schedule evidence')
    return stamp


def build_evidence(control, cycles, delivery_date, schedule_key):
    """Only call with control/cycles read by the trusted source transaction."""
    cycles = validate_cycles(cycles)
    if type(delivery_date) is not str or date.fromisoformat(delivery_date).isoformat() != delivery_date:
        raise ValueError('Invalid delivery date')
    matches = []
    for cycle in cycles:
        start = date.fromisoformat(delivery_date) - timedelta(days=cycle['deliveryOffsetDays'])
        if start.isoweekday() == cycle['orderDay'] and DAYS[cycle['orderDay']] == schedule_key:
            matches.append((cycle, start))
    if len(matches) != 1:
        raise StructuredApiError(409, 'No unique schedule cycle matches this delivery date.',
                                 'ORDER_SCHEDULE_AMBIGUOUS')
    cycle, start = matches[0]
    return read_evidence({
        'version': 3, 'provenance': 'cycle', 'date': start.isoformat(),
        'sendDate': (start + timedelta(days=cycle['sendOffsetDays'])).isoformat(),
        'timezone': control['timezone'], 'scheduleKey': schedule_key,
        **{key: cycle[key] for key in ('orderDay', 'sendOffsetDays', 'loadOffsetDays', 'deliveryOffsetDays')},
        'sourceRevision': control['revision'], 'sourceOwnerUid': control['ownerUid'],
        'sourceRouteNumber': control['routeNumber'],
    })


def create_v3_order(db, *, owner_uid, route, order):
    """Dormant server primitive, not an authorization boundary or public API.

    Caller must authorize ordering first. Owner is canonical owner, not an
    arbitrary team actor. Reads both ownership anchors and source inside the
    same transaction as create. No PG projection or caller revision is used.
    Existing IDs are refused, never overwritten or restamped on retry.
    """
    validate_scope(owner_uid, route)
    order = deepcopy(order)
    if (type(order.get('id')) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', order['id'])
            or order.get('routeNumber') != route or order.get('status') != 'draft'
            or any(key in order for key in ('scheduledOrder', 'adjustmentAnchor', 'submittedAt'))):
        raise ValueError('Invalid unstamped draft')
    ref = db.collection('routes').document(route).collection('orders').document(order['id'])

    @firestore.transactional
    def create(transaction):
        _, _, control, cycles, _, _ = _context(db, transaction, owner_uid, route)
        stamp = build_evidence(control, cycles, order['expectedDeliveryDate'], order['scheduleKey'])
        start = date.fromisoformat(stamp['date'])
        saved = {**order, 'scheduledOrder': stamp,
                 'expectedLoadDate': (start + timedelta(days=stamp['loadOffsetDays'])).isoformat()}
        validate_order_evidence(saved)
        transaction.create(ref, saved)
        return saved

    return create(db.transaction(max_attempts=3))
