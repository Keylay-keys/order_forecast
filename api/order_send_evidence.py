"""Additive order timing shared by the guarded API and reminder worker.

New shared orders stay V2. A server-only record freezes Send at creation, in the
same transaction. No existing order is enrolled, converted, or restamped here.
Callers must authorize ordering and enforce rollout gates before using create.
"""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from zoneinfo import ZoneInfo

from google.cloud import firestore

from .errors import StructuredApiError
from .order_schedule_evidence import build_evidence, read_evidence
from .order_schedule_window import _submitted_millis, resolve_scheduled_adjustment_window
from .schedule_send_extension import _context, _response
from .order_send_access import read_send_route_access
from .schedule_source_contract import validate_scope

COLLECTION = 'orderSendEvidence'
POLICY = 'send_next_day_v1'
KEYS = {'version', 'policy', 'orderId', 'createdAtMs', 'scheduledOrder', 'requestHash'}
SEND_KEYS = {'sendDate', 'sendOffsetDays', 'sourceRevision', 'sourceOwnerUid', 'sourceRouteNumber'}


def is_send_order_id(value):
    return type(value) is str and re.fullmatch(r'order-[0-9]{1,10}-send-[a-f0-9]{64}', value) is not None


def read_send_evidence(db, order, transaction=None):
    """Exact reserved-ID lookup. Missing evidence cannot select legacy timing."""
    if not is_send_order_id(order.get('id')):
        return None
    record = db.collection(COLLECTION).document(order['id']).get(transaction=transaction)
    evidence = record.to_dict() if record.exists else None
    order_with_send_evidence(order, evidence)
    return evidence


def legacy_order_stamp(stamp):
    stamp = read_evidence(stamp)
    if stamp['version'] != 3:
        raise ValueError('Expected Send evidence')
    return {**{key: value for key, value in stamp.items() if key not in SEND_KEYS}, 'version': 2}


def order_with_send_evidence(order, evidence):
    """Validated in-memory view only. Evidence must be fetched by trusted code.

    Not an authorization check. Does not consult today's Schedule or fall back
    to Finalize-based math when the supplied evidence is missing/invalid.
    """
    if (type(order) is not dict or type(evidence) is not dict or set(evidence) not in (KEYS, KEYS | {'creatorUid'})
            or type(evidence['version']) is not int or evidence['version'] != 1
            or evidence['policy'] != POLICY or type(evidence['orderId']) is not str
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', evidence['orderId'])
            or order.get('id') != evidence['orderId']
            or type(evidence['createdAtMs']) is not int or evidence['createdAtMs'] <= 0
            or _submitted_millis(order.get('createdAt')) != evidence['createdAtMs']
            or type(evidence['requestHash']) is not str or not re.fullmatch(r'[a-f0-9]{64}', evidence['requestHash'])):
        raise ValueError('Invalid order Send linkage')
    stamp = read_evidence(evidence['scheduledOrder'])
    if stamp['version'] != 3:
        raise ValueError('Expected Send evidence')
    creator = evidence.get('creatorUid', stamp['sourceOwnerUid'])
    if type(creator) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', creator):
        raise ValueError('Invalid order creator linkage')
    legacy = read_evidence(order.get('scheduledOrder'))
    start = date.fromisoformat(stamp['date'])
    if (legacy != legacy_order_stamp(stamp) or order.get('routeNumber') != stamp['sourceRouteNumber']
            or order.get('userId') != creator or order.get('scheduleKey') != stamp['scheduleKey']
            or order.get('expectedLoadDate') != (start + timedelta(days=stamp['loadOffsetDays'])).isoformat()
            or order.get('expectedDeliveryDate') != (start + timedelta(days=stamp['deliveryOffsetDays'])).isoformat()):
        raise ValueError('Order does not match Send evidence')
    return {**deepcopy(order), 'scheduledOrder': stamp}


def resolve_send_order_window(order, evidence, cutoff, now):
    view = order_with_send_evidence(order, evidence)
    result = resolve_scheduled_adjustment_window(view, cutoff, now)
    if (now < evidence['createdAtMs'] or result['finalizedAtMs'] is not None
            and result['finalizedAtMs'] < evidence['createdAtMs']):
        raise ValueError('Invalid order creation chronology')
    return result


def create_send_order(db, uid, payload):
    """Route-authorized creation, with atomic evidence and same-request retries.

    Receipt survives removal of the draft; retry never resurrects an order.
    There is deliberately no attach-to-existing-order or history backfill path.
    """
    body = payload.model_dump(mode='json')
    route = payload.routeNumber
    validate_scope(uid, route)
    if (type(payload.requestId) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', payload.requestId)
            or type(payload.expectedScheduleRevision) is not int
            or not 1 <= payload.expectedScheduleRevision <= 9007199254740991):
        raise StructuredApiError(422, 'Confirmed Send schedule required.', 'ORDER_CREATE_INVALID')
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    key = hashlib.sha256(f'{POLICY}:{uid}:{route}:{payload.requestId}'.encode()).hexdigest()
    # Published clients derive the route from orderId.split('-')[1].
    order_id = f'order-{route}-send-{key}'
    ref = db.document(f'routes/{route}/orders/{order_id}')
    evidence_ref = db.document(f'{COLLECTION}/{order_id}')

    @firestore.transactional
    def create(transaction):
        access = read_send_route_access(db, transaction, uid, route)
        existing = ref.get(transaction=transaction)
        record = evidence_ref.get(transaction=transaction)
        if record.exists:
            evidence = record.to_dict()
            if evidence.get('requestHash') != digest:
                raise StructuredApiError(409, 'Request already used for another order.', 'ORDER_CREATE_CONFLICT')
            if not existing.exists:
                raise StructuredApiError(409, 'Order was already removed.', 'ORDER_CREATE_REMOVED')
            saved = existing.to_dict()
            order_with_send_evidence(saved, evidence)
            return {'order': saved, 'sendEvidence': evidence, 'replayed': True}
        if existing.exists:
            raise StructuredApiError(409, 'Order already exists.', 'ORDER_CREATE_CONFLICT')
        _, current, zone, _, extension = _context(db, transaction, uid, route, owner_only=False)
        source = _response(route, current, zone, extension)
        if (source['revision'] != payload.expectedScheduleRevision or source['status'] != 'ready'
                or not source['confirmedCycles']):
            raise StructuredApiError(409, 'Schedule changed. Confirm the dates again.', 'ORDER_SCHEDULE_REVISION_CONFLICT')
        stamp = build_evidence({'ownerUid': access.owner_uid, 'routeNumber': route, 'revision': source['revision'], 'timezone': zone},
                               source['confirmedCycles'], payload.deliveryDate.isoformat(), payload.scheduleKey)
        now = datetime.now(timezone.utc)
        order = {'id': order_id, 'routeNumber': route, 'userId': uid,
                 'orderDate': now.astimezone(ZoneInfo(zone)).date().isoformat(),
                 'expectedDeliveryDate': payload.deliveryDate.isoformat(),
                 'expectedLoadDate': (date.fromisoformat(stamp['date']) + timedelta(days=stamp['loadOffsetDays'])).isoformat(),
                 'scheduleKey': stamp['scheduleKey'], 'scheduledOrder': legacy_order_stamp(stamp),
                 'status': 'draft', 'stores': [], 'createdAt': now, 'updatedAt': now,
                 'notes': payload.notes, 'isHolidaySchedule': payload.isHolidaySchedule,
                 **({'coreItemPolicyVersion': 1, 'coreItemOverrides': []} if payload.coreItemPolicyVersion == 1 else {})}
        evidence = {'version': 1, 'policy': POLICY, 'orderId': order_id, 'createdAtMs': _submitted_millis(now),
                    'scheduledOrder': stamp, 'requestHash': digest}
        if uid != access.owner_uid:
            evidence['creatorUid'] = uid
        order_with_send_evidence(order, evidence)
        transaction.create(ref, order)
        transaction.create(evidence_ref, evidence)
        return {'order': order, 'sendEvidence': evidence, 'replayed': False}

    return create(db.transaction(max_attempts=3))
