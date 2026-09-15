"""Explicit first four-date confirmation. No inferred Send, no historical writes."""
import hashlib
import json
import re
from datetime import datetime, timezone

from google.cloud import firestore

from .errors import StructuredApiError
from .schedule_source_contract import invalid, validate_cycles, validate_scope, validate_timezone
from .schedule_source_store import RECEIPT_LIMIT, _context, _owner_context, _read, _response, corrupt


def _legacy(value):
    if type(value) is not list or len(value) > 7:
        raise invalid()
    for cycle in value:
        if (type(cycle) is not dict or not {'orderDay', 'loadDay', 'deliveryDay'} <= set(cycle)
                or not set(cycle) <= {'orderDay', 'loadDay', 'deliveryDay', 'loadOffsetDays',
                                      'deliveryOffsetDays', 'scheduleVersion', 'needsScheduleReview'}
                or ('scheduleVersion' in cycle and (type(cycle['scheduleVersion']) is not int or cycle['scheduleVersion'] != 2))
                or any(type(cycle[k]) is not int or not 1 <= cycle[k] <= 7 for k in ('orderDay', 'loadDay', 'deliveryDay'))):
            raise invalid()
        for key in ('loadOffsetDays', 'deliveryOffsetDays'):
            if key in cycle and (type(cycle[key]) is not int or not 0 <= cycle[key] <= 60):
                raise invalid()
        if 'needsScheduleReview' in cycle and type(cycle['needsScheduleReview']) is not bool:
            raise invalid()
    return value


def validate_enrollment(value):
    if type(value) is not dict or set(value) != {'cycles', 'expectedCycles', 'requestId'}:
        raise invalid()
    if type(value['requestId']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', value['requestId']):
        raise invalid()
    return {**value, 'cycles': validate_cycles(value['cycles']), 'expectedCycles': _legacy(value['expectedCycles'])}


def read_enrollment_owner(db, uid, route):
    """Ownership check before HTTP body parsing, repeated at commit."""
    validate_scope(uid, route)

    @firestore.transactional
    def read(transaction):
        _owner_context(db, transaction, uid, route)
    read(db.transaction(max_attempts=3))


def enroll_schedule(db, uid, route, body):
    validate_scope(uid, route)
    body = validate_enrollment(body)
    digest = hashlib.sha256(json.dumps({'kind': 'enrollment', **body}, sort_keys=True,
                                      separators=(',', ':'), allow_nan=False).encode()).hexdigest()

    @firestore.transactional
    def save(transaction):
        user_ref, user = _owner_context(db, transaction, uid, route)
        # Do not switch an additive route back into the superseded replacement
        # format, even if an old rollout flag is accidentally enabled later.
        if db.document(f'orderScheduleSendExtensions/{route}').get(transaction=transaction).exists:
            raise corrupt()
        if 'orderScheduleControl' in user:
            _, user, control, _, _, pending = _context(db, transaction, uid, route)
            receipts = user.get('orderScheduleWriteReceipts', [])
            if type(receipts) is not list or len(receipts) > RECEIPT_LIMIT:
                raise corrupt()
            for receipt in receipts:
                if type(receipt) is not dict:
                    raise corrupt()
                if receipt.get('id') == body['requestId']:
                    if (set(receipt) != {'id', 'hash', 'revision', 'timezone'} or receipt.get('hash') != digest
                            or type(receipt.get('revision')) is not int or receipt['revision'] != 1):
                        raise StructuredApiError(409, 'This request identifier was already used.', 'SCHEDULE_REQUEST_CONFLICT')
                    timezone_name = validate_timezone(receipt.get('timezone'))
                    return _response({**control, 'revision': 1, 'timezone': timezone_name}, body['cycles'], pending,
                                     current_revision=control['revision'], replayed=True)
            raise StructuredApiError(409, 'This schedule was already upgraded. Reload it before editing.', 'SCHEDULE_REVISION_CONFLICT')
        pending_ref = db.collection('orderScheduleProjectionWork').document(route)
        if _read(pending_ref, transaction) or user.get('orderScheduleWriteReceipts'):
            raise corrupt()
        try:
            current = _legacy(user['userSettings']['notifications']['scheduling']['orderCycles'])
            timezone_name = validate_timezone(user['profile']['timezone'])
        except (KeyError, TypeError, StructuredApiError):
            raise corrupt() from None
        if current != body['expectedCycles']:
            raise StructuredApiError(409, 'The saved schedule changed. Review it before saving.', 'SCHEDULE_REVISION_CONFLICT')
        control = {'version': 3, 'revision': 1, 'routeNumber': route, 'ownerUid': uid, 'timezone': timezone_name}
        pending = {'version': 1, 'ownerUid': uid, 'routeNumber': route, 'sourceRevision': 1,
                   'status': 'pending', 'dueAt': datetime.now(timezone.utc), 'attempts': 0,
                   'leaseToken': None, 'leaseUntil': None}
        transaction.update(user_ref, {'userSettings.notifications.scheduling.orderCycles': body['cycles'],
                                     'userSettings.timestamps.updatedAt': firestore.SERVER_TIMESTAMP,
                                     'orderScheduleControl': control,
                                     'orderScheduleWriteReceipts': [{'id': body['requestId'], 'hash': digest,
                                                                    'revision': 1, 'timezone': timezone_name}]})
        transaction.set(pending_ref, pending)
        return _response(control, body['cycles'], pending)
    return save(db.transaction(max_attempts=3))
