"""Atomic source save plus bounded retry receipts and pending projection intent.

Only already opted-in owner schedules are accepted. This does not opt in users,
project to PostgreSQL, or replace the separately required old-client fence.
"""
import hashlib
import json
import re
from datetime import datetime, timezone

from google.cloud import firestore

from .errors import StructuredApiError
from .route_ownership import extract_owned_routes_for_owner
from .schedule_source_contract import MAX_REVISION, validate_cycles, validate_scope, validate_timezone, validate_write

RECEIPT_LIMIT = 32
CONTROL_KEYS = {'version', 'revision', 'routeNumber', 'ownerUid', 'timezone'}


def denied():
    return StructuredApiError(403, 'Schedule access denied.', 'SCHEDULE_ACCESS_DENIED')


def corrupt():
    return StructuredApiError(409, 'Schedule source needs review.', 'SCHEDULE_SOURCE_CONFLICT')


def _read(ref, transaction):
    return ref.get(transaction=transaction).to_dict() or {}


def _owner_context(db, transaction, uid, route):
    validate_scope(uid, route)
    user_ref = db.collection('users').document(uid)
    user = _read(user_ref, transaction)
    anchors = {name: _read(db.collection(name).document(route), transaction)
               for name in ('routes', 'routeNumbers', 'routeEntitlements')}
    if (anchors['routes'].get('ownerUid') != uid or anchors['routeNumbers'].get('userId') != uid
            or extract_owned_routes_for_owner(user) != [route]):
        raise denied()
    # Alias fields may expose a half-complete ownership transfer too.
    if any(value != uid for anchor in anchors.values() for key in ('ownerUid', 'userId', 'userID')
           if (value := anchor.get(key)) is not None):
        raise denied()
    return user_ref, user


def _context(db, transaction, uid, route):
    user_ref, user = _owner_context(db, transaction, uid, route)
    control = user.get('orderScheduleControl')
    if control is None:
        raise StructuredApiError(409, 'This schedule is not enabled for the new save path.', 'SCHEDULE_NOT_ENABLED')
    if (type(control) is not dict or set(control) != CONTROL_KEYS
            or type(control['version']) is not int or control['version'] != 3
            or type(control['revision']) is not int or not 1 <= control['revision'] <= MAX_REVISION
            or control['ownerUid'] != uid or control['routeNumber'] != route):
        raise corrupt()
    try:
        validate_timezone(control['timezone'])
        cycles = validate_cycles(user['userSettings']['notifications']['scheduling']['orderCycles'])
    except (StructuredApiError, KeyError, TypeError):
        raise corrupt() from None
    pending_ref = db.collection('orderScheduleProjectionWork').document(route)
    pending = _read(pending_ref, transaction)
    if pending and (type(pending.get('version')) is not int or pending['version'] != 1
                    or pending.get('ownerUid') != uid or pending.get('routeNumber') != route
                    or type(pending.get('sourceRevision')) is not int
                    or pending['sourceRevision'] != control['revision']
                    or pending.get('status') not in {'pending', 'leased', 'projected', 'blocked'}):
        raise corrupt()
    return user_ref, user, control, cycles, pending_ref, pending


def _response(control, cycles, pending, *, current_revision=None, replayed=False):
    return {'routeNumber': control['routeNumber'], 'revision': control['revision'],
            'currentRevision': control['revision'] if current_revision is None else current_revision,
            'timezone': control['timezone'], 'cycles': cycles, 'replayed': replayed,
            'projection': {'sourceRevision': pending.get('sourceRevision'),
                           'status': pending.get('status', 'unknown')}}


def read_source(db, uid, route):
    validate_scope(uid, route)

    @firestore.transactional
    def read(transaction):
        _, _, control, cycles, _, pending = _context(db, transaction, uid, route)
        return _response(control, cycles, pending)

    return read(db.transaction(max_attempts=3))


def read_current_schedule(db, route, legacy_uid=None):
    """Read current authority, never a delayed listener payload. No auto opt-in.

    Legacy owner discovery remains the caller's responsibility when no canonical
    route pointer exists. Once a control marker exists, all strict source checks
    apply, including malformed/null markers. No legacy fallback is permitted.
    """
    validate_scope(legacy_uid or 'reader', route)

    @firestore.transactional
    def read(transaction):
        anchor = _read(db.collection('routes').document(route), transaction)
        pending = _read(db.collection('orderScheduleProjectionWork').document(route), transaction)
        uid = anchor.get('ownerUid') or anchor.get('userId') or legacy_uid
        if not uid:
            if pending:
                raise corrupt()
            return None, {}, None
        validate_scope(uid, route)
        user = _read(db.collection('users').document(uid), transaction)
        if 'orderScheduleControl' not in user:
            if pending:
                # A half-finished ownership transfer or missing marker must not
                # make an already protected route look like a legacy schedule.
                raise corrupt()
            return uid, user, None
        _, user, control, cycles, _, _ = _context(db, transaction, uid, route)
        return uid, user, {'control': control, 'cycles': cycles}

    return read(db.transaction(max_attempts=3))


def save_source(db, uid, route, body):
    validate_scope(uid, route)
    body = validate_write(body)
    digest = hashlib.sha256(json.dumps({'cycles': body['cycles'], 'expectedRevision': body['expectedRevision']},
                                      sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()

    @firestore.transactional
    def save(transaction):
        user_ref, user, control, _, pending_ref, pending = _context(db, transaction, uid, route)
        receipts = user.get('orderScheduleWriteReceipts', [])
        if type(receipts) is not list or len(receipts) > RECEIPT_LIMIT:
            raise corrupt()
        receipt_ids = set()
        for receipt in receipts:
            if (type(receipt) is not dict or set(receipt) != {'id', 'hash', 'revision', 'timezone'}
                    or type(receipt['revision']) is not int or not 1 <= receipt['revision'] <= control['revision']
                    or type(receipt['id']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', receipt['id'])
                    or type(receipt['hash']) is not str or not re.fullmatch(r'[0-9a-f]{64}', receipt['hash'])
                    or receipt['id'] in receipt_ids):
                raise corrupt()
            receipt_ids.add(receipt['id'])
            try:
                validate_timezone(receipt['timezone'])
            except StructuredApiError:
                raise corrupt() from None
        for receipt in receipts:
            if receipt['id'] == body['requestId']:
                if receipt['hash'] != digest:
                    raise StructuredApiError(409, 'This request identifier was already used.', 'SCHEDULE_REQUEST_CONFLICT')
                return _response({**control, 'revision': receipt['revision'], 'timezone': receipt['timezone']},
                                 body['cycles'], pending, current_revision=control['revision'], replayed=True)
        if control['revision'] != body['expectedRevision']:
            raise StructuredApiError(409, 'The saved schedule changed. Your draft must be reviewed.',
                                     'SCHEDULE_REVISION_CONFLICT', {'revision': control['revision']})
        try:
            timezone_name = validate_timezone(user['profile']['timezone'])
        except (StructuredApiError, KeyError, TypeError):
            raise corrupt() from None
        next_control = {**control, 'revision': control['revision'] + 1, 'timezone': timezone_name}
        receipts = (receipts + [{'id': body['requestId'], 'hash': digest, 'revision': next_control['revision'],
                                 'timezone': timezone_name}])[-RECEIPT_LIMIT:]
        now = datetime.now(timezone.utc)
        next_pending = {'version': 1, 'ownerUid': uid, 'routeNumber': route,
                        'sourceRevision': next_control['revision'], 'status': 'pending',
                        'dueAt': now, 'attempts': 0, 'leaseToken': None, 'leaseUntil': None}
        # All reads precede writes. A failed commit leaves neither partial source
        # nor missing delivery intent. No wake-up notification is required here.
        transaction.update(user_ref, {'userSettings.notifications.scheduling.orderCycles': body['cycles'],
                                      'userSettings.timestamps.updatedAt': firestore.SERVER_TIMESTAMP,
                                      'orderScheduleControl': next_control, 'orderScheduleWriteReceipts': receipts})
        transaction.set(pending_ref, next_pending)
        return _response(next_control, body['cycles'], next_pending)

    return save(db.transaction(max_attempts=3))
