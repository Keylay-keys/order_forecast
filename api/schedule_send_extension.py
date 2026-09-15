"""Additive Send-day storage behind the separately gated send-settings endpoint.

The shared settings remain V2. No enrollment/control marker, projection queue,
PostgreSQL fence, or order stamp is created here. Old clients keep their ordinary
write path. A confirmed Send belongs to an exact legacy cycle plus timezone, not
to its array position or just its weekday. Changed cycles need reconfirmation on
the updated client; reading this extension never repairs shared settings.
Multi-route/team saves use a route-local V2 extension and leave all personal
settings unchanged. V1 extension saves preserve the single-owner legacy path.
"""
from copy import deepcopy
import hashlib
import json
import re

from google.cloud import firestore

from .errors import StructuredApiError
from .schedule_enrollment import _legacy
from .schedule_source_contract import MAX_REVISION, invalid, validate_cycles, validate_scope, validate_timezone
from .schedule_source_store import RECEIPT_LIMIT, corrupt
from .order_send_access import read_send_route_access
from .route_ownership import extract_owned_routes_for_owner

COLLECTION = 'orderScheduleSendExtensions'
KEYS = {'version', 'ownerUid', 'routeNumber', 'revision', 'timezone', 'cycles', 'receipts'}


def legacy_cycles(cycles):
    """Only a validated, explicit four-date selection can produce this view."""
    return [{**{key: value for key, value in cycle.items() if key != 'sendOffsetDays'},
             'scheduleVersion': 2} for cycle in validate_cycles(cycles)]


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def _request(value):
    if (type(value) is not dict
            or set(value) != {'cycles', 'expectedCycles', 'expectedRevision', 'requestId'}
            or type(value['expectedRevision']) is not int
            or not 0 <= value['expectedRevision'] < MAX_REVISION
            or type(value['requestId']) is not str
            or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', value['requestId'])):
        raise invalid()
    return {**deepcopy(value), 'cycles': validate_cycles(value['cycles']),
            'expectedCycles': deepcopy(_legacy(value['expectedCycles']))}


def _extension(value, uid, route):
    if value is None:
        return None
    if (type(value) is not dict or type(value.get('version')) is not int
            or value['version'] not in (1, 2)
            or set(value) != (KEYS if value['version'] == 1 else KEYS | {'legacyOwnerBase'})
            or value['ownerUid'] != uid or value['routeNumber'] != route
            or type(value['revision']) is not int or not 1 <= value['revision'] <= MAX_REVISION):
        raise corrupt()
    try:
        validate_cycles(value['cycles'])
        validate_timezone(value['timezone'])
        if value['version'] == 2:
            _legacy(value['legacyOwnerBase'])
    except StructuredApiError:
        raise corrupt() from None
    receipts = value['receipts']
    if type(receipts) is not list or not 1 <= len(receipts) <= RECEIPT_LIMIT:
        raise corrupt()
    ids = set()
    for receipt in receipts:
        if (type(receipt) is not dict or set(receipt) != {'id', 'hash', 'revision'}
                or type(receipt['id']) is not str or not re.fullmatch(r'[A-Za-z0-9_-]{16,64}', receipt['id'])
                or receipt['id'] in ids or type(receipt['hash']) is not str
                or not re.fullmatch(r'[0-9a-f]{64}', receipt['hash'])
                or type(receipt['revision']) is not int or not 1 <= receipt['revision'] <= value['revision']):
            raise corrupt()
        ids.add(receipt['id'])
    return value


def route_legacy_cycles(owner_cycles, extension):
    """A route override never writes the owner's account-wide legacy settings.

    Preserve old-app edits: an intentional change to the shared legacy base
    invalidates the override until this route is reviewed and saved again.
    Callers must validate the extension before using this projection.
    """
    if extension and extension['version'] == 2 and owner_cycles == extension['legacyOwnerBase']:
        return legacy_cycles(extension['cycles'])
    return deepcopy(owner_cycles)


def _context(db, transaction, uid, route, *, owner_only=True):
    access = read_send_route_access(db, transaction, uid, route, owner_only=owner_only)
    user_ref, user = access.owner_ref, access.owner
    # Never mix this candidate with an already enrolled replacement-format route.
    if ('orderScheduleControl' in user
            or db.document(f'orderScheduleProjectionWork/{route}').get(transaction=transaction).exists):
        raise corrupt()
    try:
        current = deepcopy(_legacy(user['userSettings']['notifications']['scheduling']['orderCycles']))
        timezone = validate_timezone(user['profile']['timezone'])
    except (StructuredApiError, KeyError, TypeError):
        raise corrupt() from None
    ref = db.document(f'{COLLECTION}/{route}')
    snapshot = ref.get(transaction=transaction)
    extension = _extension(snapshot.to_dict() if snapshot.exists else None, access.owner_uid, route)
    return user_ref, route_legacy_cycles(current, extension), timezone, ref, extension


def _response(route, current, timezone, extension, *, saved_revision=None):
    matches = []
    missing = []
    # Exact-cycle matching is intentionally conservative. A legacy save that
    # drops optional fields can require re-confirmation, but can never borrow a
    # Send from a different cycle. Duplicate starts cannot be uniquely identified.
    for index, cycle in enumerate(current):
        candidates = []
        if extension and extension['timezone'] == timezone:
            candidates = [confirmed for confirmed in extension['cycles']
                          if legacy_cycles([confirmed])[0] == cycle]
        unique_start = sum(item['orderDay'] == cycle['orderDay'] for item in current) == 1
        if len(candidates) == 1 and unique_start:
            matches.append(deepcopy(candidates[0]))
        else:
            missing.append(index)
    result = {'routeNumber': route, 'revision': extension['revision'] if extension else 0,
              'timezone': timezone, 'legacyCycles': deepcopy(current), 'confirmedCycles': matches,
              'needsConfirmation': missing, 'status': 'needs_confirmation' if missing else 'ready'}
    if saved_revision is not None:
        result['savedRevision'] = saved_revision
    return result


def read_send_extension(db, uid, route):
    validate_scope(uid, route)

    @firestore.transactional
    def read(transaction):
        owner_ref, current, timezone, _, extension = _context(db, transaction, uid, route, owner_only=False)
        return {**_response(route, current, timezone, extension), 'canEdit': True,
                'usesPersonalLegacy': owner_ref.id == uid and (not extension or extension['version'] == 1)}
    return read(db.transaction(max_attempts=3))


def read_route_legacy_view(db, route):
    """Trusted worker read of a route-local override, including explicit empty.

    None means no route override. Errors must propagate, never select stale SQL
    or a different user's legacy settings. No client writes or repair occurs.
    """
    validate_scope('schedule-reader', route)

    @firestore.transactional
    def read(transaction):
        snapshot = db.document(f'{COLLECTION}/{route}').get(transaction=transaction)
        if not snapshot.exists:
            return None
        data = snapshot.to_dict()
        if type(data) is not dict:
            raise corrupt()
        validate_scope(data.get('ownerUid'), route)
        extension = _extension(data, data.get('ownerUid'), route)
        if extension['version'] == 1:
            return None
        owner_ref, cycles, timezone, _, _ = _context(db, transaction, extension['ownerUid'], route)
        return {'ownerUid': owner_ref.id, 'cycles': cycles, 'timezone': timezone}

    return read(db.transaction(max_attempts=3))


def save_send_extension(db, uid, route, body):
    """CAS on both legacy values and extension revision, with same-ID retries.

    Caller must perform HTTP authentication, entitlement and capability checks.
    This transaction rechecks route ownership and current actor assignment. Production writes remain
    gated until client/order/adjustment compatibility is also tested.
    """
    validate_scope(uid, route)
    body = _request(body)

    @firestore.transactional
    def save(transaction):
        access = read_send_route_access(db, transaction, uid, route)
        user_ref, current, timezone, ref, extension = _context(db, transaction, uid, route, owner_only=False)
        route_scoped = (uid != access.owner_uid or len(extract_owned_routes_for_owner(access.owner)) > 1
                        or bool(extension and extension['version'] == 2))
        # Actor-bound receipts prevent another assigned user from borrowing an
        # acknowledged save. Existing owner V1 receipts retain their digest.
        digest = _digest({'actorUid': uid, 'request': body}) if route_scoped else _digest(body)
        revision = extension['revision'] if extension else 0
        receipts = extension['receipts'] if extension else []
        for receipt in receipts:
            if receipt['id'] == body['requestId']:
                legacy_owner_retry = (uid == access.owner_uid
                                      and receipt['hash'] == _digest(body))
                if receipt['hash'] != digest and not legacy_owner_retry:
                    raise StructuredApiError(409, 'This request identifier was already used.', 'SCHEDULE_REQUEST_CONFLICT')
                # Report the present state separately from the historical save.
                # A retry must not restore values an old device changed later.
                return {**_response(route, current, timezone, extension, saved_revision=receipt['revision']),
                        'replayed': True, 'requestId': body['requestId']}
        if current != body['expectedCycles'] or revision != body['expectedRevision']:
            raise StructuredApiError(409, 'The saved schedule changed. Your draft is kept.', 'SCHEDULE_REVISION_CONFLICT')
        next_extension = {'version': 2 if route_scoped else 1, 'ownerUid': access.owner_uid, 'routeNumber': route,
                          'revision': revision + 1, 'timezone': timezone, 'cycles': body['cycles'],
                          'receipts': (receipts + [{'id': body['requestId'], 'hash': digest,
                                                   'revision': revision + 1}])[-RECEIPT_LIMIT:]}
        next_legacy = legacy_cycles(body['cycles'])
        if route_scoped:
            next_extension['legacyOwnerBase'] = deepcopy(_legacy(
                access.owner['userSettings']['notifications']['scheduling']['orderCycles']))
        else:
            transaction.update(user_ref, {'userSettings.notifications.scheduling.orderCycles': next_legacy,
                                         'userSettings.timestamps.updatedAt': firestore.SERVER_TIMESTAMP})
        transaction.set(ref, next_extension)
        return {**_response(route, next_legacy, timezone, next_extension, saved_revision=revision + 1),
                'replayed': False, 'requestId': body['requestId']}
    return save(db.transaction(max_attempts=3))
