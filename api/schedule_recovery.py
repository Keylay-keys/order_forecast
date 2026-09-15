"""Leased latest-state recovery, independent of listener events and write gates.

No clients/threads/connections are initialized on import. An explicitly enabled
runtime supplies Firestore and a dedicated projector connection factory.
"""
from datetime import datetime, timedelta, timezone
import re
from uuid import uuid4

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

from .errors import StructuredApiError
from .schedule_projection import ProjectionConflict, project_source
from .schedule_source_contract import validate_scope
from .schedule_source_store import _context

LEASE_SECONDS = 30
MAX_BATCH = 20


def _now(value):
    value = datetime.now(timezone.utc) if value is None else value
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError('aware_clock_required')
    return value


def _valid_time(value):
    return isinstance(value, datetime) and value.tzinfo is not None


def claim(db, reference, now=None):
    now = _now(now)
    match = re.fullmatch(r'orderScheduleProjectionWork/([0-9]{1,10})', reference.path)
    if not match:
        raise ValueError('invalid_projection_path')
    route = match.group(1)

    @firestore.transactional
    def take(transaction):
        pending = reference.get(transaction=transaction).to_dict() or {}
        if pending.get('status') not in {'pending', 'leased'}:
            return None
        if _valid_time(pending.get('dueAt')) and pending['dueAt'] > now:
            return None
        try:
            validate_scope(pending.get('ownerUid'), route)
            if (not _valid_time(pending.get('dueAt')) or type(pending.get('attempts')) is not int
                    or not 0 <= pending['attempts'] <= 1000000
                    or pending['status'] == 'leased' and (not _valid_time(pending.get('leaseUntil'))
                                                         or pending['leaseUntil'] > now)):
                raise ProjectionConflict('invalid_delivery_state')
            _, _, control, cycles, _, _ = _context(db, transaction, pending['ownerUid'], route)
        except (StructuredApiError, ProjectionConflict):
            transaction.update(reference, {'status': 'blocked', 'errorCode': 'source_conflict',
                                            'leaseToken': None, 'leaseUntil': None})
            return None
        token = str(uuid4())
        transaction.update(reference, {'status': 'leased', 'leaseToken': token,
                                        'leaseUntil': now + timedelta(seconds=LEASE_SECONDS),
                                        'dueAt': now + timedelta(seconds=LEASE_SECONDS),
                                        'attempts': min(1000000, pending['attempts'] + 1)})
        return {'reference': reference, 'ownerUid': control['ownerUid'], 'revision': control['revision'],
                'token': token, 'source': {'control': control, 'cycles': cycles}}

    return take(db.transaction(max_attempts=3))


def finish(db, lease, outcome, now=None):
    now = _now(now)
    if outcome not in {'projected', 'retry', 'blocked'}:
        raise ValueError('invalid_outcome')

    @firestore.transactional
    def acknowledge(transaction):
        reference = lease['reference']
        pending = reference.get(transaction=transaction).to_dict() or {}
        if (pending.get('status') != 'leased' or pending.get('ownerUid') != lease['ownerUid']
                or pending.get('sourceRevision') != lease['revision'] or pending.get('leaseToken') != lease['token']):
            return False
        # Even a completed SQL transaction cannot bless an ownership transfer.
        try:
            _, _, control, cycles, _, _ = _context(db, transaction, lease['ownerUid'], lease['source']['control']['routeNumber'])
            if {'control': control, 'cycles': cycles} != lease['source']:
                raise ProjectionConflict('source_changed_without_revision')
        except (StructuredApiError, ProjectionConflict):
            final = 'blocked'
        else:
            final = outcome
        update = {'status': 'pending' if final == 'retry' else final, 'leaseToken': None, 'leaseUntil': None,
                  'errorCode': None if final == 'projected' else 'projection_retry' if final == 'retry' else 'source_conflict'}
        if final == 'retry':
            update['dueAt'] = now + timedelta(seconds=min(3600, 5 * 2 ** min(pending['attempts'], 10)))
        transaction.update(reference, update)
        return final == 'projected'

    return acknowledge(db.transaction(max_attempts=3))


def process_one(db, connect, reference, now=None):
    lease = claim(db, reference, now)
    if lease is None:
        return 'not_claimed'
    try:
        result = project_source(connect, lease['source'])
        outcome = 'projected' if result['status'] in {'projected', 'already_projected'} else 'blocked'
        if outcome == 'projected':
            # A committed projection alone must not consume the only durable
            # refresh intent. Failure here leaves the same revision retryable.
            enqueue_refresh(db, lease['source'])
    except ProjectionConflict:
        outcome = 'blocked'
    except Exception:
        # Connection loss after COMMIT is ambiguous. Retrying the same revision
        # verifies the complete projection without replacing newer rows.
        outcome = 'retry'
    acknowledged = finish(db, lease, outcome, now)
    return 'projected' if acknowledged else 'pending' if outcome == 'retry' else 'not_acknowledged'


def enqueue_refresh(db, source):
    try:
        from ..scripts.schedule_forecast_refresh import enqueue_schedule_refresh
    except ImportError:
        from scripts.schedule_forecast_refresh import enqueue_schedule_refresh
    return enqueue_schedule_refresh(db, source)


def recover_due(db, connect, now=None, limit=MAX_BATCH):
    query_time = _now(now)
    if type(limit) is not int or not 1 <= limit <= MAX_BATCH:
        raise ValueError('invalid_batch_size')
    due = (db.collection('orderScheduleProjectionWork')
           .where(filter=FieldFilter('status', 'in', ['pending', 'leased']))
           .where(filter=FieldFilter('dueAt', '<=', query_time)).order_by('dueAt').limit(limit))
    results = []
    for snapshot in due.stream():
        # Invalid namespace cannot turn a query result into a different write path.
        if not re.fullmatch(r'orderScheduleProjectionWork/[0-9]{1,10}', snapshot.reference.path):
            continue
        try:
            # Preserve None in normal operation: each claim/ack reads its own
            # fresh clock rather than reusing the batch-start query timestamp.
            results.append(process_one(db, connect, snapshot.reference, now))
        except Exception:
            # A failed Firestore ack leaves the lease reclaimable. Other due
            # routes still receive their turn in this bounded batch.
            results.append('retryable_failure')
    return results
