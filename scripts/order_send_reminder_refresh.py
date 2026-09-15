"""Bounded settings-change reconciliation for the additive Send pilot.

One server-only checkpoint per route/actor, no historical order scan.
Events are hints: ownership/settings are reread in every transaction. New drafts
still use the existing API reconciliation when their lines are persisted.
"""
import hashlib
import json
import logging
import re

from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# Filename-based CronJob entry points import this as a top-level scripts module.
# Match schedule_sync/schedule_utils: expose only this installed package root,
# never a cwd-dependent path, before importing shared API modules.
if not __package__:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from order_forecast.api import order_send_rollout as rollout
except ModuleNotFoundError as error:
    if error.name != 'order_forecast':
        raise
    from api import order_send_rollout as rollout

try:
    from .order_adjustment_schedule import build_canonical_reminder_fields, meaningful_lines, reminder_fields_equal, trusted_adjustment_settings
except ImportError:
    from order_adjustment_schedule import build_canonical_reminder_fields, meaningful_lines, reminder_fields_equal, trusted_adjustment_settings

COLLECTION = 'orderSendReminderRefresh'
PAGE_SIZE = 25
ENABLED_ENV = 'ORDER_SEND_REMINDER_REFRESH_ENABLED'
ROUTES_ENV = 'ORDER_SEND_REMINDER_REFRESH_ROUTES'
ROUTE_PAGE_SIZE = 10
ROUTE_CURSOR_PATH = 'orderSendReminderRefreshControl/routes'
ROUTE_DIRECTORY = 'orderSendReminderRoutes'
logger = logging.getLogger('order_send_reminder_refresh')


def pilot_routes():
    return rollout.pilot_routes(ENABLED_ENV, ROUTES_ENV, 10)


def route_enabled(route):
    return rollout.route_enabled(route, ENABLED_ENV, ROUTES_ENV, 10)


def _helpers():
    try:
        from order_forecast.api.order_send_access import read_send_route_access
        from order_forecast.api.order_send_evidence import is_send_order_id, read_send_evidence, order_with_send_evidence
    except ModuleNotFoundError as error:
        if error.name != 'order_forecast':
            raise
        from api.order_send_access import read_send_route_access
        from api.order_send_evidence import is_send_order_id, read_send_evidence, order_with_send_evidence
    return read_send_route_access, is_send_order_id, read_send_evidence, order_with_send_evidence


def _settings(user):
    try:
        raw = user['userSettings']['notifications']['orderAdjustmentSettings']
        if not isinstance(raw, dict):
            raise TypeError()
    except (KeyError, TypeError):
        raw = {}
    settings = trusted_adjustment_settings(raw)
    signature = hashlib.sha256(json.dumps(settings, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    return settings, signature


def _validate_job(job, route, uid):
    if (job.get('version') not in (1, 2) or type(job.get('version')) is not int
            or job.get('routeNumber') != route
            or (job.get('actorUid') if job.get('version') == 2 else job.get('ownerUid')) != uid
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(uid or ''))
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(job.get('ownerUid') or ''))
            or type(job.get('pending')) is not bool
            or type(job.get('invalidCount', 0)) is not int or job.get('invalidCount', 0) < 0
            or not re.fullmatch(r'[a-f0-9]{64}', str(job.get('settingsHash') or ''))
            or job.get('cursor') is not None and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(job['cursor']))):
        raise ValueError('Invalid reminder refresh checkpoint')


def enqueue_current(db, uid, route):
    if not route_enabled(route):
        return 'disabled'
    access_for, _, _, _ = _helpers()

    @firestore.transactional
    def enqueue(tx):
        access = access_for(db, tx, uid, route)
        member = uid != access.owner_uid
        ref = db.collection(COLLECTION).document(route)
        if member:
            ref = ref.collection('members').document(uid)
        snap = ref.get(transaction=tx)
        directory = db.collection(ROUTE_DIRECTORY).document(route)
        directory_missing = (rollout.all_authorized_enabled(ENABLED_ENV, ROUTES_ENV)
                             and not directory.get(transaction=tx).exists)
        _, signature = _settings(access.actor)
        existing = snap.to_dict() if snap.exists else None
        if directory_missing:
            # Discover a member's first event without creating or rearming the
            # owner's checkpoint. This index contains no preferences or history.
            tx.set(directory, {'routeNumber': route})
        if existing:
            _validate_job(existing, route, uid)
            if (existing['settingsHash'] == signature and not existing.get('blocked')
                    and existing['ownerUid'] == access.owner_uid):
                return 'unchanged'
        tx.set(ref, {'version': 2 if member else 1, 'routeNumber': route, 'ownerUid': access.owner_uid,
                     **({'actorUid': uid} if member else {}),
                     'settingsHash': signature, 'pending': True, 'cursor': None,
                     'invalidCount': 0,
                     'updatedAt': firestore.SERVER_TIMESTAMP})
        return 'queued'
    return enqueue(db.transaction(max_attempts=3))


def process_page(db, route, *, now_ms, actor_uid=None):
    if not route_enabled(route):
        return {'status': 'disabled', 'read': 0, 'changed': 0}
    access_for, is_send, read_evidence, view = _helpers()
    ref = db.collection(COLLECTION).document(route)
    if actor_uid is not None:
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(actor_uid)):
            raise ValueError('Invalid reminder actor')
        ref = ref.collection('members').document(actor_uid)

    @firestore.transactional
    def process(tx):
        snap = ref.get(transaction=tx)
        job = snap.to_dict() if snap.exists else None
        if not job:
            return {'status': 'idle', 'read': 0, 'changed': 0}
        uid = actor_uid or job.get('ownerUid')
        _validate_job(job, route, uid)
        if not job['pending']:
            return {'status': 'idle', 'read': 0, 'changed': 0}
        try:
            access = access_for(db, tx, uid, route)
            if access.owner_uid != job['ownerUid']:
                if actor_uid is not None:
                    tx.update(ref, {'pending': False, 'blocked': 'owner_changed',
                                    'updatedAt': firestore.SERVER_TIMESTAMP})
                    return {'status': 'access_denied', 'read': 0, 'changed': 0}
                raise ValueError('Reminder route owner changed')
        except Exception as error:
            # A revoked member cannot crowd out valid pending members forever.
            # Transient read failures retain the checkpoint for retry.
            if actor_uid is None or getattr(error, 'status_code', None) != 403:
                raise
            tx.update(ref, {'pending': False, 'blocked': 'access_denied',
                            'updatedAt': firestore.SERVER_TIMESTAMP})
            return {'status': 'access_denied', 'read': 0, 'changed': 0}
        settings, signature = _settings(access.actor)
        # A newer save, including a legacy device save, invalidates the old cursor.
        cursor = job['cursor'] if signature == job['settingsHash'] else None
        collection = db.collection('routes').document(route).collection('orderAdjustments')
        query = collection.where(filter=FieldFilter('status', '==', 'draft')).order_by('__name__').limit(PAGE_SIZE)
        if cursor:
            query = query.start_after({'__name__': collection.document(cursor)})
        docs = list(query.stream(transaction=tx))
        updates = []
        invalid = 0
        for doc in docs:
            data = doc.to_dict() or {}
            order_id = data.get('sourceOrderId')
            if not is_send(order_id):
                continue
            if actor_uid is not None and data.get('userId') != uid:
                continue  # This actor's checkpoint never changes another author.
            if (data.get('routeNumber') != route or data.get('userId') != uid
                    or data.get('id') != doc.id):
                invalid += 1
                continue
            if data.get('reminderStatus') in ('sending', 'sent'):
                continue  # An active claim or successful send cannot be re-armed.
            source_snap = db.document(f'routes/{route}/orders/{order_id}').get(transaction=tx)
            source = source_snap.to_dict() if source_snap.exists else None
            try:
                evidence = read_evidence(db, source or {'id': order_id}, tx)
                source_view = view(source, evidence)
            except (ValueError, KeyError, TypeError):
                invalid += 1
                continue  # Never fall back to another window or rewrite history.
            fields = build_canonical_reminder_fields(source_view, settings=settings,
                has_meaningful_lines=meaningful_lines(data), status='draft', now_ms=now_ms, allow_v3=True)
            if fields.get('reminderStatus') == data.get('reminderStatus') == 'skipped' and fields.get('reminderSkipReason') == data.get('reminderSkipReason'):
                fields['reminderSkippedAtMs'] = data.get('reminderSkippedAtMs', now_ms)
            if not reminder_fields_equal(data, fields):
                updates.append((doc.reference, fields))
        # All reads precede writes. Page effects and cursor advance are atomic.
        for adjustment_ref, fields in updates:
            tx.update(adjustment_ref, {**fields, 'updatedAt': firestore.SERVER_TIMESTAMP})
        more = len(docs) == PAGE_SIZE
        tx.update(ref, {'settingsHash': signature, 'cursor': docs[-1].id if more else None,
                       'pending': more, 'invalidCount': invalid + (job.get('invalidCount', 0) if cursor else 0),
                       'updatedAt': firestore.SERVER_TIMESTAMP})
        return {'status': 'pending' if more else 'complete', 'read': len(docs), 'changed': len(updates), 'invalid': invalid}
    return process(db.transaction(max_attempts=3))


def run_pending(db, *, now_ms):
    results = {}
    routes = pilot_routes()
    route_scan = None
    route_jobs = []
    route_cursor = None
    if rollout.all_authorized_enabled(ENABLED_ENV, ROUTES_ENV):
        # Only server-owned refresh checkpoints, never users or order history.
        # Rotate past malformed/failed rows so later routes are not starved.
        route_scan = db.document(ROUTE_CURSOR_PATH)
        state = route_scan.get()
        route_cursor = (state.to_dict() or {}).get('after') if state.exists else None
        if route_cursor is not None and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(route_cursor)):
            raise ValueError('Invalid route scan cursor')
        collection = db.collection(ROUTE_DIRECTORY)
        query = collection.order_by('__name__').limit(ROUTE_PAGE_SIZE)
        route_jobs = list((query.start_after({'__name__': collection.document(route_cursor)}) if route_cursor else query).stream())
        if not route_jobs and route_cursor:
            route_jobs = list(query.stream())
        routes = tuple(job.id for job in route_jobs if re.fullmatch(r'[0-9]{1,10}', job.id))
    for route in routes:
        try:
            results[route] = process_page(db, route, now_ms=now_ms)
            if results[route].get('invalid'):
                logger.warning('reminder_refresh_invalid route=%s count=%s', route, results[route]['invalid'])
        except Exception as error:
            # Checkpoint remains pending for next tick; one route cannot stop others.
            logger.warning('reminder_refresh_failed route=%s error_type=%s', route, type(error).__name__)
            results[route] = {'status': 'retry'}
        # Single-field bounded query, no new composite index. Completed/revoked
        # checkpoints leave the pending set; failed reads remain retryable.
        try:
            members = db.collection(COLLECTION).document(route).collection('members')
            scan = db.collection(COLLECTION).document(route).collection('scanCursor').document('members')
            state = scan.get()
            cursor = (state.to_dict() or {}).get('after') if state.exists else None
            if cursor is not None and not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', str(cursor)):
                raise ValueError('Invalid member scan cursor')
            query = members.where(filter=FieldFilter('pending', '==', True)).order_by('__name__').limit(PAGE_SIZE)
            jobs = list((query.start_after({'__name__': members.document(cursor)}) if cursor else query).stream())
            if not jobs and cursor:
                jobs = list(query.stream())
            counts = {}
            for job in jobs:
                try:
                    result = process_page(db, route, now_ms=now_ms, actor_uid=job.id)
                    counts[result['status']] = counts.get(result['status'], 0) + 1
                except Exception as error:
                    counts['retry'] = counts.get('retry', 0) + 1
                    logger.warning('member_reminder_refresh_failed route=%s error_type=%s', route, type(error).__name__)
            # Rotate even past failed checkpoints, so a bad member cannot
            # starve later actors. Unprocessed work remains pending after crash.
            if jobs or cursor:
                scan.set({'after': jobs[-1].id if len(jobs) == PAGE_SIZE else None})
            if counts:
                results[route]['members'] = counts
        except Exception as error:
            logger.warning('member_reminder_refresh_query_failed route=%s error_type=%s', route, type(error).__name__)
            results[route]['members'] = {'retry': 1}
    if route_scan is not None and (route_jobs or route_cursor):
        route_scan.set({'after': route_jobs[-1].id if len(route_jobs) == ROUTE_PAGE_SIZE else None})
    return results
