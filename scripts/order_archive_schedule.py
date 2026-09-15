"""Preserve immutable four-date evidence independently of cleanup eligibility."""
from copy import deepcopy
from datetime import datetime, timezone
import re


def is_send_archive(order_id):
    return isinstance(order_id, str) and re.fullmatch(r'order-[0-9]{1,10}-send-[a-f0-9]{64}', order_id) is not None


def _send_helpers():
    try:
        from ..api.order_send_evidence import read_send_evidence, resolve_send_order_window
    except ImportError:
        from api.order_send_evidence import read_send_evidence, resolve_send_order_window
    return read_send_evidence, resolve_send_order_window


def load_archive_send_evidence(db, order):
    if not is_send_archive(order.get('id')):
        return None
    return _send_helpers()[0](db, order)


def require_archive_send_owner(db, uid, route):
    """Keep evidence route-scoped, including currently assigned team members.

    Historical helper name retained for the existing archive response callers.
    Their ordinary endpoint authorization remains in place.
    """
    from google.cloud import firestore
    try:
        from ..api.order_send_access import read_send_route_access
    except ImportError:
        from api.order_send_access import read_send_route_access

    @firestore.transactional
    def check(transaction):
        read_send_route_access(db, transaction, uid, route)

    check(db.transaction(max_attempts=3))


def read_archive_send_evidence(cur, order_id, route_number):
    cur.execute('SELECT evidence FROM order_archive_send_evidence WHERE order_id=%s AND route_number=%s',
                [order_id, str(route_number)])
    row = cur.fetchone()
    return row.get('evidence') if isinstance(row, dict) else row[0] if row else None


def store_archive_send_evidence(cur, order, evidence, *, replay=False):
    if not is_send_archive(order.get('id')):
        return
    from psycopg2.extras import Json
    if not replay:
        cur.execute('INSERT INTO order_archive_send_evidence(order_id, route_number, evidence) VALUES (%s,%s,%s) '
                    'ON CONFLICT (order_id) DO NOTHING', [order['id'], order['routeNumber'], Json(evidence)])
    stored = read_archive_send_evidence(cur, order['id'], order['routeNumber'])
    if stored != evidence:
        raise ValueError('Archived Send evidence mismatch')


def archive_schedule_fields(order, send_evidence=None):
    if is_send_archive(order.get('id')):
        if order.get('status') != 'finalized':
            raise ValueError('Only finalized Send orders can be archived')
        # Cutoff is irrelevant to preservation, but the common validator checks
        # truthful Finalize and creation chronology without changing either.
        _send_helpers()[1](order, send_evidence, '23:59', int(datetime.now(timezone.utc).timestamp() * 1000))
        return {key: deepcopy(order[key]) for key in ('scheduledOrder', 'expectedLoadDate', 'adjustmentAnchor', 'submittedAt')}
    stamp = order.get('scheduledOrder')
    if stamp is None or isinstance(stamp, dict) and type(stamp.get('version')) is int and stamp['version'] == 2:
        return None
    try:
        from ..api.order_schedule_evidence import validate_order_evidence
    except ImportError:
        from api.order_schedule_evidence import validate_order_evidence
    if order.get('status') != 'finalized':
        raise ValueError('Only finalized schedule evidence can be archived')
    validated = validate_order_evidence(order)
    if validated['version'] != 3:
        raise ValueError('Unsupported archive schedule evidence')
    return {
        'scheduledOrder': deepcopy(validated),
        'expectedLoadDate': order['expectedLoadDate'],
        'adjustmentAnchor': deepcopy(order['adjustmentAnchor']),
        'submittedAt': order['submittedAt'],
    }


def restore_archive_schedule(archived, row):
    if is_send_archive(archived.get('id')):
        evidence = row.get('send_evidence')
        if not isinstance(evidence, dict):
            raise ValueError('Archived Send evidence missing')
        pickup = row.get('expected_load_date')
        archived.update(scheduledOrder=deepcopy(row.get('scheduled_order')),
                        expectedLoadDate=pickup.isoformat() if hasattr(pickup, 'isoformat') else pickup,
                        adjustmentAnchor=deepcopy(row.get('adjustment_anchor')),
                        submittedAt=row.get('finalized_at'),
                        createdAt=datetime.fromtimestamp(evidence['createdAtMs'] / 1000, timezone.utc))
        archive_schedule_fields(archived, evidence)
        archived.update(createdAt=archived['createdAt'].isoformat(), submittedAt=archived['submittedAt'].isoformat(),
                        sendEvidence=deepcopy(evidence))
        return archived
    stamp = row.get('scheduled_order')
    if stamp is None:
        return archived
    archived['scheduledOrder'] = deepcopy(stamp)
    if isinstance(stamp, dict) and type(stamp.get('version')) is int and stamp['version'] == 2:
        return archived
    pickup = row.get('expected_load_date')
    archived.update(expectedLoadDate=pickup.isoformat() if hasattr(pickup, 'isoformat') else pickup,
                    adjustmentAnchor=deepcopy(row.get('adjustment_anchor')),
                    submittedAt=row.get('finalized_at'))
    archive_schedule_fields(archived)  # Missing or contradictory evidence fails closed.
    archived['submittedAt'] = archived['submittedAt'].isoformat()
    return archived
