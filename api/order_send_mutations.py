"""Send-backed canonical mutations: validate identity, not a new mail deadline.

Supplier confirmation and recovery are not permission to send another email.
Reallocation retains its separate delivery-date boundary. Ordinary orders never
read the additive evidence or change their existing authorization path here.
"""
from datetime import datetime, timezone

from .errors import StructuredApiError
from .order_send_evidence import is_send_order_id, read_send_evidence, resolve_send_order_window
from .order_send_access import read_send_route_access


def mutation_now():
    """Request clock, independently replaceable by isolated endpoint tests."""
    return datetime.now(timezone.utc)


def validate_send_mutation_source(db, transaction, *, order, order_id, route, uid, now):
    if not is_send_order_id(order_id):
        return None
    if db is None:
        raise StructuredApiError(503, 'Saved order state is unavailable.', 'ORDER_SEND_UNAVAILABLE')
    # Existing endpoint-specific owner/approval restrictions remain in place.
    # This shared linkage check must not exclude an otherwise authorized member.
    read_send_route_access(db, transaction, uid, route)
    if order.get('id') != order_id or order.get('routeNumber') != route:
        raise StructuredApiError(409, 'Order identity changed.', 'ADJUSTMENT_SOURCE_CHANGED')
    if order.get('status') != 'finalized':
        raise StructuredApiError(409, 'Only finalized orders can be adjusted.', 'ADJUSTMENT_ORDER_NOT_FINALIZED')
    try:
        evidence = read_send_evidence(db, order, transaction)
        # The shared resolver validates the frozen linkage and truthful Finalize.
        # This placeholder cutoff does not authorize an action: status is ignored.
        # No current setting or expired deadline can obstruct receipt recovery.
        result = resolve_send_order_window(order, evidence, '23:59', int(now.timestamp() * 1000))
    except ValueError:
        raise StructuredApiError(409, 'Saved Send evidence needs review.', 'ORDER_SEND_EVIDENCE_INVALID') from None
    return result['timezone']


def read_send_finalize_receipt(db, *, order_id, route, uid, now):
    """Acknowledge a committed Finalize without replaying any side effects.

    The saved order is the receipt. Recheck the current owner and immutable
    evidence in one read transaction, including on concurrent/lost-response
    retries. A draft, missing anchor or different owner cannot be acknowledged.
    """
    from google.cloud import firestore

    if not is_send_order_id(order_id):
        raise ValueError('Finalize receipt is only available for Send orders')

    @firestore.transactional
    def read(transaction):
        snapshot = db.document(f'routes/{route}/orders/{order_id}').get(transaction=transaction)
        order = snapshot.to_dict() if snapshot.exists else {}
        validate_send_mutation_source(db, transaction, order=order, order_id=order_id,
                                      route=route, uid=uid, now=now)
        return {'orderId': order_id, 'status': 'finalized', 'replayed': True,
                'scheduledOrder': order['scheduledOrder'],
                'sync': {'synced': False, 'status': 'not_repeated'},
                'forecastQueue': {'enqueued': False, 'status': 'not_repeated'}}

    return read(db.transaction(max_attempts=3))
