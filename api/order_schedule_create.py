"""Gated, owner-pilot creation with atomic source evidence and retry receipts."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os

from google.cloud import firestore

from .errors import StructuredApiError
from .order_adjustment_schedule_v2_release import request_capabilities
from .order_schedule_evidence import build_evidence, validate_order_evidence
from .schedule_source_store import _context
try:
    from ..scripts.order_schedule_v3_release import v3_consumers_allowed
except ImportError:
    from scripts.order_schedule_v3_release import v3_consumers_allowed


def require_creation_enabled(route, headers):
    if (os.environ.get('ORDER_SCHEDULE_V3_CREATION_ENABLED') != 'true'
            or not v3_consumers_allowed(route)
            or route not in os.environ.get('ORDER_SCHEDULE_V3_PILOT_ROUTES', '').split(',')):
        raise StructuredApiError(409, 'New schedule ordering is not enabled yet.', 'ORDER_SCHEDULE_UPGRADE_REQUIRED')
    if 'order-schedule-v3' not in request_capabilities(headers):
        raise StructuredApiError(409, 'Update the app before creating an order.', 'ORDER_SCHEDULE_UPGRADE_REQUIRED')


def create_confirmed_order(db, uid, payload):
    """Caller must authorize ordering and check release gates first.

    The first write rechecks canonical ownership and the preview revision in
    its transaction. A retry returns the existing order, never a fresh stamp.
    Receipt remains if a user later deletes the draft, preventing resurrection.
    """
    body = payload.model_dump(mode='json')
    if not payload.requestId or payload.expectedScheduleRevision is None:
        raise StructuredApiError(422, 'A saved schedule and request identifier are required.', 'ORDER_CREATE_INVALID')
    route = payload.routeNumber
    request_key = hashlib.sha256(f'{uid}:{route}:{payload.requestId}'.encode()).hexdigest()
    digest = hashlib.sha256(json.dumps(body, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    order_id = f'order-{route}-{request_key[:40]}'
    ref = db.document(f'routes/{route}/orders/{order_id}')
    receipt_ref = db.document(f'orderCreationReceipts/{request_key}')

    @firestore.transactional
    def create(transaction):
        _, _, control, cycles, _, _ = _context(db, transaction, uid, route)
        receipt = receipt_ref.get(transaction=transaction).to_dict()
        existing = ref.get(transaction=transaction)
        if receipt:
            if receipt != {'version': 1, 'ownerUid': uid, 'routeNumber': route,
                           'orderId': order_id, 'requestHash': digest}:
                raise StructuredApiError(409, 'This request was already used for another order.', 'ORDER_CREATE_CONFLICT')
            if not existing.exists:
                raise StructuredApiError(409, 'This order was already removed. Start a new order.', 'ORDER_CREATE_REMOVED')
            saved = existing.to_dict()
            validate_order_evidence(saved)
            return saved
        if existing.exists:
            raise StructuredApiError(409, 'Order identifier already exists.', 'ORDER_CREATE_CONFLICT')
        if control['revision'] != payload.expectedScheduleRevision:
            raise StructuredApiError(409, 'Schedule changed. Choose the delivery date again.', 'ORDER_SCHEDULE_REVISION_CONFLICT')
        stamp = build_evidence(control, cycles, payload.deliveryDate.isoformat(), payload.scheduleKey)
        now = datetime.now(timezone.utc)
        from zoneinfo import ZoneInfo
        order = {
            'id': order_id, 'routeNumber': route, 'userId': uid,
            'orderDate': now.astimezone(ZoneInfo(control['timezone'])).date().isoformat(),
            'expectedDeliveryDate': payload.deliveryDate.isoformat(),
            'expectedLoadDate': (date.fromisoformat(stamp['date']) + timedelta(days=stamp['loadOffsetDays'])).isoformat(),
            'scheduleKey': payload.scheduleKey, 'scheduledOrder': stamp,
            'status': 'draft', 'stores': [], 'createdAt': now, 'updatedAt': now,
            'notes': payload.notes, 'isHolidaySchedule': payload.isHolidaySchedule,
            **({'coreItemPolicyVersion': 1, 'coreItemOverrides': []} if payload.coreItemPolicyVersion == 1 else {}),
        }
        validate_order_evidence(order)
        transaction.create(ref, order)
        transaction.create(receipt_ref, {'version': 1, 'ownerUid': uid, 'routeNumber': route,
                                         'orderId': order_id, 'requestHash': digest})
        return order

    return create(db.transaction(max_attempts=3))
