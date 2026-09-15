"""Additive Send-order transport. Creation off by default, saved reads permanent."""
import logging
import time
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request, Response
from starlette.concurrency import run_in_threadpool
from ..dependencies import get_firestore, require_route_feature_access, verify_firebase_token
from ..errors import StructuredApiError
from ..middleware.rate_limit import limiter, authenticated_or_ip_key
from ..models import OrderCreateRequest
from ..order_adjustment_schedule_v2_release import request_capabilities
from ..order_send_evidence import create_send_order, read_send_evidence, resolve_send_order_window
from ..schedule_source_contract import validate_scope
from ..order_send_rollout import route_enabled
from .schedule_source import _parse

router = APIRouter()
logger = logging.getLogger('api.order_send')
CAPABILITY = 'order-send-v1'
ENABLED_ENV = 'ORDER_SEND_CREATION_ENABLED'
ROUTES_ENV = 'ORDER_SEND_PILOT_ROUTES'


def _now_ms():
    return int(time.time() * 1000)


async def guard(route, token, db, headers):
    validate_scope(token.get('uid'), route)
    await require_route_feature_access(route, 'ordering', token, db)
    if CAPABILITY not in request_capabilities(headers):
        raise StructuredApiError(409, 'An app update is required.', 'ORDER_SEND_UPGRADE_REQUIRED')


def parse_body(value):
    if type(value) is not dict or set(value) - set(OrderCreateRequest.model_fields):
        raise StructuredApiError(422, 'Invalid order request.', 'ORDER_CREATE_INVALID')
    try:
        return OrderCreateRequest.model_validate(value)
    except ValueError:
        raise StructuredApiError(422, 'Invalid order request.', 'ORDER_CREATE_INVALID') from None


async def safe_call(fn, *args):
    try:
        return await run_in_threadpool(fn, *args)
    except (StructuredApiError, HTTPException):
        raise
    except Exception as error:
        logger.warning('order_send_unavailable error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Order state could not be verified. Retry the same request.', 'ORDER_SEND_UNAVAILABLE') from None


@router.post('/orders/send')
@limiter.limit('10/minute', key_func=authenticated_or_ip_key)
async def post_send_order(request: Request, response: Response, route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                          decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    await guard(route, decoded_token, db, request.headers)
    if not route_enabled(route, ENABLED_ENV, ROUTES_ENV):
        raise StructuredApiError(409, 'New order timing is not enabled yet.', 'ORDER_SEND_CREATION_DISABLED')
    payload = await _parse(request, parse_body)
    if payload.routeNumber != route:
        raise StructuredApiError(403, 'Route mismatch.', 'ORDER_ROUTE_MISMATCH')
    result = await safe_call(create_send_order, db, decoded_token['uid'], payload)
    response.headers['Cache-Control'] = 'private, no-store'
    return {'id': result['order']['id'], 'routeNumber': route, 'sendEvidence': result['sendEvidence'], 'replayed': result['replayed']}


def read_saved(db, uid, route, order_id):
    from google.cloud import firestore
    from ..order_send_access import read_send_route_access
    @firestore.transactional
    def read(transaction):
        read_send_route_access(db, transaction, uid, route)
        snapshot = db.collection('routes').document(route).collection('orders').document(order_id).get(transaction=transaction)
        order = snapshot.to_dict() if snapshot.exists else None
        if not order or order.get('id') != order_id or order.get('routeNumber') != route:
            raise StructuredApiError(404, 'Order not found.', 'ORDER_NOT_FOUND')
        return read_send_evidence(db, order, transaction)
    return read(db.transaction(max_attempts=3))


@router.get('/orders/{order_id}/send-evidence')
@limiter.limit('60/minute', key_func=authenticated_or_ip_key)
async def get_send_evidence(request: Request, response: Response,
                            order_id: str = Path(..., pattern=r'^order-[0-9]{1,10}-send-[a-f0-9]{64}$'),
                            route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                            decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    await guard(route, decoded_token, db, request.headers)
    evidence = await safe_call(read_saved, db, decoded_token['uid'], route, order_id)
    response.headers['Cache-Control'] = 'private, no-store'
    return {'id': order_id, 'routeNumber': route, 'sendEvidence': evidence}


def read_saved_window(db, uid, route, order_id):
    """Read-only mail-handoff preflight, not a receipt or an email send lease."""
    from google.cloud import firestore
    from ..order_send_access import read_send_route_access

    @firestore.transactional
    def read(transaction):
        access = read_send_route_access(db, transaction, uid, route)
        user = access.actor
        snapshot = db.document(f'routes/{route}/orders/{order_id}').get(transaction=transaction)
        order = snapshot.to_dict() if snapshot.exists else None
        if not order or order.get('id') != order_id or order.get('routeNumber') != route:
            raise StructuredApiError(404, 'Order not found.', 'ORDER_NOT_FOUND')
        evidence = read_send_evidence(db, order, transaction)
        try:
            cutoff = user['userSettings']['notifications']['orderAdjustmentSettings']['cutoffTimeLocal']
            checked_at = _now_ms()
            window = resolve_send_order_window(order, evidence, cutoff, checked_at)
        except (KeyError, TypeError, ValueError):
            raise StructuredApiError(409, 'The saved adjustment window needs review.', 'ORDER_SEND_WINDOW_INVALID') from None
        return {'id': order_id, 'routeNumber': route, 'checkedAtMs': checked_at, 'window': window}
    return read(db.transaction(max_attempts=3))


@router.get('/orders/{order_id}/adjustment-window')
@limiter.limit('30/minute', key_func=authenticated_or_ip_key)
async def get_send_window(request: Request, response: Response,
                          order_id: str = Path(..., pattern=r'^order-[0-9]{1,10}-send-[a-f0-9]{64}$'),
                          route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                          decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    await guard(route, decoded_token, db, request.headers)
    result = await safe_call(read_saved_window, db, decoded_token['uid'], route, order_id)
    response.headers['Cache-Control'] = 'private, no-store'
    return result
