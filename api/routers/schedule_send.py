"""Additive Send settings. Never enrolls a route into replacement-format data."""
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from starlette.concurrency import run_in_threadpool

from ..dependencies import get_firestore, require_route_access, verify_firebase_token
from ..errors import StructuredApiError
from ..middleware.rate_limit import authenticated_or_ip_key, limiter
from ..order_adjustment_schedule_v2_release import request_capabilities
from ..schedule_source_contract import validate_scope
from ..order_send_rollout import route_enabled
from ..schedule_send_extension import read_send_extension, save_send_extension, _request
from .schedule_source import _parse

router = APIRouter()
logger = logging.getLogger('api.schedule_send')
CAPABILITY = 'order-schedule-send-v1'
WRITE_ENV = 'ORDER_SCHEDULE_SEND_WRITES_ENABLED'
PILOT_ENV = 'ORDER_SCHEDULE_SEND_PILOT_ROUTES'


async def _guard(route, token, db, headers):
    uid = token.get('uid')
    validate_scope(uid, route)
    await require_route_access(route, token, db)
    if CAPABILITY not in request_capabilities(headers):
        raise StructuredApiError(409, 'This feature requires an app update.', 'SCHEDULE_UPGRADE_REQUIRED')
    try:
        # Rechecks current owner and the actor's route assignment before body parsing.
        result = await run_in_threadpool(read_send_extension, db, uid, route)
    except (StructuredApiError, HTTPException):
        raise
    except Exception as error:
        logger.warning('schedule_send_unavailable phase=read error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Could not load the saved schedule. Try again.', 'SCHEDULE_UNAVAILABLE') from None
    return uid, result


@router.get('/schedule/send-settings')
@limiter.limit('30/minute', key_func=authenticated_or_ip_key)
async def get_send_settings(request: Request, response: Response,
                            route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                            decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    _, result = await _guard(route, decoded_token, db, request.headers)
    response.headers['Cache-Control'] = 'private, no-store'
    return result


@router.put('/schedule/send-settings')
@limiter.limit('10/minute', key_func=authenticated_or_ip_key)
async def put_send_settings(request: Request, response: Response,
                            route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                            decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    uid, _ = await _guard(route, decoded_token, db, request.headers)
    if not route_enabled(route, WRITE_ENV, PILOT_ENV):
        raise StructuredApiError(409, 'Schedule saving is not available yet. Your draft is kept.', 'SCHEDULE_WRITES_DISABLED')
    body = await _parse(request, _request)
    try:
        result = await run_in_threadpool(save_send_extension, db, uid, route, body)
    except (StructuredApiError, HTTPException):
        raise
    except Exception as error:
        logger.warning('schedule_send_unavailable phase=write error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Save not confirmed. Retry the same request.', 'SCHEDULE_UNAVAILABLE') from None
    response.headers['Cache-Control'] = 'private, no-store'
    return result
