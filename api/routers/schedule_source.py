"""Owner-only V3 source API. Writes default off; no automatic opt-in."""
import json
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from starlette.concurrency import run_in_threadpool

from ..dependencies import get_firestore, require_route_access, verify_firebase_token
from ..errors import StructuredApiError
from ..middleware.rate_limit import authenticated_or_ip_key, limiter
from ..order_adjustment_schedule_v2_release import request_capabilities
from ..schedule_source_contract import validate_scope, validate_write
from ..schedule_source_store import read_source, save_source
from ..schedule_enrollment import enroll_schedule, read_enrollment_owner, validate_enrollment

router = APIRouter()
logger = logging.getLogger('api.schedule_source')
MAX_BODY_BYTES = 16_384
WRITE_ENV = 'ORDER_SCHEDULE_V3_SOURCE_WRITES_ENABLED'
PILOT_ENV = 'ORDER_SCHEDULE_V3_PILOT_ROUTES'
CAPABILITY = 'order-schedule-v3'


def _write_gate(route, headers):
    pilots = {value.strip() for value in os.environ.get(PILOT_ENV, '').split(',') if value.strip()}
    if os.environ.get(WRITE_ENV) != 'true' or route not in pilots:
        raise StructuredApiError(409, 'New schedule saves are not enabled.', 'SCHEDULE_WRITES_DISABLED')
    if CAPABILITY not in request_capabilities(headers):
        raise StructuredApiError(409, 'A compatible app update is required.', 'SCHEDULE_UPGRADE_REQUIRED')


async def _guard(route, token, db):
    uid = token.get('uid')
    validate_scope(uid, route)
    try:
        await require_route_access(route, token, db)
        # Includes transactional canonical ownership checks before body parsing.
        current = await run_in_threadpool(read_source, db, uid, route)
    except (StructuredApiError, HTTPException):
        raise
    except Exception as error:
        logger.warning('schedule_source_unavailable phase=read error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Schedule service is temporarily unavailable.', 'SCHEDULE_UNAVAILABLE') from None
    return uid, current


def _unique_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate key')
        result[key] = value
    return result


def _reject_nonfinite(_):
    raise ValueError('invalid number')


async def _parse(request, validate=validate_write):
    if request.headers.get('content-type', '').split(';')[0].strip().lower() != 'application/json':
        raise StructuredApiError(415, 'Content-Type must be application/json.', 'SCHEDULE_INVALID')
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw) + len(chunk) > MAX_BODY_BYTES:
            raise StructuredApiError(413, 'Schedule request is too large.', 'SCHEDULE_TOO_LARGE')
        raw.extend(chunk)
    try:
        body = json.loads(raw, object_pairs_hook=_unique_keys, parse_constant=_reject_nonfinite)
    except (ValueError, UnicodeError, RecursionError):
        raise StructuredApiError(400, 'Request body must be valid JSON.', 'SCHEDULE_INVALID') from None
    return validate(body)


@router.put('/schedule/enroll')
@limiter.limit('10/minute', key_func=authenticated_or_ip_key)
async def put_schedule_enrollment(request: Request, response: Response,
                                  route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                                  decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    uid = decoded_token.get('uid')
    validate_scope(uid, route)
    await require_route_access(route, decoded_token, db)
    _write_gate(route, request.headers)
    if os.environ.get('ORDER_SCHEDULE_V3_ENROLLMENT_ENABLED') != 'true':
        raise StructuredApiError(409, 'Four-date setup is not enabled yet.', 'SCHEDULE_NOT_ENABLED')
    from ..order_schedule_create import require_creation_enabled
    require_creation_enabled(route, request.headers)
    try:
        await run_in_threadpool(read_enrollment_owner, db, uid, route)
        body = await _parse(request, validate_enrollment)
        result = await run_in_threadpool(enroll_schedule, db, uid, route, body)
    except (StructuredApiError, HTTPException):
        raise
    except Exception as error:
        logger.warning('schedule_enrollment_unavailable error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Save status is unknown. Retry the same request.', 'SCHEDULE_UNAVAILABLE') from None
    response.headers['Cache-Control'] = 'private, no-store'
    return result


@router.get('/schedule/source')
@limiter.limit('30/minute', key_func=authenticated_or_ip_key)
async def get_schedule_source(request: Request, response: Response,
                              route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                              decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    _, result = await _guard(route, decoded_token, db)
    response.headers['Cache-Control'] = 'private, no-store'
    return result


@router.put('/schedule/source')
@limiter.limit('10/minute', key_func=authenticated_or_ip_key)
async def put_schedule_source(request: Request, response: Response,
                              route: str = Query(..., pattern=r'^[0-9]{1,10}$'),
                              decoded_token: dict = Depends(verify_firebase_token), db=Depends(get_firestore)):
    uid, _ = await _guard(route, decoded_token, db)
    _write_gate(route, request.headers)
    body = await _parse(request)
    try:
        # Rechecks ownership and source revision within the commit transaction.
        result = await run_in_threadpool(save_source, db, uid, route, body)
    except StructuredApiError:
        raise
    except Exception as error:
        logger.warning('schedule_source_unavailable phase=write error_type=%s', type(error).__name__)
        raise StructuredApiError(503, 'Save status is unknown. Retry with the same request identifier.',
                                 'SCHEDULE_UNAVAILABLE') from None
    response.headers['Cache-Control'] = 'private, no-store'
    return result
