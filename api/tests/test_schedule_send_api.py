import json
import logging
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api.errors import StructuredApiError, install_api_error_handlers
from order_forecast.api.routers import schedule_send as api
from order_forecast.api.usage_analytics import classify_api_feature, extract_route_hint

CYCLE = {'scheduleVersion': 3, 'orderDay': 2, 'sendOffsetDays': 2, 'loadOffsetDays': 6,
         'deliveryOffsetDays': 6, 'loadDay': 1, 'deliveryDay': 1, 'needsScheduleReview': False}
LEGACY = {**{key: value for key, value in CYCLE.items() if key != 'sendOffsetDays'}, 'scheduleVersion': 2}
BODY = {'cycles': [CYCLE], 'expectedCycles': [LEGACY], 'expectedRevision': 0, 'requestId': 'send-request-000001'}


class SendScheduleHttpTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(api.router, prefix='/api')
        install_api_error_handlers(app, debug_mode=False, app_logger=logging.getLogger('test.send_schedule'))
        app.dependency_overrides[api.verify_firebase_token] = lambda: {'uid': 'owner-a'}
        app.dependency_overrides[api.get_firestore] = lambda: object()
        self.app = app
        self.client = TestClient(app)
        self.path = '/api/schedule/send-settings?route=000001'
        self.headers = {'x-routespark-capabilities': api.CAPABILITY}
        self.patchers = [patch.object(api.limiter, 'enabled', False),
                         patch.object(api, 'require_route_access', new=AsyncMock()),
                         patch.object(api, 'read_send_extension', return_value={'revision': 0}),
                         patch.object(api, 'save_send_extension', return_value={'revision': 1, 'requestId': BODY['requestId']}),
                         patch.dict('os.environ', {api.WRITE_ENV: 'true', api.PILOT_ENV: '000001'})]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self):
        self.client.close()
        for patcher in reversed(self.patchers):
            patcher.stop()

    def test_verified_identity_and_exact_request_go_to_additive_writer(self):
        response = self.client.put(self.path, json=BODY, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'private, no-store')
        self.assertEqual(api.save_send_extension.call_args.args[1:], ('owner-a', '000001', BODY))

    def test_current_read_available_when_writes_paused(self):
        with patch.dict('os.environ', {api.WRITE_ENV: ''}):
            response = self.client.get(self.path, headers=self.headers)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers['cache-control'], 'private, no-store')
        api.save_send_extension.assert_not_called()

    def test_full_audience_keeps_capability_and_live_access_guards(self):
        with patch.dict('os.environ', {'ORDER_SEND_ROLLOUT_MODE': 'all_authorized', api.PILOT_ENV: ''}):
            self.assertEqual(self.client.put(self.path, json=BODY, headers=self.headers).status_code, 200)
            api.save_send_extension.reset_mock()
            self.assertEqual(self.client.put(self.path, json=BODY).status_code, 409)
            api.read_send_extension.side_effect = StructuredApiError(403, 'Denied', 'SCHEDULE_ACCESS_DENIED')
            self.assertEqual(self.client.put(self.path, content=b'invalid', headers=self.headers).status_code, 403)
            api.save_send_extension.assert_not_called()

    def test_new_feature_gates_do_not_use_replacement_enrollment_flags(self):
        for changes, headers in [({api.WRITE_ENV: ''}, self.headers), ({api.PILOT_ENV: '000002'}, self.headers), ({}, {})]:
            with patch.dict('os.environ', changes):
                self.assertEqual(self.client.put(self.path, content=b'invalid', headers=headers).status_code, 409)
        api.save_send_extension.assert_not_called()

    def test_old_capability_does_not_enable_new_endpoint(self):
        for capability in ['scheduled-order-day-v2', 'order-schedule-v3']:
            self.assertEqual(self.client.get(self.path, headers={'x-routespark-capabilities': capability}).status_code, 409)
        api.read_send_extension.assert_not_called()

    def test_authentication_denial_precedes_all_reads(self):
        def denied():
            raise HTTPException(401, 'Authentication required')
        self.app.dependency_overrides[api.verify_firebase_token] = denied
        self.assertEqual(self.client.put(self.path, content=b'invalid', headers=self.headers).status_code, 401)
        api.read_send_extension.assert_not_called()
        api.save_send_extension.assert_not_called()

    def test_route_and_canonical_owner_denial_precede_body_parsing(self):
        for target in [api.require_route_access, api.read_send_extension]:
            target.side_effect = StructuredApiError(403, 'Denied', 'SCHEDULE_ACCESS_DENIED')
            self.assertEqual(self.client.put(self.path, content=b'invalid', headers=self.headers).status_code, 403)
            target.side_effect = None
        api.save_send_extension.assert_not_called()

    def test_input_limits_and_duplicate_keys(self):
        for raw, expected in [(b'{', 400), (b'{"cycles":[],"cycles":[]}', 400),
                              (b'{"x":NaN}', 400), (b' ' * 16385, 413),
                              (json.dumps({**BODY, 'ownerUid': 'forged'}).encode(), 422),
                              (json.dumps({**BODY, 'expectedRevision': True}).encode(), 422)]:
            self.assertEqual(self.client.put(self.path, content=raw,
                             headers={**self.headers, 'content-type': 'application/json'}).status_code, expected)
        self.assertEqual(self.client.put(self.path, content=b'{}', headers=self.headers).status_code, 415)
        api.save_send_extension.assert_not_called()

    def test_failures_are_sanitized_and_write_status_is_unknown(self):
        api.save_send_extension.side_effect = RuntimeError('sensitive-database-value')
        response = self.client.put(self.path, json=BODY, headers=self.headers)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('sensitive-database-value', response.text)
        self.assertEqual(response.json()['code'], 'SCHEDULE_UNAVAILABLE')
        api.read_send_extension.side_effect = RuntimeError('another-secret')
        response = self.client.get(self.path, headers=self.headers)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('another-secret', response.text)

    def test_route_shape_and_telemetry(self):
        self.assertEqual(self.client.get('/api/schedule/send-settings?route=bad', headers=self.headers).status_code, 422)
        api.read_send_extension.assert_not_called()
        self.assertEqual(classify_api_feature('/api/schedule/send-settings'), 'schedule')
        self.assertEqual(extract_route_hint('/api/schedule/send-settings', {'route': '000001'}), '000001')
        from order_forecast.api.routers.schedule import router
        # Exercise nested router dispatch, not FastAPI's internal route-list
        # representation, which differs between local and production versions.
        parent_app = FastAPI()
        parent_app.include_router(router, prefix='/api')
        parent_app.dependency_overrides.update(self.app.dependency_overrides)
        with TestClient(parent_app) as parent_client:
            self.assertEqual(parent_client.get(self.path, headers=self.headers).status_code, 200)
            self.assertEqual(parent_client.put(self.path, json=BODY, headers=self.headers).status_code, 200)
