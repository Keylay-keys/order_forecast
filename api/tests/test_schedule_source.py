"""Credential-free contract and HTTP boundary tests for the disabled V3 API."""
import json
import logging
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from order_forecast.api.errors import StructuredApiError, install_api_error_handlers
from order_forecast.api.routers import schedule_source as api
from order_forecast.api.schedule_source_contract import validate_write
from order_forecast.api.usage_analytics import classify_api_feature, extract_route_hint

CYCLE = {'scheduleVersion': 3, 'orderDay': 2, 'sendOffsetDays': 2, 'loadOffsetDays': 6,
         'deliveryOffsetDays': 6, 'loadDay': 1, 'deliveryDay': 1, 'needsScheduleReview': False}
BODY = {'cycles': [CYCLE], 'expectedRevision': 1, 'requestId': 'request-0000000001'}


class ContractTests(unittest.TestCase):
    def test_exact_dates_and_empty_clear(self):
        self.assertEqual(validate_write(BODY), BODY)
        self.assertEqual(validate_write({**BODY, 'cycles': []})['cycles'], [])

    def test_invalid_contract(self):
        cases = [{**BODY, 'ownerUid': 'other'}, {**BODY, 'expectedRevision': True},
                 {**BODY, 'expectedRevision': 0}, {**BODY, 'expectedRevision': 1.0},
                 {**BODY, 'requestId': '../invalid-path'}, {**BODY, 'cycles': [CYCLE, CYCLE]}]
        for key, value in [('orderDay', True), ('scheduleVersion', 2), ('sendOffsetDays', 7),
                           ('sendOffsetDays', -1), ('sendOffsetDays', '2'), ('loadDay', 2),
                           ('deliveryOffsetDays', 61), ('needsScheduleReview', True),
                           ('sendOffsetDays', float('nan')), ('sendOffsetDays', float('inf'))]:
            cases.append({**BODY, 'cycles': [{**CYCLE, key: value}]})
        cases += [{**BODY, 'cycles': [{k: v for k, v in CYCLE.items() if k != 'sendOffsetDays'}]},
                  {**BODY, 'cycles': [{**CYCLE, 'unknown': 1}]},
                  {**BODY, 'cycles': [{**CYCLE, 'sendOffsetDays': 10 ** 1000}]}]
        for body in cases:
            with self.subTest(body=body), self.assertRaises(StructuredApiError):
                validate_write(body)


class HttpTests(unittest.TestCase):
    def test_enrollment_refuses_activation_without_order_creation_support(self):
        with patch.dict('os.environ', {'ORDER_SCHEDULE_V3_ENROLLMENT_ENABLED': 'true',
                                      'ORDER_SCHEDULE_V3_CREATION_ENABLED': ''}), patch.object(api, 'enroll_schedule') as save:
            result = self.client.put('/api/schedule/enroll?route=000001', content=b'bad', headers=self.headers)
            self.assertEqual(result.status_code, 409)
            save.assert_not_called()

    def test_enrollment_is_separately_gated_and_checks_owner_before_body(self):
        path = '/api/schedule/enroll?route=000001'
        with patch.dict('os.environ', {'ORDER_SCHEDULE_V3_ENROLLMENT_ENABLED': ''}), patch.object(api, 'enroll_schedule') as save:
            self.assertEqual(self.client.put(path, content=b'bad', headers=self.headers).status_code, 409)
            save.assert_not_called()
        with patch.dict('os.environ', {'ORDER_SCHEDULE_V3_ENROLLMENT_ENABLED': 'true'}), \
                patch.object(api, 'read_enrollment_owner', side_effect=StructuredApiError(403, 'denied', 'SCHEDULE_ACCESS_DENIED')), \
                patch.object(api, 'enroll_schedule') as save:
            self.assertEqual(self.client.put(path, content=b'bad', headers=self.headers).status_code, 403)
            save.assert_not_called()

    def test_enrollment_uses_only_verified_identity_and_validated_body(self):
        legacy = {k: v for k, v in CYCLE.items() if k != 'sendOffsetDays'}
        legacy['scheduleVersion'] = 2
        body = {'cycles': [CYCLE], 'expectedCycles': [legacy], 'requestId': 'enrollment-00000001'}
        with patch.dict('os.environ', {'ORDER_SCHEDULE_V3_ENROLLMENT_ENABLED': 'true'}), \
                patch.object(api, 'read_enrollment_owner'), patch.object(api, 'enroll_schedule', return_value={'revision': 1}) as save:
            result = self.client.put('/api/schedule/enroll?route=000001', json=body, headers=self.headers)
            self.assertEqual(result.status_code, 200, result.text)
            self.assertEqual(save.call_args.args[1:], ('owner-a', '000001', body))
            incomplete = {**body, 'cycles': [legacy]}
            self.assertEqual(self.client.put('/api/schedule/enroll?route=000001', json=incomplete, headers=self.headers).status_code, 422)
            self.assertEqual(save.call_count, 1)

    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(api.router, prefix='/api')
        install_api_error_handlers(self.app, debug_mode=False, app_logger=logging.getLogger('test.schedule'))
        self.app.dependency_overrides[api.verify_firebase_token] = lambda: {'uid': 'owner-a'}
        self.app.dependency_overrides[api.get_firestore] = lambda: object()
        self.client = TestClient(self.app)
        self.headers = {'x-routespark-capabilities': api.CAPABILITY}
        self.path = '/api/schedule/source?route=000001'
        self.stack = []
        for patcher in [patch.object(api.limiter, 'enabled', False),
                        patch.dict('os.environ', {api.WRITE_ENV: 'true', api.PILOT_ENV: '000001',
                            'ORDER_SCHEDULE_V3_CREATION_ENABLED': 'true',
                            'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMERS_ENABLED': 'true',
                            'ORDER_ADJUSTMENT_SCHEDULE_V3_CONSUMER_ROUTES': '000001'}),
                        patch.object(api, 'require_route_access', new=AsyncMock()),
                        patch.object(api, 'read_source', return_value={'revision': 1}),
                        patch.object(api, 'save_source', return_value={'revision': 2, 'projection': {'status': 'pending'}})]:
            self.stack.append(patcher)
            patcher.start()

    def tearDown(self):
        self.client.close()
        for patcher in reversed(self.stack):
            patcher.stop()

    def test_success_uses_verified_uid_and_returns_pending_not_projected(self):
        result = self.client.put(self.path, json=BODY, headers=self.headers)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()['projection']['status'], 'pending')
        self.assertEqual(result.headers['cache-control'], 'private, no-store')
        self.assertEqual(api.save_source.call_args.args[1:], ('owner-a', '000001', BODY))

    def test_default_off_and_pilot_and_capability_gates(self):
        for env, headers, code in [({api.WRITE_ENV: ''}, self.headers, 'SCHEDULE_WRITES_DISABLED'),
                                   ({api.PILOT_ENV: '000002'}, self.headers, 'SCHEDULE_WRITES_DISABLED'),
                                   ({}, {}, 'SCHEDULE_UPGRADE_REQUIRED')]:
            with self.subTest(code=code), patch.dict('os.environ', env):
                result = self.client.put(self.path, json=BODY, headers=headers)
                self.assertEqual(result.json()['code'], code)
        api.save_source.assert_not_called()

    def test_read_stays_available_when_write_gate_is_off(self):
        with patch.dict('os.environ', {api.WRITE_ENV: ''}):
            self.assertEqual(self.client.get(self.path).status_code, 200)

    def test_auth_failure_precedes_body_or_store_reads(self):
        async def no_auth():
            raise HTTPException(401, 'Authentication required')
        self.app.dependency_overrides[api.verify_firebase_token] = no_auth
        self.assertEqual(self.client.put(self.path, content=b'bad').status_code, 401)
        api.read_source.assert_not_called()
        api.save_source.assert_not_called()

    def test_route_and_owner_denial_precede_body_parse(self):
        for target in (api.require_route_access, api.read_source):
            target.side_effect = StructuredApiError(403, 'Denied', 'SCHEDULE_ACCESS_DENIED')
            result = self.client.put(self.path, content=b'not-json', headers=self.headers)
            self.assertEqual(result.status_code, 403)
            target.side_effect = None
        api.save_source.assert_not_called()

    def test_invalid_json_and_extra_fields(self):
        for raw, expected in [(b'{"cycles":[],"cycles":[]}', 400), (b'{"x":NaN}', 400),
                              (b'{', 400), (json.dumps({**BODY, 'timezone': 'UTC'}).encode(), 422),
                              (b' ' * (api.MAX_BODY_BYTES + 1), 413)]:
            result = self.client.put(self.path, content=raw, headers={**self.headers, 'content-type': 'application/json'})
            self.assertEqual(result.status_code, expected, result.text)
        api.save_source.assert_not_called()

    def test_wrong_content_type(self):
        result = self.client.put(self.path, content=b'{}', headers=self.headers)
        self.assertEqual(result.status_code, 415)

    def test_conflict_and_unknown_save_status_are_safe(self):
        api.save_source.side_effect = StructuredApiError(409, 'Changed', 'SCHEDULE_REVISION_CONFLICT', {'revision': 2})
        self.assertEqual(self.client.put(self.path, json=BODY, headers=self.headers).json()['details'], {'revision': 2})
        api.save_source.side_effect = RuntimeError('credential-or-database-secret')
        result = self.client.put(self.path, json=BODY, headers=self.headers)
        self.assertEqual(result.status_code, 503)
        self.assertIn('same request identifier', result.text)
        self.assertNotIn('credential-or-database-secret', result.text)

    def test_invalid_route_cannot_reach_store(self):
        self.assertEqual(self.client.get('/api/schedule/source?route=not-a-route').status_code, 422)
        api.read_source.assert_not_called()

    def test_registered_with_existing_router_and_usage_attribution(self):
        from order_forecast.api.routers import schedule
        routes = [route for route in schedule.router.routes if route.path == '/schedule/source']
        self.assertEqual({method for route in routes for method in route.methods}, {'GET', 'PUT'})
        self.assertEqual(classify_api_feature('/api/schedule/source'), 'schedule')
        self.assertEqual(extract_route_hint('/api/schedule/source', {'route': '000001'}), '000001')
        self.assertEqual(classify_api_feature('/api/schedule/enroll'), 'schedule')
        self.assertEqual(extract_route_hint('/api/schedule/enroll', {'route': '000001'}), '000001')


if __name__ == '__main__':
    unittest.main()
