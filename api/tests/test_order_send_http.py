"""Isolated HTTP contract. No credentials, network services, or database writes."""
import logging
import os
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from order_forecast.api.errors import StructuredApiError, install_api_error_handlers
from order_forecast.api.routers import order_send as api


class SendHttpTests(unittest.TestCase):
    def setUp(self):
        app = FastAPI()
        app.include_router(api.router, prefix='/api')
        install_api_error_handlers(app, debug_mode=False, app_logger=logging.getLogger('test.send'))
        self.app = app
        app.dependency_overrides[api.verify_firebase_token] = lambda: {'uid': 'owner'}
        app.dependency_overrides[api.get_firestore] = lambda: object()
        self.id = 'order-989262-send-' + 'a' * 64
        self.body = {'routeNumber': '989262', 'deliveryDate': '2026-09-14', 'scheduleKey': 'tuesday',
                     'expectedScheduleRevision': 1, 'requestId': 'http-send-order-request-0001'}
        self.headers = {'x-routespark-capabilities': api.CAPABILITY}
        self.url = '/api/orders/send?route=989262'
        self.read_url = f'/api/orders/{self.id}/send-evidence?route=989262'
        patches = [patch.dict(os.environ, {api.ENABLED_ENV: 'true', api.ROUTES_ENV: '989262'}),
                   patch.object(api.limiter, 'enabled', False),
                   patch.object(api, 'require_route_feature_access', new=AsyncMock()),
                   patch.object(api, 'create_send_order', return_value={
                       'order': {'id': self.id}, 'sendEvidence': {'test': True}, 'replayed': False}),
                   patch.object(api, 'read_saved', return_value={'test': True})]
        self.mocks = [p.start() for p in patches]
        for p in patches: self.addCleanup(p.stop)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def post(self, **kwargs):
        return self.client.post(self.url, headers=self.headers, **kwargs)

    def test_capability_required_for_create_and_read(self):
        self.assertEqual(self.client.post(self.url, json=self.body).status_code, 409)
        self.assertEqual(self.client.get(self.read_url).status_code, 409)
        self.mocks[3].assert_not_called(); self.mocks[4].assert_not_called()

    def test_identity_and_route_guard_precede_body_parsing(self):
        def denied(): raise StructuredApiError(401, 'Sign in', 'UNAUTHORIZED')
        self.app.dependency_overrides[api.verify_firebase_token] = denied
        self.assertEqual(self.post(content='not-json').status_code, 401)
        self.app.dependency_overrides[api.verify_firebase_token] = lambda: {'uid': 'owner'}
        self.mocks[2].side_effect = StructuredApiError(403, 'Denied', 'ROUTE_ACCESS_DENIED')
        self.assertEqual(self.post(content='not-json').status_code, 403)
        self.assertEqual(self.client.get(self.read_url, headers=self.headers).status_code, 403)
        self.mocks[3].assert_not_called(); self.mocks[4].assert_not_called()

    def test_write_gate_is_exact_and_does_not_disable_saved_reads(self):
        for flag, routes in [('false', '989262'), ('TRUE', '989262'), ('true', '*'),
                             ('true', ' 989262'), ('true', '989262,'), ('true', '111111'),
                             ('true', ','.join(['989262'] * 101))]:
            with self.subTest(flag=flag, routes=routes), patch.dict(os.environ, {api.ENABLED_ENV: flag, api.ROUTES_ENV: routes}):
                self.assertEqual(self.post(json=self.body).status_code, 409)
                self.assertEqual(self.client.get(self.read_url, headers=self.headers).status_code, 200)
        self.mocks[3].assert_not_called()

    def test_scope_mismatch_and_unknown_fields_never_reach_writer(self):
        for body, code in [({**self.body, 'routeNumber': '111111'}, 403),
                           ({**self.body, 'sendDate': '2099-01-01'}, 422),
                           ({**self.body, 'scheduledOrder': {}}, 422), ([], 422)]:
            self.assertEqual(self.post(json=body).status_code, code)
        self.mocks[3].assert_not_called()

    def test_full_audience_still_requires_ordering_entitlement_and_capability(self):
        with patch.dict(os.environ, {'ORDER_SEND_ROLLOUT_MODE': 'all_authorized', api.ROUTES_ENV: ''}):
            self.assertEqual(self.post(json=self.body).status_code, 200)
            self.mocks[3].reset_mock()
            self.assertEqual(self.client.post(self.url, json=self.body).status_code, 409)
            self.mocks[2].side_effect = StructuredApiError(403, 'Denied', 'ROUTE_ACCESS_DENIED')
            self.assertEqual(self.post(content='not-json').status_code, 403)
            self.mocks[3].assert_not_called()

    def test_bounded_json_rejects_wrong_type_duplicate_keys_and_nonfinite_numbers(self):
        self.assertEqual(self.post(content='{}').status_code, 415)
        for raw, status in [('{' + ' ' * 17000 + '}', 413),
                            ('{"routeNumber":"989262","routeNumber":"111111"}', 400),
                            ('{"expectedScheduleRevision":NaN}', 400)]:
            response = self.client.post(self.url, content=raw, headers={**self.headers, 'content-type': 'application/json'})
            self.assertEqual(response.status_code, status, response.text)
        self.mocks[3].assert_not_called()

    def test_errors_are_sanitized_and_success_is_private(self):
        response = self.post(json=self.body)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers['cache-control'], 'private, no-store')
        self.mocks[3].side_effect = RuntimeError('private database contents')
        response = self.post(json=self.body)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('private database contents', response.text)
        self.mocks[4].side_effect = ValueError('private evidence')
        response = self.client.get(self.read_url, headers=self.headers)
        self.assertEqual(response.status_code, 503)
        self.assertNotIn('private evidence', response.text)

    def test_existing_order_activity_classification_and_query_route_attribution(self):
        from order_forecast.api.usage_analytics import classify_api_feature, extract_route_hint
        self.assertEqual(classify_api_feature('/api/orders/send'), 'orders')
        self.assertEqual(classify_api_feature(f'/api/orders/{self.id}/send-evidence'), 'orders')
        # The route is deliberately in the query, not only in the JSON body.
        self.assertEqual(extract_route_hint('/api/orders/send', {'route': '989262'}), '989262')

    def test_window_preflight_requires_auth_capability_and_is_readable_with_creation_off(self):
        url = f'/api/orders/{self.id}/adjustment-window?route=989262'
        with patch.object(api, 'read_saved_window', return_value={'window': {'status': 'closed'}}) as read:
            self.assertEqual(self.client.get(url).status_code, 409)
            read.assert_not_called()
            with patch.dict(os.environ, {api.ENABLED_ENV: 'false'}):
                response = self.client.get(url, headers=self.headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()['window']['status'], 'closed')
                self.assertEqual(response.headers['cache-control'], 'private, no-store')
            self.mocks[2].side_effect = StructuredApiError(403, 'Denied', 'ROUTE_ACCESS_DENIED')
            self.assertEqual(self.client.get(url, headers=self.headers).status_code, 403)
            read.assert_called_once()


if __name__ == '__main__': unittest.main()
