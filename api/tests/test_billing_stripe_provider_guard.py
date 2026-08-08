import json
import unittest
from unittest.mock import patch

from starlette.requests import Request

from order_forecast.api.routers import billing


class _FakeSnapshot:
    def __init__(self, data):
        self._data = data
        self.exists = data is not None

    def to_dict(self):
        return self._data


class _FakeDocument:
    def __init__(self, data, key):
        self._data = data
        self._key = key

    def get(self):
        return _FakeSnapshot(self._data.get(self._key))


class _FakeCollection:
    def __init__(self, data):
        self._data = data

    def document(self, key):
        return _FakeDocument(self._data, key)


class _FakeDB:
    def __init__(self, collections):
        self._collections = collections

    def collection(self, name):
        return _FakeCollection(self._collections.setdefault(name, {}))


def _build_request():
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/billing/stripe/checkout",
            "headers": [(b"x-correlation-id", b"test-correlation")],
            "client": ("testclient", 123),
            "server": ("testserver", 80),
            "scheme": "http",
        }
    )


class StripeCheckoutProviderGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_checkout_rejects_active_apple_route_entitlement(self):
        db = _FakeDB(
            {
                "routes": {},
                "users": {
                    "owner-1": {
                        "profile": {
                            "role": "owner",
                            "routeNumber": "961767",
                            "email": "owner@example.com",
                        },
                        "subscriptions": {"routes": {}},
                    }
                },
                "routeEntitlements": {
                    "961767": {
                        "active": True,
                        "provider": "apple",
                    }
                },
            }
        )
        payload = billing.StripeCheckoutSessionRequest(
            routeNumber="961767",
            plan="solo",
            interval="monthly",
        )

        with patch.object(
            billing,
            "require_route_access",
            return_value={
                "profile": {
                    "role": "owner",
                    "routeNumber": "961767",
                    "email": "owner@example.com",
                },
            },
        ), patch.object(
            billing,
            "_stripe_api_request_form",
            side_effect=AssertionError("Stripe API should not be called on provider conflict"),
        ):
            response = await billing.create_stripe_checkout_session(
                request=_build_request(),
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(body["code"], "ENTITLEMENT_PROVIDER_CONFLICT")
        self.assertEqual(body["details"]["existingProvider"], "apple")
        self.assertEqual(body["details"]["incomingProvider"], "stripe")

    async def test_checkout_rejects_active_stripe_route_entitlement_without_legacy_shadow(self):
        db = _FakeDB(
            {
                "routes": {},
                "users": {
                    "owner-1": {
                        "profile": {
                            "role": "owner",
                            "routeNumber": "961767",
                            "email": "owner@example.com",
                        },
                        "subscriptions": {"routes": {}},
                    }
                },
                "routeEntitlements": {
                    "961767": {
                        "active": True,
                        "provider": "stripe",
                    }
                },
            }
        )
        payload = billing.StripeCheckoutSessionRequest(
            routeNumber="961767",
            plan="solo",
            interval="monthly",
        )

        with patch.object(
            billing,
            "require_route_access",
            return_value={
                "profile": {
                    "role": "owner",
                    "routeNumber": "961767",
                    "email": "owner@example.com",
                },
            },
        ), patch.object(
            billing,
            "_stripe_api_request_form",
            side_effect=AssertionError("Stripe API should not be called for active Stripe entitlement"),
        ):
            response = await billing.create_stripe_checkout_session(
                request=_build_request(),
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(body["code"], "STRIPE_SUBSCRIPTION_ALREADY_ACTIVE")


class AppleSandboxEntitlementGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_verify_rejects_sandbox_transaction_before_writing_entitlement(self):
        db = _FakeDB({"routeEntitlements": {}})
        payload = billing.AppleVerifyRequest(
            routeNumber="961767",
            productId="com.keylay.routespark.solo.monthly",
            transactionId="2000000000000001",
        )
        entitlement = billing.BillingEntitlement(
            routeNumber="961767",
            active=True,
            plan="solo",
            provider="apple",
            interval="monthly",
            currentPeriodEndMs=1893456000000,
            source="route_entitlements",
            features=billing._feature_payload_for_plan("solo"),
        )

        with patch.object(billing, "APPLE_BILLING_VERIFICATION_ENABLED", True), patch.object(
            billing,
            "_apple_credentials_configured",
            return_value=True,
        ), patch.object(
            billing,
            "require_route_access",
            return_value={"profile": {"role": "owner", "routeNumber": "961767"}},
        ), patch.object(
            billing,
            "_resolve_apple_transaction",
            return_value={"environment": "Sandbox", "response": {}},
        ), patch.object(
            billing,
            "_build_entitlement_from_apple_transaction",
            return_value={
                "entitlement": entitlement,
                "meta": {
                    "environment": "Sandbox",
                    "appStoreTransactionId": "2000000000000001",
                    "appleOriginalTransactionId": "2000000000000001",
                },
            },
        ), patch.object(
            billing,
            "_resolve_owner_uid_for_billing_write",
            return_value="owner-1",
        ), patch.object(
            billing,
            "_write_route_entitlement_from_apple",
            side_effect=AssertionError("sandbox entitlement should not be written"),
        ), patch.object(
            billing,
            "_write_legacy_subscription_shadow_from_apple",
            side_effect=AssertionError("sandbox legacy shadow should not be written"),
        ):
            response = await billing.verify_apple_subscription(
                request=_build_request(),
                payload=payload,
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        body = json.loads(response.body)
        self.assertEqual(response.status_code, 403)
        self.assertEqual(body["code"], "APPLE_SANDBOX_ENTITLEMENT_NOT_ALLOWED")

    async def test_entitlement_read_treats_sandbox_route_entitlement_as_inactive(self):
        db = _FakeDB(
            {
                "routeEntitlements": {
                    "961767": {
                        "active": True,
                        "provider": "apple",
                        "appleEnvironment": "Sandbox",
                        "plan": "solo",
                        "interval": "monthly",
                        "features": {"scanner": True},
                    }
                },
                "users": {
                    "owner-1": {
                        "profile": {"role": "owner", "routeNumber": "961767"},
                        "subscriptions": {
                            "routes": {
                                "961767": {
                                    "active": True,
                                    "provider": "apple",
                                    "plan": "solo",
                                }
                            }
                        },
                    }
                },
                "routes": {},
                "routeNumbers": {},
            }
        )

        with patch.object(
            billing,
            "require_route_access",
            return_value={"profile": {"role": "owner", "routeNumber": "961767"}},
        ):
            response = await billing.get_billing_entitlement(
                request=_build_request(),
                route="961767",
                decoded_token={"uid": "owner-1"},
                db=db,
            )

        self.assertTrue(response.ok)
        self.assertFalse(response.entitlement.active)
        self.assertEqual(response.entitlement.provider, "apple")
        self.assertEqual(response.entitlement.resolvedFrom, "route_entitlements")

    async def test_entitlement_read_allows_allowlisted_sandbox_route(self):
        db = _FakeDB(
            {
                "routeEntitlements": {
                    "900000": {
                        "active": True,
                        "provider": "apple",
                        "appleEnvironment": "Sandbox",
                        "ownerUid": "apple-review-owner",
                        "plan": "solo",
                        "interval": "monthly",
                        "features": {"managementDashboard": True},
                    }
                },
                "users": {},
                "routes": {},
                "routeNumbers": {},
            }
        )

        with patch.object(billing, "APPLE_SANDBOX_BILLING_ALLOWED_ROUTES", {"900000"}), patch.object(
            billing,
            "require_route_access",
            return_value={"profile": {"role": "owner", "routeNumber": "900000"}},
        ):
            response = await billing.get_billing_entitlement(
                request=_build_request(),
                route="900000",
                decoded_token={"uid": "apple-review-owner"},
                db=db,
            )

        self.assertTrue(response.ok)
        self.assertTrue(response.entitlement.active)
        self.assertTrue(response.entitlement.features["managementDashboard"])


if __name__ == "__main__":
    unittest.main()
