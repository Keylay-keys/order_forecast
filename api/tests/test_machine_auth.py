import json
import os
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, Mock, patch

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from google.auth import exceptions as google_auth_exceptions

from order_forecast.api.machine_auth import (
    MACHINE_AUDIENCE,
    MACHINE_ISSUER,
    TASK_DISPATCHER_EMAIL,
    TASK_EVENT_AUDIENCE,
    MachineAuthError,
    authenticate_internal_headers,
    load_machine_trust,
    verify_machine_jwt,
)
from order_forecast.api.middleware.internal_machine_auth import (
    InternalMachineAuthMiddleware,
)


NOW = int(time.time())
KID = "pcf-mac-ocr-2026q3-01"
PRINCIPAL = "pcf-macos-vision-01"


def _key_material():
    private_key = Ed25519PrivateKey.generate()
    public_pem = private_key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    return private_key, public_pem


def _trust(public_pem, *, capabilities=None):
    return {
        "version": 1,
        "issuer": MACHINE_ISSUER,
        "audience": MACHINE_AUDIENCE,
        "keys": {
            KID: {
                "algorithm": "EdDSA",
                "principal": PRINCIPAL,
                "capabilities": frozenset(capabilities or {"pcf.claim", "pcf.heartbeat", "pcf.result"}),
                "publicKeyPem": public_pem,
            }
        },
    }


def _token(private_key, **overrides):
    claims = {
        "iss": MACHINE_ISSUER,
        "aud": MACHINE_AUDIENCE,
        "sub": PRINCIPAL,
        "iat": NOW,
        "nbf": NOW,
        "exp": NOW + 300,
        "jti": "abcdefghijklmnop",
    }
    claims.update(overrides)
    return jwt.encode(claims, private_key, algorithm="EdDSA", headers={"kid": KID, "typ": "JWT"})


def _google_token(**overrides):
    claims = {
        "iss": "https://accounts.google.com",
        "aud": TASK_EVENT_AUDIENCE,
        "email": TASK_DISPATCHER_EMAIL,
        "email_verified": True,
        "sub": "123456789012345678901",
        "iat": NOW,
        "exp": NOW + 3600,
    }
    claims.update(overrides)
    private_key = generate_private_key(public_exponent=65537, key_size=2048)
    return jwt.encode(
        claims,
        private_key,
        algorithm="RS256",
        headers={"kid": "google-test-key", "typ": "JWT"},
    )


class MachineJwtTests(unittest.TestCase):
    def setUp(self):
        self.private_key, self.public_pem = _key_material()
        self.trust = _trust(self.public_pem)

    def test_valid_machine_token_uses_server_side_capability(self):
        principal = verify_machine_jwt(
            _token(self.private_key), "pcf.claim", trust=self.trust, now=NOW
        )
        self.assertEqual(principal.principal, PRINCIPAL)
        self.assertEqual(principal.key_id, KID)
        self.assertEqual(principal.capability, "pcf.claim")

    def test_valid_key_cannot_claim_ungranted_backup_capability(self):
        with self.assertRaises(MachineAuthError) as raised:
            verify_machine_jwt(
                _token(self.private_key), "pcf.backup.verify", trust=self.trust, now=NOW
            )
        self.assertEqual((raised.exception.status_code, raised.exception.code), (403, "capability_forbidden"))

    def test_token_lifetime_over_five_minutes_is_rejected(self):
        with self.assertRaises(MachineAuthError) as raised:
            verify_machine_jwt(
                _token(self.private_key, exp=NOW + 301),
                "pcf.claim",
                trust=self.trust,
                now=NOW,
            )
        self.assertEqual(raised.exception.code, "machine_auth_invalid")

    def test_principal_must_match_key_record(self):
        with self.assertRaises(MachineAuthError):
            verify_machine_jwt(
                _token(self.private_key, sub="pcf-dell-backup-01"),
                "pcf.claim",
                trust=self.trust,
                now=NOW,
            )

    def test_unexpected_claim_is_rejected(self):
        with self.assertRaises(MachineAuthError):
            verify_machine_jwt(
                _token(self.private_key, capability="pcf.backup.verify"),
                "pcf.claim",
                trust=self.trust,
                now=NOW,
            )

    def test_secret_projection_rotation_reloads_changed_public_trust(self):
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trust.json"
            document = {
                "version": 1,
                "issuer": MACHINE_ISSUER,
                "audience": MACHINE_AUDIENCE,
                "keys": {
                    KID: {
                        "algorithm": "EdDSA",
                        "principal": PRINCIPAL,
                        "capabilities": ["pcf.claim"],
                        "publicKeyPem": self.public_pem,
                    }
                },
            }
            path.write_text(json.dumps(document), encoding="utf-8")
            os.utime(path, ns=(NOW * 1_000_000_000, NOW * 1_000_000_000))
            first = load_machine_trust(str(path))

            document["keys"][KID]["principal"] = "pcf-macos-vision-02"
            path.write_text(json.dumps(document), encoding="utf-8")
            os.utime(path, ns=((NOW + 1) * 1_000_000_000, (NOW + 1) * 1_000_000_000))
            second = load_machine_trust(str(path))

            self.assertEqual(first["keys"][KID]["principal"], PRINCIPAL)
            self.assertEqual(second["keys"][KID]["principal"], "pcf-macos-vision-02")

    def test_trust_document_rejects_private_key_material(self):
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "trust.json"
            private_pem = self.private_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ).decode("ascii")
            path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "issuer": MACHINE_ISSUER,
                        "audience": MACHINE_AUDIENCE,
                        "keys": {
                            KID: {
                                "algorithm": "EdDSA",
                                "principal": PRINCIPAL,
                                "capabilities": ["pcf.claim"],
                                "publicKeyPem": private_pem,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaises(MachineAuthError) as raised:
                load_machine_trust(str(path))
            self.assertEqual(
                (raised.exception.status_code, raised.exception.code),
                (503, "machine_auth_unavailable"),
            )


class RequestContractTests(unittest.TestCase):
    def setUp(self):
        self.private_key, self.public_pem = _key_material()
        self.trust = _trust(self.public_pem)

    def test_cookie_is_rejected_before_any_google_verification(self):
        verifier = Mock()
        with self.assertRaises(MachineAuthError) as raised:
            authenticate_internal_headers(
                "POST",
                "/internal/v1/pcf/events",
                {"Authorization": f"Bearer {_google_token()}", "Cookie": "session=browser"},
                google_verifier=verifier,
                now=NOW,
            )
        self.assertEqual(raised.exception.code, "machine_auth_invalid")
        verifier.assert_not_called()

    def test_machine_token_is_accepted_only_on_machine_endpoint(self):
        token = _token(self.private_key)
        principal = authenticate_internal_headers(
            "POST",
            "/internal/v1/pcf/claims",
            {"Authorization": f"Bearer {token}"},
            trust=self.trust,
            now=NOW,
        )
        self.assertEqual(principal.provider, "routespark_machine_jwt")

    def test_cloud_tasks_identity_is_pinned_to_event_endpoint(self):
        claims = {
            "iss": "https://accounts.google.com",
            "aud": TASK_EVENT_AUDIENCE,
            "email": TASK_DISPATCHER_EMAIL,
            "email_verified": True,
            "sub": "123456789012345678901",
            "iat": NOW,
            "exp": NOW + 3600,
        }
        verifier = Mock(return_value=claims)
        principal = authenticate_internal_headers(
            "POST",
            "/internal/v1/pcf/events",
            {"Authorization": f"Bearer {_google_token()}"},
            google_verifier=verifier,
            now=NOW,
        )
        self.assertEqual(verifier.call_count, 1)
        self.assertEqual(verifier.call_args.args[1], TASK_EVENT_AUDIENCE)
        self.assertEqual(principal.principal, TASK_DISPATCHER_EMAIL)
        self.assertEqual(principal.capability, "pcf.event.ingest")

    def test_wrong_cloud_tasks_service_account_is_rejected(self):
        verifier = Mock(
            return_value={
                "iss": "https://accounts.google.com",
                "aud": TASK_EVENT_AUDIENCE,
                "email": "other@routespark-1f47d.iam.gserviceaccount.com",
                "email_verified": True,
                "sub": "123456789012345678901",
                "iat": NOW,
                "exp": NOW + 3600,
            }
        )
        with self.assertRaises(MachineAuthError) as raised:
            authenticate_internal_headers(
                "POST",
                "/internal/v1/pcf/events",
                {
                    "Authorization": "Bearer "
                    + _google_token(
                        email="other@routespark-1f47d.iam.gserviceaccount.com"
                    )
                },
                google_verifier=verifier,
                now=NOW,
            )
        self.assertEqual(raised.exception.code, "machine_auth_invalid")

    def test_google_certificate_dependency_failure_is_transient(self):
        verifier = Mock(side_effect=google_auth_exceptions.TransportError("offline"))
        with self.assertRaises(MachineAuthError) as raised:
            authenticate_internal_headers(
                "POST",
                "/internal/v1/pcf/events",
                {"Authorization": f"Bearer {_google_token()}"},
                google_verifier=verifier,
                now=NOW,
            )
        self.assertEqual((raised.exception.status_code, raised.exception.code), (503, "machine_auth_unavailable"))

    def test_firebase_issuer_is_rejected_before_google_certificate_verification(self):
        verifier = Mock()
        token = _google_token(
            iss="https://securetoken.google.com/routespark-1f47d",
            aud="routespark-1f47d",
            email="user@example.com",
        )
        with self.assertRaises(MachineAuthError) as raised:
            authenticate_internal_headers(
                "POST",
                "/internal/v1/pcf/events",
                {"Authorization": f"Bearer {token}"},
                google_verifier=verifier,
                now=NOW,
            )
        self.assertEqual(raised.exception.code, "machine_auth_invalid")
        verifier.assert_not_called()

    def test_unknown_internal_path_fails_closed(self):
        with self.assertRaises(MachineAuthError) as raised:
            authenticate_internal_headers(
                "POST",
                "/internal/v1/pcf/not-real",
                {"Authorization": "Bearer token"},
            )
        self.assertEqual((raised.exception.status_code, raised.exception.code), (404, "internal_endpoint_not_found"))


class MiddlewareOrderingTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _capture(messages):
        async def send(message):
            messages.append(message)

        return send

    async def test_rejected_request_never_reads_body_or_calls_downstream(self):
        downstream = AsyncMock()
        middleware = InternalMachineAuthMiddleware(downstream, enabled=True)
        receive = AsyncMock(side_effect=AssertionError("body must not be read"))
        messages = []

        await middleware(
            {
                "type": "http",
                "method": "POST",
                "path": "/internal/v1/pcf/claims",
                "headers": [],
            },
            receive,
            self._capture(messages),
        )

        receive.assert_not_called()
        downstream.assert_not_called()
        self.assertEqual(messages[0]["status"], 401)
        self.assertNotIn(b"access-control-allow-origin", dict(messages[0]["headers"]))

    async def test_options_preflight_is_not_cors_enabled(self):
        downstream = AsyncMock()
        middleware = InternalMachineAuthMiddleware(downstream, enabled=True)
        messages = []
        await middleware(
            {
                "type": "http",
                "method": "OPTIONS",
                "path": "/internal/v1/pcf/events",
                "headers": [(b"origin", b"https://routespark.pro")],
            },
            AsyncMock(),
            self._capture(messages),
        )
        downstream.assert_not_called()
        self.assertEqual(messages[0]["status"], 404)
        self.assertNotIn(b"access-control-allow-origin", dict(messages[0]["headers"]))

    async def test_success_sets_principal_before_downstream_body_handling(self):
        downstream = AsyncMock()
        middleware = InternalMachineAuthMiddleware(downstream, enabled=True)
        principal = object()
        scope = {
            "type": "http",
            "method": "POST",
            "path": "/internal/v1/pcf/claims",
            "headers": [(b"authorization", b"Bearer test")],
        }
        with patch(
            "order_forecast.api.middleware.internal_machine_auth.authenticate_internal_headers",
            return_value=principal,
        ):
            await middleware(scope, AsyncMock(), AsyncMock())
        self.assertIs(scope["state"]["machine_principal"], principal)
        downstream.assert_awaited_once()

    async def test_default_disabled_gate_rejects_before_auth_or_body(self):
        downstream = AsyncMock()
        middleware = InternalMachineAuthMiddleware(downstream, enabled=False)
        receive = AsyncMock(side_effect=AssertionError("body must not be read"))
        messages = []
        with patch(
            "order_forecast.api.middleware.internal_machine_auth.authenticate_internal_headers"
        ) as authenticate:
            await middleware(
                {
                    "type": "http",
                    "method": "POST",
                    "path": "/internal/v1/pcf/events",
                    "headers": [(b"authorization", b"Bearer ignored")],
                },
                receive,
                self._capture(messages),
            )
        authenticate.assert_not_called()
        receive.assert_not_called()
        downstream.assert_not_called()
        self.assertEqual(messages[0]["status"], 404)


if __name__ == "__main__":
    unittest.main()
