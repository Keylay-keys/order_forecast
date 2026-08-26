"""Fail-closed authentication primitives for RouteSpark internal APIs.

The public web API and internal machine API intentionally have different trust
domains.  Firebase ID tokens authenticate product users; they are never valid
on ``/internal/v1``.  Cloud Tasks uses a pinned Google service-account OIDC
identity only for durable event delivery.  Mac/Dell agents use short-lived
Ed25519 JWTs whose private keys remain outside the cluster.

This module parses no request bodies.  The ASGI middleware calls it from raw
headers before FastAPI or Pydantic can consume a body.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

import jwt
import requests
from cachecontrol import CacheControl
from google.auth import exceptions as google_auth_exceptions
from google.auth.transport import requests as google_auth_requests
from google.oauth2 import id_token as google_id_token


INTERNAL_PREFIX = "/internal/v1/pcf"
MACHINE_ISSUER = "https://machine.routespark.internal/pcf"
MACHINE_AUDIENCE = "https://api.routespark.pro/internal/v1/pcf"
TASK_EVENT_AUDIENCE = "https://api.routespark.pro/internal/v1/pcf/events"
TASK_DISPATCHER_EMAIL = (
    "pcf-task-dispatcher@routespark-1f47d.iam.gserviceaccount.com"
)
TRUST_FILE_ENV = "PCF_MACHINE_TRUST_FILE"
DEFAULT_TRUST_FILE = "/var/run/secrets/routespark/pcf-machine-auth/trust.json"

MAX_BEARER_BYTES = 8192
MAX_TRUST_FILE_BYTES = 65536
MAX_MACHINE_TOKEN_SECONDS = 300
MAX_GOOGLE_TOKEN_SECONDS = 3600
CLOCK_SKEW_SECONDS = 30

_KID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_PRINCIPAL_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_JTI_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")
_WORK_PATH_RE = re.compile(
    rf"^{re.escape(INTERNAL_PREFIX)}/work/"
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/(heartbeat|result)$"
)

ALLOWED_CAPABILITIES = frozenset(
    {
        "pcf.claim",
        "pcf.heartbeat",
        "pcf.result",
        "pcf.backup.verify",
    }
)


@dataclass(frozen=True)
class InternalEndpoint:
    provider: str
    capability: str


@dataclass(frozen=True)
class MachinePrincipal:
    principal: str
    provider: str
    capability: str
    key_id: str


class MachineAuthError(Exception):
    """Typed authentication error safe to return without token material."""

    def __init__(self, status_code: int, code: str):
        super().__init__(code)
        self.status_code = status_code
        self.code = code


def endpoint_contract(method: str, path: str) -> Optional[InternalEndpoint]:
    """Return the exact provider/capability contract for an internal endpoint."""
    normalized_method = method.upper()
    if normalized_method == "POST" and path == f"{INTERNAL_PREFIX}/events":
        return InternalEndpoint("google_cloud_tasks_oidc", "pcf.event.ingest")
    if normalized_method == "POST" and path == f"{INTERNAL_PREFIX}/claims":
        return InternalEndpoint("routespark_machine_jwt", "pcf.claim")
    match = _WORK_PATH_RE.fullmatch(path)
    if match and normalized_method == ("POST" if match.group(1) == "heartbeat" else "PUT"):
        capability = "pcf.heartbeat" if match.group(1) == "heartbeat" else "pcf.result"
        return InternalEndpoint("routespark_machine_jwt", capability)
    if (
        normalized_method == "POST"
        and path == f"{INTERNAL_PREFIX}/backups/offbox-verifications"
    ):
        return InternalEndpoint("routespark_machine_jwt", "pcf.backup.verify")
    return None


def _bounded_string(value: Any, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError("invalid string")
    return value


def _validate_trust_document(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != {"version", "issuer", "audience", "keys"}:
        raise ValueError("invalid trust document shape")
    if raw["version"] != 1:
        raise ValueError("unsupported trust document version")
    if raw["issuer"] != MACHINE_ISSUER or raw["audience"] != MACHINE_AUDIENCE:
        raise ValueError("trust document issuer/audience mismatch")
    keys = raw["keys"]
    if not isinstance(keys, dict) or not 1 <= len(keys) <= 16:
        raise ValueError("invalid trust key count")

    validated: dict[str, Any] = {}
    for kid, record in keys.items():
        if not isinstance(kid, str) or not _KID_RE.fullmatch(kid):
            raise ValueError("invalid trust key id")
        if not isinstance(record, dict) or set(record) != {
            "algorithm",
            "principal",
            "capabilities",
            "publicKeyPem",
        }:
            raise ValueError("invalid trust key record")
        if record["algorithm"] != "EdDSA":
            raise ValueError("unsupported trust key algorithm")
        principal = _bounded_string(record["principal"], maximum=64)
        if not _PRINCIPAL_RE.fullmatch(principal):
            raise ValueError("invalid trust principal")
        capabilities = record["capabilities"]
        if (
            not isinstance(capabilities, list)
            or not capabilities
            or len(capabilities) != len(set(capabilities))
            or not set(capabilities).issubset(ALLOWED_CAPABILITIES)
        ):
            raise ValueError("invalid trust capabilities")
        public_key = _bounded_string(record["publicKeyPem"], maximum=4096)
        if "PRIVATE KEY" in public_key or "BEGIN PUBLIC KEY" not in public_key:
            raise ValueError("trust document must contain public keys only")
        validated[kid] = {
            "algorithm": "EdDSA",
            "principal": principal,
            "capabilities": frozenset(capabilities),
            "publicKeyPem": public_key,
        }

    return {
        "version": 1,
        "issuer": MACHINE_ISSUER,
        "audience": MACHINE_AUDIENCE,
        "keys": validated,
    }


@lru_cache(maxsize=16)
def _load_machine_trust_versioned(
    path: str,
    modified_ns: int,
    size: int,
) -> dict[str, Any]:
    del modified_ns
    if size <= 0 or size > MAX_TRUST_FILE_BYTES:
        raise ValueError("invalid trust document size")
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return _validate_trust_document(raw)


def load_machine_trust(path: Optional[str] = None) -> dict[str, Any]:
    """Load public trust and observe atomic Kubernetes Secret rotations."""
    trust_path = Path(path or os.environ.get(TRUST_FILE_ENV, DEFAULT_TRUST_FILE))
    try:
        stat = trust_path.stat()
        return _load_machine_trust_versioned(
            str(trust_path),
            stat.st_mtime_ns,
            stat.st_size,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise MachineAuthError(503, "machine_auth_unavailable") from exc


def _extract_bearer(authorization: Optional[str]) -> str:
    if not authorization or len(authorization.encode("utf-8")) > MAX_BEARER_BYTES:
        raise MachineAuthError(401, "machine_auth_invalid")
    parts = authorization.split(" ")
    if len(parts) != 2 or parts[0] != "Bearer" or not parts[1]:
        raise MachineAuthError(401, "machine_auth_invalid")
    if any(char.isspace() for char in parts[1]):
        raise MachineAuthError(401, "machine_auth_invalid")
    return parts[1]


def verify_machine_jwt(
    token: str,
    required_capability: str,
    *,
    trust: Optional[dict[str, Any]] = None,
    now: Optional[int] = None,
) -> MachinePrincipal:
    """Verify one short-lived RouteSpark JWT and server-side capability map."""
    if required_capability not in ALLOWED_CAPABILITIES:
        raise MachineAuthError(403, "capability_forbidden")
    trust_document = trust or load_machine_trust()
    try:
        header = jwt.get_unverified_header(token)
        if set(header) - {"alg", "kid", "typ"}:
            raise ValueError("unexpected JWT header")
        if header.get("alg") != "EdDSA" or header.get("typ") not in (None, "JWT"):
            raise ValueError("invalid JWT algorithm/type")
        kid = header.get("kid")
        if not isinstance(kid, str) or not _KID_RE.fullmatch(kid):
            raise ValueError("invalid key id")
        key = trust_document["keys"].get(kid)
        if key is None:
            raise ValueError("unknown key id")
        claims = jwt.decode(
            token,
            key["publicKeyPem"],
            algorithms=["EdDSA"],
            audience=MACHINE_AUDIENCE,
            issuer=MACHINE_ISSUER,
            leeway=CLOCK_SKEW_SECONDS,
            options={"require": ["iss", "aud", "sub", "iat", "nbf", "exp", "jti"]},
        )
        if set(claims) != {"iss", "aud", "sub", "iat", "nbf", "exp", "jti"}:
            raise ValueError("unexpected JWT claims")
        principal = claims["sub"]
        if principal != key["principal"]:
            raise ValueError("principal/key mismatch")
        issued_at = int(claims["iat"])
        not_before = int(claims["nbf"])
        expires_at = int(claims["exp"])
        current = int(time.time()) if now is None else int(now)
        if not_before != issued_at:
            raise ValueError("nbf must equal iat")
        if expires_at <= issued_at or expires_at - issued_at > MAX_MACHINE_TOKEN_SECONDS:
            raise ValueError("invalid token lifetime")
        if issued_at > current + CLOCK_SKEW_SECONDS:
            raise ValueError("token issued in future")
        if current - issued_at > MAX_MACHINE_TOKEN_SECONDS + CLOCK_SKEW_SECONDS:
            raise ValueError("token too old")
        if not isinstance(claims["jti"], str) or not _JTI_RE.fullmatch(claims["jti"]):
            raise ValueError("invalid jti")
        if required_capability not in key["capabilities"]:
            raise MachineAuthError(403, "capability_forbidden")
        return MachinePrincipal(principal, "routespark_machine_jwt", required_capability, kid)
    except MachineAuthError:
        raise
    except Exception as exc:
        raise MachineAuthError(401, "machine_auth_invalid") from exc


GoogleVerifier = Callable[[str, str], Mapping[str, Any]]


_google_request_lock = threading.Lock()
_google_request: Optional[google_auth_requests.Request] = None


def _cached_google_request() -> google_auth_requests.Request:
    """Cache Google's signing-certificate response per its HTTP cache headers."""
    global _google_request
    if _google_request is None:
        with _google_request_lock:
            if _google_request is None:
                _google_request = google_auth_requests.Request(
                    session=CacheControl(requests.Session())
                )
    return _google_request


def _default_google_verifier(token: str, audience: str) -> Mapping[str, Any]:
    return google_id_token.verify_oauth2_token(
        token,
        _cached_google_request(),
        audience=audience,
    )


def _prevalidate_google_token_shape(token: str) -> None:
    """Reject a wrong issuer/algorithm without fetching Google certificates."""
    header = jwt.get_unverified_header(token)
    if set(header) - {"alg", "kid", "typ"}:
        raise ValueError("unexpected Google JWT header")
    if header.get("alg") != "RS256" or header.get("typ") not in (None, "JWT"):
        raise ValueError("invalid Google JWT algorithm/type")
    kid = header.get("kid")
    if not isinstance(kid, str) or not 1 <= len(kid) <= 128:
        raise ValueError("invalid Google key id")
    claims = jwt.decode(
        token,
        options={
            "verify_signature": False,
            "verify_aud": False,
            "verify_exp": False,
            "verify_iat": False,
            "verify_nbf": False,
        },
    )
    if claims.get("iss") not in {"accounts.google.com", "https://accounts.google.com"}:
        raise ValueError("invalid Google issuer")
    if claims.get("aud") != TASK_EVENT_AUDIENCE:
        raise ValueError("invalid Google audience")
    if claims.get("email") != TASK_DISPATCHER_EMAIL or claims.get("email_verified") is not True:
        raise ValueError("invalid dispatcher principal")


def verify_cloud_tasks_oidc(
    token: str,
    *,
    verifier: Optional[GoogleVerifier] = None,
    now: Optional[int] = None,
) -> MachinePrincipal:
    """Verify Cloud Tasks' Google-signed dispatcher identity for event ingress."""
    try:
        _prevalidate_google_token_shape(token)
        claims = (verifier or _default_google_verifier)(token, TASK_EVENT_AUDIENCE)
        if claims.get("iss") not in {"accounts.google.com", "https://accounts.google.com"}:
            raise ValueError("invalid Google issuer")
        if claims.get("aud") != TASK_EVENT_AUDIENCE:
            raise ValueError("invalid Google audience")
        if claims.get("email") != TASK_DISPATCHER_EMAIL or claims.get("email_verified") is not True:
            raise ValueError("invalid dispatcher principal")
        if not isinstance(claims.get("sub"), str) or not claims["sub"]:
            raise ValueError("missing dispatcher subject")
        issued_at = int(claims["iat"])
        expires_at = int(claims["exp"])
        current = int(time.time()) if now is None else int(now)
        if expires_at <= issued_at or expires_at - issued_at > MAX_GOOGLE_TOKEN_SECONDS:
            raise ValueError("invalid Google token lifetime")
        if issued_at > current + CLOCK_SKEW_SECONDS:
            raise ValueError("Google token issued in future")
        if current - issued_at > MAX_GOOGLE_TOKEN_SECONDS + CLOCK_SKEW_SECONDS:
            raise ValueError("Google token too old")
        return MachinePrincipal(
            TASK_DISPATCHER_EMAIL,
            "google_cloud_tasks_oidc",
            "pcf.event.ingest",
            claims["sub"],
        )
    except google_auth_exceptions.TransportError as exc:
        raise MachineAuthError(503, "machine_auth_unavailable") from exc
    except MachineAuthError:
        raise
    except Exception as exc:
        raise MachineAuthError(401, "machine_auth_invalid") from exc


def authenticate_internal_headers(
    method: str,
    path: str,
    headers: Mapping[str, str],
    *,
    trust: Optional[dict[str, Any]] = None,
    google_verifier: Optional[GoogleVerifier] = None,
    now: Optional[int] = None,
) -> MachinePrincipal:
    """Authenticate raw headers for an exact internal endpoint contract."""
    endpoint = endpoint_contract(method, path)
    if endpoint is None:
        raise MachineAuthError(404, "internal_endpoint_not_found")
    lowered = {key.lower(): value for key, value in headers.items()}
    if "cookie" in lowered:
        raise MachineAuthError(401, "machine_auth_invalid")
    token = _extract_bearer(lowered.get("authorization"))
    if endpoint.provider == "google_cloud_tasks_oidc":
        return verify_cloud_tasks_oidc(token, verifier=google_verifier, now=now)
    return verify_machine_jwt(
        token,
        endpoint.capability,
        trust=trust,
        now=now,
    )
