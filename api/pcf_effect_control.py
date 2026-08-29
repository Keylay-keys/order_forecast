"""Short-lived, exact admission fences for fixed PCF cluster effects."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID

from .pcf_effect_runtime import EffectClaim


MAX_CONTROL_BYTES = 65_536
MAX_FENCES = 100
MAX_FENCE_HORIZON = timedelta(hours=1)
PROTECTED_REVIEW_ROUTE = "900000"
_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_DELIVERY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTROL_KEYS = frozenset(
    {"schemaVersion", "effectsEnabled", "allProvenWork", "fences"}
)
_FENCE_KEYS = frozenset(
    {
        "workId",
        "routeNumber",
        "deliveryNumber",
        "inputHash",
        "expiresAt",
        "evidenceSha256",
    }
)


class EffectControlError(RuntimeError):
    """The fixed-effect allowlist cannot grant mutation authority."""


class _DuplicateJsonKey(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise EffectControlError("pcf_effect_control_time_invalid")
    return value.astimezone(timezone.utc)


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EffectControlError("pcf_effect_control_invalid")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise EffectControlError("pcf_effect_control_invalid") from exc
    return _utc(parsed)


@dataclass(frozen=True)
class EffectFence:
    work_id: UUID
    route: str
    delivery: str
    input_hash: str
    expires_at: datetime
    evidence_sha256: str

    def admits(self, claim: EffectClaim, *, now: datetime) -> bool:
        checked_at = _utc(now)
        return (
            claim.work_id == self.work_id
            and claim.route == self.route
            and claim.delivery == self.delivery
            and claim.input_hash == self.input_hash
            and checked_at < self.expires_at <= checked_at + MAX_FENCE_HORIZON
        )


@dataclass(frozen=True)
class EffectRuntimeControl:
    effects_enabled: bool
    all_proven_work: bool
    fences: tuple[EffectFence, ...]

    def admitted_work_ids(self, *, now: datetime) -> tuple[UUID, ...]:
        """Return exact live IDs; an invalid broad switch never widens scope."""
        checked_at = _utc(now)
        if not self.effects_enabled or self.all_proven_work:
            return ()
        return tuple(
            sorted(
                (
                    fence.work_id
                    for fence in self.fences
                    if checked_at < fence.expires_at
                    <= checked_at + MAX_FENCE_HORIZON
                ),
                key=str,
            )
        )

    def admits(self, claim: EffectClaim, *, now: datetime) -> bool:
        if not self.effects_enabled or self.all_proven_work:
            return False
        return any(fence.admits(claim, now=now) for fence in self.fences)


def parse_effect_control(payload: Any) -> EffectRuntimeControl:
    if not isinstance(payload, Mapping) or frozenset(payload) != _CONTROL_KEYS:
        raise EffectControlError("pcf_effect_control_invalid")
    if payload.get("schemaVersion") != 1:
        raise EffectControlError("pcf_effect_control_invalid")
    enabled = payload.get("effectsEnabled")
    all_proven = payload.get("allProvenWork")
    raw_fences = payload.get("fences")
    if (
        type(enabled) is not bool
        or type(all_proven) is not bool
        or not isinstance(raw_fences, list)
        or len(raw_fences) > MAX_FENCES
    ):
        raise EffectControlError("pcf_effect_control_invalid")
    # This implementation deliberately has no broad-rollout mode. Adding one
    # requires a later reviewed contract rather than interpreting a boolean as
    # permission to mutate every route.
    if all_proven:
        raise EffectControlError("pcf_effect_control_broad_scope_unsupported")

    fences: list[EffectFence] = []
    identities: set[tuple[UUID, str, str, str]] = set()
    for raw in raw_fences:
        if not isinstance(raw, Mapping) or frozenset(raw) != _FENCE_KEYS:
            raise EffectControlError("pcf_effect_control_invalid")
        try:
            work_id = UUID(str(raw.get("workId")))
        except (TypeError, ValueError) as exc:
            raise EffectControlError("pcf_effect_control_invalid") from exc
        route = raw.get("routeNumber")
        delivery = raw.get("deliveryNumber")
        input_hash = raw.get("inputHash")
        evidence = raw.get("evidenceSha256")
        if (
            not isinstance(route, str)
            or _ROUTE_RE.fullmatch(route) is None
            or route == PROTECTED_REVIEW_ROUTE
            or not isinstance(delivery, str)
            or _DELIVERY_RE.fullmatch(delivery) is None
            or not isinstance(input_hash, str)
            or _SHA256_RE.fullmatch(input_hash) is None
            or not isinstance(evidence, str)
            or _SHA256_RE.fullmatch(evidence) is None
        ):
            raise EffectControlError("pcf_effect_control_invalid")
        identity = (work_id, route, delivery, input_hash)
        if identity in identities:
            raise EffectControlError("pcf_effect_control_duplicate")
        identities.add(identity)
        fences.append(
            EffectFence(
                work_id,
                route,
                delivery,
                input_hash,
                _timestamp(raw.get("expiresAt")),
                evidence,
            )
        )
    if enabled and not fences:
        raise EffectControlError("pcf_effect_control_empty")
    return EffectRuntimeControl(enabled, all_proven, tuple(fences))


def load_effect_control(path: Path) -> EffectRuntimeControl:
    try:
        size = path.stat().st_size
        if not 1 <= size <= MAX_CONTROL_BYTES:
            raise EffectControlError("pcf_effect_control_unavailable")
        raw = path.read_bytes()
        if len(raw) != size:
            raise EffectControlError("pcf_effect_control_unavailable")
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
    except EffectControlError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise EffectControlError("pcf_effect_control_unavailable") from exc
    return parse_effect_control(payload)
