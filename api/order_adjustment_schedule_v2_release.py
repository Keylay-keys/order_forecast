"""Fail-closed release controls for scheduled-order-day stamp writers."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping


STAMP_WRITES_ENV = "ORDER_ADJUSTMENT_SCHEDULE_V2_STAMP_WRITES_ENABLED"
WORKER_COMPATIBLE_ENV = "ORDER_ADJUSTMENT_SCHEDULE_V2_WORKER_COMPATIBLE"
MIN_CLIENT_VERSION_ENV = "ORDER_ADJUSTMENT_SCHEDULE_V2_MIN_CLIENT_VERSION"
REQUIRED_CAPABILITY_ENV = "ORDER_ADJUSTMENT_SCHEDULE_V2_REQUIRED_CAPABILITY"
VERSION_PATTERN = re.compile(r"^[0-9]+(?:\.[0-9]+){0,3}$")
CAPABILITY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def _enabled(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _version_tuple(value: str | None) -> tuple[int, ...] | None:
    candidate = str(value or "").strip()
    if not VERSION_PATTERN.fullmatch(candidate):
        return None
    parts = tuple(int(part) for part in candidate.split("."))
    return parts + (0,) * (4 - len(parts))


@dataclass(frozen=True)
class OrderAdjustmentScheduleV2ReleaseControls:
    stamp_writes_enabled: bool
    worker_v2_compatible: bool
    minimum_client_version: str | None
    required_capability: str | None

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "OrderAdjustmentScheduleV2ReleaseControls":
        env = environment if environment is not None else os.environ
        minimum = str(env.get(MIN_CLIENT_VERSION_ENV) or "").strip() or None
        required_capability = str(env.get(REQUIRED_CAPABILITY_ENV) or "").strip() or None
        return cls(
            stamp_writes_enabled=_enabled(env.get(STAMP_WRITES_ENV)),
            worker_v2_compatible=_enabled(env.get(WORKER_COMPATIBLE_ENV)),
            minimum_client_version=minimum,
            required_capability=required_capability,
        )

    def stamp_write_allowed(
        self,
        client_version: str | None,
        capabilities: set[str] | None = None,
    ) -> bool:
        if not self.stamp_writes_enabled or not self.worker_v2_compatible:
            return False
        minimum = _version_tuple(self.minimum_client_version)
        actual = _version_tuple(client_version)
        required = self.required_capability
        if (
            minimum is None
            or actual is None
            or required is None
            or CAPABILITY_PATTERN.fullmatch(required) is None
            or required not in (capabilities or set())
        ):
            return False
        return actual >= minimum

    def diagnostics(self) -> dict[str, object]:
        return {
            "stampWritesEnabled": self.stamp_writes_enabled,
            "workerV2Compatible": self.worker_v2_compatible,
            "minimumClientVersion": self.minimum_client_version,
            "minimumClientVersionValid": _version_tuple(self.minimum_client_version) is not None,
            "requiredCapability": self.required_capability,
            "requiredCapabilityValid": bool(
                self.required_capability
                and CAPABILITY_PATTERN.fullmatch(self.required_capability)
            ),
        }


def request_client_version(headers: Mapping[str, str] | None) -> str | None:
    if not headers:
        return None
    value = str(headers.get("x-routespark-app-version") or "").strip()
    return value or None


def request_capabilities(headers: Mapping[str, str] | None) -> set[str]:
    if not headers:
        return set()
    raw = str(headers.get("x-routespark-capabilities") or "")
    return {
        token
        for part in raw.split(",")
        if (token := part.strip().lower())
        and CAPABILITY_PATTERN.fullmatch(token)
    }


def stamp_write_allowed_for_headers(
    headers: Mapping[str, str] | None,
    environment: Mapping[str, str] | None = None,
) -> bool:
    controls = OrderAdjustmentScheduleV2ReleaseControls.from_environment(environment)
    return controls.stamp_write_allowed(
        request_client_version(headers),
        request_capabilities(headers),
    )
