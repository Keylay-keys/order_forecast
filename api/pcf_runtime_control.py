"""Strict, fail-closed consumer for the atomic PCF runtime-control document."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping


CONTROL_DOCUMENT_VERSION = "pcf-runtime-control-document-v1"
CONTROL_PATH_ENV = "PCF_RUNTIME_CONTROL_PATH"
DEFAULT_CONTROL_PATH = Path("/app/config/pcf-runtime-control.json")
MAX_CONTROL_BYTES = 16_384
BOOTSTRAP_REVISION = "0" * 64
EVIDENCE_ROOT = "/Volumes/Extreme SSD/routespark_offload/pcf-migration/firestore-read-redesign/"
STAGES = ("off", "ingress", "shadow", "claims", "projection")
DISCOVERY_MODES = frozenset(("legacy", "bounded", "disabled"))
WRITE_AUTHORITIES = frozenset(("legacy_mac", "cluster"))
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$")
_DOCUMENT_KEYS = frozenset(
    (
        "contractVersion",
        "revision",
        "previousRevision",
        "updatedAt",
        "approvedBy",
        "goDecisionEvidence",
        "controls",
    )
)
_CONTROL_KEYS = frozenset(
    (
        "PCF_FIRESTORE_DISCOVERY_MODE",
        "PCF_CLUSTER_STAGE",
        "PCF_FIREBASE_WRITE_AUTHORITY",
    )
)


class RuntimeControlError(RuntimeError):
    """The mounted control cannot grant any PCF runtime authority."""


class _DuplicateJsonKey(ValueError):
    pass


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _DuplicateJsonKey(key)
        value[key] = item
    return value


def _strict_string(value: Any, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise RuntimeControlError("pcf_runtime_control_invalid")
    return value


def _parse_utc(value: Any) -> datetime:
    text = _strict_string(value, maximum=40)
    if _UTC_RE.fullmatch(text) is None:
        raise RuntimeControlError("pcf_runtime_control_invalid")
    try:
        parsed = datetime.fromisoformat(text.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise RuntimeControlError("pcf_runtime_control_invalid") from exc
    return parsed.astimezone(timezone.utc)


def canonical_control_bytes(payload: Mapping[str, Any]) -> bytes:
    if not isinstance(payload, Mapping):
        raise RuntimeControlError("pcf_runtime_control_invalid")
    canonical = {key: payload[key] for key in payload if key != "revision"}
    return json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def control_revision(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_control_bytes(payload)).hexdigest()


@dataclass(frozen=True)
class PcfRuntimeControl:
    revision: str
    previous_revision: str
    updated_at: datetime
    approved_by: str
    go_decision_evidence: str
    discovery_mode: str
    cluster_stage: str
    firebase_write_authority: str

    @property
    def stage_index(self) -> int:
        return STAGES.index(self.cluster_stage)

    def allows_stage(self, minimum: str) -> bool:
        if minimum not in STAGES:
            raise ValueError("unknown PCF runtime stage")
        return self.stage_index >= STAGES.index(minimum)

    @property
    def allows_ttl_repair(self) -> bool:
        return (
            self.cluster_stage == "projection"
            and self.firebase_write_authority == "cluster"
            and self.discovery_mode == "disabled"
        )

    @property
    def allows_effects(self) -> bool:
        """Grant the fixed effect worker only at the final write-authority state."""
        return self.allows_ttl_repair


def parse_runtime_control(payload: Any) -> PcfRuntimeControl:
    if not isinstance(payload, Mapping) or frozenset(payload) != _DOCUMENT_KEYS:
        raise RuntimeControlError("pcf_runtime_control_invalid")
    if payload.get("contractVersion") != CONTROL_DOCUMENT_VERSION:
        raise RuntimeControlError("pcf_runtime_control_invalid")
    revision = payload.get("revision")
    previous_revision = payload.get("previousRevision")
    if (
        not isinstance(revision, str)
        or _SHA256_RE.fullmatch(revision) is None
        or not isinstance(previous_revision, str)
        or _SHA256_RE.fullmatch(previous_revision) is None
        or revision != control_revision(payload)
    ):
        raise RuntimeControlError("pcf_runtime_control_invalid")
    approved_by = _strict_string(payload.get("approvedBy"), maximum=128)
    evidence = _strict_string(payload.get("goDecisionEvidence"), maximum=2048)
    evidence_path = PurePosixPath(evidence)
    if (
        not evidence_path.is_absolute()
        or ".." in evidence_path.parts
        or not evidence.startswith(EVIDENCE_ROOT)
        or evidence_path.suffix != ".json"
    ):
        raise RuntimeControlError("pcf_runtime_control_invalid")
    controls = payload.get("controls")
    if not isinstance(controls, Mapping) or frozenset(controls) != _CONTROL_KEYS:
        raise RuntimeControlError("pcf_runtime_control_invalid")
    discovery = controls.get("PCF_FIRESTORE_DISCOVERY_MODE")
    stage = controls.get("PCF_CLUSTER_STAGE")
    authority = controls.get("PCF_FIREBASE_WRITE_AUTHORITY")
    if discovery not in DISCOVERY_MODES or stage not in STAGES or authority not in WRITE_AUTHORITIES:
        raise RuntimeControlError("pcf_runtime_control_invalid")
    if stage == "projection" and (authority != "cluster" or discovery != "disabled"):
        raise RuntimeControlError("pcf_runtime_control_invalid")
    return PcfRuntimeControl(
        revision=revision,
        previous_revision=previous_revision,
        updated_at=_parse_utc(payload.get("updatedAt")),
        approved_by=approved_by,
        go_decision_evidence=evidence,
        discovery_mode=discovery,
        cluster_stage=stage,
        firebase_write_authority=authority,
    )


def load_runtime_control(path: Path | None = None) -> PcfRuntimeControl:
    configured = path
    if configured is None:
        raw_path = os.environ.get(CONTROL_PATH_ENV, "").strip()
        configured = Path(raw_path) if raw_path else DEFAULT_CONTROL_PATH
    try:
        size = configured.stat().st_size
        if not 1 <= size <= MAX_CONTROL_BYTES:
            raise RuntimeControlError("pcf_runtime_control_unavailable")
        raw = configured.read_bytes()
        if len(raw) != size:
            raise RuntimeControlError("pcf_runtime_control_unavailable")
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_strict_object)
    except RuntimeControlError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, _DuplicateJsonKey) as exc:
        raise RuntimeControlError("pcf_runtime_control_unavailable") from exc
    return parse_runtime_control(payload)
