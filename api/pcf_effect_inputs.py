"""Strict immutable inputs and external-evidence proof for PCF effects."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .pcf_effect_runtime import EffectClaim
from .pcf_event_contract import validate_pcf_ocr_context
from .pcf_worker_contract import (
    EvidencePackage,
    canonical_result_identity,
    manifest_pages,
    validate_evidence_package,
    validate_proposal,
)
from .pcf_work_runtime import EXPECTED_BUCKET


EVIDENCE_MOUNT_ROOT = Path("/mnt/routespark-pcf-evidence")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_WORKER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_UTC_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:"
    r"[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|\+00:00)$"
)
_RESULT_KEYS = frozenset(
    {
        "schemaVersion",
        "resultHash",
        "acceptanceEventId",
        "acceptedAt",
        "claimTokenHash",
        "request",
    }
)
_REQUEST_KEYS = frozenset(
    {
        "schemaVersion",
        "workId",
        "attemptNumber",
        "inputHashVersion",
        "inputHash",
        "workerId",
        "workerBuildVersion",
        "ocrContractVersion",
        "completedAt",
        "proposal",
        "evidencePackage",
    }
)
_ARTIFACT_LIMITS = {
    "source_image": 20_000_000,
    "before_payload": 10_000_000,
    "proposed_result": 10_000_000,
    "package_manifest": 10_000_000,
}


class EffectInputError(RuntimeError):
    """The immutable result or its authoritative evidence failed closed."""


@dataclass(frozen=True)
class EffectArtifact:
    role: str
    relative_path: str
    sha256: str
    size_bytes: int
    object_path: str | None
    generation: str | None


@dataclass(frozen=True)
class EffectInput:
    work_id: uuid.UUID
    work_attempt: int
    route: str
    delivery: str
    input_hash: str
    accepted_at: datetime
    completed_at: datetime
    confidence: int
    loading_date: str
    proposal: dict[str, Any]
    pages: tuple[dict[str, str | int], ...]
    artifacts: tuple[EffectArtifact, ...]
    evidence_manifest_sha256: str
    evidence_run_relative_path: str


@dataclass(frozen=True)
class EvidenceProof:
    work_id: uuid.UUID
    run_relative_path: str
    manifest_sha256: str
    artifact_count: int
    evidence_sha256: str
    verified_at: datetime


def _utc(value: Any, *, name: str) -> datetime:
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise EffectInputError(f"pcf_effect_{name}_invalid")
    try:
        parsed = datetime.fromisoformat(
            value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value
        )
    except ValueError as exc:
        raise EffectInputError(f"pcf_effect_{name}_invalid") from exc
    return parsed.astimezone(timezone.utc)


def _canonical_digest(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _strict_relative(value: Any) -> str:
    if not isinstance(value, str) or not value or len(value) > 512 or "\\" in value:
        raise EffectInputError("pcf_effect_evidence_path_invalid")
    parsed = PurePosixPath(value)
    if parsed.is_absolute() or ".." in parsed.parts or "." in parsed.parts:
        raise EffectInputError("pcf_effect_evidence_path_invalid")
    return parsed.as_posix()


def load_effect_input(claim: EffectClaim) -> EffectInput:
    """Revalidate the stored result and bind it to one effect claim."""
    if not isinstance(claim, EffectClaim):
        raise ValueError("invalid PCF effect claim")
    stored = claim.result
    if not isinstance(stored, Mapping) or frozenset(stored) != _RESULT_KEYS:
        raise EffectInputError("pcf_effect_result_invalid")
    request = stored.get("request")
    if not isinstance(request, Mapping) or frozenset(request) != _REQUEST_KEYS:
        raise EffectInputError("pcf_effect_result_invalid")
    try:
        acceptance_id = uuid.UUID(str(stored.get("acceptanceEventId")))
        request_work_id = uuid.UUID(str(request.get("workId")))
    except (TypeError, ValueError) as exc:
        raise EffectInputError("pcf_effect_result_invalid") from exc
    result_hash = stored.get("resultHash")
    token_hash = stored.get("claimTokenHash")
    worker_id = request.get("workerId")
    if (
        stored.get("schemaVersion") != 1
        or not isinstance(result_hash, str)
        or _SHA256_RE.fullmatch(result_hash) is None
        or canonical_result_identity(dict(request))[0] != result_hash
        or acceptance_id.int == 0
        or not isinstance(token_hash, str)
        or _SHA256_RE.fullmatch(token_hash) is None
        or request.get("schemaVersion") != 1
        or request_work_id != claim.work_id
        or type(request.get("attemptNumber")) is not int
        or request["attemptNumber"] != claim.work_attempt
        or request.get("inputHashVersion") != claim.input_hash_version
        or request.get("inputHash") != claim.input_hash
        or not isinstance(worker_id, str)
        or _WORKER_RE.fullmatch(worker_id) is None
        or not isinstance(request.get("workerBuildVersion"), str)
        or not str(request["workerBuildVersion"]).strip()
        or not isinstance(request.get("ocrContractVersion"), str)
        or not str(request["ocrContractVersion"]).strip()
    ):
        raise EffectInputError("pcf_effect_result_invalid")
    accepted_at = _utc(stored.get("acceptedAt"), name="accepted_at")
    completed_at = _utc(request.get("completedAt"), name="completed_at")
    if completed_at > accepted_at + timedelta(seconds=30):
        raise EffectInputError("pcf_effect_result_invalid")
    try:
        pages = manifest_pages(
            claim.manifest,
            route=claim.route,
            delivery=claim.delivery,
            expected_bucket=EXPECTED_BUCKET,
        )
        ocr_context = validate_pcf_ocr_context(
            claim.ocr_context,
            manifest_containers={str(page["containerCode"]) for page in pages},
        )
        confidence = validate_proposal(
            request.get("proposal"),
            route=claim.route,
            source_delivery=claim.delivery,
            pages=pages,
            expected_source_loading_date=ocr_context["validatedLoadingDate"],
        )
        evidence = EvidencePackage.model_validate(request.get("evidencePackage"))
        validate_evidence_package(
            evidence,
            route=claim.route,
            delivery=claim.delivery,
            input_hash=claim.input_hash,
            pages=pages,
        )
    except Exception as exc:
        if isinstance(exc, EffectInputError):
            raise
        raise EffectInputError("pcf_effect_result_invalid") from exc
    if confidence != claim.confidence:
        raise EffectInputError("pcf_effect_result_invalid")
    artifacts = tuple(
        EffectArtifact(
            artifact.role,
            _strict_relative(artifact.relative_path),
            artifact.sha256,
            artifact.size_bytes,
            artifact.object_path,
            artifact.generation,
        )
        for artifact in evidence.artifacts
    )
    run_relative = f"{claim.work_id}-attempt-{claim.work_attempt}"
    return EffectInput(
        claim.work_id,
        claim.work_attempt,
        claim.route,
        claim.delivery,
        claim.input_hash,
        accepted_at,
        completed_at,
        confidence,
        str(ocr_context["validatedLoadingDate"]),
        dict(request["proposal"]),
        pages,
        artifacts,
        evidence.manifest_sha256,
        run_relative,
    )


def _hash_exact_file(path: Path, *, expected_size: int, maximum: int) -> str:
    if (
        type(expected_size) is not int
        or not 1 <= expected_size <= maximum
        or path.is_symlink()
    ):
        raise EffectInputError("pcf_effect_evidence_file_invalid")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise EffectInputError("pcf_effect_evidence_unavailable") from exc
    digest = hashlib.sha256()
    observed = 0
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != expected_size:
            raise EffectInputError("pcf_effect_evidence_file_invalid")
        while True:
            chunk = os.read(descriptor, min(1_048_576, maximum + 1 - observed))
            if not chunk:
                break
            observed += len(chunk)
            if observed > maximum:
                raise EffectInputError("pcf_effect_evidence_file_invalid")
            digest.update(chunk)
    finally:
        os.close(descriptor)
    if observed != expected_size:
        raise EffectInputError("pcf_effect_evidence_file_invalid")
    return digest.hexdigest()


def verify_external_evidence(
    effect_input: EffectInput,
    *,
    mount_root: Path = EVIDENCE_MOUNT_ROOT,
    now: datetime | None = None,
) -> EvidenceProof:
    """Re-hash every exact artifact from the read-only evidence authority."""
    if not isinstance(effect_input, EffectInput):
        raise ValueError("invalid PCF effect input")
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    root = mount_root.expanduser()
    if not root.is_absolute() or root.is_symlink() or not root.is_dir():
        raise EffectInputError("pcf_effect_evidence_unavailable")
    run_root = root / effect_input.evidence_run_relative_path
    if run_root.is_symlink() or not run_root.is_dir():
        raise EffectInputError("pcf_effect_evidence_unavailable")
    root_resolved = root.resolve(strict=True)
    run_resolved = run_root.resolve(strict=True)
    if run_resolved.parent != root_resolved:
        raise EffectInputError("pcf_effect_evidence_path_invalid")

    observed: list[dict[str, Any]] = []
    for artifact in effect_input.artifacts:
        maximum = _ARTIFACT_LIMITS.get(artifact.role)
        if maximum is None:
            raise EffectInputError("pcf_effect_evidence_file_invalid")
        path = run_resolved.joinpath(*PurePosixPath(artifact.relative_path).parts)
        try:
            resolved_parent = path.parent.resolve(strict=True)
        except OSError as exc:
            raise EffectInputError("pcf_effect_evidence_unavailable") from exc
        if resolved_parent != run_resolved and run_resolved not in resolved_parent.parents:
            raise EffectInputError("pcf_effect_evidence_path_invalid")
        digest = _hash_exact_file(
            path,
            expected_size=artifact.size_bytes,
            maximum=maximum,
        )
        if digest != artifact.sha256:
            raise EffectInputError("pcf_effect_evidence_hash_mismatch")
        observed.append(
            {
                "relativePath": artifact.relative_path,
                "role": artifact.role,
                "sha256": digest,
                "sizeBytes": artifact.size_bytes,
            }
        )
    manifest = next(
        (item for item in observed if item["role"] == "package_manifest"),
        None,
    )
    if manifest is None or manifest["sha256"] != effect_input.evidence_manifest_sha256:
        raise EffectInputError("pcf_effect_evidence_manifest_mismatch")
    proof_payload = {
        "artifactCount": len(observed),
        "artifacts": sorted(observed, key=lambda item: item["relativePath"]),
        "inputHash": effect_input.input_hash,
        "manifestSha256": effect_input.evidence_manifest_sha256,
        "runRelativePath": effect_input.evidence_run_relative_path,
        "workId": str(effect_input.work_id),
    }
    return EvidenceProof(
        effect_input.work_id,
        effect_input.evidence_run_relative_path,
        effect_input.evidence_manifest_sha256,
        len(observed),
        _canonical_digest(proof_payload),
        observed_at,
    )
