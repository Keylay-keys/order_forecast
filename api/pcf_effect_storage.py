"""Exact-generation Cloud Storage effects for PCF projection and cleanup."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from google.api_core.exceptions import NotFound, PreconditionFailed

from .pcf_work_runtime import EXPECTED_BUCKET


_SOURCE_RE = re.compile(
    r"^routes/(?P<route>[0-9]{1,10})/unprocessed/"
    r"(?P<delivery>[0-9]{8,12})_(?P<container>[0-9]{18})_"
    r"page_(?P<page>[1-9][0-9]{0,2})\.(?:jpg|jpeg|png)$",
    re.IGNORECASE,
)
_CANONICAL_RE = re.compile(
    r"^routes/(?P<route>[0-9]{1,10})/pcfs/"
    r"(?P<delivery>[0-9]{8,12})/(?P<container>[0-9]{18})/"
    r"page_(?P<page>[1-9][0-9]{0,2})\.jpg$"
)
_MD5_RE = re.compile(r"^[A-Za-z0-9+/]{22}==$")


class StorageEffectError(RuntimeError):
    """An exact PCF Storage effect failed before it could be proved."""


@dataclass(frozen=True)
class SourceObject:
    route: str
    delivery: str
    container: str
    page: int
    object_path: str
    generation: int
    size_bytes: int
    md5_hash: str


@dataclass(frozen=True)
class CanonicalObjectProof:
    source_path: str
    source_generation: int
    canonical_path: str
    canonical_generation: int
    size_bytes: int
    md5_hash: str
    disposition: Literal["applied", "already_correct"]
    evidence_sha256: str
    verified_at: datetime


@dataclass(frozen=True)
class SourceCleanupProof:
    source_path: str
    source_generation: int
    disposition: Literal["applied", "already_absent", "superseded_input"]
    current_generation: int | None
    evidence_sha256: str
    verified_at: datetime


@dataclass(frozen=True)
class SourcePreflightProof:
    source_path: str
    source_generation: int
    disposition: Literal["current", "already_absent", "superseded_input"]
    current_generation: int | None
    evidence_sha256: str
    verified_at: datetime


def _digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _positive_integer(value: Any, *, name: str, maximum: int) -> int:
    if isinstance(value, bool):
        raise StorageEffectError(f"pcf_storage_{name}_invalid")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise StorageEffectError(f"pcf_storage_{name}_invalid") from exc
    if parsed < 1 or parsed > maximum or str(parsed) != str(value):
        raise StorageEffectError(f"pcf_storage_{name}_invalid")
    return parsed


def source_object(raw: dict[str, Any], *, route: str, delivery: str) -> SourceObject:
    """Convert one normalized manifest page to a path-bound object."""
    if not isinstance(raw, dict):
        raise StorageEffectError("pcf_storage_source_invalid")
    path = raw.get("storagePath")
    container = raw.get("containerCode")
    page = raw.get("pageNumber")
    md5_hash = raw.get("md5Hash")
    match = _SOURCE_RE.fullmatch(path) if isinstance(path, str) else None
    if (
        match is None
        or match.group("route") != route
        or match.group("delivery") != delivery
        or match.group("container") != str(container)
        or int(match.group("page")) != page
        or not isinstance(md5_hash, str)
        or _MD5_RE.fullmatch(md5_hash) is None
    ):
        raise StorageEffectError("pcf_storage_source_invalid")
    return SourceObject(
        route=route,
        delivery=delivery,
        container=str(container),
        page=_positive_integer(page, name="page", maximum=999),
        object_path=path,
        generation=_positive_integer(
            raw.get("storageGeneration"),
            name="generation",
            maximum=9_223_372_036_854_775_807,
        ),
        size_bytes=_positive_integer(
            raw.get("size"), name="size", maximum=20_000_000
        ),
        md5_hash=md5_hash,
    )


def canonical_path(
    source: SourceObject,
    *,
    target_delivery: str,
    target_page: int | None = None,
) -> str:
    page = source.page if target_page is None else _positive_integer(
        target_page, name="page", maximum=999
    )
    value = (
        f"routes/{source.route}/pcfs/{target_delivery}/"
        f"{source.container}/page_{page}.jpg"
    )
    if _CANONICAL_RE.fullmatch(value) is None:
        raise StorageEffectError("pcf_storage_destination_invalid")
    return value


def _blob_identity(blob: Any, *, expected_path: str) -> tuple[int, int, str]:
    if blob is None or str(getattr(blob, "name", "")) != expected_path:
        raise StorageEffectError("pcf_storage_object_missing")
    generation = _positive_integer(
        getattr(blob, "generation", None),
        name="generation",
        maximum=9_223_372_036_854_775_807,
    )
    size = _positive_integer(
        getattr(blob, "size", None), name="size", maximum=20_000_000
    )
    md5_hash = getattr(blob, "md5_hash", None)
    if not isinstance(md5_hash, str) or _MD5_RE.fullmatch(md5_hash) is None:
        raise StorageEffectError("pcf_storage_md5_invalid")
    return generation, size, md5_hash


class ExactPcfObjectStore:
    """Small Storage adapter with no prefix/list/bulk capability."""

    def __init__(self, bucket: Any, *, clock: Any = None) -> None:
        if str(getattr(bucket, "name", "")) != EXPECTED_BUCKET:
            raise ValueError("PCF object store requires the frozen bucket contract")
        self._bucket = bucket
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _get(self, path: str, *, generation: int | None = None) -> Any:
        return self._bucket.get_blob(path, generation=generation)

    def _current_generation(self, path: str) -> int | None:
        blob = self._get(path)
        if blob is None:
            return None
        generation, _, _ = _blob_identity(blob, expected_path=path)
        return generation

    def ensure_canonical_copy(
        self,
        source: SourceObject,
        *,
        target_delivery: str,
        target_page: int | None = None,
    ) -> CanonicalObjectProof:
        """Copy one frozen source generation with destination CAS and read-back."""
        if not isinstance(source, SourceObject):
            raise ValueError("invalid PCF source object")
        destination = canonical_path(
            source, target_delivery=target_delivery, target_page=target_page
        )
        source_blob = self._get(source.object_path, generation=source.generation)
        source_generation, source_size, source_md5 = _blob_identity(
            source_blob, expected_path=source.object_path
        )
        if (
            source_generation != source.generation
            or source_size != source.size_bytes
            or source_md5 != source.md5_hash
        ):
            raise StorageEffectError("pcf_storage_source_identity_mismatch")

        current = self._get(destination)
        current_generation = 0
        if current is not None:
            current_generation, current_size, current_md5 = _blob_identity(
                current, expected_path=destination
            )
            if current_size == source.size_bytes and current_md5 == source.md5_hash:
                return self._canonical_proof(
                    source, destination, current_generation, "already_correct"
                )
        try:
            self._bucket.copy_blob(
                source_blob,
                self._bucket,
                destination,
                source_generation=source.generation,
                if_source_generation_match=source.generation,
                if_generation_match=current_generation,
            )
        except PreconditionFailed as exc:
            raced = self._get(destination)
            if raced is not None:
                generation, size, md5_hash = _blob_identity(
                    raced, expected_path=destination
                )
                if size == source.size_bytes and md5_hash == source.md5_hash:
                    return self._canonical_proof(
                        source, destination, generation, "already_correct"
                    )
            raise StorageEffectError("pcf_storage_destination_conflict") from exc

        verified = self._get(destination)
        generation, size, md5_hash = _blob_identity(
            verified, expected_path=destination
        )
        if size != source.size_bytes or md5_hash != source.md5_hash:
            raise StorageEffectError("pcf_storage_copy_unverified")
        return self._canonical_proof(source, destination, generation, "applied")

    def prove_canonical(
        self,
        source: SourceObject,
        *,
        target_delivery: str,
        target_page: int | None = None,
    ) -> CanonicalObjectProof:
        """Read and prove current canonical bytes without copying or listing."""
        if not isinstance(source, SourceObject):
            raise ValueError("invalid PCF source object")
        destination = canonical_path(
            source, target_delivery=target_delivery, target_page=target_page
        )
        blob = self._get(destination)
        generation, size, md5_hash = _blob_identity(
            blob, expected_path=destination
        )
        if size != source.size_bytes or md5_hash != source.md5_hash:
            raise StorageEffectError("pcf_storage_canonical_unverified")
        return self._canonical_proof(
            source, destination, generation, "already_correct"
        )

    def _canonical_proof(
        self,
        source: SourceObject,
        destination: str,
        canonical_generation: int,
        disposition: Literal["applied", "already_correct"],
    ) -> CanonicalObjectProof:
        verified_at = self._clock().astimezone(timezone.utc)
        payload = {
            "canonicalGeneration": canonical_generation,
            "canonicalPath": destination,
            "md5Hash": source.md5_hash,
            "sizeBytes": source.size_bytes,
            "sourceGeneration": source.generation,
            "sourcePath": source.object_path,
        }
        return CanonicalObjectProof(
            source.object_path,
            source.generation,
            destination,
            canonical_generation,
            source.size_bytes,
            source.md5_hash,
            disposition,
            _digest(payload),
            verified_at,
        )

    def verify_canonical(self, proof: CanonicalObjectProof) -> CanonicalObjectProof:
        """Independently re-read one exact canonical generation."""
        if not isinstance(proof, CanonicalObjectProof):
            raise ValueError("invalid PCF canonical proof")
        blob = self._get(proof.canonical_path, generation=proof.canonical_generation)
        generation, size, md5_hash = _blob_identity(
            blob, expected_path=proof.canonical_path
        )
        if (
            generation != proof.canonical_generation
            or size != proof.size_bytes
            or md5_hash != proof.md5_hash
        ):
            raise StorageEffectError("pcf_storage_canonical_unverified")
        source_match = _SOURCE_RE.fullmatch(proof.source_path)
        canonical_match = _CANONICAL_RE.fullmatch(proof.canonical_path)
        if source_match is None or canonical_match is None:
            raise StorageEffectError("pcf_storage_proof_path_invalid")
        source = SourceObject(
            route=canonical_match.group("route"),
            delivery=source_match.group("delivery"),
            container=canonical_match.group("container"),
            page=int(source_match.group("page")),
            object_path=proof.source_path,
            generation=proof.source_generation,
            size_bytes=proof.size_bytes,
            md5_hash=proof.md5_hash,
        )
        return self._canonical_proof(
            source,
            proof.canonical_path,
            proof.canonical_generation,
            "already_correct",
        )

    def cleanup_source(self, source: SourceObject) -> SourceCleanupProof:
        """Delete only the current frozen generation; a successor wins."""
        if not isinstance(source, SourceObject):
            raise ValueError("invalid PCF source object")
        current = self._get(source.object_path)
        if current is None:
            return self._cleanup_proof(source, "already_absent", None)
        generation, size, md5_hash = _blob_identity(
            current, expected_path=source.object_path
        )
        if generation != source.generation:
            return self._cleanup_proof(source, "superseded_input", generation)
        if size != source.size_bytes or md5_hash != source.md5_hash:
            raise StorageEffectError("pcf_storage_source_identity_mismatch")
        try:
            current.delete(if_generation_match=source.generation)
        except NotFound:
            latest = self._current_generation(source.object_path)
            disposition = "already_absent" if latest is None else "superseded_input"
            return self._cleanup_proof(source, disposition, latest)
        except PreconditionFailed:
            latest = self._current_generation(source.object_path)
            if latest is not None and latest != source.generation:
                return self._cleanup_proof(source, "superseded_input", latest)
            raise StorageEffectError("pcf_storage_delete_precondition_failed") from None
        latest = self._current_generation(source.object_path)
        if latest is None:
            return self._cleanup_proof(source, "applied", None)
        if latest != source.generation:
            return self._cleanup_proof(source, "superseded_input", latest)
        raise StorageEffectError("pcf_storage_delete_unverified")

    def preflight_source(self, source: SourceObject) -> SourcePreflightProof:
        """Classify one exact source before an all-object cleanup begins."""
        if not isinstance(source, SourceObject):
            raise ValueError("invalid PCF source object")
        current = self._get(source.object_path)
        if current is None:
            disposition = "already_absent"
            generation = None
        else:
            generation, size, md5_hash = _blob_identity(
                current, expected_path=source.object_path
            )
            if generation != source.generation:
                disposition = "superseded_input"
            elif size != source.size_bytes or md5_hash != source.md5_hash:
                raise StorageEffectError("pcf_storage_source_identity_mismatch")
            else:
                disposition = "current"
        verified_at = self._clock().astimezone(timezone.utc)
        payload = {
            "currentGeneration": generation,
            "sourceGeneration": source.generation,
            "sourcePath": source.object_path,
        }
        return SourcePreflightProof(
            source.object_path,
            source.generation,
            disposition,
            generation,
            _digest(payload),
            verified_at,
        )

    def _cleanup_proof(
        self,
        source: SourceObject,
        disposition: Literal["applied", "already_absent", "superseded_input"],
        current_generation: int | None,
    ) -> SourceCleanupProof:
        verified_at = self._clock().astimezone(timezone.utc)
        payload = {
            "currentGeneration": current_generation,
            "sourceGeneration": source.generation,
            "sourcePath": source.object_path,
        }
        return SourceCleanupProof(
            source.object_path,
            source.generation,
            disposition,
            current_generation,
            _digest(payload),
            verified_at,
        )
