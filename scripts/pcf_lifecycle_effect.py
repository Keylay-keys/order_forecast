"""Exact, retry-safe lifecycle archive orchestration.

The orchestration is backend-agnostic and has no Firebase credentials.  A
runtime adapter must implement every exact-path and precondition operation. The
engine never offers prefix, route-wide, delivery-wide, recursive, or bulk
deletion primitives.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Optional, Protocol, Sequence

try:
    from .pcf_lifecycle_due import (
        ClaimedLifecycleWork,
        ProjectedContainer,
        cleanup_authority_decision,
    )
except ImportError:
    from pcf_lifecycle_due import (  # type: ignore
        ClaimedLifecycleWork,
        ProjectedContainer,
        cleanup_authority_decision,
    )

try:
    from order_forecast.api.pcf_event_contract import business_hash
except ImportError:
    from api.pcf_event_contract import business_hash  # type: ignore


class ArchiveContractError(ValueError):
    """The source or archive identity violates an exact-path invariant."""


@dataclass(frozen=True)
class ExactStorageObject:
    source_path: str
    archive_path: str
    generation: str
    size: int
    md5_hash: str


@dataclass(frozen=True)
class ExactContainerSnapshot:
    projected: ProjectedContainer
    document_revision: str
    document_hash: str
    objects: tuple[ExactStorageObject, ...]


@dataclass(frozen=True)
class ExactDeliverySnapshot:
    route_number: str
    delivery_id: str
    parent_revision: str
    parent_hash: str
    containers: tuple[ExactContainerSnapshot, ...]
    unmapped_objects: tuple[ExactStorageObject, ...] = ()


@dataclass(frozen=True)
class ArchiveManifest:
    semantic_key: str
    route_number: str
    delivery_id: str
    source_business_hash: str
    parent_revision: str
    parent_hash: str
    containers: tuple[ExactContainerSnapshot, ...]
    manifest_hash: str
    unmapped_objects: tuple[ExactStorageObject, ...] = ()


@dataclass(frozen=True)
class ArchiveEffectResult:
    completed: bool
    disposition: str
    manifest_hash: Optional[str] = None


class ExactArchiveAdapter(Protocol):
    """Required exact operations; deliberately no mass-delete surface."""

    def assert_exact_mutation_fence(self, claimed: ClaimedLifecycleWork) -> bool: ...

    def refresh_claim_authority(
        self,
        claimed: ClaimedLifecycleWork,
        *,
        begin_effect: bool,
    ) -> Optional[datetime]: ...

    def read_current_retention(self, route_number: str) -> tuple[int, str]: ...

    def read_exact_delivery(
        self, route_number: str, delivery_id: str
    ) -> Optional[ExactDeliverySnapshot]: ...

    def read_archive_manifest(
        self, route_number: str, delivery_id: str
    ) -> Optional[ArchiveManifest]: ...

    def create_or_verify_archive_manifest(self, manifest: ArchiveManifest) -> bool: ...

    def copy_archive_documents(self, manifest: ArchiveManifest) -> None: ...

    def copy_exact_object(self, obj: ExactStorageObject) -> None: ...

    def verify_archive(self, manifest: ArchiveManifest) -> bool: ...

    def list_exact_source_objects(
        self, route_number: str, delivery_id: str
    ) -> Sequence[ExactStorageObject]: ...

    def delete_exact_source_object(self, obj: ExactStorageObject) -> None: ...

    def delete_source_documents_atomically(self, manifest: ArchiveManifest) -> None: ...

    def verify_exact_source_absent(self, route_number: str, delivery_id: str) -> bool: ...


def _expected_parent_path(route_number: str, delivery_id: str) -> str:
    return f"routes/{route_number}/pcfs/{delivery_id}"


def _expected_object_prefix(route_number: str, delivery_id: str, container_id: str) -> str:
    return f"routes/{route_number}/pcfs/{delivery_id}/{container_id}/"


def _expected_archive_prefix(route_number: str, delivery_id: str, container_id: str) -> str:
    return f"routes/{route_number}/archivedPCFs/{delivery_id}/{container_id}/"


def _expected_delivery_source_prefix(route_number: str, delivery_id: str) -> str:
    return f"routes/{route_number}/pcfs/{delivery_id}/"


def _expected_delivery_archive_prefix(route_number: str, delivery_id: str) -> str:
    return f"routes/{route_number}/archivedPCFs/{delivery_id}/"


def expected_archive_path(
    route_number: str,
    delivery_id: str,
    source_path: str,
) -> str:
    """Map one exact source object to the identical archive-relative suffix."""
    source_prefix = _expected_delivery_source_prefix(route_number, delivery_id)
    if not source_path.startswith(source_prefix):
        raise ArchiveContractError("object escaped exact delivery source prefix")
    suffix = source_path[len(source_prefix) :]
    if not suffix or suffix.startswith("/") or any(part in {"", ".", ".."} for part in suffix.split("/")):
        raise ArchiveContractError("object has an invalid source-relative suffix")
    return _expected_delivery_archive_prefix(route_number, delivery_id) + suffix


def _object_identity(obj: ExactStorageObject) -> tuple[str, str, str, int, str]:
    return (
        obj.source_path,
        obj.archive_path,
        obj.generation,
        obj.size,
        obj.md5_hash,
    )


def _validate_snapshot(snapshot: ExactDeliverySnapshot) -> None:
    if not snapshot.containers:
        raise ArchiveContractError("delivery snapshot must contain at least one container")
    seen_containers: set[str] = set()
    seen_sources: set[str] = set()
    seen_archives: set[str] = set()
    for container in snapshot.containers:
        projected = container.projected
        if projected.route_number != snapshot.route_number:
            raise ArchiveContractError("container route does not match delivery")
        if projected.delivery_id != snapshot.delivery_id:
            raise ArchiveContractError("container delivery does not match parent")
        if projected.container_id in seen_containers:
            raise ArchiveContractError("duplicate container identity")
        seen_containers.add(projected.container_id)
        source_prefix = _expected_object_prefix(
            snapshot.route_number, snapshot.delivery_id, projected.container_id
        )
        archive_prefix = _expected_archive_prefix(
            snapshot.route_number, snapshot.delivery_id, projected.container_id
        )
        for obj in container.objects:
            if not obj.source_path.startswith(source_prefix):
                raise ArchiveContractError("object escaped exact source container prefix")
            if obj.archive_path != expected_archive_path(
                snapshot.route_number, snapshot.delivery_id, obj.source_path
            ):
                raise ArchiveContractError("archive path changed the exact source-relative suffix")
            if not obj.archive_path.startswith(archive_prefix):
                raise ArchiveContractError("object escaped exact archive container prefix")
            if obj.source_path in seen_sources or obj.archive_path in seen_archives:
                raise ArchiveContractError("duplicate object identity")
            if not obj.generation or obj.size < 0 or not obj.md5_hash:
                raise ArchiveContractError("object generation, size, and MD5 are required")
            seen_sources.add(obj.source_path)
            seen_archives.add(obj.archive_path)
    known_prefixes = tuple(
        _expected_object_prefix(
            snapshot.route_number,
            snapshot.delivery_id,
            row.projected.container_id,
        )
        for row in snapshot.containers
    )
    for obj in snapshot.unmapped_objects:
        if any(obj.source_path.startswith(prefix) for prefix in known_prefixes):
            raise ArchiveContractError("unmapped object belongs to a known container")
        if obj.archive_path != expected_archive_path(
            snapshot.route_number, snapshot.delivery_id, obj.source_path
        ):
            raise ArchiveContractError("archive path changed the exact source-relative suffix")
        if obj.source_path in seen_sources or obj.archive_path in seen_archives:
            raise ArchiveContractError("duplicate object identity")
        if not obj.generation or obj.size < 0 or not obj.md5_hash:
            raise ArchiveContractError("object generation, size, and MD5 are required")
        seen_sources.add(obj.source_path)
        seen_archives.add(obj.archive_path)


def _manifest_material(
    *,
    semantic_key: str,
    route_number: str,
    delivery_id: str,
    source_business_hash: str,
    parent_revision: str,
    parent_hash: str,
    containers: Sequence[ExactContainerSnapshot],
    unmapped_objects: Sequence[ExactStorageObject],
) -> Mapping[str, Any]:
    return {
        "deliveryId": delivery_id,
        "parentHash": parent_hash,
        "parentRevision": parent_revision,
        "routeNumber": route_number,
        "semanticKey": semantic_key,
        "sourceBusinessHash": source_business_hash,
        "containers": [
            {
                "containerId": container.projected.container_id,
                "documentHash": container.document_hash,
                "documentRevision": container.document_revision,
                "objects": [
                    {
                        "archivePath": obj.archive_path,
                        "generation": obj.generation,
                        "md5Hash": obj.md5_hash,
                        "size": obj.size,
                        "sourcePath": obj.source_path,
                    }
                    for obj in sorted(container.objects, key=lambda row: row.source_path)
                ],
                "sourceBusinessHash": container.projected.source_business_hash,
            }
            for container in sorted(
                containers, key=lambda row: row.projected.container_id
            )
        ],
        "unmappedObjects": [
            {
                "archivePath": obj.archive_path,
                "generation": obj.generation,
                "md5Hash": obj.md5_hash,
                "size": obj.size,
                "sourcePath": obj.source_path,
            }
            for obj in sorted(unmapped_objects, key=lambda row: row.source_path)
        ],
    }


def _computed_manifest_hash(manifest: ArchiveManifest) -> str:
    return business_hash(
        _manifest_material(
            semantic_key=manifest.semantic_key,
            route_number=manifest.route_number,
            delivery_id=manifest.delivery_id,
            source_business_hash=manifest.source_business_hash,
            parent_revision=manifest.parent_revision,
            parent_hash=manifest.parent_hash,
            containers=manifest.containers,
            unmapped_objects=manifest.unmapped_objects,
        )
    )


def build_archive_manifest(
    claimed: ClaimedLifecycleWork,
    snapshot: ExactDeliverySnapshot,
) -> ArchiveManifest:
    if snapshot.route_number != claimed.route_number or snapshot.delivery_id != claimed.delivery_id:
        raise ArchiveContractError("claim and exact delivery identity differ")
    _validate_snapshot(snapshot)
    ordered_containers = tuple(
        sorted(snapshot.containers, key=lambda row: row.projected.container_id)
    )
    material = _manifest_material(
        semantic_key=claimed.semantic_key,
        route_number=snapshot.route_number,
        delivery_id=snapshot.delivery_id,
        source_business_hash=claimed.source_business_hash,
        parent_revision=snapshot.parent_revision,
        parent_hash=snapshot.parent_hash,
        containers=ordered_containers,
        unmapped_objects=tuple(
            sorted(snapshot.unmapped_objects, key=lambda row: row.source_path)
        ),
    )
    return ArchiveManifest(
        semantic_key=claimed.semantic_key,
        route_number=snapshot.route_number,
        delivery_id=snapshot.delivery_id,
        source_business_hash=claimed.source_business_hash,
        parent_revision=snapshot.parent_revision,
        parent_hash=snapshot.parent_hash,
        containers=ordered_containers,
        manifest_hash=business_hash(material),
        unmapped_objects=tuple(
            sorted(snapshot.unmapped_objects, key=lambda row: row.source_path)
        ),
    )


def _manifest_objects(manifest: ArchiveManifest) -> tuple[ExactStorageObject, ...]:
    return tuple(
        sorted(
            (
                obj
                for container in manifest.containers
                for obj in container.objects
            ),
            key=lambda value: value.source_path,
        )
    ) + tuple(sorted(manifest.unmapped_objects, key=lambda value: value.source_path))


def _snapshot_document_identity(snapshot: ExactDeliverySnapshot) -> tuple[Any, ...]:
    return (
        snapshot.parent_revision,
        snapshot.parent_hash,
        tuple(
            (
                row.projected.container_id,
                row.document_revision,
                row.document_hash,
                row.projected.source_business_hash,
            )
            for row in sorted(snapshot.containers, key=lambda value: value.projected.container_id)
        ),
    )


def _manifest_document_identity(manifest: ArchiveManifest) -> tuple[Any, ...]:
    return (
        manifest.parent_revision,
        manifest.parent_hash,
        tuple(
            (
                row.projected.container_id,
                row.document_revision,
                row.document_hash,
                row.projected.source_business_hash,
            )
            for row in manifest.containers
        ),
    )


def execute_exact_archive(
    claimed: ClaimedLifecycleWork,
    adapter: ExactArchiveAdapter,
) -> ArchiveEffectResult:
    """Archive and clean one exact claimed delivery with live CAS authority."""
    if not adapter.assert_exact_mutation_fence(claimed):
        return ArchiveEffectResult(False, "mutation_fence_not_proven")
    now_utc = adapter.refresh_claim_authority(claimed, begin_effect=False)
    if now_utc is None:
        return ArchiveEffectResult(False, "claim_authority_not_active")

    retention_days, policy_revision = adapter.read_current_retention(claimed.route_number)
    snapshot = adapter.read_exact_delivery(claimed.route_number, claimed.delivery_id)
    if snapshot is None:
        existing = adapter.read_archive_manifest(claimed.route_number, claimed.delivery_id)
        if (
            existing is not None
            and existing.semantic_key == claimed.semantic_key
            and adapter.verify_archive(existing)
            and adapter.verify_exact_source_absent(claimed.route_number, claimed.delivery_id)
        ):
            return ArchiveEffectResult(True, "already_complete", existing.manifest_hash)
        return ArchiveEffectResult(False, "source_missing_without_verified_archive")

    authority = cleanup_authority_decision(
        claimed,
        [row.projected for row in snapshot.containers],
        now_utc=now_utc,
        retention_days=retention_days,
        policy_revision=policy_revision,
    )
    if not authority.authorized:
        return ArchiveEffectResult(False, authority.disposition)

    existing_manifest = adapter.read_archive_manifest(
        claimed.route_number, claimed.delivery_id
    )
    if existing_manifest is not None:
        if (
            _computed_manifest_hash(existing_manifest)
            != existing_manifest.manifest_hash
            or
            existing_manifest.semantic_key != claimed.semantic_key
            or existing_manifest.source_business_hash != claimed.source_business_hash
            or _snapshot_document_identity(snapshot)
            != _manifest_document_identity(existing_manifest)
        ):
            return ArchiveEffectResult(False, "archive_manifest_conflict")
        manifest = existing_manifest
    else:
        manifest = build_archive_manifest(claimed, snapshot)
    if not adapter.create_or_verify_archive_manifest(manifest):
        return ArchiveEffectResult(False, "archive_manifest_conflict")
    adapter.copy_archive_documents(manifest)
    for obj in _manifest_objects(manifest):
        adapter.copy_exact_object(obj)
    if not adapter.verify_archive(manifest):
        return ArchiveEffectResult(False, "archive_verification_failed", manifest.manifest_hash)

    if not adapter.assert_exact_mutation_fence(claimed):
        return ArchiveEffectResult(False, "mutation_fence_lost", manifest.manifest_hash)
    delete_authority_utc = adapter.refresh_claim_authority(claimed, begin_effect=True)
    if delete_authority_utc is None:
        return ArchiveEffectResult(False, "claim_authority_lost", manifest.manifest_hash)
    current_retention, current_policy_revision = adapter.read_current_retention(
        claimed.route_number
    )
    second_snapshot = adapter.read_exact_delivery(claimed.route_number, claimed.delivery_id)
    if second_snapshot is None:
        if adapter.verify_exact_source_absent(claimed.route_number, claimed.delivery_id):
            return ArchiveEffectResult(True, "already_complete", manifest.manifest_hash)
        return ArchiveEffectResult(False, "source_changed_after_archive", manifest.manifest_hash)
    second_authority = cleanup_authority_decision(
        claimed,
        [row.projected for row in second_snapshot.containers],
        now_utc=delete_authority_utc,
        retention_days=current_retention,
        policy_revision=current_policy_revision,
    )
    if not second_authority.authorized:
        return ArchiveEffectResult(False, second_authority.disposition, manifest.manifest_hash)
    if _snapshot_document_identity(second_snapshot) != _manifest_document_identity(manifest):
        return ArchiveEffectResult(False, "source_documents_changed", manifest.manifest_hash)

    manifest_objects = {obj.source_path: obj for obj in _manifest_objects(manifest)}
    current_objects = {
        obj.source_path: obj
        for obj in adapter.list_exact_source_objects(claimed.route_number, claimed.delivery_id)
    }
    if set(current_objects) - set(manifest_objects):
        return ArchiveEffectResult(False, "unexpected_source_object", manifest.manifest_hash)
    for path, current in current_objects.items():
        if _object_identity(current) != _object_identity(manifest_objects[path]):
            return ArchiveEffectResult(False, "source_object_changed", manifest.manifest_hash)
    for path in sorted(current_objects):
        adapter.delete_exact_source_object(manifest_objects[path])

    if adapter.list_exact_source_objects(claimed.route_number, claimed.delivery_id):
        return ArchiveEffectResult(False, "source_objects_remain", manifest.manifest_hash)
    if not adapter.assert_exact_mutation_fence(claimed):
        return ArchiveEffectResult(False, "mutation_fence_lost", manifest.manifest_hash)
    if adapter.refresh_claim_authority(claimed, begin_effect=False) is None:
        return ArchiveEffectResult(False, "claim_authority_lost", manifest.manifest_hash)
    adapter.delete_source_documents_atomically(manifest)
    if not adapter.verify_exact_source_absent(claimed.route_number, claimed.delivery_id):
        return ArchiveEffectResult(False, "source_cleanup_not_verified", manifest.manifest_hash)
    return ArchiveEffectResult(True, "archived_verified", manifest.manifest_hash)
