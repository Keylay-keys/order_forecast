"""Production-shaped PCF lifecycle runtime adapters.

The runtime deliberately exposes only exact document/object operations. It is
disabled unless a short-lived control file admits the exact semantic key and
records the independently verified legacy-mutation fence.
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence

from google.api_core.exceptions import (
    AlreadyExists,
    FailedPrecondition,
    NotFound,
    PreconditionFailed,
)
from google.cloud import firestore, storage

from order_forecast.api.pcf_event_contract import (
    business_hash,
    canonical_date,
    firestore_revision,
    normalize_google_update_time,
    projection_event_business_hash,
)
from order_forecast.scripts.pcf_lifecycle_due import (
    ClaimedLifecycleWork,
    ProjectedContainer,
    load_projected_delivery,
    refresh_lifecycle_claim_authority,
)
from order_forecast.scripts.pcf_lifecycle_effect import (
    ArchiveContractError,
    ArchiveManifest,
    ExactContainerSnapshot,
    ExactDeliverySnapshot,
    ExactStorageObject,
    expected_archive_path,
)


_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_ENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CONTROL_KEYS = {"schemaVersion", "effectsEnabled", "allProvenWork", "fences"}
_FENCE_KEYS = {
    "semanticKey",
    "routeNumber",
    "deliveryId",
    "expiresAt",
    "evidenceSha256",
}


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ArchiveContractError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc)


def _google_time(value: datetime) -> str:
    normalized = _utc(value)
    return normalized.strftime("%Y-%m-%dT%H:%M:%S.%f000Z")


def _parse_rfc3339(value: Any, name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ArchiveContractError(f"{name} must be RFC3339 text")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ArchiveContractError(f"{name} must be RFC3339 text") from exc
    return _utc(parsed)


def _plain_firestore(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ArchiveContractError("non-integral Firestore floats are not hashable")
        return int(value)
    if isinstance(value, datetime):
        return _google_time(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _plain_firestore(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain_firestore(item) for item in value]
    latitude = getattr(value, "latitude", None)
    longitude = getattr(value, "longitude", None)
    if latitude is not None and longitude is not None:
        return {"latitude": str(latitude), "longitude": str(longitude)}
    path = getattr(value, "path", None)
    if isinstance(path, str):
        return {"documentPath": path}
    raise ArchiveContractError(
        f"unsupported Firestore value in archive hash: {type(value).__name__}"
    )


def _document_hash(data: Mapping[str, Any]) -> str:
    return business_hash(_plain_firestore(data))


@dataclass(frozen=True)
class LifecycleFence:
    semantic_key: str
    route_number: str
    delivery_id: str
    expires_at: datetime
    evidence_sha256: str


@dataclass(frozen=True)
class LifecycleRuntimeControl:
    effects_enabled: bool
    all_proven_work: bool
    fences: tuple[LifecycleFence, ...]

    @classmethod
    def parse(cls, payload: Any) -> "LifecycleRuntimeControl":
        if not isinstance(payload, Mapping) or set(payload) != _CONTROL_KEYS:
            raise ArchiveContractError("lifecycle control keys are invalid")
        if payload["schemaVersion"] != 1:
            raise ArchiveContractError("unsupported lifecycle control schemaVersion")
        if not isinstance(payload["effectsEnabled"], bool) or not isinstance(
            payload["allProvenWork"], bool
        ):
            raise ArchiveContractError("lifecycle control booleans are invalid")
        raw_fences = payload["fences"]
        if not isinstance(raw_fences, list) or len(raw_fences) > 100:
            raise ArchiveContractError("lifecycle fences must be an array of at most 100")
        fences: list[LifecycleFence] = []
        seen: set[str] = set()
        for raw in raw_fences:
            if not isinstance(raw, Mapping) or set(raw) != _FENCE_KEYS:
                raise ArchiveContractError("lifecycle fence keys are invalid")
            semantic_key = raw["semanticKey"]
            route = raw["routeNumber"]
            delivery = raw["deliveryId"]
            digest = raw["evidenceSha256"]
            if not isinstance(semantic_key, str) or not 2 <= len(semantic_key) <= 2048:
                raise ArchiveContractError("lifecycle semantic key is invalid")
            if not isinstance(route, str) or _ROUTE_RE.fullmatch(route) is None:
                raise ArchiveContractError("lifecycle fence route is invalid")
            if not isinstance(delivery, str) or _ENTITY_RE.fullmatch(delivery) is None:
                raise ArchiveContractError("lifecycle fence delivery is invalid")
            if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
                raise ArchiveContractError("lifecycle fence evidence digest is invalid")
            if semantic_key in seen:
                raise ArchiveContractError("duplicate lifecycle semantic key fence")
            seen.add(semantic_key)
            fences.append(
                LifecycleFence(
                    semantic_key=semantic_key,
                    route_number=route,
                    delivery_id=delivery,
                    expires_at=_parse_rfc3339(raw["expiresAt"], "expiresAt"),
                    evidence_sha256=digest,
                )
            )
        if payload["effectsEnabled"] and not payload["allProvenWork"] and not fences:
            raise ArchiveContractError("enabled lifecycle effects require an exact fence")
        return cls(
            effects_enabled=payload["effectsEnabled"],
            all_proven_work=payload["allProvenWork"],
            fences=tuple(fences),
        )

    @classmethod
    def read(cls, path: Path) -> "LifecycleRuntimeControl":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ArchiveContractError("lifecycle control file is unavailable") from exc
        return cls.parse(payload)

    def allowed_semantic_keys(self, now_utc: datetime) -> tuple[str, ...]:
        now = _utc(now_utc)
        return tuple(
            sorted(
                fence.semantic_key
                for fence in self.fences
                if now < fence.expires_at <= now + timedelta(hours=1)
            )
        )

    def admits(self, claimed: ClaimedLifecycleWork, now_utc: datetime) -> bool:
        if not self.effects_enabled:
            return False
        now = _utc(now_utc)
        return any(
            fence.semantic_key == claimed.semantic_key
            and fence.route_number == claimed.route_number
            and fence.delivery_id == claimed.delivery_id
            and now < fence.expires_at <= now + timedelta(hours=1)
            for fence in self.fences
        ) or self.all_proven_work


class PostgresClaimAuthority:
    """Short transactions for lease CAS; never holds PostgreSQL over Firebase I/O."""

    def __init__(
        self,
        connection_provider: Callable[[], Any],
        connection_releaser: Callable[[Any], None],
        *,
        lease_seconds: int = 300,
    ) -> None:
        self._connection_provider = connection_provider
        self._connection_releaser = connection_releaser
        self._lease_seconds = lease_seconds

    def refresh(
        self, claimed: ClaimedLifecycleWork, *, begin_effect: bool
    ) -> Optional[datetime]:
        conn = self._connection_provider()
        try:
            checked_at = refresh_lifecycle_claim_authority(
                conn,
                work_id=claimed.id,
                claim_token=claimed.claim_token,
                lease_seconds=self._lease_seconds,
                begin_effect=begin_effect,
            )
            conn.commit()
            return checked_at
        except Exception:
            conn.rollback()
            raise
        finally:
            self._connection_releaser(conn)


class PostgresProjectionLoader:
    def __init__(
        self,
        connection_provider: Callable[[], Any],
        connection_releaser: Callable[[Any], None],
    ) -> None:
        self._connection_provider = connection_provider
        self._connection_releaser = connection_releaser

    def load(self, route_number: str, delivery_id: str) -> list[ProjectedContainer]:
        conn = self._connection_provider()
        try:
            rows = load_projected_delivery(conn, route_number, delivery_id)
            conn.rollback()
            return rows
        finally:
            self._connection_releaser(conn)


def _normalized_source_items(data: Mapping[str, Any]) -> tuple[list[int], list[dict[str, Any]]]:
    pages = data.get("pages")
    if not isinstance(pages, list) or not pages:
        raise ArchiveContractError("source container pages are missing")
    page_numbers: list[int] = []
    items: list[dict[str, Any]] = []
    for page_index, page in enumerate(pages, start=1):
        if not isinstance(page, Mapping):
            raise ArchiveContractError("source page is malformed")
        page_number = page.get("pageNumber", page_index)
        if isinstance(page_number, bool) or not isinstance(page_number, int):
            raise ArchiveContractError("source page number is malformed")
        page_numbers.append(page_number)
        raw_items = page.get("items")
        if not isinstance(raw_items, list):
            raise ArchiveContractError("source page items are malformed")
        for item_position, item in enumerate(raw_items):
            if not isinstance(item, Mapping):
                raise ArchiveContractError("source item is malformed")
            product = str(item.get("product") or "").strip()
            if not product:
                raise ArchiveContractError("source item product is missing")
            raw_expiry = item.get("bestBefore", item.get("expiryDate"))
            expiry = None if raw_expiry in (None, "") else canonical_date(str(raw_expiry))
            guaranteed = item.get("guaranteed")
            if guaranteed is not None:
                if not isinstance(guaranteed, Mapping):
                    raise ArchiveContractError("source guarantee is malformed")
                guarantee_time = guaranteed.get("guaranteeExpiresAt")
                if isinstance(guarantee_time, datetime):
                    guarantee_time = _google_time(guarantee_time)
                guaranteed = {
                    "isGuaranteed": guaranteed.get("isGuaranteed") is True,
                    "guaranteeExpiresAt": guarantee_time,
                }
            items.append(
                {
                    "pageNumber": page_number,
                    "itemPosition": item_position,
                    "product": product,
                    "description": str(item.get("description") or "").strip(),
                    "expiryDate": expiry,
                    "visibleOnList": item.get("visibleOnList") is not False,
                    "isLowQuantity": item.get("isLowQuantity") is True,
                    "isShortCoded": item.get("isShortCoded") is True,
                    "guaranteed": guaranteed,
                }
            )
    if len(page_numbers) != len(set(page_numbers)):
        raise ArchiveContractError("source page numbers are not unique")
    return sorted(page_numbers), items


def _manifest_object_payload(obj: ExactStorageObject) -> dict[str, Any]:
    return {
        "sourcePath": obj.source_path,
        "archivePath": obj.archive_path,
        "generation": obj.generation,
        "size": obj.size,
        "md5Hash": obj.md5_hash,
    }


def _manifest_payload(manifest: ArchiveManifest) -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "semanticKey": manifest.semantic_key,
        "routeNumber": manifest.route_number,
        "deliveryId": manifest.delivery_id,
        "sourceBusinessHash": manifest.source_business_hash,
        "parentRevision": manifest.parent_revision,
        "parentHash": manifest.parent_hash,
        "manifestHash": manifest.manifest_hash,
        "containers": [
            {
                "containerId": row.projected.container_id,
                "documentRevision": row.document_revision,
                "documentHash": row.document_hash,
                "sourceBusinessHash": row.projected.source_business_hash,
                "objects": [_manifest_object_payload(obj) for obj in row.objects],
            }
            for row in manifest.containers
        ],
        "unmappedObjects": [
            _manifest_object_payload(obj) for obj in manifest.unmapped_objects
        ],
    }


def _object_from_payload(value: Any) -> ExactStorageObject:
    if not isinstance(value, Mapping) or set(value) != {
        "sourcePath", "archivePath", "generation", "size", "md5Hash"
    }:
        raise ArchiveContractError("stored archive object manifest is invalid")
    return ExactStorageObject(
        source_path=str(value["sourcePath"]),
        archive_path=str(value["archivePath"]),
        generation=str(value["generation"]),
        size=int(value["size"]),
        md5_hash=str(value["md5Hash"]),
    )


def _manifest_from_payload(value: Any) -> ArchiveManifest:
    expected = {
        "schemaVersion", "semanticKey", "routeNumber", "deliveryId",
        "sourceBusinessHash", "parentRevision", "parentHash", "manifestHash",
        "containers", "unmappedObjects",
    }
    if not isinstance(value, Mapping) or set(value) != expected or value["schemaVersion"] != 1:
        raise ArchiveContractError("stored archive manifest is invalid")
    containers: list[ExactContainerSnapshot] = []
    for row in value["containers"]:
        row_expected = {
            "containerId", "documentRevision", "documentHash",
            "sourceBusinessHash", "objects",
        }
        if not isinstance(row, Mapping) or set(row) != row_expected:
            raise ArchiveContractError("stored archive container manifest is invalid")
        projected = ProjectedContainer(
            route_number=str(value["routeNumber"]),
            delivery_id=str(value["deliveryId"]),
            container_id=str(row["containerId"]),
            active=False,
            items=(),
            source_revision=str(row["documentRevision"]),
            source_business_hash=str(row["sourceBusinessHash"]),
        )
        containers.append(
            ExactContainerSnapshot(
                projected=projected,
                document_revision=str(row["documentRevision"]),
                document_hash=str(row["documentHash"]),
                objects=tuple(_object_from_payload(obj) for obj in row["objects"]),
            )
        )
    return ArchiveManifest(
        semantic_key=str(value["semanticKey"]),
        route_number=str(value["routeNumber"]),
        delivery_id=str(value["deliveryId"]),
        source_business_hash=str(value["sourceBusinessHash"]),
        parent_revision=str(value["parentRevision"]),
        parent_hash=str(value["parentHash"]),
        containers=tuple(containers),
        manifest_hash=str(value["manifestHash"]),
        unmapped_objects=tuple(
            _object_from_payload(obj) for obj in value["unmappedObjects"]
        ),
    )


class FirebaseExactArchiveAdapter:
    """Firebase/Storage adapter with exact paths and generation preconditions."""

    def __init__(
        self,
        *,
        firestore_client: firestore.Client,
        bucket: storage.Bucket,
        control_path: Path,
        claim_authority: PostgresClaimAuthority,
        projection_loader: PostgresProjectionLoader,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self._db = firestore_client
        self._bucket = bucket
        self._control_path = control_path
        self._claim_authority = claim_authority
        self._projection_loader = projection_loader
        self._clock = clock
        self._raw_parent: Optional[dict[str, Any]] = None
        self._raw_containers: dict[str, dict[str, Any]] = {}

    def assert_exact_mutation_fence(self, claimed: ClaimedLifecycleWork) -> bool:
        return LifecycleRuntimeControl.read(self._control_path).admits(
            claimed, self._clock()
        )

    def refresh_claim_authority(
        self, claimed: ClaimedLifecycleWork, *, begin_effect: bool
    ) -> Optional[datetime]:
        return self._claim_authority.refresh(claimed, begin_effect=begin_effect)

    def read_current_retention(self, route_number: str) -> tuple[int, str]:
        if _ROUTE_RE.fullmatch(str(route_number)) is None:
            raise ArchiveContractError("route number is invalid")
        route = self._db.document(f"routes/{route_number}").get()
        route_data = route.to_dict() or {}
        owner_uid = str(route_data.get("ownerUid") or "").strip()
        if not route.exists or _ENTITY_RE.fullmatch(owner_uid) is None:
            raise ArchiveContractError("authoritative route owner is unavailable")
        owner = self._db.document(f"users/{owner_uid}").get()
        owner_data = owner.to_dict() or {}
        profile = owner_data.get("profile") if isinstance(owner_data.get("profile"), Mapping) else {}
        if (
            not owner.exists
            or profile.get("role") not in {"owner", "ownerOnly"}
            or str(profile.get("routeNumber")) != str(route_number)
            or owner.update_time is None
        ):
            raise ArchiveContractError("authoritative owner contract is invalid")
        database = (
            (owner_data.get("userSettings") or {}).get("database")
            if isinstance(owner_data.get("userSettings"), Mapping)
            else {}
        )
        raw = database.get("pcfRetentionDays", 4) if isinstance(database, Mapping) else 4
        if isinstance(raw, bool) or not isinstance(raw, int) or not 1 <= raw <= 3650:
            raise ArchiveContractError("authoritative PCF retention is invalid")
        return raw, firestore_revision(_google_time(owner.update_time), "upsert")

    def _storage_object(self, blob: storage.Blob, route: str, delivery: str) -> ExactStorageObject:
        if blob.generation is None or blob.size is None or not blob.md5_hash:
            blob.reload()
        generation = str(blob.generation or "")
        if not generation.isdigit() or int(generation) < 1 or blob.size is None or not blob.md5_hash:
            raise ArchiveContractError("source object metadata is incomplete")
        return ExactStorageObject(
            source_path=blob.name,
            archive_path=expected_archive_path(route, delivery, blob.name),
            generation=generation,
            size=int(blob.size),
            md5_hash=str(blob.md5_hash),
        )

    def read_exact_delivery(
        self, route_number: str, delivery_id: str
    ) -> Optional[ExactDeliverySnapshot]:
        if _ROUTE_RE.fullmatch(str(route_number)) is None or _ENTITY_RE.fullmatch(str(delivery_id)) is None:
            raise ArchiveContractError("delivery identity is invalid")
        parent_ref = self._db.document(f"routes/{route_number}/pcfs/{delivery_id}")
        parent = parent_ref.get()
        if not parent.exists:
            return None
        if parent.update_time is None:
            raise ArchiveContractError("source parent revision is unavailable")
        parent_data = parent.to_dict() or {}
        if "_clusterArchive" in parent_data:
            raise ArchiveContractError("source parent uses reserved archive metadata")
        container_docs = sorted(
            parent_ref.collection("containers").stream(), key=lambda row: row.id
        )
        projected_rows = {
            row.container_id: row
            for row in self._projection_loader.load(str(route_number), str(delivery_id))
        }
        if set(projected_rows) != {row.id for row in container_docs}:
            raise ArchiveContractError("source documents and current projection differ")
        prefix = f"routes/{route_number}/pcfs/{delivery_id}/"
        source_objects = [
            self._storage_object(blob, str(route_number), str(delivery_id))
            for blob in self._bucket.list_blobs(prefix=prefix)
            if blob.name != prefix
        ]
        known_prefixes = {
            row.id: f"{prefix}{row.id}/" for row in container_docs
        }
        containers: list[ExactContainerSnapshot] = []
        self._raw_parent = dict(parent_data)
        self._raw_containers = {}
        parent_loading = parent_data.get("loadingDate")
        loading_date = None if parent_loading in (None, "") else canonical_date(str(parent_loading))
        for snapshot in container_docs:
            if snapshot.update_time is None:
                raise ArchiveContractError("source container revision is unavailable")
            data = snapshot.to_dict() or {}
            if "_clusterArchive" in data:
                raise ArchiveContractError("source container uses reserved archive metadata")
            row = projected_rows[snapshot.id]
            page_numbers, items = _normalized_source_items(data)
            source_revision = firestore_revision(_google_time(snapshot.update_time), "upsert")
            event = {
                "schemaVersion": 1,
                "sourceEventId": f"archive-check/{route_number}/{delivery_id}/{snapshot.id}",
                "eventType": "projection.pcf_container.changed",
                "mutationKind": "upsert",
                "occurredAt": _google_time(snapshot.update_time),
                "sourcePath": f"routes/{route_number}/pcfs/{delivery_id}/containers/{snapshot.id}",
                "sourceRevision": source_revision,
                "entity": {
                    "route": str(route_number),
                    "delivery": str(delivery_id),
                    "container": snapshot.id,
                },
                "automationExcluded": False,
                "fixture": None,
                "payload": {
                    "active": row.active,
                    "loadingDate": loading_date,
                    "pageNumbers": page_numbers,
                    "items": items,
                },
            }
            current_hash = projection_event_business_hash(event)
            if source_revision != row.source_revision or current_hash != row.source_business_hash:
                raise ArchiveContractError("source container is newer than current projection")
            self._raw_containers[snapshot.id] = dict(data)
            containers.append(
                ExactContainerSnapshot(
                    projected=ProjectedContainer(
                        route_number=str(route_number),
                        delivery_id=str(delivery_id),
                        container_id=snapshot.id,
                        active=row.active,
                        items=tuple(items),
                        source_revision=source_revision,
                        source_business_hash=current_hash,
                    ),
                    document_revision=source_revision,
                    document_hash=_document_hash(data),
                    objects=tuple(
                        sorted(
                            (
                                obj for obj in source_objects
                                if obj.source_path.startswith(known_prefixes[snapshot.id])
                            ),
                            key=lambda obj: obj.source_path,
                        )
                    ),
                )
            )
        unmapped = tuple(
            sorted(
                (
                    obj for obj in source_objects
                    if not any(obj.source_path.startswith(item) for item in known_prefixes.values())
                ),
                key=lambda obj: obj.source_path,
            )
        )
        return ExactDeliverySnapshot(
            route_number=str(route_number),
            delivery_id=str(delivery_id),
            parent_revision=firestore_revision(_google_time(parent.update_time), "upsert"),
            parent_hash=_document_hash(parent_data),
            containers=tuple(containers),
            unmapped_objects=unmapped,
        )

    def _archive_parent_ref(self, route: str, delivery: str):
        return self._db.document(f"routes/{route}/archivedPCFs/{delivery}")

    def read_archive_manifest(self, route_number: str, delivery_id: str) -> Optional[ArchiveManifest]:
        snapshot = self._archive_parent_ref(route_number, delivery_id).get()
        if not snapshot.exists:
            return None
        data = snapshot.to_dict() or {}
        cluster = data.get("_clusterArchive")
        if not isinstance(cluster, Mapping) or set(cluster) != {"manifest"}:
            raise ArchiveContractError("archive parent is not cluster-manifested")
        return _manifest_from_payload(cluster["manifest"])

    def create_or_verify_archive_manifest(self, manifest: ArchiveManifest) -> bool:
        if self._raw_parent is None:
            raise ArchiveContractError("source parent was not read")
        payload = _manifest_payload(manifest)
        document = dict(self._raw_parent)
        document["_archive"] = {
            "archivedAt": firestore.SERVER_TIMESTAMP,
            "manifestHash": manifest.manifest_hash,
            "originalPath": f"routes/{manifest.route_number}/pcfs/{manifest.delivery_id}",
            "reason": "Expired items beyond retention period",
            "semanticKey": manifest.semantic_key,
        }
        document["_clusterArchive"] = {"manifest": payload}
        ref = self._archive_parent_ref(manifest.route_number, manifest.delivery_id)
        try:
            ref.create(document)
            return True
        except AlreadyExists:
            existing = ref.get().to_dict() or {}
            return existing.get("_clusterArchive") == {"manifest": payload}

    def copy_archive_documents(self, manifest: ArchiveManifest) -> None:
        parent_ref = self._archive_parent_ref(manifest.route_number, manifest.delivery_id)
        for row in manifest.containers:
            raw = self._raw_containers.get(row.projected.container_id)
            if raw is None:
                raise ArchiveContractError("source container was not read")
            document = dict(raw)
            document["_archive"] = {
                "manifestHash": manifest.manifest_hash,
                "originalPath": (
                    f"routes/{manifest.route_number}/pcfs/{manifest.delivery_id}/"
                    f"containers/{row.projected.container_id}"
                ),
                "semanticKey": manifest.semantic_key,
            }
            ref = parent_ref.collection("containers").document(row.projected.container_id)
            try:
                ref.create(document)
            except AlreadyExists:
                existing = ref.get().to_dict() or {}
                archive = existing.get("_archive")
                original = {key: value for key, value in existing.items() if key != "_archive"}
                if (
                    not isinstance(archive, Mapping)
                    or archive.get("manifestHash") != manifest.manifest_hash
                    or _document_hash(original) != row.document_hash
                ):
                    raise ArchiveContractError("archive container conflicts with manifest")

    def copy_exact_object(self, obj: ExactStorageObject) -> None:
        parts = obj.source_path.split("/")
        if len(parts) < 5 or parts[0] != "routes" or parts[2] != "pcfs":
            raise ArchiveContractError("source object path is invalid")
        if obj.archive_path != expected_archive_path(
            parts[1], parts[3], obj.source_path
        ):
            raise ArchiveContractError("archive path mapping is invalid")
        source = self._bucket.blob(obj.source_path, generation=int(obj.generation))
        try:
            copied = self._bucket.copy_blob(
                source,
                self._bucket,
                obj.archive_path,
                source_generation=int(obj.generation),
                if_source_generation_match=int(obj.generation),
                if_generation_match=0,
            )
        except (FailedPrecondition, PreconditionFailed):
            copied = self._bucket.get_blob(obj.archive_path)
            if copied is None:
                raise ArchiveContractError("archive object create precondition failed")
        if copied.size is None or copied.md5_hash is None:
            copied.reload()
        if int(copied.size or -1) != obj.size or copied.md5_hash != obj.md5_hash:
            raise ArchiveContractError("archive object content conflicts with manifest")

    def verify_archive(self, manifest: ArchiveManifest) -> bool:
        parent = self._archive_parent_ref(manifest.route_number, manifest.delivery_id).get()
        if not parent.exists:
            return False
        parent_data = parent.to_dict() or {}
        if parent_data.get("_clusterArchive") != {"manifest": _manifest_payload(manifest)}:
            return False
        parent_original = {
            key: value for key, value in parent_data.items()
            if key not in {"_archive", "_clusterArchive"}
        }
        if _document_hash(parent_original) != manifest.parent_hash:
            return False
        containers = sorted(
            parent.reference.collection("containers").stream(), key=lambda row: row.id
        )
        if [row.id for row in containers] != [row.projected.container_id for row in manifest.containers]:
            return False
        for source, archived in zip(manifest.containers, containers):
            data = archived.to_dict() or {}
            meta = data.pop("_archive", None)
            if not isinstance(meta, Mapping) or meta.get("manifestHash") != manifest.manifest_hash:
                return False
            if _document_hash(data) != source.document_hash:
                return False
        expected = {
            obj.archive_path: obj
            for row in manifest.containers
            for obj in row.objects
        }
        expected.update({obj.archive_path: obj for obj in manifest.unmapped_objects})
        prefix = f"routes/{manifest.route_number}/archivedPCFs/{manifest.delivery_id}/"
        archived_objects = {
            blob.name: blob for blob in self._bucket.list_blobs(prefix=prefix)
            if blob.name != prefix
        }
        if set(archived_objects) != set(expected):
            return False
        return all(
            int(archived_objects[path].size or -1) == obj.size
            and archived_objects[path].md5_hash == obj.md5_hash
            for path, obj in expected.items()
        )

    def list_exact_source_objects(
        self, route_number: str, delivery_id: str
    ) -> Sequence[ExactStorageObject]:
        prefix = f"routes/{route_number}/pcfs/{delivery_id}/"
        return tuple(
            self._storage_object(blob, route_number, delivery_id)
            for blob in self._bucket.list_blobs(prefix=prefix)
            if blob.name != prefix
        )

    def delete_exact_source_object(self, obj: ExactStorageObject) -> None:
        try:
            self._bucket.blob(obj.source_path, generation=int(obj.generation)).delete(
                if_generation_match=int(obj.generation)
            )
        except NotFound:
            return

    def delete_source_documents_atomically(self, manifest: ArchiveManifest) -> None:
        parent_ref = self._db.document(
            f"routes/{manifest.route_number}/pcfs/{manifest.delivery_id}"
        )
        expected = {row.projected.container_id: row for row in manifest.containers}
        transaction = self._db.transaction()

        @firestore.transactional
        def delete_exact(transaction):
            parent = parent_ref.get(transaction=transaction)
            containers = sorted(
                parent_ref.collection("containers").stream(transaction=transaction),
                key=lambda row: row.id,
            )
            if not parent.exists or parent.update_time is None:
                raise ArchiveContractError("source parent disappeared before document delete")
            if set(expected) != {row.id for row in containers}:
                raise ArchiveContractError("source container set changed before document delete")
            if firestore_revision(_google_time(parent.update_time), "upsert") != manifest.parent_revision:
                raise ArchiveContractError("source parent changed before document delete")
            for snapshot in containers:
                row = expected[snapshot.id]
                if (
                    snapshot.update_time is None
                    or firestore_revision(_google_time(snapshot.update_time), "upsert")
                    != row.document_revision
                    or _document_hash(snapshot.to_dict() or {}) != row.document_hash
                ):
                    raise ArchiveContractError("source container changed before document delete")
            for snapshot in containers:
                transaction.delete(
                    snapshot.reference,
                    option=self._db.write_option(last_update_time=snapshot.update_time),
                )
            transaction.delete(
                parent_ref,
                option=self._db.write_option(last_update_time=parent.update_time),
            )

        delete_exact(transaction)

    def verify_exact_source_absent(self, route_number: str, delivery_id: str) -> bool:
        parent_ref = self._db.document(f"routes/{route_number}/pcfs/{delivery_id}")
        if parent_ref.get().exists:
            return False
        if any(parent_ref.collection("containers").stream()):
            return False
        prefix = f"routes/{route_number}/pcfs/{delivery_id}/"
        return not any(self._bucket.list_blobs(prefix=prefix, max_results=1))
