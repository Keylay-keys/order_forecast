"""Strict, versioned contracts for the credential-less PCF worker API."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    field_validator,
)

from .pcf_event_contract import EventContractError, validate_pcf_ocr_context


RESULT_SCHEMA_VERSION = 1
INPUT_HASH_VERSION = "pcf-work-input-v1"
REQUIRED_CAPABILITY = "macos_vision"
OMITTED_SAPS = frozenset({"22770", "32346", "37433", "52094", "52191", "52271", "53078"})

_ROUTE_RE = re.compile(r"^[0-9]{1,10}$")
_DELIVERY_RE = re.compile(r"^[0-9]{8,12}$")
_CONTAINER_RE = re.compile(r"^[0-9]{18}$")
_PRODUCT_RE = re.compile(r"^[0-9]{4,5}$")
_DAYS_RE = re.compile(r"^[0-9]{1,3}\*?$")
_HEX_RE = re.compile(r"^[0-9a-f]{64}$")
_MD5_RE = re.compile(r"^[A-Za-z0-9+/]{22}==$")
_WORKER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,63}$")
_STORAGE_PATH_RE = re.compile(
    r"^routes/(?P<route>[0-9]+)/unprocessed/(?P<delivery>[0-9]+)_"
    r"(?P<container>[^/]+)_page_(?P<page>[0-9]+)\.(?:jpg|jpeg|png)$",
    re.IGNORECASE,
)


class ContractValidationError(ValueError):
    """A safe, non-secret-bearing worker-contract failure."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)


class ClaimRequest(_StrictModel):
    worker_id: str = Field(alias="workerId", min_length=3, max_length=64)
    required_capability: Literal["macos_vision"] = Field(alias="requiredCapability")
    supported_result_schema_versions: list[StrictInt] = Field(
        alias="supportedResultSchemaVersions", min_length=1, max_length=8
    )
    wait_seconds: StrictInt = Field(alias="waitSeconds", ge=0, le=30)

    @field_validator("worker_id")
    @classmethod
    def _validate_worker_id(cls, value: str) -> str:
        if _WORKER_RE.fullmatch(value) is None:
            raise ValueError("invalid workerId")
        return value

    @field_validator("supported_result_schema_versions")
    @classmethod
    def _validate_versions(cls, value: list[int]) -> list[int]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate result schema version")
        return value


class AttemptRequest(_StrictModel):
    attempt_number: StrictInt = Field(alias="attemptNumber", ge=1)
    claim_token: str = Field(alias="claimToken", min_length=32, max_length=512)


class EvidenceArtifact(_StrictModel):
    role: Literal["source_image", "before_payload", "proposed_result", "package_manifest"]
    relative_path: str = Field(alias="relativePath", min_length=1, max_length=512)
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    size_bytes: StrictInt = Field(alias="sizeBytes", ge=1)
    object_path: str | None = Field(alias="objectPath", max_length=1024)
    generation: str | None = Field(
        min_length=1, max_length=20, pattern=r"^[1-9][0-9]{0,19}$"
    )


class EvidencePackage(_StrictModel):
    route: str = Field(min_length=6, max_length=6, pattern=r"^[0-9]{6}$")
    delivery: str = Field(min_length=8, max_length=12, pattern=r"^[0-9]{8,12}$")
    input_hash_version: Literal["pcf-work-input-v1"] = Field(alias="inputHashVersion")
    input_hash: str = Field(
        alias="inputHash", min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    manifest_sha256: str = Field(
        alias="manifestSha256", min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    manifest_relative_path: str = Field(
        alias="manifestRelativePath", min_length=1, max_length=512
    )
    artifacts: list[EvidenceArtifact] = Field(min_length=4, max_length=256)


class ResultRequest(AttemptRequest):
    schema_version: Literal[1] = Field(alias="schemaVersion")
    work_id: UUID = Field(alias="workId")
    input_hash_version: Literal["pcf-work-input-v1"] = Field(alias="inputHashVersion")
    input_hash: str = Field(
        alias="inputHash", min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$"
    )
    worker_id: str = Field(alias="workerId", min_length=3, max_length=64)
    worker_build_version: str = Field(alias="workerBuildVersion", min_length=1, max_length=128)
    ocr_contract_version: str = Field(alias="ocrContractVersion", min_length=1, max_length=128)
    completed_at: AwareDatetime = Field(alias="completedAt")
    proposal: dict[str, Any]
    evidence_package: EvidencePackage = Field(alias="evidencePackage")

    @field_validator("work_id", mode="before")
    @classmethod
    def _parse_work_id(cls, value: Any) -> UUID:
        if not isinstance(value, str):
            raise ValueError("workId must be a canonical UUID string")
        parsed = UUID(value)
        if str(parsed) != value:
            raise ValueError("workId must be a canonical UUID string")
        return parsed

    @field_validator("completed_at", mode="before")
    @classmethod
    def _parse_completed_at(cls, value: Any) -> datetime:
        return _parse_rfc3339(value, "completedAt")


class BackupReceiptRequest(_StrictModel):
    backup_id: str = Field(
        alias="backupId",
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{7,127}$",
    )
    dump_name: str = Field(
        alias="dumpName",
        min_length=6,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,249}\.dump$",
    )
    sha256: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    size_bytes: StrictInt = Field(alias="sizeBytes", ge=1)
    covers_through: AwareDatetime = Field(alias="coversThrough")
    verified_at: AwareDatetime = Field(alias="verifiedAt")
    off_box_targets: list[str] = Field(alias="offBoxTargets", min_length=2, max_length=2)
    restore_inventory_verified: StrictBool = Field(alias="restoreInventoryVerified")

    @field_validator("covers_through", "verified_at", mode="before")
    @classmethod
    def _parse_timestamp(cls, value: Any, info: Any) -> datetime:
        return _parse_rfc3339(value, info.field_name)

    @field_validator("off_box_targets")
    @classmethod
    def _validate_targets(cls, value: list[str]) -> list[str]:
        if len(set(value)) != 2 or any(not item or len(item) > 255 for item in value):
            raise ValueError("invalid off-box targets")
        return value


def _parse_rfc3339(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field} must be an RFC3339 UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be an RFC3339 UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be an RFC3339 UTC timestamp")
    return parsed


def _calendar_date(value: Any, *, field: str) -> datetime:
    if not isinstance(value, str):
        raise ContractValidationError(f"{field} must be MM/DD/YYYY")
    try:
        return datetime.strptime(value, "%m/%d/%Y").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ContractValidationError(f"{field} must be MM/DD/YYYY") from exc


def _safe_relative_path(value: str) -> bool:
    path = PurePosixPath(value)
    return (
        not path.is_absolute()
        and value == path.as_posix()
        and value not in {"", "."}
        and ".." not in path.parts
        and "\\" not in value
    )


def manifest_pages(
    manifest: Any,
    *,
    route: str,
    delivery: str,
    expected_bucket: str,
) -> tuple[dict[str, str | int], ...]:
    """Return a canonical exact-generation page inventory or fail closed."""
    if not isinstance(manifest, dict) or set(manifest) != {
        "containerCount",
        "pageCount",
        "objects",
    }:
        raise ContractValidationError("invalid manifest shape")
    pages = manifest.get("objects")
    if not isinstance(pages, list) or not 1 <= len(pages) <= 100:
        raise ContractValidationError("manifest pages must be a bounded nonempty list")
    if type(manifest.get("pageCount")) is not int or manifest.get("pageCount") != len(pages):
        raise ContractValidationError("manifest page count mismatch")
    normalized: list[dict[str, str | int]] = []
    logical_keys: set[str] = set()
    objects: set[tuple[str, str]] = set()
    for raw in pages:
        if not isinstance(raw, dict) or set(raw) != {
            "containerCode",
            "uploadPageNumber",
            "objectPath",
            "generation",
            "size",
            "md5Hash",
        }:
            raise ContractValidationError("invalid manifest page shape")
        container = raw["containerCode"]
        page_number = raw["uploadPageNumber"]
        bucket = expected_bucket
        object_path = raw["objectPath"]
        generation = raw["generation"]
        if not isinstance(container, str) or not _CONTAINER_RE.fullmatch(container):
            raise ContractValidationError("invalid manifest container")
        if type(page_number) is not int or page_number < 1:
            raise ContractValidationError("invalid manifest page number")
        if bucket != expected_bucket:
            raise ContractValidationError("manifest bucket is outside the signer boundary")
        if not isinstance(object_path, str) or len(object_path) > 1024:
            raise ContractValidationError("invalid manifest object path")
        match = _STORAGE_PATH_RE.fullmatch(object_path)
        if (
            match is None
            or match.group("route") != route
            or match.group("delivery") != delivery
            or match.group("container") != container
            or int(match.group("page")) != page_number
        ):
            raise ContractValidationError("manifest object path escaped work identity")
        if not isinstance(generation, str) or not generation.isdigit() or int(generation) < 1:
            raise ContractValidationError("invalid manifest object generation")
        if type(raw["size"]) is not int or not 1 <= raw["size"] <= 20_000_000:
            raise ContractValidationError("invalid manifest object size")
        if not isinstance(raw["md5Hash"], str) or not _MD5_RE.fullmatch(raw["md5Hash"]):
            raise ContractValidationError("invalid manifest object MD5")
        logical_key = f"{container}_{page_number}"
        identity = (object_path, generation)
        if logical_key in logical_keys or identity in objects:
            raise ContractValidationError("duplicate manifest page identity")
        logical_keys.add(logical_key)
        objects.add(identity)
        normalized.append(
            {
                "logicalPageKey": logical_key,
                "containerCode": container,
                "pageNumber": page_number,
                "storageBucket": bucket,
                "storagePath": object_path,
                "storageGeneration": generation,
                "size": raw["size"],
                "md5Hash": raw["md5Hash"],
            }
        )
    if (
        type(manifest.get("containerCount")) is not int
        or manifest.get("containerCount") != len({page["containerCode"] for page in normalized})
    ):
        raise ContractValidationError("manifest container count mismatch")
    canonical = sorted(
        normalized,
        key=lambda page: (
            str(page["storagePath"]),
            int(str(page["storageGeneration"])),
        ),
    )
    return tuple(canonical)


def _validate_identity_evidence(raw: Any, *, value: str, field: str) -> None:
    if not isinstance(raw, dict) or set(raw) != {"value", "status", "evidence"}:
        raise ContractValidationError(f"invalid {field} identity evidence")
    if raw["value"] != value or raw["status"] not in {"verified", "needs_review"}:
        raise ContractValidationError(f"invalid {field} identity value")
    evidence = raw["evidence"]
    if (
        not isinstance(evidence, list)
        or not evidence
        or any(not isinstance(item, str) or not item.strip() or len(item) > 512 for item in evidence)
    ):
        raise ContractValidationError(f"invalid {field} identity evidence")


def validate_proposal(
    proposal: Any,
    *,
    route: str,
    source_delivery: str,
    pages: tuple[dict[str, str | int], ...],
    expected_source_loading_date: str,
) -> int:
    """Validate full upload coverage, identity, arithmetic, stars, and omissions."""
    required = {
        "route",
        "delivery",
        "loadingDate",
        "identityVerification",
        "deliveryAssignments",
        "confidence",
        "confidenceTier",
        "confidenceReasons",
        "uncertainRows",
        "containers",
    }
    if not isinstance(proposal, dict) or set(proposal) != required:
        raise ContractValidationError("invalid proposal shape")
    if (
        proposal["route"] != route
        or proposal["delivery"] != source_delivery
        or not isinstance(route, str)
        or re.fullmatch(r"^[0-9]{6}$", route) is None
        or not isinstance(source_delivery, str)
        or _DELIVERY_RE.fullmatch(source_delivery) is None
    ):
        raise ContractValidationError("proposal source identity mismatch")
    confidence = proposal["confidence"]
    if type(confidence) is not int or not 0 <= confidence <= 100:
        raise ContractValidationError("invalid confidence")
    expected_tier = "high" if confidence >= 92 else ("medium" if confidence >= 75 else "low")
    if proposal["confidenceTier"] != expected_tier:
        raise ContractValidationError("confidence tier mismatch")
    for field in ("confidenceReasons", "uncertainRows"):
        values = proposal[field]
        if (
            not isinstance(values, list)
            or len(values) > 256
            or any(not isinstance(value, str) or len(value) > 1024 for value in values)
        ):
            raise ContractValidationError(f"invalid {field}")

    top_loading = _calendar_date(proposal["loadingDate"], field="loadingDate")
    manifest_containers = {str(page["containerCode"]) for page in pages}
    assignments = proposal["deliveryAssignments"]
    if not isinstance(assignments, list) or not assignments:
        raise ContractValidationError("delivery assignments are required")
    delivery_to_loading: dict[str, datetime] = {}
    container_to_delivery: dict[str, str] = {}
    assignment_status: dict[str, str] = {}
    for raw in assignments:
        if not isinstance(raw, dict) or set(raw) != {
            "deliveryNumber",
            "loadingDate",
            "containerCodes",
            "status",
            "evidence",
        }:
            raise ContractValidationError("invalid delivery assignment shape")
        delivery = raw["deliveryNumber"]
        if not isinstance(delivery, str) or not _DELIVERY_RE.fullmatch(delivery):
            raise ContractValidationError("invalid assigned delivery")
        if delivery in delivery_to_loading:
            raise ContractValidationError("duplicate assigned delivery")
        delivery_to_loading[delivery] = _calendar_date(raw["loadingDate"], field="assignment loadingDate")
        if raw["status"] not in {"verified", "needs_review"}:
            raise ContractValidationError("invalid assignment status")
        assignment_status[delivery] = raw["status"]
        evidence = raw["evidence"]
        if not isinstance(evidence, list) or not evidence or any(
            not isinstance(item, str) or not item.strip() or len(item) > 512 for item in evidence
        ):
            raise ContractValidationError("invalid assignment evidence")
        codes = raw["containerCodes"]
        if not isinstance(codes, list) or not codes:
            raise ContractValidationError("assignment containers are required")
        for code in codes:
            if not isinstance(code, str) or not _CONTAINER_RE.fullmatch(code):
                raise ContractValidationError("invalid assigned container")
            if code in container_to_delivery:
                raise ContractValidationError("container assigned more than once")
            container_to_delivery[code] = delivery
    if set(container_to_delivery) != manifest_containers:
        raise ContractValidationError("assignments do not exactly cover manifest containers")

    authoritative_delivery = (
        source_delivery if source_delivery in delivery_to_loading else next(iter(delivery_to_loading), "")
    )
    if source_delivery not in delivery_to_loading:
        if (
            len(delivery_to_loading) != 1
            or assignment_status.get(authoritative_delivery) != "verified"
            or set(container_to_delivery) != manifest_containers
        ):
            raise ContractValidationError("unsafe source-free delivery override")
    authoritative_loading = delivery_to_loading[authoritative_delivery]
    if (
        source_delivery in delivery_to_loading
        and authoritative_loading.strftime("%m/%d/%Y") != expected_source_loading_date
    ):
        raise ContractValidationError("proposal loading date differs from frozen queue input")
    identity = proposal["identityVerification"]
    if not isinstance(identity, dict) or set(identity) != {"deliveryNumber", "loadingDate"}:
        raise ContractValidationError("invalid identity verification shape")
    _validate_identity_evidence(
        identity["deliveryNumber"], value=authoritative_delivery, field="delivery"
    )
    _validate_identity_evidence(
        identity["loadingDate"],
        value=authoritative_loading.strftime("%m/%d/%Y"),
        field="loading date",
    )
    if top_loading != authoritative_loading:
        raise ContractValidationError("top-level loading date is not authoritative")

    expected_pages = {(str(page["containerCode"]), int(page["pageNumber"])) for page in pages}
    actual_pages: set[tuple[str, int]] = set()
    containers = proposal["containers"]
    if not isinstance(containers, list) or not containers:
        raise ContractValidationError("proposal containers are required")
    seen_containers: set[str] = set()
    for container in containers:
        if not isinstance(container, dict) or set(container) != {"containerCode", "pages"}:
            raise ContractValidationError("invalid proposal container shape")
        code = container["containerCode"]
        if not isinstance(code, str) or not _CONTAINER_RE.fullmatch(code) or code in seen_containers:
            raise ContractValidationError("invalid or duplicate proposal container")
        seen_containers.add(code)
        loading = delivery_to_loading[container_to_delivery.get(code, "")]
        container_pages = container["pages"]
        if not isinstance(container_pages, list) or not container_pages:
            raise ContractValidationError("proposal pages are required")
        for page in container_pages:
            if not isinstance(page, dict) or set(page) != {"pageNumber", "items"}:
                raise ContractValidationError("invalid proposal page shape")
            page_number = page["pageNumber"]
            if type(page_number) is not int or page_number < 1:
                raise ContractValidationError("invalid or duplicate proposal page")
            key = (code, page_number)
            if key in actual_pages:
                raise ContractValidationError("invalid or duplicate proposal page")
            actual_pages.add(key)
            items = page["items"]
            if not isinstance(items, list):
                raise ContractValidationError("proposal items must be a list")
            for item in items:
                if not isinstance(item, dict) or set(item) != {
                    "product", "bestBefore", "days", "isShortCoded", "description"
                }:
                    raise ContractValidationError("invalid proposal row shape")
                product = item["product"]
                days = item["days"]
                short = item["isShortCoded"]
                description = item["description"]
                if (
                    not isinstance(product, str)
                    or not _PRODUCT_RE.fullmatch(product)
                    or product in OMITTED_SAPS
                ):
                    raise ContractValidationError("invalid or omitted SAP")
                if not isinstance(days, str) or not _DAYS_RE.fullmatch(days):
                    raise ContractValidationError("invalid Days value")
                if type(short) is not bool or short != days.endswith("*"):
                    raise ContractValidationError("short-code star mismatch")
                if not isinstance(description, str) or not description.strip() or len(description) > 512:
                    raise ContractValidationError("invalid row description")
                best_before = item["bestBefore"]
                if best_before == "":
                    if days != "0" or short:
                        raise ContractValidationError("blank Best Before requires unstarred Days 0")
                else:
                    expiry = _calendar_date(best_before, field="bestBefore")
                    if (expiry - loading).days != int(days.rstrip("*")):
                        raise ContractValidationError("Best Before/Days arithmetic mismatch")
    if seen_containers != manifest_containers or actual_pages != expected_pages:
        raise ContractValidationError("proposal does not exactly cover manifest pages")
    return confidence


def validate_evidence_package(
    evidence: EvidencePackage,
    *,
    route: str,
    delivery: str,
    input_hash: str,
    pages: tuple[dict[str, str | int], ...],
) -> None:
    if (
        evidence.route != route
        or evidence.delivery != delivery
        or evidence.input_hash != input_hash
        or not _HEX_RE.fullmatch(evidence.manifest_sha256)
        or not _safe_relative_path(evidence.manifest_relative_path)
    ):
        raise ContractValidationError("evidence package identity mismatch")
    roles: dict[str, list[EvidenceArtifact]] = {}
    paths: set[str] = set()
    for artifact in evidence.artifacts:
        if not _safe_relative_path(artifact.relative_path) or artifact.relative_path in paths:
            raise ContractValidationError("unsafe or duplicate evidence artifact path")
        paths.add(artifact.relative_path)
        if not _HEX_RE.fullmatch(artifact.sha256):
            raise ContractValidationError("invalid evidence artifact hash")
        roles.setdefault(artifact.role, []).append(artifact)
        if artifact.role == "source_image":
            if not artifact.object_path or not artifact.generation:
                raise ContractValidationError("source image evidence lacks object identity")
        elif artifact.object_path is not None or artifact.generation is not None:
            raise ContractValidationError("non-image evidence cannot select a Storage object")
    for singleton in ("before_payload", "proposed_result", "package_manifest"):
        if len(roles.get(singleton, [])) != 1:
            raise ContractValidationError(f"evidence requires exactly one {singleton}")
    package_manifest = roles["package_manifest"][0]
    if (
        package_manifest.relative_path != evidence.manifest_relative_path
        or package_manifest.sha256 != evidence.manifest_sha256
    ):
        raise ContractValidationError("evidence manifest reference mismatch")
    expected_objects = {
        (str(page["storagePath"]), str(page["storageGeneration"])) for page in pages
    }
    observed_objects = {
        (str(item.object_path), str(item.generation)) for item in roles.get("source_image", [])
    }
    if observed_objects != expected_objects or len(roles.get("source_image", [])) != len(pages):
        raise ContractValidationError("source image evidence does not exactly cover manifest")


def canonical_result_identity(raw: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Hash a semantic result without retaining the opaque claim token."""
    retained = dict(raw)
    retained.pop("claimToken", None)
    encoded = json.dumps(retained, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest(), retained


def validate_result_for_work(
    request: ResultRequest,
    *,
    route: str,
    delivery: str,
    input_hash: str,
    manifest: dict[str, Any],
    ocr_context: dict[str, Any],
    expected_bucket: str,
) -> int:
    if str(request.work_id) == "00000000-0000-0000-0000-000000000000":
        raise ContractValidationError("invalid work ID")
    if request.worker_id and not _WORKER_RE.fullmatch(request.worker_id):
        raise ContractValidationError("invalid worker ID")
    if not _HEX_RE.fullmatch(request.input_hash) or request.input_hash != input_hash:
        raise ContractValidationError("input hash mismatch")
    pages = manifest_pages(
        manifest,
        route=route,
        delivery=delivery,
        expected_bucket=expected_bucket,
    )
    try:
        normalized_ocr_context = validate_pcf_ocr_context(
            ocr_context,
            manifest_containers={str(page["containerCode"]) for page in pages},
        )
    except EventContractError as exc:
        raise ContractValidationError("invalid frozen OCR context") from exc
    confidence = validate_proposal(
        request.proposal,
        route=route,
        source_delivery=delivery,
        pages=pages,
        expected_source_loading_date=normalized_ocr_context["validatedLoadingDate"],
    )
    validate_evidence_package(
        request.evidence_package,
        route=route,
        delivery=delivery,
        input_hash=input_hash,
        pages=pages,
    )
    return confidence
