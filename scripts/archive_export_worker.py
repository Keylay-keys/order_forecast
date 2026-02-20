#!/usr/bin/env python3
"""Archive export background worker.

Polls Firestore archiveExports queue docs, claims queued jobs, builds export zip
artifacts from archived PCF data/images, uploads to Firebase Storage, and updates
job status lifecycle.

Usage:
    python scripts/archive_export_worker.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import shutil
import socket
import tempfile
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from firebase_admin import credentials, firestore as admin_firestore, initialize_app, storage
from google.cloud import firestore as gcf_firestore


EXPORT_COLLECTION = "archiveExports"
EXPORT_LOCK_COLLECTION = "archiveExportLocks"
WORKER_ID = f"archive-export-{socket.gethostname()}-{os.getpid()}"

POLL_SECONDS = int(os.environ.get("ARCHIVE_EXPORT_POLL_SECONDS", "30"))
WORKER_TIMEOUT_SECONDS = int(os.environ.get("ARCHIVE_EXPORT_WORKER_TIMEOUT_SECONDS", "2700"))  # 45m
HEARTBEAT_SECONDS = int(os.environ.get("ARCHIVE_EXPORT_HEARTBEAT_SECONDS", "30"))
MAX_GLOBAL_CONCURRENCY = int(os.environ.get("ARCHIVE_EXPORT_WORKER_CONCURRENCY", "3"))
HDD_ARCHIVE_BASE = Path(os.environ.get("PCF_ARCHIVE_PATH", "/mnt/archive/pcf/pcf_archive"))
ARTIFACT_TTL_DAYS = int(os.environ.get("ARCHIVE_EXPORT_ARTIFACT_TTL_DAYS", "14"))

logger = logging.getLogger("archive_export_worker")


@dataclass
class DeliveryEntry:
    delivery_number: str
    source: str  # firebase|hdd
    created_at: Optional[str]


class WorkerError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = True):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1000)
    if hasattr(value, "timestamp"):
        try:
            return int(value.timestamp() * 1000)
        except Exception:
            return None
    if isinstance(value, dict):
        seconds = value.get("seconds") or value.get("_seconds")
        nanos = value.get("nanoseconds") or value.get("_nanoseconds") or value.get("nanos") or 0
        if seconds is None:
            return None
        try:
            return int((float(seconds) + float(nanos) / 1_000_000_000) * 1000)
        except Exception:
            return None
    return None


def _normalize_iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if hasattr(value, "date"):
        try:
            return value.date().isoformat()
        except Exception:
            pass
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return s
        # Firestore stringified timestamps often begin with yyyy-mm-dd
        if len(s) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", s[:10]):
            return s[:10]
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ensure_firebase(sa_path: str) -> None:
    bucket_name = str(os.environ.get("FIREBASE_STORAGE_BUCKET") or "").strip()
    options: Dict[str, Any] = {}
    if bucket_name:
        options["storageBucket"] = bucket_name
    try:
        initialize_app(credentials.Certificate(sa_path), options=options or None)
    except ValueError:
        # Already initialized.
        pass


def _parse_date_or_raise(value: Any, field_name: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise WorkerError("INVALID_DATE_RANGE", f"Invalid {field_name}: {value}", retryable=False) from exc
    raise WorkerError("INVALID_DATE_RANGE", f"Missing {field_name}", retryable=False)


def _retry_delay_seconds(attempt_count: int) -> int:
    # Exponential backoff: 60, 120, 240 ... capped at 1800s
    return min(60 * (2 ** max(attempt_count - 1, 0)), 1800)


def _stale_threshold_seconds() -> int:
    return min(10 * 60, max(WORKER_TIMEOUT_SECONDS - 60, 60))


def _lock_ttl_seconds() -> int:
    # Keep lock alive a bit longer than worker timeout to cover jitter.
    return max(WORKER_TIMEOUT_SECONDS + 120, 15 * 60)


def _build_export_filename(route: str, from_date: str, to_date: str) -> str:
    return f"pcf_export_{route}_{from_date}_{to_date}_part01.zip"


def _bucket() -> storage.bucket:
    bucket_name = str(os.environ.get("FIREBASE_STORAGE_BUCKET") or "").strip()
    if bucket_name:
        return storage.bucket(name=bucket_name)
    bucket = storage.bucket()
    if not getattr(bucket, "name", None):
        raise WorkerError(
            "STORAGE_BUCKET_NOT_CONFIGURED",
            "Set FIREBASE_STORAGE_BUCKET for archive export worker",
            retryable=False,
        )
    return bucket


def _list_active_delivery_ids(db: admin_firestore.client, route_number: str) -> set[str]:
    active = set()
    docs = db.collection("routes").document(route_number).collection("pcfs").select([]).stream()
    for doc in docs:
        active.add(doc.id)
    return active


def _find_latest_hdd_run(route_number: str, delivery_number: str) -> Optional[Path]:
    delivery_dir = HDD_ARCHIVE_BASE / route_number / delivery_number
    if not delivery_dir.exists() or not delivery_dir.is_dir():
        return None
    runs = [p for p in delivery_dir.iterdir() if p.is_dir()]
    if not runs:
        return None
    # Run folders are timestamp strings (YYYYMMDD_hhmmss); lexical sort works.
    return sorted(runs, key=lambda p: p.name, reverse=True)[0]


def _hdd_run_date(run_path: Path) -> Optional[str]:
    name = run_path.name
    if len(name) >= 8 and name[:8].isdigit():
        try:
            return datetime.strptime(name[:8], "%Y%m%d").date().isoformat()
        except ValueError:
            return None
    return None


def _collect_deliveries_in_range(
    *,
    db: admin_firestore.client,
    route_number: str,
    from_date: date,
    to_date: date,
) -> List[DeliveryEntry]:
    active_ids = _list_active_delivery_ids(db, route_number)

    seen: Dict[str, DeliveryEntry] = {}

    # Source 1: Firestore archivedPCFs
    arch_ref = db.collection("routes").document(route_number).collection("archivedPCFs")
    for doc in arch_ref.stream():
        data = doc.to_dict() or {}
        created = _normalize_iso_date(data.get("createdAt"))
        if not created:
            continue
        try:
            created_date = date.fromisoformat(created)
        except ValueError:
            continue
        if created_date < from_date or created_date > to_date:
            continue
        seen[doc.id] = DeliveryEntry(delivery_number=doc.id, source="firebase", created_at=created)

    # Source 2: HDD (skip if Firestore already has it or if still active)
    route_dir = HDD_ARCHIVE_BASE / route_number
    if route_dir.exists() and route_dir.is_dir():
        for item in route_dir.iterdir():
            if not item.is_dir():
                continue
            delivery_number = item.name
            if delivery_number in seen:
                continue
            if delivery_number in active_ids:
                continue
            run_path = _find_latest_hdd_run(route_number, delivery_number)
            if not run_path:
                continue
            created = _hdd_run_date(run_path)
            if not created:
                continue
            try:
                created_date = date.fromisoformat(created)
            except ValueError:
                continue
            if created_date < from_date or created_date > to_date:
                continue
            seen[delivery_number] = DeliveryEntry(delivery_number=delivery_number, source="hdd", created_at=created)

    return sorted(
        seen.values(),
        key=lambda d: d.created_at or "0000-00-00",
        reverse=True,
    )


def _load_firestore_delivery_detail(
    *,
    db: admin_firestore.client,
    route_number: str,
    delivery_number: str,
) -> Optional[Dict[str, Any]]:
    doc_ref = (
        db.collection("routes")
        .document(route_number)
        .collection("archivedPCFs")
        .document(delivery_number)
    )
    doc = doc_ref.get()
    if not doc.exists:
        return None

    data = doc.to_dict() or {}
    containers: List[Dict[str, Any]] = []
    for cdoc in doc_ref.collection("containers").stream():
        cdata = cdoc.to_dict() or {}
        cdata["containerCode"] = cdoc.id
        containers.append(cdata)

    data["deliveryNumber"] = delivery_number
    data["containers"] = containers
    data["source"] = "firebase"
    return data


def _load_hdd_delivery_detail(route_number: str, delivery_number: str) -> Optional[Dict[str, Any]]:
    run_path = _find_latest_hdd_run(route_number, delivery_number)
    if not run_path:
        return None

    fs_path = run_path / "firestore" / "delivery.json"
    if not fs_path.exists():
        return None

    try:
        data = json.loads(fs_path.read_text())
    except Exception:
        return None

    containers: List[Dict[str, Any]] = []
    containers_dir = run_path / "firestore" / "containers"
    if containers_dir.exists() and containers_dir.is_dir():
        for file_path in sorted(containers_dir.glob("*.json")):
            try:
                containers.append(json.loads(file_path.read_text()))
            except Exception:
                continue

    data["deliveryNumber"] = delivery_number
    data["containers"] = containers
    data["source"] = "hdd"
    data["_runPath"] = str(run_path)
    return data


def _download_firebase_container_images(
    *,
    route_number: str,
    delivery_number: str,
    container_code: str,
    target_dir: Path,
) -> List[Dict[str, Any]]:
    target_dir.mkdir(parents=True, exist_ok=True)
    bucket = _bucket()
    prefix = f"routes/{route_number}/archivedPCFs/{delivery_number}/{container_code}/"

    images: List[Dict[str, Any]] = []
    blobs = sorted(bucket.list_blobs(prefix=prefix), key=lambda b: b.name)
    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        filename = Path(blob.name).name
        local_path = target_dir / filename
        blob.download_to_filename(str(local_path))
        stat = local_path.stat()
        images.append(
            {
                "filename": filename,
                "sha256": _sha256_file(local_path),
                "bytes": stat.st_size,
                "path": str(local_path),
            }
        )

    return images


def _copy_hdd_images(
    *,
    run_path: Path,
    delivery_number: str,
    target_delivery_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    images_dir = run_path / "images"
    if not images_dir.exists() or not images_dir.is_dir():
        return [], [f"Delivery {delivery_number} has no HDD images directory"]

    copied: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # Expected naming from pipeline: {delivery}_{container}_page_{page}.jpg
    pattern = re.compile(rf"^{re.escape(delivery_number)}_(.+?)_page_(\d+)\.(jpg|jpeg|png)$", re.IGNORECASE)

    for src in sorted(images_dir.iterdir()):
        if not src.is_file():
            continue
        m = pattern.match(src.name)
        if not m:
            target = target_delivery_dir / "containers" / "unknown" / src.name
        else:
            container_code = m.group(1)
            target = target_delivery_dir / "containers" / container_code / src.name

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, target)
        copied.append(
            {
                "filename": target.name,
                "sha256": _sha256_file(target),
                "bytes": target.stat().st_size,
                "path": str(target),
            }
        )

    if not copied:
        warnings.append(f"Delivery {delivery_number} has zero HDD images")

    return copied, warnings


def _zip_directory(source_dir: Path, dest_zip: Path) -> None:
    with zipfile.ZipFile(dest_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in source_dir.rglob("*"):
            if not file_path.is_file():
                continue
            arcname = file_path.relative_to(source_dir)
            zf.write(file_path, arcname.as_posix())


def _build_export_artifact(
    *,
    db: admin_firestore.client,
    route_number: str,
    export_id: str,
    from_date: date,
    to_date: date,
    heartbeat_cb,
) -> Dict[str, Any]:
    deliveries = _collect_deliveries_in_range(
        db=db,
        route_number=route_number,
        from_date=from_date,
        to_date=to_date,
    )
    if not deliveries:
        raise WorkerError("NO_ARCHIVE_DATA_IN_RANGE", "No archived PCF data found in requested range", retryable=False)

    export_root_name = f"route_{route_number}_pcf_export_{from_date.isoformat()}_{to_date.isoformat()}"

    with tempfile.TemporaryDirectory(prefix=f"archive_export_{export_id}_") as tmp:
        tmp_path = Path(tmp)
        root_dir = tmp_path / export_root_name
        deliveries_dir = root_dir / "deliveries"
        deliveries_dir.mkdir(parents=True, exist_ok=True)

        manifest: Dict[str, Any] = {
            "exportId": export_id,
            "routeNumber": route_number,
            "dateRange": {"from": from_date.isoformat(), "to": to_date.isoformat()},
            "status": "ready",
            "totalDeliveriesRequested": len(deliveries),
            "totalDeliveriesExported": 0,
            "deliveries": [],
            "warnings": [],
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

        for entry in deliveries:
            heartbeat_cb()

            delivery_dir = deliveries_dir / entry.delivery_number
            metadata_dir = delivery_dir / "metadata"
            metadata_dir.mkdir(parents=True, exist_ok=True)

            detail: Optional[Dict[str, Any]]
            warnings: List[str] = []

            if entry.source == "firebase":
                detail = _load_firestore_delivery_detail(
                    db=db,
                    route_number=route_number,
                    delivery_number=entry.delivery_number,
                )
            else:
                detail = _load_hdd_delivery_detail(route_number, entry.delivery_number)

            if not detail:
                warning = f"Delivery {entry.delivery_number} missing from {entry.source} archive source"
                manifest["warnings"].append(warning)
                continue

            (metadata_dir / "delivery.json").write_text(json.dumps(detail, indent=2, default=str))

            container_rows: List[Dict[str, Any]] = []
            containers = detail.get("containers") if isinstance(detail.get("containers"), list) else []

            if entry.source == "firebase":
                for container in containers:
                    heartbeat_cb()
                    container_code = str(
                        container.get("containerCode")
                        or container.get("containerId")
                        or container.get("code")
                        or "unknown"
                    )
                    target_container_dir = delivery_dir / "containers" / container_code
                    images = _download_firebase_container_images(
                        route_number=route_number,
                        delivery_number=entry.delivery_number,
                        container_code=container_code,
                        target_dir=target_container_dir,
                    )
                    if not images:
                        warnings.append(f"Delivery {entry.delivery_number} container {container_code} has no archived images")
                    container_rows.append(
                        {
                            "containerCode": container_code,
                            "images": [{"filename": i["filename"], "sha256": i["sha256"], "bytes": i["bytes"]} for i in images],
                        }
                    )
            else:
                run_path_raw = detail.get("_runPath")
                run_path = Path(str(run_path_raw)) if run_path_raw else None
                if run_path and run_path.exists():
                    copied, copy_warnings = _copy_hdd_images(
                        run_path=run_path,
                        delivery_number=entry.delivery_number,
                        target_delivery_dir=delivery_dir,
                    )
                    warnings.extend(copy_warnings)
                    # Approximate container rows by parsing copied files.
                    by_container: Dict[str, List[Dict[str, Any]]] = {}
                    for img in copied:
                        m = re.match(rf"^{re.escape(entry.delivery_number)}_(.+?)_page_\d+", img["filename"], re.IGNORECASE)
                        key = m.group(1) if m else "unknown"
                        by_container.setdefault(key, []).append(
                            {"filename": img["filename"], "sha256": img["sha256"], "bytes": img["bytes"]}
                        )
                    container_rows = [
                        {"containerCode": code, "images": rows}
                        for code, rows in sorted(by_container.items(), key=lambda x: x[0])
                    ]
                else:
                    warnings.append(f"Delivery {entry.delivery_number} HDD run path missing")

            manifest["deliveries"].append(
                {
                    "deliveryNumber": entry.delivery_number,
                    "source": entry.source,
                    "containers": container_rows,
                    "warnings": warnings,
                }
            )
            if warnings:
                manifest["warnings"].extend(warnings)

        manifest["totalDeliveriesExported"] = len(manifest["deliveries"])
        if manifest["totalDeliveriesExported"] == 0:
            raise WorkerError("NO_ARCHIVE_DATA_IN_RANGE", "Requested range contained no exportable deliveries", retryable=False)

        if manifest["warnings"]:
            manifest["status"] = "ready_partial"

        (root_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))

        zip_name = _build_export_filename(route_number, from_date.isoformat(), to_date.isoformat())
        zip_path = tmp_path / zip_name
        _zip_directory(root_dir, zip_path)

        bucket = _bucket()
        blob_path = f"exports/pcf/{route_number}/{export_id}/{zip_name}"
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(str(zip_path), content_type="application/zip")

        zip_size = zip_path.stat().st_size
        return {
            "status": manifest["status"],
            "blobPath": blob_path,
            "parts": [zip_name],
            "sizeBytes": zip_size,
            "warnings": manifest["warnings"],
            "requested": manifest["totalDeliveriesRequested"],
            "exported": manifest["totalDeliveriesExported"],
        }


def _update_job_heartbeat(job_ref: admin_firestore.DocumentReference) -> None:
    job_ref.update(
        {
            "workerHeartbeatAt": admin_firestore.SERVER_TIMESTAMP,
            "updatedAt": admin_firestore.SERVER_TIMESTAMP,
        }
    )


def _upsert_route_lock(
    db: admin_firestore.client,
    *,
    route_number: str,
    export_id: str,
) -> None:
    lock_ref = db.collection(EXPORT_LOCK_COLLECTION).document(route_number)
    locked_until = datetime.now(timezone.utc) + timedelta(seconds=_lock_ttl_seconds())
    lock_ref.set(
        {
            "routeNumber": route_number,
            "exportId": export_id,
            "lockedByWorker": WORKER_ID,
            "lockedAt": admin_firestore.SERVER_TIMESTAMP,
            "lockedUntil": locked_until,
            "updatedAt": admin_firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def _clear_route_lock(
    db: admin_firestore.client,
    *,
    route_number: str,
    export_id: str,
) -> None:
    lock_ref = db.collection(EXPORT_LOCK_COLLECTION).document(route_number)
    tx = db.transaction()

    @gcf_firestore.transactional
    def _delete_if_owned(transaction, doc_ref):
        snap = doc_ref.get(transaction=transaction)
        if not snap.exists:
            return
        data = snap.to_dict() or {}
        current_export = str(data.get("exportId") or "").strip()
        if current_export and current_export != export_id:
            return
        transaction.delete(doc_ref)

    try:
        _delete_if_owned(tx, lock_ref)
    except Exception as exc:
        logger.warning("Failed clearing route lock route=%s export=%s: %s", route_number, export_id, exc)


def _finalize_job_success(
    *,
    job_ref: admin_firestore.DocumentReference,
    result: Dict[str, Any],
) -> None:
    expires_at_dt = datetime.now(timezone.utc) + timedelta(days=ARTIFACT_TTL_DAYS)
    expires_at_ms = int(expires_at_dt.timestamp() * 1000)

    job_ref.update(
        {
            "status": result["status"],
            "readyAt": admin_firestore.SERVER_TIMESTAMP,
            "updatedAt": admin_firestore.SERVER_TIMESTAMP,
            "workerHeartbeatAt": admin_firestore.SERVER_TIMESTAMP,
            "artifactStoragePath": result["blobPath"],
            "artifactExpiresAtMs": expires_at_ms,
            "artifact": {
                "storagePath": result["blobPath"],
                "parts": result["parts"],
                "expiresAt": expires_at_dt,
                "sizeBytes": result["sizeBytes"],
            },
            "result": {
                "warningCount": len(result.get("warnings") or []),
                "warnings": result.get("warnings") or [],
                "totalDeliveriesRequested": result.get("requested") or 0,
                "totalDeliveriesExported": result.get("exported") or 0,
            },
            "errorCode": None,
            "errorMessage": None,
            "retryAfterMs": None,
        }
    )


def _finalize_job_failure(
    *,
    job_ref: admin_firestore.DocumentReference,
    current_data: Dict[str, Any],
    error_code: str,
    error_message: str,
    retryable: bool,
) -> None:
    attempt_count = int(current_data.get("attemptCount") or 0) + 1
    max_attempts = int(current_data.get("maxAttempts") or 3)

    if retryable and attempt_count < max_attempts:
        delay_seconds = _retry_delay_seconds(attempt_count)
        retry_after_ms = int((datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).timestamp() * 1000)
        job_ref.update(
            {
                "status": "queued",
                "attemptCount": attempt_count,
                "retryAfterMs": retry_after_ms,
                "updatedAt": admin_firestore.SERVER_TIMESTAMP,
                "workerHeartbeatAt": admin_firestore.SERVER_TIMESTAMP,
                "errorCode": error_code,
                "errorMessage": error_message[:1000],
            }
        )
        logger.warning(
            "Re-queued export %s after failure (%s): attempt=%s/%s retry_in=%ss",
            job_ref.id,
            error_code,
            attempt_count,
            max_attempts,
            delay_seconds,
        )
        return

    job_ref.update(
        {
            "status": "failed",
            "attemptCount": attempt_count,
            "updatedAt": admin_firestore.SERVER_TIMESTAMP,
            "workerHeartbeatAt": admin_firestore.SERVER_TIMESTAMP,
            "errorCode": error_code,
            "errorMessage": error_message[:1000],
        }
    )
    logger.error(
        "Export %s failed permanently (%s): %s",
        job_ref.id,
        error_code,
        error_message,
    )


def _recover_stale_processing_jobs(db: admin_firestore.client) -> None:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    stale_threshold_ms = _stale_threshold_seconds() * 1000
    timeout_ms = WORKER_TIMEOUT_SECONDS * 1000

    docs = db.collection(EXPORT_COLLECTION).where("status", "==", "processing").stream()
    for doc in docs:
        data = doc.to_dict() or {}
        started_ms = _to_epoch_ms(data.get("startedAt")) or _to_epoch_ms(data.get("updatedAt"))
        heartbeat_ms = _to_epoch_ms(data.get("workerHeartbeatAt")) or started_ms
        if not started_ms:
            started_ms = now_ms
        if not heartbeat_ms:
            heartbeat_ms = now_ms

        is_stale = (now_ms - heartbeat_ms) > stale_threshold_ms
        timed_out = (now_ms - started_ms) > timeout_ms
        if not is_stale and not timed_out:
            continue

        job_ref = doc.reference
        reason_code = "WORKER_TIMEOUT" if timed_out else "STALE_PROCESSING_JOB"
        reason_message = "Processing job timed out" if timed_out else "Processing job became stale"
        _finalize_job_failure(
            job_ref=job_ref,
            current_data=data,
            error_code=reason_code,
            error_message=reason_message,
            retryable=True,
        )
        route_number = str(data.get("routeNumber") or "").strip()
        if route_number:
            _clear_route_lock(db, route_number=route_number, export_id=doc.id)


def _count_processing_jobs(db: admin_firestore.client) -> int:
    return sum(1 for _ in db.collection(EXPORT_COLLECTION).where("status", "==", "processing").stream())


def _route_has_processing_job(db: admin_firestore.client, route_number: str) -> bool:
    docs = (
        db.collection(EXPORT_COLLECTION)
        .where("routeNumber", "==", route_number)
        .where("status", "==", "processing")
        .limit(1)
        .stream()
    )
    return any(True for _ in docs)


def _claim_one_queued_job(db: admin_firestore.client) -> Optional[admin_firestore.DocumentSnapshot]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if _count_processing_jobs(db) >= MAX_GLOBAL_CONCURRENCY:
        return None

    docs = list(db.collection(EXPORT_COLLECTION).where("status", "==", "queued").stream())
    docs.sort(key=lambda d: _to_epoch_ms((d.to_dict() or {}).get("createdAt")) or 0)

    for doc in docs:
        data = doc.to_dict() or {}
        route_number = str(data.get("routeNumber") or "").strip()
        if not route_number:
            continue
        retry_after_ms = data.get("retryAfterMs") if isinstance(data.get("retryAfterMs"), int) else None
        if retry_after_ms is not None and now_ms < retry_after_ms:
            continue
        if _route_has_processing_job(db, route_number):
            continue

        tx = db.transaction()

        @gcf_firestore.transactional
        def _claim(transaction, doc_ref):
            snap = doc_ref.get(transaction=transaction)
            snap_data = snap.to_dict() or {}
            if str(snap_data.get("status") or "").strip().lower() != "queued":
                return False
            retry_after_inner = snap_data.get("retryAfterMs") if isinstance(snap_data.get("retryAfterMs"), int) else None
            inner_now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            if retry_after_inner is not None and inner_now_ms < retry_after_inner:
                return False

            transaction.update(
                doc_ref,
                {
                    "status": "processing",
                    "claimedBy": WORKER_ID,
                    "startedAt": admin_firestore.SERVER_TIMESTAMP,
                    "workerHeartbeatAt": admin_firestore.SERVER_TIMESTAMP,
                    "updatedAt": admin_firestore.SERVER_TIMESTAMP,
                },
            )
            return True

        try:
            ok = _claim(tx, doc.reference)
            if not ok:
                continue
            claimed = doc.reference.get()
            return claimed
        except Exception as exc:
            logger.warning("Failed to claim export %s: %s", doc.id, exc)
            continue

    return None


def _process_job(db: admin_firestore.client, doc: admin_firestore.DocumentSnapshot) -> None:
    data = doc.to_dict() or {}
    job_ref = doc.reference

    route_number = str(data.get("routeNumber") or "").strip()
    if not route_number:
        _finalize_job_failure(
            job_ref=job_ref,
            current_data=data,
            error_code="EXPORT_ROUTE_INVALID",
            error_message="Missing routeNumber",
            retryable=False,
        )
        return

    _upsert_route_lock(db, route_number=route_number, export_id=doc.id)

    try:
        from_date = _parse_date_or_raise(data.get("fromDate"), "fromDate")
        to_date = _parse_date_or_raise(data.get("toDate"), "toDate")
        if from_date > to_date:
            raise WorkerError("INVALID_DATE_RANGE", "fromDate is after toDate", retryable=False)

        logger.info(
            "Processing export %s route=%s range=%s..%s",
            doc.id,
            route_number,
            from_date.isoformat(),
            to_date.isoformat(),
        )

        last_heartbeat = 0.0

        def heartbeat_cb() -> None:
            nonlocal last_heartbeat
            now = time.time()
            if now - last_heartbeat >= HEARTBEAT_SECONDS:
                _update_job_heartbeat(job_ref)
                _upsert_route_lock(db, route_number=route_number, export_id=doc.id)
                last_heartbeat = now

        heartbeat_cb()
        result = _build_export_artifact(
            db=db,
            route_number=route_number,
            export_id=doc.id,
            from_date=from_date,
            to_date=to_date,
            heartbeat_cb=heartbeat_cb,
        )
        heartbeat_cb()
        _finalize_job_success(job_ref=job_ref, result=result)
        logger.info("Export %s completed status=%s", doc.id, result["status"])

    except WorkerError as exc:
        _finalize_job_failure(
            job_ref=job_ref,
            current_data=data,
            error_code=exc.code,
            error_message=exc.message,
            retryable=exc.retryable,
        )
    except Exception as exc:
        logger.exception("Unexpected export error for %s", doc.id)
        _finalize_job_failure(
            job_ref=job_ref,
            current_data=data,
            error_code="EXPORT_PROCESSING_ERROR",
            error_message=str(exc),
            retryable=True,
        )
    finally:
        _clear_route_lock(db, route_number=route_number, export_id=doc.id)


def run_worker(sa_path: str, *, poll_seconds: int, run_once: bool = False) -> None:
    _ensure_firebase(sa_path)
    db = admin_firestore.client()

    logger.info("Archive export worker started: worker=%s poll=%ss", WORKER_ID, poll_seconds)

    while True:
        try:
            _recover_stale_processing_jobs(db)
            claimed = _claim_one_queued_job(db)
            if claimed is None:
                if run_once:
                    return
                time.sleep(poll_seconds)
                continue

            _process_job(db, claimed)
            if run_once:
                return

        except KeyboardInterrupt:
            logger.info("Archive export worker interrupted, shutting down")
            return
        except Exception:
            logger.exception("Archive export worker loop failure")
            if run_once:
                raise
            time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive export queue worker")
    parser.add_argument(
        "--serviceAccount",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        help="Path to Firebase service account JSON",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=POLL_SECONDS,
        help="Queue polling interval in seconds",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll/claim/process cycle and exit",
    )
    return parser.parse_args()


def main() -> int:
    configure_logging()
    args = parse_args()

    sa_path = str(args.serviceAccount or "").strip()
    if not sa_path:
        logger.error("--serviceAccount (or GOOGLE_APPLICATION_CREDENTIALS) is required")
        return 2
    if not Path(sa_path).exists():
        logger.error("Service account path does not exist: %s", sa_path)
        return 2

    run_worker(sa_path, poll_seconds=max(args.poll_seconds, 5), run_once=bool(args.once))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
