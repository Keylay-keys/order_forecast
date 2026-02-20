#!/usr/bin/env python3
"""Archive purge + artifact-expiry worker.

Responsibilities:
1) Purge archived PCF deliveries older than retention policy (default 90 days)
   while respecting active export locks.
2) Write auditable purge events and per-delivery checkpoints.
3) Expire old export artifacts from Firebase Storage.

Usage:
    python scripts/archive_purge_worker.py --serviceAccount /path/to/sa.json
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import socket
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from firebase_admin import credentials, firestore as admin_firestore, initialize_app, storage


WORKER_ID = f"archive-purge-{socket.gethostname()}-{os.getpid()}"
EXPORT_COLLECTION = "archiveExports"
EXPORT_LOCK_COLLECTION = "archiveExportLocks"
PURGE_EVENT_COLLECTION = "archivePurgeEvents"
PURGE_CHECKPOINT_COLLECTION = "archivePurgeCheckpoints"

POLL_SECONDS = int(os.environ.get("ARCHIVE_PURGE_POLL_SECONDS", "300"))
MAX_DELIVERIES_PER_ROUTE_PER_CYCLE = int(os.environ.get("ARCHIVE_PURGE_ROUTE_BATCH_LIMIT", "50"))
DEFAULT_RETENTION_DAYS = int(os.environ.get("ARCHIVE_PURGE_RETENTION_DAYS_DEFAULT", "90"))
ARCHIVE_PURGE_ENABLED = os.environ.get("ARCHIVE_PURGE_ENABLED", "false").lower() == "true"
HDD_ARCHIVE_BASE = Path(os.environ.get("PCF_ARCHIVE_PATH", "/mnt/archive/pcf/pcf_archive"))

logger = logging.getLogger("archive_purge_worker")


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


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
        text = value.strip()
        if len(text) >= 10:
            prefix = text[:10]
            try:
                date.fromisoformat(prefix)
                return prefix
            except ValueError:
                return None
    return None


def _ensure_firebase(sa_path: str) -> None:
    bucket_name = str(os.environ.get("FIREBASE_STORAGE_BUCKET") or "").strip()
    options: Dict[str, Any] = {}
    if bucket_name:
        options["storageBucket"] = bucket_name
    try:
        initialize_app(credentials.Certificate(sa_path), options=options or None)
    except ValueError:
        pass


def _bucket() -> storage.bucket:
    bucket_name = str(os.environ.get("FIREBASE_STORAGE_BUCKET") or "").strip()
    if bucket_name:
        return storage.bucket(name=bucket_name)
    bucket = storage.bucket()
    if not getattr(bucket, "name", None):
        raise RuntimeError("FIREBASE_STORAGE_BUCKET is required for purge worker")
    return bucket


def _latest_hdd_run_date(route_number: str, delivery_number: str) -> Optional[date]:
    delivery_dir = HDD_ARCHIVE_BASE / route_number / delivery_number
    if not delivery_dir.exists() or not delivery_dir.is_dir():
        return None
    runs = [p for p in delivery_dir.iterdir() if p.is_dir()]
    if not runs:
        return None
    latest = sorted(runs, key=lambda p: p.name, reverse=True)[0]
    if len(latest.name) < 8 or not latest.name[:8].isdigit():
        return None
    try:
        return datetime.strptime(latest.name[:8], "%Y%m%d").date()
    except ValueError:
        return None


def _is_route_locked(db: admin_firestore.client, route_number: str) -> bool:
    lock_ref = db.collection(EXPORT_LOCK_COLLECTION).document(route_number)
    lock_doc = lock_ref.get()
    if not lock_doc.exists:
        return False

    data = lock_doc.to_dict() or {}
    locked_until_ms = _to_epoch_ms(data.get("lockedUntil"))
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    if locked_until_ms is None or locked_until_ms <= now_ms:
        # Stale/expired lock; clear to avoid indefinite blocks.
        try:
            lock_ref.delete()
        except Exception as exc:
            logger.warning("Failed deleting stale route lock for %s: %s", route_number, exc)
        return False

    return True


def _active_delivery_ids(db: admin_firestore.client, route_number: str) -> Set[str]:
    refs = db.collection("routes").document(route_number).collection("pcfs").select([]).stream()
    return {doc.id for doc in refs}


def _retention_days_for_route(db: admin_firestore.client, route_number: str, route_data: Dict[str, Any]) -> int:
    owner_uid = str(route_data.get("ownerUid") or route_data.get("userId") or "").strip()
    if not owner_uid:
        return DEFAULT_RETENTION_DAYS

    owner_doc = db.collection("users").document(owner_uid).get()
    if not owner_doc.exists:
        return DEFAULT_RETENTION_DAYS

    owner = owner_doc.to_dict() or {}
    user_settings = owner.get("userSettings") if isinstance(owner.get("userSettings"), dict) else {}
    database = user_settings.get("database") if isinstance(user_settings.get("database"), dict) else {}
    raw = database.get("archivedPcfRetentionDays")
    try:
        days = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_RETENTION_DAYS

    # V1 bounds from plan.
    if days < 30 or days > 120:
        return DEFAULT_RETENTION_DAYS
    return days


def _archive_anchor_date_for_firestore_doc(data: Dict[str, Any]) -> Optional[date]:
    """Return the retention anchor date for an archived Firestore delivery.

    Prefer archive timestamp (true "time-in-archive" retention), then fall back to
    legacy fields for backward compatibility.
    """
    archive_meta = data.get("_archive") if isinstance(data.get("_archive"), dict) else {}

    archived = _normalize_iso_date(archive_meta.get("archivedAt"))
    if not archived:
        archived = _normalize_iso_date(data.get("archivedAt"))
    if archived:
        try:
            return date.fromisoformat(archived)
        except ValueError:
            pass

    created = _normalize_iso_date(data.get("createdAt"))
    if created:
        try:
            return date.fromisoformat(created)
        except ValueError:
            pass

    return None


def _delete_firestore_delivery(db: admin_firestore.client, route_number: str, delivery_number: str) -> Dict[str, int]:
    delivery_ref = (
        db.collection("routes")
        .document(route_number)
        .collection("archivedPCFs")
        .document(delivery_number)
    )
    doc = delivery_ref.get()
    if not doc.exists:
        return {"firestoreDocsDeleted": 0, "containerDocsDeleted": 0}

    container_deleted = 0
    for cdoc in delivery_ref.collection("containers").stream():
        cdoc.reference.delete()
        container_deleted += 1

    delivery_ref.delete()
    return {"firestoreDocsDeleted": 1, "containerDocsDeleted": container_deleted}


def _delete_storage_prefix(route_number: str, delivery_number: str) -> int:
    prefix = f"routes/{route_number}/archivedPCFs/{delivery_number}/"
    deleted = 0
    bucket = _bucket()
    for blob in bucket.list_blobs(prefix=prefix):
        try:
            blob.delete()
            deleted += 1
        except Exception as exc:
            logger.warning("Failed deleting blob %s: %s", blob.name, exc)
    return deleted


def _delete_hdd_delivery(route_number: str, delivery_number: str) -> bool:
    path = HDD_ARCHIVE_BASE / route_number / delivery_number
    if not path.exists():
        return False
    shutil.rmtree(path, ignore_errors=False)
    return True


def _checkpoint_doc_id(route_number: str, delivery_number: str) -> str:
    return f"{route_number}_{delivery_number}"


def _already_purged_checkpoint(db: admin_firestore.client, route_number: str, delivery_number: str) -> bool:
    ref = db.collection(PURGE_CHECKPOINT_COLLECTION).document(_checkpoint_doc_id(route_number, delivery_number))
    doc = ref.get()
    if not doc.exists:
        return False
    data = doc.to_dict() or {}
    return str(data.get("status") or "").strip().lower() == "completed"


def _mark_checkpoint(
    db: admin_firestore.client,
    *,
    route_number: str,
    delivery_number: str,
    status: str,
    event_id: str,
    details: Dict[str, Any],
) -> None:
    ref = db.collection(PURGE_CHECKPOINT_COLLECTION).document(_checkpoint_doc_id(route_number, delivery_number))
    payload = {
        "routeNumber": route_number,
        "deliveryNumber": delivery_number,
        "status": status,
        "eventId": event_id,
        "updatedAt": admin_firestore.SERVER_TIMESTAMP,
        "workerId": WORKER_ID,
        "details": details,
    }
    if status == "completed":
        payload["purgedAt"] = admin_firestore.SERVER_TIMESTAMP
    ref.set(payload, merge=True)


def _cleanup_expired_export_artifacts(db: admin_firestore.client) -> Dict[str, int]:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    scanned = 0
    expired = 0
    blobs_deleted = 0

    def _iter_status(status: str):
        return db.collection(EXPORT_COLLECTION).where("status", "==", status).stream()

    docs = list(_iter_status("ready")) + list(_iter_status("ready_partial")) + list(_iter_status("expired"))
    seen: Set[str] = set()

    for doc in docs:
        if doc.id in seen:
            continue
        seen.add(doc.id)
        scanned += 1
        data = doc.to_dict() or {}

        expires_ms = data.get("artifactExpiresAtMs") if isinstance(data.get("artifactExpiresAtMs"), int) else None
        if expires_ms is None:
            artifact = data.get("artifact") if isinstance(data.get("artifact"), dict) else {}
            expires_ms = _to_epoch_ms(artifact.get("expiresAt"))

        if expires_ms is None or expires_ms > now_ms:
            continue

        artifact = data.get("artifact") if isinstance(data.get("artifact"), dict) else {}
        primary_path = str(data.get("artifactStoragePath") or artifact.get("storagePath") or "").strip()
        parts = artifact.get("parts") if isinstance(artifact.get("parts"), list) else []

        paths = set()
        if primary_path:
            paths.add(primary_path)
            parent = primary_path.rsplit("/", 1)[0] if "/" in primary_path else ""
            if parent:
                for part in parts:
                    part_name = str(part or "").strip()
                    if part_name:
                        paths.add(f"{parent}/{part_name}")

        bucket = _bucket()
        for path in sorted(paths):
            try:
                blob = bucket.blob(path)
                if blob.exists():
                    blob.delete()
                    blobs_deleted += 1
            except Exception as exc:
                logger.warning("Artifact cleanup failed for %s (%s): %s", doc.id, path, exc)

        doc.reference.update(
            {
                "status": "expired",
                "updatedAt": admin_firestore.SERVER_TIMESTAMP,
                "artifact": {
                    "storagePath": primary_path or None,
                    "parts": parts,
                    "expiresAt": artifact.get("expiresAt"),
                    "sizeBytes": artifact.get("sizeBytes") or 0,
                    "cleanupAt": admin_firestore.SERVER_TIMESTAMP,
                },
            }
        )
        expired += 1

    return {"scanned": scanned, "expired": expired, "blobsDeleted": blobs_deleted}


def _process_route_purge(db: admin_firestore.client, route_number: str, route_data: Dict[str, Any]) -> Dict[str, Any]:
    if _is_route_locked(db, route_number):
        return {"route": route_number, "skipped": True, "reason": "route_locked"}

    retention_days = _retention_days_for_route(db, route_number, route_data)
    cutoff_date = datetime.now(timezone.utc).date() - timedelta(days=retention_days)
    active_ids = _active_delivery_ids(db, route_number)

    candidates: Dict[str, Dict[str, Any]] = {}

    # Firestore archived deliveries.
    arch_docs = db.collection("routes").document(route_number).collection("archivedPCFs").stream()
    for doc in arch_docs:
        if doc.id in active_ids:
            continue
        data = doc.to_dict() or {}
        anchor_date = _archive_anchor_date_for_firestore_doc(data)
        if not anchor_date:
            continue
        if anchor_date > cutoff_date:
            continue
        row = candidates.setdefault(
            doc.id,
            {"deliveryNumber": doc.id, "sources": set(), "retentionAnchorDates": []},
        )
        row["sources"].add("firestore")
        row["retentionAnchorDates"].append(anchor_date.isoformat())

    # HDD archived deliveries.
    route_dir = HDD_ARCHIVE_BASE / route_number
    if route_dir.exists() and route_dir.is_dir():
        for item in route_dir.iterdir():
            if not item.is_dir():
                continue
            delivery_number = item.name
            if delivery_number in active_ids:
                continue
            latest_run_date = _latest_hdd_run_date(route_number, delivery_number)
            if not latest_run_date:
                continue

            row = candidates.setdefault(
                delivery_number,
                {"deliveryNumber": delivery_number, "sources": set(), "retentionAnchorDates": []},
            )
            has_firestore_source = "firestore" in row["sources"]

            # If this delivery is already eligible via Firestore archive timestamp,
            # include HDD source too so both copies purge together.
            if not has_firestore_source and latest_run_date > cutoff_date:
                continue

            row["sources"].add("hdd")
            row["retentionAnchorDates"].append(latest_run_date.isoformat())

    if not candidates:
        return {
            "route": route_number,
            "retentionDays": retention_days,
            "cutoffDate": cutoff_date.isoformat(),
            "skipped": True,
            "reason": "no_candidates",
        }

    delivery_rows = sorted(
        candidates.values(),
        key=lambda r: min(r.get("retentionAnchorDates") or ["9999-12-31"]),
    )
    delivery_rows = delivery_rows[:MAX_DELIVERIES_PER_ROUTE_PER_CYCLE]

    event_id = f"purge_{route_number}_{int(datetime.now(timezone.utc).timestamp())}_{uuid4().hex[:8]}"
    event_ref = db.collection(PURGE_EVENT_COLLECTION).document(event_id)
    event_ref.set(
        {
            "eventId": event_id,
            "routeNumber": route_number,
            "workerId": WORKER_ID,
            "retentionDays": retention_days,
            "cutoffDate": cutoff_date.isoformat(),
            "status": "processing",
            "startedAt": admin_firestore.SERVER_TIMESTAMP,
            "candidateCount": len(delivery_rows),
            "successCount": 0,
            "failureCount": 0,
        }
    )

    success_count = 0
    failure_count = 0
    skipped_checkpoint = 0

    for row in delivery_rows:
        delivery_number = row["deliveryNumber"]
        sources = set(row.get("sources") or set())

        if _already_purged_checkpoint(db, route_number, delivery_number):
            skipped_checkpoint += 1
            continue

        delivery_event_ref = event_ref.collection("deliveries").document(delivery_number)
        delivery_event_ref.set(
            {
                "deliveryNumber": delivery_number,
                "sources": sorted(sources),
                "status": "processing",
                "startedAt": admin_firestore.SERVER_TIMESTAMP,
            }
        )

        try:
            stats = {
                "firestoreDocsDeleted": 0,
                "containerDocsDeleted": 0,
                "storageBlobsDeleted": 0,
                "hddDeleted": False,
            }

            if "firestore" in sources:
                fs_stats = _delete_firestore_delivery(db, route_number, delivery_number)
                stats["firestoreDocsDeleted"] = fs_stats["firestoreDocsDeleted"]
                stats["containerDocsDeleted"] = fs_stats["containerDocsDeleted"]

            # Delete storage prefix regardless; it is idempotent and cleans orphaned blobs.
            stats["storageBlobsDeleted"] = _delete_storage_prefix(route_number, delivery_number)

            if "hdd" in sources:
                stats["hddDeleted"] = _delete_hdd_delivery(route_number, delivery_number)

            _mark_checkpoint(
                db,
                route_number=route_number,
                delivery_number=delivery_number,
                status="completed",
                event_id=event_id,
                details=stats,
            )

            delivery_event_ref.update(
                {
                    "status": "completed",
                    "completedAt": admin_firestore.SERVER_TIMESTAMP,
                    "stats": stats,
                }
            )
            success_count += 1

        except Exception as exc:
            message = str(exc)[:1000]
            _mark_checkpoint(
                db,
                route_number=route_number,
                delivery_number=delivery_number,
                status="failed",
                event_id=event_id,
                details={"error": message},
            )
            delivery_event_ref.update(
                {
                    "status": "failed",
                    "completedAt": admin_firestore.SERVER_TIMESTAMP,
                    "error": message,
                }
            )
            logger.warning("Purge failed route=%s delivery=%s: %s", route_number, delivery_number, exc)
            failure_count += 1

    status = "completed" if failure_count == 0 else "completed_with_errors"
    event_ref.update(
        {
            "status": status,
            "successCount": success_count,
            "failureCount": failure_count,
            "skippedCheckpointCount": skipped_checkpoint,
            "completedAt": admin_firestore.SERVER_TIMESTAMP,
        }
    )

    return {
        "route": route_number,
        "retentionDays": retention_days,
        "cutoffDate": cutoff_date.isoformat(),
        "status": status,
        "successCount": success_count,
        "failureCount": failure_count,
        "skippedCheckpointCount": skipped_checkpoint,
    }


def run_worker(sa_path: str, *, poll_seconds: int, run_once: bool = False) -> None:
    _ensure_firebase(sa_path)
    db = admin_firestore.client()

    logger.info(
        "Archive purge worker started: worker=%s poll=%ss enabled=%s",
        WORKER_ID,
        poll_seconds,
        ARCHIVE_PURGE_ENABLED,
    )

    while True:
        try:
            if not ARCHIVE_PURGE_ENABLED:
                logger.info("ARCHIVE_PURGE_ENABLED=false; purge cycle skipped")
                if run_once:
                    return
                time.sleep(poll_seconds)
                continue

            artifact_summary = _cleanup_expired_export_artifacts(db)
            logger.info(
                "Artifact cleanup cycle: scanned=%s expired=%s blobsDeleted=%s",
                artifact_summary["scanned"],
                artifact_summary["expired"],
                artifact_summary["blobsDeleted"],
            )

            route_docs = db.collection("routes").stream()
            route_count = 0
            for route_doc in route_docs:
                route_count += 1
                route_number = str(route_doc.id or "").strip()
                if not route_number.isdigit():
                    continue
                result = _process_route_purge(db, route_number, route_doc.to_dict() or {})
                if result.get("skipped"):
                    logger.info("Purge skip route=%s reason=%s", route_number, result.get("reason"))
                else:
                    logger.info(
                        "Purge route=%s status=%s success=%s failure=%s",
                        route_number,
                        result.get("status"),
                        result.get("successCount"),
                        result.get("failureCount"),
                    )

            logger.info("Purge cycle completed over %s routes", route_count)

            if run_once:
                return
            time.sleep(poll_seconds)

        except KeyboardInterrupt:
            logger.info("Archive purge worker interrupted, shutting down")
            return
        except Exception:
            logger.exception("Archive purge worker cycle failure")
            if run_once:
                raise
            time.sleep(poll_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive purge + artifact cleanup worker")
    parser.add_argument(
        "--serviceAccount",
        default=os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        help="Path to Firebase service account JSON",
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=POLL_SECONDS,
        help="Polling interval in seconds",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one cycle and exit",
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

    run_worker(sa_path, poll_seconds=max(30, int(args.poll_seconds)), run_once=bool(args.once))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
