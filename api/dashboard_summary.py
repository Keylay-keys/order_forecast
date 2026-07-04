"""Dashboard summary mirror and freshness helpers.

Firebase remains the source of truth. PostgreSQL is the normal serving mirror,
but callers only receive ``fresh: true`` when the mirror revision matches the
Firebase summary watermark.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json, RealDictCursor

logger = logging.getLogger(__name__)

DASHBOARD_SUMMARY_TABLE = "route_dashboard_summaries"


@dataclass(frozen=True)
class SourceWatermark:
    revision: Optional[str]
    updated_at: Optional[datetime]


@dataclass(frozen=True)
class MirrorRecord:
    route_number: str
    payload: Dict[str, Any]
    source_revision: Optional[str]
    source_updated_at: Optional[datetime]
    mirrored_revision: Optional[str]
    mirrored_at: Optional[datetime]
    updated_at: Optional[datetime]


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Any) -> Optional[str]:
    dt = _to_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _to_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        try:
            return value.to_datetime()
        except Exception:
            return None
    if hasattr(value, "timestamp"):
        try:
            return datetime.fromtimestamp(value.timestamp(), timezone.utc)
        except Exception:
            return None
    return None


def _coerce_revision(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return str(int(value))
    text = str(value).strip()
    return text or None


def _source_watermark_from_doc(data: Dict[str, Any]) -> SourceWatermark:
    revision = (
        _coerce_revision(data.get("sourceRevision"))
        or _coerce_revision(data.get("updatedAtMs"))
        or _coerce_revision(data.get("mirroredRevision"))
    )
    updated_at = _to_datetime(data.get("sourceUpdatedAt")) or _to_datetime(data.get("updatedAt"))
    if updated_at is None and isinstance(data.get("updatedAtMs"), (int, float)):
        updated_at = datetime.fromtimestamp(float(data["updatedAtMs"]) / 1000, timezone.utc)
    return SourceWatermark(revision=revision, updated_at=updated_at)


def load_firebase_source_watermark(db: Any, route_number: str) -> SourceWatermark:
    """Read the authoritative route summary watermark from Firebase.

    This is intentionally one route-level document read. The heavier container
    row read happens only when the mirror needs repair.
    """
    snap = db.collection("routeDashboardSummaries").document(route_number).get()
    if not getattr(snap, "exists", False):
        return SourceWatermark(revision=None, updated_at=None)
    return _source_watermark_from_doc(snap.to_dict() or {})


def _normalize_summary_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    item = {
        "description": str(raw.get("description") or ""),
        "product": str(raw.get("product") or ""),
        "expiryDate": str(raw.get("expiryDate") or ""),
        "daysLeft": int(raw.get("daysLeft") or 0),
        "isShortCoded": bool(raw.get("isShortCoded")),
        "deliveryNumber": str(raw.get("deliveryNumber") or ""),
        "containerCode": str(raw.get("containerCode") or ""),
        "visibleOnList": raw.get("visibleOnList") is not False,
        "isLowQuantity": bool(raw.get("isLowQuantity")),
        "pageNumber": int(raw.get("pageNumber") or 1),
    }
    guarantee = raw.get("guaranteed")
    if isinstance(guarantee, dict) and guarantee.get("isGuaranteed"):
        item["guaranteed"] = {
            "isGuaranteed": True,
            "guaranteeExpiresAt": str(guarantee.get("guaranteeExpiresAt") or ""),
        }
    return item


def _normalize_container_row(raw: Dict[str, Any], fallback_id: str) -> Dict[str, Any]:
    expiring_items = [
        _normalize_summary_item(item)
        for item in (raw.get("expiringItems") if isinstance(raw.get("expiringItems"), list) else [])
        if isinstance(item, dict)
    ]
    return {
        "id": str(raw.get("containerCode") or fallback_id),
        "deliveryNumber": str(raw.get("deliveryNumber") or ""),
        "containerCode": str(raw.get("containerCode") or fallback_id),
        "loadingDate": raw.get("loadingDate"),
        "itemCount": int(raw.get("itemCount") or 0),
        "expiringCount": int(raw.get("expiringCount") or 0),
        "expiredCount": int(raw.get("expiredCount") or 0),
        "routeId": str(raw.get("routeId") or raw.get("routeNumber") or ""),
        "status": raw.get("status"),
        "createdAt": raw.get("createdAt"),
        "updatedAt": raw.get("updatedAt"),
        "userId": raw.get("userId"),
        "allItemsExpired": bool(raw.get("allItemsExpired")),
        "items": expiring_items,
    }


def _build_counts(active_pcfs: List[Dict[str, Any]]) -> Dict[str, int]:
    expired = sum(
        1
        for row in active_pcfs
        if int(row.get("itemCount") or 0) > 0 and int(row.get("expiredCount") or 0) >= int(row.get("itemCount") or 0)
    )
    expiring = sum(1 for row in active_pcfs if int(row.get("expiringCount") or 0) > 0)
    return {
        "totalPCFs": len(active_pcfs),
        "activePCFs": len(active_pcfs),
        "expiringPCFs": expiring,
        "expiredPCFs": expired,
        "totalContainers": len(active_pcfs),
        "expiredContainers": expired,
    }


def build_payload_from_firebase_summary(db: Any, route_number: str, watermark: SourceWatermark) -> Dict[str, Any]:
    """Build API payload from compact Firebase summary rows, not full PCF pages."""
    rows_ref = (
        db.collection("routeDashboardSummaries")
        .document(route_number)
        .collection("containers")
        .where("active", "==", True)
    )
    active_pcfs = [
        _normalize_container_row(snap.to_dict() or {}, getattr(snap, "id", ""))
        for snap in rows_ref.stream()
    ]
    expiring_items: List[Dict[str, Any]] = []
    for row in active_pcfs:
        expiring_items.extend(row.get("items") or [])

    return {
        "routeNumber": route_number,
        "activePcfs": active_pcfs,
        "expiringItems": expiring_items,
        "counts": _build_counts(active_pcfs),
        "generatedAt": _utc_now().isoformat(),
        "sourceWatermark": {
            "revision": watermark.revision,
            "latestUpdatedAt": _iso(watermark.updated_at),
        },
    }


def ensure_dashboard_summary_table(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DASHBOARD_SUMMARY_TABLE} (
              route_number TEXT PRIMARY KEY,
              payload JSONB NOT NULL,
              source_revision TEXT,
              source_updated_at TIMESTAMPTZ,
              mirrored_revision TEXT,
              mirrored_at TIMESTAMPTZ,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    conn.commit()


def load_mirror(conn: Any, route_number: str) -> Optional[MirrorRecord]:
    ensure_dashboard_summary_table(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT route_number, payload, source_revision, source_updated_at,
                   mirrored_revision, mirrored_at, updated_at
            FROM {DASHBOARD_SUMMARY_TABLE}
            WHERE route_number = %s
            """,
            (route_number,),
        )
        row = cur.fetchone()
    if not row:
        return None
    payload = row.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}
    return MirrorRecord(
        route_number=str(row.get("route_number") or route_number),
        payload=payload,
        source_revision=row.get("source_revision"),
        source_updated_at=row.get("source_updated_at"),
        mirrored_revision=row.get("mirrored_revision"),
        mirrored_at=row.get("mirrored_at"),
        updated_at=row.get("updated_at"),
    )


def upsert_mirror(
    conn: Any,
    *,
    route_number: str,
    payload: Dict[str, Any],
    source_watermark: SourceWatermark,
) -> MirrorRecord:
    ensure_dashboard_summary_table(conn)
    now = _utc_now()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            INSERT INTO {DASHBOARD_SUMMARY_TABLE}
              (route_number, payload, source_revision, source_updated_at,
               mirrored_revision, mirrored_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (route_number) DO UPDATE SET
              payload = EXCLUDED.payload,
              source_revision = EXCLUDED.source_revision,
              source_updated_at = EXCLUDED.source_updated_at,
              mirrored_revision = EXCLUDED.mirrored_revision,
              mirrored_at = EXCLUDED.mirrored_at,
              updated_at = EXCLUDED.updated_at
            RETURNING route_number, payload, source_revision, source_updated_at,
                      mirrored_revision, mirrored_at, updated_at
            """,
            (
                route_number,
                Json(payload),
                source_watermark.revision,
                source_watermark.updated_at,
                source_watermark.revision,
                now,
                now,
            ),
        )
        row = cur.fetchone()
    conn.commit()
    return MirrorRecord(
        route_number=str(row.get("route_number") or route_number),
        payload=row.get("payload") or payload,
        source_revision=row.get("source_revision"),
        source_updated_at=row.get("source_updated_at"),
        mirrored_revision=row.get("mirrored_revision"),
        mirrored_at=row.get("mirrored_at"),
        updated_at=row.get("updated_at"),
    )


def _fresh_response(record: MirrorRecord, source_watermark: SourceWatermark, source: str) -> Dict[str, Any]:
    payload = dict(record.payload or {})
    generated_at = _utc_now().isoformat()
    payload["freshness"] = {
        "source": source,
        "fresh": True,
        "sourceRevision": source_watermark.revision,
        "mirroredRevision": record.mirrored_revision,
        "sourceUpdatedAt": _iso(source_watermark.updated_at),
        "mirroredAt": _iso(record.mirrored_at),
        "generatedAt": generated_at,
    }
    payload["generatedAt"] = payload.get("generatedAt") or generated_at
    return payload


def _stale_response(
    record: Optional[MirrorRecord],
    source_watermark: SourceWatermark,
    stale_reason: str,
    route_number: str,
) -> Dict[str, Any]:
    payload = dict(record.payload or {}) if record else {}
    generated_at = _utc_now().isoformat()
    payload.setdefault("routeNumber", record.route_number if record else route_number)
    payload.setdefault("activePcfs", [])
    payload.setdefault("expiringItems", [])
    payload.setdefault("counts", _build_counts([]))
    payload["freshness"] = {
        "source": "cache",
        "fresh": False,
        "sourceRevision": source_watermark.revision,
        "mirroredRevision": record.mirrored_revision if record else None,
        "sourceUpdatedAt": _iso(source_watermark.updated_at),
        "mirroredAt": _iso(record.mirrored_at) if record else None,
        "generatedAt": generated_at,
        "staleReason": stale_reason,
    }
    payload["generatedAt"] = payload.get("generatedAt") or generated_at
    return payload


def get_dashboard_summary_payload(
    *,
    db: Any,
    conn: Any,
    route_number: str,
) -> Dict[str, Any]:
    """Return a dashboard summary with explicit freshness metadata."""
    source_watermark = load_firebase_source_watermark(db, route_number)
    mirror = load_mirror(conn, route_number)

    if source_watermark.revision and mirror and mirror.mirrored_revision == source_watermark.revision:
        return _fresh_response(mirror, source_watermark, "postgres")

    stale_reason = "mirror_missing" if mirror is None else "mirror_behind"

    if not source_watermark.revision:
        return _stale_response(mirror, source_watermark, stale_reason, route_number)

    try:
        payload = build_payload_from_firebase_summary(db, route_number, source_watermark)
        repaired = upsert_mirror(
            conn,
            route_number=route_number,
            payload=payload,
            source_watermark=source_watermark,
        )
        return _fresh_response(repaired, source_watermark, "firebase_readthrough")
    except Exception:
        logger.exception("dashboard_summary.readthrough_failed route=%s", route_number)
        return _stale_response(mirror, source_watermark, "rebuild_failed", route_number)
