"""PostgreSQL storage for personal Portal SAP-order preferences.

The browser never talks to PostgreSQL directly.  Every value is bound as a SQL
parameter and writes use an atomic compare-and-swap revision contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from psycopg2.extras import Json

from .dependencies import get_pg_connection, return_pg_connection


@dataclass(frozen=True)
class PortalSapOrderPreference:
    sap_order: list[str]
    revision: int
    updated_at: datetime


@dataclass(frozen=True)
class PortalSapOrderRead:
    exists: bool
    preference: Optional[PortalSapOrderPreference]


class PortalSapOrderConflict(Exception):
    """The expected revision no longer matches the committed row."""

    def __init__(self, current: PortalSapOrderPreference) -> None:
        super().__init__("Portal SAP order revision conflict")
        self.current = current


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _preference_from_row(row: Sequence[object]) -> PortalSapOrderPreference:
    raw_order, revision, updated_at = row
    if not isinstance(raw_order, list):
        raise RuntimeError("portal SAP order row is structurally invalid")
    if not isinstance(updated_at, datetime):
        raise RuntimeError("portal SAP order timestamp is structurally invalid")
    return PortalSapOrderPreference(
        sap_order=[str(item) for item in raw_order],
        revision=int(revision),
        updated_at=_normalize_timestamp(updated_at),
    )


def read_portal_sap_order(
    firebase_uid: str,
    route_number: str,
    *,
    acquire: Callable[[], object] = get_pg_connection,
    release: Callable[[object], None] = return_pg_connection,
) -> PortalSapOrderRead:
    """Read one exact UID/route row without creating a default record."""
    conn = None
    try:
        conn = acquire()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sap_order, revision, updated_at
                FROM portal_sap_order_preferences
                WHERE firebase_uid = %s AND route_number = %s
                """,
                (firebase_uid, route_number),
            )
            row = cur.fetchone()
        conn.rollback()
        if row is None:
            return PortalSapOrderRead(exists=False, preference=None)
        return PortalSapOrderRead(exists=True, preference=_preference_from_row(row))
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            release(conn)


def save_portal_sap_order(
    firebase_uid: str,
    route_number: str,
    sap_order: list[str],
    expected_revision: int,
    *,
    acquire: Callable[[], object] = get_pg_connection,
    release: Callable[[object], None] = return_pg_connection,
) -> PortalSapOrderPreference:
    """Atomically insert or replace one preference at an expected revision."""
    conn = None
    try:
        conn = acquire()
        row = None
        with conn.cursor() as cur:
            if expected_revision == 0:
                cur.execute(
                    """
                    INSERT INTO portal_sap_order_preferences (
                        firebase_uid,
                        route_number,
                        sap_order,
                        revision
                    )
                    VALUES (%s, %s, %s, 1)
                    ON CONFLICT (firebase_uid, route_number) DO NOTHING
                    RETURNING sap_order, revision, updated_at
                    """,
                    (firebase_uid, route_number, Json(sap_order)),
                )
                row = cur.fetchone()
            else:
                cur.execute(
                    """
                    UPDATE portal_sap_order_preferences
                    SET sap_order = %s,
                        revision = revision + 1,
                        updated_at = NOW()
                    WHERE firebase_uid = %s
                      AND route_number = %s
                      AND revision = %s
                    RETURNING sap_order, revision, updated_at
                    """,
                    (Json(sap_order), firebase_uid, route_number, expected_revision),
                )
                row = cur.fetchone()

            if row is None:
                cur.execute(
                    """
                    SELECT sap_order, revision, updated_at
                    FROM portal_sap_order_preferences
                    WHERE firebase_uid = %s AND route_number = %s
                    """,
                    (firebase_uid, route_number),
                )
                current_row = cur.fetchone()
                if current_row is None:
                    # A nonzero expected revision against an absent row is also
                    # a conflict.  Keep the returned shape bounded and useful.
                    current = PortalSapOrderPreference(
                        sap_order=[],
                        revision=0,
                        updated_at=datetime.now(timezone.utc),
                    )
                else:
                    current = _preference_from_row(current_row)
                conn.rollback()
                raise PortalSapOrderConflict(current)

        committed = _preference_from_row(row)
        conn.commit()
        return committed
    except PortalSapOrderConflict:
        raise
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            release(conn)
