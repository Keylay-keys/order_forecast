"""Dedicated least-privilege PostgreSQL pool for internal PCF APIs."""

from __future__ import annotations

import os
import threading
from typing import Any

from psycopg2 import pool


PCF_RUNTIME_ROLE = "routespark_pcf_app"
PCF_POOL_MAX_LIMIT = 20
_pool: pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required for the internal PCF database")
    return value


def get_pcf_pg_pool() -> pool.ThreadedConnectionPool:
    """Create the PCF pool only from its dedicated runtime credential."""
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        user = _required_environment("PCF_POSTGRES_USER")
        if user != PCF_RUNTIME_ROLE:
            raise RuntimeError("PCF_POSTGRES_USER must be routespark_pcf_app")
        password = _required_environment("PCF_POSTGRES_PASSWORD")
        try:
            pool_max = int(os.environ.get("PCF_POSTGRES_POOL_MAX", "5"))
        except ValueError as exc:
            raise RuntimeError("PCF_POSTGRES_POOL_MAX must be an integer") from exc
        if not 1 <= pool_max <= PCF_POOL_MAX_LIMIT:
            raise RuntimeError(
                f"PCF_POSTGRES_POOL_MAX must be between 1 and {PCF_POOL_MAX_LIMIT}"
            )
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=pool_max,
            host=os.environ.get(
                "PCF_POSTGRES_HOST", os.environ.get("POSTGRES_HOST", "localhost")
            ),
            port=int(
                os.environ.get(
                    "PCF_POSTGRES_PORT", os.environ.get("POSTGRES_PORT", "5432")
                )
            ),
            database=os.environ.get(
                "PCF_POSTGRES_DB", os.environ.get("POSTGRES_DB", "routespark")
            ),
            user=user,
            password=password,
            connect_timeout=10,
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=3,
            options="-c statement_timeout=10000 -c lock_timeout=3000",
        )
        return _pool


def get_pcf_pg_connection() -> Any:
    conn = get_pcf_pg_pool().getconn()
    conn.autocommit = False
    return conn


def return_pcf_pg_connection(conn: Any, *, discard: bool = False) -> None:
    if conn is None:
        return
    try:
        if not conn.closed:
            conn.rollback()
    finally:
        get_pcf_pg_pool().putconn(conn, close=discard or bool(conn.closed))
