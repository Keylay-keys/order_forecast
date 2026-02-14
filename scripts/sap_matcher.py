"""SAP matching helpers for promo imports.

Provides exact lookup against the product catalog and fuzzy matching
with optional correction overrides stored in PostgreSQL.
"""

from __future__ import annotations

import difflib
import os
from typing import Dict, Optional, Tuple

import psycopg2


def _get_connection():
    """Get PostgreSQL connection using environment variables."""
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        database=os.environ.get('POSTGRES_DB', 'routespark'),
        user=os.environ.get('POSTGRES_USER', 'routespark'),
        password=os.environ.get('POSTGRES_PASSWORD', ''),
    )


def _fetch_catalog(conn, route_number: str) -> Dict[str, Dict]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sap, full_name, short_name
        FROM product_catalog
        WHERE route_number = %s
        """,
        [route_number],
    )
    rows = cur.fetchall()
    cur.close()
    
    catalog: Dict[str, Dict] = {}
    for sap, full_name, short_name in rows:
        catalog[sap] = {
            "sap": sap,
            "full_name": full_name or "",
            "short_name": short_name or "",
        }
    return catalog


def _fetch_corrections(conn, route_number: str, promo_account: Optional[str]) -> Dict[str, str]:
    """Returns wrong_sap -> correct_sap mapping."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT wrong_sap, correct_sap
            FROM sap_corrections
            WHERE route_number = %s
              AND (promo_account IS NULL OR promo_account = %s)
            """,
            [route_number, promo_account],
        )
        rows = cur.fetchall()
        cur.close()
        return {row[0]: row[1] for row in rows}
    except Exception:
        return {}


def _score_match(candidate: str, target: str) -> float:
    return difflib.SequenceMatcher(None, candidate.lower(), target.lower()).ratio()


def match_sap(
    description: str,
    route_number: str,
    promo_account: Optional[str] = None,
    db_path: Optional[str] = None,  # Ignored - kept for API compatibility
) -> Tuple[Optional[str], float, Dict]:
    """Match a description to a SAP.

    Returns (sap, confidence, debug_info)
    """
    conn = _get_connection()
    catalog = _fetch_catalog(conn, route_number)
    corrections = _fetch_corrections(conn, route_number, promo_account)

    # 1) Check if description is already a SAP
    if description in catalog:
        conn.close()
        return description, 1.0, {"strategy": "exact_sap"}

    # 2) Corrections table override
    if description in corrections:
        conn.close()
        return corrections[description], 0.95, {"strategy": "correction"}

    # 3) Fuzzy match against full_name/short_name
    best_sap = None
    best_score = 0.0
    for sap, meta in catalog.items():
        for field in [meta.get("full_name", ""), meta.get("short_name", "")]:
            if not field:
                continue
            score = _score_match(description, field)
            if score > best_score:
                best_score = score
                best_sap = sap

    debug = {"strategy": "fuzzy", "score": best_score}
    conn.close()
    return best_sap, best_score, debug


def apply_correction(
    wrong_sap: str,
    correct_sap: str,
    route_number: str,
    promo_account: Optional[str] = None,
    description_match: Optional[str] = None,
    db_path: Optional[str] = None,  # Ignored - kept for API compatibility
) -> None:
    """Persist a correction mapping to PostgreSQL."""
    conn = _get_connection()
    cur = conn.cursor()
    
    # Ensure table exists (should already exist from schema)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sap_corrections (
            id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            wrong_sap VARCHAR NOT NULL,
            correct_sap VARCHAR NOT NULL,
            promo_account VARCHAR,
            description_match VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(route_number, wrong_sap, promo_account)
        )
        """
    )

    cur.execute(
        """
        INSERT INTO sap_corrections (
            id, route_number, wrong_sap, correct_sap, promo_account, description_match
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (route_number, wrong_sap, promo_account) DO UPDATE SET
            correct_sap = EXCLUDED.correct_sap,
            description_match = EXCLUDED.description_match
        """,
        [
            f"{route_number}-{wrong_sap}-{promo_account or 'any'}",
            route_number,
            wrong_sap,
            correct_sap,
            promo_account,
            description_match,
        ],
    )
    conn.commit()
    cur.close()
    conn.close()
