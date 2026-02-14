"""FastAPI dependencies for authentication, database connections, and shared resources.

All endpoints use these dependencies for:
- Firebase token verification
- Firestore client access
- PostgreSQL queries (via direct connection pool)
- Route access validation
"""

from __future__ import annotations

import os
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List
from functools import lru_cache

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import firebase_admin
from firebase_admin import auth, credentials, firestore

# =============================================================================
# CONFIGURATION
# =============================================================================

# Paths
API_DIR = Path(__file__).parent
ORDER_FORECAST_DIR = API_DIR.parent
DATA_DIR = ORDER_FORECAST_DIR / "data"

# PostgreSQL connection settings (from environment)
PG_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
PG_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
PG_DATABASE = os.environ.get('POSTGRES_DB', 'routespark')
PG_USER = os.environ.get('POSTGRES_USER', 'routespark')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', '')

# Service account path (same as other scripts)
SERVICE_ACCOUNT_PATH = os.environ.get(
    "GOOGLE_APPLICATION_CREDENTIALS",
    str(ORDER_FORECAST_DIR.parent / "routespark-firebase-adminsdk.json")
)

# Security settings
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"
MAX_TOKEN_AGE_SECONDS = int(os.environ.get("MAX_TOKEN_AGE_SECONDS", 3600))  # Default 1 hour
CLOCK_SKEW_SECONDS = int(os.environ.get("CLOCK_SKEW_SECONDS", 300))  # Default 5 min
SKIP_TOKEN_AGE_CHECK = (
    DEBUG_MODE
    and os.environ.get("SKIP_TOKEN_AGE_CHECK", "").lower() in ("1", "true")
)

# Logging
logger = logging.getLogger("api.dependencies")
security_logger = logging.getLogger("security")

# Bearer token scheme
bearer_scheme = HTTPBearer(auto_error=False)


# =============================================================================
# FIREBASE INITIALIZATION
# =============================================================================

_firebase_app: Optional[firebase_admin.App] = None
_firestore_client = None


def get_firebase_app() -> firebase_admin.App:
    """Get or initialize Firebase Admin app."""
    global _firebase_app
    
    if _firebase_app is not None:
        return _firebase_app
    
    # Check if already initialized
    try:
        _firebase_app = firebase_admin.get_app()
        return _firebase_app
    except ValueError:
        pass
    
    # Initialize with service account
    if not Path(SERVICE_ACCOUNT_PATH).exists():
        raise RuntimeError(f"Service account not found: {SERVICE_ACCOUNT_PATH}")
    
    cred = credentials.Certificate(SERVICE_ACCOUNT_PATH)
    _firebase_app = firebase_admin.initialize_app(cred)
    logger.info("Firebase Admin initialized")
    return _firebase_app


def get_firestore() -> firestore.Client:
    """Get Firestore client (singleton)."""
    global _firestore_client
    
    if _firestore_client is None:
        get_firebase_app()  # Ensure initialized
        _firestore_client = firestore.client()
        logger.info("Firestore client initialized")
    
    return _firestore_client


# =============================================================================
# POSTGRESQL CONNECTION POOL
# =============================================================================

import psycopg2
from psycopg2 import pool

# Thread-safe connection pool (5-10 connections)
_pg_pool: Optional[pool.ThreadedConnectionPool] = None


def get_pg_pool() -> pool.ThreadedConnectionPool:
    """Get or create the PostgreSQL connection pool.
    
    Uses a small pool (5-10 connections) suitable for web-api concurrency.
    Pool is created lazily on first use and reused across requests.
    """
    global _pg_pool
    
    if _pg_pool is None:
        _pg_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        logger.info(f"PostgreSQL connection pool created (2-10 connections to {PG_HOST}:{PG_PORT})")
    
    return _pg_pool


def get_pg_connection():
    """Get a PostgreSQL connection from the pool.
    
    Returns a connection from the pool. Caller must return it using:
        get_pg_pool().putconn(conn)
    
    Or use as context manager for auto-return on close (psycopg2 2.9+).
    For read-only queries and health checks.
    """
    return get_pg_pool().getconn()


def return_pg_connection(conn):
    """Return a connection to the pool."""
    try:
        get_pg_pool().putconn(conn)
    except Exception as e:
        logger.warning(f"Error returning connection to pool: {e}")


# =============================================================================
# DATABASE CLIENT (Direct PostgreSQL)
# =============================================================================

# Add scripts directory to path for pg_utils import
import sys
SCRIPTS_DIR = ORDER_FORECAST_DIR / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from psycopg2.extras import RealDictCursor


class DirectPGConnection:
    """Wrapper that provides cursor-like interface via direct PostgreSQL.

    Uses the connection pool for all queries (no more message bus).
    """

    def execute(self, sql: str, params: Optional[list] = None):
        """Execute SQL and return result object."""
        conn = get_pg_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Convert ? placeholders to %s for psycopg2
                pg_sql = sql.replace('?', '%s')
                cur.execute(pg_sql, params)

                # Check if this is a SELECT query
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    rows = [dict(row) for row in cur.fetchall()]
                    return DirectPGResult({'columns': columns, 'rows': rows})
                else:
                    conn.commit()
                    return DirectPGResult({'affected_rows': cur.rowcount})
        finally:
            return_pg_connection(conn)


class DirectPGResult:
    """Wrapper to make direct PG results look like cursor results."""

    def __init__(self, result: dict):
        self._result = result
        self._rows = result.get('rows', [])
        self._columns = result.get('columns', [])

    def _row_to_tuple(self, row):
        """Convert row to tuple, preserving column order from query."""
        if isinstance(row, dict):
            if self._columns:
                return tuple(row.get(col) for col in self._columns)
            return tuple(row.values())
        if isinstance(row, (list, tuple)):
            return tuple(row)
        return (row,)

    def fetchone(self):
        if self._rows:
            return self._row_to_tuple(self._rows[0])
        return None

    def fetchall(self):
        return [self._row_to_tuple(row) for row in self._rows]


def get_db_client() -> DirectPGConnection:
    """Get database connection via direct PostgreSQL.

    Uses the connection pool for all queries.
    """
    return DirectPGConnection()


# Legacy alias for compatibility
def get_duckdb() -> DirectPGConnection:
    """DEPRECATED: Use get_db_client() instead. This now uses PostgreSQL."""
    logger.warning("get_duckdb() is deprecated, use get_db_client()")
    return get_db_client()


# =============================================================================
# AUTHENTICATION
# =============================================================================

async def verify_firebase_token(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme)
) -> Dict[str, Any]:
    """Verify Firebase ID token from Authorization header.
    
    Security checks:
    - Valid signature (RS256)
    - Not expired
    - Not revoked
    - Token age < 1 hour (force refresh)
    - Not from the future (clock skew attack)
    
    Returns:
        Decoded token claims including 'uid'
    
    Raises:
        HTTPException 401 on any auth failure
    """
    # Check for Authorization header, fall back to ?token= query param
    # (needed for <img src="..."> tags which can't send headers)
    if credentials is None:
        token = request.query_params.get("token")
        if not token:
            _log_auth_failure(request, "missing_auth_header")
            raise HTTPException(401, "Missing Authorization header")
    else:
        token = credentials.credentials
    
    try:
        # Ensure Firebase is initialized
        get_firebase_app()
        
        # Verify token with revocation check
        decoded = auth.verify_id_token(token, check_revoked=True)
        
        # Additional security checks (can be disabled for testing with clock skew)
        if not SKIP_TOKEN_AGE_CHECK:
            # Use epoch seconds to avoid naive datetime timezone offsets.
            now = time.time()
            issued_at = decoded.get('iat', 0)
            
            # Reject tokens issued too long ago
            if now - issued_at > MAX_TOKEN_AGE_SECONDS:
                _log_auth_failure(request, "token_too_old", uid=decoded.get('uid'))
                raise HTTPException(401, "Token too old, please re-authenticate")
            
            # Reject tokens from the future
            if issued_at > now + CLOCK_SKEW_SECONDS:
                _log_auth_failure(request, "future_token", uid=decoded.get('uid'))
                raise HTTPException(401, "Invalid token timestamp")
        
        return decoded
        
    except auth.RevokedIdTokenError:
        _log_auth_failure(request, "revoked_token")
        raise HTTPException(401, "Token has been revoked")
    except auth.ExpiredIdTokenError:
        _log_auth_failure(request, "expired_token")
        raise HTTPException(401, "Token has expired")
    except auth.InvalidIdTokenError as e:
        _log_auth_failure(request, "invalid_token", error=str(e))
        raise HTTPException(401, "Invalid token")
    except Exception as e:
        _log_auth_failure(request, "auth_error", error=str(e))
        raise HTTPException(401, "Authentication failed")


def _log_auth_failure(request: Request, reason: str, **extra):
    """Log authentication failure for security monitoring."""
    security_logger.warning({
        "event": "auth_failure",
        "reason": reason,
        "ip": request.client.host if request.client else "unknown",
        "user_agent": request.headers.get("user-agent", "unknown"),
        "path": request.url.path,
        **extra
    })


# =============================================================================
# AUTHORIZATION (Route Access)
# =============================================================================

async def require_route_access(
    route_number: str,
    decoded_token: Dict[str, Any] = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore)
) -> Dict[str, Any]:
    """Verify user has access to the specified route.
    
    CRITICAL: Always fetches fresh user doc from Firestore.
    Never trusts claims in the token for authorization.
    
    Args:
        route_number: Route to access
        decoded_token: Verified Firebase token
        db: Firestore client
    
    Returns:
        User document data
    
    Raises:
        HTTPException 400 for invalid route format
        HTTPException 403 if user lacks access
    """
    uid = decoded_token['uid']
    
    # Validate route_number format (prevent injection)
    if not route_number.isdigit() or len(route_number) > 10:
        raise HTTPException(400, "Invalid route number format")
    
    # Fetch user document from Firestore (source of truth)
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        _log_access_failure(uid, route_number, "user_not_found")
        raise HTTPException(403, "Access denied")
    
    user_data = user_doc.to_dict()
    
    # Check access (same logic as Firestore rules)
    if not has_access_to_route(user_data, route_number):
        _log_access_failure(uid, route_number, "no_route_access")
        raise HTTPException(403, "Access denied")
    
    return user_data


def has_access_to_route(user_data: Dict[str, Any], route_number: str) -> bool:
    """Check if user has access to route.
    
    Mirrors Firestore rules logic from firestore.rules lines 14-32.
    """
    profile = user_data.get('profile', {})
    assignments = user_data.get('routeAssignments', {})
    
    return (
        str(profile.get('routeNumber', '')) == route_number or
        str(profile.get('currentRoute', '')) == route_number or
        route_number in [str(r) for r in (profile.get('additionalRoutes') or [])] or
        route_number in [str(r) for r in assignments.keys()]
    )


def _log_access_failure(uid: str, route_number: str, reason: str):
    """Log authorization failure for security monitoring."""
    security_logger.warning({
        "event": "unauthorized_route_access",
        "uid": uid,
        "route_number": route_number,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat()
    })


# =============================================================================
# ROUTE OWNER TIMEZONE
# =============================================================================

def get_route_timezone(db: firestore.Client, route_number: str) -> Optional[str]:
    """Get timezone for a route's owner.
    
    Routes use ownerUid (with legacy userId fallback).
    Then fetches users/{uid}/profile/timezone.
    """
    # Get route document
    route_ref = db.collection('routes').document(route_number)
    route_doc = route_ref.get()
    
    if not route_doc.exists:
        return None
    
    route_data = route_doc.to_dict()
    owner_uid = route_data.get('ownerUid') or route_data.get('userId')
    
    if not owner_uid:
        return None
    
    # Get owner's timezone
    user_ref = db.collection('users').document(owner_uid)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        return None
    
    user_data = user_doc.to_dict()
    return user_data.get('profile', {}).get('timezone')
