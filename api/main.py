"""RouteSpark Web Portal API - Main Application.

FastAPI application providing REST API for the web portal.
Handles order history, creation, forecasts, and system health.

Security: All endpoints require Firebase Auth except /api/health.

Usage:
    uvicorn api.main:app --host 127.0.0.1 --port 8000
    
Or via supervisor.py.
"""

from __future__ import annotations

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .dependencies import get_firebase_app, get_duckdb, get_firestore
from .routers import auth, history, health, orders, forecast, reference, low_quantity, settings, schedule
from .middleware.rate_limit import setup_rate_limiting
from .middleware.honeypots import setup_honeypots
from .middleware.brute_force import setup_brute_force_protection
from .middleware.code_protection import setup_code_protection

# =============================================================================
# CONFIGURATION
# =============================================================================

# Environment
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"
API_VERSION = "1.0.0"

# CORS - strict origin allowlist
ALLOWED_ORIGINS = [
    "http://localhost:5173",           # Vite dev server
    "http://localhost:3000",           # Alternative dev port
    "https://portal.routespark.pro",   # Production
]

# Logging setup
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s'
)
logger = logging.getLogger("api.main")


# =============================================================================
# LIFESPAN (Startup/Shutdown)
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: startup and shutdown hooks."""
    # Startup
    logger.info(f"Starting RouteSpark API v{API_VERSION}")
    logger.info(f"Debug mode: {DEBUG_MODE}")
    
    try:
        # Initialize Firebase
        get_firebase_app()
        logger.info("Firebase initialized")
        
        # Initialize Firestore
        get_firestore()
        logger.info("Firestore connected")
        
        # Initialize DuckDB (read-only)
        get_duckdb()
        logger.info("DuckDB connected")
        
    except Exception as e:
        logger.error(f"Startup failed: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down RouteSpark API")


# =============================================================================
# APPLICATION
# =============================================================================

# Create app with conditional docs
if DEBUG_MODE:
    app = FastAPI(
        title="RouteSpark Web Portal API",
        version=API_VERSION,
        lifespan=lifespan,
    )
else:
    # Production: disable docs endpoints
    app = FastAPI(
        title="RouteSpark Web Portal API",
        version=API_VERSION,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )


# =============================================================================
# MIDDLEWARE
# =============================================================================
# Order matters! Outermost middleware runs first.
# 1. Code protection (blocklist + enumeration) - reject bad actors early
# 2. Honeypots - detect scanners
# 3. Brute force - track auth failures
# 4. Rate limiting - general rate limits
# 5. CORS - allow cross-origin

# Security middleware (order: last added = first to run)
# So we add in REVERSE order of desired execution:
# Desired: blocklist → honeypots → brute_force → rate_limit
setup_rate_limiting(app)           # 4th: Rate limits (runs last)
setup_brute_force_protection(app)  # 3rd: Auth failure tracking
setup_honeypots(app)               # 2nd: Honeypot path detection
setup_code_protection(app)         # 1st: Blocklist + enumeration (runs first)

# CORS - strict origins only
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
    max_age=3600,
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    
    # Remove headers that reveal implementation
    if "server" in response.headers:
        del response.headers["server"]
    if "x-powered-by" in response.headers:
        del response.headers["x-powered-by"]
    
    # Add security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    
    return response


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all requests for debugging."""
    start_time = datetime.utcnow()
    
    response = await call_next(request)
    
    duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
    
    # Log request (redact auth header)
    logger.debug(
        f"{request.method} {request.url.path} "
        f"-> {response.status_code} ({duration_ms:.0f}ms)"
    )
    
    return response


# =============================================================================
# EXCEPTION HANDLERS
# =============================================================================

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions with generic error response."""
    # Log full error internally
    logger.exception(f"Unhandled exception on {request.url.path}")
    
    # Return generic error to client (no stack traces)
    if DEBUG_MODE:
        return JSONResponse(
            status_code=500,
            content={
                "error": str(exc),
                "code": "INTERNAL_ERROR",
                "type": type(exc).__name__
            }
        )
    else:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "code": "INTERNAL_ERROR"
            }
        )


# =============================================================================
# ROUTERS
# =============================================================================

app.include_router(auth.router, prefix="/api/auth", tags=["Authentication"])
app.include_router(history.router, prefix="/api", tags=["History"])
app.include_router(health.router, prefix="/api", tags=["Health"])
app.include_router(orders.router, prefix="/api", tags=["Orders"])
app.include_router(forecast.router, prefix="/api", tags=["Forecast"])
app.include_router(reference.router, prefix="/api", tags=["Reference"])
app.include_router(low_quantity.router, prefix="/api", tags=["Low Quantity"])
app.include_router(settings.router, prefix="/api", tags=["Settings"])
app.include_router(schedule.router, prefix="/api", tags=["Schedule"])


# =============================================================================
# ROOT ENDPOINT
# =============================================================================

@app.get("/")
async def root():
    """Root endpoint - API info."""
    return {
        "name": "RouteSpark Web Portal API",
        "version": API_VERSION,
        "status": "running"
    }


@app.get("/api")
async def api_root():
    """API root - available endpoints."""
    return {
        "endpoints": {
            "auth": "/api/auth/verify",
            "history": "/api/history",
            "health": "/api/health",
            "orders": "/api/orders",
            "forecast": "/api/forecast",
            "reference": "/api/products",
            "low_quantity": "/api/low-quantity",
        }
    }
