"""Pydantic models for the Web Portal API.

These models mirror the TypeScript interfaces in src/types/order.ts
to ensure compatibility between mobile app and web portal.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator
import re


# =============================================================================
# INPUT VALIDATION PATTERNS
# =============================================================================

ROUTE_NUMBER_PATTERN = re.compile(r'^\d{1,10}$')
SCHEDULE_KEY_PATTERN = re.compile(r'^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$')
SAP_PATTERN = re.compile(r'^[\w\-]{1,20}$')
STORE_ID_PATTERN = re.compile(r'^[\w\-]{1,50}$')


# =============================================================================
# REQUEST MODELS (Input Validation)
# =============================================================================

class HistoryRequest(BaseModel):
    """Request parameters for order history."""
    route: str = Field(..., description="Route number (digits only)")
    weeks: int = Field(default=12, ge=1, le=52, description="Number of weeks of history")
    
    @validator('route')
    def validate_route(cls, v):
        if not ROUTE_NUMBER_PATTERN.match(v):
            raise ValueError('Route must be 1-10 digits')
        return v


class OrderCreateRequest(BaseModel):
    """Request to create a new order."""
    routeNumber: str = Field(..., description="Route number")
    deliveryDate: date = Field(..., description="Expected delivery date")
    scheduleKey: str = Field(..., description="Order day (monday, tuesday, etc.)")
    isHolidaySchedule: Optional[bool] = None
    notes: Optional[str] = None
    
    @validator('routeNumber')
    def validate_route(cls, v):
        if not ROUTE_NUMBER_PATTERN.match(v):
            raise ValueError('Route must be 1-10 digits')
        return v
    
    @validator('scheduleKey')
    def validate_schedule_key(cls, v):
        if not SCHEDULE_KEY_PATTERN.match(v.lower()):
            raise ValueError('scheduleKey must be a day of week')
        return v.lower()
    
    @validator('deliveryDate')
    def validate_delivery_date(cls, v):
        from datetime import timedelta
        today = date.today()
        # Allow dates up to 14 days in past (for corrections) and 90 days in future
        min_date = today - timedelta(days=14)
        max_date = today + timedelta(days=90)
        if v < min_date:
            raise ValueError('Delivery date too far in past')
        if v > max_date:
            raise ValueError('Delivery date too far in future')
        return v


class ForecastRequest(BaseModel):
    """Request parameters for forecast retrieval."""
    route: str = Field(..., description="Route number")
    deliveryDate: date = Field(..., description="Delivery date for forecast")
    scheduleKey: str = Field(..., description="Order day (monday, tuesday, etc.)")
    
    @validator('route')
    def validate_route(cls, v):
        if not ROUTE_NUMBER_PATTERN.match(v):
            raise ValueError('Route must be 1-10 digits')
        return v
    
    @validator('scheduleKey')
    def validate_schedule_key(cls, v):
        if not SCHEDULE_KEY_PATTERN.match(v.lower()):
            raise ValueError('scheduleKey must be a day of week')
        return v.lower()


# =============================================================================
# ORDER ITEM MODELS (Mirrors src/types/order.ts)
# =============================================================================

class PriorOrderContext(BaseModel):
    """Context from prior order for the same store/item."""
    orderId: str
    orderDate: Optional[str] = None
    deliveryDate: str
    quantity: int
    scheduleKey: Optional[str] = None
    daysAgo: Optional[int] = None


class ExpiryReplacement(BaseModel):
    """Info about expiry-driven replacement order."""
    expiryDate: str
    minUnitsRequired: int
    reason: str  # 'low_qty_expiry'


class OrderItem(BaseModel):
    """Single product line item in an order."""
    sap: str
    quantity: int = Field(..., ge=0, description="Units (not cases)")
    enteredAt: Optional[datetime] = None
    
    # Forecast metadata
    forecastSuggestedUnits: Optional[float] = None
    forecastSuggestedCases: Optional[float] = None
    userAdjusted: Optional[bool] = None
    userDelta: Optional[int] = None
    forecastId: Optional[str] = None
    
    # Promo info
    promoActive: Optional[bool] = None
    promoLiftPct: Optional[float] = None
    
    # Calendar context
    isFirstWeekend: Optional[bool] = None
    
    # ML metadata
    confidence: Optional[float] = None
    source: Optional[str] = None  # 'baseline', 'promo_adjusted', 'historical'
    
    # Prior order context
    priorOrderContext: Optional[PriorOrderContext] = None
    
    # Expiry replacement
    expiryReplacement: Optional[ExpiryReplacement] = None
    
    @validator('sap')
    def validate_sap(cls, v):
        if not SAP_PATTERN.match(v):
            raise ValueError('Invalid SAP format')
        return v


class StoreOrder(BaseModel):
    """Items for a single store within an order."""
    storeId: str
    storeName: str
    items: List[OrderItem]
    enteredAt: Optional[datetime] = None
    completed: Optional[bool] = None
    
    @validator('storeId')
    def validate_store_id(cls, v):
        if not STORE_ID_PATTERN.match(v):
            raise ValueError('Invalid store ID format')
        return v


class Order(BaseModel):
    """Full order document (mirrors Firestore structure)."""
    id: str
    routeNumber: str
    userId: str
    orderDate: str  # ISO date string YYYY-MM-DD
    expectedDeliveryDate: str
    scheduleKey: Optional[str] = None
    status: str = Field(..., pattern='^(draft|finalized)$')
    stores: List[StoreOrder]
    createdAt: datetime
    updatedAt: datetime
    submittedAt: Optional[datetime] = None
    orderCycleId: Optional[str] = None
    notes: Optional[str] = None
    isHolidaySchedule: Optional[bool] = None


class OrderUpdateRequest(BaseModel):
    """Request to update an existing order."""
    stores: List[StoreOrder]
    notes: Optional[str] = None
    updatedAt: Optional[datetime] = None


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class UserInfo(BaseModel):
    """User information returned from auth verify."""
    uid: str
    email: Optional[str] = None
    displayName: Optional[str] = None
    routes: List[str]  # Routes user has access to
    currentRoute: Optional[str] = None
    role: Optional[str] = None  # 'owner' or 'team_member'


class HealthStatus(BaseModel):
    """Health check response."""
    status: str  # 'healthy', 'degraded', 'unhealthy'
    duckdb: Dict[str, Any]
    firebase: Dict[str, Any]
    timestamp: datetime


class DuckDBHealth(BaseModel):
    """DuckDB-specific health info."""
    status: str
    lastSync: Optional[datetime] = None
    syncAgeMinutes: Optional[int] = None
    fileSize: Optional[int] = None
    orderCount: Optional[int] = None


class OrderHistoryItem(BaseModel):
    """Single order in history list."""
    orderId: str
    routeNumber: str
    scheduleKey: str
    deliveryDate: str
    orderDate: Optional[str] = None
    finalizedAt: Optional[datetime] = None
    totalCases: int
    totalUnits: int
    storeCount: int
    status: str


class OrderHistoryResponse(BaseModel):
    """Paginated order history response."""
    items: List[OrderHistoryItem]
    total: int
    offset: int
    limit: int


class ErrorResponse(BaseModel):
    """Standard error response format."""
    error: str
    code: str
    details: Optional[Dict[str, Any]] = None


class ForecastItem(BaseModel):
    """Single item in a forecast."""
    storeId: str
    storeName: str
    sap: str
    productName: Optional[str] = None
    recommendedUnits: float
    recommendedCases: Optional[float] = None
    confidence: Optional[float] = None
    source: Optional[str] = None
    promoActive: Optional[bool] = None
    promoLiftPct: Optional[float] = None
    priorOrderContext: Optional[PriorOrderContext] = None
    expiryReplacement: Optional[ExpiryReplacement] = None


class ForecastPayload(BaseModel):
    """Forecast payload returned when available."""
    forecastId: str
    deliveryDate: str
    scheduleKey: str
    generatedAt: datetime
    items: List[ForecastItem]


class ForecastResponse(BaseModel):
    """Forecast availability response."""
    forecastAvailable: bool
    reason: Optional[str] = None  # 'insufficient_history' | 'no_data'
    forecast: Optional[ForecastPayload] = None
    isStale: Optional[bool] = None
    staleReason: Optional[str] = None
