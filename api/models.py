"""Pydantic models for the Web Portal API.

These models mirror the TypeScript interfaces in src/types/order.ts
to ensure compatibility between mobile app and web portal.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Optional, Dict, Any, Literal
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


# =============================================================================
# INPUT VALIDATION PATTERNS
# =============================================================================

ROUTE_NUMBER_PATTERN = re.compile(r'^\d{1,10}$')
SCHEDULE_KEY_PATTERN = re.compile(r'^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$')
SAP_PATTERN = re.compile(r'^[\w\-]{1,20}$')
STORE_ID_PATTERN = re.compile(r'^[\w\-]{1,50}$')


def normalize_sap_order(
    value: Optional[List[str]],
    *,
    allow_none: bool = True,
) -> Optional[List[str]]:
    """Validate one bounded canonical SAP sequence without coercion.

    This validator is shared by the legacy order compatibility field and the
    Portal preference endpoint.  ``fullmatch`` is deliberate: ``re.match``
    accepts a trailing newline and weakens the canonical token boundary.
    """
    if value is None:
        if allow_none:
            return None
        raise ValueError('sapOrder is required')
    if not isinstance(value, list):
        raise ValueError('sapOrder must be an array')
    if len(value) > 2000:
        raise ValueError('sapOrder must contain at most 2000 items')
    if any(not isinstance(item, str) for item in value):
        raise ValueError('sapOrder must contain only strings')
    if len(value) != len(set(value)):
        raise ValueError('sapOrder must not contain duplicates')
    if any(SAP_PATTERN.fullmatch(item) is None for item in value):
        raise ValueError('sapOrder contains an invalid SAP')
    return list(value)


def _parse_firestore_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if hasattr(value, "to_datetime"):
        try:
            return value.to_datetime()
        except Exception:
            return None
    if isinstance(value, dict):
        seconds = value.get("seconds") or value.get("_seconds")
        nanos = value.get("nanoseconds") or value.get("_nanoseconds") or value.get("nanos")
        if seconds is not None:
            try:
                return datetime.fromtimestamp(
                    seconds + (nanos or 0) / 1_000_000_000,
                    tz=timezone.utc,
                )
            except Exception:
                return None
    return None


# =============================================================================
# REQUEST MODELS (Input Validation)
# =============================================================================

class HistoryRequest(BaseModel):
    """Request parameters for order history."""
    route: str = Field(..., description="Route number (digits only)")
    weeks: int = Field(default=12, ge=1, le=52, description="Number of weeks of history")
    
    @field_validator('route')
    @classmethod
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
    allowUnresolvedHolidaySchedule: bool = False
    notes: Optional[str] = None
    coreItemPolicyVersion: Optional[int] = Field(default=None, ge=1, le=1)
    requestId: Optional[str] = Field(default=None, pattern=r'^[A-Za-z0-9_-]{16,64}$')
    expectedScheduleRevision: Optional[int] = Field(default=None, strict=True, ge=1, le=9007199254740991)
    
    @field_validator('routeNumber')
    @classmethod
    def validate_route(cls, v):
        if not ROUTE_NUMBER_PATTERN.match(v):
            raise ValueError('Route must be 1-10 digits')
        return v
    
    @field_validator('scheduleKey')
    @classmethod
    def validate_schedule_key(cls, v):
        if not SCHEDULE_KEY_PATTERN.match(v.lower()):
            raise ValueError('scheduleKey must be a day of week')
        return v.lower()
    
    @field_validator('deliveryDate')
    @classmethod
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
    
    @field_validator('route')
    @classmethod
    def validate_route(cls, v):
        if not ROUTE_NUMBER_PATTERN.match(v):
            raise ValueError('Route must be 1-10 digits')
        return v
    
    @field_validator('scheduleKey')
    @classmethod
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


class WholeCaseAdjustment(BaseModel):
    """Whole-case enforcement adjustment metadata."""
    adjustedUnits: int
    preEnforcementUnits: Optional[int] = None
    trigger: str  # 'promo_history' | 'low_qty_expiry' | 'allocation_remainder'
    targetStoreId: Optional[str] = None


class InboundTransferUse(BaseModel):
    """Inbound transfer consumption record (units drawn from shared pool)."""
    transferKey: str
    fromRouteNumber: str
    sap: str
    units: int = Field(..., ge=0)
    casePack: int = Field(default=1, ge=1)
    transferDate: Optional[str] = None
    sourceOrderId: Optional[str] = None


class RouteTransferAllocation(BaseModel):
    """Outbound route transfer allocation (route splitting)."""
    sap: str
    toRouteNumber: str
    units: int = Field(..., ge=0)
    casePack: int = Field(default=1, ge=1)
    transferDate: Optional[str] = None
    sourceOrderId: Optional[str] = None


class InboundTransferStoreAllocation(BaseModel):
    """Inbound transfer units assigned into one store/SAP order row."""
    transferKey: str
    storeId: str
    sap: str
    units: int = Field(..., ge=0)
    fromRouteNumber: str
    casePack: int = Field(default=1, ge=1)
    transferDate: Optional[str] = None
    sourceOrderId: Optional[str] = None


class CoreItemOverride(BaseModel):
    """Per-order acknowledgement allowing one zero-quantity Core item."""
    storeId: str
    sap: str
    overriddenAtMs: int = Field(..., ge=1)
    overriddenByUserId: str


class OrderMutation(BaseModel):
    """Server-owned description of the most recent canonical order mutation."""
    kind: Literal['finalization', 'store_reallocation', 'full_adjustment']
    mutationId: str = Field(..., min_length=1, max_length=255)
    atMs: int = Field(..., ge=1)

    model_config = ConfigDict(extra='forbid')


class StoreReallocationSummary(BaseModel):
    """Compact order-level marker used by current and archived history."""
    count: int = Field(..., ge=1)
    lastAppliedAtMs: int = Field(..., ge=1)
    lastAdjustmentId: str = Field(..., min_length=1, max_length=255)

    model_config = ConfigDict(extra='forbid')


class StoreReallocationMoveRequest(BaseModel):
    """Semantic move input. Display metadata is always resolved server-side."""
    sap: str
    fromStoreId: str
    toStoreId: str
    unitQuantity: int = Field(..., ge=1, le=1_000_000)

    @field_validator('sap')
    @classmethod
    def validate_move_sap(cls, value: str) -> str:
        normalized = value.strip()
        if not SAP_PATTERN.fullmatch(normalized):
            raise ValueError('Invalid SAP format')
        return normalized

    @field_validator('fromStoreId', 'toStoreId')
    @classmethod
    def validate_move_store_id(cls, value: str) -> str:
        normalized = value.strip()
        if not STORE_ID_PATTERN.fullmatch(normalized):
            raise ValueError('Invalid store ID format')
        return normalized

    model_config = ConfigDict(extra='forbid')


class StoreReallocationRequest(BaseModel):
    reallocationId: str = Field(..., min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    baseOrderRevision: int = Field(..., ge=0)
    moves: List[StoreReallocationMoveRequest] = Field(..., min_length=1, max_length=250)

    @field_validator('moves')
    @classmethod
    def validate_unique_moves(
        cls,
        moves: List[StoreReallocationMoveRequest],
    ) -> List[StoreReallocationMoveRequest]:
        keys = [(move.sap, move.fromStoreId, move.toStoreId) for move in moves]
        if len(keys) != len(set(keys)):
            raise ValueError('Duplicate store reallocation move')
        return moves

    model_config = ConfigDict(extra='forbid')


class StoreReallocationResponse(BaseModel):
    orderId: str
    reallocationId: str
    orderRevision: int = Field(..., ge=1)
    appliedAtMs: int = Field(..., ge=1)
    reallocationCount: int = Field(..., ge=1)
    idempotent: bool
    projectionStatus: Literal['pending', 'succeeded', 'failed']


class FullOrderAdjustmentConfirmRequest(BaseModel):
    adjustmentId: str = Field(..., min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    sentBatchId: str = Field(..., min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    # Legacy clients may still send this audit hint. Confirmation authority comes
    # from the immutable sent batch, never from a mutable client order snapshot.
    baseOrderRevision: Optional[int] = Field(default=None, ge=0)
    acceptedSaps: List[str] = Field(default_factory=list, max_length=250)
    notes: Optional[str] = Field(default=None, max_length=2000)

    model_config = ConfigDict(extra="forbid")

    @field_validator("acceptedSaps")
    @classmethod
    def validate_accepted_saps(cls, value: List[str]) -> List[str]:
        normalized = [str(sap).strip() for sap in value]
        if any(not sap or len(sap) > 20 for sap in normalized):
            raise ValueError("acceptedSaps contains an invalid SAP")
        if len(set(normalized)) != len(normalized):
            raise ValueError("acceptedSaps contains duplicates")
        return normalized


class FullOrderAdjustmentConfirmResponse(BaseModel):
    orderId: str
    adjustmentId: str
    orderRevision: int
    changed: bool
    idempotent: bool
    projectionStatus: Optional[Literal['pending', 'succeeded', 'failed']] = None

    model_config = ConfigDict(extra='forbid')


class AdjustmentReminderReconcileRequest(BaseModel):
    expectedSettingsUpdatedAtMs: Optional[int] = Field(default=None, ge=0)

    model_config = ConfigDict(extra="forbid")


class AdjustmentReminderReconcileResponse(BaseModel):
    orderId: str
    adjustmentId: str
    changed: bool
    fields: Dict[str, Any]

    model_config = ConfigDict(extra="forbid")


class OrderItem(BaseModel):
    """Single product line item in an order."""
    sap: str
    quantity: int = Field(..., ge=0, description="Units (not cases)")
    enteredAt: Optional[datetime] = None
    
    # Forecast metadata
    forecastRecommendedUnits: Optional[float] = Field(None, alias="forecastSuggestedUnits")
    forecastRecommendedCases: Optional[float] = Field(None, alias="forecastSuggestedCases")
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

    @field_validator('enteredAt', mode='before')
    @classmethod
    def validate_entered_at(cls, v):
        parsed = _parse_firestore_timestamp(v)
        return parsed if parsed is not None else v

    # Expiry replacement
    expiryReplacement: Optional[ExpiryReplacement] = None

    # Whole-case adjustment
    wholeCaseAdjustment: Optional[WholeCaseAdjustment] = None
    
    @field_validator('sap')
    @classmethod
    def validate_sap(cls, v):
        if not SAP_PATTERN.match(v):
            raise ValueError('Invalid SAP format')
        return v

    # Keep support for the full declared Pydantic >=2.0 range. This preserves
    # the API's ability to accept either the field name or its legacy alias.
    model_config = ConfigDict(populate_by_name=True)


class StoreOrder(BaseModel):
    """Items for a single store within an order."""
    storeId: str
    storeName: str
    items: List[OrderItem]
    enteredAt: Optional[datetime] = None
    completed: Optional[bool] = None
    
    @field_validator('storeId')
    @classmethod
    def validate_store_id(cls, v):
        if not STORE_ID_PATTERN.match(v):
            raise ValueError('Invalid store ID format')
        return v

    @field_validator('enteredAt', mode='before')
    @classmethod
    def validate_store_entered_at(cls, v):
        parsed = _parse_firestore_timestamp(v)
        return parsed if parsed is not None else v


class ScheduledOrder(BaseModel):
    """Immutable cycle-day authority used by source-backed adjustment scheduling."""

    version: Literal[2]
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    timezone: str = Field(min_length=1, max_length=64)
    scheduleKey: str = Field(pattern=SCHEDULE_KEY_PATTERN.pattern)
    orderDay: int = Field(ge=1, le=7, strict=True)
    loadOffsetDays: int = Field(ge=0, le=60, strict=True)
    deliveryOffsetDays: int = Field(ge=0, le=60, strict=True)
    provenance: Literal["cycle", "backfill"]

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_cycle_identity(self) -> "ScheduledOrder":
        try:
            stamped_date = date.fromisoformat(self.date)
            ZoneInfo(self.timezone)
        except (TypeError, ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("Invalid scheduled order date or timezone") from exc
        day_keys = ("", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
        if stamped_date.isoweekday() != self.orderDay or self.scheduleKey != day_keys[self.orderDay]:
            raise ValueError("Scheduled order weekday identity is inconsistent")
        if self.deliveryOffsetDays < self.loadOffsetDays:
            raise ValueError("Delivery offset must not precede load offset")
        return self


class ScheduledOrderV3(ScheduledOrder):
    """Self-contained source evidence. Never reconstructed from current settings."""

    version: Literal[3]
    provenance: Literal["cycle"]
    sendDate: str = Field(pattern=r"^[1-9]\d{3}-\d{2}-\d{2}$")
    sendOffsetDays: int = Field(ge=0, le=60, strict=True)
    sourceRevision: int = Field(ge=1, le=9007199254740991, strict=True)
    sourceOwnerUid: str = Field(pattern=r"^[A-Za-z0-9_-]{1,128}$")
    sourceRouteNumber: str = Field(pattern=r"^[0-9]{1,10}$")

    @model_validator(mode="after")
    def validate_source_identity(self) -> "ScheduledOrderV3":
        if self.sendOffsetDays > self.loadOffsetDays:
            raise ValueError("Send offset must not follow pickup")
        if not re.fullmatch(r"[1-9]\d{3}-\d{2}-\d{2}", self.date, re.ASCII):
            raise ValueError("Invalid start date")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.sourceOwnerUid):
            raise ValueError("Invalid source owner")
        if not re.fullmatch(r"[0-9]{1,10}", self.sourceRouteNumber):
            raise ValueError("Invalid source route")
        try:
            from .errors import StructuredApiError
            from .schedule_source_contract import validate_timezone
            validate_timezone(self.timezone)
            start = date.fromisoformat(self.date)
            if (start + timedelta(days=self.sendOffsetDays)).isoformat() != self.sendDate:
                raise ValueError("Send date does not match saved offset")
            start + timedelta(days=self.deliveryOffsetDays)
        except (ValueError, OverflowError, StructuredApiError) as exc:
            raise ValueError("Invalid V3 schedule evidence") from exc
        return self


class AdjustmentAnchor(BaseModel):
    """Immutable server finalization authority for an adjustment window."""

    version: Literal[1]
    finalizedAtMs: int = Field(gt=0, strict=True)
    finalizedDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    timezone: str = Field(min_length=1, max_length=64)

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def validate_finalization_identity(self) -> "AdjustmentAnchor":
        try:
            finalized_date = date.fromisoformat(self.finalizedDate)
            zone = ZoneInfo(self.timezone)
            instant_date = datetime.fromtimestamp(
                self.finalizedAtMs / 1000, timezone.utc
            ).astimezone(zone).date()
        except (ValueError, OSError, ZoneInfoNotFoundError) as exc:
            raise ValueError("Adjustment finalization authority is invalid") from exc
        if instant_date != finalized_date:
            raise ValueError("Adjustment finalization date is inconsistent")
        return self


class Order(BaseModel):
    """Full order document (mirrors Firestore structure)."""
    id: str
    routeNumber: str
    userId: str
    orderDate: str  # ISO date string YYYY-MM-DD
    expectedDeliveryDate: str
    expectedLoadDate: Optional[str] = None
    scheduleKey: Optional[str] = None
    scheduledOrder: Optional[ScheduledOrder | ScheduledOrderV3] = None
    adjustmentAnchor: Optional[AdjustmentAnchor] = None

    @model_validator(mode="after")
    def validate_saved_schedule_link(self) -> "Order":
        if isinstance(self.scheduledOrder, ScheduledOrderV3):
            from .order_schedule_evidence import validate_order_evidence
            validate_order_evidence(self.model_dump())
        return self

    status: str = Field(..., pattern='^(draft|finalized)$')
    stores: List[StoreOrder]
    createdAt: datetime
    updatedAt: datetime
    submittedAt: Optional[datetime] = None
    orderCycleId: Optional[str] = None
    notes: Optional[str] = None
    isHolidaySchedule: Optional[bool] = None
    orderAdjustmentAppliedAtMs: Optional[int] = None
    orderRevision: Optional[int] = Field(default=None, ge=0)
    lastMutation: Optional[OrderMutation] = None
    storeReallocationSummary: Optional[StoreReallocationSummary] = None
    coreItemPolicyVersion: Optional[int] = Field(default=None, ge=1, le=1)
    coreItemOverrides: Optional[List[CoreItemOverride]] = None
    # User-defined SAP row order for the portal editor and finalized printouts.
    sapOrder: Optional[List[str]] = None
    forecastContext: Optional[Dict[str, Any]] = None

    # Transfer metadata (route splitting)
    routeTransfers: Optional[List[RouteTransferAllocation]] = None
    inboundTransfersUsed: Optional[List[InboundTransferUse]] = None
    inboundTransferStoreAllocations: Optional[List[InboundTransferStoreAllocation]] = None
    routeSplittingEnabled: Optional[bool] = None

    # Some legacy writers have persisted Firestore timestamps as JSON-like maps:
    #   {"seconds": ..., "nanoseconds": ...}
    # instead of native Timestamp types. Accept and coerce these into datetimes
    # so API reads don't 500 on old/bad data.
    @field_validator('createdAt', 'updatedAt', 'submittedAt', mode='before')
    @classmethod
    def validate_order_timestamps(cls, v):
        parsed = _parse_firestore_timestamp(v)
        return parsed if parsed is not None else v


class OrderUpdateRequest(BaseModel):
    """Request to update an existing order."""
    stores: List[StoreOrder]
    notes: Optional[str] = None
    updatedAt: Optional[datetime] = None

    # Transfer metadata (web portal parity)
    routeTransfers: Optional[List[RouteTransferAllocation]] = None
    inboundTransfersUsed: Optional[List[InboundTransferUse]] = None
    routeSplittingEnabled: Optional[bool] = None
    sapOrder: Optional[List[str]] = Field(default=None, max_length=2000)

    @field_validator('sapOrder')
    @classmethod
    def validate_sap_order(cls, v):
        return normalize_sap_order(v)

    @field_validator('updatedAt', mode='before')
    @classmethod
    def validate_updated_at(cls, v):
        parsed = _parse_firestore_timestamp(v)
        return parsed if parsed is not None else v


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
    ownedPrimaryRoute: Optional[str] = None
    ownedRoutes: List[str] = Field(default_factory=list)


class HealthStatus(BaseModel):
    """Health check response."""
    status: str  # 'healthy', 'degraded', 'unhealthy'
    database: Dict[str, Any]
    firebase: Dict[str, Any]
    timestamp: datetime


class DatabaseHealth(BaseModel):
    """Database health info."""
    status: str
    lastSync: Optional[datetime] = None
    syncAgeMinutes: Optional[int] = None
    orderCount: Optional[int] = None
    routesCount: Optional[int] = None


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
    orderRevision: Optional[int] = Field(default=None, ge=0)
    storeReallocationSummary: Optional[StoreReallocationSummary] = None


class OrderHistoryResponse(BaseModel):
    """Paginated order history response."""
    items: List[OrderHistoryItem]
    total: int
    offset: int
    limit: int
    nextCursor: Optional[str] = None


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
    p10Units: Optional[float] = None
    p50Units: Optional[float] = None
    p90Units: Optional[float] = None
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


class OrderForecastSnapshotItem(BaseModel):
    storeId: str
    sap: str
    recommendedUnits: int = Field(..., ge=0)
    source: str
    promoActive: Optional[bool] = None


class OrderForecastContext(BaseModel):
    schemaVersion: Literal[2]
    forecastId: str
    generationMode: Literal['model', 'last_order']
    forecastDeliveryDate: str
    orderDeliveryDate: str
    scheduleKey: str
    generatedAt: str
    attachedAt: datetime
    activeCarryItemCount: int = Field(..., ge=0)
    eligibleItemCount: int = Field(..., ge=0)
    artifactFingerprint: str
    generationInputFingerprint: str
    activeCarryFingerprint: str
    eligibilityFingerprint: str
    items: List[OrderForecastSnapshotItem]


class AttachForecastResponse(BaseModel):
    orderId: str
    forecastId: str
    forecastItemCount: int
    forecastContext: OrderForecastContext
    updatedAt: datetime
    attachedAt: datetime
    idempotent: bool


class ForecastPreparingResponse(BaseModel):
    status: Literal['preparing'] = 'preparing'
    reason: Literal['missing', 'expired', 'unsupported_or_unready', 'incomplete']
    jobId: Optional[str] = None
    retryAfterSeconds: int = 5
    deliveryDate: str
    scheduleKey: str


class ForecastResponse(BaseModel):
    """Forecast availability response."""
    forecastAvailable: bool
    reason: Optional[str] = None  # 'insufficient_history' | 'no_data'
    forecast: Optional[ForecastPayload] = None
    isStale: Optional[bool] = None
    staleReason: Optional[str] = None


class ForecastStatusResponse(BaseModel):
    """Route forecast readiness and model-training status."""
    orderCount: Optional[int] = None
    minOrdersRequired: Optional[int] = None
    hasTrainedModel: bool = False
    forecastAvailable: bool = False
    forecastMode: Optional[Literal['last_order', 'model']] = None
    preparationStatus: Optional[Literal['queued', 'running', 'failed']] = None
    preparationFailureReason: Optional[
        Literal['insufficient_history', 'generation_failed']
    ] = None
    lastUpdated: Optional[datetime] = None
    lastFinalizedAt: Optional[datetime] = None


class ApplyForecastItem(BaseModel):
    """Item to apply from forecast to order."""
    storeId: str
    storeName: Optional[str] = None
    sap: str
    recommendedUnits: float
    recommendedCases: Optional[float] = None
    p10Units: Optional[float] = None
    p50Units: Optional[float] = None
    p90Units: Optional[float] = None
    confidence: Optional[float] = None
    source: Optional[str] = None
    promoActive: Optional[bool] = None
    promoLiftPct: Optional[float] = None
    isFirstWeekend: Optional[bool] = None
    priorOrderContext: Optional[Dict[str, Any]] = None
    expiryReplacement: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(extra="forbid")


class ApplyForecastRequest(BaseModel):
    """Request to apply forecast items to an order."""
    forecastId: str = ""
    items: List[ApplyForecastItem] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")
