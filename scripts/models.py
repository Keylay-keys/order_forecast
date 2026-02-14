"""Core dataclasses for the forecasting pipeline.

These classes are shared across loaders, trainers, and writers so we have a
typed contract for the data that moves between Firebase, DuckDB, and the ML
components.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any


# Common aliases
ISODateStr = str  # expected format: YYYY-MM-DD or MM/DD/YYYY
DocId = str


@dataclass
class Product:
    """Product metadata from the user's order guide / catalog."""

    sap: str
    name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    case_pack: Optional[int] = None  # units per case
    tray: Optional[int] = None  # pack size / tray for unit→case conversion
    additional: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StoreConfig:
    """Store configuration from Firebase (delivery days and active items)."""

    store_id: str
    store_name: str
    delivery_days: List[str] = field(default_factory=list)  # e.g., ["monday", "thursday"]
    active_saps: List[str] = field(default_factory=list)
    additional: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OrderItem:
    sap: str
    quantity: float
    cases: Optional[float] = None
    promo_active: bool = False
    promo_id: Optional[str] = None
    # Forecast feedback fields - for ML correction learning
    user_adjusted: bool = False  # True if user modified the forecasted quantity
    forecasted_quantity: Optional[float] = None  # Original forecast suggestion
    forecasted_cases: Optional[float] = None  # Original forecast cases
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StoreOrder:
    store_id: str
    store_name: str
    items: List[OrderItem] = field(default_factory=list)
    entered_at: Optional[datetime] = None


@dataclass
class Order:
    id: DocId
    route_number: str
    schedule_key: str  # monday, thursday, etc.
    expected_delivery_date: ISODateStr
    order_date: Optional[ISODateStr] = None
    status: str = "finalized"  # draft | finalized
    stores: List[StoreOrder] = field(default_factory=list)
    user_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriorOrderContext:
    """Context about a prior order for the same store/item in an overlapping delivery window."""
    order_id: str
    order_date: ISODateStr
    delivery_date: ISODateStr
    quantity: int
    schedule_key: str


@dataclass
class ExpiryReplacementInfo:
    """Metadata for items injected due to low-quantity expiry replacement."""
    expiry_date: str            # When current stock expires (YYYY-MM-DD)
    min_units_required: int     # Floor quantity (typically 1 case worth)
    reason: str = "low_qty_expiry"


@dataclass
class ForecastItem:
    store_id: str
    store_name: str
    sap: str
    recommended_units: float
    recommended_cases: Optional[float] = None
    p10_units: Optional[float] = None
    p50_units: Optional[float] = None
    p90_units: Optional[float] = None
    promo_active: bool = False
    promo_lift_pct: Optional[float] = None
    is_first_weekend: Optional[bool] = None
    confidence: Optional[float] = None
    source: Optional[str] = None
    prior_order_context: Optional[PriorOrderContext] = None  # If item was ordered in overlapping delivery
    last_order_quantity: Optional[int] = None  # Quantity ordered in the most recent same-schedule order
    expiry_replacement: Optional[ExpiryReplacementInfo] = None  # If item injected due to low-qty expiry
    whole_case_adjustment: Optional[Dict[str, Any]] = None  # Metadata when whole-case enforcement adjusted units


@dataclass
class ForecastPayload:
    forecast_id: str
    route_number: str
    delivery_date: ISODateStr
    schedule_key: str
    generated_at: datetime
    items: List[ForecastItem] = field(default_factory=list)


@dataclass
class ForecastResponse:
    forecast_available: bool
    forecast: Optional[ForecastPayload] = None
    reason: Optional[str] = None  # e.g., insufficient_history, no_data


@dataclass
class Correction:
    """User correction/feedback row."""

    forecast_id: str
    order_id: str
    route_number: str
    schedule_key: str
    delivery_date: ISODateStr
    store_id: str
    sap: str
    predicted_units: float
    final_units: float
    promo_active: bool = False
    promo_id: Optional[str] = None
    submitted_at: Optional[datetime] = None

    @property
    def correction_delta(self) -> float:
        return self.final_units - self.predicted_units

    @property
    def correction_ratio(self) -> float:
        if self.predicted_units == 0:
            return 0.0
        return self.final_units / self.predicted_units
