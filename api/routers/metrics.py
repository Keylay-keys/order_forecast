"""Owner-only settlement metrics for the web portal.

The endpoint reads reviewed, normalized settlement facts. It never parses source
documents and never treats OCR candidates as financial truth.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import logging
import math
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from google.cloud import firestore
from pydantic import BaseModel, Field, field_validator, model_validator

from ..dependencies import get_firestore, verify_firebase_token
from ..middleware.rate_limit import rate_limit_history, rate_limit_write
from ..business_access import require_business_access


router = APIRouter()
logger = logging.getLogger("api.metrics")

SETTLEMENT_COLLECTION = "settlements"
EXPENSE_COLLECTION = "business_expenses"
PAYROLL_COLLECTION = "payroll_costs"
SUPPORTED_SCHEMA_VERSION = 1
ACCEPTED_REVIEW_STATUS = "accepted"
CENT = Decimal("0.01")
MAX_ABS_MONEY = Decimal("1000000000")


class ExpenseCreateRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    category: Literal[
        "fuel",
        "vehicle",
        "insurance",
        "storage",
        "supplies",
        "maintenance",
        "professional",
        "licenses_tax",
        "technology",
        "other",
    ]
    amount: Decimal = Field(..., gt=0, le=MAX_ABS_MONEY, max_digits=12, decimal_places=2)
    vendor: Optional[str] = Field(default=None, max_length=120)
    note: Optional[str] = Field(default=None, max_length=500)

    @field_validator("date")
    @classmethod
    def validate_date(cls, value: str) -> str:
        return _date(value, "date")

    @field_validator("vendor", "note", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = " ".join(str(value).split()).strip()
        return text or None


class PayrollCreateRequest(BaseModel):
    routeNumber: str = Field(..., pattern=r"^\d{1,10}$")
    periodStart: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    periodEnd: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    workerLabel: str = Field(..., min_length=1, max_length=120)
    workerType: Literal["owner", "w2_employee", "contractor_1099"]
    payType: Literal["hourly", "salary", "flat"]
    hours: Optional[Decimal] = Field(default=None, ge=0, le=744, max_digits=6, decimal_places=2)
    hourlyRate: Optional[Decimal] = Field(default=None, ge=0, le=10000, max_digits=8, decimal_places=2)
    grossPay: Decimal = Field(..., ge=0, le=MAX_ABS_MONEY, max_digits=12, decimal_places=2)
    employerCosts: Decimal = Field(default=Decimal("0"), ge=0, le=MAX_ABS_MONEY, max_digits=12, decimal_places=2)
    reimbursements: Decimal = Field(default=Decimal("0"), ge=0, le=MAX_ABS_MONEY, max_digits=12, decimal_places=2)
    status: Literal["accrued", "paid"] = "accrued"
    paidDate: Optional[str] = Field(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    note: Optional[str] = Field(default=None, max_length=500)

    @field_validator("periodStart", "periodEnd", "paidDate")
    @classmethod
    def validate_dates(cls, value: Optional[str], info: Any) -> Optional[str]:
        return _date(value, info.field_name) if value else None

    @field_validator("workerLabel", "note", mode="before")
    @classmethod
    def normalize_text(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        text = " ".join(str(value).split()).strip()
        return text or None

    @model_validator(mode="after")
    def validate_period_and_pay(self):
        if self.periodEnd < self.periodStart:
            raise ValueError("periodEnd must be on or after periodStart")
        if self.status == "paid" and not self.paidDate:
            raise ValueError("paidDate is required when status is paid")
        if self.payType == "hourly" and (self.hours is None or self.hourlyRate is None):
            raise ValueError("hours and hourlyRate are required for hourly payroll")
        return self


def _money(value: Any, field: str, *, required: bool = True) -> Optional[Decimal]:
    if value is None or value == "":
        if required:
            raise ValueError(f"missing {field}")
        return None
    if isinstance(value, bool):
        raise ValueError(f"invalid {field}")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid {field}") from exc
    if not amount.is_finite() or abs(amount) > MAX_ABS_MONEY:
        raise ValueError(f"invalid {field}")
    return amount.quantize(CENT, rounding=ROUND_HALF_UP)


def _date(value: Any, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"invalid {field}") from exc
    return parsed.strftime("%Y-%m-%d")


def _iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if not math.isfinite(float(value)):
            return None
        return datetime.utcfromtimestamp(float(value) / 1000).isoformat() + "Z"
    text = str(value).strip()
    return text or None


def _float(value: Optional[Decimal]) -> Optional[float]:
    return float(value) if value is not None else None


def _optional_money_map(source: Any, fields: List[str]) -> Dict[str, Optional[float]]:
    data = source if isinstance(source, dict) else {}
    return {
        field: _float(_money(data.get(field), field, required=False))
        for field in fields
    }


def _normalize_settlement(snapshot: Any, route: str) -> Dict[str, Any]:
    data = snapshot.to_dict() or {}
    if int(data.get("schemaVersion") or 0) != SUPPORTED_SCHEMA_VERSION:
        raise ValueError("unsupported schemaVersion")
    if str(data.get("reviewStatus") or "").strip().lower() != ACCEPTED_REVIEW_STATUS:
        raise ValueError("settlement is not accepted")
    if str(data.get("routeNumber") or "").strip() != route:
        raise ValueError("route mismatch")

    period_start = _date(data.get("periodStart"), "periodStart")
    period_end = _date(data.get("periodEnd"), "periodEnd")
    if period_end < period_start:
        raise ValueError("period end precedes period start")
    currency = str(data.get("currency") or "USD").strip().upper()
    if currency != "USD":
        raise ValueError("unsupported currency")

    pay = data.get("pay") if isinstance(data.get("pay"), dict) else {}
    total_purchases = _money(pay.get("totalPurchases"), "pay.totalPurchases")
    non_sbt_gross = _money(pay.get("grossSalesLessBuyback"), "pay.grossSalesLessBuyback")
    sbt_gross = _money(pay.get("sbtGross"), "pay.sbtGross")
    margin_discount = _money(pay.get("totalMarginDiscount"), "pay.totalMarginDiscount")
    sales_returns = _money(pay.get("salesReturns"), "pay.salesReturns")
    net_cash_sales = _money(pay.get("netCashSales"), "pay.netCashSales")
    hhc_rental = _money(pay.get("hhcRentalFee"), "pay.hhcRentalFee")
    sbt_credits = _money(pay.get("sbtCredits"), "pay.sbtCredits")
    prior_adjustments = _money(pay.get("priorAdjustments"), "pay.priorAdjustments")
    current_adjustments = _money(pay.get("currentAdjustments"), "pay.currentAdjustments")
    stated_new_settlement = _money(pay.get("newSettlement"), "pay.newSettlement")

    magnitude_fields = {
        "totalPurchases": total_purchases,
        "salesReturns": sales_returns,
        "netCashSales": net_cash_sales,
        "hhcRentalFee": hhc_rental,
        "sbtCredits": sbt_credits,
    }
    if any(value is not None and value < 0 for value in magnitude_fields.values()):
        raise ValueError("deduction magnitudes must be nonnegative")

    components = [
        ("totalPurchases", "Product purchased", -total_purchases),
        ("grossSalesLessBuyback", "Non-SBT gross sales", non_sbt_gross),
        ("sbtGross", "SBT gross sales", sbt_gross),
        ("totalMarginDiscount", "Margin credited", margin_discount),
        ("salesReturns", "Sales returns", -sales_returns),
        ("netCashSales", "Cash already retained", -net_cash_sales),
        ("hhcRentalFee", "HHC rental fee", -hhc_rental),
        ("sbtCredits", "SBT credits", -sbt_credits),
        ("priorAdjustments", "Prior adjustments", prior_adjustments),
        ("currentAdjustments", "Current adjustments", current_adjustments),
    ]
    calculated = sum((amount for _, _, amount in components), Decimal("0")).quantize(CENT)
    difference = (stated_new_settlement - calculated).quantize(CENT)

    account = _optional_money_map(
        data.get("account"),
        ["previousBalance", "payment", "totalNewSettlement", "amountToPay"],
    )
    margin = _optional_money_map(
        data.get("margin"),
        [
            "marginOnPurchases",
            "marginDiscountPromotions",
            "marginDiscountSbt",
            "totalMarginDiscount",
        ],
    )
    sales = _optional_money_map(
        data.get("sales"),
        ["grossSales", "returns", "promotions", "networkAllowances", "netSales"],
    )
    inventory = _optional_money_map(
        data.get("inventory"),
        ["initialInventory", "currentInventory", "truckInventoryChange"],
    )

    return {
        "id": str(snapshot.id),
        "routeNumber": route,
        "schemaVersion": SUPPORTED_SCHEMA_VERSION,
        "currency": currency,
        "periodStart": period_start,
        "periodEnd": period_end,
        "statementReceivedAt": _iso(data.get("statementReceivedAt")),
        "availableAt": _iso(data.get("availableAt")),
        "pay": {
            "components": [
                {"key": key, "label": label, "amount": float(amount)}
                for key, label, amount in components
            ],
            "calculatedNewSettlement": float(calculated),
            "statedNewSettlement": float(stated_new_settlement),
            "difference": float(difference),
            "reconciled": abs(difference) <= CENT,
        },
        "account": account,
        "margin": margin,
        "sales": sales,
        "inventory": inventory,
        "source": {
            "documentId": str(data.get("sourceDocumentId") or ""),
            "pageCount": max(0, min(int(data.get("sourcePageCount") or 0), 100)),
            "reviewStatus": ACCEPTED_REVIEW_STATUS,
            "reviewedAt": _iso(data.get("reviewedAt")),
        },
    }


@router.get("/metrics/settlements")
@rate_limit_history
async def list_settlement_metrics(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$", description="Route number"),
    limit: int = Query(default=13, ge=1, le=104),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await require_business_access(route, decoded_token, db)

    collection = db.collection("routes").document(route).collection(SETTLEMENT_COLLECTION)
    snapshots = list(
        collection.order_by("periodEnd", direction=firestore.Query.DESCENDING)
        .limit(min(limit * 2, 208))
        .stream()
    )

    items: List[Dict[str, Any]] = []
    for snapshot in snapshots:
        try:
            item = _normalize_settlement(snapshot, route)
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping unusable reviewed settlement %s: %s", snapshot.id, exc)
            continue
        items.append(item)

    items.sort(key=lambda item: (item["periodEnd"], item["id"]), reverse=True)
    items = items[:limit]
    return {
        "routeNumber": route,
        "hasData": bool(items),
        "latest": items[0] if items else None,
        "items": items,
    }


async def _require_owner(route: str, decoded_token: dict, db: firestore.Client) -> Dict[str, Any]:
    return await require_business_access(route, decoded_token, db)


def _bounded_text(value: Any, maximum: int) -> Optional[str]:
    if value is None:
        return None
    text = " ".join(str(value).split()).strip()
    return text[:maximum] or None


def _serialize_expense(snapshot: Any, route: str) -> Optional[Dict[str, Any]]:
    data = snapshot.to_dict() or {}
    if data.get("archivedAt") is not None or str(data.get("routeNumber") or "") != route:
        return None
    try:
        amount = _money(data.get("amount"), "amount")
        expense_date = _date(data.get("date"), "date")
    except ValueError:
        return None
    return {
        "id": str(snapshot.id),
        "routeNumber": route,
        "date": expense_date,
        "category": _bounded_text(data.get("category"), 40) or "other",
        "amount": float(amount),
        "vendor": _bounded_text(data.get("vendor"), 120),
        "note": _bounded_text(data.get("note"), 500),
        "createdAt": _iso(data.get("createdAt")),
        "updatedAt": _iso(data.get("updatedAt")),
    }


def _serialize_payroll(snapshot: Any, route: str) -> Optional[Dict[str, Any]]:
    data = snapshot.to_dict() or {}
    if data.get("archivedAt") is not None or str(data.get("routeNumber") or "") != route:
        return None
    try:
        period_start = _date(data.get("periodStart"), "periodStart")
        period_end = _date(data.get("periodEnd"), "periodEnd")
        gross_pay = _money(data.get("grossPay"), "grossPay")
        employer_costs = _money(data.get("employerCosts") or 0, "employerCosts")
        reimbursements = _money(data.get("reimbursements") or 0, "reimbursements")
        hours = _money(data.get("hours"), "hours", required=False)
        hourly_rate = _money(data.get("hourlyRate"), "hourlyRate", required=False)
    except ValueError:
        return None
    total_cost = (gross_pay + employer_costs + reimbursements).quantize(CENT)
    return {
        "id": str(snapshot.id),
        "routeNumber": route,
        "periodStart": period_start,
        "periodEnd": period_end,
        "workerLabel": _bounded_text(data.get("workerLabel"), 120) or "Worker",
        "workerType": str(data.get("workerType") or "unspecified"),
        "payType": str(data.get("payType") or "flat"),
        "hours": _float(hours),
        "hourlyRate": _float(hourly_rate),
        "grossPay": float(gross_pay),
        "employerCosts": float(employer_costs),
        "reimbursements": float(reimbursements),
        "totalCost": float(total_cost),
        "status": "paid" if data.get("status") == "paid" else "accrued",
        "paidDate": data.get("paidDate") if data.get("paidDate") else None,
        "note": _bounded_text(data.get("note"), 500),
        "createdAt": _iso(data.get("createdAt")),
        "updatedAt": _iso(data.get("updatedAt")),
    }


def _date_in_range(value: str, start_date: Optional[str], end_date: Optional[str]) -> bool:
    return (start_date is None or value >= start_date) and (end_date is None or value <= end_date)


@router.get("/metrics/expenses")
@rate_limit_history
async def list_expenses(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    startDate: Optional[str] = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    endDate: Optional[str] = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=250, ge=1, le=500),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(route, decoded_token, db)
    start = _date(startDate, "startDate") if startDate else None
    end = _date(endDate, "endDate") if endDate else None
    if start and end and end < start:
        raise HTTPException(400, "endDate must be on or after startDate")

    collection = db.collection("routes").document(route).collection(EXPENSE_COLLECTION)
    snapshots = list(collection.order_by("date", direction=firestore.Query.DESCENDING).limit(limit * 2).stream())
    items = []
    for snapshot in snapshots:
        item = _serialize_expense(snapshot, route)
        if item and _date_in_range(item["date"], start, end):
            items.append(item)
    items.sort(key=lambda item: (item["date"], item["id"]), reverse=True)
    items = items[:limit]
    return {"routeNumber": route, "items": items, "total": len(items)}


@router.post("/metrics/expenses")
@rate_limit_write
async def create_expense(
    request: Request,
    payload: ExpenseCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(payload.routeNumber, decoded_token, db)
    now = datetime.now(timezone.utc)
    ref = db.collection("routes").document(payload.routeNumber).collection(EXPENSE_COLLECTION).document()
    stored = {
        "routeNumber": payload.routeNumber,
        "date": payload.date,
        "category": payload.category,
        "amount": float(payload.amount.quantize(CENT)),
        "vendor": payload.vendor,
        "note": payload.note,
        "createdByUid": decoded_token["uid"],
        "createdAt": now,
        "updatedAt": now,
    }
    ref.set(stored)
    return _serialize_expense(type("Snapshot", (), {"id": ref.id, "to_dict": lambda self: stored})(), payload.routeNumber)


@router.delete("/metrics/expenses/{expense_id}")
@rate_limit_write
async def archive_expense(
    request: Request,
    expense_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(route, decoded_token, db)
    if not expense_id or len(expense_id) > 128 or "/" in expense_id:
        raise HTTPException(400, "Invalid expense ID")
    ref = db.collection("routes").document(route).collection(EXPENSE_COLLECTION).document(expense_id)
    snapshot = ref.get()
    if not snapshot.exists:
        raise HTTPException(404, "Expense not found")
    data = snapshot.to_dict() or {}
    if str(data.get("routeNumber") or "") != route:
        raise HTTPException(404, "Expense not found")
    ref.update({"archivedAt": datetime.now(timezone.utc), "archivedByUid": decoded_token["uid"]})
    return {"ok": True, "id": expense_id}


@router.get("/metrics/payroll")
@rate_limit_history
async def list_payroll(
    request: Request,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    startDate: Optional[str] = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    endDate: Optional[str] = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=250, ge=1, le=500),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(route, decoded_token, db)
    start = _date(startDate, "startDate") if startDate else None
    end = _date(endDate, "endDate") if endDate else None
    if start and end and end < start:
        raise HTTPException(400, "endDate must be on or after startDate")

    collection = db.collection("routes").document(route).collection(PAYROLL_COLLECTION)
    snapshots = list(collection.order_by("periodEnd", direction=firestore.Query.DESCENDING).limit(limit * 2).stream())
    items = []
    for snapshot in snapshots:
        item = _serialize_payroll(snapshot, route)
        if item and _date_in_range(item["periodEnd"], start, end):
            items.append(item)
    items.sort(key=lambda item: (item["periodEnd"], item["id"]), reverse=True)
    items = items[:limit]
    return {"routeNumber": route, "items": items, "total": len(items)}


@router.post("/metrics/payroll")
@rate_limit_write
async def create_payroll_cost(
    request: Request,
    payload: PayrollCreateRequest,
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(payload.routeNumber, decoded_token, db)
    now = datetime.now(timezone.utc)
    ref = db.collection("routes").document(payload.routeNumber).collection(PAYROLL_COLLECTION).document()
    stored = {
        "routeNumber": payload.routeNumber,
        "periodStart": payload.periodStart,
        "periodEnd": payload.periodEnd,
        "workerLabel": payload.workerLabel,
        "workerType": payload.workerType,
        "payType": payload.payType,
        "hours": float(payload.hours) if payload.hours is not None else None,
        "hourlyRate": float(payload.hourlyRate) if payload.hourlyRate is not None else None,
        "grossPay": float(payload.grossPay.quantize(CENT)),
        "employerCosts": float(payload.employerCosts.quantize(CENT)),
        "reimbursements": float(payload.reimbursements.quantize(CENT)),
        "status": payload.status,
        "paidDate": payload.paidDate,
        "note": payload.note,
        "createdByUid": decoded_token["uid"],
        "createdAt": now,
        "updatedAt": now,
    }
    ref.set(stored)
    return _serialize_payroll(type("Snapshot", (), {"id": ref.id, "to_dict": lambda self: stored})(), payload.routeNumber)


@router.delete("/metrics/payroll/{payroll_id}")
@rate_limit_write
async def archive_payroll_cost(
    request: Request,
    payroll_id: str,
    route: str = Query(..., pattern=r"^\d{1,10}$"),
    decoded_token: dict = Depends(verify_firebase_token),
    db: firestore.Client = Depends(get_firestore),
) -> Dict[str, Any]:
    await _require_owner(route, decoded_token, db)
    if not payroll_id or len(payroll_id) > 128 or "/" in payroll_id:
        raise HTTPException(400, "Invalid payroll ID")
    ref = db.collection("routes").document(route).collection(PAYROLL_COLLECTION).document(payroll_id)
    snapshot = ref.get()
    if not snapshot.exists:
        raise HTTPException(404, "Payroll record not found")
    data = snapshot.to_dict() or {}
    if str(data.get("routeNumber") or "") != route:
        raise HTTPException(404, "Payroll record not found")
    ref.update({"archivedAt": datetime.now(timezone.utc), "archivedByUid": decoded_token["uid"]})
    return {"ok": True, "id": payroll_id}
