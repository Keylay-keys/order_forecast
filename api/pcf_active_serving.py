"""Bounded PostgreSQL read model for active PCF list/detail responses.

This module performs no Firebase I/O and owns no fallback decision.  Callers
must complete fresh route-access authorization first.  A route is readable only
when its compact control row proves cohort enablement, parity, and a bounded
ingress-health window; every other state fails closed as unavailable.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo

from psycopg2.extras import RealDictCursor


DENVER = ZoneInfo("America/Denver")
MAX_ACTIVE_CONTAINERS_PER_ROUTE = 500
MAX_ACTIVE_CONTAINERS_PER_DELIVERY = 100
MAX_ITEMS_PER_CONTAINER = 5000


class ProjectionUnavailable(RuntimeError):
    """The cluster projection is intentionally not usable for this request."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class ProjectionContractError(RuntimeError):
    """Stored projection data violates the bounded serving contract."""


@dataclass(frozen=True)
class RouteProjectionState:
    route_number: str
    last_projected_revision: str
    healthy_until: datetime


def _aware_utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ProjectionContractError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _load_route_state(
    conn: Any,
    route_number: str,
    *,
    now_utc: datetime,
) -> RouteProjectionState:
    now_utc = _aware_utc(now_utc, "now_utc")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT route_number, eligible, serving_enabled,
                   reconciliation_status, last_projected_revision,
                   parity_source_revision, parity_projection_revision,
                   source_entity_count, projection_entity_count,
                   source_business_hash, projection_business_hash,
                   healthy_until
            FROM pcf_projection_routes
            WHERE route_number = %s
            """,
            (route_number,),
        )
        row = cur.fetchone()
    if row is None:
        raise ProjectionUnavailable("route_state_missing")
    if not bool(row.get("eligible")) or not bool(row.get("serving_enabled")):
        raise ProjectionUnavailable("route_not_enabled")
    if row.get("reconciliation_status") != "matched":
        raise ProjectionUnavailable("projection_not_reconciled")
    source_revision = row.get("parity_source_revision")
    parity_revision = row.get("parity_projection_revision")
    if not source_revision or source_revision != parity_revision:
        raise ProjectionUnavailable("projection_revision_mismatch")
    if row.get("source_entity_count") != row.get("projection_entity_count"):
        raise ProjectionUnavailable("projection_count_mismatch")
    source_hash = row.get("source_business_hash")
    if not source_hash or source_hash != row.get("projection_business_hash"):
        raise ProjectionUnavailable("projection_hash_mismatch")
    last_projected_revision = str(row.get("last_projected_revision") or "")
    if not last_projected_revision:
        raise ProjectionUnavailable("projection_revision_missing")
    healthy_until = row.get("healthy_until")
    if not isinstance(healthy_until, datetime):
        raise ProjectionUnavailable("projection_health_unknown")
    healthy_until = _aware_utc(healthy_until, "healthy_until")
    if healthy_until <= now_utc:
        raise ProjectionUnavailable("projection_health_expired")
    return RouteProjectionState(
        route_number=route_number,
        last_projected_revision=last_projected_revision,
        healthy_until=healthy_until,
    )


def _parse_date(value: Any, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ProjectionContractError(f"invalid {name}") from exc
    raise ProjectionContractError(f"invalid {name}")


def _display_date(value: Any, name: str) -> str | None:
    parsed = _parse_date(value, name)
    return parsed.strftime("%m/%d/%Y") if parsed else None


def _display_date_sort_key(value: Any) -> date:
    if value is None:
        return date.min
    if not isinstance(value, str):
        raise ProjectionContractError("invalid displayed loadingDate")
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError as exc:
        raise ProjectionContractError("invalid displayed loadingDate") from exc


def _iso_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise ProjectionContractError("projection timestamp must be a datetime")
    return _aware_utc(value, "projection timestamp").isoformat()


def _business_date(now_utc: datetime) -> date:
    return _aware_utc(now_utc, "now_utc").astimezone(DENVER).date()


def _normalize_guarantee(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or value.get("isGuaranteed") is not True:
        raise ProjectionContractError("invalid guaranteed item projection")
    expires_at = value.get("guaranteeExpiresAt")
    if not isinstance(expires_at, str) or not expires_at:
        raise ProjectionContractError("guarantee expiry is missing")
    return {"isGuaranteed": True, "guaranteeExpiresAt": expires_at}


def _normalize_item(
    raw: Any,
    *,
    container_id: str,
    business_date: date,
) -> tuple[int, dict[str, Any], dict[str, Any], int | None]:
    if not isinstance(raw, Mapping):
        raise ProjectionContractError("projected item must be an object")
    try:
        page_number = int(raw["pageNumber"])
        item_position = int(raw["itemPosition"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ProjectionContractError("projected item position is invalid") from exc
    if not 1 <= page_number <= 1000 or not 0 <= item_position <= 9999:
        raise ProjectionContractError("projected item position is out of range")
    product = raw.get("product")
    description = raw.get("description")
    if not isinstance(product, str) or not product:
        raise ProjectionContractError("projected item product is invalid")
    if not isinstance(description, str):
        raise ProjectionContractError("projected item description is invalid")
    expiry = _parse_date(raw.get("expiryDate"), "expiryDate")
    days_left = (expiry - business_date).days if expiry else None
    is_short_coded = bool(raw.get("isShortCoded"))
    page_item: dict[str, Any] = {
        "product": product,
        "description": description,
        "bestBefore": expiry.strftime("%m/%d/%Y") if expiry else None,
        "days": (
            f"{days_left}{'*' if is_short_coded else ''}"
            if days_left is not None
            else ""
        ),
        "isShortCoded": is_short_coded,
        "visibleOnList": raw.get("visibleOnList") is not False,
        "isLowQuantity": bool(raw.get("isLowQuantity")),
    }
    guarantee = _normalize_guarantee(raw.get("guaranteed"))
    if guarantee is not None:
        page_item["guaranteed"] = guarantee
    product_item = {
        **page_item,
        "containerCode": container_id,
        "pageNumber": page_number,
    }
    return page_number, page_item, product_item, days_left


def _normalize_page_numbers(value: Any, items: Iterable[Mapping[str, Any]]) -> list[int]:
    if not isinstance(value, list):
        raise ProjectionContractError("page_numbers must decode to an array")
    try:
        page_numbers = [int(number) for number in value]
    except (TypeError, ValueError) as exc:
        raise ProjectionContractError("page_numbers contains a non-integer") from exc
    if (
        len(page_numbers) != len(set(page_numbers))
        or any(number < 1 or number > 1000 for number in page_numbers)
    ):
        raise ProjectionContractError("page_numbers is invalid")
    item_pages = {int(item["pageNumber"]) for item in items}
    if not item_pages.issubset(set(page_numbers)):
        raise ProjectionContractError("projected item references an absent page")
    return sorted(page_numbers)


def _container_from_row(row: Mapping[str, Any], *, business_date: date) -> dict[str, Any]:
    container_id = str(row.get("container_id") or "")
    if not container_id:
        raise ProjectionContractError("container identity is missing")
    raw_items = row.get("items")
    if not isinstance(raw_items, list) or len(raw_items) > MAX_ITEMS_PER_CONTAINER:
        raise ProjectionContractError("container item payload is invalid or oversized")
    page_numbers = _normalize_page_numbers(row.get("page_numbers"), raw_items)
    pages: dict[int, list[dict[str, Any]]] = {number: [] for number in page_numbers}
    products: list[dict[str, Any]] = []
    expired_items = 0
    expiring_items = 0
    seen_positions: set[tuple[int, int]] = set()
    for raw_item in raw_items:
        page_number, page_item, product_item, days_left = _normalize_item(
            raw_item,
            container_id=container_id,
            business_date=business_date,
        )
        position = (page_number, int(raw_item["itemPosition"]))
        if position in seen_positions:
            raise ProjectionContractError("duplicate projected item position")
        seen_positions.add(position)
        pages[page_number].append(page_item)
        products.append(product_item)
        if days_left is not None:
            if days_left <= 0:
                expired_items += 1
            elif days_left <= 7:
                expiring_items += 1
    return {
        "containerCode": container_id,
        "loadingDate": _display_date(row.get("loading_date"), "loadingDate"),
        "pageCount": len(page_numbers),
        "totalItems": len(raw_items),
        "expiringItems": expiring_items,
        "expiredItems": expired_items,
        "pages": [
            {"pageNumber": number, "items": pages[number]}
            for number in page_numbers
        ],
        "products": products,
        "sourceRevision": str(row.get("source_revision") or ""),
        "projectionUpdatedAt": _iso_datetime(row.get("updated_at")),
    }


def _load_rows(
    conn: Any,
    *,
    route_number: str,
    delivery_id: str | None,
    limit: int,
) -> list[Mapping[str, Any]]:
    delivery_clause = "AND delivery_id = %s" if delivery_id is not None else ""
    params: tuple[Any, ...] = (
        (route_number, delivery_id, limit + 1)
        if delivery_id is not None
        else (route_number, limit + 1)
    )
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT route_number, delivery_id, container_id, loading_date,
                   page_numbers, items, source_revision, updated_at
            FROM items_current
            WHERE route_number = %s
              {delivery_clause}
              AND active = TRUE
              AND deleted_at IS NULL
            ORDER BY loading_date DESC NULLS LAST, delivery_id DESC, container_id
            LIMIT %s
            """,
            params,
        )
        rows = list(cur.fetchall())
    if len(rows) > limit:
        raise ProjectionUnavailable("projection_result_limit_exceeded")
    return rows


def _delivery_from_containers(
    route_number: str,
    delivery_id: str,
    containers: list[dict[str, Any]],
) -> dict[str, Any]:
    products = [item for container in containers for item in container["products"]]
    updated_values = [
        container.get("projectionUpdatedAt")
        for container in containers
        if container.get("projectionUpdatedAt")
    ]
    loading_date = next(
        (container["loadingDate"] for container in containers if container.get("loadingDate")),
        None,
    )
    return {
        "deliveryNumber": delivery_id,
        "routeNumber": route_number,
        "status": "active",
        "loadingDate": loading_date,
        "containerCount": len(containers),
        "totalItems": sum(container["totalItems"] for container in containers),
        "expiringItems": sum(container["expiringItems"] for container in containers),
        "expiredItems": sum(container["expiredItems"] for container in containers),
        "containers": [
            {
                "containerCode": container["containerCode"],
                "pageCount": container["pageCount"],
                "totalItems": container["totalItems"],
            }
            for container in containers
        ],
        "products": products,
        "createdAt": None,
        "updatedAt": max(updated_values) if updated_values else None,
    }


def list_active_deliveries_from_postgres(
    conn: Any,
    route_number: str,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = _load_route_state(conn, route_number, now_utc=now_utc)
    rows = _load_rows(
        conn,
        route_number=route_number,
        delivery_id=None,
        limit=MAX_ACTIVE_CONTAINERS_PER_ROUTE,
    )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    business_date = _business_date(now_utc)
    for row in rows:
        grouped[str(row["delivery_id"])].append(
            _container_from_row(row, business_date=business_date)
        )
    deliveries = [
        _delivery_from_containers(route_number, delivery_id, containers)
        for delivery_id, containers in grouped.items()
    ]
    deliveries.sort(
        key=lambda delivery: (
            _display_date_sort_key(delivery.get("loadingDate")),
            delivery["deliveryNumber"],
        ),
        reverse=True,
    )
    return {
        "items": deliveries,
        "total": len(deliveries),
        "route": route_number,
        "source": "postgres",
        "projectionRevision": state.last_projected_revision,
        "healthyUntil": state.healthy_until.isoformat(),
    }


def get_active_delivery_from_postgres(
    conn: Any,
    route_number: str,
    delivery_id: str,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any] | None:
    now_utc = now_utc or datetime.now(timezone.utc)
    state = _load_route_state(conn, route_number, now_utc=now_utc)
    rows = _load_rows(
        conn,
        route_number=route_number,
        delivery_id=delivery_id,
        limit=MAX_ACTIVE_CONTAINERS_PER_DELIVERY,
    )
    if not rows:
        return None
    business_date = _business_date(now_utc)
    containers = [
        _container_from_row(row, business_date=business_date) for row in rows
    ]
    delivery = _delivery_from_containers(route_number, delivery_id, containers)
    return {
        "deliveryNumber": delivery_id,
        "routeNumber": route_number,
        "status": delivery["status"],
        "containers": containers,
        "createdAt": delivery["createdAt"],
        "updatedAt": delivery["updatedAt"],
        "source": "postgres",
        "projectionRevision": state.last_projected_revision,
        "healthyUntil": state.healthy_until.isoformat(),
    }


def get_dashboard_summary_from_postgres(
    conn: Any,
    route_number: str,
    *,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Build the mobile dashboard contract directly from ``items_current``.

    The projection health proof is evaluated before the first item row is
    loaded. This function deliberately has no Firebase read-through: callers
    expose an unavailable projection to the client's bounded availability
    fallback instead of repairing PostgreSQL during a user request.
    """
    now_utc = now_utc or datetime.now(timezone.utc)
    state = _load_route_state(conn, route_number, now_utc=now_utc)
    rows = _load_rows(
        conn,
        route_number=route_number,
        delivery_id=None,
        limit=MAX_ACTIVE_CONTAINERS_PER_ROUTE,
    )
    payload = build_dashboard_summary_payload(
        route_number,
        rows,
        now_utc=now_utc,
    )
    generated_at = now_utc.isoformat()
    payload["freshness"] = {
        "source": "postgres",
        "fresh": True,
        "sourceRevision": state.last_projected_revision,
        "mirroredRevision": state.last_projected_revision,
        "generatedAt": generated_at,
        "healthyUntil": state.healthy_until.isoformat(),
    }
    payload["generatedAt"] = generated_at
    return payload


def build_dashboard_summary_payload(
    route_number: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    now_utc: datetime,
) -> dict[str, Any]:
    """Build a stable dashboard payload from already-bounded projection rows.

    The helper performs no route-health lookup and no database I/O. Effect
    finalization uses it inside its transaction; serving adds the independent
    route-health envelope after loading the mirror.
    """
    now_utc = _aware_utc(now_utc, "now_utc")
    business_date = _business_date(now_utc)
    active_pcfs: list[dict[str, Any]] = []
    expiring_items: list[dict[str, Any]] = []

    for row in rows:
        delivery_id = str(row["delivery_id"])
        container = _container_from_row(row, business_date=business_date)
        dashboard_items: list[dict[str, Any]] = []
        for product in container["products"]:
            expiry_date = product.get("bestBefore")
            if not expiry_date:
                continue
            days_text = str(product.get("days") or "").rstrip("*")
            try:
                days_left = int(days_text)
            except ValueError as exc:
                raise ProjectionContractError(
                    "projected item days value is invalid"
                ) from exc
            item = {
                "description": product["description"],
                "product": product["product"],
                "expiryDate": expiry_date,
                "daysLeft": days_left,
                "isShortCoded": bool(product.get("isShortCoded")),
                "deliveryNumber": delivery_id,
                "containerCode": container["containerCode"],
                "visibleOnList": product.get("visibleOnList") is not False,
                "isLowQuantity": bool(product.get("isLowQuantity")),
                "pageNumber": int(product["pageNumber"]),
            }
            if product.get("guaranteed") is not None:
                item["guaranteed"] = product["guaranteed"]
            dashboard_items.append(item)
        expiring_items.extend(dashboard_items)
        item_count = int(container["totalItems"])
        expired_count = int(container["expiredItems"])
        active_pcfs.append(
            {
                "id": container["containerCode"],
                "deliveryNumber": delivery_id,
                "containerCode": container["containerCode"],
                "loadingDate": container["loadingDate"],
                "itemCount": item_count,
                "expiringCount": int(container["expiringItems"]),
                "expiredCount": expired_count,
                "routeId": route_number,
                "status": "active",
                "createdAt": None,
                "updatedAt": container["projectionUpdatedAt"],
                "allItemsExpired": item_count > 0 and expired_count >= item_count,
                "items": dashboard_items,
            }
        )

    expired_pcfs = sum(
        row["itemCount"] > 0 and row["expiredCount"] >= row["itemCount"]
        for row in active_pcfs
    )
    expiring_pcfs = sum(row["expiringCount"] > 0 for row in active_pcfs)
    return {
        "routeNumber": route_number,
        "activePcfs": active_pcfs,
        "expiringItems": expiring_items,
        "counts": {
            "totalPCFs": len(active_pcfs),
            "activePCFs": len(active_pcfs),
            "expiringPCFs": expiring_pcfs,
            "expiredPCFs": expired_pcfs,
            "totalContainers": len(active_pcfs),
            "expiredContainers": expired_pcfs,
        },
    }
