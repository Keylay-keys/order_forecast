"""Firebase writer for forecasts and related artifacts.

This module is write-only. It complements firebase_loader.py.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from google.cloud import firestore  # type: ignore
from google.cloud.firestore_v1.base_query import FieldFilter

# Support running as script without package context
try:
    from .models import ForecastPayload
    from .pg_utils import fetch_all as pg_fetch_all
except ImportError:
    from models import ForecastPayload
    try:
        from pg_utils import fetch_all as pg_fetch_all
    except Exception:
        pg_fetch_all = None


def get_firestore_client(service_account_path: Optional[str] = None) -> firestore.Client:
    """Return a Firestore client using an optional service account JSON."""
    if service_account_path:
        return firestore.Client.from_service_account_json(service_account_path)
    return firestore.Client()


def _ts(dt: Optional[datetime]):
    if dt is None:
        return firestore.SERVER_TIMESTAMP
    return dt


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _serialize_whole_case_adjustment(adj: object) -> Optional[dict]:
    """Map ForecastItem.whole_case_adjustment (snake_case) -> payload (camelCase)."""
    if not isinstance(adj, dict):
        return None
    try:
        return {
            "adjustedUnits": int(adj.get("adjusted_units")) if adj.get("adjusted_units") is not None else None,
            "preEnforcementUnits": int(adj.get("pre_enforcement_units")) if adj.get("pre_enforcement_units") is not None else None,
            "trigger": adj.get("trigger"),
            "targetStoreId": adj.get("target_store_id"),
        }
    except Exception:
        return None


def _archive_forecast_to_disk(route_number: str, forecast: ForecastPayload, expires_at: datetime) -> None:
    """Persist a copy of a forecast to local disk (server HDD).

    This is intentionally separate from Firestore. Cached forecasts in Firestore are TTL-based
    and will disappear; the on-disk archive is for restamping/training/debugging.
    """
    root = os.environ.get("ROUTESPARK_FORECAST_ARCHIVE_DIR")
    if not root:
        return

    # Layout: {root}/{route}/{deliveryDate}/{scheduleKey}/{forecastId}.json
    target_dir = os.path.join(
        root,
        str(route_number),
        str(forecast.delivery_date),
        str(forecast.schedule_key),
    )
    os.makedirs(target_dir, exist_ok=True)

    payload = {
        "forecastId": forecast.forecast_id,
        "routeNumber": str(route_number),
        "deliveryDate": str(forecast.delivery_date),
        "scheduleKey": str(forecast.schedule_key),
        "generatedAt": _iso(forecast.generated_at),
        "expiresAt": _iso(expires_at),
        "items": [
            {
                "storeId": item.store_id,
                "storeName": item.store_name,
                "sap": item.sap,
                "recommendedUnits": item.recommended_units,
                "recommendedCases": item.recommended_cases,
                "p10Units": item.p10_units,
                "p50Units": item.p50_units,
                "p90Units": item.p90_units,
                "promoActive": item.promo_active,
                "promoLiftPct": item.promo_lift_pct,
                "isFirstWeekend": item.is_first_weekend,
                "confidence": item.confidence,
                "source": item.source,
                **({"priorOrderContext": {
                    "orderId": item.prior_order_context.order_id,
                    "orderDate": item.prior_order_context.order_date,
                    "deliveryDate": item.prior_order_context.delivery_date,
                    "quantity": item.prior_order_context.quantity,
                    "scheduleKey": item.prior_order_context.schedule_key,
                }} if item.prior_order_context else {}),
                **({"expiryReplacement": {
                    "expiryDate": item.expiry_replacement.expiry_date,
                    "minUnitsRequired": item.expiry_replacement.min_units_required,
                    "reason": item.expiry_replacement.reason,
                }} if item.expiry_replacement else {}),
                **({"wholeCaseAdjustment": wca}
                   if (wca := _serialize_whole_case_adjustment(item.whole_case_adjustment)) else {}),
            }
            for item in forecast.items
        ],
        "archivedAt": datetime.utcnow().isoformat() + "Z",
    }

    final_path = os.path.join(target_dir, f"{forecast.forecast_id}.json")
    fd, tmp_path = tempfile.mkstemp(dir=target_dir, prefix=f"{forecast.forecast_id}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
            f.write("\n")
        os.replace(tmp_path, final_path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except Exception:
            pass


def _to_nonempty_str(value: object) -> Optional[str]:
    if isinstance(value, str):
        s = value.strip()
        if s:
            return s
    return None


def _to_int_units(value: object) -> int:
    try:
        return max(0, int(round(float(value or 0))))
    except Exception:
        return 0


def _infer_case_pack(item: dict) -> Optional[int]:
    units = item.get("recommendedUnits")
    cases = item.get("recommendedCases")
    try:
        u = float(units or 0)
        c = float(cases or 0)
        if u <= 0 or c <= 0:
            return None
        cp = int(round(u / c))
        if cp > 0:
            return cp
    except Exception:
        return None
    return None


def _load_route_group(db: firestore.Client, route_number: str) -> Tuple[str, List[str]]:
    """
    Resolve a route group from users collection.

    Returns:
        (route_group_id, routes_in_group)
    """
    route_number = str(route_number)

    user_doc = None
    users = db.collection("users")

    # Primary route owner
    primary_hits = list(users.where(filter=FieldFilter("profile.routeNumber", "==", route_number)).limit(1).stream())
    if primary_hits:
        user_doc = primary_hits[0].to_dict() or {}
    else:
        # Additional route owner
        addl_hits = list(users.where(filter=FieldFilter("profile.additionalRoutes", "array_contains", route_number)).limit(1).stream())
        if addl_hits:
            user_doc = addl_hits[0].to_dict() or {}

    if not user_doc:
        return route_number, [route_number]

    profile = user_doc.get("profile") if isinstance(user_doc.get("profile"), dict) else {}
    group_id = _to_nonempty_str(profile.get("routeNumber")) or route_number

    routes: List[str] = []

    def _add_route(raw: object) -> None:
        sr = _to_nonempty_str(raw)
        if sr and sr not in routes:
            routes.append(sr)

    # Canonical profile routes.
    _add_route(profile.get("routeNumber"))
    _add_route(profile.get("currentRoute"))
    additional = profile.get("additionalRoutes") if isinstance(profile.get("additionalRoutes"), list) else []
    for r in additional:
        _add_route(r)

    # Defensive: include owner routes from routeAssignments to survive profile drift.
    route_assignments = user_doc.get("routeAssignments")
    if isinstance(route_assignments, dict):
        for r, assignment in route_assignments.items():
            if not isinstance(assignment, dict):
                continue
            if _to_nonempty_str(assignment.get("role")) == "owner":
                _add_route(r)

    if group_id not in routes:
        routes.insert(0, group_id)
    if route_number not in routes:
        routes.append(route_number)

    return group_id, routes


def _load_route_owner_doc(db: firestore.Client, route_number: str) -> dict:
    """Resolve owner user doc for a route (best-effort)."""
    route_number = str(route_number)
    users = db.collection("users")
    primary_hits = list(
        users.where(filter=FieldFilter("profile.routeNumber", "==", route_number)).limit(1).stream()
    )
    if primary_hits:
        return primary_hits[0].to_dict() or {}

    addl_hits = list(
        users.where(filter=FieldFilter("profile.additionalRoutes", "array_contains", route_number)).limit(1).stream()
    )
    if addl_hits:
        return addl_hits[0].to_dict() or {}
    return {}


def _normalize_pooling_policy(raw: object) -> str:
    policy = _to_nonempty_str(raw) or "disabled"
    policy = policy.lower()
    if policy not in ("disabled", "auto_slow_movers", "eligible_list"):
        return "disabled"
    return policy


def _load_phase6_pooling_contract(db: firestore.Client, route_number: str) -> dict:
    """Load Phase 6 pooling contract settings (feature-flagged)."""
    route_group_id, routes = _load_route_group(db, route_number)
    user_doc = _load_route_owner_doc(db, route_group_id)
    profile = user_doc.get("profile") if isinstance(user_doc.get("profile"), dict) else {}
    user_settings = user_doc.get("userSettings") if isinstance(user_doc.get("userSettings"), dict) else {}
    pooling = user_settings.get("pooling") if isinstance(user_settings.get("pooling"), dict) else {}

    policy = _normalize_pooling_policy(pooling.get("pooledSapPolicy"))
    pooled_saps_raw = pooling.get("pooledSaps") if isinstance(pooling.get("pooledSaps"), list) else []
    pooled_saps = sorted({str(s).strip() for s in pooled_saps_raw if str(s).strip()})

    master_route_number = _to_nonempty_str(pooling.get("masterRouteNumber")) \
        or _to_nonempty_str(profile.get("routeNumber")) \
        or str(route_group_id)
    route_numbers = sorted({str(r).strip() for r in routes if str(r).strip()})

    enabled = bool(pooling.get("enabled", False))
    return {
        "contractVersion": "phase6-draft-v1",
        "enabled": enabled,
        "masterRouteNumber": str(master_route_number),
        "routeGroupId": str(route_group_id),
        "routeNumbers": route_numbers,
        "pooledSapPolicy": policy,
        "pooledSaps": pooled_saps,
    }


def _routes_with_schedule_key(route_numbers: List[str], schedule_key: str) -> List[str]:
    """Return subset of routes that actively run this schedule key."""
    if not route_numbers:
        return []
    if pg_fetch_all is None:
        return route_numbers
    try:
        rows = pg_fetch_all(
            """
            SELECT route_number
            FROM user_schedules
            WHERE route_number = ANY(%s)
              AND is_active = TRUE
              AND LOWER(schedule_key) = LOWER(%s)
            """,
            [list(route_numbers), str(schedule_key)],
        )
        valid = {str(r.get("route_number")) for r in rows if r.get("route_number")}
        return [r for r in route_numbers if r in valid]
    except Exception:
        # Fail open for scaffolding stage; runtime flag remains off by default.
        return route_numbers


def _build_auto_slow_mover_allowlist(
    route_numbers: List[str],
    schedule_key: str,
    case_pack_by_sap: Dict[str, int],
) -> set[str]:
    """Derive slow-mover SAPs for Phase 6 auto policy.

    Rule (route-group + schedule scoped):
    - low order frequency (sap_order_count / total_orders <= threshold)
    - low average cases when ordered (avg_units_per_order / case_pack <= threshold)
    """
    if not route_numbers or not case_pack_by_sap:
        return set()
    if pg_fetch_all is None:
        return set()

    lookback_days = int(os.environ.get("FORECAST_PHASE6_SLOW_MOVER_LOOKBACK_DAYS", "180"))
    max_order_rate = float(os.environ.get("FORECAST_PHASE6_SLOW_MOVER_MAX_ORDER_RATE", "0.35"))
    max_avg_cases = float(os.environ.get("FORECAST_PHASE6_SLOW_MOVER_MAX_AVG_CASES", "1.0"))
    min_sap_orders = int(os.environ.get("FORECAST_PHASE6_SLOW_MOVER_MIN_SAP_ORDERS", "1"))

    try:
        total_row = pg_fetch_all(
            """
            SELECT COUNT(DISTINCT o.order_id) AS total_orders
            FROM orders_historical o
            WHERE o.route_number = ANY(%s)
              AND LOWER(o.schedule_key) = LOWER(%s)
              AND o.delivery_date >= (CURRENT_DATE - (%s || ' days')::interval)
            """,
            [list(route_numbers), str(schedule_key), int(lookback_days)],
        )
        total_orders = int((total_row[0] or {}).get("total_orders") or 0) if total_row else 0
        if total_orders <= 0:
            return set()

        sap_rows = pg_fetch_all(
            """
            SELECT
                li.sap AS sap,
                COUNT(DISTINCT o.order_id) AS sap_order_count,
                AVG(li.quantity::float) AS avg_units_per_order
            FROM orders_historical o
            JOIN order_line_items li ON li.order_id = o.order_id
            WHERE o.route_number = ANY(%s)
              AND LOWER(o.schedule_key) = LOWER(%s)
              AND o.delivery_date >= (CURRENT_DATE - (%s || ' days')::interval)
              AND li.sap IS NOT NULL
              AND li.quantity > 0
            GROUP BY li.sap
            """,
            [list(route_numbers), str(schedule_key), int(lookback_days)],
        )

        slow_saps: set[str] = set()
        for row in sap_rows:
            sap = str(row.get("sap") or "").strip()
            if not sap:
                continue
            if sap not in case_pack_by_sap:
                continue
            case_pack = max(1, int(case_pack_by_sap.get(sap, 1) or 1))
            sap_orders = int(row.get("sap_order_count") or 0)
            if sap_orders < max(1, min_sap_orders):
                continue
            avg_units = float(row.get("avg_units_per_order") or 0.0)
            order_rate = float(sap_orders) / float(total_orders)
            avg_cases = float(avg_units) / float(case_pack)
            if order_rate <= max_order_rate and avg_cases <= max_avg_cases:
                slow_saps.add(sap)
        return slow_saps
    except Exception:
        # Fail closed in auto policy mode to avoid over-suggesting pooled transfers.
        return set()


def _load_cached_forecast_items(
    db: firestore.Client,
    route_number: str,
    delivery_date: str,
    schedule_key: str,
) -> List[dict]:
    cached_ref = db.collection("forecasts").document(route_number).collection("cached")
    docs = list(
        cached_ref
        .where(filter=FieldFilter("deliveryDate", "==", delivery_date))
        .where(filter=FieldFilter("scheduleKey", "==", schedule_key))
        .stream()
    )
    if not docs:
        return []

    # Prefer the newest payload if duplicate cache docs exist for this cycle.
    def _sort_key(doc) -> float:
        data = doc.to_dict() or {}
        ts = data.get("generatedAt") or data.get("createdAt")
        try:
            if hasattr(ts, "timestamp"):
                return float(ts.timestamp())
        except Exception:
            pass
        return 0.0

    docs.sort(key=_sort_key, reverse=True)
    data = docs[0].to_dict() or {}
    items = data.get("items")
    if isinstance(items, list):
        return [i for i in items if isinstance(i, dict)]
    return []


def _aggregate_route_items(items: List[dict]) -> Dict[str, Dict[str, int]]:
    """
    Aggregate store-level forecast rows into route-level totals by SAP.
    Returns: {sap: {"units": int, "case_pack": int}}
    """
    totals: Dict[str, Dict[str, int]] = {}
    for item in items:
        sap = _to_nonempty_str(item.get("sap"))
        if not sap:
            continue
        units = _to_int_units(item.get("recommendedUnits"))
        if units <= 0:
            continue
        case_pack = _infer_case_pack(item)
        existing = totals.get(sap)
        if not existing:
            totals[sap] = {"units": units, "case_pack": int(case_pack or 0)}
        else:
            existing["units"] += units
            if case_pack and int(case_pack) > existing.get("case_pack", 0):
                existing["case_pack"] = int(case_pack)
    return totals


def _load_user_transfer_patterns(
    db: firestore.Client,
    route_group_id: str,
) -> set[tuple[str, str, str]]:
    """
    Load user-created historical transfer patterns for this route group.

    Returns:
        set of (from_route_number, to_route_number, sap)

    Notes:
      - Only user-created reasons are considered (`manual`, `pooled_order`)
      - Forecast-generated suggestions (`sourceOrderId` starts with `forecast:`) are ignored
    """
    transfers_ref = db.collection("routeTransfers").document(route_group_id).collection("transfers")
    docs: List = []
    try:
        docs = list(
            transfers_ref.where(
                filter=FieldFilter("reason", "in", ["manual", "pooled_order"])
            ).stream()
        )
    except Exception:
        # Fallback: if query/index support is unavailable, stream and filter locally.
        docs = list(transfers_ref.stream())

    patterns: set[tuple[str, str, str]] = set()
    for doc in docs:
        data = doc.to_dict() or {}
        reason = _to_nonempty_str(data.get("reason"))
        if reason not in ("manual", "pooled_order"):
            continue

        source_order_id = _to_nonempty_str(data.get("sourceOrderId")) or ""
        if source_order_id.startswith("forecast:"):
            continue

        from_route = _to_nonempty_str(data.get("fromRouteNumber"))
        to_route = _to_nonempty_str(data.get("toRouteNumber"))
        sap = _to_nonempty_str(data.get("sap"))
        units = _to_int_units(data.get("units"))

        if not from_route or not to_route or not sap:
            continue
        if from_route == to_route or units <= 0:
            continue

        patterns.add((from_route, to_route, sap))

    return patterns


def _cleanup_forecast_transfer_suggestions(
    transfers_ref,
    source_marker: str,
    expected_keys: set[str],
) -> Tuple[int, int]:
    """
    Cleanup stale forecast-generated transfer suggestions for a cycle.

    Returns:
        (removed_count, canceled_count)
    """
    stale_docs = list(
        transfers_ref
        .where(filter=FieldFilter("sourceOrderId", "==", source_marker))
        .stream()
    )

    removed = 0
    canceled = 0
    for doc in stale_docs:
        data = doc.to_dict() or {}
        if data.get("reason") != "rebalance":
            continue
        if doc.id in expected_keys:
            continue
        reserved_by = data.get("reservedBy") if isinstance(data.get("reservedBy"), dict) else {}
        reserved_total = sum(_to_int_units(v) for v in reserved_by.values())
        if reserved_total > 0:
            doc.reference.set(
                {
                    "status": "canceled",
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                },
                merge=True,
            )
            canceled += 1
        else:
            doc.reference.delete()
            removed += 1

    return removed, canceled


def write_forecast_transfer_suggestions(
    db: firestore.Client,
    route_number: str,
    forecast: ForecastPayload,
) -> None:
    """
    Generate/refresh planned cross-route transfer suggestions for a forecast cycle.

    This populates routeTransfers/{routeGroupId}/transfers/* so receiving routes can
    see inbound units immediately in the app's Transfer Portal.
    """
    enabled = os.environ.get("FORECAST_ENABLE_TRANSFER_SUGGESTIONS", "0").lower() in ("1", "true", "yes")
    if not enabled:
        return

    route_group_id, routes = _load_route_group(db, route_number)
    if len(routes) < 2:
        return

    delivery_date = str(forecast.delivery_date)
    schedule_key = str(forecast.schedule_key)
    source_marker = f"forecast:{delivery_date}:{schedule_key}"
    transfers_ref = db.collection("routeTransfers").document(route_group_id).collection("transfers")

    # Phase 6 draft scaffold (feature-flagged, off by default):
    # - enforce same-schedule route subset
    # - enforce pooled SAP policy contract
    phase6_enabled = os.environ.get("FORECAST_PHASE6_POOLING_ENABLED", "0").lower() in ("1", "true", "yes")
    pooled_policy = "disabled"
    pooled_sap_allowlist: set[str] = set()
    phase6_contract_version = "phase6-draft-v1"
    if phase6_enabled:
        contract = _load_phase6_pooling_contract(db, route_number)
        pooled_policy = str(contract.get("pooledSapPolicy") or "disabled")
        pooled_sap_allowlist = {
            str(s).strip() for s in (contract.get("pooledSaps") or []) if str(s).strip()
        }
        routes = _routes_with_schedule_key(routes, schedule_key)
        print(
            f"[firebase_writer] Phase6 pooling contract {phase6_contract_version} "
            f"group={route_group_id} schedule={schedule_key} policy={pooled_policy} routes={len(routes)}"
        )
        if len(routes) < 2 or pooled_policy == "disabled":
            removed, canceled = _cleanup_forecast_transfer_suggestions(
                transfers_ref=transfers_ref,
                source_marker=source_marker,
                expected_keys=set(),
            )
            if removed or canceled:
                print(
                    f"[firebase_writer] Transfer suggestions cleanup for {route_group_id} "
                    f"{delivery_date}/{schedule_key}: removed={removed} canceled={canceled}"
                )
            return

    # History gate: suggestions are allowed only when we have user-created history
    # for the same (from_route, to_route, sap) pattern.
    user_patterns = _load_user_transfer_patterns(db, route_group_id)
    if not user_patterns:
        removed, canceled = _cleanup_forecast_transfer_suggestions(
            transfers_ref=transfers_ref,
            source_marker=source_marker,
            expected_keys=set(),
        )
        print(
            f"[firebase_writer] Transfer suggestions skipped for {route_group_id} "
            f"{delivery_date}/{schedule_key}: no user transfer history"
        )
        if removed or canceled:
            print(
                f"[firebase_writer] Transfer suggestions cleanup for {route_group_id} "
                f"{delivery_date}/{schedule_key}: removed={removed} canceled={canceled}"
            )
        return

    # Load all route forecasts for this same cycle from Firestore cache.
    route_agg: Dict[str, Dict[str, Dict[str, int]]] = {}
    for r in routes:
        items = _load_cached_forecast_items(db, r, delivery_date, schedule_key)
        route_agg[r] = _aggregate_route_items(items)

    # Build suggestions only for SAPs where multiple routes have demand and at least
    # one route's demand is below case-pack (classic split-case signal).
    by_sap: Dict[str, Dict[str, Dict[str, int]]] = {}
    for r, sap_map in route_agg.items():
        for sap, info in sap_map.items():
            if sap not in by_sap:
                by_sap[sap] = {}
            by_sap[sap][r] = info

    phase6_auto_slow_saps: set[str] = set()
    if phase6_enabled and pooled_policy == "auto_slow_movers":
        case_pack_by_sap: Dict[str, int] = {}
        for sap, demand_map in by_sap.items():
            cp = max(int(demand_map[r].get("case_pack", 0)) for r in demand_map.keys())
            if cp > 0:
                case_pack_by_sap[sap] = cp
        phase6_auto_slow_saps = _build_auto_slow_mover_allowlist(
            route_numbers=routes,
            schedule_key=schedule_key,
            case_pack_by_sap=case_pack_by_sap,
        )
        print(
            f"[firebase_writer] Phase6 auto_slow_movers allowlist "
            f"{route_group_id} {delivery_date}/{schedule_key}: {len(phase6_auto_slow_saps)} sap(s)"
        )

    suggestions: Dict[str, dict] = {}
    for sap, demand_map in by_sap.items():
        if phase6_enabled:
            if pooled_policy == "eligible_list" and pooled_sap_allowlist and sap not in pooled_sap_allowlist:
                continue
            if pooled_policy == "auto_slow_movers":
                if sap not in phase6_auto_slow_saps:
                    continue

        demand_routes = [r for r, info in demand_map.items() if int(info.get("units", 0)) > 0]
        if len(demand_routes) < 2:
            continue

        case_pack = max(int(demand_map[r].get("case_pack", 0)) for r in demand_routes)
        if case_pack <= 0:
            continue

        small_routes = [r for r in demand_routes if int(demand_map[r].get("units", 0)) < case_pack]
        if not small_routes:
            continue

        # Prefer primary/master route as purchase route when it has demand.
        # Otherwise use the route with the highest demand.
        if route_group_id in demand_routes:
            purchase_route = route_group_id
        else:
            purchase_route = sorted(
                demand_routes,
                key=lambda r: (int(demand_map[r].get("units", 0)), r),
                reverse=True,
            )[0]

        # Don't create self-transfers.
        for to_route in small_routes:
            if to_route == purchase_route:
                continue
            if (purchase_route, to_route, sap) not in user_patterns:
                continue
            units = int(demand_map[to_route].get("units", 0))
            if units <= 0:
                continue
            transfer_key = f"{source_marker}:{purchase_route}:{to_route}:{sap}".replace("/", "_")
            payload = {
                "routeGroupId": route_group_id,
                "purchaseRouteNumber": purchase_route,
                "fromRouteNumber": purchase_route,
                "toRouteNumber": to_route,
                "sap": sap,
                "units": units,
                "casePack": case_pack,
                "transferDate": delivery_date,
                "deliveryDate": delivery_date,
                "scheduleKey": schedule_key,
                "status": "planned",
                "reason": "rebalance",
                "sourceOrderId": source_marker,
                "forecastId": str(forecast.forecast_id),
                "updatedAt": firestore.SERVER_TIMESTAMP,
                "createdAt": firestore.SERVER_TIMESTAMP,
            }
            if phase6_enabled:
                payload["poolingContractVersion"] = phase6_contract_version
                payload["pooledSapPolicy"] = pooled_policy
            suggestions[transfer_key] = payload

    # Upsert current suggestions.
    for transfer_key, payload in suggestions.items():
        transfers_ref.document(transfer_key).set(payload, merge=True)

    # Cleanup stale suggestions for this forecast cycle.
    removed, canceled = _cleanup_forecast_transfer_suggestions(
        transfers_ref=transfers_ref,
        source_marker=source_marker,
        expected_keys=set(suggestions.keys()),
    )

    if suggestions:
        print(
            f"[firebase_writer] Transfer suggestions updated for {route_group_id} "
            f"{delivery_date}/{schedule_key}: {len(suggestions)} suggestion(s)"
        )
    if removed or canceled:
        print(
            f"[firebase_writer] Transfer suggestions cleanup for {route_group_id} "
            f"{delivery_date}/{schedule_key}: removed={removed} canceled={canceled}"
        )


def write_cached_forecast(
    db: firestore.Client,
    route_number: str,
    forecast: ForecastPayload,
    ttl_days: int = 7,
) -> None:
    """Write a forecast to forecasts/{route}/cached/{forecastId} with TTL.
    
    Deletes any existing forecasts for the same delivery_date/schedule_key
    to prevent duplicates from piling up.
    """
    # Just use datetime - Firestore auto-converts to Timestamp
    expires_at = forecast.generated_at + timedelta(days=ttl_days)
    
    # Delete existing forecasts for the same delivery/schedule to prevent duplicates
    cached_ref = db.collection("forecasts").document(route_number).collection("cached")
    existing = cached_ref.where(filter=FieldFilter("deliveryDate", "==", forecast.delivery_date))\
                        .where(filter=FieldFilter("scheduleKey", "==", forecast.schedule_key))\
                        .stream()
    
    deleted_count = 0
    for old_doc in existing:
        old_doc.reference.delete()
        deleted_count += 1
    
    if deleted_count > 0:
        print(f"[firebase_writer] Deleted {deleted_count} existing forecast(s) for {forecast.delivery_date}/{forecast.schedule_key}")

    doc_ref = db.collection("forecasts").document(route_number).collection("cached").document(
        forecast.forecast_id
    )

    doc_ref.set(
        {
            "forecastId": forecast.forecast_id,
            "routeNumber": route_number,
            "deliveryDate": forecast.delivery_date,
            "scheduleKey": forecast.schedule_key,
            "generatedAt": _ts(forecast.generated_at),
            "expiresAt": expires_at,
            "items": [
                {
                    "storeId": item.store_id,
                    "storeName": item.store_name,
                    "sap": item.sap,
                    "recommendedUnits": item.recommended_units,
                    "recommendedCases": item.recommended_cases,
                    "p10Units": item.p10_units,
                    "p50Units": item.p50_units,
                    "p90Units": item.p90_units,
                    "promoActive": item.promo_active,
                    "promoLiftPct": item.promo_lift_pct,
                    "isFirstWeekend": item.is_first_weekend,
                    "confidence": item.confidence,
                    "source": item.source,
                    # Prior order context for overlapping deliveries
                    **({"priorOrderContext": {
                        "orderId": item.prior_order_context.order_id,
                        "orderDate": item.prior_order_context.order_date,
                        "deliveryDate": item.prior_order_context.delivery_date,
                        "quantity": item.prior_order_context.quantity,
                        "scheduleKey": item.prior_order_context.schedule_key,
                    }} if item.prior_order_context else {}),
                    # Expiry replacement metadata for low-qty floor injection
                    **({"expiryReplacement": {
                        "expiryDate": item.expiry_replacement.expiry_date,
                        "minUnitsRequired": item.expiry_replacement.min_units_required,
                        "reason": item.expiry_replacement.reason,
                    }} if item.expiry_replacement else {}),
                    **({"wholeCaseAdjustment": wca}
                       if (wca := _serialize_whole_case_adjustment(item.whole_case_adjustment)) else {}),
                }
                for item in forecast.items
            ],
            "createdAt": firestore.SERVER_TIMESTAMP,
        }
    )
    _archive_forecast_to_disk(route_number=route_number, forecast=forecast, expires_at=expires_at)
