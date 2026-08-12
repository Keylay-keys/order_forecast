"""Canonical schema-v2 forecast validation and fingerprinting."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple


SCHEMA_VERSION = 2
READY_STATE = "ready"
GENERATION_MODES = {"model", "last_order"}
Key = Tuple[str, str]


class ForecastContractError(ValueError):
    """Raised when a forecast cannot be safely published or attached."""


def canonical_key(store_id: Any, sap: Any) -> Key:
    key = (str(store_id or "").strip(), str(sap or "").strip())
    if not all(key):
        raise ForecastContractError("missing_store_or_sap")
    return key


def stable_fingerprint(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def key_fingerprint(keys: Iterable[Key]) -> str:
    return stable_fingerprint([list(key) for key in sorted(set(keys))])


def generation_input_fingerprint(
    route_number: str,
    delivery_date: str,
    schedule_key: str,
    active_carry_case_packs: Iterable[Tuple[str, str, int]],
) -> str:
    return stable_fingerprint({
        "contractVersion": 2,
        "routeNumber": str(route_number),
        "deliveryDate": delivery_date,
        "scheduleKey": schedule_key,
        "activeCarry": [list(row) for row in sorted(set(active_carry_case_packs))],
    })


def normalize_units(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ForecastContractError("recommended_units_not_numeric")
    if not math.isfinite(float(value)) or float(value) < 0 or not float(value).is_integer():
        raise ForecastContractError("recommended_units_not_nonnegative_integer")
    return int(value)


def normalize_items(items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: set[Key] = set()
    for raw in items:
        key = canonical_key(raw.get("storeId"), raw.get("sap"))
        if key in seen:
            raise ForecastContractError(f"duplicate_forecast_key:{key[0]}:{key[1]}")
        seen.add(key)
        row = dict(raw)
        row["storeId"], row["sap"] = key
        row["recommendedUnits"] = normalize_units(raw.get("recommendedUnits"))
        row["source"] = str(raw.get("source") or "unknown")
        normalized.append(row)
    return sorted(normalized, key=lambda row: (row["storeId"], row["sap"]))


def artifact_semantic_payload(artifact: Mapping[str, Any]) -> Dict[str, Any]:
    eligibility = artifact.get("eligibility") or {}
    return {
        "schemaVersion": artifact.get("schemaVersion"),
        "state": artifact.get("state"),
        "forecastId": str(artifact.get("forecastId") or ""),
        "generationMode": artifact.get("generationMode"),
        "routeNumber": str(artifact.get("routeNumber") or ""),
        "deliveryDate": artifact.get("deliveryDate"),
        "scheduleKey": artifact.get("scheduleKey"),
        "generationInputFingerprint": artifact.get("generationInputFingerprint"),
        "eligibility": {
            "activeCarryItemCount": eligibility.get("activeCarryItemCount"),
            "emittedItemCount": eligibility.get("emittedItemCount"),
            "zeroItemCount": eligibility.get("zeroItemCount"),
            "activeCarryFingerprint": eligibility.get("activeCarryFingerprint"),
        },
        "items": normalize_items(artifact.get("items") or []),
    }


def validate_ready_artifact(
    artifact: Mapping[str, Any],
    *,
    route_number: str,
    delivery_date: str,
    schedule_key: str,
    active_carry_keys: Iterable[Key] | None = None,
) -> List[Dict[str, Any]]:
    if artifact.get("schemaVersion") != SCHEMA_VERSION or artifact.get("state") != READY_STATE:
        raise ForecastContractError("unsupported_or_unready_forecast")
    if artifact.get("generationMode") not in GENERATION_MODES:
        raise ForecastContractError("unsupported_generation_mode")
    if str(artifact.get("routeNumber") or "") != str(route_number):
        raise ForecastContractError("route_mismatch")
    if artifact.get("deliveryDate") != delivery_date or artifact.get("scheduleKey") != schedule_key:
        raise ForecastContractError("target_mismatch")

    items = normalize_items(artifact.get("items") or [])
    item_keys = {(row["storeId"], row["sap"]) for row in items}
    eligibility = artifact.get("eligibility") or {}
    expected_zero_count = sum(row["recommendedUnits"] == 0 for row in items)
    if eligibility.get("emittedItemCount") != len(items):
        raise ForecastContractError("emitted_count_mismatch")
    if eligibility.get("zeroItemCount") != expected_zero_count:
        raise ForecastContractError("zero_count_mismatch")
    if eligibility.get("activeCarryItemCount") != len(item_keys):
        raise ForecastContractError("active_carry_count_mismatch")
    if eligibility.get("activeCarryFingerprint") != key_fingerprint(item_keys):
        raise ForecastContractError("active_carry_fingerprint_mismatch")
    if active_carry_keys is not None and set(active_carry_keys) != item_keys:
        raise ForecastContractError("active_carry_stale")
    if artifact.get("artifactFingerprint") != stable_fingerprint(artifact_semantic_payload(artifact)):
        raise ForecastContractError("artifact_fingerprint_mismatch")
    return items


def build_order_snapshot(
    artifact_items: Sequence[Mapping[str, Any]],
    active_carry_keys: Iterable[Key],
    order_item_keys: Iterable[Key],
) -> Tuple[List[Dict[str, Any]], str]:
    by_key = {canonical_key(row.get("storeId"), row.get("sap")): row for row in artifact_items}
    active = set(active_carry_keys)
    eligible = active | set(order_item_keys)
    snapshot: List[Dict[str, Any]] = []
    for key in sorted(eligible):
        raw = by_key.get(key)
        if raw is None and key in active:
            raise ForecastContractError(f"missing_active_carry_key:{key[0]}:{key[1]}")
        snapshot.append({
            "storeId": key[0],
            "sap": key[1],
            "recommendedUnits": normalize_units(raw.get("recommendedUnits")) if raw else 0,
            "source": str(raw.get("source") or "unknown") if raw else "order_only_zero",
            **({"promoActive": bool(raw["promoActive"])} if raw and "promoActive" in raw else {}),
        })
    return snapshot, key_fingerprint(eligible)
