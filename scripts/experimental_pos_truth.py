"""Test-lab helpers for resolving POS invoice UPCs to reference SAPs.

This module is deliberately not imported by the production forecast engine.
It normalizes the retailer encodings observed on route 989262 while failing
closed on catalog aliases unless route/order context supports exactly one SAP.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable, Mapping, Optional


def normalize_upc_digits(value: Any) -> str:
    """Strip formatting without silently discarding meaningful digits."""
    return re.sub(r"\D", "", str(value or ""))


def gs1_check_digit(payload: str) -> int:
    """Return the GS1 modulo-10 check digit for a numeric payload."""
    if not payload or not payload.isdigit():
        raise ValueError("GS1 payload must contain digits")
    weighted = 0
    for offset, char in enumerate(reversed(payload)):
        weighted += int(char) * (3 if offset % 2 == 0 else 1)
    return (10 - (weighted % 10)) % 10


def has_valid_gs1_check_digit(value: str) -> bool:
    digits = normalize_upc_digits(value)
    return len(digits) >= 2 and gs1_check_digit(digits[:-1]) == int(digits[-1])


def reference_catalog_keys(invoice_upc: Any) -> tuple[str, ...]:
    """Return validated reference-catalog lookup keys for an invoice UPC.

    The RouteSpark reference catalog stores the 10-digit manufacturer/item
    body (for example ``7373100249``). Harmon's prints a leading zero plus that
    body, Sam's prints the body directly, and Walmart prints a zero-padded
    GTIN-14 containing a UPC-A check digit.
    """
    raw = normalize_upc_digits(invoice_upc)
    keys: list[str] = []

    if raw:
        keys.append(raw)
    if len(raw) == 10:
        keys.append(raw)
    elif len(raw) == 11 and raw.startswith("0"):
        # Harmon's vendor receipt representation: leading zero plus body, with
        # no printed check digit.
        keys.append(raw[1:])
    elif len(raw) == 12 and has_valid_gs1_check_digit(raw):
        # UPC-A: number-system digit + manufacturer/item body + check digit.
        keys.append(raw[1:-1])
    elif (
        len(raw) == 14
        and raw.startswith("00")
        and has_valid_gs1_check_digit(raw)
    ):
        # Walmart's GTIN-14 is a zero-padded UPC-A.
        keys.append(raw[3:-1])

    return tuple(dict.fromkeys(key for key in keys if key))


@dataclass(frozen=True)
class UPCResolution:
    invoice_upc: str
    catalog_keys: tuple[str, ...]
    sap_candidates: tuple[str, ...]
    resolved_sap: Optional[str]
    status: str


def build_reference_upc_index(
    catalog_items: Iterable[Mapping[str, Any]],
) -> dict[str, tuple[str, ...]]:
    """Build an immutable digit-normalized UPC index from active items."""
    mutable: dict[str, set[str]] = {}
    for item in catalog_items:
        if item.get("active") is False:
            continue
        key = normalize_upc_digits(item.get("upc"))
        sap = str(item.get("sap") or "").strip()
        if key and sap:
            mutable.setdefault(key, set()).add(sap)
    return {key: tuple(sorted(values)) for key, values in mutable.items()}


def resolve_invoice_upc(
    invoice_upc: Any,
    reference_index: Mapping[str, Iterable[str]],
    *,
    supported_route_saps: Optional[Iterable[str]] = None,
) -> UPCResolution:
    """Resolve an invoice UPC, failing closed when aliases remain ambiguous."""
    raw = normalize_upc_digits(invoice_upc)
    keys = reference_catalog_keys(raw)
    candidates = tuple(
        sorted(
            {
                str(sap)
                for key in keys
                for sap in reference_index.get(key, ())
            }
        )
    )

    if not candidates:
        return UPCResolution(raw, keys, candidates, None, "unmatched")
    if len(candidates) == 1:
        return UPCResolution(raw, keys, candidates, candidates[0], "unique_catalog")

    supported = {str(sap) for sap in (supported_route_saps or ())}
    supported_candidates = tuple(sap for sap in candidates if sap in supported)
    if len(supported_candidates) == 1:
        return UPCResolution(
            raw,
            keys,
            candidates,
            supported_candidates[0],
            "unique_route_context",
        )

    return UPCResolution(raw, keys, candidates, None, "ambiguous_catalog")
