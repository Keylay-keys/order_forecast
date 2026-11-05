#!/usr/bin/env python3
"""Build the local-only Business Metrics fixture from normalized settlements.

This utility is intentionally separate from production ingestion. It consumes
the reviewed external-drive manifest, selects the Friday final for each week
(or a clearly provisional fallback when no final exists), verifies PDF hashes,
reconciles every printed settlement equation, and writes one local JSON file.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


CENT = Decimal("0.01")
MAX_SOURCE_BYTES = 30 * 1024 * 1024
MAX_SOURCE_PAGES = 100
REVISION_PRIORITY = {"preliminary": 0, "final": 1}

ADJUSTMENT_FIELDS = (
    ("Returns", "salesReturns", "Sales returns"),
    ("Net Cash Sales", "netCashSales", "Cash-account sales already collected"),
    ("HHC rental fee", "hhcRentalFee", "HHC rental fee"),
    ("Building lease", "buildingLease", "Building lease"),
    ("Other Accruals", "otherAccruals", "Other accruals"),
    ("SBT credits", "sbtCredits", "SBT credits"),
    ("SBT Buybacks", "sbtBuybacks", "SBT buybacks"),
    ("Sales Incentives", "salesIncentives", "Sales incentives"),
    ("Distributor Security Deposits", "distributorSecurityDeposits", "Distributor security deposits"),
    ("Prior Adjustments", "priorAdjustments", "Prior adjustments"),
    ("Current Adjustments", "currentAdjustments", "Current adjustments"),
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def money(value: str) -> Decimal:
    return Decimal(value.replace(",", "")).quantize(CENT)


def as_number(value: Decimal | None) -> float | None:
    return None if value is None else float(value.quantize(CENT))


def required(pattern: str, text: str, field: str, group: int = 1) -> str:
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if not match:
        raise ValueError(f"Missing required settlement field: {field}")
    return match.group(group)


def optional_money(pattern: str, text: str, group: int = 1) -> Decimal | None:
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    return money(match.group(group)) if match else None


def optional_zero_money(pattern: str, label_pattern: str, text: str, field: str) -> Decimal:
    """Return zero only when an optional printed row is wholly absent.

    A present label with an unreadable amount remains a hard failure. Later
    statement equations independently verify that an omitted row was truly zero.
    """
    value = optional_money(pattern, text)
    if value is not None:
        return value
    if re.search(label_pattern, text, re.IGNORECASE | re.MULTILINE):
        raise ValueError(f"Missing required settlement field: {field}")
    return Decimal("0.00")


def safe_manifest_pdf(root: Path, relative_value: str) -> Path:
    relative = Path(relative_value)
    if relative.is_absolute() or ".." in relative.parts or relative.suffix.lower() != ".pdf":
        raise ValueError(f"Unsafe normalized PDF path in manifest: {relative_value!r}")
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve()
    if resolved_root not in resolved.parents:
        raise ValueError(f"Settlement PDF escapes the archive root: {relative_value!r}")
    if not resolved.is_file():
        raise ValueError(f"Settlement PDF is missing: {resolved}")
    size = resolved.stat().st_size
    if size <= 0 or size > MAX_SOURCE_BYTES:
        raise ValueError(f"Settlement PDF size is outside the accepted range: {resolved}")
    return resolved


def pdf_text(path: Path, first_page: int | None = None, last_page: int | None = None) -> str:
    command = ["pdftotext"]
    if first_page is not None:
        command.extend(["-f", str(first_page)])
    if last_page is not None:
        command.extend(["-l", str(last_page)])
    command.extend(["-layout", str(path), "-"])
    return subprocess.run(command, check=True, capture_output=True, text=True).stdout


def parse_period(value: str) -> tuple[str, str]:
    match = re.fullmatch(r"\s*(\d{1,2}/\d{1,2}/\d{4})\s*-\s*(\d{1,2}/\d{1,2}/\d{4})\s*", value)
    if not match:
        raise ValueError(f"Invalid billing period: {value!r}")
    return tuple(datetime.strptime(item, "%m/%d/%Y").date().isoformat() for item in match.groups())  # type: ignore[return-value]


def extract_statement(row: dict[str, str], root: Path, generated_at: str) -> dict[str, Any]:
    route = row["route"]
    if not re.fullmatch(r"\d{1,10}", route):
        raise ValueError(f"Invalid route in manifest: {route!r}")
    revision = row["revision"]
    if revision not in REVISION_PRIORITY:
        raise ValueError(f"Invalid revision in manifest: {revision!r}")

    path = safe_manifest_pdf(root, row["normalized_pdf_path"])
    if sha256(path) != row["pdf_sha256"]:
        raise ValueError(f"PDF hash mismatch: {path}")
    page_count = int(row["page_count"])
    if page_count <= 0 or page_count > MAX_SOURCE_PAGES:
        raise ValueError(f"Invalid page count in manifest: {page_count}")

    page_one = pdf_text(path, 1, 1)
    all_pages = pdf_text(path)
    printed_route = required(
        r"Distributor Number\s+([0-9]+)\s*/\s*[0-9]+", page_one, "route"
    )
    printed_week = datetime.strptime(
        required(r"Settlement Week Ending\s+([0-9/]+)", page_one, "week ending"),
        "%m/%d/%Y",
    ).date().isoformat()
    if printed_route != route or printed_week != row["week_ending"]:
        raise ValueError(f"Manifest/PDF identity mismatch: {path}")
    period_start, period_end = parse_period(row["billing_period"])
    if period_end != printed_week:
        raise ValueError(f"Billing period does not end on statement week: {path}")

    purchases_match = re.search(
        r"^\s*Total Purchases\s+([0-9,]+\.\d{2})\s+([0-9,]+\.\d{2})\s+[0-9.]+\s*%\s*$",
        page_one,
        re.IGNORECASE | re.MULTILINE,
    )
    if not purchases_match:
        raise ValueError(f"Missing purchases/margin row: {path}")
    margin_on_purchases = money(purchases_match.group(2))

    total_purchases = money(
        required(r"^\s*Total Purchases\s+(-[0-9,]+\.\d{2})\s*$", page_one, "signed total purchases")
    )
    non_sbt_gross = money(
        required(r"^\s*Gross Sales - Bybk\s+([-0-9,]+\.\d{2})\s*$", page_one, "gross sales less buyback")
    )
    sbt_gross = optional_zero_money(
        r"^\s*SBTv1 Gross\s+([-0-9,]+\.\d{2})\s*$",
        r"^\s*SBTv1 Gross\b",
        page_one,
        "SBT gross",
    )
    margin_credited = money(
        required(r"^\s*Total Margin Discount\s+([-0-9,]+\.\d{2})\s*$", page_one, "total margin discount")
    )
    stated_new_settlement = money(
        required(r"^\s*New Settlement\s+([-0-9,]+\.\d{2})\s*$", page_one, "new settlement")
    )

    adjustment_start = page_one.find("Truck inventory")
    adjustment_end = page_one.find("Total Adjustments", adjustment_start)
    if adjustment_start < 0 or adjustment_end < 0:
        raise ValueError(f"Missing adjustment section boundaries: {path}")
    adjustment_block = page_one[adjustment_start:adjustment_end]

    components: list[dict[str, Any]] = [
        {"key": "totalPurchases", "label": "Product purchased", "amount": as_number(total_purchases)},
        {"key": "grossSalesLessBuyback", "label": "Non-SBT gross sales", "amount": as_number(non_sbt_gross)},
        {"key": "sbtGross", "label": "SBT gross sales", "amount": as_number(sbt_gross)},
        {"key": "totalMarginDiscount", "label": "Gross route margin", "amount": as_number(margin_credited)},
    ]
    adjustment_sum = Decimal("0")
    adjustment_values: dict[str, Decimal] = {}
    for printed_label, key, display_label in ADJUSTMENT_FIELDS:
        value = optional_money(
            rf"^.*?{re.escape(printed_label)}\s+([-0-9,]+\.\d{{2}})\s*$",
            adjustment_block,
        )
        if value is not None:
            adjustment_sum += value
            adjustment_values[key] = value
            components.append({"key": key, "label": display_label, "amount": as_number(value)})
    printed_adjustments = money(
        required(r"^\s*Total Adjustments\s+([-0-9,]+\.\d{2})\s*$", page_one, "total adjustments")
    )
    if abs(adjustment_sum - printed_adjustments) > CENT:
        raise ValueError(
            f"Adjustment detail does not reconcile for {path}: {adjustment_sum} != {printed_adjustments}"
        )

    calculated = sum((Decimal(str(item["amount"])) for item in components), Decimal("0")).quantize(CENT)
    difference = (stated_new_settlement - calculated).quantize(CENT)
    if abs(difference) > CENT:
        raise ValueError(
            f"New Settlement does not reconcile for {path}: {calculated} != {stated_new_settlement}"
        )

    sales_match = re.search(
        r"^\s*Total Sales\s+([-0-9,]+\.\d{2})\s+([-0-9,]+\.\d{2})\s+([-0-9,]+\.\d{2})\s+([-0-9,]+\.\d{4})\s+([-0-9,]+\.\d{2})\s+([-0-9,]+\.\d{2})\s*$",
        all_pages,
        re.IGNORECASE | re.MULTILINE,
    )
    if not sales_match:
        raise ValueError(f"Missing Total Sales summary row: {path}")
    gross_sales, sales_returns, promotions, network_allowances, net_sales = (
        money(sales_match.group(index)) for index in range(1, 6)
    )
    # SBTv1 Gross on page 1 is already net of SBTv1 Returns. The Sales Summary
    # repeats those store returns in its Returns column for visibility, so
    # subtracting that column again would double count them. The settlement's
    # signed Returns component is the amount used in the printed Net Inv total.
    settlement_returns = adjustment_values.get("salesReturns", Decimal("0"))
    calculated_net_sales = (
        gross_sales + settlement_returns + promotions + network_allowances
    ).quantize(CENT)
    if abs(calculated_net_sales - net_sales) > CENT:
        raise ValueError(
            f"Total Sales row does not reconcile for {path}: {calculated_net_sales} != {net_sales}"
        )

    margin_promotions = optional_money(
        r"^\s*Margin Discount on Promotions\s+([-0-9,]+\.\d{2})", page_one
    )
    margin_sbt = optional_money(
        r"^\s*Margin Discount on SBT\s+([-0-9,]+\.\d{2})", page_one
    )
    calculated_margin = (
        margin_on_purchases
        + (margin_promotions or Decimal("0"))
        + (margin_sbt or Decimal("0"))
    ).quantize(CENT)
    if abs(calculated_margin - margin_credited) > CENT:
        raise ValueError(
            f"Margin row does not reconcile for {path}: {calculated_margin} != {margin_credited}"
        )

    previous_balance = optional_money(r"^\s*Previous Balance\s+([-0-9,]+\.\d{2})\s*$", page_one)
    payment = optional_money(r"^\s*Payment as of\s+\S+\s+\d{1,2},\s+\d{4}\s+([-0-9,]+\.\d{2})\s*$", page_one)
    total_new_settlement = optional_money(r"^\s*Total New Settlement\s+([-0-9,]+\.\d{2})\s*$", page_one)
    amount_to_pay = optional_money(r"^\s*Total Amount (?:To Pay|Due Upon Receipt)\s+([-0-9,]+\.\d{2})\s*$", page_one)

    initial_inventory_match = re.search(
        r"^\s*Initial Inv\..*\n\s*([-0-9,]+\.\d{2})\s*$", page_one, re.IGNORECASE | re.MULTILINE
    )
    current_inventory_match = re.search(
        r"^\s*Current Inv\..*\n\s*([-0-9,]+\.\d{2})", page_one, re.IGNORECASE | re.MULTILINE
    )
    initial_inventory = money(initial_inventory_match.group(1)) if initial_inventory_match else None
    current_inventory = money(current_inventory_match.group(1)) if current_inventory_match else None
    truck_inventory_change = optional_money(
        r"^.*Truck inventory\s+([-0-9,]+\.\d{2})\s*$", page_one
    )
    if initial_inventory is not None and current_inventory is not None and truck_inventory_change is not None:
        calculated_inventory_change = (initial_inventory - current_inventory).quantize(CENT)
        if abs(calculated_inventory_change - truck_inventory_change) > CENT:
            raise ValueError(
                f"Inventory change does not reconcile for {path}: {calculated_inventory_change} != {truck_inventory_change}"
            )
    if previous_balance is not None and payment is not None and total_new_settlement is not None:
        calculated_account_total = (
            previous_balance + payment + stated_new_settlement
        ).quantize(CENT)
        if abs(calculated_account_total - total_new_settlement) > CENT:
            raise ValueError(
                f"Account Summary does not reconcile for {path}: {calculated_account_total} != {total_new_settlement}"
            )
    if amount_to_pay is not None and total_new_settlement is not None:
        if abs(amount_to_pay - total_new_settlement) > CENT:
            raise ValueError(
                f"Amount to pay does not match Total New Settlement for {path}"
            )

    source_status = "accepted" if revision == "final" else "provisional"
    return {
        "id": f"week-{period_end}",
        "routeNumber": route,
        "schemaVersion": 1,
        "currency": "USD",
        "periodStart": period_start,
        "periodEnd": period_end,
        "statementReceivedAt": row["source_received_at"],
        "availableAt": row["model_available_at"],
        "pay": {
            "components": components,
            "calculatedNewSettlement": as_number(calculated),
            "statedNewSettlement": as_number(stated_new_settlement),
            "difference": as_number(difference),
            "reconciled": True,
        },
        "account": {
            "previousBalance": as_number(previous_balance),
            "payment": as_number(payment),
            "totalNewSettlement": as_number(total_new_settlement),
            "amountToPay": as_number(amount_to_pay),
        },
        "margin": {
            "marginOnPurchases": as_number(margin_on_purchases),
            "marginDiscountPromotions": as_number(margin_promotions),
            "marginDiscountSbt": as_number(margin_sbt),
            "totalMarginDiscount": as_number(margin_credited),
        },
        "sales": {
            "grossSales": as_number(gross_sales),
            "returns": as_number(sales_returns),
            "promotions": as_number(promotions),
            "networkAllowances": as_number(network_allowances),
            "netSales": as_number(net_sales),
        },
        "inventory": {
            "initialInventory": as_number(initial_inventory),
            "currentInventory": as_number(current_inventory),
            "truckInventoryChange": as_number(truck_inventory_change),
        },
        "source": {
            "documentId": f"sha256:{row['pdf_sha256']}",
            "pageCount": page_count,
            "reviewStatus": source_status,
            "revision": revision,
            "finalMissing": revision != "final",
            "validationMethod": "machine_extracted_cross_reconciled",
            "reviewedAt": generated_at,
        },
    }


def select_weekly_rows(rows: list[dict[str, str]], route: str) -> list[dict[str, str]]:
    route_rows = [row for row in rows if row.get("route") == route]
    if not route_rows:
        raise ValueError(f"No manifest rows found for route {route}")
    by_week: dict[str, list[dict[str, str]]] = {}
    for row in route_rows:
        by_week.setdefault(row["week_ending"], []).append(row)
    selected: list[dict[str, str]] = []
    for week, candidates in sorted(by_week.items()):
        revisions = [candidate["revision"] for candidate in candidates]
        if len(revisions) != len(set(revisions)):
            raise ValueError(f"Duplicate revision for route {route}, week {week}")
        selected.append(max(candidates, key=lambda item: REVISION_PRIORITY.get(item["revision"], -1)))
    return selected


def build_fixture(manifest: Path, archive_root: Path, output: Path, route: str) -> dict[str, Any]:
    # Production extraction imports this module without building a CSV fixture.
    import csv

    rows = list(csv.DictReader(manifest.open(encoding="utf-8")))
    if not rows:
        raise ValueError("Settlement manifest is empty")
    selected = select_weekly_rows(rows, route)
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    settlements = [extract_statement(row, archive_root, generated_at) for row in selected]
    settlements.sort(key=lambda item: item["periodEnd"], reverse=True)

    expenses: list[dict[str, Any]] = []
    payroll: list[dict[str, Any]] = []
    if output.is_file():
        previous = json.loads(output.read_text(encoding="utf-8"))
        previous_route = previous.get("routes", {}).get(route, {})
        expenses = previous_route.get("expenses", [])
        payroll = previous_route.get("payroll", [])
        if not isinstance(expenses, list) or not isinstance(payroll, list):
            raise ValueError("Existing expense/payroll fixture data is invalid")

    provisional = [item["periodEnd"] for item in settlements if item["source"]["finalMissing"]]
    final_count = len(settlements) - len(provisional)
    payload = {
        "schemaVersion": 1,
        "readOnly": True,
        "generatedAt": generated_at,
        "source": {
            "kind": "normalized-settlement-email-series",
            "routeNumber": route,
            "periodStart": min(item["periodStart"] for item in settlements),
            "periodEnd": max(item["periodEnd"] for item in settlements),
            "statementCount": len(settlements),
            "finalStatementCount": final_count,
            "provisionalStatementCount": len(provisional),
            "provisionalWeekEndings": provisional,
            "manifestSha256": sha256(manifest),
            "productionWrite": False,
        },
        "routes": {
            route: {
                "settlements": settlements,
                "expenses": expenses,
                "payroll": payroll,
            }
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_name(f".{output.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)
    return payload


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--archiveRoot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--route", default="989262")
    args = parser.parse_args()
    if not re.fullmatch(r"\d{1,10}", args.route):
        parser.error("--route must contain 1-10 digits")
    payload = build_fixture(args.manifest, args.archiveRoot, args.output, args.route)
    source = payload["source"]
    print(
        json.dumps(
            {
                "output": str(args.output),
                "route": args.route,
                "weeks": source["statementCount"],
                "final_weeks": source["finalStatementCount"],
                "provisional_weeks": source["provisionalWeekEndings"],
                "period_start": source["periodStart"],
                "period_end": source["periodEnd"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
