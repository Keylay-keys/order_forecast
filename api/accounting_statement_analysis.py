#!/usr/bin/env python3
"""Bounded cluster single-PDF reader. Never writes financial records."""
import importlib.util
import json
import os
import re
import resource
import subprocess
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

if __package__:
    from .accounting_statement_timestamp import printed_issued_at
    from .accounting_pdf_process import run_bounded
else:
    # Isolated Python startup omits site hooks and implicit script-path imports.
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from accounting_statement_timestamp import printed_issued_at
    from accounting_pdf_process import run_bounded

spec = importlib.util.spec_from_file_location("settlements", Path(__file__).resolve().parents[1] / "scripts/build_settlement_metrics_lab.py")
legacy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(legacy)


def cents(value):
    return None if value is None else int(Decimal(str(value)) * 100)


def analyze(pdf, route):
    sandbox = "/usr/local/bin/routespark-pdf-sandbox"
    info = run_bounded([sandbox, "pdfinfo", str(pdf), str(os.getpid())], stdout_limit=64 * 1024, timeout=10).decode("utf-8")
    pages = int(legacy.required(r"^Pages:\s+(\d+)$", info, "pages"))
    if not 1 <= pages <= 100:
        raise ValueError("Page limit")
    text = run_bounded([sandbox, "pdftotext", str(pdf), str(os.getpid())], stdout_limit=4 * 1024 * 1024, timeout=20).decode("utf-8")
    first = text.split("\f")[0]
    # Printed identity only: the upload name is never a route, date or revision authority.
    billing = legacy.required(r"Billing Period\s+(\d{1,2}/\d{1,2}/\d{4}\s*-\s*\d{1,2}/\d{1,2}/\d{4})", first, "billing period")
    week = datetime.strptime(legacy.required(r"Settlement Week Ending\s+([0-9/]+)", first, "week"), "%m/%d/%Y").date().isoformat()
    now = datetime.now(timezone.utc).isoformat()
    row = dict(route=route, revision="preliminary", normalized_pdf_path=pdf.name,
               pdf_sha256=legacy.sha256(pdf), page_count=str(pages), week_ending=week,
               billing_period=billing, source_received_at=now, model_available_at=now)
    # This worker has its own module instance. Reuse the extractor on bounded, cached text.
    legacy.pdf_text = lambda path, first_page=None, last_page=None: first if first_page else text
    result = legacy.extract_statement(row, pdf.parent, now)
    components = [{"key": item["key"], "label": item["label"], "cents": cents(item["amount"])} for item in result["pay"]["components"]]
    labels = {key: label for label, key, _ in legacy.ADJUSTMENT_FIELDS}
    adjustments = [dict(item, label=labels[item["key"]]) for item in components if item["key"] in labels]
    total_adjustments = cents(legacy.optional_money(r"^\s*Total Adjustments\s+([-0-9,]+\.\d{2})\s*$", first))
    account = {key: cents(value) for key, value in result["account"].items()}
    margin = {key: cents(value) for key, value in result["margin"].items()}
    inventory = {key: cents(value) for key, value in result["inventory"].items()}
    new = cents(result["pay"]["statedNewSettlement"])
    checks = []
    def check(label, calculated, printed):
        checks.append(dict(label=label, calculatedCents=calculated, printedCents=printed))
    check("Total Adjustments", sum(item["cents"] for item in adjustments), total_adjustments)
    check("New Settlement (source components)", sum(item["cents"] for item in components), new)
    check("Total Margin Discount", sum(value or 0 for key, value in margin.items() if key != "totalMarginDiscount"), margin["totalMarginDiscount"])
    initial, current, truck = (inventory[key] for key in ("initialInventory", "currentInventory", "truckInventoryChange"))
    # Older statements omit both inventory snapshots; their signed product movement rows
    # still independently establish the printed Truck inventory amount.
    inventory_calculated = sum(item['cents'] for item in components[:3]) if initial is None and current is None else None if initial is None or current is None else initial - current
    check("Truck inventory", inventory_calculated, truck)
    check("New Settlement", None if truck is None or total_adjustments is None else margin["totalMarginDiscount"] + truck + total_adjustments, new)
    previous, payment, total, due = (account[key] for key in ("previousBalance", "payment", "totalNewSettlement", "amountToPay"))
    check("Total New Settlement", None if previous is None or payment is None else previous + payment + new, total)
    check("Total Amount To Pay", total, due)
    sales = result["sales"]
    returns = next((item["cents"] for item in adjustments if item["key"] == "salesReturns"), 0)
    check("Net sales", cents(sales["grossSales"]) + returns + cents(sales["promotions"]) + cents(sales["networkAllowances"]), cents(sales["netSales"]))
    block = first[first.find("Truck inventory"):first.find("Total Adjustments")]
    cash = next((item["cents"] for item in adjustments if item["key"] == "netCashSales"), None)
    cash_state = "unknown" if re.search("Net Cash Sales", block, re.I) and cash is None else "present" if cash else "none"
    issues = [f'{item["label"]}: missing value or figures do not match exactly.' for item in checks if item["printedCents"] is None or item["calculatedCents"] is None or item["printedCents"] != item["calculatedCents"]]
    for label, key, _ in legacy.ADJUSTMENT_FIELDS:
        if re.search(re.escape(label), block, re.I) and not any(item['key'] == key for item in adjustments):
            issues.append(f'{label} could not be read; it has not been treated as zero.')
    if cash_state == "unknown":
        issues.append("Net Cash Sales could not be read. Cash-account setup is unknown.")
    return dict(schemaVersion=1, route=route, printedIssuedAt=printed_issued_at(first), periodStart=result["periodStart"], periodEnd=result["periodEnd"],
                _evidence=dict(readerVersion="accounting-pdf-v1", sourceText=text, extracted=result),
                pageCount=pages, adjustments=adjustments, marginCents=margin["totalMarginDiscount"],
                inventoryCents=truck, totalAdjustmentsCents=total_adjustments, newSettlementCents=new,
                account=account, amountToPayLabel="Total Amount Due Upon Receipt" if re.search(r'Total Amount Due Upon Receipt', first, re.I) else "Total Amount To Pay",
                checks=checks, cashState=cash_state, cashSalesCents=None if cash_state == "unknown" else -(cash or 0),
                hhcRentalFeeCents=-next((item["cents"] for item in adjustments if item["key"] == "hhcRentalFee"), 0),
                issues=issues)


if __name__ == "__main__":
    # Bound this trusted Python interpreter; each native PDF tool additionally
    # receives its own enforced filesystem/syscall sandbox and memory/CPU limits.
    resource.setrlimit(resource.RLIMIT_CPU, (30, 30))
    if sys.platform != 'darwin':
        resource.setrlimit(resource.RLIMIT_AS, (192 * 1024 * 1024, 192 * 1024 * 1024))
    try:
        print(json.dumps(analyze(Path(sys.argv[1]).resolve(), sys.argv[2]), allow_nan=False))
    except Exception:
        print(json.dumps({"error": "This PDF could not be fully reconciled for this route. The original is saved. Check the route, readable text, and complete statement before retrying; no money was recorded."}))
