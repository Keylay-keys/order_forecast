"""Canonical, read-only facts for the Accountant handoff.

The preview deliberately contains no presentation labels or document bytes.
Export generation consumes the same snapshot builder in a separate module.
"""
from copy import deepcopy
from datetime import date, datetime
import hashlib
import json
import re

from .accounting_domain import Accounting, fail, now
from .accounting_performance import gross_sales_cents
from . import accounting_evidence as evidence


MAX_SOURCE_FILES = 5000
MAX_SOURCE_BYTES = 1024 * 1024 * 1024
STATEMENT_FACT_FIELDS = (
    "grossSalesCents",
    "marginCents",
    "inventoryCents",
    "totalAdjustmentsCents",
    "newSettlementCents",
    "previousBalanceCents",
    "statementPaymentCents",
    "amountToPayCents",
    "handheldRentalCents",
    "cashAccountSalesCents",
)


def _unverified():
    fail("Accountant handoff evidence could not be verified.", 503)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def _digest(value):
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _year(value):
    if type(value) is not int or not 2000 <= value <= 2100:
        fail("Choose a valid handoff year.")
    return str(value)


def _is_day(value):
    if type(value) is not str or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        return False
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        return False


def _is_timestamp(value):
    if type(value) is not str:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.tzinfo is not None
    except ValueError:
        return False


def _original_descriptor(store, route, kind, upload_id, file_id):
    manifest = store.get(f"{kind}-upload:{upload_id}")
    if type(manifest) is not dict or manifest.get("route") != route or type(manifest.get("files")) is not list:
        _unverified()
    matches = [item for item in manifest["files"] if type(item) is dict and item.get("id") == file_id]
    if len(matches) != 1:
        _unverified()
    expected = matches[0]
    try:
        if hasattr(store, "original_metadata"):
            source = store.original_metadata(kind, upload_id, file_id)
        else:
            source = store.original(kind, upload_id, file_id)
            content = bytes(source["content"])
            if hashlib.sha256(content).hexdigest() != source["sha256"]:
                _unverified()
            source = dict(source, bytes=len(content))
        if (source is None or source["sha256"] != expected["sha256"]
                or source["mediaType"] != expected.get("mediaType", "application/pdf")
                or source["bytes"] != expected["bytes"]):
            _unverified()
        return {
            "fileId": file_id,
            "sha256": source["sha256"],
            "mediaType": source["mediaType"],
            "bytes": source["bytes"],
        }
    except (KeyError, TypeError, ValueError):
        _unverified()


def _confirmed_statement(store, route, period):
    try:
        review = period["statementReview"]
        upload_id, file_id, attempt_id = (review[name] for name in ("uploadId", "fileId", "attemptId"))
        attempt = store.attempt("statement", upload_id, file_id, attempt_id)
        source = _original_descriptor(store, route, "statement", upload_id, file_id)
        if (period.get("revision") not in ("preliminary", "final")
                or type(attempt) is not dict or attempt.get("id") != attempt_id
                or attempt.get("uploadId") != upload_id or attempt.get("fileId") != file_id
                or attempt.get("route") != route or attempt.get("sha256") != source["sha256"]
                or period.get("source") != "sha256:" + source["sha256"]):
            _unverified()
        analysis = attempt.get("analysis")
        if not evidence.valid_analysis(analysis, route) or analysis.get("issues"):
            _unverified()
        account = analysis["account"]
        adjustments_by_key = {item["key"]: item["cents"] for item in analysis["adjustments"]}
        required_amounts = [analysis[name] for name in ("marginCents", "inventoryCents", "totalAdjustmentsCents",
                                                        "newSettlementCents", "hhcRentalFeeCents", "cashSalesCents")]
        required_amounts += [account[name] for name in ("previousBalance", "payment", "totalNewSettlement", "amountToPay")]
        checks = [
            all(type(item.get(name)) is int for item in analysis["checks"] for name in ("calculatedCents", "printedCents")),
            all(item["calculatedCents"] == item["printedCents"] for item in analysis["checks"]),
            all(type(value) is int for value in required_amounts),
            len(adjustments_by_key) == len(analysis["adjustments"]),
            sum(item["cents"] for item in analysis["adjustments"]) == analysis["totalAdjustmentsCents"],
            analysis["marginCents"] + analysis["inventoryCents"] + analysis["totalAdjustmentsCents"] == analysis["newSettlementCents"],
            analysis["cashSalesCents"] == -adjustments_by_key.get("netCashSales", 0),
            analysis["hhcRentalFeeCents"] == -adjustments_by_key.get("hhcRentalFee", 0),
            account["previousBalance"] + account["payment"] + analysis["newSettlementCents"] == account["totalNewSettlement"],
            account["totalNewSettlement"] == account["amountToPay"],
        ]
        if not all(checks):
            _unverified()
        comparisons = {
            "periodStart": analysis["periodStart"],
            "periodEnd": analysis["periodEnd"],
            "newSettlementCents": analysis["newSettlementCents"],
            "hhcRentalFeeCents": analysis["hhcRentalFeeCents"],
            "cashSalesCents": analysis["cashSalesCents"],
            "amountToPayCents": account["amountToPay"],
            "previousBalanceCents": account["previousBalance"],
            "statementPaymentCents": account["payment"],
        }
        if any(period.get(key) != value for key, value in comparisons.items()):
            _unverified()
        facts = {
            "grossSalesCents": gross_sales_cents(attempt),
            "marginCents": analysis["marginCents"],
            "inventoryCents": analysis["inventoryCents"],
            "totalAdjustmentsCents": analysis["totalAdjustmentsCents"],
            "newSettlementCents": analysis["newSettlementCents"],
            "previousBalanceCents": account["previousBalance"],
            "statementPaymentCents": account["payment"],
            "amountToPayCents": account["amountToPay"],
            "handheldRentalCents": analysis["hhcRentalFeeCents"],
            "cashAccountSalesCents": analysis["cashSalesCents"],
            "adjustments": [
                {"key": item["key"], "sourceLabel": item["label"], "cents": item["cents"]}
                for item in analysis["adjustments"]
            ],
        }
        return {
            "periodStart": analysis["periodStart"],
            "periodEnd": analysis["periodEnd"],
            "crossesCalendarYear": analysis["periodStart"][:4] != analysis["periodEnd"][:4],
            "version": period["revision"],
            "source": {
                "uploadId": upload_id,
                "fileId": file_id,
                "attemptId": attempt_id,
                "sha256": source["sha256"],
                "mediaType": source["mediaType"],
            },
            "facts": facts,
            "unknownFields": [name for name in STATEMENT_FACT_FIELDS if facts[name] is None],
            "_sourceBytes": source["bytes"],
        }
    except (KeyError, TypeError, ValueError):
        _unverified()


def _copy_payment(item, cash=False):
    keys = ["id", "periodEnd", "receivedDate", "amountCents", "method", "note"]
    if cash:
        keys += ["customer", "storeId"]
    return {key: deepcopy(item[key]) for key in keys if key in item}


def _copy_expense(item):
    return {
        "id": item["id"],
        "date": item["date"],
        "vendor": item["vendor"],
        "category": item["category"],
        "amountCents": item["amountCents"],
        "paymentMethod": item.get("paymentMethod"),
        "note": item.get("note", ""),
        "splits": deepcopy(item.get("splits", [])),
        "supportingPurchaseIds": [],
    }


def _gap(code, record_type, record_id, dates, fields=()):
    return {
        "code": code,
        "recordType": record_type,
        "recordId": record_id,
        "dates": dates,
        "fields": list(fields),
    }


def _statement_summaries(periods):
    result = {}
    for name in STATEMENT_FACT_FIELDS:
        values = [row["facts"][name] for row in periods]
        known = [value for value in values if value is not None]
        result[name] = {
            "valueCents": sum(known),
            "knownCount": len(known),
            "unknownCount": len(values) - len(known),
        }
    return result


def build_handoff_preview(store, route, year, *, generated_at=None):
    """Build the canonical handoff preview without returning original bytes."""
    year_text = _year(year)
    accounting = Accounting(store, route, f"{year_text}-12-31")
    expenses_ledger = accounting.load("expenses")
    money_ledger = accounting.load("money-in")
    cash_ledger = accounting.load("cash-accounts")

    gaps = []
    statement_periods = []
    source_file_count = 0
    source_bytes = 0
    if any(type(period) is not dict or not _is_day(period.get("periodStart")) or not _is_day(period.get("periodEnd"))
           for period in money_ledger["settlements"]):
        _unverified()
    selected_periods = [period for period in money_ledger["settlements"] if period["periodEnd"][:4] == year_text]
    if len({period["periodEnd"] for period in selected_periods}) != len(selected_periods):
        _unverified()
    for period in sorted(selected_periods, key=lambda item: item["periodEnd"]):
        row = _confirmed_statement(store, route, period)
        source_bytes += row.pop("_sourceBytes")
        source_file_count += 1
        statement_periods.append(row)
        if row["unknownFields"]:
            gaps.append(_gap("STATEMENT_FACTS_UNKNOWN", "settlementStatement", row["source"]["attemptId"],
                             {"periodStart": row["periodStart"], "periodEnd": row["periodEnd"]}, row["unknownFields"]))
        if row["version"] == "preliminary":
            gaps.append(_gap("PRELIMINARY_STATEMENT_ONLY", "settlementStatement", row["source"]["attemptId"],
                             {"periodStart": row["periodStart"], "periodEnd": row["periodEnd"]}))
        if row["crossesCalendarYear"]:
            gaps.append(_gap("BILLING_PERIOD_CROSSES_CALENDAR_YEAR", "settlementStatement", row["source"]["attemptId"],
                             {"periodStart": row["periodStart"], "periodEnd": row["periodEnd"]}))

    if any(type(item) is not dict or not _is_day(item.get("periodEnd")) or not _is_day(item.get("receivedDate"))
           for item in money_ledger["remittances"]):
        _unverified()
    settlement_payments = sorted(
        [_copy_payment(item) for item in money_ledger["remittances"] if item["receivedDate"][:4] == year_text],
        key=lambda item: (item["receivedDate"], item["id"]),
    )
    if any(type(item) is not dict or not _is_day(item.get("periodEnd")) or not _is_day(item.get("receivedDate"))
           for item in cash_ledger["payments"]):
        _unverified()
    cash_collections = sorted(
        [_copy_payment(item, cash=True) for item in cash_ledger["payments"]
         if not item.get("archivedAt") and item["receivedDate"][:4] == year_text],
        key=lambda item: (item["receivedDate"], item["id"]),
    )
    if any(type(item) is not dict or not _is_day(item.get("date")) for item in expenses_ledger["items"]):
        _unverified()
    paid_expenses = sorted(
        [_copy_expense(item) for item in expenses_ledger["items"]
         if not item.get("archivedAt") and item["date"][:4] == year_text],
        key=lambda item: (item["date"], item["id"]),
    )
    if len({item["id"] for item in paid_expenses}) != len(paid_expenses):
        _unverified()
    expenses_by_id = {item["id"]: item for item in paid_expenses}
    all_active_expense_ids = {item["id"] for item in expenses_ledger["items"] if not item.get("archivedAt")}

    supporting_purchases = []
    relevant_reviews = []
    seen_purchase_ids = set()
    for manifest in store.documents("receipt-upload:"):
        if type(manifest) is not dict or manifest.get("route") != route or type(manifest.get("files")) is not list:
            _unverified()
        review = evidence.receipt_review(store, route, manifest)
        try:
            created_at = manifest["createdAt"]
            purchase_id = manifest["id"]
            if purchase_id in seen_purchase_ids:
                _unverified()
            seen_purchase_ids.add(purchase_id)
            included = review["includeInAccountantHandoff"]
            grouped = review["groupedFileIds"]
            linked = review.get("linkedExpenseId")
            archived = bool(review.get("archivedAt"))
            if (not _is_timestamp(created_at)
                    or type(included) is not bool
                    or type(grouped) is not list or not grouped or len(grouped) != len(set(grouped))):
                _unverified()
        except (KeyError, TypeError):
            _unverified()
        relevant = created_at[:4] == year_text or linked in expenses_by_id
        if relevant:
            relevant_reviews.append({
                "purchaseId": purchase_id,
                "revision": review.get("revision"),
                "archivedAt": review.get("archivedAt"),
                "linkedExpenseId": linked,
                "includeInAccountantHandoff": included,
                "groupedFileIds": grouped,
            })
        if archived or not included:
            continue
        if linked not in expenses_by_id:
            if linked in all_active_expense_ids:
                continue
            if created_at[:4] == year_text and not linked:
                gaps.append(_gap("MARKED_PURCHASE_WITHOUT_PAID_EXPENSE", "purchase", purchase_id,
                                 {"createdAt": created_at}))
            elif created_at[:4] == year_text and linked:
                gaps.append(_gap("MARKED_PURCHASE_WITHOUT_PAID_EXPENSE", "purchase", purchase_id,
                                 {"createdAt": created_at}, ["linkedExpenseId"]))
            continue
        files = []
        for file_id in grouped:
            descriptor = _original_descriptor(store, route, "receipt", purchase_id, file_id)
            files.append(descriptor)
            source_file_count += 1
            source_bytes += descriptor["bytes"]
        purchase = {
            "purchaseId": purchase_id,
            "expenseId": linked,
            "createdAt": created_at,
            "files": sorted(files, key=lambda item: item["fileId"]),
        }
        supporting_purchases.append(purchase)
        expenses_by_id[linked]["supportingPurchaseIds"].append(purchase_id)

    supporting_purchases.sort(key=lambda item: (item["createdAt"], item["purchaseId"]))
    for item in paid_expenses:
        item["supportingPurchaseIds"].sort()
        if not item["supportingPurchaseIds"]:
            gaps.append(_gap("PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS", "paidExpense", item["id"],
                             {"paymentDate": item["date"]}))

    gaps.sort(key=lambda item: (item["code"], item["recordType"], item["recordId"]))
    receipt_digest = _digest(sorted(relevant_reviews, key=lambda item: item["purchaseId"]))
    revisions = {
        "expenses": expenses_ledger["revision"],
        "moneyIn": money_ledger["revision"],
        "cashAccounts": cash_ledger["revision"],
        "receiptReviewsDigest": receipt_digest,
    }
    placement = {
        "statementFacts": "billing_period_end",
        "settlementPayments": "received_date",
        "cashAccountCollections": "received_date",
        "paidExpenses": "payment_date",
    }
    summaries = {
        "statementFacts": _statement_summaries(statement_periods),
        "settlementPayments": {"valueCents": sum(item["amountCents"] for item in settlement_payments), "count": len(settlement_payments)},
        "cashAccountCollections": {"valueCents": sum(item["amountCents"] for item in cash_collections), "count": len(cash_collections)},
        "paidExpenses": {"valueCents": sum(item["amountCents"] for item in paid_expenses), "count": len(paid_expenses)},
    }
    snapshot_material = {
        "schemaVersion": 1,
        "route": route,
        "year": year,
        "revisions": revisions,
        "placement": placement,
        "statementPeriods": statement_periods,
        "settlementPayments": settlement_payments,
        "cashAccountCollections": cash_collections,
        "paidExpenses": paid_expenses,
        "supportingPurchases": supporting_purchases,
        "summaries": summaries,
        "gaps": gaps,
    }
    too_large = source_file_count > MAX_SOURCE_FILES or source_bytes > MAX_SOURCE_BYTES
    return {
        **snapshot_material,
        "generatedAt": generated_at or now(),
        "snapshotId": _digest(snapshot_material),
        "export": {
            "available": not too_large,
            "sourceFileCount": source_file_count,
            "sourceBytes": source_bytes,
            "maximumSourceFileCount": MAX_SOURCE_FILES,
            "maximumSourceBytes": MAX_SOURCE_BYTES,
            "reasonCode": "HANDOFF_EXPORT_TOO_LARGE" if too_large else None,
        },
    }
