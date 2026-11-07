"""Bounded Accountant handoff ZIP generation from a canonical snapshot."""
from dataclasses import dataclass
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import textwrap
import zipfile

from .accounting_domain import fail
from .accountant_handoff import HandoffEvidenceUnverified


# Approved v1 copy for the Accountant handoff export.
# Machine keys and archive paths are fixed by the API contract; these visible
# headings can change without changing the JSON preview schema.
DEFAULT_EXPORT_COPY = {
    "version": "labels-20261107-3",
    "title": "Accountant handoff",
    "route": "Route",
    "year": "Year",
    "generated": "Generated",
    "disclaimer": (
        "RouteSpark organizes statement activity by billing period and recorded payments by received date. "
        "These groupings do not determine tax treatment. Your tax professional should apply your accounting method."
    ),
    "statementActivity": "Statement activity",
    "settlementPayments": "Settlement payments",
    "cashAccountCollections": "Cash-account collections",
    "paidExpenses": "Paid expenses",
    "supportingDocuments": "Supporting documents",
    "bills": "Bills",
    "receiptsNotRecorded": "Receipts not recorded as paid expenses",
    "moneyInNotRecorded": (
        "None recorded in RouteSpark for this year. This is not a statement that no income was received. "
        "See the year-end tax paperwork the owner received."
    ),
    "statementsNotIncluded": "No settlement statements are included for this year.",
    "dataGaps": "Items to check",
    "yes": "Yes",
    "no": "No",
    "recordTypes": {
        "statement": "Settlement statement",
        "adjustment": "Statement adjustment",
        "settlementPayment": "Settlement payment",
        "cashCollection": "Cash-account collection",
    },
    "factLabels": {
        "grossSalesCents": "Gross sales",
        "marginCents": "Total Margin Discount",
        "inventoryCents": "Truck inventory",
        "totalAdjustmentsCents": "Total Adjustments",
        "newSettlementCents": "New Settlement",
        "previousBalanceCents": "Previous Balance",
        "statementPaymentCents": "Payment as of (statement)",
        "amountToPayCents": "Total Amount To Pay",
        "handheldRentalCents": "Handheld rental",
        "cashAccountSalesCents": "Cash-account sales",
    },
    "categoryLabels": {
        "fuel": "Fuel", "storage": "Warehouse / storage", "vehicle_note": "Vehicle note",
        "vehicle_maintenance": "Vehicle maintenance", "vehicle_insurance": "Vehicle insurance",
        "business_insurance": "Business insurance", "business_loan": "Business loan",
        "equipment": "Equipment & tools", "office_supplies": "Office supplies",
        "route_supplies": "Route supplies", "professional": "Professional services",
        "licenses_tax": "Licenses & tax", "technology": "Technology", "other": "Other",
    },
    "gapLabels": {
        "MARKED_PURCHASE_WITHOUT_PAID_EXPENSE": "Purchase marked for the handoff is not linked to a paid expense",
        "PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS": "Paid expense has no supporting documents",
        "STATEMENT_EVIDENCE_UNVERIFIED": "Saved settlement statement could not be verified and is not included",
        "SUPPORTING_DOCUMENT_EVIDENCE_UNVERIFIED": "Supporting documents could not be verified and are not included",
        "LINKED_PURCHASE_NOT_MARKED": "Receipt linked to a paid expense is unchecked and is not included",
        "STATEMENT_FACTS_UNKNOWN": "Some statement figures are not available",
        "PRELIMINARY_STATEMENT_ONLY": "Only a preliminary statement is confirmed for this billing period",
        "BILLING_PERIOD_CROSSES_CALENDAR_YEAR": "Billing period crosses into another calendar year",
    },
    "gapRecordTypes": {
        "settlementStatement": "Settlement statement",
        "purchase": "Purchase",
        "paidExpense": "Paid expense",
    },
    "statementStatuses": {"final": "Final", "preliminary": "Preliminary"},
    "paymentMethods": {
        "card": "Card", "bank": "Bank transfer", "cash": "Cash",
        "check": "Check", "other": "Other",
    },
    "fieldLabels": {
        "grossSalesCents": "Gross sales",
        "marginCents": "Total Margin Discount",
        "inventoryCents": "Truck inventory",
        "totalAdjustmentsCents": "Total Adjustments",
        "newSettlementCents": "New Settlement",
        "previousBalanceCents": "Previous Balance",
        "statementPaymentCents": "Payment as of (statement)",
        "amountToPayCents": "Total Amount To Pay",
        "handheldRentalCents": "Handheld rental",
        "cashAccountSalesCents": "Cash-account sales",
        "linkedExpenseId": "Paid expense link",
    },
    "incomeHeaders": [
        "Record type", "Billing period start", "Billing period end", "Received date",
        "Crosses calendar year", "Statement status", "Gross sales", "Total Margin Discount",
        "Truck inventory", "Total Adjustments", "New Settlement", "Previous Balance",
        "Payment as of (statement)", "Total Amount To Pay", "Handheld rental",
        "Cash-account sales", "Adjustment", "Amount", "Payment method", "Customer", "Note",
        "Statement file",
    ],
    "expenseHeaders": [
        "Payment date", "Payee", "Category", "Category amount", "Expense total",
        "Payment method", "Note", "Supporting documents folder",
    ],
    "gapHeaders": [
        "Item to check", "Record type", "Billing period start", "Billing period end",
        "Payment date", "Date added", "Missing details",
    ],
    "billHeaders": [
        "Date added", "Payee", "Category", "Note", "Status today", "Schedule today",
        "Expected amount today", "Payments recorded in year", "Total recorded in year",
    ],
    "receiptHeaders": ["Date added", "Documents", "Folder", "File status"],
}


@dataclass(frozen=True)
class ExportArtifact:
    path: str
    manifest_sha256: str
    snapshot_id: str


def _unverified(details=None):
    raise HandoffEvidenceUnverified(details)


def _spreadsheet_text(value):
    text = "" if value is None else str(value)
    if text.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + text
    return text


class _SpreadsheetNumber(str):
    """A server-generated decimal that spreadsheet programs may parse as numeric."""


def _csv_money(cents):
    if cents is None:
        return ""
    if type(cents) is not int:
        _unverified()
    sign = "-" if cents < 0 else ""
    absolute = abs(cents)
    return _SpreadsheetNumber(f"{sign}{absolute // 100}.{absolute % 100:02d}")


def _csv_bytes(headers, rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(headers)
    for row in rows:
        if len(row) != len(headers):
            _unverified()
        writer.writerow([
            str(value) if isinstance(value, _SpreadsheetNumber)
            else value if isinstance(value, int)
            else _spreadsheet_text(value)
            for value in row
        ])
    return output.getvalue().encode("utf-8-sig")


def _statement_path(period):
    return f'settlement-statements/{period["periodStart"]}_to_{period["periodEnd"]}.pdf'


def _supporting_folder(purchase):
    return _evidence_folder(purchase.get("folder"), "supporting-documents")


def _evidence_folder(value, prefix):
    if type(value) is not str or re.fullmatch(prefix + r"/[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{3}", value) is None:
        _unverified()
    return value


def _supporting_folders(snapshot):
    folders = {}
    for purchase in snapshot["supportingPurchases"]:
        folders.setdefault(purchase["expenseId"], set()).add(_supporting_folder(purchase))
    return {key: "; ".join(sorted(value)) for key, value in folders.items()}


def _payment_method(copy, value):
    if value in (None, ""):
        return ""
    try:
        return copy["paymentMethods"][value]
    except KeyError:
        _unverified()


def _income_csv(snapshot, copy):
    headers = copy["incomeHeaders"]
    record_types = copy["recordTypes"]
    rows = []
    for period in snapshot["statementPeriods"]:
        facts = period["facts"]
        statement_path = _statement_path(period)
        rows.append([
            record_types["statement"], period["periodStart"], period["periodEnd"], "",
            copy["yes"] if period["crossesCalendarYear"] else copy["no"],
            copy["statementStatuses"][period["version"]], _csv_money(facts["grossSalesCents"]),
            _csv_money(facts["marginCents"]), _csv_money(facts["inventoryCents"]),
            _csv_money(facts["totalAdjustmentsCents"]), _csv_money(facts["newSettlementCents"]),
            _csv_money(facts["previousBalanceCents"]), _csv_money(facts["statementPaymentCents"]),
            _csv_money(facts["amountToPayCents"]), _csv_money(facts["handheldRentalCents"]),
            _csv_money(facts["cashAccountSalesCents"]), "", "", "", "", "", statement_path,
        ])
        for adjustment in facts["adjustments"]:
            rows.append([
                record_types["adjustment"], period["periodStart"], period["periodEnd"], "",
                copy["yes"] if period["crossesCalendarYear"] else copy["no"],
                copy["statementStatuses"][period["version"]], "", "", "", "", "", "", "", "", "", "",
                adjustment["sourceLabel"], _csv_money(adjustment["cents"]), "", "", "", statement_path,
            ])
    for payment in snapshot["settlementPayments"]:
        rows.append([
            record_types["settlementPayment"], "", payment["periodEnd"], payment["receivedDate"], "", "",
            "", "", "", "", "", "", "", "", "", "", "", _csv_money(payment["amountCents"]),
            _payment_method(copy, payment["method"]), "", payment.get("note", ""), "",
        ])
    for payment in snapshot["cashAccountCollections"]:
        rows.append([
            record_types["cashCollection"], "", payment["periodEnd"], payment["receivedDate"], "", "",
            "", "", "", "", "", "", "", "", "", "", "", _csv_money(payment["amountCents"]),
            _payment_method(copy, payment["method"]), payment.get("customer", ""), payment.get("note", ""), "",
        ])
    return _csv_bytes(headers, rows)


def _expenses_csv(snapshot, copy):
    headers = copy["expenseHeaders"]
    supporting_folders = _supporting_folders(snapshot)
    rows = []
    for expense in snapshot["paidExpenses"]:
        allocations = expense["splits"] or [{"category": expense["category"], "amountCents": expense["amountCents"]}]
        for allocation in allocations:
            rows.append([
                expense["date"], expense["vendor"], copy["categoryLabels"][allocation["category"]],
                _csv_money(allocation["amountCents"]), _csv_money(expense["amountCents"]),
                _payment_method(copy, expense.get("paymentMethod")),
                expense.get("note", ""), supporting_folders.get(expense["id"], ""),
            ])
    return _csv_bytes(headers, rows)


def _bills_csv(snapshot, copy):
    rows = []
    for bill in snapshot["bills"]:
        schedule = (f'Monthly on day {bill["anchorDay"]}' if bill["frequency"] == "monthly"
                    else f'One time, due {bill["dueDate"]}')
        rows.append([
            bill["createdAt"][:10], bill["vendor"], copy["categoryLabels"][bill["category"]],
            bill.get("note", ""), "Active" if bill["status"] == "active" else "Stopped", schedule,
            _csv_money(bill["amountCents"]), bill["paymentCount"], _csv_money(bill["paymentTotalCents"]),
        ])
    return _csv_bytes(copy["billHeaders"], rows)


def _receipts_not_recorded_csv(snapshot, copy):
    rows = []
    for receipt in snapshot["receiptsNotRecorded"]:
        folder = (_evidence_folder(receipt.get("folder"), "receipts-not-recorded") + "/"
                  if receipt["fileStatus"] == "included" else "")
        rows.append([
            receipt["createdAt"][:10], receipt["documentCount"],
            folder,
            "Included" if receipt["fileStatus"] == "included" else "Could not be verified, file not included",
        ])
    return _csv_bytes(copy["receiptHeaders"], rows)


def _gaps_csv(snapshot, copy):
    headers = copy["gapHeaders"]
    rows = []
    for gap in snapshot["gaps"]:
        dates = gap["dates"]
        try:
            issue = copy["gapLabels"][gap["code"]]
            fields = [copy["fieldLabels"][name] for name in gap["fields"]]
            record_type = copy["gapRecordTypes"][gap["recordType"]]
        except KeyError:
            _unverified()
        rows.append([
            issue, record_type, dates.get("periodStart", ""),
            dates.get("periodEnd", ""), dates.get("paymentDate", ""), dates.get("createdAt", "")[:10],
            ", ".join(fields),
        ])
    return _csv_bytes(headers, rows)


def _money(cents):
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}${cents // 100:,}.{cents % 100:02d}"


def _count(value, singular, plural=None):
    return f"{value} {singular if value == 1 else (plural or singular + 's')}"


def _generated_at(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (AttributeError, ValueError):
        _unverified()
    hour = parsed.strftime("%I").lstrip("0") or "0"
    return f'{parsed.strftime("%B")} {parsed.day}, {parsed.year}, {hour}:{parsed.strftime("%M %p")} UTC'


def _statement_fact_summary(label, total):
    known = total["knownCount"]
    unknown = total["unknownCount"]
    if known:
        line = f'{label}: {_money(total["valueCents"])} from {_count(known, "billing period")}'
        if unknown:
            line += f'. {_count(unknown, "billing period")} not available.'
        return line
    return f'{label}: Not available for {_count(unknown, "billing period")}'


def _summary_lines(snapshot, copy):
    summaries = snapshot["summaries"]
    lines = [
        copy["title"],
        f'{copy["route"]}: {snapshot["route"]}',
        f'{copy["year"]}: {snapshot["year"]}',
        f'{copy["generated"]}: {_generated_at(snapshot["generatedAt"])}',
        "",
        copy["disclaimer"],
    ]
    money_in = snapshot["moneyIn"]
    if not any(money_in.values()):
        lines += ["", "Money in", copy["moneyInNotRecorded"]]
    if snapshot["statementPeriods"]:
        lines += ["", copy["statementActivity"], f'Billing periods: {len(snapshot["statementPeriods"])}']
        for key in ("grossSalesCents", "marginCents", "amountToPayCents", "handheldRentalCents", "cashAccountSalesCents"):
            lines.append(_statement_fact_summary(copy["factLabels"][key], summaries["statementFacts"][key]))
    if money_in["unverifiedStatementCount"]:
        lines += ["", f'{_count(money_in["unverifiedStatementCount"], "saved statement")} could not be verified and is not included. See items to check.']
    if (not money_in["verifiedStatementCount"] and not money_in["unverifiedStatementCount"]
            and (money_in["paymentCount"] or money_in["collectionCount"])):
        lines += ["", copy["statementsNotIncluded"]]
    if snapshot["settlementPayments"]:
        lines += ["", copy["settlementPayments"], f'{_money(summaries["settlementPayments"]["valueCents"])} from {_count(summaries["settlementPayments"]["count"], "payment")}']
    if snapshot["cashAccountCollections"]:
        lines += ["", copy["cashAccountCollections"], f'{_money(summaries["cashAccountCollections"]["valueCents"])} from {_count(summaries["cashAccountCollections"]["count"], "collection")}']
    if snapshot["paidExpenses"]:
        lines += ["", copy["paidExpenses"], f'{_money(summaries["paidExpenses"]["valueCents"])} from {_count(summaries["paidExpenses"]["count"], "expense")}']
        lines += [
            f'{copy["categoryLabels"][item["category"]]}: {_money(item["valueCents"])}'
            for item in summaries["paidExpensesByCategory"]
        ]
    if snapshot["export"]["supportingDocumentFileCount"]:
        lines += ["", copy["supportingDocuments"], _count(snapshot["export"]["supportingDocumentFileCount"], "file")]
    if snapshot["bills"]:
        lines += ["", copy["bills"], f'{_count(len(snapshot["bills"]), "bill")}, as of this download. Recorded payments are already in paid expenses. This list does not show whether a payment was owed or missed.']
    if snapshot["receiptsNotRecorded"]:
        lines += ["", copy["receiptsNotRecorded"], f'{_count(len(snapshot["receiptsNotRecorded"]), "receipt")}, {_count(snapshot["export"]["unrecordedReceiptFileCount"], "file")}. Not included in paid expense totals.']
    if snapshot["receiptsLeftOutCount"]:
        lines += [f'{_count(snapshot["receiptsLeftOutCount"], "unchecked receipt")} left out.']
    if snapshot["gaps"]:
        lines += ["", copy["dataGaps"], _count(len(snapshot["gaps"]), "item")]
    return lines


def _pdf_escape(value):
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _pdf_bytes(lines):
    wrapped = []
    for line in lines:
        wrapped.extend(textwrap.wrap(str(line), width=92, replace_whitespace=True) or [""])
    pages = [wrapped[index:index + 48] for index in range(0, max(1, len(wrapped)), 48)] or [[]]
    objects = {
        1: b"<< /Type /Catalog /Pages 2 0 R >>",
        3: b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    }
    page_refs = []
    next_id = 4
    for page_lines in pages:
        page_id, content_id = next_id, next_id + 1
        next_id += 2
        page_refs.append(f"{page_id} 0 R")
        commands = ["BT", "/F1 10 Tf", "54 756 Td", "13 TL"]
        for line in page_lines:
            safe = str(line).encode("latin-1", "replace").decode("latin-1")
            commands += [f"({_pdf_escape(safe)}) Tj", "T*"]
        commands.append("ET")
        stream = "\n".join(commands).encode("latin-1")
        objects[page_id] = f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents {content_id} 0 R >>".encode()
        objects[content_id] = f"<< /Length {len(stream)} >>\nstream\n".encode() + stream + b"\nendstream"
    objects[2] = f'<< /Type /Pages /Kids [{" ".join(page_refs)}] /Count {len(page_refs)} >>'.encode()

    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for object_id in range(1, max(objects) + 1):
        offsets.append(len(output))
        output.extend(f"{object_id} 0 obj\n".encode())
        output.extend(objects[object_id])
        output.extend(b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(offsets)}\n".encode())
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    return bytes(output)


def _entry(zip_file, entries, path, content, media_type, *, compress=True):
    if path.startswith("/") or "\\" in path or ".." in Path(path).parts:
        _unverified()
    if path in zip_file.NameToInfo:
        _unverified()
    info = zipfile.ZipInfo(path, date_time=(1980, 1, 1, 0, 0, 0))
    info.compress_type = zipfile.ZIP_DEFLATED if compress else zipfile.ZIP_STORED
    info.external_attr = 0o100600 << 16
    zip_file.writestr(info, content)
    entries.append({
        "path": path,
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "mediaType": media_type,
    })


def _safe_id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9-]{1,80}", value) is None:
        _unverified()
    return value


def _source(store, route, kind, upload_id, file_id, expected, details=None):
    try:
        source = store.original(kind, upload_id, file_id)
        content = bytes(source["content"])
        if (source["sha256"] != expected["sha256"] or source["mediaType"] != expected["mediaType"]
                or len(content) != expected["bytes"] or hashlib.sha256(content).hexdigest() != expected["sha256"]):
            _unverified(details)
        return content
    except (KeyError, TypeError, ValueError, RuntimeError):
        _unverified(details)


def create_handoff_export(store, route, snapshot, copy, *, temp_directory=None):
    """Create one exact temporary ZIP. The caller owns successful cleanup."""
    if (type(snapshot) is not dict or snapshot.get("route") != route
            or snapshot.get("schemaVersion") != 1 or type(snapshot.get("snapshotId")) is not str):
        _unverified()
    if not snapshot.get("export", {}).get("available"):
        fail("This handoff is too large for one download.", 413)
    if copy.get("version") != DEFAULT_EXPORT_COPY["version"]:
        fail("Accountant handoff labels are unavailable.", 503)

    temporary = tempfile.NamedTemporaryFile(prefix="routespark-handoff-", suffix=".zip",
                                             dir=temp_directory, delete=False)
    path = temporary.name
    temporary.close()
    entries = []
    try:
        with zipfile.ZipFile(path, "w", allowZip64=True) as bundle:
            _entry(bundle, entries, "summary.pdf", _pdf_bytes(_summary_lines(snapshot, copy)), "application/pdf")
            if (snapshot["statementPeriods"] or snapshot["settlementPayments"]
                    or snapshot["cashAccountCollections"]):
                _entry(bundle, entries, "income-by-billing-period.csv", _income_csv(snapshot, copy), "text/csv")
            if snapshot["paidExpenses"]:
                _entry(bundle, entries, "paid-expenses.csv", _expenses_csv(snapshot, copy), "text/csv")
            if snapshot["bills"]:
                _entry(bundle, entries, "bills.csv", _bills_csv(snapshot, copy), "text/csv")
            if snapshot["receiptsNotRecorded"]:
                _entry(bundle, entries, "receipts-not-recorded.csv", _receipts_not_recorded_csv(snapshot, copy), "text/csv")
            if snapshot["gaps"]:
                _entry(bundle, entries, "items-to-check.csv", _gaps_csv(snapshot, copy), "text/csv")

            for period in snapshot["statementPeriods"]:
                source = period["source"]
                details = {
                    "recordType": "statement",
                    "periodStart": period["periodStart"],
                    "periodEnd": period["periodEnd"],
                }
                # Byte counts are measured by the canonical preview but kept out
                # of the public source object. Reconcile them through the original
                # manifest before reading content.
                manifest = store.get("statement-upload:" + source["uploadId"])
                if type(manifest) is not dict or manifest.get("route") != route:
                    _unverified(details)
                match = next((item for item in (manifest or {}).get("files", []) if item.get("id") == source["fileId"]), None)
                if match is None:
                    _unverified(details)
                expected = {"sha256": source["sha256"], "mediaType": source["mediaType"], "bytes": match.get("bytes")}
                content = _source(store, route, "statement", source["uploadId"], source["fileId"], expected, details)
                filename = _statement_path(period)
                _entry(bundle, entries, filename, content, "application/pdf", compress=False)

            for purchase in snapshot["supportingPurchases"]:
                purchase_id = _safe_id(purchase["purchaseId"])
                expense_id = _safe_id(purchase["expenseId"])
                details = {"recordType": "receipt", "createdDate": purchase["createdAt"][:10]}
                for index, source in enumerate(purchase["files"], 1):
                    file_id = _safe_id(source["fileId"])
                    content = _source(store, route, "receipt", purchase_id, file_id, source, details)
                    extension = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}.get(source["mediaType"])
                    if extension is None:
                        _unverified(details)
                    filename = f'{_supporting_folder(purchase)}/{index:02d}.{extension}'
                    _entry(bundle, entries, filename, content, source["mediaType"], compress=False)

            for receipt in snapshot["receiptsNotRecorded"]:
                purchase_id = _safe_id(receipt["purchaseId"])
                details = {"recordType": "receipt", "createdDate": receipt["createdAt"][:10]}
                folder = _evidence_folder(receipt.get("folder"), "receipts-not-recorded")
                for index, source in enumerate(receipt["files"], 1):
                    file_id = _safe_id(source["fileId"])
                    content = _source(store, route, "receipt", purchase_id, file_id, source, details)
                    extension = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}.get(source["mediaType"])
                    if extension is None:
                        _unverified(details)
                    _entry(bundle, entries, f"{folder}/{index:02d}.{extension}", content,
                           source["mediaType"], compress=False)

            manifest = {
                "schemaVersion": 1,
                "copyVersion": copy["version"],
                "snapshot": snapshot,
                "entries": entries,
            }
            manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            info = zipfile.ZipInfo("manifest.json", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100600 << 16
            bundle.writestr(info, manifest_bytes)
        return ExportArtifact(path=path, manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                              snapshot_id=snapshot["snapshotId"])
    except Exception:
        Path(path).unlink(missing_ok=True)
        raise
