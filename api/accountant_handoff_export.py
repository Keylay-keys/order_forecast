"""Bounded Accountant handoff ZIP generation from a canonical snapshot."""
from dataclasses import dataclass
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import tempfile
import textwrap
import zipfile

from .accounting_domain import fail


# Provisional v1 copy for Claude's review before the endpoint is released.
# Machine keys and archive paths are fixed by the API contract; these visible
# headings can change without changing the JSON preview schema.
DEFAULT_EXPORT_COPY = {
    "version": "draft-20261106",
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
    "dataGaps": "Data gaps",
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
        "marginCents": "Margin",
        "amountToPayCents": "Total amount to pay",
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
        "MARKED_PURCHASE_WITHOUT_PAID_EXPENSE": "Marked supporting documents are not linked to a paid expense",
        "PAID_EXPENSE_WITHOUT_SUPPORTING_DOCUMENTS": "Paid expense has no supporting documents in the handoff",
        "STATEMENT_FACTS_UNKNOWN": "Some settlement statement facts are unavailable",
        "PRELIMINARY_STATEMENT_ONLY": "Only a preliminary settlement statement is confirmed",
        "BILLING_PERIOD_CROSSES_CALENDAR_YEAR": "Billing period crosses calendar years",
    },
    "incomeHeaders": [
        "Record type", "Record ID", "Billing period start", "Billing period end", "Received date",
        "Crosses calendar year", "Statement version", "Gross sales cents", "Margin cents",
        "Inventory cents", "Total adjustments cents", "New settlement cents", "Previous balance cents",
        "Statement payment cents", "Total amount to pay cents", "Handheld rental cents",
        "Cash-account sales cents", "Adjustment key", "Adjustment source label", "Amount cents",
        "Payment method", "Customer", "Store ID", "Note",
    ],
    "expenseHeaders": [
        "Expense ID", "Payment date", "Payee", "Category", "Category amount cents",
        "Expense total cents", "Payment method", "Note", "Supporting purchase IDs",
    ],
    "gapHeaders": [
        "Issue", "Gap code", "Record type", "Record ID", "Billing period start", "Billing period end",
        "Payment date", "Created at", "Fields",
    ],
}


@dataclass(frozen=True)
class ExportArtifact:
    path: str
    manifest_sha256: str
    snapshot_id: str


def _unverified():
    fail("Accountant handoff evidence could not be verified.", 503)


def _spreadsheet_text(value):
    text = "" if value is None else str(value)
    if text.startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + text
    return text


def _csv_bytes(headers, rows):
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\r\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([_spreadsheet_text(value) if not isinstance(value, int) else value for value in row])
    return output.getvalue().encode("utf-8-sig")


def _income_csv(snapshot, copy):
    headers = copy["incomeHeaders"]
    record_types = copy["recordTypes"]
    rows = []
    for period in snapshot["statementPeriods"]:
        facts = period["facts"]
        source = period["source"]
        rows.append([
            record_types["statement"], source["attemptId"], period["periodStart"], period["periodEnd"], "",
            copy["yes"] if period["crossesCalendarYear"] else copy["no"], period["version"], facts["grossSalesCents"],
            facts["marginCents"], facts["inventoryCents"], facts["totalAdjustmentsCents"],
            facts["newSettlementCents"], facts["previousBalanceCents"], facts["statementPaymentCents"],
            facts["amountToPayCents"], facts["handheldRentalCents"], facts["cashAccountSalesCents"],
            "", "", "", "", "", "", "",
        ])
        for adjustment in facts["adjustments"]:
            rows.append([
                record_types["adjustment"], source["attemptId"], period["periodStart"], period["periodEnd"], "",
                copy["yes"] if period["crossesCalendarYear"] else copy["no"], period["version"], "", "", "", "", "",
                "", "", "", "", "", adjustment["key"], adjustment["sourceLabel"], adjustment["cents"],
                "", "", "", "",
            ])
    for payment in snapshot["settlementPayments"]:
        rows.append([
            record_types["settlementPayment"], payment["id"], "", payment["periodEnd"], payment["receivedDate"], "", "",
            "", "", "", "", "", "", "", "", "", "", "", "", payment["amountCents"],
            payment["method"], "", "", payment.get("note", ""),
        ])
    for payment in snapshot["cashAccountCollections"]:
        rows.append([
            record_types["cashCollection"], payment["id"], "", payment["periodEnd"], payment["receivedDate"], "", "",
            "", "", "", "", "", "", "", "", "", "", "", "", payment["amountCents"],
            payment["method"], payment.get("customer", ""), payment.get("storeId", ""), payment.get("note", ""),
        ])
    return _csv_bytes(headers, rows)


def _expenses_csv(snapshot, copy):
    headers = copy["expenseHeaders"]
    rows = []
    for expense in snapshot["paidExpenses"]:
        allocations = expense["splits"] or [{"category": expense["category"], "amountCents": expense["amountCents"]}]
        for allocation in allocations:
            rows.append([
                expense["id"], expense["date"], expense["vendor"], copy["categoryLabels"][allocation["category"]],
                allocation["amountCents"], expense["amountCents"], expense.get("paymentMethod") or "",
                expense.get("note", ""), ";".join(expense["supportingPurchaseIds"]),
            ])
    return _csv_bytes(headers, rows)


def _gaps_csv(snapshot, copy):
    headers = copy["gapHeaders"]
    rows = []
    for gap in snapshot["gaps"]:
        dates = gap["dates"]
        rows.append([
            copy["gapLabels"][gap["code"]], gap["code"], gap["recordType"], gap["recordId"], dates.get("periodStart", ""),
            dates.get("periodEnd", ""), dates.get("paymentDate", ""), dates.get("createdAt", ""),
            ";".join(gap["fields"]),
        ])
    return _csv_bytes(headers, rows)


def _money(cents):
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}${cents // 100:,}.{cents % 100:02d}"


def _summary_lines(snapshot, copy):
    summaries = snapshot["summaries"]
    lines = [
        copy["title"],
        f'{copy["route"]}: {snapshot["route"]}',
        f'{copy["year"]}: {snapshot["year"]}',
        f'{copy["generated"]}: {snapshot["generatedAt"]}',
        "",
        copy["disclaimer"],
        "",
        copy["statementActivity"],
        f'Billing periods: {len(snapshot["statementPeriods"])}',
    ]
    for key in ("grossSalesCents", "marginCents", "amountToPayCents", "handheldRentalCents", "cashAccountSalesCents"):
        total = summaries["statementFacts"][key]
        suffix = f'; {total["unknownCount"]} unavailable' if total["unknownCount"] else ""
        lines.append(f'{copy["factLabels"][key]}: {_money(total["valueCents"])} across {total["knownCount"]} known{suffix}')
    lines += [
        "",
        copy["settlementPayments"],
        f'{_money(summaries["settlementPayments"]["valueCents"])} across {summaries["settlementPayments"]["count"]} records',
        "",
        copy["cashAccountCollections"],
        f'{_money(summaries["cashAccountCollections"]["valueCents"])} across {summaries["cashAccountCollections"]["count"]} records',
        "",
        copy["paidExpenses"],
        f'{_money(summaries["paidExpenses"]["valueCents"])} across {summaries["paidExpenses"]["count"]} records',
        "",
        copy["supportingDocuments"],
        f'{snapshot["export"]["sourceFileCount"] - len(snapshot["statementPeriods"])} files',
        "",
        copy["dataGaps"],
        f'{len(snapshot["gaps"])} items',
    ]
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


def _source(store, route, kind, upload_id, file_id, expected):
    source = store.original(kind, upload_id, file_id)
    try:
        content = bytes(source["content"])
        if (source["sha256"] != expected["sha256"] or source["mediaType"] != expected["mediaType"]
                or len(content) != expected["bytes"] or hashlib.sha256(content).hexdigest() != expected["sha256"]):
            _unverified()
        return content
    except (KeyError, TypeError, ValueError):
        _unverified()


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
            _entry(bundle, entries, "income-by-billing-period.csv", _income_csv(snapshot, copy), "text/csv")
            _entry(bundle, entries, "paid-expenses.csv", _expenses_csv(snapshot, copy), "text/csv")
            _entry(bundle, entries, "data-gaps.csv", _gaps_csv(snapshot, copy), "text/csv")

            for period in snapshot["statementPeriods"]:
                source = period["source"]
                # Byte counts are measured by the canonical preview but kept out
                # of the public source object. Reconcile them through the original
                # manifest before reading content.
                manifest = store.get("statement-upload:" + source["uploadId"])
                if type(manifest) is not dict or manifest.get("route") != route:
                    _unverified()
                match = next((item for item in (manifest or {}).get("files", []) if item.get("id") == source["fileId"]), None)
                if match is None:
                    _unverified()
                expected = {"sha256": source["sha256"], "mediaType": source["mediaType"], "bytes": match.get("bytes")}
                content = _source(store, route, "statement", source["uploadId"], source["fileId"], expected)
                filename = f'settlement-statements/{period["periodStart"]}_to_{period["periodEnd"]}.pdf'
                _entry(bundle, entries, filename, content, "application/pdf", compress=False)

            for purchase in snapshot["supportingPurchases"]:
                purchase_id = _safe_id(purchase["purchaseId"])
                expense_id = _safe_id(purchase["expenseId"])
                for index, source in enumerate(purchase["files"], 1):
                    file_id = _safe_id(source["fileId"])
                    content = _source(store, route, "receipt", purchase_id, file_id, source)
                    extension = {"image/jpeg": "jpg", "image/png": "png", "application/pdf": "pdf"}.get(source["mediaType"])
                    if extension is None:
                        _unverified()
                    filename = f'supporting-documents/{purchase["createdAt"][:10]}/{expense_id}/{purchase_id}/{index:02d}.{extension}'
                    _entry(bundle, entries, filename, content, source["mediaType"], compress=False)

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
