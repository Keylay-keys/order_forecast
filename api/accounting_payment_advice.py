"""Gruma payment advice evidence and exact, tenant-scoped settlement matching.

Matched advice PDFs are discarded; their validated facts and delivery digests
remain. No email subject/date supplies financial facts. Call save/reconcile only
inside the existing owner/route transaction.
"""
from datetime import datetime
from decimal import Decimal
import hashlib
import os
from pathlib import Path
import re
import tempfile
from uuid import uuid4

from .accounting_domain import now

PREFIX = "payment-advice:"
MONEY = r"(?:[0-9]{1,3}(?:,[0-9]{3})+|[0-9]+)\.[0-9]{2}"
DATE = r"[0-9]{2}/[0-9]{2}/[0-9]{4}"
ERROR = "This payment advice needs review. Its route, settlement reference, date and amounts must agree before a payment can be recorded."


def one(pattern, text):
    matches = re.findall(pattern, text, re.M)
    if len(matches) != 1:
        raise ValueError(ERROR)
    return matches[0]


def date(value):
    parsed = datetime.strptime(value, "%m/%d/%Y").date()
    if parsed.strftime("%m/%d/%Y") != value:
        raise ValueError(ERROR)
    return parsed.isoformat()


def cents(value):
    value = int(Decimal(value.replace(",", "")) * 100)
    if not 0 <= value <= 100_000_000:
        raise ValueError(ERROR)
    return value


def parse_text(text, route):
    """First supported format: one USD settlement, no deductions, one page."""
    if not isinstance(text, str) or len(text) > 256 * 1024 or not re.fullmatch(r"[0-9]{6}", route):
        raise ValueError(ERROR)
    if not all(label in text for label in ("Mission Foods", "Payment advice", "PAYMENT DETAIL")):
        raise ValueError(ERROR)
    document, issued = one(r"Document\s*/\s*Date\s+([0-9]{6,20})\s*/\s*(" + DATE + r")", text)
    account = one(r"Your account with us\s+([0-9]{6})\b", text)
    cleared = one(r"We have cleared the items listed below with document ([0-9]{6,20})\.", text)
    footer_doc, footer_date, paid = one(r"Payment document\s+Date\s+Currency\s+Payment amount\s+([0-9]{6,20})\s+(" + DATE + r")\s+USD\s+\*+(" + MONEY + r")\*+", text)
    table = one(r"Invoice number[^\f]*?\n([\s\S]*?)SUM TOTAL", text)
    row_pattern = r"\s*([0-9]{8})/([0-9]{6})-\s+(" + DATE + r")\s+Net Settlement/([0-9]{8})\s+(" + MONEY + r")\s+(" + MONEY + r")\s*"
    rows = []
    for line in table.splitlines():
        line = line.strip()
        if not line or set(line) == {"_"} or line == "amount":
            continue
        match = re.fullmatch(row_pattern, line)
        if not match:
            raise ValueError(ERROR)
        rows.append(match.groups())
    if len(rows) != 1:
        raise ValueError(ERROR)
    invoice_date, invoice_route, billed, description_date, deductions, gross = rows[0]
    total_deductions, total = one(r"SUM TOTAL\s+(" + MONEY + r")\s+(" + MONEY + r")", text)
    payment_date, period_end = date(issued), date(billed)
    paid_cents = cents(paid)
    if (account != route or invoice_route != route or cleared != document or footer_doc != document
        or date(footer_date) != payment_date or invoice_date != description_date
        or invoice_date != period_end.replace("-", "") or payment_date < period_end
        or cents(deductions) != 0 or cents(total_deductions) != 0
        or not paid_cents or cents(gross) != paid_cents or cents(total) != paid_cents):
        raise ValueError(ERROR)
    return dict(schemaVersion=1, route=route, documentNumber=document,
                paymentDate=payment_date, periodEnd=period_end, amountCents=paid_cents, currency="USD")


def read_bytes(content, route):
    # Only the email reader needs the isolated PDF runtime. API reconciliation
    # reads already validated facts and must not depend on that runtime.
    from .accounting_pdf_capacity import capacity
    from .accounting_pdf_process import run_bounded
    if not 0 < len(content) <= 15 * 1024 * 1024 or not content.startswith(b"%PDF-"):
        raise ValueError(ERROR)
    sandbox = "/usr/local/bin/routespark-pdf-sandbox"
    with capacity(route), tempfile.TemporaryDirectory(prefix="accounting-advice-") as directory:
        source = Path(directory) / "source.pdf"
        source.write_bytes(content)
        info = run_bounded([sandbox, "pdfinfo", str(source), str(os.getpid())], stdout_limit=64 * 1024, timeout=10, new_session=True).decode("utf-8")
        if one(r"^Pages:\s+([0-9]+)\s*$", info) != "1":
            raise ValueError(ERROR)
        # Raw content order keeps the right-hand document header together;
        # layout order interleaves the recipient address into that header.
        text = run_bounded([sandbox, "pdftotext-raw", str(source), str(os.getpid())], stdout_limit=256 * 1024, timeout=20, new_session=True).decode("utf-8")
    return parse_text(text, route)


def save(store, advice, message_id, content):
    key = PREFIX + advice["documentNumber"]
    previous = store.get(key)
    digest = hashlib.sha256(content).hexdigest()
    if previous:
        if any(previous.get(name) != value for name, value in advice.items()):
            if previous.get("status") != "conflict":
                store.put(key, {**previous, "status": "conflict", "conflictingMessageId": message_id,
                    "detail": "Different payment advices use the same document number. Review the payment in Money in. Existing payments were preserved."})
            raise ValueError("A different payment advice uses the same document number. Review it in Money in; existing payments were preserved.")
        return previous
    record = dict(advice, messageId=message_id, sha256=digest, receivedAt=now(), status="pending")
    store.put(key, record)
    return record


def neutral_arrival_detail(detail):
    # Exact legacy system copy only; preserve mismatch reasons and user content.
    if detail == "Payment recorded automatically from a matching Gruma payment advice.":
        return "Payment recorded automatically from a matching company payment advice."
    return detail


def public_payment_notes(ledger):
    """Present legacy system notes without rewriting stored financial evidence."""
    payments = []
    for payment in ledger["remittances"]:
        evidence = payment.get("paymentAdvice") or {}
        number = evidence.get("documentNumber")
        legacy = f"Confirmed by Gruma payment advice {number}. Date is the company payment date."
        if number and payment.get("note") == legacy:
            payment = {**payment, "note": f"Confirmed by company payment advice {number}. Date is the company payment date."}
        payments.append(payment)
    return {**ledger, "remittances": payments}


def arrival_status(result, records):
    """Project current advice state so later statement confirmation clears waiting."""
    numbers = result.get("paymentAdviceDocuments", [])
    current = [records.get(number) for number in numbers]
    errors = result.get("paymentAdviceErrors", [])
    complete = bool(numbers) and not errors and not result.get("statementReadErrors") and all(record and record.get("status") in ("recorded", "already_recorded") for record in current)
    details = list(dict.fromkeys([neutral_arrival_detail(record.get("detail", "Payment advice needs review in Money in.")) for record in current if record] + errors))
    matched = sorted({record["documentNumber"] for record in current if record and record.get("status") in ("recorded", "already_recorded")})
    recorded = sorted({record["documentNumber"] for record in current if record and record.get("status") == "recorded"})
    result = {**result, "paymentAdviceRecorded": len(recorded),
              "paymentAdviceMatchedDocuments": matched, "paymentAdviceRecordedDocuments": recorded,
              "detail": " ".join(details) or "Payment advice needs review in Money in."}
    if result.get("statements"):
        result["detail"] += " Statements from this email are also saved in Money in."
    if result.get('paymentAdviceRemovedDocuments'):
        result['detail'] += ' Matched payment-advice PDFs were removed; payment details were kept.'
    if not complete:
        result['detail'] += ' Unmatched payment-advice PDFs are kept for review.'
    return ("ready" if complete else "needs_attention"), result


def reconcile(accounting, ledger=None):
    """No PDF reads or external calls while the route transaction is held."""
    store = accounting.store
    if ledger is None:
        ledger = accounting.load("money-in")
    advices = store.documents(PREFIX)
    changed = False
    for advice in advices:
        if advice.get("status") not in ("pending", "needs_attention") or advice.get("route") != accounting.route:
            continue
        period = next((p for p in ledger["settlements"] if p["periodEnd"] == advice["periodEnd"]), None)
        payment = next((p for p in ledger["remittances"] if p["periodEnd"] == advice["periodEnd"]), None)
        period_label = datetime.strptime(advice['periodEnd'], '%Y-%m-%d').strftime('%m/%d/%Y')
        status, detail = "pending", f"Payment advice for the billing period ending {period_label} is saved. Confirm the final statement to match its payment."
        if period and period.get("revision") == "final" and period.get("statementReview") and period.get("source", "").startswith("sha256:"):
            peers = [a for a in advices if a.get("periodEnd") == advice["periodEnd"] and a.get("documentNumber") != advice["documentNumber"]]
            if peers:
                status, detail = "needs_attention", f"More than one payment advice document refers to the billing period ending {period_label}. Review the payment in Money in."
            elif period.get("amountToPayCents") != advice["amountCents"]:
                expected = period.get('amountToPayCents')
                amount = f"${expected / 100:,.2f}" if isinstance(expected, int) else 'an unread amount'
                status, detail = "needs_attention", f"Billing period ending {period_label}: payment advice ${advice['amountCents'] / 100:,.2f} does not match the final statement's {amount}. No payment was changed."
            elif advice["paymentDate"] > accounting.today:
                status, detail = "needs_attention", f"The payment advice for the billing period ending {period_label} has a future payment date. Review it in Money in."
            elif payment:
                status = "already_recorded" if payment["amountCents"] == advice["amountCents"] else "needs_attention"
                detail = (f"The payment advice matches the final statement for the billing period ending {period_label}. Your existing payment was preserved."
                          if status == 'already_recorded' else f"Billing period ending {period_label}: the recorded payment is ${payment['amountCents'] / 100:,.2f}, while the advice is ${advice['amountCents'] / 100:,.2f}. Your existing payment was preserved. Review it in Money in.")
            elif len(ledger["remittances"]) >= 520:
                status, detail = "needs_attention", "The payment record limit was reached. Review the payment in Money in."
            else:
                stamp = now()
                payment = dict(id=str(uuid4()), periodEnd=advice["periodEnd"], receivedDate=advice["paymentDate"],
                    amountCents=advice["amountCents"], method="other",
                    note=f"Confirmed by company payment advice {advice['documentNumber']}. Date is the company payment date.",
                    createdAt=stamp, updatedAt=stamp,
                    paymentAdvice={k: advice[k] for k in ("documentNumber", "messageId", "sha256", "paymentDate", "amountCents")})
                ledger["remittances"].append(payment)
                changed = True
                status, detail = "recorded", "Payment recorded automatically from a matching company payment advice."
        updated = {**advice, "status": status, "detail": detail}
        if payment:
            updated["remittanceId"] = payment["id"]
        if updated != advice:
            store.put(PREFIX + advice["documentNumber"], updated)
            if status in ('recorded', 'already_recorded'):
                store.queue_payment_advice_cleanup(advice['documentNumber'])
    return accounting.save("money-in", ledger) if changed else ledger
