"""Read-only metrics from a source-verified, immutable statement attempt."""
from decimal import Decimal, InvalidOperation


def _cents(value):
    if type(value) not in (int, float):
        raise ValueError("Missing amount")
    scaled = Decimal(str(value)) * 100
    if not scaled.is_finite() or scaled != scaled.to_integral_value() or abs(scaled) > 100_000_000:
        raise ValueError("Invalid amount")
    return int(scaled)


def gross_sales_cents(attempt):
    """Project only the gross-sales denominator; never expose PDF text.

    The caller verifies the original SHA-256 first. Reconcile retained extractor
    fields against that attempt's public net-sales check, including signed
    settlement returns (the summary's SBT returns must not be subtracted twice).
    Older or incomplete evidence remains unknown, without changing the attempt.
    """
    try:
        evidence = attempt["_evidence"]
        extracted = evidence["extracted"]
        analysis = attempt["analysis"]
        if evidence["readerVersion"] != "accounting-pdf-v1" or extracted["schemaVersion"] != 1 or extracted["currency"] != "USD":
            return None
        if extracted["routeNumber"] != attempt["route"] or analysis["route"] != attempt["route"]:
            return None
        if any(extracted[key] != analysis[key] for key in ("periodStart", "periodEnd")) or extracted["source"]["documentId"] != "sha256:" + attempt["sha256"]:
            return None
        checks = [row for row in analysis["checks"] if row["label"] == "Net sales"]
        if len(checks) != 1:
            return None
        check = checks[0]
        sales = extracted["sales"]
        gross, promotions, allowances, net = (_cents(sales[key]) for key in ("grossSales", "promotions", "networkAllowances", "netSales"))
        returns = [row["cents"] for row in analysis["adjustments"] if row["key"] == "salesReturns"]
        if len(returns) > 1 or any(type(value) is not int or abs(value) > 100_000_000 for value in returns):
            return None
        if type(check["printedCents"]) is not int or type(check["calculatedCents"]) is not int:
            return None
        if gross + sum(returns) + promotions + allowances != net or check["printedCents"] != net or check["calculatedCents"] != net:
            return None
        return gross
    except (KeyError, TypeError, ValueError, InvalidOperation):
        return None
