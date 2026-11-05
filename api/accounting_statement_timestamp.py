"""Printed first-page issue time, kept timezone-free exactly as printed."""
import re
from datetime import datetime


def printed_issued_at(text):
    if not isinstance(text, str):
        return None
    first_line = next((line.strip() for line in text.split("\f", 1)[0].splitlines() if line.strip()), "")
    match = re.fullmatch(r"Gruma Corporation[ \t]+(\d{2})/(\d{2})/(\d{2})[ \t]+(\d{2}):(\d{2})", first_line)
    if not match:
        return None
    month, day, year, hour, minute = map(int, match.groups())
    try:
        return datetime(2000 + year, month, day, hour, minute).isoformat(timespec="minutes")
    except ValueError:
        return None


def statement_issued_at(attempt):
    evidence = attempt.get("_evidence") or {}
    return printed_issued_at(evidence.get("sourceText")) if isinstance(evidence, dict) else None
