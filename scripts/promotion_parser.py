import re
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

import pandas as pd
from PyPDF2 import PdfReader

DATE_RE = re.compile(r"\d{1,2}/\d{1,2}/\d{2,4}")
SAP_RE = re.compile(r"\d{3,6}")
PRICE_RE = re.compile(r"(\d+\s*for\s*\$?\d+(?:\.\d{2})?|\$\d+(?:\.\d{2})?|\bBOGO\b)", re.IGNORECASE)

HEADER_KEYWORDS = (
    "Account Activity Promo Date",
    "StartPromo Date",
    "FinishAd Date",
    "Price Items on Promo",
    "SAP Codes",
    "One on One Sheet Week",
    "Weekly Executables",
)


def _read_pdf_lines(path: Path) -> List[str]:
    reader = PdfReader(str(path))
    lines: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if any(keyword in line for keyword in HEADER_KEYWORDS):
                continue
            lines.append(line)
    return lines


def _yield_records(raw_lines: Iterable[str]) -> Iterator[str]:
    buffer: Optional[str] = None
    current_account: Optional[str] = None

    for line in raw_lines:
        if any(keyword in line for keyword in HEADER_KEYWORDS):
            continue

        date_matches = list(DATE_RE.finditer(line))
        prefix = line[: date_matches[0].start()].strip() if date_matches else ""
        has_two_dates = len(date_matches) >= 2

        if has_two_dates and prefix:
            if buffer:
                yield buffer.strip()
            buffer = line
            current_account = prefix
        elif has_two_dates:
            if buffer:
                yield buffer.strip()
            inferred_prefix = current_account or ""
            line_with_account = f"{inferred_prefix} {line}".strip()
            buffer = line_with_account
        else:
            if buffer:
                buffer += " " + line
            else:
                buffer = line

    if buffer:
        yield buffer.strip()


def _parse_record(line: str) -> List[dict]:
    date_matches = list(DATE_RE.finditer(line))
    if len(date_matches) < 2:
        return []

    account = line[: date_matches[0].start()].strip()

    promo_start = line[date_matches[0].start() : date_matches[0].end()]
    promo_end = line[date_matches[1].start() : date_matches[1].end()]

    ad_date = None
    content_start_idx = date_matches[1].end()
    if len(date_matches) >= 3:
        ad_date = line[date_matches[2].start() : date_matches[2].end()]
        content_start_idx = date_matches[2].end()

    content = line[content_start_idx:].strip()

    sap_matches = list(SAP_RE.finditer(content))
    if not sap_matches:
        return []

    details = content[: sap_matches[0].start()].strip()
    sap_chunk = content[sap_matches[0].start() :]

    price = None
    description = details
    price_match = PRICE_RE.search(details)
    if price_match:
        price = price_match.group(0).strip()
        description = (details[: price_match.start()] + " " + details[price_match.end() :]).strip()
        description = " ".join(description.split())

    sap_codes = [code.strip() for code in SAP_RE.findall(sap_chunk)]
    if not sap_codes:
        return []

    records = []
    for code in sap_codes:
        records.append(
            {
                "account": account,
                "promo_start": promo_start,
                "promo_end": promo_end,
                "ad_date": ad_date,
                "price_text": price,
                "description": description,
                "sap_code": code,
                "source_line": line,
            }
        )
    return records


def _normalize_date(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = text.strip()
    if not text:
        return None
    parts = text.split('/')
    if len(parts) == 3 and len(parts[2]) == 2:
        parts[2] = f"20{parts[2]}"
        return '/'.join(parts)
    return text


def extract_promotions_from_pdf(path: str) -> pd.DataFrame:
    pdf_path = Path(path)
    raw_lines = _read_pdf_lines(pdf_path)
    rows: List[dict] = []

    for record in _yield_records(raw_lines):
        rows.extend(_parse_record(record))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    for col in ("promo_start", "promo_end", "ad_date"):
        df[col] = df[col].apply(_normalize_date)
        df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    df["sap_code"] = df["sap_code"].astype(str)
    df["description"] = df["description"].str.strip()
    df["price_text"] = df["price_text"].str.strip()

    return df


def extract_promotions_from_pdfs(paths: Iterable[str]) -> pd.DataFrame:
    frames = [extract_promotions_from_pdf(path) for path in paths]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
