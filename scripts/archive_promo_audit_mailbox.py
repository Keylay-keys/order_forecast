"""Archive the promo audit mailbox to disk.

This captures the current mailbox state as a reproducible filesystem archive:
- raw wrapper emails from the audit inbox
- attached source .eml files
- extracted promo attachments (xlsx/pdf/etc)
- JSONL manifests tying everything together

The archive is intended to live on the external drive and become the stable
source of truth for later warehouse rebuilds.
"""

from __future__ import annotations

import argparse
import email
import hashlib
import imaplib
import json
import os
import re
from dataclasses import dataclass
from email import policy
from email.message import EmailMessage, Message
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive promo audit mailbox")
    parser.add_argument("--imap-user", default=os.environ.get("PROMO_IMAP_USER"))
    parser.add_argument("--imap-pass", default=os.environ.get("PROMO_IMAP_PASS"))
    parser.add_argument("--mailbox", default="INBOX")
    parser.add_argument("--imap-host", default="imap.gmail.com")
    parser.add_argument("--imap-port", type=int, default=993)
    parser.add_argument(
        "--output-dir",
        default="/Volumes/Extreme SSD/routespark_promo_audit/restore-2025-09-24/raw_mailbox_archive/mission_forms",
        help="Archive root on disk",
    )
    args = parser.parse_args()
    if not args.imap_user or not args.imap_pass:
        parser.error("IMAP credentials required via --imap-user/--imap-pass or PROMO_IMAP_USER/PROMO_IMAP_PASS")
    return args


@dataclass
class ArchiveStats:
    wrapper_messages: int = 0
    attached_source_emls: int = 0
    extracted_attachments: int = 0


def ensure_dirs(root: Path) -> dict[str, Path]:
    paths = {
        "root": root,
        "wrappers": root / "wrappers",
        "source_emls": root / "source_emls",
        "attachments": root / "attachments",
        "manifests": root / "manifests",
    }
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def clean_name(value: str, fallback: str) -> str:
    text = re.sub(r"[\\/:*?\"<>|]+", " ", (value or "").strip())
    text = re.sub(r"\s+", " ", text).strip()
    return (text[:120] or fallback)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def email_date_iso(msg: Message) -> str:
    raw = msg.get("Date")
    if not raw:
        return ""
    try:
        dt = parsedate_to_datetime(raw)
    except Exception:
        return ""
    if dt is None:
        return ""
    return dt.isoformat()


def stable_message_key(message_id: str, fallback_seed: str) -> str:
    base = message_id.strip("<> ") if message_id else fallback_seed
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return base[:180]


def save_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def iter_imap_messages(client: imaplib.IMAP4_SSL, mailbox: str) -> Iterable[tuple[bytes, bytes]]:
    client.select(mailbox, readonly=True)
    status, data = client.search(None, "ALL")
    if status != "OK":
        raise RuntimeError(f"IMAP search failed: {status}")
    for msg_id in [item for item in data[0].split() if item]:
        status, fetched = client.fetch(msg_id, "(RFC822)")
        if status != "OK" or not fetched or not fetched[0]:
            continue
        raw = fetched[0][1]
        if raw:
            yield msg_id, raw


def extract_attached_messages(msg: EmailMessage) -> list[EmailMessage]:
    attached: list[EmailMessage] = []
    for part in msg.walk():
        if part.get_content_type() != "message/rfc822":
            continue
        payload = part.get_payload()
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, EmailMessage):
                    attached.append(item)
        elif isinstance(payload, EmailMessage):
            attached.append(payload)
    return attached


def archive_source_attachments(source_key: str, msg: EmailMessage, attachments_root: Path) -> list[dict]:
    records: list[dict] = []
    for index, part in enumerate(msg.walk(), start=1):
        if part.get_content_maintype() == "multipart":
            continue
        if part.get_content_type() == "message/rfc822":
            continue
        disp = part.get_content_disposition()
        filename = part.get_filename()
        data = part.get_payload(decode=True)
        if not data:
            continue
        ext = Path(filename).suffix.lower() if filename else ""
        safe_filename = clean_name(filename or f"part_{index}{ext}", f"part_{index}{ext or '.bin'}")
        attachment_rel = Path(source_key[:2]) / source_key / safe_filename
        target = attachments_root / attachment_rel
        save_bytes(target, data)
        records.append(
            {
                "source_key": source_key,
                "attachment_index": index,
                "filename": filename or "",
                "content_type": part.get_content_type(),
                "content_disposition": disp or "",
                "size_bytes": len(data),
                "sha256": sha256_bytes(data),
                "relative_path": str(Path("attachments") / attachment_rel),
            }
        )
    return records


def main() -> None:
    args = parse_args()
    root = Path(args.output_dir)
    paths = ensure_dirs(root)

    wrapper_manifest_path = paths["manifests"] / "wrapper_messages.jsonl"
    source_manifest_path = paths["manifests"] / "source_messages.jsonl"
    attachment_manifest_path = paths["manifests"] / "attachments.jsonl"

    client = imaplib.IMAP4_SSL(args.imap_host, args.imap_port)
    client.login(args.imap_user, args.imap_pass)

    stats = ArchiveStats()
    wrapper_records: list[dict] = []
    source_records: list[dict] = []
    attachment_records: list[dict] = []

    for ordinal, (msg_id, raw) in enumerate(iter_imap_messages(client, args.mailbox), start=1):
        stats.wrapper_messages += 1
        wrapper_msg = BytesParser(policy=policy.default).parsebytes(raw)
        wrapper_subject = clean_name(wrapper_msg.get("Subject", ""), f"wrapper_{ordinal}")
        wrapper_date = email_date_iso(wrapper_msg)[:10] or "unknown-date"
        wrapper_key = stable_message_key(wrapper_msg.get("Message-ID", ""), f"wrapper_{ordinal}_{sha256_bytes(raw)[:12]}")
        wrapper_rel = Path(wrapper_date[:4]) / f"{wrapper_date}_{wrapper_key}_{wrapper_subject}.eml"
        wrapper_path = paths["wrappers"] / wrapper_rel
        save_bytes(wrapper_path, raw)

        wrapper_record = {
            "wrapper_imap_id": msg_id.decode(),
            "wrapper_key": wrapper_key,
            "message_id": wrapper_msg.get("Message-ID", ""),
            "subject": wrapper_msg.get("Subject", "") or "",
            "from": wrapper_msg.get("From", "") or "",
            "to": wrapper_msg.get("To", "") or "",
            "date": email_date_iso(wrapper_msg),
            "sha256": sha256_bytes(raw),
            "size_bytes": len(raw),
            "relative_path": str(Path("wrappers") / wrapper_rel),
        }
        wrapper_records.append(wrapper_record)

        source_messages = extract_attached_messages(wrapper_msg)
        for source_index, source_msg in enumerate(source_messages, start=1):
            source_raw = source_msg.as_bytes(policy=policy.default)
            source_subject = clean_name(source_msg.get("Subject", ""), f"source_{source_index}")
            source_date = email_date_iso(source_msg)[:10] or wrapper_date
            source_key = stable_message_key(
                source_msg.get("Message-ID", ""),
                f"{wrapper_key}_source_{source_index}_{sha256_bytes(source_raw)[:12]}",
            )
            source_rel = Path(source_date[:4]) / f"{source_date}_{source_key}_{source_subject}.eml"
            source_path = paths["source_emls"] / source_rel
            save_bytes(source_path, source_raw)
            stats.attached_source_emls += 1

            source_record = {
                "wrapper_key": wrapper_key,
                "source_key": source_key,
                "source_index": source_index,
                "message_id": source_msg.get("Message-ID", "") or "",
                "subject": source_msg.get("Subject", "") or "",
                "from": source_msg.get("From", "") or "",
                "to": source_msg.get("To", "") or "",
                "date": email_date_iso(source_msg),
                "sha256": sha256_bytes(source_raw),
                "size_bytes": len(source_raw),
                "relative_path": str(Path("source_emls") / source_rel),
            }
            source_records.append(source_record)

            extracted = archive_source_attachments(source_key, source_msg, paths["attachments"])
            stats.extracted_attachments += len(extracted)
            attachment_records.extend(extracted)

    client.logout()

    wrapper_manifest_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=True) for row in wrapper_records) + ("\n" if wrapper_records else ""),
        encoding="utf-8",
    )
    source_manifest_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=True) for row in source_records) + ("\n" if source_records else ""),
        encoding="utf-8",
    )
    attachment_manifest_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=True) for row in attachment_records) + ("\n" if attachment_records else ""),
        encoding="utf-8",
    )

    summary = {
        "archive_root": str(root),
        "mailbox": args.mailbox,
        "wrapper_messages": stats.wrapper_messages,
        "attached_source_emls": stats.attached_source_emls,
        "extracted_attachments": stats.extracted_attachments,
    }
    (paths["manifests"] / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
