"""Exact Firestore timestamp normalization helpers."""

from __future__ import annotations

import calendar
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Optional

from .errors import StructuredApiError


def timestamp_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    for method_name in ("to_millis", "ToMilliseconds"):
        method = getattr(value, method_name, None)
        if callable(method):
            return int(method())
    if isinstance(value, Mapping):
        seconds = value.get("seconds")
        nanos = value.get("nanoseconds", value.get("nanos", 0))
        if isinstance(seconds, int) and not isinstance(seconds, bool):
            return seconds * 1000 + int(nanos or 0) // 1_000_000
    elif isinstance(value, datetime):
        aware = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        utc_value = aware.astimezone(timezone.utc)
        whole_seconds = calendar.timegm(utc_value.utctimetuple())
        full_nanoseconds = (
            getattr(value, "nanosecond", getattr(value, "_nanosecond", 0))
            or value.microsecond * 1000
        )
        return whole_seconds * 1000 + int(full_nanoseconds) // 1_000_000
    else:
        seconds = getattr(value, "seconds", None)
        nanos = getattr(value, "nanoseconds", getattr(value, "nanos", 0))
        if isinstance(seconds, int) and not isinstance(seconds, bool):
            return seconds * 1000 + int(nanos or 0) // 1_000_000
    raise StructuredApiError(422, "Invalid settings revision", "INVALID_SETTINGS_REVISION")
