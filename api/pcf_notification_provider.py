"""Small Telegram adapter with conservative acceptance classification."""

from __future__ import annotations

import json
import re
import socket
import urllib.error
import urllib.request
from collections.abc import Mapping
from http.client import IncompleteRead, RemoteDisconnected
from typing import Any, Callable

from .pcf_effect_notification import (
    NotificationEnvelope,
    NotificationProviderResult,
)


_TOKEN_RE = re.compile(r"^[0-9]{6,15}:[A-Za-z0-9_-]{20,128}$")
_CHAT_RE = re.compile(r"^(?:-?[0-9]{5,24}|@[A-Za-z][A-Za-z0-9_]{4,31})$")
MAX_RESPONSE_BYTES = 65_536


class TelegramNotificationProvider:
    """Send one admin signal without ever retrying an ambiguous acceptance."""

    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        timeout_seconds: int = 10,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        if (
            not isinstance(bot_token, str)
            or _TOKEN_RE.fullmatch(bot_token) is None
            or not isinstance(chat_id, str)
            or _CHAT_RE.fullmatch(chat_id) is None
            or type(timeout_seconds) is not int
            or not 1 <= timeout_seconds <= 30
            or not callable(opener)
        ):
            raise ValueError("invalid Telegram notification provider configuration")
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    def __call__(self, envelope: NotificationEnvelope) -> NotificationProviderResult:
        if not isinstance(envelope, NotificationEnvelope):
            raise ValueError("invalid PCF notification envelope")
        body = json.dumps(
            {
                "chat_id": self._chat_id,
                "text": f"{envelope.title}\n\n{envelope.body}",
                "disable_notification": False,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = urllib.request.Request(
            self._url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                status = int(getattr(response, "status", 0))
                raw = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError:
            return NotificationProviderResult("failed_before_acceptance")
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                return NotificationProviderResult("outcome_unknown")
            return NotificationProviderResult("failed_before_acceptance")
        except (TimeoutError, socket.timeout, IncompleteRead, RemoteDisconnected):
            return NotificationProviderResult("outcome_unknown")
        except OSError:
            return NotificationProviderResult("failed_before_acceptance")

        if status != 200:
            return NotificationProviderResult("failed_before_acceptance")
        if len(raw) > MAX_RESPONSE_BYTES:
            return NotificationProviderResult("outcome_unknown")
        try:
            payload = json.loads(raw.decode("utf-8"))
            if not isinstance(payload, Mapping):
                raise TypeError("Telegram response must be an object")
            message_id = payload["result"]["message_id"]
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError):
            return NotificationProviderResult("outcome_unknown")
        if payload.get("ok") is not True or type(message_id) is not int or message_id < 1:
            return NotificationProviderResult("outcome_unknown")
        return NotificationProviderResult("delivered", f"telegram:{message_id}")
