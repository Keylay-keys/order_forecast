from __future__ import annotations

import io
import json
import socket
import urllib.error

import pytest

from order_forecast.api.pcf_effect_notification import NotificationEnvelope
from order_forecast.api.pcf_notification_provider import TelegramNotificationProvider


TOKEN = "123456789:" + "a" * 32
CHAT = "-123456789"
ENVELOPE = NotificationEnvelope(
    "a" * 64,
    "telegram",
    "ocr_post_send_review",
    "routespark-admin",
    "Review required",
    "Route 988200 delivery 1806248060 needs review.",
)


class Response:
    def __init__(self, status, payload):
        self.status = status
        self._body = io.BytesIO(payload)

    def read(self, maximum):
        return self._body.read(maximum)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def provider(opener):
    return TelegramNotificationProvider(
        bot_token=TOKEN,
        chat_id=CHAT,
        opener=opener,
    )


def test_verified_success_returns_stable_provider_receipt_and_fixed_payload():
    observed = {}

    def open_request(request, *, timeout):
        observed["url"] = request.full_url
        observed["payload"] = json.loads(request.data)
        observed["timeout"] = timeout
        return Response(200, b'{"ok":true,"result":{"message_id":42}}')

    result = provider(open_request)(ENVELOPE)
    assert result.disposition == "delivered"
    assert result.receipt_id == "telegram:42"
    assert observed["payload"]["chat_id"] == CHAT
    assert "988200" in observed["payload"]["text"]
    assert observed["timeout"] == 10
    assert TOKEN in observed["url"]


def test_known_http_rejection_is_retryable_pre_acceptance():
    def rejected(request, *, timeout):
        raise urllib.error.HTTPError(request.full_url, 503, "unavailable", {}, None)

    assert provider(rejected)(ENVELOPE).disposition == "failed_before_acceptance"


def test_timeout_or_invalid_success_body_is_never_automatically_retried():
    def timed_out(_request, *, timeout):
        raise urllib.error.URLError(socket.timeout())

    assert provider(timed_out)(ENVELOPE).disposition == "outcome_unknown"
    assert provider(lambda *_args, **_kwargs: Response(200, b"not-json"))(
        ENVELOPE
    ).disposition == "outcome_unknown"


def test_only_definite_connection_failure_is_pre_acceptance():
    def dns_failed(_request, *, timeout):
        raise urllib.error.URLError(socket.gaierror())

    def reset(_request, *, timeout):
        raise urllib.error.URLError(ConnectionResetError())

    assert provider(dns_failed)(ENVELOPE).disposition == "failed_before_acceptance"
    assert provider(reset)(ENVELOPE).disposition == "outcome_unknown"


def test_credentials_and_destination_are_strict():
    for token, chat in (("bad", CHAT), (TOKEN, "abc")):
        with pytest.raises(ValueError, match="configuration"):
            TelegramNotificationProvider(bot_token=token, chat_id=chat)
