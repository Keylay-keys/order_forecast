from __future__ import annotations

import sys
from pathlib import Path

import pytest

from order_forecast.api.pcf_effect_service import EffectServiceError
from order_forecast.scripts import pcf_effect_worker_service


def test_disabled_process_exits_before_controls_credentials_or_database(
    monkeypatch, capsys
):
    monkeypatch.setenv("PCF_EFFECT_WORKER_ENABLED", "false")
    monkeypatch.setattr(sys, "argv", ["pcf-effect-worker"])
    monkeypatch.setattr(
        pcf_effect_worker_service,
        "_validate_preflight",
        lambda *_args: (_ for _ in ()).throw(AssertionError("preflight reached")),
    )
    assert pcf_effect_worker_service.main() == 0
    assert "pcf_effect_worker_disabled" in capsys.readouterr().out


def test_enabled_flag_is_exact(monkeypatch):
    monkeypatch.setenv("PCF_EFFECT_WORKER_ENABLED", "TRUE")
    with pytest.raises(EffectServiceError, match="must be true or false"):
        pcf_effect_worker_service._enabled()


def test_storage_effect_identity_uses_exact_short_lived_impersonation(monkeypatch):
    source_credentials = object()
    delegated_credentials = object()
    expected_bucket = object()
    observed = {}

    monkeypatch.setattr(
        pcf_effect_worker_service.service_account.Credentials,
        "from_service_account_file",
        staticmethod(
            lambda path, scopes: observed.update(
                source_path=path,
                source_scopes=scopes,
            ) or source_credentials
        ),
    )
    monkeypatch.setattr(
        pcf_effect_worker_service.impersonated_credentials,
        "Credentials",
        lambda **kwargs: observed.update(impersonation=kwargs) or delegated_credentials,
    )

    class FakeStorageClient:
        def __init__(self, *, project, credentials):
            observed["client"] = {
                "project": project,
                "credentials": credentials,
            }

        def bucket(self, name):
            observed["bucket"] = name
            return expected_bucket

    monkeypatch.setattr(
        pcf_effect_worker_service.google_storage,
        "Client",
        FakeStorageClient,
    )

    result = pcf_effect_worker_service._impersonated_bucket(
        Path("/app/config/firebase-writer.json"),
        target_principal=(
            "routespark-pcf-object-projector@routespark-1f47d.iam.gserviceaccount.com"
        ),
        project_id="routespark-1f47d",
        bucket_name="routespark-1f47d.firebasestorage.app",
    )

    assert result is expected_bucket
    assert observed["source_path"] == "/app/config/firebase-writer.json"
    assert observed["source_scopes"] == [
        "https://www.googleapis.com/auth/cloud-platform"
    ]
    assert observed["impersonation"] == {
        "source_credentials": source_credentials,
        "target_principal": (
            "routespark-pcf-object-projector@routespark-1f47d.iam.gserviceaccount.com"
        ),
        "target_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
        "lifetime": 900,
    }
    assert observed["client"] == {
        "project": "routespark-1f47d",
        "credentials": delegated_credentials,
    }
    assert observed["bucket"] == "routespark-1f47d.firebasestorage.app"
