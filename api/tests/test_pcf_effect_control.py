from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from order_forecast.api.pcf_effect_control import (
    EffectControlError,
    load_effect_control,
    parse_effect_control,
)
from order_forecast.api.tests.test_pcf_effect_worker import claim


NOW = datetime(2026, 8, 29, 23, 0, tzinfo=timezone.utc)


def payload(*, enabled=True, all_proven=False, route="988200"):
    current = claim()
    return {
        "schemaVersion": 1,
        "effectsEnabled": enabled,
        "allProvenWork": all_proven,
        "fences": [
            {
                "workId": str(current.work_id),
                "routeNumber": route,
                "deliveryNumber": current.delivery,
                "inputHash": current.input_hash,
                "expiresAt": (NOW + timedelta(minutes=30)).isoformat().replace(
                    "+00:00", "Z"
                ),
                "evidenceSha256": "b" * 64,
            }
        ],
    }


def test_exact_live_fence_admits_only_matching_work_identity():
    control = parse_effect_control(payload())
    current = claim()
    assert control.admitted_work_ids(now=NOW) == (current.work_id,)
    assert control.admits(current, now=NOW)

    changed = claim()
    object.__setattr__(changed, "input_hash", "c" * 64)
    assert not control.admits(changed, now=NOW)


def test_disabled_expired_and_far_future_controls_admit_zero():
    disabled = payload(enabled=False)
    disabled["fences"] = []
    assert parse_effect_control(disabled).admitted_work_ids(now=NOW) == ()

    for offset in (-1, 61):
        current = payload()
        current["fences"][0]["expiresAt"] = (
            NOW + timedelta(minutes=offset)
        ).isoformat().replace("+00:00", "Z")
        assert parse_effect_control(current).admitted_work_ids(now=NOW) == ()


def test_empty_enabled_broad_scope_review_route_and_duplicate_fail_closed():
    empty = payload()
    empty["fences"] = []
    duplicate = payload()
    duplicate["fences"].append(dict(duplicate["fences"][0]))
    for current, code in (
        (empty, "empty"),
        (payload(all_proven=True), "broad_scope"),
        (payload(route="900000"), "invalid"),
        (duplicate, "duplicate"),
    ):
        with pytest.raises(EffectControlError, match=code):
            parse_effect_control(current)


def test_file_loader_rejects_duplicates_and_malformed_json(tmp_path: Path):
    path = tmp_path / "effect-control.json"
    path.write_text(json.dumps(payload()), encoding="utf-8")
    assert load_effect_control(path).admitted_work_ids(now=NOW) == (
        UUID(payload()["fences"][0]["workId"]),
    )

    path.write_text('{"schemaVersion":1,"schemaVersion":1}', encoding="utf-8")
    with pytest.raises(EffectControlError, match="unavailable"):
        load_effect_control(path)

    path.write_text("not-json", encoding="utf-8")
    with pytest.raises(EffectControlError, match="unavailable"):
        load_effect_control(path)
