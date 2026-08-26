"""Fail-closed cross-runtime fixture identity classification."""

from __future__ import annotations

from typing import Any, Mapping


def classify_fixture_document(document: Any) -> str:
    if not isinstance(document, Mapping):
        return "ordinary_data"
    metadata = document.get("metadata")
    nested = metadata if isinstance(metadata, Mapping) else {}
    automation_excluded = document.get("automationExcluded") is True or nested.get(
        "automationExcluded"
    ) is True
    markers = [
        marker
        for marker in (document.get("fixture"), nested.get("fixture"))
        if marker is not None
    ]
    if not automation_excluded and not markers:
        return "ordinary_data"
    if not automation_excluded or not markers:
        return "invalid_fixture_marker"
    normalized: list[tuple[str, str]] = []
    for marker in markers:
        if not isinstance(marker, Mapping) or set(marker) != {"identity", "generation"}:
            return "invalid_fixture_marker"
        identity = marker.get("identity")
        generation = marker.get("generation")
        if not isinstance(identity, str) or not identity or not isinstance(generation, str) or not generation:
            return "invalid_fixture_marker"
        normalized.append((identity, generation))
    if len(set(normalized)) != 1:
        return "invalid_fixture_marker"
    return "valid_fixture"
