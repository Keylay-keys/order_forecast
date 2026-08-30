"""Fail-closed composition helpers for the fixed PCF effect controller."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from psycopg2.extras import RealDictCursor

from .pcf_effect_control import EffectRuntimeControl, load_effect_control
from .pcf_effect_controller import EffectControllerReport, run_effect_controller_cycle
from .pcf_effect_runtime import EffectClaim, claim_due_effect
from .pcf_effect_worker import EffectHandler
from .pcf_runtime_control import load_runtime_control


MAX_SERVICE_ACCOUNT_BYTES = 65_536
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com$")


class EffectServiceError(RuntimeError):
    """The worker process cannot safely acquire fixed-effect authority."""


@dataclass(frozen=True)
class ServiceAccountIdentity:
    path: Path
    project_id: str
    client_email: str


@dataclass(frozen=True)
class ControlledEffectCycleReport:
    runtime_revision: str
    admitted_work_count: int
    effect_control_enabled: bool
    controller: EffectControllerReport


def service_account_identity(
    path: Path,
    *,
    expected_project: str,
) -> ServiceAccountIdentity:
    try:
        size = path.stat().st_size
        if not 1 <= size <= MAX_SERVICE_ACCOUNT_BYTES:
            raise EffectServiceError("pcf_effect_service_account_unavailable")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except EffectServiceError:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EffectServiceError("pcf_effect_service_account_unavailable") from exc
    project = payload.get("project_id") if isinstance(payload, Mapping) else None
    email = payload.get("client_email") if isinstance(payload, Mapping) else None
    if (
        not isinstance(expected_project, str)
        or not expected_project
        or project != expected_project
        or not isinstance(email, str)
        or _EMAIL_RE.fullmatch(email) is None
    ):
        raise EffectServiceError("pcf_effect_service_account_identity_mismatch")
    return ServiceAccountIdentity(path, project, email)


def validate_service_account_separation(
    paths: tuple[Path, ...],
    *,
    expected_project: str,
    expected_direct_identities: tuple[str, ...] = (),
    delegated_identities: tuple[str, ...] = (),
) -> tuple[ServiceAccountIdentity, ...]:
    if len(paths) < 2:
        raise EffectServiceError("pcf_effect_service_accounts_not_separate")
    identities = tuple(
        service_account_identity(path, expected_project=expected_project)
        for path in paths
    )
    if expected_direct_identities and (
        len(expected_direct_identities) != len(identities)
        or tuple(identity.client_email for identity in identities)
        != expected_direct_identities
    ):
        raise EffectServiceError("pcf_effect_service_account_identity_mismatch")
    expected_suffix = f"@{expected_project}.iam.gserviceaccount.com"
    if any(
        not isinstance(email, str)
        or _EMAIL_RE.fullmatch(email) is None
        or not email.endswith(expected_suffix)
        for email in delegated_identities
    ):
        raise EffectServiceError("pcf_effect_service_account_identity_mismatch")
    all_emails = [identity.client_email for identity in identities]
    all_emails.extend(delegated_identities)
    if len(set(all_emails)) != len(all_emails):
        raise EffectServiceError("pcf_effect_service_accounts_not_separate")
    return identities


class PostgresSuccessorGuard:
    """Close the read transaction before exact Storage cleanup starts."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def __call__(self, claim: EffectClaim) -> bool:
        if not isinstance(claim, EffectClaim):
            raise ValueError("invalid PCF successor claim")
        try:
            with self._conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM pcf_work_items
                        WHERE route_number = %s
                          AND delivery_number = %s
                          AND source_revision COLLATE "C" > %s COLLATE "C"
                          AND status NOT IN ('superseded', 'cancelled')
                    ) AS successor_exists
                    """,
                    (claim.route, claim.delivery, claim.source_revision),
                )
                row = cur.fetchone()
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise
        if row is None or type(row.get("successor_exists")) is not bool:
            raise EffectServiceError("pcf_effect_successor_guard_invalid")
        return row["successor_exists"]


def _guarded_claim(
    control: EffectRuntimeControl,
) -> Callable[..., EffectClaim | None]:
    def claim(conn: Any, **kwargs: Any) -> EffectClaim | None:
        now = kwargs.get("now")
        current = claim_due_effect(conn, **kwargs)
        if current is not None and (
            not isinstance(now, datetime) or not control.admits(current, now=now)
        ):
            # run_one_effect rolls this transaction back before any handler or
            # external effect is reachable.
            raise EffectServiceError("pcf_effect_claim_not_allowlisted")
        return current

    return claim


def run_controlled_effect_cycle(
    conn: Any,
    db: Any,
    handlers: Mapping[str, EffectHandler],
    *,
    actor_id: str,
    runtime_control_path: Path,
    effect_control_path: Path,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    controller_fn: Callable[..., EffectControllerReport] = run_effect_controller_cycle,
) -> ControlledEffectCycleReport:
    now = now_fn().astimezone(timezone.utc)
    runtime = load_runtime_control(runtime_control_path)
    if not runtime.is_fresh(now) or not runtime.allows_effects:
        raise EffectServiceError("pcf_effect_runtime_authority_denied")
    control = load_effect_control(effect_control_path)
    admitted = control.admitted_work_ids(now=now)
    report = controller_fn(
        conn,
        db,
        handlers,
        actor_id=actor_id,
        admitted_work_ids=admitted,
        now_fn=lambda: now,
        claim_fn=_guarded_claim(control),
    )
    return ControlledEffectCycleReport(
        runtime.revision,
        len(admitted),
        control.effects_enabled,
        report,
    )
