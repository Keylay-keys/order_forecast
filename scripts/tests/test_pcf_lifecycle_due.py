from __future__ import annotations

import copy
import random
import unittest
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from order_forecast.api.pcf_event_contract import business_hash, firestore_revision
from order_forecast.scripts.pcf_lifecycle_due import (
    ClaimedLifecycleWork,
    LifecycleContractError,
    MAX_ROUTE_REPLAN_DELIVERIES,
    ProjectedContainer,
    build_cleanup_plan,
    business_date_at,
    cleanup_authority_decision,
    claim_one_lifecycle_work,
    plan_projected_route_from_current_policy,
    resolve_cleanup_due_at,
)


def revision(day: int, hour: int = 12, rank: str = "upsert") -> str:
    return firestore_revision(
        f"2026-08-{day:02d}T{hour:02d}:00:00.000000000Z",
        rank,
    )


def item(expiry: str | None, product: str = "100") -> dict:
    return {
        "description": "Item",
        "expiryDate": expiry,
        "isLowQuantity": False,
        "isShortCoded": False,
        "itemPosition": 0,
        "pageNumber": 1,
        "product": product,
        "visibleOnList": True,
    }


def container(
    container_id: str,
    expiries: list[str | None],
    *,
    source_revision: str | None = None,
    active: bool = True,
) -> ProjectedContainer:
    items = tuple(item(expiry, str(index + 100)) for index, expiry in enumerate(expiries))
    return ProjectedContainer(
        route_number="989262",
        delivery_id="1806242572",
        container_id=container_id,
        active=active,
        items=items,
        source_revision=source_revision or revision(20),
        source_business_hash=business_hash(
            {"active": active, "container": container_id, "items": list(items)}
        ),
    )


def claimed_from(candidate, *, deadline_days: int = 30) -> ClaimedLifecycleWork:
    return ClaimedLifecycleWork(
        id=1,
        semantic_key=candidate.semantic_key,
        route_number=candidate.route_number,
        delivery_id=candidate.delivery_id,
        container_id=candidate.anchor_container_id,
        effective_business_date=candidate.effective_business_date,
        due_at=candidate.due_at,
        deadline_at=candidate.due_at + timedelta(days=deadline_days),
        source_revision=candidate.source_revision,
        source_business_hash=candidate.source_business_hash,
        decision_watermark=candidate.decision_watermark,
        claim_token="test-token",
        attempt_count=1,
    )


class CleanupCalendarTests(unittest.TestCase):
    def test_strict_retention_boundary_adds_retention_plus_one_days(self):
        plan = build_cleanup_plan(
            [container("C01", ["2026-08-20"])],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertEqual(plan.reason, "scheduled")
        self.assertEqual(plan.candidate.effective_business_date, date(2026, 8, 25))
        self.assertEqual(
            plan.candidate.due_at,
            datetime(2026, 8, 25, 8, 0, tzinfo=timezone.utc),
        )

    def test_latest_item_across_every_container_controls_delivery_due_date(self):
        plan = build_cleanup_plan(
            [
                container("C02", ["2026-08-20", "2026-08-30"]),
                container("C01", ["2026-08-25"]),
            ],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertEqual(plan.candidate.latest_expiry_date, date(2026, 8, 30))
        self.assertEqual(plan.candidate.earliest_expiry_date, date(2026, 8, 20))
        self.assertEqual(plan.candidate.anchor_container_id, "C02")
        self.assertEqual(plan.candidate.item_count, 3)
        self.assertEqual(plan.candidate.effective_business_date, date(2026, 9, 4))

    def test_latest_expiry_tie_uses_stable_container_identity(self):
        plan = build_cleanup_plan(
            [
                container("C10", ["2026-08-30"]),
                container("C02", ["2026-08-30"]),
            ],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertEqual(plan.candidate.anchor_container_id, "C02")

    def test_missing_expiry_fails_closed(self):
        plan = build_cleanup_plan(
            [container("C01", ["2026-08-20", None])],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertIsNone(plan.candidate)
        self.assertEqual(plan.reason, "missing_or_invalid_expiry")
        self.assertEqual(plan.missing_expiry_count, 1)

    def test_empty_container_fails_closed_even_when_another_container_is_valid(self):
        plan = build_cleanup_plan(
            [container("C01", ["2026-08-20"]), container("C02", [])],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertIsNone(plan.candidate)
        self.assertEqual(plan.reason, "empty_container")

    def test_no_containers_fails_closed(self):
        plan = build_cleanup_plan(
            [], retention_days=4, policy_revision=revision(21)
        )
        self.assertEqual(plan.reason, "no_containers")
        self.assertIsNone(plan.candidate)

    def test_inactive_but_authoritative_container_is_not_silently_omitted(self):
        plan = build_cleanup_plan(
            [container("C01", ["2026-08-20"], active=False)],
            retention_days=4,
            policy_revision=revision(21),
        )
        self.assertIsNotNone(plan.candidate)
        self.assertEqual(plan.candidate.item_count, 1)

    def test_route_policy_replan_query_is_active_scoped_and_hard_bounded(self):
        class Cursor:
            def __init__(self):
                self.sql = ""
                self.params = ()

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params):
                self.sql = " ".join(sql.split())
                self.params = params

            def fetchall(self):
                return [(f"D{index}",) for index in range(MAX_ROUTE_REPLAN_DELIVERIES + 1)]

        class Connection:
            def __init__(self):
                self.last_cursor = None

            def cursor(self, *args, **kwargs):
                self.last_cursor = Cursor()
                return self.last_cursor

        conn = Connection()
        with self.assertRaisesRegex(
            LifecycleContractError,
            "route_active_delivery_limit_exceeded",
        ):
            plan_projected_route_from_current_policy(
                conn,
                route_number="989262",
                now_utc=datetime(2026, 8, 28, tzinfo=timezone.utc),
            )
        self.assertIn("HAVING BOOL_OR(active)", conn.last_cursor.sql)
        self.assertIn("LIMIT %s", conn.last_cursor.sql)
        self.assertEqual(
            conn.last_cursor.params,
            ("989262", MAX_ROUTE_REPLAN_DELIVERIES + 1),
        )

    def test_spring_dst_gap_moves_two_am_to_first_valid_instant(self):
        self.assertEqual(
            resolve_cleanup_due_at(date(2027, 3, 14)),
            datetime(2027, 3, 14, 9, 0, tzinfo=timezone.utc),
        )

    def test_normal_winter_and_summer_offsets_are_correct(self):
        self.assertEqual(
            resolve_cleanup_due_at(date(2027, 1, 15)),
            datetime(2027, 1, 15, 9, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            resolve_cleanup_due_at(date(2027, 7, 15)),
            datetime(2027, 7, 15, 8, 0, tzinfo=timezone.utc),
        )

    def test_business_date_uses_denver_not_runtime_utc(self):
        instant = datetime(2026, 8, 25, 5, 30, tzinfo=timezone.utc)
        self.assertEqual(business_date_at(instant), date(2026, 8, 24))

    def test_invalid_timezone_retention_and_naive_datetime_are_rejected(self):
        with self.assertRaises(LifecycleContractError):
            resolve_cleanup_due_at(date(2026, 8, 25), timezone_name="Not/AZone")
        with self.assertRaises(LifecycleContractError):
            build_cleanup_plan(
                [container("C01", ["2026-08-20"])],
                retention_days=0,
                policy_revision=revision(21),
            )
        with self.assertRaises(LifecycleContractError):
            business_date_at(datetime(2026, 8, 25, 2, 0))

    def test_planning_is_independent_of_container_input_order(self):
        containers = [
            container("C03", ["2026-08-29"], source_revision=revision(22)),
            container("C01", ["2026-08-20"], source_revision=revision(20)),
            container("C02", ["2026-08-25"], source_revision=revision(21)),
        ]
        forward = build_cleanup_plan(
            containers, retention_days=4, policy_revision=revision(23)
        ).candidate
        reverse = build_cleanup_plan(
            list(reversed(containers)),
            retention_days=4,
            policy_revision=revision(23),
        ).candidate
        self.assertEqual(forward, reverse)

    def test_reference_predicate_matches_two_thousand_generated_deliveries(self):
        rng = random.Random(989262)
        base = date(2026, 1, 1)
        for case_number in range(2000):
            retention_days = rng.randint(1, 30)
            generated = []
            all_expiries = []
            expected_schedulable = True
            for container_number in range(rng.randint(1, 5)):
                expiries = []
                for _ in range(rng.randint(0, 12)):
                    if rng.random() < 0.08:
                        expiry = None
                        expected_schedulable = False
                    else:
                        expiry_date = base + timedelta(days=rng.randint(0, 730))
                        expiry = expiry_date.isoformat()
                        all_expiries.append(expiry_date)
                    expiries.append(expiry)
                if not expiries:
                    expected_schedulable = False
                generated.append(
                    container(
                        f"C{container_number + 1:02d}",
                        expiries,
                        source_revision=revision(20 + (container_number % 3)),
                    )
                )
            plan = build_cleanup_plan(
                generated,
                retention_days=retention_days,
                policy_revision=revision(24),
            )
            with self.subTest(case=case_number):
                self.assertEqual(plan.candidate is not None, expected_schedulable)
                if expected_schedulable:
                    expected_date = max(all_expiries) + timedelta(
                        days=retention_days + 1
                    )
                    self.assertEqual(
                        plan.candidate.effective_business_date,
                        expected_date,
                    )

    def test_every_denver_due_time_for_ten_years_stays_on_its_business_date(self):
        current = date(2026, 1, 1)
        end = date(2036, 1, 1)
        while current < end:
            resolved = resolve_cleanup_due_at(current)
            local = resolved.astimezone(ZoneInfo("America/Denver"))
            self.assertEqual(local.date(), current)
            self.assertGreaterEqual((local.hour, local.minute), (2, 0))
            self.assertLessEqual((local.hour, local.minute), (3, 0))
            current += timedelta(days=1)


class CleanupAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.containers = [
            container("C01", ["2026-08-19", "2026-08-20"]),
            container("C02", ["2026-08-18"]),
        ]
        self.policy_revision = revision(21)
        self.plan = build_cleanup_plan(
            self.containers,
            retention_days=4,
            policy_revision=self.policy_revision,
        )
        self.claimed = claimed_from(self.plan.candidate)

    def decide(self, observed=None, now=None, retention_days=4, policy_revision=None):
        return cleanup_authority_decision(
            self.claimed,
            observed if observed is not None else self.containers,
            now_utc=now or self.claimed.due_at,
            retention_days=retention_days,
            policy_revision=policy_revision or self.policy_revision,
        )

    def test_due_claim_recomputes_days_left_and_authorizes_only_after_retention(self):
        decision = self.decide()
        self.assertTrue(decision.authorized)
        self.assertEqual(decision.disposition, "authorized")
        self.assertEqual(decision.days_left_by_container["C01"], (-6, -5))
        self.assertEqual(decision.days_left_by_container["C02"], (-7,))

    def test_before_due_time_is_not_authorized(self):
        decision = self.decide(now=self.claimed.due_at - timedelta(seconds=1))
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "not_due")

    def test_changed_item_expiry_supersedes_the_claim(self):
        changed = copy.deepcopy(self.containers)
        changed[0] = container("C01", ["2026-08-19", "2026-09-20"], source_revision=revision(22))
        decision = self.decide(observed=changed)
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "superseded_input")

    def test_one_item_on_one_container_inside_retention_blocks_the_whole_delivery(self):
        mixed = [
            self.containers[0],
            container(
                "C02",
                ["2026-08-24"],
                source_revision=revision(22),
            ),
        ]
        decision = self.decide(observed=mixed)
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "superseded_input")
        self.assertEqual(decision.days_left_by_container["C02"], (-1,))

    def test_added_or_removed_container_supersedes_the_claim(self):
        added = [*self.containers, container("C03", ["2026-08-17"], source_revision=revision(22))]
        self.assertEqual(self.decide(observed=added).disposition, "superseded_input")
        self.assertEqual(self.decide(observed=self.containers[:1]).disposition, "superseded_input")

    def test_missing_expiry_blocks_even_after_claim(self):
        changed = [container("C01", [None], source_revision=revision(22)), self.containers[1]]
        decision = self.decide(observed=changed)
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "missing_or_invalid_expiry")

    def test_retention_change_supersedes_schedule(self):
        decision = self.decide(retention_days=8, policy_revision=revision(22))
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "superseded_schedule")

    def test_noop_revision_advance_does_not_invalidate_stable_business_work(self):
        advanced = [
            ProjectedContainer(
                **{
                    **self.containers[0].__dict__,
                    "source_revision": revision(22),
                }
            ),
            self.containers[1],
        ]
        decision = self.decide(observed=advanced)
        self.assertTrue(decision.authorized)
        self.assertEqual(decision.disposition, "authorized")

    def test_expired_deadline_blocks_effect(self):
        decision = self.decide(now=self.claimed.deadline_at + timedelta(seconds=1))
        self.assertFalse(decision.authorized)
        self.assertEqual(decision.disposition, "deadline_expired")


class FailClosedClaimAdmissionTests(unittest.TestCase):
    def test_missing_or_empty_allowlist_returns_without_touching_database(self):
        class NoDatabaseAllowed:
            def cursor(self, *args, **kwargs):
                raise AssertionError("database must not be touched for fail-closed admission")

        now = datetime.now(timezone.utc)
        self.assertIsNone(
            claim_one_lifecycle_work(
                NoDatabaseAllowed(), worker_id="worker-1", now_utc=now
            )
        )
        self.assertIsNone(
            claim_one_lifecycle_work(
                NoDatabaseAllowed(),
                worker_id="worker-1",
                now_utc=now,
                allowed_semantic_keys=[],
            )
        )


if __name__ == "__main__":
    unittest.main()
