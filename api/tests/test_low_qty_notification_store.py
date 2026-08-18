import os
import unittest
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from order_forecast.scripts import low_qty_notification_store as store


def _preference(route: str = "989262") -> store.EnabledPreference:
    return store.EnabledPreference(
        route_number=route,
        owner_uid=f"owner-{route}",
        reminder_minute_local=8 * 60,
        timezone_name="America/Denver",
        next_due_at=datetime(2026, 8, 19, 14, 0, tzinfo=timezone.utc),
    )


def _preference_row() -> dict:
    return {
        "route_number": "989262",
        "owner_uid": "owner-989262",
        "reminder_minute_local": 8 * 60,
        "timezone": "America/Denver",
        "next_due_at": datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        "preference_version": 4,
    }


def _execution_row(**overrides) -> dict:
    row = {
        "route_number": "989262",
        "scheduled_local_date": date(2026, 8, 18),
        "scheduled_for_utc": datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        "claimed_preference_version": 4,
        "owner_uid": "owner-989262",
        "status": "processing",
        "claim_token": "claim-token",
        "claimed_at": datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        "lease_expires_at": datetime(2026, 8, 18, 14, 5, tzinfo=timezone.utc),
        "attempt_count": 1,
        "computed_payload": None,
        "computed_saps": None,
    }
    row.update(overrides)
    return row


def _claim() -> store.ClaimedExecution:
    return store.ClaimedExecution(
        route_number="989262",
        scheduled_local_date=date(2026, 8, 18),
        scheduled_for_utc=datetime(2026, 8, 18, 14, 0, tzinfo=timezone.utc),
        claimed_preference_version=4,
        owner_uid="owner-989262",
        reminder_minute_local=8 * 60,
        timezone_name="America/Denver",
        claim_token="claim-token",
        attempt_count=1,
    )


class LowQtyNotificationStoreTests(unittest.TestCase):
    def test_connection_is_bounded_and_not_autocommit(self):
        conn = MagicMock()
        with patch.object(store.psycopg2, "connect", return_value=conn) as connect, patch.dict(
            os.environ,
            {
                "POSTGRES_CONNECT_TIMEOUT_SECONDS": "7",
                "POSTGRES_STATEMENT_TIMEOUT_MS": "11000",
                "POSTGRES_LOCK_TIMEOUT_MS": "4000",
                "POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS": "22000",
            },
        ):
            observed = store._pg_connect()

        self.assertIs(observed, conn)
        self.assertFalse(conn.autocommit)
        kwargs = connect.call_args.kwargs
        self.assertEqual(kwargs["connect_timeout"], 7)
        self.assertIn("statement_timeout=11000", kwargs["options"])
        self.assertIn("lock_timeout=4000", kwargs["options"])
        self.assertIn("idle_in_transaction_session_timeout=22000", kwargs["options"])

    def test_run_counts_are_read_then_rolled_back(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (7, 2)
        now_utc = datetime(2026, 8, 18, 14, 5, tzinfo=timezone.utc)

        counts = store.load_preference_run_counts(
            now_utc=now_utc,
            connect=lambda: conn,
        )

        self.assertEqual(counts, {"enabled": 7, "due": 2})
        sql, params = cursor.execute.call_args.args
        self.assertIn("COUNT(*) FILTER", sql)
        self.assertEqual(params, (now_utc,))
        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()

    def test_enabled_snapshot_is_read_only_and_route_ordered(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            {
                "route_number": "961825",
                "owner_uid": "owner-a",
                "reminder_minute_local": 480,
                "timezone": "America/Denver",
            }
        ]

        rows = store.load_enabled_preference_snapshot(connect=lambda: conn)

        self.assertEqual(rows[0]["route_number"], "961825")
        sql = cursor.execute.call_args.args[0]
        self.assertIn("WHERE enabled = TRUE", sql)
        self.assertIn("ORDER BY route_number", sql)
        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()

    def test_preference_validation_is_strict(self):
        invalid = (
            store.EnabledPreference("route-1", "owner", 480, "America/Denver", _preference().next_due_at),
            store.EnabledPreference("989262", "", 480, "America/Denver", _preference().next_due_at),
            store.EnabledPreference("989262", None, 480, "America/Denver", _preference().next_due_at),
            store.EnabledPreference("989262", "owner", 1440, "America/Denver", _preference().next_due_at),
            store.EnabledPreference("989262", "owner", 480, "Mars/Olympus", _preference().next_due_at),
            store.EnabledPreference("989262", "owner", 480, "America/Denver", datetime(2026, 8, 19)),
        )
        for preference in invalid:
            with self.subTest(preference=preference), self.assertRaises(ValueError):
                preference.validated()

    def test_complete_snapshot_is_sorted_and_commits_one_transaction(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.rowcount = 2

        result = store.reconcile_complete_enabled_snapshot(
            [_preference("989262"), _preference("961825")],
            connect=lambda: conn,
        )

        self.assertEqual(cursor.execute.call_count, 3)
        first_params = cursor.execute.call_args_list[0].args[1]
        second_params = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(first_params[0], "961825")
        self.assertEqual(second_params[0], "989262")
        disable_sql, disable_params = cursor.execute.call_args_list[2].args
        self.assertIn("NOT (route_number = ANY(%s))", disable_sql)
        self.assertEqual(disable_params, (["961825", "989262"],))
        conn.commit.assert_called_once_with()
        conn.rollback.assert_not_called()
        conn.close.assert_called_once_with()
        self.assertEqual(result, {"enabled": 2, "disabled": 2})

    def test_duplicate_route_is_rejected_before_database_connection(self):
        connect = MagicMock()
        with self.assertRaisesRegex(ValueError, "duplicate authoritative route"):
            store.reconcile_complete_enabled_snapshot(
                [_preference("989262"), _preference("989262")],
                connect=connect,
            )
        connect.assert_not_called()

    def test_snapshot_failure_rolls_back_without_a_disable_commit(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, RuntimeError("write failed")]

        with self.assertRaisesRegex(RuntimeError, "write failed"):
            store.reconcile_complete_enabled_snapshot(
                [_preference("961825"), _preference("989262")],
                connect=lambda: conn,
            )

        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()
        conn.close.assert_called_once_with()

    def test_upsert_uses_bound_parameters(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        preference = _preference()

        store.upsert_enabled_preference(preference, connect=lambda: conn)

        sql, params = cursor.execute.call_args.args
        self.assertIn("VALUES (%s, %s, TRUE, %s, %s, %s", sql)
        self.assertNotIn(preference.owner_uid, sql)
        self.assertEqual(params[0:2], (preference.route_number, preference.owner_uid))
        conn.commit.assert_called_once_with()

    def test_unchanged_snapshot_preserves_due_progress(self):
        sql = store.UPSERT_ENABLED_PREFERENCE_SQL
        self.assertIn(
            "ELSE low_qty_notification_preferences.next_due_at",
            sql,
        )
        version_case = sql.split("preference_version = CASE", 1)[1]
        self.assertNotIn("next_due_at IS DISTINCT FROM", version_case)

    def test_claim_scoped_disable_cannot_overwrite_a_newer_preference(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0

        changed = store.disable_claimed_preference(
            _claim(),
            "owner_not_eligible",
            connect=lambda: conn,
        )

        self.assertFalse(changed)
        sql, params = cursor.execute.call_args.args
        self.assertIn("preference_version = %s", sql)
        self.assertIn("next_due_at = %s", sql)
        self.assertEqual(params[1:], ("989262", 4, _claim().scheduled_for_utc))
        conn.commit.assert_called_once_with()

    def test_claim_selects_and_inserts_in_one_skip_locked_transaction(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [_preference_row(), _execution_row()]

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 4, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertIsNotNone(claim)
        self.assertEqual(claim.route_number, "989262")
        self.assertEqual(claim.claimed_preference_version, 4)
        select_sql = cursor.execute.call_args_list[0].args[0]
        self.assertIn("FOR UPDATE SKIP LOCKED", select_sql)
        insert_sql = cursor.execute.call_args_list[1].args[0]
        self.assertIn("ON CONFLICT (route_number, scheduled_local_date) DO NOTHING", insert_sql)
        conn.commit.assert_called_once_with()
        conn.rollback.assert_not_called()

    def test_claim_returns_none_without_due_row(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 4, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertIsNone(claim)
        conn.rollback.assert_called_once_with()
        conn.commit.assert_not_called()

    def test_stale_processing_claim_is_recovered_with_new_token(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        existing = _execution_row(
            lease_expires_at=datetime(2026, 8, 18, 14, 3, tzinfo=timezone.utc),
        )
        reclaimed = _execution_row(
            claim_token="replacement-token",
            attempt_count=2,
            claimed_at=datetime(2026, 8, 18, 14, 6, tzinfo=timezone.utc),
            lease_expires_at=datetime(2026, 8, 18, 14, 11, tzinfo=timezone.utc),
        )
        cursor.fetchone.side_effect = [_preference_row(), None, existing, reclaimed]

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 6, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertEqual(claim.claim_token, "replacement-token")
        self.assertEqual(claim.attempt_count, 2)
        reclaim_sql = cursor.execute.call_args_list[3].args[0]
        self.assertIn("status IN ('processing', 'retryable')", reclaim_sql)
        conn.commit.assert_called_once_with()

    def test_expired_new_slot_closes_without_returning_claim(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        expired_execution = _execution_row(
            status="closed",
            completion_reason="window_expired",
        )
        cursor.fetchone.side_effect = [_preference_row(), expired_execution, None]
        cursor.rowcount = 1

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 21, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertIsNone(claim)
        insert_params = cursor.execute.call_args_list[1].args[1]
        self.assertEqual(insert_params[5], "closed")
        self.assertEqual(insert_params[9], "window_expired")
        self.assertIn("preference_version = %s", cursor.execute.call_args_list[2].args[0])
        conn.commit.assert_called_once_with()

    def test_stale_dispatch_is_closed_unknown_and_never_reclaimed(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        existing = _execution_row(
            status="dispatching",
            lease_expires_at=datetime(2026, 8, 18, 14, 3, tzinfo=timezone.utc),
        )
        cursor.fetchone.side_effect = [_preference_row(), None, existing, None]
        cursor.rowcount = 1

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 6, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertIsNone(claim)
        close_params = cursor.execute.call_args_list[3].args[1]
        self.assertEqual(close_params[0], "delivery_unknown")
        conn.commit.assert_called_once_with()

    def test_attempt_limit_closes_and_advances_without_returning_work(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        exhausted = _execution_row(
            status="retryable",
            attempt_count=3,
            lease_expires_at=datetime(2026, 8, 18, 14, 3, tzinfo=timezone.utc),
        )
        cursor.fetchone.side_effect = [_preference_row(), None, exhausted, None]

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 14, 6, tzinfo=timezone.utc),
            max_attempts=3,
            connect=lambda: conn,
        )

        self.assertIsNone(claim)
        self.assertIn("attempts_exhausted", str(cursor.execute.call_args_list))
        self.assertGreaterEqual(conn.commit.call_count, 1)

    def test_completed_local_date_blocks_same_day_schedule_change(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        already_sent = _execution_row(
            status="sent",
            completed_at=datetime(2026, 8, 18, 14, 1, tzinfo=timezone.utc),
            completion_reason="accepted",
        )
        cursor.fetchone.side_effect = [_preference_row(), None, already_sent, None]

        claim = store.claim_next_due(
            now_utc=datetime(2026, 8, 18, 16, 0, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertIsNone(claim)
        conflict_lookup = cursor.execute.call_args_list[2].args[0]
        self.assertIn("scheduled_local_date = %s", conflict_lookup)
        self.assertTrue(
            any("SET next_due_at" in item.args[0] for item in cursor.execute.call_args_list)
        )

    def test_sent_completion_and_preference_cas_share_transaction(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = _execution_row(status="sent")
        cursor.rowcount = 1

        completed = store.complete_claim(
            _claim(),
            status="sent",
            reason="accepted",
            accepted_ticket_ids=["ticket-2", "ticket-1"],
            now_utc=datetime(2026, 8, 18, 14, 5, tzinfo=timezone.utc),
            connect=lambda: conn,
        )

        self.assertTrue(completed)
        self.assertEqual(cursor.execute.call_count, 2)
        terminal_sql, terminal_params = cursor.execute.call_args_list[0].args
        self.assertIn("claim_token = %s", terminal_sql)
        self.assertEqual(terminal_params[0:3], ("sent", terminal_params[1], "accepted"))
        cas_sql = cursor.execute.call_args_list[1].args[0]
        self.assertIn("preference_version = %s", cas_sql)
        self.assertIn("next_due_at = %s", cas_sql)
        conn.commit.assert_called_once_with()

    def test_sent_completion_requires_ticket_evidence(self):
        with self.assertRaisesRegex(ValueError, "accepted ticket"):
            store.complete_claim(
                _claim(),
                status="sent",
                reason="accepted",
                now_utc=datetime(2026, 8, 18, 14, 5, tzinfo=timezone.utc),
            )


if __name__ == "__main__":
    unittest.main()
