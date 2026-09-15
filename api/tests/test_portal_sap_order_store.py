from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from order_forecast.api.portal_sap_order_store import (
    PortalSapOrderConflict,
    read_portal_sap_order,
    save_portal_sap_order,
)


def connection_with_rows(*rows):
    cursor = MagicMock()
    cursor.__enter__.return_value = cursor
    cursor.fetchone.side_effect = list(rows)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


def test_absent_read_returns_no_row_without_writing():
    conn, cursor = connection_with_rows(None)
    released = []

    result = read_portal_sap_order("uid-1", "988200", acquire=lambda: conn, release=released.append)

    assert result.exists is False
    assert result.preference is None
    assert cursor.execute.call_args.args[1] == ("uid-1", "988200")
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()
    assert released == [conn]


def test_atomic_first_insert_returns_committed_state():
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    conn, cursor = connection_with_rows((["200", "100"], 1, now))

    result = save_portal_sap_order(
        "uid-1", "988200", ["200", "100"], 0,
        acquire=lambda: conn, release=lambda _conn: None,
    )

    assert result.sap_order == ["200", "100"]
    assert result.revision == 1
    assert "ON CONFLICT" in cursor.execute.call_args.args[0]
    assert cursor.execute.call_args.args[1][0:2] == ("uid-1", "988200")
    conn.commit.assert_called_once()


def test_update_and_reset_increment_exactly_once():
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    conn, cursor = connection_with_rows(([], 8, now))

    result = save_portal_sap_order(
        "uid-1", "988200", [], 7,
        acquire=lambda: conn, release=lambda _conn: None,
    )

    assert result.sap_order == []
    assert result.revision == 8
    statement, parameters = cursor.execute.call_args.args
    assert "revision = revision + 1" in statement
    assert parameters[1:] == ("uid-1", "988200", 7)
    conn.commit.assert_called_once()


def test_stale_revision_returns_bounded_current_state():
    now = datetime(2026, 9, 2, tzinfo=timezone.utc)
    conn, _cursor = connection_with_rows(None, (["300"], 4, now))

    with pytest.raises(PortalSapOrderConflict) as raised:
        save_portal_sap_order(
            "uid-1", "988200", ["100"], 3,
            acquire=lambda: conn, release=lambda _conn: None,
        )

    assert raised.value.current.sap_order == ["300"]
    assert raised.value.current.revision == 4
    conn.commit.assert_not_called()
    conn.rollback.assert_called_once()


def test_database_failure_rolls_back_and_releases_connection():
    conn, cursor = connection_with_rows()
    cursor.execute.side_effect = RuntimeError("database unavailable")
    released = []

    with pytest.raises(RuntimeError, match="database unavailable"):
        read_portal_sap_order(
            "uid-1", "988200", acquire=lambda: conn, release=released.append,
        )

    conn.rollback.assert_called_once()
    assert released == [conn]
