from unittest.mock import MagicMock, patch

import forecast_generation_worker as worker


def test_drain_once_bounds_work_and_keeps_processing_after_route_error() -> None:
    client = object()
    with patch.object(
        worker, "list_queued_generation_routes", return_value=["bad", "988200"]
    ) as list_routes, patch.object(
        worker,
        "process_generation_jobs_for_route",
        side_effect=[RuntimeError("postgres busy"), {"claimed": 1, "done": 1}],
    ) as process:
        succeeded = worker.drain_once(
            client,
            "worker-1",
            route_limit=5,
            max_jobs_per_route=2,
            sa_path="/service-account.json",
        )

    assert not succeeded
    list_routes.assert_called_once_with(limit=5)
    assert process.call_count == 2
    assert process.call_args_list[-1].args[:3] == (client, "988200", "worker-1")
    assert process.call_args_list[-1].kwargs == {
        "max_jobs": 2,
        "sa_path": "/service-account.json",
    }


def test_drain_once_survives_queue_connection_exhaustion() -> None:
    with patch.object(
        worker,
        "list_queued_generation_routes",
        side_effect=RuntimeError("remaining connection slots are reserved"),
    ), patch.object(worker, "process_generation_jobs_for_route", MagicMock()) as process:
        succeeded = worker.drain_once(
            object(),
            "worker-1",
            route_limit=20,
            max_jobs_per_route=2,
            sa_path=None,
        )

    assert not succeeded
    process.assert_not_called()
