from pathlib import Path

import runtime_supervisor


def test_order_forecast_runtime_runs_generation_worker() -> None:
    specs = runtime_supervisor.make_specs("/runtime/python")["order_forecast"]
    by_key = {spec.key: spec for spec in specs}

    assert "forecast_generation" in by_key
    worker = by_key["forecast_generation"]
    assert worker.name == "Forecast Generation Worker"
    assert worker.cmd == [
        "/runtime/python",
        str(Path(runtime_supervisor.SCRIPTS_DIR) / "forecast_generation_worker.py"),
        "--serviceAccount",
        runtime_supervisor.SA_PATH,
    ]
    assert worker.log_name == "forecast_generation_worker.log"
