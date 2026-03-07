# Forecast Tuning Harness

Purpose:
- tweak `scripts/forecast_engine.py`
- run one exact historical comparison repeatedly
- verify changes locally before any production deploy

This harness reuses the existing walk-forward logic. It does not create or push Firebase forecasts.

## Target Delivery

Current anchor case:

- route: `989262`
- order written: `2026-03-02` (Monday)
- delivery date: `2026-03-05` (Thursday)
- schedule key in the system: `monday`

## Requirements

The script reads PostgreSQL directly.

Set the same DB env vars used by the normal forecast scripts:

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=15432
export POSTGRES_DB=routespark
export POSTGRES_USER=routespark
export POSTGRES_PASSWORD='...'
```

If running from the Mac mini against the Linux server DB, use an SSH tunnel first:

```bash
ssh -f -N -L 15432:127.0.0.1:5432 keylay@routespark
```

## Run

From `order_forecast/`:

For the current anchor case, use `schedule_only` first. That matches the walk-forward fold we inspected for `2026-03-05` and keeps tuning focused on the over-forecast problem.

```bash
venv/bin/python scripts/forecast_tuning_harness.py \
  --route 989262 \
  --deliveryDate 2026-03-05 \
  --modeProfile schedule_only
```

Optional training-scope overrides:

```bash
venv/bin/python scripts/forecast_tuning_harness.py \
  --route 989262 \
  --deliveryDate 2026-03-05 \
  --modeProfile store_all_cycles
```

## Output

Artifacts are written under:

```text
logs/forecast_tuning/<route>/<deliveryDate>/<timestamp>/
```

Files:

- `summary.json`
- `compare.csv`

`summary.json` includes:

- actual vs forecast total units
- order total error
- SAP WAPE
- slow-mover error metrics
- top over-forecast lines
- top zero-actual over-forecasts

`compare.csv` includes one row per SAP with:

- `actual_units`
- `forecast_units`
- `delta_units`
- `case_pack`
- `direction`
- `zero_actual_overforecast`

## Tuning Workflow

1. Run the harness on the anchor case.
2. Change `scripts/forecast_engine.py`.
3. Run the harness again.
4. Compare:
   - `order_total_abs_error_model`
   - `order_total_wape_model`
   - `sap_wape_model`
   - `segment_slow_over_rate_model`
   - `top_zero_actual_over_forecasts`
5. Keep changes only if they reduce over-forecast without causing obvious regression.

## Priority

For this route, under-forecast is less urgent than over-forecast.

Primary target metrics:

1. reduce `order_total_units_model - order_total_units_actual`
2. reduce zero-actual over-forecasts
3. reduce slow-mover over-rate

Do not treat a lower MAE as success if the zero-actual over-forecast set gets worse.
