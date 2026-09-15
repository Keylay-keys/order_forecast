# Audited Cross-User Backtest — 2026-09-02

This directory preserves the first cross-user run made after correcting the
walk-forward harness's held-out-correction cutoff, last-order baseline,
zero-demand WAPE handling, and bounded-fold ordering.

## Status

**Exploratory evidence only. Do not use this run as a production promotion
gate.** The runner still calls the model core rather than replaying every
production post-processing step, and historical catalog, schedule, and carry
state are not yet versioned as-of each fold.

## Runtime and command

- Data read: live RouteSpark PostgreSQL history, read-only
- Run timestamp: `2026-09-02T19:24:54Z`
- Forecast runtime: deployed `order-forecast` image
  `promo-disabled-20260825-01`
- Backtester: local audited
  [`walk_forward_backtest.py`](../../../scripts/walk_forward_backtest.py),
  copied to the pod for this read-only run
- Backtester SHA-256:
  `3ced004d4c219244dff40274f159dd41d1503e2c353c097b61876a2735c86853`
- `order_forecast` base commit: `582372750ae49e0fe294765bacce7eaaa18d3ebd`
- Band calibration: disabled to avoid using current persisted calibration in
  historical folds

```bash
python scripts/walk_forward_backtest.py \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --outputDir logs/backtests
```

## Artifacts

- [`folds.csv`](folds.csv): one row per route/schedule/test delivery
- [`scorecard.csv`](scorecard.csv): route/schedule aggregate metrics
- [`sources.csv`](sources.csv): model-branch breakdown by fold
- [`manifest.json`](manifest.json): command, runtime identity, checksums, census,
  and known limitations
- [`route_961767_forecast_gate.csv`](route_961767_forecast_gate.csv):
  per-schedule order/correction gate census
- [`route_989262_recent_repeat_sequence.csv`](route_989262_recent_repeat_sequence.csv):
  removed recommendations observed again on the next same-schedule order
- [`route_989262_rejection_cooldown_counterfactual.csv`](route_989262_rejection_cooldown_counterfactual.csv):
  selected-cohort zero-cooldown error comparison

Checksums:

```text
b32cccc4dd1ef6b7c0fdf80899943aee09bc696bec96d2c13f0148ad820edf76  folds.csv
7cde18b2162bad380dbf367d364524b9a61bc6c9a7cd4bee4ed90275b0764d81  scorecard.csv
d9576c44cbe9d81d12486898c2bd5729b69cd2b52ccb605d1d1111304c04443a  sources.csv
a7310ab36a95f38b7fcfe9f8998ae3b39463e831d021e74edaa6cc3fdbd3d905  route_961767_forecast_gate.csv
e1fc6f9eee9c3b56b98c55791efa822e033693e8a67d032f546bcc3a53665b29  route_989262_recent_repeat_sequence.csv
7fc0447dab4d1b0fdde13d00337b58df15f4099386cca5f7ff0106ac9b38c7d7  route_989262_rejection_cooldown_counterfactual.csv
```

## Dataset census

The database contained 159 finalized orders across seven routes. Five routes
had at least one evaluable fold after requiring one prior same-schedule order.
The run produced 134 folds from `2025-11-06` through `2026-09-09`.

| Route | Orders | Corrected orders | Evaluated folds | Mode observed |
|---|---:|---:|---:|---|
| 900000 | 1 | 0 | 0 | insufficient history |
| 961392 | 1 | 0 | 0 | insufficient history |
| 961767 | 39 | 0 | 34 | copy last order |
| 988200 | 7 | 1 | 1 | copy last order |
| 988270 | 19 | 3 | 13 | copy last order |
| 989262 | 86 | 29 | 83 | 26 copy-last; 57 store-centric model |
| 989567 | 6 | 0 | 3 | copy last order |

## Headline result

Across all 134 folds, micro total-order WAPE was `0.2945` for the current
engine and `0.2654` for the exact prior-same-schedule baseline. The engine's
aggregate signed bias was `+28,037` units, versus `-447` for the baseline.

That difference comes from route `989262`; every evaluated fold on the other
routes remained in production's copy-last cold-start mode. On the 57
`989262` folds that actually used the model:

| Metric | Current model | Exact prior order |
|---|---:|---:|
| Micro line WAPE | 0.5123 | 0.5663 |
| Micro SAP WAPE after case rounding | 0.4577 | 0.4389 |
| Micro total-order WAPE | 0.2192 | 0.1518 |
| Aggregate signed unit bias | +28,857 | +373 |

The model improves store/SAP line error but loses after SAP aggregation and
order-total scoring, with a strong over-forecast bias. This supports testing a
two-stage occurrence/quantity model and an explicit total/case decision layer;
it does not yet prove that either candidate will generalize to other routes.
