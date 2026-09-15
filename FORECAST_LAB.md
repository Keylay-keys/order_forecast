# Forecast Engine Test Lab

The canonical forecast test-data and run-artifact location is:

`/Volumes/Extreme SSD/routespark-harnesses/forecast-engine`

From the repository, `.forecast-engine-lab` is an ignored convenience pointer
to that directory. The lab keeps production-derived data off the repository.

## Why this exists

Normal walk-forward runs must not query the cluster repeatedly. A refresh makes
one read-only, repeatable-read snapshot from PostgreSQL through the existing
`routespark-forecast/order-forecast` pod. Every later run restores that immutable
snapshot into an isolated local PostgreSQL 16 instance.

The SSD itself is exFAT, which cannot safely host a PostgreSQL data directory.
The lab therefore contains an APFS sparse image for disposable PostgreSQL data.
The server listens only on a private Unix socket, never TCP. Snapshot archives,
manifests, and all result files remain in the forecast-engine directory.

## Requirements

- macOS with `/Volumes/Extreme SSD` mounted.
- Homebrew PostgreSQL 16 at `/opt/homebrew/opt/postgresql@16/bin`.
- The existing `order_forecast/venv` Python environment.
- `hdiutil` and `diskutil` for the private APFS database image.
- `kubectl` access to the current RouteSpark cluster only when using `refresh`.

No PostgreSQL password, SSH tunnel, Docker container, or direct database
connection is needed for normal lab runs.

## First-time initialization

The current workstation is already initialized. If the lab is rebuilt on the
verified SSD, run this once from the repository root:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py init
```

The default sparse-image maximum is 12 GB and the file grows only as space is
used. To choose a different maximum during initial creation:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py \
  init --imageSize 20g
```

`init` is idempotent after successful setup. It refuses to adopt an unmanaged
database image or a drive with the wrong volume UUID.

## Everyday commands

Run from the repository root:

```bash
# Verify local tools, drive identity, database image, and CURRENT snapshot.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py doctor

# Run against CURRENT without contacting Kubernetes/PostgreSQL production.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run -- \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration

# Focus on one or more routes while still using the same local snapshot.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run -- \
  --routes 989262,961767 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration
```

The lab supplies `--asOfDate` from the snapshot manifest unless it is provided
explicitly. This freezes the lookback window, so rerunning the same snapshot
does not change merely because the wall clock advanced.

Useful walk-forward arguments after the `--` separator:

- `--routes 989262,961767`: test only those routes; omit for every route in the
  snapshot.
- `--scheduleKeys monday,thursday`: test selected schedule keys.
- `--maxFolds 5`: use the five newest eligible folds per route/schedule; `0`
  means all folds.
- `--sinceDays 365`: set the source-history window.
- `--minTrainOrders 1`: set the number of prior same-schedule orders required.
- `--ignoreBandCalibration`: prevent present-day calibration from leaking into
  historical folds; this is recommended until past-only calibration exists.
- `--disableTemporalCorrections`: diagnostic only; allows non-temporal
  correction aggregation and should not be used as promotion evidence.
- `--disableStoreCentricContext`: force same-schedule training history.
- `--testContinuousActualLearning`: **test harness only**; allow finalized
  same-schedule order quantities to qualify the learned model without requiring
  forecast-button correction rows. The finalized-order history threshold and
  schedule-safety gates remain. Corrections are still optional features when
  available.
- `--testOneCycleSlowMoverCooldown`: **test harness only**; enable the R1
  candidate that zeroes a positive slow/intermittent line for one same-schedule
  cycle when it was absent from the immediately prior finalized actual order or
  explicitly removed by an eligible past-only correction. It is off by default
  and does not change `forecast_engine.py`.
- `--testStrictOneCycleSlowMoverCooldown`: **test harness only**; enable the R2
  refinement of R1. It uses the same past-only prior-actual/removal signal but
  limits cooldown eligibility to the engine's true slow-mover flag (25% order
  frequency or less), rather than the full intermittent branch (currently up
  to 70%). R1 and R2 are mutually exclusive.
- `--testTransientSpikeQuarantine`: **test harness only**; enable the rejected
  P1 control. It caps an extreme training observation at a past-only robust
  ceiling after four same-schedule store/SAP observations. Finalized actuals
  remain unchanged for scoring. Do not combine P1 with P2.
- `--testInferredTransientLift`: **test harness only**; enable the rejected P2
  control. It starts a separate lift regime after one extreme finalized user
  quantity, protects the normal baseline, applies a bounded lift suggestion,
  and ends the regime on a near-normal or absent user quantity. P2 is evidence
  that a single trigger is too noisy; it is not the current lab champion.
- `--testHierarchicalHurdleV1`: **test harness only**; enable rejected H1. It
  replaces the old positive-line regressor with a same-schedule dense
  occurrence classifier plus conditional-quantity model.
- `--testStoreSapReconciledV1`: **test harness only**; enable rejected H2. It
  forecasts rolling store/SAP weekly totals and allocates them to a schedule,
  but does not constrain the sum to a store-order total.
- `--testStoreFirstHierarchyV1`: **test harness only**; enable rejected H3. It
  adds a store-total constraint but leaves case rounding until scoring, which
  materially inflates the emitted order.
- `--testStoreFirstCaseHierarchyV1`: **test harness only**; enable H4, the
  current leading architectural candidate. It forecasts store totals, derives
  the store/SAP/cycle mix, chooses one whole-case total per SAP, then allocates
  those fixed units among stores. H1-H4 are mutually exclusive and cannot be
  combined with R1/R2/P1/P2.
- `--testSettlementStoreShareV1`: **test harness only**; enable rejected S1.
  S1 starts with H4 and blends past-available SBT `Sale` shares into the split
  between Smith's `0135` and `0277`, preserving combined units and SAP cases.
  It requires `--settlementTruthCsv`. The current file uses simulated Monday
  availability and is workflow-counterfactual evidence, not historical app
  availability. H1-H4 and S1 are mutually exclusive.
- `--settlementTruthCsv /absolute/path.csv`: normalized daily settlement facts
  for S1. The manifest records the input path and SHA-256.
- `--asOfDate YYYY-MM-DD`: override the snapshot date intentionally. Normally
  let the lab supply it.

Do not pass `--outputDir`. The lab owns that argument so test artifacts cannot
silently escape the external-drive run directory.

### Run the R1 slow-mover experiment

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testOneCycleSlowMoverCooldown
```

Score every held-out fold against the user's finalized order quantities whether
or not the user used the forecast. Corrections are supplemental past-only
signals; they do not replace finalized actuals and are not required for an
order to be scored. For a clean comparison, run the same command without the
experimental flag and confirm both manifests use the same snapshot ID, as-of
date, arguments, and fold keys.

### Run continuous finalized-order learning

```bash
# A1 only: measure the effect of removing the correction-count gate.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testContinuousActualLearning

# Combine A1 with the existing R1 no-demand guard.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 961767,989262 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testContinuousActualLearning \
  --testOneCycleSlowMoverCooldown
```

### Run the strict true-slow refinement

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 961767,989262 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testContinuousActualLearning \
  --testStrictOneCycleSlowMoverCooldown
```

This is a matched R2 comparison against A1+R1. It changes only which lines are
eligible for the same cooldown; it does not change the model, training target,
or production forecast engine.

### Run the unannounced-lift controls

```bash
# P1: quarantine an extreme observation from the normal training baseline.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testContinuousActualLearning \
  --testOneCycleSlowMoverCooldown \
  --testTransientSpikeQuarantine

# P2: additionally infer and later end a temporary lift from user quantities.
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 \
  --sinceDays 365 \
  --minTrainOrders 1 \
  --ignoreBandCalibration \
  --testContinuousActualLearning \
  --testOneCycleSlowMoverCooldown \
  --testInferredTransientLift
```

Frozen runs:

- P1: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T072607Z`
- P2: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T073124Z`

Both controls were rejected versus A1+R1. P1 made only a very small aggregate
improvement and regressed on 11 of the 12 newest order folds. P2's active-lift
overrides helped 61 held-out lines and harmed 117, worsening line and SAP
micro WAPE. Retain these flags only for reproducible diagnosis; do not use
either as the production-promotion candidate.

### Run the store-first hierarchy candidates

```bash
# H1 dense hurdle benchmark (rejected).
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 --sinceDays 365 --minTrainOrders 1 \
  --ignoreBandCalibration --testContinuousActualLearning \
  --testHierarchicalHurdleV1

# H4 store-first, case-aware hierarchy (leading lab architecture).
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 --sinceDays 365 --minTrainOrders 1 \
  --ignoreBandCalibration --testContinuousActualLearning \
  --testStoreFirstCaseHierarchyV1
```

Frozen architecture runs:

- H1: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T074149Z`
- H2: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T074637Z`
- H3: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T074840Z`
- H4: `/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T075415Z`

H4 is not production-ready. On `989262`, it improved order-total micro WAPE
from 22.55% to 21.10% and case-rounded SAP micro WAPE from 45.29% to 42.86%,
but store/SAP line micro WAPE worsened from 53.41% to 53.85%. The next work is
the constrained store-allocation layer, followed by untouched holdout and
multi-route validation.

### Reproduce the rejected S1 settlement-share experiment

First extract and reconcile the five weekly PDF statements plus the reviewed
screenshot-only SAP `28934` rows. The extractor content-addresses and excludes
the official 20-page Mission/Gruma settlement-reading guide stored in the same
folder; any other non-statement PDF still fails strict page/header validation:

```bash
order_forecast/venv/bin/python \
  order_forecast/scripts/extract_settlement_sbt_truth.py \
  --inputDir '/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/Settlements' \
  --manualDailyCsv '/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/datasets/store_truth/989262/settlement_week_2026-08-26/review/settlement_sap_28934_validated.csv' \
  --outputCsv .codex-work/settlement_sbt_daily_989262_2026-07-16_to_2026-08-26.csv \
  --reconciliationCsv .codex-work/settlement_sbt_reconciliation_989262_2026-07-22_to_2026-08-19.csv
```

Require `392/392 SAP-weeks matched all four fields`, then run S1:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot 20260902T195217Z -- \
  --routes 989262 --sinceDays 365 --minTrainOrders 1 \
  --ignoreBandCalibration --testContinuousActualLearning \
  --testSettlementStoreShareV1 \
  --settlementTruthCsv '/absolute/path/to/settlement_sbt_daily_989262_2026-07-16_to_2026-08-26.csv'
```

Frozen S1 run:
`/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/runs/20260903T205028Z`.
S1 changed eight folds but increased store/SAP line error by 16 units overall
and by 32 units on the newest fold. It is retained only as a negative control;
do not enable it for a forecast candidate.

### Per-user maturity ladder

The hierarchy must build from retained finalized orders; it is not enabled at
full strength for a new route:

- cold start: exact prior order plus future network/product priors;
- emerging: per-order store/SAP rates with strong shrinkage;
- established: rolling weekly/monthly totals and cycle allocation (H4); and
- mature: seasonal, holiday, promotion-regime, and calibrated uncertainty
  layers.

The test harness currently retains the existing eight same-schedule order gate
before replacement architectures run. A production design needs explicit,
versioned maturity criteria and must never treat missing pre-onboarding history
as zero demand.

The learning target is always `order_line_items.quantity` from past finalized
orders. `forecast_corrections` is an optional suggestion-versus-final outcome
layer; an order without an attached forecast still trains and scores normally.
Inspect `learning_target`, `training_same_schedule_finalized_orders`,
`training_correction_orders`, and
`test_continuous_actual_learning_bypassed_correction_gate` in the fold CSV to
audit each decision.

## Store-truth evidence datasets

### Settlement source archive

The canonical settlement-email archive and its use instructions are at:

```text
/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/Settlements/README.md
```

For route `989262`, normalized 2026 statements are grouped by printed week
ending under `Settlements/normalized/2026/989262/`. The adjacent CSV and JSON
manifests record the email receipt time, report timestamp, revision status,
page count, SHA-256 provenance, and the matching raw EML/PDF source paths.

The Monday statement is preliminary and the Friday statement is the preferred
final accounting truth. Preserve both: Monday is valid temporal evidence only
after it was received, while Friday supersedes it for dashboard/accounting
totals. Never classify a revision from the export filename or `_1` suffix.
Route `988200` is an excluded multi-route example and must not enter route
`989262` metrics, forecasting truth, coverage, or completeness counts.

Historical email receipt time and actual model availability are deliberately
separate. The 2026 backlog was added to the lab after those weeks had passed,
so using the old email time in a backtest is explicitly counterfactual unless
the app's original ingestion record proves otherwise. Future automated email
ingestion must set model `available_at` to the later of provider receipt and
successful system ingestion.

Scanned settlements and POS extraction trials live under
`.forecast-engine-lab/datasets/store_truth`. They are private
production-derived evidence, separate from immutable PostgreSQL snapshots.
Each document set must contain the original source pages, a content-hash and
page-identity manifest, raw OCR output, and reviewed/accepted rows.

The first settlement dataset is:

```text
.forecast-engine-lab/datasets/store_truth/989262/settlement_week_2026-08-26/
├── README.md
├── manifest.json
├── source/
├── ocr/
└── review/settlement_sap_28934_validated.csv
```

The ordinary walk-forward runner does **not** load this directory. Rejected S1
can load an explicitly named normalized CSV for a controlled experiment; no
settlement evidence is auto-discovered. Its immediate uses are
document-contract design, OCR/agent-review accuracy measurement,
reconciliation with orders, and past-only feature research. Any adapter must
use only facts whose `available_at` precedes the held-out forecast generation
time.

Gruma SBT settlement windows are Thursday through Wednesday and are delivered
to IOs the following Monday. They are not Monday-through-Sunday reports. At
Monday receipt, the immediately preceding Thursday-through-Sunday is still
unreported. Store `source_received_at` and `system_ingested_at`; effective
`available_at` is the later value. Models must represent the four-day blind gap
and must not treat Wednesday's last reported sale as Monday-current truth.

For scan-based settlements, keep `sale_units`, receipt/`hhc_in_units`, raw
`hhc_out_units`, stale/unsalable units, and store returns separate. Mission's
official 2024 Route Settlements guide defines `HHC In` as deliveries at the SBT
store, `HHC Out` as the combined **credits and/or buybacks** field, `Sales` as
store-scanned sales, and `Rets` as store-scanned returns. Therefore `HHC Out`
must not be labeled stale, and it cannot be split into buyback versus credit
without another reconciled source. The settlement's SBT delivery summary may
distinguish `Return` and `Buy Back`; preserve those explicit types. No explicit
stale reason appears in the supplied weekly reports or guide, so require
handheld detail or a truck-inventory adjustment source before labeling shrink.
Verified buybacks return to usable route/truck inventory and may be
reallocated, while verified stales are shrink.

Track these movements as an item-level inventory ledger, not independent
weekly totals. Each event needs `movement_id`, route, SAP/UPC, units, event
time, source and destination location, handheld reason/type, memo or invoice
identity, source document, `available_at`, and review status. Apply the
following state transitions only when the handheld type is explicit:

| Handheld/event type | Store inventory | Usable truck inventory | Terminal shrink |
|---|---:|---:|---:|
| `BUYBACK` | decreases at the origin store | increases | unchanged |
| later `HHC_IN`/delivery | increases at the destination store | decreases | unchanged |
| `STALE` | closes any linked store outflow | decreases or never becomes usable | increases |

A raw settlement `HHC Out` begins as unresolved combined credit/buyback
evidence. Link it first by handheld transaction/memo identity, SAP, units, and
event time. A later `HHC In` for the same SAP may be a reallocation candidate,
but quantity/date proximity alone does not prove that the same units moved;
preserve unmatched units as an open truck pool. A `STALE` event permanently
closes its linked quantity and it must never be matched to a later delivery.
The conservation check is performed per SAP across all store locations, usable
truck inventory, and terminal shrink. A buyback followed by delivery elsewhere
is one transfer chain, not two demand events; consumer `Sales` remain the
demand signal.

The same official guide says SBT stores send sales daily; those sales appear in
the detailed report as normal HHC invoices, while the SBT Weekly Movements
report provides separate store/day/product movement fields. It also defines the
statement's displayed truck inventory as SBT store inventory plus inventory the
distributor retained at week end. Do not reinterpret that displayed dollar
balance as physical truck-only inventory or add it again to New Settlement;
continue reconciling the printed statement equation field by field.
For ordinary POS invoices,
received units are replenishment evidence and must not be relabeled as
sell-through. See
[`../.codex-work/POS_SETTLEMENT_TRUTH_AUDIT_989262_2026-09-03.md`](../.codex-work/POS_SETTLEMENT_TRUTH_AUDIT_989262_2026-09-03.md)
for the extraction and deterministic-acceptance contract.

For route `989262`, the store-source map is complete but heterogeneous:
Smith's settlement rows provide item-level sell-through; Harmon's, Sam's Club,
and Walmart POS invoices provide received-product evidence; and Bowman's weekly
finalized order plus cash amount provide SAP/unit replenishment history and a
dollar-level control. Resolve invoice UPCs from a versioned snapshot of
PostgreSQL `reference_catalog_items`. The 2026-09-03 starter-catalog audit found
244/248 rows with UPC and 12 normalized UPC groups with multiple SAP aliases,
so ambiguous matches must fail closed unless exactly one effective/recent route
SAP is supported.

For invoice evidence precedence, favor reconciled machine-printed item rows and
totals over an unsupported handwritten annotation. In the reviewed Sam's
invoice `3680905044`, the lines and printed total equal 357 and the route owner
confirmed all 357 were delivered; handwritten `Total Rec'd 352` is retained as
a rejected conflict, not accepted received quantity.

The reviewed 2026-09-03 sample contains 73 printed UPC rows across Harmon's,
Sam's, and Walmart; all 73 matched the reference catalog and all resolved after
same-delivery order context. Reconciled received-versus-ordered totals were
263 versus 253 for Harmon's, 357 versus 357 for Sam's, and 934 versus 954 for
Walmart. Use verified invoice differences to update the *next* inventory state.
Never add invoice receipts to the matching finalized order as a second demand
event.

Route-owner review identified the paired SAP `32820` differences as a 10-unit
post-order transfer from Walmart to Harmon's: ordered placement 90/30 became
actual placement 80/40, with route total unchanged at 120. Record this as an
inter-store transfer/reallocation, not a shortage, sale, or demand increase.
The separate Walmart SAP `51531` -10 remains unresolved.

PCF loading/Best Before/Days evidence may be used as an optional freshness
horizon in future H5 experiments. It is positive-only evidence: some users
upload only pages containing short-coded items, and the upload manifest defines
submitted coverage rather than the complete printed packet. Missing PCFs,
pages, or SAPs are `unknown`, never zero inventory or zero freshness risk, and
must not reduce forecast eligibility, training weight, or service level. The
current accepted PCF projection also lacks dependable per-lot quantity and
store assignment, so PCF dates cannot establish exact on-hand. See
[`../.codex-work/FRESHNESS_FULLNESS_POLICY_989262_2026-09-03.md`](../.codex-work/FRESHNESS_FULLNESS_POLICY_989262_2026-09-03.md).

H5 presentation inputs must distinguish minimum acceptable shelf units, full
shelf units, backstock permission/capacity, and observed backstock on-hand.
Backstock may justify a larger load through the next delivery date, but it is
not unlimited: cap the target by shelf-plus-backstock capacity and projected
sell-through before expiry. Missing backstock observations are unknown, not
assumed empty.

A store/SAP may have multiple user-defined selling locations. Store a stable
name/type, minimum units, full units, and effective date range for each location
(for example, Walmart `Main shelf` plus a temporary `Endcap`). Sum only active
location floors/capacities. Keep backstock as one shared store/SAP pool rather
than repeating it per location. A missing or expired temporary display must not
permanently inflate normal demand or presentation requirements.

For future app work, Stores → Store → Item should own the stable selling-location
and backstock configuration. Orders may display the active location snapshot and
accept current observed counts per location, shared backstock, or a temporary
order-only override. Do not maintain two independent defaults, and do not let an
order override silently rewrite Store configuration. Keep timestamps and
provenance so historical folds can resolve the values as-of the forecast.

The H5 design note is mirrored on the external drive at:

```text
.forecast-engine-lab/notes/FRESHNESS_FULLNESS_POLICY_989262_2026-09-03.md
```

## Pointer to fresh cluster data

This is the only normal command that contacts the cluster:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py refresh
```

It streams an allowlisted set of forecast tables from the production database
inside a read-only, repeatable-read transaction. It does not mutate Kubernetes,
PostgreSQL, forecasts, orders, or calibration. Credentials remain in the pod
and are not written to disk. To intentionally make a smaller route-only
snapshot, add `--routes 989262,961767`.

A route-only refresh becomes `CURRENT` and cannot evaluate omitted routes. Use
an all-route refresh for shared Codex/Grok/Claude comparisons.

Refresh only when new finalized orders/corrections or current catalog/schedule
state are needed. `CURRENT` is then advanced to the new immutable snapshot;
older snapshots and runs are preserved. Pin an older dataset with:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py run \
  --snapshot SNAPSHOT_ID -- --sinceDays 365 --minTrainOrders 1 --ignoreBandCalibration
```

## Inspect and verify

`doctor` is local-only and does not contact Kubernetes:

```bash
order_forecast/venv/bin/python order_forecast/scripts/forecast_lab.py doctor
```

It verifies the physical drive UUID, lab configuration, APFS image, required
programs, and the current snapshot manifest. To see available immutable data
and result sets:

```bash
ls -1 .forecast-engine-lab/snapshots
ls -1 .forecast-engine-lab/runs
```

Each snapshot has a standalone `manifest.json` and archive `SHA256`. Each run
has `lab_run_manifest.json`, which identifies the exact snapshot, code hashes,
Git state, arguments, as-of date, exit status, and whether cluster access was
used. The walk-forward runner also writes its own result manifest.

## Local business-metrics prototype

The Vite portal can display reviewed settlement data directly from this
external-drive lab without creating settlement, expense, or payroll records in
Firebase. The current route `989262` fixture is:

```text
/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/datasets/business_metrics/989262/settlement_metrics_lab.json
```

It contains 34 weekly statements covering January 1–August 26, 2026: 33 Friday
finals plus the Monday preliminary for week ending March 11, whose Friday final
was not supplied. That fallback is visibly provisional in the prototype. Every
included New Settlement equation reconciles. Across the series, gross sales are
`$573,805.01`, margin credited is `$87,808.00`, and New Settlement is
`$52,414.56`; these totals include the provisional March 11 statement. The
latest Friday-final statement's stated and independently calculated New
Settlement are both `$2,049.93`. Expenses and payroll are empty because neither
has been supplied; the dashboard must treat them as unrecorded, not zero-cost
facts.

Regenerate the local fixture from the normalized, route-989262-only manifest:

```bash
order_forecast/venv/bin/python \
  order_forecast/scripts/build_settlement_metrics_lab.py \
  --manifest '/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/Settlements/normalized/2026/manifest.csv' \
  --archiveRoot '/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/Settlements' \
  --output '/Volumes/Extreme SSD/routespark-harnesses/forecast-engine/datasets/business_metrics/989262/settlement_metrics_lab.json' \
  --route 989262
```

The builder verifies route/week identity, canonical path containment, PDF
SHA-256, detailed adjustments, and the printed New Settlement equation before
atomically replacing the fixture. It selects Friday over Monday for each week
and preserves any existing local expense/payroll arrays. It cannot write to
Firebase.

The Sales Summary `Returns` column can include SBTv1 Returns that are already
netted into `SBTv1 Gross`. To reconcile printed `Net Inv`, use the settlement's
signed `Returns` adjustment with Gross Sales, Promotions, and Network
Allowances; do not subtract the displayed all-channel Returns total a second
time. Preserve the larger Sales Summary Returns value for reporting.

Firebase Tools may continue using port `3000`. Start the read-only metrics
prototype on port `5173`:

```bash
cd /Users/kylemacmini/projects/routespark/restore-2025-09-24/web-portal
npm run dev:metrics-lab
```

Then open `http://127.0.0.1:5173/metrics`. The page displays a blue **Local
metrics lab · read only** banner. Expense and workforce entry/archive controls
are disabled while this mode is active. The local endpoint rereads the JSON on
each request and responds with `Cache-Control: private, no-store`.

The Settlement view labels Mission's inventory amount as combined SBT-store
plus retained-truck inventory value, displays the Account Summary separately,
and keeps the load/sales completeness checks unknown until explicitly reviewed.
The **SBT movements** tab documents `HHC In`, combined credit/buyback `HHC Out`,
scanned `Sales`, and scanned `Rets`. Until handheld reason events are ingested,
buyback custody, cross-store links, and stale units remain visibly unresolved;
the prototype never renders missing movement evidence as zero.

To point the prototype at a different reviewed lab file or port:

```bash
VITE_METRICS_LAB_DATA='/absolute/path/to/reviewed-lab.json' \
VITE_METRICS_LAB_PORT=5174 \
npm run dev:metrics-lab
```

Lab mode is enabled only for Vite's development server. Production builds force
it off even if the environment variables are present, and the actual settlement
fixture is not bundled into `dist`.

Run the complete focused correctness suite from `order_forecast/scripts/`:

```bash
../venv/bin/python -m unittest \
  test_experimental_hierarchical_hurdle \
  test_forecast_lab \
  test_walk_forward_backtest \
  test_forecast_dense_output \
  test_forecast_contract
```

## Troubleshooting

- `external volume is not mounted`: mount the drive at exactly
  `/Volumes/Extreme SSD`, then rerun `doctor`.
- `no CURRENT snapshot`: run `refresh` once, or provide an existing snapshot
  ID with `run --snapshot`.
- `mount point is already in use`: do not start a second lab run. Check for an
  active run before detaching anything; the lab deliberately fails closed to
  protect a running PostgreSQL process.
- `cluster snapshot export failed`: confirm the current `kubectl` context and
  the `routespark-forecast/order-forecast` pod, then retry `refresh`. Existing
  snapshots remain usable.
- A failed backtest still keeps its run directory and manifests when they were
  produced. Normal cleanup stops PostgreSQL, removes only that run's disposable
  database directory, detaches the image, and compacts unused image blocks.

## Layout

```text
forecast-engine/
├── README.md
├── lab.json
├── CURRENT
├── database/
│   └── forecast-lab-postgres.sparseimage
├── snapshots/
│   └── SNAPSHOT_ID/
│       ├── forecast_snapshot.tar.gz
│       ├── manifest.json
│       └── SHA256
├── datasets/
│   ├── business_metrics/
│   │   └── ROUTE/
│   │       └── settlement_metrics_lab.json
│   └── store_truth/
│       └── ROUTE/SOURCE_PERIOD/
│           ├── manifest.json
│           ├── source/
│           ├── ocr/
│           └── review/
└── runs/
    └── RUN_ID/
        ├── lab_run_manifest.json
        ├── walk_forward_folds_*.csv
        ├── walk_forward_scorecard_*.csv
        ├── walk_forward_sources_*.csv
        └── walk_forward_manifest_*.json
```

## Data and correctness rules

- Treat the directory as private production-derived test data. Do not commit,
  upload, or casually copy snapshots.
- Never edit a snapshot directory. Refresh creates a new one.
- Compare candidates on the same snapshot ID and fold arguments.
- Preserve each run manifest with its outputs; it records the snapshot hash,
  source revision, arguments, as-of date, and whether the work touched the
  cluster.
- A snapshot fixes dataset reproducibility and cluster load. It does not fix
  the separate backtester audit blockers: production post-processing replay,
  effective-dated catalog/schedule state, expanding past-only features, dense
  zero-demand opportunities, or stale inference lags.
