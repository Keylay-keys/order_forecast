# Order Forecast - Integration Testing & Deployment

> **Status**: Testing Phase - IN PROGRESS
> **Last Updated**: December 11, 2025 (Claude 2 tests passing)
> **Created**: December 11, 2025
> **Previous Phase**: [docs/archive/ALGORITHM_PLAN.md](docs/archive/ALGORITHM_PLAN.md) - ✅ Complete

---

## Overview

All foundational code is complete. This phase focuses on:
1. Integration testing the full pipeline
2. Replacing scaffold code with production implementations
3. Setting up deployment (listeners on Mac)
4. End-to-end verification

---

## Task Assignments

### ChatGPT - ML Pipeline Testing & Production Code

| # | Task | Description | Priority |
|---|------|-------------|----------|
| 1 | Replace scaffold prediction | `forecast_engine.py` uses lag1 passthrough - replace with trained model inference | HIGH |
| 2 | Test training pipeline | Run `training_pipeline.py` with real data, verify output | HIGH |
| 3 | Inject correction features | Connect `feedback_collector.py` output to `forecast_engine.py` | MEDIUM |
| 4 | Test model validator | Verify gating logic works (reject bad models) | MEDIUM |
| 5 | Document CLI usage | Create usage examples for all scripts | LOW |

**Test Commands**:
```bash
# Test training pipeline
cd order_forecast
source venv/bin/activate
python scripts/training_pipeline.py \
  --orders "Mission Order Form - 12_15.csv" \
  --stock data/storeStock.csv \
  --maeThreshold 5.0 \
  --rmseThreshold 8.0

# Test forecast generation
python scripts/run_forecast.py \
  --route 989262 \
  --deliveryDate 2025-12-18 \
  --serviceAccount /path/to/sa.json
```

---

### Claude 1 - App Integration Testing

| # | Task | Description | Priority |
|---|------|-------------|----------|
| 1 | Test archive request flow | Verify `OrderArchiveService.ts` sends correct requests | HIGH |
| 2 | Test correction submission | Verify `ForecastManager.ts` submits corrections on finalize | HIGH |
| 3 | Verify UI loading states | Test spinners appear during archive requests | MEDIUM |
| 4 | Test date picker | Verify `ArchiveDatePicker.tsx` works with real data | MEDIUM |
| 5 | Error handling | Test what happens when backend is offline | LOW |

**Test Scenarios**:
1. Open Order History → Verify recent orders load from Firebase
2. Tap "Load Older Orders" → Verify request written to `orderRequests/{id}`
3. Select archived date → Verify full order loads
4. Finalize order with changes → Verify corrections submitted

---

### Claude 2 - Database & Backend Testing

| # | Task | Description | Priority |
|---|------|-------------|----------|
| 1 | Test archive listener | Run `order_archive_listener.py`, verify it responds to requests | HIGH |
| 2 | Test Firebase sync | Run `db_sync.py` with real Firebase credentials | HIGH |
| 3 | Verify data integrity | Compare DuckDB data with Firebase source | MEDIUM |
| 4 | Test incremental sync | Verify `--incremental` only pulls new orders | MEDIUM |
| 5 | Performance testing | Benchmark queries with larger datasets | LOW |

**Test Commands**:
```bash
# Start archive listener (run in background)
cd order_forecast
source venv/bin/activate
python scripts/order_archive_listener.py \
  --duckdb data/analytics.duckdb \
  --serviceAccount /path/to/sa.json \
  --poll 3

# Run integration tests (in separate terminal)
python scripts/test_archive_request.py \
  --serviceAccount /path/to/sa.json \
  --route 989262

# Verify data integrity
python scripts/test_db_queries.py
```

---

## Integration Test Plan

### Phase 1: Individual Component Tests

```
□ ChatGPT: Training pipeline produces valid model
□ ChatGPT: Forecast engine generates predictions
✅ Claude 2: Archive listener responds to requests (2025-12-11)
✅ Claude 2: Firebase sync populates DuckDB correctly (JSON load verified)
□ Claude 1: Archive service sends correct request format
□ Claude 1: Correction submission works
```

### Phase 2: End-to-End Flow Tests

```
FLOW 1: Order Archive Access
─────────────────────────────
□ 1. Claude 1: User taps "Load Older Orders" in app
✅ 2. Claude 2: Listener picks up request from Firebase
✅ 3. Claude 2: Listener queries DuckDB (13 dates found)
✅ 4. Claude 2: Listener writes response to Firebase
□ 5. Claude 1: App displays archived dates
✅ 6. Claude 2: Full order loads (6 stores, 30+ items)

FLOW 2: Forecast Generation
───────────────────────────
□ 1. ChatGPT: Training pipeline runs with historical data
□ 2. ChatGPT: Model saved to models/{route}/{schedule}/
□ 3. ChatGPT: Forecast engine loads model
□ 4. ChatGPT: Forecast generated for target date
□ 5. ChatGPT: Forecast cached in Firebase
□ 6. Claude 1: App loads cached forecast (instant)

FLOW 3: Feedback Loop
─────────────────────
□ 1. Claude 1: User edits forecasted quantities
□ 2. Claude 1: User finalizes order
□ 3. Claude 1: Corrections submitted to Firebase
□ 4. Claude 2: Corrections synced to DuckDB
□ 5. ChatGPT: Next training run includes corrections
□ 6. ChatGPT: Model improves based on user feedback
```

### Phase 3: Deployment Setup

```
□ Set up Mac to run listeners on startup
□ Configure service account credentials
□ Set up logging/monitoring
□ Create startup scripts
□ Document operational procedures
```

---

## Deployment Checklist

### Prerequisites

- [ ] Firebase service account JSON available
- [ ] DuckDB populated with historical data
- [ ] Python venv with all dependencies installed
- [ ] App deployed to device/TestFlight

### Listener Startup Script

```bash
#!/bin/bash
# start_listeners.sh

cd /Users/kylemacmini/Desktop/routespark/restore-2025-09-24/order_forecast
source venv/bin/activate

# Start archive listener in background
python scripts/order_archive_listener.py \
  --db data/analytics.duckdb \
  --serviceAccount /path/to/sa.json \
  --poll 5 &

echo "Listeners started. PIDs: $!"
```

### Cron Jobs (Optional)

```bash
# Daily sync from Firebase (at 2 AM)
0 2 * * * cd /path/to/order_forecast && source venv/bin/activate && python scripts/db_sync.py --route 989262 --incremental --days 7

# Weekly full retrain (Sunday at 3 AM)
0 3 * * 0 cd /path/to/order_forecast && source venv/bin/activate && python scripts/training_pipeline.py --orders ... --stock ...
```

---

## Known Issues / TODOs

| Issue | Owner | Status |
|-------|-------|--------|
| `forecast_engine.py` uses lag1 passthrough, not trained model | ChatGPT | TODO |
| Correction features not yet injected into forecast engine | ChatGPT | TODO |
| ~~`order_archive_listener.py` writes to `orderResponses` not `orderRequests`~~ | ChatGPT | ✅ FIXED |
| ~~Need to add `google-cloud-firestore` to venv~~ | All | ✅ INSTALLED |
| `order_archive_listener.py` field mismatch (`mode` vs `type`) | Claude 2 | ✅ FIXED |

## Test Results Log

### Claude 2 - December 11, 2025

**Test: Archive Listener (order_archive_listener.py)**
```
✅ list_dates: Returned 13 dates with summaries
   - Dates: 2025-12-15, 2025-12-11, 2025-12-08, etc.
   - Format: { date, scheduleKey, totalCases, storeCount }

✅ get_order: Returned full order data
   - Order ID: order-989262-1765782000000
   - Schedule: monday
   - Stores: 6 (Bowman's first with 30 items)
   - Format matches OrderArchiveService.ts expectations
```

**Fixes Applied:**
1. Updated listener to use `type` field (not `mode`) to match app
2. Updated response format to match `ArchivedDateSummary` interface
3. Added `search_orders` support
4. Added status filter - only processes `pending` requests

---

## Success Criteria

| Metric | Target | How to Verify |
|--------|--------|---------------|
| Archive request latency | < 3 seconds | Time from tap to results |
| Forecast cache hit rate | > 95% | App logs |
| Prediction accuracy (MAE) | < 5 units | Model validator output |
| Sync reliability | 100% | No missed orders in DuckDB |
| Correction capture rate | 100% | All finalized orders have corrections |

---

## Communication

When reporting test results, use this format:

```
## Test Report: [Component Name]
**Agent**: [ChatGPT/Claude 1/Claude 2]
**Date**: YYYY-MM-DD

### Tests Run
- [ ] Test 1: PASS/FAIL
- [ ] Test 2: PASS/FAIL

### Issues Found
- Issue description

### Notes
- Any additional observations
```

---

## Next Steps After Testing

1. **Production deployment** - Move listeners to always-on server
2. **Monitoring** - Set up alerts for listener failures
3. **Scaling** - Support multiple routes
4. **Analytics** - Dashboard for prediction accuracy over time
