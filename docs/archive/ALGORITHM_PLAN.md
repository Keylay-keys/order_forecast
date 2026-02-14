# Order Forecast Algorithm - Implementation Plan

> **Status**: Implementation Phase
> **Last Updated**: December 11, 2025
> **Pilot Route**: 989262

---

## Task Assignments (Multi-Agent Collaboration)

> **IMPORTANT**: Each agent works on their assigned tasks ONLY. Do not modify files assigned to other agents.

### ChatGPT - Python Backend & ML Pipeline

**Scope**: All Python scripts for ML training, forecasting, and Firebase sync

| Task | File | Description | Dependencies |
|------|------|-------------|--------------|
| 1. Data Classes | `scripts/models.py` | Dataclasses for Order, Store, Product, Forecast, Correction | None ✅ |
| 2. Firebase Loader | `scripts/firebase_loader.py` | Load data from Firestore (stores, catalog, orders, promos) | models.py ✅ |
| 3. Firebase Writer | `scripts/firebase_writer.py` | Write forecasts to Firestore | models.py ✅ |
| 4. Feedback Collector | `scripts/feedback_collector.py` | Pull corrections from DuckDB, compute aggregate features | db_schema.py ✅ |
| 5. Baseline Model Update | `scripts/baseline_model.py` | Add correction-derived features to existing model | feedback_collector.py ✅ |
| 6. Incremental Trainer | `scripts/incremental_trainer.py` | Warm-start retraining after each order | baseline_model.py ✅ |
| 7. Model Validator | `scripts/model_validator.py` | Gate deployments on performance check | None ✅ |
| 8. Forecast Engine | `scripts/forecast_engine.py` | Core algorithm with schedule filtering, case rounding | All above ✅ |
| 9. Training Pipeline | `scripts/training_pipeline.py` | Orchestrate full ML pipeline | All above ✅ |
| 10. CLI Runner | `scripts/run_forecast.py` | CLI entry point for running forecasts | All above ✅ |
| 11. Order Archive Listener | `scripts/order_archive_listener.py` | Message bus listener for archive requests | db_schema.py ✅ |
| **12. Case Allocator** | `scripts/case_allocator.py` | **CORE**: Allocate cases across stores by historical share | db_schema.py 🔴 |
| **13. Update Forecast Engine** | `scripts/forecast_engine.py` | **CORE**: Integrate case allocator, replace per-store rounding | case_allocator.py 🔴 |

**Status**: 🔴 CRITICAL TASKS PENDING - Case allocation is the core feature!

**PRIORITY**: Tasks 12-13 must be completed before the algorithm is usable.

**Start with**: Task 1 (models.py) - everything else depends on it

**Notes for team (models.py completed)**:
- Added core dataclasses in `scripts/models.py`: `Product`, `StoreConfig`, `OrderItem`, `StoreOrder`, `Order`, `ForecastItem`, `ForecastPayload`, `ForecastResponse`, `Correction` (with delta/ratio helpers).
- Common aliases: `ISODateStr`, `DocId`. Forecast payload matches TS schema fields (recommended_units/cases, promo flags, confidence/source).
- These classes are the shared contract for Firebase loaders/writers, DuckDB sync, training/forecast engine, and feedback collector. Please import from `scripts.models` in downstream tasks.

**Notes for team (firebase_loader.py completed)**:
- New loader in `scripts/firebase_loader.py` (read-only). Helpers:
  - `get_firestore_client(service_account_path)` for admin client.
  - `load_master_catalog(route_number)` → `List[Product]` from `masterCatalog/{route}/products`.
  - `load_store_configs(route_number)` → `List[StoreConfig]` from `routes/{route}/stores` (deliveryDays + active items).
  - `load_orders(route_number, since_days=365, schedule_keys=None, status='finalized', limit=None)` → `List[Order]` with scheduleKey/date filtering; decodes nested stores/items.
  - `load_promotions(route_number)` → raw promo dicts (if present).
- Timestamp parsing guarded; scheduleKey lowercased; expectedDeliveryDate kept as string.

**Notes for team (firebase_writer.py completed)**:
- New writer in `scripts/firebase_writer.py`:
  - `write_cached_forecast(db, route_number, forecast, ttl_days=7)` → stores to `forecasts/{route}/cached/{forecastId}` with expiresAt; writes items with recommended units/cases, promo flags, confidence/source.
  - `write_forecast_feedback(db, route_number, feedback)` → appends to `forecast_feedback/{route}/entries/{autoId}` with createdAt.
  - `get_firestore_client()` helper mirrors loader.

**Notes for team (feedback_collector.py completed)**:
- New `scripts/feedback_collector.py` (DuckDB-based):
  - `fetch_corrections(db_path, route_number, schedule_keys=None, since_days=180)` → DataFrame from `forecast_corrections`.
  - `aggregate_corrections(df)` → aggregates per (store_id, sap, schedule_key): samples, avg_delta, avg_ratio, ratio_stddev, removal_rate, promo_rate, last_seen.
  - `collect_feedback_features(...)` → convenience fetch+aggregate.
- Assumes `forecast_corrections` table from db_schema. Downstream (baseline_model increment) can join these aggregates into feature_cache.

**Notes for team (baseline_model.py updated)**:
- Added optional corrections merge and feature columns:
  - New CLI arg: `--corrections <path>` (aggregated corrections CSV).
  - Correction features merged by (store, sap, delivery_dow): `corr_samples, corr_avg_delta, corr_avg_ratio, corr_ratio_stddev, corr_removal_rate, corr_promo_rate`.
  - Feature set now includes these correction features alongside lags/promos/schedule fields.
- If no corrections CSV is provided, correction features default to 0 (backward compatible).

**Notes for team (incremental_trainer.py added)**:
- New `scripts/incremental_trainer.py`: thin wrapper over baseline_model to retrain after new orders.
  - Args: `--orders`, `--stock`, optional `--corrections`, optional `--promos`.
  - Uses the updated baseline_model pipeline with correction features if provided.
  - Intended for “retrain after each finalized order”; currently retrains fresh (GBR doesn’t truly warm-start).

**Notes for team (model_validator.py added)**:
- New `scripts/model_validator.py` to gate deployments vs naive:
  - `validate_predictions(y_true, y_pred, y_naive, mae_threshold, rmse_threshold)` returns metrics + pass/fail.
  - CLI: `--csv <file> --trueCol --predCol --naiveCol --maeThreshold --rmseThreshold`.

**Notes for team (forecast_engine.py added - initial scaffold)**:
- New `scripts/forecast_engine.py` that stitches loader → basic prediction → cache write:
  - Config: route_number, delivery_date, schedule_key (auto), service_account, since_days, corrections_csv (unused in scaffold), round_cases (default True), ttl_days.
  - Uses firebase_loader to pull catalog, stores, orders (filtered by schedule), converts orders to a DataFrame, runs a placeholder prediction (currently lag1 passthrough; TODO replace with trained model output), rounds to casePack if present, filters inactive items, and writes to `forecasts/{route}/cached/{forecastId}` via firebase_writer.
  - Utilities: `normalize_delivery_date`, `weekday_key` expected from schedule_utils (needs to be added/extended if missing).
- This is a scaffold: replace `_predict` with the real model inference path and remove the temp CSV workaround. Correction features are not yet injected here.

**Notes for team (training_pipeline.py added)**:
- New `scripts/training_pipeline.py`: orchestrates training + validation.
  - Args: `--orders`, `--stock`, optional `--corrections`, optional `--promos`, `--maeThreshold`, `--rmseThreshold`, optional `--savePreds`.
  - Builds modeling DF (with corrections if provided), trains GBR, evaluates vs naive (lag1), gates on thresholds, optionally writes preds CSV.

**Notes for team (run_forecast.py added)**:
- New CLI to trigger forecast generation + cache write:
  - Args: `--route`, `--deliveryDate`, optional `--serviceAccount`, `--sinceDays`, `--roundCases`, `--ttlDays`.
  - Calls `forecast_engine.generate_forecast` and logs forecast id/item count.

**Notes for team (order_archive_listener.py added - basic)**:
- Listens on `orderRequests/*` (polling), reads from DuckDB `orders_historical`, writes responses to `orderResponses/{id}`:
  - mode `dates`: returns list of delivery dates (latest 200).
  - mode `order` + deliveryDate: returns flattened items for that date.
  - Deletes request doc after responding. Polling interval configurable (`--poll`).
  - Requires service account and path to `analytics.duckdb`.
**Reference**: See "Phase 4: ML Feedback Loop & Training" section for detailed specs

---

### Claude 1 - UI & App Integration

**Scope**: React Native app components and services

| Task | File | Description | Dependencies |
|------|------|-------------|--------------|
| 1. Order Archive Service | `src/services/OrderArchiveService.ts` | Message bus client for archive requests | None ✅ |
| 2. Forecast Manager Update | `src/services/ForecastManager.ts` | Add correction submission on order finalize | OrderArchiveService.ts ✅ |
| 3. Order History Screen | `app/(app)/order-actions/history.tsx` | Display recent + archived orders with load more | OrderArchiveService.ts ✅ |
| 4. Archive Date Picker | `src/components/order/ArchiveDatePicker.tsx` | UI for selecting archived order dates | None ✅ |
| 5. Loading States | Integrated in history.tsx | Add loading spinners for archive requests | None ✅ |

**Status**: ✅ ALL TASKS COMPLETE! App UI ready for backend integration.

**Reference**: See "Order Archive Access (Message Bus Pattern)" section for specs

**Key UX Flow** (implemented):
- Recent orders (90 days) load instantly from Firebase
- "Load Older Orders" button triggers message bus request to `orderRequests/{id}`
- Loading spinner shown during backend query (1-3 sec)
- Archived dates displayed as selectable list
- Selecting a date triggers second request for full order details

---

### Claude 2 (This Agent) - Database Setup

**Scope**: DuckDB schema, sync scripts, initial data population

| Task | File | Description | Dependencies |
|------|------|-------------|--------------|
| 1. DB Schema | `scripts/db_schema.py` | Create all DuckDB tables with indexes | None ✅ |
| 2. DB Sync | `scripts/db_sync.py` | Firebase → DuckDB sync (full + incremental) | db_schema.py ✅ |
| 3. Calendar Features | `scripts/calendar_features.py` | Populate calendar_features table | db_schema.py ✅ |
| 4. Initial Data Load | `scripts/load_json_orders.py` | Populate DuckDB from existing JSON data | All above ✅ |
| 5. Test Queries | `scripts/test_db_queries.py` | Verify schema works for ML queries | All above ✅ |
| **6. Item Allocation Cache** | `scripts/db_schema.py` | Add `item_allocation_cache` table for case splitting | db_schema.py 🔴 |
| **7. Compute Historical Shares** | `scripts/compute_shares.py` | Populate historical share data per item/store | All above 🔴 |

**Status**: 🔴 NEW TASKS ADDED for case allocation support

**Database Stats** (as of Dec 11, 2025):
- 13 orders loaded (7 Monday, 6 Thursday)
- 2,063 line items
- 6 stores, 642 store-item pairs
- 1,096 calendar dates (2024-2026)
- All 8 ML queries passing ✅

**db_sync.py Notes**:
- Uses ChatGPT's `firebase_loader.py` for Firestore reads
- Supports full sync (`--route 989262`) or incremental (`--incremental --days 30`)
- Individual entity sync: `--products-only`, `--stores-only`, `--orders-only`, `--promos-only`
- Requires `google-cloud-firestore` package and service account credentials

**Reference**: See "Complete DuckDB Schema" section for table definitions

---

### Coordination Notes

1. **Shared Dependencies**:
   - `data/analytics.duckdb` - Created by Claude 2, used by ChatGPT
   - `scripts/models.py` - Created by ChatGPT, used by all Python scripts
   - Firebase collections - Read by all, written by app

2. **Integration Points**:
   - Claude 2 creates empty DB → ChatGPT writes sync scripts that populate it
   - ChatGPT creates listener → Claude 1 creates service that calls it
   - All agents reference same Firebase collection schemas

3. **Testing Order**:
   ```
   1. Claude 2: Create DB, verify tables exist
   2. ChatGPT: Run sync, verify data populated
   3. ChatGPT: Run forecast, verify output
   4. Claude 1: Test archive request flow
   ```

---

## Quick Reference (For AI Agents)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **App Database** | Firebase/Firestore | Real-time sync, mobile-first, 90-day rolling window |
| **ML Database** | DuckDB (local) | **Complete ML data foundation** - all history, never deleted |
| **Archive Access** | Message Bus (Firebase) | App → Firebase request → Mac listener → DuckDB → response |
| **ML Model** | GradientBoostingRegressor | Handles non-linear patterns, warm-start capable |
| **Case Allocation** | **CORE FEATURE** | Sum demand → round TOTAL to cases → allocate by historical share |
| **Retraining** | After each finalized order | Most responsive to user corrections |
| **Promo Source** | User uploads to Firebase | `promos/{route}/*` collection |
| **Feedback Loop** | Closed-loop learning | Corrections stored, model learns from them |
| **Sync Strategy** | Cloud Functions + batch | Real-time listeners + daily full sync option |

> ⚠️ **CRITICAL**: The algorithm must NEVER round per-store. Always aggregate demand across stores first, round the TOTAL to full cases, then allocate back to stores based on historical share patterns.

**Database Responsibility Split**:
| Data Type | Firebase | DuckDB |
|-----------|----------|--------|
| Recent orders (90 days) | ✅ (TTL auto-delete) | ✅ (permanent) |
| Historical orders (years) | ❌ | ✅ (via message bus) |
| User corrections | ❌ | ✅ |
| Store configs | ✅ (live) | ✅ (versioned) |
| Item lists | ✅ (live) | ✅ (versioned) |
| Promos | ✅ (live) | ✅ (with learned metrics) |
| Seasonal patterns | ❌ | ✅ |
| ML features/artifacts | ❌ | ✅ |
| Order archive requests | ✅ (message bus) | ❌ |

**Key Files**:
- `scripts/baseline_model.py` - ML model training
- `scripts/db_schema.py` - DuckDB table definitions
- `scripts/db_sync.py` - Firebase → DuckDB sync (full + incremental)
- `scripts/order_archive_listener.py` - **Message bus listener for archive requests**
- `scripts/feedback_collector.py` - Pull corrections for training
- `data/analytics.duckdb` - Local analytical database (COMPLETE ML foundation)
- `src/services/ForecastManager.ts` - App-side forecast integration
- `src/services/OrderArchiveService.ts` - **App-side archive request service**
- `functions/sync_to_analytics.py` - Cloud Function listeners

---

## Overview

Build a **multi-user, ML-powered ordering algorithm** that:
1. Takes user-specific parameters from Firebase (schedule, store items, order guide)
2. Uses historical orders filtered by delivery day (Mon ≠ Thu - each is its own series)
3. Handles intelligent case splitting across delivery days
4. Learns from user corrections (feedback loop for ML training)
5. Generates forecasts proactively so pre-fill is instant
6. Works for ANY user, not just one hardcoded route

## Design Principles

1. **User-Agnostic**: No hardcoded stores, SAPs, schedules - everything from Firebase
2. **Schedule-Aware**: Algorithm knows which stores get delivered on which days
3. **Case-Split Intelligent**: If Bowman's isn't delivered Thursday, split items go to Tuesday order
4. **Learning System**: User corrections feed back into ML training
5. **Local-First**: Runs on Mac for now, Firebase for data storage

---

## Current State Assessment

### What Exists

| Component | Location | Status |
|-----------|----------|--------|
| ML Model (GradientBoosting) | `scripts/baseline_model.py` | Works, but hardcoded |
| Forecast Generator | `scripts/generate_forecast.py` | Hardcoded stores |
| Data Loaders | `scripts/mission_orders.py` | Multi-header CSV parser |
| App Integration | `src/services/ForecastManager.ts` | Calls CF, caches to Firestore |
| Historical Orders | Firestore `orders/*` + `parsed_orders_batch_no_catalog/` | 13 orders |
| User Schedules | Firebase `userSettings.notifications.scheduling.orderCycles` | Available |
| Store Configs | Firebase `routes/{route}/stores/{id}` | Has `items[]`, `deliveryDays[]` |

### Critical Gaps

| Gap | Impact | Solution |
|-----|--------|----------|
| Hardcoded store lists | Only works for one user | Load from Firebase |
| No case rounding | Forecast shows 2 units instead of 12 (case) | Use catalog `casePack` |
| No case-split logic | Doesn't move items to correct order day | Check store `deliveryDays[]` |
| Mixed schedule history | Mon/Thu averaged together | Filter by `scheduleKey` |
| No feedback loop | User corrections ignored | Store in `forecast_feedback` |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     FIREBASE (Source of Truth)                   │
│                                                                  │
│  routes/{route}/stores/{id}     → items[], deliveryDays[]       │
│  masterCatalog/{route}/products → casePack, brand, category     │
│  orders/{id}                    → Historical orders w/ scheduleKey│
│  users/{uid}/userSettings       → Order cycles (delivery days)  │
│  forecast_feedback/{route}      → User corrections for ML       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LOCAL MAC (Algorithm Runner)                  │
│                                                                  │
│  1. Load user schedule from Firebase                            │
│  2. Load store configs (items[], deliveryDays[])                │
│  3. Load historical orders (filter by scheduleKey)              │
│  4. Load user corrections (feedback for ML)                     │
│  5. Compute demand per store/SAP                                │
│  6. Handle case splitting (move items to correct order day)     │
│  7. Round to case packs                                         │
│  8. Write forecast → forecasts/{route}/cached/{id}              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    APP (Instant Pre-fill)                        │
│                                                                  │
│  ForecastManager.getCachedForecast()                            │
│    → Read from Firestore (instant)                              │
│    → Apply to draft order                                       │
│  On finalize: submit corrections → forecast_feedback            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Analytical Database Architecture

> **Decision**: Use Firebase for real-time app data + DuckDB (local) as the **complete ML data foundation**.
> **Rationale**: The analytical DB is the single source of truth for ALL algorithm learning - orders, corrections, user params, promos, seasonal patterns, EVERYTHING.

### Core Principle

**Firebase** = App-facing, real-time, ephemeral (90-day rolling window)
**DuckDB** = Algorithm-facing, permanent, complete history (years of data, never deleted)

The algorithm should be able to answer ANY question about user behavior from the analytical DB alone.

### Why Not Firebase Alone?

| Concern | Firestore Reality |
|---------|-------------------|
| **Pricing** | $0.06 per 100K reads. Training on 10K orders = $$ every run |
| **Query Model** | Optimized for "get user X's orders", NOT aggregations |
| **Data Science** | No native pandas/SQL. Export → process → import is clunky |
| **Full Scans** | ML training needs entire history. Firestore hates this |

### Hybrid Architecture with Listener Sync

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FIREBASE (Firestore)                         │
│                                                                      │
│  PURPOSE: Real-time app data, user-facing operations                │
│                                                                      │
│  STORES:                                                            │
│  • users/{uid}/userSettings     → Schedules, preferences            │
│  • routes/{route}/stores/*      → Store configs, item lists         │
│  • orders/{id}                  → Recent orders (90-day rolling)    │
│  • forecasts/{route}/cached/*   → Pre-computed forecasts            │
│  • masterCatalog/{route}/*      → Product catalog                   │
│  • promos/{route}/*             → User-uploaded promotions          │
│                                                                      │
│  CHARACTERISTICS:                                                    │
│  • Auto-sync to mobile app                                          │
│  • Real-time listeners                                              │
│  • TTL/auto-delete on old data (app stays clean)                    │
│  • Pay-per-read pricing model                                       │
└─────────────────────────────────────────────────────────────────────┘
          │                    │                    │
          │ onOrderFinalized   │ onStoreUpdate      │ onPromoUpload
          │                    │ onItemListChange   │ onScheduleChange
          ▼                    ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SYNC LISTENERS (Cloud Functions)                  │
│                                                                      │
│  • Listen to ALL relevant Firebase collections                      │
│  • Transform data for analytical schema                             │
│  • Push to analytical database                                      │
│  • Maintain referential integrity                                   │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ANALYTICAL DATABASE (DuckDB - Local)                    │
│                                                                      │
│  PURPOSE: COMPLETE ML DATA FOUNDATION                               │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ ORDER DATA (Historical, never deleted)                       │   │
│  │ • orders_historical      → Every order, forever              │   │
│  │ • order_line_items       → Individual store/item rows        │   │
│  │ • forecast_corrections   → Every user correction             │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ USER PARAMETERS (Synced from Firebase, versioned)            │   │
│  │ • user_schedules         → Order cycles (Mon→Thu delivery)   │   │
│  │ • stores                 → Store configs, delivery days      │   │
│  │ • store_items            → Which items each store carries    │   │
│  │ • product_catalog        → SAPs, case packs, brands          │   │
│  │ • order_guide            → User's order guide preferences    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ PROMO DATA                                                   │   │
│  │ • promo_history          → All promos + learned responses    │   │
│  │ • promo_items            → Which SAPs each promo affects     │   │
│  │ • promo_corrections      → User corrections during promos    │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ SEASONAL/PATTERN DATA                                        │   │
│  │ • calendar_features      → Holidays, first weekend of month  │   │
│  │ • seasonal_adjustments   → Learned lifts for special periods │   │
│  │ • weekly_patterns        → Day-of-week demand patterns       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ ML ARTIFACTS                                                 │   │
│  │ • feature_cache          → Pre-computed features per item    │   │
│  │ • model_metadata         → Training runs, performance        │   │
│  │ • prediction_log         → What we predicted vs actual       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  CHARACTERISTICS:                                                    │
│  • SINGLE SOURCE OF TRUTH for algorithm                            │
│  • Permanent storage (never delete historical data)                │
│  • SQL aggregations, window functions, joins                       │
│  • Native pandas/numpy integration                                  │
│  • Fixed cost (FREE for DuckDB)                                     │
└─────────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    LOCAL MAC (ML Pipeline)                           │
│                                                                      │
│  • Queries ONLY the analytical DB (never Firebase directly)        │
│  • Runs model training                                              │
│  • Writes forecasts back to Firebase for app consumption           │
└─────────────────────────────────────────────────────────────────────┘
```

### Complete DuckDB Schema

```sql
-- File location: order_forecast/data/analytics.duckdb

-- ============================================================================
-- ORDER DATA (Historical, never deleted)
-- ============================================================================

-- Core historical orders
CREATE TABLE orders_historical (
    order_id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,          -- 'monday', 'thursday'
    delivery_date DATE NOT NULL,
    order_date DATE NOT NULL,
    finalized_at TIMESTAMP,
    total_cases INTEGER,
    total_units INTEGER,
    store_count INTEGER,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Individual line items (one row per store/item in each order)
CREATE TABLE order_line_items (
    line_item_id VARCHAR PRIMARY KEY,       -- {order_id}-{store_id}-{sap}
    order_id VARCHAR NOT NULL REFERENCES orders_historical(order_id),
    route_number VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    delivery_date DATE NOT NULL,
    store_id VARCHAR NOT NULL,
    store_name VARCHAR,
    sap VARCHAR NOT NULL,
    product_name VARCHAR,
    quantity INTEGER NOT NULL,
    cases INTEGER NOT NULL,
    promo_id VARCHAR,                       -- NULL if no promo
    promo_active BOOLEAN DEFAULT FALSE,
    
    -- Calendar features (denormalized for query speed)
    is_first_weekend_of_month BOOLEAN,
    is_holiday_week BOOLEAN,
    is_month_end BOOLEAN,
    day_of_week INTEGER,                    -- 0=Mon, 6=Sun
    week_of_year INTEGER,
    month INTEGER,
    
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User corrections to forecasts
CREATE TABLE forecast_corrections (
    correction_id VARCHAR PRIMARY KEY,
    forecast_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    route_number VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    delivery_date DATE NOT NULL,
    store_id VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    
    -- What algorithm predicted
    predicted_units INTEGER NOT NULL,
    predicted_cases INTEGER NOT NULL,
    prediction_confidence FLOAT,
    prediction_source VARCHAR,              -- 'baseline', 'ml_v1', etc.
    
    -- What user actually submitted
    final_units INTEGER NOT NULL,
    final_cases INTEGER NOT NULL,
    
    -- Derived metrics
    correction_delta INTEGER NOT NULL,      -- final - predicted
    correction_ratio FLOAT NOT NULL,        -- final / predicted
    was_removed BOOLEAN DEFAULT FALSE,      -- User deleted this item
    
    -- Context
    promo_id VARCHAR,
    promo_active BOOLEAN DEFAULT FALSE,
    is_first_weekend_of_month BOOLEAN,
    is_holiday_week BOOLEAN,
    
    submitted_at TIMESTAMP NOT NULL
);

-- ============================================================================
-- USER PARAMETERS (Synced from Firebase, versioned)
-- ============================================================================

-- User schedule configurations
CREATE TABLE user_schedules (
    id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    order_day INTEGER NOT NULL,             -- 1=Mon, 7=Sun
    load_day INTEGER NOT NULL,
    delivery_day INTEGER NOT NULL,
    schedule_key VARCHAR NOT NULL,          -- 'monday', 'thursday'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store configurations
CREATE TABLE stores (
    store_id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    store_name VARCHAR NOT NULL,
    store_number VARCHAR,
    address VARCHAR,
    delivery_days TEXT,                     -- JSON array: ["Monday", "Friday"]
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Store item lists (which items each store carries)
CREATE TABLE store_items (
    id VARCHAR PRIMARY KEY,                 -- {store_id}-{sap}
    store_id VARCHAR NOT NULL REFERENCES stores(store_id),
    route_number VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    added_at TIMESTAMP,
    removed_at TIMESTAMP,                   -- NULL if still active
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(store_id, sap)
);

-- Product catalog
CREATE TABLE product_catalog (
    sap VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    full_name VARCHAR NOT NULL,
    short_name VARCHAR,
    brand VARCHAR,
    category VARCHAR,
    sub_category VARCHAR,
    case_pack INTEGER NOT NULL DEFAULT 1,   -- Units per case
    unit_weight FLOAT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User order guide preferences
CREATE TABLE order_guide (
    id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    user_id VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    preferred_quantity INTEGER,             -- User's typical order qty
    min_quantity INTEGER,
    max_quantity INTEGER,
    notes VARCHAR,
    is_favorite BOOLEAN DEFAULT FALSE,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PROMO DATA
-- ============================================================================

-- Promo history
CREATE TABLE promo_history (
    promo_id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    promo_name VARCHAR,
    promo_type VARCHAR,                     -- 'discount', 'bogo', 'bundle'
    start_date DATE,
    end_date DATE,
    discount_percent FLOAT,
    discount_amount FLOAT,
    source_file VARCHAR,                    -- Original PDF filename
    uploaded_by VARCHAR,
    uploaded_at TIMESTAMP,
    
    -- Learned metrics (updated after orders finalized)
    avg_quantity_lift FLOAT,
    user_correction_avg FLOAT,
    times_seen INTEGER DEFAULT 0,
    last_seen_date DATE,
    
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Which items each promo affects
CREATE TABLE promo_items (
    id VARCHAR PRIMARY KEY,                 -- {promo_id}-{sap}
    promo_id VARCHAR NOT NULL REFERENCES promo_history(promo_id),
    sap VARCHAR NOT NULL,
    special_price FLOAT,
    discount_percent FLOAT,
    synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(promo_id, sap)
);

-- ============================================================================
-- SEASONAL/PATTERN DATA
-- ============================================================================

-- Calendar features lookup
CREATE TABLE calendar_features (
    date DATE PRIMARY KEY,
    day_of_week INTEGER,                    -- 0=Mon, 6=Sun
    week_of_year INTEGER,
    month INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_first_weekend_of_month BOOLEAN,
    is_last_weekend_of_month BOOLEAN,
    is_month_start BOOLEAN,                 -- First 3 days
    is_month_end BOOLEAN,                   -- Last 3 days
    is_holiday BOOLEAN,
    holiday_name VARCHAR,
    is_holiday_week BOOLEAN,                -- Week containing a holiday
    days_until_next_holiday INTEGER,
    days_since_last_holiday INTEGER
);

-- Learned seasonal adjustments per store/item
CREATE TABLE seasonal_adjustments (
    id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    store_id VARCHAR,                       -- NULL = route-wide
    sap VARCHAR,                            -- NULL = store-wide
    
    -- Pattern type
    pattern_type VARCHAR NOT NULL,          -- 'first_weekend', 'holiday', 'summer', etc.
    pattern_value VARCHAR,                  -- Specific holiday name, month, etc.
    
    -- Learned lift
    quantity_multiplier FLOAT NOT NULL,     -- 1.2 = 20% increase
    confidence FLOAT,
    sample_count INTEGER,
    
    last_updated TIMESTAMP
);

-- ============================================================================
-- ML ARTIFACTS
-- ============================================================================

-- Pre-computed feature cache (refreshed after each order)
CREATE TABLE feature_cache (
    id VARCHAR PRIMARY KEY,                 -- {route}-{store}-{sap}-{schedule}
    route_number VARCHAR NOT NULL,
    store_id VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    
    -- Baseline demand features
    avg_quantity FLOAT,
    quantity_stddev FLOAT,
    quantity_min INTEGER,
    quantity_max INTEGER,
    last_quantity INTEGER,
    order_count INTEGER,
    
    -- Lag features
    lag_1 FLOAT,
    lag_2 FLOAT,
    lag_3 FLOAT,
    rolling_mean_4 FLOAT,
    rolling_mean_8 FLOAT,
    
    -- Correction features
    avg_correction_ratio FLOAT,
    correction_stddev FLOAT,
    last_correction_delta INTEGER,
    removal_rate FLOAT,
    correction_trend FLOAT,
    
    -- Promo response features
    avg_promo_lift FLOAT,
    promo_correction_ratio FLOAT,
    
    -- Seasonal features
    first_weekend_lift FLOAT,
    holiday_lift FLOAT,
    
    updated_at TIMESTAMP,
    
    UNIQUE(route_number, store_id, sap, schedule_key)
);

-- Model training metadata
CREATE TABLE model_metadata (
    model_id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    model_version INTEGER NOT NULL,
    
    -- Training info
    trained_at TIMESTAMP NOT NULL,
    training_rows INTEGER,
    feature_count INTEGER,
    features_used TEXT,                     -- JSON array
    
    -- Performance metrics
    validation_mae FLOAT,
    validation_rmse FLOAT,
    validation_r2 FLOAT,
    
    -- Feature importance
    feature_importance TEXT,                -- JSON object
    
    -- Status
    is_active BOOLEAN DEFAULT FALSE,        -- Currently deployed?
    deployed_at TIMESTAMP,
    retired_at TIMESTAMP,
    
    model_path VARCHAR                      -- Path to .pkl file
);

-- Prediction log (for monitoring and debugging)
CREATE TABLE prediction_log (
    prediction_id VARCHAR PRIMARY KEY,
    forecast_id VARCHAR NOT NULL,
    route_number VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    delivery_date DATE NOT NULL,
    store_id VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    
    -- Prediction details
    model_id VARCHAR,
    predicted_quantity INTEGER,
    prediction_confidence FLOAT,
    
    -- Actual outcome (filled in after order finalized)
    actual_quantity INTEGER,
    prediction_error INTEGER,               -- actual - predicted
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX idx_line_items_route_schedule ON order_line_items(route_number, schedule_key);
CREATE INDEX idx_line_items_delivery_date ON order_line_items(delivery_date);
CREATE INDEX idx_line_items_store_sap ON order_line_items(store_id, sap);
CREATE INDEX idx_corrections_store_sap ON forecast_corrections(store_id, sap);
CREATE INDEX idx_corrections_promo ON forecast_corrections(promo_id) WHERE promo_id IS NOT NULL;
CREATE INDEX idx_store_items_store ON store_items(store_id);
CREATE INDEX idx_promo_items_promo ON promo_items(promo_id);
CREATE INDEX idx_feature_cache_lookup ON feature_cache(route_number, store_id, sap, schedule_key);
```

### Sync Listeners (Cloud Functions)

```python
# functions/sync_to_analytics.py
# These Cloud Functions listen to Firebase changes and sync to analytical DB

# ============================================================================
# ORDER SYNC
# ============================================================================

@firestore_fn.on_document_updated(document="orders/{orderId}")
def on_order_finalized(event):
    """Sync finalized order to analytical database."""
    before = event.data.before.to_dict()
    after = event.data.after.to_dict()
    
    # Only sync when status changes to 'finalized'
    if before.get('status') != 'finalized' and after.get('status') == 'finalized':
        sync_order_to_analytics(after)
        sync_corrections_to_analytics(after)

# ============================================================================
# USER PARAMETER SYNC
# ============================================================================

@firestore_fn.on_document_written(document="routes/{routeId}/stores/{storeId}")
def on_store_changed(event):
    """Sync store config changes to analytical database."""
    if event.data.after.exists:
        sync_store_to_analytics(event.data.after.to_dict())
    else:
        mark_store_inactive(event.params['storeId'])

@firestore_fn.on_document_written(document="routes/{routeId}/stores/{storeId}/items/{itemId}")
def on_store_item_changed(event):
    """Sync store item list changes to analytical database."""
    if event.data.after.exists:
        sync_store_item_to_analytics(
            event.params['storeId'],
            event.data.after.to_dict()
        )
    else:
        mark_store_item_removed(
            event.params['storeId'],
            event.params['itemId']
        )

@firestore_fn.on_document_written(document="masterCatalog/{routeId}/products/{sap}")
def on_catalog_changed(event):
    """Sync product catalog changes to analytical database."""
    if event.data.after.exists:
        sync_product_to_analytics(event.data.after.to_dict())

@firestore_fn.on_document_written(document="users/{userId}/userSettings")
def on_schedule_changed(event):
    """Sync user schedule changes to analytical database."""
    if event.data.after.exists:
        sync_schedule_to_analytics(
            event.params['userId'],
            event.data.after.to_dict()
        )

# ============================================================================
# PROMO SYNC
# ============================================================================

@firestore_fn.on_document_created(document="promos/{routeId}/{promoId}")
def on_promo_uploaded(event):
    """Sync new promo to analytical database."""
    sync_promo_to_analytics(event.data.to_dict())

@firestore_fn.on_document_updated(document="promos/{routeId}/{promoId}")
def on_promo_updated(event):
    """Sync promo updates to analytical database."""
    sync_promo_to_analytics(event.data.after.to_dict())
```

### Local Sync Script (for batch operations)

```python
# scripts/db_sync.py
"""
Sync utilities for Firebase → DuckDB.
Can be run as batch job or called by Cloud Functions.
"""

import duckdb
from firebase_admin import firestore
from datetime import datetime

class AnalyticsSync:
    def __init__(self, db_path: str = 'data/analytics.duckdb'):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
    
    # ========================================================================
    # ORDER SYNC
    # ========================================================================
    
    def sync_order(self, order_data: dict):
        """Sync a finalized order and all its line items."""
        # Insert order header
        self.conn.execute("""
            INSERT INTO orders_historical 
            (order_id, route_number, user_id, schedule_key, delivery_date, 
             order_date, finalized_at, total_cases, total_units, store_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id) DO NOTHING
        """, [
            order_data['id'],
            order_data['routeNumber'],
            order_data.get('userId'),
            order_data['scheduleKey'],
            order_data['deliveryDate'],
            order_data['orderDate'],
            datetime.now(),
            order_data.get('totalCases', 0),
            order_data.get('totalUnits', 0),
            len(order_data.get('stores', []))
        ])
        
        # Insert line items with calendar features
        for store in order_data.get('stores', []):
            for item in store.get('items', []):
                calendar = self._get_calendar_features(order_data['deliveryDate'])
                self.conn.execute("""
                    INSERT INTO order_line_items
                    (line_item_id, order_id, route_number, schedule_key, delivery_date,
                     store_id, store_name, sap, product_name, quantity, cases,
                     promo_id, promo_active, is_first_weekend_of_month, is_holiday_week,
                     is_month_end, day_of_week, week_of_year, month)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (line_item_id) DO NOTHING
                """, [
                    f"{order_data['id']}-{store['storeId']}-{item['sap']}",
                    order_data['id'],
                    order_data['routeNumber'],
                    order_data['scheduleKey'],
                    order_data['deliveryDate'],
                    store['storeId'],
                    store.get('storeName'),
                    item['sap'],
                    item.get('productName'),
                    item['quantity'],
                    item.get('cases', 0),
                    item.get('promoId'),
                    item.get('promoActive', False),
                    calendar['is_first_weekend_of_month'],
                    calendar['is_holiday_week'],
                    calendar['is_month_end'],
                    calendar['day_of_week'],
                    calendar['week_of_year'],
                    calendar['month']
                ])
    
    # ========================================================================
    # USER PARAMETER SYNC
    # ========================================================================
    
    def sync_store(self, store_data: dict):
        """Sync store configuration."""
        self.conn.execute("""
            INSERT INTO stores
            (store_id, route_number, store_name, store_number, address,
             delivery_days, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (store_id) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                delivery_days = EXCLUDED.delivery_days,
                is_active = EXCLUDED.is_active,
                updated_at = EXCLUDED.updated_at,
                synced_at = CURRENT_TIMESTAMP
        """, [
            store_data['id'],
            store_data['routeNumber'],
            store_data['name'],
            store_data.get('storeNumber'),
            store_data.get('address'),
            str(store_data.get('deliveryDays', [])),
            store_data.get('isActive', True),
            store_data.get('createdAt'),
            store_data.get('updatedAt')
        ])
    
    def sync_store_items(self, store_id: str, items: list):
        """Sync store item list."""
        for sap in items:
            self.conn.execute("""
                INSERT INTO store_items (id, store_id, route_number, sap, is_active, added_at)
                VALUES (?, ?, ?, ?, TRUE, CURRENT_TIMESTAMP)
                ON CONFLICT (store_id, sap) DO UPDATE SET
                    is_active = TRUE,
                    removed_at = NULL,
                    synced_at = CURRENT_TIMESTAMP
            """, [f"{store_id}-{sap}", store_id, store_id.split('_')[0], sap])
    
    def sync_product(self, product_data: dict):
        """Sync product catalog entry."""
        self.conn.execute("""
            INSERT INTO product_catalog
            (sap, route_number, full_name, short_name, brand, category,
             sub_category, case_pack, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (sap) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                brand = EXCLUDED.brand,
                category = EXCLUDED.category,
                case_pack = EXCLUDED.case_pack,
                synced_at = CURRENT_TIMESTAMP
        """, [
            product_data['sap'],
            product_data['routeNumber'],
            product_data['fullName'],
            product_data.get('shortName'),
            product_data.get('brand'),
            product_data.get('category'),
            product_data.get('subCategory'),
            product_data.get('casePack', 1),
            product_data.get('isActive', True)
        ])
    
    def sync_promo(self, promo_data: dict):
        """Sync promo and its affected items."""
        self.conn.execute("""
            INSERT INTO promo_history
            (promo_id, route_number, promo_name, promo_type, start_date, end_date,
             discount_percent, source_file, uploaded_by, uploaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (promo_id) DO UPDATE SET
                promo_name = EXCLUDED.promo_name,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                discount_percent = EXCLUDED.discount_percent,
                synced_at = CURRENT_TIMESTAMP
        """, [
            promo_data['promoId'],
            promo_data['routeNumber'],
            promo_data.get('promoName'),
            promo_data.get('promoType'),
            promo_data.get('startDate'),
            promo_data.get('endDate'),
            promo_data.get('discountPercent'),
            promo_data.get('sourceFileName'),
            promo_data.get('uploadedBy'),
            promo_data.get('uploadedAt')
        ])
        
        # Sync affected items
        for sap in promo_data.get('affectedSaps', []):
            self.conn.execute("""
                INSERT INTO promo_items (id, promo_id, sap, discount_percent)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (promo_id, sap) DO NOTHING
            """, [
                f"{promo_data['promoId']}-{sap}",
                promo_data['promoId'],
                sap,
                promo_data.get('discountPercent')
            ])
    
    # ========================================================================
    # FULL SYNC (Initial population)
    # ========================================================================
    
    def full_sync_from_firebase(self, firebase_db, route_number: str):
        """Pull all data from Firebase and populate analytical DB."""
        print(f"Starting full sync for route {route_number}...")
        
        # Sync stores
        stores = firebase_db.collection('routes').document(route_number)\
                           .collection('stores').get()
        for store_doc in stores:
            self.sync_store({**store_doc.to_dict(), 'id': store_doc.id})
        print(f"  Synced {len(stores)} stores")
        
        # Sync catalog
        products = firebase_db.collection('masterCatalog').document(route_number)\
                             .collection('products').get()
        for prod_doc in products:
            self.sync_product({**prod_doc.to_dict(), 'routeNumber': route_number})
        print(f"  Synced {len(products)} products")
        
        # Sync orders
        orders = firebase_db.collection('orders')\
                           .where('routeNumber', '==', route_number)\
                           .where('status', '==', 'finalized').get()
        for order_doc in orders:
            self.sync_order({**order_doc.to_dict(), 'id': order_doc.id})
        print(f"  Synced {len(orders)} orders")
        
        # Sync promos
        promos = firebase_db.collection('promos').document(route_number)\
                           .collection('active').get()
        for promo_doc in promos:
            self.sync_promo({**promo_doc.to_dict(), 'promoId': promo_doc.id})
        print(f"  Synced {len(promos)} promos")
        
        print("Full sync complete!")
    
    def close(self):
        self.conn.close()
```

### Migration Path

```
Phase 1 (Now):    DuckDB locally on Mac
                  └─▶ Zero cost, zero infrastructure
                  └─▶ Full sync script for initial population
                  └─▶ Cloud Functions for real-time sync

Phase 2 (Later):  PostgreSQL on Cloud SQL
                  └─▶ If multiple machines need access
                  └─▶ Same schema, just change connection string

Phase 3 (Scale):  BigQuery
                  └─▶ If millions of orders across customers
                  └─▶ Cross-customer analytics
```

---

## Order Archive Access (Message Bus Pattern)

> **Pattern**: Firebase as message bus between app and local Mac backend
> **Same as**: PCF OCR listener architecture
> **Purpose**: Allow app to request historical orders from DuckDB on-demand

### Data Retention Strategy

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA RETENTION TIERS                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  FIREBASE (Hot - 90 days)              DUCKDB (Cold - Forever)      │
│  ─────────────────────────             ────────────────────────     │
│  • Recent orders (TTL auto-delete)     • ALL orders (permanent)     │
│  • Instant app access                  • On-demand via message bus  │
│  • Order History default view          • ML training source         │
│  • ~$5-20/month                        • FREE                       │
│                                                                      │
│  After 90 days:                                                     │
│  Firebase order deleted ──▶ Still exists in DuckDB                 │
│  App requests old order ──▶ Message bus ──▶ DuckDB ──▶ Response    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Message Bus Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                  FIREBASE AS MESSAGE BUS                             │
│              (Same pattern as PCF OCR listeners)                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────┐                ┌─────────────┐              ┌────────┐ │
│  │   APP   │ ──request──▶  │  FIREBASE   │  ◀──listen── │  MAC   │ │
│  │         │               │orderRequests│              │ BACKEND│ │
│  │         │ ◀──response── │             │  ──write──▶  │        │ │
│  │         │               │             │              │ DuckDB │ │
│  └─────────┘               └─────────────┘              └────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Order Request Collection (Firebase)

```typescript
// orderRequests/{requestId}
{
  requestId: string,
  type: "list_dates" | "get_order" | "search_orders",
  routeNumber: string,
  userId: string,
  
  // For "get_order" type
  deliveryDate?: string,
  orderId?: string,
  
  // For "search_orders" type  
  searchParams?: {
    dateFrom?: string,
    dateTo?: string,
    storeId?: string,
    sap?: string,
  },
  
  status: "pending" | "processing" | "completed" | "error",
  createdAt: Timestamp,
  completedAt?: Timestamp,
  
  // Response (filled by backend)
  result?: {
    dates?: string[],           // For list_dates
    order?: Order,              // For get_order
    orders?: OrderSummary[],    // For search_orders
    totalCount?: number,
  },
  error?: string,
}
```

### Order History User Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ORDER HISTORY - USER FLOW                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  STEP 1: User opens Order History                                   │
│  ─────────────────────────────────                                  │
│  App queries: orders/* where deliveryDate > 90 days ago             │
│  Result: Recent orders displayed instantly                          │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Recent Orders (Last 90 Days)                                 │   │
│  │ ├── Dec 15 (Mon) - 247 cases, 42 stores                     │   │
│  │ ├── Dec 11 (Thu) - 198 cases, 38 stores                     │   │
│  │ └── Dec 8 (Mon) - 256 cases, 44 stores                      │   │
│  │                                                              │   │
│  │ [Load Older Orders...]                                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  STEP 2: User taps "Load Older Orders"                              │
│  ──────────────────────────────────────                             │
│  App writes to: orderRequests/{requestId}                           │
│  {                                                                  │
│    type: "list_dates",                                              │
│    routeNumber: "989262",                                           │
│    status: "pending"                                                │
│  }                                                                  │
│                                                                      │
│  STEP 3: Backend listener fires (1-3 seconds)                       │
│  ────────────────────────────────────────────                       │
│  Mac backend queries DuckDB for available dates                     │
│  Writes response: { status: "completed", result: { dates: [...] }}  │
│                                                                      │
│  STEP 4: App displays archived date list                            │
│  ────────────────────────────────────────                           │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Archived Orders                                              │   │
│  │ ├── Sep 15 (Mon)                              [View]         │   │
│  │ ├── Sep 11 (Thu)                              [View]         │   │
│  │ ├── Sep 8 (Mon)                               [View]         │   │
│  │ └── [Show more...]                                           │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  STEP 5: User taps [View] on Sep 15                                 │
│  ──────────────────────────────────                                 │
│  App writes: orderRequests/{newRequestId}                           │
│  { type: "get_order", deliveryDate: "2025-09-15", status: "pending"}│
│                                                                      │
│  STEP 6: Backend fetches full order                                 │
│  ──────────────────────────────────                                 │
│  Mac queries DuckDB for complete order                              │
│  Writes full order data to response                                 │
│                                                                      │
│  STEP 7: App displays archived order                                │
│  ───────────────────────────────────                                │
│  User can view details, copy to new order, or export                │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Backend Listener Implementation

```python
# scripts/order_archive_listener.py
"""
Listens for order archive requests from Firebase.
Queries local DuckDB and writes responses back.
Same pattern as PCF OCR listener.
"""

import firebase_admin
from firebase_admin import firestore
import duckdb
from datetime import datetime, timedelta
import time

class OrderArchiveListener:
    def __init__(self, db_path: str = 'data/analytics.duckdb'):
        self.db_path = db_path
        self.duck = duckdb.connect(db_path, read_only=True)
        self.fs = firestore.client()
        
    def start_listening(self):
        """Start listening for archive requests."""
        print("🎧 Order Archive Listener started...")
        print(f"   Database: {self.db_path}")
        
        # Listen for pending requests
        query = self.fs.collection('orderRequests').where('status', '==', 'pending')
        query.on_snapshot(self._handle_requests)
        
        # Keep alive
        print("   Listening for requests... (Ctrl+C to stop)")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n👋 Listener stopped")
    
    def _handle_requests(self, snapshots, changes, read_time):
        """Process incoming requests."""
        for change in changes:
            if change.type.name == 'ADDED':
                doc = change.document
                request = doc.to_dict()
                request['id'] = doc.id
                
                print(f"\n📥 Request: {request['type']} for route {request['routeNumber']}")
                
                # Mark as processing
                doc.reference.update({'status': 'processing'})
                
                try:
                    if request['type'] == 'list_dates':
                        result = self._list_archived_dates(request)
                    elif request['type'] == 'get_order':
                        result = self._get_archived_order(request)
                    elif request['type'] == 'search_orders':
                        result = self._search_orders(request)
                    else:
                        raise ValueError(f"Unknown request type: {request['type']}")
                    
                    # Write success response
                    doc.reference.update({
                        'status': 'completed',
                        'result': result,
                        'completedAt': firestore.SERVER_TIMESTAMP
                    })
                    print(f"   ✅ Completed ({result.get('totalCount', 1)} results)")
                    
                except Exception as e:
                    # Write error response
                    doc.reference.update({
                        'status': 'error',
                        'error': str(e),
                        'completedAt': firestore.SERVER_TIMESTAMP
                    })
                    print(f"   ❌ Error: {e}")
    
    def _list_archived_dates(self, request: dict) -> dict:
        """List all order dates older than 90 days."""
        cutoff = datetime.now() - timedelta(days=90)
        
        dates = self.duck.execute("""
            SELECT DISTINCT 
                delivery_date,
                schedule_key,
                total_cases,
                store_count
            FROM orders_historical
            WHERE route_number = ?
              AND delivery_date < ?
            ORDER BY delivery_date DESC
            LIMIT 100
        """, [request['routeNumber'], cutoff.strftime('%Y-%m-%d')]).fetchall()
        
        return {
            'dates': [
                {
                    'date': d[0].strftime('%Y-%m-%d'),
                    'scheduleKey': d[1],
                    'totalCases': d[2],
                    'storeCount': d[3]
                } 
                for d in dates
            ],
            'totalCount': len(dates)
        }
    
    def _get_archived_order(self, request: dict) -> dict:
        """Fetch a specific archived order with all details."""
        # Get order header
        order = self.duck.execute("""
            SELECT order_id, route_number, schedule_key, delivery_date,
                   order_date, total_cases, total_units, store_count
            FROM orders_historical
            WHERE route_number = ?
              AND delivery_date = ?
            LIMIT 1
        """, [request['routeNumber'], request['deliveryDate']]).fetchone()
        
        if not order:
            raise ValueError(f"Order not found for date {request['deliveryDate']}")
        
        order_id = order[0]
        
        # Get line items grouped by store
        items = self.duck.execute("""
            SELECT store_id, store_name, sap, product_name, 
                   quantity, cases, promo_active, promo_id
            FROM order_line_items
            WHERE order_id = ?
            ORDER BY store_name, sap
        """, [order_id]).fetchall()
        
        # Group by store
        stores_dict = {}
        for item in items:
            store_id = item[0]
            if store_id not in stores_dict:
                stores_dict[store_id] = {
                    'storeId': store_id,
                    'storeName': item[1],
                    'items': []
                }
            stores_dict[store_id]['items'].append({
                'sap': item[2],
                'productName': item[3],
                'quantity': item[4],
                'cases': item[5],
                'promoActive': item[6],
                'promoId': item[7]
            })
        
        return {
            'order': {
                'orderId': order_id,
                'routeNumber': order[1],
                'scheduleKey': order[2],
                'deliveryDate': order[3].strftime('%Y-%m-%d'),
                'orderDate': order[4].strftime('%Y-%m-%d') if order[4] else None,
                'totalCases': order[5],
                'totalUnits': order[6],
                'storeCount': order[7],
                'stores': list(stores_dict.values())
            }
        }
    
    def _search_orders(self, request: dict) -> dict:
        """Search orders by various criteria."""
        params = request.get('searchParams', {})
        
        query = """
            SELECT DISTINCT 
                o.order_id,
                o.delivery_date,
                o.schedule_key,
                o.total_cases,
                o.total_units,
                o.store_count
            FROM orders_historical o
            WHERE o.route_number = ?
        """
        query_params = [request['routeNumber']]
        
        if params.get('dateFrom'):
            query += " AND o.delivery_date >= ?"
            query_params.append(params['dateFrom'])
        
        if params.get('dateTo'):
            query += " AND o.delivery_date <= ?"
            query_params.append(params['dateTo'])
        
        if params.get('storeId'):
            query += """
                AND EXISTS (
                    SELECT 1 FROM order_line_items li 
                    WHERE li.order_id = o.order_id AND li.store_id = ?
                )
            """
            query_params.append(params['storeId'])
        
        if params.get('sap'):
            query += """
                AND EXISTS (
                    SELECT 1 FROM order_line_items li 
                    WHERE li.order_id = o.order_id AND li.sap = ?
                )
            """
            query_params.append(params['sap'])
        
        query += " ORDER BY o.delivery_date DESC LIMIT 50"
        
        results = self.duck.execute(query, query_params).fetchall()
        
        return {
            'orders': [
                {
                    'orderId': r[0],
                    'deliveryDate': r[1].strftime('%Y-%m-%d'),
                    'scheduleKey': r[2],
                    'totalCases': r[3],
                    'totalUnits': r[4],
                    'storeCount': r[5]
                }
                for r in results
            ],
            'totalCount': len(results)
        }


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Order Archive Listener')
    parser.add_argument('--db', default='data/analytics.duckdb', help='Path to DuckDB database')
    parser.add_argument('--service-account', help='Path to Firebase service account JSON')
    args = parser.parse_args()
    
    # Initialize Firebase
    if args.service_account:
        cred = firebase_admin.credentials.Certificate(args.service_account)
        firebase_admin.initialize_app(cred)
    else:
        firebase_admin.initialize_app()
    
    # Start listener
    listener = OrderArchiveListener(args.db)
    listener.start_listening()
```

### App-Side Service (TypeScript)

```typescript
// src/services/OrderArchiveService.ts
import { 
  collection, doc, addDoc, onSnapshot, 
  Timestamp, query, where, orderBy, limit 
} from 'firebase/firestore';
import { getFirestore } from 'firebase/firestore';

interface ArchivedDateSummary {
  date: string;
  scheduleKey: string;
  totalCases: number;
  storeCount: number;
}

interface ArchiveRequest {
  type: 'list_dates' | 'get_order' | 'search_orders';
  routeNumber: string;
  userId: string;
  deliveryDate?: string;
  searchParams?: {
    dateFrom?: string;
    dateTo?: string;
    storeId?: string;
    sap?: string;
  };
  status: 'pending' | 'processing' | 'completed' | 'error';
  createdAt: Timestamp;
}

export class OrderArchiveService {
  private db = getFirestore();
  
  /**
   * Request list of archived order dates from backend
   */
  async listArchivedDates(routeNumber: string, userId: string): Promise<ArchivedDateSummary[]> {
    return this._makeRequest({
      type: 'list_dates',
      routeNumber,
      userId,
    }) as Promise<ArchivedDateSummary[]>;
  }
  
  /**
   * Request a specific archived order from backend
   */
  async getArchivedOrder(routeNumber: string, userId: string, deliveryDate: string): Promise<any> {
    const result = await this._makeRequest({
      type: 'get_order',
      routeNumber,
      userId,
      deliveryDate,
    });
    return result.order;
  }
  
  /**
   * Search archived orders by criteria
   */
  async searchArchivedOrders(
    routeNumber: string, 
    userId: string, 
    params: { dateFrom?: string; dateTo?: string; storeId?: string; sap?: string }
  ): Promise<any[]> {
    const result = await this._makeRequest({
      type: 'search_orders',
      routeNumber,
      userId,
      searchParams: params,
    });
    return result.orders;
  }
  
  /**
   * Internal: Make request and wait for response
   */
  private async _makeRequest(request: Partial<ArchiveRequest>): Promise<any> {
    const requestsRef = collection(this.db, 'orderRequests');
    
    // Create request document
    const docRef = await addDoc(requestsRef, {
      ...request,
      status: 'pending',
      createdAt: Timestamp.now(),
    });
    
    console.log(`[OrderArchive] Request created: ${docRef.id}`);
    
    // Wait for response (with timeout)
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        unsubscribe();
        reject(new Error('Request timed out (30s)'));
      }, 30000);
      
      const unsubscribe = onSnapshot(docRef, (snapshot) => {
        const data = snapshot.data();
        
        if (data?.status === 'completed') {
          clearTimeout(timeout);
          unsubscribe();
          console.log(`[OrderArchive] Request completed: ${docRef.id}`);
          resolve(data.result);
        } else if (data?.status === 'error') {
          clearTimeout(timeout);
          unsubscribe();
          reject(new Error(data.error || 'Unknown error'));
        }
        // Still processing - keep waiting
      });
    });
  }
}

// Export singleton
export const orderArchiveService = new OrderArchiveService();
```

### Running the Listener

```bash
# Start the order archive listener (alongside other listeners)
cd order_forecast
python scripts/order_archive_listener.py \
  --db data/analytics.duckdb \
  --service-account /path/to/serviceAccount.json

# Output:
# 🎧 Order Archive Listener started...
#    Database: data/analytics.duckdb
#    Listening for requests... (Ctrl+C to stop)
#
# 📥 Request: list_dates for route 989262
#    ✅ Completed (47 results)
#
# 📥 Request: get_order for route 989262
#    ✅ Completed (1 results)
```

### Cleanup: Auto-Delete Old Requests

```typescript
// Cloud Function to clean up old request documents
// functions/cleanupOrderRequests.ts

import * as functions from 'firebase-functions';
import * as admin from 'firebase-admin';

export const cleanupOrderRequests = functions.pubsub
  .schedule('every 24 hours')
  .onRun(async () => {
    const db = admin.firestore();
    const cutoff = admin.firestore.Timestamp.fromDate(
      new Date(Date.now() - 24 * 60 * 60 * 1000) // 24 hours ago
    );
    
    const oldRequests = await db.collection('orderRequests')
      .where('createdAt', '<', cutoff)
      .get();
    
    const batch = db.batch();
    oldRequests.docs.forEach(doc => batch.delete(doc.ref));
    await batch.commit();
    
    console.log(`Cleaned up ${oldRequests.size} old order requests`);
  });
```

---

## Promo Learning System

> **Source**: User uploads promo PDF → Firebase `promos/{route}/*`
> **Goal**: Learn per-promo user behavior, apply learned lift on promo recurrence

### Promo Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│  1. PROMO DETECTED (from user upload)                               │
│     └─▶ Flag affected items as promo_active=true                   │
│     └─▶ Apply default bump (e.g., +20% from baseline)              │
│     └─▶ Store promo metadata in promo_history table                │
├─────────────────────────────────────────────────────────────────────┤
│  2. USER REVIEWS & ADJUSTS                                          │
│     └─▶ Algorithm predicted 36 units (30 base + 20% promo bump)    │
│     └─▶ User changed to 60 units                                   │
│     └─▶ Correction stored WITH promo_id attached                   │
│     └─▶ promo_history.user_correction_avg updated                  │
├─────────────────────────────────────────────────────────────────────┤
│  3. SAME PROMO RETURNS (next quarter/year)                          │
│     └─▶ Algorithm looks up: "Last time promo X ran, user 2x'd"     │
│     └─▶ Predicts 60 units directly (learned lift)                  │
│     └─▶ promo_history.times_seen incremented                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Promo Data Structure (Firebase)

```typescript
// promos/{route}/{promoId}
{
  promoId: string,
  routeNumber: string,
  promoName: string,              // "Summer Tortilla Sale"
  startDate: string,              // "2025-06-01"
  endDate: string,                // "2025-06-30"
  affectedSaps: string[],         // ["28934", "22505", "33801"]
  discountPercent: number,        // 15 (means 15% off)
  uploadedAt: Timestamp,
  uploadedBy: string,             // userId
  
  // Source document (if from PDF)
  sourceFileName: string,
  sourceFileUrl: string,
}
```

### Promo Features for ML

```python
# Additional features when promo_active=true
PROMO_FEATURES = [
    "promo_active",               # Boolean: is there a promo?
    "promo_discount_percent",     # Size of discount (bigger = more lift)
    "promo_times_seen",           # How many times we've seen this promo before
    "promo_historical_lift",      # Avg qty increase when this promo ran before
    "promo_user_correction_avg",  # Did user historically adjust promo predictions?
    "days_into_promo",            # Early promo vs late promo behavior
    "days_until_promo_ends",      # Urgency factor
]
```

### Promo Learning Algorithm

```python
def compute_promo_adjusted_quantity(
    base_quantity: int,
    store_id: str,
    sap: str,
    promo: dict,
    promo_history: pd.DataFrame
) -> int:
    """
    Adjust base quantity based on promo and historical user behavior.
    """
    promo_id = promo.get('promoId')
    
    # Check if we've seen this promo before
    historical = promo_history[
        (promo_history['promo_id'] == promo_id) &
        (promo_history['store_id'] == store_id) &
        (promo_history['sap'] == sap)
    ]
    
    if len(historical) > 0:
        # Use learned lift from last time this promo ran
        learned_lift = historical['user_correction_avg'].mean()
        return int(base_quantity * learned_lift)
    else:
        # First time seeing this promo - use default bump
        discount = promo.get('discountPercent', 0)
        default_lift = 1.0 + (discount / 100) * 0.5  # 15% discount → 7.5% lift
        return int(base_quantity * default_lift)
```

### Promo Correction Storage

When user finalizes an order with promo items, corrections include promo context:

```python
# In forecast_corrections table
{
    "correction_id": "corr-123",
    "store_id": "walmart-456",
    "sap": "28934",
    "predicted_units": 36,
    "final_units": 60,
    "correction_ratio": 1.67,
    "promo_id": "summer-tortilla-2025",    # Links to promo
    "promo_active": True,
}
```

This allows querying:
```sql
-- What's the average correction ratio for promo X?
SELECT 
    promo_id,
    AVG(correction_ratio) as avg_lift,
    COUNT(*) as sample_count
FROM forecast_corrections
WHERE promo_id = 'summer-tortilla-2025'
GROUP BY promo_id;
```

---

## Intelligent Case Allocation (CORE FEATURE)

> **THIS IS THE CORE OF THE ALGORITHM** - Users split cases across stores constantly.
> The model MUST learn historical splitting patterns and replicate them.
> Outputting "1 full case per store" is WRONG and defeats the purpose.

### The Reality of Ordering

Users rarely order a full case for a single store. Instead, they:
1. Look at total demand across ALL stores for an item
2. Order enough FULL CASES to cover that demand
3. Split those cases across stores based on each store's needs

**Example - Item 31032 (casePack = 12)**:
```
Historical Pattern (what user actually orders):
┌─────────────────────┬─────────┬─────────┐
│ Store               │ Qty     │ Share   │
├─────────────────────┼─────────┼─────────┤
│ Kaysville Smith's   │ 8       │ 53%     │  ← Gets most
│ Farmington Smith's  │ 4       │ 27%     │
│ West Valley Smith's │ 3       │ 20%     │
├─────────────────────┼─────────┼─────────┤
│ TOTAL               │ 15      │ 100%    │
└─────────────────────┴─────────┴─────────┘

WRONG Approach (current):
  Kaysville: ceil(8/12)*12 = 12   ┐
  Farmington: ceil(4/12)*12 = 12  ├─▶ 36 units (3 cases) ❌ OVER-ORDER
  West Valley: ceil(3/12)*12 = 12 ┘

CORRECT Approach (case allocation):
  Total demand: 15 → Round to cases: ceil(15/12)*12 = 24 (2 cases)
  Allocate by share:
    Kaysville: 53% × 24 = 12.7 → 12
    Farmington: 27% × 24 = 6.5 → 6
    West Valley: 20% × 24 = 4.8 → 6 (gets remainder - round up)
  Result: 24 units (2 cases) ✅ CORRECT
```

### Algorithm: Case Allocation

```python
# scripts/case_allocator.py

def allocate_cases_across_stores(
    sap: str,
    store_predictions: dict[str, float],  # Raw predictions per store
    case_pack: int,
    historical_shares: dict[str, float],  # Each store's historical % share
) -> dict[str, int]:
    """
    Allocate full cases across stores based on historical share.
    
    Rules:
    1. Sum total predicted demand across ALL stores
    2. Round TOTAL to full cases (not per-store)
    3. Allocate units proportionally based on historical share
    4. Remainder goes to stores with highest share (they usually get more)
    5. Final output: full case quantities only, distributed across stores
    """
    # 1. Sum total predicted demand
    total_predicted = sum(store_predictions.values())
    
    if total_predicted == 0:
        return {s: 0 for s in store_predictions}
    
    # 2. Round total to full cases
    total_cases = math.ceil(total_predicted / case_pack)
    total_units = total_cases * case_pack
    
    # 3. Sort stores by historical share (descending)
    # Stores that usually get more will get priority for remainders
    sorted_stores = sorted(
        store_predictions.keys(),
        key=lambda s: historical_shares.get(s, 0),
        reverse=True
    )
    
    # 4. Allocate proportionally (floor first)
    allocations = {}
    allocated = 0
    remainders = {}  # Track fractional parts for remainder distribution
    
    for store in sorted_stores:
        share = historical_shares.get(store, 0)
        if share == 0 and store in store_predictions:
            # New store with no history - give equal share
            share = 1.0 / len(store_predictions)
        
        raw_alloc = share * total_units
        floor_alloc = int(raw_alloc)
        allocations[store] = floor_alloc
        allocated += floor_alloc
        remainders[store] = raw_alloc - floor_alloc
    
    # 5. Distribute remainder to stores with highest fractional parts
    # (tiebreaker: historical share)
    remainder = total_units - allocated
    stores_by_remainder = sorted(
        remainders.keys(),
        key=lambda s: (remainders[s], historical_shares.get(s, 0)),
        reverse=True
    )
    
    for store in stores_by_remainder:
        if remainder <= 0:
            break
        allocations[store] += 1
        remainder -= 1
    
    return allocations


def compute_historical_shares(
    conn: duckdb.DuckDBPyConnection,
    route_number: str,
    sap: str,
    schedule_key: str,
    lookback_orders: int = 12,  # Last 12 orders
) -> dict[str, float]:
    """
    Compute each store's historical share of this item.
    
    Returns: {'store_id': share} where shares sum to 1.0
    """
    query = """
    WITH recent_orders AS (
        SELECT DISTINCT order_id
        FROM orders_historical
        WHERE route_number = ?
          AND schedule_key = ?
        ORDER BY delivery_date DESC
        LIMIT ?
    ),
    store_totals AS (
        SELECT 
            li.store_id,
            SUM(li.quantity) as total_qty
        FROM order_line_items li
        JOIN recent_orders ro ON li.order_id = ro.order_id
        WHERE li.sap = ?
        GROUP BY li.store_id
    )
    SELECT 
        store_id,
        total_qty,
        total_qty * 1.0 / SUM(total_qty) OVER () as share
    FROM store_totals
    ORDER BY share DESC
    """
    
    rows = conn.execute(query, [route_number, schedule_key, lookback_orders, sap]).fetchall()
    
    return {row[0]: row[2] for row in rows}
```

### Database Support: Historical Shares Cache

Add to `feature_cache` table:

```sql
-- In feature_cache, add per-item share tracking
ALTER TABLE feature_cache ADD COLUMN IF NOT EXISTS 
    item_share FLOAT;  -- This store's % share of this item's total volume

-- New table for item-level aggregates (cross-store)
CREATE TABLE IF NOT EXISTS item_allocation_cache (
    id VARCHAR PRIMARY KEY,                 -- {route}-{sap}-{schedule}
    route_number VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    schedule_key VARCHAR NOT NULL,
    
    -- Aggregate demand across all stores
    total_avg_quantity FLOAT,               -- Average total demand per order
    total_quantity_stddev FLOAT,
    
    -- Store share breakdown (JSON)
    store_shares JSON,                      -- {"store_id": 0.53, ...}
    
    -- Splitting patterns
    avg_stores_per_order FLOAT,             -- How many stores typically get this item
    typical_split_pattern VARCHAR,          -- "even", "skewed", "single_store"
    
    updated_at TIMESTAMP,
    
    UNIQUE(route_number, sap, schedule_key)
);

CREATE INDEX idx_item_alloc_lookup 
ON item_allocation_cache(route_number, sap, schedule_key);
```

### Integration with Forecast Engine

```python
# In forecast_engine.py - REPLACE per-store prediction with allocation

class ForecastEngine:
    def generate_forecast(self, config: ForecastConfig) -> ForecastPayload:
        # ... load stores, history, catalog ...
        
        # Group predictions by SAP (item), not by store
        items_to_allocate: dict[str, dict[str, float]] = {}  # {sap: {store_id: predicted_qty}}
        
        for store in active_stores:
            for sap in store.active_items:
                if sap not in items_to_allocate:
                    items_to_allocate[sap] = {}
                
                # Get raw prediction for this store/sap
                raw_qty = self._predict_raw_quantity(store.store_id, sap, history)
                items_to_allocate[sap][store.store_id] = raw_qty
        
        # Now allocate cases across stores for each item
        forecast_items = []
        for sap, store_predictions in items_to_allocate.items():
            case_pack = catalog.get(sap, Product()).case_pack or 1
            
            # Get historical shares for this item
            shares = compute_historical_shares(
                self.duck_conn, 
                config.route_number, 
                sap, 
                config.schedule_key
            )
            
            # Allocate cases across stores
            allocations = allocate_cases_across_stores(
                sap=sap,
                store_predictions=store_predictions,
                case_pack=case_pack,
                historical_shares=shares,
            )
            
            # Create forecast items for non-zero allocations
            for store_id, units in allocations.items():
                if units > 0:
                    forecast_items.append(ForecastItem(
                        store_id=store_id,
                        store_name=stores_map[store_id].store_name,
                        sap=sap,
                        recommended_units=units,
                        recommended_cases=units // case_pack,
                        source="ml_allocated",
                    ))
        
        return ForecastPayload(items=forecast_items, ...)
```

### Key Principles

1. **Never round per-store** - Always aggregate first, then allocate
2. **Learn from history** - Historical shares are the source of truth
3. **Favor top stores** - When splitting remainders, stores that historically get more should get the extra units
4. **Output full cases only** - Final quantities must be divisible by casePack
5. **Handle new stores** - If a store has no history for an item, give it proportional share based on overall ordering pattern

---

## Delivery Day Splitting (Secondary)

In addition to case allocation, items must go to the correct order based on store delivery schedules:

**Scenario**: User ordering on Monday for Thursday delivery. Bowman's only gets deliveries on Friday.

```
Monday Order (Thursday delivery for most stores):
├── Sam's: gets full order (delivered Thursday)
├── Walmart: gets full order (delivered Thursday)
├── Bowman's: SKIP - not delivered Thursday
│
Tuesday Order (Friday delivery for Bowman's, Monday for others):
├── Bowman's: gets full order (delivered Friday)
├── Sam's: gets full order (delivered Monday)
└── Items that Bowman's needs from the split
```

```python
def assign_items_to_order(store, item, target_delivery_day, store_delivery_days):
    """
    Determine which order day an item should go on based on store's delivery schedule.
    """
    if target_delivery_day in store_delivery_days:
        return target_delivery_day
    else:
        return find_next_order_day_for_store(store, store_delivery_days)
```

---

## Data Structures

### User Configuration (Firebase)

```typescript
// users/{uid}/userSettings/notifications/scheduling
{
  orderCycles: [
    { orderDay: 1, loadDay: 3, deliveryDay: 4 },  // Mon order → Thu delivery
    { orderDay: 2, loadDay: 5, deliveryDay: 5 }   // Tue order → Fri delivery
  ]
}
```

### Store Configuration (Firebase)

```typescript
// routes/{route}/stores/{storeId}
{
  name: "Bowman's",
  deliveryDays: ["Friday"],        // When this store receives deliveries
  items: ["28934", "22505", ...]   // SAP codes this store carries
}
```

### Product Catalog (Firebase)

```typescript
// masterCatalog/{route}/products/{sap}
{
  sap: "28934",
  fullName: "Mission Soft Taco Flour 10ct",
  casePack: 15,      // CRITICAL: units per case for rounding
  brand: "Mission",
  category: "Flour"
}
```

### Historical Order (Firebase)

```typescript
// orders/{orderId}
{
  id: "order-989262-1765177200000",
  routeNumber: "989262",
  scheduleKey: "monday",           // CRITICAL: used for filtering
  expectedDeliveryDate: "12/08/2025",
  orderDate: "12/02/2025",
  status: "finalized",
  stores: [{
    storeId: "store_123",
    storeName: "Bowman's",
    items: [{ sap: "28934", quantity: 30 }]
  }]
}
```

### Forecast Output (Firebase)

```typescript
// forecasts/{route}/cached/{forecastId}
{
  forecastId: "uuid",
  routeNumber: "989262",
  deliveryDate: "2025-12-15",
  scheduleKey: "monday",
  generatedAt: Timestamp,
  expiresAt: Timestamp,            // 7-day TTL
  items: [{
    storeId: "store_123",
    storeName: "Bowman's",
    sap: "28934",
    recommendedUnits: 30,          // Case-rounded
    recommendedCases: 2,
    promoActive: false,
    confidence: 0.85,
    source: "baseline"
  }]
}
```

---

## Implementation Phases

### Phase 1: Firebase Data Layer

**Goal**: Load all user params from Firebase (no hardcoding)

#### 1.1 Data Classes
New file: `scripts/models.py`
```python
@dataclass
class OrderCycle:
    order_day: int      # 1=Mon, 7=Sun
    load_day: int
    delivery_day: int

@dataclass
class Store:
    id: str
    name: str
    items: list[str]           # SAP codes this store carries
    delivery_days: list[str]   # ["Monday", "Friday"]

@dataclass
class Product:
    sap: str
    case_pack: int
    brand: str
    category: str
    full_name: str

@dataclass
class ForecastItem:
    store_id: str
    store_name: str
    sap: str
    recommended_units: int
    recommended_cases: int
    confidence: float
    source: str
```

#### 1.2 Firebase Loader
New file: `scripts/firebase_loader.py`
```python
class FirebaseLoader:
    def __init__(self, service_account_path):
        # Initialize Firebase Admin SDK

    def load_user_schedule(self, user_id) -> list[OrderCycle]:
        """Load order cycles from users/{uid}/userSettings"""

    def load_stores(self, route_number) -> dict[str, Store]:
        """Load stores from routes/{route}/stores/*"""

    def load_catalog(self, route_number) -> dict[str, Product]:
        """Load catalog from masterCatalog/{route}/products/*"""

    def load_orders(self, route_number, schedule_key, days=90) -> list[Order]:
        """Load finalized orders filtered by scheduleKey"""
        # CRITICAL: Only returns orders matching the target schedule

    def load_feedback(self, route_number) -> list[Correction]:
        """Load user corrections from forecast_feedback/{route}/*"""
```

### Phase 2: Schedule-Aware Forecast Engine

**Goal**: Generate forecasts that respect delivery schedules

#### 2.1 Core Algorithm
New file: `scripts/forecast_engine.py`
```python
class ForecastEngine:
    def __init__(self, loader: FirebaseLoader):
        self.loader = loader

    def generate_forecast(
        self,
        route_number: str,
        user_id: str,
        target_delivery_date: date,
        schedule_key: str
    ) -> Forecast:
        # 1. Load stores and filter by delivery day
        stores = self.loader.load_stores(route_number)
        active_stores = self._filter_stores_for_delivery(stores, target_delivery_date)

        # 2. Load history (ONLY matching scheduleKey)
        history = self.loader.load_orders(route_number, schedule_key)

        # 3. Load catalog for case packs
        catalog = self.loader.load_catalog(route_number)

        # 4. Compute demand per store/SAP
        demand = self._compute_demand(history)

        # 5. Allocate items (only to active stores)
        forecast_items = self._allocate_items(demand, active_stores, catalog)

        # 6. Round to cases
        forecast_items = self._round_to_cases(forecast_items, catalog)

        return Forecast(items=forecast_items)

    def _filter_stores_for_delivery(self, stores, delivery_date):
        """Only include stores that actually get delivered on this day"""
        delivery_day_name = delivery_date.strftime('%A')  # "Monday", "Friday"
        return {
            sid: store for sid, store in stores.items()
            if delivery_day_name in store.delivery_days
        }

    def _round_to_cases(self, items, catalog):
        """Round all quantities up to complete cases"""
        for item in items:
            case_pack = catalog.get(item.sap, {}).case_pack or 1
            if case_pack > 0:
                item.recommended_cases = math.ceil(item.recommended_units / case_pack)
                item.recommended_units = item.recommended_cases * case_pack
        return items
```

### Phase 3: Output & Caching

**Goal**: Write forecasts to Firebase for instant app retrieval

#### 3.1 Firebase Writer
New file: `scripts/firebase_writer.py`
```python
class FirebaseWriter:
    def write_forecast(self, route: str, forecast: Forecast):
        """Write to forecasts/{route}/cached/{forecastId}"""
        doc_ref = db.collection('forecasts').document(route)\
                    .collection('cached').document(forecast.id)
        doc_ref.set({
            'forecastId': forecast.id,
            'routeNumber': route,
            'deliveryDate': forecast.delivery_date.isoformat(),
            'scheduleKey': forecast.schedule_key,
            'generatedAt': firestore.SERVER_TIMESTAMP,
            'expiresAt': datetime.now() + timedelta(days=7),
            'items': [item.to_dict() for item in forecast.items]
        })
```

#### 3.2 CLI Runner
New file: `scripts/run_forecast.py`
```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--route', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--service-account', required=True)
    parser.add_argument('--target-date', required=True)
    parser.add_argument('--schedule', required=True)  # monday, thursday, etc.
    args = parser.parse_args()

    loader = FirebaseLoader(args.service_account)
    engine = ForecastEngine(loader)
    writer = FirebaseWriter(args.service_account)

    forecast = engine.generate_forecast(
        args.route,
        args.user,
        datetime.strptime(args.target_date, '%Y-%m-%d').date(),
        args.schedule
    )

    writer.write_forecast(args.route, forecast)
    print(f"Generated forecast for {args.target_date} ({args.schedule})")
```

### Phase 4: ML Feedback Loop & Training

**Goal**: Learn from user corrections to improve prediction accuracy over time

**Strategy**: Retrain model after each finalized order using warm-start incremental learning

#### 4.1 The Feedback Loop (Why It Matters)

```
WITHOUT FEEDBACK (Current - Open Loop):
┌──────────────┐    ┌──────────┐    ┌──────────┐    ┌─────────────┐
│ Historical   │───▶│ ML Model │───▶│ Forecast │───▶│ User Edits  │
│ Orders       │    └──────────┘    └──────────┘    │ (LOST!)     │
└──────────────┘                                     └─────────────┘

WITH FEEDBACK (Target - Closed Loop):
┌──────────────┐    ┌──────────┐    ┌──────────┐    ┌─────────────┐
│ Historical   │───▶│ ML Model │───▶│ Forecast │───▶│ User Edits  │
│ Orders       │    └──────────┘    └──────────┘    └──────┬──────┘
└──────────────┘         ▲                                 │
                         │         ┌──────────────┐        │
                         └─────────│ Corrections  │◀───────┘
                                   │ Database     │
                                   └──────────────┘
```

**Example**: Algorithm predicts 2 cases of tortillas for Walmart. User always bumps to 4 cases.
- Without feedback: Next week predicts 2 again (repeats mistake)
- With feedback: System learns "Walmart tortillas = 2x prediction" and auto-adjusts

#### 4.2 Types of User Corrections

| Type | Example | ML Signal |
|------|---------|-----------|
| **Quantity Adjustment** | Predicted 30 units, user changed to 45 | correction_ratio = 1.5 |
| **Item Removal** | Algorithm suggested item, user deleted | removal_rate increases |
| **No Change** | User accepted prediction as-is | High confidence signal |

**Note**: Users cannot add items not in `store.items[]` - item additions are NOT a correction type.

#### 4.3 Feedback Data Structure (Firebase)

```typescript
// forecast_feedback/{route}/{feedbackId}
{
  feedbackId: string,
  forecastId: string,           // Links to the forecast being corrected
  orderId: string,              // The finalized order
  routeNumber: string,
  scheduleKey: string,          // "monday" | "thursday"
  deliveryDate: string,
  submittedAt: Timestamp,
  
  corrections: [{
    storeId: string,
    storeName: string,
    sap: string,
    
    // The prediction
    predictedUnits: number,
    predictedCases: number,
    
    // What the user actually submitted
    finalUnits: number,
    finalCases: number,
    
    // Derived metrics for ML
    correctionDelta: number,     // finalUnits - predictedUnits
    correctionRatio: number,     // finalUnits / predictedUnits (1.0 = no change)
    wasRemoved: boolean,         // Item was predicted, user deleted it
  }],
  
  // Aggregate metrics
  summary: {
    totalItems: number,
    itemsCorrected: number,
    itemsRemoved: number,
    avgCorrectionRatio: number,
    correctionRate: number,      // % of items that were changed
  }
}
```

#### 4.4 Enhanced Feature Set

Extend `baseline_model.py` features with correction-derived signals:

```python
# EXISTING features (from baseline_model.py)
BASELINE_FEATURES = [
    "lag_1",                    # Last order quantity
    "lag_2",                    # Order before last
    "rolling_mean_4",           # 4-order rolling average
    "promo_active",             # Is there a promotion?
    "delivery_dow",             # Day of week (0-6)
    "delivery_month",           # Month (1-12)
    "is_monday_delivery",       # Monday delivery flag
    "lead_time_days",           # Days between order and delivery
    "case_count",               # Cases per unit
    "tray",                     # Tray configuration
]

# NEW: Correction-derived features
CORRECTION_FEATURES = [
    "avg_correction_ratio",     # Mean of (final/predicted) for this store+sap
    "correction_std",           # Volatility - how consistent are corrections?
    "last_correction_delta",    # Most recent correction amount
    "removal_rate",             # % of times user removes this item (0.0-1.0)
    "correction_trend",         # Is user correcting more or less over time?
    "weeks_since_correction",   # Recency of last user edit
]

# Combined feature set for training
ALL_FEATURES = BASELINE_FEATURES + CORRECTION_FEATURES
```

#### 4.5 Incremental Training Strategy

Since retraining occurs after each finalized order, use **warm-start** to avoid full retraining:

```python
# scripts/incremental_trainer.py
from sklearn.ensemble import GradientBoostingRegressor
import joblib

class IncrementalTrainer:
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.model = self._load_or_create_model()
    
    def _load_or_create_model(self):
        try:
            return joblib.load(self.model_path)
        except FileNotFoundError:
            return GradientBoostingRegressor(
                warm_start=True,        # Continue from existing trees
                n_estimators=100,       # Start with 100 trees
                max_depth=4,
                learning_rate=0.1,
                random_state=42
            )
    
    def train_incremental(self, X_new, y_new):
        """Add new trees based on latest corrections."""
        # Add 10 more trees per training cycle
        self.model.n_estimators += 10
        self.model.fit(X_new, y_new)
        return self.model
    
    def save_model(self):
        joblib.dump(self.model, self.model_path)
```

#### 4.6 Model Validation Gate

Before deploying a retrained model, validate it doesn't regress:

```python
# scripts/model_validator.py
from sklearn.metrics import mean_absolute_error

class ModelValidator:
    def __init__(self, min_improvement: float = 0.05):
        self.min_improvement = min_improvement  # 5% threshold
    
    def should_deploy(self, new_model, current_model, X_val, y_val) -> bool:
        """Only deploy if new model is at least as good as current."""
        new_mae = mean_absolute_error(y_val, new_model.predict(X_val))
        current_mae = mean_absolute_error(y_val, current_model.predict(X_val))
        
        # Deploy if improvement OR not significantly worse (within 5%)
        if new_mae <= current_mae * (1 + self.min_improvement):
            return True
        
        print(f"Model rejected: new MAE {new_mae:.3f} > current {current_mae:.3f}")
        return False
```

#### 4.7 Feedback Collector

```python
# scripts/feedback_collector.py
from firebase_admin import firestore

class FeedbackCollector:
    def __init__(self, db):
        self.db = db
    
    def get_correction_features(self, route: str, store: str, sap: str) -> dict:
        """Compute aggregated correction features for a store/sap pair."""
        corrections = (
            self.db.collection('forecast_feedback')
            .document(route)
            .collection('corrections')
            .where('storeId', '==', store)
            .where('sap', '==', sap)
            .order_by('submittedAt', direction='DESCENDING')
            .limit(20)  # Last 20 corrections
            .get()
        )
        
        if not corrections:
            return {
                'avg_correction_ratio': 1.0,
                'correction_std': 0.0,
                'last_correction_delta': 0,
                'removal_rate': 0.0,
                'correction_trend': 0.0,
                'weeks_since_correction': 52,  # Default to 1 year
            }
        
        ratios = [c.get('correctionRatio', 1.0) for c in corrections]
        deltas = [c.get('correctionDelta', 0) for c in corrections]
        removals = [1 if c.get('wasRemoved') else 0 for c in corrections]
        
        return {
            'avg_correction_ratio': np.mean(ratios),
            'correction_std': np.std(ratios),
            'last_correction_delta': deltas[0] if deltas else 0,
            'removal_rate': np.mean(removals),
            'correction_trend': self._compute_trend(ratios),
            'weeks_since_correction': self._weeks_since(corrections[0]),
        }
```

#### 4.8 ML Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AFTER EACH FINALIZED ORDER                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  1. App submits order + corrections to Firebase                     │
│     └─▶ forecast_feedback/{route}/{feedbackId}                      │
│                                                                      │
│  2. Feedback Collector pulls corrections                            │
│     └─▶ Computes aggregate features per store/sap                   │
│                                                                      │
│  3. Incremental Trainer runs                                        │
│     └─▶ warm_start=True, adds 10 trees                             │
│                                                                      │
│  4. Model Validator checks performance                              │
│     └─▶ If passes: deploy to models/{route}/{scheduleKey}/         │
│     └─▶ If fails: keep current model                               │
│                                                                      │
│  5. Forecast Generator uses updated model                           │
│     └─▶ Next forecast benefits from latest corrections             │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

#### 4.9 Model Storage (Firebase Storage)

```
models/{route}/{scheduleKey}/
├── current.pkl              # Active model used for predictions
├── metadata.json            # Training stats, feature importance, last updated
└── history/
    ├── 2025-12-10.pkl       # Previous versions for rollback
    ├── 2025-12-08.pkl
    └── 2025-12-05.pkl
```

**metadata.json**:
```json
{
  "lastTrainedAt": "2025-12-10T14:30:00Z",
  "trainingRows": 1847,
  "validationMAE": 2.34,
  "featureImportance": {
    "lag_1": 0.35,
    "avg_correction_ratio": 0.22,
    "rolling_mean_4": 0.15,
    "correction_trend": 0.08
  },
  "version": 47,
  "scheduleKey": "monday"
}
```

#### 4.10 New Files for Phase 4

| File | Purpose |
|------|---------|
| `scripts/feedback_collector.py` | Pull corrections from Firebase, compute aggregate features |
| `scripts/incremental_trainer.py` | Warm-start retraining after each order |
| `scripts/model_validator.py` | Gate deployments on performance check |
| `scripts/training_pipeline.py` | Orchestrates the full ML pipeline |

---

## File Structure

```
order_forecast/
├── ALGORITHM_PLAN.md              # This document
├── requirements.txt               # Python dependencies
│
├── scripts/                       # ML Pipeline (runs on local Mac)
│   ├── models.py                  # Data classes
│   ├── db_schema.py               # DuckDB table definitions
│   ├── db_sync.py                 # Firebase → DuckDB sync (batch + incremental)
│   ├── order_archive_listener.py  # MESSAGE BUS: Listen for archive requests
│   ├── firebase_loader.py         # Load from Firestore (for app data)
│   ├── firebase_writer.py         # Write forecasts to Firestore
│   ├── forecast_engine.py         # Core forecast algorithm
│   ├── feedback_collector.py      # Pull corrections, compute features
│   ├── incremental_trainer.py     # Warm-start model retraining
│   ├── model_validator.py         # Gate deployments on performance
│   ├── training_pipeline.py       # Orchestrates full ML pipeline
│   ├── run_forecast.py            # CLI entry point
│   ├── baseline_model.py          # EXISTING: ML model definition
│   ├── generate_forecast.py       # EXISTING: Template generator
│   ├── mission_orders.py          # EXISTING: CSV parser
│   └── promotion_parser.py        # EXISTING: PDF promo parser
│
├── data/
│   ├── analytics.duckdb           # LOCAL ANALYTICAL DATABASE
│   │                              # Contains: orders, corrections, stores,
│   │                              # items, promos, seasonal patterns, etc.
│   └── (config files, sample CSVs)
│
├── models/
│   └── {route}/
│       └── {scheduleKey}/
│           ├── current.pkl        # Active trained model
│           ├── metadata.json      # Training stats, feature importance
│           └── history/           # Previous model versions (for rollback)
│
├── parsed_orders_batch_no_catalog/
│   └── *.json                     # Imported order files
│
└── tests/
    └── (unit tests for ML pipeline)

# In main project (restore-2025-09-24/):
functions/
├── sync_to_analytics.py           # Cloud Function listeners
│   ├── on_order_finalized         # → Sync order + corrections
│   ├── on_store_changed           # → Sync store config
│   ├── on_store_item_changed      # → Sync item list changes
│   ├── on_catalog_changed         # → Sync product catalog
│   ├── on_schedule_changed        # → Sync user schedules
│   └── on_promo_uploaded          # → Sync promo data
└── ...

src/services/
├── ForecastManager.ts             # App-side forecast integration
│   ├── getCachedForecast()        # Read from Firestore
│   ├── submitFeedback()           # Submit corrections
│   └── generateForecast()         # Trigger forecast generation
│
└── OrderArchiveService.ts         # MESSAGE BUS: Request archived orders
    ├── listArchivedDates()        # Request available dates from backend
    ├── getArchivedOrder()         # Request full order from backend
    └── searchArchivedOrders()     # Search orders by criteria
```

### Data Flow Summary

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA FLOW OVERVIEW                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  USER ACTION              FIREBASE                 DUCKDB            │
│  ───────────              ────────                 ──────            │
│                                                                      │
│  Finalize Order    ──▶   orders/{id}       ──▶   orders_historical  │
│                          (90-day TTL)             (permanent)        │
│                                                                      │
│  Edit Store        ──▶   routes/.../stores ──▶   stores             │
│                                                   store_items        │
│                                                                      │
│  Upload Promo      ──▶   promos/{route}/*  ──▶   promo_history      │
│                                                   promo_items        │
│                                                                      │
│  Change Schedule   ──▶   userSettings      ──▶   user_schedules     │
│                                                                      │
│  Correction        ──▶   (via order)       ──▶   forecast_corrections│
│  (implicit)                                       seasonal_adjustments│
│                                                                      │
│  ─────────────────────────────────────────────────────────────────  │
│                                                                      │
│  VIEW OLD ORDER   (Message Bus Pattern)                             │
│  ──────────────                                                     │
│  App writes       ──▶   orderRequests/{id}                          │
│  (status:pending)       (type: list_dates)                          │
│                              │                                       │
│                              │ Mac listener detects                  │
│                              ▼                                       │
│                         Mac queries DuckDB                          │
│                              │                                       │
│                              │ Mac writes response                   │
│                              ▼                                       │
│  App reads        ◀──   orderRequests/{id}                          │
│  (status:completed)     result: { dates: [...] }                    │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Execution Order

```
Phase 1: Firebase Data Layer
├── 1.1 Create models.py (data classes)
├── 1.2 Create firebase_loader.py
├── 1.3 Test: can load schedule, stores, catalog, orders
│
Phase 2: Forecast Engine
├── 2.1 Create forecast_engine.py
├── 2.2 Implement schedule filtering (store.deliveryDays)
├── 2.3 Implement store item filtering (store.items[])
├── 2.4 Implement case rounding (catalog.casePack)
├── 2.5 Test: generate forecast for route 989262
│
Phase 3: Output & Caching
├── 3.1 Create firebase_writer.py
├── 3.2 Create run_forecast.py CLI
├── 3.3 Test: write forecast, read from app
│
Phase 4: ML Training (Deferred)
├── 4.1 Create training_builder.py
├── 4.2 Extend baseline_model.py
├── 4.3 Train per-schedule models
└── 4.4 Integrate trained models into engine
```

---

## Testing

### Integration Test (Route 989262)
```bash
# Generate forecast for Monday delivery (Dec 15)
python scripts/run_forecast.py \
  --route 989262 \
  --user <your-uid> \
  --service-account /path/to/serviceAccount.json \
  --target-date 2025-12-15 \
  --schedule monday
```

### Validation Criteria
- [ ] Forecast only includes stores delivered on target day
- [ ] Stores not delivered that day are excluded (e.g., Bowman's on Monday)
- [ ] All quantities rounded to case packs
- [ ] Only SAPs in store.items[] appear for each store
- [ ] scheduleKey filtering works (Mon history → Mon forecast)

---

## Success Metrics

| Metric | Target |
|--------|--------|
| Pre-fill latency | < 500ms (Firestore read only) |
| Schedule isolation | Mon/Thu forecasts use ONLY matching history |
| Case completion | 100% of quantities divisible by casePack |
| Store filtering | Only items in store.items[] appear |
| Multi-user ready | Works for any route with Firebase data |

---

## Open Items (Deferred)

1. ~~**Promo Integration**: Add when promo data source is defined~~ ✅ DEFINED - See "Promo Learning System" section
2. **Cloud Function Trigger**: Move to CF after local version works
3. **Real-time Updates**: Pub/Sub for immediate recompute on changes
4. **Model Serving**: Deploy trained models to Cloud for inference
5. **PostgreSQL Migration**: Migrate from DuckDB to Cloud SQL when multi-machine access needed
6. **Promo PDF Parser Enhancement**: Improve `promotion_parser.py` to extract promo metadata automatically
7. **A/B Testing Framework**: Compare model versions in production before full rollout

---

## Delivery Manifest vs Order View (NEW FEATURE)

> **Problem**: Orders are submitted by schedule (Monday/Tuesday), but deliveries happen by store schedule.
> Items ordered on Monday may be delivered to Bowman's on Friday.
> When writing an invoice at a store, user needs to see "what's being delivered here" not "what was in this order."

### Core Distinction

| Concept | Definition | Example |
|---------|------------|---------|
| **Order** | What you submitted to the supplier | Monday 12/8: "I ordered 6 cases of 41095" |
| **Allocation** | Where each unit goes | "3 cases → Harmon's (Thu), 3 cases → Bowman's (Fri)" |
| **Delivery Load** | What's on the truck for a specific store/day | "Friday Bowman's: 3x 41095, 2x 52967" |

### Why This Matters

The forecast algorithm predicts **store-level demand**, but you **order at the route level**. When you order 6 cases:
- The algorithm said: Harmon's needs 3, Bowman's needs 3
- You submitted 6 total on Monday's order
- But delivery is split: Harmon's gets theirs Thursday, Bowman's gets theirs Friday

### What the App Needs

**View 1: Order History (exists)**
> "What did I submit on Monday's order?"
```
Monday 12/8 Order:
  41095: 6 cases (total ordered)
  52967: 2 cases (total ordered)
```

**View 2: Delivery Manifest (NEW)**
> "What am I delivering to each store TODAY?"
```
Thursday 12/11 Deliveries:
  Harmon's: 41095 (3), 52967 (8)
  Kaysville: ...
  
Friday 12/12 Deliveries:
  Bowman's: 41095 (3), 52967 (2)  ← from Monday's order!
```

### Database Design

```
┌─────────────────────────────────────────────────────────────┐
│ order_line_items (existing)                                  │
│   order_id, sap, store_id, quantity ← What store gets        │
│   delivery_date ← From order header                          │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼ (one-to-many for case splits)
┌─────────────────────────────────────────────────────────────┐
│ delivery_allocations (NEW)                                   │
│   allocation_id       PK                                     │
│   route_number        Which route                            │
│   source_order_id     Which order it was purchased in        │
│   source_order_date   When it was ordered                    │
│   sap                 Product                                │
│   store_id            Destination store                      │
│   store_name          For display                            │
│   quantity            Units allocated to this store          │
│   delivery_date       When it's ACTUALLY delivered           │
│   is_case_split       TRUE if different from order's primary │
│   created_at          Timestamp                              │
└─────────────────────────────────────────────────────────────┘
```

### Allocation Logic

Allocations are created automatically when an order is finalized:

```python
def create_delivery_allocations(order, store_configs):
    """
    For each store+item in the order, determine actual delivery date
    based on store's configured delivery day.
    """
    allocations = []
    primary_delivery_day = get_weekday(order.expected_delivery_date)
    
    for store in order.stores:
        store_delivery_day = get_store_delivery_day(store.id, store_configs)
        actual_delivery_date = calculate_next_delivery(
            order.order_date, 
            store_delivery_day
        )
        
        for item in store.items:
            allocations.append({
                'source_order_id': order.id,
                'source_order_date': order.order_date,
                'sap': item.sap,
                'store_id': store.id,
                'store_name': store.name,
                'quantity': item.quantity,
                'delivery_date': actual_delivery_date,
                'is_case_split': store_delivery_day != primary_delivery_day
            })
    
    return allocations
```

### Bowman's Special Case

Bowman's is ordered Tuesday but delivered Friday (same day as load pickup):

```
Tuesday Order (primary delivery = Monday):
  - Harmon's items → delivery_date = Monday, is_case_split = FALSE
  - Kaysville items → delivery_date = Monday, is_case_split = FALSE  
  - Bowman's items → delivery_date = Friday, is_case_split = TRUE
```

### Use Cases

**Use Case 1: At the store writing invoice**
```
User at Bowman's on Friday:
App shows: "Bowman's Friday Delivery"
  - 41095: 3 units (from Monday order)
  - 52967: 2 units (from Monday order)

User taps item → sees: "Ordered: Mon 12/8, Delivered: Fri 12/12"
```

**Use Case 2: Loading the truck**
```
User preparing Friday deliveries:
App shows: "Friday 12/12 Truck Load"
  - Bowman's: 41095 (3), 52967 (2)
  - Other Friday stores: ...
  - Total: X cases across Y stores
```

**Use Case 3: Reviewing what was case-split**
```
User reviewing Monday order:
App shows: "Monday 12/8 Order"
  - 41095: 6 cases total
    └─ Harmon's: 3 (Thu 12/11) ✓ delivered
    └─ Bowman's: 3 (Fri 12/12) ← case split
```

### Implementation Tasks

**Claude 2 (Database)**:

| Task | File | Description |
|------|------|-------------|
| 1. Add delivery_allocations table | `scripts/db_schema.py` | New table for case split tracking |
| 2. Backfill existing data | `scripts/backfill_allocations.py` | Populate from existing order_line_items |

**ChatGPT (Python Backend)**:

| Task | File | Description |
|------|------|-------------|
| 3. Record allocations on order sync | `scripts/order_sync_listener.py` | When syncing order, also create allocations |
| 4. Add delivery manifest query | `scripts/db_manager.py` | New request type: `get_delivery_manifest` |

**Claude 1 (App UI)**:

| Task | File | Description |
|------|------|-------------|
| 5. Delivery Manifest Service | `src/services/DeliveryManifestService.ts` | Request manifest from backend |
| 6. Delivery Manifest Screen | `app/(app)/delivery/manifest.tsx` | Show "what's going to each store today" |
| 7. Invoice Pre-fill | Update invoice screen | Pre-fill from delivery manifest, not order |

### Query Examples

```sql
-- Get Bowman's Friday delivery (including case splits from Monday order)
SELECT 
    da.sap,
    pc.full_name as product_name,
    da.quantity,
    da.source_order_id,
    da.source_order_date,
    da.is_case_split
FROM delivery_allocations da
LEFT JOIN product_catalog pc ON da.sap = pc.sap AND da.route_number = pc.route_number
WHERE da.store_id = 'store_1762926930357'  -- Bowman's
  AND da.delivery_date = '2025-12-12'       -- Friday
  AND da.route_number = '989262'
ORDER BY pc.full_name;

-- Get full truck manifest for a delivery date
SELECT 
    da.store_id,
    da.store_name,
    da.sap,
    da.quantity,
    da.is_case_split,
    da.source_order_date
FROM delivery_allocations da
WHERE da.route_number = '989262'
  AND da.delivery_date = '2025-12-12'
ORDER BY da.store_name, da.sap;
```

### Key Principles

1. **Order = what you submitted** - Never changes after submission
2. **Allocation = where it goes** - Links order items to delivery destinations
3. **Case splits are tracked** - User can see "this came from Monday's order"
4. **Invoice uses delivery manifest** - Not order history
5. **Backward compatible** - Existing order_line_items unchanged
