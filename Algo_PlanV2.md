# Order Forecast Algorithm - Implementation Plan V2

> **Status**: Active Development  
> **Last Updated**: December 14, 2025  
> **Pilot Route**: 989262  
> **Previous Version**: [docs/archive/ALGORITHM_PLAN.md](docs/archive/ALGORITHM_PLAN.md) (archived - 2800 lines)

---

## Task Assignments (Multi-Agent Collaboration)

> **IMPORTANT**: Each agent works on their assigned tasks ONLY. Do not modify files assigned to other agents.

---

### Completed Tasks Summary

#### ChatGPT - Python Backend (11 tasks complete)

| Task | File | Status |
|------|------|--------|
| Data Classes | `scripts/models.py` | ✅ |
| Firebase Loader | `scripts/firebase_loader.py` | ✅ |
| Firebase Writer | `scripts/firebase_writer.py` | ✅ |
| Feedback Collector | `scripts/feedback_collector.py` | ✅ |
| Baseline Model | `scripts/baseline_model.py` | ✅ |
| Incremental Trainer | `scripts/incremental_trainer.py` | ✅ |
| Model Validator | `scripts/model_validator.py` | ✅ |
| Forecast Engine | `scripts/forecast_engine.py` | ✅ |
| Training Pipeline | `scripts/training_pipeline.py` | ✅ |
| CLI Runner | `scripts/run_forecast.py` | ✅ |
| Order Archive Listener | `scripts/order_archive_listener.py` | ✅ |
| Case Allocator | `scripts/case_allocator.py` | ✅ |
| Delivery Day Filtering | `scripts/forecast_engine.py` | ✅ |

#### Claude 1 - App UI (9 tasks complete)

| Task | File | Status |
|------|------|--------|
| Order Archive Service | `src/services/OrderArchiveService.ts` | ✅ |
| Forecast Manager | `src/services/ForecastManager.ts` | ✅ |
| Order History Screen | `app/(app)/order-actions/history.tsx` | ✅ |
| Archive Date Picker | `src/components/order/ArchiveDatePicker.tsx` | ✅ |
| Delivery Manifest Screen | `app/(app)/order-actions/manifest/index.tsx` | ✅ |
| Store Delivery Detail | `app/(app)/order-actions/manifest/[storeId].tsx` | ✅ |
| Manifest Layout | `app/(app)/order-actions/manifest/_layout.tsx` | ✅ |

#### Claude 2 - Database (8 tasks complete)

| Task | File | Status |
|------|------|--------|
| DB Schema | `scripts/db_schema.py` | ✅ |
| DB Sync | `scripts/db_sync.py` | ✅ |
| Calendar Features | `scripts/calendar_features.py` | ✅ |
| Initial Data Load | `scripts/load_json_orders.py` | ✅ |
| Test Queries | `scripts/test_db_queries.py` | ✅ |
| DB Manager Service | `scripts/db_manager.py` | ✅ |
| DB Client | `scripts/db_client.py` | ✅ |
| Delivery Allocations | `scripts/db_schema.py` (table) | ✅ |

---

## Active Tasks - Promo Email Ingestion System

### Overview

Automate promo import from email inbox:
1. User configures email inbox in app settings
2. Local daemon monitors inbox via IMAP
3. Extracts Excel/PDF attachments
4. Parses promo data, matches SAPs to catalog
5. Imports to DuckDB, syncs to Firebase
6. Tracks user ordering behavior during promos for ML

### Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Email Inbox    │────▶│  Promo Listener  │────▶│    DuckDB       │
│  (IMAP)         │     │  (Local Daemon)  │     │  promo_history  │
└─────────────────┘     └──────────────────┘     │  promo_items    │
                               │                  │  promo_orders   │
                               ▼                  └─────────────────┘
                        ┌──────────────────┐              │
                        │  SAP Matcher     │              ▼
                        │  (Fuzzy Match)   │     ┌─────────────────┐
                        └──────────────────┘     │    Firebase     │
                                                 │  promos/active  │
                                                 └─────────────────┘
```

---

### Claude 2 - Database Tasks

| # | Task | File | Status |
|---|------|------|--------|
| 1 | Add promo_order_history table | `scripts/db_schema.py` | ✅ Done |
| 2 | Add promo_email_queue table | `scripts/db_schema.py` | ✅ Done |
| 2b | Add sap_corrections table | `scripts/db_schema.py` | ✅ Done |
| 3 | Update order sync for promo linking | `scripts/order_sync_listener.py` | ✅ Done |

**promo_order_history schema:**
```sql
CREATE TABLE promo_order_history (
    id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    promo_id VARCHAR NOT NULL,
    order_id VARCHAR NOT NULL,
    store_id VARCHAR NOT NULL,
    sap VARCHAR NOT NULL,
    promo_price FLOAT,
    quantity_ordered INTEGER NOT NULL,
    baseline_quantity FLOAT,
    quantity_lift FLOAT,
    weeks_into_promo INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**promo_email_queue schema:**
```sql
CREATE TABLE promo_email_queue (
    email_id VARCHAR PRIMARY KEY,
    route_number VARCHAR NOT NULL,
    subject VARCHAR,
    received_at TIMESTAMP,
    attachment_name VARCHAR,
    status VARCHAR DEFAULT 'pending',
    items_imported INTEGER,
    processed_at TIMESTAMP
);
```

---

### ChatGPT - Python Backend Tasks

| # | Task | File | Status |
|---|------|------|--------|
| 4 | Create promo_email_listener.py | `scripts/promo_email_listener.py` | ✅ Done |
| 5 | Enhance promo_parser.py | `scripts/promo_parser.py` | ✅ Done |
| 6 | Create sap_matcher.py | `scripts/sap_matcher.py` | ✅ Done |
| 7 | Update order_sync for promo tracking | `scripts/order_sync_listener.py` | ✅ Done |
| 8 | Add to supervisor.py | `supervisor.py` | ✅ Done |

**promo_email_listener.py requirements:**
- Poll IMAP inbox every 5 minutes
- Support Gmail, Outlook, any IMAP server
- Extract .xlsx and .pdf attachments
- Queue for processing, move to archive folder
- Handle errors gracefully

**sap_matcher.py requirements:**
- Direct SAP match (catalog lookup)
- Fuzzy description match (for wrong SAPs like 24383→51531)
- Store correction mappings for future imports
- Return confidence score

---

### Claude 1 - App UI Tasks

| # | Task | File | Status |
|---|------|------|--------|
| 9 | Promo Settings Screen | `app/(app)/settings/promos.tsx` | ✅ Done |
| 10 | Account Mapping UI | `app/(app)/settings/promo-accounts.tsx` | ✅ Done |
| 11 | Promo Queue Status | `app/(app)/promos/queue.tsx` | ✅ Done |
| 12 | Promo History View | `app/(app)/promos/history.tsx` | ✅ Done |

**Firebase schema for promo settings:**
```typescript
// users/{uid}/promoSettings
{
  enabled: boolean,
  emailAddress: string,
  imapServer: string,
  imapPort: number,
  imapUsername: string,
  imapPassword: string,  // encrypted
  folderToWatch: string,
  processedFolder: string,
  autoMatch: boolean,
  accountMappings: {
    "Smiths": ["store_farmington", "store_kaysville"],
    "Harmon's": ["store_harmons"]
  }
}
```

---

## Quick Reference

### Database Responsibility Split

| Data Type | Firebase | DuckDB |
|-----------|----------|--------|
| Recent orders (90 days) | ✅ TTL | ✅ permanent |
| Historical orders | ❌ | ✅ message bus |
| User corrections | ❌ | ✅ |
| Store configs | ✅ live | ✅ versioned |
| Promos | ✅ active | ✅ with metrics |
| Promo order history | ❌ | ✅ |

### Core Principles

1. **Case Allocation**: Sum demand across stores → round TOTAL to cases → allocate by historical share
2. **Schedule Filtering**: Monday orders ≠ Thursday orders (separate time series)
3. **Delivery Day Filtering**: Only forecast stores that receive delivery on target day
4. **Promo Learning**: Track what user orders during promos → predict lift for future promos

### Key Files

| Purpose | File |
|---------|------|
| ML Model | `scripts/baseline_model.py` |
| Forecast Engine | `scripts/forecast_engine.py` |
| DB Schema | `scripts/db_schema.py` |
| DB Manager | `scripts/db_manager.py` |
| Order Sync | `scripts/order_sync_listener.py` |
| Supervisor | `supervisor.py` |
| App Forecast | `src/services/ForecastManager.ts` |

### Running Services

```bash
# Start all backend services
cd order_forecast
python supervisor.py start -d

# Check status
python supervisor.py status

# View logs
python supervisor.py logs
```

---

## ML Training Status

| Schedule | Orders | Status |
|----------|--------|--------|
| Monday | 6 | Need 2 more for training |
| Tuesday | 7 | Need 1 more for training |

**Minimum thresholds:**
- Forecast generation: 4 orders
- Model training: 8 orders per schedule

---

## Status: Promo Email Ingestion COMPLETE

All tasks finished on Dec 14, 2025:

| Agent | Tasks | Status |
|-------|-------|--------|
| Claude 2 | Database tables (3) + order sync | ✅ Complete |
| ChatGPT | Email listener, parser, matcher, supervisor | ✅ Complete |
| Claude 1 | Settings UI, queue, history screens | ✅ Complete |

## Next Steps

1. **Integration Testing**: Test end-to-end promo import flow
2. **Configure Email**: Set PROMO_IMAP_SERVER/USERNAME/PASSWORD env vars
3. **Import Week 51 Promo**: Test with `2025 Weekly Executables #51 December 17-23.xlsx`
