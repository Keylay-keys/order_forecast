# RouteSpark Backend Supervisor

## 🖥️ Desktop Widget (Recommended)

The easiest way to manage services is the **Desktop Widget**:

1. Double-click `RouteSpark.app` on your Desktop
2. A floating widget appears in the bottom-right corner showing:
   - All 5 services with green/red status dots
   - Running/Total count (e.g., "5/5")
   - Start/Stop/Logs buttons
3. **Drag the widget** anywhere on your screen
4. Click **×** to close

**To start on login (macOS Ventura+):**
1. Open **System Settings → General → Login Items**
2. Under "Open at Login", click **+**
3. Navigate to Desktop and select `RouteSpark.app`

**For older macOS:**
1. Open **System Preferences → Users & Groups**
2. Click your user, then **Login Items** tab
3. Drag `RouteSpark.app` into the list

---

One script to rule them all. Manages all background services for the RouteSpark backend.

## Quick Start

```bash
cd /Users/kylemacmini/Desktop/routespark/restore-2025-09-24/order_forecast
source venv/bin/activate

# Check what's running
python supervisor.py status

# Start everything
python supervisor.py start

# Stop everything
python supervisor.py stop
```

## Commands

| Command | What it does |
|---------|--------------|
| `python supervisor.py status` | Show which services are running |
| `python supervisor.py start` | Start all services (stays in foreground, Ctrl+C to stop) |
| `python supervisor.py start -d` | Start all services (detached, runs in background) |
| `python supervisor.py stop` | Stop all services |
| `python supervisor.py restart` | Stop then start all services |
| `python supervisor.py logs` | Tail all log files |

## Services Managed

### 1. DB Manager (CORE)
- **Purpose**: Owns the single DuckDB connection, handles all DB operations
- **Log**: `logs/db_manager.log`
- **Script**: `scripts/db_manager.py`
- **Multi-user**: ✅ Processes requests from all services
- **Must start FIRST** - other services depend on it

All database operations go through the DB Manager:
```
Other Services → Firebase dbRequests → DB Manager → DuckDB
```

### 2. Order Sync Listener (via DBClient)
- **Purpose**: Watches ALL orders, syncs new routes via DB Manager
- **Log**: `logs/order_sync.log`
- **Script**: `scripts/order_sync_listener.py`
- **Multi-user**: ✅ Handles all routes automatically
- **No direct DB access** - uses DBClient → dbRequests → DB Manager

When a new user creates their first order, this listener:
1. Detects the new route
2. Sends sync request to DB Manager via DBClient
3. DB Manager writes to DuckDB
4. Marks the route as ready for forecasting

### 3. Archive Listener (via DBClient)
- **Purpose**: Handles app requests for historical orders
- **Log**: `logs/archive_listener.log`
- **Script**: `scripts/order_archive_listener.py`
- **Multi-user**: ✅ All routes (reads `routeNumber` from each request)
- **No direct DB access** - uses DBClient → dbRequests → DB Manager

### 4. Retrain Daemon
- **Purpose**: Auto-retrains ML model after complete order cycles
- **Log**: `logs/retrain_daemon.log`
- **Script**: `scripts/retrain_daemon.py`
- **Check interval**: Once per day
- **Multi-user**: ✅ Discovers all routes from `routes_synced` table

### 5. PCF OCR Listener
- **Purpose**: Processes PCF document uploads, runs OCR
- **Log**: `logs/pcf_listener.log`
- **Project**: `/Users/kylemacmini/projects/pcf_pipeline`
- **Script**: `pcf_core.runner --watch-all`
- **Multi-user**: ✅ All routes (uses collection group query)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Firebase (Cloud)                          │
│  ┌──────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────┐  │
│  │ /orders  │  │orderRequests │  │ dbRequests │  │ pcf_queue │  │
│  └────┬─────┘  └──────┬───────┘  └─────┬──────┘  └─────┬─────┘  │
└───────┼───────────────┼────────────────┼───────────────┼────────┘
        │               │                │               │
        ▼               ▼                ▼               ▼
┌───────────────┐ ┌────────────┐ ┌──────────────┐ ┌────────────┐
│ Order Sync    │ │ Archive    │ │ DB MANAGER   │ │ PCF OCR    │
│ Listener      │ │ Listener   │ │ (owns DB)    │ │ Listener   │
└───────┬───────┘ └──────┬─────┘ └──────┬───────┘ └────────────┘
        │                │              │
        └────────────────┴──────────────┘
                         │
                         ▼
                   ┌──────────┐
                   │  DuckDB  │
                   │analytics │
                   └──────────┘
```

**Key Benefits:**
- No lock conflicts - DB Manager owns the single connection
- Sequential processing prevents race conditions
- Services can run concurrently without blocking each other
- **All services wrapped in `caffeinate`** - Mac won't sleep while services run

## DBClient

All services use `DBClient` (`scripts/db_client.py`) to communicate with DB Manager:

```python
from db_client import DBClient
from google.cloud import firestore

fb = firestore.Client.from_service_account_json('/path/to/sa.json')
client = DBClient(fb, timeout=30)

# Query
result = client.query("SELECT * FROM orders_historical")
print(result['rows'])

# Write
client.write("INSERT INTO ...", [params])

# High-level operations
client.check_route_synced("989262")
client.get_archived_dates("989262")
client.sync_order("order-123", "989262")
```

## How Retrain Works

```
Week flow:
┌─────────────────────────────────────────────────────────┐
│  Mon: You finalize order → correction saved            │
│  Tue: You finalize order → correction saved            │
│                                                          │
│  Retrain Daemon checks every hour:                       │
│    "Are all scheduled days (Mon, Tue) complete?"        │
│                                                          │
│  When both done → triggers retrain automatically!       │
└─────────────────────────────────────────────────────────┘
```

## Log Files

All logs are in `order_forecast/logs/`:

```bash
# View all logs
tail -f logs/*.log

# View specific service
tail -f logs/order_sync.log       # New routes being synced
tail -f logs/retrain_daemon.log   # Retrain checks
tail -f logs/pcf_listener.log     # PCF processing
tail -f logs/archive_listener.log # Archive requests
```

## Troubleshooting

### Services won't start?
```bash
# Check if something's already running
python supervisor.py status

# Stop everything first
python supervisor.py stop

# Then start fresh
python supervisor.py start
```

### Need to run manually?
```bash
# Archive listener
python scripts/order_archive_listener.py \
  --serviceAccount /path/to/sa.json \
  --duckdb data/analytics.duckdb

# Retrain daemon
python scripts/retrain_daemon.py --route 989262

# Retrain check (one-time, no daemon)
python scripts/retrain_scheduler.py --route 989262 --check-only

# Force retrain now
python scripts/retrain_scheduler.py --route 989262 --force
```

### Generate a new forecast?
```bash
python test_forecast.py
```

## File Locations

```
order_forecast/
├── supervisor.py           # ← The supervisor script
├── SUPERVISOR_README.md    # ← This file
├── logs/                   # ← All service logs
│   ├── order_sync.log
│   ├── archive_listener.log
│   ├── retrain_daemon.log
│   └── pcf_listener.log
├── data/
│   └── analytics.duckdb   # ← Local ML database
└── scripts/
    ├── order_sync_listener.py   # ← NEW: Multi-user order sync
    ├── order_archive_listener.py
    ├── retrain_daemon.py
    ├── retrain_scheduler.py
    └── ...
```

## Keep Mac Awake

The supervisor runs with `caffeinate` behavior built-in. If you run `python supervisor.py start` (without `-d`), it monitors and restarts services automatically.

For true background operation:
```bash
nohup python supervisor.py start > supervisor.log 2>&1 &
```

Or use the start script:
```bash
./start_retrain_daemon.sh  # Uses caffeinate
```
