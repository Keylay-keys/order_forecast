"""DuckDB schema definitions for the Order Forecast analytical database.

This module creates and manages the DuckDB database schema used for:
- Historical order storage (permanent, never deleted)
- User corrections to forecasts
- User parameters (stores, items, schedules, promos)
- Seasonal/pattern data
- ML features and artifacts

Usage:
    from db_schema import create_schema, get_connection
    
    conn = get_connection('data/analytics.duckdb')
    create_schema(conn)
"""

from __future__ import annotations

import duckdb
from pathlib import Path
from datetime import datetime


# Default database path
DEFAULT_DB_PATH = Path(__file__).parent.parent / 'data' / 'analytics.duckdb'


def get_connection(db_path: str | Path = DEFAULT_DB_PATH, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Get a connection to the DuckDB database.
    
    Args:
        db_path: Path to the database file. Creates parent directories if needed.
        read_only: If True, open in read-only mode.
    
    Returns:
        DuckDB connection object.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path), read_only=read_only)


def create_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create all tables and indexes in the database.
    
    Safe to call multiple times - uses IF NOT EXISTS.
    
    Args:
        conn: DuckDB connection object.
    """
    # Create tables in dependency order
    _create_order_tables(conn)
    _create_user_param_tables(conn)
    _create_promo_tables(conn)
    _create_calendar_tables(conn)
    _create_ml_tables(conn)
    _create_case_allocation_tables(conn)
    _create_delivery_allocation_tables(conn)
    _create_route_sync_tables(conn)
    _create_notification_tables(conn)
    _create_indexes(conn)
    
    print(f"✅ Schema created/verified at {datetime.now().isoformat()}")


# =============================================================================
# ORDER DATA TABLES
# =============================================================================

def _create_order_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for historical order data."""
    
    # Main order header table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders_historical (
            order_id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            user_id VARCHAR,
            schedule_key VARCHAR NOT NULL,          -- 'monday', 'thursday', etc.
            delivery_date DATE NOT NULL,
            order_date DATE,
            finalized_at TIMESTAMP,
            total_cases INTEGER DEFAULT 0,
            total_units INTEGER DEFAULT 0,
            store_count INTEGER DEFAULT 0,
            status VARCHAR DEFAULT 'finalized',
            is_holiday_week BOOLEAN DEFAULT FALSE,  -- Holiday week orders excluded from training
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Individual line items (one row per store/item in each order)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_line_items (
            line_item_id VARCHAR PRIMARY KEY,       -- {order_id}-{store_id}-{sap}
            order_id VARCHAR NOT NULL,
            route_number VARCHAR NOT NULL,
            schedule_key VARCHAR NOT NULL,
            delivery_date DATE NOT NULL,
            store_id VARCHAR NOT NULL,
            store_name VARCHAR,
            sap VARCHAR NOT NULL,
            product_name VARCHAR,
            quantity INTEGER NOT NULL,
            cases INTEGER DEFAULT 0,
            promo_id VARCHAR,                       -- NULL if no promo
            promo_active BOOLEAN DEFAULT FALSE,
            
            -- Calendar features (denormalized for query speed)
            is_first_weekend_of_month BOOLEAN DEFAULT FALSE,
            is_holiday_week BOOLEAN DEFAULT FALSE,
            is_month_end BOOLEAN DEFAULT FALSE,
            day_of_week INTEGER,                    -- 0=Mon, 6=Sun
            week_of_year INTEGER,
            month INTEGER,
            
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # User corrections to forecasts
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_corrections (
            correction_id VARCHAR PRIMARY KEY,
            forecast_id VARCHAR NOT NULL,
            order_id VARCHAR NOT NULL,
            route_number VARCHAR NOT NULL,
            schedule_key VARCHAR NOT NULL,
            delivery_date DATE NOT NULL,
            store_id VARCHAR NOT NULL,
            store_name VARCHAR,
            sap VARCHAR NOT NULL,
            
            -- What algorithm predicted
            predicted_units INTEGER NOT NULL,
            predicted_cases INTEGER DEFAULT 0,
            prediction_confidence FLOAT,
            prediction_source VARCHAR,              -- 'baseline', 'ml_v1', etc.
            
            -- What user actually submitted
            final_units INTEGER NOT NULL,
            final_cases INTEGER DEFAULT 0,
            
            -- Derived metrics
            correction_delta INTEGER NOT NULL,      -- final - predicted
            correction_ratio FLOAT NOT NULL,        -- final / predicted
            was_removed BOOLEAN DEFAULT FALSE,      -- User deleted this item
            
            -- Context
            promo_id VARCHAR,
            promo_active BOOLEAN DEFAULT FALSE,
            is_first_weekend_of_month BOOLEAN DEFAULT FALSE,
            is_holiday_week BOOLEAN DEFAULT FALSE,
            
            submitted_at TIMESTAMP NOT NULL
        )
    """)
    
    print("  ✓ Order tables created")


# =============================================================================
# USER PARAMETER TABLES
# =============================================================================

def _create_user_param_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for user configuration data."""
    
    # Store ID aliases - maps old/alternate store IDs to the canonical (current) ID
    # This handles cases where stores were recreated with different IDs
    conn.execute("""
        CREATE TABLE IF NOT EXISTS store_id_aliases (
            id VARCHAR PRIMARY KEY,                 -- {route_number}-{alias_id}
            route_number VARCHAR NOT NULL,
            alias_id VARCHAR NOT NULL,              -- The old/alternate store_id
            canonical_id VARCHAR NOT NULL,          -- The current/canonical store_id
            store_name VARCHAR,                     -- Store name for reference
            source VARCHAR,                         -- How this mapping was created: 'auto', 'manual'
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, alias_id)
        )
    """)
    
    # User schedule configurations
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_schedules (
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
        )
    """)
    
    # Store configurations
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            store_name VARCHAR NOT NULL,
            store_number VARCHAR,
            address VARCHAR,
            delivery_days VARCHAR,                  -- JSON array: '["Monday", "Friday"]'
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Store item lists (which items each store carries)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS store_items (
            id VARCHAR PRIMARY KEY,                 -- {store_id}-{sap}
            store_id VARCHAR NOT NULL,
            route_number VARCHAR NOT NULL,
            sap VARCHAR NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            added_at TIMESTAMP,
            removed_at TIMESTAMP,                   -- NULL if still active
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(store_id, sap)
        )
    """)
    
    # Product catalog
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_catalog (
            sap VARCHAR NOT NULL,
            route_number VARCHAR NOT NULL,
            full_name VARCHAR NOT NULL,
            short_name VARCHAR,
            brand VARCHAR,
            category VARCHAR,
            sub_category VARCHAR,
            case_pack INTEGER NOT NULL DEFAULT 1,   -- Units per case
            tray INTEGER,                           -- Tray/pack configuration
            unit_weight FLOAT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            PRIMARY KEY (sap, route_number)
        )
    """)
    
    # User order guide preferences
    conn.execute("""
        CREATE TABLE IF NOT EXISTS order_guide (
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
        )
    """)
    
    print("  ✓ User parameter tables created")


# =============================================================================
# PROMO TABLES
# =============================================================================

def _create_promo_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for promo data."""
    
    # Promo history
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_history (
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
        )
    """)
    
    # Which items each promo affects
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_items (
            id VARCHAR PRIMARY KEY,                 -- {promo_id}-{sap}-{account}
            promo_id VARCHAR NOT NULL,
            sap VARCHAR NOT NULL,
            account VARCHAR,                        -- Store/chain name this promo applies to
            start_date DATE,                        -- Per-item start date
            end_date DATE,                          -- Per-item end date
            special_price FLOAT,
            discount_percent FLOAT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(promo_id, sap, account)
        )
    """)
    
    # Track what user ordered during promo periods (for ML learning)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_order_history (
            id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            promo_id VARCHAR NOT NULL,
            order_id VARCHAR NOT NULL,
            store_id VARCHAR NOT NULL,
            sap VARCHAR NOT NULL,
            
            -- Promo context
            promo_price FLOAT,                      -- Price during promo
            normal_price FLOAT,                     -- Normal price (if known)
            discount_percent FLOAT,
            
            -- What user ordered
            quantity_ordered INTEGER NOT NULL,
            cases_ordered INTEGER NOT NULL,
            
            -- Comparison to baseline
            baseline_quantity FLOAT,                -- What user typically orders (non-promo)
            quantity_lift FLOAT,                    -- quantity_ordered / baseline_quantity
            
            -- ML features
            weeks_into_promo INTEGER,               -- 1st week vs 4th week of promo
            is_first_promo_occurrence BOOLEAN,      -- First time seeing this promo?
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Track email ingestion queue for promo imports
    conn.execute("""
        CREATE TABLE IF NOT EXISTS promo_email_queue (
            email_id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            subject VARCHAR,
            sender VARCHAR,
            received_at TIMESTAMP,
            attachment_name VARCHAR,
            attachment_type VARCHAR,                -- 'xlsx', 'pdf'
            attachment_size INTEGER,
            status VARCHAR DEFAULT 'pending',       -- 'pending', 'processing', 'completed', 'error'
            error_message VARCHAR,
            items_imported INTEGER,
            retry_count INTEGER DEFAULT 0,
            processed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Store SAP correction mappings (wrong SAP -> correct SAP)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sap_corrections (
            id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            wrong_sap VARCHAR NOT NULL,
            correct_sap VARCHAR NOT NULL,
            promo_account VARCHAR,                  -- e.g., "Smiths", "ALB SFW IMW"
            description_match VARCHAR,              -- Description used for fuzzy matching
            confidence FLOAT,                       -- Match confidence score
            times_used INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            
            UNIQUE(route_number, wrong_sap, promo_account)
        )
    """)
    
    print("  ✓ Promo tables created")


# =============================================================================
# CALENDAR/SEASONAL TABLES
# =============================================================================

def _create_calendar_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for calendar and seasonal data."""
    
    # Calendar features lookup
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calendar_features (
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
            is_holiday BOOLEAN DEFAULT FALSE,
            holiday_name VARCHAR,
            is_holiday_week BOOLEAN DEFAULT FALSE,  -- Week containing a holiday
            days_until_next_holiday INTEGER,
            days_since_last_holiday INTEGER,
            -- Schedule-aware features (for delivery date context)
            days_until_first_weekend INTEGER,       -- Days from this date to first Sat of month (can be negative)
            covers_first_weekend BOOLEAN DEFAULT FALSE  -- TRUE if delivery on this date covers first weekend
        )
    """)
    
    # Learned seasonal adjustments per store/item
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seasonal_adjustments (
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
        )
    """)
    
    print("  ✓ Calendar tables created")


# =============================================================================
# ML ARTIFACT TABLES
# =============================================================================

def _create_ml_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for ML features and artifacts."""
    
    # Pre-computed feature cache (refreshed after each order)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feature_cache (
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
        )
    """)
    
    # Model training metadata
    conn.execute("""
        CREATE TABLE IF NOT EXISTS model_metadata (
            model_id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            schedule_key VARCHAR NOT NULL,
            model_version INTEGER NOT NULL,
            
            -- Training info
            trained_at TIMESTAMP NOT NULL,
            training_rows INTEGER,
            feature_count INTEGER,
            features_used VARCHAR,                  -- JSON array
            
            -- Performance metrics
            validation_mae FLOAT,
            validation_rmse FLOAT,
            validation_r2 FLOAT,
            
            -- Feature importance
            feature_importance VARCHAR,             -- JSON object
            
            -- Status
            is_active BOOLEAN DEFAULT FALSE,        -- Currently deployed?
            deployed_at TIMESTAMP,
            retired_at TIMESTAMP,
            
            model_path VARCHAR                      -- Path to .pkl file
        )
    """)
    
    # Prediction log (for monitoring and debugging)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prediction_log (
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
        )
    """)
    
    print("  ✓ ML artifact tables created")


# =============================================================================
# CASE ALLOCATION TABLES (CORE FEATURE)
# =============================================================================

def _create_case_allocation_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for intelligent case allocation across stores.
    
    This is a CORE FEATURE - users split cases across stores constantly.
    The algorithm must learn historical splitting patterns and replicate them.
    """
    
    # Item-level allocation cache (cross-store aggregates)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS item_allocation_cache (
            id VARCHAR PRIMARY KEY,                 -- {route}-{sap}-{schedule}
            route_number VARCHAR NOT NULL,
            sap VARCHAR NOT NULL,
            schedule_key VARCHAR NOT NULL,
            
            -- Aggregate demand across all stores
            total_avg_quantity FLOAT,               -- Average total demand per order
            total_quantity_stddev FLOAT,
            total_orders_seen INTEGER DEFAULT 0,
            
            -- Store share breakdown (JSON map: store_id -> share)
            store_shares VARCHAR,                   -- JSON: {"store_id": 0.53, ...}
            
            -- Splitting patterns
            avg_stores_per_order FLOAT,             -- How many stores typically get this item
            typical_case_count FLOAT,               -- Average cases ordered
            
            -- Pattern classification
            split_pattern VARCHAR,                  -- 'single_store', 'even_split', 'skewed', 'varies'
            dominant_store_id VARCHAR,              -- Store with highest share (if skewed)
            dominant_store_share FLOAT,             -- That store's share
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, sap, schedule_key)
        )
    """)
    
    # Historical share per store/item (denormalized for fast lookup)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS store_item_shares (
            id VARCHAR PRIMARY KEY,                 -- {route}-{store}-{sap}-{schedule}
            route_number VARCHAR NOT NULL,
            store_id VARCHAR NOT NULL,
            store_name VARCHAR,
            sap VARCHAR NOT NULL,
            schedule_key VARCHAR NOT NULL,
            
            -- Historical share of this item for this store
            share FLOAT NOT NULL,                   -- 0.0-1.0, sum across stores = 1.0
            
            -- Raw counts for computing share
            total_quantity INTEGER DEFAULT 0,       -- Sum of quantities across all orders
            order_count INTEGER DEFAULT 0,          -- How many orders included this store/item
            avg_quantity FLOAT,                     -- Average qty when this store orders this item
            
            -- Trend info
            recent_share FLOAT,                     -- Share from last 4 orders only
            share_trend FLOAT,                      -- recent_share - share (positive = increasing)
            
            last_ordered_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, store_id, sap, schedule_key)
        )
    """)
    
    print("  ✓ Case allocation tables created")


# =============================================================================
# DELIVERY ALLOCATION TABLES
# =============================================================================

def _create_delivery_allocation_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for tracking delivery allocations (case splits).
    
    Separates ORDER (what was submitted to supplier) from DELIVERY (what goes to each store).
    Critical for stores like Bowman's that have different delivery days than the order's
    primary delivery date.
    
    Example:
        Monday order (primary delivery = Thursday)
        - Harmon's items: delivery_date = Thursday, is_case_split = FALSE
        - Bowman's items: delivery_date = Friday, is_case_split = TRUE
    """
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS delivery_allocations (
            allocation_id VARCHAR PRIMARY KEY,      -- {order_id}-{store_id}-{sap}
            route_number VARCHAR NOT NULL,
            
            -- Source order info
            source_order_id VARCHAR NOT NULL,       -- Which order this was purchased in
            source_order_date DATE NOT NULL,        -- When the order was placed
            
            -- What's being delivered
            sap VARCHAR NOT NULL,
            store_id VARCHAR NOT NULL,
            store_name VARCHAR,
            quantity INTEGER NOT NULL,
            
            -- When it's actually delivered
            delivery_date DATE NOT NULL,            -- Actual delivery date for THIS store
            
            -- Case split tracking
            is_case_split BOOLEAN DEFAULT FALSE,    -- TRUE if delivery_date differs from order's primary
            
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (source_order_id) REFERENCES orders_historical(order_id)
        )
    """)
    
    # Index for fast manifest queries (what's being delivered on a date)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_delivery_manifest 
        ON delivery_allocations(route_number, delivery_date, store_id)
    """)
    
    # Index for finding all allocations from an order
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_delivery_by_order 
        ON delivery_allocations(source_order_id)
    """)


# =============================================================================
# ROUTE SYNC TRACKING TABLES
# =============================================================================

def _create_route_sync_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables to track which routes have been synced.
    
    Used for multi-user support - tracks when routes were first synced
    and their current sync status.
    """
    
    # Track synced routes (multi-user support)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS routes_synced (
            route_number VARCHAR PRIMARY KEY,
            user_id VARCHAR,
            
            -- Sync status
            first_synced_at TIMESTAMP NOT NULL,
            last_synced_at TIMESTAMP NOT NULL,
            worker_id VARCHAR,
            
            -- What was synced
            stores_count INTEGER DEFAULT 0,
            products_count INTEGER DEFAULT 0,
            orders_count INTEGER DEFAULT 0,
            schedules_synced BOOLEAN DEFAULT FALSE,
            
            -- Sync triggers
            triggered_by VARCHAR,               -- 'first_order', 'manual', 'app_request'
            trigger_order_id VARCHAR,           -- Order that triggered first sync
            
            -- Status
            sync_status VARCHAR DEFAULT 'ready',  -- 'syncing', 'ready', 'error'
            last_error VARCHAR,
            
            -- Model status
            has_trained_model BOOLEAN DEFAULT FALSE,
            model_trained_at TIMESTAMP,
            last_forecast_at TIMESTAMP
        )
    """)
    
    # Track individual sync operations for debugging
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id VARCHAR PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            sync_type VARCHAR NOT NULL,         -- 'full', 'orders', 'corrections', 'stores'
            started_at TIMESTAMP NOT NULL,
            completed_at TIMESTAMP,
            worker_id VARCHAR,
            
            -- Results
            status VARCHAR DEFAULT 'in_progress',  -- 'in_progress', 'completed', 'failed'
            records_synced INTEGER DEFAULT 0,
            error_message VARCHAR,
            
            -- Context
            triggered_by VARCHAR,               -- What triggered this sync
            trigger_id VARCHAR                  -- Order ID, request ID, etc.
        )
    """)
    
    print("  ✓ Route sync tables created")


# =============================================================================
# INDEXES
# =============================================================================

def _create_notification_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables for low-quantity notification tracking."""
    
    # Track sent notifications for deduplication
    # Prevents sending same notification multiple times per day
    conn.execute("""
        CREATE TABLE IF NOT EXISTS low_qty_notifications_sent (
            id INTEGER PRIMARY KEY,
            route_number VARCHAR NOT NULL,
            user_id VARCHAR NOT NULL,
            order_by_date VARCHAR NOT NULL,       -- YYYY-MM-DD (order date, not delivery)
            saps VARCHAR NOT NULL,                -- JSON array of SAP codes
            saps_hash VARCHAR NOT NULL,           -- MD5 hash for dedup comparison
            items_count INTEGER NOT NULL,
            sent_at TIMESTAMP NOT NULL,
            
            -- Dedup: same route+user+date+items = skip
            -- Includes user_id to allow team members to get separate notifications if added later
            UNIQUE(route_number, user_id, order_by_date, saps_hash)
        )
    """)
    
    print("  ✓ Notification tables created")


def _create_indexes(conn: duckdb.DuckDBPyConnection) -> None:
    """Create indexes for common query patterns."""
    
    # Order indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_route_schedule 
        ON orders_historical(route_number, schedule_key)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_orders_delivery_date 
        ON orders_historical(delivery_date)
    """)
    
    # Line item indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_line_items_order 
        ON order_line_items(order_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_line_items_route_schedule 
        ON order_line_items(route_number, schedule_key)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_line_items_store_sap 
        ON order_line_items(store_id, sap)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_line_items_delivery 
        ON order_line_items(delivery_date)
    """)
    
    # Correction indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_corrections_store_sap 
        ON forecast_corrections(store_id, sap)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_corrections_route_schedule 
        ON forecast_corrections(route_number, schedule_key)
    """)
    
    # Store alias indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_aliases_lookup 
        ON store_id_aliases(route_number, alias_id)
    """)
    
    # Store/item indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_items_store 
        ON store_items(store_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_items_sap 
        ON store_items(sap)
    """)
    
    # Feature cache index
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_feature_cache_lookup 
        ON feature_cache(route_number, store_id, sap, schedule_key)
    """)
    
    # Promo indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_promo_items_promo 
        ON promo_items(promo_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_promo_dates 
        ON promo_history(start_date, end_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_promo_order_history_lookup 
        ON promo_order_history(route_number, promo_id, sap)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_promo_order_history_order 
        ON promo_order_history(order_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_promo_email_queue_status 
        ON promo_email_queue(route_number, status)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sap_corrections_lookup 
        ON sap_corrections(route_number, wrong_sap)
    """)
    
    # Case allocation indexes
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_item_alloc_lookup 
        ON item_allocation_cache(route_number, sap, schedule_key)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_item_shares_lookup 
        ON store_item_shares(route_number, sap, schedule_key)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_store_item_shares_store 
        ON store_item_shares(store_id, sap)
    """)
    
    print("  ✓ Indexes created")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_table_counts(conn: duckdb.DuckDBPyConnection) -> dict:
    """Get row counts for all tables.
    
    Returns:
        Dict mapping table name to row count.
    """
    tables = [
        'orders_historical', 'order_line_items', 'forecast_corrections',
        'store_id_aliases', 'user_schedules', 'stores', 'store_items', 'product_catalog', 'order_guide',
        'promo_history', 'promo_items', 'promo_order_history', 'promo_email_queue', 'sap_corrections',
        'calendar_features', 'seasonal_adjustments',
        'feature_cache', 'model_metadata', 'prediction_log',
        'item_allocation_cache', 'store_item_shares',
        'routes_synced', 'sync_log',
        'low_qty_notifications_sent'
    ]
    
    counts = {}
    for table in tables:
        try:
            result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            counts[table] = result[0] if result else 0
        except Exception:
            counts[table] = -1  # Table doesn't exist
    
    return counts


def print_schema_summary(conn: duckdb.DuckDBPyConnection) -> None:
    """Print a summary of the database schema."""
    counts = get_table_counts(conn)
    
    print("\n📊 Database Summary")
    print("=" * 50)
    
    sections = {
        "Order Data": ['orders_historical', 'order_line_items', 'forecast_corrections'],
        "User Params": ['store_id_aliases', 'user_schedules', 'stores', 'store_items', 'product_catalog', 'order_guide'],
        "Promos": ['promo_history', 'promo_items', 'promo_order_history', 'promo_email_queue', 'sap_corrections'],
        "Calendar": ['calendar_features', 'seasonal_adjustments'],
        "ML Artifacts": ['feature_cache', 'model_metadata', 'prediction_log'],
        "Case Allocation": ['item_allocation_cache', 'store_item_shares'],
        "Route Sync": ['routes_synced', 'sync_log'],
        "Notifications": ['low_qty_notifications_sent']
    }
    
    for section, tables in sections.items():
        print(f"\n{section}:")
        for table in tables:
            count = counts.get(table, -1)
            status = f"{count:,} rows" if count >= 0 else "❌ missing"
            print(f"  {table}: {status}")


def drop_all_tables(conn: duckdb.DuckDBPyConnection, confirm: bool = False) -> None:
    """Drop all tables. Use with caution!
    
    Args:
        conn: DuckDB connection.
        confirm: Must be True to actually drop tables.
    """
    if not confirm:
        print("⚠️  Pass confirm=True to actually drop tables")
        return
    
    tables = [
        'store_item_shares', 'item_allocation_cache',
        'prediction_log', 'model_metadata', 'feature_cache',
        'seasonal_adjustments', 'calendar_features',
        'promo_order_history', 'promo_email_queue', 'sap_corrections', 'promo_items', 'promo_history',
        'order_guide', 'product_catalog', 'store_items', 'stores', 'user_schedules',
        'forecast_corrections', 'order_line_items', 'orders_historical'
    ]
    
    for table in tables:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    
    print("🗑️  All tables dropped")


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='DuckDB Schema Manager')
    parser.add_argument('--db', default=str(DEFAULT_DB_PATH), help='Path to database file')
    parser.add_argument('--create', action='store_true', help='Create schema')
    parser.add_argument('--summary', action='store_true', help='Print schema summary')
    parser.add_argument('--drop', action='store_true', help='Drop all tables (dangerous!)')
    args = parser.parse_args()
    
    print(f"📁 Database: {args.db}")
    conn = get_connection(args.db)
    
    if args.drop:
        response = input("⚠️  This will DELETE ALL DATA. Type 'yes' to confirm: ")
        if response.lower() == 'yes':
            drop_all_tables(conn, confirm=True)
    
    if args.create:
        create_schema(conn)
    
    if args.summary or not (args.create or args.drop):
        print_schema_summary(conn)
    
    conn.close()

