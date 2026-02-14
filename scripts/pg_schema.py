#!/usr/bin/env python3
"""PostgreSQL schema definitions for the Order Forecast analytical database.

Migration from DuckDB to PostgreSQL for multi-writer support.
No more single-writer lock - all services can write concurrently.

This module creates and manages the PostgreSQL database schema used for:
- Historical order storage (permanent, never deleted)
- User corrections to forecasts
- User parameters (stores, items, schedules, promos)
- Seasonal/pattern data
- ML features and artifacts

Usage:
    from pg_schema import create_schema, get_connection
    
    conn = get_connection()
    create_schema(conn)

PostgreSQL Connection:
    Host: localhost (or Docker host.docker.internal)
    Port: 5432
    Database: routespark
    User: routespark
"""

from __future__ import annotations

import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional


# Connection parameters from environment
PG_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
PG_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
PG_DATABASE = os.environ.get('POSTGRES_DB', 'routespark')
PG_USER = os.environ.get('POSTGRES_USER', 'routespark')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', '')


def get_connection(
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    autocommit: bool = False
) -> psycopg2.extensions.connection:
    """Get a connection to the PostgreSQL database.
    
    Args:
        host: PostgreSQL host (default from env or localhost)
        port: PostgreSQL port (default 5432)
        database: Database name (default 'routespark')
        user: Username (default 'routespark')
        password: Password (default from env)
        autocommit: If True, each statement auto-commits
    
    Returns:
        psycopg2 connection object.
    """
    conn = psycopg2.connect(
        host=host or PG_HOST,
        port=port or PG_PORT,
        database=database or PG_DATABASE,
        user=user or PG_USER,
        password=password or PG_PASSWORD,
    )
    conn.autocommit = autocommit
    return conn


def create_schema(conn: psycopg2.extensions.connection) -> None:
    """Create all tables and indexes in the database.
    
    Safe to call multiple times - uses IF NOT EXISTS.
    
    Args:
        conn: PostgreSQL connection object.
    """
    cur = conn.cursor()
    
    # Create tables in dependency order
    _create_order_tables(cur)
    _create_user_param_tables(cur)
    _create_promo_tables(cur)
    _create_calendar_tables(cur)
    _create_ml_tables(cur)
    _create_case_allocation_tables(cur)
    _create_delivery_allocation_tables(cur)
    _create_route_transfer_tables(cur)
    _create_route_sync_tables(cur)
    _create_notification_tables(cur)
    _create_indexes(cur)
    
    conn.commit()
    cur.close()
    
    print(f"✅ PostgreSQL schema created/verified at {datetime.now().isoformat()}")


# =============================================================================
# ORDER DATA TABLES
# =============================================================================

def _create_order_tables(cur) -> None:
    """Create tables for historical order data."""
    
    # Main order header table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders_historical (
            order_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            user_id VARCHAR(255),
            schedule_key VARCHAR(20) NOT NULL,
            delivery_date DATE NOT NULL,
            order_date DATE,
            finalized_at TIMESTAMP WITH TIME ZONE,
            total_cases INTEGER DEFAULT 0,
            total_units INTEGER DEFAULT 0,
            store_count INTEGER DEFAULT 0,
            status VARCHAR(20) DEFAULT 'finalized',
            is_holiday_week BOOLEAN DEFAULT FALSE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Individual line items (one row per store/item in each order)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_line_items (
            line_item_id VARCHAR(255) PRIMARY KEY,
            order_id VARCHAR(255) NOT NULL,
            route_number VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            delivery_date DATE NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            store_name VARCHAR(255),
            sap VARCHAR(20) NOT NULL,
            product_name VARCHAR(255),
            quantity INTEGER NOT NULL,
            cases INTEGER DEFAULT 0,
            promo_id VARCHAR(255),
            promo_active BOOLEAN DEFAULT FALSE,
            
            is_first_weekend_of_month BOOLEAN DEFAULT FALSE,
            is_holiday_week BOOLEAN DEFAULT FALSE,
            is_month_end BOOLEAN DEFAULT FALSE,
            day_of_week INTEGER,
            week_of_year INTEGER,
            month INTEGER,
            
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # User corrections to forecasts
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast_corrections (
            correction_id VARCHAR(255) PRIMARY KEY,
            forecast_id VARCHAR(255) NOT NULL,
            order_id VARCHAR(255) NOT NULL,
            route_number VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            delivery_date DATE NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            store_name VARCHAR(255),
            sap VARCHAR(20) NOT NULL,
            
            predicted_units INTEGER NOT NULL,
            predicted_cases INTEGER DEFAULT 0,
            prediction_confidence REAL,
            prediction_source VARCHAR(50),
            
            final_units INTEGER NOT NULL,
            final_cases INTEGER DEFAULT 0,
            
            correction_delta INTEGER NOT NULL,
            correction_ratio REAL NOT NULL,
            was_removed BOOLEAN DEFAULT FALSE,
            
            promo_id VARCHAR(255),
            promo_active BOOLEAN DEFAULT FALSE,
            is_first_weekend_of_month BOOLEAN DEFAULT FALSE,
            is_holiday_week BOOLEAN DEFAULT FALSE,
            
            submitted_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
    """)
    
    print("  ✓ Order tables created")


# =============================================================================
# USER PARAMETER TABLES
# =============================================================================

def _create_user_param_tables(cur) -> None:
    """Create tables for user configuration data."""
    
    # Store ID aliases
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_id_aliases (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            alias_id VARCHAR(255) NOT NULL,
            canonical_id VARCHAR(255) NOT NULL,
            store_name VARCHAR(255),
            source VARCHAR(20),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, alias_id)
        )
    """)
    
    # User schedule configurations
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_schedules (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            order_day INTEGER NOT NULL,
            load_day INTEGER NOT NULL,
            delivery_day INTEGER NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Store configurations
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            store_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            store_name VARCHAR(255) NOT NULL,
            store_number VARCHAR(50),
            address VARCHAR(500),
            delivery_days TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Store item lists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_items (
            id VARCHAR(255) PRIMARY KEY,
            store_id VARCHAR(255) NOT NULL,
            route_number VARCHAR(20) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            is_active BOOLEAN DEFAULT TRUE,
            added_at TIMESTAMP WITH TIME ZONE,
            removed_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(store_id, sap)
        )
    """)
    
    # Product catalog
    cur.execute("""
        CREATE TABLE IF NOT EXISTS product_catalog (
            sap VARCHAR(20) NOT NULL,
            route_number VARCHAR(20) NOT NULL,
            full_name VARCHAR(255) NOT NULL,
            short_name VARCHAR(100),
            brand VARCHAR(100),
            category VARCHAR(100),
            sub_category VARCHAR(100),
            case_pack INTEGER NOT NULL DEFAULT 1,
            tray INTEGER,
            unit_weight REAL,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            PRIMARY KEY (sap, route_number)
        )
    """)
    
    # User order guide preferences
    cur.execute("""
        CREATE TABLE IF NOT EXISTS order_guide (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            preferred_quantity INTEGER,
            min_quantity INTEGER,
            max_quantity INTEGER,
            notes TEXT,
            is_favorite BOOLEAN DEFAULT FALSE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("  ✓ User parameter tables created")


# =============================================================================
# PROMO TABLES
# =============================================================================

def _create_promo_tables(cur) -> None:
    """Create tables for promo data."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_history (
            promo_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            promo_name VARCHAR(255),
            promo_type VARCHAR(50),
            start_date DATE,
            end_date DATE,
            discount_percent REAL,
            discount_amount REAL,
            source_file VARCHAR(255),
            uploaded_by VARCHAR(255),
            uploaded_at TIMESTAMP WITH TIME ZONE,
            
            avg_quantity_lift REAL,
            user_correction_avg REAL,
            times_seen INTEGER DEFAULT 0,
            last_seen_date DATE,
            
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_items (
            id VARCHAR(255) PRIMARY KEY,
            promo_id VARCHAR(255) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            account VARCHAR(255),
            start_date DATE,
            end_date DATE,
            special_price REAL,
            discount_percent REAL,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(promo_id, sap, account)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_order_history (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            promo_id VARCHAR(255) NOT NULL,
            order_id VARCHAR(255) NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            
            promo_price REAL,
            normal_price REAL,
            discount_percent REAL,
            
            quantity_ordered INTEGER NOT NULL,
            cases_ordered INTEGER NOT NULL,
            
            baseline_quantity REAL,
            quantity_lift REAL,
            
            weeks_into_promo INTEGER,
            is_first_promo_occurrence BOOLEAN,
            
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_email_queue (
            email_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            subject VARCHAR(500),
            sender VARCHAR(255),
            received_at TIMESTAMP WITH TIME ZONE,
            attachment_name VARCHAR(255),
            attachment_type VARCHAR(20),
            attachment_size INTEGER,
            status VARCHAR(20) DEFAULT 'pending',
            error_message TEXT,
            items_imported INTEGER,
            retry_count INTEGER DEFAULT 0,
            processed_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sap_corrections (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            wrong_sap VARCHAR(20) NOT NULL,
            correct_sap VARCHAR(20) NOT NULL,
            promo_account VARCHAR(255),
            description_match VARCHAR(500),
            confidence REAL,
            times_used INTEGER DEFAULT 1,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE,
            
            UNIQUE(route_number, wrong_sap, promo_account)
        )
    """)
    
    print("  ✓ Promo tables created")


# =============================================================================
# CALENDAR/SEASONAL TABLES
# =============================================================================

def _create_calendar_tables(cur) -> None:
    """Create tables for calendar and seasonal data."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS calendar_features (
            date DATE PRIMARY KEY,
            day_of_week INTEGER,
            week_of_year INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER,
            is_weekend BOOLEAN,
            is_first_weekend_of_month BOOLEAN,
            is_last_weekend_of_month BOOLEAN,
            is_month_start BOOLEAN,
            is_month_end BOOLEAN,
            is_holiday BOOLEAN DEFAULT FALSE,
            holiday_name VARCHAR(100),
            is_holiday_week BOOLEAN DEFAULT FALSE,
            days_until_next_holiday INTEGER,
            days_since_last_holiday INTEGER,
            days_until_first_weekend INTEGER,
            covers_first_weekend BOOLEAN DEFAULT FALSE
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seasonal_adjustments (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            store_id VARCHAR(255),
            sap VARCHAR(20),
            
            pattern_type VARCHAR(50) NOT NULL,
            pattern_value VARCHAR(100),
            
            quantity_multiplier REAL NOT NULL,
            confidence REAL,
            sample_count INTEGER,
            
            last_updated TIMESTAMP WITH TIME ZONE
        )
    """)
    
    print("  ✓ Calendar tables created")


# =============================================================================
# ML ARTIFACT TABLES
# =============================================================================

def _create_ml_tables(cur) -> None:
    """Create tables for ML features and artifacts."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS feature_cache (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            
            avg_quantity REAL,
            quantity_stddev REAL,
            quantity_min INTEGER,
            quantity_max INTEGER,
            last_quantity INTEGER,
            order_count INTEGER,
            
            lag_1 REAL,
            lag_2 REAL,
            lag_3 REAL,
            rolling_mean_4 REAL,
            rolling_mean_8 REAL,
            
            avg_correction_ratio REAL,
            correction_stddev REAL,
            last_correction_delta INTEGER,
            removal_rate REAL,
            correction_trend REAL,
            
            avg_promo_lift REAL,
            promo_correction_ratio REAL,
            
            first_weekend_lift REAL,
            holiday_lift REAL,
            
            updated_at TIMESTAMP WITH TIME ZONE,
            
            UNIQUE(route_number, store_id, sap, schedule_key)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS model_metadata (
            model_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            model_version INTEGER NOT NULL,
            
            trained_at TIMESTAMP WITH TIME ZONE NOT NULL,
            training_rows INTEGER,
            feature_count INTEGER,
            features_used TEXT,
            
            validation_mae REAL,
            validation_rmse REAL,
            validation_r2 REAL,
            
            feature_importance TEXT,
            
            is_active BOOLEAN DEFAULT FALSE,
            deployed_at TIMESTAMP WITH TIME ZONE,
            retired_at TIMESTAMP WITH TIME ZONE,
            
            model_path VARCHAR(500)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prediction_log (
            prediction_id VARCHAR(255) PRIMARY KEY,
            forecast_id VARCHAR(255) NOT NULL,
            route_number VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            delivery_date DATE NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            
            model_id VARCHAR(255),
            predicted_quantity INTEGER,
            prediction_confidence REAL,
            
            actual_quantity INTEGER,
            prediction_error INTEGER,
            
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    print("  ✓ ML artifact tables created")


# =============================================================================
# CASE ALLOCATION TABLES
# =============================================================================

def _create_case_allocation_tables(cur) -> None:
    """Create tables for intelligent case allocation across stores."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS item_allocation_cache (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            sap VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            
            total_avg_quantity REAL,
            total_quantity_stddev REAL,
            total_orders_seen INTEGER DEFAULT 0,
            
            store_shares TEXT,
            
            avg_stores_per_order REAL,
            typical_case_count REAL,
            
            split_pattern VARCHAR(50),
            dominant_store_id VARCHAR(255),
            dominant_store_share REAL,
            
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, sap, schedule_key)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_item_shares (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            store_name VARCHAR(255),
            sap VARCHAR(20) NOT NULL,
            schedule_key VARCHAR(20) NOT NULL,
            
            share REAL NOT NULL,
            
            total_quantity INTEGER DEFAULT 0,
            order_count INTEGER DEFAULT 0,
            avg_quantity REAL,
            
            recent_share REAL,
            share_trend REAL,
            
            last_ordered_date DATE,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(route_number, store_id, sap, schedule_key)
        )
    """)
    
    print("  ✓ Case allocation tables created")


# =============================================================================
# DELIVERY ALLOCATION TABLES
# =============================================================================

def _create_delivery_allocation_tables(cur) -> None:
    """Create tables for tracking delivery allocations."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delivery_allocations (
            allocation_id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            
            source_order_id VARCHAR(255) NOT NULL,
            source_order_date DATE NOT NULL,
            
            sap VARCHAR(20) NOT NULL,
            store_id VARCHAR(255) NOT NULL,
            store_name VARCHAR(255),
            quantity INTEGER NOT NULL,
            
            delivery_date DATE NOT NULL,
            
            is_case_split BOOLEAN DEFAULT FALSE,
            
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (source_order_id) REFERENCES orders_historical(order_id)
        )
    """)
    
    print("  ✓ Delivery allocation tables created")


# =============================================================================
# ROUTE TRANSFER TABLES (INTER-ROUTE)
# =============================================================================

def _create_route_transfer_tables(cur) -> None:
    """Create tables for tracking cross-route transfers (audit layer).

    NOTE: This is intentionally separate from order_line_items and delivery_allocations.
    """
    cur.execute("""
        CREATE TABLE IF NOT EXISTS route_transfers (
            fs_doc_path TEXT PRIMARY KEY,
            route_group_id VARCHAR(20) NOT NULL,
            transfer_id VARCHAR(255) NOT NULL,

            transfer_date DATE NOT NULL,

            purchase_route_number VARCHAR(20) NOT NULL,
            from_route_number VARCHAR(20) NOT NULL,
            to_route_number VARCHAR(20) NOT NULL,

            sap VARCHAR(20) NOT NULL,
            units INTEGER NOT NULL,
            case_pack INTEGER NOT NULL,

            status VARCHAR(20) NOT NULL,
            reason VARCHAR(50),
            source_order_id VARCHAR(255),
            created_by VARCHAR(255),

            created_at TIMESTAMP WITH TIME ZONE,
            updated_at TIMESTAMP WITH TIME ZONE,
            synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
    """)

    print("  ✓ Route transfer tables created")


# =============================================================================
# ROUTE SYNC TRACKING TABLES
# =============================================================================

def _create_route_sync_tables(cur) -> None:
    """Create tables to track which routes have been synced."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS routes_synced (
            route_number VARCHAR(20) PRIMARY KEY,
            user_id VARCHAR(255),
            
            first_synced_at TIMESTAMP WITH TIME ZONE NOT NULL,
            last_synced_at TIMESTAMP WITH TIME ZONE NOT NULL,
            worker_id VARCHAR(100),
            
            stores_count INTEGER DEFAULT 0,
            products_count INTEGER DEFAULT 0,
            orders_count INTEGER DEFAULT 0,
            schedules_synced BOOLEAN DEFAULT FALSE,
            
            triggered_by VARCHAR(50),
            trigger_order_id VARCHAR(255),
            
            sync_status VARCHAR(20) DEFAULT 'ready',
            last_error TEXT,
            
            has_trained_model BOOLEAN DEFAULT FALSE,
            model_trained_at TIMESTAMP WITH TIME ZONE,
            last_forecast_at TIMESTAMP WITH TIME ZONE
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id VARCHAR(255) PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            sync_type VARCHAR(50) NOT NULL,
            started_at TIMESTAMP WITH TIME ZONE NOT NULL,
            completed_at TIMESTAMP WITH TIME ZONE,
            worker_id VARCHAR(100),
            
            status VARCHAR(20) DEFAULT 'in_progress',
            records_synced INTEGER DEFAULT 0,
            error_message TEXT,
            
            triggered_by VARCHAR(100),
            trigger_id VARCHAR(255)
        )
    """)
    
    print("  ✓ Route sync tables created")


# =============================================================================
# NOTIFICATION TABLES
# =============================================================================

def _create_notification_tables(cur) -> None:
    """Create tables for low-quantity notification tracking."""
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS low_qty_notifications_sent (
            id SERIAL PRIMARY KEY,
            route_number VARCHAR(20) NOT NULL,
            user_id VARCHAR(255) NOT NULL,
            order_by_date VARCHAR(10) NOT NULL,
            saps TEXT NOT NULL,
            saps_hash VARCHAR(32) NOT NULL,
            items_count INTEGER NOT NULL,
            sent_at TIMESTAMP WITH TIME ZONE NOT NULL,
            
            UNIQUE(route_number, user_id, order_by_date, saps_hash)
        )
    """)
    
    print("  ✓ Notification tables created")


# =============================================================================
# INDEXES
# =============================================================================

def _create_indexes(cur) -> None:
    """Create indexes for common query patterns."""
    
    indexes = [
        # Order indexes
        ("idx_orders_route_schedule", "orders_historical", "route_number, schedule_key"),
        ("idx_orders_delivery_date", "orders_historical", "delivery_date"),
        
        # Line item indexes
        ("idx_line_items_order", "order_line_items", "order_id"),
        ("idx_line_items_route_schedule", "order_line_items", "route_number, schedule_key"),
        ("idx_line_items_store_sap", "order_line_items", "store_id, sap"),
        ("idx_line_items_delivery", "order_line_items", "delivery_date"),
        
        # Correction indexes
        ("idx_corrections_store_sap", "forecast_corrections", "store_id, sap"),
        ("idx_corrections_route_schedule", "forecast_corrections", "route_number, schedule_key"),
        
        # Store alias indexes
        ("idx_store_aliases_lookup", "store_id_aliases", "route_number, alias_id"),
        
        # Store/item indexes
        ("idx_store_items_store", "store_items", "store_id"),
        ("idx_store_items_sap", "store_items", "sap"),
        
        # Feature cache index
        ("idx_feature_cache_lookup", "feature_cache", "route_number, store_id, sap, schedule_key"),
        
        # Promo indexes
        ("idx_promo_items_promo", "promo_items", "promo_id"),
        ("idx_promo_dates", "promo_history", "start_date, end_date"),
        ("idx_promo_order_history_lookup", "promo_order_history", "route_number, promo_id, sap"),
        ("idx_promo_order_history_order", "promo_order_history", "order_id"),
        ("idx_promo_email_queue_status", "promo_email_queue", "route_number, status"),
        ("idx_sap_corrections_lookup", "sap_corrections", "route_number, wrong_sap"),
        
        # Case allocation indexes
        ("idx_item_alloc_lookup", "item_allocation_cache", "route_number, sap, schedule_key"),
        ("idx_store_item_shares_lookup", "store_item_shares", "route_number, sap, schedule_key"),
        ("idx_store_item_shares_store", "store_item_shares", "store_id, sap"),
        
        # Delivery allocation indexes
        ("idx_delivery_manifest", "delivery_allocations", "route_number, delivery_date, store_id"),
        ("idx_delivery_by_order", "delivery_allocations", "source_order_id"),

        # Route transfer indexes (inter-route)
        ("idx_route_transfers_group_date", "route_transfers", "route_group_id, transfer_date"),
        ("idx_route_transfers_from", "route_transfers", "from_route_number"),
        ("idx_route_transfers_to", "route_transfers", "to_route_number"),
        ("idx_route_transfers_sap_date", "route_transfers", "sap, transfer_date"),
    ]
    
    for name, table, columns in indexes:
        try:
            cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({columns})")
        except Exception as e:
            print(f"  ⚠️  Index {name}: {e}")
    
    print("  ✓ Indexes created")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_table_counts(conn: psycopg2.extensions.connection) -> dict:
    """Get row counts for all tables."""
    cur = conn.cursor()
    
    tables = [
        'orders_historical', 'order_line_items', 'forecast_corrections',
        'store_id_aliases', 'user_schedules', 'stores', 'store_items', 'product_catalog', 'order_guide',
        'promo_history', 'promo_items', 'promo_order_history', 'promo_email_queue', 'sap_corrections',
        'calendar_features', 'seasonal_adjustments',
        'feature_cache', 'model_metadata', 'prediction_log',
        'item_allocation_cache', 'store_item_shares',
        'delivery_allocations',
        'route_transfers',
        'routes_synced', 'sync_log',
        'low_qty_notifications_sent'
    ]
    
    counts = {}
    for table in tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            counts[table] = cur.fetchone()[0]
        except Exception:
            counts[table] = -1
    
    cur.close()
    return counts


def print_schema_summary(conn: psycopg2.extensions.connection) -> None:
    """Print a summary of the database schema."""
    counts = get_table_counts(conn)
    
    print("\n📊 PostgreSQL Database Summary")
    print("=" * 50)
    
    sections = {
        "Order Data": ['orders_historical', 'order_line_items', 'forecast_corrections'],
        "User Params": ['store_id_aliases', 'user_schedules', 'stores', 'store_items', 'product_catalog', 'order_guide'],
        "Promos": ['promo_history', 'promo_items', 'promo_order_history', 'promo_email_queue', 'sap_corrections'],
        "Calendar": ['calendar_features', 'seasonal_adjustments'],
        "ML Artifacts": ['feature_cache', 'model_metadata', 'prediction_log'],
        "Case Allocation": ['item_allocation_cache', 'store_item_shares'],
        "Delivery": ['delivery_allocations'],
        "Route Sync": ['routes_synced', 'sync_log'],
        "Notifications": ['low_qty_notifications_sent']
    }
    
    for section, tables in sections.items():
        print(f"\n{section}:")
        for table in tables:
            count = counts.get(table, -1)
            status = f"{count:,} rows" if count >= 0 else "❌ missing"
            print(f"  {table}: {status}")


# =============================================================================
# CLI
# =============================================================================

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='PostgreSQL Schema Manager')
    parser.add_argument('--host', default=PG_HOST, help='PostgreSQL host')
    parser.add_argument('--port', type=int, default=PG_PORT, help='PostgreSQL port')
    parser.add_argument('--database', default=PG_DATABASE, help='Database name')
    parser.add_argument('--user', default=PG_USER, help='Username')
    parser.add_argument('--password', default=os.environ.get('PGPASSWORD', PG_PASSWORD),
                        help='Password (or set PGPASSWORD env var)')
    parser.add_argument('--create', action='store_true', help='Create schema')
    parser.add_argument('--summary', action='store_true', help='Print schema summary')
    args = parser.parse_args()
    
    print(f"📁 PostgreSQL: {args.user}@{args.host}:{args.port}/{args.database}")
    
    try:
        conn = get_connection(
            host=args.host,
            port=args.port,
            database=args.database,
            user=args.user,
            password=args.password,
        )
        
        if args.create:
            create_schema(conn)
        
        if args.summary or not args.create:
            print_schema_summary(conn)
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nMake sure PostgreSQL is running and the database exists:")
        print(f"  sudo -u postgres createuser -P {args.user}")
        print(f"  sudo -u postgres createdb -O {args.user} {args.database}")
