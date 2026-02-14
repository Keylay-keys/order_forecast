"""Test queries to verify the DuckDB schema works for ML training.

This script runs example queries that the ML pipeline will use, verifying:
- Order history aggregations
- Feature engineering queries
- Lag/rolling calculations
- Schedule-based filtering

Usage:
    python scripts/test_db_queries.py
"""

from __future__ import annotations

import argparse
from pathlib import Path
from datetime import datetime

from db_schema import get_connection, print_schema_summary


def test_basic_counts(conn) -> bool:
    """Test basic row counts."""
    print("\n📊 Test 1: Basic Row Counts")
    print("-" * 40)
    
    tables = [
        ('orders_historical', 'Orders'),
        ('order_line_items', 'Line Items'),
        ('stores', 'Stores'),
        ('store_items', 'Store-Item Pairs'),
        ('calendar_features', 'Calendar Dates'),
    ]
    
    all_ok = True
    for table, label in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        status = "✅" if count > 0 else "⚠️"
        print(f"   {status} {label}: {count:,}")
        if count == 0:
            all_ok = False
    
    return all_ok


def test_schedule_breakdown(conn) -> bool:
    """Test orders by schedule key."""
    print("\n📊 Test 2: Orders by Schedule Key")
    print("-" * 40)
    
    result = conn.execute("""
        SELECT 
            schedule_key,
            COUNT(*) as order_count,
            MIN(delivery_date) as first_delivery,
            MAX(delivery_date) as last_delivery
        FROM orders_historical
        GROUP BY schedule_key
        ORDER BY schedule_key
    """).fetchall()
    
    if not result:
        print("   ⚠️ No orders found")
        return False
    
    for row in result:
        print(f"   ✅ {row[0]}: {row[1]} orders ({row[2]} to {row[3]})")
    
    return True


def test_store_sap_demand(conn) -> bool:
    """Test demand aggregation by store/SAP (key ML query)."""
    print("\n📊 Test 3: Demand by Store/SAP (ML Feature Query)")
    print("-" * 40)
    
    result = conn.execute("""
        SELECT 
            store_name,
            sap,
            schedule_key,
            COUNT(*) as order_count,
            AVG(quantity) as avg_qty,
            MIN(quantity) as min_qty,
            MAX(quantity) as max_qty
        FROM order_line_items
        WHERE quantity > 0
        GROUP BY store_name, sap, schedule_key
        ORDER BY order_count DESC
        LIMIT 10
    """).fetchall()
    
    if not result:
        print("   ⚠️ No demand data found")
        return False
    
    print(f"   Top 10 store/SAP combinations by frequency:")
    for row in result:
        print(f"   ✅ {row[0][:15]:15} | SAP {row[1]:6} | {row[2]:8} | "
              f"{row[3]} orders | avg: {row[4]:.1f}")
    
    return True


def test_lag_calculation(conn) -> bool:
    """Test lag feature calculation (critical for ML)."""
    print("\n📊 Test 4: Lag Feature Calculation")
    print("-" * 40)
    
    result = conn.execute("""
        WITH ordered_items AS (
            SELECT 
                store_id,
                sap,
                schedule_key,
                delivery_date,
                quantity,
                LAG(quantity, 1) OVER (
                    PARTITION BY store_id, sap, schedule_key 
                    ORDER BY delivery_date
                ) as lag_1,
                LAG(quantity, 2) OVER (
                    PARTITION BY store_id, sap, schedule_key 
                    ORDER BY delivery_date
                ) as lag_2
            FROM order_line_items
            WHERE quantity > 0
        )
        SELECT 
            store_id,
            sap,
            schedule_key,
            delivery_date,
            quantity,
            lag_1,
            lag_2
        FROM ordered_items
        WHERE lag_1 IS NOT NULL
        ORDER BY store_id, sap, delivery_date
        LIMIT 10
    """).fetchall()
    
    if not result:
        print("   ⚠️ Not enough data for lag calculations (need 2+ orders)")
        return False
    
    print(f"   Sample lag features (qty, lag_1, lag_2):")
    for row in result:
        lag1 = f"{row[5]:3}" if row[5] is not None else "N/A"
        lag2 = f"{row[6]:3}" if row[6] is not None else "N/A"
        print(f"   ✅ {row[2]:8} {row[3]} | SAP {row[1]:6} | "
              f"qty={row[4]:3} | lag_1={lag1} | lag_2={lag2}")
    
    return True


def test_rolling_mean(conn) -> bool:
    """Test rolling mean calculation."""
    print("\n📊 Test 5: Rolling Mean Calculation")
    print("-" * 40)
    
    result = conn.execute("""
        WITH rolling AS (
            SELECT 
                store_id,
                store_name,
                sap,
                schedule_key,
                delivery_date,
                quantity,
                AVG(quantity) OVER (
                    PARTITION BY store_id, sap, schedule_key 
                    ORDER BY delivery_date
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as rolling_mean_4
            FROM order_line_items
            WHERE quantity > 0
        )
        SELECT *
        FROM rolling
        WHERE rolling_mean_4 IS NOT NULL
        ORDER BY store_name, sap, delivery_date
        LIMIT 10
    """).fetchall()
    
    if not result:
        print("   ⚠️ Not enough data for rolling mean (need 4+ orders)")
        return False
    
    print(f"   Sample rolling means:")
    for row in result:
        print(f"   ✅ {row[1][:12]:12} | SAP {row[2]:6} | {row[4]} | "
              f"qty={row[5]:3} | rolling_mean={row[6]:.1f}")
    
    return True


def test_calendar_join(conn) -> bool:
    """Test joining order data with calendar features."""
    print("\n📊 Test 6: Calendar Features Join")
    print("-" * 40)
    
    result = conn.execute("""
        SELECT 
            o.delivery_date,
            o.schedule_key,
            c.day_of_week,
            c.is_first_weekend_of_month,
            c.is_holiday_week,
            c.holiday_name,
            COUNT(DISTINCT o.order_id) as orders,
            SUM(li.quantity) as total_qty
        FROM orders_historical o
        JOIN calendar_features c ON o.delivery_date = c.date
        JOIN order_line_items li ON o.order_id = li.order_id
        GROUP BY 
            o.delivery_date, o.schedule_key, 
            c.day_of_week, c.is_first_weekend_of_month, 
            c.is_holiday_week, c.holiday_name
        ORDER BY o.delivery_date
    """).fetchall()
    
    if not result:
        print("   ⚠️ Could not join orders with calendar")
        return False
    
    print(f"   Orders with calendar features:")
    for row in result:
        holiday = f"🎄 {row[5]}" if row[5] else ""
        first_wknd = "📅 1st weekend" if row[3] else ""
        print(f"   ✅ {row[0]} ({row[1][:3]}) | dow={row[2]} | "
              f"qty={row[7]:,} {first_wknd} {holiday}")
    
    return True


def test_training_data_query(conn) -> bool:
    """Test a realistic training data query."""
    print("\n📊 Test 7: ML Training Data Query")
    print("-" * 40)
    
    result = conn.execute("""
        WITH features AS (
            SELECT 
                li.store_id,
                li.store_name,
                li.sap,
                li.schedule_key,
                li.delivery_date,
                li.quantity,
                li.is_first_weekend_of_month,
                li.is_holiday_week,
                li.day_of_week,
                li.month,
                
                -- Lag features
                LAG(li.quantity, 1) OVER w as lag_1,
                LAG(li.quantity, 2) OVER w as lag_2,
                
                -- Rolling mean
                AVG(li.quantity) OVER (
                    PARTITION BY li.store_id, li.sap, li.schedule_key 
                    ORDER BY li.delivery_date
                    ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
                ) as rolling_mean_4
                
            FROM order_line_items li
            WHERE li.quantity > 0
            WINDOW w AS (
                PARTITION BY li.store_id, li.sap, li.schedule_key 
                ORDER BY li.delivery_date
            )
        )
        SELECT 
            store_name,
            sap,
            schedule_key,
            COUNT(*) as rows,
            AVG(quantity) as avg_target,
            AVG(lag_1) as avg_lag1,
            AVG(rolling_mean_4) as avg_rolling
        FROM features
        WHERE lag_1 IS NOT NULL
        GROUP BY store_name, sap, schedule_key
        HAVING COUNT(*) >= 3
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """).fetchall()
    
    if not result:
        print("   ⚠️ Not enough data for training (need 3+ samples per item)")
        return False
    
    print(f"   Top store/SAP combinations with training-ready data:")
    print(f"   {'Store':<12} | {'SAP':>6} | {'Sched':>5} | {'Rows':>4} | "
          f"{'AvgQty':>6} | {'AvgLag1':>7} | {'Rolling':>7}")
    print(f"   {'-'*12}-+-{'-'*6}-+-{'-'*5}-+-{'-'*4}-+-{'-'*6}-+-{'-'*7}-+-{'-'*7}")
    
    for row in result:
        avg_lag = f"{row[5]:.1f}" if row[5] else "N/A"
        avg_roll = f"{row[6]:.1f}" if row[6] else "N/A"
        print(f"   {row[0][:12]:<12} | {row[1]:>6} | {row[2][:5]:>5} | "
              f"{row[3]:>4} | {row[4]:>6.1f} | {avg_lag:>7} | {avg_roll:>7}")
    
    return True


def test_archive_date_list(conn) -> bool:
    """Test the archive date list query (for message bus)."""
    print("\n📊 Test 8: Archive Date List (Message Bus Query)")
    print("-" * 40)
    
    result = conn.execute("""
        SELECT DISTINCT 
            delivery_date,
            schedule_key,
            total_units,
            store_count
        FROM orders_historical
        ORDER BY delivery_date DESC
        LIMIT 10
    """).fetchall()
    
    if not result:
        print("   ⚠️ No orders for archive listing")
        return False
    
    print(f"   Available order dates:")
    for row in result:
        print(f"   ✅ {row[0]} ({row[1]}) - {row[2]:,} units, {row[3]} stores")
    
    return True


def run_all_tests(conn) -> dict:
    """Run all tests and return summary."""
    tests = [
        ('Basic Counts', test_basic_counts),
        ('Schedule Breakdown', test_schedule_breakdown),
        ('Store/SAP Demand', test_store_sap_demand),
        ('Lag Calculation', test_lag_calculation),
        ('Rolling Mean', test_rolling_mean),
        ('Calendar Join', test_calendar_join),
        ('Training Data', test_training_data_query),
        ('Archive Date List', test_archive_date_list),
    ]
    
    results = {}
    for name, test_fn in tests:
        try:
            results[name] = test_fn(conn)
        except Exception as e:
            print(f"   ❌ Error: {e}")
            results[name] = False
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Test DuckDB queries')
    parser.add_argument('--db', default='data/analytics.duckdb', help='Path to database')
    args = parser.parse_args()
    
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / args.db
    
    print("=" * 60)
    print("🧪 DuckDB Schema Test Suite")
    print("=" * 60)
    print(f"📁 Database: {db_path}")
    
    conn = get_connection(db_path, read_only=True)
    
    # Run all tests
    results = run_all_tests(conn)
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"   {status}: {name}")
    
    print(f"\n   Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Database is ready for ML training.")
    else:
        print("\n⚠️  Some tests failed. Check data or schema.")
    
    conn.close()
    
    return passed == total


if __name__ == '__main__':
    success = main()
    exit(0 if success else 1)


