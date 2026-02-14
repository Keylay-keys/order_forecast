"""Retraining scheduler: triggers model retraining after a complete order cycle.

This module checks if all order days in a user's schedule have been completed
for the current week, and triggers retraining when the cycle is complete.

The logic:
1. Sync latest schedules from Firebase
2. Get order days from user_schedules table
3. Check which days have corrections this week
4. If all scheduled order days have corrections → trigger retrain

Usage:
    # Check if ready to retrain (dry run)
    python scripts/retrain_scheduler.py --route 989262 --check-only

    # Check and retrain if ready
    python scripts/retrain_scheduler.py --route 989262

    # Force retrain regardless of cycle status
    python scripts/retrain_scheduler.py --route 989262 --force
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Handle imports for both direct execution and module import
try:
    from .db_schema import get_connection
    from .db_sync import DuckDBSync
except ImportError:
    from db_schema import get_connection
    from db_sync import DuckDBSync


DEFAULT_DB_PATH = Path(__file__).parent.parent / 'data' / 'analytics.duckdb'
DEFAULT_SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'

# Minimum orders required before training makes sense
# Matches MIN_ORDERS_FOR_TRAINING in forecast_engine.py
MIN_ORDERS_FOR_TRAINING = 8


def get_week_start(date: datetime) -> datetime:
    """Get the Monday of the week containing the given date."""
    # weekday() returns 0 for Monday, 6 for Sunday
    days_since_monday = date.weekday()
    return date - timedelta(days=days_since_monday)


def get_scheduled_order_days(conn, route_number: str, user_id: Optional[str] = None) -> set:
    """Get the set of order days (1-7, Mon-Sun) from user schedules.

    Args:
        conn: DuckDB connection.
        route_number: Route to check.
        user_id: Optional specific user. If None, returns all users' days combined.

    Returns:
        Set of order day numbers (1=Monday, 7=Sunday).
    """
    if user_id:
        result = conn.execute("""
            SELECT DISTINCT order_day
            FROM user_schedules
            WHERE route_number = ? AND user_id = ? AND is_active = TRUE
        """, [route_number, user_id]).fetchall()
    else:
        result = conn.execute("""
            SELECT DISTINCT order_day
            FROM user_schedules
            WHERE route_number = ? AND is_active = TRUE
        """, [route_number]).fetchall()

    return {row[0] for row in result}


def get_correction_days_this_week(conn, route_number: str) -> set:
    """Get the set of days (1-7) that have corrections submitted this week.

    Args:
        conn: DuckDB connection.
        route_number: Route to check.

    Returns:
        Set of day numbers (1=Monday, 7=Sunday) with corrections.
    """
    week_start = get_week_start(datetime.now())

    # DuckDB DAYOFWEEK returns 0=Sunday, 1=Monday, ..., 6=Saturday
    # We want 1=Monday, ..., 7=Sunday to match the schedule format
    result = conn.execute("""
        SELECT DISTINCT
            CASE DAYOFWEEK(submitted_at)
                WHEN 0 THEN 7  -- Sunday -> 7
                ELSE DAYOFWEEK(submitted_at)  -- Mon=1, ..., Sat=6
            END as order_day
        FROM forecast_corrections
        WHERE route_number = ?
        AND submitted_at >= ?
    """, [route_number, week_start.strftime('%Y-%m-%d')]).fetchall()

    return {row[0] for row in result}


def get_order_days_this_week(conn, route_number: str) -> set:
    """Get the set of days (1-7) that have orders finalized this week.

    Fallback for when corrections aren't being tracked yet.

    Args:
        conn: DuckDB connection.
        route_number: Route to check.

    Returns:
        Set of day numbers (1=Monday, 7=Sunday) with orders.
    """
    week_start = get_week_start(datetime.now())

    result = conn.execute("""
        SELECT DISTINCT
            CASE DAYOFWEEK(order_date)
                WHEN 0 THEN 7
                ELSE DAYOFWEEK(order_date)
            END as order_day
        FROM orders_historical
        WHERE route_number = ?
        AND order_date >= ?
    """, [route_number, week_start.strftime('%Y-%m-%d')]).fetchall()

    return {row[0] for row in result}


def get_total_order_count(conn, route_number: str) -> int:
    """Get total number of orders for this route."""
    result = conn.execute("""
        SELECT COUNT(*) FROM orders_historical WHERE route_number = ?
    """, [route_number]).fetchone()
    return result[0] if result else 0


def get_order_count_per_schedule(conn, route_number: str) -> dict:
    """Get order count grouped by schedule_key.
    
    Returns:
        Dict mapping schedule_key -> order_count
        e.g. {'monday': 3, 'thursday': 8}
    """
    result = conn.execute("""
        SELECT schedule_key, COUNT(*) as cnt
        FROM orders_historical
        WHERE route_number = ?
        GROUP BY schedule_key
    """, [route_number]).fetchall()
    return {row[0]: row[1] for row in result if row[0]}


def should_retrain(
    conn,
    route_number: str,
    user_id: Optional[str] = None,
    use_orders_fallback: bool = True
) -> tuple[bool, dict]:
    """Check if we should trigger retraining for this route.

    Args:
        conn: DuckDB connection.
        route_number: Route to check.
        user_id: Optional specific user to check.
        use_orders_fallback: If True, also check orders_historical when no corrections.

    Returns:
        Tuple of (should_retrain: bool, details: dict).
    """
    day_names = ['', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    day_to_schedule = {1: 'monday', 2: 'tuesday', 3: 'wednesday', 4: 'thursday', 
                       5: 'friday', 6: 'saturday', 7: 'sunday', 0: 'sunday'}

    # Get scheduled order days first
    scheduled_days = get_scheduled_order_days(conn, route_number, user_id)

    if not scheduled_days:
        return False, {
            'reason': 'no_schedule',
            'message': 'No order schedule configured for this route',
        }

    # Check minimum order threshold PER SCHEDULE (per ORDER day)
    # schedule_key in orders_historical is based on ORDER day
    order_counts = get_order_count_per_schedule(conn, route_number)
    
    schedules_below_threshold = []
    for order_day in scheduled_days:
        schedule_key = day_to_schedule.get(order_day, 'unknown')
        count = order_counts.get(schedule_key, 0)
        if count < MIN_ORDERS_FOR_TRAINING:
            schedules_below_threshold.append((day_names[order_day], count))
    
    if schedules_below_threshold:
        details_str = ', '.join([f"{name}: {cnt}" for name, cnt in schedules_below_threshold])
        return False, {
            'reason': 'insufficient_data',
            'message': f'Need {MIN_ORDERS_FOR_TRAINING}+ orders per schedule. Low: {details_str}',
            'order_counts': order_counts,
            'min_required': MIN_ORDERS_FOR_TRAINING,
            'scheduled_days': sorted(scheduled_days),
            'scheduled_days_names': [day_names[d] for d in sorted(scheduled_days)],
        }

    # Get days with corrections this week
    completed_days = get_correction_days_this_week(conn, route_number)

    # Fallback to orders if no corrections yet
    if not completed_days and use_orders_fallback:
        completed_days = get_order_days_this_week(conn, route_number)

    # Check if all scheduled days are complete
    missing_days = scheduled_days - completed_days
    all_complete = len(missing_days) == 0

    details = {
        'scheduled_days': sorted(scheduled_days),
        'scheduled_days_names': [day_names[d] for d in sorted(scheduled_days)],
        'completed_days': sorted(completed_days),
        'completed_days_names': [day_names[d] for d in sorted(completed_days)],
        'missing_days': sorted(missing_days),
        'missing_days_names': [day_names[d] for d in sorted(missing_days)],
        'week_start': get_week_start(datetime.now()).strftime('%Y-%m-%d'),
        'all_complete': all_complete,
    }

    if all_complete:
        details['reason'] = 'cycle_complete'
        details['message'] = f"All {len(scheduled_days)} order days complete for this week"
    else:
        details['reason'] = 'cycle_incomplete'
        details['message'] = f"Waiting for orders on: {', '.join(details['missing_days_names'])}"

    return all_complete, details


def trigger_retrain(route_number: str, db_path: Path) -> dict:
    """Trigger the retraining pipeline.

    Args:
        route_number: Route to retrain.
        db_path: Path to DuckDB database.

    Returns:
        Dict with training results.
    """
    print(f"\n🚀 Triggering retraining for route {route_number}...")

    # For now, we'll just prepare the data and indicate what would happen
    # In production, this would call the actual training pipeline

    conn = get_connection(db_path)

    # Get training data stats
    stats = {}

    result = conn.execute("""
        SELECT COUNT(*) as total_orders,
               COUNT(DISTINCT schedule_key) as schedules,
               MIN(delivery_date) as earliest,
               MAX(delivery_date) as latest
        FROM orders_historical
        WHERE route_number = ?
    """, [route_number]).fetchone()

    stats['total_orders'] = result[0]
    stats['schedules'] = result[1]
    stats['date_range'] = f"{result[2]} to {result[3]}"

    result = conn.execute("""
        SELECT COUNT(*) FROM forecast_corrections WHERE route_number = ?
    """, [route_number]).fetchone()
    stats['total_corrections'] = result[0]

    result = conn.execute("""
        SELECT COUNT(*) FROM order_line_items WHERE route_number = ?
    """, [route_number]).fetchone()
    stats['total_line_items'] = result[0]

    conn.close()

    print(f"   📊 Training data: {stats['total_orders']} orders, {stats['total_line_items']} line items")
    print(f"   📊 Corrections: {stats['total_corrections']} feedback records")
    print(f"   📊 Date range: {stats['date_range']}")

    # TODO: Actually trigger training pipeline here
    # from . import incremental_trainer
    # incremental_trainer.train_from_duckdb(route_number, db_path)

    print("   ⚠️  Training pipeline integration pending - data is ready")

    return {
        'status': 'ready',
        'route': route_number,
        'stats': stats,
    }


def main():
    parser = argparse.ArgumentParser(description='Retraining scheduler')
    parser.add_argument('--route', required=True, help='Route number to check/retrain')
    parser.add_argument('--db', default=str(DEFAULT_DB_PATH), help='Database path')
    parser.add_argument('--service-account', default=DEFAULT_SA_PATH, help='Firebase service account JSON')
    parser.add_argument('--user', help='Specific user ID to check (optional)')
    parser.add_argument('--check-only', action='store_true', help='Only check status, do not retrain')
    parser.add_argument('--force', action='store_true', help='Force retrain regardless of cycle status')
    parser.add_argument('--skip-sync', action='store_true', help='Skip Firebase sync (use existing local data)')
    args = parser.parse_args()

    db_path = Path(args.db)

    print(f"📁 Database: {db_path}")
    print(f"🛤️  Route: {args.route}")

    # Step 1: Sync latest schedules from Firebase (unless skipped)
    if not args.skip_sync:
        print("\n🔄 Syncing latest schedules from Firebase...")
        sync = DuckDBSync(str(db_path), args.service_account)
        count = sync.sync_user_schedules(args.route)
        print(f"   ✅ Synced {count} schedule cycles")

        sync.close()

    # Step 2: Check if cycle is complete
    print("\n📅 Checking order cycle status...")
    conn = get_connection(db_path)

    ready, details = should_retrain(conn, args.route, args.user)

    print(f"\n   Schedule: {', '.join(details.get('scheduled_days_names', []))}")
    print(f"   Completed: {', '.join(details.get('completed_days_names', [])) or 'None'}")
    print(f"   Missing: {', '.join(details.get('missing_days_names', [])) or 'None'}")
    print(f"   Week of: {details.get('week_start', 'unknown')}")
    print(f"\n   Status: {details['message']}")

    conn.close()

    # Step 3: Retrain if ready (or forced)
    if args.check_only:
        print("\n⏸️  Check-only mode, not triggering retrain")
        return 0

    if ready or args.force:
        if args.force and not ready:
            print("\n⚠️  Force flag set, retraining despite incomplete cycle")

        result = trigger_retrain(args.route, db_path)
        print(f"\n✅ Retraining complete: {result['status']}")
    else:
        print("\n⏳ Cycle not complete, waiting for more orders")

    return 0


if __name__ == '__main__':
    exit(main())
