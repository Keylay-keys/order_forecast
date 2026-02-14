#!/usr/bin/env python3
"""Retrain daemon: runs continuously, checking for cycle completion.

Multi-user: Discovers routes via PostgreSQL.
Uses direct PostgreSQL connections (no DB Manager / DuckDB).

This daemon:
1. Gets all synced routes via PostgreSQL
2. For each route: checks if cycle complete
3. If complete, triggers retrain
4. Sleeps and repeats

Usage:
    # Run for all synced routes (default)
    python scripts/retrain_daemon.py --service-account /path/to/sa.json

    # Check every 2 hours instead of default 24 hours
    python scripts/retrain_daemon.py --interval 7200
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta

from google.cloud import firestore

try:
    from .forecast_engine import ForecastConfig, generate_forecast
    from .pg_utils import execute, fetch_all, fetch_one
    from .band_calibration import calibrate_route_if_due
    from .learning_snapshot_refresh import refresh_learning_snapshots
except ImportError:
    from forecast_engine import ForecastConfig, generate_forecast
    from pg_utils import execute, fetch_all, fetch_one
    from band_calibration import calibrate_route_if_due
    from learning_snapshot_refresh import refresh_learning_snapshots

DEFAULT_INTERVAL = 86400  # 24 hours (once per day)
DEFAULT_SA_PATH = '/Users/kylemacmini/Desktop/dev/firebase-tools/routespark-1f47d-firebase-adminsdk-tnv5k-b259331cbc.json'

def log(msg: str):
    """Print timestamped log message."""
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


def _parse_date(value) -> datetime | None:
    """Parse delivery date values into datetime."""
    if value is None:
        return None

    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value

    for fmt in ('%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(str(value), fmt)
        except ValueError:
            continue
    return None


def _normalize_timestamp(value):
    if value is None:
        return None
    if hasattr(value, "to_datetime"):
        return value.to_datetime()
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    return value


def get_synced_routes() -> list[str]:
    """Get all routes that are synced and ready via PostgreSQL."""
    try:
        rows = fetch_all("""
            SELECT route_number 
            FROM routes_synced 
            WHERE sync_status = 'ready'
            ORDER BY route_number
        """)
        return [row['route_number'] for row in rows]
    except Exception as e:
        log(f"⚠️  Error reading routes_synced: {e}")
        return []


def check_cycle_complete(route_number: str) -> dict:
    """Check if the current order cycle is complete for a route."""
    try:
        # Get schedules for this route
        schedules = fetch_all("""
            SELECT schedule_key, order_day, delivery_day
            FROM user_schedules
            WHERE route_number = %s AND is_active = TRUE
        """, [route_number])
        if not schedules:
            return {'status': 'no_schedules', 'schedules': [], 'completed': [], 'missing': []}
        
        # For each schedule, check if we have a recent order
        one_week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        completed = []
        missing = []
        
        for sched in schedules:
            schedule_key = sched['schedule_key']
            
            # Check for recent orders with this schedule key
            order_result = fetch_one("""
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND order_date >= %s
            """, [route_number, schedule_key, one_week_ago])
            
            cnt = order_result.get('cnt', 0) if order_result else 0
            
            if cnt > 0:
                completed.append(schedule_key)
            else:
                missing.append(schedule_key)
        
        all_schedule_keys = [s['schedule_key'] for s in schedules]
        
        return {
            'status': 'complete' if not missing else 'incomplete',
            'schedules': all_schedule_keys,
            'completed': completed,
            'missing': missing,
        }
        
    except Exception as e:
        log(f"⚠️  Error checking cycle: {e}")
        return {'status': 'error', 'error': str(e)}


def get_total_order_count(route_number: str, exclude_holidays: bool = True) -> int:
    """Get total order count across all schedules for a route."""
    try:
        if exclude_holidays:
            result = fetch_one("""
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND COALESCE(is_holiday_week, FALSE) = FALSE
            """, [route_number])
        else:
            result = fetch_one("""
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
            """, [route_number])
        return result.get('cnt', 0) if result else 0
    except Exception:
        return 0


def get_order_count(route_number: str, schedule_key: str, exclude_holidays: bool = True) -> int:
    """Get total order count for a route/schedule.
    
    Args:
        route_number: Route to check
        schedule_key: Schedule to check (monday, tuesday, etc.)
        exclude_holidays: If True, don't count holiday week orders (they're anomalies)
    
    Returns:
        Count of orders (excluding holiday weeks if exclude_holidays=True)
    """
    try:
        if exclude_holidays:
            # Don't count holiday weeks toward training minimum
            result = fetch_one("""
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
                  AND COALESCE(is_holiday_week, FALSE) = FALSE
            """, [route_number, schedule_key])
        else:
            result = fetch_one("""
                SELECT COUNT(*) as cnt
                FROM orders_historical
                WHERE route_number = %s
                  AND schedule_key = %s
            """, [route_number, schedule_key])
        return result.get('cnt', 0) if result else 0
    except Exception:
        return 0


def get_upcoming_delivery_dates(route_number: str) -> list:
    """Get the SINGLE next unordered delivery date across ALL schedules.
    
    Returns at most ONE delivery — the soonest chronological delivery that
    doesn't already have a finalized order.  This enforces serial forecast
    generation so that cross-cycle dependencies are respected:
    
        Forecast(CycleA) → Order(CycleA) → Forecast(CycleB) → Order(CycleB) → Retrain → …
    
    Returns list with 0 or 1 dict: 'delivery_date', 'schedule_key', 'delivery_day'.
    """
    try:
        schedules = fetch_all("""
            SELECT schedule_key, order_day, delivery_day
            FROM user_schedules
            WHERE route_number = %s AND is_active = TRUE
        """, [route_number])
        if not schedules:
            return []
        
        candidates = []
        today = datetime.now().date()
        
        for sched in schedules:
            delivery_dow = sched['delivery_day']  # 1=Mon, 7=Sun
            schedule_key = sched['schedule_key']
            
            # Find the NEXT occurrence of this delivery day that doesn't have an order
            # Look up to 14 days ahead (2 weeks) to handle case where current week is already ordered
            for days in range(1, 15):
                check_date = today + timedelta(days=days)
                # Python weekday: 0=Mon, 6=Sun; DB uses 1=Mon, 7=Sun
                check_dow = check_date.weekday() + 1
                if check_dow == delivery_dow:
                    delivery_date_str = check_date.strftime('%Y-%m-%d')
                    
                    # Check if an order already exists for this delivery date
                    order_check = fetch_one("""
                        SELECT COUNT(*) as cnt
                        FROM orders_historical
                        WHERE route_number = %s
                          AND schedule_key = %s
                          AND delivery_date = %s
                    """, [route_number, schedule_key, delivery_date_str])
                    
                    order_exists = (order_check.get('cnt', 0) if order_check else 0) > 0
                    
                    if order_exists:
                        log(f"    ⏭️  Skipping {delivery_date_str} ({schedule_key}) - order already finalized")
                        continue  # Look for the next week's delivery
                    
                    candidates.append({
                        'delivery_date': delivery_date_str,
                        'schedule_key': schedule_key,
                        'delivery_day': delivery_dow,
                    })
                    break  # Found an unordered delivery for this schedule
        
        if not candidates:
            return []
        
        # Return ONLY the soonest delivery (serial chain — cross-cycle dependency)
        candidates.sort(key=lambda x: x['delivery_date'])
        soonest = candidates[0]
        log(f"    📋 Next forecast target: {soonest['delivery_date']} ({soonest['schedule_key']})")
        return [soonest]
        
    except Exception as e:
        log(f"⚠️  Error getting upcoming delivery dates: {e}")
        return []


def _check_trained_model(route_number: str) -> bool:
    """Check if a trained model exists for this route (via routes_synced)."""
    try:
        row = fetch_one(
            "SELECT has_trained_model FROM routes_synced WHERE route_number = %s",
            [route_number],
        )
        return bool(row.get('has_trained_model', False)) if row else False
    except Exception:
        return False


def write_forecast_status(fb_client: firestore.Client, route_number: str, order_count: int, min_required: int, has_trained_model: bool = False):
    """Write forecast status metadata to Firebase for app/portal to read."""
    try:
        status_ref = fb_client.collection('forecasts').document(route_number)
        status_ref.set({
            'orderCount': order_count,
            'minOrdersRequired': min_required,
            'hasTrainedModel': has_trained_model,
            'lastUpdated': firestore.SERVER_TIMESTAMP,
        }, merge=True)
        model_label = "ML" if has_trained_model else "last-order fallback"
        log(f"    📝 Updated forecast status: {order_count}/{min_required} orders ({model_label})")
    except Exception as e:
        log(f"    ⚠️  Error writing forecast status: {e}")


def forecast_exists(fb_client: firestore.Client, route_number: str, delivery_date: str, schedule_key: str) -> bool:
    """Check if a valid (non-expired) forecast already exists for this date/schedule."""
    try:
        cached_ref = fb_client.collection('forecasts').document(route_number).collection('cached')
        # Query for matching forecasts
        from google.cloud.firestore_v1.base_query import FieldFilter
        query = cached_ref.where(filter=FieldFilter('deliveryDate', '==', delivery_date)).where(filter=FieldFilter('scheduleKey', '==', schedule_key))
        
        for doc in query.stream():
            data = doc.to_dict()
            expires_at = data.get('expiresAt')
            if expires_at:
                # Check if not expired
                from datetime import datetime, timezone
                if hasattr(expires_at, 'timestamp'):
                    # Firestore Timestamp
                    if expires_at.timestamp() > datetime.now(timezone.utc).timestamp():
                        return True
                else:
                    # Already a datetime
                    if expires_at > datetime.now(timezone.utc):
                        return True
            else:
                # No expiry, consider it valid
                return True
        return False
    except Exception as e:
        log(f"    ⚠️  Error checking forecast existence: {e}")
        return False


def generate_forecasts_for_route(
    fb_client: firestore.Client,
    route_number: str,
    sa_path: str
) -> int:
    """Generate forecast for the next upcoming delivery date (at most one).
    
    get_upcoming_delivery_dates() returns only the single soonest unordered
    delivery to enforce the serial forecast chain.
    
    Returns number of forecasts generated (0 or 1).
    """
    upcoming = get_upcoming_delivery_dates(route_number)
    if not upcoming:
        log(f"    No upcoming delivery dates found")
        return 0
    
    generated = 0
    for delivery in upcoming:
        delivery_date = delivery['delivery_date']
        schedule_key = delivery['schedule_key']
        
        # Skip if forecast already exists
        if forecast_exists(fb_client, route_number, delivery_date, schedule_key):
            log(f"    ⏭️  Forecast already exists for {delivery_date} ({schedule_key})")
            continue
        
        try:
            log(f"    🔮 Generating forecast for {delivery_date} ({schedule_key})...")
            
            config = ForecastConfig(
                route_number=route_number,
                delivery_date=delivery_date,
                schedule_key=schedule_key,
                service_account=sa_path,
                since_days=365,
                round_cases=True,
                ttl_days=7,
            )
            
            forecast = generate_forecast(config)
            log(f"    ✅ Forecast {forecast.forecast_id}: {len(forecast.items)} items")
            generated += 1
            
        except RuntimeError as e:
            # Whole-case invariant or other hard gate: skip emission and keep daemon running.
            log(f"    ❌ Forecast skipped (hard gate) for {delivery_date}: {e}")
        except ValueError as e:
            if 'insufficient_history' in str(e):
                log(f"    ⏳ Not enough history for {delivery_date}")
            else:
                log(f"    ⚠️  Forecast error for {delivery_date}: {e}")
        except Exception as e:
            log(f"    ❌ Forecast failed for {delivery_date}: {e}")
    
    return generated


def run_retrain_check(fb_client: firestore.Client, route_number: str, sa_path: str) -> bool:
    """Check cycle, possibly retrain, and ALWAYS attempt forecast generation.
    
    Forecast generation is decoupled from retraining — the forecast engine has
    its own fallback (copy last matching order) for low-data routes.  Retraining
    only happens when a full cycle is complete AND enough historical data exists.
    """
    # Minimum non-holiday orders required per schedule for training
    # 7 gives us ~2 months of data (minus holidays) which is reasonable
    MIN_ORDERS_FOR_TRAINING = 7
    
    log(f"  Checking route {route_number}...")
    
    # Get total order count for status reporting
    total_orders = get_total_order_count(route_number, exclude_holidays=True)
    
    # Check if a trained model exists for this route
    has_trained_model = _check_trained_model(route_number)
    
    # Check cycle status
    cycle = check_cycle_complete(route_number)
    
    if cycle['status'] == 'no_schedules':
        log(f"    No schedules configured")
        write_forecast_status(fb_client, route_number, total_orders, MIN_ORDERS_FOR_TRAINING, has_trained_model)
        return False
    
    log(f"    📅 Schedules: {', '.join(cycle['schedules'])}")
    log(f"    ✅ Completed: {', '.join(cycle['completed']) or 'none'}")
    log(f"    ⏳ Missing: {', '.join(cycle['missing']) or 'none'}")
    
    # Always update forecast status for app to read
    write_forecast_status(fb_client, route_number, total_orders, MIN_ORDERS_FOR_TRAINING, has_trained_model)
    
    # --- Retraining: only when cycle complete + enough data ---
    retrained = False
    if cycle['status'] == 'complete':
        # Check if we have enough data for each schedule (excluding holiday weeks)
        has_enough_data = True
        for schedule_key in cycle['schedules']:
            count = get_order_count(route_number, schedule_key, exclude_holidays=True)
            total_count = get_order_count(route_number, schedule_key, exclude_holidays=False)
            holiday_count = total_count - count
            
            if count < MIN_ORDERS_FOR_TRAINING:
                msg = f"  📊 Route {route_number}: Only {count} non-holiday orders for {schedule_key} (need {MIN_ORDERS_FOR_TRAINING})"
                if holiday_count > 0:
                    msg += f" [+{holiday_count} holiday weeks excluded]"
                log(msg)
                has_enough_data = False
        
        if has_enough_data:
            log(f"  🚀 Route {route_number}: Cycle complete! Retraining model...")
            
            # Trigger model retrain via training_pipeline.py
            try:
                try:
                    from .training_pipeline import run_pipeline  # type: ignore
                except Exception:
                    from training_pipeline import run_pipeline  # type: ignore
                metrics = run_pipeline(
                    orders_csv=None,
                    stock_csv=None,
                    promos=None,
                    corrections_csv=None,
                    calendar_csv=None,
                    mae_threshold=5.0,
                    rmse_threshold=8.0,
                    use_postgres=True,
                    route_number=route_number,
                    since_days=365,
                )
                log(f"    ✅ Training complete for {route_number}: {metrics}")
                retrained = True
            except Exception as e:
                log(f"    ⚠️  Training failed for {route_number}: {e}")
        else:
            log(f"  ⏳ Route {route_number}: Cycle complete but not enough data for retrain")
    else:
        log(f"  ⏳ Route {route_number}: Cycle not complete, skipping retrain")
    
    # --- Forecast generation: ALWAYS attempt (engine has last-order fallback) ---
    forecasts_generated = generate_forecasts_for_route(fb_client, route_number, sa_path)
    
    if forecasts_generated > 0:
        log(f"  ✅ Generated {forecasts_generated} forecast(s) for route {route_number}")
    else:
        log(f"  ℹ️  No new forecasts needed for route {route_number}")

    # --- Weekly band calibration: adjust p10/p90 width to target coverage ---
    band_calibration_enabled = os.environ.get("FORECAST_BAND_CALIBRATION_DAEMON_ENABLED", "1").lower() in ("1", "true", "yes")
    if band_calibration_enabled:
        try:
            calibration_result = calibrate_route_if_due(
                route_number=route_number,
                min_days_between_runs=int(os.environ.get("FORECAST_BAND_CALIBRATION_WEEKLY_DAYS", "7")),
                since_days=int(os.environ.get("FORECAST_BAND_CALIBRATION_SINCE_DAYS", "365")),
                min_train_orders=int(os.environ.get("FORECAST_BAND_CALIBRATION_MIN_TRAIN_ORDERS", "8")),
                max_folds=int(os.environ.get("FORECAST_BAND_CALIBRATION_MAX_FOLDS", "24")),
                target_coverage=float(os.environ.get("FORECAST_BAND_CALIBRATION_TARGET", "0.80")),
                min_lines=int(os.environ.get("FORECAST_BAND_CALIBRATION_MIN_LINES", "200")),
                interval_name=os.environ.get("FORECAST_BAND_INTERVAL_NAME", "p10_p90"),
                min_scale=float(os.environ.get("FORECAST_BAND_SCALE_MIN", "0.5")),
                max_scale=float(os.environ.get("FORECAST_BAND_SCALE_MAX", "8.0")),
                damping=float(os.environ.get("FORECAST_BAND_CALIBRATION_DAMPING", "1.0")),
                center_damping=float(os.environ.get("FORECAST_BAND_CALIBRATION_CENTER_DAMPING", "1.0")),
                max_center_step_units=float(os.environ.get("FORECAST_BAND_CALIBRATION_MAX_CENTER_STEP_UNITS", "12.0")),
                max_center_abs_units=float(os.environ.get("FORECAST_BAND_CENTER_OFFSET_MAX_ABS", "64.0")),
            )
            status = calibration_result.get("status")
            if status == "skipped_recent":
                log(
                    f"  ℹ️  Band calibration skipped for {route_number} "
                    f"(recent run {calibration_result.get('days_since_last', 0):.2f} days ago)"
                )
            elif status == "no_data":
                log(f"  ℹ️  Band calibration skipped for {route_number} (no backtest folds)")
            else:
                updated = int(calibration_result.get("updated", 0) or 0)
                log(f"  📐 Band calibration updated {updated} schedule(s) for {route_number}")
                for sched in calibration_result.get("schedules", []):
                    if sched.get("status") != "updated":
                        continue
                    log(
                        "    "
                        f"{sched.get('schedule_key')}: "
                        f"coverage={sched.get('observed_coverage', 0.0):.4f} "
                        f"scale {sched.get('old_scale', 1.0):.3f}->{sched.get('new_scale', 1.0):.3f} "
                        f"drift={sched.get('drift', 0.0):+.4f} "
                        f"skew={sched.get('skew', 0.0):+.4f}"
                    )
        except Exception as e:
            log(f"  ⚠️  Band calibration error for {route_number}: {e}")
    
    return retrained


def main():
    parser = argparse.ArgumentParser(description='Retrain daemon (multi-user, uses PostgreSQL)')
    parser.add_argument('--service-account', default=DEFAULT_SA_PATH, help='Firebase SA path')
    parser.add_argument('--interval', type=int, default=DEFAULT_INTERVAL, 
                        help=f'Check interval in seconds (default: {DEFAULT_INTERVAL} = 24h)')
    parser.add_argument('--once', action='store_true', help='Run once and exit')
    # Deprecated args kept for compatibility
    parser.add_argument('--db', help='(deprecated) Not used - direct PostgreSQL reads')
    parser.add_argument('--route', help='(deprecated) All routes are now checked automatically')
    args = parser.parse_args()
    
    log("=" * 60)
    log("🤖 Retrain Daemon Starting")
    log(f"   Mode: All synced routes (via PostgreSQL)")
    log(f"   Interval: {args.interval}s ({args.interval // 3600}h {(args.interval % 3600) // 60}m)")
    log("=" * 60)
    
    # Create Firestore client
    fb_client = firestore.Client.from_service_account_json(args.service_account)
    
    while True:
        try:
            # Get routes to check
            routes = get_synced_routes()
            
            if not routes:
                log("📭 No synced routes found")
            else:
                log(f"📋 Checking {len(routes)} route(s)...")
                
                retrained = 0
                retrained_routes: list[str] = []
                for route in routes:
                    if run_retrain_check(fb_client, route, args.service_account):
                        retrained += 1
                        retrained_routes.append(str(route))
                
                if retrained > 0:
                    log(f"🎉 Retrained {retrained}/{len(routes)} route(s)")
                else:
                    log(f"✓ Checked {len(routes)} route(s), none ready for retrain")

                # Weekly walk-forward snapshot refresh for web learning card.
                # Retrained routes are force-refreshed immediately; other routes refresh only if due.
                learning_refresh_enabled = os.environ.get("FORECAST_LEARNING_REFRESH_ENABLED", "1").lower() in ("1", "true", "yes")
                if learning_refresh_enabled:
                    try:
                        refresh_result = refresh_learning_snapshots(
                            routes=routes,
                            force_routes=set(retrained_routes),
                            since_days=int(os.environ.get("FORECAST_LEARNING_REFRESH_SINCE_DAYS", "365")),
                            min_train_orders=int(os.environ.get("FORECAST_LEARNING_REFRESH_MIN_TRAIN_ORDERS", "8")),
                            max_folds=int(os.environ.get("FORECAST_LEARNING_REFRESH_MAX_FOLDS", "24")),
                            min_days_between_runs=int(os.environ.get("FORECAST_LEARNING_REFRESH_DAYS", "7")),
                            output_dir=os.environ.get("FORECAST_BACKTEST_OUTPUT_DIR", "/app/logs/backtests"),
                            temporal_corrections=os.environ.get("FORECAST_LEARNING_REFRESH_DISABLE_TEMPORAL_CORRECTIONS", "0").lower() not in ("1", "true", "yes"),
                            ignore_band_calibration=os.environ.get("FORECAST_LEARNING_REFRESH_IGNORE_BAND_CALIBRATION", "0").lower() in ("1", "true", "yes"),
                            store_centric_context=os.environ.get("FORECAST_LEARNING_REFRESH_DISABLE_STORE_CENTRIC", "0").lower() not in ("1", "true", "yes"),
                        )
                        status = str(refresh_result.get("status", "unknown"))
                        if status == "ok":
                            log(
                                "  📈 Learning snapshots refreshed: "
                                f"{len(refresh_result.get('routes_refreshed', []))} route(s), "
                                f"{len(refresh_result.get('routes_no_data', []))} no-data"
                            )
                            if refresh_result.get("scorecard_path"):
                                log(f"    Scorecard: {refresh_result.get('scorecard_path')}")
                        elif status == "skipped_not_due":
                            log("  ℹ️  Learning snapshot refresh skipped (not due)")
                        elif status == "no_data":
                            log("  ℹ️  Learning snapshot refresh ran but produced no folds")
                        else:
                            log(f"  ⚠️  Learning snapshot refresh status: {status}")
                    except Exception as e:
                        log(f"  ⚠️  Learning snapshot refresh error: {e}")
        
        except Exception as e:
            log(f"❌ Error in check loop: {e}")
        
        if args.once:
            log("Single run mode, exiting.")
            break
        
        hours = args.interval // 3600
        mins = (args.interval % 3600) // 60
        log(f"💤 Sleeping for {hours}h {mins}m until next check...")
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
