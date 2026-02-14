"""Schedule/date helpers used by forecast_engine."""

from __future__ import annotations

from datetime import datetime, date
from typing import Optional, Dict

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False


def normalize_delivery_date(raw: str) -> str:
    """Accepts YYYY-MM-DD or MM/DD/YYYY and returns YYYY-MM-DD."""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Fallback: try date parser
    try:
        dt = datetime.fromisoformat(raw)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return raw  # leave as-is; caller can handle invalid


def weekday_key(date_str: str) -> str:
    """Return lowercase weekday key (monday, tuesday, ...) for a given date string."""
    try:
        dt = datetime.strptime(normalize_delivery_date(date_str), "%Y-%m-%d")
    except Exception:
        return "unknown"
    # Use UTC to avoid TZ drift
    dow = dt.date().weekday()  # Monday=0
    mapping = {
        0: "monday",
        1: "tuesday",
        2: "wednesday",
        3: "thursday",
        4: "friday",
        5: "saturday",
        6: "sunday",
    }
    return mapping.get(dow, "unknown")


def get_order_day_for_delivery(db, route_number: str, delivery_date_str: str) -> str:
    """Given a delivery date, return the ORDER day schedule_key.
    
    Reads from:
      - PostgreSQL: user_schedules table (preferred)
      - Firebase: users/{uid}/userSettings.notifications.scheduling.orderCycles
    
    Example:
        - Delivery on Thursday → returns 'monday' (ordered Monday)
        - Delivery on Monday → returns 'tuesday' (ordered Tuesday)
    
    Args:
        db: Firestore client
        route_number: Route to look up
        delivery_date_str: Delivery date in any format
    Returns:
        Order day key like 'monday', 'tuesday', etc.
        
    Raises:
        ValueError: If schedule data not found (no guessing/hardcoding)
    """
    DAY_NUM_TO_NAME = {1: 'monday', 2: 'tuesday', 3: 'wednesday', 
                       4: 'thursday', 5: 'friday', 6: 'saturday', 7: 'sunday'}
    DAY_NAME_TO_NUM = {v: k for k, v in DAY_NUM_TO_NAME.items()}
    
    # Get delivery day of week (1=Monday, 7=Sunday)
    delivery_dow = weekday_key(delivery_date_str)
    delivery_day_num = DAY_NAME_TO_NUM.get(delivery_dow, 0)
    
    # Get order cycles from data (PostgreSQL first, then Firebase/legacy)
    cycles = get_order_cycles(db, route_number)
    
    if not cycles:
        raise ValueError(
            f"[schedule_utils] No order cycles found for route {route_number}. "
            f"Check: users/{{uid}}/userSettings.notifications.scheduling.orderCycles"
        )
    
    # Find cycle that delivers on this day (check both deliveryDay and loadDay)
    for cycle in cycles:
        cycle_delivery_day = cycle.get('deliveryDay')
        cycle_load_day = cycle.get('loadDay')
        
        # Match if delivery date matches cycle's deliveryDay OR loadDay
        # (loadDay match handles stores like Bowman's that get same-day delivery)
        if delivery_day_num in (cycle_delivery_day, cycle_load_day):
            order_day = cycle.get('orderDay', 1)
            return DAY_NUM_TO_NAME.get(order_day, 'monday')
    
    raise ValueError(
        f"[schedule_utils] No cycle found for delivery day '{delivery_dow}' (day {delivery_day_num}). "
        f"Available cycles: {cycles}"
    )


def _get_pg_connection():
    """Get PostgreSQL connection using environment variables."""
    if not PG_AVAILABLE:
        raise RuntimeError("psycopg2 not available; cannot query PostgreSQL.")
    import os
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=int(os.environ.get('POSTGRES_PORT', 5432)),
        database=os.environ.get('POSTGRES_DB', 'routespark'),
        user=os.environ.get('POSTGRES_USER', 'routespark'),
        password=os.environ.get('POSTGRES_PASSWORD', ''),
    )


def get_order_cycles_from_postgres(route_number: str) -> list:
    """Get user's order cycles from PostgreSQL (preferred)."""
    if not PG_AVAILABLE:
        return []
    try:
        conn = _get_pg_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT order_day, load_day, delivery_day
                FROM user_schedules
                WHERE route_number = %s AND is_active = TRUE
                ORDER BY order_day
                """,
                [route_number],
            )
            rows = cur.fetchall()
        cycles = []
        for row in rows:
            cycles.append({
                'orderDay': row.get('order_day'),
                'loadDay': row.get('load_day'),
                'deliveryDay': row.get('delivery_day'),
            })
        return cycles
    except Exception as e:
        print(f"[schedule_utils] Warning: Could not get order cycles from PostgreSQL: {e}")
        return []
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_order_cycles(db, route_number: str) -> list:
    """Get user's order cycles - tries PostgreSQL first, falls back to Firebase.
    
    Args:
        db: Firestore client (for fallback)
        route_number: Route to look up
    
    Returns list of cycles like:
        [{'orderDay': 1, 'loadDay': 3, 'deliveryDay': 4}, 
         {'orderDay': 2, 'loadDay': 5, 'deliveryDay': 1}]
    """
    # Try PostgreSQL first
    cycles = get_order_cycles_from_postgres(route_number)
    if cycles:
        return cycles

    # Fallback to Firebase
    from google.cloud.firestore_v1.base_query import FieldFilter
    
    try:
        user_id = None
        
        # First, try to find the user via route document
        routes_ref = db.collection('routes').document(route_number)
        route_doc = routes_ref.get()
        
        if route_doc.exists:
            route_data = route_doc.to_dict() or {}
            user_id = route_data.get('userId')
        
        # If no route doc, find the OWNER user (role='owner') with this route
        if not user_id:
            users_ref = db.collection('users')
            query = users_ref.where(
                filter=FieldFilter('profile.routeNumber', '==', route_number)
            ).where(
                filter=FieldFilter('profile.role', '==', 'owner')
            ).limit(1)
            docs = list(query.stream())
            if docs:
                user_id = docs[0].id
        
        # Still not found? Try currentRoute field
        if not user_id:
            query = users_ref.where(
                filter=FieldFilter('profile.currentRoute', '==', route_number)
            ).where(
                filter=FieldFilter('profile.role', '==', 'owner')
            ).limit(1)
            docs = list(query.stream())
            if docs:
                user_id = docs[0].id
        
        if user_id:
            user_ref = db.collection('users').document(user_id)
            user_doc = user_ref.get()
            if user_doc.exists:
                user_data = user_doc.to_dict() or {}
                
                # Path: userSettings.notifications.scheduling.orderCycles
                user_settings = user_data.get('userSettings', {})
                notifications = user_settings.get('notifications', {})
                scheduling = notifications.get('scheduling', {})
                cycles = scheduling.get('orderCycles', [])
                
                if cycles:
                    return cycles
                
                # Fallback: check old path settings.orderCycles
                settings = user_data.get('settings', {})
                return settings.get('orderCycles', [])
    except Exception as e:
        print(f"[schedule_utils] Warning: Could not get order cycles from Firebase: {e}")
    return []


def get_days_until_next_delivery(db, route_number: str, current_delivery_dow: int) -> int:
    """Calculate days from current delivery day to next delivery day.
    
    Args:
        db: Firestore client
        route_number: Route to look up
        current_delivery_dow: Current delivery day of week (0=Mon, 6=Sun - Python convention)
        
    Returns:
        Number of days until next delivery (e.g., 4 for Thu→Mon, 3 for Mon→Thu)
    """
    cycles = get_order_cycles(db, route_number)
    
    if not cycles:
        # Default: assume 2x/week, 4 days apart
        return 4
    
    # Convert Python DOW (0=Mon) to Firebase DOW (0=Sun, 1=Mon)
    # Python: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
    # Firebase: Sun=0, Mon=1, Tue=2, Wed=3, Thu=4, Fri=5, Sat=6
    fb_current_dow = (current_delivery_dow + 1) % 7 if current_delivery_dow < 6 else 0
    # Actually simpler: Python Mon=0 → Firebase Mon=1, Python Sun=6 → Firebase Sun=0
    fb_current_dow = (current_delivery_dow + 1) % 7
    
    # Get all delivery days from cycles
    delivery_days = sorted([c.get('deliveryDay', 0) for c in cycles])
    
    if len(delivery_days) <= 1:
        # Single delivery per week, next is 7 days away
        return 7
    
    # Find next delivery day after current
    for day in delivery_days:
        if day > fb_current_dow:
            # Days until this delivery
            return day - fb_current_dow
    
    # Wrap around to next week's first delivery
    first_delivery = delivery_days[0]
    return (7 - fb_current_dow) + first_delivery


def get_schedule_info(db, route_number: str) -> Dict:
    """Get comprehensive schedule info for a route.
    
    Args:
        db: Firestore client (for fallback)
        route_number: Route to look up
    
    Returns dict with:
        - cycles: list of order cycles
        - delivery_days: list of delivery day numbers (Firebase convention)
        - days_between_deliveries: dict mapping delivery_day -> days until next
    """
    cycles = get_order_cycles(db, route_number)
    
    if not cycles:
        return {
            'cycles': [],
            'delivery_days': [],
            'days_between_deliveries': {},
        }
    
    delivery_days = sorted([c.get('deliveryDay', 0) for c in cycles])
    
    # Calculate days between each delivery day and the next
    days_between = {}
    for i, day in enumerate(delivery_days):
        if i < len(delivery_days) - 1:
            next_day = delivery_days[i + 1]
            days_between[day] = next_day - day
        else:
            # Wrap to first delivery of next week
            first_day = delivery_days[0]
            days_between[day] = (7 - day) + first_day
    
    return {
        'cycles': cycles,
        'delivery_days': delivery_days,
        'days_between_deliveries': days_between,
    }


# =============================================================================
# DATA-DRIVEN STORE/CYCLE MATCHING
# =============================================================================

DAY_NUM_TO_NAME = {1: 'monday', 2: 'tuesday', 3: 'wednesday', 
                   4: 'thursday', 5: 'friday', 6: 'saturday', 7: 'sunday'}
DAY_NAME_TO_NUM = {'monday': 1, 'tuesday': 2, 'wednesday': 3, 'thursday': 4,
                   'friday': 5, 'saturday': 6, 'sunday': 7}


def day_num_to_name(day_num: int) -> str:
    """Convert day number (1=Mon...7=Sun) to lowercase name."""
    return DAY_NUM_TO_NAME.get(day_num, 'unknown')


def day_name_to_num(day_name: str) -> int:
    """Convert day name to number (1=Mon...7=Sun)."""
    return DAY_NAME_TO_NUM.get(day_name.lower(), 0)


def get_legacy_schedule_keys(db, route_number: str, schedule_key: str) -> list[str]:
    """Return schedule keys to include for legacy (delivery-day) data.

    New orders use order-day schedule_key. Legacy orders used delivery-day.
    This maps order-day -> delivery-day using user order cycles.
    """
    keys = [schedule_key]
    try:
        cycles = get_order_cycles(db, route_number)
        if not cycles:
            return keys

        # Active schedule keys are order-day names. If the derived legacy delivery-day key
        # collides with an active order-day key (common when a cycle delivers on e.g. Monday
        # and another cycle orders on Monday), including it would mix two distinct cycles and
        # can catastrophically skew the model. In that case, we skip the legacy key.
        active_order_keys = set()
        for c in cycles:
            od = c.get('orderDay')
            if od:
                active_order_keys.add(day_num_to_name(int(od)))

        order_day_num = day_name_to_num(schedule_key)
        if not order_day_num:
            return keys

        for cycle in cycles:
            if cycle.get('orderDay') == order_day_num:
                legacy_day = cycle.get('deliveryDay')
                if legacy_day:
                    legacy_key = day_num_to_name(legacy_day)
                    if legacy_key and legacy_key not in keys:
                        if legacy_key in active_order_keys and legacy_key != schedule_key:
                            # Collision: don't mix cycles.
                            break
                        keys.append(legacy_key)
                break
    except Exception:
        return keys

    return keys


def get_cycle_delivery_days(cycle: dict) -> set:
    """Get the days a cycle delivers on (both loadDay and deliveryDay).
    
    A cycle can deliver on:
    - deliveryDay: The main delivery day for most stores
    - loadDay: For stores that get same-day delivery (e.g., Bowman's on Friday)
    
    Returns:
        Set of day names (lowercase) this cycle serves
    """
    days = set()
    if 'deliveryDay' in cycle:
        days.add(day_num_to_name(cycle['deliveryDay']))
    if 'loadDay' in cycle:
        days.add(day_num_to_name(cycle['loadDay']))
    return days


def store_in_cycle(store_delivery_days: list, cycle: dict) -> bool:
    """Check if a store belongs to an order cycle.
    
    A store belongs to a cycle if ANY of its deliveryDays matches
    the cycle's deliveryDay OR loadDay.
    
    Args:
        store_delivery_days: List of day names from store.deliveryDays (e.g., ["Thursday", "Monday"])
        cycle: Order cycle dict with orderDay, loadDay, deliveryDay
        
    Returns:
        True if store is served by this cycle
    """
    store_days = {d.lower() for d in store_delivery_days}
    cycle_days = get_cycle_delivery_days(cycle)
    return bool(store_days & cycle_days)


def get_stores_for_cycle(stores_cfg: list, cycle: dict) -> list:
    """Get all stores that belong to an order cycle.
    
    Reads store.deliveryDays and matches against cycle's deliveryDay/loadDay.
    No hardcoding - pure data match.
    
    Args:
        stores_cfg: List of StoreConfig objects with delivery_days attribute
        cycle: Order cycle dict with orderDay, loadDay, deliveryDay
        
    Returns:
        List of stores that are served by this cycle
    """
    return [s for s in stores_cfg if store_in_cycle(s.delivery_days or [], cycle)]


def get_cycle_for_store(store_delivery_days: list, order_cycles: list) -> Optional[dict]:
    """Find which order cycle serves a store on a given delivery day.
    
    Args:
        store_delivery_days: List of day names from store.deliveryDays
        order_cycles: List of order cycle dicts
        
    Returns:
        The cycle dict that serves this store, or None
    """
    for cycle in order_cycles:
        if store_in_cycle(store_delivery_days, cycle):
            return cycle
    return None


def get_cycle_weight(order_cycles: list, order_day: int) -> Dict:
    """Calculate how 'heavy' a cycle is based on days until next delivery.
    
    Derived entirely from the order cycles data.
    
    Args:
        order_cycles: List of order cycle dicts
        order_day: The orderDay number (1=Mon, etc.) of the target cycle
        
    Returns:
        Dict with:
        - days_covered: Days until next cycle's delivery
        - covers_weekend: True if delivery period includes Sat or Sun
    """
    if not order_cycles or len(order_cycles) < 2:
        return {'days_covered': 7, 'covers_weekend': True}
    
    # Sort cycles by delivery day
    sorted_cycles = sorted(order_cycles, key=lambda c: c.get('deliveryDay', 0))
    
    # Find target cycle
    target = next((c for c in order_cycles if c.get('orderDay') == order_day), None)
    if not target:
        return {'days_covered': 7, 'covers_weekend': True}
    
    target_idx = sorted_cycles.index(target)
    next_idx = (target_idx + 1) % len(sorted_cycles)
    next_cycle = sorted_cycles[next_idx]
    
    # Days until next cycle's delivery
    target_delivery = target.get('deliveryDay', 1)
    next_delivery = next_cycle.get('deliveryDay', 1)
    
    days_covered = (next_delivery - target_delivery) % 7
    if days_covered == 0:
        days_covered = 7
    
    # Check if delivery period covers weekend (Sat=6, Sun=7)
    covers_weekend = False
    for d in range(days_covered):
        day_num = ((target_delivery - 1 + d) % 7) + 1  # 1-indexed
        if day_num in (6, 7):  # Saturday or Sunday
            covers_weekend = True
            break
    
    return {
        'days_covered': days_covered,
        'covers_weekend': covers_weekend,
    }
