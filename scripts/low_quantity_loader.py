"""Low quantity items loader for forecast floor injection.

Identifies items with a single viable expiry date (low quantity = one case left in market)
and computes the correct order date to prevent shelf gaps.

Ported from: src/components/dashboard/utils/lowQuantityItems.ts

Usage:
    from low_quantity_loader import get_items_for_order_date, get_stores_for_sap

    # Get low-qty items that need ordering on a specific date
    items = get_items_for_order_date(db, 'RTK-123456', '2026-01-20')

    # Get stores that stock a specific SAP
    stores = get_stores_for_sap('RTK-123456', '31032')
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set

try:
    import pytz
    HAS_PYTZ = True
except ImportError:
    HAS_PYTZ = False

try:
    from .schedule_utils import get_order_cycles, normalize_delivery_date
except ImportError:
    from schedule_utils import get_order_cycles, normalize_delivery_date

try:
    from .pg_utils import fetch_all
except ImportError:
    from pg_utils import fetch_all

try:
    from google.cloud.firestore_v1.base_query import FieldFilter
except ImportError:
    FieldFilter = None  # Will be imported later if needed


# =============================================================================
# TIMEZONE HELPERS
# =============================================================================

def get_user_timezone(db, route_number: str) -> Optional[str]:
    """Get user's timezone from Firebase profile.

    Path: users/{userId}/profile/timezone

    Returns timezone string like 'America/Denver' or None if not found.
    """
    try:
        # Find user for this route
        routes_ref = db.collection('routes').document(route_number)
        route_doc = routes_ref.get()

        if not route_doc.exists:
            return None

        route_data = route_doc.to_dict() or {}
        user_id = route_data.get('userId')

        if not user_id:
            return None

        # Get user's timezone from profile
        user_ref = db.collection('users').document(user_id)
        user_doc = user_ref.get()

        if not user_doc.exists:
            return None

        user_data = user_doc.to_dict() or {}
        profile = user_data.get('profile', {})
        timezone = profile.get('timezone')

        if timezone:
            print(f"[low_qty] Using user timezone: {timezone}")

        return timezone

    except Exception as e:
        print(f"[low_qty] Warning: Could not get user timezone: {e}")
        return None


def get_current_datetime(timezone_str: Optional[str] = None) -> datetime:
    """Get current datetime in the specified timezone.

    Falls back to UTC if timezone is not specified or pytz is not available.
    """
    if timezone_str and HAS_PYTZ:
        try:
            tz = pytz.timezone(timezone_str)
            return datetime.now(tz).replace(tzinfo=None)  # Return naive datetime in user's TZ
        except Exception as e:
            print(f"[low_qty] Warning: Invalid timezone '{timezone_str}': {e}")

    # Fallback to server local time
    return datetime.now()


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class PCFItem:
    """Raw item from PCF container."""
    sap: str                    # product field from PCF (SAP code)
    description: str
    best_before: str            # Expiry date (various formats)
    visible_on_list: bool
    delivery_number: str
    container_code: str


@dataclass
class LowQuantityItem:
    """Item identified as low quantity (single viable expiry)."""
    sap: str
    expiry_date: str            # ISO format YYYY-MM-DD
    order_by_date: str          # ISO format YYYY-MM-DD
    delivery_date: str          # ISO format YYYY-MM-DD (when replacement arrives)
    days_left: int
    will_have_gap: bool         # True if no viable order cycle exists
    container_code: str
    delivery_number: str


# =============================================================================
# DATE PARSING HELPERS
# =============================================================================

def parse_expiry_date(date_string: str) -> Optional[datetime]:
    """Parse expiry date from various formats (MM/DD/YYYY, YYYY-MM-DD, etc.)."""
    if not date_string:
        return None

    date_string = date_string.strip()

    # Try MM/DD/YYYY
    if '/' in date_string:
        parts = date_string.split('/')
        if len(parts) == 3:
            try:
                month, day, year = int(parts[0]), int(parts[1]), int(parts[2])
                # Handle 2-digit years
                if year < 100:
                    year += 2000
                return datetime(year, month, day)
            except (ValueError, TypeError):
                pass

    # Try YYYY-MM-DD
    if '-' in date_string:
        parts = date_string.split('-')
        if len(parts) == 3:
            try:
                year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                return datetime(year, month, day)
            except (ValueError, TypeError):
                pass

    # Fallback: try standard parsing
    try:
        return datetime.fromisoformat(date_string)
    except (ValueError, TypeError):
        pass

    return None


def normalize_expiry_date(date_string: str) -> str:
    """Normalize expiry date to YYYY-MM-DD format."""
    parsed = parse_expiry_date(date_string)
    if parsed:
        return parsed.strftime('%Y-%m-%d')
    return date_string


def is_expiry_in_past(date_string: str, today: Optional[datetime] = None) -> bool:
    """Check if expiry date is in the past."""
    parsed = parse_expiry_date(date_string)
    if not parsed:
        return False

    if today is None:
        today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    # Expiry is valid through end of day
    parsed = parsed.replace(hour=23, minute=59, second=59)
    return parsed < today


def calculate_days_until_expiry(date_string: str, today: Optional[datetime] = None) -> int:
    """Calculate days from today until expiry date."""
    parsed = parse_expiry_date(date_string)
    if not parsed:
        return -1

    if today is None:
        today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    delta = parsed - today
    return delta.days


# =============================================================================
# ORDER CYCLE CALCULATION
# =============================================================================

def get_next_order_date(from_date: datetime, order_day: int) -> datetime:
    """Get the next occurrence of a specific weekday.

    Args:
        from_date: Starting date
        order_day: Target day (1=Monday, 7=Sunday - Firebase convention)

    Returns:
        Next occurrence of that weekday (could be today if it matches)
    """
    # Convert Firebase day (1=Mon, 7=Sun) to Python weekday (0=Mon, 6=Sun)
    target_weekday = (order_day - 1) % 7
    current_weekday = from_date.weekday()

    days_until = (target_weekday - current_weekday) % 7
    # Note: No noon cutoff - match client behavior (same-day valid through end of day)

    return from_date + timedelta(days=days_until)


@dataclass
class ViableCycle:
    """A viable order cycle for a given expiry date."""
    order_day: int              # Firebase convention (1=Monday)
    order_date: datetime
    delivery_date: datetime
    is_viable: bool


def calculate_next_viable_cycle(
    expiry_date: str,
    order_cycles: List[Dict],
    max_weeks: int = 12,
    today: Optional[datetime] = None,
) -> Dict:
    """Calculate the next viable order cycle for an expiring item.

    A cycle is viable if:
    1. Order date is today or later
    2. Order date is before expiry date
    3. Delivery date is on or before expiry date

    Returns the LATEST viable cycle (closest to expiry) to maximize sell-through time.

    Args:
        expiry_date: Expiry date string
        order_cycles: List of cycle configs with orderDay, deliveryDay
        max_weeks: How far ahead to check
        today: Current date (in user's timezone). If None, uses server time.

    Returns:
        Dict with 'cycles' list and 'next_viable_cycle' (or None)
    """
    if today is None:
        today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)

    expiry_dt = parse_expiry_date(expiry_date)
    if not expiry_dt:
        return {'cycles': [], 'next_viable_cycle': None}

    # Set expiry to end of day
    expiry_dt = expiry_dt.replace(hour=23, minute=59, second=59)

    if not order_cycles:
        return {'cycles': [], 'next_viable_cycle': None}

    computed_cycles: List[ViableCycle] = []

    for config in order_cycles:
        order_day = config.get('orderDay', 1)
        delivery_day = config.get('deliveryDay', 4)

        # Start from today's order date for this cycle
        order_date = get_next_order_date(today, order_day)

        for week in range(max_weeks):
            current_order_date = order_date + timedelta(weeks=week)

            # Calculate delivery date
            days_until_delivery = delivery_day - order_day
            if days_until_delivery <= 0:
                days_until_delivery += 7

            delivery_date = current_order_date + timedelta(days=days_until_delivery)

            # Normalize times for comparison
            current_order_date = current_order_date.replace(hour=0, minute=0, second=0, microsecond=0)
            delivery_date = delivery_date.replace(hour=0, minute=0, second=0, microsecond=0)

            # Check viability
            is_viable = (
                current_order_date >= today and
                current_order_date < expiry_dt and
                delivery_date <= expiry_dt
            )

            computed_cycles.append(ViableCycle(
                order_day=order_day,
                order_date=current_order_date,
                delivery_date=delivery_date,
                is_viable=is_viable,
            ))

    # Find latest viable cycle (closest to expiry = maximizes sell-through)
    viable_cycles = [c for c in computed_cycles if c.is_viable]
    viable_cycles.sort(key=lambda c: c.order_date, reverse=True)

    next_viable = viable_cycles[0] if viable_cycles else None

    return {
        'cycles': computed_cycles,
        'next_viable_cycle': next_viable,
    }


# =============================================================================
# PCF DATA LOADING
# =============================================================================

def load_pcf_items(db, route_number: str) -> List[PCFItem]:
    """Load all PCF items from Firestore for a route.

    Path: routes/{routeNumber}/pcfs/{deliveryNumber}/containers/{containerCode}

    Only loads containers where allItemsExpired == false.
    """
    items: List[PCFItem] = []

    try:
        # Get all deliveries (pcfs collection)
        pcfs_ref = db.collection('routes').document(route_number).collection('pcfs')
        deliveries = pcfs_ref.stream()

        for delivery_doc in deliveries:
            delivery_data = delivery_doc.to_dict() or {}
            delivery_number = delivery_data.get('deliveryNumber', delivery_doc.id)

            # Get active containers (not all expired)
            containers_ref = delivery_doc.reference.collection('containers')
            # Query for containers that aren't fully expired
            try:
                if FieldFilter:
                    containers_query = containers_ref.where(filter=FieldFilter('allItemsExpired', '==', False))
                else:
                    containers_query = containers_ref.where('allItemsExpired', '==', False)
                containers = containers_query.stream()
            except Exception:
                # If field doesn't exist, get all containers
                containers = containers_ref.stream()

            for container_doc in containers:
                container_data = container_doc.to_dict() or {}
                container_code = container_doc.id

                # Extract items from pages
                pages = container_data.get('pages', [])
                for page in pages:
                    page_items = page.get('items', [])
                    for item in page_items:
                        sap = item.get('product', '')
                        if not sap:
                            continue

                        best_before = item.get('bestBefore', '')
                        if not best_before:
                            continue

                        items.append(PCFItem(
                            sap=sap,
                            description=item.get('description', ''),
                            best_before=best_before,
                            visible_on_list=item.get('visibleOnList', True),
                            delivery_number=delivery_number,
                            container_code=container_code,
                        ))

        print(f"[low_qty] Loaded {len(items)} PCF items for route {route_number}")

    except Exception as e:
        print(f"[low_qty] Error loading PCF items: {e}")

    return items


# =============================================================================
# LOW QUANTITY DETECTION
# =============================================================================

def get_low_quantity_items(
    pcf_items: List[PCFItem],
    order_cycles: List[Dict],
    today: Optional[datetime] = None,
) -> List[LowQuantityItem]:
    """Identify low quantity items from PCF data.

    Low quantity = product has only one viable (non-expired) expiry date.

    This mirrors the logic in lowQuantityItems.ts

    Args:
        pcf_items: List of PCF items to analyze
        order_cycles: User's order cycle configuration
        today: Current date in user's timezone. If None, uses server time.
    """
    # Track expiry dates per SAP
    sap_expiry_stats: Dict[str, Dict[str, Set[str]]] = {}  # sap -> {all_dates, viable_dates}
    sap_visibility: Dict[str, bool] = {}

    # Track best item data per sap-date combination
    items_by_key: Dict[str, PCFItem] = {}

    for item in pcf_items:
        sap = item.sap
        normalized_date = normalize_expiry_date(item.best_before)

        # Initialize tracking
        if sap not in sap_expiry_stats:
            sap_expiry_stats[sap] = {
                'all_dates': set(),
                'viable_dates': set(),
            }

        # Track dates
        sap_expiry_stats[sap]['all_dates'].add(normalized_date)
        if not is_expiry_in_past(normalized_date, today=today):
            sap_expiry_stats[sap]['viable_dates'].add(normalized_date)

        # Track visibility (all instances must be visible)
        if sap not in sap_visibility:
            sap_visibility[sap] = item.visible_on_list
        else:
            sap_visibility[sap] = sap_visibility[sap] and item.visible_on_list

        # Store item reference
        key = f"{sap}-{normalized_date}"
        items_by_key[key] = item

    # Find low quantity items: products with only one viable expiry date
    low_qty_items: List[LowQuantityItem] = []

    for sap, stats in sap_expiry_stats.items():
        viable_count = len(stats['viable_dates'])
        all_count = len(stats['all_dates'])

        # Low quantity: single viable date, or single date that's already expired
        is_low_qty = (viable_count == 1) or (viable_count == 0 and all_count == 1)

        # Respect visibility
        if sap_visibility.get(sap) is False:
            continue

        if not is_low_qty:
            continue

        # Get the relevant date
        if viable_count > 0:
            expiry_date = list(stats['viable_dates'])[0]
        else:
            expiry_date = list(stats['all_dates'])[0]

        # Get source item
        key = f"{sap}-{expiry_date}"
        source_item = items_by_key.get(key)
        if not source_item:
            continue

        # Calculate viable order cycle
        cycle_result = calculate_next_viable_cycle(expiry_date, order_cycles, today=today)
        next_cycle = cycle_result.get('next_viable_cycle')

        days_left = calculate_days_until_expiry(expiry_date, today=today)

        if next_cycle:
            low_qty_items.append(LowQuantityItem(
                sap=sap,
                expiry_date=expiry_date,
                order_by_date=next_cycle.order_date.strftime('%Y-%m-%d'),
                delivery_date=next_cycle.delivery_date.strftime('%Y-%m-%d'),
                days_left=days_left,
                will_have_gap=False,
                container_code=source_item.container_code,
                delivery_number=source_item.delivery_number,
            ))
        else:
            # No viable cycle - will have a gap
            low_qty_items.append(LowQuantityItem(
                sap=sap,
                expiry_date=expiry_date,
                order_by_date='',  # No viable order date
                delivery_date='',
                days_left=days_left,
                will_have_gap=True,
                container_code=source_item.container_code,
                delivery_number=source_item.delivery_number,
            ))

    print(f"[low_qty] Found {len(low_qty_items)} low quantity items "
          f"({sum(1 for i in low_qty_items if i.will_have_gap)} with gaps)")

    return low_qty_items


# =============================================================================
# MAIN ENTRY POINTS
# =============================================================================

def get_items_for_order_date(
    db,
    route_number: str,
    order_date: str,
) -> List[LowQuantityItem]:
    """Get low quantity items that should be ordered on a specific date.

    This is the main entry point for forecast integration.

    Args:
        db: Firestore client
        route_number: Route to check
        order_date: Order date to match (ISO format YYYY-MM-DD)
    Returns:
        List of low quantity items where order_by_date matches
    """
    # Normalize order date
    order_date_normalized = normalize_delivery_date(order_date)

    # Get user's timezone for accurate date calculations
    user_timezone = get_user_timezone(db, route_number)
    today = get_current_datetime(user_timezone)

    # Load PCF items
    pcf_items = load_pcf_items(db, route_number)
    if not pcf_items:
        print(f"[low_qty] No PCF items found for route {route_number}")
        return []

    # Get order cycles
    order_cycles = get_order_cycles(db, route_number)
    if not order_cycles:
        print(f"[low_qty] No order cycles found for route {route_number}")
        return []

    # Get all low quantity items (using user's timezone for "today")
    low_qty_items = get_low_quantity_items(pcf_items, order_cycles, today=today)

    # Filter to items that should be ordered on the given date
    matching_items = [
        item for item in low_qty_items
        if item.order_by_date == order_date_normalized and not item.will_have_gap
    ]

    print(f"[low_qty] {len(matching_items)} items match order date {order_date_normalized}")

    return matching_items


def get_saps_to_suppress(
    db,
    route_number: str,
    order_date: str,
) -> set:
    """Get SAPs that should be suppressed (zeroed) for a given order date.
    
    These are low-qty items where:
    - They have a computed order_by_date (are flagged as low-qty)
    - Their order_by_date is NOT the given order_date
    - They are NOT high-velocity items (ordered > 50% of the time)
    
    This prevents ML from ordering low-qty items on the wrong day,
    while allowing high-velocity items to pass through even if PCF data is stale.
    
    Args:
        db: Firestore client
        route_number: Route to check
        order_date: The order date being forecast (ISO format YYYY-MM-DD)
    Returns:
        Set of SAP codes to suppress (zero out predictions)
    """
    # Get all low-qty items for this route
    user_timezone = get_user_timezone(db, route_number)
    today = get_current_datetime(user_timezone)
    
    pcf_items = load_pcf_items(db, route_number)
    if not pcf_items:
        return set()
    
    # Get order cycles from PostgreSQL or Firebase
    try:
        from schedule_utils import get_order_cycles
        order_cycles = get_order_cycles(db, route_number)
    except ImportError:
        order_cycles = []
    
    if not order_cycles:
        return set()
    
    # Get all low-qty items (not filtered by order date)
    all_low_qty = get_low_quantity_items(pcf_items, order_cycles, today=today)
    
    # Find SAPs where order_by_date != the given order_date
    candidate_saps = set()
    for item in all_low_qty:
        if item.order_by_date and item.order_by_date != order_date:
            candidate_saps.add(item.sap)
    
    if not candidate_saps:
        return set()
    
    # Exclude high-velocity items (ordered > 50% of the time)
    # These shouldn't be suppressed even if PCF data is stale
    high_velocity_saps = set()
    if candidate_saps:
        try:
            sap_list = list(candidate_saps)
            placeholders = ', '.join(['%s' for _ in sap_list])
            
            rows = fetch_all(f"""
                WITH total_orders AS (
                    SELECT COUNT(DISTINCT order_id) as total
                    FROM orders_historical
                    WHERE route_number = %s
                ),
                sap_orders AS (
                    SELECT 
                        oli.sap,
                        COUNT(DISTINCT o.order_id) as times_ordered
                    FROM order_line_items oli
                    JOIN orders_historical o ON oli.order_id = o.order_id
                    WHERE o.route_number = %s
                      AND oli.sap IN ({placeholders})
                    GROUP BY oli.sap
                )
                SELECT 
                    s.sap,
                    s.times_ordered::float / NULLIF(t.total, 0) as order_freq
                FROM sap_orders s, total_orders t
                WHERE s.times_ordered::float / NULLIF(t.total, 0) > 0.5
            """, [route_number, route_number] + sap_list)
            
            for row in rows:
                high_velocity_saps.add(str(row['sap']))
                    
            if high_velocity_saps:
                print(f"[low_qty] Excluding {len(high_velocity_saps)} high-velocity SAPs from suppression (order_freq > 50%)")
        except Exception as e:
            print(f"[low_qty] Warning: Could not check order frequency: {e}")
    
    # Final suppress list excludes high-velocity items
    suppress_saps = candidate_saps - high_velocity_saps
    
    if suppress_saps:
        print(f"[low_qty] {len(suppress_saps)} SAPs will be suppressed (order_by_date != {order_date})")
    
    return suppress_saps


def get_stores_for_sap(
    route_number: str,
    sap: str,
) -> List[str]:
    """Get list of store IDs that stock a specific SAP.

    Uses the store_items table in PostgreSQL (synced from Manage Items).

    Args:
        route_number: Route to check
        sap: SAP code to look up

    Returns:
        List of store_id strings, or empty list if none found
    """
    try:
        rows = fetch_all("""
            SELECT store_id
            FROM store_items
            WHERE route_number = %s
              AND sap = %s
              AND is_active = TRUE
        """, [route_number, sap])

        store_ids = [row['store_id'] for row in rows]

        if not store_ids:
            print(f"[low_qty] Warning: No store mapping for SAP {sap}, skipping")

        return store_ids

    except Exception as e:
        print(f"[low_qty] Error looking up stores for SAP {sap}: {e}")
        return []


def get_gap_items(
    db,
    route_number: str,
) -> List[LowQuantityItem]:
    """Get low quantity items that have no viable order cycle (will have gap).

    These items should be surfaced for manual review.

    Args:
        db: Firestore client
        route_number: Route to check
    Returns:
        List of low quantity items where will_have_gap is True
    """
    # Load PCF items
    pcf_items = load_pcf_items(db, route_number)
    if not pcf_items:
        return []

    # Get user's timezone for accurate date calculations
    user_timezone = get_user_timezone(db, route_number)
    today = get_current_datetime(user_timezone)

    # Get order cycles
    order_cycles = get_order_cycles(db, route_number)

    # Get all low quantity items
    low_qty_items = get_low_quantity_items(pcf_items, order_cycles, today=today)

    # Filter to items with gaps
    gap_items = [item for item in low_qty_items if item.will_have_gap]

    return gap_items


# =============================================================================
# CLI FOR TESTING
# =============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Low quantity items loader')
    parser.add_argument('--route', required=True, help='Route number')
    parser.add_argument('--order-date', help='Order date (YYYY-MM-DD) to filter')
    parser.add_argument('--show-gaps', action='store_true', help='Show items with no viable cycle')
    parser.add_argument('--serviceAccount', required=True, help='Path to Firebase SA JSON')

    args = parser.parse_args()

    # Initialize Firestore
    from google.cloud import firestore
    db = firestore.Client.from_service_account_json(args.serviceAccount)

    if args.show_gaps:
        items = get_gap_items(db, args.route)
        print(f"\n=== Gap Items (no viable order cycle) ===")
        for item in items:
            print(f"  {item.sap}: expires {item.expiry_date} ({item.days_left} days)")
    elif args.order_date:
        items = get_items_for_order_date(db, args.route, args.order_date)
        print(f"\n=== Items to order on {args.order_date} ===")
        for item in items:
            print(f"  {item.sap}: expires {item.expiry_date}, delivers {item.delivery_date}")
    else:
        # Show all low-qty items
        pcf_items = load_pcf_items(db, args.route)
        order_cycles = get_order_cycles(db, args.route)
        items = get_low_quantity_items(pcf_items, order_cycles)

        print(f"\n=== All Low Quantity Items ===")
        for item in items:
            status = "GAP" if item.will_have_gap else f"order {item.order_by_date}"
            print(f"  {item.sap}: expires {item.expiry_date} ({item.days_left}d) - {status}")
