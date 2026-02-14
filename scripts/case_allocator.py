"""Case allocation utilities.

Allocates full cases across stores based on predicted demand and historical share.

KEY INSIGHT: Some items are ALWAYS ordered in full cases per store (never split),
while others are routinely split across stores. The algorithm detects this pattern
and adjusts allocation accordingly.

Uses direct PostgreSQL access via pg_utils (no message bus).
"""

from __future__ import annotations

import math
from typing import Dict, Optional, Tuple, TYPE_CHECKING

# Import pg_utils for direct PostgreSQL access
try:
    from .pg_utils import fetch_all, fetch_one
except ImportError:
    from pg_utils import fetch_all, fetch_one


# =============================================================================
# Pattern Detection: Full Case vs Splittable Items
# =============================================================================

def get_item_case_pattern(
    route_number: str,
    sap: str,
    schedule_key: str,
) -> Tuple[str, float, int]:
    """
    Detect how an item is typically ordered - full cases, even splits, or variable.
    
    Returns:
        (pattern_type, confidence, split_divisor)
        
        pattern_type: 'full_case', 'half_case', 'quarter_case', or 'variable'
        confidence: 0.0-1.0, how confident we are in this pattern
        split_divisor: The divisor for rounding (1=full, 2=half, 4=quarter)
    
    Logic:
        - Look at last 50 store-level orders for this item
        - Check what fraction of case_pack the quantities align to
        - If 80%+ align to a consistent split, use that pattern
    
    Examples:
        - Item 28934 (case=15): Always 15, 30, 45 → 'full_case', divisor=1
        - Item 28938 (case=8): Always 4, 8, 12, 16 → 'half_case', divisor=2
        - Item 34117 (case=24): Always 6, 12, 18, 24 → 'quarter_case', divisor=4
        - Random quantities → 'variable', divisor=1
    """
    try:
        # Get case_pack for this item
        row = fetch_one(
            "SELECT case_pack FROM product_catalog WHERE sap = %s AND route_number = %s LIMIT 1",
            [sap, route_number]
        )
        case_pack = row.get('case_pack', 1) if row else 1

        if case_pack <= 1:
            return ('variable', 0.0, 1)  # Single unit items can't have this pattern

        # SCHEDULE KEY MIGRATION: Old orders used delivery-day as schedule_key
        # New orders use order-day. Map both to handle mixed data:
        legacy_schedule_map = {
            'tuesday': ['tuesday', 'monday'],    # Tuesday orders had old key "monday" (delivery day)
            'monday': ['monday', 'thursday'],    # Monday orders had old key "thursday" (delivery day)
        }
        schedule_keys = legacy_schedule_map.get(schedule_key.lower(), [schedule_key])

        # Build placeholders for IN clause (PostgreSQL uses %s)
        placeholders = ', '.join(['%s' for _ in schedule_keys])

        # Get recent store-level order quantities for this item
        qty_rows = fetch_all(
            f"""
            SELECT oli.store_id, oli.quantity
            FROM order_line_items oli
            JOIN orders_historical oh ON oli.order_id = oh.order_id
            WHERE oli.route_number = %s
              AND oli.sap = %s
              AND oh.schedule_key IN ({placeholders})
            ORDER BY oh.delivery_date DESC
            LIMIT 50
            """,
            [route_number, sap] + schedule_keys
        )
        
        if len(qty_rows) < 3:
            return ('variable', 0.0, 1)  # Not enough data
        
        quantities = [row.get('quantity', 0) for row in qty_rows if row.get('quantity', 0) > 0]
        if not quantities:
            return ('variable', 0.0, 1)
        
        # Check alignment to different split levels
        # Try: full case, half case, quarter case, eighth case
        split_checks = [
            ('full_case', 1),
            ('half_case', 2),
            ('quarter_case', 4),
            ('eighth_case', 8),
        ]
        
        best_pattern = 'variable'
        best_confidence = 0.0
        best_divisor = 1
        
        for pattern_name, divisor in split_checks:
            if case_pack % divisor != 0:
                continue  # Skip if case_pack isn't evenly divisible
            
            split_unit = case_pack // divisor
            if split_unit < 1:
                continue
            
            # Count how many quantities align to this split
            aligned_count = sum(1 for qty in quantities if qty % split_unit == 0)
            alignment_ratio = aligned_count / len(quantities)
            
            # We want the SMALLEST split that has high alignment
            # (full_case is better than half_case if both have 100%)
            if alignment_ratio >= 0.80 and alignment_ratio > best_confidence:
                best_pattern = pattern_name
                best_confidence = alignment_ratio
                best_divisor = divisor
        
        return (best_pattern, best_confidence, best_divisor)
    
    except Exception as e:
        print(f"[case_allocator] Warning: Could not detect case pattern for {sap}: {e}")
        return ('variable', 0.0, 1)


# =============================================================================
# Historical Share Fetcher (via direct PostgreSQL)
# =============================================================================

def get_historical_shares(
    route_number: str,
    sap: str,
    schedule_key: str,
) -> Dict[str, float]:
    """
    Fetch historical store shares for an item from PostgreSQL.

    Args:
        route_number: Route to fetch shares for
        sap: Item SAP code
        schedule_key: Schedule key (monday, tuesday, etc.)

    Returns:
        Dict mapping store_id -> share (0.0 to 1.0, sums to ~1.0)
        Empty dict if no history exists.
    """
    try:
        rows = fetch_all(
            """
            SELECT store_id, share
            FROM store_item_shares
            WHERE route_number = %s
              AND sap = %s
              AND schedule_key = %s
            ORDER BY share DESC
            """,
            [route_number, sap, schedule_key]
        )

        if not rows:
            return {}

        return {row['store_id']: row['share'] for row in rows}

    except Exception as e:
        print(f"[case_allocator] Warning: Could not fetch historical shares: {e}")
        return {}


def get_item_allocation_info(
    route_number: str,
    sap: str,
    schedule_key: str,
) -> Optional[Dict]:
    """
    Fetch cached allocation info for an item from PostgreSQL.

    Returns dict with:
        - total_avg_quantity: Average total demand per order
        - split_pattern: 'single_store', 'skewed', 'even_split', 'varies'
        - store_shares: JSON dict of store_id -> share
        - dominant_store_id: Store with highest share (if skewed)
        - typical_case_count: Average cases ordered

    Returns None if no cached info exists.
    """
    try:
        row = fetch_one(
            """
            SELECT
                total_avg_quantity,
                split_pattern,
                store_shares,
                dominant_store_id,
                dominant_store_share,
                typical_case_count,
                avg_stores_per_order
            FROM item_allocation_cache
            WHERE route_number = %s
              AND sap = %s
              AND schedule_key = %s
            LIMIT 1
            """,
            [route_number, sap, schedule_key]
        )

        if not row:
            return None

        import json
        return {
            'total_avg_quantity': row.get('total_avg_quantity'),
            'split_pattern': row.get('split_pattern'),
            'store_shares': json.loads(row.get('store_shares', '{}')) if row.get('store_shares') else {},
            'dominant_store_id': row.get('dominant_store_id'),
            'dominant_store_share': row.get('dominant_store_share'),
            'typical_case_count': row.get('typical_case_count'),
            'avg_stores_per_order': row.get('avg_stores_per_order'),
        }

    except Exception as e:
        print(f"[case_allocator] Warning: Could not fetch allocation info: {e}")
        return None


# =============================================================================
# Case Allocation Algorithm
# =============================================================================

def allocate_cases_across_stores(
    sap: str,
    store_predictions: Dict[str, float],  # store_id -> predicted units
    case_pack: int,
    historical_shares: Dict[str, float],  # store_id -> share (sums to 1.0 ideally)
    route_number: Optional[str] = None,
    schedule_key: Optional[str] = None,
) -> Dict[str, int]:
    """
    Allocate full cases across stores based on historical share.
    
    SMART BEHAVIOR:
    - If item is "full case only" (user always orders full cases per store),
      round each store's allocation to full cases individually.
    - If item is "splittable" (user sometimes splits cases across stores),
      round the TOTAL to full cases, then distribute units by share.

    Steps for splittable items:
      1. Sum total predicted units across all stores.
      2. Round TOTAL to full cases (ceil(total / case_pack) * case_pack).
      3. Allocate proportionally by share; floor allocations first.
      4. Distribute remaining units to stores with highest fractional parts.
      5. Return per-store unit allocations.
      
    Steps for full-case-only items:
      1. For each store, round predicted units to nearest case.
      2. Return per-store allocations (all multiples of case_pack).
    """
    if case_pack <= 0:
        # fallback: no case info, return rounded predictions
        return {s: int(round(u)) for s, u in store_predictions.items()}

    total_predicted = sum(store_predictions.values())
    if total_predicted <= 0:
        return {s: 0 for s in store_predictions}

    # Check ordering pattern for this item (full case, half case, quarter case, etc.)
    pattern_type = 'variable'
    split_divisor = 1
    if route_number and schedule_key:
        pattern_type, confidence, split_divisor = get_item_case_pattern(
            route_number, sap, schedule_key
        )
        if pattern_type != 'variable':
            split_unit = case_pack // split_divisor
            print(f"[ALLOCATOR] SAP {sap}: {pattern_type} detected ({confidence:.0%}) - rounding to {split_unit} units")
    
    # PATTERN DETECTED: Round TOTAL to cases first, then distribute by split_unit
    if pattern_type != 'variable' and split_divisor >= 1:
        split_unit = case_pack // split_divisor
        
        # Step 1: Round TOTAL to full cases
        total_cases = math.ceil(total_predicted / case_pack)
        total_units = total_cases * case_pack
        
        # Step 2: Distribute by share, rounding to split_unit
        allocations = {}
        allocated = 0
        remainders = {}
        
        # Sort stores by prediction (highest first)
        sorted_stores = sorted(store_predictions.keys(), 
                               key=lambda s: store_predictions.get(s, 0), reverse=True)
        
        for store_id in sorted_stores:
            pred_units = store_predictions.get(store_id, 0)
            share = pred_units / total_predicted if total_predicted > 0 else 0
            target = share * total_units
            # Round DOWN to split_unit
            floor_splits = int(target // split_unit)
            floor_alloc = floor_splits * split_unit
            allocations[store_id] = floor_alloc
            allocated += floor_alloc
            remainders[store_id] = target - floor_alloc
        
        # Step 3: Distribute remaining units by split_unit to stores with highest remainders
        remaining = total_units - allocated
        stores_by_remainder = sorted(remainders.keys(), key=lambda s: remainders[s], reverse=True)
        
        while remaining >= split_unit:
            for store_id in stores_by_remainder:
                if remaining < split_unit:
                    break
                allocations[store_id] += split_unit
                remaining -= split_unit
        
        return allocations

    # NO PATTERN (variable): Use historical weights to distribute
    total_cases = math.ceil(total_predicted / case_pack)
    total_units = total_cases * case_pack

    # Build effective shares for each store (direct lookup)
    effective_shares: Dict[str, float] = {}
    for store in store_predictions.keys():
        effective_shares[store] = historical_shares.get(store, 0.0)
    
    # If no shares found for any store, use equal shares
    total_effective = sum(effective_shares.values())
    if total_effective == 0:
        equal_share = 1.0 / len(store_predictions)
        effective_shares = {s: equal_share for s in store_predictions}
        total_effective = 1.0
    
    # Normalize so shares sum to 1.0
    for store in effective_shares:
        effective_shares[store] /= total_effective
    
    # Debug output for specific items
    if sap in ["31032"]:
        print(f"[ALLOCATOR DEBUG] SAP {sap}: total_predicted={total_predicted:.1f}, case_pack={case_pack}, total_cases={total_cases}, total_units={total_units}")
        print(f"[ALLOCATOR DEBUG] SAP {sap}: effective_shares={{{', '.join(f'{k}: {v:.1%}' for k, v in effective_shares.items())}}}")
    
    # Sort stores by share desc
    sorted_stores = sorted(
        store_predictions.keys(),
        key=lambda s: effective_shares.get(s, 0),
        reverse=True,
    )

    allocations = {}
    allocated = 0
    remainders = {}

    for store in sorted_stores:
        share = effective_shares.get(store, 1.0 / len(store_predictions))
        raw_alloc = share * total_units
        floor_alloc = int(math.floor(raw_alloc))
        allocations[store] = floor_alloc
        allocated += floor_alloc
        remainders[store] = raw_alloc - floor_alloc

    remainder = total_units - allocated
    
    # Debug output for specific items
    if sap in ["31032"]:
        print(f"[ALLOCATOR DEBUG] SAP {sap}: after floor allocation: allocated={allocated}, remainder={remainder}")
        print(f"[ALLOCATOR DEBUG] SAP {sap}: floor allocations={allocations}")
    
    # Sort stores by fractional remainder (highest first), with share as tiebreaker
    stores_by_remainder = sorted(
        remainders.keys(),
        key=lambda s: (remainders[s], effective_shares.get(s, 0)),
        reverse=True,
    )
    
    # Distribute remainder - may need multiple passes if remainder > num_stores
    while remainder > 0:
        for store in stores_by_remainder:
            if remainder <= 0:
                break
            allocations[store] += 1
            remainder -= 1

    # Debug output for specific items
    if sap in ["31032"]:
        print(f"[ALLOCATOR DEBUG] SAP {sap}: final allocations={allocations}, sum={sum(allocations.values())}")

    return allocations
