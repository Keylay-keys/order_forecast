"""Firebase → DuckDB sync utilities.

This module syncs data from Firebase/Firestore into the local DuckDB analytical
database. It uses the firebase_loader module for reading and handles the 
transformation to DuckDB schema.

Supports:
- Full sync (initial population)
- Incremental sync (new orders only)
- Individual entity sync (stores, products, promos)

Usage:
    # Full sync from Firebase
    python scripts/db_sync.py --route 989262 --service-account /path/to/sa.json
    
    # Incremental sync (last 30 days)
    python scripts/db_sync.py --route 989262 --incremental --days 30
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional
import json

# Handle imports for both direct execution and module import
try:
    from google.cloud.firestore_v1.base_query import FieldFilter
except ImportError:
    FieldFilter = None

try:
    from .db_schema import get_connection, create_schema, print_schema_summary
    from .firebase_loader import (
        get_firestore_client,
        load_master_catalog,
        load_store_configs,
        load_orders,
        load_promotions,
    )
    from .models import Product, StoreConfig, Order, StoreOrder, OrderItem
    FIREBASE_AVAILABLE = True
except ImportError:
    try:
        from db_schema import get_connection, create_schema, print_schema_summary
        from firebase_loader import (
            get_firestore_client,
            load_master_catalog,
            load_store_configs,
            load_orders,
            load_promotions,
        )
        from models import Product, StoreConfig, Order, StoreOrder, OrderItem
        FIREBASE_AVAILABLE = True
    except ImportError as e:
        FIREBASE_AVAILABLE = False
        print(f"⚠️  Import error: {e}")


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string in various formats."""
    if not date_str:
        return None
    
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%Y/%m/%d']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def get_calendar_features(date: datetime) -> dict:
    """Compute calendar features for a given date."""
    dow = date.weekday()
    week = date.isocalendar()[1]
    month = date.month
    is_first_weekend = (dow >= 5) and (date.day <= 7)
    is_month_end = date.day >= 28
    
    return {
        'day_of_week': dow,
        'week_of_year': week,
        'month': month,
        'is_first_weekend_of_month': is_first_weekend,
        'is_month_end': is_month_end,
        'is_holiday_week': False,
    }


class DuckDBSync:
    """Sync Firebase data to DuckDB."""
    
    def __init__(self, db_path: str, service_account_path: Optional[str] = None):
        """Initialize sync with database and optional Firebase credentials.
        
        Args:
            db_path: Path to DuckDB database file.
            service_account_path: Optional path to Firebase service account JSON.
        """
        self.db_path = db_path
        self.conn = get_connection(db_path)
        self.fs_client = None
        self.service_account_path = service_account_path
        
    def _get_firestore(self):
        """Lazy-load Firestore client."""
        if self.fs_client is None and FIREBASE_AVAILABLE:
            self.fs_client = get_firestore_client(self.service_account_path)
        return self.fs_client
    
    # =========================================================================
    # PRODUCT CATALOG SYNC
    # =========================================================================
    
    def sync_products(self, route_number: str) -> int:
        """Sync product catalog from Firebase.
        
        Returns:
            Number of products synced.
        """
        db = self._get_firestore()
        if not db:
            print("   ⚠️  Firebase not available")
            return 0
        
        products = load_master_catalog(db, route_number)
        count = 0
        
        for p in products:
            self.conn.execute("""
                INSERT INTO product_catalog (
                    sap, route_number, full_name, short_name, brand, 
                    category, case_pack, tray, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                ON CONFLICT (sap, route_number) DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    brand = EXCLUDED.brand,
                    category = EXCLUDED.category,
                    case_pack = EXCLUDED.case_pack,
                    tray = EXCLUDED.tray
            """, [
                p.sap,
                route_number,
                p.name,
                p.name[:50] if p.name else None,  # short_name
                p.brand,
                p.category,
                p.case_pack,
                p.tray,
            ])
            count += 1
        
        return count
    
    # =========================================================================
    # STORE CONFIG SYNC
    # =========================================================================
    
    def sync_stores(self, route_number: str) -> tuple:
        """Sync store configurations from Firebase.
        
        Returns:
            Tuple of (stores_synced, items_synced).
        """
        db = self._get_firestore()
        if not db:
            return (0, 0)
        
        stores = load_store_configs(db, route_number)
        store_count = 0
        item_count = 0
        
        for store in stores:
            # Sync store
            self.conn.execute("""
                INSERT INTO stores (
                    store_id, route_number, store_name, delivery_days, is_active
                ) VALUES (?, ?, ?, ?, TRUE)
                ON CONFLICT (store_id) DO UPDATE SET
                    store_name = EXCLUDED.store_name,
                    delivery_days = EXCLUDED.delivery_days
            """, [
                store.store_id,
                route_number,
                store.store_name,
                json.dumps(store.delivery_days),
            ])
            store_count += 1
            
            # Sync store items
            for sap in store.active_saps:
                item_id = f"{store.store_id}-{sap}"
                self.conn.execute("""
                    INSERT INTO store_items (
                        id, store_id, route_number, sap, is_active
                    ) VALUES (?, ?, ?, ?, TRUE)
                    ON CONFLICT (id) DO UPDATE SET
                        is_active = TRUE
                """, [item_id, store.store_id, route_number, sap])
                item_count += 1
        
        return (store_count, item_count)
    
    # =========================================================================
    # ORDER SYNC
    # =========================================================================
    
    def sync_order(self, order: Order) -> dict:
        """Sync a single order to DuckDB.
        
        Args:
            order: Order dataclass from firebase_loader.
            
        Returns:
            Dict with sync stats.
        """
        delivery_date = parse_date(order.expected_delivery_date)
        order_date = parse_date(order.order_date) if order.order_date else None
        
        if not delivery_date:
            return {'order_id': order.id, 'skipped': True, 'reason': 'no delivery date'}
        
        calendar = get_calendar_features(delivery_date)
        total_units = 0
        item_count = 0
        corrections_count = 0
        
        # Get forecast ID from order meta (for correction tracking)
        forecast_id = order.meta.get('forecastId', f'order-derived-{order.id}')
        now = datetime.utcnow()
        
        # Insert line items AND extract corrections
        for store in order.stores:
            for item in store.items:
                if item.quantity == 0:
                    continue
                
                total_units += int(item.quantity)
                item_count += 1
                
                line_item_id = f"{order.id}-{store.store_id}-{item.sap}"
                
                self.conn.execute("""
                    INSERT INTO order_line_items (
                        line_item_id, order_id, route_number, schedule_key,
                        delivery_date, store_id, store_name, sap, quantity, cases,
                        promo_active, promo_id,
                        is_first_weekend_of_month, is_holiday_week, is_month_end,
                        day_of_week, week_of_year, month
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (line_item_id) DO UPDATE SET
                        quantity = EXCLUDED.quantity,
                        cases = EXCLUDED.cases,
                        promo_active = EXCLUDED.promo_active
                """, [
                    line_item_id,
                    order.id,
                    order.route_number,
                    order.schedule_key,
                    delivery_date.strftime('%Y-%m-%d'),
                    store.store_id,
                    store.store_name,
                    item.sap,
                    int(item.quantity),
                    int(item.cases) if item.cases else 0,
                    item.promo_active,
                    item.promo_id,
                    calendar['is_first_weekend_of_month'],
                    calendar['is_holiday_week'],
                    calendar['is_month_end'],
                    calendar['day_of_week'],
                    calendar['week_of_year'],
                    calendar['month'],
                ])
                
                # Extract correction if forecast data exists
                # Create correction if:
                # 1. User explicitly adjusted the quantity, OR
                # 2. There was a forecast and the final quantity differs
                if item.user_adjusted or (item.forecasted_quantity is not None and item.forecasted_quantity != item.quantity):
                    predicted = float(item.forecasted_quantity) if item.forecasted_quantity is not None else 0.0
                    final = float(item.quantity)
                    delta = final - predicted
                    ratio = (final / predicted) if predicted > 0 else (1.0 if final > 0 else 0.0)
                    was_removed = (final == 0 and predicted > 0)
                    
                    correction_id = f"corr-{order.id}-{store.store_id}-{item.sap}"
                    
                    self.conn.execute("""
                        INSERT INTO forecast_corrections (
                            correction_id, forecast_id, order_id, route_number,
                            schedule_key, delivery_date, store_id, store_name, sap,
                            predicted_units, predicted_cases, final_units, final_cases,
                            correction_delta, correction_ratio, was_removed,
                            promo_id, promo_active, is_first_weekend_of_month, is_holiday_week,
                            submitted_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (correction_id) DO UPDATE SET
                            predicted_units = EXCLUDED.predicted_units,
                            final_units = EXCLUDED.final_units,
                            correction_delta = EXCLUDED.correction_delta,
                            correction_ratio = EXCLUDED.correction_ratio,
                            was_removed = EXCLUDED.was_removed,
                            submitted_at = EXCLUDED.submitted_at
                    """, [
                        correction_id,
                        forecast_id,
                        order.id,
                        order.route_number,
                        order.schedule_key,
                        delivery_date.strftime('%Y-%m-%d'),
                        store.store_id,
                        store.store_name,
                        item.sap,
                        predicted,
                        float(item.forecasted_cases) if item.forecasted_cases else 0,
                        final,
                        float(item.cases) if item.cases else 0,
                        delta,
                        ratio,
                        was_removed,
                        item.promo_id,
                        item.promo_active,
                        calendar['is_first_weekend_of_month'],
                        calendar['is_holiday_week'],
                        now
                    ])
                    corrections_count += 1
        
        # Insert order header
        self.conn.execute("""
            INSERT INTO orders_historical (
                order_id, route_number, user_id, schedule_key,
                delivery_date, order_date, status,
                total_units, store_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id) DO UPDATE SET
                total_units = EXCLUDED.total_units,
                store_count = EXCLUDED.store_count
        """, [
            order.id,
            order.route_number,
            order.user_id,
            order.schedule_key,
            delivery_date.strftime('%Y-%m-%d'),
            order_date.strftime('%Y-%m-%d') if order_date else None,
            order.status,
            total_units,
            len(order.stores),
        ])
        
        return {
            'order_id': order.id,
            'schedule_key': order.schedule_key,
            'delivery_date': delivery_date.strftime('%Y-%m-%d'),
            'stores': len(order.stores),
            'items': item_count,
            'corrections': corrections_count,
        }
    
    def sync_orders(self, route_number: str, since_days: int = 365, 
                    schedule_keys: Optional[List[str]] = None) -> list:
        """Sync orders from Firebase.
        
        Args:
            route_number: Route to sync.
            since_days: How many days of history to load.
            schedule_keys: Optional filter for specific schedules.
            
        Returns:
            List of sync result dicts.
        """
        db = self._get_firestore()
        if not db:
            return []
        
        orders = load_orders(
            db, 
            route_number, 
            since_days=since_days,
            schedule_keys=schedule_keys,
        )
        
        results = []
        for order in orders:
            result = self.sync_order(order)
            results.append(result)
        
        return results
    
    # =========================================================================
    # PROMO SYNC
    # =========================================================================
    
    def sync_promos(self, route_number: str) -> int:
        """Sync promotions from Firebase.
        
        Returns:
            Number of promos synced.
        """
        db = self._get_firestore()
        if not db:
            return 0
        
        promos = load_promotions(db, route_number)
        count = 0
        
        for promo in promos:
            promo_id = promo.get('id') or promo.get('promoId')
            if not promo_id:
                continue
            
            # Get items array - new format has detailed item objects
            items = promo.get('items') or []
            affected_saps = promo.get('affectedSaps') or []
            
            # Derive overall promo dates from items if not set at promo level
            promo_start = promo.get('startDate')
            promo_end = promo.get('endDate')
            
            if not promo_start and items:
                # Use earliest start_date from items
                item_starts = [i.get('start_date') for i in items if i.get('start_date')]
                if item_starts:
                    promo_start = min(item_starts)
            
            if not promo_end and items:
                # Use latest end_date from items
                item_ends = [i.get('end_date') for i in items if i.get('end_date')]
                if item_ends:
                    promo_end = max(item_ends)
            
            self.conn.execute("""
                INSERT INTO promo_history (
                    promo_id, route_number, promo_name, promo_type,
                    start_date, end_date, discount_percent, uploaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (promo_id) DO UPDATE SET
                    promo_name = EXCLUDED.promo_name,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date
            """, [
                promo_id,
                route_number,
                promo.get('promoName') or promo.get('name'),
                promo.get('promoType') or promo.get('type'),
                promo_start,
                promo_end,
                promo.get('discountPercent'),
                promo.get('uploadedAt'),
            ])
            count += 1
            
            # Sync items - handle both new format (detailed objects) and legacy (SAP list)
            if items:
                # New format: items have sap_code, account, start_date, end_date
                for item in items:
                    sap_str = str(item.get('sap_code') or item.get('sap') or '')
                    if not sap_str:
                        continue
                    
                    account = item.get('account', '')
                    item_start = item.get('start_date', '')
                    item_end = item.get('end_date', '')
                    price = item.get('price', '')
                    
                    # Create unique ID including account for per-store promos
                    item_id = f"{promo_id}-{sap_str}-{account}" if account else f"{promo_id}-{sap_str}"
                    
                    self.conn.execute("""
                        INSERT INTO promo_items (id, promo_id, sap, account, start_date, end_date, discount_percent, special_price)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (id) DO UPDATE SET
                            account = EXCLUDED.account,
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            discount_percent = EXCLUDED.discount_percent,
                            special_price = EXCLUDED.special_price
                    """, [
                        item_id,
                        promo_id,
                        sap_str,
                        account if account else None,
                        item_start if item_start else None,
                        item_end if item_end else None,
                        promo.get('discountPercent'),
                        float(price) if price and price.replace('.', '').isdigit() else None,
                    ])
            elif affected_saps:
                # Legacy format: just SAP codes
                for sap in affected_saps:
                    sap_str = str(sap)
                    item_id = f"{promo_id}-{sap_str}"
                    self.conn.execute("""
                        INSERT INTO promo_items (id, promo_id, sap, discount_percent)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (id) DO NOTHING
                    """, [
                        item_id,
                        promo_id,
                        sap_str,
                        promo.get('discountPercent'),
                    ])
        
        return count
    
    # =========================================================================
    # USER SCHEDULE SYNC
    # =========================================================================

    def sync_user_schedules(self, route_number: str) -> int:
        """Sync user order schedules from Firebase.

        Pulls from users/{uid}/userSettings/notifications/scheduling/orderCycles
        for users who have this route.

        Args:
            route_number: Route to sync schedules for.

        Returns:
            Number of schedule cycles synced.
        """
        db = self._get_firestore()
        if not db:
            print("   ⚠️  Firebase not available")
            return 0

        # Find users with this route - route is stored in profile.routeNumber
        if FieldFilter:
            users = db.collection('users').where(filter=FieldFilter('profile.routeNumber', '==', route_number)).stream()
        else:
            users = db.collection('users').where('profile.routeNumber', '==', route_number).stream()

        count = 0
        for user_doc in users:
            data = user_doc.to_dict()
            user_id = user_doc.id

            # Get order cycles from nested path
            order_cycles = (
                data.get('userSettings', {})
                .get('notifications', {})
                .get('scheduling', {})
                .get('orderCycles', [])
            )

            if not order_cycles:
                continue

            # Clear existing schedules for this user/route before inserting
            self.conn.execute("""
                DELETE FROM user_schedules
                WHERE user_id = ? AND route_number = ?
            """, [user_id, route_number])

            # Insert each cycle
            for i, cycle in enumerate(order_cycles):
                order_day = cycle.get('orderDay', 1)
                load_day = cycle.get('loadDay', 1)
                delivery_day = cycle.get('deliveryDay', 1)

                # Generate schedule_key from ORDER day (matches user mental model)
                # The retrain scheduler maps order_day → delivery_day for ML lookups
                day_names = ['', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                schedule_key = day_names[order_day] if 1 <= order_day <= 7 else 'unknown'

                schedule_id = f"{user_id}-{route_number}-{i}"

                now = datetime.now().isoformat()
                self.conn.execute("""
                    INSERT INTO user_schedules (
                        id, route_number, user_id, order_day, load_day, delivery_day,
                        schedule_key, is_active, created_at, updated_at, synced_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        order_day = EXCLUDED.order_day,
                        load_day = EXCLUDED.load_day,
                        delivery_day = EXCLUDED.delivery_day,
                        schedule_key = EXCLUDED.schedule_key,
                        updated_at = EXCLUDED.updated_at,
                        synced_at = EXCLUDED.synced_at
                """, [
                    schedule_id,
                    route_number,
                    user_id,
                    order_day,
                    load_day,
                    delivery_day,
                    schedule_key,
                    now,
                    now,
                    now,
                ])
                count += 1

        return count

    # =========================================================================
    # FULL SYNC
    # =========================================================================
    
    def full_sync(self, route_number: str, since_days: int = 365) -> dict:
        """Perform a full sync of all data from Firebase.
        
        Args:
            route_number: Route to sync.
            since_days: How many days of order history.
            
        Returns:
            Dict with sync stats.
        """
        print(f"\n🔄 Starting full sync for route {route_number}")
        print("=" * 60)
        
        stats = {
            'route': route_number,
            'products': 0,
            'stores': 0,
            'store_items': 0,
            'orders': 0,
            'line_items': 0,
            'promos': 0,
            'schedules': 0,
        }
        
        # Sync products
        print("\n📦 Syncing product catalog...")
        stats['products'] = self.sync_products(route_number)
        print(f"   ✅ {stats['products']} products synced")
        
        # Sync stores
        print("\n🏪 Syncing store configurations...")
        stores, items = self.sync_stores(route_number)
        stats['stores'] = stores
        stats['store_items'] = items
        print(f"   ✅ {stores} stores, {items} store-item pairs synced")
        
        # Sync orders
        print(f"\n📋 Syncing orders (last {since_days} days)...")
        order_results = self.sync_orders(route_number, since_days=since_days)
        successful = [r for r in order_results if not r.get('skipped')]
        stats['orders'] = len(successful)
        stats['line_items'] = sum(r.get('items', 0) for r in successful)
        print(f"   ✅ {stats['orders']} orders, {stats['line_items']} line items synced")
        
        # Sync promos
        print("\n🏷️  Syncing promotions...")
        stats['promos'] = self.sync_promos(route_number)
        print(f"   ✅ {stats['promos']} promos synced")
        
        # Sync user schedules
        print("\n📅 Syncing user schedules...")
        stats['schedules'] = self.sync_user_schedules(route_number)
        print(f"   ✅ {stats['schedules']} schedule cycles synced")

        print("\n" + "=" * 60)
        print("✅ Full sync complete!")
        
        return stats
    
    def close(self):
        """Close database connection."""
        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description='Sync Firebase to DuckDB')
    parser.add_argument('--route', required=True, help='Route number to sync')
    parser.add_argument('--db', default='data/analytics.duckdb', help='Database path')
    parser.add_argument('--service-account', help='Firebase service account JSON path')
    parser.add_argument('--days', type=int, default=365, help='Days of history to sync')
    parser.add_argument('--incremental', action='store_true', help='Only sync new data')
    parser.add_argument('--create-schema', action='store_true', help='Create schema first')
    parser.add_argument('--products-only', action='store_true', help='Only sync products')
    parser.add_argument('--stores-only', action='store_true', help='Only sync stores')
    parser.add_argument('--orders-only', action='store_true', help='Only sync orders')
    parser.add_argument('--promos-only', action='store_true', help='Only sync promos')
    parser.add_argument('--schedules-only', action='store_true', help='Only sync user schedules')
    args = parser.parse_args()
    
    if not FIREBASE_AVAILABLE:
        print("❌ Firebase libraries not available. Install with:")
        print("   pip install google-cloud-firestore")
        return 1
    
    # Resolve paths
    base_dir = Path(__file__).parent.parent
    db_path = base_dir / args.db
    
    print(f"📁 Database: {db_path}")
    print(f"🔑 Service Account: {args.service_account or 'Default credentials'}")
    
    # Create schema if requested
    if args.create_schema:
        conn = get_connection(db_path)
        create_schema(conn)
        conn.close()
    
    # Initialize sync
    sync = DuckDBSync(str(db_path), args.service_account)
    
    try:
        # Determine what to sync
        specific = args.products_only or args.stores_only or args.orders_only or args.promos_only or args.schedules_only

        if specific:
            # Sync specific entities
            if args.products_only:
                count = sync.sync_products(args.route)
                print(f"✅ Synced {count} products")
            
            if args.stores_only:
                stores, items = sync.sync_stores(args.route)
                print(f"✅ Synced {stores} stores, {items} items")
            
            if args.orders_only:
                days = 30 if args.incremental else args.days
                results = sync.sync_orders(args.route, since_days=days)
                successful = [r for r in results if not r.get('skipped')]
                print(f"✅ Synced {len(successful)} orders")
            
            if args.promos_only:
                count = sync.sync_promos(args.route)
                print(f"✅ Synced {count} promos")

            if args.schedules_only:
                count = sync.sync_user_schedules(args.route)
                print(f"✅ Synced {count} schedule cycles")
        else:
            # Full sync
            days = 30 if args.incremental else args.days
            stats = sync.full_sync(args.route, since_days=days)
        
        # Print summary
        print_schema_summary(sync.conn)
        
    finally:
        sync.close()
    
    return 0


if __name__ == '__main__':
    exit(main())
