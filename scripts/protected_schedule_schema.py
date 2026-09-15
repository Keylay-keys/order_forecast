"""Read-only startup checks for administratively managed schedule/history tables.

No connection creation, DDL, backfill, or automatic permission fallback.
"""

TABLE_COLUMNS = {
    'orders_historical': {
        'order_id': 'varchar', 'route_number': 'varchar', 'user_id': 'varchar',
        'schedule_key': 'varchar', 'delivery_date': 'date', 'order_date': 'date',
        'finalized_at': 'timestamptz', 'total_cases': 'int4', 'total_units': 'int4',
        'store_count': 'int4', 'status': 'varchar', 'is_holiday_week': 'bool',
        'order_revision': 'int4', 'last_mutation_kind': 'varchar',
        'last_mutation_id': 'varchar', 'last_mutation_at': 'timestamptz',
        'reallocation_count': 'int4', 'last_reallocated_at': 'timestamptz',
        'last_reallocation_id': 'varchar', 'scheduled_order': 'jsonb',
        'expected_load_date': 'date', 'adjustment_anchor': 'jsonb', 'synced_at': 'timestamptz',
    },
    'user_schedules': {
        'id': 'varchar', 'route_number': 'varchar', 'user_id': 'varchar',
        'order_day': 'int4', 'load_day': 'int4', 'delivery_day': 'int4',
        'send_offset_days': 'int4', 'load_offset_days': 'int4', 'delivery_offset_days': 'int4',
        'schedule_version': 'int4', 'needs_schedule_review': 'bool',
        'schedule_key': 'varchar', 'is_active': 'bool', 'created_at': 'timestamptz',
        'updated_at': 'timestamptz', 'synced_at': 'timestamptz',
    },
}


def validate_protected_table(cur, table):
    if table not in TABLE_COLUMNS:
        raise ValueError('unsupported_protected_table')
    cur.execute('''SELECT a.attname,t.typname FROM pg_catalog.pg_attribute a
                   JOIN pg_catalog.pg_class c ON c.oid=a.attrelid
                   JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
                   JOIN pg_catalog.pg_type t ON t.oid=a.atttypid
                   WHERE n.nspname='public' AND c.relname=%s AND c.relkind='r'
                   AND a.attnum>0 AND NOT a.attisdropped''', (table,))
    columns = dict(cur.fetchall())
    if any(columns.get(name) != kind for name, kind in TABLE_COLUMNS[table].items()):
        raise RuntimeError(f'protected_schema_migration_required:{table}')
    cur.execute('''SELECT EXISTS (
                   SELECT 1 FROM pg_catalog.pg_constraint co
                   JOIN pg_catalog.pg_attribute a ON a.attrelid=co.conrelid AND a.attnum=co.conkey[1]
                   WHERE co.conrelid=pg_catalog.to_regclass(%s) AND co.contype='p'
                   AND cardinality(co.conkey)=1 AND a.attname=%s)''',
                (f'public.{table}', 'id' if table == 'user_schedules' else 'order_id'))
    if cur.fetchone() != (True,):
        raise RuntimeError(f'protected_schema_primary_key_required:{table}')
