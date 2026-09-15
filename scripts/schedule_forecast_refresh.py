"""At-least-once handoff into the existing durable forecast job queue.

No daemon import, schema creation, or model generation. Recovery acknowledges
only after all exact targets have durable queue entries. A retry may requeue an
existing job; it never creates a second job key for the same route/key/date.
"""
from datetime import datetime, timedelta
import os
from zoneinfo import ZoneInfo
import psycopg2

try:
    from ..api.schedule_projection import ProjectionConflict, validate_document
    from ..api.schedule_source_store import read_current_schedule
except ImportError:
    from api.schedule_projection import ProjectionConflict, validate_document
    from api.schedule_source_store import read_current_schedule


def connect_forecast_queue():
    dsn = os.environ.get('ORDER_SCHEDULE_FORECAST_DSN')
    if not dsn:
        raise RuntimeError('schedule_forecast_connection_not_configured')
    conn = psycopg2.connect(dsn, connect_timeout=3,
                            options='-c statement_timeout=5000 -c lock_timeout=2000')
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute('SELECT rolsuper FROM pg_roles WHERE rolname=current_user')
            if cur.fetchone() != (False,):
                raise RuntimeError('non_superuser_forecast_role_required')
        return conn
    except Exception:
        conn.close()
        raise


def enqueue_schedule_refresh(db, source, connect=None):
    # Lazy imports keep isolated recovery tests and disabled workers free of
    # environment-driven forecast database connections.
    try:
        from .forecast_generation_queue import enqueue_generation_job
        from .forecast_contract import load_authority_generation_state
    except ImportError:
        from forecast_generation_queue import enqueue_generation_job
        from forecast_contract import load_authority_generation_state
    source = validate_document(source)
    route = source['control']['routeNumber']
    if read_current_schedule(db, route)[2] != source:
        raise ProjectionConflict('source_changed_before_refresh')
    today = datetime.now(ZoneInfo(source['control']['timezone'])).date()
    days = ('', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')
    targets = []
    # Preserve the existing 21-day next-unordered target policy. Offsets are
    # taken from validated source, not a lossy legacy SQL normalizer.
    connect = connect or connect_forecast_queue
    conn = connect()
    try:
        with conn.cursor() as cur:
            for cycle in source['cycles']:
                key = days[cycle['orderDay']]
                for offset in range(1, 22):
                    delivery = today + timedelta(days=offset)
                    if (delivery - timedelta(days=cycle['deliveryOffsetDays'])).isoweekday() != cycle['orderDay']:
                        continue
                    cur.execute('''SELECT EXISTS(SELECT 1 FROM orders_historical
                                   WHERE route_number=%s AND schedule_key=%s AND delivery_date=%s)''',
                                (route, key, delivery.isoformat()))
                    if not cur.fetchone()[0]:
                        targets.append((key, delivery.isoformat()))
                        break
    finally:
        conn.close()
    for key, delivery in targets:
        _, revision = load_authority_generation_state(db, route, delivery, key)
        if read_current_schedule(db, route)[2] != source:
            raise ProjectionConflict('source_changed_before_refresh')
        if not enqueue_generation_job(route, key, delivery, source='schedule_recovery',
                                      desired_revision=revision, refresh_reason='schedule_change',
                                      ensure_schema=False, connect=connect):
            raise RuntimeError('forecast_enqueue_not_confirmed')
    return len(targets)
