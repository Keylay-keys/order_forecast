"""Explicitly enabled, independent schedule recovery runtime.

Run as a module: python -m scripts.schedule_recovery_worker --once ...
No dependency on source-write enablement: existing work must survive write pause.
No services, credentials, or connections are initialized on import.
"""
import argparse
import logging
import os
import signal
from collections import Counter
from threading import Event

import psycopg2
from google.cloud import firestore

try:
    from ..api.schedule_recovery import recover_due
except ImportError:
    from api.schedule_recovery import recover_due

logger = logging.getLogger('schedule_recovery')
ENABLE_ENV = 'ORDER_SCHEDULE_V3_RECOVERY_ENABLED'
DSN_ENV = 'ORDER_SCHEDULE_PROJECTOR_DSN'


def connect_projector():
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        raise RuntimeError('projector_connection_not_configured')
    conn = psycopg2.connect(dsn, connect_timeout=3, application_name='schedule-recovery',
                            options='-c statement_timeout=5000 -c lock_timeout=2000')
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""SELECT pg_has_role(current_user,'routespark_schedule_projector','MEMBER'),rolsuper
                           FROM pg_roles WHERE rolname=current_user""")
            role = cur.fetchone()
            if role != (True, False):
                raise RuntimeError('dedicated_projector_role_required')
        return conn
    except Exception:
        conn.close()
        raise


def run(db, connect, stop, *, once=False):
    """An event loop invokes bounded recovery without restarting other workers."""
    while not stop.is_set():
        try:
            results = recover_due(db, connect)
            logger.info('schedule_recovery batch_size=%s outcomes=%s', len(results), dict(Counter(results)))
            if once and any(value in {'pending', 'retryable_failure', 'not_acknowledged'} for value in results):
                return 1
        except Exception as error:
            # Do not emit service-account paths, DSNs, or source documents.
            logger.warning('schedule_recovery retryable_error_type=%s', type(error).__name__)
            if once:
                return 1
        if once:
            return 0
        stop.wait(15)
    return 0


def main(argv=None):
    if os.environ.get(ENABLE_ENV) != 'true':
        logger.info('schedule_recovery disabled')
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--service-account', required=True)
    parser.add_argument('--project', required=True)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    if not os.environ.get(DSN_ENV):
        logger.error('schedule_recovery missing_projector_configuration')
        return 1
    stop = Event()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        db = firestore.Client.from_service_account_json(args.service_account, project=args.project)
        try:
            return run(db, connect_projector, stop, once=args.once)
        finally:
            db.close()
    except Exception as error:
        logger.error('schedule_recovery startup_error_type=%s', type(error).__name__)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
