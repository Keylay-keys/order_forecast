"""Shared, transactional PostgreSQL projection. Connection ownership is explicit."""
from contextlib import closing

from psycopg2.extras import Json

from .errors import StructuredApiError
from .schedule_source_contract import MAX_REVISION, validate_cycles, validate_scope, validate_timezone

DAYS = ('', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday')


class ProjectionConflict(ValueError):
    pass


def validate_document(document):
    try:
        if type(document) is not dict or set(document) != {'control', 'cycles'}:
            raise ProjectionConflict('invalid_source')
        control = document['control']
        if (type(control) is not dict or set(control) != {'version', 'revision', 'ownerUid', 'routeNumber', 'timezone'}
                or type(control['version']) is not int or control['version'] != 3
                or type(control['revision']) is not int or not 1 <= control['revision'] <= MAX_REVISION):
            raise ProjectionConflict('invalid_source')
        validate_scope(control['ownerUid'], control['routeNumber'])
        validate_timezone(control['timezone'])
        return {'control': dict(control), 'cycles': validate_cycles(document['cycles'])}
    except (StructuredApiError, KeyError, TypeError):
        raise ProjectionConflict('invalid_source') from None


def project_source(connect, document):
    """connect must return a NEW, dedicated connection, never a shared listener one."""
    source = validate_document(document)
    control = source['control']
    route, owner, revision = control['routeNumber'], control['ownerUid'], control['revision']
    with closing(connect()) as conn:
        conn.autocommit = False
        with conn, conn.cursor() as cur:
            cur.execute("SET LOCAL lock_timeout = '2s'")
            cur.execute("SET LOCAL statement_timeout = '5s'")
            cur.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s, 307113))', (route,))
            cur.execute('SELECT owner_uid, revision, source_payload FROM public.order_schedule_revisions WHERE route_number=%s FOR UPDATE', (route,))
            previous = cur.fetchone()
            if previous:
                old_owner, old_revision, old_source = previous
                if old_owner != owner:
                    raise ProjectionConflict('owner_conflict')
                if revision < old_revision:
                    return {'status': 'stale_ignored', 'revision': old_revision}
                if revision == old_revision:
                    if source != old_source:
                        raise ProjectionConflict('revision_conflict')
                    # Verify rows too: matching metadata alone must not certify
                    # a partially altered projection after an operator mistake.
                    _read(cur, route, revision)
                    return {'status': 'already_projected', 'revision': revision}
            cur.execute('''INSERT INTO public.order_schedule_revisions(route_number,owner_uid,revision,source_payload)
                           VALUES (%s,%s,%s,%s) ON CONFLICT(route_number) DO UPDATE
                           SET revision=EXCLUDED.revision,source_payload=EXCLUDED.source_payload,projected_at=now()''',
                        (route, owner, revision, Json(source)))
            cur.execute('DELETE FROM public.user_schedules WHERE route_number=%s', (route,))
            for index, cycle in enumerate(source['cycles']):
                cur.execute('''INSERT INTO public.user_schedules
                  (id,route_number,user_id,order_day,load_day,delivery_day,send_offset_days,load_offset_days,
                   delivery_offset_days,schedule_version,needs_schedule_review,schedule_key,is_active)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,3,false,%s,true)''',
                            (f'{route}-cycle-{index}', route, owner, cycle['orderDay'], cycle['loadDay'], cycle['deliveryDay'],
                             cycle['sendOffsetDays'], cycle['loadOffsetDays'], cycle['deliveryOffsetDays'], DAYS[cycle['orderDay']]))
    return {'status': 'projected', 'revision': revision}


def _read(cur, route, expected_revision):
    # One statement snapshot binds row values to their revision, including empty
    # cycle lists. expected_revision must come from trusted source state.
    cur.execute('''SELECT r.source_payload,s.id,s.user_id,s.order_day,s.load_day,s.delivery_day,
                   s.send_offset_days,s.load_offset_days,s.delivery_offset_days,s.schedule_version,
                   s.needs_schedule_review,s.schedule_key,r.revision,r.owner_uid
                   FROM public.order_schedule_revisions r LEFT JOIN public.user_schedules s
                   ON s.route_number=r.route_number AND s.is_active
                   WHERE r.route_number=%s ORDER BY s.id''', (route,))
    rows = cur.fetchall()
    if not rows:
        raise ProjectionConflict('missing_projection')
    source = validate_document(rows[0][0])
    if (source['control']['routeNumber'] != route or source['control']['revision'] != rows[0][12]
            or source['control']['ownerUid'] != rows[0][13]):
        raise ProjectionConflict('projection_mismatch')
    if source['control']['revision'] != expected_revision:
        raise ProjectionConflict('projection_not_current')
    cycles = []
    for row in rows:
        if row[1] is None:
            continue
        if (row[2] != source['control']['ownerUid'] or type(row[3]) is not int or not 1 <= row[3] <= 7
                or row[11] != DAYS[row[3]]):
            raise ProjectionConflict('projection_mismatch')
        cycles.append(dict(zip(('orderDay', 'loadDay', 'deliveryDay', 'sendOffsetDays', 'loadOffsetDays',
                                'deliveryOffsetDays', 'scheduleVersion', 'needsScheduleReview'), row[3:11])))
    if cycles != source['cycles']:
        raise ProjectionConflict('projection_mismatch')
    return source


def read_projection(connect, route, expected_revision):
    validate_scope('reader', route)
    if type(expected_revision) is not int or not 1 <= expected_revision <= MAX_REVISION:
        raise ProjectionConflict('invalid_revision')
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '5s'")
        return _read(cur, route, expected_revision)
