"""Shared listener/read boundary. No clients, connections, or threads at import."""
from dataclasses import dataclass

# The existing daemons are invoked by filename, while API/tests use packages.
if not __package__:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from ..api.errors import StructuredApiError
    from ..api.schedule_source_store import read_current_schedule
    from ..api.schedule_projection import ProjectionConflict, read_projection
    from ..api.schedule_recovery import process_one
    from .schedule_recovery_worker import connect_projector
except ImportError:
    from api.errors import StructuredApiError
    from api.schedule_source_store import read_current_schedule
    from api.schedule_projection import ProjectionConflict, read_projection
    from api.schedule_recovery import process_one
    from scripts.schedule_recovery_worker import connect_projector


@dataclass(frozen=True)
class ScheduleSyncResult:
    status: str
    count: int = 0
    revision: int | None = None

    @property
    def ready(self):
        return self.status in {'projected', 'already_projected', 'legacy_synced'}


def sync_current_v3(db, route, source, connect=None):
    """Use durable work, then re-read authority and prove SQL freshness.

    No listener enable gate: existing V3 evidence must remain recoverable during
    a source-write pause. Missing dedicated credentials produce a retry, never
    fallback to a shared legacy SQL connection.
    """
    connect = connect or connect_projector
    try:
        process_one(db, connect, db.collection('orderScheduleProjectionWork').document(route))
        _, _, current = read_current_schedule(db, route)
        if current is None or current != source:
            return ScheduleSyncResult('pending')
        projected = read_projection(connect, route, current['control']['revision'])
        if projected != current:
            raise ProjectionConflict('projection_source_mismatch')
        return ScheduleSyncResult('already_projected', len(current['cycles']), current['control']['revision'])
    except ProjectionConflict as error:
        return ScheduleSyncResult('pending' if str(error) in {'missing_projection', 'projection_not_current'} else 'blocked')
    except StructuredApiError:
        return ScheduleSyncResult('blocked')
    except Exception:
        return ScheduleSyncResult('pending')


def read_current_projection(db, route, connect, legacy_uid=None):
    """Return None only for a legacy source, never for a failed V3 read."""
    _, _, source = read_current_schedule(db, route, legacy_uid)
    if source is None:
        return None
    projected = read_projection(connect, route, source['control']['revision'])
    if projected != source:
        raise ProjectionConflict('projection_source_mismatch')
    # Detect a source change during SQL without holding a database transaction
    # over network calls. Callers creating stamps still need their own CAS.
    if read_current_schedule(db, route, legacy_uid)[2] != source:
        raise ProjectionConflict('projection_not_current')
    return projected
