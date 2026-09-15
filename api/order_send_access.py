"""Transactional route authority for Send, separate from retired V3 enrollment.

The actor and schedule owner are distinct. Multi-route owners remain owners on
each owned route; assigned members may read/use/edit its schedule. An owner-only operation
must request that mode explicitly. Ordering entitlement remains the HTTP guard's
responsibility, just as for existing Orders endpoints.
"""
from dataclasses import dataclass

from google.cloud.firestore_v1.base_query import FieldFilter

from .dependencies import has_access_to_route
from .errors import StructuredApiError
from .route_ownership import extract_owned_routes_for_owner
from .schedule_source_contract import validate_scope


def denied():
    return StructuredApiError(403, 'Route schedule access denied.', 'SCHEDULE_ACCESS_DENIED')


@dataclass(frozen=True)
class SendRouteAccess:
    actor_uid: str
    actor: dict
    owner_uid: str
    owner: dict
    owner_ref: object


def read_send_route_access(db, transaction, uid, route, *, owner_only=False):
    validate_scope(uid, route)
    actor_ref = db.document(f'users/{uid}')
    actor_snapshot = actor_ref.get(transaction=transaction)
    actor = actor_snapshot.to_dict() if actor_snapshot.exists else None
    if not isinstance(actor, dict) or not has_access_to_route(actor, route):
        raise denied()

    # Match existing Schedule ownership resolution, but all evidence belongs to
    # this transaction. Missing one anchor is not a conflicting owner. Never
    # invent a canonical record or choose one of two contradictory owners.
    owners = set()
    authority_exists = False
    for collection in ('routes', 'routeEntitlements', 'routeNumbers'):
        snapshot = db.document(f'{collection}/{route}').get(transaction=transaction)
        authority_exists |= snapshot.exists
        anchor = snapshot.to_dict() if snapshot.exists else {}
        for key in ('ownerUid', 'userId', 'userID'):
            value = anchor.get(key)
            if value is not None and value != '':
                if not isinstance(value, str) or value != value.strip():
                    raise denied()
                validate_scope(value, route)
                owners.add(value)
    if not owners and not authority_exists:
        # Established legacy Schedule fallback only when no authority document
        # exists. Two candidates are enough to fail closed, never a full scan.
        for field in ('profile.routeNumber', 'profile.currentRoute'):
            query = db.collection('users').where(filter=FieldFilter(field, '==', route)).where(
                filter=FieldFilter('profile.role', '==', 'owner')).limit(2)
            owners.update(doc.id for doc in query.stream(transaction=transaction))
    if len(owners) != 1:
        raise denied()
    owner_uid = next(iter(owners))
    validate_scope(owner_uid, route)
    owner_ref = db.document(f'users/{owner_uid}')
    if owner_uid == uid:
        owner = actor
    else:
        snapshot = owner_ref.get(transaction=transaction)
        owner = snapshot.to_dict() if snapshot.exists else None
    if not isinstance(owner, dict) or route not in extract_owned_routes_for_owner(owner):
        raise denied()
    if owner_only and uid != owner_uid:
        raise denied()
    # Explicit stale assignment links must not survive an ownership transfer.
    assignments = actor.get('routeAssignments') or {}
    if not isinstance(assignments, dict):
        raise denied()
    assignment = assignments.get(route) or {}
    if not isinstance(assignment, dict):
        raise denied()
    if uid != owner_uid:
        # A remembered currentRoute is navigation state, not a live team grant.
        # Removing the assignment must revoke access even before a device syncs.
        if (assignment.get('role') != 'team_member' or assignment.get('verified') is not True
                or assignment.get('needsApproval', False) is not False
                or assignment.get('assignedTo') not in (None, '', owner_uid)):
            raise denied()
    return SendRouteAccess(uid, actor, owner_uid, owner, owner_ref)
