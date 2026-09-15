"""Server-only Business pilot invitations; an invitation never grants ownership.

No cache: revocation and route transfers take effect on the next API request.
Clients cannot manage invitations. Keep all future Business endpoints behind
this guard, even if the portal already checked the capability.
"""

import logging
import re

from fastapi import HTTPException

from .dependencies import require_route_access
from .route_ownership import extract_owned_routes_for_owner

PILOT_COLLECTION = "accounting_pilots"
logger = logging.getLogger("api.business_access")


async def require_business_access(route: str, decoded_token: dict, db) -> dict:
    uid = decoded_token.get("uid")
    if not isinstance(uid, str) or not uid or len(uid) > 128 or "/" in uid:
        raise HTTPException(401, "Authentication required")
    if not isinstance(route, str) or not re.fullmatch(r"[0-9]{1,10}", route):
        raise HTTPException(400, "Invalid route number")

    try:
        invitation = db.collection(PILOT_COLLECTION).document(uid).get().to_dict() or {}
        routes = invitation.get("routes")
        if (
            type(invitation.get("schemaVersion")) is not int
            or invitation["schemaVersion"] != 1
            or invitation.get("enabled") is not True
            or not isinstance(routes, list)
            or not 1 <= len(routes) <= 100
            or any(not isinstance(item, str) or not re.fullmatch(r"[0-9]{1,10}", item) for item in routes)
            or route not in routes
        ):
            raise HTTPException(403, "Business is not available for this account and route")

        user_data = await require_route_access(route, decoded_token, db)
        if route not in extract_owned_routes_for_owner(user_data):
            raise HTTPException(403, "Business is not available for this account and route")

        # Independent, canonical ownership evidence: stale profile assignments
        # must not retain access following a transfer. Missing/conflicting
        # anchors fail closed instead of treating an invitation as ownership.
        route_data = db.collection("routes").document(route).get().to_dict() or {}
        claim = db.collection("routeNumbers").document(route).get().to_dict() or {}
        if route_data.get("ownerUid") != uid or claim.get("userId") != uid:
            raise HTTPException(403, "Business is not available for this account and route")
        return user_data
    except HTTPException:
        raise
    except Exception:
        logger.warning("Business access verification unavailable")
        raise HTTPException(503, "Business access could not be verified") from None
