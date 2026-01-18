"""Authentication router - token verification and user info.

Endpoints:
    GET /api/auth/verify - Verify token and return user info with accessible routes
"""

from __future__ import annotations

from typing import List
from fastapi import APIRouter, Depends, Request

from ..dependencies import verify_firebase_token, get_firestore
from ..models import UserInfo
from ..middleware.rate_limit import rate_limit_auth

router = APIRouter()


@router.get("/verify", response_model=UserInfo)
@rate_limit_auth
async def verify_token(
    request: Request,  # Required for slowapi rate limiting
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> UserInfo:
    """Verify Firebase token and return user information.
    
    Returns user info including:
    - uid, email, displayName
    - List of accessible routes
    - Current route
    - Role (owner/team_member)
    """
    uid = decoded_token['uid']
    
    # Fetch user document
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        # Token valid but user doc doesn't exist (shouldn't happen normally)
        return UserInfo(
            uid=uid,
            email=decoded_token.get('email'),
            displayName=decoded_token.get('name'),
            routes=[],
            currentRoute=None,
            role=None
        )
    
    user_data = user_doc.to_dict()
    profile = user_data.get('profile', {})
    
    # Build list of accessible routes
    routes: List[str] = []
    
    # Primary route
    primary_route = profile.get('routeNumber') or profile.get('currentRoute')
    if primary_route:
        routes.append(str(primary_route))
    
    # Additional routes
    additional = profile.get('additionalRoutes') or []
    for r in additional:
        if str(r) not in routes:
            routes.append(str(r))
    
    # Route assignments
    assignments = user_data.get('routeAssignments') or {}
    for r in assignments.keys():
        if str(r) not in routes:
            routes.append(str(r))
    
    return UserInfo(
        uid=uid,
        email=decoded_token.get('email') or profile.get('email'),
        displayName=decoded_token.get('name') or profile.get('displayName'),
        routes=routes,
        currentRoute=str(primary_route) if primary_route else None,
        role=profile.get('role')
    )
