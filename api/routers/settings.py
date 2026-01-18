"""
Settings API Router

User settings including store column order preferences.
"""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Dict, List

from ..dependencies import verify_firebase_token, get_firestore

router = APIRouter(prefix="/settings", tags=["settings"])


class StoreOrderConfig(BaseModel):
    """Store order configuration per route."""
    config: Dict[str, List[str]]  # routeNumber -> list of storeIds


class StoreOrderResponse(BaseModel):
    """Response with store order config."""
    config: Dict[str, List[str]]
    updatedAt: str | None = None


@router.get("/store-order", response_model=StoreOrderResponse)
async def get_store_order(
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> StoreOrderResponse:
    """Get the user's store column order preferences.
    
    Returns the order of store columns for spreadsheet views,
    keyed by route number.
    
    Falls back to empty config if user hasn't configured store order yet.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Get from Firestore: users/{uid} document, userSettings.storeOrder field
    user_doc = db.collection('users').document(uid).get()
    
    if not user_doc.exists:
        # User document doesn't exist - return empty config (fallback to default order)
        return StoreOrderResponse(config={})
    
    data = user_doc.to_dict()
    user_settings = data.get('userSettings', {})
    store_order = user_settings.get('storeOrder', {}) if user_settings else {}
    
    return StoreOrderResponse(
        config=store_order,
        updatedAt=None
    )


@router.put("/store-order", response_model=StoreOrderResponse)
async def set_store_order(
    body: StoreOrderConfig,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> StoreOrderResponse:
    """Set the user's store column order preferences.
    
    Stores the order of store columns for spreadsheet views,
    keyed by route number.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Save to Firestore: users/{uid} document, userSettings.storeOrder field
    user_ref = db.collection('users').document(uid)
    user_ref.update({
        'userSettings.storeOrder': body.config
    })
    
    return StoreOrderResponse(
        config=body.config,
        updatedAt=None
    )


@router.delete("/store-order/{route_number}")
async def clear_store_order(
    route_number: str,
    decoded_token: dict = Depends(verify_firebase_token),
    db = Depends(get_firestore)
) -> dict:
    """Clear the store order for a specific route.
    
    Reverts to default (alphabetical) order for that route.
    """
    uid = decoded_token.get('uid')
    if not uid:
        raise HTTPException(401, "User ID not found in token")
    
    # Get current config from user document
    user_ref = db.collection('users').document(uid)
    user_doc = user_ref.get()
    
    if not user_doc.exists:
        return {"message": "No store order configured"}
    
    data = user_doc.to_dict()
    user_settings = data.get('userSettings', {})
    config = user_settings.get('storeOrder', {}) if user_settings else {}
    
    if route_number in config:
        del config[route_number]
        user_ref.update({
            'userSettings.storeOrder': config
        })
    
    return {"message": f"Store order cleared for route {route_number}"}
