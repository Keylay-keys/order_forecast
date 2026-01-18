"""Client helper for communicating with the DB Manager service.

Other services use this to send requests to the centralized DB Manager
instead of accessing DuckDB directly.

Usage:
    from db_client import DBClient
    
    client = DBClient(firestore_client)
    
    # Query
    result = client.query("SELECT * FROM orders_historical WHERE route_number = ?", ["989262"])
    
    # Sync an order
    result = client.sync_order("order-989262-123456", "989262")
    
    # Get historical shares
    shares = client.get_historical_shares("989262", "31032", "tuesday")
"""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from google.cloud import firestore


class DBClient:
    """Client for communicating with DB Manager service via Firebase."""
    
    def __init__(self, db: firestore.Client, timeout: float = 30.0):
        """
        Args:
            db: Firestore client
            timeout: Max seconds to wait for response
        """
        self.db = db
        self.timeout = timeout
        self.requests_col = db.collection("dbRequests")
    
    def _send_request(self, request_type: str, payload: Dict) -> Dict:
        """Send a request and wait for response."""
        request_id = f"req-{uuid.uuid4()}"
        
        # Create request document
        doc_ref = self.requests_col.document(request_id)
        doc_ref.set({
            "requestId": request_id,
            "type": request_type,
            "payload": payload,
            "status": "pending",
            "createdAt": firestore.SERVER_TIMESTAMP,
        })
        
        # Poll for completion
        start_time = time.time()
        while time.time() - start_time < self.timeout:
            doc = doc_ref.get()
            if not doc.exists:
                raise Exception(f"Request {request_id} disappeared")
            
            data = doc.to_dict()
            status = data.get("status")
            
            if status == "completed":
                result = data.get("result", {})
                # Clean up the request document
                doc_ref.delete()
                return result
            
            elif status == "error":
                error = data.get("error", "Unknown error")
                doc_ref.delete()
                raise Exception(f"DB Manager error: {error}")
            
            # Still processing, wait a bit
            time.sleep(0.1)
        
        # Timeout - mark as failed and raise
        doc_ref.update({"status": "timeout"})
        raise TimeoutError(f"Request {request_id} timed out after {self.timeout}s")
    
    def query(self, sql: str, params: Optional[List] = None) -> Dict:
        """Execute a read query.
        
        Returns:
            Dict with 'columns', 'rows', 'row_count'
        """
        return self._send_request("query", {
            "sql": sql,
            "params": params or [],
        })
    
    def write(self, sql: str, params: Optional[List] = None) -> Dict:
        """Execute a write operation.
        
        Returns:
            Dict with 'success', 'affected_rows'
        """
        return self._send_request("write", {
            "sql": sql,
            "params": params or [],
        })
    
    def sync_order(self, order_id: str, route_number: str) -> Dict:
        """Sync an order from Firebase to DuckDB.
        
        Returns:
            Dict with 'success', 'orderId', 'totalUnits', etc.
        """
        return self._send_request("sync_order", {
            "orderId": order_id,
            "routeNumber": route_number,
        })
    
    def get_historical_shares(self, route_number: str, sap: str, schedule_key: str) -> Dict:
        """Get case allocation shares for a SAP.
        
        Returns:
            Dict with 'shares' mapping store_id -> share info
        """
        return self._send_request("get_historical_shares", {
            "routeNumber": route_number,
            "sap": sap,
            "scheduleKey": schedule_key,
        })
    
    def get_archived_dates(self, route_number: str) -> Dict:
        """Get list of archived order dates.
        
        Returns:
            Dict with 'dates' list
        """
        return self._send_request("get_archived_dates", {
            "routeNumber": route_number,
        })
    
    def get_order(self, route_number: str, delivery_date: str) -> Dict:
        """Get a specific archived order.
        
        Returns:
            Dict with 'order' containing full order data
        """
        return self._send_request("get_order", {
            "routeNumber": route_number,
            "deliveryDate": delivery_date,
        })
    
    def check_route_synced(self, route_number: str) -> Dict:
        """Check if a route is synced in DuckDB.
        
        Returns:
            Dict with 'synced', 'status', etc.
        """
        return self._send_request("check_route_synced", {
            "routeNumber": route_number,
        })
    
    def get_delivery_manifest(self, route_number: str, delivery_date: str, store_id: Optional[str] = None) -> Dict:
        """Get delivery manifest for a date.
        
        Args:
            route_number: Route to query
            delivery_date: Date in YYYY-MM-DD format
            store_id: Optional - filter to single store
        
        Returns:
            Dict with 'manifest' containing stores, items, totals
        """
        payload = {
            "routeNumber": route_number,
            "deliveryDate": delivery_date,
        }
        if store_id:
            payload["storeId"] = store_id
        return self._send_request("get_delivery_manifest", payload)
    
    def get_store_delivery(self, route_number: str, store_id: str, delivery_date: str) -> Dict:
        """Get delivery for a specific store on a date (for invoice pre-fill).
        
        Returns:
            Dict with 'storeDelivery' containing items for that store
        """
        return self._send_request("get_store_delivery", {
            "routeNumber": route_number,
            "storeId": store_id,
            "deliveryDate": delivery_date,
        })


# Convenience function for simple queries
def query_db(db: firestore.Client, sql: str, params: Optional[List] = None, timeout: float = 30.0) -> List[Dict]:
    """Simple helper to run a query and get rows."""
    client = DBClient(db, timeout)
    result = client.query(sql, params)
    if 'error' in result:
        raise Exception(result['error'])
    return result.get('rows', [])

