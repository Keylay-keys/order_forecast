"""RouteSpark Web Portal API.

FastAPI-based backend for the web portal, providing:
- Order history queries (DuckDB)
- Order creation and management (Firestore)
- Forecast retrieval and application
- Health and sync status

Security: Firebase Auth tokens required for all endpoints.
"""
