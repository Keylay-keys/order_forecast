#!/usr/bin/env python3
"""Generate a Firebase ID token for API testing.

Usage:
    # Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
    
    # Run with a test user UID
    python get_test_token.py <uid>
    
    # Or use a test email (will create custom token)
    python get_test_token.py --email test@example.com

The output token can be used in test_security.sh:
    export TOKEN="<output>"
    ./test_security.sh http://127.0.0.1:8000
"""

import sys
import json
import requests
import firebase_admin
from firebase_admin import credentials, auth

# Firebase Web API key (from Firebase Console > Project Settings)
# This is needed to exchange custom token for ID token
WEB_API_KEY = None  # Set this or use environment variable


def init_firebase():
    """Initialize Firebase Admin SDK."""
    if not firebase_admin._apps:
        cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred)


def create_custom_token(uid: str, claims: dict = None) -> str:
    """Create a custom token for a user."""
    init_firebase()
    return auth.create_custom_token(uid, claims).decode('utf-8')


def exchange_custom_token_for_id_token(custom_token: str, api_key: str) -> str:
    """Exchange custom token for ID token using Firebase Auth REST API."""
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key={api_key}"
    
    response = requests.post(url, json={
        "token": custom_token,
        "returnSecureToken": True
    })
    
    if response.status_code != 200:
        raise Exception(f"Token exchange failed: {response.text}")
    
    return response.json()["idToken"]


def main():
    import os
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate Firebase test token")
    parser.add_argument("uid", nargs="?", help="User UID to generate token for")
    parser.add_argument("--email", help="Email for claims (optional)")
    parser.add_argument("--route", default="989262", help="Route number for claims")
    parser.add_argument("--api-key", help="Firebase Web API key")
    args = parser.parse_args()
    
    uid = args.uid or "test-user-" + str(int(__import__('time').time()))
    api_key = args.api_key or os.environ.get("FIREBASE_WEB_API_KEY") or WEB_API_KEY
    
    # Custom claims
    claims = {}
    if args.email:
        claims["email"] = args.email
    if args.route:
        claims["routeNumber"] = args.route
    
    print(f"Generating token for UID: {uid}", file=sys.stderr)
    
    # Create custom token
    custom_token = create_custom_token(uid, claims if claims else None)
    print(f"Custom token created", file=sys.stderr)
    
    if api_key:
        # Exchange for ID token
        id_token = exchange_custom_token_for_id_token(custom_token, api_key)
        print(f"ID token obtained (valid ~1 hour)", file=sys.stderr)
        print(id_token)
    else:
        print("No API key provided - outputting custom token instead", file=sys.stderr)
        print("To get ID token, set FIREBASE_WEB_API_KEY or --api-key", file=sys.stderr)
        print(custom_token)


if __name__ == "__main__":
    main()
