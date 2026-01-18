#!/bin/bash
# OWASP API Security Top 10 Test Script
# Run against local API: ./test_security.sh http://127.0.0.1:8000
#
# Optional: Valid Firebase token in $TOKEN environment variable
# Tests that don't require auth will run without it.
#
# Usage:
#   ./test_security.sh http://127.0.0.1:8000           # Basic tests (no auth)
#   export TOKEN="your-firebase-id-token"
#   ./test_security.sh http://127.0.0.1:8000           # Full tests

BASE_URL="${1:-http://127.0.0.1:8000}"
ROUTE="989262"  # Test route
PASS=0
FAIL=0
SKIP=0

echo "=================================================="
echo "OWASP API Security Top 10 Tests"
echo "Base URL: $BASE_URL"
if [ -z "$TOKEN" ]; then
    echo "TOKEN: Not set (some tests will be skipped)"
else
    echo "TOKEN: Set (${#TOKEN} chars)"
fi
echo "=================================================="
echo ""

# Helper function
test_endpoint() {
    local name="$1"
    local expected_code="$2"
    local method="$3"
    local endpoint="$4"
    local data="$5"
    
    if [ -z "$data" ]; then
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
            -H "Authorization: Bearer ${TOKEN:-invalid}" \
            -H "Content-Type: application/json" \
            "$BASE_URL$endpoint")
    else
        response=$(curl -s -w "\n%{http_code}" -X "$method" \
            -H "Authorization: Bearer ${TOKEN:-invalid}" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "$BASE_URL$endpoint")
    fi
    
    code=$(echo "$response" | tail -1)
    body=$(echo "$response" | head -n -1)
    
    if [ "$code" -eq "$expected_code" ]; then
        echo "✅ PASS: $name (expected $expected_code, got $code)"
        ((PASS++))
    else
        echo "❌ FAIL: $name (expected $expected_code, got $code)"
        echo "   Response: $body"
        ((FAIL++))
    fi
}

echo "=== API1: Broken Object Level Authorization (BOLA) ==="
echo ""

if [ -n "$TOKEN" ]; then
    # Test cross-route access (should fail with 403)
    test_endpoint "Cross-route access denied" 403 GET "/api/history?route=000000&weeks=1"
else
    echo "⏭️  SKIP: Cross-route access (needs TOKEN)"
    ((SKIP++))
fi

echo ""
echo "=== API2: Broken Authentication ==="
echo ""

# Test with no token (always works)
TOKEN_BACKUP="$TOKEN"
TOKEN=""
test_endpoint "No auth header rejected" 401 GET "/api/auth/verify"

# Test with invalid token
TOKEN="invalid-token-12345"
test_endpoint "Invalid token rejected" 401 GET "/api/auth/verify"
TOKEN="$TOKEN_BACKUP"

echo ""
echo "=== API4: Unrestricted Resource Consumption ==="
echo ""

if [ -n "$TOKEN" ]; then
    # Test with oversized parameter
    test_endpoint "Oversized weeks rejected" 422 GET "/api/history?route=$ROUTE&weeks=5200"
else
    echo "⏭️  SKIP: Oversized weeks (needs TOKEN)"
    ((SKIP++))
fi

echo ""
echo "=== API7: SSRF Prevention ==="
echo ""

# No URL parameters should exist
test_endpoint "URL param not accepted" 404 GET "/api/pcf?route=$ROUTE&url=http://127.0.0.1"

echo ""
echo "=== API8: Security Misconfiguration ==="
echo ""

# Docs should be disabled in production
test_endpoint "Docs disabled" 404 GET "/docs"
test_endpoint "OpenAPI disabled" 404 GET "/openapi.json"

echo ""
echo "=== Layer 9: Honeypot Detection ==="
echo ""

# These paths should trigger honeypot and block
test_endpoint "Honeypot: wp-admin" 404 GET "/wp-admin"
test_endpoint "Honeypot: .env" 404 GET "/.env"
test_endpoint "Honeypot: .git" 404 GET "/.git/config"

echo ""
echo "=== Layer 11: Enumeration Detection ==="
echo ""

# Path traversal should be blocked
test_endpoint "Path traversal blocked" 400 GET "/api/../../../etc/passwd"

echo ""
echo "=== Health Endpoints ==="
echo ""

# Health should work without auth
TOKEN_BACKUP="$TOKEN"
TOKEN=""
test_endpoint "Health endpoint accessible" 200 GET "/api/health"
TOKEN="$TOKEN_BACKUP"

echo ""
echo "=================================================="
echo "Results: $PASS passed, $FAIL failed, $SKIP skipped"
echo "=================================================="

if [ $FAIL -gt 0 ]; then
    exit 1
fi

if [ $SKIP -gt 0 ]; then
    echo ""
    echo "To run all tests, set TOKEN:"
    echo "  export TOKEN=\$(python api/tests/get_test_token.py <uid> --api-key <key>)"
fi
