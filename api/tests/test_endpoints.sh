#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:8000}"
ROUTE="${TEST_ROUTE:-989262}"
WRITE_TESTS="${WRITE_TESTS:-0}"

if [[ -z "${TOKEN:-}" ]]; then
  echo "TOKEN not set. Export TOKEN first." >&2
  exit 1
fi

auth_header=(-H "Authorization: Bearer ${TOKEN}")

echo "== Web Portal API Endpoint Smoke Tests =="
echo "Base URL: ${BASE_URL}"
echo "Route: ${ROUTE}"
echo "Write tests: ${WRITE_TESTS}"

echo "-- Auth Verify"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/auth/verify" >/dev/null

echo "-- Health"
curl -sS "${BASE_URL}/api/health" >/dev/null

echo "-- Reference: Products"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/products?route=${ROUTE}" >/dev/null

echo "-- Reference: Stores"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/stores?route=${ROUTE}" >/dev/null

echo "-- Reference: Schedule"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/schedule?route=${ROUTE}" >/dev/null

echo "-- Low Quantity"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/low-quantity?route=${ROUTE}" >/dev/null

echo "-- Forecast Status"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/forecast/status?route=${ROUTE}" >/dev/null

echo "-- Active Order"
curl -sS "${auth_header[@]}" "${BASE_URL}/api/orders/active?route=${ROUTE}" >/dev/null

if [[ "${WRITE_TESTS}" == "1" ]]; then
  echo "-- Create Order"
  delivery_date=$(python - <<'PY'
import datetime
print((datetime.date.today() + datetime.timedelta(days=3)).isoformat())
PY
)

  create_resp=$(curl -sS "${auth_header[@]}" -H "Content-Type: application/json" \
    -d "{\"routeNumber\":\"${ROUTE}\",\"deliveryDate\":\"${delivery_date}\",\"scheduleKey\":\"monday\"}" \
    "${BASE_URL}/api/orders")

  order_id=$(python - <<PY
import json, sys
data=json.loads(sys.stdin.read())
print(data["id"])
PY
<<<"${create_resp}")

  updated_at=$(python - <<PY
import json, sys
data=json.loads(sys.stdin.read())
print(data["updatedAt"])
PY
<<<"${create_resp}")

  echo "-- Update Order"
  curl -sS "${auth_header[@]}" -H "Content-Type: application/json" \
    -d "{\"stores\":[{\"storeId\":\"store_test\",\"storeName\":\"Test Store\",\"items\":[{\"sap\":\"00000\",\"quantity\":1}]}],\"updatedAt\":\"${updated_at}\"}" \
    "${BASE_URL}/api/orders/${order_id}" >/dev/null

  echo "-- Apply Forecast (empty items)"
  curl -sS "${auth_header[@]}" -H "Content-Type: application/json" \
    -d "{\"forecastId\":\"test-forecast\",\"items\":[]}" \
    "${BASE_URL}/api/forecast/apply/${order_id}" >/dev/null

  echo "NOTE: Finalize not executed (avoids syncing/training side effects)."
fi

echo "✅ Endpoint smoke tests completed"
