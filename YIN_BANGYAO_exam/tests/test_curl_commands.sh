#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:5000}"

curl -s "$BASE_URL/api/v1/health" | python3 -m json.tool
curl -s "$BASE_URL/api/v1/sensors" | python3 -m json.tool
curl -s "$BASE_URL/api/v1/sensors/temperature/latest" | python3 -m json.tool
curl -s "$BASE_URL/api/v1/sensors/temperature/stats?days=7" | python3 -m json.tool
curl -s "$BASE_URL/api/v1/anomalies?sensor=temperature&limit=5" | python3 -m json.tool
curl -s -X POST "$BASE_URL/api/v1/readings" \
  -H "Content-Type: application/json" \
  -d '{"sensor":"temperature","value":36.4,"unit":"C","source":"api-test-rack"}' \
  | python3 -m json.tool
