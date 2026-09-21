#!/usr/bin/env bash
set -e

COUNT="${1:-1000}"
PRODUCER_URL="${2:-http://localhost:8081/api/transactions/trigger}"

echo "=================================================="
echo "🚀 Triggering production of $COUNT transactions..."
echo "Target URL: $PRODUCER_URL?count=$COUNT"
echo "=================================================="

RESPONSE=$(curl -s -X POST "$PRODUCER_URL?count=$COUNT" -H "Content-Type: application/json")

echo "Response from Producer Service:"
echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"
echo ""
echo "✅ Batch trigger completed!"
