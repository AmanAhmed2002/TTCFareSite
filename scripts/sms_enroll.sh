#!/usr/bin/env bash
set -euo pipefail

API="${API:-https://ttcfaresite.onrender.com}"
PHONE="${PHONE:-+16475724372}"
CARRIER="${CARRIER:-publicmobile}"

echo "-> Sending code to $PHONE ($CARRIER) via $API"
curl -sf -X POST "$API/api/sms/start" \
  -H 'content-type: application/json' \
  --data "{\"phone\":\"$PHONE\",\"carrier\":\"$CARRIER\"}" \
  | jq .

read -rp "Enter 6-digit code: " CODE

RID=$(curl -sf -X POST "$API/api/sms/verify" \
  -H 'content-type: application/json' \
  --data "{\"phone\":\"$PHONE\",\"code\":\"$CODE\"}" \
  | jq -r .id)

echo "$RID" > .sms_id
echo "✅ Verified. Recipient ID saved to .sms_id: $RID"

