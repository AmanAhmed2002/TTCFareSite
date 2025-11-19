#!/usr/bin/env bash
set -euo pipefail

API="${API:-https://ttcfaresite.onrender.com}"
RID="${RID:-$(cat .sms_id)}"
OFFSETS="${OFFSETS:-[180,60]}"   # 3 minutes and 1 minute from now
POLL_SECS="${POLL_SECS:-20}"     # how often to hit jobs/run
POLL_TOTAL="${POLL_TOTAL:-13}"   # 13 * 20s ~ 4m20s

echo "-> Scheduling test reminders for recipient $RID @ $OFFSETS"
curl -sf -X POST "$API/api/sms/reminders/test" \
  -H 'content-type: application/json' \
  --data "{\"recipientId\":\"$RID\",\"offsetsSec\":$OFFSETS}" \
  | jq .

echo "-> Polling $API/api/jobs/run every $POLL_SECS s ($POLL_TOTAL times)..."
for i in $(seq 1 "$POLL_TOTAL"); do
  sleep "$POLL_SECS"
  echo "[$i] run jobs"
  curl -sf -X POST "$API/api/jobs/run" | jq .
done

echo "✅ Done. If deliverability is OK, you should have received up to $(echo "$OFFSETS" | jq 'length') messages."

