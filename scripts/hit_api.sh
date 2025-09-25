#!/usr/bin/env bash
set -euo pipefail

APP_IP=${APP_IP:-127.0.0.1}
APP_PORT=${APP_PORT:-8080}
APP="http://${APP_IP}:${APP_PORT}"

N_ORDERS=${N_ORDERS:-5}
N_UPDATES=${N_UPDATES:-20}
OTHER_REQUESTS=${OTHER_REQUESTS:-30}
PAUSE=${PAUSE:-0.2}

STATUSES=("UPDATED" "SHIPPED" "CANCELLED" "DELIVERED")

ORDER_IDS=()

log() {
  printf '[hit_api] %s\n' "$*"
}

random_id() {
  python3 - <<'PY'
import random,string
print('ORD-' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=6)))
PY
}

create_orders() {
  log "Creating ${N_ORDERS} orders against ${APP}"
  for ((i=1; i<=N_ORDERS; i++)); do
    local id
    id=$(random_id)
    local payload
    payload=$(printf '{"id":"%s","status":"CREATED"}' "$id")
    local body status
    body=$(curl -s -w '\n%{http_code}' -X POST "${APP}/orders" \
      -H 'Content-Type: application/json' \
      -d "$payload")
    status=$(printf '%s' "$body" | tail -n1)
    if [[ "$status" =~ ^2 ]]; then
      ORDER_IDS+=("$id")
      log "[create] $id ($status)"
    else
      log "[create] failed for $id (status=$status)"
    fi
    sleep "$PAUSE"
  done
  if [[ ${#ORDER_IDS[@]} -eq 0 ]]; then
    log 'No orders created successfully; aborting.'
    exit 1
  fi
}

update_orders() {
  log "Updating ${N_UPDATES} times"
  for ((i=1; i<=N_UPDATES; i++)); do
    local idx=$((RANDOM % ${#ORDER_IDS[@]}))
    local id=${ORDER_IDS[$idx]}
    local status=${STATUSES[$((RANDOM % ${#STATUSES[@]}))]}
    local payload
    payload=$(printf '{"status":"%s"}' "$status")
    local body status_code
    body=$(curl -s -w '\n%{http_code}' -X PUT "${APP}/orders/${id}/status" \
      -H 'Content-Type: application/json' \
      -d "$payload")
    status_code=$(printf '%s' "$body" | tail -n1)
    log "[update] ${id} -> ${status} (status=${status_code})"
    sleep "$PAUSE"
  done
}

other_requests() {
  log "Issuing ${OTHER_REQUESTS} auxiliary requests"
  for ((i=1; i<=OTHER_REQUESTS; i++)); do
    case $((RANDOM % 4)) in
      0)
        curl -s "${APP}/" >/dev/null || log '[aux] GET / failed'
        ;;
      1)
        curl -s "${APP}/measurements/" >/dev/null || log '[aux] GET /measurements/ failed'
        ;;
      2)
        curl -s "${APP}/variables/" >/dev/null || log '[aux] GET /variables/ failed'
        ;;
      3)
        local idx=$((RANDOM % ${#ORDER_IDS[@]}))
        local id=${ORDER_IDS[$idx]}
        curl -s "${APP}/orders/${id}" >/dev/null || log "[aux] GET /orders/${id} failed"
        ;;
    esac
    sleep "$PAUSE"
  done
}

main() {
  log "API base: ${APP}"
  create_orders
  update_orders &
  updates_pid=$!
  other_requests &
  aux_pid=$!
  wait "$updates_pid"
  wait "$aux_pid"
  log 'Done.'
}

main "$@"
