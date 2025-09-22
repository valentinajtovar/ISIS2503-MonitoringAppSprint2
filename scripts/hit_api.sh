#!/usr/bin/env bash
set -euo pipefail
APP="http://${APP_IP:-127.0.0.1}:8080"

new_id() {
  python3 - <<'PY'
import random,string
print("ORD-"+"".join(random.choices(string.ascii_uppercase+string.digits,k=6)))
PY
}

# Crea N órdenes y luego hace M updates aleatorios
N=${N_ORDERS:-5}
M=${N_UPDATES:-20}

echo "API base: $APP"
echo "Creando $N órdenes..."
for i in $(seq 1 $N); do
  ID=$(new_id)
  curl -s -X POST "$APP/orders" \
    -H "Content-Type: application/json" \
    -d "{\"id\":\"$ID\",\"status\":\"CREATED\"}" | jq .
  sleep 0.2
done

echo "Actualizando $M veces..."
for i in $(seq 1 $M); do
  ID=$(curl -s "$APP/orders/$(ls /tmp 2>/dev/null | shuf -n 1)" >/dev/null 2>&1; new_id) # fallback ID random
  # mejor: toma un ID real de DB vía Django shell; simple: elige estado al azar
  STATUS=$(shuf -e UPDATED SHIPPED CANCELLED DELIVERED -n 1)
  VERSION=$((RANDOM % 3))
  curl -s -X PUT "$APP/orders/$ID/status" \
    -H "Content-Type: application/json" \
    -d "{\"status\":\"$STATUS\",\"version\":$VERSION}" | jq .
  sleep 0.2
done
