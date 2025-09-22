# scripts/pump_events.py
import os, time, json, random, string
import pika

RABBIT_HOST   = os.getenv("RABBIT_HOST", "127.0.0.1")
RABBIT_PORT   = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER   = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS   = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST  = os.getenv("RABBIT_VHOST", "/")
EXCHANGE      = os.getenv("RABBIT_EXCHANGE", "order_events")

STATUSES_FLOW = {
    "CREATED": ["UPDATED", "CANCELLED", "SHIPPED"],
    "UPDATED": ["SHIPPED", "CANCELLED"],
    "SHIPPED": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": [],
}

def rand_order_id(prefix="ORD", n=6):
    return f"{prefix}-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

def publish(ch, routing_key, payload):
    ch.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )

def main(rate_per_sec=2):
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
        credentials=creds, heartbeat=30, blocked_connection_timeout=10,
        connection_attempts=5, retry_delay=2.0,
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # catálogo de órdenes vivas con su estado/versión
    live = {}

    print(f"✔ Publicando aleatorio a {EXCHANGE} @ {RABBIT_HOST}:{RABBIT_PORT}  (Ctrl+C para salir)")
    sleep = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.5
    i = 0
    try:
        while True:
            i += 1
            # 50% crear, 50% actualizar alguna orden existente (si hay)
            if not live or random.random() < 0.5:
                oid = rand_order_id()
                payload = {"order_id": oid, "status": "CREATED"}
                publish(ch, "order.created", payload)
                live[oid] = {"status": "CREATED", "version": 0}
                print(f"[{i:05}] created  -> {payload}")
            else:
                oid = random.choice(list(live.keys()))
                cur = live[oid]["status"]
                nxts = STATUSES_FLOW.get(cur, [])
                if not nxts:
                    # sin transiciones posibles, sáltate
                    time.sleep(sleep); continue
                new = random.choice(nxts)
                live[oid]["version"] += 1
                payload = {"order_id": oid, "new_status": new, "version": live[oid]["version"]}
                publish(ch, "order.status.updated", payload)
                live[oid]["status"] = new
                print(f"[{i:05}] updated  -> {payload}")

            time.sleep(sleep)
    except KeyboardInterrupt:
        print("\nDetenido por usuario.")
    finally:
        conn.close()

if __name__ == "__main__":
    # Cambia la tasa si quieres más tráfico (p. ej. 10/s)
    main(rate_per_sec=int(os.getenv("EVENTS_RATE", "2")))
