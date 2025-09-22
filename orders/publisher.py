import os, json, pika

RABBIT_HOST = os.getenv("RABBIT_HOST")
RABBIT_USER = os.getenv("RABBIT_USER", "monitoring_user")
RABBIT_PASS = os.getenv("RABBIT_PASS", "isis2503")
EXCHANGE    = os.getenv("RABBIT_EXCHANGE", "order_events")

def _publish(routing_key: str, payload: dict):
    if not RABBIT_HOST:
        print("[publish] RABBIT_HOST no definido, no se envía evento")
        return
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=RABBIT_HOST,
            credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
        )
    )
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.basic_publish(exchange=EXCHANGE, routing_key=routing_key,
                     body=json.dumps(payload).encode("utf-8"))
    conn.close()

def publish_order_created(order_id: str, status: str):
    _publish("order.created", {"order_id": order_id, "status": status})

def publish_order_status_updated(order_id: str, status: str, version: int):
    _publish("order.status.updated",
             {"order_id": order_id, "new_status": status, "version": version})
