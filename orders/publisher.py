# orders/publisher.py
import os
import json
import pika

# Se leen SIEMPRE desde variables de entorno (nada hardcodeado)
RABBIT_HOST   = os.getenv("RABBIT_HOST")                # ej: 52.87.186.136
RABBIT_PORT   = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_VHOST  = os.getenv("RABBIT_VHOST", "/")
RABBIT_USER   = os.getenv("RABBIT_USER", "monitoring_user")
RABBIT_PASS   = os.getenv("RABBIT_PASS", "isis2503")
EXCHANGE      = os.getenv("RABBIT_EXCHANGE", "order_events")

def _connection_parameters() -> pika.ConnectionParameters:
    """Devuelve parámetros con timeouts y reintentos cortos.
    No bloquea la request si el broker está caído o lejos."""
    return pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS),
        heartbeat=30,
        blocked_connection_timeout=5,
        socket_timeout=5,
        connection_attempts=3,
        retry_delay=2.0,
    )

def _publish(routing_key: str, payload: dict) -> None:
    """Publica sin reventar la request si el broker falla."""
    if not RABBIT_HOST:
        # No hay host configurado → no publicamos, pero tampoco rompemos
        print("[publisher] RABBIT_HOST no definido; evento omitido")
        return
    try:
        params = _connection_parameters()
        conn = pika.BlockingConnection(params)
        ch = conn.channel()
        ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
        ch.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(payload).encode("utf-8"),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistente si la cola es durable
            ),
        )
    except Exception as e:
        # Loguea y sigue; evita que el endpoint de Django se bloquee/falle
        print(f"[publisher] Error publicando {routing_key}: {e}")
    finally:
        try:
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass

def publish_order_created(order_id: str, status: str) -> None:
    _publish("order.created", {"order_id": order_id, "status": status})

def publish_order_status_updated(order_id: str, status: str, version: int, meta: dict | None = None):
    payload = {"order_id": order_id, "new_status": status, "version": int(version)}
    if meta:
        payload["meta"] = meta
    _publish("order.status.updated", payload)

