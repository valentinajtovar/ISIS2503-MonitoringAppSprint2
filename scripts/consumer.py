# scripts/consumer.py
import os, pika

RABBIT_HOST   = os.getenv("RABBIT_HOST", "127.0.0.1")
RABBIT_PORT   = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER   = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS   = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST  = os.getenv("RABBIT_VHOST", "/")
EXCHANGE      = os.getenv("RABBIT_EXCHANGE", "order_events")

BIND_KEYS = ["order.created", "order.status.updated"]  # añade más si usas otras

def main():
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST, port=RABBIT_PORT, virtual_host=RABBIT_VHOST,
        credentials=creds, heartbeat=30, blocked_connection_timeout=10
    )
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # Cola exclusiva y autodelete para inspección
    q = ch.queue_declare(queue="", exclusive=True, auto_delete=True)
    qname = q.method.queue

    for key in BIND_KEYS:
        ch.queue_bind(exchange=EXCHANGE, queue=qname, routing_key=key)

    print(f"👂 Escuchando {BIND_KEYS} en {EXCHANGE} (cola {qname}). Ctrl+C para salir.")
    def on_msg(ch_, method, props, body):
        print(f"[x] {method.routing_key} {body.decode('utf-8')}")
        ch_.basic_ack(delivery_tag=method.delivery_tag)

    ch.basic_consume(queue=qname, on_message_callback=on_msg, auto_ack=False)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\nCerrando…")
        ch.stop_consuming()
        conn.close()

if __name__ == "__main__":
    main()
