"""Utilities for generating synthetic traffic against the order service.

The script keeps its original behaviour (publishing order events to RabbitMQ)
while optionally issuing concurrent HTTP requests to the Django API. Two knobs
control the HTTP side:

* HTTP_BASE_URL: base URL of the API, e.g. http://54.159.43.195:8080
* HTTP_PATHS: comma separated list of METHOD:/path entries. The default keeps it
  empty, but a practical example is:
      HTTP_PATHS="POST:/orders,PUT:/orders/{order_id}/status,GET:/variables/"

Any appearance of {order_id} will be substituted by a random live order tracked
by the generator. If none are available yet the request is skipped. This allows
mixing order-specific calls with general navigation (home page, measurements,
etc.) while the RabbitMQ traffic keeps flowing.
"""
from __future__ import annotations

import json
import os
import random
import string
import threading
import time
from typing import Dict, List, Tuple

import pika
import requests
from urllib.parse import urljoin

RABBIT_HOST = os.getenv("RABBIT_HOST")
RABBIT_PORT = int(os.getenv("RABBIT_PORT", "5672"))
RABBIT_USER = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS = os.getenv("RABBIT_PASS", "guest")
RABBIT_VHOST = os.getenv("RABBIT_VHOST", "/")
EXCHANGE = os.getenv("RABBIT_EXCHANGE", "order_events")
EVENT_RATE = float(os.getenv("EVENTS_RATE", "2"))

HTTP_BASE_URL = os.getenv("HTTP_BASE_URL")
HTTP_PATHS_RAW = os.getenv("HTTP_PATHS", "").strip()
HTTP_WORKERS = int(os.getenv("HTTP_WORKERS", "2"))
HTTP_DELAY = float(os.getenv("HTTP_SLEEP", "0.3"))

STATUSES_FLOW = {
    "CREATED": ["UPDATED", "CANCELLED", "SHIPPED"],
    "UPDATED": ["SHIPPED", "CANCELLED"],
    "SHIPPED": ["DELIVERED"],
    "DELIVERED": [],
    "CANCELLED": [],
}


def _parse_http_paths(raw: str) -> List[Tuple[str, str]]:
    paths: List[Tuple[str, str]] = []
    if not raw:
        return paths
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if ":" in entry:
            method, path = entry.split(":", 1)
        else:
            method, path = "GET", entry
        paths.append((method.upper(), path if path.startswith("/") else f"/{path}"))
    return paths


HTTP_PATHS = _parse_http_paths(HTTP_PATHS_RAW)


def rand_order_id(prefix: str = "ORD", length: int = 6) -> str:
    return f"{prefix}-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def publish(channel: pika.adapters.blocking_connection.BlockingChannel, routing_key: str, payload: Dict) -> None:
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(content_type="application/json", delivery_mode=2),
    )


def http_worker(name: str, live_orders: Dict[str, Dict[str, int]], stop: threading.Event) -> None:
    if not HTTP_BASE_URL or not HTTP_PATHS:
        return

    session = requests.Session()
    while not stop.is_set():
        method, path = random.choice(HTTP_PATHS)
        url = urljoin(HTTP_BASE_URL, path)

        if "{order_id}" in url:
            with LIVE_LOCK:
                order_ids = list(live_orders.keys())
            if not order_ids:
                time.sleep(HTTP_DELAY)
                continue
            order_id = random.choice(order_ids)
            url = url.replace("{order_id}", order_id)

        try:
            if method == "GET":
                session.get(url, timeout=5)
            elif method == "POST":
                payload = {"id": rand_order_id(), "status": "CREATED"}
                session.post(url, json=payload, timeout=5)
            elif method in {"PUT", "PATCH"}:
                body = {"status": random.choice(["UPDATED", "SHIPPED", "CANCELLED"])}
                session.request(method, url, json=body, timeout=5)
            else:
                session.request(method, url, timeout=5)
        except Exception as exc:  # noqa: BLE001
            print(f"[http:{name}] error calling {method} {url}: {exc}")
        finally:
            time.sleep(HTTP_DELAY)


LIVE_LOCK = threading.Lock()


def main(rate_per_sec: float = EVENT_RATE) -> None:
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host=RABBIT_VHOST,
        credentials=creds,
        heartbeat=30,
        blocked_connection_timeout=10,
        connection_attempts=5,
        retry_delay=2.0,
    )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    live_orders: Dict[str, Dict[str, int]] = {}
    stop_http = threading.Event()
    http_threads: List[threading.Thread] = []

    if HTTP_BASE_URL and HTTP_PATHS:
        for idx in range(HTTP_WORKERS):
            thread = threading.Thread(
                target=http_worker,
                name=f"http-{idx}",
                args=(f"w{idx}", live_orders, stop_http),
                daemon=True,
            )
            thread.start()
            http_threads.append(thread)
        print(f"[info] HTTP workers active against {HTTP_BASE_URL} with {len(HTTP_PATHS)} paths")

    sleep_time = 1.0 / rate_per_sec if rate_per_sec > 0 else 0.5
    print(f"[info] Publishing to {EXCHANGE} at {RABBIT_HOST}:{RABBIT_PORT} ({rate_per_sec:.1f} ev/s). Ctrl+C to stop.")

    counter = 0
    try:
        while True:
            counter += 1
            with LIVE_LOCK:
                has_orders = bool(live_orders)

            should_create = not has_orders or random.random() < 0.5
            if should_create:
                order_id = rand_order_id()
                payload = {"order_id": order_id, "status": "CREATED"}
                publish(channel, "order.created", payload)
                with LIVE_LOCK:
                    live_orders[order_id] = {"status": "CREATED", "version": 0}
                print(f"[{counter:05}] created -> {payload}")
            else:
                with LIVE_LOCK:
                    order_id = random.choice(list(live_orders.keys()))
                    status = live_orders[order_id]["status"]
                    version = live_orders[order_id]["version"]
                next_statuses = STATUSES_FLOW.get(status, [])
                if not next_statuses:
                    time.sleep(sleep_time)
                    continue
                new_status = random.choice(next_statuses)
                payload = {
                    "order_id": order_id,
                    "new_status": new_status,
                    "version": version + 1,
                }
                publish(channel, "order.status.updated", payload)
                with LIVE_LOCK:
                    live_orders[order_id]["status"] = new_status
                    live_orders[order_id]["version"] = version + 1
                print(f"[{counter:05}] updated -> {payload}")

            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\n[info] Stopped by user")
    finally:
        stop_http.set()
        for thread in http_threads:
            thread.join(timeout=1.0)
        connection.close()


if __name__ == "__main__":
    main()
