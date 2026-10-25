"""Order Producer: publishes order messages (JSON) to the RabbitMQ 'orders' queue."""

import argparse
import json
import os
import random
import sys
import uuid

import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "orders"


def make_order(event_id=None):
    return {
        "event_id": event_id or f"event-{uuid.uuid4()}",
        "order_id": f"order-{uuid.uuid4()}",
        "item_id": f"sku-{random.randint(100, 999)}",
        "quantity": random.randint(1, 5),
    }


def main():
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ.")
    parser.add_argument("--count", type=int, default=6,
                        help="number of orders to publish (default: 6)")
    parser.add_argument("--event-id",
                        help="use this exact event_id instead of a random UUID")
    parser.add_argument("--duplicate-demo", action="store_true",
                        help="publish the SAME order (same event_id) twice")
    args = parser.parse_args()

    if args.duplicate_demo:
        order = make_order(args.event_id or "duplicate-demo-001")
        orders = [order, dict(order)]
    else:
        orders = [make_order(args.event_id) for _ in range(args.count)]

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    except (pika.exceptions.AMQPConnectionError, OSError):
        print(f"ERROR: could not connect to RabbitMQ at {RABBITMQ_HOST}:5672. "
              "Is RabbitMQ running?", file=sys.stderr)
        sys.exit(1)

    channel = connection.channel()
    # Durable queue, declared by both producer and worker so either can start first.
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for order in orders:
        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(order),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
            ),
        )
        print(f"producer event=published event_id={order['event_id']} "
              f"order_id={order['order_id']} body={json.dumps(order)}", flush=True)

    connection.close()
    print(f"producer event=done published={len(orders)} queue={QUEUE_NAME}", flush=True)


if __name__ == "__main__":
    main()
