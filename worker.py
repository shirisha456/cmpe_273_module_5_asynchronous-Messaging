"""Order Worker: consumes order messages from the RabbitMQ 'orders' queue.

Flow for each message:
    receive -> check event_id -> (new) process -> record event_id -> ACK
                              -> (duplicate) skip business action -> ACK
"""

import argparse
import fcntl
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
QUEUE_NAME = "orders"
# Shared by all worker processes on this machine (one event_id per line).
PROCESSED_FILE = Path(__file__).resolve().parent / "processed_events.txt"
REQUIRED_FIELDS = ("event_id", "order_id", "item_id", "quantity")


def log(worker, event, **fields):
    extra = " ".join(f"{k}={v}" for k, v in fields.items())
    now = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{now} worker={worker} event={event} {extra}".rstrip(), flush=True)


def process_order(worker, order):
    """Simulated business action."""
    log(worker, "processing", event_id=order["event_id"], order_id=order["order_id"],
        item_id=order["item_id"], quantity=order["quantity"])
    time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="Consume order messages from RabbitMQ.")
    parser.add_argument("--name", required=True, help="worker name, e.g. worker-1")
    parser.add_argument("--crash-after-processing-once", action="store_true",
                        help="TEST MODE: after processing the first NEW message and "
                             "recording its event_id, exit immediately WITHOUT sending the ACK")
    args = parser.parse_args()
    name = args.name

    def on_message(channel, method, properties, body):
        try:
            order = json.loads(body)
            if not all(field in order for field in REQUIRED_FIELDS):
                raise ValueError("missing required fields")
        except ValueError as exc:
            # Malformed messages can never succeed, so drop them instead of redelivering forever.
            log(name, "rejected_malformed", reason=repr(str(exc)), body=body[:100])
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        event_id = order["event_id"]
        log(name, "received", event_id=event_id, order_id=order["order_id"],
            redelivered=method.redelivered, delivery_tag=method.delivery_tag)

        # Exclusive file lock: check + process + record happen atomically across
        # worker processes, so two workers can never both process the same event_id.
        with open(PROCESSED_FILE, "a+") as store:
            fcntl.flock(store, fcntl.LOCK_EX)
            store.seek(0)
            already_processed = event_id in {line.strip() for line in store}

            if already_processed:
                log(name, "duplicate_detected", event_id=event_id,
                    action="business_action_skipped")
            else:
                process_order(name, order)
                store.write(event_id + "\n")
                store.flush()
                os.fsync(store.fileno())
                log(name, "processed", event_id=event_id, order_id=order["order_id"],
                    recorded_in=PROCESSED_FILE.name)

                if args.crash_after_processing_once:
                    log(name, "intentionally_crashing_before_ack", event_id=event_id,
                        delivery_tag=method.delivery_tag)
                    # os._exit kills the process immediately: no ACK, no exception
                    # handlers, no clean AMQP close. The OS closes the TCP socket and
                    # RabbitMQ requeues the unacknowledged message.
                    os._exit(1)

        # ACK only after processing (or duplicate detection) has finished.
        channel.basic_ack(delivery_tag=method.delivery_tag)
        log(name, "acked", event_id=event_id, delivery_tag=method.delivery_tag)

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    except (pika.exceptions.AMQPConnectionError, OSError):
        print(f"ERROR: could not connect to RabbitMQ at {RABBITMQ_HOST}:5672. "
              "Is RabbitMQ running?", file=sys.stderr)
        sys.exit(1)

    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # At most one unacknowledged message per worker -> fair distribution between workers.
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)

    mode = "crash-after-processing-once" if args.crash_after_processing_once else "normal"
    log(name, "started", queue=QUEUE_NAME, host=RABBITMQ_HOST, mode=mode)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log(name, "stopping")
        channel.stop_consuming()
        connection.close()
    except pika.exceptions.AMQPConnectionError:
        print("ERROR: lost connection to RabbitMQ.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
