#!/usr/bin/env python3
"""Small helper to publish demo events into the Kafka broker in the cluster."""
import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError as exc:  # pragma: no cover - runtime dependency hint
    sys.stderr.write(
        "Missing dependency: kafka-python. Install it with 'pip install kafka-python'\n"
    )
    raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish demo messages to a Kafka topic from inside the cluster.",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic to publish to (must already exist)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        help="Kafka bootstrap servers. Defaults to $KAFKA_BOOTSTRAP_SERVERS.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages to send (0 for infinite).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.5,
        help="Delay in seconds between sends.",
    )
    parser.add_argument(
        "--message",
        default="Hello from the WolveKafka demo!",
        help="Base message to include in the payload.",
    )
    return parser.parse_args()


def build_payload(base_message: str, sequence: int) -> dict:
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    return {
        "demo_id": str(uuid.uuid4()),
        "sequence": sequence,
        "message": base_message,
        "timestamp": now,
    }


def main() -> int:
    args = parse_args()

    if not args.bootstrap_servers:
        sys.stderr.write("Bootstrap servers missing. Pass --bootstrap-servers.\n")
        return 2

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    send_forever = args.count == 0
    sent = 0
    try:
        while send_forever or sent < args.count:
            payload = build_payload(args.message, sent + 1)
            future = producer.send(args.topic, payload)
            metadata = future.get(timeout=10)
            print(
                f"Produced {payload['demo_id']} seq={payload['sequence']} "
                f"partition={metadata.partition} offset={metadata.offset}"
            )
            sent += 1
            if args.interval > 0 and (send_forever or sent < args.count):
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Interrupted, flushing events...")
    except KafkaError as exc:
        sys.stderr.write(f"Failed to publish message: {exc}\n")
        return 1
    finally:
        producer.flush()
        producer.close()

    print(f"Done. Published {sent} message(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
