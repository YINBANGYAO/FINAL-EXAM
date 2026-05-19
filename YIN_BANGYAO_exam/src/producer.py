from __future__ import annotations

import argparse
import math
import json
import random
import signal
import sys
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common import (
    BOOTSTRAP_SERVERS,
    NORMAL_RANGES,
    REALISTIC_RANGES,
    SENSORS,
    TOPIC,
    UNITS,
    bootstrap_servers,
    compute_anomaly,
    epoch_ms,
)


STOP_REQUESTED = False


def handle_stop(signum: int, frame: Any) -> None:
    global STOP_REQUESTED
    STOP_REQUESTED = True


def build_event(source: str, anomaly_ratio: float, force_anomaly: bool | None = None) -> dict[str, Any]:
    sensor = random.choice(SENSORS)
    make_anomaly = force_anomaly if force_anomaly is not None else random.random() < anomaly_ratio

    if make_anomaly:
        if sensor == "temperature":
            low, high = 35.5, REALISTIC_RANGES[sensor][1]
        elif sensor == "humidity":
            low, high = 90.5, REALISTIC_RANGES[sensor][1]
        else:
            low, high = random.choice([(REALISTIC_RANGES[sensor][0], 989.5), (1030.5, REALISTIC_RANGES[sensor][1])])
        value = random.uniform(low, high)
    else:
        low, high = NORMAL_RANGES[sensor]
        value = random.uniform(low, high)

    rounded = round(value, 2)
    return {
        "sensor": sensor,
        "value": rounded,
        "unit": UNITS[sensor],
        "timestamp": epoch_ms(),
        "source": source,
        "anomaly": compute_anomaly(sensor, rounded),
    }


def create_producer(servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers(servers),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=32768,
        request_timeout_ms=30000,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce IoT sensor readings to Kafka.")
    parser.add_argument("--count", type=int, default=100, help="Number of events to publish.")
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second.")
    parser.add_argument("--source", default="site-A-rack-12", help="Sensor site identifier.")
    parser.add_argument("--topic", default=TOPIC, help="Kafka topic name.")
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS, help="Comma-separated Kafka bootstrap servers.")
    parser.add_argument("--anomaly-ratio", type=float, default=0.12, help="Target anomaly ratio, minimum 0.10.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.count < 1:
        print("--count must be positive", file=sys.stderr)
        return 2
    if args.rate <= 0:
        print("--rate must be positive", file=sys.stderr)
        return 2
    if args.anomaly_ratio < 0.10:
        print("--anomaly-ratio must be at least 0.10 for this assessment", file=sys.stderr)
        return 2

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    producer = create_producer(args.bootstrap_servers)
    delay = 1.0 / args.rate
    sent = 0
    anomalies = 0
    anomaly_target = max(math.ceil(args.count * args.anomaly_ratio), math.ceil(args.count * 0.10))
    anomaly_indices = set(random.sample(range(args.count), anomaly_target))

    try:
        for index in range(args.count):
            if STOP_REQUESTED:
                break
            event = build_event(args.source, args.anomaly_ratio, force_anomaly=index in anomaly_indices)
            future = producer.send(args.topic, key=event["sensor"], value=event)
            try:
                metadata = future.get(timeout=10)
                sent += 1
                anomalies += int(event["anomaly"])
                print(
                    f"sent sensor={event['sensor']} partition={metadata.partition} "
                    f"offset={metadata.offset} anomaly={event['anomaly']} value={event['value']}"
                )
            except KafkaError as exc:
                print(f"Failed to publish event: {exc}", file=sys.stderr)
            time.sleep(delay)
    finally:
        producer.flush(timeout=30)
        producer.close(timeout=30)

    ratio = anomalies / sent if sent else 0.0
    print(f"Produced {sent} events to {args.topic}; anomalies={anomalies} ({ratio:.1%}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
