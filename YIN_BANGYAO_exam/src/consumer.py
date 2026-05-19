from __future__ import annotations

import argparse
import json

from kafka import KafkaConsumer

from common import BOOTSTRAP_SERVERS, TOPIC, bootstrap_servers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simple Kafka consumer for smoke tests.")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--max-messages", type=int, default=10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=bootstrap_servers(args.bootstrap_servers),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
    )

    count = 0
    for record in consumer:
        print(
            f"partition={record.partition} offset={record.offset} key={record.key} "
            f"value={json.dumps(record.value, sort_keys=True)}"
        )
        count += 1
        if count >= args.max_messages:
            break
    consumer.close()
    print(f"Read {count} messages.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
