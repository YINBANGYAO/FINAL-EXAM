from __future__ import annotations

import json
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import BOOTSTRAP_SERVERS, TOPIC, bootstrap_servers


def producer(servers: str | None = None) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers(servers or BOOTSTRAP_SERVERS),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=32768,
    )


def publish_reading(event: dict[str, Any], topic: str = TOPIC, servers: str | None = None) -> dict[str, Any]:
    client = producer(servers)
    try:
        metadata = client.send(topic, key=event["sensor"], value=event).get(timeout=10)
        client.flush(timeout=10)
        return {"topic": metadata.topic, "partition": metadata.partition, "offset": metadata.offset}
    finally:
        client.close(timeout=10)


def latest_reading(sensor_type: str, topic: str = TOPIC, servers: str | None = None, lookback: int = 100) -> dict[str, Any] | None:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers(servers or BOOTSTRAP_SERVERS),
        enable_auto_commit=False,
        consumer_timeout_ms=1500,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        if not partitions:
            return None
        topic_partitions = [TopicPartition(topic, partition) for partition in sorted(partitions)]
        consumer.assign(topic_partitions)
        consumer.seek_to_end(*topic_partitions)

        candidates: list[dict[str, Any]] = []
        for topic_partition in topic_partitions:
            end_offset = consumer.position(topic_partition)
            start_offset = max(0, end_offset - lookback)
            if start_offset >= end_offset:
                continue
            consumer.seek(topic_partition, start_offset)
            records = consumer.poll(timeout_ms=1200, max_records=lookback)
            for batch in records.values():
                for record in batch:
                    value = record.value
                    if value.get("sensor") == sensor_type:
                        value["_kafka"] = {"partition": record.partition, "offset": record.offset}
                        candidates.append(value)
        if not candidates:
            return None
        return max(candidates, key=lambda item: item.get("timestamp", 0))
    finally:
        consumer.close()
