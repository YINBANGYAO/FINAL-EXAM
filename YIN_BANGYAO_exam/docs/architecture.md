# Architecture

The AeroSense platform is an end-to-end local data engineering pipeline for IoT sensor readings.

```text
Python producer
  -> Kafka 3-broker KRaft cluster, topic sensor-events
  -> Spark Structured Streaming
     -> raw zone: original Kafka JSON
     -> curated zone: validated readings with event-time partitions
     -> consumption zone: five-minute sensor aggregates
  -> Spark SQL analytics and Flask REST API
```

Kafka provides durable ingestion with a replication factor of 3 and `min.insync.replicas=2`.
Spark parses, validates and enriches messages, then writes three data lake zones. The API exposes health, sensor metadata, latest Kafka readings, Parquet-backed statistics, anomaly lists and manual reading ingestion.
