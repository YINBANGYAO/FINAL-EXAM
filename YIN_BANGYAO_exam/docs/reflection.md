# Reflection Questions

## 1. Crash after raw write, before curated write

If the pipeline crashes after writing raw data but before writing curated data, the raw zone contains a superset of the data while the curated zone is temporarily incomplete. The impact is a consistency gap between zones: replaying from Kafka or from the raw zone is required before downstream analytics can be trusted. Separate checkpoints per sink isolate progress for each output, but true cross-sink atomicity is not guaranteed with plain file sinks. In a production design I would use idempotent batch identifiers, a transactional table format such as Delta or Iceberg, and a recovery job that reconciles raw and curated records by Kafka topic, partition and offset.

## 2. Producer scaled to 50,000 messages per second

The first bottlenecks would likely be producer network throughput, broker disk I/O, Kafka partition parallelism, and Spark micro-batch processing time. With only three partitions, Spark can only parallelize Kafka reads so far. I would increase topic partitions based on key distribution, tune producer batch size and linger time, monitor broker disk latency, and allocate more Spark executors. I would also consider compression, dedicated disks for Kafka logs, and backpressure monitoring through consumer lag.

## 3. Kafka as source of truth versus Parquet data lake

Kafka is excellent as a short-to-medium term source of truth for replayable event streams, especially when consumers need ordered, low-latency data. Its drawbacks are retention cost and query limitations for historical analytics. A Parquet data lake is better for long-term storage, compression, column pruning and SQL analytics over days or months. In this project, Kafka is the ingestion log and the Parquet lake is the analytical source of truth.

## 4. Broken sensor emitting aberrant values

The architecture detects abnormal behavior with independent Spark anomaly rules and physical plausibility validation. Values outside plausible ranges are excluded from curated data; threshold-based anomalies are retained with `is_anomaly=true`. To isolate a two-hour incident without deleting data, I would write suspicious records to a quarantine zone and keep references to topic, partition and offset. Dashboards or API filters could then exclude quarantined data by default while preserving auditability.

## 5. Adding a new CO2 sensor type

Adding `co2` requires changes in `src/common.py` for allowed sensors, units and ranges; `src/producer.py` for synthetic generation; `src/spark_pipeline.py` for validation and anomaly rules; `src/api/app.py` validation behavior through the shared constants; and documentation examples in `README.md` and `docs/architecture.md`. The data lake partition strategy already supports new sensor types because `sensor_type` is a dynamic partition column.
