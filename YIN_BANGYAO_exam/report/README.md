# AeroSense IoT Data Engineering Platform

Student: YIN BANGYAO  
Student ID: 20252190  
School e-mail: bangyao.yin@efrei.net  
https://github.com/YINBANGYAO/FINAL-EXAM

## 1. Overview

This project implements a local end-to-end data engineering platform for AeroSense IoT sensor data. It covers event generation, reliable Kafka ingestion, Spark Structured Streaming processing, a three-zone Parquet/JSON data lake, Spark SQL analytics and a Flask REST API.

Technologies used:

- Docker Compose and Confluent Kafka 7.5 in KRaft mode.
- Python 3.9+ with `kafka-python-ng`.
- PySpark 3.5.3 for streaming and analytical processing.
- Flask 3.0 for REST exposure.
- Local JSON and Parquet files for the data lake.

## 2. Architecture

```text
+-------------------+      +----------------------------+
| Python producer   | ---> | Kafka topic sensor-events  |
+-------------------+      | 3 partitions, RF=3, ISR=2 |
                           +-------------+--------------+
                                         |
                                         v
                           +----------------------------+
                           | Spark Structured Streaming |
                           | parse, validate, anomaly   |
                           | 5-minute window averages   |
                           +-------------+--------------+
                                         |
                +------------------------+-------------------------+
                v                        v                         v
       raw JSON zone              curated Parquet          consumption Parquet
       ingestion time             event time partitions    aggregate partitions
                \                        |                         /
                 \                       v                        /
                  +---------- Spark SQL analytics + Flask API ----+
```

## 3. Prerequisites

- Docker Desktop with Docker Compose v2.
- Python 3.9 or later.
- Java compatible with PySpark.

Install Python dependencies:

```bash
python -m pip install -r requirements.txt
```

For a local Linux/macOS/WSL reviewer, the default data lake path is `/tmp/datalake`. On Windows PowerShell, the following local project paths are convenient:

```powershell
$env:DATALAKE_ROOT = "$PWD\datalake"
$env:CHECKPOINT_ROOT = "$PWD\checkpoints"
```

## 4. Execution

Start Kafka:

```bash
docker compose up -d
```

Create the topic:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --if-not-exists --topic sensor-events --partitions 3 --replication-factor 3
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic sensor-events
```

Generate data:

```bash
python src/producer.py --count 300 --rate 30 --source site-A-rack-12
```

Run the Spark pipeline once over available Kafka data:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/spark_pipeline.py --once
```

Run analytics:

```bash
python src/analytics.py --datalake-root ./datalake --output-dir outputs/analytics
```

Start the API:

```bash
python src/api/app.py
```

Test the API:

```bash
bash tests/test_curl_commands.sh
```

## 5. Technical Choices

### Curated partitioning

The curated zone is partitioned by `domain`, `sensor_type`, `year`, `month` and `day`. `sensor_type` is the most common analytical filter and keeps temperature, humidity and pressure scans independent. The date columns are based on event time, not ingestion time, so late events still land in the correct business day. I considered partitioning by source, but source has higher cardinality and would create many small files.

### Spark output mode

Raw and curated writes use append semantics because each Kafka message becomes an immutable data lake record. The consumption aggregation uses update mode with `foreachBatch` because five-minute windows can be revised while the watermark is still open. This is practical for local Parquet output, but it is not fully transactional; a production version would use Delta Lake or Iceberg for idempotent upserts.

### Kafka replication and ISR

The Kafka topic uses replication factor 3 and `min.insync.replicas=2`. This allows the cluster to tolerate one broker failure while still requiring at least two replicas to acknowledge writes when producers use `acks=all`. The alternative, replication factor 1, would be faster but would lose the fault-tolerance property required by the scenario.

### Event time versus ingestion time

The raw zone is partitioned by ingestion time because it represents what Kafka delivered to the platform and is useful for operational replay. Curated and consumption zones use event time because analytics should reflect when the sensor reading happened. Keeping both timestamps makes delay and late-arrival behavior auditable.

### Delivery semantics

The intended end-to-end semantics are at-least-once. Kafka producers use `acks=all`, retries and one in-flight request per connection to preserve ordering per sensor key. Spark checkpoints track stream progress per sink. Because plain Parquet writes are not transactional across the three sinks, exactly-once is not guaranteed for every failure scenario. The design limits duplicate risk by keeping Kafka partition and offset in curated records.

## 6. Results

The local validation run produced:

- 300 Kafka events, including 36 anomalies (12.0%).
- 300 curated Parquet readings and 3 five-minute aggregate rows.
- Spark SQL CSV outputs in `outputs/analytics/`.
- Kafka UI, API, data lake and analytics screenshots in `outputs/screenshots/`.
- A full evidence report in `report/YIN_BANGYAO_exam_report.docx`.

## 7. Limitations and Improvements

- The local Parquet data lake does not provide ACID transactions.
- API reads Parquet files directly with pandas, which is acceptable for the assessment but not for large production datasets.
- Kafka latest-read lookup scans a bounded recent offset range.
- A production version should add Schema Registry, authentication, metrics, alerting and a table format such as Delta Lake or Iceberg.
