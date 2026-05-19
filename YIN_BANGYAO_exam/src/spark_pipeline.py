from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    from_json,
    hour,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
    year,
    dayofmonth,
)
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from common import BOOTSTRAP_SERVERS, CHECKPOINT_ROOT, DATALAKE_ROOT, TOPIC


EVENT_SCHEMA = StructType(
    [
        StructField("sensor", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("unit", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("source", StringType(), False),
        StructField("anomaly", BooleanType(), False),
    ]
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("AeroSenseIoTStreamingPipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming pipeline for IoT sensor events.")
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    parser.add_argument("--datalake-root", default=str(DATALAKE_ROOT))
    parser.add_argument("--checkpoint-root", default=str(CHECKPOINT_ROOT))
    parser.add_argument("--once", action="store_true", help="Process available Kafka data and stop.")
    parser.add_argument("--processing-time", default="10 seconds", help="Streaming trigger interval when --once is not used.")
    return parser.parse_args()


def not_empty(df: DataFrame) -> bool:
    return not df.rdd.isEmpty()


def write_raw_batch(path: Path):
    def writer(batch_df: DataFrame, batch_id: int) -> None:
        if not_empty(batch_df):
            (
                batch_df.write.mode("append")
                .partitionBy("source", "topic", "year", "month", "day", "hour")
                .json(str(path))
            )

    return writer


def write_curated_batch(path: Path):
    def writer(batch_df: DataFrame, batch_id: int) -> None:
        if not_empty(batch_df):
            (
                batch_df.write.mode("append")
                .option("compression", "snappy")
                .partitionBy("domain", "sensor_type", "year", "month", "day")
                .parquet(str(path))
            )

    return writer


def write_consumption_batch(path: Path):
    def writer(batch_df: DataFrame, batch_id: int) -> None:
        if not_empty(batch_df):
            (
                batch_df.write.mode("append")
                .option("compression", "snappy")
                .partitionBy("use_case", "sensor_type", "year", "month")
                .parquet(str(path))
            )

    return writer


def main() -> int:
    args = parse_args()
    datalake_root = Path(args.datalake_root)
    checkpoint_root = Path(args.checkpoint_root)
    raw_path = datalake_root / "raw"
    curated_path = datalake_root / "curated"
    consumption_path = datalake_root / "consumption"

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    raw_df = (
        kafka_stream.select(
            col("key").cast("string").alias("message_key"),
            col("value").cast("string").alias("json_payload"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            expr("current_timestamp()").alias("ingestion_time"),
        )
        .withColumn("source", lit("kafka"))
        .withColumn("topic", lit(args.topic))
        .withColumn("year", year("ingestion_time"))
        .withColumn("month", month("ingestion_time"))
        .withColumn("day", dayofmonth("ingestion_time"))
        .withColumn("hour", hour("ingestion_time"))
    )

    parsed_df = (
        raw_df.withColumn("event", from_json(col("json_payload"), EVENT_SCHEMA))
        .filter(col("event").isNotNull())
        .select(
            col("event.sensor").alias("sensor_type"),
            col("event.value").alias("value"),
            col("event.unit").alias("unit"),
            col("event.timestamp").alias("event_timestamp_ms"),
            col("event.source").alias("reading_source"),
            col("event.anomaly").alias("producer_anomaly"),
            col("message_key"),
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("ingestion_time"),
        )
        .filter(col("sensor_type").isin("temperature", "humidity", "pressure"))
        .filter(col("value").isNotNull() & col("event_timestamp_ms").isNotNull())
        .withColumn("event_time", to_timestamp(expr("from_unixtime(cast(event_timestamp_ms / 1000 as bigint))")))
        .filter(
            ((col("sensor_type") == "temperature") & col("value").between(-50.0, 80.0))
            | ((col("sensor_type") == "humidity") & col("value").between(0.0, 100.0))
            | ((col("sensor_type") == "pressure") & col("value").between(900.0, 1100.0))
        )
        .withColumn(
            "is_anomaly",
            when((col("sensor_type") == "temperature") & (col("value") > 35.0), True)
            .when((col("sensor_type") == "humidity") & (col("value") > 90.0), True)
            .when((col("sensor_type") == "pressure") & ((col("value") < 990.0) | (col("value") > 1030.0)), True)
            .otherwise(False),
        )
    )

    curated_df = (
        parsed_df.withColumn("domain", lit("iot"))
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    aggregates_df = (
        parsed_df.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "5 minutes"), col("sensor_type"))
        .agg(
            avg("value").alias("mean_value"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
            count("*").alias("observation_count"),
            spark_sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("sensor_type"),
            col("mean_value"),
            col("min_value"),
            col("max_value"),
            col("observation_count"),
            col("anomaly_count"),
        )
        .withColumn("use_case", lit("sensor_averages"))
        .withColumn("year", year("window_start"))
        .withColumn("month", month("window_start"))
    )

    raw_writer = (
        raw_df.writeStream.foreachBatch(write_raw_batch(raw_path))
        .option("checkpointLocation", str(checkpoint_root / "raw"))
        .outputMode("append")
    )
    curated_writer = (
        curated_df.writeStream.foreachBatch(write_curated_batch(curated_path))
        .option("checkpointLocation", str(checkpoint_root / "curated"))
        .outputMode("append")
    )
    consumption_writer = (
        aggregates_df.writeStream.foreachBatch(write_consumption_batch(consumption_path))
        .option("checkpointLocation", str(checkpoint_root / "consumption"))
        .outputMode("update")
    )

    if args.once:
        raw_writer = raw_writer.trigger(availableNow=True)
        curated_writer = curated_writer.trigger(availableNow=True)
        consumption_writer = consumption_writer.trigger(availableNow=True)
    else:
        raw_writer = raw_writer.trigger(processingTime=args.processing_time)
        curated_writer = curated_writer.trigger(processingTime=args.processing_time)
        consumption_writer = consumption_writer.trigger(processingTime=args.processing_time)

    queries = [
        raw_writer.queryName("raw-zone-writer").start(),
        curated_writer.queryName("curated-zone-writer").start(),
        consumption_writer.queryName("consumption-zone-writer").start(),
    ]

    try:
        for query in queries:
            query.awaitTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
