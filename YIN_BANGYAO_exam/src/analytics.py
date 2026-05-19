from __future__ import annotations

import argparse
import shutil
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from common import DATALAKE_ROOT


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Spark SQL analytics on the local data lake.")
    parser.add_argument("--datalake-root", default=str(DATALAKE_ROOT))
    parser.add_argument("--output-dir", default="outputs/analytics")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("AeroSenseIoTAnalytics")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "6")
        .getOrCreate()
    )


def save_csv(df: DataFrame, output_dir: Path, name: str) -> None:
    target = output_dir / name
    if target.exists():
        shutil.rmtree(target)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(target))


def show_and_save(df: DataFrame, output_dir: Path, name: str, title: str) -> None:
    print(f"\n=== {title} ===")
    df.show(truncate=False)
    save_csv(df, output_dir, name)


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    curated_path = Path(args.datalake_root) / "curated"

    curated = spark.read.parquet(str(curated_path))
    curated.createOrReplaceTempView("curated_readings")

    top_anomaly_hours = spark.sql(
        """
        SELECT
          date_format(event_time, 'yyyy-MM-dd HH:00') AS hour_bucket,
          COUNT(*) AS anomaly_count
        FROM curated_readings
        WHERE is_anomaly = true
        GROUP BY date_format(event_time, 'yyyy-MM-dd HH:00')
        ORDER BY anomaly_count DESC, hour_bucket ASC
        LIMIT 5
        """
    )
    show_and_save(top_anomaly_hours, output_dir, "top_anomaly_hours", "Top 5 hours by anomaly count")

    sensor_statistics = spark.sql(
        """
        SELECT
          sensor_type,
          ROUND(AVG(value), 2) AS global_mean,
          ROUND(MIN(value), 2) AS min_value,
          ROUND(MAX(value), 2) AS max_value,
          ROUND(STDDEV(value), 2) AS stddev_value,
          ROUND(100.0 * SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / COUNT(*), 2) AS anomaly_rate_pct
        FROM curated_readings
        GROUP BY sensor_type
        ORDER BY sensor_type
        """
    )
    show_and_save(sensor_statistics, output_dir, "sensor_statistics", "Statistics by sensor type")

    temperature_daily = spark.sql(
        """
        SELECT
          date_format(event_time, 'yyyy-MM-dd') AS event_day,
          ROUND(AVG(value), 2) AS mean_temperature,
          SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count
        FROM curated_readings
        WHERE sensor_type = 'temperature'
        GROUP BY date_format(event_time, 'yyyy-MM-dd')
        ORDER BY event_day
        """
    )
    show_and_save(temperature_daily, output_dir, "temperature_daily_evolution", "Daily temperature evolution")

    first_partition = (
        curated.select("sensor_type", "year", "month", "day")
        .dropDuplicates()
        .orderBy("sensor_type", "year", "month", "day")
        .limit(1)
        .collect()
    )
    if first_partition:
        row = first_partition[0]
        start = time.perf_counter()
        total_count = curated.count()
        full_scan_seconds = time.perf_counter() - start

        start = time.perf_counter()
        pruned_count = curated.where(
            (col("sensor_type") == row["sensor_type"])
            & (col("year") == row["year"])
            & (col("month") == row["month"])
            & (col("day") == row["day"])
        ).count()
        pruned_seconds = time.perf_counter() - start
        speedup = full_scan_seconds / pruned_seconds if pruned_seconds > 0 else 0.0

        pruning_rows = [
            (
                "full_scan",
                str(total_count),
                "",
                "",
                "",
                "",
                round(full_scan_seconds, 4),
                "",
            ),
            (
                "partition_pruned",
                str(pruned_count),
                row["sensor_type"],
                str(row["year"]),
                str(row["month"]),
                str(row["day"]),
                round(pruned_seconds, 4),
                round(speedup, 2),
            ),
        ]
        pruning_df = spark.createDataFrame(
            pruning_rows,
            [
                "query_type",
                "row_count",
                "sensor_type_filter",
                "year_filter",
                "month_filter",
                "day_filter",
                "execution_seconds",
                "speedup_factor",
            ],
        )
        show_and_save(pruning_df, output_dir, "partition_pruning", "Partition pruning demonstration")
    else:
        print("No partitions found for pruning demonstration.")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
