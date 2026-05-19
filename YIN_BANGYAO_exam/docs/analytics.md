# Analytics Notes

The analytics script was run after producing 300 Kafka messages and processing them through Spark Structured Streaming.

## Query 1 - Top anomaly hours

```text
hour_bucket       anomaly_count
2026-05-19 00:00 36
```

All generated events were produced during the same hour, so the top anomaly hour contains all 36 detected anomalies.

## Query 2 - Statistics by sensor type

```text
sensor_type  mean     min     max      stddev  anomaly_rate_pct
humidity     62.86    30.48   94.37    19.30   12.37
pressure     1008.85  982.12  1038.27  13.67   10.68
temperature  26.40    15.06   45.00    7.44    13.00
```

The anomaly rates are coherent with the producer configuration, which enforced a 12% global anomaly ratio.

## Query 3 - Daily temperature evolution

```text
event_day    mean_temperature  anomaly_count
2026-05-19   26.40             13
```

The temperature stream contains 100 readings, 13 of which were independently detected as anomalies by the Spark rule `temperature > 35`.

## Query 4 - Partition pruning

```text
query_type        row_count  filter                        execution_seconds  speedup
full_scan         300        none                          0.1193             n/a
partition_pruned  97         humidity/year=2026/month=5/day=19 0.0963         1.24
```

The filtered query is faster on this small local dataset because Spark can prune the `sensor_type`, `year`, `month` and `day` partition directories. The speedup is modest because the dataset has only 300 rows; it would become more significant with more days and more sensors.

CSV outputs are stored under `outputs/analytics/`.
