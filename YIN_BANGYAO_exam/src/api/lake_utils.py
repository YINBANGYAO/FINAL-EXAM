from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import DATALAKE_ROOT


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except (FileNotFoundError, ValueError, OSError):
        return pd.DataFrame()


def _to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.replace({pd.NaT: None}).to_dict(orient="records")
    for row in records:
        for key, value in list(row.items()):
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif hasattr(value, "item"):
                row[key] = value.item()
    return records


def daily_stats(sensor_type: str, days: int, root: Path | None = None) -> list[dict[str, Any]]:
    datalake_root = root or DATALAKE_ROOT
    df = _read_parquet(datalake_root / "curated")
    if df.empty:
        return []

    df = df[df["sensor_type"] == sensor_type].copy()
    if df.empty:
        return []

    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    df = df[df["event_time"] >= cutoff]
    if df.empty:
        return []

    df["event_day"] = df["event_time"].dt.strftime("%Y-%m-%d")
    grouped = (
        df.groupby("event_day")
        .agg(
            mean_value=("value", "mean"),
            min_value=("value", "min"),
            max_value=("value", "max"),
            observation_count=("value", "count"),
            anomaly_count=("is_anomaly", "sum"),
        )
        .reset_index()
        .sort_values("event_day")
    )
    grouped["mean_value"] = grouped["mean_value"].round(2)
    grouped["min_value"] = grouped["min_value"].round(2)
    grouped["max_value"] = grouped["max_value"].round(2)
    return _to_records(grouped)


def recent_anomalies(sensor_type: str | None, limit: int, root: Path | None = None) -> list[dict[str, Any]]:
    datalake_root = root or DATALAKE_ROOT
    df = _read_parquet(datalake_root / "curated")
    if df.empty:
        return []

    df = df[df["is_anomaly"] == True].copy()
    if sensor_type:
        df = df[df["sensor_type"] == sensor_type]
    if df.empty:
        return []

    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df = df.sort_values("event_time", ascending=False).head(limit)
    selected = df[["sensor_type", "value", "unit", "event_time", "reading_source", "producer_anomaly", "is_anomaly"]]
    return _to_records(selected)
