from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any


TOPIC = os.getenv("KAFKA_TOPIC", "sensor-events")
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9094,localhost:9096",
)
DATALAKE_ROOT = Path(os.getenv("DATALAKE_ROOT", "/tmp/datalake"))
CHECKPOINT_ROOT = Path(os.getenv("CHECKPOINT_ROOT", "/tmp/sensor_checkpoints"))

SENSORS = ("temperature", "humidity", "pressure")
UNITS = {"temperature": "C", "humidity": "%", "pressure": "hPa"}

NORMAL_RANGES = {
    "temperature": (15.0, 35.0),
    "humidity": (30.0, 90.0),
    "pressure": (990.0, 1030.0),
}

REALISTIC_RANGES = {
    "temperature": (15.0, 45.0),
    "humidity": (30.0, 95.0),
    "pressure": (980.0, 1040.0),
}

PLAUSIBLE_RANGES = {
    "temperature": (-50.0, 80.0),
    "humidity": (0.0, 100.0),
    "pressure": (900.0, 1100.0),
}


def bootstrap_servers(value: str | None = None) -> list[str]:
    raw = value or BOOTSTRAP_SERVERS
    return [server.strip() for server in raw.split(",") if server.strip()]


def epoch_ms() -> int:
    return int(time.time() * 1000)


def compute_anomaly(sensor: str, value: float) -> bool:
    if sensor == "temperature":
        return value > 35.0
    if sensor == "humidity":
        return value > 90.0
    if sensor == "pressure":
        return value < 990.0 or value > 1030.0
    return False


def is_plausible(sensor: str, value: float) -> bool:
    if sensor not in PLAUSIBLE_RANGES:
        return False
    low, high = PLAUSIBLE_RANGES[sensor]
    return low <= value <= high


def validate_reading_payload(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, int]:
    required = {"sensor", "value", "unit", "source"}
    missing = sorted(required - set(payload))
    if missing:
        return None, f"Missing required field(s): {', '.join(missing)}", 400

    sensor = str(payload["sensor"]).strip().lower()
    if sensor not in SENSORS:
        return None, f"Unsupported sensor type: {sensor}", 422

    try:
        value = float(payload["value"])
    except (TypeError, ValueError):
        return None, "Field 'value' must be numeric", 400

    expected_unit = UNITS[sensor]
    if str(payload["unit"]) != expected_unit:
        return None, f"Invalid unit for {sensor}. Expected {expected_unit}", 422

    if not is_plausible(sensor, value):
        low, high = PLAUSIBLE_RANGES[sensor]
        return None, f"Value for {sensor} must be between {low} and {high}", 422

    timestamp = payload.get("timestamp", epoch_ms())
    try:
        timestamp = int(timestamp)
    except (TypeError, ValueError):
        return None, "Field 'timestamp' must be an epoch millisecond integer", 400

    event = {
        "sensor": sensor,
        "value": round(value, 2),
        "unit": expected_unit,
        "timestamp": timestamp,
        "source": str(payload["source"]).strip(),
        "anomaly": bool(payload.get("anomaly", compute_anomaly(sensor, value))),
    }
    return event, None, 200
