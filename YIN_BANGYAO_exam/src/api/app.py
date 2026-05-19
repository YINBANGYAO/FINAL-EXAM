from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from flask import Flask, jsonify, request

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common import DATALAKE_ROOT, SENSORS, TOPIC, UNITS, validate_reading_payload
from kafka_utils import latest_reading, publish_reading
from lake_utils import daily_stats, recent_anomalies


def create_app() -> Flask:
    app = Flask(__name__)
    app.logger.setLevel(logging.INFO)

    def success(data, status_code: int = 200):
        return jsonify({"status": "success", "data": data, "error": None}), status_code

    def error(message: str, status_code: int):
        app.logger.warning("API error %s: %s", status_code, message)
        return jsonify({"status": "error", "data": None, "error": {"code": status_code, "message": message}}), status_code

    @app.get("/api/v1/health")
    def health():
        return success(
            {
                "service": "aerosense-api",
                "status": "ok",
                "topic": TOPIC,
                "datalake_root": str(DATALAKE_ROOT),
            }
        )

    @app.get("/api/v1/sensors")
    def sensors():
        return success([{"sensor": sensor, "unit": UNITS[sensor]} for sensor in SENSORS])

    @app.get("/api/v1/sensors/<sensor_type>/latest")
    def latest(sensor_type: str):
        sensor_type = sensor_type.lower()
        if sensor_type not in SENSORS:
            return error(f"Unsupported sensor type: {sensor_type}", 400)
        try:
            event = latest_reading(sensor_type)
        except Exception as exc:
            app.logger.exception("Kafka latest read failed")
            return error(f"Kafka read failed: {exc}", 500)
        if event is None:
            return error(f"No Kafka reading found for {sensor_type}", 404)
        return success(event)

    @app.get("/api/v1/sensors/<sensor_type>/stats")
    def stats(sensor_type: str):
        sensor_type = sensor_type.lower()
        if sensor_type not in SENSORS:
            return error(f"Unsupported sensor type: {sensor_type}", 400)
        try:
            days = int(request.args.get("days", "7"))
        except ValueError:
            return error("Query parameter 'days' must be an integer", 400)
        if not 1 <= days <= 90:
            return error("Query parameter 'days' must be between 1 and 90", 400)
        data = daily_stats(sensor_type, days)
        if not data:
            return error(f"No data lake stats found for {sensor_type}", 404)
        return success(data)

    @app.get("/api/v1/anomalies")
    def anomalies():
        sensor_type = request.args.get("sensor")
        if sensor_type:
            sensor_type = sensor_type.lower()
            if sensor_type not in SENSORS:
                return error(f"Unsupported sensor type: {sensor_type}", 400)
        try:
            limit = int(request.args.get("limit", "20"))
        except ValueError:
            return error("Query parameter 'limit' must be an integer", 400)
        if not 1 <= limit <= 500:
            return error("Query parameter 'limit' must be between 1 and 500", 400)
        return success(recent_anomalies(sensor_type, limit))

    @app.post("/api/v1/readings")
    def readings():
        if not request.is_json:
            return error("Request body must be JSON", 400)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return error("Malformed JSON request body", 400)
        event, message, status = validate_reading_payload(payload)
        if event is None:
            return error(message or "Invalid reading", status)
        try:
            metadata = publish_reading(event)
        except Exception as exc:
            app.logger.exception("Kafka publish failed")
            return error(f"Kafka publish failed: {exc}", 500)
        return success({"reading": event, "kafka": metadata}, 201)

    @app.errorhandler(404)
    def not_found(exc):
        return error("Resource not found", 404)

    @app.errorhandler(405)
    def method_not_allowed(exc):
        return error("Method not allowed", 405)

    @app.errorhandler(500)
    def internal_error(exc):
        app.logger.exception("Unhandled server error")
        return error("Internal server error", 500)

    return app


app = create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
