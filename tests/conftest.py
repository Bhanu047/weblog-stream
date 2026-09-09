"""Shared fixtures.

No Kafka here on purpose. A test suite that needs a broker fails for
reasons unrelated to the change being tested, so the broker is exercised
by docker-compose and by hand, and the logic is exercised here.
"""

from __future__ import annotations

import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

BASE = dt.datetime(2026, 3, 10, 12, 0, 0)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("weblog-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def event(
    event_id="e1",
    session_id="s1",
    seconds=0,
    path="/",
    status=200,
    bytes_sent=1024,
    duration_ms=120,
    **overrides,
):
    """One well-formed event as a JSON string."""
    payload = {
        "event_id": event_id,
        "session_id": session_id,
        "user_id": "u1",
        "event_time": (BASE + dt.timedelta(seconds=seconds)).isoformat(),
        "path": path,
        "method": "GET",
        "status": status,
        "bytes_sent": bytes_sent,
        "duration_ms": duration_ms,
        "user_agent": "Mozilla/5.0",
        "country": "US",
    }
    payload.update(overrides)
    return json.dumps(payload)


@pytest.fixture
def kafka_like(spark):
    """Build a frame shaped like Spark's Kafka source from raw JSON strings."""

    def build(values: list[str]):
        rows = [(v.encode("utf-8"), "weblog.events", 0, i) for i, v in enumerate(values)]
        return spark.createDataFrame(
            rows, "value binary, topic string, partition int, offset long"
        )

    return build
