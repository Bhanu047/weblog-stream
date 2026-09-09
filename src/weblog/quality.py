"""Validation, and the dead-letter split.

Same principle as my batch pipelines: a rejected record is written
somewhere with the reason attached, never dropped. In a stream it matters
more, not less -- there is no source file to go back to, so a record you
drop is gone for good once the Kafka retention window passes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import unparseable

MAX_DURATION_MS = 10 * 60 * 1000  # 10 minutes for one request is a hung worker
MAX_BYTES = 500 * 1024 * 1024


@dataclass(frozen=True)
class Rule:
    name: str
    ok: Callable[[], Column]


RULES: list[Rule] = [
    Rule("unparseable", lambda: ~unparseable()),
    Rule("missing_session_id", lambda: F.col("session_id").isNotNull()),
    Rule("missing_event_time", lambda: F.col("event_time").isNotNull()),
    Rule(
        "status_out_of_range",
        lambda: F.col("status").isNull() | F.col("status").between(100, 599),
    ),
    Rule(
        "negative_bytes",
        lambda: F.col("bytes_sent").isNull() | (F.col("bytes_sent") >= 0),
    ),
    Rule(
        "implausible_bytes",
        lambda: F.col("bytes_sent").isNull() | (F.col("bytes_sent") <= MAX_BYTES),
    ),
    Rule(
        "implausible_duration",
        lambda: F.col("duration_ms").isNull()
        | F.col("duration_ms").between(0, MAX_DURATION_MS),
    ),
]


def _first_failure() -> Column:
    """Name the first rule a record breaks. One pass, not one pass per rule."""
    expr = F.lit(None).cast("string")
    for rule in reversed(RULES):
        expr = F.when(~rule.ok(), F.lit(rule.name)).otherwise(expr)
    return expr


def split(frame: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Partition into (valid, dead_letter).

    The dead-letter side keeps `raw_payload` and the Kafka coordinates, so a
    bad record can be traced back to the exact offset that produced it.
    """
    tagged = frame.withColumn("rejected_by", _first_failure())
    valid = tagged.filter(F.col("rejected_by").isNull()).drop("rejected_by", "raw_payload")
    dead = tagged.filter(F.col("rejected_by").isNotNull()).select(
        "rejected_by",
        "raw_payload",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "ingested_at",
    )
    return valid, dead
