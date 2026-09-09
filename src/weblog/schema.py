"""The event contract, and how a Kafka value becomes a typed row.

The producer and the consumer are separate deployments that ship on
separate schedules. That makes the schema the only thing holding them
together, so it is declared here explicitly and never inferred.

`from_json` with a declared schema has one behaviour worth knowing: a
record it cannot parse comes back as a row of nulls rather than an error.
That is why `parse` keeps the raw bytes alongside the parsed struct -- a
null struct plus the original payload is what makes a dead-letter record
useful instead of just a count.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("path", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("bytes_sent", IntegerType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("country", StringType(), True),
    ]
)

# Columns carried through the pipeline after flattening.
EVENT_COLUMNS = [f.name for f in EVENT.fields]


def parse(raw: DataFrame, value_column: str = "value") -> DataFrame:
    """Turn a Kafka-shaped frame into typed columns.

    Expects `value` (bytes or string) and optionally Kafka's metadata
    columns. Keeps `raw_payload` so an unparseable record can be inspected
    rather than merely counted, and `ingested_at` so you can tell ingestion
    lag from event-time lag -- when someone asks why yesterday looks light,
    the difference between those two is the answer.
    """
    frame = raw.withColumn("raw_payload", F.col(value_column).cast("string"))

    for column, default in (
        ("topic", F.lit(None).cast("string")),
        ("partition", F.lit(None).cast("int")),
        ("offset", F.lit(None).cast("long")),
    ):
        if column not in frame.columns:
            frame = frame.withColumn(column, default)

    return (
        frame.withColumn("event", F.from_json(F.col("raw_payload"), EVENT))
        .select(
            F.col("event.*"),
            F.col("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("ingested_at"),
        )
    )


def unparseable() -> Column:
    """True when `from_json` produced nothing usable.

    Checking `event_id` rather than the whole struct: a payload that is
    valid JSON but the wrong shape parses into a struct of nulls, which is
    not an error to Spark but is certainly not an event.
    """
    return F.col("event_id").isNull()
