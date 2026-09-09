"""Event-time transforms: watermarking, dedup, windowed aggregation.

Every function here takes a DataFrame and returns a DataFrame, and none of
them know whether they are running on a stream or a batch. That is
deliberate: it means the logic can be tested on a static frame in
milliseconds, and only the wiring in `stream.py` needs a real streaming
test.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def with_watermark(frame: DataFrame, delay: str, column: str = "event_time") -> DataFrame:
    """Declare how late an event may be and still count.

    This is the single most consequential line in a streaming job. Too
    short and you silently drop real events during any upstream hiccup;
    too long and state grows until the executors die. It is a statement
    about the source's worst-case lateness, so it should come from
    measuring that lag, not from a round number that looked sensible.
    """
    return frame.withWatermark(column, delay)


DEDUP_KEY = "_dedup_key"


def with_dedup_key(frame: DataFrame) -> DataFrame:
    """The key deduplication runs on.

    `event_id` alone is wrong, and wrong in a way that eats records. Every
    unparseable payload has a null event_id, and Spark treats nulls as
    equal -- so a whole micro-batch of malformed records collapses into a
    single row and the rest are gone before the dead-letter split ever sees
    them. I found this by counting: ~60 malformed records in, 2 on disk.

    Falling back to the Kafka coordinates fixes it. (topic, partition,
    offset) is unique per record and stable across replays, so malformed
    records keep their individual identity while real events still
    deduplicate on event_id.

    The file source has no offsets, so identical malformed lines inside one
    watermark window still collapse there. That source is for local runs
    and tests, not for anything whose dead letters matter.
    """
    # The fallback is built from whatever identifying columns this frame
    # still carries: the validated side has already dropped raw_payload, so
    # referencing it unconditionally would fail there.
    candidates = [
        F.col(c)
        for c in ("kafka_topic", "kafka_partition", "kafka_offset", "raw_payload")
        if c in frame.columns
    ]
    fallback = F.concat_ws("|", F.lit("__unkeyed__"), *candidates)
    return frame.withColumn(DEDUP_KEY, F.coalesce(F.col("event_id"), fallback))


def deduplicate(frame: DataFrame, delay: str) -> DataFrame:
    """Drop repeated events, bounded by the watermark.

    At-least-once delivery means duplicates are normal, not exceptional --
    a producer retry after a timeout is the usual cause.

    `dropDuplicatesWithinWatermark` is the right tool rather than plain
    `dropDuplicates`: the plain version keeps every key it has ever seen,
    so on an unbounded stream the state grows forever. The watermarked
    version expires keys once they are older than the threshold, which
    trades perfect deduplication for a query that survives the week.

    It is streaming-only, though, and a transform I cannot call on a static
    frame is a transform I cannot unit-test. So the batch branch uses plain
    `dropDuplicates`, which is equivalent on bounded input -- there is no
    unbounded state to worry about when the input ends. The branch is on
    `isStreaming` rather than a flag the caller passes, so there is no way
    to wire it up wrongly.
    """
    keyed = with_dedup_key(frame)
    if not keyed.isStreaming:
        return keyed.dropDuplicates([DEDUP_KEY]).drop(DEDUP_KEY)
    return (
        keyed.withWatermark("event_time", delay)
        .dropDuplicatesWithinWatermark([DEDUP_KEY])
        .drop(DEDUP_KEY)
    )


def add_event_date(frame: DataFrame) -> DataFrame:
    """Partition key for the sink. Derived from event time, never from now().

    Using processing time here would put a late event in the wrong day's
    partition, and the partition is what downstream filters on.
    """
    return frame.withColumn("event_date", F.to_date("event_time"))


def session_windows(frame: DataFrame, window_duration: str, delay: str) -> DataFrame:
    """Per-session activity in tumbling event-time windows.

    Tumbling rather than sliding: sliding windows emit each event into
    several windows, which multiplies both state and output rows. If the
    question is "how much traffic in each five minutes", tumbling answers
    it exactly once.
    """
    return (
        frame.withWatermark("event_time", delay)
        .groupBy(
            F.window("event_time", window_duration).alias("window"),
            F.col("session_id"),
        )
        .agg(
            F.count(F.lit(1)).alias("events"),
            # Exact distinct counts are rejected on a stream: they need
            # state proportional to cardinality, unbounded. HyperLogLog
            # gives a bounded-memory estimate, which is the trade
            # Structured Streaming forces and usually the right one.
            F.approx_count_distinct("path").alias("distinct_paths"),
            F.sum("bytes_sent").alias("bytes_sent"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("server_errors"),
            F.sum(F.when(F.col("status").between(400, 499), 1).otherwise(0)).alias(
                "client_errors"
            ),
            F.min("event_time").alias("session_start"),
            F.max("event_time").alias("session_end"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "session_id",
            "events",
            "distinct_paths",
            "bytes_sent",
            F.round("avg_duration_ms", 1).alias("avg_duration_ms"),
            "server_errors",
            "client_errors",
            F.round(
                (F.col("session_end").cast("long") - F.col("session_start").cast("long"))
                / 60.0,
                2,
            ).alias("duration_min"),
        )
        .withColumn("window_date", F.to_date("window_start"))
    )


def lateness_seconds(frame: DataFrame) -> DataFrame:
    """How far behind wall-clock each event arrived.

    Worth emitting as a column rather than only a metric: it is the number
    you need to set the watermark honestly, and once it is in the table you
    can look at its distribution instead of guessing.
    """
    return frame.withColumn(
        "lateness_s",
        F.col("ingested_at").cast("long") - F.col("event_time").cast("long"),
    )
