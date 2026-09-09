"""Wiring: source -> parse -> validate -> sink.

Kept thin on purpose. Everything with logic in it lives in transform.py or
quality.py where it can be tested on a static frame; what is left here is
the part that genuinely needs a running query, and there is one test that
exercises it end to end against a file source.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import quality, schema, transform
from .config import StreamSettings
from .sinks import batch_writer, fan_out_writer

log = logging.getLogger(__name__)


def read_kafka(spark: SparkSession, settings: StreamSettings) -> DataFrame:
    """Subscribe to the topic.

    `startingOffsets=earliest` only applies on a first run -- once a
    checkpoint exists it is ignored and the committed offsets win, which is
    what makes a restart resume rather than replay.

    `failOnDataLoss=true` is deliberate. The alternative silently skips
    records aged out of Kafka retention, and a stream that quietly loses
    data is worse than one that stops and tells you.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.bootstrap_servers)
        .option("subscribe", settings.topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "true")
        .load()
    )


def read_files(spark: SparkSession, path: Path | str) -> DataFrame:
    """JSON-lines directory source.

    Used by the tests and by anyone who wants to run this without standing
    up a broker. Shaped like the Kafka frame so the rest of the pipeline
    cannot tell the difference.
    """
    return (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", 1)
        .load(str(path))
        .selectExpr("CAST(value AS STRING) AS value")
    )


def build_events(raw: DataFrame, settings: StreamSettings) -> DataFrame:
    """Kafka frame -> deduplicated, dated events ready to land."""
    parsed = schema.parse(raw)
    dated = transform.add_event_date(transform.lateness_seconds(parsed))
    return transform.deduplicate(dated, settings.watermark)


def start_ingest(raw: DataFrame, settings: StreamSettings, once: bool = False):
    """Land events and dead letters from one source."""
    events = build_events(raw, settings)
    writer = fan_out_writer(
        settings.paths.events, settings.paths.dead_letter, quality.split
    )
    query = (
        events.writeStream.foreachBatch(writer)
        .option("checkpointLocation", str(settings.paths.checkpoints / "ingest"))
        .outputMode("append")
    )
    query = query.trigger(availableNow=True) if once else query.trigger(
        processingTime=settings.trigger_interval
    )
    return query.start()


def start_sessions(raw: DataFrame, settings: StreamSettings, once: bool = False):
    """Windowed session aggregates.

    Append output mode, so a window is only emitted once the watermark has
    passed its end and it can no longer change. Update mode would emit each
    window repeatedly as it fills, which means downstream has to handle
    restatements -- fine if you need low latency, needless complexity if
    you do not.
    """
    parsed = schema.parse(raw)
    valid, _ = quality.split(parsed)
    windows = transform.session_windows(
        valid, settings.window_duration, settings.watermark
    )
    writer = batch_writer(settings.paths.sessions, "window_date", label="sessions")
    query = (
        windows.writeStream.foreachBatch(writer)
        .option("checkpointLocation", str(settings.paths.checkpoints / "sessions"))
        .outputMode("append")
    )
    query = query.trigger(availableNow=True) if once else query.trigger(
        processingTime=settings.trigger_interval
    )
    return query.start()
