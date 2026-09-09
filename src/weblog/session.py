"""SparkSession for the streaming job."""

from __future__ import annotations

from pyspark.sql import SparkSession

from .config import StreamSettings

# The Kafka source is not bundled with PySpark; it is pulled at submit time.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"


def build(
    app_name: str,
    settings: StreamSettings,
    master: str | None = None,
    with_kafka: bool = False,
) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        # A streaming job's shuffle partition count is fixed for the life of
        # the query and applies to every micro-batch. The batch default of
        # 200 means 200 tasks to aggregate a few thousand records, and the
        # scheduling overhead alone can exceed the trigger interval.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.metricsEnabled", "true")
        # Bounds how much state a single query keeps in memory before it
        # spills, which is what actually kills long-running streams.
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if with_kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    if master:
        builder = builder.master(master)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
