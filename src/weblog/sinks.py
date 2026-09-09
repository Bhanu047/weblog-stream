"""Writing micro-batches out, idempotently.

Structured Streaming guarantees exactly-once end to end only when the sink
is idempotent. The checkpoint gives you at-least-once: after a failure
Spark re-runs the last micro-batch, so whatever the sink does must be safe
to do twice with the same batch_id.

The obvious approach -- partition by event date and overwrite -- is wrong,
and wrong in a way that destroys data rather than duplicating it. Every
micro-batch in a stream writes to the *same* date partition, so batch 2
overwrites batch 1 and you end up with only the last batch's rows. I found
this by running the pipeline and comparing counts: 3,091 records in, 926 on
disk. The unit test passed because it only ever wrote one batch.

So each batch is written under its own `batch=<id>` prefix. Replaying batch
N overwrites exactly batch N's output and nothing else, which is what
idempotent actually means here. Readers see `batch` and `event_date` as
nested partition columns and can still prune on the date.

The cost is many small files over time. A real deployment compacts them on
a schedule, or uses a table format with a MERGE, which is what Delta and
Iceberg exist for.
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame

log = logging.getLogger(__name__)


def write_batch(
    frame: DataFrame, path: Path | str, partition_by: str, batch_id: int
) -> int:
    """Write one micro-batch under its own prefix. Safe to repeat."""
    if frame.isEmpty():
        return 0

    rows = frame.count()
    target = Path(path) / f"batch={batch_id}"
    (
        frame.write.mode("overwrite")
        .partitionBy(partition_by)
        .parquet(str(target))
    )
    return rows


def batch_writer(path: Path | str, partition_by: str, label: str = "sink"):
    """Build a `foreachBatch` function.

    foreachBatch rather than a built-in sink because it hands you a real
    batch DataFrame: you can write it more than one place, run a MERGE
    against a warehouse, or emit metrics -- none of which the built-in
    sinks allow. The cost is that idempotency becomes your problem.

    (The built-in Parquet sink solves idempotency with its own `_spark_metadata`
    log, which is the right answer when you only need one destination.)
    """

    def write(batch: DataFrame, batch_id: int) -> None:
        # The batch is consumed twice below -- count, then write -- and
        # without caching the whole upstream plan is recomputed for each.
        batch = batch.cache()
        try:
            rows = write_batch(batch, path, partition_by, batch_id)
            log.info("%s batch=%d rows=%d -> %s", label, batch_id, rows, path)
        finally:
            batch.unpersist()

    return write


def fan_out_writer(
    valid_path: Path | str,
    dead_path: Path | str,
    split_fn,
    valid_partition: str = "event_date",
    dead_partition: str = "rejected_by",
):
    """Split each micro-batch into valid and dead-letter, write both.

    Doing the split inside foreachBatch rather than running two separate
    queries means one read of the source instead of two, and both sides
    commit against the same checkpoint -- so a failure cannot leave a
    record written to one side and not the other.
    """

    def write(batch: DataFrame, batch_id: int) -> None:
        batch = batch.cache()
        try:
            valid, dead = split_fn(batch)
            good = write_batch(valid, valid_path, valid_partition, batch_id)
            bad = write_batch(dead, dead_path, dead_partition, batch_id)
            if bad:
                log.warning(
                    "batch=%d %d of %d records dead-lettered", batch_id, bad, good + bad
                )
            else:
                log.info("batch=%d rows=%d", batch_id, good)
        finally:
            batch.unpersist()

    return write
