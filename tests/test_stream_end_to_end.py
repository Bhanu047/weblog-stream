"""One real streaming query, start to finish.

Everything else is tested on static frames because that is faster and
deterministic. But the wiring -- watermark, trigger, checkpoint, sink --
only exists inside a running query, so it gets exercised here against a
file source with `availableNow`, which drains the input and stops.
"""

from __future__ import annotations

from src.weblog import producer, stream
from src.weblog.config import StreamSettings

from .conftest import event


def _run(spark, incoming, settings):
    query = stream.start_ingest(stream.read_files(spark, incoming), settings, once=True)
    query.awaitTermination(timeout=180)
    return query


class TestEndToEnd:
    def test_ingests_events_and_dead_letters(self, spark, tmp_path):
        incoming = tmp_path / "incoming"
        incoming.mkdir()
        (incoming / "batch.json").write_text(
            "\n".join([event(event_id="a"), event(event_id="b", seconds=5), "{broken"])
        )
        settings = StreamSettings.local(tmp_path / "warehouse")

        _run(spark, incoming, settings)

        assert spark.read.parquet(str(settings.paths.events)).count() == 2
        dead = spark.read.parquet(str(settings.paths.dead_letter))
        assert dead.count() == 1
        assert dead.collect()[0]["rejected_by"] == "unparseable"

    def test_checkpoint_is_created(self, spark, tmp_path):
        incoming = tmp_path / "incoming"
        incoming.mkdir()
        (incoming / "batch.json").write_text(event(event_id="a"))
        settings = StreamSettings.local(tmp_path / "warehouse")

        _run(spark, incoming, settings)

        # Without a checkpoint a restart replays from the beginning, which
        # is the difference between resuming and reprocessing.
        assert (settings.paths.checkpoints / "ingest").exists()

    def test_generated_traffic_survives_the_pipeline(self, spark, tmp_path):
        # The generator emits duplicates, late events and malformed payloads
        # on purpose. Nothing should be lost: every record lands on one side
        # or the other.
        incoming = tmp_path / "incoming"
        producer.write_files(incoming, batches=1, per_batch=300, seed=11)
        raw_lines = sum(
            1 for line in (incoming / "events-000.json").read_text().splitlines() if line
        )
        settings = StreamSettings.local(tmp_path / "warehouse")

        _run(spark, incoming, settings)

        good = spark.read.parquet(str(settings.paths.events)).count()
        bad = spark.read.parquet(str(settings.paths.dead_letter)).count()
        # Deduplication removes some, so accounted <= raw, never more.
        assert good > 0 and bad > 0
        assert good + bad <= raw_lines

    def test_no_records_are_lost_across_micro_batches(self, spark, tmp_path):
        # maxFilesPerTrigger=1, so three files means three micro-batches, all
        # landing on the same event_date. Every one of them must survive.
        incoming = tmp_path / "incoming"
        incoming.mkdir()
        for i in range(3):
            (incoming / f"batch-{i}.json").write_text(
                "\n".join(event(event_id=f"b{i}-{j}", seconds=j) for j in range(10))
            )
        settings = StreamSettings.local(tmp_path / "warehouse")

        _run(spark, incoming, settings)

        assert spark.read.parquet(str(settings.paths.events)).count() == 30

    def test_session_windows_run_on_a_real_stream(self, spark, tmp_path):
        # Aggregations have streaming-only restrictions that a static frame
        # never reveals -- exact countDistinct is rejected outright. Only a
        # running query proves the aggregation is legal.
        incoming = tmp_path / "incoming"
        incoming.mkdir()
        (incoming / "batch.json").write_text(
            "\n".join(event(event_id=f"e{i}", seconds=i * 20) for i in range(30))
        )
        settings = StreamSettings.local(tmp_path / "warehouse")

        query = stream.start_sessions(
            stream.read_files(spark, incoming), settings, once=True
        )
        query.awaitTermination(timeout=180)
        assert query.exception() is None

    def test_duplicates_are_collapsed(self, spark, tmp_path):
        incoming = tmp_path / "incoming"
        incoming.mkdir()
        duplicate = event(event_id="same")
        (incoming / "batch.json").write_text("\n".join([duplicate, duplicate, duplicate]))
        settings = StreamSettings.local(tmp_path / "warehouse")

        _run(spark, incoming, settings)

        assert spark.read.parquet(str(settings.paths.events)).count() == 1
