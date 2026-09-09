"""The sink is where exactly-once is won or lost, so it gets real tests.

The multi-batch tests below exist because an earlier version of this sink
partitioned by event date and overwrote. That passes any single-batch test
and silently destroys data the moment a second micro-batch lands on the
same date -- which in a stream is immediately.
"""

from __future__ import annotations

from src.weblog import quality, schema, sinks, transform

from .conftest import event


def _events(kafka_like, values):
    valid, _ = quality.split(schema.parse(kafka_like(values)))
    return transform.add_event_date(valid)


class TestWriteBatch:
    def test_writes_rows(self, spark, tmp_path, kafka_like):
        frame = _events(kafka_like, [event(event_id="a"), event(event_id="b")])
        assert sinks.write_batch(frame, tmp_path / "out", "event_date", 0) == 2
        assert spark.read.parquet(str(tmp_path / "out")).count() == 2

    def test_replaying_a_batch_does_not_duplicate(self, spark, tmp_path, kafka_like):
        # The property exactly-once rests on: Spark replays the last
        # micro-batch after a failure, so writing it twice must be a no-op.
        frame = _events(kafka_like, [event(event_id="a"), event(event_id="b")])
        path = tmp_path / "out"
        sinks.write_batch(frame, path, "event_date", 7)
        sinks.write_batch(frame, path, "event_date", 7)
        assert spark.read.parquet(str(path)).count() == 2

    def test_a_later_batch_on_the_same_date_does_not_erase_the_earlier_one(
        self, spark, tmp_path, kafka_like
    ):
        # The regression. Every micro-batch in a stream lands on today's
        # date partition; overwriting by date alone loses all but the last.
        path = tmp_path / "out"
        sinks.write_batch(_events(kafka_like, [event(event_id="a")]), path, "event_date", 0)
        sinks.write_batch(_events(kafka_like, [event(event_id="b")]), path, "event_date", 1)
        assert spark.read.parquet(str(path)).count() == 2

    def test_many_batches_all_survive(self, spark, tmp_path, kafka_like):
        path = tmp_path / "out"
        for i in range(5):
            sinks.write_batch(
                _events(kafka_like, [event(event_id=f"e{i}")]), path, "event_date", i
            )
        assert spark.read.parquet(str(path)).count() == 5

    def test_different_dates_both_kept(self, spark, tmp_path, kafka_like):
        path = tmp_path / "out"
        sinks.write_batch(_events(kafka_like, [event(event_id="a")]), path, "event_date", 0)
        sinks.write_batch(
            _events(kafka_like, [event(event_id="b", seconds=86_400)]),
            path, "event_date", 1,
        )
        dates = {r["event_date"] for r in spark.read.parquet(str(path)).collect()}
        assert len(dates) == 2

    def test_empty_batch_writes_nothing(self, spark, tmp_path, kafka_like):
        # Idle triggers are normal; they must not create empty directories.
        empty = _events(kafka_like, [event()]).limit(0)
        assert sinks.write_batch(empty, tmp_path / "out", "event_date", 0) == 0
        assert not (tmp_path / "out").exists()


class TestFanOutWriter:
    def test_splits_valid_from_dead_letter(self, spark, tmp_path, kafka_like):
        batch = transform.add_event_date(
            transform.lateness_seconds(
                schema.parse(kafka_like([event(event_id="a"), "{broken"]))
            )
        )
        write = sinks.fan_out_writer(tmp_path / "events", tmp_path / "dead", quality.split)
        write(batch, 0)

        assert spark.read.parquet(str(tmp_path / "events")).count() == 1
        dead = spark.read.parquet(str(tmp_path / "dead"))
        assert dead.count() == 1
        assert dead.collect()[0]["rejected_by"] == "unparseable"

    def test_replaying_a_batch_is_idempotent_on_both_sides(self, spark, tmp_path, kafka_like):
        batch = transform.add_event_date(
            transform.lateness_seconds(
                schema.parse(kafka_like([event(event_id="a"), "{broken"]))
            )
        )
        write = sinks.fan_out_writer(tmp_path / "events", tmp_path / "dead", quality.split)
        write(batch, 3)
        write(batch, 3)

        assert spark.read.parquet(str(tmp_path / "events")).count() == 1
        assert spark.read.parquet(str(tmp_path / "dead")).count() == 1

    def test_successive_batches_accumulate(self, spark, tmp_path, kafka_like):
        write = sinks.fan_out_writer(tmp_path / "events", tmp_path / "dead", quality.split)
        for i in range(3):
            batch = transform.add_event_date(
                transform.lateness_seconds(
                    schema.parse(kafka_like([event(event_id=f"e{i}"), "{broken"]))
                )
            )
            write(batch, i)

        assert spark.read.parquet(str(tmp_path / "events")).count() == 3
        assert spark.read.parquet(str(tmp_path / "dead")).count() == 3
