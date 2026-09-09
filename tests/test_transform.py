from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F

from src.weblog import quality, schema, transform

from .conftest import BASE, event


def _valid(kafka_like, values):
    valid, _ = quality.split(schema.parse(kafka_like(values)))
    return valid


class TestDeduplicate:
    def test_collapses_repeated_event_ids(self, kafka_like):
        # At-least-once delivery: a producer retry sends the same event twice.
        frame = _valid(kafka_like, [event(event_id="dup"), event(event_id="dup")])
        assert transform.deduplicate(frame, "10 minutes").count() == 1

    def test_keeps_genuinely_different_events(self, kafka_like):
        frame = _valid(kafka_like, [event(event_id="a"), event(event_id="b")])
        assert transform.deduplicate(frame, "10 minutes").count() == 2

    def test_unparseable_records_are_not_collapsed_together(self, kafka_like):
        # The regression. Every malformed payload parses to a null event_id,
        # and Spark treats nulls as equal -- so keying dedup on event_id
        # alone silently merged them all into one and the dead-letter split
        # never saw the rest.
        frame = schema.parse(kafka_like(["{broken one", "{broken two", "also bad"]))
        assert transform.deduplicate(frame, "10 minutes").count() == 3

    def test_identical_bad_payloads_at_different_offsets_both_survive(self, kafka_like):
        # Same bytes, different Kafka offsets: two genuinely distinct records.
        frame = schema.parse(kafka_like(["{broken", "{broken"]))
        assert transform.deduplicate(frame, "10 minutes").count() == 2

    def test_the_dedup_key_does_not_leak_into_the_output(self, kafka_like):
        frame = _valid(kafka_like, [event(event_id="a")])
        assert transform.DEDUP_KEY not in transform.deduplicate(frame, "10 minutes").columns


class TestEventDate:
    def test_derives_from_event_time_not_now(self, kafka_like):
        # A late event must land in its own day's partition, not today's.
        frame = _valid(kafka_like, [event()])
        row = transform.add_event_date(frame).collect()[0]
        assert row["event_date"] == BASE.date()

    def test_is_a_date_not_a_timestamp(self, kafka_like):
        frame = transform.add_event_date(_valid(kafka_like, [event()]))
        assert dict(frame.dtypes)["event_date"] == "date"


class TestLateness:
    def test_is_positive_for_a_past_event(self, kafka_like):
        frame = transform.lateness_seconds(_valid(kafka_like, [event()]))
        assert frame.collect()[0]["lateness_s"] > 0


class TestSessionWindows:
    def test_groups_a_session_into_one_window(self, kafka_like):
        events = [event(event_id=f"e{i}", seconds=i * 10) for i in range(6)]
        out = transform.session_windows(
            _valid(kafka_like, events), "5 minutes", "10 minutes"
        ).collect()
        assert len(out) == 1
        assert out[0]["events"] == 6

    def test_splits_across_window_boundaries(self, kafka_like):
        # 0s and 400s fall in different 5-minute tumbling windows.
        events = [event(event_id="a", seconds=0), event(event_id="b", seconds=400)]
        out = transform.session_windows(
            _valid(kafka_like, events), "5 minutes", "10 minutes"
        ).collect()
        assert len(out) == 2

    def test_separates_sessions(self, kafka_like):
        events = [
            event(event_id="a", session_id="s1"),
            event(event_id="b", session_id="s2"),
        ]
        out = transform.session_windows(
            _valid(kafka_like, events), "5 minutes", "10 minutes"
        ).collect()
        assert {r["session_id"] for r in out} == {"s1", "s2"}

    def test_counts_error_classes_separately(self, kafka_like):
        events = [
            event(event_id="a", status=200),
            event(event_id="b", status=404, seconds=1),
            event(event_id="c", status=503, seconds=2),
        ]
        row = transform.session_windows(
            _valid(kafka_like, events), "5 minutes", "10 minutes"
        ).collect()[0]
        assert row["client_errors"] == 1
        assert row["server_errors"] == 1

    def test_window_date_is_derived_for_partitioning(self, kafka_like):
        row = transform.session_windows(
            _valid(kafka_like, [event()]), "5 minutes", "10 minutes"
        ).collect()[0]
        assert row["window_date"] == BASE.date()

    def test_distinct_paths_counts_uniques_not_hits(self, kafka_like):
        events = [
            event(event_id="a", path="/"),
            event(event_id="b", path="/", seconds=1),
            event(event_id="c", path="/cart", seconds=2),
        ]
        row = transform.session_windows(
            _valid(kafka_like, events), "5 minutes", "10 minutes"
        ).collect()[0]
        assert row["events"] == 3
        assert row["distinct_paths"] == 2
