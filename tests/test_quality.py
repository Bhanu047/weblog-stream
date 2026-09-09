from __future__ import annotations

from src.weblog import quality, schema

from .conftest import event


def _parsed(kafka_like, values):
    return schema.parse(kafka_like(values))


class TestSplit:
    def test_clean_events_pass(self, kafka_like):
        valid, dead = quality.split(_parsed(kafka_like, [event()]))
        assert valid.count() == 1
        assert dead.count() == 0

    def test_partitions_the_input(self, kafka_like):
        frame = _parsed(kafka_like, [event(), "{broken", event(event_id="e2", status=999)])
        valid, dead = quality.split(frame)
        assert valid.count() + dead.count() == frame.count()

    def test_unparseable_is_dead_lettered(self, kafka_like):
        _, dead = quality.split(_parsed(kafka_like, ["{broken"]))
        assert dead.collect()[0]["rejected_by"] == "unparseable"

    def test_impossible_status_is_caught(self, kafka_like):
        _, dead = quality.split(_parsed(kafka_like, [event(status=999)]))
        assert dead.collect()[0]["rejected_by"] == "status_out_of_range"

    def test_negative_bytes_is_caught(self, kafka_like):
        _, dead = quality.split(_parsed(kafka_like, [event(bytes_sent=-5)]))
        assert dead.collect()[0]["rejected_by"] == "negative_bytes"

    def test_hung_request_is_caught(self, kafka_like):
        _, dead = quality.split(_parsed(kafka_like, [event(duration_ms=60 * 60 * 1000)]))
        assert dead.collect()[0]["rejected_by"] == "implausible_duration"

    def test_null_optional_fields_are_tolerated(self, kafka_like):
        # status/bytes/duration are genuinely optional upstream. Rejecting a
        # null would throw away good events for no reason.
        valid, dead = quality.split(
            _parsed(kafka_like, [event(status=None, bytes_sent=None, duration_ms=None)])
        )
        assert valid.count() == 1
        assert dead.count() == 0

    def test_dead_letter_keeps_the_payload_and_offset(self, kafka_like):
        _, dead = quality.split(_parsed(kafka_like, ["{broken"]))
        row = dead.collect()[0]
        assert row["raw_payload"] == "{broken"
        assert row["kafka_offset"] == 0

    def test_valid_side_drops_the_marker_columns(self, kafka_like):
        valid, _ = quality.split(_parsed(kafka_like, [event()]))
        assert "rejected_by" not in valid.columns
        assert "raw_payload" not in valid.columns


def test_rule_names_are_unique():
    names = [r.name for r in quality.RULES]
    assert len(names) == len(set(names))
