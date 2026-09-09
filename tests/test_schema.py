from __future__ import annotations

from src.weblog import schema

from .conftest import event


class TestParse:
    def test_flattens_into_declared_columns(self, kafka_like):
        out = schema.parse(kafka_like([event()]))
        for column in schema.EVENT_COLUMNS:
            assert column in out.columns

    def test_keeps_kafka_coordinates(self, kafka_like):
        row = schema.parse(kafka_like([event()])).collect()[0]
        assert row["kafka_topic"] == "weblog.events"
        assert row["kafka_partition"] == 0
        assert row["kafka_offset"] == 0

    def test_keeps_raw_payload_for_the_dead_letter(self, kafka_like):
        # Without the original bytes a dead-letter record is just a count.
        row = schema.parse(kafka_like(["{not json"])).collect()[0]
        assert row["raw_payload"] == "{not json"

    def test_malformed_json_yields_nulls_not_an_error(self, kafka_like):
        # from_json returns a null struct rather than raising. The pipeline
        # depends on this being true, so it is asserted rather than assumed.
        out = schema.parse(kafka_like(["not json at all"]))
        assert out.count() == 1
        assert out.collect()[0]["event_id"] is None

    def test_valid_json_of_the_wrong_shape_is_still_caught(self, kafka_like):
        # `{}` parses fine and produces a struct of nulls -- not an error to
        # Spark, but certainly not an event.
        out = schema.parse(kafka_like(["{}"]))
        assert out.collect()[0]["event_id"] is None

    def test_event_time_is_a_timestamp(self, kafka_like):
        out = schema.parse(kafka_like([event()]))
        assert dict(out.dtypes)["event_time"] == "timestamp"

    def test_works_without_kafka_metadata_columns(self, spark):
        # The file source has no topic/partition/offset. Parsing must still work.
        frame = spark.createDataFrame([(event(),)], "value string")
        out = schema.parse(frame)
        assert out.count() == 1
        assert out.collect()[0]["kafka_topic"] is None
