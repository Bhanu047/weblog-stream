from __future__ import annotations

import datetime as dt
import json

from src.weblog import producer


class TestGenerate:
    def test_is_deterministic_under_a_seed_and_a_fixed_clock(self):
        # Both are required. The seed pins the random choices; `now` pins the
        # timestamps, which otherwise differ by microseconds between calls.
        at = dt.datetime(2026, 3, 10, 12, 0, 0)
        assert list(producer.generate(50, seed=7, now=at)) == list(
            producer.generate(50, seed=7, now=at)
        )

    def test_a_different_seed_gives_different_output(self):
        at = dt.datetime(2026, 3, 10, 12, 0, 0)
        assert list(producer.generate(50, seed=7, now=at)) != list(
            producer.generate(50, seed=8, now=at)
        )

    def test_emits_malformed_records(self):
        # If the generator only produced clean input, the dead-letter path
        # would never be exercised by anything.
        lines = list(producer.generate(500, malformed_rate=0.2, seed=3))
        bad = 0
        for line in lines:
            try:
                json.loads(line)
            except json.JSONDecodeError:
                bad += 1
        assert bad > 0

    def test_emits_duplicate_event_ids(self):
        ids = []
        for line in producer.generate(500, duplicate_rate=0.3, malformed_rate=0, seed=5):
            ids.append(json.loads(line)["event_id"])
        assert len(ids) != len(set(ids))

    def test_no_duplicates_when_rate_is_zero(self):
        ids = [
            json.loads(line)["event_id"]
            for line in producer.generate(200, duplicate_rate=0, malformed_rate=0, seed=5)
        ]
        assert len(ids) == len(set(ids))


def test_write_files_creates_one_file_per_batch(tmp_path):
    files = producer.write_files(tmp_path / "out", batches=3, per_batch=10, seed=1)
    assert len(files) == 3
    assert all(f.exists() for f in files)
