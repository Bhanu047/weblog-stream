"""Synthetic event generator.

Exists so the repo is runnable without hooking it to anything real. It
deliberately emits the awkward cases as well as the happy path -- late
events, duplicates, malformed JSON -- because a stream that has only ever
seen clean input proves nothing.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import uuid
from pathlib import Path
from typing import Iterator

PATHS = ["/", "/search", "/product", "/cart", "/checkout", "/api/v1/items", "/health"]
METHODS = ["GET", "GET", "GET", "POST"]  # weighted towards GET
COUNTRIES = ["US", "GB", "DE", "IN", "BR", "JP"]
AGENTS = ["Mozilla/5.0", "curl/8.4.0", "python-requests/2.32", "Googlebot/2.1"]


def _uuid() -> str:
    """A UUID drawn from the seeded RNG.

    `uuid.uuid4()` reads the OS entropy source and ignores `random.seed`,
    which would make this generator unreproducible -- and a fixture you
    cannot reproduce is one you cannot write a failing test against.
    """
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def _malformed(session_id: str, when: dt.datetime) -> str:
    """A broken payload, and no two alike.

    Real corruption is a message cut off mid-flight or a serialiser that
    emitted the wrong shape -- each one carrying different bytes. Emitting
    the same three fixed strings would be unrealistic and would also hide a
    genuine limitation: without Kafka offsets to tell them apart, byte-identical
    bad records collapse during deduplication.
    """
    kind = random.choice(["truncated", "wrong_shape", "not_json"])
    if kind == "truncated":
        full = json.dumps(_event(session_id, when))
        return full[: random.randint(20, len(full) - 5)]
    if kind == "wrong_shape":
        return json.dumps({"id": _uuid(), "ts": when.isoformat(), "kind": "page_view"})
    return f"<<<binary garbage {_uuid()[:12]}>>>"


def _event(session_id: str, when: dt.datetime) -> dict:
    status = random.choices([200, 200, 200, 301, 404, 500], weights=[70, 10, 5, 5, 7, 3])[0]
    return {
        "event_id": _uuid(),
        "session_id": session_id,
        "user_id": f"u{random.randint(1, 500)}",
        "event_time": when.isoformat(),
        "path": random.choice(PATHS),
        "method": random.choice(METHODS),
        "status": status,
        "bytes_sent": random.randint(200, 50_000),
        "duration_ms": random.randint(5, 2_500),
        "user_agent": random.choice(AGENTS),
        "country": random.choice(COUNTRIES),
    }


def generate(
    count: int,
    sessions: int = 40,
    late_rate: float = 0.05,
    duplicate_rate: float = 0.03,
    malformed_rate: float = 0.02,
    now: dt.datetime | None = None,
    seed: int | None = None,
) -> Iterator[str]:
    """Yield JSON lines, including the messy ones.

    Deterministic given both `seed` and `now`: the seed pins the random
    choices, `now` pins the timestamps.

    The three rates are the point of this generator. Real streams carry
    late records, at-least-once duplicates and the occasional truncated
    payload, and each is handled by a different part of the pipeline:
    the watermark, the dedup, and the dead-letter split.
    """
    if seed is not None:
        random.seed(seed)
    now = now or dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    session_ids = [f"s{_uuid()[:10]}" for _ in range(sessions)]

    for _ in range(count):
        if random.random() < malformed_rate:
            yield _malformed(random.choice(session_ids), now)
            continue

        # Most events are recent; a slice of them arrive well behind.
        offset = (
            random.uniform(0, 120)
            if random.random() > late_rate
            else random.uniform(600, 1800)
        )
        event = _event(random.choice(session_ids), now - dt.timedelta(seconds=offset))
        line = json.dumps(event)
        yield line

        if random.random() < duplicate_rate:
            yield line  # same event_id: the dedup step must collapse these


def write_files(target: Path, batches: int = 3, per_batch: int = 500, **kw) -> list[Path]:
    """Write JSON-lines files, one per batch, for the file-source path."""
    target = Path(target)
    target.mkdir(parents=True, exist_ok=True)
    written = []
    for i in range(batches):
        path = target / f"events-{i:03d}.json"
        path.write_text("\n".join(generate(per_batch, **kw)) + "\n")
        written.append(path)
    return written


def publish_kafka(bootstrap: str, topic: str, count: int, **kw) -> int:
    """Send to a real broker. Requires `kafka-python`, an extra dependency."""
    from kafka import KafkaProducer  # imported lazily: only this path needs it

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",  # anything less and a broker failover loses records
        retries=5,
    )
    sent = 0
    for line in generate(count, **kw):
        producer.send(topic, line)
        sent += 1
    producer.flush()
    producer.close()
    return sent
