"""Configuration.

Everything the stream needs to be pointed somewhere else lives here, so
switching from a local broker to MSK is a config change rather than a code
change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def events(self) -> Path:
        """Validated events, partitioned by event date."""
        return self.root / "events"

    @property
    def dead_letter(self) -> Path:
        """Records that failed parsing or validation, with the reason."""
        return self.root / "dead_letter"

    @property
    def sessions(self) -> Path:
        """Windowed per-session aggregates."""
        return self.root / "sessions"

    @property
    def checkpoints(self) -> Path:
        """Streaming state. Deleting this replays from the earliest offset."""
        return self.root / "_checkpoints"


@dataclass(frozen=True)
class StreamSettings:
    paths: Paths
    bootstrap_servers: str = "localhost:9092"
    topic: str = "weblog.events"

    # How late an event may arrive and still be counted. Beyond this the
    # window is closed and its state dropped, which is the trade the whole
    # design turns on: a longer watermark means more correctness and more
    # memory held in state.
    watermark: str = "10 minutes"

    # Window used for the session aggregates.
    window_duration: str = "5 minutes"

    # Cap on records per trigger. Without it, a stream restarting after an
    # outage tries to swallow the whole backlog in one micro-batch and the
    # executors die on it.
    max_offsets_per_trigger: int = 50_000

    trigger_interval: str = "30 seconds"

    @classmethod
    def local(cls, root: str | Path = "warehouse", **kw) -> "StreamSettings":
        return cls(paths=Paths(Path(root)), **kw)
