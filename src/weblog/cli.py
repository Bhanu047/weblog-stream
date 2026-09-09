"""Entry point: `ingest`, `sessions`, `produce`, `report`."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from . import producer, stream
from .config import StreamSettings
from .session import build

log = logging.getLogger("weblog")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="weblog", description=__doc__)
    parser.add_argument("command", choices=["ingest", "sessions", "produce", "report"])
    parser.add_argument("--root", type=Path, default=Path("warehouse"))
    parser.add_argument("--source", choices=["kafka", "files"], default="files")
    parser.add_argument("--files", type=Path, default=Path("data/incoming"))
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="weblog.events")
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--once", action="store_true", help="drain and stop")
    parser.add_argument("--master", default="local[*]")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )
    settings = StreamSettings.local(
        args.root, bootstrap_servers=args.bootstrap, topic=args.topic
    )

    if args.command == "produce":
        if args.source == "kafka":
            sent = producer.publish_kafka(args.bootstrap, args.topic, args.count)
            log.info("published %d events to %s", sent, args.topic)
        else:
            files = producer.write_files(args.files, batches=3, per_batch=args.count // 3)
            log.info("wrote %d files under %s", len(files), args.files)
        return 0

    if args.command == "report":
        spark = build("weblog-report", settings, master=args.master)
        try:
            for name, path in (
                ("events", settings.paths.events),
                ("dead letters", settings.paths.dead_letter),
                ("sessions", settings.paths.sessions),
            ):
                if not Path(path).exists():
                    print(f"{name:<14} (not written yet)")
                    continue
                frame = spark.read.parquet(str(path))
                print(f"{name:<14} {frame.count():>8,} rows")
                if name == "dead letters" and frame.count():
                    frame.groupBy("rejected_by").count().orderBy(
                        "count", ascending=False
                    ).show(truncate=False)
        finally:
            spark.stop()
        return 0

    spark = build(
        f"weblog-{args.command}", settings, master=args.master,
        with_kafka=args.source == "kafka",
    )
    try:
        raw = (
            stream.read_kafka(spark, settings)
            if args.source == "kafka"
            else stream.read_files(spark, args.files)
        )
        start = stream.start_ingest if args.command == "ingest" else stream.start_sessions
        query = start(raw, settings, once=args.once)
        query.awaitTermination()
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
