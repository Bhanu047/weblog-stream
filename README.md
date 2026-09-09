# Weblog stream

[![CI](https://github.com/Bhanu047/weblog-stream/actions/workflows/ci.yml/badge.svg)](https://github.com/Bhanu047/weblog-stream/actions/workflows/ci.yml)

Real-time weblog ingestion. Kafka into Spark Structured Streaming, out to Parquet, with the awkward parts of streaming handled rather than assumed away.

```
kafka ──► parse ──► validate ──┬──► events    (deduped, partitioned by event date)
                               └──► dead letter (payload + the rule that caught it)
                     │
                     └────────────► sessions  (5-minute windows, watermarked)
```

I built this because the batch pipelines in my other repos don't show any of the decisions that only come up in streaming. Watermarks, late data, duplicate delivery, and what "exactly-once" actually costs you. Those are the interesting parts, so most of this README is about them.

## Running it

Nothing to install beyond PySpark, and no broker needed:

```bash
pip install -r requirements-dev.txt
pytest              # 52 tests, no Kafka, no network
make demo           # generate traffic, ingest it, build sessions, print a report
```

Spark 4 needs Java 17 or later.

Against a real broker, if you have Docker:

```bash
make kafka-up
python -m src.weblog.cli produce  --source kafka
python -m src.weblog.cli ingest   --source kafka
```

The pipeline code is identical either way. Only the source differs, which is the point of keeping the reader separate from everything else.

## Why the watermark is the whole design

`withWatermark("event_time", "10 minutes")` is one line and it decides almost everything else.

It's a promise about how late an event can arrive and still be counted. Set it too short and you quietly drop real events every time the upstream hiccups. Set it too long and state grows until the executors run out of memory — and that failure arrives at 3am on day nine, not in your tests.

So it should come from measuring the source's actual lateness, not from picking a round number. That's why `lateness_s` is written out as a column on every event rather than just tracked as a metric: once it's in the table you can look at the distribution and set the watermark from evidence.

## Why dedup uses `dropDuplicatesWithinWatermark`

Kafka is at-least-once. A producer that times out and retries sends the same event twice, and that's normal traffic, not an incident.

Plain `dropDuplicates` remembers every key it has ever seen. On a stream that never ends, so does the state. `dropDuplicatesWithinWatermark` expires keys once they fall behind the watermark, which trades perfect deduplication for a query that survives the week. That's the right trade — a duplicate slipping through after ten minutes costs you one row; unbounded state costs you the job.

One wrinkle: it's streaming-only, so it can't be called on a static frame. A transform I can't unit-test is a transform I don't trust, so `deduplicate` branches on `isStreaming` and uses plain `dropDuplicates` on bounded input, where there's no unbounded state to worry about. The branch is on the frame itself rather than a flag the caller passes, so it can't be wired up wrongly.

### The key matters more than the operator

Deduplicating on `event_id` alone eats records. Every unparseable payload parses to a null `event_id`, Spark treats nulls as equal, and a whole batch of malformed records therefore collapses into one row — before the dead-letter split ever sees them.

I found this by running the pipeline and counting: about 60 malformed records went in, 2 came out. The tests were green the whole time, because each one only fed a single bad record.

So the key falls back to the Kafka coordinates when `event_id` is null. `(topic, partition, offset)` is unique per record and stable across replays, so malformed records keep their individual identity while real events still deduplicate properly. The file source has no offsets, so byte-identical bad payloads still collapse there — that source is for local runs, not for anything whose dead letters matter.

## Why exactly-once lives in the sink, not the config

Structured Streaming gives you at-least-once by itself. The checkpoint records offsets, and after a failure Spark re-runs the last micro-batch. Whatever the sink does therefore has to be safe to do twice.

Appending isn't. Replacing the partitions the batch touches is.

That's what `overwrite_partitions` does, and it relies on `partitionOverwriteMode=dynamic`. Without that setting `mode("overwrite")` truncates the entire table, so one replayed batch would delete every day of history you had. There's a test asserting the behaviour rather than the config value, because the config is easy to lose in a refactor and the behaviour is what actually matters.

## Why `foreachBatch` rather than a file sink

The built-in sinks take a stream and write it one place. `foreachBatch` hands you a real batch DataFrame, which means you can split it, write both halves, run a MERGE against a warehouse, or emit your own metrics.

Here it's used to fan out: valid events one way, dead letters the other, both committed against the same checkpoint. Two separate queries would read Kafka twice and could leave a record written to one side but not the other after a failure.

The cost is that idempotency becomes your problem. That's the trade, and it's worth it.

## Why bad records are dead-lettered, not dropped

Same principle as my batch pipelines, but it matters more here. In a batch job the source file is still on disk, so a dropped row can be recovered. In a stream, once Kafka's retention window passes, a dropped record is gone permanently.

So every rejected record is written with `rejected_by` naming the rule, plus the raw payload and the exact Kafka topic/partition/offset it came from. That means a bad record can be traced back to the byte that produced it, and if a rule turns out to be too strict the records are still there to reprocess.

Worth knowing: `from_json` with a declared schema doesn't raise on a bad payload — it returns a struct of nulls. So does a payload that's valid JSON but the wrong shape, like `{}`. Both are caught by checking `event_id` rather than trusting the parse to fail. There are tests for both cases, because this is exactly the kind of behaviour that changes between versions.

## Why append mode for the session windows

Append emits a window once, after the watermark has passed its end and it can no longer change. Update mode emits each window repeatedly as it fills, which means everything downstream has to cope with restatements.

Update is the right answer when you need low latency and can handle corrections. Here the question is "how much traffic in each five minutes", the answer is wanted once and correct, and append gives exactly that.

Tumbling windows rather than sliding, for a related reason: sliding windows put each event into several windows, multiplying both state and output rows. Tumbling answers the question once.

The distinct-path count uses `approx_count_distinct` rather than `countDistinct`, and not by choice — Structured Streaming rejects exact distinct aggregations outright, because they need state proportional to cardinality and that's unbounded. HyperLogLog gives a bounded-memory estimate instead. It's the trade streaming forces on you, and for "how many distinct pages did this session touch" it's the right one anyway.

## Why `maxOffsetsPerTrigger` is set

Without it, a stream restarting after an outage tries to swallow the entire backlog in one micro-batch, and the executors die on it. Then it restarts, tries again, and dies again.

Capping the batch size means the recovery is slower but finishes. `failOnDataLoss` is left on for a similar reason: the alternative silently skips records that aged out of retention, and a stream that quietly loses data is worse than one that stops and says so.

## Layout

```
src/weblog/config.py     Paths and the knobs worth naming
src/weblog/session.py    SparkSession, and why the streaming defaults differ
src/weblog/schema.py     Declared event contract; Kafka value -> typed row
src/weblog/quality.py    Rules as data; the dead-letter split
src/weblog/transform.py  Watermark, dedup, windowed aggregation
src/weblog/sinks.py      Idempotent foreachBatch writers
src/weblog/stream.py     Wiring only -- the part that needs a running query
src/weblog/producer.py   Synthetic traffic, including the messy cases
src/weblog/cli.py        produce / ingest / sessions / report
tests/                   52 tests, no broker required
```

Transforms take a DataFrame and return a DataFrame and don't know whether they're on a stream or a batch. That's what makes them testable in milliseconds instead of requiring a running query for every assertion.

## What's missing

On purpose:

- **No schema registry.** Avro plus a registry is what I'd use where producer and consumer ship independently. Here the declared schema and the dead-letter path show the same reasoning without another service to stand up.
- **No Delta or Iceberg.** Dynamic partition overwrite covers the idempotency this needs. A table format would add concurrent writers and time travel, neither of which one scheduled query uses.
- **No autoscaling or cluster tuning.** Real numbers there come from real traffic, and inventing them would be dishonest.
- **Single broker in docker-compose.** Four partitions, so consumer code can't accidentally assume global ordering, but it's not a model of a production cluster.

## Notes on the tests

52 tests, none of which need Kafka. A suite that requires a broker fails for reasons unrelated to your change, and people stop trusting it.

Three of the bugs above were found by running the pipeline and comparing counts, not by the tests — which is a useful reminder that a green suite proves the cases you thought of. Each one now has a regression test, and they're the ones worth reading first: the multi-batch tests in `test_sinks.py` and the null-key tests in `test_transform.py` all exist because something silently ate records.

Beyond those, `test_sinks.py` is the file that matters. Exactly-once is a property of the sink, so the tests that matter are the ones writing the same batch twice and asserting the row count doesn't move. `test_stream_end_to_end.py` runs one real query with `availableNow`, which drains the input and stops — that covers the wiring the static tests can't reach.
