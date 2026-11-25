# Design Doc: Live Trip Stream (Ticket 2)

Author: Ayush Anand | Status: Draft v2 (per-partition watermark, TTL on ingested_at) | Builds on: Ticket 1 (Taxi Trip Analytics Service)

## 1. Goal

Turn the batch-only analytics service into one that also answers "what is happening right now?". Trips arrive as a live stream. Within seconds, the API must report trips per pickup zone for the last few minutes.

Assumption: the live feed is simulated by replaying the 43.5M historical trips in time order at a configurable speed (for example 60x), because TLC does not publish a real live feed.

Non-goals: exactly-once across the whole world, multi-region, auth.

## 2. Requirements

| Item | Target |
| --- | --- |
| Throughput | 5,000 events/sec sustained, 20,000 burst |
| Freshness | Event visible in the live API within 5 s of arrival |
| Live API latency | p95 under 100 ms |
| Correctness | No duplicate counting after a crash or replay |
| Late data | Events up to 10 min late still correct; older ones are quarantined, not lost |

## 3. Architecture

```
replayer.py -> Redpanda topic `trips.raw` -> stream consumer (windowing) -> ClickHouse `live_zone_windows`
                       |                                                       |
                       +-> `trips.dlq` (bad / too-late events)                 +-> FastAPI /live/* endpoints
```

Components:
1. **Replayer:** reads the Parquet files in pickup-time order, publishes JSON events, sleeps to simulate speed. Key = `pu_location_id`.
2. **Broker:** Redpanda, single node in docker-compose.
3. **Consumer:** Python, consumer group, reads batches, assigns each event to a window, flushes to ClickHouse.
4. **Storage:** new ClickHouse table for window aggregates. The existing batch tables stay untouched.
5. **API:** new `/live/*` routes in the existing FastAPI app.

## 4. Decision 1: Broker = Redpanda (Kafka API)

| Option | For | Against |
| --- | --- | --- |
| Kafka | Industry standard, most jobs ask for it | Needs ZooKeeper or KRaft setup, heavy for a laptop |
| **Redpanda (chosen)** | Kafka-compatible API, one binary, one docker service, fast on a laptop | Smaller community |
| Redis Streams | Simplest | Weak retention and replay story, not what companies use for this |

The client code uses the Kafka protocol (`confluent-kafka`), so moving to real Kafka later changes only the connection string. State this in the README.

Topic config: `trips.raw` with 6 partitions, keyed by `pu_location_id`, 24 h retention. Same zone always lands in the same partition, so per-zone ordering is preserved.

## 5. Decision 2: Window = 5-minute tumbling, by event time

- **Tumbling, not sliding:** each event belongs to exactly one window. Simpler, cheaper, and easy to make idempotent. A "last 30 minutes" view is just the sum of the last 6 windows at query time.
- **Event time, not arrival time:** a trip belongs to the window of its `pickup_datetime`, even if it arrives late. Arrival time would make counts wrong whenever the consumer lags.
- Window key: `(window_start, pu_location_id)`. Metrics: `trip_count`, `fare_sum`, `distance_sum`.

## 6. Decision 3: Late events = watermark with 10-minute allowed lateness

- Watermark = max event time seen so far minus 10 minutes, **tracked per Kafka partition**. An event is judged against the watermark of its own partition.
- Why per partition: with one global watermark, a consumer that is behind reads one partition far ahead of the others, which pushes the watermark forward and marks every event of the slower partitions as too late (measured: 22,748 and 25,344 of 50,000 events wrongly dropped, different each run). Order is only guaranteed inside a partition, so lateness must be measured inside it.
- The watermark reported to the API (`as_of`, `/live/health`) is the **minimum** across partitions that have seen events. Known limit: an idle partition holds this reported watermark back. It does not affect too-late decisions.
- Event with window_end above the watermark: merged into its window normally. Late but inside the allowed lateness, so the window is still updated.
- Event with window_end below the watermark: too late. Written to `trips.dlq` with reason `too_late` and counted in a `late_dropped` metric. Never silently dropped.
- Why 10 minutes: the Ticket 1 data shows how far pickup times scatter inside a file. The replayer will also inject random lateness, so this value is tested rather than guessed. Tune it after measuring.

Tradeoff stated plainly: a longer allowed lateness gives more accurate counts but a slower final answer and more memory. 10 minutes is the starting value.

## 7. Decision 4: Correctness = at-least-once + idempotent writes

The consumer can crash after writing to ClickHouse but before committing its Kafka offset. On restart it re-reads some events. Plan:

- Events are grouped per partition into fixed offset buckets (`bucket_id = offset // 5000`).
- Each flush writes the CUMULATIVE totals of a bucket, with `version` = last offset folded in. Flushing a half-full bucket twice just writes a higher version of the same row key.
- Offsets are committed only after ClickHouse accepts the insert.
- On restart each partition is rewound to the START of its bucket, so the bucket is rebuilt from scratch and produces identical (or larger) totals.
- `ReplacingMergeTree(version)` keeps the newest row per key. The API uses `argMax(..., version)` so duplicates never show before the background merge.

Why not a batch-id dedup token: flush boundaries depend on timing, so a replayed batch after a crash may not match the original batch. Fixed offset buckets make replays deterministic.

Known limit: the too-late decision depends on the watermark, which is rebuilt from the replay. DLQ messages for too-late events can be duplicated after a crash. DLQ readers must dedupe on `event_id`.

## 8. Data Model

```sql
CREATE TABLE taxi_analytics.live_zone_windows (
  window_start DateTime, pu_location_id UInt16, kafka_partition UInt16,
  bucket_id UInt64, version UInt64,
  trip_count UInt32, fare_sum Float64, distance_sum Float64,
  ingested_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(window_start)
ORDER BY (window_start, pu_location_id, kafka_partition, bucket_id)
TTL ingested_at + INTERVAL 7 DAY;
```
(Full DDL in `02_live_schema.sql`.)

TTL is on `ingested_at` (wall-clock insert time), not `window_start`. A TTL on `window_start` deletes any replayed historical data at the next merge, because replayed events are months old (found while testing: about 6,800 of 27,000 accepted events vanished).

Flush every 1 s or 5,000 events, whichever comes first. ClickHouse prefers few large inserts over many small ones.

## 9. API

- `GET /live/zones/top?minutes=15&limit=10`: top pickup zones for the last N minutes.
- `GET /live/zone/{id}?minutes=60`: window series for one zone.
- `GET /live/health`: consumer lag, last event time, late_dropped count, DLQ size.

Each response includes `as_of` (watermark time), so the client knows how fresh the data is.

## 10. Failure Modes

| Failure | Behaviour |
| --- | --- |
| Consumer crashes | Restarts from last committed offset, replayed batches deduplicate (section 7) |
| ClickHouse down | Consumer retries with backoff, does not commit offsets, lag grows, `/live/health` shows it |
| Poison message (bad JSON) | Sent to `trips.dlq` with reason, consumer continues |
| Burst above capacity | Lag grows, nothing is lost, kept as a measured number in the benchmark |
| Broker restarts | Topic data persists on a docker volume |

## 11. Testing and Benchmarks

- Unit tests: window assignment, watermark logic, late/too-late boundaries.
- Integration test: publish 100k events, kill the consumer mid-run, restart, assert final counts equal the batch table for the same time range (the batch pipeline is the ground truth).
- Benchmarks to report in README with real numbers: sustained events/sec, p50/p95 end-to-end latency, `/live` p95, consumer lag under burst, duplicate count after a crash test (must be 0).

## 12. Milestones

| Date | Deliverable |
| --- | --- |
| 28 Nov | Working pipeline: replayer, broker, consumer, table, one `/live` endpoint |
| 30 Nov | Crash-recovery test and late-event test passing |
| 1 Dec | Benchmark numbers and README updated |

## 13. Open Questions (to be answered with measurements, not guesses)

1. Is the 10-minute lateness right for the replayed data?
2. Does the single Python consumer reach 5,000 events/sec, or does it need one consumer per partition?
3. Is `argMax` collapsing fast enough on the live API once the table holds hours of data?

## 14. Changes after first end-to-end run (24 Nov)

| Change | Reason | Evidence |
| --- | --- | --- |
| Watermark per partition (section 6) | Global watermark dropped slow partitions' events during backlog | 25,344 of 50,000 wrongly too_late before, 0 after; regression test in `test_windowing.py` |
| TTL on `ingested_at` (section 8) | TTL on `window_start` deleted replayed history | deduplicated live total matched `accepted` only after the change |
| UTC tagging in `cleaning.py` (Ticket 1) | Batch table stored pickup times 5h30m early on an IST client | live vs batch disagreed until batch was re-ingested with `_wallclock_as_utc`; now 0 mismatched cells over 10,671 |
