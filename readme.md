# NYC Taxi Trip Analytics Service

An end-to-end analytical data pipeline and sub-300 ms REST API built over **12 months of [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)** (`2025-10` through `2026-09`).

* **Total Processed:** 46,494,548 rows
* **Clean Rows (`taxi_trips_clean`):** 43,533,390
* **Quarantined Rows (`taxi_trips_rejects`):** 2,961,158 (6.37% dirty rate)
* **Tests:** 27 unit/pipeline tests (`test_cleaning.py` + `test_pipeline.py`) + 5 integration tests against a real ClickHouse (`test_integration.py`), all passing
* **API p95 Latency:** 10.6 ms – 13.8 ms across all 4 endpoints (target: < 300 ms)

---

## Architecture & Design Choices

### 1. Why ClickHouse Instead of PostgreSQL?
All four API endpoints are analytical `GROUP BY` aggregations across 43.5M rows.
* **Columnar Storage (`MergeTree`):** ClickHouse reads only the 2–4 columns referenced by a query instead of scanning full row tuples.
* **Incremental Pre-Aggregation (`MaterializedView` + `SummingMergeTree`):** As `ingest.py` inserts clean rows into `taxi_trips_clean`, ClickHouse automatically updates three summary tables (`hourly_zone_stats`, `distance_bucket_stats`, `daily_trip_counts`) in real time.
* **Compact Types & Partitioning:** Partitioning by `toYYYYMM(pickup_datetime)` and ordering by `(pickup_datetime, pu_location_id, trip_hash)` with `LowCardinality`, `UInt16`, and `Float32` keeps storage compact and pruning fast.
* **Persistent Thread-Local Client & `127.0.0.1` (`api.py`):** Reuses one ClickHouse HTTP client per worker thread via `threading.local()`, saving ~25 ms per request. `CH_HOST` and the benchmark URL default to `127.0.0.1` instead of `localhost`: HTTP p50 fell from ~200 ms to ~10 ms after this change (see the benchmark section). The likely cause is slow `localhost` resolution (IPv6 `::1` tried first) on Windows + Docker Desktop; this was not isolated further.

### 2. Parallel, Vectorized, Idempotent Ingestion (`ingest.py`)
Re-running `ingest.py` never creates duplicates in either raw or summary tables:
1. **Vectorized PyArrow Validation (`cleaning.py`):** `validate_arrow_batch()` evaluates all 5 cleaning rules in C++ via `pyarrow.compute` on 50,000-row batches instead of looping over 46.5M rows as Python dicts.
2. **Parallel Workers (`--workers 4`):** Uses `ProcessPoolExecutor` to ingest multiple monthly Parquet files concurrently across CPU cores.
3. **Full Idempotency (Raw + Summary Rebuild):** Checks `(source_file, file_sha256)` in `ingestion_runs`. Completed files are skipped (`SKIP` in 2.0s for all 12 files). If any file is retried after failure or with `--force`, partial raw rows are synchronously deleted (`SETTINGS mutations_sync = 1`) and `rebuild_summary_tables()` truncates and rebuilds all 3 `SummingMergeTree` tables from `taxi_trips_clean` (~1.6s for 43.5M rows) so summary metrics never double-count.

**Measured Ingestion Throughput (same 4 files for every run, `bench_ingest.py`):**

| Workers | Files | Rows | Wall-clock (s) | Rows/sec | Speed-up |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4 (`2025-10`–`2026-01`) | 16,640,038 | 141.0 | 118,001 | 1.00x |
| 4 | 4 (`2025-10`–`2026-01`) | 16,640,038 | 93.9 | 177,227 | **1.50x** |

Parallelism helps, but far less than 4x. Per-file load time went from ~28-34 s (1 worker) to ~59-81 s (4 workers at once), so the workers are competing for something shared rather than scaling independently. Candidate causes, **not yet isolated**: the single ClickHouse container and its merges, Docker Desktop on Windows, and `clickhouse-connect` running its pure-Python fallback because an Application Control policy blocked its C extension on this machine. About 11 s of each run is the same fixed cost (deleting old rows, rebuilding the summary tables), which slightly understates the parallel gain.

### 3. Bad Data Quarantine & Reject Breakdown (`cleaning.py`)
Every row passes through `validate_trip_row()` / `validate_arrow_batch()` before insertion. Invalid rows are quarantined in `taxi_trips_rejects` with a standardized `reject_reason` and serialized JSON `raw_payload`:

| `reject_reason` | Count | Share of Rejects | Rule |
| :--- | ---: | ---: | :--- |
| `invalid_trip_distance` | 1,471,397 | 49.69% | `trip_distance <= 0` or `trip_distance > 500` miles |
| `non_positive_fare` | 881,752 | 29.78% | `fare_amount <= 0` or `total_amount <= 0` |
| `dropoff_before_pickup` | 607,994 | 20.53% | `tpep_dropoff_datetime <= tpep_pickup_datetime` |
| `out_of_range_date` | 15 | 0.00% | Stray timestamps outside `2025-01-01` – `2026-12-31` |
| `missing_required_field` | 0 | 0.00% | Null or `NaN` in core timestamps, zones, distance, or fare |

**Distance Reject Split & Business Context:**
Querying `raw_payload` inside `invalid_trip_distance` shows **1,470,312** rows (99.93%) are `trip_distance = 0`, **1,085** rows (0.07%) are `> 500` miles, and **0** are `< 0`. Zero-distance rows are often **cancelled trips, passenger no-shows, or negotiated flat-rate rides** where the meter never rolled—a product/analytics definition question (whether to count cancelled/flat-rate rides in demand heatmaps) rather than purely corrupt sensor data.

---

## Before-and-After Performance Benchmark (43,533,390 Clean Rows)

Measured over **200 iterations** per query (`python benchmark.py --iterations 200 --api-url http://127.0.0.1:8000`) on Windows 11 + Docker Desktop:

### ClickHouse Query Latency (Raw `taxi_trips_clean` vs. Materialized Views)

| Query | BEFORE p95 (ms) | AFTER p95 (ms) | Speedup |
| :--- | ---: | ---: | ---: |
| 1. Top pickup zones (`hour=18`) | 92.4 | 10.6 | **8.7x** |
| 2. Avg fare by distance bucket | 130.3 | 8.6 | **15.2x** |
| 3. Daily trip count (12 months) | 101.8 | 8.7 | **11.8x** |
| 4. Peak-hour heatmap (`7x24`) | 121.4 | 9.0 | **13.4x** |

### Live FastAPI HTTP Endpoint Latency (200 Iterations on `127.0.0.1`)

| Endpoint | DB p95 (ms) | Handler p95 (ms) | HTTP p50 (ms) | HTTP p95 (ms) | Target (< 300 ms) |
| :--- | ---: | ---: | ---: | ---: | :---: |
| `GET /analytics/top-pickup-zones?hour=18&limit=10` | 11.2 | 11.2 | 10.1 | 13.3 | PASS |
| `GET /analytics/fare-by-distance` | 8.8 | 8.8 | 8.3 | 10.6 | PASS |
| `GET /analytics/daily-trips?start_date=2025-10-01&end_date=2026-09-30` | 9.3 | 9.3 | 8.9 | 12.7 | PASS |
| `GET /analytics/peak-hour-heatmap` | 9.8 | 9.9 | 8.5 | 13.8 | PASS |

**History.** An earlier run (before the `127.0.0.1` default) showed HTTP p50 of ~200 ms with the DB at ~16-20 ms. Switching from `localhost` to `127.0.0.1` brought HTTP p50 to ~10 ms, so about 190 ms per request was name-resolution overhead, not the database or the handler. Two runs on different days are not a controlled A/B test, but the DB and handler times above now match the HTTP times, so nothing is left unaccounted for. The BEFORE column moves between runs (for example 157 ms vs 92 ms for query 1), which is cache and machine-load variance.

---

## Project Structure

```text
.
├── design.md              # One-page architecture & schema design doc
├── docker-compose.yml     # ClickHouse 24.8 container + persistent volume
├── requirements.txt       # Python dependencies
├── 01_schema.sql          # ClickHouse tables & Materialized Views DDL
├── db_init.py             # Schema initializer & taxi_zones CSV seeder
├── cleaning.py            # Row-level & vectorized PyArrow validation logic
├── test_cleaning.py       # 23 pytest unit tests for cleaning logic
├── test_pipeline.py       # API, aggregate-idempotency, & parallel-ingestion tests (fake ClickHouse)
├── test_integration.py    # Opt-in tests against a real ClickHouse, in a throw-away database
├── bench_ingest.py        # Same-files 1-worker vs N-worker ingestion timing
├── ingest.py              # Parallel, vectorized, idempotent Parquet loader
├── api.py                 # FastAPI service (4 endpoints + /health + timing headers)
├── benchmark.py           # 200-iter before/after ClickHouse & FastAPI p95 benchmark
└── .gitignore             # Excludes raw Parquet/CSV files & local caches
```

---

## Quickstart (Windows PowerShell)

1. **Download 12 months of Yellow Taxi Parquet files + [Taxi Zone Lookup CSV](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv)** into `nyc_yellow_taxi/`.
2. **Start ClickHouse & install dependencies:**
   ```powershell
   docker compose up -d
   python -m pip install -r requirements.txt
   ```
3. **Initialize schema & seed taxi zones:**
   ```powershell
   python db_init.py --schema 01_schema.sql --zones nyc_yellow_taxi/taxi_zone_lookup.csv
   ```
4. **Run all unit & pipeline tests:**
   ```powershell
   python -m pytest test_cleaning.py test_pipeline.py -v
   ```
5. **Run parallel, vectorized, idempotent ingestion:**
   ```powershell
   python ingest.py --data-dir nyc_yellow_taxi --workers 4
   ```
6. **Start the API & run the 200-iteration benchmark:**
   ```powershell
   python -m uvicorn api:app --host 127.0.0.1 --port 8000
   python benchmark.py --iterations 200 --api-url http://127.0.0.1:8000
   ```

---

## Benchmark Method & Known Limitations

**Method.** Each query runs 1 warm-up call, then N timed calls (default 200), against a local Docker ClickHouse. p95 is linearly interpolated. Run on a warm server (cache state affects the BEFORE/AFTER numbers a lot, so re-run before quoting them).

**Limits.**
* `clickhouse-connect` fell back to pure Python on the benchmark machine (an Application Control policy blocked its C extension). This is harmless here but slower on large result sets.
* `rebuild_summary_tables()` truncates then refills the three summary tables, so the API can briefly see empty aggregates during a rebuild.
* `test_pipeline.py` uses a fake ClickHouse client, so it checks the ingestion logic and not the real SQL. `test_integration.py` covers the real SQL in a separate `taxi_analytics_itest` database (it never touches the real tables). It is opt-in: `$env:RUN_CH_INTEGRATION="1"; python -m pytest test_integration.py -v`. It passed 5/5 on the author's machine (Windows 11, Docker Desktop, ClickHouse 24.8), including the negative control that proves the double-counting bug is real. Re-run on 24 Nov 2026 after the timestamp re-ingest: 5 passed.
* Parallel ingestion scales to only 1.50x on 4 workers (table above). The earlier "3.3x" figure compared different file sets and was withdrawn. Finding the shared bottleneck is open work.

## Ticket 2: Live Trip Stream

Design: see `design_stream.md`.

### Run it

```powershell
pip install -r requirements.txt
docker compose up -d --wait
# Auto-create is OFF so a typo in a topic name fails loudly. Stored in the redpanda volume; re-run if you wipe it.
docker exec taxi_redpanda rpk cluster config set auto_create_topics_enabled false
python db_init.py --schema 02_live_schema.sql --zones nyc_yellow_taxi/taxi_zone_lookup.csv
docker exec taxi_redpanda rpk topic create trips.raw -p 6 -c retention.ms=86400000
docker exec taxi_redpanda rpk topic create trips.dlq -p 1
```

**Consumer and replayer run in a Linux container.** On some Windows machines Application Control blocks the native `confluent-kafka` DLL (`ImportError: DLL load failed while importing cimpl`). Same code, same numbers, so benchmarks stay valid. Inside the container the brokers are `redpanda:9092` and ClickHouse is `clickhouse` (set `CH_HOST`).

```powershell
docker run -d --name taxi_py --network day1_default -v "${PWD}:/app" -w /app -e CH_HOST=clickhouse python:3.12-slim sleep infinity
docker exec taxi_py pip install -q clickhouse-connect pyarrow confluent-kafka

# terminal 1: consumer
docker exec taxi_py python -m stream.consumer --brokers redpanda:9092
# terminal 2: replayer (--limit 50000 first; it loads the whole month into memory)
docker exec taxi_py python -m stream.replayer --file nyc_yellow_taxi/yellow_tripdata_2025-10.parquet --speed 0 --limit 50000 --brokers redpanda:9092
# add --speed 600 for a paced stream (1 s = 10 min of trips), --late-pct 5 to inject late events

# terminal 3: API on the host (use another port if 8000 is taken)
uvicorn api:app --port 8000
curl "http://127.0.0.1:8000/live/zones/top?minutes=15"
curl "http://127.0.0.1:8000/live/health"
```

### Known limits

* **An idle partition holds back the global `as_of` watermark.** The watermark is tracked per partition and the reported one is the minimum, so a partition that receives no events stops it from advancing. Too-late decisions are not affected, because each event is judged against its own partition.

### Verified so far (50,000 events of 2025-10)

* No late injection: `too_late = 0` in 3 of 3 runs, deduplicated live total = 50,000.
* `--late-pct 5`: 429 and 377 events went to `trips.dlq` (about 0.8%). Expected about 1/6 of the late 5% (delays of 0-15 min against 10 min allowed lateness), so about 416.
* Live vs batch (`taxi_trips_clean`) for all fully covered 5-minute windows: 10,671 (window, zone) cells, 49,663 trips, fare sum 1,012,241.75, distance sum 171,323.69. 0 mismatched cells.
* Two bugs found while running this: a 7-day TTL on `window_start` deleted replayed history (now TTL on `ingested_at`), and one global watermark marked slow partitions' events too late during a backlog (now per partition, with a regression test).

### Live stream results

Measured on 24-25 Nov 2026 with `trips.raw` (6 partitions), 2025-10 data, consumer and replayer in Linux containers. Every number below is from a real run. Benchmark figures are medians of 3 runs, and nothing is rounded in our favour.

**Crash recovery** (50,000-event slice, `kill -9` on the consumer container, exit code 137). Live vs batch compares every (window, zone) cell of all fully covered 5-minute windows: 10,671 cells, 49,663 trips, fare sum 1,012,241.75, distance sum 171,323.69, identical on both sides in every run.

| Run | Killed at (committed of 50,000) | Consumer restarted from | Mismatched cells | Duplicate keys after FINAL | Events counted | Lost |
|---|---|---|---|---|---|---|
| B2 | 11,897 (24%) | bucket starts, all 6 partitions at 0 | 0 | 0 | 50,000 | 0 |
| B | 15,986 (32%) | bucket starts, all 6 partitions at 0 | 0 | 0 | 50,000 | 0 |
| A | 26,086 (52%) | bucket starts 0/0/0/5000/5000/0 | 0 | 0 | 50,000 | 0 |
| C | 37,940 (76%) | bucket starts 0/5000/5000/5000/5000/0 | 0 | 0 | 50,000 | 0 |
| D50 (10 s timeout) | 27,711 (55%) | bucket starts 0/0/0/5000/5000/0 | 0 | 0 | 50,000 | 0 |
| D50rt (10 s timeout) | 28,719 (57%) | bucket starts 0/5000/0/5000/5000/0 | 0 | 0 | 50,000 | 0 |
| D50rt2 (10 s timeout) | 26,876 (54%) | bucket starts 0/0/0/5000/5000/0 | 0 | 0 | 50,000 | 0 |

The consumer does not replay from offset 0 and does not resume at the exact committed offset. It rewinds each partition to the start of its 5,000-event bucket (`committed // 5000 * 5000`), and this matched in every run. A partition whose committed offset is under 5,000 restarts at 0. Raw row counts exceeded distinct keys by 7 to 2,304 rows in the 50,000-event runs (older cumulative versions that `FINAL` / `argMax` collapse).

Crash during a 100,000-event burst on top of 800,060 events already in the topic (900,060 in total), 3 runs: 0 duplicate keys after FINAL, 0 lost, 0 mismatched cells, with 153,150 cells and 899,607 trips equal on both sides in all 3.

**Recovery time before and after the timeout change** (`session.timeout.ms` 45000 default -> 10000, `heartbeat.interval.ms` 3000):

| Setting | Runs | Recovery time |
|---|---|---|
| Default (`session.timeout.ms` 45000) | 3 (100k-burst crash) | 46 s, 45 s, 47 s (from `docker start` until fully caught up) |
| `session.timeout.ms` 10000, `heartbeat.interval.ms` 3000 | 2 (50,000-event crash, immediate restart) | 6.0 s, 6.8 s (from `docker start` until the first commit past the kill point) |

The two rows do not end at the same point. The "before" figure runs until fully caught up, and the "after" figure stops at the first commit. A 100k backlog drains in about 1.5 s here, so a like-for-like "after" figure is probably 1-2 s higher than shown. It was not measured that way.

**Late events** (`--late-pct 5`, 50,000 events, 10-minute allowed lateness):

| Measure | Result |
|---|---|
| Events delivered out of order | 2,536 (5.07%) |
| Accepted within the 10-minute allowance | 2,107 (83.1% of late events) |
| Sent to `trips.dlq` | 429 (16.9% of late events, 0.86% of all events) |
| Accepted + DLQ vs total | 49,571 + 429 = 50,000 |
| DLQ ids vs an offline simulation of the per-partition watermark | identical (429 = 429) |
| Live vs batch, DLQ'd events removed from the batch side | 10,623 cells, 0 mismatched |

**Benchmarks** (3 runs; consumer, API, Redpanda, ClickHouse and the load generator all on one laptop):

| Metric | Run 1 | Run 2 | Run 3 | Median |
|---|---|---|---|---|
| Consumer throughput draining a 500,000-event backlog (events/s) | 53,794 | 69,181 | 65,622 | 65,622 |
| Time to drain that backlog | 9.29 s | 7.23 s | 7.62 s | 7.62 s |
| Peak consumer lag during the burst (events) | 294,999 | 279,999 | 284,999 | 284,999 |
| Sustained 5,000 events/s: achieved rate (events/s) | 5,000.5 | 5,000.2 | 5,000.05 | 5,000.2 |
| Lag at 5,000 events/s, max (events) | 5,323 | 5,303 | 5,316 | 5,316 |
| Lag at 5,000 events/s, mean (events) | 2,705 | 2,800 | 2,893 | 2,800 |
| End-to-end latency p50 (produced to visible in `/live`) | 0.5005 s | 0.5075 s | 0.4994 s | 0.5005 s |
| End-to-end latency p95 | 0.5643 s | 0.5727 s | 0.5916 s | 0.5727 s |
| End-to-end latency max | 1.0025 s | 0.9548 s | 1.0145 s | 1.0025 s |
| `/live` latency p50 under load | 33.8 ms | 37.2 ms | 37.7 ms | 37.2 ms |
| `/live` latency p95 under load | 78.0 ms | 85.7 ms | 88.2 ms | 85.7 ms |
| `/live` errors / requests | 0 / 3,036 | 0 / 2,934 | 0 / 2,894 | 0 |
| Probes seen / sent | 60 / 60 | 60 / 60 | 60 / 60 | 60 / 60 |
| Duplicate keys after a crash | 0 | 0 | 0 | 0 |

`/live` p95 by endpoint, worst run (run 3): `/live/zones/top` 95.4 ms, `/live/zone/{id}` 88.1 ms, `/live/health` 42.5 ms.

**Design targets vs measured** (targets from `design_stream.md`):

| Target | Measured | Verdict |
|---|---|---|
| 5,000 events/s sustained | 5,000.05 to 5,000.5 events/s achieved, lag max 5,303 to 5,323 events, drained 1.5 s after the producer stopped | Met |
| 20,000 events/s burst | 500,000-event backlog drained at 53,794 to 69,181 events/s (produced at 118,017 to 165,432 events/s) | Met |
| Event visible in the live API within 5 s | p95 0.5727 s (median of 3), worst max 1.0145 s | Met |
| `/live` p95 under 100 ms | 78.0 to 88.2 ms overall; 95.4 ms for `/live/zones/top` in run 3 | Met, with a 4.6 ms margin on the slowest endpoint |
| No duplicate counting after a crash or replay | 0 duplicate keys, 0 lost, 0 mismatched cells in 10 crash runs (7 x 50,000 events, 3 x 900,060 events) | Met |
| Events up to 10 min late correct, older ones quarantined not lost | 83.1% of late events accepted, 16.9% to the DLQ, accepted + DLQ = total | Met |

**The 0.5 s latency is set by the consumer's flush interval, `--flush-secs` (default 1.0 s; `--flush-events` 5,000 flushes sooner under load).** The worst latency seen (1.0145 s) is about one flush interval.

**Caveats**

* **Everything shares one laptop.** Consumer, API, Redpanda, ClickHouse and the load generator all ran on it, so these are single-machine numbers.
* **Where the load generators ran.** The load generator and the `/live` clients ran in Linux containers, not on the Windows host.
* **Spread in run 1.** Run 1 had a slower burst (53,794 events/s vs 69,181 and 65,622), so the burst figure is a median, not a best case.
* **Crash timing in the 100k-burst test.** The kill landed at about 95% of the burst, not midway (committed 895,061 of 900,060, because the poller lagged a fast consumer). The 50,000-event runs cover 24% to 76%.
* **The insert-to-commit window is unproven.** No run is proven to have died in the narrow gap between the ClickHouse insert and the offset commit. The design handles that case by rewinding to the bucket start, but these tests do not isolate it.
* **Mid-stream slice artefact.** The replayed 50,000-event slice starts mid-window, so the first 5-minute window is only partly covered. One early check showed 62 mismatched cells, all in that first window, with 0 mismatches in every other window. The checks now skip the first window, so the "0 mismatched cells" figures above exclude it.
* **Recovery time depends on the session timeout.** At the default it was 45 to 47 s. At 10 s it was 6.0 and 6.8 s (2 runs, different end point, see above).
* **End-to-end latency probes may be phase-locked to the flush cycle.** Each probe is sent about 0.5 s after the previous one was seen, so the probe cadence can line up with the 1 s flush cycle. The tight p50 to p95 spread (0.50 to 0.57 s) may understate the spread with unsynchronised arrivals. The max (about 1.0 s) is the bound to rely on.

### Ticket 1 timestamp bug (fixed 24 Nov 2026)

Found while comparing live and batch windows: every pickup/dropoff time in `taxi_trips_clean` was stored 5 h 30 min early. Cause: `clickhouse-connect` converts a naive datetime to an epoch using the client machine's local timezone (IST on the ingesting laptop). Fix: `_wallclock_as_utc` in `cleaning.py` tags pickup and dropoff as UTC in both the batch and single-row paths, with a regression test (`test_timestamps_are_tagged_utc_so_client_timezone_cannot_shift_them`). All 43,533,390 rows were re-ingested on 24 Nov 2026 in 342 s with 4 workers (about 127k rows/s, in line with the 1.50x table above) and the summary tables rebuilt. Row counts per file are unchanged, and the hour-of-day pattern is now realistic (night low, evening peak). Live and batch now agree directly, with 0 mismatched cells and no time adjustment.
