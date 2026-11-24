# NYC Taxi Trip Analytics Service

An end-to-end analytical data pipeline and sub-300 ms REST API built over **12 months of [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)** (`2025-10` through `2026-09`).

* **Total Processed:** 46,494,548 rows
* **Clean Rows (`taxi_trips_clean`):** 43,533,390
* **Quarantined Rows (`taxi_trips_rejects`):** 2,961,158 (6.37% dirty rate)
* **Unit & Pipeline Tests:** 27 tests (`test_cleaning.py` + `test_pipeline.py`)
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

**Measured Ingestion Throughput (Vectorized PyArrow):**

| Mode | Files / Rows | Wall-Clock Time | Effective Throughput |
| :--- | :--- | ---: | ---: |
| Sequential (`--workers 1`) | 1 file (`2025-10`, 4,428,699 rows) | 94.9s (101.6s w/ rebuild) | ~46,667 rows/sec |
| Parallel (`--workers 4`) | 4 files (`2025-10`–`2026-01`, 16,640,038 rows) | 109.2s total | **~152,381 rows/sec (3.3x)** |

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
├── test_pipeline.py       # API, aggregate-idempotency, & parallel-ingestion tests
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
* `test_pipeline.py` uses a fake ClickHouse client, so it checks the ingestion logic and not the real SQL. A test against the Docker container is still to do.
* The parallel speedup (3.3x) compares different file sets (1 file vs 4 files); a same-files 1-worker vs 4-worker run is still to do.
