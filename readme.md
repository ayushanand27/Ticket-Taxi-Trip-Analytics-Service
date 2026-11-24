# NYC Taxi Trip Analytics Service

An end-to-end analytical data pipeline and sub-300 ms REST API built over **12 months of [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)** (`2025-10` through `2026-09`).

* **Total Processed:** 46,494,548 rows
* **Clean Rows (`taxi_trips_clean`):** 43,533,390
* **Quarantined Rows (`taxi_trips_rejects`):** 2,961,158 (6.37% dirty rate)
* **Unit Tests:** 23/23 passing (`pytest`)
* **API p95 Latency:** 219.1 ms – 231.1 ms across all 4 endpoints (target: < 300 ms)

---

## Architecture & Design Choices

### 1. Why ClickHouse Instead of PostgreSQL?
All four API endpoints are analytical `GROUP BY` aggregations across 43.5M rows.
* **Columnar Storage (`MergeTree`):** ClickHouse reads only the 2–4 columns referenced by a query instead of scanning full row tuples.
* **Incremental Pre-Aggregation (`MaterializedView` + `SummingMergeTree`):** As `ingest.py` inserts clean rows into `taxi_trips_clean`, ClickHouse automatically updates three summary tables (`hourly_zone_stats`, `distance_bucket_stats`, `daily_trip_counts`) in real time.
* **Compact Types & Partitioning:** Partitioning by `toYYYYMM(pickup_datetime)` and ordering by `(pickup_datetime, pu_location_id, trip_hash)` with `LowCardinality`, `UInt16`, and `Float32` keeps storage compact and pruning fast.
* **Persistent Thread-Local Client (`api.py`):** Reuses one ClickHouse HTTP client per Uvicorn worker thread via `threading.local()`, avoiding per-request connection handshake overhead while remaining thread-safe.

### 2. Parallel, Idempotent Ingestion (`ingest.py`)
Re-running `ingest.py` never creates duplicates:
1. **Parallel Workers (`--workers 4`):** Uses `ProcessPoolExecutor` to ingest multiple monthly Parquet files concurrently across CPU cores.
2. **Bounded-Memory Streaming:** Each Parquet file is streamed in 50,000-row Arrow batches (`pyarrow.parquet.ParquetFile.iter_batches`) so worker RAM stays flat.
3. **SHA-256 + `ingestion_runs` Ledger:** Before loading a file, the pipeline computes its SHA-256 checksum and checks `taxi_analytics.ingestion_runs`. Completed files are skipped (`SKIP` — verified at 2.0s for all 12 files on re-run); failed runs have partial rows synchronously deleted (`SETTINGS mutations_sync = 1`) before retrying.

### 3. Bad Data Quarantine & Reject Breakdown (`cleaning.py`)
Every row passes through a pure, unit-tested validator (`validate_trip_row`) before insertion. Invalid rows are never silently dropped; they are written to `taxi_trips_rejects` with a standardized `reject_reason` and serialized JSON `raw_payload`.

**Observed Reject Breakdown (2,961,158 total rejected rows across 12 months):**

| `reject_reason` | Count | Share of Rejects | Rule |
| :--- | ---: | ---: | :--- |
| `invalid_trip_distance` | 1,471,397 | 49.69% | `trip_distance <= 0` or `trip_distance > 500` miles |
| `non_positive_fare` | 881,752 | 29.78% | `fare_amount <= 0` or `total_amount <= 0` |
| `dropoff_before_pickup` | 607,994 | 20.53% | `tpep_dropoff_datetime <= tpep_pickup_datetime` |
| `out_of_range_date` | 15 | 0.00% | Stray timestamps outside `2025-01-01` – `2026-12-31` |
| `missing_required_field` | 0 | 0.00% | Null or `NaN` in core timestamps, zones, distance, or fare |

---

## Before-and-After Performance Benchmark (43,533,390 Clean Rows)

Measured over 20 iterations per query (`python benchmark.py --iterations 20 --api-url http://localhost:8000`) on Windows 11 + Docker Desktop:

### ClickHouse Query Latency (Raw `taxi_trips_clean` vs. Materialized Views)

| Query | BEFORE p95 (ms) | AFTER p95 (ms) | Speedup |
| :--- | ---: | ---: | ---: |
| 1. Top pickup zones (`hour=18`) | 133.8 | 71.1 | 1.9x |
| 2. Avg fare by distance bucket | 227.9 | 65.1 | 3.5x |
| 3. Daily trip count (12 months) | 142.4 | 62.5 | 2.3x |
| 4. Peak-hour heatmap (`7x24`) | 197.6 | 61.7 | 3.2x |

### Live FastAPI HTTP Endpoint Latency (With Thread-Local Connection Reuse)

| Endpoint | p50 (ms) | p95 (ms) | Target (< 300 ms) |
| :--- | ---: | ---: | :---: |
| `GET /analytics/top-pickup-zones?hour=18&limit=10` | 207.9 | 231.1 | PASS |
| `GET /analytics/fare-by-distance` | 201.6 | 219.1 | PASS |
| `GET /analytics/daily-trips?start_date=2025-10-01&end_date=2026-09-30` | 206.1 | 220.1 | PASS |
| `GET /analytics/peak-hour-heatmap` | 206.0 | 221.3 | PASS |

---

## Project Structure

```text
.
├── DESIGN.md              # One-page architecture & schema design doc
├── docker-compose.yml     # ClickHouse 24.8 container + persistent volume
├── requirements.txt       # Python dependencies
├── 01_schema.sql          # ClickHouse tables & Materialized Views DDL
├── db_init.py             # Schema initializer & taxi_zones CSV seeder
├── cleaning.py            # Pure row validation & JSON quarantine helpers
├── test_cleaning.py       # 23 pytest unit tests for cleaning logic
├── ingest.py              # Parallel, idempotent Parquet -> ClickHouse loader
├── api.py                 # FastAPI service (4 analytics endpoints + /health)
├── benchmark.py           # Before/after ClickHouse & FastAPI p95 benchmark
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
4. **Run unit tests:**
   ```powershell
   python -m pytest test_cleaning.py -v
   ```
5. **Run parallel, idempotent ingestion:**
   ```powershell
   python ingest.py --data-dir nyc_yellow_taxi --workers 4
   ```
6. **Start the API & run the benchmark:**
   ```powershell
   python -m uvicorn api:app --host 0.0.0.0 --port 8000
   python benchmark.py --iterations 20 --api-url http://localhost:8000
   ```
