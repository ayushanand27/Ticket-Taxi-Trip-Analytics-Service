# NYC Taxi Trip Analytics Service

An end-to-end analytical data pipeline and sub-300 ms REST API built over **12 months of [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)** (`2025-10` through `2026-09`).

* **Total Processed:** 46,494,548 rows
* **Clean Rows (`taxi_trips_clean`):** 43,533,390
* **Quarantined Rows (`taxi_trips_rejects`):** 2,961,158 (6.37% dirty rate)
* **Unit Tests:** 23/23 passing (`pytest`)
* **API p95 Latency:** 245.1 ms – 257.3 ms across all 4 endpoints (target: < 300 ms)

---

## Architecture & Design Choices

### 1. Why ClickHouse Instead of PostgreSQL?
All four API endpoints are analytical `GROUP BY` aggregations across 43.5M rows.
* **Columnar Storage (`MergeTree`):** ClickHouse reads only the 2–4 columns referenced by a query instead of scanning full row tuples.
* **Incremental Pre-Aggregation (`MaterializedView` + `SummingMergeTree`):** As `ingest.py` inserts clean rows into `taxi_trips_clean`, ClickHouse automatically updates three summary tables (`hourly_zone_stats`, `distance_bucket_stats`, `daily_trip_counts`) in real time.
* **Compact Types & Partitioning:** Partitioning by `toYYYYMM(pickup_datetime)` and ordering by `(pickup_datetime, pu_location_id, trip_hash)` with `LowCardinality`, `UInt16`, and `Float32` keeps storage compact and pruning fast.

### 2. Idempotent Ingestion (`ingest.py`)
Re-running `ingest.py` never creates duplicates:
1. Each Parquet file is streamed in 50,000-row Arrow batches (`pyarrow.parquet.ParquetFile.iter_batches`) to keep memory flat.
2. Before loading a file, the pipeline computes its **SHA-256 checksum** and checks `taxi_analytics.ingestion_runs`.
3. If `(source_file, file_sha256)` is already `completed`, the file is skipped (`SKIP` — verified at 2.0s for all 12 files on re-run).
4. If a prior run `failed` mid-way, partial rows for that `source_file` are synchronously deleted (`SETTINGS mutations_sync = 1`) before retrying.

### 3. Bad Data Quarantine (`cleaning.py` & `taxi_trips_rejects`)
Every row passes through a pure, unit-tested validator (`validate_trip_row`) before insertion. Invalid rows are never silently dropped; they are written to `taxi_trips_rejects` with a standardized `reject_reason` and serialized JSON `raw_payload`:

| `reject_reason` | Rule |
| :--- | :--- |
| `missing_required_field` | Null or `NaN` in pickup/dropoff timestamps, location IDs, distance, or fare |
| `out_of_range_date` | Stray timestamps outside the `2025-01-01` – `2026-12-31` window |
| `dropoff_before_pickup` | `tpep_dropoff_datetime <= tpep_pickup_datetime` |
| `invalid_trip_distance` | `trip_distance <= 0` or `trip_distance > 500` miles |
| `non_positive_fare` | `fare_amount <= 0` or `total_amount <= 0` |

---

## Before-and-After Performance Benchmark (43,533,390 Clean Rows)

Measured over 20 iterations per query (`python benchmark.py --iterations 20 --api-url http://localhost:8000`) on Windows 11 + Docker Desktop:

### ClickHouse Query Latency (Raw `taxi_trips_clean` vs. Materialized Views)

| Query | BEFORE p95 (ms) | AFTER p95 (ms) | Speedup |
| :--- | ---: | ---: | ---: |
| 1. Top pickup zones (`hour=18`) | 109.4 | 89.1 | 1.2x |
| 2. Avg fare by distance bucket | 191.4 | 70.8 | 2.7x |
| 3. Daily trip count (12 months) | 154.2 | 77.3 | 2.0x |
| 4. Peak-hour heatmap (`7x24`) | 175.3 | 67.2 | 2.6x |

### Live FastAPI HTTP Endpoint Latency (Including Network + JSON Serialization)

| Endpoint | p50 (ms) | p95 (ms) | Target (< 300 ms) |
| :--- | ---: | ---: | :---: |
| `GET /analytics/top-pickup-zones?hour=18&limit=10` | 234.5 | 254.1 | PASS |
| `GET /analytics/fare-by-distance` | 228.0 | 245.1 | PASS |
| `GET /analytics/daily-trips?start_date=2025-10-01&end_date=2026-09-30` | 224.7 | 248.2 | PASS |
| `GET /analytics/peak-hour-heatmap` | 229.9 | 257.3 | PASS |

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
├── ingest.py              # Streaming, idempotent Parquet -> ClickHouse loader
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
5. **Run idempotent ingestion:**
   ```powershell
   python ingest.py --data-dir nyc_yellow_taxi
   ```
6. **Start the API & run the benchmark:**
   ```powershell
   python -m uvicorn api:app --host 0.0.0.0 --port 8000
   python benchmark.py --iterations 20 --api-url http://localhost:8000
   ```
