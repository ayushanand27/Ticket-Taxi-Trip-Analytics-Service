# Design Document: NYC Taxi Trip Analytics Service

## 1. Overview & Goals

This service ingests 12 months of [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (\~30–40 million rows in Parquet format) into an analytical database, quarantines dirty records with explicit rejection reasons, and exposes a low-latency REST API (<300 ms p95 on the full dataset) for four core analytics queries:

1. **Top pickup zones by hour**

2. **Average fare by distance bucket**

3. **Daily trip count for a date range**

4. **Peak-hour heatmap data** (day-of-week × hour-of-day)

## 2. Database Choice: ClickHouse (Recommended) vs. PostgreSQL

| Criterion | ClickHouse (Recommended) | PostgreSQL | 
| ----- | ----- | ----- | 
| **Storage Engine** | Columnar (`MergeTree`) — reads only the 2–4 columns needed per query | Row-oriented heap — reads full row tuples even for narrow aggregations | 
| **30–40M Row Aggregations** | Vectorized SIMD execution; scans tens of millions of rows in \~100–400 ms | Sequential/parallel scans take several seconds without heavy manual tuning | 
| **Pre-Aggregation** | Native `MaterializedView` + `SummingMergeTree` / `AggregatingMergeTree` updates automatically on insert | Requires manual summary tables or refreshable materialized views | 
| **Bulk Ingestion** | Native Parquet & Arrow streaming at hundreds of thousands of rows/sec | Slower `COPY` + B-tree index write amplification at 40M rows | 
| **Local Windows Setup** | Single Docker container (`clickhouse/clickhouse-server`) | Single Docker container (`postgres`) | 

**Decision:** Use **ClickHouse** because all four API endpoints are analytical `GROUP BY` aggregations over 30–40M rows. Combining `MergeTree` partitioning with incremental `MaterializedView`s guarantees p95 latency well under 300 ms while still allowing us to benchmark **before** (raw `taxi_trips_clean` scan) vs. **after** (pre-aggregated views) numbers directly.

## 3. Data Model & Schema

### 3.1 `taxi_zones` (Dimension Table)

Loaded once from [`taxi_zone_lookup.csv`](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) to map numeric `PULocationID` / `DOLocationID` values to human-readable boroughs and zones.

* `location_id` (`UInt16`), `borough` (`LowCardinality(String)`), `zone` (`String`), `service_zone` (`LowCardinality(String)`)

### 3.2 `taxi_trips_clean` (Fact Table — `MergeTree`)

* **Partitioning:** `PARTITION BY toYYYYMM(pickup_datetime)` (isolates each month for fast pruning and clean partition-level recovery).

* **Ordering Key:** `ORDER BY (pickup_datetime, pu_location_id, trip_hash)` (optimizes time-range and zone-based queries).

* **Columns:**

  * `trip_hash` (`UInt64`) — deterministic hash of `(source_file, row_index)` for traceability

  * `source_file` (`LowCardinality(String)`), `ingested_at` (`DateTime`)

  * `vendor_id` (`UInt8`), `pickup_datetime` (`DateTime`), `dropoff_datetime` (`DateTime`)

  * `passenger_count` (`Nullable(UInt8)`), `trip_distance` (`Float32`)

  * `pu_location_id` (`UInt16`), `do_location_id` (`UInt16`)

  * `payment_type` (`UInt8`), `fare_amount` (`Float32`), `tip_amount` (`Float32`), `total_amount` (`Float32`), `cbd_congestion_fee` (`Nullable(Float32)`) *(added by TLC starting in 2025)*

### 3.3 `taxi_trips_rejects` (Quarantine Table — `MergeTree`)

* **Columns:** `source_file` (`LowCardinality(String)`), `row_index` (`UInt64`), `reject_reason` (`LowCardinality(String)`), `raw_payload` (`String` JSON), `ingested_at` (`DateTime`).

* Preserves every rejected row with its exact failure reason so data quality issues can be audited without losing data.

### 3.4 `ingestion_runs` (Idempotency Ledger)

* **Columns:** `source_file` (`String`), `file_sha256` (`String`), `status` (`Enum8('started', 'completed', 'failed')`), `clean_rows` (`UInt64`), `reject_rows` (`UInt64`), `started_at` (`DateTime`), `finished_at` (`Nullable(DateTime)`).

* **Idempotency Strategy (Re-runnable Without Duplicates):** Before ingesting a Parquet file, the pipeline computes its SHA-256 hash and checks `ingestion_runs`:

  1. If `(source_file, file_sha256)` is already `completed`, the file is skipped.

  2. If a previous run `failed` (partial load), the pipeline deletes any partial rows for that `source_file` (`ALTER TABLE ... DELETE WHERE source_file = ...`) before re-ingesting, guaranteeing zero duplicates.

### 3.5 Pre-Aggregated Materialized Views (For <300 ms p95)

1. `mv_hourly_zone_stats` (`SummingMergeTree`): `pickup_hour`, `pu_location_id`, `trip_count` → serves **Top pickup zones by hour** and **Peak-hour heatmap** (day-of-week × hour).

2. `mv_distance_bucket_stats` (`SummingMergeTree`): `distance_bucket` (e.g., `0-1`, `1-3`, `3-5`, `5-10`, `10+` miles), `trip_count`, `total_fare_sum` → serves **Average fare by distance bucket**.

3. `mv_daily_trip_counts` (`SummingMergeTree`): `trip_date`, `trip_count` → serves **Daily trip count for a date range**.

## 4. Bad Data Handling & Cleaning Rules

Each row passes through a pure, unit-tested validator (`validate_trip_row`) before insertion. On the first failing rule, the row is routed to `taxi_trips_rejects` with a standardized `reject_reason`:

| Check | Condition for Rejection | `reject_reason` | 
| ----- | ----- | ----- | 
| **Missing Core Fields** | `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, or `DOLocationID` is null | `missing_required_field` | 
| **Time Ordering** | `tpep_dropoff_datetime <= tpep_pickup_datetime` | `dropoff_before_pickup` | 
| **Invalid Distance** | `trip_distance <= 0` or `trip_distance > 500` | `invalid_trip_distance` | 
| **Negative / Zero Fare** | `fare_amount <= 0` or `total_amount <= 0` | `non_positive_fare` | 
| **Stray Date Outlier** | Pickup timestamp outside the expected 2025–2026 dataset window (NYC TLC files occasionally contain stray dates like `2002` or `2099`) | `out_of_range_date` | 

## 5. Step-by-Step Implementation Plan

1. **Step 1 (Done):** Design doc & database selection.

2. **Step 2:** Project structure, Docker Compose for ClickHouse, and SQL schema/view DDL.

3. **Step 3:** Pure cleaning/validation module + `pytest` unit tests (with full docstrings & inline explanations).

4. **Step 4:** Idempotent chunked Parquet ingestion script (`pyarrow` + `clickhouse-connect`).

5. **Step 5:** FastAPI service with the 4 endpoints + before/after p95 latency benchmark script.

6. **Step 6:** Final `README.md` with architecture, design trade-offs, and benchmark results.