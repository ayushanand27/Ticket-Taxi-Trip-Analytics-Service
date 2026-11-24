-- ============================================================================
-- 01_schema.sql — ClickHouse DDL for NYC Taxi Trip Analytics Service
-- ============================================================================
-- WHY THIS FILE EXISTS:
--   Defines the database, dimension table, clean fact table, rejects quarantine
--   table, idempotency ledger, and pre-aggregated Materialized Views.
--   All statements use `IF NOT EXISTS` so re-running schema initialization is safe.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS taxi_analytics;

-- ----------------------------------------------------------------------------
-- 1. Dimension Table: taxi_zones
-- WHY: Maps numeric PULocationID / DOLocationID (1..265) to borough and zone names.
-- ENGINE = MergeTree() ordered by location_id makes JOINs / dictionary lookups instant.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_analytics.taxi_zones
(
    location_id   UInt16,
    borough       LowCardinality(String),
    zone          String,
    service_zone  LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY location_id;

-- ----------------------------------------------------------------------------
-- 2. Fact Table: taxi_trips_clean
-- WHY: Stores validated Yellow Taxi trips (~30-40M rows across 12 months).
--   - PARTITION BY toYYYYMM(pickup_datetime): Keeps each month in separate disk
--     parts so date-range queries skip irrelevant months and failed-month rollbacks
--     are fast.
--   - ORDER BY (pickup_datetime, pu_location_id, trip_hash): Physically sorts rows
--     by time then pickup zone, which matches our time-range & zone queries.
--   - LowCardinality / UInt8 / UInt16 / Float32: Shrinks memory & disk footprint by
--     ~2-3x compared to 64-bit defaults, speeding up full-table scans.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_analytics.taxi_trips_clean
(
    trip_hash           UInt64,
    source_file         LowCardinality(String),
    ingested_at         DateTime DEFAULT now(),
    vendor_id           UInt8,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    passenger_count     Nullable(UInt8),
    trip_distance       Float32,
    pu_location_id      UInt16,
    do_location_id      UInt16,
    payment_type        UInt8,
    fare_amount         Float32,
    tip_amount          Float32,
    total_amount        Float32,
    cbd_congestion_fee  Nullable(Float32)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_datetime, pu_location_id, trip_hash);

-- ----------------------------------------------------------------------------
-- 3. Quarantine Table: taxi_trips_rejects
-- WHY: Dirty rows (negative fares, zero distance, dropoff <= pickup, etc.) must
--   never be silently dropped. Storing `reject_reason` + `raw_payload` (JSON)
--   lets engineers audit data quality issues and replay fixes later.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_analytics.taxi_trips_rejects
(
    source_file    LowCardinality(String),
    row_index      UInt64,
    reject_reason  LowCardinality(String),
    raw_payload    String,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_file, reject_reason, row_index);

-- ----------------------------------------------------------------------------
-- 4. Idempotency Ledger: ingestion_runs
-- WHY: Guarantees the pipeline is re-runnable without creating duplicates.
--   Before loading a Parquet file, the pipeline checks (source_file, file_sha256).
--   If status = 'completed', the file is skipped. If a prior run 'failed',
--   partial rows for that source_file are deleted before re-ingesting.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS taxi_analytics.ingestion_runs
(
    run_id       UUID DEFAULT generateUUIDv4(),
    source_file  String,
    file_sha256  String,
    status       Enum8('started' = 1, 'completed' = 2, 'failed' = 3),
    clean_rows   UInt64 DEFAULT 0,
    reject_rows  UInt64 DEFAULT 0,
    error_msg    String DEFAULT '',
    started_at   DateTime DEFAULT now(),
    finished_at  Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY (source_file, started_at);

-- ============================================================================
-- 5. Pre-Aggregated Target Tables & Materialized Views (For < 300 ms p95)
-- WHY: Scanning 40M raw rows on every API request can exceed 300 ms under load.
--   ClickHouse Materialized Views compute partial sums/counts incrementally *as
--   rows are inserted* into `taxi_trips_clean` and store them in SummingMergeTree
--   tables. API queries then read a few thousand pre-aggregated rows in < 20 ms.
-- ============================================================================

-- 5a. Hourly Zone Stats (serves Endpoint 1: Top pickup zones by hour & Endpoint 4: Peak-hour heatmap)
CREATE TABLE IF NOT EXISTS taxi_analytics.hourly_zone_stats
(
    pickup_hour     UInt8,   -- 0..23
    pickup_dow      UInt8,   -- 1 (Monday) .. 7 (Sunday) via toDayOfWeek()
    pu_location_id  UInt16,
    trip_count      UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (pickup_hour, pickup_dow, pu_location_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_analytics.mv_hourly_zone_stats
TO taxi_analytics.hourly_zone_stats
AS
SELECT
    toHour(pickup_datetime)      AS pickup_hour,
    toDayOfWeek(pickup_datetime) AS pickup_dow,
    pu_location_id,
    count()                      AS trip_count
FROM taxi_analytics.taxi_trips_clean
GROUP BY pickup_hour, pickup_dow, pu_location_id;

-- 5b. Distance Bucket Stats (serves Endpoint 2: Average fare by distance bucket)
-- WHY: SummingMergeTree stores both `trip_count` and `total_fare_sum` so that
--   query time computes exact `sum(total_fare_sum) / sum(trip_count)` (never average of averages!).
CREATE TABLE IF NOT EXISTS taxi_analytics.distance_bucket_stats
(
    bucket_order    UInt8,
    distance_bucket LowCardinality(String),
    trip_count      UInt64,
    total_fare_sum  Float64
)
ENGINE = SummingMergeTree()
ORDER BY (bucket_order, distance_bucket);

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_analytics.mv_distance_bucket_stats
TO taxi_analytics.distance_bucket_stats
AS
SELECT
    multiIf(
        trip_distance < 1.0,  1,
        trip_distance < 3.0,  2,
        trip_distance < 5.0,  3,
        trip_distance < 10.0, 4,
        5
    ) AS bucket_order,
    multiIf(
        trip_distance < 1.0,  '0-1 mi',
        trip_distance < 3.0,  '1-3 mi',
        trip_distance < 5.0,  '3-5 mi',
        trip_distance < 10.0, '5-10 mi',
        '10+ mi'
    ) AS distance_bucket,
    count()                         AS trip_count,
    sum(toFloat64(fare_amount))     AS total_fare_sum
FROM taxi_analytics.taxi_trips_clean
GROUP BY bucket_order, distance_bucket;

-- 5c. Daily Trip Counts (serves Endpoint 3: Daily trip count for a date range)
CREATE TABLE IF NOT EXISTS taxi_analytics.daily_trip_counts
(
    trip_date   Date,
    trip_count  UInt64
)
ENGINE = SummingMergeTree()
ORDER BY trip_date;

CREATE MATERIALIZED VIEW IF NOT EXISTS taxi_analytics.mv_daily_trip_counts
TO taxi_analytics.daily_trip_counts
AS
SELECT
    toDate(pickup_datetime) AS trip_date,
    count()                 AS trip_count
FROM taxi_analytics.taxi_trips_clean
GROUP BY trip_date;