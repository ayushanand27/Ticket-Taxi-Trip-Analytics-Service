-- 02_live_schema.sql - Live Trip Stream (Ticket 2)
-- Rows are CUMULATIVE per (window, zone, partition, bucket); `version` = last
-- Kafka offset folded in. ReplacingMergeTree(version) keeps the newest, so a
-- replayed bucket after a crash never double counts. The API collapses with
-- argMax(..., version) so duplicates never show before background merges.
-- TTL is on ingested_at (insert time), NOT window_start: replayed data has old
-- window_start values and would be deleted at the next merge.
-- The consumer tracks the watermark per partition (see design_stream.md section 6);
-- live_consumer_status.watermark is the minimum across partitions.
CREATE TABLE IF NOT EXISTS taxi_analytics.live_zone_windows
(
    window_start   DateTime,
    pu_location_id UInt16,
    kafka_partition UInt16,
    bucket_id      UInt64,
    version        UInt64,
    trip_count     UInt32,
    fare_sum       Float64,
    distance_sum   Float64,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(window_start)
ORDER BY (window_start, pu_location_id, kafka_partition, bucket_id)
TTL ingested_at + INTERVAL 7 DAY;

-- Consumer health snapshot (one row per flush) for /live/health.
CREATE TABLE IF NOT EXISTS taxi_analytics.live_consumer_status
(
    reported_at   DateTime DEFAULT now(),
    watermark     Nullable(DateTime),
    max_event_time Nullable(DateTime),
    late_dropped  UInt64,
    events_total  UInt64
)
ENGINE = MergeTree
ORDER BY reported_at
TTL reported_at + INTERVAL 1 DAY;
