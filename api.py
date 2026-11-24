"""
api.py — FastAPI Analytics Service for NYC Yellow Taxi Trip Data.

WHY THIS MODULE EXISTS:
    Exposes the 4 required analytics endpoints over the ingested 30-40M row
    ClickHouse dataset. Every endpoint queries our pre-aggregated
    `SummingMergeTree` Materialized View target tables (`hourly_zone_stats`,
    `distance_bucket_stats`, `daily_trip_counts`) instead of scanning
    `taxi_trips_clean` directly, keeping p95 latency well under 300 ms.

USAGE (Windows PowerShell):
    python -m uvicorn api:app --host 0.0.0.0 --port 8000
    # Interactive docs: http://localhost:8000/docs
"""

from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(
    title="NYC Taxi Trip Analytics Service",
    description="Sub-300ms p95 analytics over 12 months (~30-40M rows) of NYC Yellow Taxi trips.",
    version="1.0.0",
)


def get_client() -> Client:
    """Create a ClickHouse HTTP client for request handling.

    WHY:
        `clickhouse-connect` is not thread-safe across concurrent async/worker
        threads if a single global Client instance is shared. Creating a
        lightweight client per request (backed by urllib3 connection pooling)
        prevents `concurrent queries within the same session` errors under load.
    """
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "taxi_user"),
        password=os.getenv("CH_PASSWORD", "taxi_password"),
        database="taxi_analytics",
    )


@app.get("/health")
def health() -> Dict[str, Any]:
    """Verify ClickHouse connectivity and report current clean/reject row counts.

    WHY:
        Provides an instant sanity check after ingestion so you can confirm the
        API sees all ~30-40M clean rows and quarantined rejects.
    """
    client = get_client()
    clean_count = client.query("SELECT count() FROM taxi_analytics.taxi_trips_clean").result_rows[0][0]
    reject_count = client.query("SELECT count() FROM taxi_analytics.taxi_trips_rejects").result_rows[0][0]
    return {
        "status": "ok",
        "clean_rows": int(clean_count),
        "reject_rows": int(reject_count),
    }


@app.get("/analytics/top-pickup-zones")
def top_pickup_zones_by_hour(
    hour: int = Query(..., ge=0, le=23, description="Hour of day (0-23)"),
    limit: int = Query(10, ge=1, le=50, description="Number of top pickup zones to return"),
) -> Dict[str, Any]:
    """Return the top `limit` pickup zones for a given `hour` of the day.

    WHY:
        Reads from `hourly_zone_stats` (SummingMergeTree) instead of scanning
        40M raw rows, then joins the tiny 265-row `taxi_zones` table for names.
        We always use `sum(trip_count)` because SummingMergeTree merges parts
        asynchronously in the background.
    """
    client = get_client()
    query = """
        SELECT
            h.pu_location_id,
            any(z.borough)    AS borough,
            any(z.zone)       AS zone,
            sum(h.trip_count) AS trips
        FROM taxi_analytics.hourly_zone_stats AS h
        LEFT JOIN taxi_analytics.taxi_zones AS z
            ON h.pu_location_id = z.location_id
        WHERE h.pickup_hour = %(hour)s
        GROUP BY h.pu_location_id
        ORDER BY trips DESC
        LIMIT %(limit)s
    """
    rows = client.query(query, parameters={"hour": hour, "limit": limit}).result_rows
    return {
        "hour": hour,
        "limit": limit,
        "zones": [
            {
                "pu_location_id": int(r[0]),
                "borough": r[1] or "Unknown",
                "zone": r[2] or "Unknown",
                "trip_count": int(r[3]),
            }
            for r in rows
        ],
    }


@app.get("/analytics/fare-by-distance")
def average_fare_by_distance_bucket() -> Dict[str, Any]:
    """Return trip counts and average base fare grouped by distance bucket.

    WHY:
        Reads from `distance_bucket_stats` and computes `sum(total_fare_sum) /
        sum(trip_count)` rather than averaging pre-computed averages, which
        would be mathematically wrong across differently sized partitions.
    """
    client = get_client()
    query = """
        SELECT
            distance_bucket,
            sum(trip_count)                                         AS trips,
            round(sum(total_fare_sum) / sum(trip_count), 2)         AS avg_fare
        FROM taxi_analytics.distance_bucket_stats
        GROUP BY bucket_order, distance_bucket
        ORDER BY bucket_order ASC
    """
    rows = client.query(query).result_rows
    return {
        "buckets": [
            {
                "distance_bucket": str(r[0]),
                "trip_count": int(r[1]),
                "avg_fare_amount": float(r[2]),
            }
            for r in rows
        ]
    }


@app.get("/analytics/daily-trips")
def daily_trip_count(
    start_date: date = Query(..., description="Inclusive start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Inclusive end date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """Return daily Yellow Taxi trip counts between `start_date` and `end_date`.

    WHY:
        Reads from `daily_trip_counts` (at most ~365 rows for a full year)
        instead of scanning 40M rows with `toDate(pickup_datetime)`.
    """
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    client = get_client()
    query = """
        SELECT
            trip_date,
            sum(trip_count) AS trips
        FROM taxi_analytics.daily_trip_counts
        WHERE trip_date BETWEEN %(start_date)s AND %(end_date)s
        GROUP BY trip_date
        ORDER BY trip_date ASC
    """
    rows = client.query(
        query,
        parameters={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
    ).result_rows
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "days": [
            {"date": str(r[0]), "trip_count": int(r[1])}
            for r in rows
        ],
    }


@app.get("/analytics/peak-hour-heatmap")
def peak_hour_heatmap() -> Dict[str, Any]:
    """Return trip counts by (day_of_week, hour_of_day) for a 7x24 heatmap.

    WHY:
        Reuses `hourly_zone_stats` by aggregating across `pu_location_id`
        (`pickup_dow` 1=Monday..7=Sunday, `pickup_hour` 0..23), avoiding a
        fourth materialized view while still returning 168 cells in < 10 ms.
    """
    client = get_client()
    query = """
        SELECT
            pickup_dow,
            pickup_hour,
            sum(trip_count) AS trips
        FROM taxi_analytics.hourly_zone_stats
        GROUP BY pickup_dow, pickup_hour
        ORDER BY pickup_dow ASC, pickup_hour ASC
    """
    rows = client.query(query).result_rows
    return {
        "cells": [
            {
                "day_of_week": int(r[0]),
                "hour": int(r[1]),
                "trip_count": int(r[2]),
            }
            for r in rows
        ]
    }