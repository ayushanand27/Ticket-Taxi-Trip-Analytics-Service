"""
api.py — FastAPI Analytics Service for NYC Yellow Taxi Trip Data.

WHY THIS MODULE EXISTS:
    Exposes the 4 required analytics endpoints over the ingested 43.5M-row
    ClickHouse dataset. Every endpoint queries our pre-aggregated
    `SummingMergeTree` Materialized View target tables (`hourly_zone_stats`,
    `distance_bucket_stats`, `daily_trip_counts`) instead of scanning
    `taxi_trips_clean` directly.

PERFORMANCE & OBSERVABILITY:
    1. Defaults `CH_HOST` to `127.0.0.1` instead of `localhost` so Windows does
       not waste ~100-150 ms trying IPv6 (`::1`) before falling back to IPv4.
    2. Reuses one persistent ClickHouse client per worker thread via
       `threading.local()`, saving ~25 ms of per-request handshake overhead.
    3. Adds `X-DB-Time-Ms` and `X-Handler-Time-Ms` response headers on every
       endpoint so benchmark scripts can separate DB time from HTTP overhead.

USAGE (Windows PowerShell):
    python -m uvicorn api:app --host 127.0.0.1 --port 8000
"""

from __future__ import annotations

import os
import threading
import time
from datetime import date
from typing import Any, Dict

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from fastapi import FastAPI, HTTPException, Query, Response

app = FastAPI(
    title="NYC Taxi Trip Analytics Service",
    description="Sub-300ms p95 analytics over 12 months (~43.5M clean rows) of NYC Yellow Taxi trips.",
    version="1.2.0",
)

# WHY: Thread-local storage gives each Uvicorn worker thread one persistent
# ClickHouse client, avoiding per-request handshake overhead (~25 ms) while
# keeping concurrent requests thread-safe.
_thread_local = threading.local()


def get_client() -> Client:
    """Return a reused ClickHouse HTTP client bound to the current worker thread.

    WHY:
        Defaults to `127.0.0.1` rather than `localhost` to avoid Windows IPv6
        (`::1`) resolution/fallback delays when connecting to Docker Desktop.
    """
    client: Client | None = getattr(_thread_local, "ch_client", None)
    if client is None:
        client = clickhouse_connect.get_client(
            host=os.getenv("CH_HOST", "127.0.0.1"),
            port=int(os.getenv("CH_PORT", "8123")),
            username=os.getenv("CH_USER", "taxi_user"),
            password=os.getenv("CH_PASSWORD", "taxi_password"),
            database="taxi_analytics",
        )
        _thread_local.ch_client = client
    return client


@app.get("/health")
def health(response: Response) -> Dict[str, Any]:
    """Verify ClickHouse connectivity and report current clean/reject row counts."""
    t_handler = time.perf_counter()
    client = get_client()
    t_db = time.perf_counter()
    clean_count = client.query("SELECT count() FROM taxi_analytics.taxi_trips_clean").result_rows[0][0]
    reject_count = client.query("SELECT count() FROM taxi_analytics.taxi_trips_rejects").result_rows[0][0]
    response.headers["X-DB-Time-Ms"] = f"{(time.perf_counter() - t_db) * 1000:.2f}"
    response.headers["X-Handler-Time-Ms"] = f"{(time.perf_counter() - t_handler) * 1000:.2f}"
    return {
        "status": "ok",
        "clean_rows": int(clean_count),
        "reject_rows": int(reject_count),
    }


@app.get("/analytics/top-pickup-zones")
def top_pickup_zones_by_hour(
    response: Response,
    hour: int = Query(..., ge=0, le=23, description="Hour of day (0-23)"),
    limit: int = Query(10, ge=1, le=50, description="Number of top pickup zones to return"),
) -> Dict[str, Any]:
    """Return the top `limit` pickup zones for a given `hour` of the day."""
    t_handler = time.perf_counter()
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
    t_db = time.perf_counter()
    rows = client.query(query, parameters={"hour": hour, "limit": limit}).result_rows
    response.headers["X-DB-Time-Ms"] = f"{(time.perf_counter() - t_db) * 1000:.2f}"
    response.headers["X-Handler-Time-Ms"] = f"{(time.perf_counter() - t_handler) * 1000:.2f}"
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
def average_fare_by_distance_bucket(response: Response) -> Dict[str, Any]:
    """Return trip counts and average base fare grouped by distance bucket."""
    t_handler = time.perf_counter()
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
    t_db = time.perf_counter()
    rows = client.query(query).result_rows
    response.headers["X-DB-Time-Ms"] = f"{(time.perf_counter() - t_db) * 1000:.2f}"
    response.headers["X-Handler-Time-Ms"] = f"{(time.perf_counter() - t_handler) * 1000:.2f}"
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
    response: Response,
    start_date: date = Query(..., description="Inclusive start date (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Inclusive end date (YYYY-MM-DD)"),
) -> Dict[str, Any]:
    """Return daily Yellow Taxi trip counts between `start_date` and `end_date`."""
    if end_date < start_date:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    t_handler = time.perf_counter()
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
    t_db = time.perf_counter()
    rows = client.query(
        query,
        parameters={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
    ).result_rows
    response.headers["X-DB-Time-Ms"] = f"{(time.perf_counter() - t_db) * 1000:.2f}"
    response.headers["X-Handler-Time-Ms"] = f"{(time.perf_counter() - t_handler) * 1000:.2f}"
    return {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "days": [
            {"date": str(r[0]), "trip_count": int(r[1])}
            for r in rows
        ],
    }


@app.get("/analytics/peak-hour-heatmap")
def peak_hour_heatmap(response: Response) -> Dict[str, Any]:
    """Return trip counts by (day_of_week, hour_of_day) for a 7x24 heatmap."""
    t_handler = time.perf_counter()
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
    t_db = time.perf_counter()
    rows = client.query(query).result_rows
    response.headers["X-DB-Time-Ms"] = f"{(time.perf_counter() - t_db) * 1000:.2f}"
    response.headers["X-Handler-Time-Ms"] = f"{(time.perf_counter() - t_handler) * 1000:.2f}"
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