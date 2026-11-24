"""
benchmark.py — Before-and-After p95 Latency Benchmark (Raw Scan vs. Materialized Views + API).

WHY THIS MODULE EXISTS:
    Ticket requirement #2 states:
      "p95 latency under 300 ms on the full data. You'll need indexes or
       pre-aggregated tables, and you must show before-and-after numbers."

    This script runs each of the 4 analytical queries 20 times in two modes:
      1. BEFORE (Raw `taxi_trips_clean` full scan)
      2. AFTER  (Pre-aggregated Materialized View tables)
    And optionally benchmarks the live FastAPI HTTP endpoints (`--api-url`)
    to verify end-to-end HTTP p95 latency including JSON serialization.

USAGE (Windows PowerShell):
    # Terminal 1: Start the API
    python -m uvicorn api:app --host 0.0.0.0 --port 8000

    # Terminal 2: Run the benchmark (20 iterations per query)
    python benchmark.py --iterations 20 --api-url http://localhost:8000
"""

from __future__ import annotations

import argparse
import math
import os
import statistics
import time
from typing import Callable, Dict, List, Tuple

import clickhouse_connect
import httpx


def percentile(values: List[float], pct: float) -> float:
    """Compute a linear-interpolated percentile (e.g., p50 or p95) in milliseconds.

    WHY:
        Writing an explicit percentile helper avoids pulling in NumPy just for
        one summary statistic and makes the p95 calculation transparent.
    """
    ordered = sorted(values)
    if not ordered:
        return 0.0
    k = (len(ordered) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


def time_fn(fn: Callable[[], object], iterations: int) -> Dict[str, float]:
    """Execute `fn` `iterations` times (plus 1 warmup call) and return latency stats in ms.

    WHY:
        One warmup call ensures OS page cache and HTTP connection setup don't
        artificially distort steady-state query latency, while still measuring
        real full-data query execution across `iterations` runs.
    """
    fn()  # Warmup
    samples_ms: List[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn()
        samples_ms.append((time.perf_counter() - t0) * 1000.0)

    return {
        "min": min(samples_ms),
        "p50": statistics.median(samples_ms),
        "p95": percentile(samples_ms, 95.0),
        "max": max(samples_ms),
    }


def main() -> None:
    """Run before/after ClickHouse benchmarks and optional FastAPI HTTP benchmarks."""
    parser = argparse.ArgumentParser(description="Benchmark raw vs. pre-aggregated queries.")
    parser.add_argument("--iterations", type=int, default=20, help="Runs per query")
    parser.add_argument("--api-url", default="http://localhost:8000", help="FastAPI base URL")
    parser.add_argument("--skip-api", action="store_true", help="Skip live HTTP endpoint benchmark")
    args = parser.parse_args()

    client = clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "localhost"),
        port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "taxi_user"),
        password=os.getenv("CH_PASSWORD", "taxi_password"),
        database="taxi_analytics",
    )

    total_rows = client.query("SELECT count() FROM taxi_analytics.taxi_trips_clean").result_rows[0][0]
    print(f"\nBenchmarking on {total_rows:,} clean rows ({args.iterations} iterations/query)\n")

    # WHY: Each entry pairs the BEFORE (raw 40M-row scan) SQL with the AFTER
    # (pre-aggregated SummingMergeTree) SQL for the exact same analytical question.
    queries: List[Tuple[str, str, str]] = [
        (
            "1. Top pickup zones (hour=18)",
            """
            SELECT t.pu_location_id, any(z.borough), any(z.zone), count() AS trips
            FROM taxi_analytics.taxi_trips_clean AS t
            LEFT JOIN taxi_analytics.taxi_zones AS z ON t.pu_location_id = z.location_id
            WHERE toHour(t.pickup_datetime) = 18
            GROUP BY t.pu_location_id
            ORDER BY trips DESC
            LIMIT 10
            """,
            """
            SELECT h.pu_location_id, any(z.borough), any(z.zone), sum(h.trip_count) AS trips
            FROM taxi_analytics.hourly_zone_stats AS h
            LEFT JOIN taxi_analytics.taxi_zones AS z ON h.pu_location_id = z.location_id
            WHERE h.pickup_hour = 18
            GROUP BY h.pu_location_id
            ORDER BY trips DESC
            LIMIT 10
            """,
        ),
        (
            "2. Avg fare by distance bucket",
            """
            SELECT
                multiIf(trip_distance < 1.0, 1, trip_distance < 3.0, 2, trip_distance < 5.0, 3, trip_distance < 10.0, 4, 5) AS b,
                count() AS trips,
                round(avg(fare_amount), 2) AS avg_fare
            FROM taxi_analytics.taxi_trips_clean
            GROUP BY b
            ORDER BY b ASC
            """,
            """
            SELECT
                distance_bucket,
                sum(trip_count) AS trips,
                round(sum(total_fare_sum) / sum(trip_count), 2) AS avg_fare
            FROM taxi_analytics.distance_bucket_stats
            GROUP BY bucket_order, distance_bucket
            ORDER BY bucket_order ASC
            """,
        ),
        (
            "3. Daily trip count (12 months)",
            """
            SELECT toDate(pickup_datetime) AS d, count() AS trips
            FROM taxi_analytics.taxi_trips_clean
            WHERE pickup_datetime >= '2025-10-01 00:00:00' AND pickup_datetime < '2026-10-01 00:00:00'
            GROUP BY d
            ORDER BY d ASC
            """,
            """
            SELECT trip_date, sum(trip_count) AS trips
            FROM taxi_analytics.daily_trip_counts
            WHERE trip_date BETWEEN '2025-10-01' AND '2026-09-30'
            GROUP BY trip_date
            ORDER BY trip_date ASC
            """,
        ),
        (
            "4. Peak-hour heatmap (7x24)",
            """
            SELECT toDayOfWeek(pickup_datetime) AS dow, toHour(pickup_datetime) AS hr, count() AS trips
            FROM taxi_analytics.taxi_trips_clean
            GROUP BY dow, hr
            ORDER BY dow ASC, hr ASC
            """,
            """
            SELECT pickup_dow, pickup_hour, sum(trip_count) AS trips
            FROM taxi_analytics.hourly_zone_stats
            GROUP BY pickup_dow, pickup_hour
            ORDER BY pickup_dow ASC, pickup_hour ASC
            """,
        ),
    ]

    print(f"{'Query':<34} | {'BEFORE p95 (ms)':>15} | {'AFTER p95 (ms)':>14} | {'Speedup':>8}")
    print("-" * 80)
    for name, raw_sql, mv_sql in queries:
        before = time_fn(lambda s=raw_sql: client.query(s).result_rows, args.iterations)
        after = time_fn(lambda s=mv_sql: client.query(s).result_rows, args.iterations)
        speedup = before["p95"] / max(after["p95"], 0.01)
        print(f"{name:<34} | {before['p95']:>15.1f} | {after['p95']:>14.1f} | {speedup:>7.1f}x")

    if not args.skip_api:
        print("\nLive FastAPI HTTP Endpoint p95 Latency:")
        print(f"{'Endpoint':<48} | {'p50 (ms)':>10} | {'p95 (ms)':>10} | {'<300ms?':>7}")
        print("-" * 84)
        endpoints = [
            "/analytics/top-pickup-zones?hour=18&limit=10",
            "/analytics/fare-by-distance",
            "/analytics/daily-trips?start_date=2025-10-01&end_date=2026-09-30",
            "/analytics/peak-hour-heatmap",
        ]
        with httpx.Client(base_url=args.api_url, timeout=10.0) as http:
            for ep in endpoints:
                stats = time_fn(lambda u=ep: http.get(u).raise_for_status(), args.iterations)
                ok = "PASS" if stats["p95"] < 300.0 else "FAIL"
                print(f"{ep:<48} | {stats['p50']:>10.1f} | {stats['p95']:>10.1f} | {ok:>7}")


if __name__ == "__main__":
    main()