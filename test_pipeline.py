"""
test_pipeline.py — Unit & Integration-Style Tests for API, Idempotency, Aggregates, and Parallel Ingestion.

WHY THIS FILE EXISTS:
    `test_cleaning.py` covers row-level validation, whereas this suite verifies
    the rest of the system without requiring a live Docker container:
      1. FastAPI endpoints (`/health`, all 4 analytics routes, and 400 validation).
      2. Idempotency & summary-table rebuild on retry/`--force` (proving that
         summary tables never double-count after a failed run or forced reload).
      3. Vectorized `validate_arrow_batch()` parity with `validate_trip_row()`.
      4. Parallel ingestion (`workers=2`) over real temporary Parquet files.

USAGE (Windows PowerShell):
    python -m pytest test_cleaning.py test_pipeline.py -v
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api
import ingest
from cleaning import validate_arrow_batch, validate_trip_row


class FakeQueryResult:
    """Minimal stand-in for `clickhouse_connect` QueryResult."""

    def __init__(self, rows: List[tuple]) -> None:
        self.result_rows = rows


class FakeClickHouseClient:
    """In-memory mock ClickHouse client tracking raw rows, summary tables, and run statuses.

    WHY:
        Lets us test idempotent retries, summary-table rebuilds, and FastAPI
        endpoints deterministically in <0.2s without needing Docker running.
    """

    def __init__(self) -> None:
        self.clean_rows: List[Dict[str, Any]] = []
        self.reject_rows: List[List[Any]] = []
        self.runs: List[Dict[str, Any]] = []
        self.hourly_zone_stats: List[tuple] = []
        self.distance_bucket_stats: List[tuple] = []
        self.daily_trip_counts: List[tuple] = []
        self.commands: List[str] = []

    def command(self, sql: str, parameters: Dict[str, Any] | None = None) -> None:
        self.commands.append(sql)
        if "DELETE WHERE source_file" in sql and parameters:
            sf = parameters["source_file"]
            if "taxi_trips_clean" in sql:
                self.clean_rows = [r for r in self.clean_rows if r["source_file"] != sf]
            elif "taxi_trips_rejects" in sql:
                self.reject_rows = [r for r in self.reject_rows if r[0] != sf]
        elif "TRUNCATE TABLE IF EXISTS taxi_analytics.hourly_zone_stats" in sql:
            self.hourly_zone_stats.clear()
        elif "TRUNCATE TABLE IF EXISTS taxi_analytics.distance_bucket_stats" in sql:
            self.distance_bucket_stats.clear()
        elif "TRUNCATE TABLE IF EXISTS taxi_analytics.daily_trip_counts" in sql:
            self.daily_trip_counts.clear()
        elif "INSERT INTO taxi_analytics.hourly_zone_stats" in sql:
            # Simulate exact rebuild from clean_rows
            counts: Dict[tuple, int] = {}
            for r in self.clean_rows:
                dt: datetime = r["pickup_datetime"]
                key = (dt.hour, dt.isoweekday(), r["pu_location_id"])
                counts[key] = counts.get(key, 0) + 1
            self.hourly_zone_stats = [(k[0], k[1], k[2], v) for k, v in counts.items()]
        elif "INSERT INTO taxi_analytics.distance_bucket_stats" in sql:
            self.distance_bucket_stats = [(1, "0-1 mi", len(self.clean_rows), 10.0)]
        elif "INSERT INTO taxi_analytics.daily_trip_counts" in sql:
            self.daily_trip_counts = [("2026-03-15", len(self.clean_rows))]

    def insert(
        self,
        table: str,
        data: List[Any],
        column_names: List[str],
        column_oriented: bool = False,
    ) -> None:
        if table == "taxi_analytics.ingestion_runs":
            row = dict(zip(column_names, data[0]))
            self.runs.append(row)
        elif table == "taxi_analytics.taxi_trips_clean":
            n = len(data[0]) if column_oriented else len(data)
            for i in range(n):
                rec = {
                    col: (data[c_idx][i] if column_oriented else data[i][c_idx])
                    for c_idx, col in enumerate(column_names)
                }
                self.clean_rows.append(rec)
                # Simulate Materialized View trigger on insert (including before cleanup!)
                dt: datetime = rec["pickup_datetime"]
                self.hourly_zone_stats.append((dt.hour, dt.isoweekday(), rec["pu_location_id"], 1))
        elif table == "taxi_analytics.taxi_trips_rejects":
            self.reject_rows.extend(data)

    def query(self, sql: str, parameters: Dict[str, Any] | None = None) -> FakeQueryResult:
        if "FROM taxi_analytics.ingestion_runs" in sql and parameters:
            sf = parameters["source_file"]
            sha = parameters["file_sha256"]
            matches = [r for r in self.runs if r["source_file"] == sf and r["file_sha256"] == sha]
            if not matches:
                return FakeQueryResult([])
            return FakeQueryResult([(matches[-1]["status"],)])
        if "SELECT count() FROM taxi_analytics.taxi_trips_clean" in sql:
            return FakeQueryResult([(len(self.clean_rows),)])
        if "SELECT count() FROM taxi_analytics.taxi_trips_rejects" in sql:
            return FakeQueryResult([(len(self.reject_rows),)])
        if "FROM taxi_analytics.hourly_zone_stats AS h" in sql:
            return FakeQueryResult([(161, "Manhattan", "Midtown Center", 42)])
        if "FROM taxi_analytics.distance_bucket_stats" in sql:
            return FakeQueryResult([("1-3 mi", 100, 14.5)])
        if "FROM taxi_analytics.daily_trip_counts" in sql:
            return FakeQueryResult([("2026-03-15", 500)])
        if "FROM taxi_analytics.hourly_zone_stats" in sql:
            return FakeQueryResult([(1, 18, 500)])
        return FakeQueryResult([])


def _write_sample_parquet(path: Path) -> None:
    """Create a small Parquet file containing 2 valid rows and 3 distinct dirty rows."""
    table = pa.table(
        {
            "VendorID": pa.array([2, 2, 2, 2, 2], type=pa.int32()),
            "tpep_pickup_datetime": pa.array(
                [
                    datetime(2026, 3, 15, 18, 0, 0),  # valid
                    datetime(2026, 3, 15, 18, 10, 0), # valid
                    datetime(2026, 3, 15, 18, 20, 0), # invalid distance (0.0)
                    datetime(2026, 3, 15, 18, 30, 0), # negative fare (-5.0)
                    datetime(2026, 3, 15, 18, 40, 0), # dropoff before pickup
                ],
                type=pa.timestamp("us"),
            ),
            "tpep_dropoff_datetime": pa.array(
                [
                    datetime(2026, 3, 15, 18, 15, 0),
                    datetime(2026, 3, 15, 18, 25, 0),
                    datetime(2026, 3, 15, 18, 35, 0),
                    datetime(2026, 3, 15, 18, 45, 0),
                    datetime(2026, 3, 15, 18, 35, 0),
                ],
                type=pa.timestamp("us"),
            ),
            "passenger_count": pa.array([1, 2, 1, 1, 1], type=pa.int64()),
            "trip_distance": pa.array([2.5, 4.0, 0.0, 1.5, 1.5], type=pa.float64()),
            "PULocationID": pa.array([161, 236, 161, 161, 161], type=pa.int32()),
            "DOLocationID": pa.array([236, 161, 236, 236, 236], type=pa.int32()),
            "payment_type": pa.array([1, 1, 1, 1, 1], type=pa.int64()),
            "fare_amount": pa.array([15.0, 20.0, 10.0, -5.0, 12.0], type=pa.float64()),
            "tip_amount": pa.array([3.0, 4.0, 0.0, 0.0, 0.0], type=pa.float64()),
            "total_amount": pa.array([20.0, 26.0, 12.0, 5.0, 15.0], type=pa.float64()),
            "cbd_congestion_fee": pa.array([0.75, 0.75, 0.75, 0.75, 0.75], type=pa.float64()),
        }
    )
    pq.write_table(table, path)


def test_vectorized_arrow_matches_row_validator(tmp_path: Path) -> None:
    """Vectorized `validate_arrow_batch` must produce identical results to `validate_trip_row`."""
    pq_path = tmp_path / "sample.parquet"
    _write_sample_parquet(pq_path)

    pf = pq.ParquetFile(pq_path)
    batch = next(pf.iter_batches(batch_size=10))
    clean_dict, reject_rows = validate_arrow_batch(batch, source_file="sample.parquet", row_offset=0)

    row_clean = []
    row_rejects = []
    for idx, raw in enumerate(batch.to_pylist()):
        res = validate_trip_row(raw, source_file="sample.parquet", row_index=idx)
        if res.is_valid and res.cleaned_row is not None:
            row_clean.append(res.cleaned_row)
        else:
            row_rejects.append((idx, res.reject_reason))

    assert len(clean_dict["trip_hash"]) == len(row_clean) == 2
    assert clean_dict["trip_hash"] == [r["trip_hash"] for r in row_clean]
    assert [(r[1], r[2]) for r in reject_rows] == row_rejects


def test_idempotent_retry_rebuilds_summary_without_double_counting(tmp_path: Path) -> None:
    """Retrying a failed or `--force`d file must rebuild summary tables so counts never double."""
    pq_path = tmp_path / "yellow_tripdata_2026-03.parquet"
    _write_sample_parquet(pq_path)

    fake_ch = FakeClickHouseClient()
    sha = ingest.compute_file_sha256(pq_path)

    # Simulate a partial/failed prior run that left 1 orphan row in clean + summary
    fake_ch.insert(
        "taxi_analytics.taxi_trips_clean",
        [[1, "yellow_tripdata_2026-03.parquet", datetime(2026, 3, 15, 18, 0, 0), 161]],
        ["trip_hash", "source_file", "pickup_datetime", "pu_location_id"],
    )
    ingest.record_run_status(fake_ch, pq_path.name, sha, status="failed")

    # Retry ingestion: must clean up partial row, ingest 2 clean rows, and rebuild summary!
    clean_cnt, reject_cnt, skipped = ingest.run_ingestion(
        client=fake_ch,  # type: ignore[arg-type]
        parquet_files=[pq_path],
        batch_size=10,
        workers=1,
        force=False,
        host="127.0.0.1",
        port=8123,
        user="u",
        password="p",
    )

    assert skipped == 0
    assert clean_cnt == 2
    assert reject_cnt == 3
    assert len(fake_ch.clean_rows) == 2
    # Summary table sum must equal clean_rows (2), NOT 1 + 2 = 3!
    assert sum(row[3] for row in fake_ch.hourly_zone_stats) == 2

    # Second run without --force must skip completely
    c2, r2, s2 = ingest.run_ingestion(
        client=fake_ch,  # type: ignore[arg-type]
        parquet_files=[pq_path],
        batch_size=10,
        workers=1,
        force=False,
        host="127.0.0.1",
        port=8123,
        user="u",
        password="p",
    )
    assert (c2, r2, s2) == (0, 0, 1)


def test_parallel_ingestion_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Verify `workers=2` dispatches all pending files and aggregates counts."""
    f1 = tmp_path / "yellow_tripdata_2026-01.parquet"
    f2 = tmp_path / "yellow_tripdata_2026-02.parquet"
    _write_sample_parquet(f1)
    _write_sample_parquet(f2)

    fake_ch = FakeClickHouseClient()

    # Use ThreadPoolExecutor in-process so FakeClickHouseClient can be tested cleanly
    from concurrent.futures import ThreadPoolExecutor

    monkeypatch.setattr(ingest, "ProcessPoolExecutor", ThreadPoolExecutor)
    monkeypatch.setattr(ingest.clickhouse_connect, "get_client", lambda **_: fake_ch)

    clean_cnt, reject_cnt, skipped = ingest.run_ingestion(
        client=fake_ch,  # type: ignore[arg-type]
        parquet_files=[f1, f2],
        batch_size=10,
        workers=2,
        force=False,
        host="127.0.0.1",
        port=8123,
        user="u",
        password="p",
    )
    assert (clean_cnt, reject_cnt, skipped) == (4, 6, 0)


def test_api_endpoints_and_timing_headers(monkeypatch: pytest.MonkeyPatch) -> None:
    """All 4 analytics endpoints + /health must return expected JSON and timing headers."""
    fake_ch = FakeClickHouseClient()
    fake_ch.clean_rows = [{"id": 1}, {"id": 2}]
    fake_ch.reject_rows = [["f", 0, "invalid_trip_distance", "{}"]]
    monkeypatch.setattr(api, "get_client", lambda: fake_ch)

    client = TestClient(api.app)

    r_health = client.get("/health")
    assert r_health.status_code == 200
    assert r_health.json() == {"status": "ok", "clean_rows": 2, "reject_rows": 1}
    assert "X-DB-Time-Ms" in r_health.headers

    r_zones = client.get("/analytics/top-pickup-zones?hour=18&limit=5")
    assert r_zones.status_code == 200
    assert r_zones.json()["zones"][0]["zone"] == "Midtown Center"

    r_dist = client.get("/analytics/fare-by-distance")
    assert r_dist.status_code == 200
    assert r_dist.json()["buckets"][0]["avg_fare_amount"] == 14.5

    r_daily = client.get("/analytics/daily-trips?start_date=2026-03-01&end_date=2026-03-31")
    assert r_daily.status_code == 200
    assert r_daily.json()["days"][0]["trip_count"] == 500

    r_bad_dates = client.get("/analytics/daily-trips?start_date=2026-03-31&end_date=2026-03-01")
    assert r_bad_dates.status_code == 400

    r_heat = client.get("/analytics/peak-hour-heatmap")
    assert r_heat.status_code == 200
    assert r_heat.json()["cells"][0] == {"day_of_week": 1, "hour": 18, "trip_count": 500}