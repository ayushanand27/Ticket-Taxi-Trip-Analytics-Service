"""
ingest.py — Parallel, Vectorized, Idempotent Parquet Ingestion Pipeline for ClickHouse.

WHY THIS MODULE EXISTS:
    Loads 12 months of NYC Yellow Taxi Parquet files (~46.5M raw rows) into
    `taxi_analytics.taxi_trips_clean` while quarantining dirty rows into
    `taxi_analytics.taxi_trips_rejects`.

    Key engineering guarantees:
      1. Vectorized PyArrow Validation: Uses `validate_arrow_batch()` (`pyarrow.compute`)
         to filter valid rows in C++ rather than looping over 46.5M rows in Python.
      2. Parallel File Processing (`--workers`): Uses `ProcessPoolExecutor` to
         ingest multiple monthly Parquet files concurrently across CPU cores.
      3. Full Idempotency (Raw + Summary Tables): Checks `(source_file, file_sha256)`
         in `ingestion_runs`. Completed files are skipped. If any file is retried
         (after failure or `--force`), partial raw rows are deleted synchronously
         AND `rebuild_summary_tables()` rebuilds the 3 `SummingMergeTree` tables
         from `taxi_trips_clean` so summary metrics never double-count.

USAGE (Windows PowerShell):
    python ingest.py --data-dir nyc_yellow_taxi --workers 4
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import clickhouse_connect
import pyarrow.parquet as pq
from clickhouse_connect.driver.client import Client

from cleaning import validate_arrow_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

CLEAN_COLUMNS = [
    "trip_hash",
    "source_file",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "cbd_congestion_fee",
]

REJECT_COLUMNS = [
    "source_file",
    "row_index",
    "reject_reason",
    "raw_payload",
]


def compute_file_sha256(file_path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    """Compute the SHA-256 hex digest of a file without loading it into RAM."""
    hasher = hashlib.sha256()
    with file_path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_latest_run_status(
    client: Client,
    source_file: str,
    file_sha256: str,
) -> Optional[str]:
    """Check `ingestion_runs` for the latest status of `(source_file, file_sha256)`."""
    result = client.query(
        """
        SELECT status
        FROM taxi_analytics.ingestion_runs
        WHERE source_file = %(source_file)s
          AND file_sha256 = %(file_sha256)s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        parameters={"source_file": source_file, "file_sha256": file_sha256},
    )
    if not result.result_rows:
        return None
    return str(result.result_rows[0][0])


def cleanup_partial_file_rows(client: Client, source_file: str) -> None:
    """Synchronously delete any rows from a prior run of `source_file`.

    WHY:
        Deleting from `taxi_trips_clean` does NOT automatically delete deltas
        already written to downstream `SummingMergeTree` summary tables. Callers
        must invoke `rebuild_summary_tables()` after cleanup+re-ingestion so
        summary tables stay 100% consistent with `taxi_trips_clean`.
    """
    logger.warning("Cleaning up partial rows for %s before retrying...", source_file)
    for table in ("taxi_analytics.taxi_trips_clean", "taxi_analytics.taxi_trips_rejects"):
        client.command(
            f"ALTER TABLE {table} DELETE WHERE source_file = %(source_file)s SETTINGS mutations_sync = 1",
            parameters={"source_file": source_file},
        )


def rebuild_summary_tables(client: Client) -> None:
    """Truncate and rebuild all 3 pre-aggregated summary tables from `taxi_trips_clean`.

    WHY:
        ClickHouse `ALTER TABLE taxi_trips_clean DELETE` does not propagate
        deletions to Materialized View target tables (`hourly_zone_stats`,
        `distance_bucket_stats`, `daily_trip_counts`). Rebuilding them from
        `taxi_trips_clean` after any retry or `--force` run eliminates any
        possibility of double-counting in the API endpoints.
    """
    logger.info("Rebuilding pre-aggregated summary tables from taxi_trips_clean...")
    rebuilds = [
        (
            "taxi_analytics.hourly_zone_stats",
            """
            INSERT INTO taxi_analytics.hourly_zone_stats
            SELECT
                toHour(pickup_datetime)      AS pickup_hour,
                toDayOfWeek(pickup_datetime) AS pickup_dow,
                pu_location_id,
                count()                      AS trip_count
            FROM taxi_analytics.taxi_trips_clean
            GROUP BY pickup_hour, pickup_dow, pu_location_id
            """,
        ),
        (
            "taxi_analytics.distance_bucket_stats",
            """
            INSERT INTO taxi_analytics.distance_bucket_stats
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
                count()                     AS trip_count,
                sum(toFloat64(fare_amount)) AS total_fare_sum
            FROM taxi_analytics.taxi_trips_clean
            GROUP BY bucket_order, distance_bucket
            """,
        ),
        (
            "taxi_analytics.daily_trip_counts",
            """
            INSERT INTO taxi_analytics.daily_trip_counts
            SELECT
                toDate(pickup_datetime) AS trip_date,
                count()                 AS trip_count
            FROM taxi_analytics.taxi_trips_clean
            GROUP BY trip_date
            """,
        ),
    ]
    for target_table, insert_sql in rebuilds:
        client.command(f"TRUNCATE TABLE IF EXISTS {target_table}")
        client.command(insert_sql)
        logger.info("  Rebuilt %s", target_table)


def record_run_status(
    client: Client,
    source_file: str,
    file_sha256: str,
    status: str,
    clean_rows: int = 0,
    reject_rows: int = 0,
    error_msg: str = "",
) -> None:
    """Append an immutable audit entry to `taxi_analytics.ingestion_runs`."""
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    finished_at = now_naive if status in ("completed", "failed") else None
    client.insert(
        table="taxi_analytics.ingestion_runs",
        data=[
            [
                source_file,
                file_sha256,
                status,
                clean_rows,
                reject_rows,
                error_msg[:500],
                now_naive,
                finished_at,
            ]
        ],
        column_names=[
            "source_file",
            "file_sha256",
            "status",
            "clean_rows",
            "reject_rows",
            "error_msg",
            "started_at",
            "finished_at",
        ],
    )


def ingest_parquet_file(
    client: Client,
    file_path: Path,
    batch_size: int = 50_000,
    skip_precheck: bool = False,
    file_sha256: Optional[str] = None,
) -> Tuple[int, int, bool]:
    """Stream, vector-validate, and load a single Parquet file into ClickHouse."""
    source_file = file_path.name
    sha = file_sha256 or compute_file_sha256(file_path)

    if not skip_precheck:
        existing_status = get_latest_run_status(client, source_file, sha)
        if existing_status == "completed":
            logger.info("SKIP %s (already completed with matching SHA-256)", source_file)
            return 0, 0, True
        if existing_status is not None:
            cleanup_partial_file_rows(client, source_file)
            rebuild_summary_tables(client)

    record_run_status(client, source_file, sha, status="started")
    started = time.perf_counter()

    clean_total = 0
    reject_total = 0
    row_offset = 0

    try:
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        logger.info("START %s (%s rows)", source_file, f"{total_rows:,}")

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            clean_dict, reject_batch = validate_arrow_batch(
                batch,
                source_file=source_file,
                row_offset=row_offset,
            )
            row_offset += batch.num_rows

            if clean_dict:
                # WHY: `column_oriented=True` inserts columnar Python lists directly
                # without transposing 50k rows into row-tuples first.
                clean_cols = [clean_dict[col] for col in CLEAN_COLUMNS]
                client.insert(
                    table="taxi_analytics.taxi_trips_clean",
                    data=clean_cols,
                    column_names=CLEAN_COLUMNS,
                    column_oriented=True,
                )
                clean_total += len(clean_dict["trip_hash"])

            if reject_batch:
                client.insert(
                    table="taxi_analytics.taxi_trips_rejects",
                    data=reject_batch,
                    column_names=REJECT_COLUMNS,
                )
                reject_total += len(reject_batch)

        elapsed = time.perf_counter() - started
        record_run_status(
            client,
            source_file,
            sha,
            status="completed",
            clean_rows=clean_total,
            reject_rows=reject_total,
        )
        logger.info(
            "DONE %s in %.1fs | clean=%s | rejects=%s",
            source_file,
            elapsed,
            f"{clean_total:,}",
            f"{reject_total:,}",
        )
        return clean_total, reject_total, False

    except Exception as exc:
        record_run_status(
            client,
            source_file,
            sha,
            status="failed",
            clean_rows=clean_total,
            reject_rows=reject_total,
            error_msg=str(exc),
        )
        logger.error("FAILED %s: %s", source_file, exc)
        raise


def _worker_ingest_file(
    file_path_str: str,
    file_sha256: str,
    batch_size: int,
    host: str,
    port: int,
    user: str,
    password: str,
) -> Tuple[str, int, int, bool]:
    """Process-pool worker entry point for parallel file ingestion."""
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database="taxi_analytics",
    )
    path = Path(file_path_str)
    clean_cnt, reject_cnt, skipped = ingest_parquet_file(
        client=client,
        file_path=path,
        batch_size=batch_size,
        skip_precheck=True,
        file_sha256=file_sha256,
    )
    return path.name, clean_cnt, reject_cnt, skipped


def run_ingestion(
    client: Client,
    parquet_files: List[Path],
    batch_size: int,
    workers: int,
    force: bool,
    host: str,
    port: int,
    user: str,
    password: str,
) -> Tuple[int, int, int]:
    """Coordinate idempotency pre-checks, parallel/sequential ingestion, and MV rebuilds.

    WHY:
        Separating pre-check cleanup from parallel worker execution ensures that
        if any file is retried or `--force`d, we clean raw tables before workers
        start and rebuild summary tables once after all workers finish—preventing
        race conditions and guaranteeing zero double-counting in summary tables.
    """
    to_ingest: List[Tuple[Path, str]] = []
    skipped_files = 0
    needs_mv_rebuild = False

    for path in parquet_files:
        sha = compute_file_sha256(path)
        existing_status = get_latest_run_status(client, path.name, sha)
        if existing_status == "completed" and not force:
            logger.info("SKIP %s (already completed with matching SHA-256)", path.name)
            skipped_files += 1
            continue
        if existing_status is not None or force:
            cleanup_partial_file_rows(client, path.name)
            needs_mv_rebuild = True
        to_ingest.append((path, sha))

    grand_clean = 0
    grand_reject = 0

    if to_ingest:
        if workers <= 1:
            for path, sha in to_ingest:
                clean_cnt, reject_cnt, _ = ingest_parquet_file(
                    client=client,
                    file_path=path,
                    batch_size=batch_size,
                    skip_precheck=True,
                    file_sha256=sha,
                )
                grand_clean += clean_cnt
                grand_reject += reject_cnt
        else:
            logger.info("Starting parallel ingestion with %d workers for %d files", workers, len(to_ingest))
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(
                        _worker_ingest_file,
                        str(path),
                        sha,
                        batch_size,
                        host,
                        port,
                        user,
                        password,
                    )
                    for path, sha in to_ingest
                ]
                for fut in as_completed(futures):
                    _, clean_cnt, reject_cnt, _ = fut.result()
                    grand_clean += clean_cnt
                    grand_reject += reject_cnt

    if needs_mv_rebuild:
        rebuild_summary_tables(client)

    return grand_clean, grand_reject, skipped_files


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the parallel ingestion runner."""
    parser = argparse.ArgumentParser(description="Parallel, vectorized NYC Yellow Taxi Parquet ingester.")
    parser.add_argument("--data-dir", type=Path, default=Path("nyc_yellow_taxi"))
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit-files", type=int, default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--host", default=os.getenv("CH_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "taxi_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "taxi_password"))
    return parser.parse_args()


def main() -> None:
    """Discover Parquet files and run coordinated ingestion."""
    args = parse_args()
    parquet_files = sorted(args.data_dir.glob("yellow_tripdata_*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No yellow_tripdata_*.parquet files found in {args.data_dir}")

    if args.limit_files is not None:
        parquet_files = parquet_files[: args.limit_files]

    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database="taxi_analytics",
    )

    t0 = time.perf_counter()
    grand_clean, grand_reject, skipped_files = run_ingestion(
        client=client,
        parquet_files=parquet_files,
        batch_size=args.batch_size,
        workers=args.workers,
        force=args.force,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
    )

    logger.info(
        "ALL DONE in %.1fs | files=%d (skipped=%d) | new_clean=%s | new_rejects=%s",
        time.perf_counter() - t0,
        len(parquet_files),
        skipped_files,
        f"{grand_clean:,}",
        f"{grand_reject:,}",
    )


if __name__ == "__main__":
    main()