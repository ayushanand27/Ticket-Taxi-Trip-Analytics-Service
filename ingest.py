"""
ingest.py — Idempotent, Streaming Parquet Ingestion Pipeline for ClickHouse.

WHY THIS MODULE EXISTS:
    Loads 12 months of NYC Yellow Taxi Parquet files (~30-40M rows) into
    `taxi_analytics.taxi_trips_clean` while quarantining dirty rows into
    `taxi_analytics.taxi_trips_rejects` with explicit rejection reasons.

    Key engineering guarantees:
      1. Streaming / Low Memory: Reads Parquet files in 50,000-row batches via
         `pyarrow.parquet.ParquetFile.iter_batches()` so RAM stays flat (~200 MB)
         instead of loading 40M rows into memory at once.
      2. Re-runnable Without Duplicates (Idempotent): Computes each file's SHA-256
         checksum and checks `taxi_analytics.ingestion_runs`. Completed files are
         skipped; failed/partial files have their partial rows deleted synchronously
         before retrying.
      3. Observable Progress: Logs per-batch throughput and per-file clean/reject
         counts so you can immediately see where time is spent or if anything breaks.

USAGE (Windows PowerShell):
    # Test on 1 file first to verify end-to-end speed:
    python ingest.py --data-dir nyc_yellow_taxi --limit-files 1

    # Ingest all 12 months (safe to re-run; already-completed files are skipped):
    python ingest.py --data-dir nyc_yellow_taxi
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import clickhouse_connect
import pyarrow.parquet as pq
from clickhouse_connect.driver.client import Client

from cleaning import serialize_raw_payload, validate_trip_row

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

# WHY: Explicit column order for ClickHouse inserts. Matching this tuple to
# `cleaned_row` keys avoids dictionary-lookup overhead on 40M rows.
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
    """Compute the SHA-256 hex digest of a file without loading it into RAM.

    WHY:
        Using SHA-256 instead of just filename ensures that if NYC TLC republishes
        a corrected Parquet file with the same name, our pipeline detects the
        new hash and re-ingests it cleanly instead of falsely skipping it.
    """
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
    """Check `ingestion_runs` for the latest status of `(source_file, file_sha256)`.

    WHY:
        Returns `'completed'`, `'started'`, `'failed'`, or `None` (never seen).
        This drives our idempotency decision before touching any trip rows.
    """
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
    """Synchronously delete any rows from an incomplete prior run of `source_file`.

    WHY:
        In ClickHouse, `ALTER TABLE ... DELETE` is asynchronous by default.
        Passing `SETTINGS mutations_sync = 1` forces ClickHouse to finish the
        deletion before we start inserting the retry, preventing race-condition
        duplicates.
    """
    logger.warning("Cleaning up partial rows for %s before retrying...", source_file)
    for table in ("taxi_analytics.taxi_trips_clean", "taxi_analytics.taxi_trips_rejects"):
        client.command(
            f"ALTER TABLE {table} DELETE WHERE source_file = %(source_file)s SETTINGS mutations_sync = 1",
            parameters={"source_file": source_file},
        )


def record_run_status(
    client: Client,
    source_file: str,
    file_sha256: str,
    status: str,
    clean_rows: int = 0,
    reject_rows: int = 0,
    error_msg: str = "",
) -> None:
    """Append an immutable audit entry to `taxi_analytics.ingestion_runs`.

    WHY:
        Append-only status logging avoids ClickHouse `ALTER TABLE UPDATE`
        mutations and preserves a full history of every ingestion attempt.
    """
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
    force: bool = False,
) -> Tuple[int, int, bool]:
    """Stream, validate, and load a single Parquet file into ClickHouse.

    WHY:
        Processes one `pyarrow` RecordBatch at a time so memory stays bounded.
        Valid rows go to `taxi_trips_clean` (which automatically feeds our 3
        Materialized Views on insert); dirty rows go to `taxi_trips_rejects`.

    Returns:
        Tuple of `(clean_count, reject_count, was_skipped)`.
    """
    source_file = file_path.name
    file_sha256 = compute_file_sha256(file_path)

    existing_status = get_latest_run_status(client, source_file, file_sha256)
    if existing_status == "completed" and not force:
        logger.info("SKIP %s (already completed with matching SHA-256)", source_file)
        return 0, 0, True

    if existing_status is not None or force:
        cleanup_partial_file_rows(client, source_file)

    record_run_status(client, source_file, file_sha256, status="started")
    started = time.perf_counter()

    clean_total = 0
    reject_total = 0
    global_row_idx = 0

    try:
        parquet_file = pq.ParquetFile(file_path)
        total_rows = parquet_file.metadata.num_rows
        logger.info("START %s (%s rows)", source_file, f"{total_rows:,}")

        for batch in parquet_file.iter_batches(batch_size=batch_size):
            # WHY: Converting one 50k-row Arrow batch to Python dicts is fast
            # (~100 ms) and keeps our pure `validate_trip_row` unit-tested logic
            # as the single source of truth for data quality.
            rows: List[Dict[str, Any]] = batch.to_pylist()
            clean_batch: List[List[Any]] = []
            reject_batch: List[List[Any]] = []

            for raw_row in rows:
                res = validate_trip_row(raw_row, source_file=source_file, row_index=global_row_idx)
                if res.is_valid and res.cleaned_row is not None:
                    clean_batch.append([res.cleaned_row[col] for col in CLEAN_COLUMNS])
                else:
                    reject_batch.append(
                        [
                            source_file,
                            global_row_idx,
                            res.reject_reason or "unknown_error",
                            serialize_raw_payload(raw_row),
                        ]
                    )
                global_row_idx += 1

            if clean_batch:
                client.insert(
                    table="taxi_analytics.taxi_trips_clean",
                    data=clean_batch,
                    column_names=CLEAN_COLUMNS,
                )
                clean_total += len(clean_batch)

            if reject_batch:
                client.insert(
                    table="taxi_analytics.taxi_trips_rejects",
                    data=reject_batch,
                    column_names=REJECT_COLUMNS,
                )
                reject_total += len(reject_batch)

            logger.info(
                "  %s progress: %s / %s rows (clean=%s, rejects=%s)",
                source_file,
                f"{global_row_idx:,}",
                f"{total_rows:,}",
                f"{clean_total:,}",
                f"{reject_total:,}",
            )

        elapsed = time.perf_counter() - started
        record_run_status(
            client,
            source_file,
            file_sha256,
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
            file_sha256,
            status="failed",
            clean_rows=clean_total,
            reject_rows=reject_total,
            error_msg=str(exc),
        )
        logger.error("FAILED %s: %s", source_file, exc)
        raise


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the ingestion runner."""
    parser = argparse.ArgumentParser(description="Idempotent NYC Yellow Taxi Parquet ingester.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("nyc_yellow_taxi"),
        help="Directory containing yellow_tripdata_*.parquet files",
    )
    parser.add_argument("--batch-size", type=int, default=50_000, help="Rows per Arrow/ClickHouse batch")
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Optional cap on number of Parquet files to ingest (great for quick smoke tests)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-ingest files even if already marked completed (deletes prior rows first)",
    )
    parser.add_argument("--host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "taxi_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "taxi_password"))
    return parser.parse_args()


def main() -> None:
    """Discover Parquet files, ingest them in chronological order, and print summary."""
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

    grand_clean = 0
    grand_reject = 0
    skipped_files = 0
    t0 = time.perf_counter()

    for path in parquet_files:
        clean_cnt, reject_cnt, skipped = ingest_parquet_file(
            client=client,
            file_path=path,
            batch_size=args.batch_size,
            force=args.force,
        )
        grand_clean += clean_cnt
        grand_reject += reject_cnt
        if skipped:
            skipped_files += 1

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