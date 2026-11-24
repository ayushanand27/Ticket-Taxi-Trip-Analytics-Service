"""
bench_ingest.py — Fair 1-worker vs N-worker ingestion timing on the SAME files.

WHY THIS MODULE EXISTS:
    The README used to claim a "3.3x" parallel speed-up by comparing 1 file at
    1 worker against 4 files at 4 workers. Those are different workloads, so the
    number proved nothing. This script times the exact same set of Parquet files
    once per worker count and reports the real speed-up.

WHY IT USES --force:
    Re-ingesting already-loaded files normally just SKIPs. `force=True` deletes
    and reloads them, so every run does the full amount of work. Because the
    pipeline is idempotent, the final data is identical afterwards.

WHAT THE TIMING INCLUDES (and why it is still fair):
    The whole `run_ingestion()` call: SHA-256 check, delete of old rows, parallel
    load, and the one-time summary-table rebuild. The delete and rebuild cost the
    same for every worker count, so they slightly UNDERSTATE the parallel gain;
    a result that still looks good is therefore conservative.
    Before timing, every file is read once (SHA-256) so the OS file cache is warm
    for all runs and the first run is not penalised for cold disk reads.

SAFETY:
    This modifies the real `taxi_analytics` tables (delete + reload of the chosen
    files). It refuses to run without `--yes`.

USAGE (Windows PowerShell):
    python bench_ingest.py --data-dir nyc_yellow_taxi --files 4 --yes
"""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import List, Tuple

import clickhouse_connect
import pyarrow.parquet as pq

from ingest import compute_file_sha256, run_ingestion


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Time ingestion of the same files at several worker counts.")
    parser.add_argument("--data-dir", type=Path, default=Path("nyc_yellow_taxi"))
    parser.add_argument("--files", type=int, default=4, help="How many monthly files to use (same set every run)")
    parser.add_argument("--workers-list", type=int, nargs="+", default=[1, 4], help="Worker counts to compare")
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--yes", action="store_true", help="Confirm that the chosen files will be deleted and reloaded")
    parser.add_argument("--host", default=os.getenv("CH_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "taxi_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "taxi_password"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    files: List[Path] = sorted(args.data_dir.glob("yellow_tripdata_*.parquet"))[: args.files]
    if not files:
        raise FileNotFoundError(f"No yellow_tripdata_*.parquet files in {args.data_dir}")

    total_rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    print(f"Files ({len(files)}): {', '.join(f.name for f in files)}")
    print(f"Total rows per run: {total_rows:,}")

    if not args.yes:
        print("\nThis will DELETE and RELOAD these files in the real `taxi_analytics` tables.")
        print("The final data is identical, but it takes minutes per run. Re-run with --yes to proceed.")
        return

    client = clickhouse_connect.get_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database="taxi_analytics",
    )

    # Warm the OS file cache so no run pays for cold reads.
    for path in files:
        compute_file_sha256(path)

    results: List[Tuple[int, float]] = []
    for workers in args.workers_list:
        print(f"\n=== {workers} worker(s) ===")
        started = time.perf_counter()
        run_ingestion(
            client=client,
            parquet_files=files,
            batch_size=args.batch_size,
            workers=workers,
            force=True,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )
        results.append((workers, time.perf_counter() - started))

    baseline = results[0][1]
    print("\n| Workers | Files | Rows | Wall-clock (s) | Rows/sec | Speed-up vs first |")
    print("| ---: | ---: | ---: | ---: | ---: | ---: |")
    for workers, seconds in results:
        print(
            f"| {workers} | {len(files)} | {total_rows:,} | {seconds:.1f} | "
            f"{total_rows / seconds:,.0f} | {baseline / seconds:.2f}x |"
        )


if __name__ == "__main__":
    main()
