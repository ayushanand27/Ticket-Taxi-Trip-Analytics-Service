"""
db_init.py — Database Schema Initializer & Taxi Zone Seeder.

WHY THIS MODULE EXISTS:
    Provides a single, idempotent entry point to:
      1. Wait for the ClickHouse Docker container to become healthy.
      2. Execute `01_schema.sql` statement by statement (logging each statement
         so any SQL syntax or permission failure pinpoints the exact block).
      3. Load `taxi_zone_lookup.csv` into `taxi_analytics.taxi_zones` without
         creating duplicate rows if run multiple times.

USAGE (Windows PowerShell):
    python db_init.py --schema 01_schema.sql --zones nyc_yellow_taxi/taxi_zone_lookup.csv
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import time
from pathlib import Path
from typing import List, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client

# WHY: Structured logging with timestamps makes it immediately obvious which step
# failed during setup instead of printing ambiguous tracebacks.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)


def get_clickhouse_client(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
    max_retries: int = 10,
    retry_delay_sec: float = 2.0,
) -> Client:
    """Create a ClickHouse HTTP client, retrying until the server is ready.

    WHY:
        When running `docker compose up -d` followed immediately by `db_init.py`,
        ClickHouse may take a few seconds to finish initializing. Retrying avoids
        flaky startup crashes.

    Args:
        host: Hostname where ClickHouse is reachable (default: localhost).
        port: HTTP interface port (default: 8123).
        username: ClickHouse username.
        password: ClickHouse password.
        database: Target database name (connects to 'default' first if needed).
        max_retries: Maximum connection attempts before failing loudly.
        retry_delay_sec: Seconds to sleep between attempts.

    Returns:
        An active `clickhouse_connect` Client instance.

    Raises:
        RuntimeError: If ClickHouse does not respond after `max_retries`.
    """
    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            # WHY: Connect to 'default' first because `taxi_analytics` might not exist
            # before `01_schema.sql` runs (even though docker-compose sets CLICKHOUSE_DB).
            client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=username,
                password=password,
                database="default",
            )
            client.ping()
            logger.info("Connected to ClickHouse at %s:%s (attempt %d)", host, port, attempt)
            return client
        except Exception as exc:  # noqa: BLE001 — intentional broad catch during startup poll
            last_error = exc
            logger.warning(
                "ClickHouse not ready yet (attempt %d/%d): %s",
                attempt,
                max_retries,
                exc,
            )
            time.sleep(retry_delay_sec)

    raise RuntimeError(
        f"Could not connect to ClickHouse at {host}:{port} after {max_retries} attempts"
    ) from last_error


def split_sql_statements(sql_text: str) -> List[str]:
    """Split a multi-statement SQL file into individual executable statements.

    WHY:
        ClickHouse's HTTP `command()` endpoint executes one SQL statement per call.
        Stripping `--` comment lines first ensures comment-only blocks aren't sent
        as empty queries.

    Args:
        sql_text: Raw contents of a `.sql` file.

    Returns:
        List of non-empty SQL statements without trailing semicolons.
    """
    cleaned_lines: List[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        # WHY: Ignore pure comment lines so semicolons inside comments never break splitting.
        if stripped.startswith("--") or not stripped:
            continue
        cleaned_lines.append(line)

    joined = "\n".join(cleaned_lines)
    return [stmt.strip() for stmt in joined.split(";") if stmt.strip()]


def apply_schema(client: Client, schema_path: Path) -> None:
    """Execute all DDL statements in `schema_path` sequentially.

    WHY:
        Executes each `CREATE DATABASE / TABLE / MATERIALIZED VIEW` statement
        individually and logs the first line of the statement so any schema error
        is immediately traceable.

    Args:
        client: Active ClickHouse client.
        schema_path: Path to `01_schema.sql`.
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    sql_text = schema_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)
    logger.info("Applying %d DDL statements from %s", len(statements), schema_path)

    for idx, stmt in enumerate(statements, start=1):
        first_line = stmt.splitlines()[0][:80]
        try:
            client.command(stmt)
            logger.info("  [%d/%d] OK: %s", idx, len(statements), first_line)
        except Exception as exc:
            logger.error("  [%d/%d] FAILED: %s", idx, len(statements), first_line)
            raise RuntimeError(f"DDL failed at statement {idx}: {first_line}") from exc


def load_taxi_zones(client: Client, zones_csv_path: Path) -> None:
    """Idempotently load `taxi_zone_lookup.csv` into `taxi_analytics.taxi_zones`.

    WHY:
        We truncate `taxi_zones` before inserting because it is a tiny 265-row
        reference table; truncating + reloading guarantees zero duplicates on
        re-runs and picks up any upstream TLC zone updates cleanly.

    Args:
        client: Active ClickHouse client.
        zones_csv_path: Path to `taxi_zone_lookup.csv`.
    """
    if not zones_csv_path.exists():
        raise FileNotFoundError(
            f"Taxi zones CSV not found at {zones_csv_path}. "
            "Download it from https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
        )

    rows: List[Tuple[int, str, str, str]] = []
    with zones_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for record in reader:
            # WHY: Explicitly cast LocationID to int and default missing strings to "Unknown"
            # so ClickHouse's strict UInt16 / String types never fail on malformed CSV cells.
            rows.append(
                (
                    int(record["LocationID"]),
                    (record.get("Borough") or "Unknown").strip(),
                    (record.get("Zone") or "Unknown").strip(),
                    (record.get("service_zone") or "Unknown").strip(),
                )
            )

    # WHY: TRUNCATE + INSERT is atomic enough for a static 265-row dimension table
    # and guarantees idempotency without needing ReplacingMergeTree deduplication.
    client.command("TRUNCATE TABLE IF EXISTS taxi_analytics.taxi_zones")
    client.insert(
        table="taxi_analytics.taxi_zones",
        data=rows,
        column_names=["location_id", "borough", "zone", "service_zone"],
    )
    logger.info("Loaded %d taxi zones into taxi_analytics.taxi_zones", len(rows))


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments with sensible defaults matching `docker-compose.yml`."""
    parser = argparse.ArgumentParser(description="Initialize ClickHouse schema and taxi zones.")
    parser.add_argument("--schema", type=Path, default=Path("01_schema.sql"), help="Path to SQL DDL file")
    parser.add_argument(
        "--zones",
        type=Path,
        default=Path("nyc_yellow_taxi/taxi_zone_lookup.csv"),
        help="Path to taxi_zone_lookup.csv",
    )
    parser.add_argument("--host", default=os.getenv("CH_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--user", default=os.getenv("CH_USER", "taxi_user"))
    parser.add_argument("--password", default=os.getenv("CH_PASSWORD", "taxi_password"))
    return parser.parse_args()


def main() -> None:
    """Entry point: connect, apply DDL, and seed taxi_zones."""
    args = parse_args()
    client = get_clickhouse_client(
        host=args.host,
        port=args.port,
        username=args.user,
        password=args.password,
        database="taxi_analytics",
    )
    apply_schema(client, args.schema)
    load_taxi_zones(client, args.zones)
    logger.info("Step 2 complete: ClickHouse schema and taxi_zones are ready.")


if __name__ == "__main__":
    main()