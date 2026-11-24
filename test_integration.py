"""
test_integration.py — Integration tests against a REAL ClickHouse server.

WHY THIS MODULE EXISTS:
    `test_pipeline.py` uses a fake ClickHouse client. That proves our Python
    logic, but it cannot prove that the real SQL does what we think. The most
    important claim in this project is "re-running ingestion never double-counts
    the summary tables". That claim depends on ClickHouse behaviour (materialized
    views fire on INSERT, but `ALTER ... DELETE` does NOT undo them), so it has
    to be checked against a real server.

WHY THE TESTS RUN IN A SEPARATE DATABASE:
    The real data (43.5M rows, hours of ingestion) lives in `taxi_analytics`.
    These tests must never be able to touch it. So they:
      1. create a throw-away database called `taxi_analytics_itest`,
      2. apply `01_schema.sql` to it with the database name swapped,
      3. load a patched copy of `ingest.py` whose SQL points at that database,
      4. drop the throw-away database when the tests finish.
    A safety assertion fails the run if any un-patched `taxi_analytics` name is
    left in the patched code, so a mistake cannot silently hit the real tables.

WHY IT IS OPT-IN:
    It needs Docker + ClickHouse running. A plain `pytest` run on a laptop
    without Docker must not fail, so the whole module is skipped unless
    `RUN_CH_INTEGRATION=1` is set.

USAGE (Windows PowerShell, Docker Desktop running, `docker compose up -d` done):
    $env:RUN_CH_INTEGRATION = "1"
    python -m pytest test_integration.py -v
"""

from __future__ import annotations

import os
import re
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Iterator

import clickhouse_connect
import pytest

from db_init import split_sql_statements

# WHY: reuse the exact same 5-row sample (2 valid, 3 dirty) as the unit tests so
# both test files agree on what "correct" means. One source of truth.
from test_pipeline import _write_sample_parquet

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_CH_INTEGRATION") != "1",
    reason="Set RUN_CH_INTEGRATION=1 (with ClickHouse running) to run integration tests.",
)

REPO_DIR = Path(__file__).resolve().parent
PROD_DB = "taxi_analytics"
TEST_DB = "taxi_analytics_itest"

# The sample file's expected outcome: 2 clean trips, 3 rejects (one per reason).
EXPECTED_CLEAN = 2
EXPECTED_REJECTS = {
    "invalid_trip_distance": 1,
    "non_positive_fare": 1,
    "dropoff_before_pickup": 1,
}
SUMMARY_TABLES = ("hourly_zone_stats", "distance_bucket_stats", "daily_trip_counts")
ALL_TABLES = (
    "taxi_trips_clean",
    "taxi_trips_rejects",
    "ingestion_runs",
    *SUMMARY_TABLES,
)


def _patch_db_name(text: str) -> str:
    """Swap the production database name for the test one, and prove nothing was missed.

    WHY:
        `ingest.py` hard-codes `taxi_analytics.` in its SQL. Rewriting the name is
        the least invasive way to point it at a scratch database without changing
        production code. The regex check fails loudly if any occurrence survived.
    """
    patched = text.replace(PROD_DB, TEST_DB)
    leftovers = re.findall(rf"{PROD_DB}(?!_itest)", patched)
    assert not leftovers, "Refusing to run: un-patched production database name found."
    return patched


def _load_ingest_for_test_db() -> types.ModuleType:
    """Execute a patched copy of `ingest.py` as module `ingest_itest`.

    WHY:
        Importing `ingest` normally would give us the production-bound module.
        Executing patched source gives us the SAME functions (`run_ingestion`,
        `rebuild_summary_tables`, ...) bound to the scratch database.
    """
    source = (REPO_DIR / "ingest.py").read_text(encoding="utf-8")
    patched = _patch_db_name(source)
    module = types.ModuleType("ingest_itest")
    module.__file__ = str(REPO_DIR / "ingest.py")
    sys.modules["ingest_itest"] = module
    exec(compile(patched, str(REPO_DIR / "ingest.py"), "exec"), module.__dict__)
    return module


@pytest.fixture(scope="module")
def ch_env() -> Iterator[SimpleNamespace]:
    """Create the scratch database + schema once, drop it when the module ends."""
    host = os.getenv("CH_HOST", "127.0.0.1")
    port = int(os.getenv("CH_PORT", "8123"))
    user = os.getenv("CH_USER", "taxi_user")
    password = os.getenv("CH_PASSWORD", "taxi_password")

    assert TEST_DB != PROD_DB
    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database="default"
        )
    except Exception as exc:  # server not running, wrong credentials, ...
        pytest.skip(f"ClickHouse not reachable at {host}:{port}: {exc}")

    client.command(f"DROP DATABASE IF EXISTS {TEST_DB}")
    schema_sql = _patch_db_name((REPO_DIR / "01_schema.sql").read_text(encoding="utf-8"))
    for statement in split_sql_statements(schema_sql):
        client.command(statement)

    ingest_mod = _load_ingest_for_test_db()
    env = SimpleNamespace(
        client=client, ingest=ingest_mod, host=host, port=port, user=user, password=password
    )
    try:
        yield env
    finally:
        client.command(f"DROP DATABASE IF EXISTS {TEST_DB}")


@pytest.fixture()
def env(ch_env: SimpleNamespace) -> SimpleNamespace:
    """Empty every scratch table before each test so tests cannot affect each other."""
    for table in ALL_TABLES:
        ch_env.client.command(f"TRUNCATE TABLE {TEST_DB}.{table}")
    return ch_env


@pytest.fixture()
def sample_file(tmp_path: Path) -> Path:
    """A monthly-style Parquet file with 2 valid and 3 dirty rows."""
    path = tmp_path / "yellow_tripdata_2026-03.parquet"
    _write_sample_parquet(path)
    return path


def _scalar(env: SimpleNamespace, sql: str) -> Any:
    return env.client.query(sql).result_rows[0][0]


def _run(env: SimpleNamespace, path: Path, force: bool = False):
    """Run the real `run_ingestion` (single process) against the scratch database."""
    return env.ingest.run_ingestion(
        client=env.client,
        parquet_files=[path],
        batch_size=50_000,
        workers=1,  # WHY: the patched module is not importable by child processes.
        force=force,
        host=env.host,
        port=env.port,
        user=env.user,
        password=env.password,
    )


def _summary_trip_total(env: SimpleNamespace, table: str) -> int:
    # WHY: always `sum(trip_count)`; SummingMergeTree merges parts lazily, so raw
    # row counts of a summary table are not meaningful.
    return int(_scalar(env, f"SELECT sum(trip_count) FROM {TEST_DB}.{table}"))


def _assert_state_is_exactly_one_copy(env: SimpleNamespace) -> None:
    """Raw tables and ALL three summary tables must reflect exactly one copy of the file."""
    assert int(_scalar(env, f"SELECT count() FROM {TEST_DB}.taxi_trips_clean")) == EXPECTED_CLEAN
    rejects = dict(
        env.client.query(
            f"SELECT reject_reason, count() FROM {TEST_DB}.taxi_trips_rejects GROUP BY reject_reason"
        ).result_rows
    )
    assert rejects == EXPECTED_REJECTS
    for table in SUMMARY_TABLES:
        assert _summary_trip_total(env, table) == EXPECTED_CLEAN, f"{table} does not match raw data"


def test_first_ingest_loads_clean_rejects_and_summaries(env: SimpleNamespace, sample_file: Path) -> None:
    """Baseline: one ingest fills raw tables and the materialized views correctly.

    WHY: if this fails, nothing else in this file means anything.
    """
    clean, rejects, skipped = _run(env, sample_file)
    assert (clean, rejects, skipped) == (EXPECTED_CLEAN, sum(EXPECTED_REJECTS.values()), 0)
    _assert_state_is_exactly_one_copy(env)

    # Spot-check values, not just counts: the endpoints serve these exact numbers.
    buckets = {
        row[0]: (int(row[1]), float(row[2]))
        for row in env.client.query(
            f"SELECT distance_bucket, sum(trip_count), sum(total_fare_sum) "
            f"FROM {TEST_DB}.distance_bucket_stats GROUP BY distance_bucket"
        ).result_rows
    }
    assert buckets == {"1-3 mi": (1, 15.0), "3-5 mi": (1, 20.0)}  # 2.5 mi / $15, 4.0 mi / $20

    days = env.client.query(
        f"SELECT trip_date, sum(trip_count) FROM {TEST_DB}.daily_trip_counts GROUP BY trip_date"
    ).result_rows
    assert [(str(d), int(n)) for d, n in days] == [("2026-03-15", 2)]


def test_completed_file_is_skipped_on_rerun(env: SimpleNamespace, sample_file: Path) -> None:
    """Re-running without --force must be a no-op (same SHA-256, status 'completed').

    WHY: this is the cheap, common case (the "SKIP in 2.0s for 12 files" claim).
    """
    _run(env, sample_file)
    runs_before = int(_scalar(env, f"SELECT count() FROM {TEST_DB}.ingestion_runs"))

    clean, rejects, skipped = _run(env, sample_file)

    assert (clean, rejects, skipped) == (0, 0, 1)
    assert int(_scalar(env, f"SELECT count() FROM {TEST_DB}.ingestion_runs")) == runs_before
    _assert_state_is_exactly_one_copy(env)


def test_force_reingest_does_not_double_count_summaries(env: SimpleNamespace, sample_file: Path) -> None:
    """THE key test: `--force` re-ingest must leave raw AND summary tables at one copy.

    WHY: this is the bug found in review. Deleting raw rows does not undo what the
    materialized views already added to the summary tables, so without the rebuild
    step the API would silently report double the trips.
    """
    _run(env, sample_file)
    _run(env, sample_file, force=True)
    _assert_state_is_exactly_one_copy(env)


def test_retry_after_failed_run_does_not_double_count(env: SimpleNamespace, sample_file: Path) -> None:
    """A file whose latest run is 'failed' must be cleaned and reloaded without --force.

    WHY: simulates a crash. We load the file, then append a newer 'failed' ledger
    row for the same SHA-256 (as the pipeline would after an exception). The next
    plain run must treat the earlier rows as partial, delete them, reload, and
    rebuild the summaries.
    """
    _run(env, sample_file)
    sha = env.ingest.compute_file_sha256(sample_file)
    future = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=1)  # WHY: must sort after the 'completed' row
    env.client.insert(
        table=f"{TEST_DB}.ingestion_runs",
        data=[[sample_file.name, sha, "failed", 0, 0, "simulated crash", future, future]],
        column_names=[
            "source_file", "file_sha256", "status", "clean_rows",
            "reject_rows", "error_msg", "started_at", "finished_at",
        ],
    )

    clean, rejects, skipped = _run(env, sample_file)

    assert skipped == 0
    assert (clean, rejects) == (EXPECTED_CLEAN, sum(EXPECTED_REJECTS.values()))
    _assert_state_is_exactly_one_copy(env)


def test_without_rebuild_the_summaries_would_double_count(env: SimpleNamespace, sample_file: Path) -> None:
    """Negative control: prove the double-counting bug is REAL and the test can detect it.

    WHY:
        A test that has never been seen failing proves little. Here we deliberately
        skip `rebuild_summary_tables()` (delete raw rows, re-ingest, no rebuild) and
        assert the summaries are WRONG (2x). If ClickHouse ever changes so that this
        no longer double counts, this test fails and tells us the rebuild step may
        be unnecessary.
    """
    _run(env, sample_file)
    env.ingest.cleanup_partial_file_rows(env.client, sample_file.name)
    env.ingest.ingest_parquet_file(
        client=env.client,
        file_path=sample_file,
        skip_precheck=True,
    )

    assert int(_scalar(env, f"SELECT count() FROM {TEST_DB}.taxi_trips_clean")) == EXPECTED_CLEAN
    for table in SUMMARY_TABLES:
        assert _summary_trip_total(env, table) == 2 * EXPECTED_CLEAN, (
            f"{table} was expected to double count when the rebuild step is skipped"
        )
