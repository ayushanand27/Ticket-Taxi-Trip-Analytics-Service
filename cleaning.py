"""
cleaning.py — Pure & Vectorized Validation Logic for NYC Yellow Taxi Trips.

WHY THIS MODULE EXISTS:
    Provides two complementary layers of validation:
      1. `validate_trip_row()`: Pure Python row-level validator, ideal for
         readable unit tests and single-record debugging.
      2. `validate_arrow_batch()`: Vectorized PyArrow (`pyarrow.compute`)
         validator that evaluates all 5 cleaning rules in C++ across an entire
         50,000-row RecordBatch at once, avoiding per-row Python loops for the
         ~93.6% valid rows.

    Both functions enforce the exact same rule precedence so unit tests and
    production ingestion stay 100% consistent.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc

# WHY: NYC TLC monthly Parquet files occasionally contain stray corrupt timestamps
# from years like 2002, 2009, or 2099. Bounding to our 2025-2026 dataset window
# prevents garbage partitions from being created in ClickHouse.
MIN_VALID_PICKUP_DT = datetime(2025, 1, 1, 0, 0, 0)
MAX_VALID_PICKUP_DT = datetime(2026, 12, 31, 23, 59, 59)

# WHY: NYC taxi trips over 500 miles in a single metered ride are virtually always
# GPS/odometer glitches that skew average distance and fare-bucket metrics.
MAX_VALID_DISTANCE_MILES = 500.0


@dataclass(frozen=True)
class ValidationResult:
    """Result of validating a single raw Parquet trip row."""

    is_valid: bool
    cleaned_row: Optional[Dict[str, Any]] = None
    reject_reason: Optional[str] = None


def _is_missing(value: Any) -> bool:
    """Return True if `value` is None or a floating-point NaN."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _to_naive_datetime(value: Any) -> Optional[datetime]:
    """Normalize a timestamp value to a timezone-naive Python `datetime` (wall clock as written)."""
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        return datetime.fromisoformat(value).replace(tzinfo=None)
    return None


def _wallclock_as_utc(value: datetime) -> datetime:
    """Tag a wall-clock datetime as UTC before it goes to ClickHouse.

    WHY:
        clickhouse-connect turns a *naive* datetime into an epoch using the CLIENT machine's
        local timezone. On an IST laptop that stored every timestamp 5h30m earlier than the
        Parquet value (found by comparing live vs batch windows in Ticket 2). Marking the value
        UTC makes the stored epoch match the wall clock in the file, on any machine.
    """
    return value.replace(tzinfo=timezone.utc)


def compute_trip_hash(source_file: str, row_index: int) -> int:
    """Compute a deterministic 64-bit unsigned integer ID for a trip row.

    WHY:
        Python's built-in `hash()` is randomly salted per process (`PYTHONHASHSEED`).
        Using `blake2b` with an 8-byte digest guarantees the exact same `UInt64`
        `trip_hash` across runs and worker processes.
    """
    payload = f"{source_file}:{row_index}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


def serialize_raw_payload(raw_row: Dict[str, Any]) -> str:
    """Safely serialize a raw Parquet row dictionary to a JSON string."""
    sanitized: Dict[str, Any] = {}
    for key, val in raw_row.items():
        if isinstance(val, datetime):
            sanitized[key] = val.isoformat()
        elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            sanitized[key] = None
        else:
            sanitized[key] = val
    return json.dumps(sanitized, default=str)


def validate_trip_row(
    raw_row: Dict[str, Any],
    source_file: str,
    row_index: int,
) -> ValidationResult:
    """Validate and normalize a single NYC Yellow Taxi trip record."""
    required_fields = (
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
    )
    for field in required_fields:
        if _is_missing(raw_row.get(field)):
            return ValidationResult(is_valid=False, reject_reason="missing_required_field")

    pickup_dt = _to_naive_datetime(raw_row["tpep_pickup_datetime"])
    dropoff_dt = _to_naive_datetime(raw_row["tpep_dropoff_datetime"])
    if pickup_dt is None or dropoff_dt is None:
        return ValidationResult(is_valid=False, reject_reason="missing_required_field")

    if not (MIN_VALID_PICKUP_DT <= pickup_dt <= MAX_VALID_PICKUP_DT):
        return ValidationResult(is_valid=False, reject_reason="out_of_range_date")
    if not (MIN_VALID_PICKUP_DT <= dropoff_dt <= MAX_VALID_PICKUP_DT):
        return ValidationResult(is_valid=False, reject_reason="out_of_range_date")

    if dropoff_dt <= pickup_dt:
        return ValidationResult(is_valid=False, reject_reason="dropoff_before_pickup")

    trip_distance = float(raw_row["trip_distance"])
    if trip_distance <= 0.0 or trip_distance > MAX_VALID_DISTANCE_MILES:
        return ValidationResult(is_valid=False, reject_reason="invalid_trip_distance")

    fare_amount = float(raw_row["fare_amount"])
    total_amount = float(raw_row["total_amount"])
    if fare_amount <= 0.0 or total_amount <= 0.0:
        return ValidationResult(is_valid=False, reject_reason="non_positive_fare")

    vendor_raw = raw_row.get("VendorID")
    vendor_id = 0 if _is_missing(vendor_raw) else max(0, int(vendor_raw))

    passenger_raw = raw_row.get("passenger_count")
    passenger_count = None if _is_missing(passenger_raw) else max(0, int(passenger_raw))

    payment_raw = raw_row.get("payment_type")
    payment_type = 0 if _is_missing(payment_raw) else max(0, int(payment_raw))

    tip_raw = raw_row.get("tip_amount")
    tip_amount = 0.0 if _is_missing(tip_raw) else float(tip_raw)

    cbd_raw = raw_row.get("cbd_congestion_fee")
    cbd_congestion_fee = None if _is_missing(cbd_raw) else float(cbd_raw)

    cleaned_row = {
        "trip_hash": compute_trip_hash(source_file, row_index),
        "source_file": source_file,
        "vendor_id": vendor_id,
        "pickup_datetime": _wallclock_as_utc(pickup_dt),
        "dropoff_datetime": _wallclock_as_utc(dropoff_dt),
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "pu_location_id": int(raw_row["PULocationID"]),
        "do_location_id": int(raw_row["DOLocationID"]),
        "payment_type": payment_type,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "total_amount": total_amount,
        "cbd_congestion_fee": cbd_congestion_fee,
    }
    return ValidationResult(is_valid=True, cleaned_row=cleaned_row)


def _arrow_missing_mask(arr: pa.Array) -> pa.BooleanArray:
    """Return a boolean mask where `arr` is null or floating-point NaN."""
    null_mask = pc.is_null(arr)
    if pa.types.is_floating(arr.type):
        nan_mask = pc.fill_null(pc.is_nan(arr), False)
        return pc.or_(null_mask, nan_mask)
    return null_mask


def validate_arrow_batch(
    batch: pa.RecordBatch,
    source_file: str,
    row_offset: int,
) -> Tuple[Dict[str, List[Any]], List[List[Any]]]:
    """Vectorized PyArrow batch validator matching `validate_trip_row` rules.

    WHY:
        Evaluating all 5 cleaning rules in C++ via `pyarrow.compute` avoids
        converting 43.5M valid rows into Python dicts one-by-one. Only the ~6.4%
        rejected rows are materialized as Python dicts for `raw_payload` JSON.

    Args:
        batch: A `pyarrow.RecordBatch` from `ParquetFile.iter_batches()`.
        source_file: Parquet filename for traceability.
        row_offset: Starting 0-based row index of this batch in `source_file`.

    Returns:
        `(clean_columns_dict, reject_rows_list)`:
          - `clean_columns_dict`: Column-oriented dict ready for ClickHouse columnar insert.
          - `reject_rows_list`: `[source_file, row_index, reject_reason, raw_payload]` rows.
    """
    n_rows = batch.num_rows
    if n_rows == 0:
        return {}, []

    # 1. Missing required fields mask
    required_cols = (
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
    )
    missing_mask = pa.array([False] * n_rows, type=pa.bool_())
    for col_name in required_cols:
        if col_name not in batch.schema.names:
            missing_mask = pa.array([True] * n_rows, type=pa.bool_())
            break
        missing_mask = pc.or_(missing_mask, _arrow_missing_mask(batch.column(col_name)))

    pickup_col = pc.cast(batch.column("tpep_pickup_datetime"), pa.timestamp("us"))
    dropoff_col = pc.cast(batch.column("tpep_dropoff_datetime"), pa.timestamp("us"))
    dist_col = pc.cast(batch.column("trip_distance"), pa.float64())
    fare_col = pc.cast(batch.column("fare_amount"), pa.float64())
    total_col = pc.cast(batch.column("total_amount"), pa.float64())

    # 2. Out-of-range date mask
    min_ts = pa.scalar(MIN_VALID_PICKUP_DT, type=pa.timestamp("us"))
    max_ts = pa.scalar(MAX_VALID_PICKUP_DT, type=pa.timestamp("us"))
    out_of_range_mask = pc.fill_null(
        pc.or_(
            pc.or_(pc.less(pickup_col, min_ts), pc.greater(pickup_col, max_ts)),
            pc.or_(pc.less(dropoff_col, min_ts), pc.greater(dropoff_col, max_ts)),
        ),
        False,
    )

    # 3. Dropoff <= Pickup mask
    dropoff_before_mask = pc.fill_null(pc.less_equal(dropoff_col, pickup_col), False)

    # 4. Invalid trip distance mask (<= 0 or > 500)
    invalid_dist_mask = pc.fill_null(
        pc.or_(pc.less_equal(dist_col, 0.0), pc.greater(dist_col, MAX_VALID_DISTANCE_MILES)),
        False,
    )

    # 5. Non-positive fare mask (fare <= 0 or total <= 0)
    non_pos_fare_mask = pc.fill_null(
        pc.or_(pc.less_equal(fare_col, 0.0), pc.less_equal(total_col, 0.0)),
        False,
    )

    any_bad_mask = pc.or_(
        missing_mask,
        pc.or_(
            out_of_range_mask,
            pc.or_(dropoff_before_mask, pc.or_(invalid_dist_mask, non_pos_fare_mask)),
        ),
    )
    valid_mask = pc.invert(any_bad_mask)

    # Extract clean rows in C++ using batch.filter(valid_mask)
    clean_batch = batch.filter(valid_mask)
    valid_indices = [row_offset + i for i, ok in enumerate(valid_mask.to_pylist()) if ok]

    clean_dict: Dict[str, List[Any]] = {}
    if clean_batch.num_rows > 0:
        schema_names = set(clean_batch.schema.names)
        n_clean = clean_batch.num_rows

        def _col_or_default(name: str, default_val: Any) -> List[Any]:
            if name not in schema_names:
                return [default_val] * n_clean
            vals = clean_batch.column(name).to_pylist()
            return [default_val if _is_missing(v) else v for v in vals]

        clean_dict = {
            "trip_hash": [compute_trip_hash(source_file, idx) for idx in valid_indices],
            "source_file": [source_file] * n_clean,
            "vendor_id": [max(0, int(v)) for v in _col_or_default("VendorID", 0)],
            "pickup_datetime": [
                _wallclock_as_utc(v) for v in clean_batch.column("tpep_pickup_datetime").to_pylist()
            ],
            "dropoff_datetime": [
                _wallclock_as_utc(v) for v in clean_batch.column("tpep_dropoff_datetime").to_pylist()
            ],
            "passenger_count": [
                None if v is None else max(0, int(v))
                for v in _col_or_default("passenger_count", None)
            ],
            "trip_distance": [float(v) for v in clean_batch.column("trip_distance").to_pylist()],
            "pu_location_id": [int(v) for v in clean_batch.column("PULocationID").to_pylist()],
            "do_location_id": [int(v) for v in clean_batch.column("DOLocationID").to_pylist()],
            "payment_type": [max(0, int(v)) for v in _col_or_default("payment_type", 0)],
            "fare_amount": [float(v) for v in clean_batch.column("fare_amount").to_pylist()],
            "tip_amount": [float(v) for v in _col_or_default("tip_amount", 0.0)],
            "total_amount": [float(v) for v in clean_batch.column("total_amount").to_pylist()],
            "cbd_congestion_fee": [
                None if v is None else float(v)
                for v in _col_or_default("cbd_congestion_fee", None)
            ],
        }

    # Extract only the bad rows (~6.4%) to assign exact reject_reason + raw JSON
    reject_rows: List[List[Any]] = []
    bad_batch = batch.filter(any_bad_mask)
    if bad_batch.num_rows > 0:
        bad_indices = [row_offset + i for i, bad in enumerate(any_bad_mask.to_pylist()) if bad]
        m_list = missing_mask.filter(any_bad_mask).to_pylist()
        oor_list = out_of_range_mask.filter(any_bad_mask).to_pylist()
        dbp_list = dropoff_before_mask.filter(any_bad_mask).to_pylist()
        dist_list = invalid_dist_mask.filter(any_bad_mask).to_pylist()

        for i, raw_row in enumerate(bad_batch.to_pylist()):
            if m_list[i]:
                reason = "missing_required_field"
            elif oor_list[i]:
                reason = "out_of_range_date"
            elif dbp_list[i]:
                reason = "dropoff_before_pickup"
            elif dist_list[i]:
                reason = "invalid_trip_distance"
            else:
                reason = "non_positive_fare"

            reject_rows.append(
                [
                    source_file,
                    bad_indices[i],
                    reason,
                    serialize_raw_payload(raw_row),
                ]
            )

    return clean_dict, reject_rows