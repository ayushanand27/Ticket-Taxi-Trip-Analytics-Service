"""
cleaning.py — Pure, Unit-Testable Validation & Cleaning Logic for NYC Yellow Taxi Trips.

WHY THIS MODULE EXISTS:
    Separates data validation from database I/O so that every cleaning rule can be
    tested in isolation with `pytest` in milliseconds—without needing Docker or
    ClickHouse running.

    Every row from a Yellow Taxi Parquet file passes through `validate_trip_row()`.
    Valid rows are normalized into a typed dictionary matching `taxi_trips_clean`.
    Dirty rows (negative fares, zero distance, dropoff before pickup, etc.) are
    never silently dropped; they return an explicit `reject_reason` so the
    ingestion pipeline can quarantine them in `taxi_trips_rejects`.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

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
    """Result of validating a single raw Parquet trip row.

    WHY:
        Returning a structured object instead of raising exceptions makes batch
        processing fast (~40M rows) and keeps control flow explicit: callers check
        `result.is_valid` and route to either `clean_batch` or `reject_batch`.
    """

    is_valid: bool
    cleaned_row: Optional[Dict[str, Any]] = None
    reject_reason: Optional[str] = None


def _is_missing(value: Any) -> bool:
    """Return True if `value` is None or a floating-point NaN.

    WHY:
        PyArrow/Pandas can represent missing numeric values as either Python `None`
        or `float('nan')`. Checking both prevents `NaN <= 0` from silently evaluating
        to `False` and slipping into the clean table.
    """
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _to_naive_datetime(value: Any) -> Optional[datetime]:
    """Normalize a timestamp value to a timezone-naive Python `datetime`.

    WHY:
        NYC TLC timestamps represent local NYC wall-clock time without timezone
        offsets, and ClickHouse's `DateTime` column expects naive timestamps.
        Handling ISO strings as well as `datetime` objects makes unit testing easy.
    """
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, str):
        return datetime.fromisoformat(value).replace(tzinfo=None)
    return None


def compute_trip_hash(source_file: str, row_index: int) -> int:
    """Compute a deterministic 64-bit unsigned integer ID for a trip row.

    WHY:
        Python's built-in `hash()` is randomly salted per process (`PYTHONHASHSEED`),
        so it changes every time you re-run the script. Using `blake2b` with an
        8-byte digest guarantees the exact same `UInt64` `trip_hash` on every run.

    Args:
        source_file: Basename of the Parquet file (e.g. `yellow_tripdata_2026-01.parquet`).
        row_index: 0-based row offset within that Parquet file.

    Returns:
        Deterministic unsigned 64-bit integer suitable for ClickHouse `UInt64`.
    """
    payload = f"{source_file}:{row_index}".encode("utf-8")
    digest = hashlib.blake2b(payload, digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


def serialize_raw_payload(raw_row: Dict[str, Any]) -> str:
    """Safely serialize a raw Parquet row dictionary to a JSON string.

    WHY:
        Rejected rows are stored in `taxi_trips_rejects.raw_payload` for auditing.
        Parquet rows contain `datetime` objects and occasional `NaN` floats that
        crash standard `json.dumps()`, so we convert them cleanly here.
    """
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
    """Validate and normalize a single NYC Yellow Taxi trip record.

    WHY:
        Checks are ordered from cheapest/most fundamental (missing fields) to
        domain rules (date bounds, time ordering, distance, fare) so the first
        clear failure produces a deterministic `reject_reason`.

    Args:
        raw_row: Dictionary of column -> value from a Parquet row.
        source_file: Filename being ingested (stored for traceability).
        row_index: 0-based row index in `source_file`.

    Returns:
        ValidationResult with `is_valid=True` and `cleaned_row`, OR
        `is_valid=False` and `reject_reason`.
    """
    # 1. Check required fields (must be present and non-NaN)
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

    # 2. Parse & validate timestamps
    pickup_dt = _to_naive_datetime(raw_row["tpep_pickup_datetime"])
    dropoff_dt = _to_naive_datetime(raw_row["tpep_dropoff_datetime"])
    if pickup_dt is None or dropoff_dt is None:
        return ValidationResult(is_valid=False, reject_reason="missing_required_field")

    # WHY: Reject stray timestamps outside 2025-2026 so ClickHouse doesn't create
    # bogus monthly partitions for years like 2002 or 2099.
    if not (MIN_VALID_PICKUP_DT <= pickup_dt <= MAX_VALID_PICKUP_DT):
        return ValidationResult(is_valid=False, reject_reason="out_of_range_date")
    if not (MIN_VALID_PICKUP_DT <= dropoff_dt <= MAX_VALID_PICKUP_DT):
        return ValidationResult(is_valid=False, reject_reason="out_of_range_date")

    # 3. Check time ordering (dropoff must be strictly after pickup)
    if dropoff_dt <= pickup_dt:
        return ValidationResult(is_valid=False, reject_reason="dropoff_before_pickup")

    # 4. Check trip distance (must be > 0 and <= 500 miles)
    trip_distance = float(raw_row["trip_distance"])
    if trip_distance <= 0.0 or trip_distance > MAX_VALID_DISTANCE_MILES:
        return ValidationResult(is_valid=False, reject_reason="invalid_trip_distance")

    # 5. Check fare amounts (must be strictly positive; catches negative/refund/void trips)
    fare_amount = float(raw_row["fare_amount"])
    total_amount = float(raw_row["total_amount"])
    if fare_amount <= 0.0 or total_amount <= 0.0:
        return ValidationResult(is_valid=False, reject_reason="non_positive_fare")

    # 6. Normalize optional & integer fields to match `taxi_trips_clean` schema
    # WHY: TLC Parquet files sometimes have null `passenger_count` or `VendorID`
    # or `cbd_congestion_fee` (added in 2025). Explicit casting prevents ClickHouse
    # type-mismatch errors during high-speed batch inserts.
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
        "pickup_datetime": pickup_dt,
        "dropoff_datetime": dropoff_dt,
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