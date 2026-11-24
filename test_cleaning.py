"""
test_cleaning.py — Unit Tests for NYC Yellow Taxi Cleaning & Validation Logic.

WHY THIS FILE EXISTS:
    Verifies every cleaning rule, edge case (NaN, null optional fields, stray dates,
    negative fares, zero distance, dropoff <= pickup), and helper function in
    `cleaning.py` without requiring a running database.

USAGE (Windows PowerShell):
    python -m pytest test_cleaning.py -v
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

import pytest

from cleaning import compute_trip_hash, serialize_raw_payload, validate_trip_row


def make_valid_row(**overrides: Any) -> Dict[str, Any]:
    """Build a minimal valid 2026 Yellow Taxi row, with optional field overrides.

    WHY:
        Using a factory helper keeps each test focused on the single dirty field
        it is testing instead of repeating 15-field dictionaries in every test.
    """
    base: Dict[str, Any] = {
        "VendorID": 2,
        "tpep_pickup_datetime": datetime(2026, 3, 15, 14, 10, 0),
        "tpep_dropoff_datetime": datetime(2026, 3, 15, 14, 28, 0),
        "passenger_count": 1,
        "trip_distance": 3.4,
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 18.5,
        "tip_amount": 3.7,
        "total_amount": 25.45,
        "cbd_congestion_fee": 0.75,
    }
    base.update(overrides)
    return base


def test_valid_row_passes_and_normalizes() -> None:
    """A clean row should pass validation and populate all target schema fields."""
    row = make_valid_row()
    result = validate_trip_row(row, source_file="yellow_tripdata_2026-03.parquet", row_index=42)

    assert result.is_valid is True
    assert result.reject_reason is None
    assert result.cleaned_row is not None
    assert result.cleaned_row["source_file"] == "yellow_tripdata_2026-03.parquet"
    assert result.cleaned_row["pu_location_id"] == 161
    assert result.cleaned_row["fare_amount"] == pytest.approx(18.5)
    assert result.cleaned_row["cbd_congestion_fee"] == pytest.approx(0.75)


def test_nullable_optional_fields_are_accepted() -> None:
    """Missing optional fields (passenger_count, cbd_congestion_fee) must not fail valid trips."""
    row = make_valid_row(passenger_count=None, cbd_congestion_fee=float("nan"), tip_amount=None)
    result = validate_trip_row(row, source_file="yellow_tripdata_2026-03.parquet", row_index=1)

    assert result.is_valid is True
    assert result.cleaned_row is not None
    assert result.cleaned_row["passenger_count"] is None
    assert result.cleaned_row["cbd_congestion_fee"] is None
    assert result.cleaned_row["tip_amount"] == 0.0


@pytest.mark.parametrize(
    "field",
    [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ],
)
def test_missing_required_field_rejected(field: str) -> None:
    """Rows missing any core timestamp, zone, distance, or fare field must be rejected."""
    row = make_valid_row(**{field: None})
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "missing_required_field"


def test_nan_distance_rejected_as_missing() -> None:
    """A float('nan') distance must be caught as missing rather than bypassing `<= 0`."""
    row = make_valid_row(trip_distance=float("nan"))
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "missing_required_field"


@pytest.mark.parametrize(
    ("pickup", "dropoff"),
    [
        # Dropoff 5 minutes BEFORE pickup
        (datetime(2026, 3, 15, 14, 30, 0), datetime(2026, 3, 15, 14, 25, 0)),
        # Dropoff EQUAL to pickup (0-second trip)
        (datetime(2026, 3, 15, 14, 30, 0), datetime(2026, 3, 15, 14, 30, 0)),
    ],
)
def test_dropoff_before_or_equal_pickup_rejected(pickup: datetime, dropoff: datetime) -> None:
    """Trips where dropoff <= pickup must be rejected with `dropoff_before_pickup`."""
    row = make_valid_row(tpep_pickup_datetime=pickup, tpep_dropoff_datetime=dropoff)
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "dropoff_before_pickup"


@pytest.mark.parametrize("bad_distance", [0.0, -1.2, 500.1])
def test_invalid_trip_distance_rejected(bad_distance: float) -> None:
    """Zero, negative, or extreme (>500 mi) distances must be rejected."""
    row = make_valid_row(trip_distance=bad_distance)
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "invalid_trip_distance"


@pytest.mark.parametrize(
    ("fare", "total"),
    [
        (-10.0, 15.0),  # Negative fare
        (0.0, 15.0),    # Zero fare
        (15.0, -5.0),   # Negative total_amount
        (15.0, 0.0),    # Zero total_amount
    ],
)
def test_non_positive_fare_rejected(fare: float, total: float) -> None:
    """Negative or zero fare/total amounts must be rejected with `non_positive_fare`."""
    row = make_valid_row(fare_amount=fare, total_amount=total)
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "non_positive_fare"


@pytest.mark.parametrize(
    "stray_pickup",
    [
        datetime(2008, 12, 31, 23, 59, 0),  # Stray old date seen in real TLC files
        datetime(2099, 1, 1, 0, 0, 0),      # Stray future date
    ],
)
def test_out_of_range_date_rejected(stray_pickup: datetime) -> None:
    """Stray timestamps outside 2025-2026 must be quarantined as `out_of_range_date`."""
    row = make_valid_row(
        tpep_pickup_datetime=stray_pickup,
        tpep_dropoff_datetime=datetime(2026, 3, 15, 14, 28, 0),
    )
    result = validate_trip_row(row, source_file="test.parquet", row_index=0)

    assert result.is_valid is False
    assert result.reject_reason == "out_of_range_date"


def test_compute_trip_hash_is_deterministic() -> None:
    """Same (source_file, row_index) must always yield the exact same UInt64 hash."""
    h1 = compute_trip_hash("yellow_tripdata_2026-01.parquet", 12345)
    h2 = compute_trip_hash("yellow_tripdata_2026-01.parquet", 12345)
    h3 = compute_trip_hash("yellow_tripdata_2026-01.parquet", 12346)

    assert h1 == h2
    assert h1 != h3
    assert 0 <= h1 < 2**64


def test_serialize_raw_payload_handles_datetime_and_nan() -> None:
    """Raw payload serializer must convert datetimes and NaNs without raising TypeError."""
    raw = make_valid_row(trip_distance=float("nan"))
    encoded = serialize_raw_payload(raw)
    decoded = json.loads(encoded)

    assert decoded["tpep_pickup_datetime"] == "2026-03-15T14:10:00"
    assert decoded["trip_distance"] is None