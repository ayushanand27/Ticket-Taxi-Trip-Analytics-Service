"""
windowing.py - pure (no Kafka, no ClickHouse) window + watermark logic.

Kept dependency-free so it can be unit tested in milliseconds.

Model:
  * 5-minute tumbling windows keyed by EVENT time (pickup time).
  * Watermark = max event time seen - allowed lateness, tracked PER PARTITION.
    A too-late decision uses the event's own partition. One global watermark
    wrongly dropped events of slow partitions whenever the consumer read
    another partition far ahead (backlog). The reported watermark is the
    minimum across partitions, so an idle partition holds it back.
  * Events are grouped into fixed offset buckets per Kafka partition
    (bucket_id = offset // BUCKET_SIZE). Each bucket's rows are CUMULATIVE
    and carry a version (last offset seen). Replaying a bucket after a crash
    rebuilds identical or larger totals, and ReplacingMergeTree(version)
    keeps the latest, so replays never double count.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

WINDOW_SECONDS = 300
ALLOWED_LATENESS = timedelta(minutes=10)
BUCKET_SIZE = 5000


def window_start(ts: datetime, size_seconds: int = WINDOW_SECONDS) -> datetime:
    """Floor a timestamp to the start of its tumbling window."""
    total_seconds = ts.hour * 3600 + ts.minute * 60 + ts.second
    floored = (total_seconds // size_seconds) * size_seconds
    return ts.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=floored)


def bucket_of(offset: int, size: int = BUCKET_SIZE) -> int:
    return offset // size


def bucket_start_offset(offset: int, size: int = BUCKET_SIZE) -> int:
    return (offset // size) * size


@dataclass
class Totals:
    trips: int = 0
    fare: float = 0.0
    distance: float = 0.0


@dataclass
class Bucket:
    partition: int
    bucket_id: int
    last_offset: int = -1
    dirty: bool = False
    cells: Dict[Tuple[datetime, int], Totals] = field(default_factory=dict)


class WindowAggregator:
    """Assigns events to windows, tracks the watermark, builds flush rows."""

    def __init__(self, lateness: timedelta = ALLOWED_LATENESS,
                 window_seconds: int = WINDOW_SECONDS, bucket_size: int = BUCKET_SIZE):
        self.lateness = lateness
        self.window_seconds = window_seconds
        self.bucket_size = bucket_size
        self.partition_max: Dict[int, datetime] = {}
        self.buckets: Dict[Tuple[int, int], Bucket] = {}
        self.late_dropped = 0

    @property
    def max_event_time(self) -> datetime | None:
        return max(self.partition_max.values()) if self.partition_max else None

    @property
    def watermark(self) -> datetime | None:
        """Reported watermark = slowest partition's watermark."""
        if not self.partition_max:
            return None
        return min(self.partition_max.values()) - self.lateness

    def add(self, partition: int, offset: int, pickup: datetime, zone: int,
            fare: float, distance: float) -> str:
        """Return 'ok' or 'too_late'. Offsets still advance the bucket either way."""
        bucket = self._bucket(partition, offset)
        bucket.last_offset = max(bucket.last_offset, offset)

        pmax = self.partition_max.get(partition)
        wm = pmax - self.lateness if pmax is not None else None
        w_start = window_start(pickup, self.window_seconds)
        w_end = w_start + timedelta(seconds=self.window_seconds)
        if wm is not None and w_end <= wm:
            self.late_dropped += 1
            return "too_late"

        if pmax is None or pickup > pmax:
            self.partition_max[partition] = pickup

        cell = bucket.cells.setdefault((w_start, zone), Totals())
        cell.trips += 1
        cell.fare += fare
        cell.distance += distance
        bucket.dirty = True
        return "ok"

    def _bucket(self, partition: int, offset: int) -> Bucket:
        key = (partition, bucket_of(offset, self.bucket_size))
        b = self.buckets.get(key)
        if b is None:
            b = self.buckets[key] = Bucket(partition, key[1])
        return b

    def flush_rows(self) -> List[list]:
        """Rows for every dirty bucket: cumulative totals + version = last_offset."""
        rows: List[list] = []
        for b in self.buckets.values():
            if not b.dirty:
                continue
            for (w_start, zone), t in b.cells.items():
                rows.append([w_start, zone, b.partition, b.bucket_id,
                             b.last_offset, t.trips, t.fare, t.distance])
            b.dirty = False
        return rows

    def drop_completed(self) -> None:
        """Forget buckets whose final offset was seen and already flushed."""
        done = [k for k, b in self.buckets.items()
                if not b.dirty and (b.last_offset + 1) % self.bucket_size == 0]
        for k in done:
            del self.buckets[k]
