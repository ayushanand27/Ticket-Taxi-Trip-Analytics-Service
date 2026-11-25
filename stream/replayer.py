"""
replayer.py - replays historical taxi trips into Redpanda as a live stream.

Events are sent in pickup-time order. `--speed 60` means 1 real second = 60
stream minutes... no: 1 real second = 60 seconds of trip time. `--speed 0`
sends as fast as possible (use this for throughput benchmarks).
`--late-pct 5 --max-late-min 15` makes 5% of events arrive up to 15 minutes late.

Usage:
    python -m stream.replayer --file nyc_yellow_taxi/yellow_tripdata_2025-01.parquet --speed 600
"""
from __future__ import annotations

import argparse
import json
import logging
import random
import time
from pathlib import Path

import pyarrow.parquet as pq
from confluent_kafka import Producer

from cleaning import validate_arrow_batch

log = logging.getLogger("replayer")
TOPIC = "trips.raw"


def load_sorted_events(path: Path):
    """Read the file, keep only clean rows, return events sorted by pickup time."""
    pf = pq.ParquetFile(path)
    events = []
    offset = 0
    for batch in pf.iter_batches(batch_size=100_000):
        clean, _rejects = validate_arrow_batch(batch, path.name, offset)
        offset += batch.num_rows
        for i in range(len(clean.get("trip_hash", []))):
            events.append({
                "event_id": clean["trip_hash"][i],
                "pickup": clean["pickup_datetime"][i],
                "zone": clean["pu_location_id"][i],
                "fare": clean["fare_amount"][i],
                "distance": clean["trip_distance"][i],
            })
    events.sort(key=lambda e: e["pickup"])
    return events


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", required=True, type=Path)
    ap.add_argument("--speed", type=float, default=60.0, help="stream seconds per real second; 0 = max speed")
    ap.add_argument("--brokers", default="127.0.0.1:19092")
    ap.add_argument("--limit", type=int, default=0, help="stop after N events (0 = all)")
    ap.add_argument("--late-pct", type=float, default=0.0)
    ap.add_argument("--max-late-min", type=float, default=15.0)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    events = load_sorted_events(args.file)
    if args.limit:
        events = events[: args.limit]
    log.info("loaded %d clean events", len(events))

    # Lateness injection: a late event is SENT later than its event time says.
    # We emulate it by delaying its position in the send order.
    send_order = list(range(len(events)))
    if args.late_pct > 0:
        from datetime import timedelta
        keyed = []
        for i, e in enumerate(events):
            delay = timedelta(minutes=random.uniform(0, args.max_late_min)) if random.random() * 100 < args.late_pct else timedelta(0)
            keyed.append((e["pickup"] + delay, i))
        keyed.sort()
        send_order = [i for _, i in keyed]

    producer = Producer({"bootstrap.servers": args.brokers, "linger.ms": 20, "compression.type": "lz4"})
    start_wall = time.time()
    start_stream = events[0]["pickup"]
    sent = 0
    for i in send_order:
        e = events[i]
        if args.speed > 0:
            target = (e["pickup"] - start_stream).total_seconds() / args.speed
            sleep_for = target - (time.time() - start_wall)
            if sleep_for > 0:
                time.sleep(sleep_for)
        payload = dict(e, pickup=e["pickup"].isoformat())
        while True:
            try:
                producer.produce(TOPIC, key=str(e["zone"]), value=json.dumps(payload))
                break
            except BufferError:
                producer.poll(0.1)
        sent += 1
        if sent % 10_000 == 0:
            producer.poll(0)
            log.info("sent %d events (%.0f/s)", sent, sent / (time.time() - start_wall))
    producer.flush()
    log.info("done: %d events in %.1fs", sent, time.time() - start_wall)


if __name__ == "__main__":
    main()
