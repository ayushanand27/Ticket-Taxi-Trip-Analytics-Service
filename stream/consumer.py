"""
consumer.py - reads trips.raw, builds 5-min windows, writes live_zone_windows.

Delivery: at-least-once + idempotent writes (see design_stream.md section 7).
  * Rows are cumulative per (partition, offset bucket) with version = last offset.
  * Offsets are committed only AFTER ClickHouse accepts the insert.
  * On (re)start each partition is rewound to the START of its offset bucket so
    the bucket is rebuilt from scratch; ReplacingMergeTree(version) dedupes.
  * Bad JSON and too-late events go to trips.dlq, never silently dropped.

Usage:
    python -m stream.consumer
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime

import clickhouse_connect
from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

from stream.windowing import BUCKET_SIZE, WindowAggregator, bucket_start_offset

log = logging.getLogger("consumer")
TOPIC, DLQ = "trips.raw", "trips.dlq"
LIVE_COLUMNS = ["window_start", "pu_location_id", "kafka_partition", "bucket_id",
                "version", "trip_count", "fare_sum", "distance_sum"]


def ch_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CH_HOST", "127.0.0.1"), port=int(os.getenv("CH_PORT", "8123")),
        username=os.getenv("CH_USER", "taxi_user"), password=os.getenv("CH_PASSWORD", "taxi_password"),
        database="taxi_analytics")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--brokers", default="127.0.0.1:19092")
    ap.add_argument("--group", default="live-windows")
    ap.add_argument("--flush-secs", type=float, default=1.0)
    ap.add_argument("--flush-events", type=int, default=5000)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    ch = ch_client()
    agg = WindowAggregator()
    dlq = Producer({"bootstrap.servers": args.brokers})
    consumer = Consumer({
        "bootstrap.servers": args.brokers, "group.id": args.group,
        "enable.auto.commit": False, "auto.offset.reset": "earliest",
        # WHY: the broker only reassigns a dead consumer's partitions after the session
        # times out. The 45 s default made crash recovery take ~45 s in testing.
        "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000,
    })

    def on_assign(c, partitions):
        # Rewind to the start of the offset bucket so the bucket is rebuilt fully.
        committed = c.committed(partitions, timeout=10)
        for p in committed:
            if p.offset >= 0:
                p.offset = bucket_start_offset(p.offset, BUCKET_SIZE)
        log.info("assigned %s", [(p.partition, p.offset) for p in committed])
        c.assign(committed)

    consumer.subscribe([TOPIC], on_assign=on_assign)

    next_offsets: dict[int, int] = {}
    events_total = since_flush = 0
    last_flush = time.time()

    def flush() -> None:
        nonlocal since_flush, last_flush
        rows = agg.flush_rows()
        if rows:
            ch.insert("live_zone_windows", rows, column_names=LIVE_COLUMNS)  # raises on failure
        ch.insert("live_consumer_status",
                  [[agg.watermark, agg.max_event_time, agg.late_dropped, events_total]],
                  column_names=["watermark", "max_event_time", "late_dropped", "events_total"])
        # Commit only after ClickHouse accepted the data.
        if next_offsets:
            consumer.commit(offsets=[TopicPartition(TOPIC, p, o) for p, o in next_offsets.items()],
                            asynchronous=False)
        agg.drop_completed()
        since_flush, last_flush = 0, time.time()

    try:
        while True:
            msg = consumer.poll(0.2)
            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
            else:
                events_total += 1
                since_flush += 1
                try:
                    e = json.loads(msg.value())
                    pickup = datetime.fromisoformat(e["pickup"])
                    verdict = agg.add(msg.partition(), msg.offset(), pickup,
                                      int(e["zone"]), float(e["fare"]), float(e["distance"]))
                    if verdict == "too_late":
                        dlq.produce(DLQ, key=str(e["zone"]),
                                    value=json.dumps({"reason": "too_late", "event": e}))
                except (ValueError, KeyError, TypeError) as exc:
                    dlq.produce(DLQ, value=json.dumps({"reason": f"bad_message: {exc}",
                                                       "raw": msg.value().decode("utf-8", "replace")}))
                next_offsets[msg.partition()] = msg.offset() + 1
            if since_flush >= args.flush_events or (since_flush and time.time() - last_flush >= args.flush_secs):
                flush()
                dlq.poll(0)
    except KeyboardInterrupt:
        pass
    finally:
        flush()
        dlq.flush()
        consumer.close()


if __name__ == "__main__":
    main()
