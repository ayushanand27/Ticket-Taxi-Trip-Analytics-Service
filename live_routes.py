"""live_routes.py - /live/* endpoints over live_zone_windows (Ticket 2)."""
from fastapi import APIRouter, Query

router = APIRouter(prefix="/live", tags=["live"])

# Collapse cumulative bucket rows to the newest version first, so replays never double count.
_LATEST = """
    SELECT window_start, pu_location_id, kafka_partition, bucket_id,
           argMax(trip_count, version)  AS trips,
           argMax(fare_sum, version)    AS fare,
           argMax(distance_sum, version) AS dist
    FROM taxi_analytics.live_zone_windows
    WHERE window_start >= %(since)s
    GROUP BY window_start, pu_location_id, kafka_partition, bucket_id
"""


def _client():
    from api import get_client  # imported lazily to avoid a circular import
    return get_client()


def _as_of(client):
    """Newest event time the consumer has seen. Replayed data is historical, so
    'last N minutes' is measured from this, not from wall-clock now()."""
    row = client.query(
        "SELECT max(max_event_time) FROM taxi_analytics.live_consumer_status").result_rows
    return row[0][0] if row and row[0][0] else None


@router.get("/zones/top")
def top_zones(minutes: int = Query(15, ge=5, le=240), limit: int = Query(10, ge=1, le=50)):
    c = _client()
    as_of = _as_of(c)
    if as_of is None:
        return {"as_of": None, "zones": []}
    sql = f"""
        SELECT l.pu_location_id, any(z.zone), any(z.borough), sum(l.trips) AS trips, sum(l.fare)
        FROM ({_LATEST}) AS l
        LEFT JOIN taxi_analytics.taxi_zones AS z ON z.location_id = l.pu_location_id
        GROUP BY l.pu_location_id ORDER BY trips DESC LIMIT %(limit)s"""
    since = as_of.replace(microsecond=0) - __import__("datetime").timedelta(minutes=minutes)
    rows = c.query(sql, parameters={"since": since, "limit": limit}).result_rows
    return {"as_of": as_of.isoformat(), "minutes": minutes,
            "zones": [{"zone_id": r[0], "zone": r[1] or "Unknown", "borough": r[2] or "Unknown",
                       "trips": r[3], "fare_sum": round(r[4], 2)} for r in rows]}


@router.get("/zone/{zone_id}")
def zone_series(zone_id: int, minutes: int = Query(60, ge=5, le=1440)):
    c = _client()
    as_of = _as_of(c)
    if as_of is None:
        return {"as_of": None, "zone_id": zone_id, "windows": []}
    since = as_of.replace(microsecond=0) - __import__("datetime").timedelta(minutes=minutes)
    sql = f"""SELECT window_start, sum(trips), sum(fare) FROM ({_LATEST})
              WHERE pu_location_id = %(z)s GROUP BY window_start ORDER BY window_start"""
    rows = c.query(sql, parameters={"since": since, "z": zone_id}).result_rows
    return {"as_of": as_of.isoformat(), "zone_id": zone_id,
            "windows": [{"window_start": r[0].isoformat(), "trips": r[1], "fare_sum": round(r[2], 2)} for r in rows]}


@router.get("/health")
def live_health():
    c = _client()
    r = c.query("""SELECT reported_at, watermark, max_event_time, late_dropped, events_total
                   FROM taxi_analytics.live_consumer_status ORDER BY reported_at DESC LIMIT 1""").result_rows
    if not r:
        return {"status": "no_data"}
    x = r[0]
    return {"status": "ok", "last_report": x[0].isoformat(),
            "watermark": x[1].isoformat() if x[1] else None,
            "max_event_time": x[2].isoformat() if x[2] else None,
            "late_dropped": x[3], "events_total": x[4]}
