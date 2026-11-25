from datetime import datetime, timedelta

from stream.windowing import WindowAggregator, window_start


def t(h, m, s=0):
    return datetime(2025, 1, 1, h, m, s)


def test_window_start_floors_to_5_min():
    assert window_start(t(10, 7, 59)) == t(10, 5)
    assert window_start(t(10, 5, 0)) == t(10, 5)
    assert window_start(t(10, 4, 59)) == t(10, 0)


def test_events_in_same_window_are_summed():
    a = WindowAggregator()
    a.add(0, 0, t(10, 1), 132, 10.0, 2.0)
    a.add(0, 1, t(10, 3), 132, 20.0, 3.0)
    rows = a.flush_rows()
    assert len(rows) == 1
    _, zone, part, bucket, version, trips, fare, dist = rows[0]
    assert (zone, part, bucket, version, trips, fare, dist) == (132, 0, 0, 1, 2, 30.0, 5.0)


def test_late_but_inside_allowed_lateness_is_counted():
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(0, 0, t(10, 30), 1, 5.0, 1.0)          # watermark -> 10:20
    assert a.add(0, 1, t(10, 22), 1, 5.0, 1.0) == "ok"   # window ends 10:25 > 10:20
    assert a.late_dropped == 0


def test_too_late_is_dropped_and_counted():
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(0, 0, t(10, 30), 1, 5.0, 1.0)          # watermark 10:20
    assert a.add(0, 1, t(10, 12), 1, 5.0, 1.0) == "too_late"  # window ends 10:15 <= 10:20
    assert a.late_dropped == 1


def test_boundary_window_end_equal_to_watermark_is_too_late():
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(0, 0, t(10, 30), 1, 1.0, 1.0)          # watermark 10:20
    assert a.add(0, 1, t(10, 19), 1, 1.0, 1.0) == "too_late"   # window 10:15-10:20, end == wm


def test_watermark_never_moves_backwards():
    a = WindowAggregator()
    a.add(0, 0, t(10, 30), 1, 1.0, 1.0)
    wm = a.watermark
    a.add(0, 1, t(10, 25), 1, 1.0, 1.0)
    assert a.watermark == wm


def test_replay_of_a_bucket_gives_identical_cumulative_rows():
    """Crash simulation: process the same bucket twice from scratch -> same rows."""
    events = [(0, i, t(10, i % 10), 7, 2.0, 1.0) for i in range(100)]
    first, second = WindowAggregator(), WindowAggregator()
    for e in events:
        first.add(*e)
    for e in events:
        second.add(*e)
    assert first.flush_rows() == second.flush_rows()


def test_partial_flush_then_more_events_gives_higher_version_and_bigger_total():
    a = WindowAggregator()
    a.add(0, 0, t(10, 1), 5, 1.0, 1.0)
    r1 = a.flush_rows()[0]
    a.add(0, 1, t(10, 2), 5, 1.0, 1.0)
    r2 = a.flush_rows()[0]
    assert r2[4] > r1[4] and r2[5] == 2 and r1[5] == 1


def test_clean_bucket_not_reflushed():
    a = WindowAggregator()
    a.add(0, 0, t(10, 1), 5, 1.0, 1.0)
    assert len(a.flush_rows()) == 1
    assert a.flush_rows() == []


def test_completed_bucket_is_dropped_from_memory():
    a = WindowAggregator(bucket_size=3)
    for i in range(3):
        a.add(0, i, t(10, 1), 5, 1.0, 1.0)
    a.flush_rows()
    a.drop_completed()
    assert a.buckets == {}


def test_partition_read_far_ahead_does_not_make_other_partitions_late():
    """Regression: with one global watermark, a consumer that reads partition A far
    ahead (backlog) marked every event of the slower partition B as too late."""
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(3, 0, t(13, 50), 1, 1.0, 1.0)                  # partition 3 is read up to 13:50
    assert a.add(0, 0, t(0, 1), 1, 1.0, 1.0) == "ok"     # partition 0 is still at 00:01
    assert a.add(0, 1, t(0, 2), 1, 1.0, 1.0) == "ok"
    assert a.late_dropped == 0


def test_lateness_is_still_enforced_inside_one_partition():
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(3, 0, t(13, 50), 1, 1.0, 1.0)
    a.add(0, 0, t(10, 30), 1, 1.0, 1.0)                  # partition 0 watermark 10:20
    assert a.add(0, 1, t(10, 12), 1, 1.0, 1.0) == "too_late"
    assert a.late_dropped == 1


def test_global_watermark_is_the_slowest_partition():
    a = WindowAggregator(lateness=timedelta(minutes=10))
    a.add(3, 0, t(13, 50), 1, 1.0, 1.0)
    a.add(0, 0, t(10, 30), 1, 1.0, 1.0)
    assert a.watermark == t(10, 20)
    assert a.max_event_time == t(13, 50)
