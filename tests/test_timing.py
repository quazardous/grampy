"""Durations, instants and retry delays."""
from __future__ import annotations

import random

import pytest

from quazardous.grampy.timing import Rate, Retry, admit, seconds, shift


@pytest.mark.parametrize("given, expected", [
    (0, 0.0), (90, 90.0), (1.5, 1.5), ("45", 45.0), ("30s", 30.0),
    ("10m", 600.0), ("2h", 7200.0), ("7d", 604800.0), (" 1.5 m ", 90.0),
])
def test_durations_read_as_seconds(given, expected):
    assert seconds(given) == expected


@pytest.mark.parametrize("given", ["soon", "10w", "-5s", -1, True, ""])
def test_what_is_not_a_duration_is_refused(given):
    with pytest.raises(ValueError):
        seconds(given)


def test_shift_keeps_the_journal_format():
    assert shift("2026-01-01T23:59:50+00:00", 15) == "2026-01-02T00:00:05+00:00"
    assert shift("2026-01-01T01:00:00+01:00", 0) == "2026-01-01T00:00:00+00:00"


@pytest.mark.parametrize("backoff, waits", [
    ("constant", [10, 10, 10, 10]),
    ("linear", [10, 20, 30, 40]),
    ("exponential", [10, 20, 40, 60]),
])
def test_backoff_shapes_capped_by_max_delay(backoff, waits):
    retry = Retry(limit=4, delay="10s", backoff=backoff, max_delay="1m")
    assert [retry.wait(attempt) for attempt in (1, 2, 3, 4)] == waits


def test_jitter_stays_within_its_fraction():
    retry = Retry(limit=1, delay=100, jitter=0.1)
    rng = random.Random(7)
    waits = [retry.wait(1, rng) for _ in range(200)]
    assert all(90 <= w <= 110 for w in waits)
    assert len(set(waits)) > 1


@pytest.mark.parametrize("kwargs", [
    {"limit": 0}, {"limit": 1, "backoff": "fast"}, {"limit": 1, "jitter": 1.0},
    {"limit": 1, "delay": "soon"}, {"limit": 1, "max_delay": -3},
])
def test_a_retry_that_cannot_hold_is_refused(kwargs):
    with pytest.raises(ValueError):
        Retry(**kwargs)


# ── GCRA ──────────────────────────────────────────────────────────────


def test_a_fresh_band_lets_its_burst_through_then_spaces_cells():
    band = Rate(limit=6, period="1m")               # T = 10 s, burst 6
    k, (tat,) = admit((band,), [None], now=0.0, want=10)
    assert k == 6 and tat == 60.0
    assert admit((band,), [tat], now=0.0, want=1)[0] == 0
    assert admit((band,), [tat], now=10.0, want=5)[0] == 1
    assert admit((band,), [tat], now=35.0, want=5)[0] == 3


def test_burst_one_spreads_evenly():
    band = Rate(limit=60, period="1m", burst=1)     # T = 1 s
    k, (tat,) = admit((band,), [None], now=100.0, want=5)
    assert (k, tat) == (1, 101.0)
    assert admit((band,), [tat], now=100.5, want=5)[0] == 0
    assert admit((band,), [tat], now=101.0, want=5)[0] == 1


def test_every_band_applies_and_advances_by_what_passed():
    minute, hour = Rate(3, "1m"), Rate(5, "1h")
    k, tats = admit((minute, hour), [None, None], now=0.0, want=10)
    assert k == 3 and tats == [60.0, 3 * 720.0]
    k, tats = admit((minute, hour), tats, now=60.0, want=10)
    assert k == 2, "the hour band has 2 left though the minute band has 3"
    assert tats == [60.0 + 2 * 20.0, 5 * 720.0]


def test_nothing_admitted_consumes_nothing():
    band = Rate(2, "1m")
    k, (tat,) = admit((band,), [120.0], now=0.0, want=4)
    assert k == 0 and tat == 120.0


@pytest.mark.parametrize("kwargs", [{"limit": 0, "period": "1m"},
                                    {"limit": 1, "period": "0s"},
                                    {"limit": 1, "period": "1m", "burst": 0}])
def test_a_rate_that_cannot_hold_is_refused(kwargs):
    with pytest.raises(ValueError):
        Rate(**kwargs)


def test_stamp_reads_any_moment_into_utc_to_the_second():
    from datetime import datetime, timedelta, timezone

    from quazardous.grampy.timing import stamp
    utc = "2026-01-01T09:00:00+00:00"
    assert stamp("2026-01-01T11:00:00+02:00") == utc
    assert stamp("2026-01-01T09:00:00Z") == utc, "Z, even on 3.10"
    assert stamp("2026-01-01 09:00:00+00:00") == utc, "a space for the T"
    assert stamp("2026-01-01T09:00:00.999999+00:00") == utc, "fractions dropped"
    assert stamp(datetime(2026, 1, 1, 10, tzinfo=timezone(timedelta(hours=1)))) == utc


def test_stamp_refuses_a_moment_without_a_zone_or_not_a_moment():
    from datetime import datetime

    import pytest

    from quazardous.grampy.timing import stamp
    with pytest.raises(ValueError, match="no time zone"):
        stamp("2026-01-01T09:00:00")
    with pytest.raises(ValueError, match="no time zone"):
        stamp(datetime(2026, 1, 1, 9))
    with pytest.raises(ValueError, match="not a moment"):
        stamp("yesterday")
