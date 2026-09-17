"""Durations, instants and retry delays."""
from __future__ import annotations

import random

import pytest

from grampy.timing import Retry, seconds, shift


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
