"""TIME, AS THE JOURNAL WRITES IT — durations, instants, and retry delays.

    seconds      "90", "30s", "10m", "2h", "7d", a number or a timedelta
    canonical    a timedelta as the text a graph stores ("5m")
    shift        an ISO instant moved by a number of seconds, same format
    Retry        a declared retry policy: how many times, how long to wait
    Rate         a rate limit band: so many per period, with a burst
    admit        how many of a batch a set of bands lets through now (GCRA)

────────────────────────────────────────────────────────────────────────
INSTANTS ARE TEXT, AND THEY COMPARE AS TEXT
────────────────────────────────────────────────────────────────────────

Rows carry ISO-8601 instants in UTC, to the second (`utc_now`). One fixed
format and one offset make lexical order the time order, in every storage
— which is what lets a driver compare `started_at < older_than` without
knowing what a date is. `shift` keeps that format.
"""
from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from .names import Backoff

_DURATION = re.compile(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*")
_UNIT = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}

#: THE BACKOFF SHAPES A RETRY MAY TAKE (the names Serverless Workflow uses).
BACKOFFS = tuple(Backoff)


def canonical(duration: Any) -> Any:
    """A `timedelta` as the text form durations are STORED in; anything else
    unchanged.

    `timedelta` is how Python says a duration, and it is accepted wherever a
    duration is. It is not how a duration is WRITTEN DOWN: a graph goes to
    JSON, which has no timedelta, and `"30s"` reads better than `30.0` in a
    stored document. So it is normalised at the door, once.
    """
    if not isinstance(duration, timedelta):
        return duration
    total = duration.total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if total and total % size == 0:
            return f"{int(total // size)}{unit}"
    return f"{int(total)}s" if total == int(total) else f"{total}s"


def seconds(duration: float | int | str | timedelta) -> float:
    """A duration in seconds: a number, a `timedelta`, or text with a unit
    (s, m, h, d)."""
    if isinstance(duration, bool):
        raise ValueError(f"not a duration: {duration!r}")
    if isinstance(duration, timedelta):
        duration = duration.total_seconds()
    if isinstance(duration, (int, float)):
        value = float(duration)
    else:
        match = _DURATION.fullmatch(str(duration))
        if not match:
            raise ValueError(f"not a duration: {duration!r} — expected e.g. 30s, 10m, 2h, 7d")
        value = float(match.group(1)) * _UNIT[match.group(2)]
    if value < 0:
        raise ValueError(f"a duration is never negative: {duration!r}")
    return value


def shift(moment: str, by: float) -> str:
    """`moment` moved by `by` seconds, in the journal's format (UTC, seconds)."""
    instant = datetime.fromisoformat(moment).astimezone(timezone.utc)
    return (instant + timedelta(seconds=by)).isoformat(timespec="seconds")


@dataclass(frozen=True)
class Retry:
    """A DECLARED RETRY. When the node fails, the failure is archived and the
    node is scheduled again, at most `limit` times per subject, after
    `delay` grown by `backoff`:

        constant     delay, delay, delay, …
        linear       delay, 2·delay, 3·delay, …
        exponential  delay, 2·delay, 4·delay, …

    capped by `max_delay`, then spread by `jitter` — a fraction: 0.1 moves
    each wait by up to ±10 %, so that subjects failing together do not all
    come back in the same second. Past the limit, the failure stands.
    """

    limit: int
    delay: float | int | str = 0
    backoff: str = Backoff.EXPONENTIAL
    max_delay: float | int | str | None = None
    jitter: float = 0.0

    def __post_init__(self) -> None:
        for name in ("delay", "max_delay"):
            object.__setattr__(self, name, canonical(getattr(self, name)))
        if self.limit < 1:
            raise ValueError(f"a retry needs limit >= 1, got {self.limit}")
        object.__setattr__(self, "backoff", Backoff.of(self.backoff, "backoff"))
        if not 0 <= self.jitter < 1:
            raise ValueError(f"jitter is a fraction in [0, 1), got {self.jitter}")
        seconds(self.delay)
        if self.max_delay is not None:
            seconds(self.max_delay)

    def wait(self, attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait before retry number `attempt` (1 for the first)."""
        base = seconds(self.delay)
        if self.backoff == Backoff.LINEAR:
            base *= attempt
        elif self.backoff == Backoff.EXPONENTIAL:
            base *= 2 ** (attempt - 1)
        if self.max_delay is not None:
            base = min(base, seconds(self.max_delay))
        if self.jitter:
            base *= 1 + (rng or random).uniform(-self.jitter, self.jitter)
        return max(base, 0.0)


@dataclass(frozen=True)
class Rate:
    """A RATE LIMIT BAND: at most `limit` per `period`, spread evenly, with up
    to `burst` at once (default: `limit` — a full period's worth). Several
    bands on one node all apply: `(Rate(100, "1m"), Rate(1000, "1h"))`.

    Kept as the GENERIC CELL RATE ALGORITHM (ITU-T I.371): one number per
    band, the theoretical arrival time of the next cell, instead of a
    counter and a window to refresh."""

    limit: int
    period: float | int | str
    burst: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "period", canonical(self.period))
        if self.limit < 1:
            raise ValueError(f"a rate needs limit >= 1, got {self.limit}")
        if seconds(self.period) <= 0:
            raise ValueError(f"a rate needs a period that lasts, got {self.period!r}")
        if self.burst is not None and self.burst < 1:
            raise ValueError(f"a burst is at least 1, got {self.burst}")

    @property
    def interval(self) -> float:
        """T: the time one cell costs."""
        return seconds(self.period) / self.limit

    @property
    def tolerance(self) -> float:
        """τ: how far ahead of schedule the band lets cells run."""
        return ((self.burst or self.limit) - 1) * self.interval


def admit(bands: tuple[Rate, ...], tats: list[float | None], now: float,
          want: int) -> tuple[int, list[float]]:
    """How many of `want` cells the bands let through at `now`, and each
    band's new theoretical arrival time once they are taken.

    Per band, with `lag = max(TAT, now) − now`, the cells that fit are
    `⌊(τ + T − lag) / T⌋`; the batch takes the smallest count over the bands,
    and EVERY band advances by that count — a cell refused by one band
    consumes nothing in the others. A band never used has `TAT = None`."""
    if want <= 0 or not bands:
        return max(want, 0), [t if t is not None else now for t in tats]
    fits = want
    for band, tat in zip(bands, tats, strict=True):
        lag = max(tat if tat is not None else now, now) - now
        fits = min(fits, max(0, math.floor((band.tolerance + band.interval - lag)
                                           / band.interval + 1e-9)))
    return fits, [max(tat if tat is not None else now, now) + fits * band.interval
                  for band, tat in zip(bands, tats, strict=True)]
