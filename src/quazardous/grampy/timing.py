"""TIME, AS THE JOURNAL WRITES IT — durations, instants, and retry delays.

    seconds      "90", "30s", "10m", "2h", "7d" or a number → seconds
    shift        an ISO instant moved by a number of seconds, same format
    Retry        a declared retry policy: how many times, how long to wait

────────────────────────────────────────────────────────────────────────
INSTANTS ARE TEXT, AND THEY COMPARE AS TEXT
────────────────────────────────────────────────────────────────────────

Rows carry ISO-8601 instants in UTC, to the second (`utc_now`). One fixed
format and one offset make lexical order the time order, in every storage
— which is what lets a driver compare `started_at < older_than` without
knowing what a date is. `shift` keeps that format.
"""
from __future__ import annotations

import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_DURATION = re.compile(r"\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*")
_UNIT = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}

#: THE BACKOFF SHAPES A RETRY MAY TAKE (the names Serverless Workflow uses).
BACKOFFS = ("constant", "linear", "exponential")


def seconds(duration: float | int | str) -> float:
    """A duration in seconds: a number, or text with a unit (s, m, h, d)."""
    if isinstance(duration, bool):
        raise ValueError(f"not a duration: {duration!r}")
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
    backoff: str = "exponential"
    max_delay: float | int | str | None = None
    jitter: float = 0.0

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError(f"a retry needs limit >= 1, got {self.limit}")
        if self.backoff not in BACKOFFS:
            raise ValueError(f"unknown backoff {self.backoff!r} — expected one of {BACKOFFS}")
        if not 0 <= self.jitter < 1:
            raise ValueError(f"jitter is a fraction in [0, 1), got {self.jitter}")
        seconds(self.delay)
        if self.max_delay is not None:
            seconds(self.max_delay)

    def wait(self, attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait before retry number `attempt` (1 for the first)."""
        base = seconds(self.delay)
        if self.backoff == "linear":
            base *= attempt
        elif self.backoff == "exponential":
            base *= 2 ** (attempt - 1)
        if self.max_delay is not None:
            base = min(base, seconds(self.max_delay))
        if self.jitter:
            base *= 1 + (rng or random).uniform(-self.jitter, self.jitter)
        return max(base, 0.0)
