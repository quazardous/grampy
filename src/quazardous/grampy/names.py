"""GRAMPY'S OWN WORDS — every value the library gives a meaning to.

A node's name, a subject, a policy are YOURS: any string you like. A status,
a history reason, a lane's merge mode are GRAMPY'S: they are members here, so
that a reader tells the two apart at a glance.

    from quazardous.grampy import Status, Reason

    journal.progress("s1") == {"fetch": Status.DONE}
    [row for row in journal.history("s1") if row["reason"] == Reason.RETRY]

EACH MEMBER IS ALSO ITS STRING. `Status.DONE == "done"`, it hashes like
`"done"`, and it is stored, written to JSON and compared as `"done"` — so
nothing stored changes, and a string still works wherever a member does. What
a storage hands back stays a plain string; it compares equal all the same.

`str()` and f-strings give the value on every Python: a bare `(str, Enum)`
writes `Status.DONE` in an f-string from 3.12 on, and `done` before it.
"""
from __future__ import annotations

from enum import Enum
from typing import TypeVar

N = TypeVar("N", bound="Name")


class Name(str, Enum):
    """A word of grampy's: a string, and a member of a closed set."""

    def __str__(self) -> str:
        return str(self.value)

    def __format__(self, spec: str) -> str:
        return format(str(self.value), spec)

    @classmethod
    def of(cls: type[N], value: str, what: str) -> N:
        """The member for `value`, or a `ValueError` naming what was expected."""
        try:
            return cls(value)
        except ValueError:
            raise ValueError(
                f"unknown {what} {value!r} — expected one of "
                f"{[m.value for m in cls]}") from None


class Status(Name):
    """Where a node stands for a subject: its row in the journal."""

    #: Taken by a worker, not concluded yet.
    RUNNING = "running"
    #: Waiting to be taken again — a retry's row; `started_at` is when it is due.
    SCHEDULED = "scheduled"
    DONE = "done"
    #: Given up: an optional node skipped — by `skip`, or once its grace ran out.
    SKIPPED = "skipped"
    #: A branch a choice did not take, and what only it leads to.
    OMITTED = "omitted"
    #: Did not produce; satisfies no child unless the child's edge says so.
    FAILED = "failed"


class Reason(Name):
    """Why a row went to the history. `RETRY` and `LOOP` are COUNTED: they
    are what a retry limit and a loop bound read."""

    FORGET = "forget"
    RELEASE = "release"
    RETRY = "retry"
    LOOP = "loop"
    MIGRATE = "migrate"
    ARRIVAL = "arrival"
    LANE = "lane"
    SIGNAL = "signal"


class Outcome(Name):
    """What a history NOTE records, rather than a node's row: what became of
    an arrival in a lane (counted by `arrive` and `settle`, the status of a
    `LANE` row), and a signal received (the status of a `SIGNAL` row)."""

    RECEIVED = "received"
    #: Not a note: what `counts()` reports for a lane — the arrivals waiting.
    WAITING = "waiting"
    QUEUED = "queued"
    MERGED = "merged"
    SKIPPED = "skipped"
    DROPPED = "dropped"
    ENTERED = "entered"


#: WHAT NAMES A MERGE FUNCTION rather than one of the modes below.
MERGE_FN = "fn:"


class Merge(Name):
    """How a lane merges a new arrival into the one waiting."""

    #: The arrival waiting keeps its ref.
    FIRST = "first"
    #: The new arrival's ref replaces it.
    LAST = "last"
    #: Every ref is kept, in the order they came.
    ALL = "all"
    #: DRIVER PROTOCOL ONLY: the stored list of refs is replaced as given.
    SET = "set"

    @staticmethod
    def fn(name: str) -> str:
        """A merge decided by the function the journal was given as `name`:
        `Lane(merge=Merge.fn("my-rule"))`."""
        return f"{MERGE_FN}{name}"


class Position(Name):
    """Where a merged arrival stands in its lane."""

    FIRST = "first"
    LAST = "last"


class WhileRunning(Name):
    """What an arrival does while a pass of its subject is still running."""

    QUEUE = "queue"
    SKIP = "skip"


class Backoff(Name):
    """How a retry's wait grows from one attempt to the next."""

    CONSTANT = "constant"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class Per(Name):
    """Whose budget a node's rate and concurrency count."""

    #: One budget, shared by every policy.
    ALL = "all"
    #: One budget per policy.
    POLICY = "policy"
