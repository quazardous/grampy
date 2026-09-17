"""THE NODE JOURNAL — who started what, and where it stands.

    claim     take a node for eligible subjects
    conclude  finish it, skip it, or declare it failed
    adopt     record work already done that the journal does not know
    forget    erase it — "never started", the initial state
    release   give back the leases of a dead worker
    progress  what is recorded for ONE subject
    stages    what a BATCH went through, with durations

────────────────────────────────────────────────────────────────────────
THE LOGIC HERE, THE STORAGE IN A DRIVER
────────────────────────────────────────────────────────────────────────

The journal validates against the graph — the node exists, it is
optional before being skipped, a conclusion is a known status — and
derives what the rule needs: the node's parents and its descendants. The
driver (`JournalDriver`) only stores rows and makes the claim ATOMIC.

`grampy.drivers.memory` keeps rows in a dict; `grampy.drivers.postgres`
writes them with SQLAlchemy Core into a table the caller declares. Both
pass the same contract (`grampy.testing.JournalContract`).

────────────────────────────────────────────────────────────────────────
ELIGIBILITY COMES FROM OUTSIDE, AND THE JOURNAL DOES NOT READ IT
────────────────────────────────────────────────────────────────────────

A claim must stay ATOMIC across two things: the node rows and the
application's eligible subjects. Two calls would leave a window where two
workers take the same subject.

The journal therefore receives candidates as an OPAQUE value, in the
driver's own terms — an ordered iterable of subjects for the memory
driver, a `SELECT` for the postgres one. "Not terminal, sorted by
priority" is the application's sentence; it is written once, on its
side, and travels here unread.

────────────────────────────────────────────────────────────────────────
IT NEVER COMMITS
────────────────────────────────────────────────────────────────────────

A driver works on its owner's connection, so a node row and the
application's projection can be written in one transaction. The caller
decides where the transaction ends, because it knows what it put in it.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import Any, Protocol

from .dag import (
    NODE_CONCLUDED,
    NODE_DONE,
    NODE_FAILED,
    NODE_RUNNING,
    NODE_SKIPPED,
    Node,
    descendants,
    node,
)


def utc_now() -> str:
    """The default clock: an ISO-8601 UTC timestamp, to the second.

    TEXT, NOT A DATETIME: rows compare their `started_at` lexically
    (`release`), which is correct for one fixed format and offset.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class JournalDriver(Protocol):
    """The storage a journal needs. Every method works on node rows:
    `(subject, node) → status, started_at, finished_at`, one row per pair.

    A driver NEVER validates against the graph — the journal did — and
    never commits.
    """

    def claim(self, name: str, *, parents: tuple[str, ...],
              after: tuple[str, ...], candidates: Any, limit: int,
              require_parents: bool, now: str) -> list[Any]:
        """ATOMICALLY insert `running` rows for up to `limit` candidates,
        in candidate order, that have no row for `name`, no row for any
        node of `after`, and — when `require_parents` — a satisfying row
        for every parent. Return the subjects taken."""

    def skip(self, name: str, *, parents: tuple[str, ...], candidates: Any,
             now: str) -> int:
        """Insert `skipped` rows for candidates without a row for `name`
        whose parents are all satisfying. Return the count inserted."""

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str) -> int:
        """Set `status` and `finished_at` on RUNNING rows only."""

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Insert `done` rows, never overwriting an existing row."""

    def forget(self, name: str, subjects: list[Any]) -> int:
        """Delete the rows."""

    def release(self, name: str, *, older_than: str) -> int:
        """Delete RUNNING rows started before `older_than`."""

    def progress(self, subject: Any) -> dict[str, str]:
        """`{node: status}` for one subject."""

    def status_counts(self, name: str) -> dict[str, int]:
        """`{status: count}` for one node."""

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        """`{subject: [[node, ended, seconds, status], …]}`, skipped rows
        excluded, ordered by `(ended, node)`; a running row ends `at`."""

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> Any:
        """"Every parent has a satisfying row" — in the driver's terms."""


class NodeJournal:
    """Progress per node, validated against ONE graph, stored by a driver."""

    def __init__(self, driver: JournalDriver, dag: tuple[Node, ...], *,
                 clock: Callable[[], str] = utc_now) -> None:
        self.driver = driver
        self.dag = dag
        self._clock = clock

    # -- take --------------------------------------------------------------

    def claim(self, name: str, limit: int, *, candidates: Any,
              require_parents: bool = True) -> list[Any]:
        """Take up to `limit` candidates for this node. Return their subjects.

        THE INSERT IS THE LOCK. Nobody reads then marks: two workers aiming
        at the same subject, the second one conflicts and gets nothing.

        THE RULE IS `dag.claimable`, in the driver's terms — parents
        concluded, nobody holding, no descendant started.

        `require_parents=False` — A NAMED EXCEPTION. A backfill over subjects
        older than the graph has no parent rows to show; requiring them would
        always return nothing. The caller lifting it must hold a proof outside
        the graph. The two other guards still hold, and so does the lock.
        """
        node(name, self.dag)
        return self.driver.claim(
            name, parents=node(name, self.dag).parents,
            after=tuple(sorted(descendants(name, self.dag))),
            candidates=candidates, limit=int(limit),
            require_parents=require_parents, now=self._clock())

    def parents_concluded(self, name: str, subject: Any) -> Any:
        """ALL PARENTS CONCLUDED — `done` or `skipped` — for this subject.

        Returned in the driver's terms: a boolean for the memory driver, a
        SQL expression for the postgres one, so an application can count
        what is waiting for a node in its own query.
        """
        return self.driver.parents_concluded(node(name, self.dag).parents, subject)

    # -- conclude ----------------------------------------------------------

    def conclude(self, name: str, subjects: list[Any], *,
                 status: str = NODE_DONE) -> int:
        """Finish this node on these subjects. Return the count touched.

        ONLY RUNNING ROWS ARE TOUCHED: a duplicate report — a worker retrying
        after a network timeout — recounts nothing and rewrites nothing.
        """
        if not subjects:
            return 0
        if status not in NODE_CONCLUDED:
            raise ValueError(
                f"unknown conclusion status: {status!r} — expected "
                f"{NODE_DONE}, {NODE_SKIPPED} or {NODE_FAILED}")
        node(name, self.dag)
        return self.driver.conclude(name, _unique(subjects), status=status,
                                    now=self._clock())

    def fail(self, name: str, subjects: list[Any]) -> int:
        """This node did not produce. `failed` DOES NOT satisfy its children."""
        return self.conclude(name, subjects, status=NODE_FAILED)

    def skip(self, name: str, *, candidates: Any) -> int:
        """Mark this OPTIONAL node as given up, on the candidates.

        ONLY A CLAIMABLE NODE IS SKIPPED — parents concluded. Without that
        invariant, skipping a node on a subject not there yet would let its
        children start early, since they only name their DIRECT parents.
        """
        n = node(name, self.dag)
        if not n.optional:
            raise ValueError(
                f"node {name!r} is not optional — skipping it would move "
                f"the graph forward without its work")
        return self.driver.skip(name, parents=n.parents, candidates=candidates,
                                now=self._clock())

    def adopt(self, name: str, subjects: list[Any]) -> int:
        """Record work ALREADY DONE that the journal does not know.

        For subjects older than the graph: with no row, they are neither
        claimable nor skippable downstream, and they wait without an error.
        The application ATTESTS the work — a reached state, a file — and
        the journal writes what is already true.

        NEVER OVERWRITES: a row that exists keeps its status, `failed`
        included.
        """
        node(name, self.dag)
        if not subjects:
            return 0
        return self.driver.adopt(name, _unique(subjects), now=self._clock())

    # -- undo --------------------------------------------------------------

    def forget(self, name: str, subjects: list[Any]) -> int:
        """Erase this node for these subjects — "never started".

        THE STEP BACK. A retry, a janitor, a replay: the subject goes back,
        and its node must become claimable again. The ABSENCE of a row is
        "never started"; a `to_redo` status would say the same thing while
        complicating the claim.
        """
        if not subjects:
            return 0
        node(name, self.dag)
        return self.driver.forget(name, _unique(subjects))

    def release(self, name: str, older_than: str) -> int:
        """Give back the leases a dead worker has held for too long."""
        node(name, self.dag)
        return self.driver.release(name, older_than=older_than)

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        """What is recorded for ONE subject — exactly what `dag.claimable` reads."""
        return self.driver.progress(subject)

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        """What a BATCH went through — `{subject: [[node, end, seconds, status], …]}`.

        ONE READ FOR THE BATCH, not one per subject. ONLY NODES THAT WORKED:
        a `skipped` row never started. A node STILL RUNNING ends `at`.
        """
        return self.driver.stages([str(s) for s in subjects], at=at)

    def counts(self, name: str) -> dict[str, int]:
        """How many subjects stand where, for this node."""
        node(name, self.dag)
        by_status = self.driver.status_counts(name)
        return {status: int(by_status.get(status, 0))
                for status in (NODE_RUNNING, NODE_DONE, NODE_SKIPPED, NODE_FAILED)}

    def node_for_state(self, state: str) -> str | None:
        """The node whose WORKING state this is, if any.

        `check_dag` refuses two nodes posting the same state, so this returns
        at most one name. `None` is not an error: a state may belong to no
        node.
        """
        for n in self.dag:
            if n.working == state:
                return n.name
        return None


def _unique(subjects: Iterable[Any]) -> list[Any]:
    """The subjects once each, in their order."""
    return list(dict.fromkeys(subjects))
