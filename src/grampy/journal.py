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
optional before being skipped, a conclusion is a known status — and it
DECIDES: what a claim may take is `dag.claimable`, evaluated here, in
Python, on rows the driver read. The driver (`JournalDriver`) stores rows
and offers a few atomic operations; it never decides what is claimable.

ONE RULE, ONE PLACE. A driver that re-expressed the rule in its own
language — SQL, a script — would be a second copy to keep in step, and
every future rule (joins, choices, gates) would have to be written once per
storage. A driver MAY pre-filter what it reads, to read less; it may never
be the one that says yes.

────────────────────────────────────────────────────────────────────────
READ, DECIDE, WRITE — AND THE REVISION CLOSES THE GAP
────────────────────────────────────────────────────────────────────────

Deciding in Python means reading first and writing after, and something
may happen in between. The one thing that can turn a yes into a no is a
row DISAPPEARING: a `forget` takes the parent away, the child must not be
taken on its strength. (Rows appearing only ever close a node or satisfy
it later; a claim that missed them is a claim that ran a moment earlier.)

So every subject carries a REVISION. `forget` raises it, atomically with
the deletion. A claim reads the revision with the rows, and writes with
`insert_if_unchanged`: the row is inserted only if the revision is still
the one read. A claim that decided on rows since forgotten inserts nothing.

HOW a driver makes that atomic is its own affair — a lock for memory, a
row lock held to the end of the transaction for a database. The shared
concurrency tests (`grampy.testing`) hold every driver to the same
outcome, whatever it blocks on.

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

from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timezone
from typing import Any, NamedTuple, Protocol

from .dag import (
    NODE_CONCLUDED,
    NODE_DONE,
    NODE_FAILED,
    NODE_RUNNING,
    NODE_SATISFYING,
    NODE_SKIPPED,
    Node,
    claimable,
    descendants,
    node,
)

#: HOW MANY CANDIDATES A CLAIM READS AT ONCE, at least — more when the
#: limit is higher. A page is one read and at most one write.
PAGE = 200


def utc_now() -> str:
    """The default clock: an ISO-8601 UTC timestamp, to the second.

    TEXT, NOT A DATETIME: rows compare their `started_at` lexically
    (`release`), which is correct for one fixed format and offset.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Entry(NamedTuple):
    """A candidate as a driver read it: its revision, and its rows on the
    nodes the decision needs — `{node: status}`, absent nodes left out."""

    subject: Any
    revision: int
    rows: dict[str, str]


class JournalDriver(Protocol):
    """The storage a journal needs. Every method works on node rows:
    `(subject, node) → status, started_at, finished_at`, one row per pair,
    and on one REVISION per subject — 0 for a subject never forgotten.

    A driver NEVER validates against the graph, never decides what is
    claimable — the journal does both — and never commits.
    """

    def scan(self, candidates: Any, *, name: str, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int) -> Iterator[list[Entry]]:
        """The candidates, IN THEIR ORDER, page by page — each with its
        revision and its rows on `nodes`.

        A PRE-FILTER, NEVER A DECISION: the driver MAY leave out a candidate
        that holds a row for `name`, or one of whose `parents` is not
        `done`/`skipped`. It must not leave out anything else."""

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str) -> list[Any]:
        """ATOMICALLY, for each `(subject, revision)`: insert a `status` row
        for `name` if the subject has no row for `name` AND its revision is
        still `revision`. A `skipped` row is finished at `now`. Return the
        subjects inserted."""

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str) -> int:
        """Set `status` and `finished_at` on RUNNING rows only."""

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Insert `done` rows, never overwriting an existing row."""

    def forget(self, name: str, subjects: list[Any]) -> int:
        """Delete the rows AND raise each subject's revision, atomically:
        no `insert_if_unchanged` that read the old revision may succeed
        afterwards. Return the count of rows deleted."""

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

        THE RULE IS `dag.claimable` — parents concluded, nobody holding, no
        descendant started — decided here on the rows the driver read, and
        written with `insert_if_unchanged`. Two workers aiming at the same
        subject: one row, one winner. A `forget` in between: no row.

        `require_parents=False` — A NAMED EXCEPTION. A backfill over subjects
        older than the graph has no parent rows to show; requiring them would
        always return nothing. The caller lifting it must hold a proof outside
        the graph. The two other guards still hold, and so does the write.
        """
        n = node(name, self.dag)
        limit = int(limit)
        if limit <= 0:
            return []
        after = tuple(sorted(descendants(name, self.dag)))
        parents = n.parents if require_parents else ()
        now = self._clock()
        taken: list[Any] = []
        for page in self.driver.scan(candidates, name=name,
                                     nodes=(name, *n.parents, *after),
                                     parents=parents, page=max(limit, PAGE)):
            chosen = [(e.subject, e.revision) for e in page
                      if self._takable(n, after, e.rows, require_parents)]
            chosen = chosen[:limit - len(taken)]
            if chosen:
                taken += self.driver.insert_if_unchanged(
                    name, chosen, status=NODE_RUNNING, now=now)
            if len(taken) >= limit:
                break
        return taken

    def _takable(self, n: Node, after: tuple[str, ...], rows: dict[str, str],
                 require_parents: bool) -> bool:
        if require_parents:
            return claimable(n.name, self.dag, rows)
        return n.name not in rows and not any(d in rows for d in after)

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
        now = self._clock()
        count = 0
        for page in self.driver.scan(candidates, name=name,
                                     nodes=(name, *n.parents),
                                     parents=n.parents, page=PAGE):
            chosen = [(e.subject, e.revision) for e in page
                      if name not in e.rows
                      and all(e.rows.get(p) in NODE_SATISFYING for p in n.parents)]
            if chosen:
                count += len(self.driver.insert_if_unchanged(
                    name, chosen, status=NODE_SKIPPED, now=now))
        return count

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
