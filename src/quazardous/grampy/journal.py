"""THE NODE JOURNAL — who started what, and where it stands.

    claim     take a node for eligible subjects — a `Lease`, with its token
    conclude  finish it, or declare it failed — with the lease's token
    adopt     record work already done that the journal does not know
    forget    erase it — "never started", the initial state
    release   give back the leases of a dead worker
    expire    give back every lease held longer than its node allows
    enroll    give subjects a channel, whose settings then apply to them
    migrate   move subjects pinned to one graph version onto this one
    signal    record that an awaited event happened, for subjects
    settle    conclude waits and skip optional nodes past their grace
    history   every row forget, release or a loop took away, kept
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
A CONCLUSION PROVES IT HOLDS THE LEASE
────────────────────────────────────────────────────────────────────────

A worker can be slow rather than dead. Its lease released, the node
claimed again by another worker, the first one comes back and reports:
without a proof, it would conclude the SECOND worker's row. Every claim
therefore issues a token, unique to that claim, stored on the rows it
took; `conclude` and `fail` touch only the rows holding the token they
bring (a fencing token). `token=None` is the operator's override, written
on purpose, never a default.

────────────────────────────────────────────────────────────────────────
NOTHING IS LOST: A ROW TAKEN AWAY IS ARCHIVED
────────────────────────────────────────────────────────────────────────

The absence of a row still means "never started", and the claim still
reads only current rows. But a row does not vanish: `forget`, `release`
and a declared loop MOVE it to the history, with when and why. That is
where "how many times did this subject go through review", "who held it
before the lease was released" and the loop's bound are read.

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

import random
import secrets
from collections.abc import Callable, Iterable, Iterator
from datetime import datetime, timezone
from typing import Any, NamedTuple, Protocol

from .dag import (
    NODE_CONCLUDED,
    NODE_DONE,
    NODE_FAILED,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SCHEDULED,
    NODE_SKIPPED,
    Node,
    accepts,
    claimable,
    descendants,
    joined,
    node,
    omitted_by,
)
from .graph import Graph
from .timing import seconds, shift

#: HOW MANY CANDIDATES A CLAIM READS AT ONCE, at least — more when the
#: limit is higher. A page is one read and at most one write.
PAGE = 200


def utc_now() -> str:
    """The default clock: an ISO-8601 UTC timestamp, to the second.

    TEXT, NOT A DATETIME: rows compare their `started_at` lexically
    (`release`), which is correct for one fixed format and offset.
    """
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Lease(list):
    """The subjects a claim took — a plain list — and `token`, the proof a
    worker brings back to `conclude` or `fail` them."""

    def __init__(self, subjects: Iterable[Any], token: str) -> None:
        super().__init__(subjects)
        self.token = token


class Entry(NamedTuple):
    """A candidate as a driver read it: its revision, and its rows on the
    nodes the decision needs — `{node: status}`, absent nodes left out."""

    subject: Any
    revision: int
    rows: dict[str, str]
    #: `{node: due}` for the `scheduled` rows among `rows`.
    due: dict[str, str] = {}
    #: `{node: finished_at}` for the concluded rows among `rows`.
    finished: dict[str, str] = {}
    #: The subject's channel, None when it has none.
    channel: str | None = None
    #: The graph version the subject is pinned to, None before its first write.
    version: str | None = None


class JournalDriver(Protocol):
    """The storage a journal needs. Every method works on node rows:
    `(subject, node) → status, started_at, finished_at`, one row per pair,
    and on one REVISION per subject — 0 for a subject never forgotten.

    A SUBJECT IS THE APPLICATION'S ID: unique, stable, an `int` or a `str`,
    stored and returned exactly as given — never converted, built or split.

    A driver NEVER validates against the graph, never decides what is
    claimable — the journal does both — and never commits.
    """

    def scan(self, candidates: Any, *, name: str, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str) -> Iterator[list[Entry]]:
        """The candidates, IN THEIR ORDER, page by page — each with its
        revision, its rows on `nodes`, and when its `scheduled` rows are due.

        A PRE-FILTER, NEVER A DECISION: the driver MAY leave out a candidate
        that holds a row for `name` — other than a `scheduled` row due by
        `now` — or one of whose `parents` has no `NODE_SATISFYING` row
        (`parents` is empty when the node joins in a way a pre-filter cannot
        know). It must not leave out anything else."""

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        """ATOMICALLY, for each `(subject, revision)`: write a `status` row
        for `name`, holding `lease`, started at `now`, if the subject's
        revision is still `revision` AND it has no row for `name` — or only a
        `scheduled` row due by `now`, which the new row replaces. Any row
        but a `running` one is finished at `now`. Return the subjects
        written."""

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        """Set `status` and `finished_at` on RUNNING rows only — and, unless
        `lease` is None, only on rows holding that lease. For every subject
        concluded, ATOMICALLY with it: insert an `omitted` row finished at
        `now` for each node of `omit` that has no row; then ARCHIVE with
        reason `loop` and delete the rows of every node of `reset` — the
        concluded row included when it is among them — raising the
        subject's revision as `forget` does; or, when `reschedule` is given,
        ARCHIVE the concluded row with reason `retry` and replace it with a
        `scheduled` row started at `reschedule`, its due time."""

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Insert `done` rows, never overwriting an existing row."""

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        """ARCHIVE with reason `forget` and delete the rows, AND raise each
        subject's revision, atomically: no `insert_if_unchanged` that read
        the old revision may succeed afterwards. Return the count deleted."""

    def release(self, name: str, *, older_than: str, now: str,
                only: tuple[str, ...] | None = None, exclude: tuple[str, ...] = (),
                version: str | None = None) -> int:
        """ARCHIVE with reason `release` and delete the RUNNING rows started
        before `older_than` — of subjects whose channel is in `only` when
        given, and not in `exclude` (a subject without channel is never in
        either) — and, when `version` is given, of subjects pinned to it or
        to none."""

    def enroll(self, subjects: list[Any], channel: str | None) -> int:
        """Record each subject's channel in the registry (the revisions),
        creating its entry when needed. Return the count written."""

    def pin(self, subjects: list[Any], version: str) -> int:
        """Record `version` for the subjects that have none yet — never
        overwrite one. Return the count newly pinned."""

    def versions(self, subjects: list[Any]) -> dict[Any, str]:
        """`{subject: version}` for the subjects pinned to one."""

    def rewrite(self, subject: Any, *, rename: dict[str, str], drop: tuple[str, ...],
                version: str, now: str) -> None:
        """ATOMICALLY for one subject: archive with reason `migrate` and
        delete the rows of `drop`, rename the rows of `rename` (old → new;
        the journal guarantees no two land on one name), pin the subject to
        `version` — overwriting — and raise its revision."""

    def channels(self, subjects: list[Any]) -> dict[Any, str]:
        """`{subject: channel}` for the subjects that have one."""

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """The archived rows of one subject, oldest archive first (ties by
        node): `node, status, started_at, finished_at, lease, archived_at,
        reason`."""

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        """`{subject: rows of `name` archived with `reason`}`, subjects
        without any left out."""

    def signal(self, subjects: list[Any], event: str, *, now: str, ref: str | None) -> int:
        """Append to each subject's history a row `node=event`,
        `status="received"`, `reason="signal"`, archived at `now`, `ref` in
        `lease`. Return the count written."""

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        """`{subject: latest archived_at}` of the history rows of `name` —
        with `reason` when given, any reason otherwise; subjects without any
        left out."""

    def now(self) -> str:
        """The storage's clock, in the journal's format (`utc_now`): ONE
        source of time for every process writing to the same storage."""

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

    def __init__(self, driver: JournalDriver, dag: tuple[Node, ...] | Graph, *,
                 clock: Callable[[], str] | None = None,
                 rng: random.Random | None = None) -> None:
        """`dag` is a tuple of nodes, or a `Graph` whose channels change some
        settings per source. `clock` defaults to the driver's
        (`JournalDriver.now`): workers on several machines then share one
        time. `rng` spreads retry jitter."""
        self.driver = driver
        self.graph = dag if isinstance(dag, Graph) else None
        self.dag = dag.nodes if isinstance(dag, Graph) else dag
        #: SUBJECTS ARE PINNED TO THE VERSION THEY STARTED ON: a journal on a
        #: `Graph` takes only subjects of its version, or not yet pinned, and
        #: pins them on their first write. None for a tuple of nodes.
        self.version = dag.document.version if isinstance(dag, Graph) else None
        self._clock = clock or driver.now
        self._rng = rng or random.Random()

    # -- take --------------------------------------------------------------

    def claim(self, name: str, limit: int, *, candidates: Any,
              require_parents: bool = True) -> Lease:
        """Take up to `limit` candidates for this node. Return their subjects,
        as a `Lease` whose `token` the worker brings back to conclude them.

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
        if n.wait is not None:
            raise ValueError(
                f"node {name!r} waits for {n.wait!r}: it is settled (`settle`), "
                f"never claimed")
        limit = int(limit)
        token = secrets.token_hex(16)
        if limit <= 0:
            return Lease([], token)
        after = tuple(sorted(descendants(name, self.dag)))
        parents = n.parents if require_parents and not n.custom_join else ()
        now = self._clock()
        chosen: list[tuple[Any, int]] = []
        seen: set[Any] = set()
        for page in self.driver.scan(candidates, name=name,
                                     nodes=(name, *n.parents, *after),
                                     parents=parents, page=max(limit, PAGE), now=now):
            for e in page:
                # A SUBJECT LISTED TWICE COUNTS ONCE: it would otherwise take
                # a place in the limit and be refused by the write.
                if e.subject in seen or not self._mine(e):
                    continue
                seen.add(e.subject)
                if self._takable(n, after, _due_away(name, e, now), require_parents):
                    chosen.append((e.subject, e.revision))
                    if len(chosen) >= limit:
                        break
            if len(chosen) >= limit:
                break
        # ONE WRITE PER CLAIM. A storage that locks while writing takes its
        # locks in one go, in one order, and two claimers cannot hold each
        # other. The price: a subject refused by the write (another claimer
        # took it, a revision moved) is not replaced — under contention a
        # claim may take fewer than `limit`, never a wrong one.
        taken = (self.driver.insert_if_unchanged(
                     name, chosen, status=NODE_RUNNING, now=now, lease=token)
                 if chosen else [])
        self._pin(taken)
        return Lease(taken, token)

    def _mine(self, entry: Entry) -> bool:
        return self.version is None or entry.version in (None, self.version)

    def _pin(self, subjects: list[Any]) -> None:
        if self.version is not None and subjects:
            self.driver.pin(list(subjects), self.version)

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

    def conclude(self, name: str, subjects: list[Any], *, token: str | None,
                 status: str = NODE_DONE, branch: str | None = None) -> int:
        """Finish this node on these subjects. Return the count touched.

        ONLY RUNNING ROWS HOLDING `token` ARE TOUCHED: a duplicate report — a
        worker retrying after a network timeout — recounts nothing, and a
        worker whose lease went to someone else rewrites nothing. `token` is
        required; `None` concludes whoever holds the rows (an operator's act).

        A CHOICE CONCLUDES BY NAMING ITS BRANCH. `branch` is required to
        conclude a choice `done` or `skipped`, refused anywhere else; the
        other branches and what only they reach are `omitted` in the same
        write (`dag.omitted_by`). A failed choice omits nothing: its branches
        wait on it like any child on a failed parent.
        """
        if not subjects:
            return 0
        n = node(name, self.dag)
        if status not in NODE_CONCLUDED or status == NODE_OMITTED:
            raise ValueError(
                f"unknown conclusion status: {status!r} — expected "
                f"{NODE_DONE}, {NODE_SKIPPED} or {NODE_FAILED}")
        omit: tuple[str, ...] = ()
        subjects = _unique(subjects)
        now = self._clock()
        touched = 0
        retry_of = self._per_channel(name, "retry", subjects)
        if any(r is not None for r in retry_of.values()) and status == NODE_FAILED \
                and branch is None:
            # FIRST, THE RETRIES — each subject under its channel's policy: under
            # the limit, the failure is archived and the node scheduled again,
            # later for later attempts.
            tries = self.driver.archived(subjects, name, "retry")
            by_due: dict[str, list[Any]] = {}
            for s in subjects:
                policy = retry_of[s]
                if policy is not None and tries.get(s, 0) < policy.limit:
                    wait = policy.wait(tries.get(s, 0) + 1, self._rng)
                    by_due.setdefault(shift(now, wait), []).append(s)
            for due, group in by_due.items():
                touched += self.driver.conclude(name, group, status=status, now=now,
                                                lease=token, omit=(), reset=(),
                                                reschedule=due)
            retried = {s for group in by_due.values() for s in group}
            subjects = [s for s in subjects if s not in retried]
            if not subjects:
                return touched
        if n.loop is not None and status in n.loop.on:
            # THE WAY BACK, per subject: under the bound, the conclusion sends
            # it to `loop.to` in the same write; at the bound, it stands.
            reset = (n.loop.to, *sorted(descendants(n.loop.to, self.dag)))
            passes = self.driver.archived(subjects, n.loop.to, "loop")
            back = [s for s in subjects if passes.get(s, 0) < n.loop.max]
            stay = [s for s in subjects if passes.get(s, 0) >= n.loop.max]
            if back:
                touched += self.driver.conclude(name, back, status=status, now=now,
                                                lease=token, omit=(), reset=reset,
                                                reschedule=None)
            if stay:
                touched += self.driver.conclude(name, stay, status=status, now=now,
                                                lease=token, omit=(), reset=(),
                                                reschedule=None)
            return touched
        if n.choice and status != NODE_FAILED:
            if branch is None:
                raise ValueError(
                    f"node {name!r} is a choice: concluding it needs `branch=`, "
                    f"one of its children")
            omit = omitted_by(name, branch, self.dag)
        elif branch is not None:
            raise ValueError(
                f"`branch` given, but node {name!r} "
                f"{'failed' if n.choice else 'is not a choice'}")
        return touched + self.driver.conclude(name, subjects, status=status, now=now,
                                              lease=token, omit=omit, reset=(),
                                              reschedule=None)

    def fail(self, name: str, subjects: list[Any], *, token: str | None,
             branch: str | None = None) -> int:
        """This node did not produce. `failed` satisfies no child — unless a
        child's edge accepts it (`Node.on`). `branch` is accepted only to be
        refused: a failed choice names nothing."""
        return self.conclude(name, subjects, token=token, status=NODE_FAILED,
                             branch=branch)

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
        chosen: dict[tuple[Any, int], None] = {}
        for page in self.driver.scan(candidates, name=name,
                                     nodes=(name, *n.parents),
                                     parents=() if n.custom_join else n.parents,
                                     page=PAGE, now=now):
            chosen.update(dict.fromkeys(
                (e.subject, e.revision) for e in page
                if self._mine(e) and name not in e.rows and joined(name, self.dag, e.rows)))
        # One write, as for a claim.
        written = self.driver.insert_if_unchanged(
            name, list(chosen), status=NODE_SKIPPED, now=now, lease=None) if chosen else []
        self._pin(written)
        return len(written)

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
        count = self.driver.adopt(name, _unique(subjects), now=self._clock())
        self._pin(_unique(subjects))
        return count

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
        return self.driver.forget(name, _unique(subjects), now=self._clock())

    def release(self, name: str, older_than: str) -> int:
        """Give back the leases a dead worker has held for too long."""
        node(name, self.dag)
        return self.driver.release(name, older_than=older_than, now=self._clock())

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        """What is recorded for ONE subject — exactly what `dag.claimable` reads."""
        return self.driver.progress(subject)

    def expire(self) -> dict[str, int]:
        """Release, on every node declaring a `lease`, the rows held longer
        than it allows. Return `{node: count}` for the nodes that released
        something. Meant to be called on a schedule — a janitor — by the
        application: grampy runs no thread of its own."""
        now = self._clock()
        released: dict[str, int] = {}
        for n in self.dag:
            special = {channel: variant.lease for channel, variant in self._variants(n.name)
                       if variant.lease != n.lease}
            count = 0
            if n.lease is not None:
                count += self.driver.release(
                    n.name, older_than=shift(now, -seconds(n.lease)), now=now,
                    exclude=tuple(sorted(special)), version=self.version)
            for channel, lease in special.items():
                if lease is not None:
                    count += self.driver.release(
                        n.name, older_than=shift(now, -seconds(lease)), now=now,
                        only=(channel,), version=self.version)
            if count:
                released[n.name] = count
        return released

    def enroll(self, subjects: list[Any], channel: str | None) -> int:
        """Give these subjects a CHANNEL — their source. The graph's settings
        for that channel (retries, leases, timeouts, graces) then apply to
        them; the structure of the workflow stays the same for all."""
        if not subjects:
            return 0
        count = self.driver.enroll(_unique(subjects), channel)
        self._pin(_unique(subjects))
        return count

    def pinned(self, subject: Any) -> str | None:
        """The graph version the subject is pinned to, None before its first
        write."""
        return self.driver.versions([subject]).get(subject)

    def migrate(self, subjects: list[Any], source: Graph,
                mapping: dict[str, str | None] | None = None) -> int:
        """MOVE SUBJECTS FROM `source` — another version of this graph — ONTO
        THIS ONE, or refuse them all.

        `mapping` names what became of each source node: another name, or
        `None` when it is gone (its rows are archived, reason `migrate`). A
        node left out keeps its name, and must exist here.

        A subject is COMPLIANT when its journal could have been written on
        this graph: every row, once renamed, stands where the rule of this
        graph lets a row stand — its parents joined (Rinderle, Reichert and
        Dadam's compliance criterion, on the current rows only, which are
        already the latest pass of any loop). A dropped node held by a worker
        makes a subject non-compliant too.

        ALL OR NOTHING: every subject is checked before anything is written;
        one failure raises `MigrationError` naming each non-compliant subject
        and why, and nothing moves. Subjects already on this version, or
        pinned to a third one, are refused the same way. Return the count
        migrated."""
        if self.graph is None or self.version is None:
            raise ValueError("migrate needs a journal built on a versioned Graph")
        if source.document.version == self.version:
            raise ValueError(f"the source is already version {self.version!r}")
        mapping = dict(mapping or {})
        here = {n.name for n in self.dag}
        there = {n.name for n in source.nodes}
        unknown = sorted(set(mapping) - there)
        if unknown:
            raise ValueError(f"mapping names nodes the source does not have: {unknown}")
        full = {name: mapping.get(name, name) for name in there}
        missing = sorted(name for name, target in full.items()
                         if target is not None and target not in here)
        if missing:
            raise ValueError(f"source nodes with nowhere to go on version {self.version!r}: "
                             f"{missing} — map them to a node or to None")
        landed = [t for t in full.values() if t is not None]
        if len(landed) != len(set(landed)):
            raise ValueError("two source nodes are mapped onto the same node")

        subjects = _unique(subjects)
        pinned = self.driver.versions(subjects)
        problems: dict[Any, str] = {}
        plans: dict[Any, tuple[dict[str, str], tuple[str, ...]]] = {}
        for subject in subjects:
            if pinned.get(subject, source.document.version) != source.document.version:
                problems[subject] = f"pinned to version {pinned[subject]!r}"
                continue
            progress = self.driver.progress(subject)
            drop = tuple(sorted(name for name in progress if full.get(name, name) is None))
            held = [name for name in drop if progress[name] in (NODE_RUNNING, NODE_SCHEDULED)]
            if held:
                problems[subject] = f"{held} would be dropped while held or scheduled"
                continue
            moved: dict[str, str] = {}
            rename: dict[str, str] = {}
            for name, status in progress.items():
                target = full.get(name, name)
                if target is not None:
                    moved[target] = status
                    if target != name:
                        rename[name] = target
            stray = sorted(name for name in moved if name not in here)
            if stray:
                problems[subject] = f"rows on {stray}, which version {self.version!r} lacks"
                continue
            unjoined = sorted(name for name in moved if not joined(name, self.dag, moved))
            if unjoined:
                problems[subject] = (f"{unjoined} could not have run on version "
                                     f"{self.version!r}: their parents are not joined")
                continue
            plans[subject] = (rename, drop)
        if problems:
            raise MigrationError(problems)
        now = self._clock()
        target_version: str = self.version
        for subject, (renamed, dropped) in plans.items():
            self.driver.rewrite(subject, rename=renamed, drop=dropped,
                                version=target_version, now=now)
        return len(plans)

    def channel(self, subject: Any) -> str | None:
        """The subject's channel, None when it has none."""
        return self.driver.channels([subject]).get(subject)

    def settings(self, name: str, channel: str | None) -> Node:
        """The node as a subject of `channel` sees it."""
        node(name, self.dag)
        if self.graph is None:
            return node(name, self.dag)
        return node(name, self.graph.variant(channel))

    def _variants(self, name: str) -> list[tuple[str, Node]]:
        if self.graph is None:
            return []
        return [(channel, node(name, self.graph.variant(channel)))
                for channel in self.graph.channels]

    def _per_channel(self, name: str, setting: str, subjects: list[Any]) -> dict[Any, Any]:
        """`{subject: value of `setting` on `name` for its channel}`."""
        default = getattr(node(name, self.dag), setting)
        if self.graph is None or not any(
                name in overrides and setting in overrides[name]
                for overrides in self.graph.channels.values()):
            return dict.fromkeys(subjects, default)
        channels = self.driver.channels(subjects)
        return {s: getattr(self.settings(name, channels.get(s)), setting) for s in subjects}

    def signal(self, subjects: list[Any], event: str, ref: str | None = None) -> int:
        """Record that `event` happened for these subjects — DURABLY, before
        any wait for it may have begun: a wait settles on a signal received
        since it last went back (a loop, a forget), however early."""
        if not subjects:
            return 0
        return self.driver.signal(_unique(subjects), event, now=self._clock(), ref=ref)

    def settle(self, candidates: Any) -> dict[str, dict[str, int]]:
        """CONCLUDE WHAT NO WORKER DOES, on the candidates, for every node:

            wait      `done` when a signal was received since the node last
                      went back; `failed` once `timeout` has passed since
                      its parents concluded
            grace     an optional node still untaken `grace` after its
                      parents concluded is `skipped`

        Time runs from the LATEST accepted parent's conclusion; a node
        without parents has no clock and never times out. Return
        `{node: {status: count}}` for what was written. Meant for the
        application's janitor, like `expire`."""
        now = self._clock()
        out: dict[str, dict[str, int]] = {}
        for n in self.dag:
            if n.wait is None and n.grace is None and all(
                    v.grace is None for _, v in self._variants(n.name)):
                continue
            after = tuple(sorted(descendants(n.name, self.dag)))
            entries = [e for page in self.driver.scan(
                           candidates, name=n.name, nodes=(n.name, *n.parents, *after),
                           parents=() if n.custom_join else n.parents, page=PAGE, now=now)
                       for e in page]
            ready = list({e.subject: e for e in entries
                          if self._mine(e)
                          and claimable(n.name, self.dag, _due_away(n.name, e, now))}.values())
            if not ready:
                continue
            decided: dict[str, list[tuple[Any, int]]] = {}
            subjects = [e.subject for e in ready]
            heard: dict[Any, str] = {}
            went_back: dict[Any, str] = {}
            if n.wait is not None:
                heard = self.driver.latest(subjects, n.wait, "signal")
                went_back = self.driver.latest(subjects, n.name, None)
            for e in ready:
                since = _joined_since(n, e)
                seen_by = self.settings(n.name, e.channel)
                if n.wait is not None:
                    at = heard.get(e.subject)
                    if at is not None and at >= went_back.get(e.subject, ""):
                        decided.setdefault(NODE_DONE, []).append((e.subject, e.revision))
                        continue
                    if (seen_by.timeout is not None and since is not None
                            and shift(since, seconds(seen_by.timeout)) <= now):
                        decided.setdefault(NODE_FAILED, []).append((e.subject, e.revision))
                elif (since is not None and seen_by.grace is not None
                      and shift(since, seconds(seen_by.grace)) <= now):
                    decided.setdefault(NODE_SKIPPED, []).append((e.subject, e.revision))
            for status, chosen in decided.items():
                written = self.driver.insert_if_unchanged(n.name, chosen, status=status,
                                                          now=now, lease=None)
                self._pin(written)
                if written:
                    out.setdefault(n.name, {})[status] = len(written)
        return out

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """Every row taken away from ONE subject — by `forget`, `release` or
        a loop — oldest first, each with `archived_at` and `reason`."""
        return self.driver.history(subject)

    def passes(self, subject: Any, name: str) -> int:
        """How many times a declared loop sent this subject back through
        `name` — what `Loop.max` bounds."""
        node(name, self.dag)
        return self.driver.archived([subject], name, "loop").get(subject, 0)

    def retries(self, subject: Any, name: str) -> int:
        """How many times a failure of `name` was retried for this subject."""
        node(name, self.dag)
        return self.driver.archived([subject], name, "retry").get(subject, 0)

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
                for status in (NODE_RUNNING, NODE_SCHEDULED, *NODE_CONCLUDED)}

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


class MigrationError(ValueError):
    """Subjects that cannot move to the new version — `problems` says why,
    subject by subject. Nothing was written."""

    def __init__(self, problems: dict[Any, str]) -> None:
        self.problems = problems
        lines = "; ".join(f"{s!r}: {why}" for s, why in list(problems.items())[:10])
        more = f" (+{len(problems) - 10} more)" if len(problems) > 10 else ""
        super().__init__(f"{len(problems)} subject(s) not compliant — {lines}{more}")


def _joined_since(n: Node, entry: Entry) -> str | None:
    """When the node became joined: the latest conclusion among the parents
    it accepts. None for a root, or when no conclusion time is known."""
    times = [entry.finished[p] for p in n.parents
             if entry.rows.get(p) in accepts(n, p) and p in entry.finished]
    return max(times) if times else None


def _due_away(name: str, entry: Entry, now: str) -> dict[str, str]:
    """The rows the rule reads: a `scheduled` row of `name` due by `now`
    counts as absent — the node may be taken again."""
    if entry.rows.get(name) == NODE_SCHEDULED and entry.due.get(name, now) <= now:
        return {k: v for k, v in entry.rows.items() if k != name}
    return entry.rows


def _unique(subjects: Iterable[Any]) -> list[Any]:
    """The subjects once each, in their order."""
    return list(dict.fromkeys(subjects))
