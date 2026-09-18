"""THE NODE JOURNAL — who started what, and where it stands.

    claim     take a node for eligible subjects — a `Lease`, with its token
    conclude  finish it, or declare it failed — with the lease's token
    adopt     record work already done that the journal does not know
    forget    erase it — "never started", the initial state
    release   give back the leases of a dead worker
    expire    give back every lease held longer than its node allows
    enroll    give subjects a policy, whose settings then apply to them
    migrate   move subjects pinned to another graph onto this one
    signal    record that an awaited event happened, for subjects
    arrive    a subject comes back: it waits in a lane before running again
    settle    conclude waits, let due arrivals through their lane, and skip
              optional nodes past their grace
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

import json
import random
import secrets
from collections.abc import Callable, Iterable, Iterator
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, NamedTuple, Protocol

from .dag import (
    NODE_CONCLUDED,
    Lane,
    Node,
    accepts,
    claimable,
    descendants,
    joined,
    node,
    omitted_by,
)
from .graph import Graph
from .names import Merge, Outcome, Per, Reason, Status, WhileRunning
from .timing import admit, seconds, shift, stamp

#: HOW MANY CANDIDATES A CLAIM READS AT ONCE, at least — more when the
#: limit is higher. A page is one read and at most one write.
PAGE = 200


def _ready_at(n: Node, entry: Entry) -> str | None:
    """WHEN THIS SUBJECT BECAME READY for a grouping node: the last of its
    parents to conclude. A node with no parent has no clock, so a group of
    those only ever goes when it is full."""
    ends = [entry.finished[p] for p in n.parents if p in entry.finished]
    return max(ends) if ends else None


def _encode_refs(refs: tuple[str, ...]) -> str | None:
    """The refs a lane keeps, as the text a driver stores. `None` when there
    is nothing to keep, so a lane that keeps one ref stores no list."""
    return json.dumps(list(refs)) if refs else None


def _decode_refs(stored: str | None) -> tuple[str, ...]:
    return tuple(json.loads(stored)) if stored else ()


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


class Keyed(NamedTuple):
    """A CANDIDATE CARRYING ITS GROUPING KEY, for a node that groups, in a
    plain iterable of candidates. In SQL, the key is the column named
    `grampy_key`; in the items layer, `Adapter.group_of`. A bare tuple is
    never read as one: an extra value that only happened to be there would
    otherwise group subjects by it, silently."""

    subject: Any
    key: str | None


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
    #: The subject's policy, None when it has none.
    policy: str | None = None
    #: The graph the subject is pinned to, None before its first write.
    version: str | None = None
    #: WHAT THE CANDIDATES SAID TO GROUP THIS SUBJECT BY — a second column of
    #: the query, or the second half of a `(subject, key)` pair. Compared,
    #: never read; None when the candidates named none.
    key: str | None = None


class _Ready(NamedTuple):
    """A candidate a grouping node could take: what it is, what groups it,
    and since when it has been waiting for its group to fill."""

    subject: Any
    revision: int
    key: str | None
    ready_at: str | None
    policy: str | None


class Arrival(NamedTuple):
    """A subject waiting in a lane: what it brings (`ref`, opaque —
    what came back, NOT the graph's `document.version`),
    its `place` in the lane, when it FIRST arrived, and whether it is
    urgent.

    `refs` holds EVERY ref still waiting, for a lane that keeps them —
    text the journal encodes and decodes, stored by the driver as it is.
    `journal.refs(subject, node)` gives it back as a tuple.
    """

    ref: str | None
    place: str
    arrived_at: str
    urgent: bool = False
    refs: str | None = None


class JournalDriver(Protocol):
    """The storage a journal needs. Every method works on node rows:
    `(subject, node) → status, started_at, finished_at`, one row per pair,
    and on one REVISION per subject — 0 for a subject never forgotten.

    A SUBJECT IS THE APPLICATION'S ID: unique, stable, an `int` or a `str`,
    stored and returned exactly as given — never converted, built or split.

    A driver NEVER validates against the graph, never decides what is
    claimable — the journal does both — and never commits.

    OPTIONAL, `skip_where(name, candidates, *, parents, now, version) ->
    subjects`: `skip` in one write, for a node joining its parents plainly.
    It writes a `skipped` row, started and finished at `now`, for every
    candidate with no row for `name`, a satisfying row for every parent,
    pinned to `version` or to none (any, when `version` is None), and whose
    revision has not moved — exactly what the journal's own loop would
    write, which the shared contract checks. Leave it out, and the journal
    reads the candidates and writes as for a claim.
    """

    def scan(self, candidates: Any, *, name: str | None, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str,
             after: tuple[str, ...] = ()) -> Iterator[list[Entry]]:
        """The candidates, IN THEIR ORDER, page by page — each with its
        revision, its rows on `nodes`, and when its `scheduled` rows are due.

        A PRE-FILTER, NEVER A DECISION: the driver MAY leave out a candidate
        that holds a row for `name` — other than a `scheduled` row due by
        `now` — or one of whose `parents` has no `NODE_SATISFYING` row
        (`parents` is empty when the node joins in a way a pre-filter cannot
        know), or one holding a row, of any status, for a node of `after` —
        the nodes after `name`, whose start means the subject has moved past
        it. It must not leave out anything else; with `name` None, nothing
        for a row it holds. A driver that pre-filters nothing is correct,
        only slower."""

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
        before `older_than` — of subjects whose policy is in `only` when
        given, and not in `exclude` (a subject without policy is never in
        either) — and, when `version` is given, of subjects pinned to it or
        to none."""

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        """Record each subject's policy in the registry (the revisions),
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
        the journal guarantees no two land on one name) — the arrivals
        waiting in those nodes deleted and renamed alike — pin the subject
        to `version`, overwriting, and raise its revision."""

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        """`{subject: policy}` for the subjects that have one."""

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """The archived rows of one subject, oldest archive first (ties by
        node): `node, status, started_at, finished_at, lease, archived_at,
        reason`."""

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        """`{subject: rows of `name` archived with `reason`}`, subjects
        without any left out."""

    def prune_history(self, before: str, keep: tuple[str, ...]) -> int:
        """Delete the history rows archived before `before` whose reason is
        not in `keep`. Return the count deleted."""

    def note(self, subjects: list[Any], name: str, *, status: str, reason: str,
             now: str, ref: str | None) -> int:
        """Append to each subject's history a row `node=name`, `status`,
        `reason`, started, finished and archived at `now`, `ref` in `lease`.
        Return the count written."""

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None, now: str,
               merge: str, position: str, urgent: bool,
               refs: str | None = None) -> dict[Any, str]:
        """ATOMICALLY per subject, the arrival of `name`: when none waits,
        store one — `ref`, place and first arrival at `now`, `urgent`; when
        one waits, merge — `ref` replaced when `merge` is `Merge.LAST`, place
        moved to `now` when `position` is `Position.LAST`, `urgent` kept once
        set; `Merge.SET` replaces `refs` as given. Return
        `{subject: Outcome.QUEUED | Outcome.MERGED}`."""

    def arrivals(self, subjects: list[Any], name: str) -> dict[Any, Arrival]:
        """`{subject: Arrival}` of the subjects waiting in `name`."""

    def enter(self, name: str, entries: list[tuple[Any, int]], *,
              archive: tuple[str, ...], now: str) -> list[Any]:
        """ATOMICALLY, for each `(subject, revision)` whose revision is still
        `revision`, that has an arrival waiting in `name`, and none of whose
        rows of `archive` is `running` or `scheduled`: archive with reason
        `arrival` and delete its rows of `archive`, raising its revision as
        `forget` does; append to its history the arrival — `node=name`,
        `status=Outcome.ENTERED`, started at its first arrival, finished and
        archived at `now`, its `ref` in `lease`, reason `lane` — and delete
        it; insert a `done` row for `name`, started at the first arrival and
        finished at `now`. Return the subjects that entered."""

    def queued(self, name: str) -> int:
        """How many arrivals wait in `name`."""

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        """`{subject: latest archived_at}` of the history rows of `name` —
        with `reason` when given, any reason otherwise; subjects without any
        left out."""

    def now(self) -> str:
        """The storage's clock, in the journal's format (`utc_now`): ONE
        source of time for every process writing to the same storage."""

    def guard(self, keys: list[str]) -> AbstractContextManager[None]:
        """SERIALISE writers on `keys` — sorted — from entering the block
        until the caller's transaction ends (for a storage without
        transactions, until the block ends). What a claim under a rate or
        concurrency limit reads and writes inside it, no other claim on the
        same keys can interleave."""

    def limits(self, keys: list[str]) -> dict[str, float]:
        """`{key: value}` of the stored limiter state, keys never set left out."""

    def set_limits(self, values: dict[str, float]) -> None:
        """Store limiter state, creating the keys as needed."""

    def running(self, name: str, policies: tuple[str | None, ...] | None) -> int:
        """How many rows of `name` are RUNNING — of subjects whose policy is
        in `policies` (None in it stands for "no policy"), or of all
        subjects when `policies` is None."""

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
                 rng: random.Random | None = None,
                 mergers: dict[str, Callable[[tuple[str, ...], str | None],
                                             Any]] | None = None) -> None:
        """`dag` is a tuple of nodes, or a `Graph` whose policies change some
        settings per policy. `clock` defaults to the driver's
        (`JournalDriver.now`): workers on several machines then share one
        time. `rng` spreads retry jitter. `mergers` names the functions a
        lane may merge with (`Lane(merge="fn:<name>")`)."""
        self.driver = driver
        self.graph = dag if isinstance(dag, Graph) else None
        self.dag = dag.nodes if isinstance(dag, Graph) else dag
        #: SUBJECTS ARE PINNED TO THE GRAPH THEY STARTED ON: a journal on a
        #: `Graph` takes only subjects pinned to it, or not yet pinned, and
        #: pins them on their first write — once, never again. It is the whole
        #: `Document.identity`, so two workflows sharing a version string stay
        #: strangers. None for a tuple of nodes.
        self.version = dag.document.identity if isinstance(dag, Graph) else None
        # AN INJECTED CLOCK IS READ INTO THE ONE FORMAT (`timing.stamp`): in
        # another zone or layout, its times would sort wrong as text. The
        # driver's own clock already writes that format.
        self._clock = (lambda: stamp(clock())) if clock is not None else driver.now
        self._rng = rng or random.Random()
        #: THE MERGE FUNCTIONS A LANE MAY NAME. The graph stays data — it
        #: holds `fn:<name>`, never the code — and the name is resolved here,
        #: the way a named guard is resolved in a statechart.
        self._mergers = dict(mergers or {})

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
        if n.lane is not None:
            raise ValueError(
                f"node {name!r} is a lane: subjects `arrive` in it and `settle` lets "
                f"them through, it is never claimed")
        limit = int(limit)
        token = secrets.token_hex(16)
        if limit <= 0:
            return Lease([], token)
        after = tuple(sorted(descendants(name, self.dag)))
        parents = n.parents if require_parents and not n.custom_join else ()
        now = self._clock()
        chosen: list[tuple[Any, int]] = []
        policy_of: dict[Any, str | None] = {}
        seen: set[Any] = set()
        # A GROUP IS GATHERED BEFORE IT IS JUDGED: the claim must read enough
        # candidates to know whether one is complete, so `limit` alone is not
        # how far it reads.
        enough = limit if n.group is None else max(limit, n.group.size)
        ready: list[_Ready] = []
        # A GROUPING CLAIM READS AND WRITES UNDER THE GUARD, taken before it
        # reads anything. Two claimers would otherwise each decide on a state
        # the other was about to change, and each win a piece of one group:
        # a bag of two and a bag of one, neither of them a group, and no row
        # looking wrong. Guarding the NODE rather than the key keeps it to one
        # lock, held over a read a grouping node makes rarely and in bulk.
        def gather() -> list[tuple[Any, int]]:
            """Read candidates until there is enough to decide."""
            for page in self.driver.scan(candidates, name=name,
                                         nodes=(name, *n.parents, *after),
                                         parents=parents, page=max(enough, PAGE),
                                         now=now, after=after):
                for e in page:
                    # A SUBJECT LISTED TWICE COUNTS ONCE: it would otherwise
                    # take a place in the limit and be refused by the write.
                    if e.subject in seen or not self._mine(e):
                        continue
                    seen.add(e.subject)
                    if not self._takable(n, after, _due_away(name, e, now),
                                         require_parents):
                        continue
                    if n.group is not None:
                        ready.append(_Ready(e.subject, e.revision, e.key,
                                            _ready_at(n, e), e.policy))
                        continue
                    chosen.append((e.subject, e.revision))
                    policy_of[e.subject] = e.policy
                    if len(chosen) >= limit:
                        return chosen
                if n.group is None and len(chosen) >= limit:
                    return chosen
            if n.group is None:
                return chosen
            picked = self._group(n, ready, now)
            policy_of.update({m.subject: m.policy for m in picked})
            return [(m.subject, m.revision) for m in picked]

        def write(group: list[tuple[Any, int]]) -> list[Any]:
            if not group:
                return []
            if self._limited(n):
                return self._within_limits(
                    n, group, policy_of, now,
                    lambda part: self.driver.insert_if_unchanged(
                        name, part, status=Status.RUNNING, now=now, lease=token))
            return self.driver.insert_if_unchanged(
                name, group, status=Status.RUNNING, now=now, lease=token)

        # ONE WRITE PER CLAIM. A storage that locks while writing takes its
        # locks in one go, in one order, and two claimers cannot hold each
        # other. The price: a subject refused by the write is not replaced —
        # under contention a claim may take fewer than `limit`, never a wrong
        # one.
        if n.group is None:
            taken = write(gather())
        else:
            with self.driver.guard([f"group|{name}"]):
                taken = write(gather())
        self._pin(taken)
        return Lease(taken, token)

    def _group(self, n: Node, ready: list[_Ready], now: str) -> list[_Ready]:
        """THE ONE GROUP THIS CLAIM MAY TAKE, or nothing.

        Candidates keep the order the application gave them, so the group
        that goes is the one whose members have waited longest. A group goes
        when it is FULL, or when its oldest member has been ready longer than
        `max_wait` — an incomplete group then goes as it is, and the worker
        sees how many it really got.

        Nothing is written while a group fills: a short group simply is not
        claimable, so there is no half-gathered state to repair after a
        crash. The price is that the claim reads its candidates again next
        time, which is the same price every claim already pays.
        """
        group = n.group
        assert group is not None
        gathered: dict[Any, list[_Ready]] = {}
        for member in ready:
            if group.per_key and member.key is None:
                # NO KEY IS NOT ONE KEY: grouping every keyless candidate
                # together would hand out groups nobody declared.
                raise ValueError(
                    f"node {n.name!r} groups by key, and a candidate carries none: "
                    f"name the key column `grampy_key` in SQL, pass Keyed(subject, "
                    f"key) otherwise, or give the adapter a group_of")
            gathered.setdefault(member.key if group.per_key else None, []).append(member)
        late = (shift(now, -seconds(group.max_wait))
                if group.max_wait is not None else None)
        for members in gathered.values():
            if len(members) >= group.size:
                return members[:group.size]
            # AN INCOMPLETE GROUP GOES ONLY WHEN IT HAS WAITED. `ready_at` is
            # None for a node with no parents: no clock, so no way to be late.
            oldest = [m.ready_at for m in members if m.ready_at is not None]
            if late is not None and oldest and min(oldest) <= late:
                return members
        return []

    def _limited(self, n: Node) -> bool:
        return bool(n.rate) or n.concurrency is not None or any(
            v.rate or v.concurrency is not None for _, v in self._variants(n.name))

    def _within_limits(self, n: Node, chosen: list[tuple[Any, int]],
                       policy_of: dict[Any, str | None], now: str,
                       write: Callable[[list[tuple[Any, int]]], list[Any]]) -> list[Any]:
        """RATE AND CONCURRENCY, decided here, kept by the storage's guard.

        Candidates are grouped by budget — one for the node, or one per
        policy with `per=Per.POLICY`. Under the guard of every budget's keys,
        each group is cut to what `concurrency` leaves free and what the rate
        bands let through (`timing.admit`), written by `write` — a claim, or
        a lane letting subjects in — and the bands advance by what was
        actually written."""
        groups: dict[str | None, list[tuple[Any, int]]] = {}
        for subject, revision in chosen:
            budget = policy_of.get(subject) if n.per == Per.POLICY else None
            groups.setdefault(budget, []).append((subject, revision))
        keys: dict[str | None, tuple[Node, list[str], str]] = {}
        for budget in groups:
            seen_by = self.settings(n.name, budget) if n.per == Per.POLICY else n
            prefix = f"{n.name}|{budget if budget is not None else '*'}"
            keys[budget] = (seen_by, [f"rate|{prefix}|{i}" for i in range(len(seen_by.rate))],
                            f"running|{prefix}")
        guarded = sorted({k for _, bands, lock in keys.values() for k in (*bands, lock)})
        instant = datetime.fromisoformat(now).timestamp()
        taken: list[Any] = []
        with self.driver.guard(guarded):
            stored = self.driver.limits([k for _, bands, _ in keys.values() for k in bands])
            advanced: dict[str, float] = {}
            for budget, group in sorted(groups.items(), key=lambda g: str(g[0])):
                seen_by, band_keys, _ = keys[budget]
                allowed = len(group)
                if seen_by.concurrency is not None:
                    busy = self.driver.running(
                        n.name, None if n.per != Per.POLICY else (budget,))
                    allowed = min(allowed, max(0, seen_by.concurrency - busy))
                if seen_by.rate:
                    allowed, _ = admit(seen_by.rate, [stored.get(k) for k in band_keys],
                                       instant, allowed)
                written = write(group[:allowed]) if allowed else []
                taken += written
                if seen_by.rate:
                    _, tats = admit(seen_by.rate, [stored.get(k) for k in band_keys],
                                    instant, len(written))
                    advanced.update(zip(band_keys, tats, strict=True))
            if advanced:
                self.driver.set_limits(advanced)
        return taken

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
                 status: str = Status.DONE, branch: str | None = None) -> int:
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
        if status not in NODE_CONCLUDED or status == Status.OMITTED:
            raise ValueError(
                f"unknown conclusion status: {status!r} — expected "
                f"{Status.DONE}, {Status.SKIPPED} or {Status.FAILED}")
        omit: tuple[str, ...] = ()
        subjects = _unique(subjects)
        now = self._clock()
        touched = 0
        retry_of = self._per_policy(name, "retry", subjects)
        if any(r is not None for r in retry_of.values()) and status == Status.FAILED \
                and branch is None:
            # FIRST, THE RETRIES — each subject under its policy's policy: under
            # the limit, the failure is archived and the node scheduled again,
            # later for later attempts.
            tries = self.driver.archived(subjects, name, Reason.RETRY)
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
            passes = self.driver.archived(subjects, n.loop.to, Reason.LOOP)
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
        if n.choice and status != Status.FAILED:
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
        return self.conclude(name, subjects, token=token, status=Status.FAILED,
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
        # A DRIVER MAY SKIP IN ONE WRITE (`skip_where`, optional): the rule
        # for a plain join is SQL-shaped — no row, every parent satisfying —
        # and the contract proves the fast path equal to the loop below. A
        # custom join (`on`, `need`) always takes the loop.
        fast = getattr(self.driver, "skip_where", None)
        if fast is not None and not n.custom_join:
            written = fast(name, candidates, parents=n.parents, now=now, version=self.version)
        else:
            chosen: dict[tuple[Any, int], None] = {}
            for page in self.driver.scan(candidates, name=name,
                                         nodes=(name, *n.parents),
                                         parents=() if n.custom_join else n.parents,
                                         page=PAGE, now=now):
                chosen.update(dict.fromkeys(
                    (e.subject, e.revision) for e in page
                    if self._mine(e) and name not in e.rows
                    and joined(name, self.dag, e.rows)))
            # One write, as for a claim.
            written = self.driver.insert_if_unchanged(
                name, list(chosen), status=Status.SKIPPED, now=now,
                lease=None) if chosen else []
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

    def release(self, name: str, older_than: str | datetime) -> int:
        """Give back the leases a dead worker has held for too long."""
        node(name, self.dag)
        return self.driver.release(name, older_than=stamp(older_than), now=self._clock(),
                                   version=self.version)

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        """What is recorded for ONE subject — exactly what `dag.claimable` reads.

        A JOURNAL ONLY SPEAKS ABOUT ITS OWN SUBJECTS: one pinned to another
        graph reads empty, as it claims empty. `migrate` deliberately looks
        past that, through the driver, since crossing is its whole job."""
        if self.version is not None:
            pinned = self.driver.versions([subject]).get(subject)
            if pinned is not None and pinned != self.version:
                return {}
        return self.driver.progress(subject)

    def expire(self) -> dict[str, int]:
        """Release, on every node declaring a `lease`, the rows held longer
        than it allows. Return `{node: count}` for the nodes that released
        something. Meant to be called on a schedule — a janitor — by the
        application: grampy runs no thread of its own."""
        now = self._clock()
        released: dict[str, int] = {}
        for n in self.dag:
            special = {policy: variant.lease for policy, variant in self._variants(n.name)
                       if variant.lease != n.lease}
            count = 0
            if n.lease is not None:
                count += self.driver.release(
                    n.name, older_than=shift(now, -seconds(n.lease)), now=now,
                    exclude=tuple(sorted(special)), version=self.version)
            for policy, lease in special.items():
                if lease is not None:
                    count += self.driver.release(
                        n.name, older_than=shift(now, -seconds(lease)), now=now,
                        only=(policy,), version=self.version)
            if count:
                released[n.name] = count
        return released

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        """Give these subjects a POLICY — their source. The graph's settings
        for that policy (retries, leases, timeouts, graces) then apply to
        them; the structure of the workflow stays the same for all."""
        if not subjects:
            return 0
        count = self.driver.enroll(_unique(subjects), policy)
        self._pin(_unique(subjects))
        return count

    def pinned(self, subject: Any) -> str | None:
        """The graph the subject is pinned to — a `Document.identity` — or
        None before its first write.

        IT IS WRITTEN ONCE. The first write pins the subject, and no later
        one moves it: only `migrate` changes it, on purpose."""
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
        if source.document.identity == self.version:
            raise ValueError(f"the source is already {self.version!r}")
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
            raise ValueError(f"source nodes with nowhere to go on {self.version!r}: "
                             f"{missing} — map them to a node or to None")
        landed = [t for t in full.values() if t is not None]
        if len(landed) != len(set(landed)):
            raise ValueError("two source nodes are mapped onto the same node")

        subjects = _unique(subjects)
        pinned = self.driver.versions(subjects)
        problems: dict[Any, str] = {}
        plans: dict[Any, tuple[dict[str, str], tuple[str, ...]]] = {}
        for subject in subjects:
            if pinned.get(subject, source.document.identity) != source.document.identity:
                problems[subject] = f"pinned to {pinned[subject]!r}"
                continue
            progress = self.driver.progress(subject)
            drop = tuple(sorted(name for name in progress if full.get(name, name) is None))
            held = [name for name in drop if progress[name] in (Status.RUNNING, Status.SCHEDULED)]
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
                problems[subject] = f"rows on {stray}, which {self.version!r} lacks"
                continue
            unjoined = sorted(name for name in moved if not joined(name, self.dag, moved))
            if unjoined:
                problems[subject] = (f"{unjoined} could not have run on "
                                     f"{self.version!r}: their parents are not joined")
                continue
            plans[subject] = (rename, drop)
        if problems:
            raise MigrationError(problems)
        # Every renamed or dropped node is passed, rows or not: an arrival
        # waiting in a lane follows its node too.
        every_rename = {name: target for name, target in full.items()
                        if target is not None and target != name}
        every_drop = tuple(sorted(name for name, target in full.items() if target is None))
        now = self._clock()
        target_version: str = self.version
        for subject in plans:
            self.driver.rewrite(subject, rename=every_rename, drop=every_drop,
                                version=target_version, now=now)
        return len(plans)

    def policy(self, subject: Any) -> str | None:
        """The subject's policy, None when it has none."""
        return self.driver.policies([subject]).get(subject)

    def settings(self, name: str, policy: str | None) -> Node:
        """The node as a subject of `policy` sees it."""
        node(name, self.dag)
        if self.graph is None:
            return node(name, self.dag)
        return node(name, self.graph.variant(policy))

    def _variants(self, name: str) -> list[tuple[str, Node]]:
        if self.graph is None:
            return []
        return [(policy, node(name, self.graph.variant(policy)))
                for policy in self.graph.policies]

    def _per_policy(self, name: str, setting: str, subjects: list[Any]) -> dict[Any, Any]:
        """`{subject: value of `setting` on `name` for its policy}`."""
        default = getattr(node(name, self.dag), setting)
        if self.graph is None or not any(
                name in overrides and setting in overrides[name]
                for overrides in self.graph.policies.values()):
            return dict.fromkeys(subjects, default)
        policies = self.driver.policies(subjects)
        return {s: getattr(self.settings(name, policies.get(s)), setting) for s in subjects}

    def signal(self, subjects: list[Any], event: str, ref: str | None = None) -> int:
        """Record that `event` happened for these subjects — DURABLY, before
        any wait for it may have begun: a wait settles on a signal received
        since it last went back (a loop, a forget), however early."""
        if not subjects:
            return 0
        return self.driver.note(_unique(subjects), event, status=Outcome.RECEIVED,
                                reason=Reason.SIGNAL, now=self._clock(), ref=ref)

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None = None,
               urgent: bool = False) -> dict[str, int]:
        """THESE SUBJECTS CAME BACK — a new version of each, `ref` naming it
        (opaque, optional) — and wait in the lane `name` to run again.

        An arrival for a subject already waiting is merged, as the lane says
        (`Lane.merge`, `Lane.position`); the merge is noted in the history.
        With `while_running=WhileRunning.SKIP`, an arrival for a subject whose pass is
        still running is dropped, and noted. `urgent` lets the arrival through
        at the next `settle` whatever its cooldown or delay — never over a
        running pass.

        Nothing runs here: `settle` lets due arrivals in. Return
        `{Outcome.QUEUED: n, Outcome.MERGED: n, Outcome.SKIPPED: n}` — keys that
        are also the strings `"queued"`, `"merged"`, `"skipped"`."""
        n = node(name, self.dag)
        if n.lane is None:
            raise ValueError(f"node {name!r} is not a lane: nothing arrives in it")
        out: dict[str, int] = {Outcome.QUEUED: 0, Outcome.MERGED: 0, Outcome.SKIPPED: 0}
        subjects = _unique(subjects)
        if not subjects:
            return out
        now = self._clock()
        lane_of = self._per_policy(name, "lane", subjects)
        after = (name, *sorted(descendants(name, self.dag)))
        groups: dict[Lane, list[Any]] = {}
        for subject in subjects:
            lane = lane_of[subject]
            if lane.while_running == WhileRunning.SKIP and any(
                    self.driver.progress(subject).get(x) in (Status.RUNNING, Status.SCHEDULED)
                    for x in after):
                self.driver.note([subject], name, status=Outcome.SKIPPED, reason=Reason.LANE,
                                 now=now, ref=ref)
                out[Outcome.SKIPPED] += 1
                continue
            groups.setdefault(lane, []).append(subject)
        for lane, group in groups.items():
            if lane.keeps_every_ref:
                for subject in group:
                    self._keep_every_ref(name, subject, lane, ref, now, urgent, out)
                continue
            outcome = self.driver.arrive(name, group, ref=ref, now=now, merge=lane.merge,
                                         position=lane.position, urgent=urgent)
            merged = [s for s in group if outcome.get(s) == Outcome.MERGED]
            if merged:
                self.driver.note(merged, name, status=Outcome.MERGED, reason=Reason.LANE, now=now,
                                 ref=ref)
            out[Outcome.MERGED] += len(merged)
            out[Outcome.QUEUED] += sum(1 for s in group if outcome.get(s) == Outcome.QUEUED)
        self._pin(subjects)
        return out

    def _keep_every_ref(self, name: str, subject: Any, lane: Lane, ref: str | None,
                        now: str, urgent: bool, out: dict[str, int]) -> None:
        """MERGE BY READING WHAT WAITS, THEN WRITING — under the driver's
        guard, on this subject's place in this lane.

        `first`, `last` and `dedupe` decide without looking, so one atomic
        write does them. Keeping every ref, or asking a function, cannot:
        two workers arriving at once would each start from the state before
        the other, and one would overwrite the other's version. The guard is
        the same one rate and concurrency take (`arrival|<node>|<subject>`).
        """
        merger = self._merger(lane)
        with self.driver.guard([f"arrival|{name}|{subject}"]):
            current = self.driver.arrivals([subject], name).get(subject)
            kept = _decode_refs(current.refs) if current is not None else ()
            wanted = tuple(merger(kept, ref))
            dropped = [r for r in kept if r not in wanted]
            outcome = self.driver.arrive(
                name, [subject], ref=wanted[-1] if wanted else None, now=now,
                merge=Merge.SET, position=lane.position, urgent=urgent,
                refs=_encode_refs(wanted))
        if dropped:
            # A REF LET GO IS STILL SAID: past `max_size`, or refused by the
            # function, it leaves a trace rather than vanishing.
            for gone in dropped:
                self.driver.note([subject], name, status=Outcome.DROPPED, reason=Reason.LANE,
                                 now=now, ref=gone)
        if outcome.get(subject) == Outcome.MERGED:
            self.driver.note([subject], name, status=Outcome.MERGED, reason=Reason.LANE,
                             now=now, ref=ref)
            out[Outcome.MERGED] += 1
        else:
            out[Outcome.QUEUED] += 1

    def _merger(self, lane: Lane) -> Callable[[tuple[str, ...], str | None], Any]:
        """The function this lane merges with: `all`'s, or the application's
        under the name the lane gives."""
        named = lane.merger
        if named is None:
            size = lane.max_size
            return lambda kept, arriving: (
                (*kept, arriving) if arriving is not None else kept)[-size:]
        try:
            return self._mergers[named]
        except KeyError:
            raise ValueError(
                f"lane merge {lane.merge!r} names a function the journal was not "
                f"given — pass it as NodeJournal(..., mergers={{{named!r}: fn}}); "
                f"it has {sorted(self._mergers)}") from None

    def arrival(self, subject: Any, name: str) -> Arrival | None:
        """What waits for this subject in the lane `name`, if anything."""
        node(name, self.dag)
        return self.driver.arrivals([subject], name).get(subject)

    def refs(self, subject: Any, name: str) -> tuple[str, ...]:
        """EVERY VERSION WAITING for this subject in the lane `name`, oldest
        first — what a lane keeping them all has gathered.

        A lane keeping one version gives that one; a subject with nothing
        waiting gives nothing."""
        arrival = self.arrival(subject, name)
        if arrival is None:
            return ()
        kept = _decode_refs(arrival.refs)
        return kept if kept else ((arrival.ref,) if arrival.ref is not None else ())

    def settle(self, candidates: Any) -> dict[str, dict[str, int]]:
        """CONCLUDE WHAT NO WORKER DOES, on the candidates, for every node:

            wait      `done` when a signal was received since the node last
                      went back; `failed` once `timeout` has passed since
                      its parents concluded
            lane      a due arrival enters: the previous pass is archived and
                      the lane is `done` (`Lane`); reported as `entered`
            grace     an optional node still untaken `grace` after its
                      parents concluded is `skipped`

        Time runs from the LATEST accepted parent's conclusion; a node
        without parents has no clock and never times out. Return
        `{node: {status: count}}` for what was written. Meant for the
        application's janitor, like `expire`."""
        now = self._clock()
        out: dict[str, dict[str, int]] = {}
        for n in self.dag:
            if n.lane is not None:
                entered = self._let_in(n, candidates, now)
                if entered:
                    out.setdefault(n.name, {})[Outcome.ENTERED] = entered
                continue
            if n.wait is None and n.grace is None and all(
                    v.grace is None for _, v in self._variants(n.name)):
                continue
            after = tuple(sorted(descendants(n.name, self.dag)))
            entries = [e for page in self.driver.scan(
                           candidates, name=n.name, nodes=(n.name, *n.parents, *after),
                           parents=() if n.custom_join else n.parents, page=PAGE, now=now,
                           after=after)
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
                heard = self.driver.latest(subjects, n.wait, Reason.SIGNAL)
                went_back = self.driver.latest(subjects, n.name, None)
            for e in ready:
                since = _joined_since(n, e)
                seen_by = self.settings(n.name, e.policy)
                if n.wait is not None:
                    at = heard.get(e.subject)
                    if at is not None and at >= went_back.get(e.subject, ""):
                        decided.setdefault(Status.DONE, []).append((e.subject, e.revision))
                        continue
                    if (seen_by.timeout is not None and since is not None
                            and shift(since, seconds(seen_by.timeout)) <= now):
                        decided.setdefault(Status.FAILED, []).append((e.subject, e.revision))
                elif (since is not None and seen_by.grace is not None
                      and shift(since, seconds(seen_by.grace)) <= now):
                    decided.setdefault(Status.SKIPPED, []).append((e.subject, e.revision))
            for status, chosen in decided.items():
                written = self.driver.insert_if_unchanged(n.name, chosen, status=status,
                                                          now=now, lease=None)
                self._pin(written)
                if written:
                    out.setdefault(n.name, {})[status] = len(written)
        return out

    def _let_in(self, n: Node, candidates: Any, now: str) -> int:
        """THE LANE'S DOOR. An arrival enters when the subject's parents are
        joined, nothing of its previous pass runs, and it is due: urgent, or
        past both its `delay` (from its place) and its `cooldown` (from the
        end of the previous pass) — or past `max_wait` from its first arrival
        whatever the rest. Due arrivals enter in the order of their places,
        within the lane's `rate`."""
        after = tuple(sorted(descendants(n.name, self.dag)))
        pass_nodes = (n.name, *after)
        entries: dict[Any, Entry] = {}
        order: dict[Any, int] = {}
        # No pre-filter on the lane's own row: the previous pass holds one.
        for page in self.driver.scan(candidates, name=None, nodes=(*pass_nodes, *n.parents),
                                     parents=(), page=PAGE, now=now):
            for e in page:
                if e.subject not in entries and self._mine(e):
                    order[e.subject] = len(order)
                    entries[e.subject] = e
        if not entries:
            return 0
        waiting = self.driver.arrivals(list(entries), n.name)
        due: list[tuple[str, int, Any]] = []
        for subject, arrival in waiting.items():
            e = entries[subject]
            if not joined(n.name, self.dag, e.rows):
                continue
            if any(e.rows.get(x) in (Status.RUNNING, Status.SCHEDULED) for x in pass_nodes):
                continue
            lane = self.settings(n.name, e.policy).lane
            assert lane is not None
            if not arrival.urgent:
                ready = [arrival.place]
                if lane.delay is not None:
                    ready.append(shift(arrival.place, seconds(lane.delay)))
                ended = [e.finished[x] for x in pass_nodes if x in e.finished]
                if lane.cooldown is not None and ended:
                    ready.append(shift(max(ended), seconds(lane.cooldown)))
                late = (lane.max_wait is not None
                        and shift(arrival.arrived_at, seconds(lane.max_wait)) <= now)
                if max(ready) > now and not late:
                    continue
            due.append((arrival.place, order[subject], subject))
        chosen = [(subject, entries[subject].revision) for _, _, subject in sorted(due)]
        if not chosen:
            return 0

        def write(group: list[tuple[Any, int]]) -> list[Any]:
            return self.driver.enter(n.name, group, archive=pass_nodes, now=now)

        if self._limited(n):
            return len(self._within_limits(
                n, chosen, {s: entries[s].policy for s, _ in chosen}, now, write))
        return len(write(chosen))

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """Every row taken away from ONE subject — by `forget`, `release` or
        a loop — oldest first, each with `archived_at` and `reason`."""
        return self.driver.history(subject)

    #: THE HISTORY ROWS A BOUND COUNTS — a retry limit, a loop's `max`.
    #: Pruning never touches them: they are the counters.
    COUNTED = (Reason.RETRY, Reason.LOOP)

    def prune_history(self, before: str | datetime) -> int:
        """KEEP THE HISTORY FROM GROWING FOREVER: delete what was archived
        before `before`, except the rows a bound counts (`COUNTED`).

        Forgets, releases, lane notes and signals go; the `retry` and `loop`
        rows stay, whatever their age. A subject pruned, then brought back —
        a replay, a requeue, a late result — keeps the retries and the loop
        passes it had used: a limit cannot be reset by a clean-up. Meant for
        the application's janitor, like `expire`. Return the count deleted.
        """
        return self.driver.prune_history(stamp(before), keep=self.COUNTED)

    def passes(self, subject: Any, name: str) -> int:
        """How many times a declared loop sent this subject back through
        `name` — what `Loop.max` bounds."""
        node(name, self.dag)
        return self.driver.archived([subject], name, Reason.LOOP).get(subject, 0)

    def retries(self, subject: Any, name: str) -> int:
        """How many times a failure of `name` was retried for this subject."""
        node(name, self.dag)
        return self.driver.archived([subject], name, Reason.RETRY).get(subject, 0)

    def stages(self, subjects: list[Any], *,
               at: str | datetime) -> dict[str, list[list[Any]]]:
        """What a BATCH went through — `{subject: [[node, end, seconds, status], …]}`.

        ONE READ FOR THE BATCH, not one per subject. ONLY NODES THAT WORKED:
        a `skipped` row never started. A node STILL RUNNING ends `at`.
        """
        return self.driver.stages(list(subjects), at=stamp(at))

    def counts(self, name: str) -> dict[str, int]:
        """How many subjects stand where, for this node."""
        n = node(name, self.dag)
        by_status = self.driver.status_counts(name)
        out: dict[str, int] = {status: int(by_status.get(status, 0))
                               for status in (Status.RUNNING, Status.SCHEDULED, *NODE_CONCLUDED)}
        if n.lane is not None:
            out[Outcome.WAITING] = int(self.driver.queued(name))
        return out

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
    if entry.rows.get(name) == Status.SCHEDULED and entry.due.get(name, now) <= now:
        return {k: v for k, v in entry.rows.items() if k != name}
    return entry.rows


def _unique(subjects: Iterable[Any]) -> list[Any]:
    """The subjects once each, in their order."""
    return list(dict.fromkeys(subjects))
