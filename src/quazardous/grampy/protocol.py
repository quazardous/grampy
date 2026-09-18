"""WHAT A JOURNAL AND ITS DRIVER EXCHANGE — the data types, and the driver
protocol split into capabilities: the core every journal needs, and one per
feature a graph may use (versions, limits, lanes) or an application may read
(reading). `NodeJournal` checks, when it is built, that its driver offers
what its graph needs. See docs/writing-a-driver.md.
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager
from datetime import datetime, timezone
from typing import Any, NamedTuple, Protocol

from .dag import (
    Node,
)
from .graph import Graph


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
    #: WHAT THE CANDIDATES SAID TO GROUP THIS SUBJECT BY — their column named
    #: `grampy_key`, or the key of a `Keyed`. Compared, never read; None when
    #: the candidates named none.
    key: str | None = None

class Page(list):
    """WHAT `scan` YIELDS: a page of entries — a plain list does as well —
    and, for a driver that reads its clock in the page query
    (`scan_reads_clock`), the storage's time as that page was read."""

    def __init__(self, entries: Iterable[Entry] = (), now: str | None = None) -> None:
        super().__init__(entries)
        self.now = now

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

class CoreDriver(Protocol):
    """WHAT EVERY JOURNAL NEEDS: node rows, revisions, the history, the
    clock and the guard. Every method works on node rows:
    `(subject, node) → status, started_at, finished_at`, one row per pair,
    and on one REVISION per subject — 0 for a subject never forgotten.

    A SUBJECT IS THE APPLICATION'S ID: unique, stable, an `int` or a `str`,
    stored and returned exactly as given — never converted, built or split.

    A driver NEVER validates against the graph, never decides what is
    claimable — the journal does both — and never commits.

    The history is core, not an extra: retry limits and loop bounds count
    its rows (`archived`, `latest`). So is `guard`: groups and lanes
    serialise on it, not only limits.
    """

    def scan(self, candidates: Any, *, name: str | None, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str | None,
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

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        """`{subject: policy}` for the subjects that have one."""

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """The archived rows of one subject, oldest archive first (ties by
        node): `node, status, started_at, finished_at, lease, archived_at,
        reason`."""

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        """`{subject: rows of `name` archived with `reason`}`, subjects
        without any left out."""

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        """`{subject: latest archived_at}` of the history rows of `name` —
        with `reason` when given, any reason otherwise; subjects without any
        left out."""

    def note(self, subjects: list[Any], name: str, *, status: str, reason: str,
             now: str, ref: str | None) -> int:
        """Append to each subject's history a row `node=name`, `status`,
        `reason`, started, finished and archived at `now`, `ref` in `lease`.
        Return the count written."""

    def prune_history(self, before: str, keep: tuple[str, ...]) -> int:
        """Delete the history rows archived before `before` whose reason is
        not in `keep`. Return the count deleted."""

    def now(self) -> str:
        """The storage's clock, in the journal's format (`utc_now`): ONE
        source of time for every process writing to the same storage."""

    def guard(self, keys: list[str]) -> AbstractContextManager[None]:
        """SERIALISE writers on `keys` — sorted — from entering the block
        until the caller's transaction ends (for a storage without
        transactions, until the block ends). What a claim under a rate or
        concurrency limit reads and writes inside it, no other claim on the
        same keys can interleave."""

    def progress(self, subject: Any) -> dict[str, str]:
        """`{node: status}` for one subject."""

class VersionDriver(Protocol):
    """NEEDED AS SOON AS THE JOURNAL IS BUILT ON A `Graph`: every write
    pins its subjects to the graph they started on, and a migration moves
    them to another."""

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

class LimitDriver(Protocol):
    """NEEDED BY A NODE WITH A `rate` OR A `concurrency`, in the graph or
    in one of its policies."""

    def limits(self, keys: list[str]) -> dict[str, float]:
        """`{key: value}` of the stored limiter state, keys never set left out."""

    def set_limits(self, values: dict[str, float]) -> None:
        """Store limiter state, creating the keys as needed."""

    def running(self, name: str, policies: tuple[str | None, ...] | None) -> int:
        """How many rows of `name` are RUNNING — of subjects whose policy is
        in `policies` (None in it stands for "no policy"), or of all
        subjects when `policies` is None."""

class LaneDriver(Protocol):
    """NEEDED BY A NODE WITH A `lane`, in the graph or in one of its
    policies."""

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

class ReadingDriver(Protocol):
    """READS FOR THE APPLICATION, never on a write path: `journal.counts`,
    `journal.stages` and `journal.parents_concluded` need them, nothing
    else does."""

    def status_counts(self, name: str) -> dict[str, int]:
        """`{status: count}` for one node."""

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        """`{subject: [[node, ended, seconds, status], …]}`, skipped rows
        excluded, ordered by `(ended, node)`; a running row ends `at`."""

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> Any:
        """"Every parent has a satisfying row" — in the driver's terms."""

class JournalDriver(CoreDriver, VersionDriver, LimitDriver, LaneDriver, ReadingDriver,
                    Protocol):
    """THE WHOLE STORAGE a journal may need: every capability. A driver
    offers the core and the capabilities its graphs use; the journal says,
    when it is built, which one is missing (`MissingCapability`).

    OPTIONAL, `skip_where(name, candidates, *, parents, now, version,
    limit=None) -> subjects`: `skip` in one write, for a node joining its
    parents plainly. It writes a `skipped` row, started and finished at
    `now`, for every candidate with no row for `name`, a satisfying row for
    every parent, pinned to `version` or to none (any, when `version` is
    None), and whose revision has not moved — with `limit`, for the first
    `limit` such candidates in their order. Exactly what the journal's own
    loop would write, which the shared contract checks. `limit` is passed
    only when the caller sets one. Leave it out, and the journal reads the
    candidates and writes as for a claim.

    OPTIONAL, `scan_reads_clock = True`: the driver's `scan` accepts
    `now=None`, then uses the storage's own clock — the time `now()` would
    return — in the page query, and yields `Page`s carrying it. A journal on
    the driver's clock then reads the time with the first page instead of in
    a statement of its own.

    OPTIONAL, `node_times(name, *, waiting) -> {key: time}`: the earliest
    `started_at` of `name`'s `running` rows and of its `scheduled` rows (its
    next retry due), under those statuses as keys, and — when `waiting` —
    the earliest `arrived_at` of its lane's arrivals, under `"waiting"`; a
    key left out when nothing stands there. `journal.snapshot` reads it for
    its ages; without it, the ages are None and the counts remain.

    OPTIONAL, `progress_many(subjects) -> {subject: progress}` and
    `rewrite_many(subjects, *, rename, drop, version, now)`: `progress` and
    `rewrite` for many subjects at once — a subject without rows may be left
    out of the first. Leave them out, and the journal calls the single ones
    subject by subject: the same result, a round trip each.
    """

#: The capabilities a driver may offer, by name: the core, and one per
#: feature a graph may use.
CAPABILITIES: dict[str, type] = {
    "core": CoreDriver, "versions": VersionDriver, "limits": LimitDriver,
    "lanes": LaneDriver, "reading": ReadingDriver}

def capability_methods(capability: str) -> tuple[str, ...]:
    """The methods a driver offers when it offers `capability`."""
    protocol = CAPABILITIES[capability]
    return tuple(sorted(n for n, v in vars(protocol).items()
                        if callable(v) and not n.startswith("_")))

class MissingCapability(TypeError):
    """THE DRIVER LACKS A CAPABILITY this journal needs — raised when the
    journal is built (or, for `reading`, when a reading method is called),
    naming the capability, why it is needed and the methods missing."""

    def __init__(self, capability: str, why: str, missing: list[str]) -> None:
        self.capability = capability
        self.missing = missing
        super().__init__(
            f"the driver lacks the {capability!r} capability, needed by {why}: it has "
            f"no {', '.join(missing)} — see docs/writing-a-driver.md")

def _require(driver: Any, capability: str, why: str) -> None:
    missing = [m for m in capability_methods(capability)
               if not callable(getattr(driver, m, None))]
    if missing:
        raise MissingCapability(capability, why, missing)

def needed_capabilities(dag: tuple[Node, ...] | Graph) -> dict[str, str]:
    """`{capability: why}` for what a journal on `dag` needs of its driver
    — `reading` aside, needed only by the reading methods. A policy's
    variant of a node counts as much as the node itself."""
    needs = {"core": "every journal"}
    variants: list[tuple[Node, ...]] = [tuple(dag)] if not isinstance(dag, Graph) else [
        dag.nodes, *(dag.variant(p) for p in sorted(dag.policies))]
    if isinstance(dag, Graph):
        needs["versions"] = "a journal on a Graph (its subjects are pinned to it)"
    for nodes in variants:
        for n in nodes:
            if (n.rate or n.concurrency is not None) and "limits" not in needs:
                needs["limits"] = f"node {n.name!r}, which has a rate or a concurrency"
            if n.lane is not None and "lanes" not in needs:
                needs["lanes"] = f"node {n.name!r}, which is a lane"
    return needs
