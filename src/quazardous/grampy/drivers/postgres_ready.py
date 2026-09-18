"""A POSTGRESQL LAYOUT WITH A READY LIST — row per node, plus a hint.

    PostgresReadyDriver(execute, table, revisions, history, ready, graph=…,
                        subject="subject", limits=None, arrivals=None)

`PostgresDriver`'s tables, and one more: `ready`, the subject and `node`
as primary key, nothing else. An application picks this layout knowingly,
for what it gains and what it costs (`docs/drivers.md`).

────────────────────────────────────────────────────────────────────────
A HINT, NEVER A DECISION
────────────────────────────────────────────────────────────────────────

A pair `(subject, node)` in `ready` says only "this node MAY have become
claimable for this subject". The claim of a node with parents reads only
the candidates listed there — and then applies every pre-filter
`PostgresDriver` applies, before the journal judges each one as it always
does. The rule stays in the core.

What must hold is one direction only: A SUBJECT THAT CAN BE CLAIMED IS
LISTED. So every write that can make a node claimable lists it, in the same
transaction:

    a row concluded, adopted or skipped    its children
    an omitted row                         its children
    a row taken away (forget, release,     the node itself
      loop, migration, lane entry)
    a retry scheduled                      the node itself

and a claim, which writes a row for the node, strikes the pair out. Listing
too much costs a wasted read; listing too little would starve a subject, so
the driver always errs on the side of listing. The list stands for "its
parents concluded", so it is read only when the journal asks for that
pre-filter: a node without parents, a node with a custom join (`on`,
`need`) and a claim waiving its parents are claimed as `PostgresDriver`
claims them.

The driver reads the graph's SHAPE — who is whose child — and nothing else
of it. Give it the graph the journal is given.

────────────────────────────────────────────────────────────────────────
WHAT THE LAYOUT BUYS, WHAT IT COSTS
────────────────────────────────────────────────────────────────────────

A claim deep in the graph walks the candidates probing one small index
instead of testing each for a row and for its parents. The candidates
still set the order, so a claim still walks them in that order: the gain is
per candidate walked, and largest when few are ready. Every conclusion pays
an insert into `ready`, every claim a delete.

An application moving an existing table to this layout, or writing rows
itself, calls `refill()` once: it lists every pair the rows allow.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ..dag import Node
from ..graph import Graph
from ..names import Status
from .postgres import PostgresDriver


class PostgresReadyDriver(PostgresDriver):
    """`PostgresDriver`, with the claim of a node with parents narrowed to
    the pairs listed in `ready`."""

    def __init__(self, execute: Callable[[Any], Any], table: sa.Table,
                 revisions: sa.Table, history: sa.Table, ready: sa.Table, *,
                 graph: tuple[Node, ...] | Graph, subject: str = "subject",
                 limits: sa.Table | None = None,
                 arrivals: sa.Table | None = None) -> None:
        super().__init__(execute, table, revisions, history, subject=subject,
                         limits=limits, arrivals=arrivals)
        missing = [c for c in (subject, "node") if c not in ready.c]
        if missing:
            raise ValueError(f"table {ready.name!r} lacks the column(s) {missing} — "
                             f"it needs {subject!r} and node")
        self.ready = ready
        nodes = graph.nodes if isinstance(graph, Graph) else tuple(graph)
        self._parents = {n.name: tuple(n.parents) for n in nodes}
        self._children: dict[str, tuple[str, ...]] = {
            n.name: tuple(c.name for c in nodes if n.name in c.parents) for n in nodes}

    # -- the list ------------------------------------------------------------

    def _listed(self, names: Iterable[str]) -> list[str]:
        """The nodes worth listing: those with parents."""
        return sorted({n for n in names if self._parents.get(n)})

    def _list(self, subjects: Iterable[Any], names: Iterable[str]) -> None:
        names = self._listed(names)
        pairs = [(s, n) for s in sorted(set(subjects)) for n in names]
        if pairs:
            self._execute(self._insert_rows(self.ready, {self._key: [s for s, _ in pairs],
                                                         "node": [n for _, n in pairs]})
                          .on_conflict_do_nothing())

    def _children_of(self, names: Iterable[str]) -> list[str]:
        return [c for n in names for c in self._children.get(n, ())]

    def _strike(self, subjects: list[Any], name: str) -> None:
        if subjects:
            self._execute(sa.delete(self.ready).where(
                self.ready.c.node == name, self._in(self.ready.c[self._key], sorted(subjects))))

    def refill(self) -> int:
        """List every pair the node rows allow: each node with parents, for
        every subject holding a row of one of its parents and none of its
        own — or only a scheduled one. For a table this driver did not
        write. Return the count listed."""
        t, count = self.table, 0
        own = t.alias("own")
        for name, parents in sorted(self._parents.items()):
            if not parents:
                continue
            count += len(self._execute(
                postgresql.insert(self.ready).from_select(
                    [self._key, "node"],
                    sa.select(self._subject, sa.literal(name)).distinct()
                    .where(t.c.node.in_(list(parents)), ~sa.exists().where(
                        own.c[self._key] == self._subject, own.c.node == name,
                        own.c.status != Status.SCHEDULED)))
                .on_conflict_do_nothing()
                .returning(self.ready.c[self._key])).fetchall())
        return count

    # -- the claim reads the list -------------------------------------------

    def _narrow(self, query: Any, candidate: Any, name: str | None,
                parents: tuple[str, ...]) -> Any:
        # ONLY WHERE THE JOURNAL ASKS FOR THE PARENTS' PRE-FILTER: the list
        # stands for "parents concluded", so a claim that waives that test —
        # or a join the pre-filter cannot know — reads every candidate.
        if name is None or not parents:
            return query
        r = self.ready
        return query.where(sa.exists().where(r.c[self._key] == candidate, r.c.node == name))

    # -- every write keeps it listing enough ---------------------------------

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        written = super().insert_if_unchanged(name, entries, status=status, now=now,
                                              lease=lease)
        self._strike(written, name)
        if status != Status.RUNNING:
            self._list(written, self._children_of([name]))
        return written

    def skip_where(self, name: str, candidates: Any, *, parents: tuple[str, ...],
                   now: str, version: str | None) -> list[Any]:
        written = super().skip_where(name, candidates, parents=parents, now=now,
                                     version=version)
        self._strike(written, name)
        self._list(written, self._children_of([name]))
        return written

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        touched = super().conclude(name, subjects, status=status, now=now, lease=lease,
                                   omit=omit, reset=reset, reschedule=reschedule)
        if touched:
            # Listing the subjects asked rather than those concluded errs on
            # the side the list may err on.
            names = [*self._children_of([name, *omit]), *reset]
            if reschedule is not None:
                names.append(name)
            self._list(subjects, names)
        return touched

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        count = super().adopt(name, subjects, now=now)
        self._strike(list(subjects), name)
        self._list(subjects, self._children_of([name]))
        return count

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        count = super().forget(name, subjects, now=now)
        self._list(subjects, [name])
        return count

    def release(self, name: str, *, older_than: str, now: str,
                only: tuple[str, ...] | None = None, exclude: tuple[str, ...] = (),
                version: str | None = None) -> int:
        t = self.table
        # Read before: every row this release can take is running and older
        # already, so the subjects read are a superset of those released.
        maybe = [r[0] for r in self._execute(
            sa.select(self._subject).where(t.c.node == name, t.c.status == Status.RUNNING,
                                           t.c.started_at < older_than)).fetchall()]
        count = super().release(name, older_than=older_than, now=now, only=only,
                                exclude=exclude, version=version)
        if count:
            self._list(maybe, [name])
        return count

    def rewrite_many(self, subjects: list[Any], *, rename: dict[str, str],
                     drop: tuple[str, ...], version: str, now: str) -> None:
        super().rewrite_many(subjects, rename=rename, drop=drop, version=version, now=now)
        self._list(subjects, self._parents)

    def enter(self, name: str, entries: list[tuple[Any, int]], *,
              archive: tuple[str, ...], now: str) -> list[Any]:
        entering = super().enter(name, entries, archive=archive, now=now)
        self._list(entering, [*self._children_of([name]), *archive])
        return entering
