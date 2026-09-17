"""THE POSTGRESQL DRIVER — node rows in tables the application declares.

    PostgresDriver(execute, table, revisions, subject="request_id")

────────────────────────────────────────────────────────────────────────
THE TABLES ARE INJECTED
────────────────────────────────────────────────────────────────────────

The application owns its schema and its migrations; this driver owns
none. It needs two `sqlalchemy.Table`s sharing a subject column (named by
`subject`):

    table       node rows: `node`, `status`, `started_at`, `finished_at`,
                `lease` — all text, `(subject, node)` as primary key,
                `finished_at` and `lease` nullable. Timestamps are ISO-8601
                text, compared lexically.
    revisions   one row per subject: `revision`, an integer, the subject as
                primary key. Rows are created on demand.

────────────────────────────────────────────────────────────────────────
THE CONNECTION IS INJECTED TOO, AS AN `execute`
────────────────────────────────────────────────────────────────────────

`execute(statement)` runs a SQLAlchemy Core statement and returns a
result with `fetchall()`, `fetchone()` and `rowcount`, rows indexable by
position. A SQLAlchemy `Connection.execute` fits as is; an application
executing through its own driver compiles first. The driver never
commits.

────────────────────────────────────────────────────────────────────────
HOW THIS DRIVER KEEPS `insert_if_unchanged` HONEST
────────────────────────────────────────────────────────────────────────

What follows is PostgreSQL's business, not grampy's: the guarantee is the
journal's, this is one way to hold it.

Under READ COMMITTED an `INSERT … SELECT` reads a snapshot and never
looks again: a parent deleted by a transaction committing meanwhile goes
unnoticed. The shared concurrency test caught exactly that — a child
running with its parent forgotten. Only a row the statement LOCKS is
re-read at its latest version. So:

    forget   raises the subject's revision FIRST — an `UPDATE`, whose row
             lock is held until the transaction ends — then deletes, in a
             second statement that sees everything committed before it.
    insert   locks the revision rows `FOR SHARE`, in the statement that
             inserts, and keeps only those whose revision is still the one
             read. A claim meeting a forget in flight waits for it, then
             finds the revision raised and inserts nothing.

Subjects are written in sorted order, and the journal writes once per
claim, so that two claiming transactions take their locks in the same
order. A transaction mixing several claims and forgets on the same
subjects can still meet a deadlock: PostgreSQL reports it as an error,
and the caller retries the transaction.

────────────────────────────────────────────────────────────────────────
THE PRIORITY SURVIVES THE NESTING, BECAUSE IT IS MADE A COLUMN
────────────────────────────────────────────────────────────────────────

`candidates` is an opaque `SELECT` whose FIRST column is the subject, its
`ORDER BY` being the priority. SQL does not promise that a subquery's
`ORDER BY` still holds once the outer query filters and limits it, so the
ordering is turned into a `row_number()` column: pages are read in that
order, each one after the last rank seen.

COUNTS COME FROM `RETURNING`, NOT `rowcount`: SQLAlchemy reports `-1`
for a Core `INSERT` on some execution paths, and a count that silently
reads `-1` would pass for "nothing done".
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ..dag import NODE_DONE, NODE_RUNNING, NODE_SATISFYING, NODE_SKIPPED
from ..journal import Entry

#: THE COLUMNS THE NODE TABLE MUST CARRY, besides the subject.
REQUIRED_COLUMNS = ("node", "status", "started_at", "finished_at", "lease")
#: THE COLUMNS THE REVISIONS TABLE MUST CARRY, besides the subject.
REVISION_COLUMNS = ("revision",)


def _ranked(candidates: Any) -> Any:
    """The candidates as subquery `c`, with their priority as column `grampy_rank`.

    `_order_by_clauses` is SQLAlchemy's own record of a select's ordering;
    there is no public accessor. If it disappears, raise rather than claim in
    an arbitrary order that nothing would reveal.
    """
    order = getattr(candidates, "_order_by_clauses", None)
    if order is None:  # pragma: no cover - depends on SQLAlchemy
        raise RuntimeError(
            "SQLAlchemy no longer exposes `Select._order_by_clauses`: the "
            "candidates' priority cannot be carried into the claim")
    rank = (sa.func.row_number().over(order_by=list(order)) if order
            else sa.func.row_number().over())
    return candidates.order_by(None).add_columns(rank.label("grampy_rank")).subquery("c")


def _epoch(moment: Any) -> Any:
    """The seconds of an ISO timestamp stored as text — a `numeric`, which
    `round(x, 1)` accepts."""
    return sa.extract("epoch", sa.cast(moment, sa.TIMESTAMP(timezone=True)))


class PostgresDriver:
    """Node rows in `table`, revisions in `revisions`, written with
    SQLAlchemy Core."""

    def __init__(self, execute: Callable[[Any], Any], table: sa.Table,
                 revisions: sa.Table, *, subject: str = "request_id") -> None:
        for t, needed in ((table, REQUIRED_COLUMNS), (revisions, REVISION_COLUMNS)):
            missing = [c for c in (subject, *needed) if c not in t.c]
            if missing:
                raise ValueError(
                    f"table {t.name!r} lacks the column(s) {missing} — it needs "
                    f"{subject!r} and {', '.join(needed)}")
        self._execute = execute
        self.table = table
        self.revisions = revisions
        self._subject = table.c[subject]
        self._rev_subject = revisions.c[subject]

    # -- write -------------------------------------------------------------

    def scan(self, candidates: Any, *, name: str, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int) -> Iterator[list[Entry]]:
        """Pre-filters in SQL what cannot be taken — a row for `name`, a
        parent not concluded — so that a page is mostly takable."""
        t, r = self.table, self.revisions
        c = _ranked(candidates)
        candidate = list(c.c)[0]
        held = t.alias("d")
        query = (
            sa.select(candidate, c.c.grampy_rank,
                      sa.func.coalesce(r.c.revision, 0))
            .select_from(c.outerjoin(r, self._rev_subject == candidate))
            .where(~sa.exists().where(held.c[self._subject.key] == candidate,
                                      held.c.node == name))
            .order_by(c.c.grampy_rank)
            .limit(int(page)))
        if parents:
            query = query.where(self.parents_concluded(parents, candidate))
        last = None
        while True:
            paged = query if last is None else query.where(c.c.grampy_rank > last)
            found = self._execute(paged).fetchall()
            if not found:
                return
            last = found[-1][1]
            rows: dict[Any, dict[str, str]] = {f[0]: {} for f in found}
            for subject, n, status in self._execute(
                    sa.select(self._subject, t.c.node, t.c.status)
                    .where(self._subject.in_(list(rows)),
                           t.c.node.in_(list(nodes)))).fetchall():
                rows[subject][n] = status
            yield [Entry(f[0], int(f[2]), rows[f[0]]) for f in found]
            if len(found) < page:
                return

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        if not entries:
            return []
        t, r = self.table, self.revisions
        entries = sorted(entries, key=lambda e: e[0])
        self._execute(
            postgresql.insert(r)
            .values([{self._rev_subject.key: s, "revision": 0} for s, _ in entries])
            .on_conflict_do_nothing())
        read = sa.values(sa.column("subject", self._subject.type),
                         sa.column("revision", sa.Integer),
                         name="read").data(entries)
        unchanged = (
            sa.select(self._rev_subject)
            .join(read, sa.and_(self._rev_subject == read.c.subject,
                                r.c.revision == read.c.revision))
            .with_for_update(read=True, of=r)
            .cte("unchanged"))
        columns = [self._subject.key, "node", "status", "started_at", "lease"]
        values = [unchanged.c[self._rev_subject.key], sa.literal(name),
                  sa.literal(status), sa.literal(now),
                  sa.cast(sa.literal(lease), t.c.lease.type)]
        if status == NODE_SKIPPED:
            columns.append("finished_at")
            values.append(sa.literal(now))
        rows = self._execute(
            postgresql.insert(t)
            .from_select(columns, sa.select(*values)
                         .order_by(unchanged.c[self._rev_subject.key]))
            .on_conflict_do_nothing()
            .returning(self._subject)).fetchall()
        return [row[0] for row in rows]

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None) -> int:
        t = self.table
        update = sa.update(t).where(t.c.node == name, t.c.status == NODE_RUNNING,
                                    self._subject.in_(list(subjects)))
        if lease is not None:
            update = update.where(t.c.lease == lease)
        cur = self._execute(update.values(status=status, finished_at=now))
        return cur.rowcount

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        cur = self._execute(
            postgresql.insert(self.table)
            .values([{self._subject.key: s, "node": name, "status": NODE_DONE,
                      "started_at": now, "finished_at": now} for s in subjects])
            .on_conflict_do_nothing()
            .returning(self._subject))
        return len(cur.fetchall())

    def forget(self, name: str, subjects: list[Any]) -> int:
        """Revision first, deletion second — two statements, see the module."""
        t, r = self.table, self.revisions
        subjects = sorted(subjects)
        raise_revision = postgresql.insert(r).values(
            [{self._rev_subject.key: s, "revision": 1} for s in subjects])
        self._execute(raise_revision.on_conflict_do_update(
            index_elements=[self._rev_subject.key],
            set_={"revision": r.c.revision + 1}))
        cur = self._execute(
            sa.delete(t).where(t.c.node == name, self._subject.in_(subjects)))
        return cur.rowcount

    def release(self, name: str, *, older_than: str) -> int:
        t = self.table
        cur = self._execute(
            sa.delete(t).where(t.c.node == name, t.c.status == NODE_RUNNING,
                               t.c.started_at < older_than))
        return cur.rowcount

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        t = self.table
        return {r[0]: r[1] for r in self._execute(
            sa.select(t.c.node, t.c.status).where(self._subject == subject)
        ).fetchall()}

    def status_counts(self, name: str) -> dict[str, int]:
        t = self.table
        return {r[0]: int(r[1]) for r in self._execute(
            sa.select(t.c.status, sa.func.count())
            .where(t.c.node == name).group_by(t.c.status)).fetchall()}

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        """PER SLICE OF 400: `IN (…)` carries one parameter per subject."""
        t = self.table
        out: dict[str, list[list[Any]]] = {}
        for start in range(0, len(subjects), 400):
            chunk = list(subjects[start:start + 400])
            ended = sa.func.coalesce(t.c.finished_at, at)
            rows = self._execute(
                sa.select(
                    self._subject, t.c.node, t.c.status, ended.label("ended"),
                    sa.func.round(_epoch(ended) - _epoch(t.c.started_at), 1)
                    .label("seconds"))
                .where(self._subject.in_(chunk), t.c.status != NODE_SKIPPED)
                .order_by(sa.literal_column("ended"), t.c.node)).fetchall()
            for r in rows:
                out.setdefault(str(r[0]), []).append([r[1], r[3], r[4], r[2]])
        return out

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> Any:
        """A SQL expression: the count of satisfying parent rows equals the
        number of parents — zero parents, zero rows required.

        `subject` is a column or a value; the expression correlates with it.
        """
        concluded = self.table.alias("p")
        return sa.select(sa.func.count()).select_from(concluded).where(
            concluded.c[self._subject.key] == subject,
            concluded.c.node.in_(list(parents)),
            concluded.c.status.in_(NODE_SATISFYING),
        ).scalar_subquery() == len(parents)
