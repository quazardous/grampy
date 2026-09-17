"""THE POSTGRESQL DRIVER — node rows in a table the application declares.

    PostgresDriver(execute, table, subject="request_id")

────────────────────────────────────────────────────────────────────────
THE TABLE IS INJECTED
────────────────────────────────────────────────────────────────────────

The application owns its schema and its migrations; this driver owns
none. It needs a `sqlalchemy.Table` with a subject column (named by
`subject`) and four columns: `node`, `status`, `started_at`,
`finished_at` — all text, `(subject, node)` as primary key. Timestamps
are ISO-8601 text, compared lexically.

────────────────────────────────────────────────────────────────────────
THE CONNECTION IS INJECTED TOO, AS AN `execute`
────────────────────────────────────────────────────────────────────────

`execute(statement)` runs a SQLAlchemy Core statement and returns a
result with `fetchall()`, `fetchone()` and `rowcount`, rows indexable by
position. A SQLAlchemy `Connection.execute` fits as is; an application
executing through its own driver compiles first. The driver never
commits.

────────────────────────────────────────────────────────────────────────
THE INSERT IS THE LOCK
────────────────────────────────────────────────────────────────────────

A claim is ONE `INSERT … SELECT … ON CONFLICT DO NOTHING RETURNING`.
Two workers aiming at the same subject: the second conflicts, `DO
NOTHING` drops it, `RETURNING` gives it nothing. There is no read, so no
window between reading and writing.

`candidates` is an opaque `SELECT` whose FIRST column is the subject,
its `ORDER BY` being the priority; it is nested as subquery `c`.

────────────────────────────────────────────────────────────────────────
THE PRIORITY SURVIVES THE NESTING, BECAUSE IT IS MADE A COLUMN
────────────────────────────────────────────────────────────────────────

SQL does not promise that a subquery's `ORDER BY` still holds once the
outer query filters and limits it: the planner is free to take any
`limit` rows. The contract caught exactly that — `["b", "a", "c"]` with
a limit of two returned `["a", "c"]`. The candidates' ordering is
therefore turned into a `row_number()` column, and the OUTER query
orders and limits by it.

COUNTS COME FROM `RETURNING`, NOT `rowcount`: SQLAlchemy reports `-1`
for a Core `INSERT` on some execution paths, and a count that silently
reads `-1` would pass for "nothing done".
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from ..dag import NODE_DONE, NODE_RUNNING, NODE_SATISFYING, NODE_SKIPPED

#: THE COLUMNS THE TABLE MUST CARRY, besides the subject.
REQUIRED_COLUMNS = ("node", "status", "started_at", "finished_at")


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
            else sa.literal(0))
    return candidates.order_by(None).add_columns(rank.label("grampy_rank")).subquery("c")


def _epoch(moment: Any) -> Any:
    """The seconds of an ISO timestamp stored as text — a `numeric`, which
    `round(x, 1)` accepts."""
    return sa.extract("epoch", sa.cast(moment, sa.TIMESTAMP(timezone=True)))


class PostgresDriver:
    """Node rows in `table`, written with SQLAlchemy Core."""

    def __init__(self, execute: Callable[[Any], Any], table: sa.Table, *,
                 subject: str = "request_id") -> None:
        missing = [c for c in (subject, *REQUIRED_COLUMNS) if c not in table.c]
        if missing:
            raise ValueError(
                f"table {table.name!r} lacks the column(s) {missing} — a node "
                f"table needs {subject!r} and {', '.join(REQUIRED_COLUMNS)}")
        self._execute = execute
        self.table = table
        self._subject = table.c[subject]

    # -- write -------------------------------------------------------------

    def claim(self, name: str, *, parents: tuple[str, ...],
              after: tuple[str, ...], candidates: Any, limit: int,
              require_parents: bool, now: str) -> list[Any]:
        t = self.table
        c = _ranked(candidates)
        candidate = list(c.c)[0]
        held = t.alias("d")
        started = t.alias("a")
        chosen = (
            sa.select(candidate, sa.literal(name), sa.literal(NODE_RUNNING),
                      sa.literal(now))
            .where(~sa.exists().where(held.c[self._subject.key] == candidate,
                                      held.c.node == name),
                   ~sa.exists().where(started.c[self._subject.key] == candidate,
                                      started.c.node.in_(list(after)))))
        if require_parents:
            chosen = chosen.where(self.parents_concluded(parents, candidate))
        rows = self._execute(
            postgresql.insert(t)
            .from_select([self._subject.key, "node", "status", "started_at"],
                         chosen.order_by(c.c.grampy_rank).limit(int(limit)))
            .on_conflict_do_nothing()
            .returning(self._subject)).fetchall()
        return [r[0] for r in rows]

    def skip(self, name: str, *, parents: tuple[str, ...], candidates: Any,
             now: str) -> int:
        t = self.table
        c = candidates.subquery("c")
        candidate = list(c.c)[0]
        held = t.alias("d")
        cur = self._execute(
            postgresql.insert(t)
            .from_select(
                [self._subject.key, "node", "status", "started_at", "finished_at"],
                sa.select(candidate, sa.literal(name), sa.literal(NODE_SKIPPED),
                          sa.literal(now), sa.literal(now))
                .where(~sa.exists().where(held.c[self._subject.key] == candidate,
                                          held.c.node == name),
                       self.parents_concluded(parents, candidate)))
            .on_conflict_do_nothing()
            .returning(self._subject))
        return len(cur.fetchall())

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str) -> int:
        t = self.table
        cur = self._execute(
            sa.update(t)
            .where(t.c.node == name, t.c.status == NODE_RUNNING,
                   self._subject.in_(list(subjects)))
            .values(status=status, finished_at=now))
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
        t = self.table
        cur = self._execute(
            sa.delete(t).where(t.c.node == name, self._subject.in_(list(subjects))))
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
