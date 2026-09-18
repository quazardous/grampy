"""THE POSTGRESQL DRIVER — node rows in tables the application declares.

    PostgresDriver(execute, table, revisions, history, subject="subject",
                   limits=None, arrivals=None)

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
    revisions   the subject registry, one row per subject: `revision`, an
                integer, `policy` and `version`, nullable text; the subject
                as primary key. Rows are created on demand.
    history     the rows taken away: the node table's columns, plus
                `archived_at` and `reason` (text); append-only, no key
                required. An `id` column (an identity) is read, when there
                is one, to give rows archived in the same second back in
                the order they were written.
    limits      optional, needed by nodes with a `rate` or a `concurrency`:
                `key` (text, primary key) and `value` (double precision).
    arrivals    optional, needed by graphs with a lane: the subject and `node`
                as primary key, `ref` (nullable text), `place` and
                `arrived_at` (text), `urgent` (boolean), `refs` (nullable
                text) — every version waiting, for a lane that keeps them.

────────────────────────────────────────────────────────────────────────
THE CONNECTION IS INJECTED TOO, AS AN `execute`
────────────────────────────────────────────────────────────────────────

`execute(statement)` runs a SQLAlchemy Core statement and returns a
result with `fetchall()`, `fetchone()` and `rowcount`, rows indexable by
position. A SQLAlchemy `Connection.execute` fits as is; an application
executing through its own driver compiles first. The driver never
commits — and it REFUSES TO RUN OUTSIDE A TRANSACTION: under an
autocommitting executor its locks and its two-statement writes would
protect nothing, silently, so its first write checks, once.

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
from contextlib import contextmanager
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.dml import UpdateBase

from ..dag import (
    NODE_SATISFYING,
)
from ..journal import Arrival, Entry
from ..names import Merge, Outcome, Position, Reason, Status

#: THE COLUMNS THE NODE TABLE MUST CARRY, besides the subject.
REQUIRED_COLUMNS = ("node", "status", "started_at", "finished_at", "lease")
#: THE COLUMNS THE REVISIONS TABLE MUST CARRY, besides the subject.
REVISION_COLUMNS = ("revision", "policy", "version")
#: THE COLUMNS THE HISTORY TABLE MUST CARRY, besides the subject.
HISTORY_COLUMNS = (*REQUIRED_COLUMNS, "archived_at", "reason")
#: THE COLUMNS THE ARRIVALS TABLE MUST CARRY, besides the subject.
ARRIVAL_COLUMNS = ("node", "ref", "place", "arrived_at", "urgent", "refs")


_VIRTUAL_TRANSACTION = sa.text(
    "SELECT virtualtransaction FROM pg_locks "
    "WHERE pid = pg_backend_pid() AND locktype = 'virtualxid'")
_ADVISORY_HELD = sa.text(
    "SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'")
_OUTSIDE_A_TRANSACTION = (
    "the journal runs outside a transaction: its executor autocommits, which "
    "voids every lock and every two-statement write it relies on. Open a "
    "transaction around the journal call and commit after it.")


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
    # A LIMIT OR AN OFFSET PICKS ROWS BY THE ORDER: it keeps its ORDER BY, or
    # it would pick whichever rows came first.
    if candidates._limit_clause is not None or candidates._offset_clause is not None:
        return candidates.add_columns(rank.label("grampy_rank")).subquery("c")
    return candidates.order_by(None).add_columns(rank.label("grampy_rank")).subquery("c")


def _epoch(moment: Any) -> Any:
    """The seconds of an ISO timestamp stored as text — a `numeric`, which
    `round(x, 1)` accepts."""
    return sa.extract("epoch", sa.cast(moment, sa.TIMESTAMP(timezone=True)))


class PostgresCommon:
    """WHAT EVERY POSTGRESQL LAYOUT SHARES — the history, the limits, the
    lanes' arrivals, the clock and the guard. None of it depends on how the
    node rows are laid out, so a layout is a subclass that adds only that.

    `subject` names the subject column, the same in every table.
    """

    def __init__(self, execute: Callable[[Any], Any], history: sa.Table, *,
                 subject: str, limits: sa.Table | None,
                 arrivals: sa.Table | None) -> None:
        missing = [c for c in (subject, *HISTORY_COLUMNS) if c not in history.c]
        if missing:
            raise ValueError(
                f"table {history.name!r} lacks the column(s) {missing} — it needs "
                f"{subject!r} and {', '.join(HISTORY_COLUMNS)}")
        self._raw_execute = execute
        self._in_transaction = False
        self._key = subject
        self.history_table = history
        if limits is not None and any(c not in limits.c for c in ("key", "value")):
            raise ValueError(f"table {limits.name!r} lacks key or value — a limits table "
                             f"needs both")
        self.limits_table = limits
        if arrivals is not None:
            missing = [c for c in (subject, *ARRIVAL_COLUMNS) if c not in arrivals.c]
            if missing:
                raise ValueError(f"table {arrivals.name!r} lacks the column(s) {missing}")
        self.arrivals_table = arrivals

    # -- the transaction the caller owes ----------------------------------------

    def _execute(self, statement: Any) -> Any:
        """Run `statement`, having checked ONCE, before the first write, that
        the caller's executor runs inside a transaction."""
        if not self._in_transaction and isinstance(statement, UpdateBase):
            self._require_transaction()
        return self._raw_execute(statement)

    def _require_transaction(self) -> None:
        """NOT UNDER AUTOCOMMIT. Every guarantee here — the advisory locks of
        `guard`, a revision raised then rows deleted — holds only because the
        statements share one transaction. An autocommitting executor voids
        them all without an error, so it is refused, by reading the
        session's transaction twice: under autocommit each statement is a
        transaction of its own. Two statements, once per driver."""
        seen = [self._raw_execute(_VIRTUAL_TRANSACTION).fetchone()[0] for _ in range(2)]
        if seen[0] != seen[1]:
            raise RuntimeError(_OUTSIDE_A_TRANSACTION)
        self._in_transaction = True

    def now(self) -> str:
        """The server's clock at this statement, in the journal's format."""
        return self._execute(sa.select(sa.func.to_char(
            sa.func.timezone("UTC", sa.func.statement_timestamp()),
            'YYYY-MM-DD"T"HH24:MI:SS"+00:00"'))).fetchone()[0]

    @contextmanager
    def guard(self, keys: list[str]) -> Iterator[None]:
        """Transaction-scoped advisory locks, taken in a statement of their
        own before the claim reads anything: that claim's statements then
        see every commit of the claimer they waited for. No table: a lock
        is the server's, not a row's."""
        for key in sorted(keys):
            self._execute(sa.select(sa.func.pg_advisory_xact_lock(
                sa.func.hashtextextended(key, 0))))
        # A transaction-scoped lock taken under autocommit is gone by the
        # next statement: check it is still held, or the guard guards nothing.
        if keys and not self._execute(_ADVISORY_HELD).fetchone()[0]:
            raise RuntimeError(_OUTSIDE_A_TRANSACTION)
        yield

    def limits(self, keys: list[str]) -> dict[str, float]:
        table = self._need_limits()
        return {row[0]: float(row[1]) for row in self._execute(
            sa.select(table.c.key, table.c.value).where(table.c.key.in_(list(keys)))
        ).fetchall() if row[1] is not None}

    def set_limits(self, values: dict[str, float]) -> None:
        table = self._need_limits()
        if not values:
            return
        insert = postgresql.insert(table).values(
            [{"key": k, "value": v} for k, v in sorted(values.items())])
        self._execute(insert.on_conflict_do_update(
            index_elements=["key"], set_={"value": insert.excluded.value}))

    def _need_limits(self) -> sa.Table:
        if self.limits_table is None:
            raise ValueError("this graph limits rate or concurrency: give the "
                             "driver a `limits` table")
        return self.limits_table

    def history(self, subject: Any) -> list[dict[str, Any]]:
        h = self.history_table
        keys = [*REQUIRED_COLUMNS, "archived_at", "reason"]
        return [dict(zip(keys, row, strict=True)) for row in self._execute(
            sa.select(*[h.c[k] for k in keys])
            .where(h.c[self._key] == subject)
            .order_by(h.c.archived_at, h.c.node,
                      *([h.c.id] if "id" in h.c else []))).fetchall()]

    def note(self, subjects: list[Any], name: str, *, status: str, reason: str,
             now: str, ref: str | None) -> int:
        h = self.history_table
        rows = self._execute(
            sa.insert(h).values([{self._key: s, "node": name, "status": status,
                                  "started_at": now, "finished_at": now, "lease": ref,
                                  "archived_at": now, "reason": reason}
                                 for s in subjects])
            .returning(h.c[self._key])).fetchall()
        return len(rows)

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None, now: str,
               merge: str, position: str, urgent: bool,
               refs: str | None = None) -> dict[Any, str]:
        """One upsert; `xmax = 0` tells a row inserted from a row updated."""
        a = self._need_arrivals()
        subject = a.c[self._key]
        insert = postgresql.insert(a).values(
            [{self._key: s, "node": name, "ref": ref, "place": now,
              "arrived_at": now, "urgent": urgent, "refs": refs} for s in sorted(subjects)])
        merged: dict[str, Any] = {"urgent": sa.or_(a.c.urgent, insert.excluded.urgent)}
        if merge in (Merge.LAST, Merge.SET):
            merged["ref"] = insert.excluded.ref
        if merge == Merge.SET:
            merged["refs"] = insert.excluded.refs
        if position == Position.LAST:
            merged["place"] = insert.excluded.place
        rows = self._execute(
            insert.on_conflict_do_update(index_elements=[self._key, "node"],
                                         set_=merged)
            .returning(subject, sa.literal_column("xmax") == 0)).fetchall()
        return {row[0]: Outcome.QUEUED if row[1] else Outcome.MERGED for row in rows}

    def arrivals(self, subjects: list[Any], name: str) -> dict[Any, Arrival]:
        a = self._need_arrivals()
        subject = a.c[self._key]
        return {row[0]: Arrival(row[1], row[2], row[3], bool(row[4]), row[5])
                for row in self._execute(
                    sa.select(subject, a.c.ref, a.c.place, a.c.arrived_at, a.c.urgent,
                              a.c.refs)
                    .where(a.c.node == name, subject.in_(list(subjects)))).fetchall()}

    def queued(self, name: str) -> int:
        a = self._need_arrivals()
        return int(self._execute(
            sa.select(sa.func.count()).select_from(a).where(a.c.node == name)).fetchone()[0])

    def _need_arrivals(self) -> sa.Table:
        if self.arrivals_table is None:
            raise ValueError("this graph has a lane: give the driver an "
                             "`arrivals` table")
        return self.arrivals_table

    def _rewrite_arrivals(self, subject: Any, *, rename: dict[str, str],
                          drop: tuple[str, ...]) -> None:
        """The arrivals waiting in dropped nodes go; those in renamed nodes
        follow their node."""
        a = self.arrivals_table
        if a is None or not (drop or rename):
            return
        a_subject = a.c[self._key]
        if drop:
            self._execute(sa.delete(a).where(a_subject == subject,
                                             a.c.node.in_(list(drop))))
        if rename:
            columns = [self._key, *ARRIVAL_COLUMNS]
            waiting = [dict(zip(columns, row, strict=True)) for row in self._execute(
                sa.delete(a).where(a_subject == subject, a.c.node.in_(list(rename)))
                .returning(*[a.c[c] for c in columns])).fetchall()]
            for row in waiting:
                row["node"] = rename[row["node"]]
            if waiting:
                self._execute(sa.insert(a).values(waiting))

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        h = self.history_table
        subject = h.c[self._key]
        query = (sa.select(subject, sa.func.max(h.c.archived_at))
                 .where(subject.in_(list(subjects)), h.c.node == name)
                 .group_by(subject))
        if reason is not None:
            query = query.where(h.c.reason == reason)
        return {row[0]: row[1] for row in self._execute(query).fetchall()}

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        h = self.history_table
        subject = h.c[self._key]
        return {row[0]: int(row[1]) for row in self._execute(
            sa.select(subject, sa.func.count())
            .where(subject.in_(list(subjects)), h.c.node == name, h.c.reason == reason)
            .group_by(subject)).fetchall()}


class PostgresDriver(PostgresCommon):
    """Node rows in `table`, revisions in `revisions`, written with
    SQLAlchemy Core."""

    def __init__(self, execute: Callable[[Any], Any], table: sa.Table,
                 revisions: sa.Table, history: sa.Table, *,
                 subject: str = "subject", limits: sa.Table | None = None,
                 arrivals: sa.Table | None = None) -> None:
        for t, needed in ((table, REQUIRED_COLUMNS), (revisions, REVISION_COLUMNS)):
            missing = [c for c in (subject, *needed) if c not in t.c]
            if missing:
                raise ValueError(
                    f"table {t.name!r} lacks the column(s) {missing} — it needs "
                    f"{subject!r} and {', '.join(needed)}")
        super().__init__(execute, history, subject=subject, limits=limits,
                         arrivals=arrivals)
        self.table = table
        self.revisions = revisions
        self._subject = table.c[subject]
        self._rev_subject = revisions.c[subject]

    # -- write -------------------------------------------------------------

    def scan(self, candidates: Any, *, name: str | None, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str) -> Iterator[list[Entry]]:
        """Pre-filters in SQL what cannot be taken — a row for `name`, a
        parent not concluded — so that a page is mostly takable."""
        t, r = self.table, self.revisions
        c = _ranked(candidates)
        columns = list(c.c)
        candidate = columns[0]
        # A SECOND COLUMN IS THE GROUPING KEY, carried and never read. The
        # rank this driver adds is always last, so anything between the two
        # is the caller's.
        grouped = columns[1] if len(columns) > 2 else sa.literal(None)
        held = t.alias("d")
        query = (
            sa.select(candidate, c.c.grampy_rank,
                      sa.func.coalesce(r.c.revision, 0), r.c.policy, r.c.version,
                      grouped.label("grampy_key"))
            .select_from(c.outerjoin(r, self._rev_subject == candidate))
            .order_by(c.c.grampy_rank)
            .limit(int(page)))
        if name is not None:
            query = query.where(~sa.exists().where(
                held.c[self._subject.key] == candidate, held.c.node == name,
                ~sa.and_(held.c.status == Status.SCHEDULED, held.c.started_at <= now)))
        if parents:
            query = query.where(self.parents_concluded(parents, candidate))
        query = self._narrow(query, candidate, name, parents)
        last = None
        while True:
            paged = query if last is None else query.where(c.c.grampy_rank > last)
            found = self._execute(paged).fetchall()
            if not found:
                return
            last = found[-1][1]
            rows: dict[Any, dict[str, str]] = {f[0]: {} for f in found}
            due: dict[Any, dict[str, str]] = {f[0]: {} for f in found}
            finished: dict[Any, dict[str, str]] = {f[0]: {} for f in found}
            for subject, n, status, started, ended in self._execute(
                    sa.select(self._subject, t.c.node, t.c.status, t.c.started_at,
                              t.c.finished_at)
                    .where(self._subject.in_(list(rows)),
                           t.c.node.in_(list(nodes)))).fetchall():
                rows[subject][n] = status
                if status == Status.SCHEDULED:
                    due[subject][n] = started
                if ended is not None:
                    finished[subject][n] = ended
            yield [Entry(f[0], int(f[2]), rows[f[0]], due[f[0]], finished[f[0]], f[3], f[4],
                         f[5])
                   for f in found]
            if len(found) < page:
                return

    def _narrow(self, query: Any, candidate: Any, name: str | None,
                parents: tuple[str, ...]) -> Any:
        """Where a layout adds its own pre-filter to a page; none here."""
        return query

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
        if status != Status.RUNNING:
            columns.append("finished_at")
            values.append(sa.literal(now))
        insert = postgresql.insert(t).from_select(
            columns, sa.select(*values).order_by(unchanged.c[self._rev_subject.key]))
        # A `scheduled` row due by now is replaced; any other row wins.
        rows = self._execute(
            insert.on_conflict_do_update(
                index_elements=[self._subject.key, "node"],
                set_={c: insert.excluded[c] for c in columns[2:]}
                | ({} if "finished_at" in columns else {"finished_at": None}),
                where=sa.and_(t.c.status == Status.SCHEDULED, t.c.started_at <= now))
            .returning(self._subject)).fetchall()
        return [row[0] for row in rows]

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        """The omitted rows follow in a second statement of the same
        transaction: the concluded rows stay locked until it ends, and no
        other transaction sees the conclusion without its omissions."""
        t = self.table
        update = sa.update(t).where(t.c.node == name, t.c.status == Status.RUNNING,
                                    self._subject.in_(sorted(subjects)))
        if lease is not None:
            update = update.where(t.c.lease == lease)
        concluded = [row[0] for row in self._execute(
            update.values(status=status, finished_at=now)
            .returning(self._subject)).fetchall()]
        if omit and concluded:
            self._execute(
                postgresql.insert(t)
                .values([{self._subject.key: s, "node": other, "status": Status.OMITTED,
                          "started_at": now, "finished_at": now}
                         for s in sorted(concluded) for other in omit])
                .on_conflict_do_nothing())
        if reset and concluded:
            self._raise_revisions(concluded)
            self._take_away(sa.and_(self._subject.in_(sorted(concluded)),
                                    t.c.node.in_(list(reset))), now=now, reason=Reason.LOOP)
        if reschedule is not None and concluded:
            self._take_away(sa.and_(self._subject.in_(sorted(concluded)), t.c.node == name),
                            now=now, reason=Reason.RETRY)
            self._execute(postgresql.insert(t).values(
                [{self._subject.key: s, "node": name, "status": Status.SCHEDULED,
                  "started_at": reschedule} for s in sorted(concluded)]))
        return len(concluded)

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        cur = self._execute(
            postgresql.insert(self.table)
            .values([{self._subject.key: s, "node": name, "status": Status.DONE,
                      "started_at": now, "finished_at": now} for s in subjects])
            .on_conflict_do_nothing()
            .returning(self._subject))
        return len(cur.fetchall())

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Revision first, deletion second — two statements, see the module."""
        t = self.table
        subjects = sorted(subjects)
        self._raise_revisions(subjects)
        return self._take_away(sa.and_(t.c.node == name, self._subject.in_(subjects)),
                               now=now, reason=Reason.FORGET)

    def release(self, name: str, *, older_than: str, now: str,
                only: tuple[str, ...] | None = None, exclude: tuple[str, ...] = (),
                version: str | None = None) -> int:
        t, r = self.table, self.revisions
        where = sa.and_(t.c.node == name, t.c.status == Status.RUNNING,
                        t.c.started_at < older_than)
        if only is not None:
            where = sa.and_(where, self._subject.in_(
                sa.select(self._rev_subject).where(r.c.policy.in_(list(only)))))
        if exclude:
            where = sa.and_(where, self._subject.not_in(
                sa.select(self._rev_subject).where(r.c.policy.in_(list(exclude)))))
        if version is not None:
            where = sa.and_(where, self._subject.not_in(
                sa.select(self._rev_subject).where(r.c.version.is_not(None),
                                                   r.c.version != version)))
        return self._take_away(where, now=now, reason=Reason.RELEASE)

    def running(self, name: str, policies: tuple[str | None, ...] | None) -> int:
        t, r = self.table, self.revisions
        query = sa.select(sa.func.count()).select_from(t).where(
            t.c.node == name, t.c.status == Status.RUNNING)
        if policies is not None:
            named = [c for c in policies if c is not None]
            with_policy = sa.select(self._rev_subject).where(r.c.policy.in_(named))
            test: sa.ColumnElement[bool] = self._subject.in_(with_policy)
            if None in policies:
                test = sa.or_(test, self._subject.not_in(
                    sa.select(self._rev_subject).where(r.c.policy.is_not(None))))
            query = query.where(test)
        return int(self._execute(query).fetchone()[0])

    def pin(self, subjects: list[Any], version: str) -> int:
        r = self.revisions
        subjects = sorted(subjects)
        self._execute(
            postgresql.insert(r)
            .values([{self._rev_subject.key: s, "revision": 0} for s in subjects])
            .on_conflict_do_nothing())
        return len(self._execute(
            sa.update(r).where(self._rev_subject.in_(subjects), r.c.version.is_(None))
            .values(version=version).returning(self._rev_subject)).fetchall())

    def versions(self, subjects: list[Any]) -> dict[Any, str]:
        r = self.revisions
        return {row[0]: row[1] for row in self._execute(
            sa.select(self._rev_subject, r.c.version)
            .where(self._rev_subject.in_(list(subjects)), r.c.version.is_not(None))).fetchall()}

    def rewrite(self, subject: Any, *, rename: dict[str, str], drop: tuple[str, ...],
                version: str, now: str) -> None:
        """Rows renamed by delete and re-insert: an in-place rename could
        collide with the primary key halfway through a chain of renames."""
        t, r = self.table, self.revisions
        self._raise_revisions([subject])
        self._execute(sa.update(r).where(self._rev_subject == subject).values(version=version))
        if drop:
            self._take_away(sa.and_(self._subject == subject, t.c.node.in_(list(drop))),
                            now=now, reason=Reason.MIGRATE)
        self._rewrite_arrivals(subject, rename=rename, drop=drop)
        if not rename:
            return
        columns = [self._subject.key, *REQUIRED_COLUMNS]
        moved = [dict(zip(columns, row, strict=True)) for row in self._execute(
            sa.delete(t).where(self._subject == subject, t.c.node.in_(list(rename)))
            .returning(*[t.c[c] for c in columns])).fetchall()]
        for row in moved:
            row["node"] = rename[row["node"]]
        if moved:
            self._execute(sa.insert(t).values(moved))

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        r = self.revisions
        rows = self._execute(
            postgresql.insert(r)
            .values([{self._rev_subject.key: s, "revision": 0, "policy": policy}
                     for s in sorted(subjects)])
            .on_conflict_do_update(index_elements=[self._rev_subject.key],
                                   set_={"policy": policy})
            .returning(self._rev_subject)).fetchall()
        return len(rows)

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        r = self.revisions
        return {row[0]: row[1] for row in self._execute(
            sa.select(self._rev_subject, r.c.policy)
            .where(self._rev_subject.in_(list(subjects)), r.c.policy.is_not(None))).fetchall()}

    def _raise_revisions(self, subjects: list[Any]) -> None:
        r = self.revisions
        self._execute(
            postgresql.insert(r)
            .values([{self._rev_subject.key: s, "revision": 1} for s in sorted(subjects)])
            .on_conflict_do_update(index_elements=[self._rev_subject.key],
                                   set_={"revision": r.c.revision + 1}))

    def _take_away(self, where: Any, *, now: str, reason: str) -> int:
        """Delete the rows and archive them, in ONE statement: the rows the
        DELETE removed are the rows the INSERT archives."""
        t, h = self.table, self.history_table
        columns = [self._subject.key, *REQUIRED_COLUMNS]
        gone = sa.delete(t).where(where).returning(*[t.c[c] for c in columns]).cte("gone")
        rows = self._execute(
            sa.insert(h).from_select(
                [*columns, "archived_at", "reason"],
                sa.select(*[gone.c[c] for c in columns], sa.literal(now), sa.literal(reason)))
            .returning(h.c[self._subject.key])).fetchall()
        return len(rows)

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        t = self.table
        return {r[0]: r[1] for r in self._execute(
            sa.select(t.c.node, t.c.status).where(self._subject == subject)
        ).fetchall()}

    def enter(self, name: str, entries: list[tuple[Any, int]], *,
              archive: tuple[str, ...], now: str) -> list[Any]:
        """LOCK FIRST, CHECK AFTER. The revision rows are locked `FOR UPDATE`
        in a statement of their own — a claim writing on these subjects waits
        for this transaction, and finds the revision raised. The checks that
        follow start after the lock, so they see every claim committed
        before it: a pass running is never archived."""
        a, t, h = self._need_arrivals(), self.table, self.history_table
        if not entries:
            return []
        wanted = dict(entries)
        subjects = sorted(wanted)
        self._execute(
            postgresql.insert(self.revisions)
            .values([{self._rev_subject.key: s, "revision": 0} for s in subjects])
            .on_conflict_do_nothing())
        current = {row[0]: int(row[1]) for row in self._execute(
            sa.select(self._rev_subject, self.revisions.c.revision)
            .where(self._rev_subject.in_(subjects))
            .order_by(self._rev_subject).with_for_update()).fetchall()}
        busy = {row[0] for row in self._execute(
            sa.select(self._subject).where(
                self._subject.in_(subjects), t.c.node.in_(list(archive)),
                t.c.status.in_((Status.RUNNING, Status.SCHEDULED)))).fetchall()}
        a_subject = a.c[self._subject.key]
        waiting = {row[0]: (row[1], row[2]) for row in self._execute(
            sa.select(a_subject, a.c.ref, a.c.arrived_at)
            .where(a.c.node == name, a_subject.in_(subjects))
            .order_by(a_subject).with_for_update()).fetchall()}
        entering = [s for s in subjects
                    if current.get(s) == wanted[s] and s not in busy and s in waiting]
        if not entering:
            return []
        self._raise_revisions(entering)
        self._take_away(sa.and_(self._subject.in_(entering), t.c.node.in_(list(archive))),
                        now=now, reason=Reason.ARRIVAL)
        self._execute(sa.delete(a).where(a.c.node == name, a_subject.in_(entering)))
        self._execute(sa.insert(h).values(
            [{self._subject.key: s, "node": name, "status": Outcome.ENTERED,
              "started_at": waiting[s][1], "finished_at": now, "lease": waiting[s][0],
              "archived_at": now, "reason": Reason.LANE} for s in entering]))
        self._execute(sa.insert(t).values(
            [{self._subject.key: s, "node": name, "status": Status.DONE,
              "started_at": waiting[s][1], "finished_at": now} for s in entering]))
        return entering

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
                .where(self._subject.in_(chunk),
                       t.c.status.not_in((Status.SKIPPED, Status.OMITTED, Status.SCHEDULED)))
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
