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
executing through its own driver compiles first. A JSON value may come back
decoded or as text, whichever the adapter gives: the driver reads both. The driver never
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
             finds the revision raised and inserts nothing. A subject with
             no revision row yet gets one inserted by that same statement,
             which holds it as surely as a lock would.

Subjects are written in sorted order, and the journal writes once per
claim, so that two claiming transactions take their locks in the same
order. A transaction mixing several claims and forgets on the same
subjects can still meet a deadlock: PostgreSQL reports it as an error,
and the caller retries the transaction.

────────────────────────────────────────────────────────────────────────
THE PRIORITY IS READ IN SLICES, ALONG ITS OWN INDEX
────────────────────────────────────────────────────────────────────────

`candidates` is an opaque `SELECT` whose FIRST column is the subject, its
`ORDER BY` being the priority. A claim reads it in slices: the candidates'
own ORDER BY expressions made columns, the subject breaking ties, `LIMIT
n` — a query PostgreSQL answers along the index that serves that order,
reading as far as the slice and no further — and only then filtered. The
next slice resumes after the last key judged; one that yields little is
followed by a larger one (`_slices`).

Not a `row_number()` over the candidates: it numbered every one of them
before a page could be cut, the first page included — a whole sort of the
queue on every claim. Nor the filters on the whole candidate set: on a
large table the planner, estimating them badly, scanned and sorted it all.

COUNTS COME FROM `RETURNING`, NOT `rowcount`: SQLAlchemy reports `-1`
for a Core `INSERT` on some execution paths, and a count that silently
reads `-1` would pass for "nothing done".
"""
from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, NamedTuple

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import operators
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import UnaryExpression, _textual_label_reference

from ..dag import (
    NODE_SATISFYING,
)
from ..names import Merge, Outcome, Position, Reason, Status
from ..protocol import Arrival, Entry, Page

#: A label named by text in an ORDER BY: no column to page by.
_TextualLabelReference = _textual_label_reference
#: The modifiers an ORDER BY term may wear: direction and NULLs placement.
_ORDERING = (operators.asc_op, operators.desc_op, operators.nulls_first_op,
             operators.nulls_last_op)

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


class _Key(NamedTuple):
    """One column a page is ordered by: where it is, which way, and where
    its NULLs go."""

    column: Any
    descending: bool
    nulls_last: bool


def _keyed(candidates: Any) -> tuple[Any, list[_Key]] | None:
    """The candidates as subquery `c`, their ORDER BY expressions exposed as
    columns `grampy_o0`, `grampy_o1`… — and the keys a page is ordered and
    resumed by: those, then the subject, which breaks every tie.

    PAGES BY KEY, NOT BY RANK. A `row_number()` over the candidates numbers
    every one of them before any page can be cut — a whole sort of the queue
    on every claim. Ordered by the keys themselves, a page reads as far as
    it needs, along the index that serves the candidates' order, and the
    next one resumes after the last key read.

    None when an ORDER BY term cannot be made a column: the caller numbers
    the candidates instead (`_ranked`)."""
    order = getattr(candidates, "_order_by_clauses", None)
    if order is None:  # pragma: no cover - depends on SQLAlchemy
        raise RuntimeError(
            "SQLAlchemy no longer exposes `Select._order_by_clauses`: the "
            "candidates' priority cannot be carried into the claim")
    terms: list[tuple[Any, bool, bool]] = []
    for clause in order:
        element, descending, nulls = clause, False, None
        while isinstance(element, UnaryExpression) and element.modifier in _ORDERING:
            if element.modifier in (operators.nulls_first_op, operators.nulls_last_op):
                nulls = nulls if nulls is not None else (
                    element.modifier is operators.nulls_last_op)
            elif element.modifier is operators.desc_op:
                descending = True
            element = element.element
        if isinstance(element, _TextualLabelReference):
            # ORDER BY "label": the selected column of that name, if any.
            named = candidates.selected_columns.get(element.element)
            if named is None:
                return None
            element = named
        if not isinstance(element, sa.ColumnElement):
            return None
        # PostgreSQL's default: NULLs last ascending, first descending.
        terms.append((element, descending, (not descending) if nulls is None else nulls))
    limited = candidates._limit_clause is not None or candidates._offset_clause is not None
    base = candidates if limited else candidates.order_by(None)
    c = base.add_columns(*[e.label(f"grampy_o{i}") for i, (e, _, _) in enumerate(terms)]
                         ).subquery("c")
    keys = [_Key(c.c[f"grampy_o{i}"], d, n) for i, (_, d, n) in enumerate(terms)]
    keys.append(_Key(list(c.c)[0], False, True))
    return c, keys


def _paged(candidates: Any) -> tuple[Any, list[_Key]]:
    """The candidates as subquery `c`, and the keys pages go by: their own
    ORDER BY when it can be made columns (`_keyed`), their rank otherwise."""
    keyed = _keyed(candidates)
    if keyed is not None:
        return keyed
    c = _ranked(candidates)
    return c, [_Key(c.c.grampy_rank, False, True)]


#: HOW LARGE A SLICE OF CANDIDATES MAY GROW, at most.
SLICE_MAX = 65_536


def _slices(execute: Callable[[Any], Any], candidates: Any, page: int,
            judge: Callable[[Any, Any], tuple[Any, list[Any]]],
            columns: Callable[[Any, Any], tuple[list[Any], Any]],
            ) -> Iterator[tuple[list[Any], str]]:
    """THE CANDIDATES IN SLICES, each slice read in their own order — by
    their ORDER BY keys, `LIMIT n`, which PostgreSQL can only answer along
    the index that serves that order, whatever it estimates of the filters —
    and filtered after: one statement a slice. Yields `(takable rows, the
    clock read)`, the rows ordered as the candidates are.

    A slice resumes after the last candidate judged, taken or not; one
    candidate more than the slice is read to know whether another follows.
    At most `page` takable rows come back from a slice — the next one
    resumes after the last of them — and a slice that yields fewer is
    followed by one twice as large, up to `SLICE_MAX`: a queue where few can
    be taken is read in a handful of statements, not one per page.

    `judge(chunk, candidate)` gives what a slice is joined to and the
    conditions that make one of its candidates takable; `columns(picked,
    candidate)` the columns to read of each takable one — candidate first,
    the clock last — and what they are read from. They are read only for the
    rows that come back."""
    c, keys = _paged(candidates)
    size = max(int(page), 1)
    last: tuple[Any, ...] | None = None
    names = [k.column.key for k in keys]
    while True:
        # One candidate more than the slice: it says whether another follows,
        # without a last read that finds nothing.
        first = sa.select(*c.c).order_by(*_ordered(keys)).limit(size + 1)
        if last is not None:
            first = first.where(_after(keys, last))
        inner = first.subquery("slice")
        slice_keys = [_Key(inner.c[n], k.descending, k.nulls_last) for n, k in zip(
            names, keys, strict=True)]
        chunk = (sa.select(*inner.c, sa.func.row_number().over(
                     order_by=_ordered(slice_keys)).label("grampy_n"))
                 .cte("chunk").prefix_with("MATERIALIZED"))
        subject = list(chunk.c)[0]
        count = sa.select(sa.func.count()).select_from(chunk).scalar_subquery()
        # THE TAKABLE ONES: the conditions in the WHERE, so PostgreSQL picks
        # the join that suits the slice, and stops at the first `page`.
        where, takable = judge(chunk, subject)
        picked = (sa.select(*chunk.c).select_from(where)
                  .where(chunk.c.grampy_n <= size, *takable)
                  .order_by(chunk.c.grampy_n).limit(int(page)).subquery("picked"))
        candidate = picked.c[subject.key]
        grouped = (picked.c["grampy_key"] if "grampy_key" in picked.c
                   else sa.literal(None)).label("grampy_key")
        read, source = columns(picked, candidate)
        read = [read[0], *read[1:4], grouped, *read[4:]]
        rows = sa.select(*read, sa.true().label("grampy_ok"), count.label("grampy_count"),
                         picked.c.grampy_n, *[picked.c[n] for n in names]).select_from(source)
        # THE LAST CANDIDATE OF THE SLICE, taken or not: where the next resumes.
        tail = sa.select(chunk.c[subject.key], *[sa.null()] * (len(read) - 2), read[-1],
                         sa.false(), count, chunk.c.grampy_n, *[chunk.c[n] for n in names]
                         ).where(chunk.c.grampy_n == sa.func.least(count, size))
        both = sa.union_all(rows, tail).subquery("both")
        found = execute(sa.select(*both.c).order_by(both.c.grampy_n)).fetchall()
        if not found:
            return
        width = len(read)
        taken = [f for f in found if f[width]]
        ends = [f for f in found if not f[width]]
        yield taken, found[0][width - 1]
        if len(taken) >= page:
            last = tuple(taken[-1][width + 3:])      # more may follow in this slice
            continue
        if not ends or int(ends[0][width + 1]) <= size:
            return
        last = tuple(ends[0][width + 3:])
        size = min(size * 2, SLICE_MAX)


def _ordered(keys: list[_Key]) -> list[Any]:
    """The ORDER BY clauses of `keys`."""
    out = []
    for k in keys:
        term = k.column.desc() if k.descending else k.column.asc()
        out.append(term.nulls_last() if k.nulls_last else term.nulls_first())
    return out


def _after(keys: list[_Key], last: tuple[Any, ...]) -> Any:
    """THE ROWS AFTER `last` in the order of `keys`: some key past its value,
    every key before it tied — NULLs and directions spelled out, so any
    ORDER BY resumes exactly where its page ended."""
    alternatives = []
    for i, (k, value) in enumerate(zip(keys, last, strict=True)):
        if value is None:
            # Past a NULL: only non-NULLs, and only when NULLs come first.
            past = sa.false() if k.nulls_last else k.column.is_not(None)
        else:
            bound = sa.literal(value, type_=k.column.type)
            beyond = k.column < bound if k.descending else k.column > bound
            past = sa.or_(beyond, k.column.is_(None)) if k.nulls_last else beyond
        tied = [prev.column.is_not_distinct_from(sa.literal(v, type_=prev.column.type))
                for prev, v in zip(keys[:i], last[:i], strict=True)]
        alternatives.append(sa.and_(*tied, past))
    return sa.or_(*alternatives)


def _plain(candidates: Any) -> Any:
    """The candidates as subquery `c`, unranked — for a read whose result
    does not depend on their order. A LIMIT or an OFFSET keeps its ORDER BY."""
    if candidates._limit_clause is not None or candidates._offset_clause is not None:
        return candidates.subquery("c")
    return candidates.order_by(None).subquery("c")


def _decoded(value: Any) -> Any:
    """A JSON value as the executor returned it — decoded already (psycopg's
    default), or as text (asyncpg's default, a text loader, a host's shim).
    The driver takes an `execute` callable to stay neutral about that layer;
    this is where it has to be."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    return json.loads(value) if isinstance(value, str) else value


def _epoch(moment: Any) -> Any:
    """The seconds of an ISO timestamp stored as text — a `numeric`, which
    `round(x, 1)` accepts."""
    return sa.extract("epoch", sa.cast(moment, sa.TIMESTAMP(timezone=True)))


def _clock() -> Any:
    """The server's clock at the current statement, in the journal's format."""
    return sa.func.to_char(sa.func.timezone("UTC", sa.func.statement_timestamp()),
                           'YYYY-MM-DD"T"HH24:MI:SS"+00:00"')


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

    # -- lists bound as arrays -------------------------------------------------

    @staticmethod
    def _array(values: Any, type_: Any) -> Any:
        """A list bound as ONE parameter, a PostgreSQL array — whatever its
        length. A value per element would stop at the 65,535 parameters one
        statement may bind: a skip over 40,000 subjects failed outright."""
        return sa.bindparam(None, list(values), type_=postgresql.ARRAY(type_))

    def _insert_rows(self, table: sa.Table, varying: dict[str, Any],
                     **constants: Any) -> Any:
        """`INSERT INTO table (…) SELECT … FROM unnest(:arrays)`: one row per
        element of the lists in `varying` (a column name each, all the same
        length), with `constants` on every row. However many rows, one
        parameter per column."""
        rows = self._unnest(**{k: (v, table.c[k].type) for k, v in varying.items()})
        consts = [sa.cast(sa.null() if v is None else sa.literal(v, table.c[k].type),
                          table.c[k].type) for k, v in constants.items()]
        return postgresql.insert(table).from_select(
            [*varying, *constants], sa.select(*[rows.c[k] for k in varying], *consts))

    def _in(self, column: Any, values: Any) -> Any:
        """`column = ANY(:array)`: an `IN` bound as one parameter."""
        return column == sa.any_(self._array(values, column.type))

    def _unnest(self, **columns: tuple[Any, Any]) -> Any:
        """`unnest(:a, :b, …)` as a table named after its columns — rows to
        insert, bound as one array per column. `columns` maps a name to
        `(values, type)`."""
        return sa.func.unnest(*[self._array(v, t) for v, t in columns.values()]
                              ).table_valued(*columns).render_derived(name="rows")

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

    #: `scan(now=None)` reads the server's clock in the page query.
    scan_reads_clock = True

    def now(self) -> str:
        """The server's clock at this statement, in the journal's format."""
        return self._execute(sa.select(_clock())).fetchone()[0]

    @contextmanager
    def guard(self, keys: list[str]) -> Iterator[None]:
        """Transaction-scoped advisory locks, taken in a statement of their
        own before the claim reads anything: that claim's statements then
        see every commit of the claimer they waited for. No table: a lock
        is the server's, not a row's."""
        # ONE STATEMENT, THE LOCKS IN ONE ORDER: every caller takes them by
        # ascending hash, so two callers wanting the same keys queue instead
        # of each holding one the other waits for. The ordered subquery is
        # fenced (`OFFSET 0`) so the locks are taken in its order.
        if keys:
            key = sa.func.unnest(self._array(sorted(set(keys)), sa.Text)
                                 ).table_valued("key").render_derived(name="k")
            hashed = sa.func.hashtextextended(key.c.key, 0)
            ordered = (sa.select(hashed.label("h")).select_from(key).distinct()
                       .order_by(hashed).offset(0).subquery("ordered"))
            self._execute(sa.select(sa.func.pg_advisory_xact_lock(ordered.c.h)))
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
        pairs = sorted(values.items())
        insert = self._insert_rows(table, {"key": [k for k, _ in pairs],
                                           "value": [v for _, v in pairs]})
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
            self._insert_rows(h, {self._key: list(subjects)}, node=name, status=status,
                              started_at=now, finished_at=now, lease=ref,
                              archived_at=now, reason=reason)
            .returning(h.c[self._key])).fetchall()
        return len(rows)

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None, now: str,
               merge: str, position: str, urgent: bool,
               refs: str | None = None) -> dict[Any, str]:
        """One upsert; `xmax = 0` tells a row inserted from a row updated."""
        a = self._need_arrivals()
        subject = a.c[self._key]
        insert = self._insert_rows(a, {self._key: sorted(subjects)}, node=name, ref=ref,
                                   place=now, arrived_at=now, urgent=urgent, refs=refs)
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
                    .where(a.c.node == name, self._in(subject, list(subjects)))).fetchall()}

    def _waiting_since(self, name: str) -> dict[str, str]:
        """`{"waiting": first arrival}` of a lane, empty when none waits."""
        a = self._need_arrivals()
        first = self._execute(sa.select(sa.func.min(a.c.arrived_at))
                              .where(a.c.node == name)).fetchone()[0]
        return {} if first is None else {"waiting": first}

    def queued(self, name: str) -> int:
        a = self._need_arrivals()
        return int(self._execute(
            sa.select(sa.func.count()).select_from(a).where(a.c.node == name)).fetchone()[0])

    def _need_arrivals(self) -> sa.Table:
        if self.arrivals_table is None:
            raise ValueError("this graph has a lane: give the driver an "
                             "`arrivals` table")
        return self.arrivals_table

    def _rewrite_arrivals(self, subjects: list[Any], *, rename: dict[str, str],
                          drop: tuple[str, ...]) -> None:
        """The arrivals waiting in dropped nodes go; those in renamed nodes
        follow their node."""
        a = self.arrivals_table
        if a is None or not (drop or rename):
            return
        a_subject = a.c[self._key]
        if drop:
            self._execute(sa.delete(a).where(self._in(a_subject, subjects),
                                             a.c.node.in_(list(drop))))
        if rename:
            columns = [self._key, *ARRIVAL_COLUMNS]
            waiting = self._execute(
                sa.delete(a).where(self._in(a_subject, subjects), a.c.node.in_(list(rename)))
                .returning(*[a.c[c] for c in columns])).fetchall()
            if waiting:
                rows = {c: [row[i] for row in waiting] for i, c in enumerate(columns)}
                rows["node"] = [rename[n] for n in rows["node"]]
                self._execute(self._insert_rows(a, rows))

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        h = self.history_table
        subject = h.c[self._key]
        query = (sa.select(subject, sa.func.max(h.c.archived_at))
                 .where(self._in(subject, list(subjects)), h.c.node == name)
                 .group_by(subject))
        if reason is not None:
            query = query.where(h.c.reason == reason)
        return {row[0]: row[1] for row in self._execute(query).fetchall()}

    def prune_history(self, before: str, keep: tuple[str, ...]) -> int:
        h = self.history_table
        return len(self._execute(
            sa.delete(h).where(h.c.archived_at < before, h.c.reason.not_in(list(keep)))
            .returning(h.c[self._key])).fetchall())

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        h = self.history_table
        subject = h.c[self._key]
        return {row[0]: int(row[1]) for row in self._execute(
            sa.select(subject, sa.func.count())
            .where(self._in(subject, list(subjects)), h.c.node == name, h.c.reason == reason)
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
             parents: tuple[str, ...], page: int, now: str | None,
             after: tuple[str, ...] = ()) -> Iterator[list[Entry]]:
        """ONE STATEMENT PER SLICE OF CANDIDATES: a slice read in their own
        order, the subjects of it that can be taken, their revision and
        their rows on `nodes` — and, when `now` is None, the server's clock.
        A slice that yields little is followed by a larger one (`_slices`)."""
        t, r = self.table, self.revisions
        clock = _clock() if now is None else sa.literal(now)

        def judge(chunk: Any, candidate: Any) -> tuple[Any, list[Any]]:
            held, later = t.alias("d"), t.alias("later")
            takable: list[Any] = []
            if name is not None:
                takable.append(~sa.exists().where(
                    held.c[self._subject.key] == candidate, held.c.node == name,
                    ~sa.and_(held.c.status == Status.SCHEDULED, held.c.started_at <= clock)))
            if parents:
                takable.append(self.parents_concluded(parents, candidate))
            if after:
                # THE SUBJECT HAS MOVED PAST `name`: a row, any, on a node after it.
                takable.append(~sa.exists().where(
                    later.c[self._subject.key] == candidate, later.c.node.in_(list(after))))
            return chunk, [*takable, *self._narrowing(candidate, name, parents)]

        def columns(judged: Any, candidate: Any) -> tuple[list[Any], Any]:
            own = t.alias("own")
            rows_of = (sa.select(sa.func.json_agg(sa.func.json_build_array(
                           own.c.node, own.c.status, own.c.started_at, own.c.finished_at)))
                       .where(own.c[self._subject.key] == candidate,
                              own.c.node.in_(list(nodes)))
                       .scalar_subquery())
            return ([candidate, sa.func.coalesce(r.c.revision, 0), r.c.policy, r.c.version,
                     rows_of, clock], judged.outerjoin(r, self._rev_subject == candidate))

        for found, now_read in _slices(self._execute, candidates, page, judge, columns):
            entries = []
            for f in found:
                rows: dict[str, str] = {}
                due: dict[str, str] = {}
                finished: dict[str, str] = {}
                for n, status, started, ended in _decoded(f[5]) or ():
                    rows[n] = status
                    if status == Status.SCHEDULED:
                        due[n] = started
                    if ended is not None:
                        finished[n] = ended
                entries.append(Entry(f[0], int(f[1]), rows, due, finished, f[2], f[3], f[4]))
            yield Page(entries, now=now_read)

    def _narrowing(self, candidate: Any, name: str | None,
                   parents: tuple[str, ...]) -> list[Any]:
        """The conditions a layout adds to what a slice may take; none here."""
        return []

    def _unchanged(self, entries: list[tuple[Any, int]]) -> Any:
        """THE SUBJECTS WHOSE REVISION IS STILL THE ONE READ, held until the
        transaction ends — a CTE of the statement that writes, so the check
        and the write are one round trip.

        A subject with a revision row is held by locking that row `FOR
        SHARE`, if it still carries the revision read. A subject without one
        (revision 0, never registered) gets it inserted here, and the row
        this statement inserted is held as surely: a `forget` raising its
        revision waits for this transaction. The two branches never meet —
        a row the statement can see is not inserted, and a row it inserts
        was not there to see. A row inserted meanwhile by another
        transaction is in neither: that subject is not written, which a
        claim may always do."""
        r = self.revisions
        read = self._unnest(subject=([s for s, _ in entries], self._subject.type),
                            revision=([v for _, v in entries], sa.Integer))
        held = (sa.select(self._rev_subject.label("subject"))
                .join(read, sa.and_(self._rev_subject == read.c.subject,
                                    r.c.revision == read.c.revision))
                .with_for_update(read=True, of=r))
        fresh = [s for s, v in entries if v == 0]
        if not fresh:
            return held.cte("unchanged")
        locked = held.cte("locked")
        seeded = (self._insert_rows(r, {self._rev_subject.key: fresh}, revision=0)
                  .on_conflict_do_nothing()
                  .returning(self._rev_subject.label("subject"))
                  .cte("seeded"))
        return sa.union_all(sa.select(locked.c.subject),
                            sa.select(seeded.c.subject)).cte("unchanged")

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        if not entries:
            return []
        t = self.table
        entries = sorted(entries, key=lambda e: e[0])
        unchanged = self._unchanged(entries)
        columns = [self._subject.key, "node", "status", "started_at", "lease"]
        values = [unchanged.c.subject, sa.literal(name),
                  sa.literal(status), sa.literal(now),
                  sa.cast(sa.literal(lease), t.c.lease.type)]
        if status != Status.RUNNING:
            columns.append("finished_at")
            values.append(sa.literal(now))
        insert = postgresql.insert(t).from_select(
            columns, sa.select(*values).order_by(unchanged.c.subject))
        # A `scheduled` row due by now is replaced; any other row wins.
        rows = self._execute(
            insert.on_conflict_do_update(
                index_elements=[self._subject.key, "node"],
                set_={c: insert.excluded[c] for c in columns[2:]}
                | ({} if "finished_at" in columns else {"finished_at": None}),
                where=sa.and_(t.c.status == Status.SCHEDULED, t.c.started_at <= now))
            .returning(self._subject)).fetchall()
        return [row[0] for row in rows]

    def ready_count(self, name: str, candidates: Any, *, parents: tuple[str, ...],
                    after: tuple[str, ...], now: str,
                    version: str | None) -> tuple[int, str | None]:
        """HOW MANY CANDIDATES A CLAIM OF `name` COULD TAKE NOW, and when the
        oldest of them became ready — ONE statement, counted in SQL by the
        rule the claim applies: no row for `name` (or only a retry due), every
        parent satisfying, no row on a node of `after`, pinned to `version`
        or to none. For a node joining its parents plainly."""
        t, r = self.table, self.revisions
        key = self._subject.key
        c = _plain(candidates)
        candidate = list(c.c)[0]
        held, later, parent = t.alias("held"), t.alias("later"), t.alias("parent")
        seen = r.alias("seen")
        conditions = [~sa.exists().where(
            held.c[key] == candidate, held.c.node == name,
            ~sa.and_(held.c.status == Status.SCHEDULED, held.c.started_at <= now))]
        if parents:
            conditions.append(self.parents_concluded(parents, candidate))
        if after:
            conditions.append(~sa.exists().where(
                later.c[key] == candidate, later.c.node.in_(list(after))))
        if version is not None:
            conditions.append(sa.or_(seen.c.version.is_(None), seen.c.version == version))
        since = (sa.select(sa.func.max(parent.c.finished_at))
                 .where(parent.c[key] == candidate, parent.c.node.in_(list(parents)))
                 .scalar_subquery() if parents else sa.null())
        ready = (sa.select(candidate.label("subject"), since.label("since"))
                 .select_from(c.outerjoin(seen, seen.c[self._rev_subject.key] == candidate))
                 .where(*conditions).distinct().subquery("ready"))
        count, first = self._execute(
            sa.select(sa.func.count(), sa.func.min(ready.c.since))).fetchone()
        return int(count), first

    def skip_where(self, name: str, candidates: Any, *, parents: tuple[str, ...],
                   now: str, version: str | None, limit: int | None = None) -> list[Any]:
        """`skip` IN ONE STATEMENT, instead of reading every candidate page.

        The rule, in SQL: no row for `name` at all, every parent satisfying,
        a subject pinned to `version` or to none. The candidates and the rule
        are evaluated ONCE, in the CTE `read`; the claim's guard then holds
        each subject read — its revision row locked `FOR SHARE` if it still
        carries the revision read, or inserted by this same statement if it
        had none — and a subject whose revision moved meanwhile (a parent
        forgotten) is not skipped. Unordered unless `limit` asks for the
        first candidates, by their own keys: a whole skip does not depend
        on their order."""
        t, r = self.table, self.revisions
        key, rev_key = self._subject.key, self._rev_subject.key
        c, keys = (_plain(candidates), []) if limit is None else _paged(candidates)
        candidate = list(c.c)[0]
        held = t.alias("held")
        eligible = [~sa.exists().where(held.c[key] == candidate, held.c.node == name)]
        if parents:
            eligible.append(self.parents_concluded(parents, candidate))
        seen = r.alias("seen")
        if version is not None:
            eligible.append(sa.or_(seen.c.version.is_(None), seen.c.version == version))
        read = (sa.select(candidate.label("subject"), seen.c.revision.label("revision"))
                .select_from(c.outerjoin(seen, seen.c[rev_key] == candidate))
                .where(*eligible))
        if limit is not None:
            # The first `limit` by the candidates' own keys — no numbering
            # of them all — then each subject once.
            first = read.order_by(*_ordered(keys)).limit(int(limit)).subquery("first")
            read = sa.select(first.c.subject, first.c.revision)
        read = read.distinct()
        read = read.cte("read")
        locked = (sa.select(self._rev_subject.label("subject"))
                  .join(read, sa.and_(self._rev_subject == read.c.subject,
                                      r.c.revision == read.c.revision))
                  .with_for_update(read=True, of=r)
                  .cte("locked"))
        seeded = (postgresql.insert(r).from_select(
                      [rev_key, "revision"],
                      sa.select(read.c.subject, sa.literal(0))
                      .where(read.c.revision.is_(None)))
                  .on_conflict_do_nothing()
                  .returning(self._rev_subject.label("subject"))
                  .cte("seeded"))
        unchanged = sa.union_all(sa.select(locked.c.subject),
                                 sa.select(seeded.c.subject)).cte("unchanged")
        rows = self._execute(
            postgresql.insert(t).from_select(
                [key, "node", "status", "started_at", "finished_at"],
                sa.select(unchanged.c.subject, sa.literal(name),
                          sa.literal(Status.SKIPPED), sa.literal(now), sa.literal(now))
                .order_by(unchanged.c.subject))
            .on_conflict_do_nothing()
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
                                    self._in(self._subject, sorted(subjects)))
        if lease is not None:
            update = update.where(t.c.lease == lease)
        concluded = [row[0] for row in self._execute(
            update.values(status=status, finished_at=now)
            .returning(self._subject)).fetchall()]
        if omit and concluded:
            pairs = [(s, other) for s in sorted(concluded) for other in omit]
            self._execute(
                self._insert_rows(t, {self._subject.key: [x for x, _ in pairs],
                                      "node": [o for _, o in pairs]},
                                  status=Status.OMITTED, started_at=now, finished_at=now)
                .on_conflict_do_nothing())
        if reset and concluded:
            self._raise_revisions(concluded)
            self._take_away(sa.and_(self._in(self._subject, sorted(concluded)),
                                    t.c.node.in_(list(reset))), now=now, reason=Reason.LOOP)
        if reschedule is not None and concluded:
            self._take_away(sa.and_(self._in(self._subject, sorted(concluded)), t.c.node == name),
                            now=now, reason=Reason.RETRY)
            self._execute(self._insert_rows(
                t, {self._subject.key: sorted(concluded)}, node=name,
                status=Status.SCHEDULED, started_at=reschedule))
        return len(concluded)

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        cur = self._execute(
            self._insert_rows(self.table, {self._subject.key: list(subjects)}, node=name,
                              status=Status.DONE, started_at=now, finished_at=now)
            .on_conflict_do_nothing()
            .returning(self._subject))
        return len(cur.fetchall())

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Revision first, deletion second — two statements, see the module."""
        t = self.table
        subjects = sorted(subjects)
        self._raise_revisions(subjects)
        return self._take_away(sa.and_(t.c.node == name, self._in(self._subject, subjects)),
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
            self._insert_rows(r, {self._rev_subject.key: subjects}, revision=0)
            .on_conflict_do_nothing())
        return len(self._execute(
            sa.update(r).where(self._in(self._rev_subject, subjects), r.c.version.is_(None))
            .values(version=version).returning(self._rev_subject)).fetchall())

    def versions(self, subjects: list[Any]) -> dict[Any, str]:
        r = self.revisions
        return {row[0]: row[1] for row in self._execute(
            sa.select(self._rev_subject, r.c.version)
            .where(self._in(self._rev_subject, subjects),
                   r.c.version.is_not(None))).fetchall()}

    def rewrite(self, subject: Any, *, rename: dict[str, str], drop: tuple[str, ...],
                version: str, now: str) -> None:
        self.rewrite_many([subject], rename=rename, drop=drop, version=version, now=now)

    def rewrite_many(self, subjects: list[Any], *, rename: dict[str, str],
                     drop: tuple[str, ...], version: str, now: str) -> None:
        """Rows renamed by delete and re-insert: an in-place rename could
        collide with the primary key halfway through a chain of renames. The
        same statements whatever the number of subjects."""
        t, r = self.table, self.revisions
        subjects = sorted(set(subjects))
        if not subjects:
            return
        self._raise_revisions(subjects)
        self._execute(sa.update(r).where(self._in(self._rev_subject, subjects))
                      .values(version=version))
        if drop:
            self._take_away(sa.and_(self._in(self._subject, subjects),
                                    t.c.node.in_(list(drop))),
                            now=now, reason=Reason.MIGRATE)
        self._rewrite_arrivals(subjects, rename=rename, drop=drop)
        if not rename:
            return
        columns = [self._subject.key, *REQUIRED_COLUMNS]
        moved = self._execute(
            sa.delete(t).where(self._in(self._subject, subjects), t.c.node.in_(list(rename)))
            .returning(*[t.c[c] for c in columns])).fetchall()
        if moved:
            rows = {c: [row[i] for row in moved] for i, c in enumerate(columns)}
            rows["node"] = [rename[n] for n in rows["node"]]
            self._execute(self._insert_rows(t, rows))

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        r = self.revisions
        rows = self._execute(
            self._insert_rows(r, {self._rev_subject.key: sorted(subjects)},
                              revision=0, policy=policy)
            .on_conflict_do_update(index_elements=[self._rev_subject.key],
                                   set_={"policy": policy})
            .returning(self._rev_subject)).fetchall()
        return len(rows)

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        r = self.revisions
        return {row[0]: row[1] for row in self._execute(
            sa.select(self._rev_subject, r.c.policy)
            .where(self._in(self._rev_subject, subjects),
                   r.c.policy.is_not(None))).fetchall()}

    def _raise_revisions(self, subjects: list[Any]) -> None:
        r = self.revisions
        self._execute(
            self._insert_rows(r, {self._rev_subject.key: sorted(subjects)}, revision=1)
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
        return self.progress_many([subject]).get(subject, {})

    def progress_many(self, subjects: list[Any]) -> dict[Any, dict[str, str]]:
        t = self.table
        out: dict[Any, dict[str, str]] = {}
        for subject, name, status in self._execute(
                sa.select(self._subject, t.c.node, t.c.status)
                .where(self._in(self._subject, list(subjects)))).fetchall():
            out.setdefault(subject, {})[name] = status
        return out

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
            self._insert_rows(self.revisions, {self._rev_subject.key: subjects}, revision=0)
            .on_conflict_do_nothing())
        current = {row[0]: int(row[1]) for row in self._execute(
            sa.select(self._rev_subject, self.revisions.c.revision)
            .where(self._in(self._rev_subject, subjects))
            .order_by(self._rev_subject).with_for_update()).fetchall()}
        busy = {row[0] for row in self._execute(
            sa.select(self._subject).where(
                self._in(self._subject, subjects), t.c.node.in_(list(archive)),
                t.c.status.in_((Status.RUNNING, Status.SCHEDULED)))).fetchall()}
        a_subject = a.c[self._subject.key]
        waiting = {row[0]: (row[1], row[2]) for row in self._execute(
            sa.select(a_subject, a.c.ref, a.c.arrived_at)
            .where(a.c.node == name, self._in(a_subject, subjects))
            .order_by(a_subject).with_for_update()).fetchall()}
        entering = [s for s in subjects
                    if current.get(s) == wanted[s] and s not in busy and s in waiting]
        if not entering:
            return []
        self._raise_revisions(entering)
        self._take_away(sa.and_(self._in(self._subject, entering), t.c.node.in_(list(archive))),
                        now=now, reason=Reason.ARRIVAL)
        self._execute(sa.delete(a).where(a.c.node == name, self._in(a_subject, entering)))
        self._execute(self._insert_rows(
            h, {self._subject.key: entering, "started_at": [waiting[x][1] for x in entering],
                "lease": [waiting[x][0] for x in entering]},
            node=name, status=Outcome.ENTERED, finished_at=now, archived_at=now,
            reason=Reason.LANE))
        self._execute(self._insert_rows(
            t, {self._subject.key: entering, "started_at": [waiting[x][1] for x in entering]},
            node=name, status=Status.DONE, finished_at=now))
        return entering

    def node_times(self, name: str, *, waiting: bool) -> dict[str, str]:
        t = self.table
        out = {r[0]: r[1] for r in self._execute(
            sa.select(t.c.status, sa.func.min(t.c.started_at))
            .where(t.c.node == name, t.c.status.in_([Status.RUNNING, Status.SCHEDULED]))
            .group_by(t.c.status)).fetchall()}
        out.update(self._waiting_since(name) if waiting else {})
        return out

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
                .where(self._in(self._subject, chunk),
                       t.c.status.not_in((Status.SKIPPED, Status.OMITTED, Status.SCHEDULED)))
                .order_by(sa.literal_column("ended"), t.c.node)).fetchall()
            for r in rows:
                out.setdefault(str(r[0]), []).append([r[1], r[3], r[4], r[2]])
        return out

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> Any:
        """A SQL expression: every parent has a satisfying row — true for no
        parent at all. `subject` is a column or a value; the expression
        correlates with it.

        ONE `EXISTS` PER PARENT, not a count compared to their number: the
        planner turns each into a semi-join and estimates it, where a count
        per candidate was estimated at a fraction of the rows it kept — and
        costed high enough to be compiled, which cost more than it ran."""
        conditions: list[Any] = []
        for parent in parents:
            concluded = self.table.alias(f"p_{len(conditions)}")
            conditions.append(sa.exists().where(
                concluded.c[self._subject.key] == subject,
                concluded.c.node == parent,
                concluded.c.status.in_(NODE_SATISFYING)))
        return sa.and_(sa.true(), *conditions)
