"""THE SQLITE DRIVER — node rows in tables the application declares, with
the standard library's `sqlite3` only.

    SqliteDriver(conn, table="grampy_nodes", revisions="grampy_revisions",
                 history="grampy_history", subject="subject")

────────────────────────────────────────────────────────────────────────
WHY A SECOND SQL DRIVER
────────────────────────────────────────────────────────────────────────

SQLite has one writer at a time, no row lock, no `SKIP LOCKED`, no
`FOR SHARE`. A guarantee that only held thanks to PostgreSQL's locks would
fail here first — which is the point: the shared contract runs on both.

────────────────────────────────────────────────────────────────────────
THE TABLES ARE THE APPLICATION'S
────────────────────────────────────────────────────────────────────────

The driver creates nothing on its own. `schema()` returns the DDL of the
two tables it expects, for an application that wants them as they are:

    table       subject, node, status, started_at, finished_at, lease —
                `(subject, node)` as primary key; timestamps ISO-8601 text
    revisions   subject (primary key), revision (integer)
    history     the node table's columns, plus archived_at and reason

Names are identifiers checked against `[A-Za-z_][A-Za-z0-9_]*`: they are
written into SQL, never taken from users.

────────────────────────────────────────────────────────────────────────
CANDIDATES
────────────────────────────────────────────────────────────────────────

A `Query(sql, params)` — or a bare SQL string — whose FIRST column is the
subject and whose `ORDER BY` is the priority; it is read in order, page by
page. Any other iterable is taken as the subjects themselves, in order.

────────────────────────────────────────────────────────────────────────
HOW THIS DRIVER KEEPS `insert_if_unchanged` HONEST
────────────────────────────────────────────────────────────────────────

One statement per subject: `INSERT … SELECT … WHERE` the stored revision
is still the one read, `ON CONFLICT DO NOTHING`. SQLite serialises
writers: the statement runs holding the database's write lock, and sees
every commit made before it. A `forget` raises the revision and deletes in
the same transaction, so a claim writing after it finds the revision
moved, and one writing before it is deleted by it.

The driver never commits. A writer meeting another transaction's lock
waits for the connection's busy timeout — give connections one
(`sqlite3.connect(path, timeout=…)`) when several share a file.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from typing import Any, NamedTuple

from ..dag import (
    NODE_DONE,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SATISFYING,
    NODE_SCHEDULED,
    NODE_SKIPPED,
)
from ..journal import Entry, utc_now

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Query(NamedTuple):
    """Candidates as SQL: the first column is the subject, the order is the
    priority."""

    sql: str
    params: tuple[Any, ...] = ()


def schema(table: str = "grampy_nodes", revisions: str = "grampy_revisions",
           history: str = "grampy_history", subject: str = "subject") -> list[str]:
    """The `CREATE TABLE` statements the driver expects."""
    for name in (table, revisions, history, subject):
        _check(name)
    return [
        f"CREATE TABLE IF NOT EXISTS {table} ("
        f"{subject} NOT NULL, node TEXT NOT NULL, status TEXT NOT NULL, "
        f"started_at TEXT NOT NULL, finished_at TEXT, lease TEXT, "
        f"PRIMARY KEY ({subject}, node))",
        f"CREATE TABLE IF NOT EXISTS {revisions} ("
        f"{subject} NOT NULL PRIMARY KEY, revision INTEGER NOT NULL)",
        f"CREATE TABLE IF NOT EXISTS {history} ("
        f"{subject} NOT NULL, node TEXT NOT NULL, status TEXT NOT NULL, "
        f"started_at TEXT NOT NULL, finished_at TEXT, lease TEXT, "
        f"archived_at TEXT NOT NULL, reason TEXT NOT NULL)",
    ]


class SqliteDriver:
    """Node rows in `table`, revisions in `revisions`, on one connection."""

    def __init__(self, conn: sqlite3.Connection, *, table: str = "grampy_nodes",
                 revisions: str = "grampy_revisions", history: str = "grampy_history",
                 subject: str = "subject") -> None:
        for name in (table, revisions, history, subject):
            _check(name)
        self.conn = conn
        self.table, self.revisions, self.subject = table, revisions, subject
        self.history_table = history

    # -- write -------------------------------------------------------------

    def now(self) -> str:
        """SQLite has no server: every process shares the machine's clock."""
        return utc_now()

    def scan(self, candidates: Any, *, name: str, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str) -> Iterator[list[Entry]]:
        """No pre-filter: the journal decides on every candidate read."""
        subjects = self._candidates(candidates)
        while True:
            batch = _take(subjects, page)
            if not batch:
                return
            yield self._entries(batch, nodes)
            if len(batch) < page:
                return

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        finished = now if status == NODE_SKIPPED else None
        taken: list[Any] = []
        for subject, revision in sorted(entries, key=lambda e: str(e[0])):
            cur = self.conn.execute(
                f"INSERT INTO {self.table} "
                f"({self.subject}, node, status, started_at, finished_at, lease) "
                f"SELECT ?, ?, ?, ?, ?, ? "
                f"WHERE COALESCE((SELECT revision FROM {self.revisions} "
                f"WHERE {self.subject} = ?), 0) = ? "
                f"ON CONFLICT ({self.subject}, node) DO UPDATE SET "
                f"status = excluded.status, started_at = excluded.started_at, "
                f"finished_at = excluded.finished_at, lease = excluded.lease "
                f"WHERE status = ? AND started_at <= ?",
                (subject, name, status, now, finished, lease, subject, revision,
                 NODE_SCHEDULED, now))
            if cur.rowcount == 1:
                taken.append(subject)
        return taken

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        count = 0
        for subject in subjects:
            sql = (f"UPDATE {self.table} SET status = ?, finished_at = ? "
                   f"WHERE node = ? AND status = ? AND {self.subject} = ?")
            params: list[Any] = [status, now, name, NODE_RUNNING, subject]
            if lease is not None:
                sql += " AND lease = ?"
                params.append(lease)
            if self.conn.execute(sql, params).rowcount != 1:
                continue
            count += 1
            for other in omit:
                self.conn.execute(
                    f"INSERT INTO {self.table} "
                    f"({self.subject}, node, status, started_at, finished_at) "
                    f"VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                    (subject, other, NODE_OMITTED, now, now))
            if reset:
                self._raise_revision(subject)
                for other in reset:
                    self._take_away("node = ? AND {s} = ?", (other, subject),
                                    now=now, reason="loop")
            if reschedule is not None:
                self._take_away("node = ? AND {s} = ?", (name, subject),
                                now=now, reason="retry")
                self.conn.execute(
                    f"INSERT INTO {self.table} ({self.subject}, node, status, started_at) "
                    f"VALUES (?, ?, ?, ?)", (subject, name, NODE_SCHEDULED, reschedule))
        return count

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        count = 0
        for subject in subjects:
            count += self.conn.execute(
                f"INSERT INTO {self.table} "
                f"({self.subject}, node, status, started_at, finished_at) "
                f"VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (subject, name, NODE_DONE, now, now)).rowcount
        return count

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Revision first, deletion second, in the caller's transaction."""
        count = 0
        for subject in subjects:
            self._raise_revision(subject)
            count += self._take_away("node = ? AND {s} = ?", (name, subject),
                                     now=now, reason="forget")
        return count

    def release(self, name: str, *, older_than: str, now: str) -> int:
        return self._take_away("node = ? AND status = ? AND started_at < ?",
                               (name, NODE_RUNNING, older_than), now=now, reason="release")

    def _raise_revision(self, subject: Any) -> None:
        self.conn.execute(
            f"INSERT INTO {self.revisions} ({self.subject}, revision) VALUES (?, 1) "
            f"ON CONFLICT ({self.subject}) DO UPDATE SET revision = revision + 1",
            (subject,))

    def _take_away(self, where: str, params: tuple[Any, ...], *, now: str,
                   reason: str) -> int:
        """Archive, then delete, under the same write lock: SQLite lets no
        other writer in between."""
        where = where.format(s=self.subject)
        columns = f"{self.subject}, node, status, started_at, finished_at, lease"
        self.conn.execute(
            f"INSERT INTO {self.history_table} ({columns}, archived_at, reason) "
            f"SELECT {columns}, ?, ? FROM {self.table} WHERE {where}",
            (now, reason, *params))
        return self.conn.execute(
            f"DELETE FROM {self.table} WHERE {where}", params).rowcount

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        return dict(self.conn.execute(
            f"SELECT node, status FROM {self.table} WHERE {self.subject} = ?",
            (subject,)).fetchall())

    def history(self, subject: Any) -> list[dict[str, Any]]:
        keys = ["node", "status", "started_at", "finished_at", "lease", "archived_at", "reason"]
        return [dict(zip(keys, row, strict=True)) for row in self.conn.execute(
            f"SELECT {', '.join(keys)} FROM {self.history_table} "
            f"WHERE {self.subject} = ? ORDER BY archived_at, node, rowid",
            (subject,)).fetchall()]

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        counts: dict[Any, int] = {}
        for chunk in _chunks(subjects):
            marks = ", ".join("?" * len(chunk))
            counts.update(self.conn.execute(
                f"SELECT {self.subject}, COUNT(*) FROM {self.history_table} "
                f"WHERE node = ? AND reason = ? AND {self.subject} IN ({marks}) "
                f"GROUP BY {self.subject}", (name, reason, *chunk)).fetchall())
        return counts

    def status_counts(self, name: str) -> dict[str, int]:
        return dict(self.conn.execute(
            f"SELECT status, COUNT(*) FROM {self.table} WHERE node = ? GROUP BY status",
            (name,)).fetchall())

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        lines = []
        for chunk in _chunks(subjects):
            marks = ", ".join("?" * len(chunk))
            lines += self.conn.execute(
                f"SELECT COALESCE(finished_at, ?), node, {self.subject}, started_at, status "
                f"FROM {self.table} WHERE status NOT IN (?, ?, ?) "
                f"AND {self.subject} IN ({marks})",
                (at, NODE_SKIPPED, NODE_OMITTED, NODE_SCHEDULED, *chunk)).fetchall()
        out: dict[str, list[list[Any]]] = {}
        for ended, n, subject, started, status in sorted(lines, key=lambda x: (x[0], x[1])):
            seconds = round(_epoch(ended) - _epoch(started), 1)
            out.setdefault(str(subject), []).append([n, ended, seconds, status])
        return out

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> bool:
        if not parents:
            return True
        marks = ", ".join("?" * len(parents))
        satisfying = ", ".join("?" * len(NODE_SATISFYING))
        found = self.conn.execute(
            f"SELECT COUNT(*) FROM {self.table} WHERE {self.subject} = ? "
            f"AND node IN ({marks}) AND status IN ({satisfying})",
            (subject, *parents, *NODE_SATISFYING)).fetchone()[0]
        return found == len(parents)

    # -- inside ------------------------------------------------------------

    def _candidates(self, candidates: Any) -> Iterator[Any]:
        if isinstance(candidates, str):
            candidates = Query(candidates)
        if isinstance(candidates, Query):
            # Read at once: the cursor must not stay open while this very
            # connection writes the claim.
            return iter([row[0] for row in
                         self.conn.execute(candidates.sql, candidates.params).fetchall()])
        return iter(candidates)

    def _entries(self, batch: list[Any], nodes: tuple[str, ...]) -> list[Entry]:
        rows: dict[Any, dict[str, str]] = {s: {} for s in batch}
        due: dict[Any, dict[str, str]] = {s: {} for s in batch}
        revisions: dict[Any, int] = {}
        for chunk in _chunks(list(rows)):
            marks = ", ".join("?" * len(chunk))
            node_marks = ", ".join("?" * len(nodes))
            for subject, n, status, started in self.conn.execute(
                    f"SELECT {self.subject}, node, status, started_at FROM {self.table} "
                    f"WHERE {self.subject} IN ({marks}) AND node IN ({node_marks})",
                    (*chunk, *nodes)).fetchall():
                rows[subject][n] = status
                if status == NODE_SCHEDULED:
                    due[subject][n] = started
            revisions.update(self.conn.execute(
                f"SELECT {self.subject}, revision FROM {self.revisions} "
                f"WHERE {self.subject} IN ({marks})", chunk).fetchall())
        return [Entry(s, int(revisions.get(s, 0)), rows[s], due[s]) for s in batch]


def _check(name: str) -> None:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"not a plain SQL identifier: {name!r}")


def _take(iterator: Iterator[Any], size: int) -> list[Any]:
    out = []
    for item in iterator:
        out.append(item)
        if len(out) >= size:
            break
    return out


def _chunks(items: list[Any], size: int = 400) -> Iterator[list[Any]]:
    """PER SLICE OF 400: SQLite caps the parameters of one statement."""
    items = list(items)
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _epoch(moment: str) -> float:
    return datetime.fromisoformat(moment).timestamp()
