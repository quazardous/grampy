"""The SQLite driver passes the contract — standard library only, always run."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager

import pytest

from quazardous.grampy import NodeJournal
from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema
from quazardous.grampy.testing import JournalContract


def ordered_subjects(subjects):
    """An ordered `SELECT` over literal values."""
    if not subjects:
        return Query("SELECT NULL WHERE 0")
    values = ", ".join("(?, ?)" for _ in subjects)
    params = tuple(x for i, s in enumerate(subjects) for x in (s, i))
    return Query(f"SELECT column1 FROM (VALUES {values}) ORDER BY column2", params)


def keyed_subjects(pairs):
    """Candidates carrying a grouping key: subject, key, then the order."""
    pairs = list(pairs)
    if not pairs:
        return Query("SELECT NULL, NULL WHERE 0")
    values = ", ".join("(?, ?, ?)" for _ in pairs)
    params = tuple(x for i, (s, k) in enumerate(pairs) for x in (s, k, i))
    return Query(f"SELECT column1, column2 FROM (VALUES {values}) ORDER BY column3",
                 params)


def connect(path):
    return sqlite3.connect(path, timeout=30, check_same_thread=False)


class SqliteHarness:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.count = 0

    #: A claim reads the page's rows in two statements after the pre-filter,
    #: then writes one statement per subject: SQLite runs in-process, where a
    #: statement costs no round trip.
    statement_bounds = {"claim": (5, 1)}

    def journal(self, dag, clock, subject_type=str, mergers=None):
        conn = sqlite3.connect(":memory:")
        for statement in schema():
            conn.execute(statement)
        self.conn = conn
        return NodeJournal(SqliteDriver(conn), dag, clock=clock, mergers=mergers)

    @contextmanager
    def statements(self):
        sent = []
        self.conn.set_trace_callback(sent.append)
        try:
            yield lambda: len(sent)
        finally:
            self.conn.set_trace_callback(None)

    def journal_on(self, journal, dag, clock):
        return NodeJournal(SqliteDriver(journal.driver.conn), dag, clock=clock)

    def candidates(self, subjects):
        # Past SQLite's cap on bound variables, the subjects themselves: the
        # driver takes any iterable, in its order.
        return list(subjects) if len(subjects) > 1000 else ordered_subjects(subjects)

    def keyed(self, pairs):
        return keyed_subjects(pairs)

    def seed(self, journal, subject, progress):
        for name, status in progress.items():
            journal.driver.conn.execute(
                "INSERT INTO grampy_nodes (subject, node, status, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (subject, name, status, "2026-01-01T00:00:00+00:00",
                 None if status == "running" else "2026-01-01T00:00:00+00:00"))

    def parents_concluded(self, journal, name, subject):
        return journal.parents_concluded(name, subject)

    def store(self, dag, clock):
        self.count += 1
        return SqliteStore(self.tmp_path / f"shared_{self.count}.db", dag, clock)


class SqliteStore:
    """A database FILE, so that every session opens its own connection."""

    def __init__(self, path, dag, clock):
        self.path, self.dag, self.clock = path, dag, clock
        conn = connect(path)
        for statement in schema():
            conn.execute(statement)
        conn.commit()
        conn.close()

    def session(self):
        return SqliteSession(self)

    def close(self):
        self.path.unlink(missing_ok=True)


class SqliteSession:
    def __init__(self, store):
        self.conn = connect(store.path)
        self.journal = NodeJournal(SqliteDriver(self.conn), store.dag, clock=store.clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects)

    def keyed(self, pairs):
        return keyed_subjects(pairs)

    def commit(self):
        self.conn.commit()
        self.conn.close()

    def rollback(self):
        self.conn.rollback()
        self.conn.close()


class TestSqliteDriver(JournalContract):
    @pytest.fixture
    def harness(self, tmp_path):
        return SqliteHarness(tmp_path)


def test_names_that_are_not_identifiers_are_refused():
    with pytest.raises(ValueError, match="identifier"):
        SqliteDriver(sqlite3.connect(":memory:"), table="nodes; DROP TABLE x")
    with pytest.raises(ValueError, match="identifier"):
        schema(subject="a b")


def test_candidates_may_be_plain_subjects():
    conn = sqlite3.connect(":memory:")
    for statement in schema():
        conn.execute(statement)
    from quazardous.grampy import Node
    journal = NodeJournal(SqliteDriver(conn), (Node("a"),))
    assert sorted(journal.claim("a", 5, candidates=["x", "y"])) == ["x", "y"]


def test_the_subject_column_may_carry_the_application_s_own_name():
    """The column is the application's: grampy must not impose `subject`."""
    conn = sqlite3.connect(":memory:")
    for statement in schema(subject="record_id"):
        conn.execute(statement)
    from quazardous.grampy import Node
    journal = NodeJournal(SqliteDriver(conn, subject="record_id"),
                          (Node("a"), Node("b", parents=("a",))))
    lease = journal.claim("a", 5, candidates=["x"])
    journal.conclude("a", ["x"], token=lease.token)
    assert journal.claim("b", 5, candidates=["x"]) == ["x"]
    assert [row[0] for row in conn.execute("SELECT record_id FROM grampy_nodes")] == ["x", "x"]


def test_the_subject_column_may_be_typed_and_then_serves_a_join_by_its_index():
    """Untyped, the column holds ints and strings alike, but SQLite cannot
    search it from a typed column of the application's: it scans."""
    def plan(statements):
        conn = sqlite3.connect(":memory:")
        for statement in statements:
            conn.execute(statement)
        conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY)")
        return " ".join(row[3] for row in conn.execute(
            "EXPLAIN QUERY PLAN SELECT d.id FROM docs d WHERE NOT EXISTS "
            "(SELECT 1 FROM grampy_nodes n WHERE n.subject = d.id AND n.node = 'a')"))

    assert "SEARCH n" in plan(schema(subject_type="INTEGER"))
    assert "SEARCH n" not in plan(schema()), "the trap the type avoids"


def test_a_typed_table_works_like_the_untyped_one():
    conn = sqlite3.connect(":memory:")
    for statement in schema(subject_type="integer"):
        conn.execute(statement)
    from quazardous.grampy import Node
    journal = NodeJournal(SqliteDriver(conn), (Node("a"), Node("b", parents=("a",))))
    lease = journal.claim("a", 5, candidates=[3, 1, 2])
    journal.conclude("a", list(lease), token=lease.token)
    assert sorted(journal.claim("b", 5, candidates=[3, 1, 2])) == [1, 2, 3]


def test_an_unknown_subject_type_is_refused():
    with pytest.raises(ValueError, match="subject_type"):
        schema(subject_type="REAL")
