"""The SQLite driver passes the contract — standard library only, always run."""
from __future__ import annotations

import sqlite3

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


def connect(path):
    return sqlite3.connect(path, timeout=30, check_same_thread=False)


class SqliteHarness:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.count = 0

    def journal(self, dag, clock, subject_type=str):
        conn = sqlite3.connect(":memory:")
        for statement in schema():
            conn.execute(statement)
        return NodeJournal(SqliteDriver(conn), dag, clock=clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects)

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
