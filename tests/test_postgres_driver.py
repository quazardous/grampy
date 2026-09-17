"""The postgres driver passes the contract — against a real server.

Skipped unless `GRAMPY_TEST_PG_DSN` names a database, e.g.
`postgresql+psycopg://user:pass@localhost/grampy_test`. Each journal gets a
fresh table in one transaction that is rolled back at the end: nothing is
left behind.
"""
from __future__ import annotations

import itertools
import os

import pytest

DSN = os.environ.get("GRAMPY_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="GRAMPY_TEST_PG_DSN is not set")

sa = pytest.importorskip("sqlalchemy")

from quazardous.grampy import NodeJournal  # noqa: E402
from quazardous.grampy.drivers.postgres import PostgresDriver  # noqa: E402
from quazardous.grampy.testing import JournalContract  # noqa: E402

_TABLES = itertools.count()


def _type(subject_type):
    return sa.BigInteger if subject_type is int else sa.Text


def node_table(metadata, name, subject_type=str):
    """A node table whose subject column is NOT called `request_id`."""
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), primary_key=True),
        sa.Column("node", sa.Text, primary_key=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text),
        sa.Column("lease", sa.Text))


def history_table(metadata, name, subject_type=str):
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), nullable=False),
        sa.Column("node", sa.Text, nullable=False),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text),
        sa.Column("lease", sa.Text),
        sa.Column("archived_at", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False))


def limits_table(metadata, name):
    return sa.Table(
        name, metadata,
        sa.Column("key", sa.Text, primary_key=True),
        sa.Column("value", sa.Float))


def revision_table(metadata, name, subject_type=str):
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), primary_key=True),
        sa.Column("revision", sa.Integer, nullable=False),
        sa.Column("channel", sa.Text),
        sa.Column("version", sa.Text))


def ordered_subjects(subjects, subject_type=str):
    """An ordered `SELECT subject` over literal values, typed like the
    subject column — even when empty, or PostgreSQL refuses to compare."""
    kind = _type(subject_type)
    if not subjects:
        return sa.select(sa.cast(sa.null(), kind).label("subject")).where(sa.false())
    values = sa.values(sa.column("subject", kind), sa.column("rank", sa.Integer),
                       name="candidates").data([(s, i) for i, s in enumerate(subjects)])
    return sa.select(values.c.subject).order_by(values.c.rank)


class PostgresHarness:
    def __init__(self, conn):
        self.conn = conn
        self.subject_type = str

    def journal(self, dag, clock, subject_type=str):
        self.subject_type = subject_type
        metadata, n = sa.MetaData(), next(_TABLES)
        table = node_table(metadata, f"grampy_nodes_{n}", subject_type)
        revisions = revision_table(metadata, f"grampy_revisions_{n}", subject_type)
        history = history_table(metadata, f"grampy_history_{n}", subject_type)
        limits = limits_table(metadata, f"grampy_limits_{n}")
        metadata.create_all(self.conn)
        return NodeJournal(
            PostgresDriver(self.conn.execute, table, revisions, history, subject="subject",
                           limits=limits),
            dag, clock=clock)

    def journal_on(self, journal, dag, clock):
        d = journal.driver
        return NodeJournal(PostgresDriver(self.conn.execute, d.table, d.revisions,
                                          d.history_table, subject="subject",
                                          limits=d.limits_table),
                           dag, clock=clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects, self.subject_type)

    def seed(self, journal, subject, progress):
        for name, status in progress.items():
            self.conn.execute(sa.insert(journal.driver.table).values(
                subject=subject, node=name, status=status,
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None if status == "running" else "2026-01-01T00:00:00+00:00"))

    def parents_concluded(self, journal, name, subject):
        return bool(self.conn.execute(
            sa.select(journal.parents_concluded(name, subject))).scalar())

    def store(self, dag, clock):
        return PostgresStore(self.conn.engine, dag, clock)


class PostgresStore:
    """Tables COMMITTED on their own connection, so that sessions on other
    connections see them; dropped at `close`."""

    def __init__(self, engine, dag, clock):
        self.engine, self.dag, self.clock = engine, dag, clock
        self.metadata, n = sa.MetaData(), next(_TABLES)
        self.table = node_table(self.metadata, f"grampy_shared_{n}")
        self.revisions = revision_table(self.metadata, f"grampy_shared_revisions_{n}")
        self.history = history_table(self.metadata, f"grampy_shared_history_{n}")
        self.limits = limits_table(self.metadata, f"grampy_shared_limits_{n}")
        self.metadata.create_all(engine)

    def session(self):
        return PostgresSession(self)

    def close(self):
        self.metadata.drop_all(self.engine)


class PostgresSession:
    def __init__(self, store):
        self.conn = store.engine.connect()
        self.transaction = self.conn.begin()
        self.journal = NodeJournal(
            PostgresDriver(self.conn.execute, store.table, store.revisions,
                           store.history, subject="subject", limits=store.limits),
            store.dag, clock=store.clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects)

    def commit(self):
        self.transaction.commit()
        self.conn.close()

    def rollback(self):
        self.transaction.rollback()
        self.conn.close()


@pytest.fixture(scope="module")
def engine():
    engine = sa.create_engine(DSN, pool_size=10, max_overflow=20)
    yield engine
    engine.dispose()


class TestPostgresDriver(JournalContract):
    @pytest.fixture
    def harness(self, engine):
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                yield PostgresHarness(conn)
            finally:
                transaction.rollback()


def test_a_table_without_the_node_columns_is_refused():
    metadata = sa.MetaData()
    table = sa.Table("bad", metadata, sa.Column("subject", sa.Text))
    revisions = revision_table(metadata, "revisions")
    history = history_table(metadata, "history")
    with pytest.raises(ValueError, match="lacks the column"):
        PostgresDriver(lambda statement: None, table, revisions, history, subject="subject")


def test_a_revisions_table_without_its_column_is_refused():
    metadata = sa.MetaData()
    table = node_table(metadata, "nodes")
    revisions = sa.Table("bad", metadata, sa.Column("subject", sa.Text))
    history = history_table(metadata, "history")
    with pytest.raises(ValueError, match="lacks the column"):
        PostgresDriver(lambda statement: None, table, revisions, history, subject="subject")
