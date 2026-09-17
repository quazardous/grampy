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

from grampy import NodeJournal  # noqa: E402
from grampy.drivers.postgres import PostgresDriver  # noqa: E402
from grampy.testing import JournalContract  # noqa: E402

_TABLES = itertools.count()


def node_table(metadata, name):
    """A node table whose subject column is NOT called `request_id`."""
    return sa.Table(
        name, metadata,
        sa.Column("subject", sa.Text, primary_key=True),
        sa.Column("node", sa.Text, primary_key=True),
        sa.Column("status", sa.Text, nullable=False),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("finished_at", sa.Text))


def ordered_subjects(subjects):
    """An ordered `SELECT subject` over literal values."""
    if not subjects:
        return sa.select(sa.cast(sa.null(), sa.Text).label("subject")).where(sa.false())
    values = sa.values(sa.column("subject", sa.Text), sa.column("rank", sa.Integer),
                       name="candidates").data([(s, i) for i, s in enumerate(subjects)])
    return sa.select(values.c.subject).order_by(values.c.rank)


class PostgresHarness:
    def __init__(self, conn):
        self.conn = conn

    def journal(self, dag, clock):
        table = node_table(sa.MetaData(), f"grampy_nodes_{next(_TABLES)}")
        table.create(self.conn)
        return NodeJournal(PostgresDriver(self.conn.execute, table, subject="subject"),
                           dag, clock=clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects)

    def seed(self, journal, subject, progress):
        for name, status in progress.items():
            self.conn.execute(sa.insert(journal.driver.table).values(
                subject=subject, node=name, status=status,
                started_at="2026-01-01T00:00:00+00:00",
                finished_at=None if status == "running" else "2026-01-01T00:00:00+00:00"))

    def parents_concluded(self, journal, name, subject):
        return bool(self.conn.execute(
            sa.select(journal.parents_concluded(name, subject))).scalar())


@pytest.fixture(scope="module")
def engine():
    engine = sa.create_engine(DSN)
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
    table = sa.Table("bad", sa.MetaData(), sa.Column("subject", sa.Text))
    with pytest.raises(ValueError, match="lacks the column"):
        PostgresDriver(lambda statement: None, table, subject="subject")
