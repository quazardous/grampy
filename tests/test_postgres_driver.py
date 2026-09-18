"""The postgres driver passes the contract — against a real server.

Skipped unless `GRAMPY_TEST_PG_DSN` names a database, e.g.
`postgresql+psycopg://user:pass@localhost/grampy_test`. Each journal gets a
fresh table in one transaction that is rolled back at the end: nothing is
left behind.
"""
from __future__ import annotations

import itertools
import os
from contextlib import contextmanager

import pytest

DSN = os.environ.get("GRAMPY_TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="GRAMPY_TEST_PG_DSN is not set")

sa = pytest.importorskip("sqlalchemy")

from sqlalchemy.dialects import postgresql  # noqa: E402
from sqlalchemy.dialects.postgresql import JSONB  # noqa: E402

from quazardous.grampy import NodeJournal  # noqa: E402
from quazardous.grampy.drivers.postgres import PostgresDriver  # noqa: E402
from quazardous.grampy.drivers.postgres_ready import PostgresReadyDriver  # noqa: E402
from quazardous.grampy.drivers.postgres_subject import PostgresSubjectDriver  # noqa: E402
from quazardous.grampy.testing import JournalContract  # noqa: E402

_TABLES = itertools.count()


def _type(subject_type):
    return sa.BigInteger if subject_type is int else sa.Text


def node_table(metadata, name, subject_type=str):
    """A node table with the default subject column name."""
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


def arrivals_table(metadata, name, subject_type=str):
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), primary_key=True),
        sa.Column("node", sa.Text, primary_key=True),
        sa.Column("ref", sa.Text),
        sa.Column("place", sa.Text, nullable=False),
        sa.Column("arrived_at", sa.Text, nullable=False),
        sa.Column("urgent", sa.Boolean, nullable=False),
        sa.Column("refs", sa.Text))


def revision_table(metadata, name, subject_type=str):
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), primary_key=True),
        sa.Column("revision", sa.Integer, nullable=False),
        sa.Column("policy", sa.Text),
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


def keyed_subjects(pairs, subject_type=str):
    """Candidates carrying a grouping key: subject, key, then the order."""
    kind = _type(subject_type)
    pairs = list(pairs)
    if not pairs:
        return sa.select(sa.cast(sa.null(), kind).label("subject"),
                         sa.cast(sa.null(), sa.Text).label("grampy_key")).where(sa.false())
    values = sa.values(sa.column("subject", kind), sa.column("grampy_key", sa.Text),
                       sa.column("rank", sa.Integer),
                       name="candidates").data([(s, k, i) for i, (s, k) in enumerate(pairs)])
    return sa.select(values.c.subject, values.c.grampy_key).order_by(values.c.rank)


def subject_table(metadata, name, subject_type=str):
    """The one table of the row-per-subject layout."""
    return sa.Table(
        name, metadata,
        sa.Column("subject", _type(subject_type), primary_key=True),
        sa.Column("revision", sa.Integer, nullable=False),
        sa.Column("policy", sa.Text),
        sa.Column("version", sa.Text),
        sa.Column("nodes", JSONB, nullable=False, server_default="{}"))


class RowLayout:
    """One row per (subject, node): `PostgresDriver`."""

    #: A page read, its rows, the revisions seeded, the write.
    claim_statements = (4, 0)

    def tables(self, metadata, prefix, subject_type=str):
        return {"table": node_table(metadata, f"{prefix}_nodes", subject_type),
                "revisions": revision_table(metadata, f"{prefix}_revisions", subject_type),
                "history": history_table(metadata, f"{prefix}_history", subject_type),
                "limits": limits_table(metadata, f"{prefix}_limits"),
                "arrivals": arrivals_table(metadata, f"{prefix}_arrivals", subject_type)}

    def driver(self, execute, tables, dag):
        return PostgresDriver(execute, tables["table"], tables["revisions"],
                              tables["history"], subject="subject",
                              limits=tables["limits"], arrivals=tables["arrivals"])

    def seed(self, conn, tables, subject, progress, driver=None):
        for name, status in progress.items():
            conn.execute(sa.insert(tables["table"]).values(
                subject=subject, node=name, status=status, started_at=SEEDED,
                finished_at=None if status == "running" else SEEDED))


class SubjectLayout:
    """One row per subject, progress in JSONB: `PostgresSubjectDriver`."""

    #: A page read with its progress, then one upsert.
    claim_statements = (2, 0)

    def tables(self, metadata, prefix, subject_type=str):
        return {"subjects": subject_table(metadata, f"{prefix}_subjects", subject_type),
                "history": history_table(metadata, f"{prefix}_history", subject_type),
                "limits": limits_table(metadata, f"{prefix}_limits"),
                "arrivals": arrivals_table(metadata, f"{prefix}_arrivals", subject_type)}

    def driver(self, execute, tables, dag):
        return PostgresSubjectDriver(execute, tables["subjects"], tables["history"],
                                     subject="subject", limits=tables["limits"],
                                     arrivals=tables["arrivals"])

    def seed(self, conn, tables, subject, progress, driver=None):
        rows = {name: {"status": status, "started_at": SEEDED,
                       "finished_at": None if status == "running" else SEEDED,
                       "lease": None}
                for name, status in progress.items()}
        t = tables["subjects"]
        insert = postgresql.insert(t).values(subject=subject, revision=0, nodes=rows)
        conn.execute(insert.on_conflict_do_update(
            index_elements=["subject"],
            set_={"nodes": t.c.nodes.op("||", return_type=JSONB)(insert.excluded.nodes)}))


SEEDED = "2026-01-01T00:00:00+00:00"


class ReadyLayout(RowLayout):
    """Row per node plus the ready list: `PostgresReadyDriver`. Seeded rows
    are written behind the driver's back, so the list is refilled after."""

    #: The row layout's four, and the pairs struck from the list.
    claim_statements = (5, 0)

    def tables(self, metadata, prefix, subject_type=str):
        tables = super().tables(metadata, prefix, subject_type)
        tables["ready"] = sa.Table(
            f"{prefix}_ready", metadata,
            sa.Column("subject", _type(subject_type), primary_key=True),
            sa.Column("node", sa.Text, primary_key=True))
        return tables

    def driver(self, execute, tables, dag):
        return PostgresReadyDriver(execute, tables["table"], tables["revisions"],
                                   tables["history"], tables["ready"], graph=dag,
                                   subject="subject", limits=tables["limits"],
                                   arrivals=tables["arrivals"])

    def seed(self, conn, tables, subject, progress, driver):
        super().seed(conn, tables, subject, progress, driver)
        driver.refill()




class PostgresHarness:
    def __init__(self, conn, layout):
        self.conn = conn
        self.layout = layout
        self.subject_type = str
        self._tables = {}

    def journal(self, dag, clock, subject_type=str, mergers=None):
        self.subject_type = subject_type
        metadata = sa.MetaData()
        tables = self.layout.tables(metadata, f"grampy_{next(_TABLES)}", subject_type)
        metadata.create_all(self.conn)
        driver = self.layout.driver(self.conn.execute, tables, dag)
        self._tables[id(driver)] = tables
        return NodeJournal(driver, dag, clock=clock, mergers=mergers)

    def journal_on(self, journal, dag, clock):
        tables = self._tables[id(journal.driver)]
        driver = self.layout.driver(self.conn.execute, tables, dag)
        self._tables[id(driver)] = tables
        return NodeJournal(driver, dag, clock=clock)

    def candidates(self, subjects):
        if len(subjects) <= 1000:
            return ordered_subjects(subjects, self.subject_type)
        # PAST THE PARAMETER CAP, a table: what an application's candidates
        # are anyway.
        metadata = sa.MetaData()
        table = sa.Table(f"grampy_candidates_{next(_TABLES)}", metadata,
                         sa.Column("subject", _type(self.subject_type)),
                         sa.Column("rank", sa.Integer, primary_key=True))
        metadata.create_all(self.conn)
        self.conn.execute(sa.insert(table), [{"subject": x, "rank": i}
                                             for i, x in enumerate(subjects)])
        return sa.select(table.c.subject).order_by(table.c.rank)

    @property
    def statement_bounds(self):
        return {"claim": self.layout.claim_statements}

    @contextmanager
    def statements(self):
        sent = []

        def seen(*_):
            sent.append(1)

        sa.event.listen(self.conn, "before_cursor_execute", seen)
        try:
            yield lambda: len(sent)
        finally:
            sa.event.remove(self.conn, "before_cursor_execute", seen)

    def keyed(self, pairs):
        return keyed_subjects(pairs, self.subject_type)

    def seed(self, journal, subject, progress):
        self.layout.seed(self.conn, self._tables[id(journal.driver)], subject, progress,
                         journal.driver)

    def parents_concluded(self, journal, name, subject):
        return bool(self.conn.execute(
            sa.select(journal.parents_concluded(name, subject))).fetchone()[0])

    def store(self, dag, clock):
        return PostgresStore(self.conn.engine, self.layout, dag, clock)


class PostgresStore:
    """Tables COMMITTED on their own connection, so that sessions on other
    connections see them; dropped at `close`."""

    def __init__(self, engine, layout, dag, clock):
        self.engine, self.layout, self.dag, self.clock = engine, layout, dag, clock
        self.metadata = sa.MetaData()
        self.tables = layout.tables(self.metadata, f"grampy_shared_{next(_TABLES)}")
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
            store.layout.driver(self.conn.execute, store.tables, store.dag),
            store.dag, clock=store.clock)

    def candidates(self, subjects):
        return ordered_subjects(subjects)

    def keyed(self, pairs):
        return keyed_subjects(pairs)

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


class _OnPostgres(JournalContract):
    layout: object

    @pytest.mark.xfail(strict=True, reason=(
        "every PostgreSQL layout binds a value per subject and fails past "
        "psycopg's 65,535-parameter cap; binding arrays fixes it — then this "
        "passes and the marker must go"))
    def test_one_call_handles_tens_of_thousands_of_subjects(self, harness, clock):
        super().test_one_call_handles_tens_of_thousands_of_subjects(harness, clock)

    @pytest.fixture
    def harness(self, engine):
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                yield PostgresHarness(conn, self.layout)
            finally:
                transaction.rollback()


class TestPostgresDriver(_OnPostgres):
    layout = RowLayout()


class TestPostgresSubjectDriver(_OnPostgres):
    layout = SubjectLayout()


class TestPostgresReadyDriver(_OnPostgres):
    layout = ReadyLayout()


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


def test_a_lane_without_an_arrivals_table_says_so():
    from quazardous.grampy import Lane, Node
    metadata = sa.MetaData()
    driver = PostgresDriver(lambda statement: None, node_table(metadata, "nodes"),
                            revision_table(metadata, "revisions"),
                            history_table(metadata, "history"), subject="subject")
    journal = NodeJournal(driver, (Node("in", lane=Lane()),), clock=lambda: "2026-01-01")
    with pytest.raises(ValueError, match="arrivals"):
        journal.driver.arrivals(["s1"], "in")


class _DocumentedResult:
    """What the driver's docstring promises of a result, and nothing more."""

    def __init__(self, result):
        self._result = result
        self.rowcount = result.rowcount

    def fetchall(self):
        return self._result.fetchall()

    def fetchone(self):
        return self._result.fetchone()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_the_driver_needs_only_the_result_methods_it_documents(engine, layout):
    """An application executing through its own driver returns what the
    docstring lists — `fetchall`, `fetchone`, `rowcount` — and no more."""
    from quazardous.grampy import Lane, Node
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        metadata.create_all(conn)
        dag = (Node("in", lane=Lane()), Node("a", parents=("in",), concurrency=1))
        driver = layout.driver(lambda statement: _DocumentedResult(conn.execute(statement)),
                               tables, dag)
        journal = NodeJournal(driver, dag)
        assert driver.now()
        assert driver.running("a", None) == 0
        journal.arrive("in", ["s1"])
        assert driver.queued("in") == 1
        conn.rollback()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_an_autocommitting_executor_is_refused(engine, layout):
    """Under autocommit every advisory lock is released by the next statement
    and a revision raised is committed before the rows go: measured, and
    silent. The driver refuses at its first write, and a guard checks its
    locks are still held."""
    from quazardous.grampy import Node
    metadata = sa.MetaData()
    tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
    metadata.create_all(engine)
    try:
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            dag = (Node("a", concurrency=1),)
            driver = layout.driver(conn.execute, tables, dag)
            journal = NodeJournal(driver, dag)
            with pytest.raises(RuntimeError, match="outside a transaction"):
                journal.forget("a", ["s1"])
            driver._in_transaction = True      # past the first check: the guard's own
            with pytest.raises(RuntimeError, match="outside a transaction"):
                journal.claim("a", 1, candidates=ordered_subjects(["s1"]))
    finally:
        metadata.drop_all(engine)
