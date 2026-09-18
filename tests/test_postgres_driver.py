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

#: TABLE NAMES UNIQUE PER PROCESS: two runs against one database would
#: otherwise create the same table, and the second waits on the first's open
#: transaction until it ends.
_TABLES = (f"{os.getpid()}_{n}" for n in itertools.count())


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
        sa.Column("id", sa.BigInteger, sa.Identity(), primary_key=True),
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

    #: The page read — its rows and the clock with it — then the write,
    #: seeding the revisions it needs.
    claim_statements = (2, 0)

    #: Whatever the number of subjects. Arrive: their progress read, the
    #: skipped noted, the rest written. Keeping every ref: the locks, the
    #: check they are held, what waits read, one write per set of refs (two
    #: here), the ref let go and the merges noted. Migrate: versions,
    #: progress, revisions raised, repinned, the rows and the arrivals of the
    #: renamed node moved (delete, insert).
    batched_statements = {"arrive": (3, 0), "keep every ref": (7, 0), "migrate": (7, 0)}

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

    #: As the row layout, but a migration renames inside each subject's
    #: document, in the same UPDATE as the new pin.
    batched_statements = {"arrive": (3, 0), "keep every ref": (7, 0), "migrate": (5, 0)}

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

    #: The row layout's two, and the pairs struck from the list.
    claim_statements = (3, 0)

    #: The row layout's, and the renamed subjects listed again.
    batched_statements = {"arrive": (3, 0), "keep every ref": (7, 0), "migrate": (8, 0)}

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
        return {"claim": self.layout.claim_statements, **self.layout.batched_statements}

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


class _JsonAsText(_DocumentedResult):
    """An executor whose adapter returns JSON as text — asyncpg's default, a
    text loader, a host's shim: every list or dict in a row, as a string."""

    @staticmethod
    def _row(row):
        import json
        return None if row is None else tuple(
            json.dumps(v) if isinstance(v, (list, dict)) else v for v in row)

    def fetchall(self):
        return [self._row(r) for r in super().fetchall()]

    def fetchone(self):
        return self._row(super().fetchone())


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_json_returned_as_text_reads_as_json(engine, layout):
    """THE DRIVER TAKES AN `execute` CALLABLE to stay neutral about the
    adapter behind it: the JSON it reads back — a claim page's rows, a
    subject's document — must work decoded or as text."""
    from quazardous.grampy import Lane, Node
    dag = (Node("in", lane=Lane()), Node("a", parents=("in",)),
           Node("b", parents=("a",), optional=True), Node("c", parents=("b",)))
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        metadata.create_all(conn)
        driver = layout.driver(lambda statement: _JsonAsText(conn.execute(statement)),
                               tables, dag)
        journal = NodeJournal(driver, dag, clock=lambda: SEEDED)
        subjects = ["s1", "s2"]
        journal.arrive("in", subjects)
        assert journal.settle(ordered_subjects(subjects))["in"] == {"entered": 2}
        lease = journal.claim("a", 10, candidates=ordered_subjects(subjects))
        assert sorted(lease) == subjects, "the claim page's rows, read as text"
        journal.conclude("a", list(lease), token=lease.token)
        assert journal.skip("b", candidates=ordered_subjects(subjects)) == 2
        assert sorted(journal.claim("c", 10, candidates=ordered_subjects(subjects))) == subjects
        assert journal.progress_many(subjects)["s1"] == {
            "in": "done", "a": "done", "b": "skipped", "c": "running"}
        journal.arrive("in", ["s1"])
        conn.rollback()


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


def test_the_tables_the_documentation_declares_are_the_ones_the_driver_takes():
    """The docs' code is not run by the README test: build its tables here."""
    import re
    from pathlib import Path
    text = (Path(__file__).parent.parent / "docs" / "drivers.md").read_text()
    block = next(b for b in re.findall(r"```python\n(.*?)```", text, re.S)
                 if "arrivals = sa.Table" in b)
    declarations = block.split("journal = NodeJournal(")[0]
    scope = {"sa": sa, "metadata": sa.MetaData()}
    exec(declarations, scope)
    PostgresDriver(lambda statement: None, scope["nodes"], scope["revisions"],
                   scope["history"], subject="job_id", limits=scope["limits"],
                   arrivals=scope["arrivals"])


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_a_group_claims_without_a_limits_table(engine, layout):
    """A guard is an advisory lock: it needs no table. A graph with a group
    and no rate or concurrency is given none."""
    from quazardous.grampy import Group, Node
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = dict(layout.tables(metadata, f"grampy_{next(_TABLES)}"), limits=None)
        metadata.create_all(conn)
        dag = (Node("pack", group=Group(size=2)),)
        journal = NodeJournal(layout.driver(conn.execute, tables, dag), dag)
        lease = journal.claim("pack", 2, candidates=keyed_subjects([("a", "red"), ("b", "red")]))
        assert sorted(lease) == ["a", "b"]
        conn.rollback()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_candidates_with_a_limit_are_the_first_by_their_order(engine, layout):
    """A LIMIT picks rows by the ORDER BY it comes with, and must keep it."""
    from quazardous.grampy import Node
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        items = sa.Table(f"grampy_items_{next(_TABLES)}", metadata,
                         sa.Column("id", sa.Text), sa.Column("priority", sa.Integer))
        metadata.create_all(conn)
        # Written in the reverse of their priority, so that table order lies.
        conn.execute(sa.insert(items), [{"id": f"s{i}", "priority": i}
                                        for i in reversed(range(200))])
        dag = (Node("a"),)
        journal = NodeJournal(layout.driver(conn.execute, tables, dag), dag)
        top = sa.select(items.c.id).order_by(items.c.priority).limit(3)
        assert sorted(journal.claim("a", 10, candidates=top)) == ["s0", "s1", "s2"]
        conn.rollback()


def test_a_column_that_is_not_named_grampy_key_is_never_a_key(engine):
    """select(id, priority) used to group by priority, silently."""
    from quazardous.grampy import Group, Node
    layout = RowLayout()
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        items = sa.Table(f"grampy_items_{next(_TABLES)}", metadata,
                         sa.Column("id", sa.Text), sa.Column("priority", sa.Integer),
                         sa.Column("colour", sa.Text))
        metadata.create_all(conn)
        conn.execute(sa.insert(items), [{"id": "a", "priority": 1, "colour": "red"},
                                        {"id": "b", "priority": 1, "colour": "blue"}])
        dag = (Node("pack", group=Group(size=2)), Node("plain"))
        journal = NodeJournal(layout.driver(conn.execute, tables, dag), dag)
        by_priority = sa.select(items.c.id, items.c.priority).order_by(items.c.id)
        with pytest.raises(ValueError, match="grampy_key"):
            journal.claim("pack", 2, candidates=by_priority)
        assert sorted(journal.claim("plain", 2, candidates=by_priority)) == ["a", "b"], (
            "on a plain node the extra column is the query's own business")
        by_colour = sa.select(items.c.id, items.c.colour.label("grampy_key"))
        assert journal.claim("pack", 2, candidates=by_colour.order_by(items.c.id)) == [], (
            "red and blue are two keys: no group of two")
        conn.rollback()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
@pytest.mark.parametrize("versioned", [False, True], ids=["tuple", "graph"])
def test_skip_in_one_write_equals_the_loop(engine, layout, versioned):
    """`skip` WRITES: a fast path slightly wrong would let children start
    early. On every progress of the contract's sweep — scheduled rows and a
    subject of another graph version included — the one-write skip and the
    journal's own loop must skip exactly the same subjects."""
    from quazardous.grampy import Document, Graph, Status
    from quazardous.grampy.testing import DIAMOND, _progresses

    # The last subject would be skipped, were it not pinned to another graph.
    progresses = [*_progresses(), {"start": Status.DONE, "right": Status.SCHEDULED},
                  {"start": Status.DONE}]
    subjects = [f"s{i}" for i in range(len(progresses))]
    dag = Graph(Document("sweep"), DIAMOND) if versioned else DIAMOND

    def run(fast):
        with engine.connect() as conn, conn.begin():
            metadata = sa.MetaData()
            tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
            metadata.create_all(conn)
            driver = layout.driver(conn.execute, tables, DIAMOND)
            if not fast:
                driver.skip_where = None           # the journal falls back to its loop
            journal = NodeJournal(driver, dag, clock=lambda: SEEDED)
            for subject, progress in zip(subjects, progresses, strict=True):
                layout.seed(conn, tables, subject, progress, driver)
            pinned = subjects[-1]
            if versioned and "subjects" in tables:  # one subject belongs to another graph
                conn.execute(sa.update(tables["subjects"])
                             .where(tables["subjects"].c.subject == pinned)
                             .values(version="elsewhere"))
            elif versioned:
                conn.execute(sa.update(tables["revisions"])
                             .where(tables["revisions"].c.subject == pinned)
                             .values(version="elsewhere"))
                conn.execute(sa.insert(tables["revisions"]).from_select(
                    ["subject", "revision", "version"],
                    sa.select(sa.literal(pinned), sa.literal(0), sa.literal("elsewhere"))
                    .where(~sa.exists().where(tables["revisions"].c.subject == pinned))))
            count = journal.skip("right", candidates=ordered_subjects(subjects))
            after = {s: journal.progress(s) for s in subjects}
            conn.rollback()
            return count, after

    fast_count, fast = run(True)
    loop_count, loop = run(False)
    assert fast_count == loop_count
    assert fast == loop
    assert fast_count > 0, "the sweep must skip something, or it proves nothing"
    assert ("right" in fast[subjects[-1]]) is not versioned


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_on_the_server_clock_a_claim_reads_the_time_with_its_page(engine, layout):
    """THE CONTRACT INJECTS A CLOCK; a journal left on the driver's reads the
    server's time in the page query (`scan_reads_clock`). That time must be
    the one the claim judges by: a retry due long ago is taken, one due in a
    far future is not — and no statement is spent reading the time apart."""
    from quazardous.grampy.testing import DIAMOND
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        metadata.create_all(conn)
        driver = layout.driver(conn.execute, tables, DIAMOND)
        journal = NodeJournal(driver, DIAMOND)             # the server's clock
        for subject in ("past", "future"):
            layout.seed(conn, tables, subject, {"start": "scheduled"}, driver)
        ahead = "2999-01-01T00:00:00+00:00"
        if "subjects" in tables:
            t = tables["subjects"]
            conn.execute(sa.update(t).where(t.c.subject == "future").values(
                nodes=sa.func.jsonb_set(
                    t.c.nodes, sa.cast(["start", "started_at"], postgresql.ARRAY(sa.Text)),
                    sa.func.to_jsonb(sa.cast(ahead, sa.Text)))))
        else:
            t = tables["table"]
            conn.execute(sa.update(t).where(t.c.subject == "future").values(started_at=ahead))
        journal.claim("start", 1, candidates=ordered_subjects(["warm"]))
        sent = []
        sa.event.listen(conn, "before_cursor_execute", lambda *_: sent.append(1))
        taken = journal.claim("start", 10,
                              candidates=ordered_subjects(["past", "future", "fresh"]))
        assert sorted(taken) == ["fresh", "past"]
        assert len(sent) <= layout.claim_statements[0], "the time was read apart"
        started = journal.driver.now()
        assert journal.progress("past") == {"start": "running"}
        assert started >= "2026-01-01T00:00:00+00:00"
        conn.rollback()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_a_subject_is_pinned_by_its_first_write_and_never_again(engine, layout):
    """ON A GRAPH, the page says which subjects are pinned already: a claim
    pins only the others, so a subject deep in its run costs no pin."""
    from quazardous.grampy import Document, Graph
    from quazardous.grampy.testing import DIAMOND
    graph = Graph(Document("pins"), DIAMOND)
    subjects = [f"s{i}" for i in range(5)]
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        metadata.create_all(conn)
        journal = NodeJournal(layout.driver(conn.execute, tables, DIAMOND), graph,
                              clock=lambda: SEEDED)
        lease = journal.claim("start", 5, candidates=ordered_subjects(subjects))
        journal.conclude("start", list(lease), token=lease.token)
        assert {journal.pinned(s) for s in subjects} == {graph.document.identity}
        sent = []
        sa.event.listen(conn, "before_cursor_execute", lambda *_: sent.append(1))
        assert len(journal.claim("left", 5, candidates=ordered_subjects(subjects))) == 5
        assert len(sent) <= layout.claim_statements[0], "pinned again"
        conn.rollback()


@pytest.mark.parametrize("layout", [RowLayout(), SubjectLayout(), ReadyLayout()],
                         ids=["row", "subject", "ready"])
def test_a_claim_where_every_candidate_moved_past_reads_no_page_by_page(engine, layout):
    """THE WORST CASE OF THE DESCENDANT FILTER: every candidate waiting at
    the node has a row after it and none for it — a replay that left the
    later steps in place. Taken page by page, the claim read them all to
    take nothing; it must stop within two statements, however many."""
    from quazardous.grampy.testing import DIAMOND
    subjects = [f"s{i}" for i in range(600)]          # three pages' worth
    with engine.connect() as conn, conn.begin():
        metadata = sa.MetaData()
        tables = layout.tables(metadata, f"grampy_{next(_TABLES)}")
        metadata.create_all(conn)
        driver = layout.driver(conn.execute, tables, DIAMOND)
        journal = NodeJournal(driver, DIAMOND, clock=lambda: SEEDED)
        for subject in subjects:
            layout.seed(conn, tables, subject, {"start": "done", "end": "done"}, driver)
        journal.claim("left", 1, candidates=ordered_subjects(["warm"]))
        sent = []
        sa.event.listen(conn, "before_cursor_execute", lambda *_: sent.append(1))
        assert journal.claim("left", 30, candidates=ordered_subjects(subjects)) == []
        assert len(sent) <= 2, f"{len(sent)} statements to take nothing"
        conn.rollback()
