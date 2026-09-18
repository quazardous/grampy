"""THE THREE POSTGRESQL LAYOUTS, SIDE BY SIDE, ON THE SAME WORK.

    GRAMPY_BENCH_PG_DSN=postgresql+psycopg://user:pass@localhost/bench \\
        uv run --with sqlalchemy --with 'psycopg[binary]' \\
        python benchmarks/layouts.py [subjects ...]

Each layout gets its own tables, filled with the same progress straight in
SQL (a journal would take hours to write 100,000 subjects), then runs the
same operations. Every operation runs inside a savepoint rolled back after
it, so each repetition starts from the same state. For each one: the
statements sent — counted at the cursor, which is what a round trip costs
on a real network — and the median wall time of five runs.

A local server has no network in between: its times rank the layouts, they
do not predict a remote one. The statement counts transfer as they are.

The tables are created in a schema of their own, dropped at the end.
"""
from __future__ import annotations

import os
import statistics
import sys
import time
from collections.abc import Callable
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from quazardous.grampy import Node, NodeJournal
from quazardous.grampy.drivers.postgres import PostgresDriver
from quazardous.grampy.drivers.postgres_ready import PostgresReadyDriver
from quazardous.grampy.drivers.postgres_subject import PostgresSubjectDriver

SCHEMA = "grampy_bench"
AT = "2026-01-01T00:00:00+00:00"
RUNS = 5

#: A chain with an optional step: fetch → parse → (enrich) → match → publish.
GRAPH = (
    Node("fetch"),
    Node("parse", parents=("fetch",)),
    Node("enrich", parents=("parse",), optional=True),
    Node("match", parents=("enrich",)),
    Node("publish", parents=("match",)),
)

#: WHAT THE TABLE HOLDS before any operation, as `(node, status, which
#: subjects)` — `i` is the subject, 0-based. Every subject went through
#: fetch and parse; half were enriched, a tenth of those matched, one in a
#: hundred published — the shape of a backlog deep in a pipeline. A third
#: of those enriched then came back for a replay of `parse`: its row is gone,
#: the later ones stay.
STATE = (
    ("fetch", "done", "true"),
    ("parse", "done", "i % 3 <> 0 OR i % 2 = 1"),
    ("enrich", "done", "i % 2 = 0"),
    ("match", "done", "i % 20 = 0"),
    ("publish", "done", "i % 200 = 0"),
)


def tables(metadata: sa.MetaData) -> dict[str, sa.Table]:
    s = SCHEMA
    t = {
        "items": sa.Table("items", metadata, sa.Column("id", sa.BigInteger, primary_key=True),
                          schema=s),
        "history": sa.Table(
            "history", metadata, sa.Column("subject", sa.BigInteger, nullable=False),
            *[sa.Column(c, sa.Text) for c in ("node", "status", "started_at",
                                                "finished_at", "lease", "archived_at",
                                                "reason")], schema=s),
    }
    for layout in ("row", "ready"):
        t[f"{layout}_nodes"] = sa.Table(
            f"{layout}_nodes", metadata,
            sa.Column("subject", sa.BigInteger, primary_key=True),
            sa.Column("node", sa.Text, primary_key=True),
            sa.Column("status", sa.Text, nullable=False),
            sa.Column("started_at", sa.Text, nullable=False),
            sa.Column("finished_at", sa.Text), sa.Column("lease", sa.Text), schema=s)
        t[f"{layout}_revisions"] = sa.Table(
            f"{layout}_revisions", metadata,
            sa.Column("subject", sa.BigInteger, primary_key=True),
            sa.Column("revision", sa.Integer, nullable=False),
            sa.Column("policy", sa.Text), sa.Column("version", sa.Text), schema=s)
    t["ready"] = sa.Table("ready", metadata,
                          sa.Column("subject", sa.BigInteger, primary_key=True),
                          sa.Column("node", sa.Text, primary_key=True), schema=s)
    t["subjects"] = sa.Table(
        "subjects", metadata, sa.Column("subject", sa.BigInteger, primary_key=True),
        sa.Column("revision", sa.Integer, nullable=False), sa.Column("policy", sa.Text),
        sa.Column("version", sa.Text),
        sa.Column("nodes", JSONB, nullable=False, server_default="{}"), schema=s)
    return t


def fill(conn: sa.Connection, n: int) -> None:
    """The same progress in the three layouts, in SQL."""
    q = f'"{SCHEMA}"'
    run = lambda sql: conn.execute(sa.text(sql))  # noqa: E731
    run(f"INSERT INTO {q}.items SELECT i FROM generate_series(0, {n - 1}) i")
    for layout in ("row", "ready"):
        run(f"INSERT INTO {q}.{layout}_revisions SELECT i, 0 FROM generate_series(0, {n - 1}) i")
        for node, status, which in STATE:
            run(f"INSERT INTO {q}.{layout}_nodes SELECT i, '{node}', '{status}', '{AT}', "
                f"'{AT}' FROM generate_series(0, {n - 1}) i WHERE {which}")
    run(f"INSERT INTO {q}.subjects SELECT i, 0, NULL, NULL, '{{}}' "
        f"FROM generate_series(0, {n - 1}) i")
    for node, status, which in STATE:
        run(f"UPDATE {q}.subjects SET nodes = nodes || jsonb_build_object('{node}', "
            f"jsonb_build_object('status', '{status}', 'started_at', '{AT}', "
            f"'finished_at', '{AT}', 'lease', NULL)) WHERE {which.replace('i', 'subject')}")
    run(f"ANALYZE {q}.items; ANALYZE {q}.row_nodes; ANALYZE {q}.ready_nodes; "
        f"ANALYZE {q}.subjects; ANALYZE {q}.row_revisions; ANALYZE {q}.ready_revisions")


def journals(conn: sa.Connection, t: dict[str, sa.Table]) -> dict[str, NodeJournal]:
    ex = conn.execute
    ready = PostgresReadyDriver(ex, t["ready_nodes"], t["ready_revisions"], t["history"],
                                t["ready"], graph=GRAPH)
    ready.refill()
    conn.execute(sa.text(f'ANALYZE "{SCHEMA}".ready'))
    return {
        "row per node": NodeJournal(PostgresDriver(ex, t["row_nodes"], t["row_revisions"],
                                                   t["history"]), GRAPH),
        "row per subject": NodeJournal(PostgresSubjectDriver(ex, t["subjects"],
                                                             t["history"]), GRAPH),
        "ready list": NodeJournal(ready, GRAPH),
    }


def measure(conn: sa.Connection, work: Callable[[], Any]) -> tuple[int, float, Any]:
    """Statements and median milliseconds of `work`, each run rolled back."""
    count = [0]

    def seen(*_: Any) -> None:
        count[0] += 1

    times, result = [], None
    for _ in range(RUNS):
        count[0] = 0
        savepoint = conn.begin_nested()
        sa.event.listen(conn, "before_cursor_execute", seen)
        started = time.perf_counter()
        try:
            result = work()
        except sa.exc.DBAPIError as error:
            # A FAILURE IS A RESULT: said in the table, not hidden by a crash.
            return count[0], float("nan"), f"fails: {str(error.orig).splitlines()[0]}"
        finally:
            times.append((time.perf_counter() - started) * 1000)
            sa.event.remove(conn, "before_cursor_execute", seen)
            savepoint.rollback()
    return count[0], statistics.median(times), result


def operations(j: NodeJournal, items: sa.Table, n: int) -> dict[str, Callable[[], Any]]:
    everyone = sa.select(items.c.id).order_by(items.c.id)

    def claim(name: str, limit: int = 30) -> Callable[[], int]:
        return lambda: len(j.claim(name, limit, candidates=everyone))

    def claim_and_conclude(name: str) -> Callable[[], int]:
        def run() -> int:
            lease = j.claim(name, 30, candidates=everyone)
            return j.conclude(name, list(lease), token=lease.token)
        return run

    return {
        "claim 30 at the entry (fetch, nothing to do)": claim("fetch"),
        "claim 30 at parse (a third replayed, past it)": claim("parse"),
        "claim 30 at match (half ready)": claim("match"),
        "claim 30 at publish (1 in 20 ready)": claim("publish"),
        "claim + conclude 30 at publish": claim_and_conclude("publish"),
        "forget parse on 30 subjects": lambda: j.forget("parse", list(range(0, 90, 3))),
        f"skip sweep of enrich over all {n:,}": lambda: j.skip("enrich", candidates=everyone),
    }


def main(sizes: list[int]) -> None:
    dsn = os.environ.get("GRAMPY_BENCH_PG_DSN")
    if not dsn:
        sys.exit("set GRAMPY_BENCH_PG_DSN to a PostgreSQL database you can write to")
    engine = sa.create_engine(dsn)
    for n in sizes:
        with engine.connect() as conn:
            conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
            conn.execute(sa.text(f'CREATE SCHEMA "{SCHEMA}"'))
            metadata = sa.MetaData()
            t = tables(metadata)
            metadata.create_all(conn)
            fill(conn, n)
            conn.commit()
            with conn.begin():
                js = journals(conn, t)
                rows: dict[str, dict[str, tuple[int, float, Any]]] = {}
                for layout, j in js.items():
                    for label, work in operations(j, t["items"], n).items():
                        rows.setdefault(label, {})[layout] = measure(conn, work)
            conn.execute(sa.text(f'DROP SCHEMA "{SCHEMA}" CASCADE'))
            conn.commit()
        print(f"\n### {n:,} subjects\n")
        print("| operation | " + " | ".join(js) + " |")
        print("|---|" + "---:|" * len(js))
        for label, by in rows.items():
            results = {r[2] for r in by.values()}
            flag = "" if len(results) == 1 else f" ⚠ results differ: {sorted(map(str, results))}"
            print(f"| {label}{flag} | " + " | ".join(
                by[k][2] if isinstance(by[k][2], str) and by[k][2].startswith("fails")
                else f"{by[k][0]} st · {by[k][1]:.1f} ms" for k in js) + " |")


if __name__ == "__main__":
    main([int(a) for a in sys.argv[1:]] or [10_000, 100_000])
