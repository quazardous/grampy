"""WHICH INDEX CHANGES WHICH PLAN, measured on the layouts' benchmark data.

    GRAMPY_BENCH_PG_DSN=postgresql+psycopg://user:pass@localhost/bench \\
        uv run --with sqlalchemy --with 'psycopg[binary]' \\
        python benchmarks/indexes.py [subjects]

The data of `benchmarks/layouts.py`, plus a history (three archived rows a
subject) and arrivals waiting in a lane. Each journal call below is run by
the real driver; the statements it sends are captured, then each is run
under `EXPLAIN (ANALYZE, BUFFERS)` without, then with, the candidate index —
inside a savepoint rolled back after, so a write measured is never kept.

For each: the time PostgreSQL reports, the pages read, and the scans its
plan chose. An index that changes no plan is not worth its cost on every
write. A local server ranks, it does not predict a remote one.
"""
from __future__ import annotations

import os
import re
import sys
from collections.abc import Callable
from typing import Any

import sqlalchemy as sa
from layouts import AT, SCHEMA, fill, tables

from quazardous.grampy.drivers.postgres import PostgresDriver
from quazardous.grampy.drivers.postgres_subject import PostgresSubjectDriver

LATER = "2026-06-01T00:00:00+00:00"


def arrivals(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "arrivals", metadata,
        sa.Column("subject", sa.BigInteger, primary_key=True),
        sa.Column("node", sa.Text, primary_key=True),
        *[sa.Column(c, sa.Text) for c in ("ref", "place", "arrived_at", "refs")],
        sa.Column("urgent", sa.Boolean, nullable=False, server_default=sa.false()),
        schema=SCHEMA)


def more(conn: sa.Connection, n: int) -> None:
    """A history — a retry of `match`, a forget of `parse`, a signal — and
    arrivals waiting in two lanes, a fifth of the subjects in each."""
    q = f'"{SCHEMA}"'
    for node, reason in (("match", "retry"), ("parse", "forget"), ("clicked", "signal")):
        conn.execute(sa.text(
            f"INSERT INTO {q}.history (subject, node, status, started_at, finished_at, "
            f"archived_at, reason) SELECT i, '{node}', 'failed', '{AT}', '{AT}', '{AT}', "
            f"'{reason}' FROM generate_series(0, {n - 1}) i"))
    for lane, which in (("arrive", 0), ("refresh", 1)):
        conn.execute(sa.text(
            f"INSERT INTO {q}.arrivals (subject, node, place, arrived_at) "
            f"SELECT i, '{lane}', '{AT}', '{AT}' FROM generate_series(0, {n - 1}) i "
            f"WHERE i % 10 = {which} OR i % 10 = {which + 2}"))
    conn.execute(sa.text(f"ANALYZE {q}.history; ANALYZE {q}.arrivals"))


def captured(conn: sa.Connection, call: Callable[[], Any]) -> list[tuple[str, Any]]:
    sent: list[tuple[str, Any]] = []

    def seen(_c: Any, _cur: Any, statement: str, parameters: Any, *_: Any) -> None:
        sent.append((statement, parameters))

    savepoint = conn.begin_nested()
    sa.event.listen(conn, "before_cursor_execute", seen)
    try:
        call()
    finally:
        sa.event.remove(conn, "before_cursor_execute", seen)
        savepoint.rollback()
    return sent


def explain(conn: sa.Connection, sent: list[tuple[str, Any]]) -> tuple[float, int, str]:
    """Total execution time, pages read and the scans chosen, over the
    statements a call sent."""
    total, pages, scans = 0.0, 0, []
    for statement, parameters in sent:
        if statement.lstrip().upper().startswith(("SELECT PG_", "SHOW")):
            continue
        savepoint = conn.begin_nested()
        plan = "\n".join(r[0] for r in conn.exec_driver_sql(
            "EXPLAIN (ANALYZE, BUFFERS) " + statement, parameters).fetchall())
        savepoint.rollback()
        total += float(re.search(r"Execution Time: ([\d.]+)", plan).group(1))
        pages += sum(int(h) + int(r or 0) for h, r in re.findall(
            r"Buffers: shared hit=(\d+)(?: read=(\d+))?", plan)[:1])
        scans += re.findall(
            r"((?:Index Only |Index |Bitmap Index |Seq )Scan) (?:using \S+ )?on (\S+)", plan)
    kinds = sorted({f"{kind.strip()} on {table.split('.')[-1]}" for kind, table in scans})
    return total, pages, "; ".join(kinds)


def main(n: int) -> None:
    dsn = os.environ.get("GRAMPY_BENCH_PG_DSN")
    if not dsn:
        sys.exit("set GRAMPY_BENCH_PG_DSN to a PostgreSQL database you can write to")
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn.execute(sa.text(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'))
        conn.execute(sa.text(f'CREATE SCHEMA "{SCHEMA}"'))
        metadata = sa.MetaData()
        t = tables(metadata)
        t["arrivals"] = arrivals(metadata)
        metadata.create_all(conn)
        fill(conn, n)
        more(conn, n)
        conn.commit()
        s = f'"{SCHEMA}"'
        with conn.begin():
            row = PostgresDriver(conn.execute, t["row_nodes"], t["row_revisions"],
                                 t["history"], arrivals=t["arrivals"])
            subj = PostgresSubjectDriver(conn.execute, t["subjects"], t["history"],
                                         arrivals=t["arrivals"])
            some = list(range(0, n, max(1, n // 30)))[:30]
            # EACH INDEX WITH THE CALLS IT MAY SERVE: all measured before it
            # exists, then after — so a "without" never has it.
            groups = [
                (f"CREATE INDEX ON {s}.history (subject, node, reason, archived_at)", [
                    ("history", "retries of a node, 30 subjects (a retry limit)",
                     lambda: row.archived(some, "match", "retry")),
                    ("history", "latest signal, 30 subjects (settling a wait)",
                     lambda: row.latest(some, "clicked", "signal")),
                ]),
                (f"CREATE INDEX ON {s}.row_nodes (node, status, started_at)", [
                    ("row per node", "running rows of a node (a concurrency limit)",
                     lambda: row.running("match", None)),
                    ("row per node", "status counts of a node (journal.counts)",
                     lambda: row.status_counts("match")),
                    ("row per node", "stale leases of a node released",
                     lambda: row.release("parse", older_than=LATER, now=LATER)),
                ]),
                (f"CREATE INDEX ON {s}.arrivals (node)", [
                    ("arrivals", "arrivals waiting in a lane (journal.counts)",
                     lambda: row.queued("arrive")),
                ]),
                # The driver reads a field as `jsonb_extract_path_text`: an index
                # on another spelling of it would never be used.
                (f"CREATE INDEX ON {s}.subjects "
                 f"(jsonb_extract_path_text(nodes, 'match', 'status'))", [
                    ("row per subject", "running rows of a node",
                     lambda: subj.running("match", None)),
                    ("row per subject", "status counts of a node",
                     lambda: subj.status_counts("match")),
                ]),
            ]
            print(f"\n### {n:,} subjects, {3 * n:,} history rows\n")
            print("| index | call | without | with | plan without → with |")
            print("|---|---|---:|---:|---|")
            for index, calls in groups:
                sent = [(table, label, captured(conn, call)) for table, label, call in calls]
                before = [explain(conn, statements) for _, _, statements in sent]
                conn.execute(sa.text(index))
                conn.execute(sa.text(f"ANALYZE {s}.{index.split(f'{s}.')[1].split()[0]}"))
                shown = index.split(" ON ")[1].replace(f"{s}.", "")
                for (_table, label, statements), was in zip(sent, before, strict=True):
                    now = explain(conn, statements)
                    print(f"| `{shown}` | {label} | {was[0]:.1f} ms · {was[1]} pages | "
                          f"{now[0]:.1f} ms · {now[1]} pages | {was[2]} → {now[2]} |")
            conn.rollback()
        conn.execute(sa.text(f'DROP SCHEMA "{SCHEMA}" CASCADE'))
        conn.commit()


if __name__ == "__main__":
    main(int(sys.argv[1]) if sys.argv[1:] else 100_000)
