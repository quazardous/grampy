"""A CLAIM AGAINST A LARGE QUEUE: what one claim costs when the candidates
are a big table ordered by an index — the shape a production queue has.

    GRAMPY_BENCH_PG_DSN=postgresql+psycopg://user:pass@localhost/bench \\
        uv run --with sqlalchemy --with 'psycopg[binary]' \\
        python benchmarks/claim_queue.py [subjects]

The data of `benchmarks/layouts.py`, with the items given a queue order —
`(score, seq)`, indexed — and the candidates read in that order. For each
claim: the statements sent, the median time of five runs, and for the page
query the blocks it read and the temporary blocks it wrote, from
`EXPLAIN (ANALYZE, BUFFERS)`. A claim should read a page, whatever the size
of the queue; temporary blocks mean it sorted the whole queue instead.
"""
from __future__ import annotations

import os
import re
import statistics
import sys
import time
from typing import Any

import sqlalchemy as sa
from indexes import captured
from layouts import SCHEMA, fill, journals, tables

RUNS = 5


def queue(conn: sa.Connection, items: sa.Table) -> None:
    """A queue order on the items: a score of 100 levels, then a sequence."""
    q = f'"{SCHEMA}".items'
    conn.execute(sa.text(f"ALTER TABLE {q} ADD COLUMN score integer, ADD COLUMN seq bigint"))
    conn.execute(sa.text(f"UPDATE {q} SET score = (id * 7919) % 100, seq = id"))
    conn.execute(sa.text(f"CREATE INDEX items_queue ON {q} (score, seq)"))
    conn.execute(sa.text(f"ANALYZE {q}"))


def plan_of(conn: sa.Connection, statement: str, parameters: Any) -> tuple[float, int, int]:
    savepoint = conn.begin_nested()
    plan = "\n".join(r[0] for r in conn.exec_driver_sql(
        "EXPLAIN (ANALYZE, BUFFERS) " + statement, parameters).fetchall())
    savepoint.rollback()
    ms = float(re.search(r"Execution Time: ([\d.]+)", plan).group(1))
    # The first Buffers line is the top node's: everything the query read.
    first = next((line for line in plan.splitlines() if "Buffers:" in line), "")
    blocks = sum(int(x) for x in re.findall(r"shared (?:hit|read)=(\d+)", first)) or sum(
        int(x) for x in re.findall(r"(?:hit|read)=(\d+)", first))
    temp = sum(int(x) for x in re.findall(r"temp read=\d+ written=(\d+)", plan)) or sum(
        int(x) for x in re.findall(r"temp written=(\d+)", plan))
    return ms, blocks, temp


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
        metadata.create_all(conn)
        fill(conn, n)
        queue(conn, t["items"])
        conn.commit()
        items = sa.Table("items", sa.MetaData(), autoload_with=conn, schema=SCHEMA)
        conn.commit()
        queued = sa.select(items.c.id).order_by(items.c.score, items.c.seq)
        print(f"\n### {n:,} subjects, candidates ordered by (score, seq)\n")
        print("| claim 30 | layout | statements | median | reads: ms · blocks · temp blocks |")
        print("|---|---|---:|---:|---|")
        with conn.begin():
            for layout, j in journals(conn, t).items():
                for node in ("match", "publish"):
                    counts, times = [], []
                    for _ in range(RUNS):
                        sent: list[int] = []

                        def seen(*_: Any, _sent: list[int] = sent) -> None:
                            _sent.append(1)

                        savepoint = conn.begin_nested()
                        sa.event.listen(conn, "before_cursor_execute", seen)
                        started = time.perf_counter()
                        j.claim(node, 30, candidates=queued)
                        times.append((time.perf_counter() - started) * 1000)
                        sa.event.remove(conn, "before_cursor_execute", seen)
                        savepoint.rollback()
                        counts.append(len(sent))
                    statements = captured(conn, lambda j=j, node=node: j.claim(
                        node, 30, candidates=queued))
                    # Every read of the claim — every slice — not only the first.
                    pages = [s for s in statements if "grampy" in s[0]
                             and s[0].lstrip()[:6] in ("SELECT", "WITH c")]
                    plans = [plan_of(conn, *p) for p in pages]
                    ms = sum(p[0] for p in plans)
                    blocks = sum(p[1] for p in plans)
                    temp = sum(p[2] for p in plans)
                    print(f"| {node} | {layout} | {max(counts)} | "
                          f"{statistics.median(times):.0f} ms | "
                          f"{ms:.0f} ms · {blocks:,} · {temp:,} |")
            conn.rollback()
        conn.execute(sa.text(f'DROP SCHEMA "{SCHEMA}" CASCADE'))
        conn.commit()


if __name__ == "__main__":
    main(int(sys.argv[1]) if sys.argv[1:] else 1_300_000)
