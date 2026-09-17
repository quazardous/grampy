# Drivers, candidates, and your own data

The journal never commits and never reads the application's tables. It holds
ids and rows per `(subject, node)`; everything else stays yours.

- [Candidates are your sentence](#candidates-are-your-sentence)
- [From ids to your objects](#from-ids-to-your-objects)
- [The drivers that ship](#the-drivers-that-ship)
- [PostgreSQL: the tables you declare](#postgresql-the-tables-you-declare)
- [SQLite](#sqlite)
- [Writing a driver](#writing-a-driver)

## Candidates are your sentence

Eligibility — "not finished, by priority", "this tenant first" — is passed to
`claim` as opaque **candidates**, in the driver's own terms: an ordered
iterable for the memory driver, a `Query(sql, params)` for SQLite, a SQLAlchemy
`SELECT` for PostgreSQL. The first column is the subject, the order is the
priority.

It does **not** have to know the workflow's states: it is a coarse scope, and
the journal decides, node by node, from its own rows. The same candidates serve
every node and `settle`:

```python
offers = Query("SELECT id FROM offers WHERE NOT archived ORDER BY priority, seen_at")
journal.claim("scrape", 50, candidates=offers)   # the scraper worker
journal.claim("ai_tag", 10, candidates=offers)   # the tagging worker, elsewhere
journal.settle(offers)                            # the janitor
```

It is read **inside the claim's transaction**: selecting first and claiming
after would let two workers take the same subject in between. To narrow a huge
table, `journal.parents_concluded("ai_tag", subject)` gives that test in your
driver's terms, and the PostgreSQL driver already pre-filters what it safely
can.

## From ids to your objects

A claim returns ids, never your rows: grampy does not know what an offer is.
Loading is one line, in your ORM, on the whole batch:

```python
lease = journal.claim("ai_tag", 10, candidates=offers)
for offer in Offer.objects.filter(id__in=list(lease)):   # your ORM, your query
    ...                                                   # your work
journal.conclude("ai_tag", list(lease), token=lease.token)
```

Half the journal's work — `expire`, `settle`, counts, diagrams — never needs
the data at all.

## The drivers that ship

| driver | needs | for |
|---|---|---|
| `drivers.memory` | nothing | tests, prototypes, one process; the reference the others are confronted with |
| `drivers.sqlite` | `sqlite3` (standard library) | small deployments, a file shared by several processes |
| `drivers.postgres` | SQLAlchemy Core (`grampy-q[postgres]`) | production, several machines |

## PostgreSQL: the tables you declare

```python
import sqlalchemy as sa
from quazardous.grampy.drivers.postgres import PostgresDriver

nodes = sa.Table("job_nodes", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("node", sa.Text, primary_key=True),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("started_at", sa.Text, nullable=False),
    sa.Column("finished_at", sa.Text),
    sa.Column("lease", sa.Text))
revisions = sa.Table("job_revisions", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("revision", sa.Integer, nullable=False),
    sa.Column("policy", sa.Text),
    sa.Column("version", sa.Text))
history = sa.Table("job_node_history", metadata,
    *[sa.Column(c.name, c.type) for c in nodes.columns],
    sa.Column("archived_at", sa.Text, nullable=False),
    sa.Column("reason", sa.Text, nullable=False))

limits = sa.Table("job_limits", metadata,        # only for nodes with rate/concurrency
    sa.Column("key", sa.Text, primary_key=True),
    sa.Column("value", sa.Float))
arrivals = sa.Table("job_arrivals", metadata,    # only for graphs with a lane
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("node", sa.Text, primary_key=True),
    sa.Column("ref", sa.Text),
    sa.Column("place", sa.Text, nullable=False),
    sa.Column("arrived_at", sa.Text, nullable=False),
    sa.Column("urgent", sa.Boolean, nullable=False))

journal = NodeJournal(
    PostgresDriver(conn.execute, nodes, revisions, history, subject="job_id",
                   limits=limits, arrivals=arrivals), DAG)
eligible = sa.select(jobs.c.job_id).where(jobs.c.state != "done").order_by(jobs.c.priority)
journal.claim("fetch", 50, candidates=eligible)
```

## SQLite

```python
from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema

for statement in schema():        # the DDL of the tables it expects
    conn.execute(statement)
journal = NodeJournal(SqliteDriver(conn), DAG)
```

## Writing a driver

Implement `quazardous.grampy.journal.JournalDriver` — a few storage operations,
no rule — and subclass `quazardous.grampy.testing.JournalContract` with a
harness fixture; see `tests/test_memory_driver.py`. The contract includes
concurrency tests written in sessions (open, act, commit): how your storage
stays correct under them is up to it, the outcome is not.

No guarantee may rely on a storage-specific mechanism: it is defined in the
driver protocol and proven by the shared contract.
