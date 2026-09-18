# Drivers, candidates, and your own data

The journal never commits and never reads the application's tables. It holds
ids and rows per `(subject, node)`; everything else stays yours.

- [Candidates are your sentence](#candidates-are-your-sentence)
- [From ids to your objects](#from-ids-to-your-objects)
- [The drivers that ship](#the-drivers-that-ship)
- [PostgreSQL: the tables you declare](#postgresql-the-tables-you-declare)
- [PostgreSQL: three layouts, your choice](#postgresql-three-layouts-your-choice)
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

Reading grampy's own tables — to see where a subject stands, what is stuck,
what went out in one batch — is expected, and
[has a page of its own](your-data.md). Writing to them is not.

**Written once, in [the items layer](items.md).** That loading line, and the
handlers that go with it — which branch a choice takes, whether an optional
step is for this item — belong in an adapter rather than at every call site:

```python
lease = items.claim("ai_tag", 10, candidates=offers)   # objects in and out
for offer in lease:
    ...
items.conclude("ai_tag", lease)
```

It is the canonical way to use grampy. The id-based calls above stay exactly
as they are, and stay the right choice when you already hold ids.

## The drivers that ship

| driver | needs | for |
|---|---|---|
| `drivers.memory` | nothing | tests, prototypes, one process; the reference the others are confronted with |
| `drivers.sqlite` | `sqlite3` (standard library) | small deployments, a file shared by several processes |
| `drivers.postgres`, `postgres_subject`, `postgres_ready` | SQLAlchemy Core (`grampy-q[postgres]`) | production, several machines; three layouts, [your choice](#postgresql-three-layouts-your-choice) |

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

## PostgreSQL: three layouts, your choice

There is more than one right way to lay a journal out in tables, and grampy
does not pick for you: **each layout is a driver class**, and you instantiate
the one whose costs suit your workload. Nothing switches on its own. All three
pass the same contract, concurrency included, and give the same answers.

| driver | tables | a claim | what it costs |
|---|---|---|---|
| `postgres.PostgresDriver` | a row per (subject, node), a revision per subject | a page read, a read of its rows, then a seeding insert and the write | the most round trips; a plain index serves "no row for this node" |
| `postgres_subject.PostgresSubjectDriver` | **one row per subject**: revision, policy, version and every node as JSONB | the page brings the progress; the write is **one** upsert | every write rewrites the subject's row, and two claims on two nodes of one subject wait for each other; "no row for node X" at scale wants an expression index |
| `postgres_ready.PostgresReadyDriver` | `PostgresDriver`'s, plus a `ready` list | the page reads only the candidates listed as possibly ready for the node | every conclusion lists the children it may unblock, every claim strikes its pair; needs the graph's shape (`graph=`) |

```python
from quazardous.grampy.drivers.postgres_subject import PostgresSubjectDriver

subjects = sa.Table("job_subjects", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("revision", sa.Integer, nullable=False),
    sa.Column("policy", sa.Text),
    sa.Column("version", sa.Text),
    sa.Column("nodes", JSONB, nullable=False, server_default="{}"))

journal = NodeJournal(PostgresSubjectDriver(conn.execute, subjects, history,
                                            subject="job_id"), DAG)
```

`history`, `limits` and `arrivals` are the same tables in every layout. The
ready list is `(subject, node)` as primary key and nothing else; a table this
driver did not write (a migration, rows written by hand) is listed once with
`driver.refill()`.

**The ready list is a hint, never a decision.** A pair listed means "may have
become claimable"; the claim still applies every pre-filter and the journal
still judges every candidate. It is read only where the journal asks for the
parents' pre-filter, so a node without parents, a node with a custom join and
a claim waiving its parents read every candidate, as in `PostgresDriver`.

### Measured

`benchmarks/layouts.py`, on a local PostgreSQL 16: a chain fetch → parse →
(enrich) → match → publish, half the subjects enriched, a third of those
replayed at `parse`. Statements are counted at the cursor, which is what a
round trip costs on a real network; times are the median of five runs, and
rank the layouts rather than predict a remote server.

| 100,000 subjects | row per node | row per subject | ready list |
|---|---:|---:|---:|
| claim 30, half ready | 5 st · 516 ms | **3 st · 99 ms** | 6 st · 438 ms |
| claim 30, 1 in 20 ready | 5 st · 308 ms | **3 st · 59 ms** | 6 st · 298 ms |
| claim and conclude 30 | 7 st · 273 ms | **5 st · 58 ms** | 8 st · 277 ms |
| claim 30, every candidate past the node | 169 st · 24.1 s | **85 st · 6.9 s** | 169 st · 26.3 s |
| forget 30 | 3 st · 4.7 ms | 3 st · 4.3 ms | 4 st · 4.8 ms |

On this workload the subject layout wins every claim, and **the ready list
gains nothing**: the claim still walks the candidates in their order, and the
list adds a probe to each rather than removing one. It stays available, and
proven, for a workload where few subjects are ever ready and the candidates
are cheap to walk; measure yours before picking it.

The fourth line is the case to watch in any layout: when every candidate
waiting at a node has a descendant started — a replay that left the later
steps in place — the claim reads the whole candidate set to take nothing.

Run the bench on your own shape of data: its table sizes and progress are
at the top of the file.

## SQLite

```python
from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema

for statement in schema(subject_type="INTEGER"):   # the DDL of the tables it expects
    conn.execute(statement)
journal = NodeJournal(SqliteDriver(conn), DAG)
```

**Give `subject_type` the type of your ids.** Left out, the subject column is
declared without a type, so it can hold ints and strings alike; but SQLite
then cannot use its index when *your* typed column is compared to it, and a
join of your table on grampy's scans instead of searching. Measured on one
such query at 10,000 subjects: 18.5 ms untyped, 0.1 ms typed.

The driver pre-filters each page in SQL (no row for the node, parents
concluded) before reading the rows of what is left. It reads the candidate
set at once, on purpose: its cursor cannot stay open while the same
connection writes the claim. On 100,000 subjects that read is the fixed cost
of a claim, about 35 ms on a laptop; a claim where nothing is takable took
670 ms before the pre-filter and 268 ms with it.

SQLite runs in your process: there is no round trip to save, so the
PostgreSQL layouts above have no SQLite counterpart. What pays here is reading
fewer rows.

## A transaction around every call

The journal never opens a transaction and never commits: **your code does,
around each journal call.** Its guarantees live inside that transaction — the
locks a claim takes, the writes made of two statements — so an executor that
autocommits would void them all without an error. Both SQL drivers refuse it:
their first write raises, and says so.

## Writing a driver

Implement `quazardous.grampy.journal.JournalDriver` — a few storage operations,
no rule — and subclass `quazardous.grampy.testing.JournalContract` with a
harness fixture; see `tests/test_memory_driver.py`. The contract includes
concurrency tests written in sessions (open, act, commit): how your storage
stays correct under them is up to it, the outcome is not.

No guarantee may rely on a storage-specific mechanism: it is defined in the
driver protocol and proven by the shared contract.
