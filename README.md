# grampy

A small workflow graph for work queues that already live in a storage —
a database, a key-value store, or plain memory: the storage is a driver.

You declare a DAG of nodes. Each *subject* (a job, a request, a file…)
goes through the nodes; a **node journal** records, per subject and per
node, whether the node is `running`, `done`, `skipped` or `failed`.
Workers **claim** a node for eligible subjects, **conclude** it, and the
graph decides what becomes claimable next — forks run in parallel, joins
wait for all their parents.

```python
from grampy import Node, NodeJournal, check_dag
from grampy.drivers.memory import MemoryDriver

DAG = (
    Node("fetch", working="fetching", state="fetched"),
    Node("crop", parents=("fetch",), optional=True),
    Node("read", parents=("fetch",), optional=True),
    Node("judge", parents=("crop", "read"), state="judged"),
)
check_dag(DAG)

journal = NodeJournal(MemoryDriver(), DAG)
lease = journal.claim("fetch", 10, candidates=["s1", "s2"])   # ['s1', 's2']
journal.conclude("fetch", ["s1"], token=lease.token)
journal.claim("crop", 10, candidates=["s1", "s2"])            # ['s1'] — s2 still fetching
```

## What is in the box

| module | what it does |
|---|---|
| `grampy.dag` | `Node`, the statuses, `check_dag`, and the **pure** claim rule: `claimable`, `claimable_nodes`, `descendants`, `ancestors` |
| `grampy.states` | derived from the graph: `replay_targets`, `replayed_after`, `to_undo`, `source_state`, `allowed_transitions` |
| `grampy.journal` | `NodeJournal` — the logic (validation, rule inputs, clock) over a `JournalDriver` protocol |
| `grampy.drivers.memory` | dict-based, deterministic, no dependency — the reference driver |
| `grampy.drivers.postgres` | SQLAlchemy Core on tables **you** declare; candidates read page by page, rows inserted only if the subject's revision is unchanged |
| `grampy.testing` | `JournalContract`, the test suite every driver must pass — concurrency included |

## The rules

- A node is **claimable** when it has no row, no descendant has started,
  and every parent is `done` or `skipped`. `failed` satisfies nobody.
- **Never backwards**: once a descendant has started, the node is closed.
  Going back is `forget` — the absence of a row means "never started".
- Only an **optional** node can be `skip`ped, and only when it is itself
  claimable (its parents concluded).
- `adopt` records work done outside the journal and never overwrites.
- **A conclusion proves it holds the lease.** Every claim returns a `Lease`
  — the subjects taken, and a `token` unique to that claim. `conclude` and
  `fail` only touch rows holding the token they bring, so a slow worker
  whose lease was released and taken by another rewrites nothing.
  `token=None` is an explicit operator override.
- **The journal decides, the driver stores.** A claim reads the candidates'
  rows, applies the rule in Python, and writes only if nothing was
  forgotten in between: every subject carries a revision that `forget`
  raises. Two workers never hold the same node, and a node is never taken
  on the strength of a parent forgotten meanwhile.
- `Node.working` / `Node.state` are **projections** an application may
  mirror on its subjects; nothing in grampy reads them to decide. They
  are what `allowed_transitions` and the replay helpers are derived from.

## Drivers

The journal never commits and never reads the application's tables.
Eligibility ("not finished, by priority") is passed to `claim` as opaque
**candidates** in the driver's own terms: an ordered iterable for the
memory driver, a `SELECT` whose first column is the subject for postgres.
Its order is the priority.

```python
import sqlalchemy as sa
from grampy.drivers.postgres import PostgresDriver

nodes = sa.Table("job_nodes", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("node", sa.Text, primary_key=True),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("started_at", sa.Text, nullable=False),
    sa.Column("finished_at", sa.Text),
    sa.Column("lease", sa.Text))
revisions = sa.Table("job_revisions", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("revision", sa.Integer, nullable=False))

journal = NodeJournal(PostgresDriver(conn.execute, nodes, revisions, subject="job_id"), DAG)
eligible = sa.select(jobs.c.job_id).where(jobs.c.state != "done").order_by(jobs.c.priority)
journal.claim("fetch", 50, candidates=eligible)
```

Writing a driver: implement `grampy.journal.JournalDriver` — a few storage
operations, no rule — and subclass `grampy.testing.JournalContract` with a
harness fixture, see `tests/test_memory_driver.py`. The contract includes
concurrency tests written in sessions (open, act, commit): how your storage
stays correct under them is up to it, the outcome is not.

## Tests

```bash
pip install -e ".[test]"        # or: uvx --with pytest pytest
pytest                          # memory driver, graph, states
GRAMPY_TEST_PG_DSN=postgresql+psycopg://user:pass@localhost/test \
    pytest                      # + the postgres driver (needs sqlalchemy and a driver)
```

## License

MIT
