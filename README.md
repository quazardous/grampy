# grampy

A small workflow graph for work queues that already live in a database.

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
journal.claim("fetch", 10, candidates=["s1", "s2"])   # ['s1', 's2']
journal.conclude("fetch", ["s1"])
journal.claim("crop", 10, candidates=["s1", "s2"])    # ['s1'] — s2 still fetching
```

## What is in the box

| module | what it does |
|---|---|
| `grampy.dag` | `Node`, the statuses, `check_dag`, and the **pure** claim rule: `claimable`, `claimable_nodes`, `descendants`, `ancestors` |
| `grampy.states` | derived from the graph: `replay_targets`, `replayed_after`, `to_undo`, `source_state`, `allowed_transitions` |
| `grampy.journal` | `NodeJournal` — the logic (validation, rule inputs, clock) over a `JournalDriver` protocol |
| `grampy.drivers.memory` | dict-based, deterministic, no dependency — the reference driver |
| `grampy.drivers.postgres` | SQLAlchemy Core on a table **you** declare; a claim is one `INSERT … SELECT … ON CONFLICT DO NOTHING RETURNING` |
| `grampy.testing` | `JournalContract`, the test suite every driver must pass |

## The rules

- A node is **claimable** when it has no row, no descendant has started,
  and every parent is `done` or `skipped`. `failed` satisfies nobody.
- **Never backwards**: once a descendant has started, the node is closed.
  Going back is `forget` — the absence of a row means "never started".
- Only an **optional** node can be `skip`ped, and only when it is itself
  claimable (its parents concluded).
- `adopt` records work done outside the journal and never overwrites.
- `Node.working` / `Node.state` are **projections** an application may
  mirror on its subjects; nothing in grampy reads them to decide. They
  are what `allowed_transitions` and the replay helpers are derived from.

## Drivers

The journal never commits and never reads the application's tables.
Eligibility ("not finished, by priority") is passed to `claim` as opaque
**candidates** in the driver's own terms: an ordered iterable for the
memory driver, a `SELECT` whose first column is the subject for postgres.
That keeps the claim atomic across the node rows and the application's
own filter.

```python
import sqlalchemy as sa
from grampy.drivers.postgres import PostgresDriver

nodes = sa.Table("job_nodes", metadata,
    sa.Column("job_id", sa.Text, primary_key=True),
    sa.Column("node", sa.Text, primary_key=True),
    sa.Column("status", sa.Text, nullable=False),
    sa.Column("started_at", sa.Text, nullable=False),
    sa.Column("finished_at", sa.Text))

journal = NodeJournal(PostgresDriver(conn.execute, nodes, subject="job_id"), DAG)
eligible = sa.select(jobs.c.job_id).where(jobs.c.state != "done").order_by(jobs.c.priority)
journal.claim("fetch", 50, candidates=eligible)
```

Writing a driver: implement `grampy.journal.JournalDriver` and subclass
`grampy.testing.JournalContract` with a harness fixture — see
`tests/test_memory_driver.py`.

## Tests

```bash
pip install -e ".[test]"        # or: uvx --with pytest pytest
pytest                          # memory driver, graph, states
GRAMPY_TEST_PG_DSN=postgresql+psycopg://user:pass@localhost/test \
    pytest                      # + the postgres driver (needs sqlalchemy and a driver)
```

## License

MIT
