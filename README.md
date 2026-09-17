# grampy

A small workflow graph for work queues that already live in a storage —
a database, a key-value store, or plain memory: the storage is a driver.

```bash
pip install grampy-q              # from quazardous import grampy
pip install "grampy-q[postgres]"  # + the PostgreSQL driver (SQLAlchemy)
```

**[Try the brick sorter →](https://quazardous.github.io/grampy/)** — a sorting
line where TNT bricks get quarantined, defused with retries or rejected, run by
the real grampy in your browser. Locally: `python docs/demo/build.py && python -m
http.server 8765 -d docs/demo` (then http://localhost:8765).

The package lives in the `quazardous` namespace; the distribution is
`grampy-q` (`grampy` was already taken on PyPI; the *q* is for queue).

You declare a DAG of nodes. Each *subject* (a job, a request, a file…)
goes through the nodes; a **node journal** records, per subject and per
node, whether the node is `running`, `done`, `skipped`, `failed` or
`omitted`.
Workers **claim** a node for eligible subjects, **conclude** it, and the
graph decides what becomes claimable next — forks run in parallel, joins
wait for all their parents.

```python
from quazardous.grampy import Node, NodeJournal, check_dag
from quazardous.grampy.drivers.memory import MemoryDriver

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
| `quazardous.grampy.dag` | `Node`, the statuses, `check_dag`, and the **pure** claim rule: `claimable`, `claimable_nodes`, `descendants`, `ancestors` |
| `quazardous.grampy.graph` | the graph **as data**: `Graph(Document(name, version), nodes)`, a canonical dict / JSON form, strict reading with the path of every error |
| `quazardous.grampy.timing` | durations (`30s`, `10m`, `7d`), ISO instants, `Retry` and its backoff |
| `quazardous.grampy.diagram` | `to_mermaid` / `to_dot`: every mechanism drawn (choice ◇, wait ⬡, failure edges, loops, badges), with live counts from `overlay(journal)` |
| `quazardous.grampy.states` | derived from the graph: `replay_targets`, `replayed_after`, `to_undo`, `source_state`, `allowed_transitions` |
| `quazardous.grampy.journal` | `NodeJournal` — the logic (validation, rule inputs, clock) over a `JournalDriver` protocol |
| `quazardous.grampy.drivers.memory` | dict-based, deterministic, no dependency — the reference driver |
| `quazardous.grampy.drivers.sqlite` | standard-library `sqlite3` on tables you declare (`schema()` gives the DDL); one writer at a time, and the same contract |
| `quazardous.grampy.drivers.postgres` | SQLAlchemy Core on tables **you** declare; candidates read page by page, rows inserted only if the subject's revision is unchanged |
| `quazardous.grampy.testing` | `JournalContract`, the test suite every driver must pass — concurrency included |

## The rules

- **A subject is identified by the application.** Its id is unique, stable,
  and an integer or a string; grampy stores it and gives it back exactly as
  given, never builds nor splits one. The storage's subject column follows
  the application's type (`BIGINT`, `TEXT`…). The contract runs with both.

- A node is **claimable** when it has no row, no descendant has started,
  and it is **joined**: by default every parent is `done`, `skipped` or
  `omitted`; `failed` satisfies nobody.
- **Joins are data.** `on` says, per parent, which statuses a node accepts —
  `Node("refund", parents=("pay", "reserve"), on={"reserve": ("failed",)})`
  runs on a failure — and `need=k` starts a node once `k` parents are
  accepted: the others, not started yet, are closed by it.
- **Nothing is lost.** `forget`, `release` and loops move rows to a
  history (`journal.history(subject)`), with when and why.
- **Loops are declared and bounded.** `Node("review", parents=("draft",),
  loop=Loop(to="draft", max=3))`: a failed review sends the subject back to
  `draft`, in the same write, at most three times; after that the failure
  stands and a failure edge can escalate.
- **Retries are declared.** `Node("call", retry=Retry(limit=3, delay="10s",
  backoff="exponential", max_delay="5m", jitter=0.1))`: a failure is
  archived and the node `scheduled` again; the row becomes claimable when
  due. Past the limit the failure stands, for a loop or a failure edge.
- **Leases per node.** `Node("fetch", lease="2m")`, `Node("ai_tag", lease="1h")`:
  `journal.expire()`, called by the application's janitor, gives back every
  row held longer than its node allows — archived, then claimable again.
- **Waits, signals and grace.** `Node("clicked", parents=("send",),
  wait="email.clicked", timeout="7d")` is not worked but settled:
  `journal.signal(subjects, "email.clicked")` records the event durably —
  even before the wait begins — and `journal.settle(candidates)` concludes
  the wait `done`, or `failed` once the timeout has passed since its parents
  concluded. `Node(optional=True, grace="1d")` is skipped by `settle` when
  nobody took it in time.
- **Lanes: subjects that come back.** `Node("arrive", lane=Lane.throttle(
  cooldown="24h", max_wait="3d"))` is a way in that no worker claims:
  `journal.arrive("arrive", ["offer-12"], ref="v7")` puts a new version of the
  subject in it, and `journal.settle(candidates)` lets it through once due —
  archiving the previous pass through what follows, in the same write.
  A version arriving while one waits is merged (`merge="first"|"last"`, keeping
  the first one's place or not); `cooldown` holds a subject back that long
  after its last pass ended, `delay` waits for quiet, `max_wait` caps both, an
  `urgent=True` arrival skips them, and a pass still running is never cut
  short (`while_running="queue"|"skip"`). A `rate` on the lane lets arrivals
  out in the order they came. Presets carry the names other tools use:
  `Lane.throttle` (Graphile Worker's `preserve_run_at`), `Lane.debounce`,
  `Lane.dedupe`.
- **Channels: one workflow, several sources.** `journal.enroll(subjects,
  "partner-a")` records a subject's channel; a `Graph(..., channels={"partner-a":
  {"call": {"retry": Retry(5, "1m")}}})` changes, for that channel only, a
  node's `retry`, `lease`, `timeout`, `grace`, `rate`, `concurrency` or `lane`
  settings — never the structure.
- **Rate limits and concurrency, on any node.** `Node("summarise",
  parents=("fetch",), rate=(Rate(100, "1m"), Rate(1000, "1h", burst=50)),
  concurrency=4)` — a claim takes no more than every band lets through
  (GCRA, one number stored per band) nor more than 4 rows running at once.
  `per="channel"` gives each channel its own budget. The journal decides; the
  driver only guards the budget's keys while it does, so two claimers never
  overspend it.
- **Versions and migration.** A journal on a `Graph` pins each subject to
  its `document.version` and leaves the subjects of other versions alone, so
  v1 and v2 run side by side. `journal_v2.migrate(subjects, V1, {"crop": "trim",
  "old_step": None})` moves the subjects whose rows could have been written on
  v2 — renamed, dropped nodes archived — or refuses them all, naming each one
  that is not compliant and why.
- **One clock.** The journal takes its time from the driver — the database
  server for PostgreSQL — so workers on several machines agree on what is
  due.
- **An exclusive choice names its branch.** A `choice` node concludes with
  `branch=`; the other branches, and every node only they lead to, are
  written `omitted` in the same write. A join after the branches goes on.
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

## Drawing a graph

```python
from quazardous.grampy.diagram import overlay, to_dot, to_mermaid, to_state_diagram

print(to_mermaid(graph))                    # paste into any Markdown that renders Mermaid
print(to_mermaid(graph, overlay(journal)))  # with ▶ running ✓ done ✗ failed … per node
print(to_state_diagram(graph))              # the same graph as a statechart: choices,
                                            # forks, joins, loops and retries
```

`quazardous.grampy.diagram` is optional: `import quazardous.grampy` never loads it.

![A brick sorter: a choice, a wait with a timeout, retries, a failure edge, a loop and a 2-of-3 join](docs/brick-sorter.png)

## Drivers

The journal never commits and never reads the application's tables.
Eligibility ("not finished, by priority") is passed to `claim` as opaque
**candidates** in the driver's own terms: an ordered iterable for the
memory driver, a `Query(sql, params)` for SQLite, a SQLAlchemy `SELECT` for
postgres — the first column is the subject, the order is the priority.

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
    sa.Column("channel", sa.Text),
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

Writing a driver: implement `quazardous.grampy.journal.JournalDriver` — a few storage
operations, no rule — and subclass `quazardous.grampy.testing.JournalContract` with a
harness fixture, see `tests/test_memory_driver.py`. The contract includes
concurrency tests written in sessions (open, act, commit): how your storage
stays correct under them is up to it, the outcome is not.

## Tests

```bash
pip install -e ".[test]"        # or: uvx --with pytest pytest
pytest                          # graph, states, memory and SQLite drivers
GRAMPY_TEST_PG_DSN=postgresql+psycopg://user:pass@localhost/test \
    pytest                      # + the postgres driver (needs sqlalchemy and a driver)
```

## License

MIT
