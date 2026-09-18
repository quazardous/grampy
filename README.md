# grampy

[![PyPI](https://img.shields.io/pypi/v/grampy-q?cacheSeconds=3600)](https://pypi.org/project/grampy-q/)
[![Python](https://img.shields.io/pypi/pyversions/grampy-q)](https://pypi.org/project/grampy-q/)
[![License: MIT](https://img.shields.io/pypi/l/grampy-q)](LICENSE)
[![CI](https://github.com/quazardous/grampy/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/quazardous/grampy/actions/workflows/ci.yml)

A small workflow graph for work queues that already live in a storage —
a database, a key-value store, or plain memory: the storage is a driver.

```bash
pip install grampy-q
pip install "grampy-q[postgres]"  # + the PostgreSQL drivers (SQLAlchemy)
```

The distribution is `grampy-q` — the *q* is for queue, `grampy` being taken on
PyPI — and it is imported as `quazardous.grampy`.

**[Try the brick sorter →](https://quazardous.github.io/grampy/)** — a sorting
line where bricks are sorted by colour, TNT bricks get quarantined, defused
with retries or thrown away, and a sorted brick sent back waits in a lane
before it runs again. It runs the real library in your browser.

You declare a DAG of nodes. Each *subject* (a job, a request, an offer…) goes
through the nodes; a **node journal** records, per subject and per node,
whether the node is `running`, `done`, `skipped`, `failed` or `omitted`.
Workers **claim** a node for eligible subjects, **conclude** it, and the graph
decides what becomes claimable next — forks run in parallel, joins wait for
their parents.

```python
from dataclasses import dataclass

from quazardous.grampy import Node, NodeJournal
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.items import Adapter, Items

@dataclass
class Order:                                   # your object, as it is
    id: int
    total: float

class Orders(Adapter):
    def id_of(self, order):  return order.id   # the one thing grampy must know

GRAPH = (Node("pay"), Node("ship", parents=("pay",)))
items = Items(NodeJournal(MemoryDriver(), GRAPH), Orders())
orders = [Order(1, 30.0), Order(2, 12.5)]

lease = items.claim("pay", 10, candidates=orders)       # orders in, orders out
for order in lease:
    print("paying", order.id, order.total)
items.conclude("pay", lease)

print(items.progress(orders[0]))                        # {'pay': Status.DONE}
print(list(items.claim("ship", 10, candidates=orders)))  # pay is done: ship is next
```

That runs as pasted. A larger graph, with subjects that differ:

```python
from dataclasses import dataclass

from quazardous.grampy import Node, NodeJournal, check_dag
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.items import Adapter, Items

DAG = (
    # `working` and `state`: what YOUR status column says while the node runs
    # and once it is done — the journal records `running` and `done`.
    Node("fetch", working="fetching", state="fetched"),
    Node("crop", parents=("fetch",), optional=True),
    Node("read", parents=("fetch",), optional=True),
    Node("judge", parents=("crop", "read"), state="judged"),
)
check_dag(DAG)

@dataclass(frozen=True)
class Doc:
    id: str
    scanned: bool = True

class DocsAdapter(Adapter):                   # how grampy reads YOUR object
    def id_of(self, doc):  return doc.id
    def applies(self, doc, node):             # one graph, subjects that differ
        return node != "crop" or doc.scanned

def my_loader():                              # your query, your ORM
    return [Doc("s1"), Doc("s2", scanned=False)]

items = Items(NodeJournal(MemoryDriver(), DAG), DocsAdapter())
lease = items.claim("fetch", 10, candidates=my_loader())    # objects in…
for doc in lease:                                           # …and objects out
    ...
items.conclude("fetch", lease)
```

`working` and `state` are projections, not commands: `allowed_transitions()`
turns them into the transitions your own status column may take — see
[the rules](https://github.com/quazardous/grampy/blob/main/docs/rules.md#subjects-and-the-claim-rule).

Handing grampy your objects is the canonical way to use it — see
[items](https://github.com/quazardous/grampy/blob/main/docs/items.md). The
core underneath works on ids alone and stays available: `journal.claim("fetch",
10, candidates=[…])` returns subjects, and never reads your data.

## Why not a status column?

A `status` column and a few `UPDATE`s hold one line of steps, one worker at a
time. What grampy adds, each proven by the shared driver contract
(`quazardous.grampy.testing.JournalContract`):

- **Two workers, one subject.** A claim is atomic and returns a token; a slow
  worker whose lease went to another writes nothing
  (`test_concurrent_claimers_never_take_a_subject_twice`,
  `test_a_released_lease_cannot_conclude_the_next_one`).
- **A graph, not a line.** Forks run in parallel, joins wait for their
  parents, optional steps and exclusive choices do not block what follows
  (`test_the_driver_claims_exactly_what_the_rule_says`,
  `test_a_child_waits_for_its_parent_to_conclude`).
- **Going back.** Forget, replay and loops keep what they take away, with when
  and why (`test_forget_and_release_archive_what_they_take_away`).
- **Time.** Retries, leases, waits and grace periods, settled by one janitor
  call (`test_a_failure_is_retried_when_due_then_stands`).

## What it does

Each line links to [the rules](https://github.com/quazardous/grampy/blob/main/docs/rules.md), where it is spelled out.

| | |
|---|---|
| [joins as data](https://github.com/quazardous/grampy/blob/main/docs/rules.md#joins-choices-and-failure-edges) | which parent statuses a node accepts, `k` of `n`, a step that runs **on a failure** |
| [exclusive choices](https://github.com/quazardous/grampy/blob/main/docs/rules.md#joins-choices-and-failure-edges) | a node names its branch; what only the others lead to is `omitted` at once |
| [loops and history](https://github.com/quazardous/grampy/blob/main/docs/rules.md#going-back-history-loops-replay) | bounded ways back, every row taken away kept with when and why |
| [retries](https://github.com/quazardous/grampy/blob/main/docs/rules.md#time-retries-leases-waits-grace) | declared backoff — constant, linear or exponential, capped, with jitter |
| [leases](https://github.com/quazardous/grampy/blob/main/docs/rules.md#time-retries-leases-waits-grace) | per node, given back by `journal.expire()` when a worker dies |
| [waits and grace](https://github.com/quazardous/grampy/blob/main/docs/rules.md#time-retries-leases-waits-grace) | settled by a durable signal, recorded even before the wait, or failed at its timeout |
| [groups](https://github.com/quazardous/grampy/blob/main/docs/rules.md#groups-subjects-worked-together) | subjects worked together — five of a colour, ten thousand for one file — a whole group or none |
| [lanes](https://github.com/quazardous/grampy/blob/main/docs/rules.md#lanes-subjects-that-come-back) | a subject that comes back waits, merges with the version waiting, runs again after a cooldown — a document edited five times in a minute runs once, on its last version |
| [policies](https://github.com/quazardous/grampy/blob/main/docs/rules.md#policies-one-workflow-different-limits) | one workflow, subjects treated differently: their own retries, leases and **budgets** |
| [rate and concurrency](https://github.com/quazardous/grampy/blob/main/docs/rules.md#rate-limits-and-concurrency) | several bands at once (GCRA, with bursts), a cap per node, per policy |
| [versions](https://github.com/quazardous/grampy/blob/main/docs/rules.md#versions-and-migration) | subjects pinned to the graph they started on, migrated all or nothing |
| [items](https://github.com/quazardous/grampy/blob/main/docs/items.md) | speak your objects: handlers name a branch, or give up a step one kind skips |
| [drawings](https://github.com/quazardous/grampy/blob/main/docs/drawings.md) | Mermaid flowchart, Mermaid state diagram, Graphviz, with live counts |

## Storage

The journal never commits and never reads your tables. Eligibility is **your**
query, passed to `claim` and read inside its transaction; a claim gives back
ids, and you load your own objects. See [drivers](https://github.com/quazardous/grampy/blob/main/docs/drivers.md).

| driver | needs |
|---|---|
| `drivers.memory` | nothing — the reference the others are confronted with |
| `drivers.sqlite` | the standard library |
| `drivers.postgres` | SQLAlchemy Core — three table layouts, [your choice](https://github.com/quazardous/grampy/blob/main/docs/drivers.md#postgresql-three-layouts-your-choice) |

Any other storage: implement the driver protocol and pass the shared contract
(`quazardous.grampy.testing.JournalContract`), concurrency tests included.

## Next to other tools

grampy is a journal, not a runner: the comparison is about where the state
lives. Each cell links the page that says so; **—** is a question that page
does not settle. Checked on 2026-09-18.

| | your own database | enqueue in your transaction | a server of its own | joins between steps | Python |
|---|---|---|---|---|---|
| **grampy** | yes: memory, SQLite, PostgreSQL | yes | no | yes: all parents, `k` of `n`, on a failure | yes |
| Graphile Worker | [yes, PostgreSQL](https://worker.graphile.org/docs) | — ([`add_job` is SQL](https://worker.graphile.org/docs/sql-add-job); rollback not stated) | [no](https://worker.graphile.org/docs) | [—](https://github.com/quazardous/grampy/blob/main/docs/concepts.md) | [no, Node.js](https://worker.graphile.org/docs) |
| BullMQ | [Redis; PostgreSQL optional](https://docs.bullmq.io/guide/postgresql) | — | no, but Redis or PostgreSQL | [flows: all children](https://docs.bullmq.io/guide/flows/continue-parent) | [yes, `bullmq`](https://docs.bullmq.io/python/introduction) |
| Hatchet | [no, the engine's](https://docs.hatchet.run/self-hosting) | — | [yes](https://docs.hatchet.run/self-hosting) | [all parents](https://docs.hatchet.run/v1/directed-acyclic-graphs) | [yes](https://docs.hatchet.run/home) |
| Inngest | [no, a managed state store](https://www.inngest.com/docs/learn/how-functions-are-executed) | — | [yes, or Inngest Cloud](https://www.inngest.com/docs/self-hosting) | [in code, `Promise.all`](https://www.inngest.com/docs/guides/step-parallelism) | [yes](https://www.inngest.com/docs/reference/python) |
| Temporal | [no, the service's](https://docs.temporal.io/temporal-service/persistence) | — | [yes, or Temporal Cloud](https://docs.temporal.io/temporal-service) | in code | [yes](https://docs.temporal.io/develop/python) |
| Oban | [yes](https://oban.hexdocs.pm/Oban.html) | [yes, `Ecto.Multi`](https://oban.hexdocs.pm/Oban.html) | [no](https://oban.hexdocs.pm/Oban.html) | [*Pro* workflows](https://oban.pro/docs/pro/Oban.Pro.Workflow.html) | [yes, `oban-py`, PostgreSQL only](https://oban.pro/docs/py/index.html) |

How each notion maps, tool by tool: [the same notions in other tools](https://github.com/quazardous/grampy/blob/main/docs/concepts.md).

## Documentation

- [The rules](https://github.com/quazardous/grampy/blob/main/docs/rules.md) — every mechanism, spelled out.
- [Items](https://github.com/quazardous/grampy/blob/main/docs/items.md) — objects instead of ids, the canonical way.
- [Drivers and candidates](https://github.com/quazardous/grampy/blob/main/docs/drivers.md) — tables, queries, your own data.
- [Writing a driver](https://github.com/quazardous/grampy/blob/main/docs/writing-a-driver.md) — the capabilities, what each method
  may return, certifying with the contract.
- [grampy's tables next to yours](https://github.com/quazardous/grampy/blob/main/docs/your-data.md) — what to read, what never to write.
- [Drawings](https://github.com/quazardous/grampy/blob/main/docs/drawings.md) — diagrams from a graph.
- [The same notions in other tools](https://github.com/quazardous/grampy/blob/main/docs/concepts.md) — Graphile Worker,
  BullMQ, Hatchet, Inngest, Temporal, Oban.
- [CONTRIBUTING](https://github.com/quazardous/grampy/blob/main/CONTRIBUTING.md) — tests, lint, how to release.

## Tests

```bash
pip install -e ".[test]"        # or: uvx --with pytest pytest
pytest                          # graph, states, memory and SQLite drivers
GRAMPY_TEST_PG_DSN=postgresql+psycopg://user:pass@localhost/test \
    pytest                      # + the postgres driver (needs sqlalchemy and a driver)
```

## License

MIT
