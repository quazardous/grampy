# grampy

A small workflow graph for work queues that already live in a storage —
a database, a key-value store, or plain memory: the storage is a driver.

```bash
pip install grampy-q              # from quazardous import grampy
pip install "grampy-q[postgres]"  # + the PostgreSQL driver (SQLAlchemy)
```

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
from quazardous.grampy import Node, NodeJournal, check_dag
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.items import Adapter, Items

DAG = (
    Node("fetch", working="fetching", state="fetched"),
    Node("crop", parents=("fetch",), optional=True),
    Node("read", parents=("fetch",), optional=True),
    Node("judge", parents=("crop", "read"), state="judged"),
)
check_dag(DAG)

class Docs(Adapter):                       # how grampy reads YOUR object
    def id_of(self, doc):  return doc.id
    def load(self, ids):   return [LIBRARY[i] for i in ids]
    def applies(self, doc, node):          # one graph, subjects that differ
        return node != "crop" or doc.scanned

items = Items(NodeJournal(MemoryDriver(), DAG), Docs())
lease = items.claim("fetch", 10, candidates=my_loader())    # objects in…
for doc in lease:                                           # …and objects out
    ...
items.conclude("fetch", lease)
```

Handing grampy your objects is the canonical way to use it — see
[items](https://github.com/quazardous/grampy/blob/main/docs/items.md). The
core underneath works on ids alone and stays available: `journal.claim("fetch",
10, candidates=[…])` returns subjects, and never reads your data.

The package lives in the `quazardous` namespace; the distribution is `grampy-q`
(`grampy` was already taken on PyPI; the *q* is for queue).

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
| [lanes](https://github.com/quazardous/grampy/blob/main/docs/rules.md#lanes-subjects-that-come-back) | a subject that comes back waits, merges with the version waiting, runs again after a cooldown |
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
| `drivers.postgres` | SQLAlchemy Core |

Any other storage: implement the driver protocol and pass the shared contract
(`quazardous.grampy.testing.JournalContract`), concurrency tests included.

## Documentation

- [The rules](https://github.com/quazardous/grampy/blob/main/docs/rules.md) — every mechanism, spelled out.
- [Items](https://github.com/quazardous/grampy/blob/main/docs/items.md) — objects instead of ids, the canonical way.
- [Drivers and candidates](https://github.com/quazardous/grampy/blob/main/docs/drivers.md) — tables, queries, your own data.
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
