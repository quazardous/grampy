# Writing a driver

A driver stores rows and offers atomic operations on them. It never decides:
what is claimable, what a join accepts, when a retry is due — the journal
decides all of it, and the driver only has to answer and write exactly what it
is asked. This page says what a driver must offer, what each method may and
may not return, and how to prove it with the shared contract.

- [Capabilities: offer what your graphs use](#capabilities-offer-what-your-graphs-use)
- [The data a driver keeps](#the-data-a-driver-keeps)
- [Exact, or a superset](#exact-or-a-superset)
- [Transactions](#transactions)
- [Grouping keys](#grouping-keys)
- [Optional fast paths](#optional-fast-paths)
- [Certifying a driver](#certifying-a-driver)

## Capabilities: offer what your graphs use

The protocol is split into five capabilities, each a `typing.Protocol`
importable from `quazardous.grampy`. `JournalDriver` is all five.

| capability | protocol | needed by | methods |
|---|---|---|---|
| `core` | `CoreDriver` | every journal | `scan`, `insert_if_unchanged`, `conclude`, `adopt`, `forget`, `release`, `enroll`, `policies`, `history`, `archived`, `latest`, `note`, `prune_history`, `now`, `guard`, `progress` |
| `versions` | `VersionDriver` | a journal built on a `Graph` | `pin`, `versions`, `rewrite` |
| `limits` | `LimitDriver` | a node with a `rate` or a `concurrency` | `limits`, `set_limits`, `running` |
| `lanes` | `LaneDriver` | a node with a `lane` | `arrive`, `arrivals`, `enter`, `queued` |
| `reading` | `ReadingDriver` | `journal.counts`, `journal.stages`, `journal.parents_concluded` | `status_counts`, `stages`, `parents_concluded` |

Some methods sit in the core even though they look like extras. The history
(`note`, `archived`, `latest`) is there because retry limits and loop bounds
count its rows: without it, `Retry` and `Loop` cannot be honoured. `guard` is
there because groups and lanes serialise on it, not only limits. A node's
variant in a policy counts as much as the node: a policy giving a node a
concurrency needs `limits`.

**The journal checks when it is built.** A graph that needs a capability the
driver lacks raises `MissingCapability`, naming the capability, what needs it,
and the missing methods — not an `AttributeError` at the first claim.
`needed_capabilities(graph)` tells you in advance; `capability_methods(name)`
lists a capability's methods. The reading methods are checked when they are
called, since no graph needs them.

## The data a driver keeps

- **Node rows:** one per `(subject, node)`, with `status`, `started_at`,
  `finished_at` and `lease`.
- **A revision per subject:** 0 for a subject never forgotten, raised by every
  write that takes rows away (`forget`, a loop, a lane entry, a migration).
  The registry holding it also holds the subject's policy and the version it
  is pinned to.
- **The history:** every row taken away, with when and why (`reason`), and the
  notes the journal appends (signals, merges, skips).
- With `limits`: the limiter state, a value per key. With `lanes`: the arrivals
  waiting in each lane.

**What a SQL driver reads back depends on the adapter**, not on the driver:
psycopg decodes JSON, asyncpg returns it as text by default, and a host may
put a shim in between. A driver that takes an `execute` callable reads what it
selects in either form — the PostgreSQL drivers decode JSON given as text.

**A subject is the application's id:** an `int` or a `str`, stored and returned
exactly as given — never converted, built or split. Times are strings in the
journal's format (`utc_now()`), compared as text: store them as given.

## Exact, or a superset

Most methods are **exact**: they do exactly what their docstring says, no
more, no less. A few are **pre-filters**, allowed to return more than needed
because the journal judges each result again:

| method | may return | must never |
|---|---|---|
| `scan` | a superset: it may leave out a candidate the docstring lists (a row for the node, a parent not satisfying, a descendant started), and pre-filtering nothing is correct, only slower | leave out anything else, or change the candidates' order |
| `progress_many` | subjects without rows may be left out | leave out a subject that has rows |
| every other method | exactly its docstring | — |

The atomic ones are where a driver earns its keep:

- `insert_if_unchanged` writes only for subjects whose revision is still the
  one read, and without a row for the node (a `scheduled` row due by now
  aside). Two claimers racing on one subject: one writes, the other gets
  nothing.
- `forget`, `conclude` with `reset`, `enter` and `rewrite` raise the revision
  in the same unit as they take rows away, so no claim that read the old
  revision can succeed afterwards.
- `guard(keys)` serialises writers on the same keys until the caller's
  transaction ends. Take the keys in one global order (the journal passes them
  sorted), or two callers may each hold one the other waits for.

The PostgreSQL ready list shows what a superset may be: a pair listed in
`ready` means "may have become claimable", the claim still pre-filters and the
journal still judges, so listing too much costs a read and listing too little
would starve a subject — the driver always errs on the side of listing.

## Transactions

The journal never opens a transaction and never commits: **the application
does, around each journal call.** A driver's guarantees live inside that
transaction — locks held until it ends, writes made of two statements seen
as one. A driver on a transactional storage should refuse to run outside one
(both SQL drivers raise on their first write); a storage without transactions
must make each call atomic on its own, as the memory driver does.

## Grouping keys

A node that groups (`Node(group=…)`) takes subjects together by a key the
candidates carry. In SQL candidates, the key is **the column named
`grampy_key`**, and nothing else — a column selected to order by is never
taken for a key. In a plain iterable, it is **`Keyed(subject, key)`**; a bare
tuple is refused. `scan` returns it in `Entry.key`, compared by the journal and
never interpreted by the driver.

## Optional fast paths

Five methods and one flag are optional. Leave them out and the journal does
the same work through the required ones, with more round trips:

| method | replaces | condition |
|---|---|---|
| `skip_where(name, candidates, *, parents, now, version, limit=None)` | `skip` reading every candidate page | writes exactly what the loop would — with `limit`, for the first `limit` eligible candidates; used only for a node joining its parents plainly. `limit` is passed only when the caller sets one |
| `progress_many(subjects)` | `progress` per subject | in `arrive` and `migrate` |
| `rewrite_many(subjects, *, rename, drop, version, now)` | `rewrite` per subject | in `migrate` |
| `ready_count(name, candidates, *, parents, after, now, version)` | walking the candidates for a snapshot's `ready` | counts exactly what a claim could take, and returns the oldest ready time; for a node joining its parents plainly |
| `node_times(name, *, waiting)` | nothing: without it, a snapshot's ages are None | the earliest `running` and `scheduled` start, and a lane's first arrival |
| `scan_reads_clock = True` | `now()` before each claim | `scan(now=None)` uses the storage's clock in the page query and yields `Page`s carrying it; used when the journal runs on the driver's clock |

A fast path must give the same result as the loop it replaces. The PostgreSQL
drivers test `skip_where` against the journal's own loop on every progress of
the contract's sweep; `progress_many` and `rewrite_many` answer to the
contract's lane and migration tests, which every driver runs.

## Certifying a driver

Subclass `quazardous.grampy.testing.JournalContract` and give it a `harness`
fixture; `tests/test_memory_driver.py` is the smallest example, and the
contract's module docstring lists what a harness provides. The contract
includes concurrency tests written in sessions (open, act, commit): how your
storage stays correct under them is up to it, the outcome is not.

```python
class TestMyDriver(JournalContract):
    @pytest.fixture
    def harness(self):
        return MyHarness()
```

A harness may declare:

- `capabilities` — the ones the driver offers, `"core"` always. A test that
  needs one left out is skipped; left out entirely, every capability is
  expected, and a missing one fails. `tests/test_capabilities.py` certifies a
  core-only driver this way.
- `statement_bounds` and `statements()` — the most statements a claim,
  `arrive`, keeping every ref and `migrate` may send, as `(base,
  per_subject)`. The contract holds the driver to its own word, which is how a
  round trip per subject is kept from coming back.

The one test long by its size — a call over 70,000 subjects, past the number of
values one SQL statement may bind — runs only with `GRAMPY_PERF=1`.

No guarantee may rely on a storage-specific mechanism: it is defined in the
driver protocol and proven by the shared contract.
