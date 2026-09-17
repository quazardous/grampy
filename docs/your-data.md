# grampy's tables next to yours

grampy holds ids and rows per `(subject, node)`. Your data stays yours, and
the two live side by side in the same database.

This page is about that cohabitation: **what you may read, what you must not
write, and what the columns actually mean.** If you find yourself guessing at
SQL to answer a question about your own subjects, something here should have
told you instead.

- [What grampy owns](#what-grampy-owns)
- [Reading is expected](#reading-is-expected)
- [Writing is not](#writing-is-not)
- [Where to put the tables](#where-to-put-the-tables)
- [One query, not one per subject](#one-query-not-one-per-subject)

## What grampy owns

Five tables, of which two are optional. You declare them — grampy creates
nothing — so they are yours to name, index and back up. What follows is what
each column **means when you read it**; the shape to create is in
[drivers](drivers.md#postgresql-the-tables-you-declare).

### The node table — where each subject stands

| column | what it says |
|---|---|
| subject | your id, exactly as you gave it |
| `node` | the node this row is about |
| `status` | `running`, `done`, `skipped`, `failed`, `omitted`, `scheduled` |
| `started_at` | when the row was written — **see the trap below** |
| `finished_at` | when it concluded; `NULL` while running |
| `lease` | the token of the claim that took it |

**No row means "never started".** The absence of a row is the initial state,
and it is how the claim rule reads a graph: there is nothing to look up for a
node a subject has not reached.

**The trap:** on a `scheduled` row — a retry waiting for its delay —
`started_at` holds **when it becomes due**, not when anything started. That is
what makes a claim able to pick it up at the right moment with one comparison.
Read it as "due at" whenever the status is `scheduled`.

**`lease` survives the conclusion.** It is not cleared when a node finishes, so
every subject taken by one claim still carries that claim's token afterwards.
That is what makes a batch findable: the members of a group all share it, and
it identifies the claim that actually succeeded — if a lease expired and the
work was taken again, the token is the second one's.

### The revisions table — one row per subject

| column | what it says |
|---|---|
| subject | your id |
| `revision` | a counter `forget` raises; the claim's guard against a lost update |
| `policy` | the policy this subject runs under, or `NULL` |
| `version` | the graph it is pinned to — `namespace/name@version` |

### The history table

The node table's columns, plus `archived_at` and `reason` — `forget`,
`release`, `loop`, `retry`, `migrate`, `arrival`, `lane`. Nothing is ever
deleted: a row taken away moves here. This is where "how many times did this
go round" and "who held it before" are answered.

### The two optional ones

`limits` (key, value) carries rate and concurrency state, and is needed only by
graphs that declare them. `arrivals` carries what waits in a lane — `ref`, the
whole `refs` list when the lane keeps them, `place`, `arrived_at`, `urgent`.

## Reading is expected

Join on the subject. That column holds your id, with your type, so it joins to
your table with no translation:

```sql
-- where is each offer, and since when
SELECT o.id, o.title, n.node, n.status, n.started_at
FROM offers o
JOIN job_nodes n ON n.job_id = o.id
WHERE o.tenant = :tenant;

-- what is stuck: running for more than an hour
SELECT o.id, n.node, n.started_at
FROM offers o JOIN job_nodes n ON n.job_id = o.id
WHERE n.status = 'running' AND n.finished_at IS NULL
  AND n.started_at < :one_hour_ago;

-- everything that went out in one batch, by its claim token
SELECT o.* FROM offers o JOIN job_nodes n ON n.job_id = o.id
WHERE n.node = 'pack' AND n.lease = :token;

-- how many are at each step
SELECT node, status, count(*) FROM job_nodes GROUP BY node, status;
```

These are **lookups**: they read a fact already written, and they stay correct
whatever the graph later becomes.

Be careful with the other kind — a query that tries to work out *what is
claimable* re-implements the claim rule in SQL, and will quietly disagree with
it the day a join, a choice or a loop changes. When you need that, let the
claim answer: it exists for exactly this, and its candidates are
[your sentence](drivers.md#candidates-are-your-sentence).

## Writing is not

**Never write to grampy's tables yourself.** Not an `UPDATE` to unstick
something, not a `DELETE` to clean up, not an `INSERT` to pre-seed.

The reason is not territorial. A claim reads rows, decides in Python, then
writes only if the subject's `revision` has not moved — that is what stops two
workers taking the same node, and what stops a node being taken on the strength
of a parent forgotten meanwhile. **A write from outside does not raise that
revision**, so it slips through a window the library spends its whole design
closing. Nothing will look wrong; a claim will simply be granted that should
not have been, once in a while, under load.

Everything you might reach for SQL to do has a call:

| the temptation | the call |
|---|---|
| unstick a node | `journal.forget(node, subjects)` |
| give back a dead worker's leases | `journal.release(...)`, `journal.expire()` |
| mark work done that happened elsewhere | `journal.adopt(node, subjects)` |
| skip a step | `journal.skip(node, candidates=…)` |
| move subjects to a new graph | `journal.migrate(...)` |

Each of them archives what it takes away, with a reason, so the history stays
true. A hand-written `DELETE` loses that silently.

## Where to put the tables

**In the same database as your own.** The journal never commits: it writes
through your connection, inside your transaction, so a node row and your own
row move together or not at all. Put grampy's tables elsewhere and you have two
transactions and the dual-write problem back.

Same schema or a schema of their own is your call — you name the tables. The
indexes that matter are the primary keys the driver needs; add whatever else
your own queries want, they are your tables.

## One query, not one per subject

Reading a subject's progress one at a time is fine for a handful and expensive
for a page:

```python
for offer in offers:                      # N queries, one per offer
    progress = journal.progress(offer.id)
```

There is no plural read for this today — `journal.progress` and
`journal.arrival` take one subject. `journal.stages(subjects, at=…)` does take
a batch, and so does the driver's own `arrivals(subjects, node)`.

Until the plural reads exist, a dashboard over many subjects is better served
by one of the joins above, run against the tables directly. That is a legitimate
use of this page, not a workaround.

Back to the [documentation index](README.md).
