# Operating in production

What to add to grampy's tables once they hold real volume, what grows, and
what to watch. Every index below is recommended because a measured plan
changed; one that changed nothing is listed as such, since each index costs
every write.

- [Indexes](#indexes)
- [What grows](#what-grows)
- [The janitor](#the-janitor)
- [What to watch](#what-to-watch)

## Indexes

The primary keys serve the claim: `(subject, node)` on the node rows, the
subject on the revisions and on the subjects table. What they do not serve is
every call that reads **by node** rather than by subject, and the history,
which has no key of its own.

Measured with `benchmarks/indexes.py` on a local PostgreSQL 16, with 100,000
subjects and 300,000 history rows. Each call is run by the real driver, and
its statements are run under `EXPLAIN (ANALYZE, BUFFERS)` without, then with,
the index:

| index | call | without | with |
|---|---|---:|---:|
| `history (subject, node, reason, archived_at)` | retries of a node, 30 subjects (a retry limit) | 22.1 ms · 5,303 pages | 0.3 ms · 123 pages |
| | latest signal, 30 subjects (settling a wait) | 21.7 ms · 5,303 pages | 0.1 ms · 120 pages |
| `nodes (node, status, started_at)` | running rows of a node (a concurrency limit) | 14.8 ms · 2,950 pages | 0.0 ms · 15 pages |
| | status counts of a node (`journal.counts`) | 15.6 ms · 2,958 pages | 1.1 ms · 70 pages |
| | stale leases of a node released | 66.1 ms · 2,950 pages | 0.1 ms · 3 pages |
| `subjects (jsonb_extract_path_text(nodes, 'match', 'status'))` | running rows of a node, row per subject | 27.4 ms · 9,321 pages | 0.0 ms · 6 pages |
| | status counts of a node, row per subject | 20.5 ms · 9,337 pages | 2.5 ms · 404 pages |
| `arrivals (node)` | arrivals waiting in a lane | 4–5 ms · 494 pages | 4–7 ms · 265 pages |

A local server has no network in between, and a warm cache: the times rank
the plans, they do not predict yours. The pages read transfer as they are.

### Every layout: the history

```sql
CREATE INDEX ON job_node_history (job_id, node, reason, archived_at);
```

Retry limits and loop bounds count history rows (`archived`), and a wait is
settled by the latest signal (`latest`): without it, each of those reads the
whole history, and the history only grows.

### Row per node, and the ready list

```sql
CREATE INDEX ON job_nodes (node, status, started_at);
```

Serves every read by node: a node's running rows (a concurrency limit reads
them on every claim), `journal.counts`, and `release` of stale leases.

### Row per subject

A node's status lives inside the JSON document, so there is no one index for
every node: index **the nodes you read by status** — those with a
concurrency limit, and those whose counts you watch:

```sql
CREATE INDEX ON job_subjects (jsonb_extract_path_text(nodes, 'call', 'status'));
```

Write it exactly so. The driver reads a field as
`jsonb_extract_path_text(nodes, node, field)`, and PostgreSQL uses an
expression index only for the same expression: an index on
`nodes -> 'call' ->> 'status'` is never used.

### Not recommended

- `arrivals (node)`: the plan changed, the time did not. The table holds only
  what waits, and the primary key already starts with the subject a lane's
  door reads by.
- An index on the revisions' `policy` or `version`: no call reads them
  without a subject, which the primary key serves.

## What grows

| table | grows with | kept small by |
|---|---|---|
| node rows (or the subjects' documents) | subjects × nodes | your own retention of finished subjects |
| history | every retry, loop, forget, release, lane entry, signal and note | `journal.prune_history(before)` |
| ready list (that layout) | subjects that may have become claimable | claims, which strike their pair |
| arrivals | versions waiting in a lane | `settle`, which lets them in |

The history is the one that grows without bound. `journal.prune_history(
before)` deletes what was archived before a date — except the `retry` and
`loop` rows, which retry limits and loop bounds count: a subject brought back
after pruning still has only the retries it had left. See
[your data](your-data.md#the-history-table).

## The janitor

`expire()` gives back expired leases; `settle(candidates)` concludes waits,
lets due arrivals in and skips what is past its grace; `prune_history`
bounds the history. On a large backlog, work in passes:

```python
while True:
    with conn.begin():
        done = journal.settle(candidates, limit=1000)
    if all(sum(counts.values()) < 1000 for counts in done.values()):
        break
```

A pass writes at most `limit` subjects per node, in a transaction of its own
size; passes repeated until one falls short end where one unbounded call
would. `skip(name, candidates=…, limit=…)` works the same way.

## What to watch

`journal.snapshot(candidates)` gives, per node, what grampy alone can
compute right — the rest (sampling, history, dashboards, thresholds, alerts)
is yours, since it depends on your tools and your business:

| key | what it says |
|---|---|
| `running`, `scheduled`, `done`, … | rows per status, as `journal.counts` |
| `oldest_running` | seconds the longest-running row has run — a stuck worker, before `expire` |
| `next_due` | seconds until the earliest retry is due; negative when overdue |
| `waiting`, `oldest_waiting` | a lane's arrivals, and how long the oldest has waited |
| `ready` | how many candidates a claim could take now, by the claim's own rule |
| `oldest_ready` | seconds since the oldest of those became ready — starvation |

Read between two samples: **a growing node** is `ready` rising; **a starving
one** is `oldest_ready` rising while `ready` does not fall; **a stuck worker**
is `oldest_running` past the node's lease. `ready` needs your candidates and
counts before rate and concurrency limits; without candidates, the snapshot
gives only what the journal knows.

It reads, never writes. On the PostgreSQL layouts `ready` is one statement per
node. On the bundled benchmark — 100,000 subjects, five nodes, without the
indexes above — a whole snapshot takes 16 statements and about 1 s with your
candidates, 11 statements and 0.4–0.6 s without: fine for a sample every
minute, too much for every claim. `nodes=` narrows it to the ones you watch.

As Prometheus metrics, with no dependency:

```python
def metrics(snapshot, prefix="grampy"):
    lines = []
    for node, values in snapshot.items():
        for key, value in values.items():
            if value is not None:
                lines.append(f'{prefix}_{key}{{node="{node}"}} {value}')
    return "\n".join(lines) + "\n"
```

Also worth a look:

- **The history's size**, and how old its oldest row is: it tells whether
  pruning keeps up.
- **The claim's page query**, the statement you will see most: it carries
  your candidates. Its cost is yours as much as grampy's; `EXPLAIN` it with
  your candidates to see whether your own filter is indexed.

Run `benchmarks/indexes.py` on your own shape of data before adding an index
this page does not measure.
