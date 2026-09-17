# The rules

What grampy guarantees, mechanism by mechanism. The short version is in the
[README](../README.md); the names other tools give these notions are in
[concepts](concepts.md).

- [Subjects and the claim rule](#subjects-and-the-claim-rule)
- [Joins, choices and failure edges](#joins-choices-and-failure-edges)
- [Going back: history, loops, replay](#going-back-history-loops-replay)
- [Time: retries, leases, waits, grace](#time-retries-leases-waits-grace)
- [Lanes: subjects that come back](#lanes-subjects-that-come-back)
- [Channels: one workflow, several sources](#channels-one-workflow-several-sources)
- [Rate limits and concurrency](#rate-limits-and-concurrency)
- [Versions and migration](#versions-and-migration)
- [What holds it together](#what-holds-it-together)

## Subjects and the claim rule

**A subject is identified by the application.** Its id is unique, stable, and
an integer or a string; grampy stores it and gives it back exactly as given,
never builds nor splits one. The storage's subject column follows the
application's type (`BIGINT`, `TEXT`…), and the driver contract runs with both.

grampy holds ids, never your data: between claiming and working, you load your
own objects — see [drivers](drivers.md#candidates-are-your-sentence).

A node is **claimable** when it has no row, no descendant has started, and it
is **joined**: by default every parent is `done`, `skipped` or `omitted`;
`failed` satisfies nobody.

**Never backwards**: once a descendant has started, the node is closed. Going
back is `forget` — the absence of a row means "never started".

Only an **optional** node can be `skip`ped, and only when it is itself
claimable (its parents concluded). `adopt` records work done outside the
journal and never overwrites.

`Node.working` / `Node.state` are **projections** an application may mirror on
its subjects; nothing in grampy reads them to decide. They are what
`allowed_transitions` and the replay helpers are derived from.

## Joins, choices and failure edges

**Joins are data.** `on` says, per parent, which statuses a node accepts —
`Node("refund", parents=("pay", "reserve"), on={"reserve": ("failed",)})` runs
on a failure — and `need=k` starts a node once `k` parents are accepted: the
others, not started yet, are closed by it.

**An exclusive choice names its branch.** A `choice` node concludes with
`branch=`; the other branches, and every node only they lead to, are written
`omitted` in the same write. A join after the branches goes on, since
`omitted` satisfies it.

## Going back: history, loops, replay

**Nothing is lost.** `forget`, `release` and loops move rows to a history
(`journal.history(subject)`), with when and why.

**Loops are declared and bounded.** `Node("review", parents=("draft",),
loop=Loop(to="draft", max=3))`: a failed review sends the subject back to
`draft`, in the same write, at most three times; after that the failure stands
and a failure edge can escalate.

## Time: retries, leases, waits, grace

**Retries are declared.** `Node("call", retry=Retry(limit=3, delay="10s",
backoff="exponential", max_delay="5m", jitter=0.1))`: a failure is archived and
the node `scheduled` again; the row becomes claimable when due. Past the limit
the failure stands, for a loop or a failure edge.

**Leases per node.** `Node("fetch", lease="2m")`, `Node("ai_tag", lease="1h")`:
`journal.expire()`, called by the application's janitor, gives back every row
held longer than its node allows — archived, then claimable again.

**Waits, signals and grace.** `Node("clicked", parents=("send",),
wait="email.clicked", timeout="7d")` is not worked but settled:
`journal.signal(subjects, "email.clicked")` records the event durably — even
before the wait begins — and `journal.settle(candidates)` concludes the wait
`done`, or `failed` once the timeout has passed since its parents concluded.
`Node(optional=True, grace="1d")` is skipped by `settle` when nobody took it in
time.

**One clock.** The journal takes its time from the driver — the database server
for PostgreSQL — so workers on several machines agree on what is due.

## Lanes: subjects that come back

`Node("arrive", lane=Lane.throttle(cooldown="24h", max_wait="3d"))` is a way in
that no worker claims: `journal.arrive("arrive", ["offer-12"], ref="v7")` puts
a new version of the subject in it, and `journal.settle(candidates)` lets it
through once due — archiving the previous pass through what follows, in the
same write.

A version arriving while one waits is merged (`merge="first"|"last"`, keeping
the first one's place or not); `cooldown` holds a subject back that long after
its last pass ended, `delay` waits for quiet, `max_wait` caps both, an
`urgent=True` arrival skips them, and a pass still running is never cut short
(`while_running="queue"|"skip"`). A `rate` on the lane lets arrivals out in the
order they came.

Presets carry the names other tools use: `Lane.throttle` (Graphile Worker's
`preserve_run_at`), `Lane.debounce`, `Lane.dedupe`.

## Channels: one workflow, several sources

`journal.enroll(subjects, "partner-a")` records a subject's channel; a
`Graph(..., channels={"partner-a": {"call": {"retry": Retry(5, "1m")}}})`
changes, for that channel only, a node's `retry`, `lease`, `timeout`, `grace`,
`rate`, `concurrency` or `lane` settings — never the structure.

### One source needs an extra step

A channel never adds a node: the workflow would differ per source, and so
would its joins, its migrations and its drawing. Three ways to get the same
result, cheapest first.

**1. Declare the step for everyone, skip it where it does not apply.** An
`optional` node is satisfied by being `skipped`, so nothing downstream waits
for it:

```python
GRAPH = Graph(Document("offers"), (
    Node("scrape"),
    Node("enrich", parents=("scrape",), optional=True),   # only some sources
    Node("publish", parents=("enrich",)),
), channels={"plain-source": {"enrich": {"grace": "1s"}}})   # …skipped there
```

- for the sources that need it, a worker claims `enrich` as usual;
- for `plain-source`, `journal.settle(candidates)` skips it once its grace has
  passed — a second, so at the first janitor pass.

The two halves work together: the **worker** offers only the subjects of the
sources that want the step — which subjects it offers is its own sentence —
and the **grace** is the safety net, so the ones nobody will take are skipped
instead of waiting forever. A worker that offered every subject would claim
the step before the grace ever elapsed.

Without a grace the node is never skipped on its own, and the application can
still skip it explicitly for the subjects it chooses:
`journal.skip("enrich", candidates=…)`.

**2. The routes really differ: a `choice` on the channel.** Put a choice node
first and conclude it with the branch that source takes; the other branches,
and whatever only they lead to, are `omitted` in the same write, and a join
further down still proceeds.

**3. Almost nothing is shared: a second graph.** Another `Document`, its own
nodes, and those subjects pinned to it. Two graphs mean two things to keep
alive — worth it only when they really are two workflows.

## Rate limits and concurrency

`Node("summarise", parents=("fetch",), rate=(Rate(100, "1m"), Rate(1000, "1h",
burst=50)), concurrency=4)` — a claim takes no more than every band lets
through (GCRA, one number stored per band) nor more than 4 rows running at
once. `per="channel"` gives each channel its own budget. The journal decides;
the driver only guards the budget's keys while it does, so two claimers never
overspend it.

## Versions and migration

A journal on a `Graph` pins each subject to its `document.version` and leaves
the subjects of other versions alone, so v1 and v2 run side by side.
`journal_v2.migrate(subjects, V1, {"crop": "trim", "old_step": None})` moves the
subjects whose rows could have been written on v2 — renamed, dropped nodes
archived — or refuses them all, naming each one that is not compliant and why.

## What holds it together

**A conclusion proves it holds the lease.** Every claim returns a `Lease` — the
subjects taken, and a `token` unique to that claim. `conclude` and `fail` only
touch rows holding the token they bring, so a slow worker whose lease was
released and taken by another rewrites nothing. `token=None` is an explicit
operator override.

**The journal decides, the driver stores.** A claim reads the candidates' rows,
applies the rule in Python, and writes only if nothing was forgotten in
between: every subject carries a revision that `forget` raises. Two workers
never hold the same node, and a node is never taken on the strength of a parent
forgotten meanwhile.
