# Changelog

All notable changes to this project will be documented in this file.

> This is a curated, human-readable record — **not a commit log**. Each
> entry says *what changed and why it matters to a user*, in plain
> language, not *how* it was implemented. Skip internal refactors.
>
> **House style** for editors:
> - One short bullet per change. Multi-paragraph entries are only for
>   the major changes a user really needs to read in full.
> - No internal tracker IDs (`#NNN`, `PROJ-123`) unless that tracker
>   has a public link — they're noise otherwise. Mention the change,
>   not the ticket.
> - **Version bump = SemVer**: any `### Added` entry is at least
>   MINOR; `### Fixed` alone is PATCH; breaking change is MAJOR.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- On PostgreSQL, a claim failed when the executor returned JSON as text —
  asyncpg's default, a text loader, a host's own shim: the rows the claim's
  page brings back were walked as a string. JSON is now read decoded or as
  text, on every layout.

## [0.5.0] - 2026-09-18

### Added

- `journal.progress_many(subjects)` and `Items.progress_many(objects)` read
  many subjects' progress at once — one query on the PostgreSQL layouts —
  instead of one `progress` call per subject.
- On the PostgreSQL drivers, `arrive`, a lane keeping every version, and
  `migrate` send the same few statements whatever the number of subjects,
  instead of a few per subject: a migration of 30 subjects went from 156
  statements to 7, an arrival from 32 to 3. A driver of your own may offer
  the same through the optional `progress_many` and `rewrite_many`; without
  them the journal goes subject by subject, as before.
- The shared driver contract checks a migration that swaps two nodes over
  several subjects, and holds a driver that declares it to its number of
  statements for `arrive`, keeping every ref and `migrate`.
- The driver protocol is split into five capabilities: `CoreDriver`, and
  `VersionDriver`, `LimitDriver`, `LaneDriver`, `ReadingDriver` for what a
  graph may use. A driver offers the core and what its graphs need; a journal
  whose graph needs a capability the driver lacks refuses to be built, naming
  it and its missing methods (`MissingCapability`), instead of failing at the
  first claim. `JournalDriver` is still the whole of it.
- The shared contract certifies a driver on what it declares: a harness
  listing its `capabilities` has the tests needing another one skipped.
- A guide to writing a driver: the capabilities, which methods may return a
  superset and which must be exact, transactions, grouping keys, the optional
  fast paths, and certification.
- A guide to operating in production: the indexes worth adding on each
  PostgreSQL layout, each backed by a measured plan before and after
  (`benchmarks/indexes.py`), what grows and how the janitor keeps it bounded.
- `skip(..., limit=)` and `settle(..., limit=)`, on the journal and on
  `Items`, bound a janitor's pass: at most `limit` subjects written — per node for `settle` —
  the first in the order they would be taken. Call again while a pass writes
  `limit`; repeated passes end where one unbounded call does, which the shared
  contract checks on every driver.
- A claim on PostgreSQL costs two statements — one read, one write — on the
  row and subject layouts, three with the ready list, down from five (seven on
  a `Graph`) for a journal on the server's clock: the page
  query brings the node rows and the server's clock with it, the write seeds
  the revision rows it needs, and a subject already pinned to its graph is not
  pinned again. A driver of your own may read the clock in its page too
  (`scan_reads_clock`, and `scan(now=None)`).

### Changed

- The driver protocol and the types a driver exchanges live in
  `quazardous.grampy.protocol`; importing them from `quazardous.grampy` or
  `quazardous.grampy.journal` works as before.
- `JournalContract`'s model tests draw ten random runs by default, fifty
  with `GRAMPY_PERF=1`: a driver's default test run stays quick, the full
  mass is run knowingly.

### Fixed

- A skip on PostgreSQL could take seconds where one query takes a fraction:
  the parents' check, written as a count per candidate, was misestimated
  enough that PostgreSQL compiled the plan (JIT) at a cost higher than running
  it, and the candidates were read twice. The check is now one `EXISTS` per
  parent — which speeds up every claim's page too, and `parents_concluded` in
  your own queries — and a skip is one statement on every layout, the subject
  layout included (it went page by page). On the bundled benchmark, a skip
  of 50,000 subjects: 1.3 s → 0.6 s (row per node), 28 s → 0.8 s (row per
  subject); a claim deep in the graph: 0.47 s → 0.21 s.
- The memory driver read one subject's progress by scanning every row of
  every subject: a journal slowed down as it grew. Rows are now indexed by
  subject; the demo's simulation runs in less than half the time.

## [0.4.0] - 2026-09-18

### Added

- `journal.skip` on `PostgresDriver` and `PostgresReadyDriver` writes in two
  statements whatever the number of candidates, instead of reading them page
  by page: the rule and the revision guard are applied in SQL. A driver may
  offer the same through the optional `skip_where` method; the others keep
  the loop.
- `journal.prune_history(before)` keeps the history table from growing
  forever: it deletes what was archived before a date, except the `retry` and
  `loop` rows that retry limits and loop bounds count — so a subject pruned
  and brought back later still has only the retries it had left.
- Two more PostgreSQL layouts, picked by the class you instantiate — nothing
  switches on its own. `PostgresSubjectDriver` keeps one row per subject, its
  progress in JSONB: a claim reads its page and the progress at once and
  writes in one statement, 4 to 5 times faster than the row-per-node layout
  on the bundled benchmark. `PostgresReadyDriver` adds a list of subjects that
  may be ready; it is correct and proven, and measured no faster on that
  benchmark, which the documentation says. Both pass the same contract as the
  original driver, concurrency included.
- The SQLite driver's `schema()` takes `subject_type` ("INTEGER" or "TEXT"):
  declared with your ids' type, the subject column serves joins from your own
  tables by its index instead of scanning.
- The SQLite driver filters each page of candidates in SQL before reading
  their rows: a claim that finds nothing to take reads 2.5 times faster on a
  large table.
- The shared driver contract checks one call on 70,000 subjects — past the
  number of values a statement may bind, where a driver sending one per
  subject fails outright — and holds a driver to the number of statements a
  claim may send, when its harness declares one. The first is a performance
  test, run only with `GRAMPY_PERF=1`; every bundled driver passes it.
- `benchmarks/layouts.py` runs the three layouts on the same data and prints
  statements and time per operation, to measure your own shape of data.
- grampy's own words have names: `Status`, `Reason`, `Outcome`, `Merge`,
  `Position`, `WhileRunning`, `Backoff` and `Per`, importable from
  `quazardous.grampy`. A node name or a policy is yours, any string; a status
  or a history reason is grampy's, and now reads as such
  (`Status.DONE`, `Reason.RETRY`). Each member is also its string, so nothing
  stored changes, strings keep working everywhere, and what a storage returns
  still compares equal. `Merge.fn("name")` names a merge function without
  typing its prefix.

### Changed

- A node that groups finds the key in the candidates' column named
  `grampy_key` — or `Keyed(subject, key)` in a plain iterable — and nowhere
  else. Before, any second column counted: a query selecting a priority to
  order by grouped subjects by their priority, silently. A node grouping by
  key now refuses a candidate that carries none. **If you group today,**
  label your key column `grampy_key`, or wrap pairs in `Keyed`.
- The README opens with an example that runs as pasted — two plain objects
  through a two-step graph — and its larger example defines the loader it
  calls. Both are run by the test suite as written. It also answers "why not
  a status column?", shows where each claim is proven, gives a use for lanes,
  and compares grampy with six other tools, each cell linked to its source.
- grampy's words print as they are written: a progress reads
  `{'pay': Status.DONE}`.

### Fixed

- The PostgreSQL driver asked of an `execute` result more than its
  documentation promises (`.scalar()`, on the clock, the concurrency count and
  a lane's queue): an application executing through its own driver, honouring
  the documented `fetchall` / `fetchone` / `rowcount`, failed on its first
  claim. It now needs only those, and a test holds every layout to it.
- A claim on a node that replayed subjects had moved past read every
  candidate to take nothing: the SQL drivers now leave out, in the query
  itself, a subject whose later steps have started. The journal names them to
  the driver (`scan(..., after=)`); a driver of your own must accept the
  argument, and may ignore it.
- The PostgreSQL drivers failed outright on a call over more than about
  32,700 subjects — a janitor's `skip` over a large table, a bulk `forget` —
  on the 65,535 values one statement may bind. Every list of subjects is now
  bound as one array, whatever its length.
- An injected clock in another time zone or layout — `11:00+02:00`, `Z`, a
  space for the `T` — was compared as text with the journal's UTC times, and
  misordered them by hours without an error. Every time that comes from
  outside (a clock, `release(older_than=)`, `stages(at=)`) is now read into
  UTC; a `datetime` works too, and a time without a zone is refused.
- `journal.release()` handed back leases held on subjects of another graph
  version; it now leaves them to the journal of their own version, as
  `expire()` already did.
- A PostgreSQL graph with a group or a lane keeping every version, but no rate
  or concurrency, asked for a `limits` table it never uses; the guard it takes
  is a lock, and needs no table.
- PostgreSQL candidates carrying a `LIMIT` or an `OFFSET` keep the `ORDER BY`
  that chooses which rows those are.
- History rows archived in the same second come back in the order they were
  written, on PostgreSQL too when the history table has an `id` identity
  column (the documentation's does).
- `stages()` passes subjects to the storage as they are, instead of as text.
- The documentation's PostgreSQL arrivals table lacked the `refs` column the
  driver requires; it is there now, and the documented tables are built by the
  test suite.
- The SQL drivers refuse to run outside a transaction. Under an autocommitting
  executor every lock they take was released by the next statement, and the
  writes made of two statements lost their guarantee, with no error at all;
  they now raise on their first write, saying to open a transaction around
  the journal call.
- The documentation says how to settle rows left `running` without a lease by
  a version before leases: `conclude(…, token=None)` when the work is known
  done, `release` to hand them back.

## [0.3.1] - 2026-09-18

### Fixed

- The adapter examples in the README and the items page crashed as written:
  they still defined `inflate` with the signature of the method it replaced,
  and failed on the very objects they passed in as candidates. The README's
  example is now run by the test suite, so it cannot quietly break again.

## [0.3.0] - 2026-09-18

### Added

- Work several subjects together: a node can gather a group of a size you set,
  sharing a key your candidates carry — five bricks of one colour, ten thousand
  lines for one file — and hand them to a worker under a single lease. A group
  goes whole or not at all, and nothing is stored while one fills. Set a
  maximum wait and a short group goes anyway once its oldest member has waited
  that long; leave it out and the group waits until it is full.
- A page on living next to grampy's tables: what each column means when you
  read it, the joins worth having, and the one thing never to do — write to
  them yourself, which slips past the guard every claim relies on.

### Changed

- The documentation no longer calls two different things a version. A subject
  has one graph version, pinned for its lifetime, and as many refs as it has
  comebacks.

## [0.2.0] - 2026-09-18

The first release. Installed as `grampy-q`, imported as
`from quazardous import grampy`.

> It starts at 0.2.0 because 0.1.0 was built, rehearsed on TestPyPI and
> never published: the API moved too much that evening to stand behind it,
> and a version number on TestPyPI can never be reused.

### Added

- Lanes can keep every version a subject brings back, not just one:
  `Lane.batch()` gathers them in the order they arrived, up to a size
  you set, and `journal.refs()` reads the batch. Beyond that size the
  oldest is let go, and the history says so.
- Decide a merge yourself: a lane may name a function
  (`Lane(merge="fn:my-rule")`) that the journal is given, and that says
  what stays. The workflow document still holds only the name, so it
  stays data. Merges that have to read before writing are serialised, so
  two versions arriving at the same instant never overwrite each other.
- Say a duration the way Python says one: `lease=timedelta(minutes=2)`
  works wherever `"2m"` did, and is written down in the same short text
  form, so a stored graph still reads well and still round-trips to JSON.
- Hand grampy your own objects instead of ids: an adapter says how to read
  one, and handlers answer the questions that need the data — which branch a
  choice takes, whether an optional step is for this item at all. A claim
  then loads the batch in one query and gives the objects back. It is the
  canonical way to use grampy; the id-based API underneath is unchanged.
  The janitor's pass speaks items too, so an application need never hold
  an id of its own.
- Declare a workflow as a graph of nodes, with forks that run in parallel,
  joins that wait for all their parents, and optional nodes that can be
  skipped without blocking their children.
- An online demo: a brick sorting line animated in the browser, run by the
  real library, that sorts bricks by colour; a TNT brick hides its colour,
  waits for the bomb squad, gets retried at the defuse station, and either
  shows its colour or ends in the waste bin; a sorted brick sent back waits
  in an inbox lane before it runs again.
- Draw a workflow as a Mermaid flowchart or a Graphviz graph, every
  mechanism with its own shape — lanes, rate limits and concurrency
  included — optionally with the live count of subjects per step; or as a
  Mermaid state diagram, read the way a statechart is.
- Write a workflow as data: a versioned JSON document (name, namespace,
  version, nodes) that can be stored, compared between versions and read
  back; unknown keys and wrong types are refused with their path.
- Say how a node joins its parents, as data: which statuses of each parent
  it accepts (a compensation that runs when a step failed), and how many
  parents are enough (two engines out of three).
- Keep the history: forgetting a node, releasing a lease or looping back
  archives the rows taken away, with when and why, readable per subject.
- Declare retries: a failed node is scheduled again after a delay that
  grows (constant, linear or exponential, capped, with jitter), a limited
  number of times, before the failure counts.
- Give each node its own lease: one call releases every claim held longer
  than its node allows, so a slow step and a fast one need not share a
  timeout.
- Wait for external events: signals are recorded durably, even before the
  wait begins, and a janitor call settles waiting steps as done — or failed
  after a timeout, so that a reminder or an escalation can follow.
- Give optional steps a grace period, after which they are skipped instead
  of blocking the steps that follow them.
- Change a workflow while subjects are in flight: each subject stays on the
  graph version it started on, and a migration moves the subjects that fit
  the new version — or refuses them all, saying which do not and why.
- Treat some subjects differently without a second workflow: put them under a
  named policy, and let that policy change retries, leases, timeouts, grace
  periods, rate limits and lanes — never the steps themselves. Each policy can
  hold its own rate and concurrency budget, which is the one thing an
  application cannot enforce on its own across several machines.
- Let subjects come back: a lane is a way in where each new version of a
  subject waits — merged with the one already waiting, kept back by a
  cooldown after its last pass, a quiet delay or a maximum wait — and then
  runs through the workflow again, the previous pass kept in the history.
  Urgent arrivals skip the wait, a running pass is never cut short, and a
  rate on the lane lets arrivals out in the order they came.
- Protect a costly resource anywhere in a workflow: give a node rate limits
  (several bands at once, such as 100 a minute and 1000 an hour, with bursts)
  and a cap on how many subjects it works at the same time, shared by all
  policies or counted per policy. Workers claiming at the same moment never
  go past them, on any storage.
- The journal reads the time from the storage — the database server for
  PostgreSQL — so that workers on different machines agree on what is due.
- Declare bounded loops: a node that fails (or ends another chosen way)
  sends the subject back to an earlier node, at most a given number of
  times, after which the failure stands and can be escalated.
- Exclusive choices: a choice node concludes by naming its branch, and the
  other branches — with whatever only they lead to — are marked omitted at
  once, so the steps after the branches still proceed.
- Validate a graph at startup: unknown parents, duplicate names, cycles,
  several entry points, a state posted by two nodes and joins that could
  never be met are rejected before any work is claimed.
- Identify subjects with your own ids, integers or strings, given back
  exactly as they were given.
- Ask which nodes a subject can start next, as a pure function of its
  progress — no storage needed.
- Claim a node for a batch of subjects on behalf of remote workers, then
  conclude, fail or skip it; a subject is never handed out twice for the
  same node, nor started on a parent another worker is forgetting at the
  same moment. Each claim returns a token, and only the holder of that
  token can conclude or fail what it took: a slow worker whose lease was
  released and handed to another cannot overwrite the newcomer's work.
- Go back in a workflow: forget a node to run it again, list what must be
  undone when a subject returns to an earlier state, and find where a
  replay can start.
- Record work done outside the journal (adopt), and release claims left
  behind by workers that never came back.
- Derive the allowed state transitions from the graph, so a database
  trigger or an assertion can refuse any transition the workflow does not
  declare.
- In-memory driver with no dependency, deterministic and thread-safe, for
  tests and prototypes.
- SQLite driver on the standard library alone, for small deployments and
  tests against a real file shared by several processes.
- PostgreSQL driver (SQLAlchemy Core) working on tables you declare — node
  rows, one revision per subject, the history, and two more only when a
  workflow needs them (rate and concurrency state, lane arrivals) — with
  eligibility expressed as your own `SELECT`.
- A shared contract test suite any new driver can subclass to prove it
  behaves like the reference, including concurrency tests (competing
  claimers, claims racing a requeue) and random sequences of operations on
  random graphs checked step by step against the rule.

[Unreleased]: https://github.com/quazardous/grampy/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/quazardous/grampy/releases/tag/v0.5.0
[0.4.0]: https://github.com/quazardous/grampy/releases/tag/v0.4.0
[0.3.1]: https://github.com/quazardous/grampy/releases/tag/v0.3.1
[0.3.0]: https://github.com/quazardous/grampy/releases/tag/v0.3.0
[0.2.0]: https://github.com/quazardous/grampy/releases/tag/v0.2.0
