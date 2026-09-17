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

The first release, once the API has settled. Installed as `grampy-q`,
imported as `from quazardous import grampy`.

### Added

- Hand grampy your own objects instead of ids: an adapter says how to read
  one, and handlers answer the questions that need the data — which branch a
  choice takes, whether an optional step is for this item at all. A claim
  then loads the batch in one query and gives the objects back. It is the
  canonical way to use grampy; the id-based API underneath is unchanged.
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

[Unreleased]: https://github.com/quazardous/grampy/commits/main
