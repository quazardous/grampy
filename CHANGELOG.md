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

First release, planned as 0.1.0. Installed as `grampy-q`, imported as
`from quazardous import grampy`.

### Added

- Declare a workflow as a graph of nodes, with forks that run in parallel,
  joins that wait for all their parents, and optional nodes that can be
  skipped without blocking their children.
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
- Run one workflow for several sources: give subjects a channel, and let a
  channel change retries, leases, timeouts and grace periods without
  changing the steps themselves.
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
- PostgreSQL driver (SQLAlchemy Core) working on three tables you declare —
  node rows, one revision per subject, and the history — with eligibility
  expressed as your own `SELECT`.
- A shared contract test suite any new driver can subclass to prove it
  behaves like the reference, including concurrency tests (competing
  claimers, claims racing a requeue) and random sequences of operations on
  random graphs checked step by step against the rule.
