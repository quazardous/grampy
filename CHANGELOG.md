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

First release, planned as 0.1.0.

### Added

- Declare a workflow as a graph of nodes, with forks that run in parallel,
  joins that wait for all their parents, and optional nodes that can be
  skipped without blocking their children.
- Validate a graph at startup: unknown parents, duplicate names, cycles,
  several entry points and a state posted by two nodes are rejected before
  any work is claimed.
- Ask which nodes a subject can start next, as a pure function of its
  progress — no storage needed.
- Claim a node for a batch of subjects on behalf of remote workers, then
  conclude, fail or skip it; a subject is never handed out twice for the
  same node.
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
- PostgreSQL driver (SQLAlchemy Core) working on a node table you declare,
  with eligibility expressed as your own `SELECT`.
- A shared contract test suite any new driver can subclass to prove it
  behaves like the reference.
