# Contributing

Thanks for your interest in grampy!

## Reporting bugs

Open an issue on the [GitHub tracker](https://github.com/quazardous/grampy/issues) with:

- What you tried to do.
- What you expected to happen.
- What actually happened (paste error messages / unexpected output).
- Your environment: Python version, OS, which driver, and for the
  PostgreSQL driver the SQLAlchemy and PostgreSQL versions.

The best report is a few lines: a small graph, the calls made on the
journal with the memory driver, and the result that looks wrong.

## Getting help

For usage questions, open an issue on the tracker and label it `question`.

## Sending a pull request

1. Fork and branch off `main` (one feature per branch).
2. Make the change. Keep diffs focused — small PRs review fast.
3. Add or update tests. The suite must stay green:

   ```bash
   pip install -e ".[test,postgres]"
   pytest
   ```

   Changes to the journal or a driver must also pass against PostgreSQL:

   ```bash
   GRAMPY_TEST_PG_DSN=postgresql+psycopg://user:pass@localhost/test pytest
   ```

4. Lint with `ruff check`.
5. Add a line to `CHANGELOG.md` under `[Unreleased]` if a user would
   notice the change.
6. Open the PR. Describe the **what** and the **why**; mechanical diff
   details belong in the commit messages, not the PR body.

## Design rules

These keep grampy small and storage-agnostic. A PR that breaks one needs
to say why.

- **The core imports only the standard library.** Only
  `grampy.drivers.postgres` may import SQLAlchemy, and only
  `grampy.testing` may import pytest and Hypothesis. `tests/test_isolation.py` enforces
  it.
- **Python 3.10 compatible.** No `match`/`case`, no 3.11+ syntax or
  standard-library additions.
- **The rule lives in the core, not in the drivers.** What is claimable is
  decided by the journal with `grampy.dag`, in Python. A driver stores rows
  and offers a few atomic operations; it may pre-filter what it reads,
  never decide. Every driver passes `grampy.testing.JournalContract`
  unchanged.
- **No guarantee rests on one storage's mechanism.** A guarantee is written
  in the driver protocol and proven by the shared contract, concurrency
  tests included. Locks, isolation levels and SQL tricks are how a driver
  keeps it, documented inside that driver.
- **The journal never commits and never reads the application's tables.**
  Eligibility comes in as opaque candidates.
- **Generic vocabulary.** Subject, node, run — nothing tied to one
  application's domain.

## Adding a driver

Implement the `grampy.journal.JournalDriver` protocol, then subclass
`grampy.testing.JournalContract` with a `harness` fixture, as
`tests/test_memory_driver.py` does.

## Commit messages

Subject line of 72 characters or fewer, in the imperative mood, followed
by a blank line and a body that explains the why. English only.

## Code of conduct

Be kind and assume good faith.
