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

4. Lint with `ruff check`, and type-check with
   `cd src && mypy --explicit-package-bases --namespace-packages quazardous/grampy`.
5. Add a line to `CHANGELOG.md` under `[Unreleased]` if a user would
   notice the change.
6. Open the PR. Describe the **what** and the **why**; mechanical diff
   details belong in the commit messages, not the PR body.

## Design rules

These keep grampy small and storage-agnostic. A PR that breaks one needs
to say why.

- **`quazardous` stays a namespace.** No `__init__.py` in `src/quazardous/`:
  other distributions may install beside grampy under the same prefix.
- **The core imports only the standard library.** Only
  `quazardous.grampy.drivers.postgres` may import SQLAlchemy, and only
  `quazardous.grampy.testing` may import pytest and Hypothesis. `tests/test_isolation.py` enforces
  it.
- **Python 3.10 compatible.** No `match`/`case`, no 3.11+ syntax or
  standard-library additions.
- **The rule lives in the core, not in the drivers.** What is claimable is
  decided by the journal with `quazardous.grampy.dag`, in Python. A driver stores rows
  and offers a few atomic operations; it may pre-filter what it reads,
  never decide. Every driver passes `quazardous.grampy.testing.JournalContract`
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

Implement the `quazardous.grampy.journal.JournalDriver` protocol, then subclass
`quazardous.grampy.testing.JournalContract` with a `harness` fixture, as
`tests/test_memory_driver.py` does.

## How to release

Maintainers only. Nothing is uploaded by hand: a tag does it.

1. Move the `[Unreleased]` entries of `CHANGELOG.md` under
   `## [X.Y.Z] - YYYY-MM-DD` (SemVer: any `Added` is at least a minor bump).
2. Set `__version__ = "X.Y.Z"` in `src/quazardous/grampy/__init__.py`.
3. Commit, then tag and push the tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
4. The `release` workflow builds the sdist and the wheel, runs
   `twine check`, checks that the tag matches the version, runs the tests
   against the **installed** wheel, and publishes to TestPyPI.
5. Approve the `pypi` environment in GitHub: the same files go to PyPI,
   then a GitHub release is created with the CHANGELOG section.

A version uploaded to PyPI can never be uploaded again, even after being
deleted: a mistake costs a version number.

One-time setup (repository owner): on pypi.org and test.pypi.org, add a
*pending trusted publisher* for project `grampy-q`, owner `quazardous`,
repository `grampy`, workflow `release.yml`, environment `pypi`
(`testpypi` on TestPyPI); in the GitHub repository settings, create the
`testpypi` and `pypi` environments, the latter with a required reviewer.

## Commit messages

Subject line of 72 characters or fewer, in the imperative mood, followed
by a blank line and a body that explains the why. English only.

## Code of conduct

Be kind and assume good faith.
