# grampy — rules for agents

This repository is public (MIT). Anything committed here is read by
strangers, so the rules below are about keeping it readable cold.

## English everywhere

Code, comments, docstrings, README, CHANGELOG, commit messages and PR
descriptions are in English, whatever language the conversation is in.

## Nothing from a host application

grampy was extracted from a private application, and must stay generic.
Never commit hostnames, secrets, dev-machine paths (`/home/...`), private
tracker IDs (`#NNN`) or the vocabulary of the application that uses it.
Use the generic terms: subject, node, run. Context about that application
belongs in `CLAUDE.local.md` (not versioned) or on the tracker thread.

## Package layout

Distribution `grampy-q`, import `quazardous.grampy`. `src/quazardous/` is a
PEP 420 namespace: never add an `__init__.py` there.

## Keep the core dependency-free and 3.10-compatible

The core imports only the standard library (`tests/test_isolation.py`
checks it). The main consumer runs Python 3.10: no `match`/`case`, no
3.11+ features. `requires-python` stays `>=3.10`.

## One package, nothing forced

Rendering, interop, drivers and the test contract live in the same package
but are never imported by `quazardous.grampy` itself: an application loads
them — and their dependencies — only by importing them
(`tests/test_isolation.py` checks it). A module needing a third-party
library declares it as an extra in `pyproject.toml`.

## Drivers follow the contract

The claim rule lives in the core (`quazardous.grampy.dag`, applied
by `quazardous.grampy.journal`); drivers store rows and offer atomic operations, they
never decide. No guarantee may depend on a storage-specific mechanism: it
is defined in the driver protocol and proven by the shared contract,
concurrency tests included. A change to the journal or a driver must pass
`quazardous.grampy.testing.JournalContract` on every driver — PostgreSQL when
`GRAMPY_TEST_PG_DSN` is set. Say explicitly when PostgreSQL was not run.

## Changelog

Every user-visible change adds a line to `CHANGELOG.md` under
`[Unreleased]`, written for a user, without ticket IDs.

## Commands

```bash
uv run --with pytest --with sqlalchemy pytest   # tests (PG skipped without DSN)
uv run --with ruff ruff check                   # lint
cd src && uv run --with mypy python -m mypy --python-version 3.10 \
    --ignore-missing-imports --explicit-package-bases \
    --namespace-packages quazardous/grampy      # types — CI runs this too
```

CI runs all three. Running only the first two has let red builds through.
