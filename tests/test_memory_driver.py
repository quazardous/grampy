"""The memory driver passes the contract."""
from __future__ import annotations

import pytest

from quazardous.grampy import NodeJournal
from quazardous.grampy.drivers.memory import MemoryDriver, Row
from quazardous.grampy.testing import JournalContract


class MemoryHarness:
    def journal(self, dag, clock, subject_type=str):
        return NodeJournal(MemoryDriver(), dag, clock=clock)

    def journal_on(self, journal, dag, clock):
        return NodeJournal(journal.driver, dag, clock=clock)

    def candidates(self, subjects):
        return list(subjects)

    def seed(self, journal, subject, progress):
        for name, status in progress.items():
            journal.driver.rows[(subject, name)] = Row(
                status, "2026-01-01T00:00:00+00:00",
                None if status == "running" else "2026-01-01T00:00:00+00:00")

    def parents_concluded(self, journal, name, subject):
        return journal.parents_concluded(name, subject)

    def store(self, dag, clock):
        return MemoryStore(dag, clock)


class MemoryStore:
    """One driver shared by every session: memory has no transaction, each
    call is atomic on its own and visible at once."""

    def __init__(self, dag, clock):
        self.driver, self.dag, self.clock = MemoryDriver(), dag, clock

    def session(self):
        return MemorySession(NodeJournal(self.driver, self.dag, clock=self.clock))

    def close(self):
        pass


class MemorySession:
    def __init__(self, journal):
        self.journal = journal

    def candidates(self, subjects):
        return list(subjects)

    def commit(self):
        pass

    def rollback(self):
        pass


class TestMemoryDriver(JournalContract):
    @pytest.fixture
    def harness(self):
        return MemoryHarness()
