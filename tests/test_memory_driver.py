"""The memory driver passes the contract."""
from __future__ import annotations

import pytest

from grampy import NodeJournal
from grampy.drivers.memory import MemoryDriver, Row
from grampy.testing import JournalContract


class MemoryHarness:
    def journal(self, dag, clock):
        return NodeJournal(MemoryDriver(), dag, clock=clock)

    def candidates(self, subjects):
        return list(subjects)

    def seed(self, journal, subject, progress):
        for name, status in progress.items():
            journal.driver.rows[(subject, name)] = Row(
                status, "2026-01-01T00:00:00+00:00",
                None if status == "running" else "2026-01-01T00:00:00+00:00")

    def parents_concluded(self, journal, name, subject):
        return journal.parents_concluded(name, subject)


class TestMemoryDriver(JournalContract):
    @pytest.fixture
    def harness(self):
        return MemoryHarness()
