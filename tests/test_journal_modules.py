"""The journal's parts live in modules of their own — `protocol`, `limits`,
`lanes`, `migration` — and every name `quazardous.grampy.journal` offered
before still imports from it."""
from __future__ import annotations

import importlib

import pytest

import quazardous.grampy as grampy

#: What `quazardous.grampy.journal` defined before its parts moved out.
BEFORE = (
    "Arrival", "CAPABILITIES", "CoreDriver", "Entry", "JournalDriver", "Keyed",
    "LaneDriver", "Lease", "LimitDriver", "MigrationError", "MissingCapability",
    "NODE_CONCLUDED", "NodeJournal", "PAGE", "Page", "ReadingDriver", "VersionDriver",
    "capability_methods", "needed_capabilities", "utc_now",
)


@pytest.mark.parametrize("name", BEFORE)
def test_every_name_of_the_journal_module_still_imports_from_it(name):
    journal = importlib.import_module("quazardous.grampy.journal")
    assert hasattr(journal, name)
    assert name in journal.__all__ or name.isupper()


@pytest.mark.parametrize("name", [n for n in BEFORE if hasattr(grampy, n)])
def test_the_package_and_the_journal_module_give_the_same_object(name):
    from quazardous.grampy import journal
    assert getattr(grampy, name) is getattr(journal, name)


def test_the_journal_is_assembled_from_its_parts():
    from quazardous.grampy.journal import NodeJournal
    from quazardous.grampy.lanes import _Lanes
    from quazardous.grampy.limits import _Limits
    from quazardous.grampy.migration import _Migration
    assert issubclass(NodeJournal, (_Lanes)) and issubclass(NodeJournal, _Limits)
    assert issubclass(NodeJournal, _Migration)
