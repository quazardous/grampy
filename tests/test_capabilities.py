"""A driver offers the core and the capabilities its graphs use — no more.

The journal says, when it is built, which capability a graph needs and the
driver lacks; the contract certifies a driver on what it declares.
"""
from __future__ import annotations

import pytest

from quazardous.grampy import (
    Document,
    Graph,
    Lane,
    MissingCapability,
    Node,
    NodeJournal,
    Per,
    capability_methods,
    needed_capabilities,
)
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.testing import JournalContract
from quazardous.grampy.timing import Rate
from test_memory_driver import MemoryHarness, MemorySession


class Only:
    """A driver offering only the methods of `capabilities`: the memory
    driver behind it, the rest hidden."""

    def __init__(self, inner: MemoryDriver, capabilities: tuple[str, ...]) -> None:
        self.inner = inner
        self._offered = {m for c in capabilities for m in capability_methods(c)}

    def __getattr__(self, name):
        if name in self._offered:
            return getattr(self.inner, name)
        raise AttributeError(name)


def core_only() -> Only:
    return Only(MemoryDriver(), ("core",))


def test_a_plain_graph_needs_only_the_core():
    NodeJournal(core_only(), (Node("a"), Node("b", parents=("a",))))


@pytest.mark.parametrize("graph, capability, where", [
    (Graph(Document("g"), (Node("a"),)), "versions", "a journal on a Graph"),
    ((Node("a", rate=(Rate(1, "1m"),)),), "limits", "node 'a'"),
    ((Node("a", concurrency=2),), "limits", "node 'a'"),
    ((Node("a", lane=Lane()),), "lanes", "node 'a'"),
], ids=["graph", "rate", "concurrency", "lane"])
def test_the_journal_names_the_capability_a_graph_needs_and_the_driver_lacks(
        graph, capability, where):
    with pytest.raises(MissingCapability) as refused:
        NodeJournal(core_only(), graph)
    assert refused.value.capability == capability
    assert where in str(refused.value)
    assert refused.value.missing == list(capability_methods(capability))


def test_a_policy_s_variant_counts_as_much_as_the_node():
    graph = Graph(Document("g"), (Node("a", per=Per.POLICY),),
                  policies={"busy": {"a": {"concurrency": 1}}})
    assert set(needed_capabilities(graph)) == {"core", "versions", "limits"}
    with pytest.raises(MissingCapability, match="'limits'"):
        NodeJournal(Only(MemoryDriver(), ("core", "versions")), graph)


def test_the_reading_methods_ask_for_reading_when_called_not_before():
    journal = NodeJournal(core_only(), (Node("a"),))
    with pytest.raises(MissingCapability, match="'reading'.*journal.counts"):
        journal.counts("a")


def test_the_capabilities_cover_the_whole_protocol_once():
    from quazardous.grampy import CAPABILITIES, JournalDriver, capability_methods
    every = [m for c in CAPABILITIES for m in capability_methods(c)]
    assert len(every) == len(set(every)), "a method in two capabilities"
    whole = {n for n in dir(JournalDriver) if not n.startswith("_")}
    assert set(every) == whole


class CoreOnlyHarness(MemoryHarness):
    capabilities = ("core",)

    def journal(self, dag, clock, subject_type=str, mergers=None):
        return NodeJournal(core_only(), dag, clock=clock, mergers=mergers)

    def journal_on(self, journal, dag, clock):
        return NodeJournal(journal.driver, dag, clock=clock)

    def seed(self, journal, subject, progress):
        super().seed(type("J", (), {"driver": journal.driver.inner})(), subject, progress)

    def store(self, dag, clock):
        return CoreOnlyStore(dag, clock)


class CoreOnlyStore:
    def __init__(self, dag, clock):
        self.driver, self.dag, self.clock = core_only(), dag, clock

    def session(self):
        return MemorySession(NodeJournal(self.driver, self.dag, clock=self.clock))

    def close(self):
        pass


class TestACoreOnlyDriverCertifiesItsCore(JournalContract):
    """The whole contract on a core-only driver: what it offers passes, what
    needs a capability it does not declare is skipped, not failed."""

    @pytest.fixture
    def harness(self):
        return CoreOnlyHarness()
