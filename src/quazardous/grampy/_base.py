"""THE JOURNAL'S SHARED STATE — its driver, graph, version and clock — and
the helpers every part of it uses. `NodeJournal` is assembled from this base
and the parts built on it (limits, lanes, migration).
"""
from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from typing import Any

from .dag import (
    Node,
    node,
)
from .graph import Graph
from .protocol import Entry, JournalDriver, _require, needed_capabilities
from .timing import stamp

#: HOW MANY CANDIDATES A CLAIM READS AT ONCE, at least — more when the
#: limit is higher. A page is one read and at most one write.
PAGE = 200


class _JournalBase:
    """What every part of the journal shares."""

    def __init__(self, driver: JournalDriver, dag: tuple[Node, ...] | Graph, *,
                 clock: Callable[[], str] | None = None,
                 rng: random.Random | None = None,
                 mergers: dict[str, Callable[[tuple[str, ...], str | None],
                                             Any]] | None = None) -> None:
        """`dag` is a tuple of nodes, or a `Graph` whose policies change some
        settings per policy. `clock` defaults to the driver's
        (`JournalDriver.now`): workers on several machines then share one
        time. `rng` spreads retry jitter. `mergers` names the functions a
        lane may merge with (`Lane(merge="fn:<name>")`)."""
        # WHAT THE GRAPH USES, THE DRIVER MUST OFFER — said now, naming what
        # is missing, rather than as an AttributeError at the first claim.
        for capability, why in needed_capabilities(dag).items():
            _require(driver, capability, why)
        self.driver = driver
        self.graph = dag if isinstance(dag, Graph) else None
        self.dag = dag.nodes if isinstance(dag, Graph) else dag
        #: SUBJECTS ARE PINNED TO THE GRAPH THEY STARTED ON: a journal on a
        #: `Graph` takes only subjects pinned to it, or not yet pinned, and
        #: pins them on their first write — once, never again. It is the whole
        #: `Document.identity`, so two workflows sharing a version string stay
        #: strangers. None for a tuple of nodes.
        self.version = dag.document.identity if isinstance(dag, Graph) else None
        # AN INJECTED CLOCK IS READ INTO THE ONE FORMAT (`timing.stamp`): in
        # another zone or layout, its times would sort wrong as text. The
        # driver's own clock already writes that format.
        self._clock = (lambda: stamp(clock())) if clock is not None else driver.now
        #: THE TIME COMES WITH THE FIRST PAGE when the journal runs on the
        #: driver's clock and the driver can read it there.
        self._clock_in_scan = clock is None and bool(getattr(driver, "scan_reads_clock", False))
        self._rng = rng or random.Random()
        #: THE MERGE FUNCTIONS A LANE MAY NAME. The graph stays data — it
        #: holds `fn:<name>`, never the code — and the name is resolved here,
        #: the way a named guard is resolved in a statechart.
        self._mergers = dict(mergers or {})

    def _mine(self, entry: Entry) -> bool:
        return self.version is None or entry.version in (None, self.version)

    def _pin(self, subjects: list[Any]) -> None:
        if self.version is not None and subjects:
            self.driver.pin(list(subjects), self.version)

    def _progress_many(self, subjects: list[Any]) -> dict[Any, dict[str, str]]:
        """`{subject: progress}` in one read when the driver offers it."""
        many = getattr(self.driver, "progress_many", None)
        if many is not None:
            return dict(many(subjects)) if subjects else {}
        return {s: self.driver.progress(s) for s in subjects}

    def policy(self, subject: Any) -> str | None:
        """The subject's policy, None when it has none."""
        return self.driver.policies([subject]).get(subject)

    def settings(self, name: str, policy: str | None) -> Node:
        """The node as a subject of `policy` sees it."""
        node(name, self.dag)
        if self.graph is None:
            return node(name, self.dag)
        return node(name, self.graph.variant(policy))

    def _variants(self, name: str) -> list[tuple[str, Node]]:
        if self.graph is None:
            return []
        return [(policy, node(name, self.graph.variant(policy)))
                for policy in self.graph.policies]

    def _per_policy(self, name: str, setting: str, subjects: list[Any]) -> dict[Any, Any]:
        """`{subject: value of `setting` on `name` for its policy}`."""
        default = getattr(node(name, self.dag), setting)
        if self.graph is None or not any(
                name in overrides and setting in overrides[name]
                for overrides in self.graph.policies.values()):
            return dict.fromkeys(subjects, default)
        policies = self.driver.policies(subjects)
        return {s: getattr(self.settings(name, policies.get(s)), setting) for s in subjects}


def _unique(subjects: Iterable[Any]) -> list[Any]:
    """The subjects once each, in their order."""
    return list(dict.fromkeys(subjects))
