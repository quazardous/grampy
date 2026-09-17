"""grampy — a small workflow graph for queues that already live in a storage.

    grampy.dag        the graph and the pure claim rule
    grampy.graph      the graph as data: a versioned document, JSON in and out
    grampy.states     replays and allowed transitions, derived from the graph
    grampy.journal    the node journal: the logic, over a storage driver
    grampy.drivers    `memory` (reference, tests) and `postgres` (SQLAlchemy Core)
    grampy.testing    the contract every driver must pass

The core imports nothing but the standard library. Only
`grampy.drivers.postgres` needs SQLAlchemy.
"""
from __future__ import annotations

from .dag import (
    NODE_CONCLUDED,
    NODE_DONE,
    NODE_FAILED,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SATISFYING,
    NODE_SKIPPED,
    DagError,
    Node,
    accepts,
    ancestors,
    check_dag,
    claimable,
    claimable_nodes,
    descendants,
    joined,
    node,
    omitted_by,
)
from .graph import Document, Graph, GraphFormatError
from .journal import JournalDriver, Lease, NodeJournal, utc_now
from .states import (
    allowed_transitions,
    replay_targets,
    replayed_after,
    source_state,
    to_undo,
)

__version__ = "0.1.0"

__all__ = [
    "NODE_CONCLUDED", "NODE_DONE", "NODE_FAILED", "NODE_OMITTED", "NODE_RUNNING",
    "NODE_SATISFYING", "NODE_SKIPPED", "DagError", "Document", "Graph", "GraphFormatError",
    "JournalDriver", "Lease", "Node", "accepts", "joined", "omitted_by",
    "NodeJournal", "allowed_transitions", "ancestors", "check_dag", "claimable",
    "claimable_nodes", "descendants", "node", "replay_targets", "replayed_after",
    "source_state", "to_undo", "utc_now",
]
