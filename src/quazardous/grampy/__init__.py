"""grampy — a small workflow graph for queues that already live in a storage.

    from quazardous import grampy          # distribution: grampy-q

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
    NODE_SCHEDULED,
    NODE_SKIPPED,
    DagError,
    Group,
    Lane,
    Loop,
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
from .journal import (
    Arrival,
    JournalDriver,
    Keyed,
    Lease,
    MigrationError,
    NodeJournal,
    utc_now,
)
from .names import Backoff, Merge, Name, Outcome, Per, Position, Reason, Status, WhileRunning
from .states import (
    allowed_transitions,
    replay_targets,
    replayed_after,
    source_state,
    to_undo,
)
from .timing import Rate, Retry

__version__ = "0.4.0"

__all__ = [
    "Backoff", "Merge", "Name", "Outcome", "Per", "Position", "Reason", "Status",
    "WhileRunning",
    "NODE_CONCLUDED", "NODE_DONE", "NODE_FAILED", "NODE_OMITTED", "NODE_RUNNING",
    "NODE_SATISFYING", "NODE_SCHEDULED", "NODE_SKIPPED", "Rate", "Retry", "DagError",
    "Document", "Graph", "GraphFormatError",
    "Arrival", "Group", "JournalDriver", "Keyed", "Lane", "Lease", "Loop", "MigrationError",
    "Node", "accepts",
    "joined", "omitted_by",
    "NodeJournal", "allowed_transitions", "ancestors", "check_dag", "claimable",
    "claimable_nodes", "descendants", "node", "replay_targets", "replayed_after",
    "source_state", "to_undo", "utc_now",
]
