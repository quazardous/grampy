"""Replays and transitions, derived from an invented media pipeline.

    fetch ──┬── crop ── encode ── match ──┐
            │                             ├── judge ── audit (once)
            └── read (optional) ──────────┘
"""
from __future__ import annotations

import pytest

from grampy import (
    DagError,
    Node,
    allowed_transitions,
    replay_targets,
    replayed_after,
    source_state,
    to_undo,
)

PIPELINE = (
    Node("fetch", working="fetching", state="fetched"),
    Node("crop", parents=("fetch",), working="cropping", optional=True),
    Node("read", parents=("fetch",), optional=True),
    Node("encode", parents=("crop",), working="encoding", state="encoded"),
    Node("match", parents=("encode",), working="matching", state="matched"),
    Node("judge", parents=("match", "read"), state="judged"),
    Node("audit", parents=("judge",), once=True),
)


def test_the_source_walks_through_nodes_that_produce_nothing():
    assert source_state("fetch", PIPELINE, entry="new") == "new"
    assert source_state("crop", PIPELINE, entry="new") == "fetched"
    assert source_state("encode", PIPELINE, entry="new") == "fetched"


def test_a_join_takes_its_subjects_from_the_most_downstream_producer():
    assert source_state("judge", PIPELINE, entry="new") == "matched"


def test_two_unrelated_producers_make_the_source_ambiguous():
    graph = (Node("root"), Node("a", parents=("root",), state="a_done"),
             Node("b", parents=("root",), state="b_done"),
             Node("join", parents=("a", "b"), state="joined"))
    with pytest.raises(DagError, match="ambiguous"):
        source_state("join", graph, entry="new")


def test_a_once_node_is_not_replayed():
    assert replayed_after("match", PIPELINE) == ("judge",)
    assert replayed_after("judge", PIPELINE) == ()


def test_replay_targets_are_states_with_something_to_redo():
    assert replay_targets(PIPELINE) == {"fetched", "encoded", "matched"}


def test_to_undo_forgets_the_downstream_of_the_producer():
    assert to_undo("matched", PIPELINE) == ("audit", "judge")
    assert to_undo("fetched", PIPELINE) == ("audit", "crop", "encode",
                                            "judge", "match", "read")


def test_to_undo_from_the_entry_forgets_everything_in_declaration_order():
    assert to_undo("new", PIPELINE) == tuple(n.name for n in PIPELINE)


def test_allowed_transitions_are_derived_edge_by_edge():
    pairs = allowed_transitions(PIPELINE, entry="new", exits=("failed", "aborted"))
    forward = {("new", "fetching"), ("fetching", "fetched"),
               ("fetched", "cropping"), ("cropping", "fetched"),
               ("fetched", "encoding"), ("encoding", "encoded"),
               ("encoded", "matching"), ("matching", "matched"),
               ("matched", "judged")}
    returns = {("fetching", "new"), ("encoding", "fetched"), ("matching", "encoded")}
    replays = {("judged", "fetched"), ("judged", "encoded"), ("judged", "matched")}
    states = {"new", "fetching", "fetched", "cropping", "encoding", "encoded",
              "matching", "matched", "judged"}
    exits = {(s, e) for s in states for e in ("failed", "aborted")}
    assert pairs == forward | returns | replays | exits
    assert len(pairs) == 9 + 3 + 3 + 18


def test_no_transition_goes_from_a_state_to_itself():
    graph = (Node("only", working="busy", state=None),)
    assert allowed_transitions(graph, entry="idle") == {("idle", "busy"), ("busy", "idle")}
