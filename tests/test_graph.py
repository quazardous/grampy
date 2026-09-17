"""The graph as data — written, read back, refused when it lies."""
from __future__ import annotations

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from grampy import DagError, Document, Graph, GraphFormatError, Node

DIAMOND = (
    Node("start", working="starting", state="started"),
    Node("left", parents=("start",)),
    Node("right", parents=("start",), optional=True, once=True),
    Node("end", parents=("left", "right")),
)


def test_the_canonical_form_writes_only_what_differs_from_a_default():
    graph = Graph(Document("orders", version="3", namespace="shop"), DIAMOND)
    assert graph.to_dict() == {
        "document": {"dsl": "grampy/1", "namespace": "shop",
                     "name": "orders", "version": "3"},
        "nodes": {
            "start": {"working": "starting", "state": "started"},
            "left": {"parents": ["start"]},
            "right": {"parents": ["start"], "optional": True, "once": True},
            "end": {"parents": ["left", "right"]},
        },
    }


def test_json_round_trips():
    graph = Graph(Document("orders"), DIAMOND)
    assert Graph.from_json(graph.to_json()) == graph


def test_a_graph_that_does_not_hold_together_is_refused_on_construction():
    with pytest.raises(DagError):
        Graph(Document("broken"), (Node("a"), Node("b", parents=("ghost",))))


@pytest.mark.parametrize("data, path", [
    ([], "$: expected an object"),
    ({"document": {"name": "x"}}, "$: missing key(s) ['nodes']"),
    ({"document": {"name": "x"}, "nodes": {}, "extra": 1}, "$: unknown key(s) ['extra']"),
    ({"document": {"name": "x", "dsl": "grampy/99"}, "nodes": {"a": {}}},
     "$.document.dsl"),
    ({"document": {}, "nodes": {"a": {}}}, "$.document: missing key(s) ['name']"),
    ({"document": {"name": 3}, "nodes": {"a": {}}}, "$.document.name"),
    ({"document": {"name": "x"}, "nodes": []}, "$.nodes: expected an object"),
    ({"document": {"name": "x"}, "nodes": {"a": {"optionnal": True}}},
     "$.nodes.a: unknown key(s) ['optionnal']"),
    ({"document": {"name": "x"}, "nodes": {"a": {"parents": "b"}}}, "$.nodes.a.parents"),
    ({"document": {"name": "x"}, "nodes": {"a": {"parents": [1]}}}, "$.nodes.a.parents[0]"),
    ({"document": {"name": "x"}, "nodes": {"a": {"optional": "yes"}}}, "$.nodes.a.optional"),
    ({"document": {"name": "x"}, "nodes": {"a": {"state": ""}}}, "$.nodes.a.state"),
])
def test_a_document_that_lies_is_refused_with_its_path(data, path):
    with pytest.raises(GraphFormatError) as caught:
        Graph.from_dict(data)
    assert path in str(caught.value)


def test_a_node_declared_twice_in_json_is_refused():
    text = '{"document": {"name": "x"}, "nodes": {"a": {}, "a": {"optional": true}}}'
    with pytest.raises(GraphFormatError, match="appears twice"):
        Graph.from_json(text)


def test_not_json_is_refused():
    with pytest.raises(GraphFormatError, match="not JSON"):
        Graph.from_json("{nope")


names = st.text(alphabet="abcdefghij_", min_size=1, max_size=6)
labels = st.one_of(st.none(), st.text(alphabet="xyz", min_size=1, max_size=4))


@st.composite
def graphs(draw):
    chosen = draw(st.lists(names, min_size=1, max_size=7, unique=True))
    nodes = []
    used_labels: set[str] = set()
    for i, name in enumerate(chosen):
        parents = () if i == 0 else tuple(draw(st.lists(
            st.sampled_from(chosen[:i]), min_size=1, max_size=3, unique=True)))
        working, state = draw(labels), draw(labels)
        # One author per posted state: check_dag's rule, respected here.
        if working in used_labels or (state is not None and state in (used_labels | {working})):
            working = state = None
        used_labels |= {x for x in (working, state) if x}
        nodes.append(Node(name, parents=parents, working=working, state=state,
                          optional=draw(st.booleans()), once=draw(st.booleans())))
    document = Document(draw(names), version=draw(names), namespace=draw(names))
    return Graph(document, tuple(nodes))


@given(graphs())
def test_any_graph_survives_a_round_trip_through_json(graph):
    text = graph.to_json()
    assert Graph.from_json(text) == graph
    assert Graph.from_json(text).to_dict() == json.loads(text)
