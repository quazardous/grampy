"""The graph as data — written, read back, refused when it lies."""
from __future__ import annotations

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from quazardous.grampy import (
    NODE_CONCLUDED,
    DagError,
    Document,
    Graph,
    GraphFormatError,
    Lane,
    Loop,
    Node,
)
from quazardous.grampy.timing import Retry

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


def test_joins_and_choices_are_written_as_data():
    graph = Graph(Document("saga"), (
        Node("order", choice=True),
        Node("pay", parents=("order",)),
        Node("cancel", parents=("order",)),
        Node("refund", parents=("pay",), on={"pay": ("failed",)}),
        Node("end", parents=("pay", "cancel"), need=1),
        Node("check", parents=("end",), loop=Loop(to="pay", max=3), lease="10m",
             retry=Retry(limit=2, delay="30s", max_delay="5m", jitter=0.1)),
    ))
    nodes = graph.to_dict()["nodes"]
    assert nodes["order"] == {"choice": True}
    assert nodes["refund"] == {"parents": ["pay"], "on": {"pay": ["failed"]}}
    assert nodes["end"] == {"parents": ["pay", "cancel"], "need": 1}
    assert nodes["check"] == {
        "parents": ["end"], "loop": {"to": "pay", "max": 3, "on": ["failed"]}, "lease": "10m",
        "retry": {"limit": 2, "delay": "30s", "backoff": "exponential",
                  "max_delay": "5m", "jitter": 0.1}}
    assert Graph.from_json(graph.to_json()) == graph


def test_waits_and_graces_are_written_as_data():
    graph = Graph(Document("onboarding"), (
        Node("send"),
        Node("clicked", parents=("send",), wait="email.clicked", timeout="7d"),
        Node("survey", parents=("send",), optional=True, grace="1d"),
    ))
    nodes = graph.to_dict()["nodes"]
    assert nodes["clicked"] == {"parents": ["send"], "wait": "email.clicked", "timeout": "7d"}
    assert nodes["survey"] == {"parents": ["send"], "grace": "1d", "optional": True}
    assert Graph.from_json(graph.to_json()) == graph


def test_lanes_are_written_as_data():
    graph = Graph(Document("x"), (
        Node("in", lane=Lane.debounce("5m", max_wait="1h")),
        Node("work", parents=("in",)),
    ), policies={"slow": {"in": {"lane": Lane.throttle("1d")}}})
    data = graph.to_dict()
    assert data["nodes"]["in"] == {"lane": {"position": "last", "delay": "5m",
                                            "max_wait": "1h"}}
    assert data["policies"] == {"slow": {"in": {"lane": {"cooldown": "1d"}}}}
    assert Graph.from_json(graph.to_json()) == graph
    assert graph.variant("slow")[0].lane == Lane.throttle("1d")


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
    ({"document": {"name": "x"}, "nodes": {"a": {"on": []}}}, "$.nodes.a.on"),
    ({"document": {"name": "x"}, "nodes": {"a": {"on": {"b": "failed"}}}}, "$.nodes.a.on.b"),
    ({"document": {"name": "x"}, "nodes": {"a": {"on": {"b": [3]}}}}, "$.nodes.a.on.b[0]"),
    ({"document": {"name": "x"}, "nodes": {"a": {"need": "2"}}}, "$.nodes.a.need"),
    ({"document": {"name": "x"}, "nodes": {"a": {"need": True}}}, "$.nodes.a.need"),
    ({"document": {"name": "x"}, "nodes": {"a": {"choice": 1}}}, "$.nodes.a.choice"),
    ({"document": {"name": "x"}, "nodes": {"a": {"loop": {"to": "a"}}}},
     "$.nodes.a.loop: missing key(s) ['max']"),
    ({"document": {"name": "x"}, "nodes": {"a": {"loop": {"to": "a", "max": "2"}}}},
     "$.nodes.a.loop.max"),
    ({"document": {"name": "x"}, "nodes": {"a": {"loop": {"to": "a", "max": 2, "on": "x"}}}},
     "$.nodes.a.loop.on"),
    ({"document": {"name": "x"}, "nodes": {"a": {"retry": {"limit": 0}}}}, "$.nodes.a.retry"),
    ({"document": {"name": "x"}, "nodes": {"a": {"retry": {"limit": 1, "backoff": "fast"}}}},
     "$.nodes.a.retry"),
    ({"document": {"name": "x"}, "nodes": {"a": {"retry": {"limit": 1, "delay": "soon"}}}},
     "$.nodes.a.retry"),
    ({"document": {"name": "x"}, "nodes": {"a": {"lease": [10]}}}, "$.nodes.a.lease"),
    ({"document": {"name": "x"}, "nodes": {"a": {"wait": 3}}}, "$.nodes.a.wait"),
    ({"document": {"name": "x"}, "nodes": {"a": {"grace": True}}}, "$.nodes.a.grace"),
    ({"document": {"name": "x"}, "nodes": {"a": {"lane": {"cooldwon": "1h"}}}},
     "$.nodes.a.lane: unknown key(s) ['cooldwon']"),
    ({"document": {"name": "x"}, "nodes": {"a": {"lane": {"merge": 1}}}}, "$.nodes.a.lane.merge"),
    ({"document": {"name": "x"}, "nodes": {"a": {"lane": {"delay": []}}}}, "$.nodes.a.lane.delay"),
    ({"document": {"name": "x"}, "nodes": {"a": {"lane": "throttle"}}}, "$.nodes.a.lane"),
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
        on = {p: tuple(draw(st.lists(st.sampled_from(NODE_CONCLUDED), min_size=1,
                                     max_size=3, unique=True)))
              for p in parents if draw(st.booleans())}
        need = draw(st.integers(1, len(parents))) if parents and draw(st.booleans()) else None
        nodes.append(Node(name, parents=parents, working=working, state=state,
                          optional=draw(st.booleans()), once=draw(st.booleans()),
                          on=on, need=need))
    # A choice needs a branch: only nodes with children may be one.
    with_children = {p for n in nodes for p in n.parents}
    nodes = [Node(n.name, parents=n.parents, working=n.working, state=n.state,
                  optional=n.optional and n.name not in with_children, once=n.once,
                  on=n.on, need=n.need,
                  choice=n.name in with_children and draw(st.booleans()))
             for n in nodes]
    # A loop goes back to the node itself or to its first parent, never on a choice.
    nodes = [n if n.choice or not draw(st.booleans()) else
             Node(n.name, parents=n.parents, working=n.working, state=n.state,
                  optional=n.optional, once=n.once, on=n.on, need=n.need,
                  loop=Loop(to=(n.parents or (n.name,))[0], max=draw(st.integers(1, 5)),
                            on=tuple(draw(st.lists(st.sampled_from(("done", "skipped", "failed")),
                                                   min_size=1, max_size=3, unique=True)))))
             for n in nodes]
    document = Document(draw(names), version=draw(names), namespace=draw(names))
    return Graph(document, tuple(nodes))


@given(graphs())
def test_any_graph_survives_a_round_trip_through_json(graph):
    text = graph.to_json()
    assert Graph.from_json(text) == graph
    assert Graph.from_json(text).to_dict() == json.loads(text)


def test_policies_change_settings_and_round_trip():
    graph = Graph(Document("offers"), (
        Node("scrape", lease="2m"),
        Node("call", parents=("scrape",), retry=Retry(limit=1)),
    ), policies={"slow": {"scrape": {"lease": "10m"},
                          "call": {"retry": Retry(limit=4, delay="1m")}}})
    assert graph.variant("slow")[0].lease == "10m"
    assert graph.variant("other")[0].lease == "2m"
    assert graph.variant(None)[1].retry == Retry(limit=1)
    data = graph.to_dict()
    assert data["policies"] == {"slow": {
        "scrape": {"lease": "10m"},
        "call": {"retry": {"limit": 4, "delay": "1m", "backoff": "exponential"}}}}
    assert Graph.from_json(graph.to_json()) == graph


@pytest.mark.parametrize("policies, message", [
    ({"x": {"ghost": {"lease": "1m"}}}, "does not exist"),
    ({"x": {"a": {"parents": []}}}, "never the structure"),
    ({"x": {"a": {"grace": "1m"}}}, "not"),
    ({"x": {"a": {"lease": "0s"}}}, "lease"),
    ({"x": {"a": {"lane": Lane()}}}, "never adds or removes"),
])
def test_a_policy_that_would_break_the_graph_is_refused(policies, message):
    with pytest.raises(DagError, match=message):
        Graph(Document("x"), (Node("a"),), policies=policies)


@pytest.mark.parametrize("data, path", [
    ({"document": {"name": "x"}, "nodes": {"a": {}}, "policies": []}, "$.policies"),
    ({"document": {"name": "x"}, "nodes": {"a": {}}, "policies": {"c": {"a": {"need": 1}}}},
     "$.policies.c.a: unknown key(s) ['need']"),
    ({"document": {"name": "x"}, "nodes": {"a": {}}, "policies": {"c": {"a": {"lease": []}}}},
     "$.policies.c.a.lease"),
])
def test_a_policies_document_that_lies_is_refused(data, path):
    with pytest.raises(GraphFormatError) as caught:
        Graph.from_dict(data)
    assert path in str(caught.value)
