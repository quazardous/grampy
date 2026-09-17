"""The graph, without storage — invented workflows, every case in two lines.

None of these graphs belongs to an application. When an application's
graph changes, this file does not move: it does not talk about it.
"""
from __future__ import annotations

import pytest

from quazardous.grampy import (
    NODE_DONE,
    NODE_FAILED,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SKIPPED,
    DagError,
    Loop,
    Node,
    ancestors,
    check_dag,
    claimable,
    claimable_nodes,
    descendants,
    node,
    omitted_by,
)

#: Three nodes in a row.
LINE = (Node("a"), Node("b", parents=("a",)), Node("c", parents=("b",)))

#: A fork, a join, an optional branch — the diamond.
DIAMOND = (
    Node("start"),
    Node("left", parents=("start",)),
    Node("right", parents=("start",), optional=True),
    Node("end", parents=("left", "right")),
)

#: Three branches joining, for a join of more than two.
STAR = (
    Node("trunk"),
    Node("b1", parents=("trunk",)),
    Node("b2", parents=("trunk",)),
    Node("b3", parents=("trunk",)),
    Node("tip", parents=("b1", "b2", "b3")),
)

#: A long branch, so that "descendant" is not mistaken for "direct child".
DEEP = (
    Node("r"),
    Node("x1", parents=("r",)),
    Node("x2", parents=("x1",)),
    Node("x3", parents=("x2",)),
)

#: A fork where one branch is NOT an ancestor of the other's children.
SIBLINGS = (
    Node("fetch"),
    Node("crop", parents=("fetch",)),
    Node("read", parents=("fetch",)),
    Node("encode", parents=("crop",)),
    Node("judge", parents=("encode", "read")),
)


# ── the line: a node waits for its parent ────────────────────────────────

def test_only_the_root_starts_on_an_empty_progress():
    assert claimable_nodes(LINE, {}) == {"a"}


def test_each_node_waits_for_the_previous_one():
    assert claimable_nodes(LINE, {"a": NODE_DONE}) == {"b"}
    assert claimable_nodes(LINE, {"a": NODE_DONE, "b": NODE_DONE}) == {"c"}


def test_a_complete_progress_returns_nothing():
    assert claimable_nodes(LINE, {n.name: NODE_DONE for n in LINE}) == set()


def test_a_running_node_is_not_taken_again():
    assert claimable_nodes(LINE, {"a": NODE_RUNNING}) == set()


# ── the fork ────────────────────────────────────────────────────────────

def test_the_fork_returns_both_branches_at_once():
    assert claimable_nodes(DIAMOND, {"start": NODE_DONE}) == {"left", "right"}


def test_taking_one_branch_does_not_remove_the_other():
    assert claimable_nodes(DIAMOND, {"start": NODE_DONE, "left": NODE_RUNNING}) == {"right"}


# ── the join ────────────────────────────────────────────────────────────

def test_the_join_waits_for_all_its_parents():
    one = {"start": NODE_DONE, "left": NODE_DONE}
    assert "end" not in claimable_nodes(DIAMOND, one)
    assert claimable_nodes(DIAMOND, {**one, "right": NODE_DONE}) == {"end"}


def test_a_join_of_three_waits_for_all_three():
    two = {"trunk": NODE_DONE, "b1": NODE_DONE, "b2": NODE_DONE}
    assert "tip" not in claimable_nodes(STAR, two)
    assert claimable_nodes(STAR, {**two, "b3": NODE_DONE}) == {"tip"}


# ── skipped concludes, failed does not ──────────────────────────────────

def test_a_skipped_parent_satisfies_like_a_done_one():
    progress = {"start": NODE_DONE, "left": NODE_DONE, "right": NODE_SKIPPED}
    assert claimable_nodes(DIAMOND, progress) == {"end"}


def test_a_failed_parent_does_not_satisfy():
    progress = {"start": NODE_DONE, "left": NODE_FAILED, "right": NODE_DONE}
    assert claimable_nodes(DIAMOND, progress) == set()


def test_a_failure_at_the_root_stops_everything():
    assert claimable_nodes(DIAMOND, {"start": NODE_FAILED}) == set()


# ── never going backwards ───────────────────────────────────────────────

def test_a_started_descendant_closes_the_node():
    assert not claimable("b", LINE, {"a": NODE_DONE, "c": NODE_RUNNING})


def test_a_descendant_is_not_only_a_direct_child():
    assert descendants("r", DEEP) == {"x1", "x2", "x3"}
    assert not claimable("x1", DEEP, {"x3": NODE_RUNNING})


def test_a_concluded_descendant_closes_too():
    assert not claimable("b", LINE, {"a": NODE_DONE, "c": NODE_DONE})


def test_a_started_sibling_closes_nothing():
    assert claimable("right", DIAMOND, {"start": NODE_DONE, "left": NODE_DONE})


# ── edges ───────────────────────────────────────────────────────────────

def test_an_unknown_node_raises_rather_than_returning_false():
    with pytest.raises(DagError, match="unknown node"):
        claimable("ghost", LINE, {})


def test_a_single_node_graph_holds():
    alone = (Node("all"),)
    check_dag(alone)
    assert claimable_nodes(alone, {}) == {"all"}
    assert claimable_nodes(alone, {"all": NODE_DONE}) == set()


def test_the_descendants_of_a_leaf_are_empty():
    assert descendants("c", LINE) == set()
    assert descendants("end", DIAMOND) == set()


def test_node_raises_on_an_unknown_name():
    with pytest.raises(DagError, match="unknown node"):
        node("ghost", LINE)


# ── the shape of the graph ──────────────────────────────────────────────

def test_check_dag_accepts_the_invented_workflows():
    for graph in (LINE, DIAMOND, STAR, DEEP, SIBLINGS):
        check_dag(graph)


def test_check_dag_refuses_an_unknown_parent():
    with pytest.raises(DagError, match="does not exist"):
        check_dag((Node("a"), Node("b", parents=("missing",))))


def test_check_dag_refuses_a_cycle():
    with pytest.raises(DagError, match="cycle"):
        check_dag((Node("a", parents=("c",)), Node("b", parents=("a",)),
                   Node("c", parents=("b",))))


def test_a_cycle_would_indeed_make_the_queue_silent():
    cycle = (Node("a", parents=("b",)), Node("b", parents=("a",)))
    assert claimable_nodes(cycle, {}) == set()


def test_check_dag_refuses_two_roots():
    with pytest.raises(DagError, match="root"):
        check_dag((Node("a"), Node("b")))


def test_check_dag_refuses_two_nodes_with_one_name():
    with pytest.raises(DagError, match="two nodes are named"):
        check_dag((Node("a"), Node("a", parents=("a",))))


def test_check_dag_refuses_two_nodes_posting_the_same_state():
    with pytest.raises(DagError, match="ambiguous"):
        check_dag((Node("a", state="same"), Node("b", parents=("a",), state="same")))


# ── a whole run ─────────────────────────────────────────────────────────

def test_a_workflow_runs_to_the_end():
    progress: dict[str, str] = {}
    trace: list[frozenset[str]] = []
    while ready := claimable_nodes(DIAMOND, progress):
        trace.append(frozenset(ready))
        for name in ready:
            progress[name] = NODE_DONE
    assert trace == [frozenset({"start"}), frozenset({"left", "right"}),
                     frozenset({"end"})]
    assert len(progress) == len(DIAMOND), "a node was never taken"


# ── ancestors ───────────────────────────────────────────────────────────

def test_ancestors_say_what_a_reached_node_attests():
    assert ancestors("encode", SIBLINGS) == {"crop", "fetch"}
    assert ancestors("fetch", SIBLINGS) == set()


def test_ancestors_exclude_the_sibling_branch():
    assert "read" not in ancestors("encode", SIBLINGS)
    assert "read" in ancestors("judge", SIBLINGS)


def test_ancestors_and_descendants_never_overlap():
    for graph in (LINE, DIAMOND, STAR, DEEP, SIBLINGS):
        for n in graph:
            up, down = ancestors(n.name, graph), descendants(n.name, graph)
            assert not (up & down)
            assert n.name not in up and n.name not in down


# ── joins as data ─────────────────────────────────────────────────────

#: An order: pay and reserve in parallel, ship on both, refund when the
#: payment went through but the reservation failed.
SAGA = (
    Node("order"),
    Node("pay", parents=("order",)),
    Node("reserve", parents=("order",)),
    Node("ship", parents=("pay", "reserve")),
    Node("refund", parents=("pay", "reserve"), on={"reserve": ("failed",)}),
)


def test_a_failure_edge_starts_on_the_failure_only():
    base = {"order": NODE_DONE, "pay": NODE_DONE}
    assert claimable_nodes(SAGA, {**base, "reserve": NODE_FAILED}) == {"refund"}
    assert claimable_nodes(SAGA, {**base, "reserve": NODE_DONE}) == {"ship"}
    assert not claimable("refund", SAGA, {"order": NODE_DONE, "pay": NODE_FAILED,
                                          "reserve": NODE_FAILED})


#: Three engines, two are enough.
QUORUM = (
    Node("scan"),
    Node("e1", parents=("scan",)),
    Node("e2", parents=("scan",)),
    Node("e3", parents=("scan",)),
    Node("merge", parents=("e1", "e2", "e3"), need=2),
)


def test_k_of_n_starts_at_k_and_closes_the_late_parent():
    progress = {"scan": NODE_DONE, "e1": NODE_DONE, "e2": NODE_RUNNING}
    assert not claimable("merge", QUORUM, progress)
    progress["e2"] = NODE_DONE
    assert claimable_nodes(QUORUM, progress) == {"e3", "merge"}
    progress["merge"] = NODE_RUNNING
    assert not claimable("e3", QUORUM, progress), "merge started: e3 is closed"


def test_k_of_n_counts_only_accepted_statuses():
    progress = {"scan": NODE_DONE, "e1": NODE_DONE, "e2": NODE_FAILED, "e3": NODE_RUNNING}
    assert not claimable("merge", QUORUM, progress)


#: A moderation: one of three routes, then a common end.
ROUTE = (
    Node("classify", choice=True),
    Node("publish", parents=("classify",)),
    Node("review", parents=("classify",)),
    Node("reject", parents=("classify",)),
    Node("notify", parents=("reject",)),
    Node("audit", parents=("review", "notify")),
    Node("end", parents=("publish", "review", "notify")),
)


def test_a_choice_omits_the_other_branches_and_what_only_they_reach():
    assert omitted_by("classify", "publish", ROUTE) == ("review", "reject", "notify", "audit")
    assert omitted_by("classify", "reject", ROUTE) == ("publish", "review")
    assert omitted_by("classify", "review", ROUTE) == ("publish", "reject", "notify")


def test_omitted_satisfies_the_join_after_the_branches():
    progress = {"classify": NODE_DONE, "publish": NODE_DONE}
    progress.update(dict.fromkeys(omitted_by("classify", "publish", ROUTE), NODE_OMITTED))
    assert claimable_nodes(ROUTE, progress) == {"end"}


def test_a_choice_is_named_and_its_branch_exists():
    with pytest.raises(DagError, match="not a choice"):
        omitted_by("publish", "review", ROUTE)
    with pytest.raises(DagError, match="not a branch"):
        omitted_by("classify", "audit", ROUTE)


@pytest.mark.parametrize("nodes, message", [
    ((Node("a"), Node("b", parents=("a",), on={"x": ("done",)})), "not one of its parents"),
    ((Node("a"), Node("b", parents=("a",), on={"a": ()})), "accepts nothing"),
    ((Node("a"), Node("b", parents=("a",), on={"a": ("gone",)})), "unknown status"),
    ((Node("a"), Node("b", parents=("a",), need=2)), "need=2"),
    ((Node("a"), Node("b", parents=("a",), need=0)), "need=0"),
    ((Node("a", choice=True),), "choice without any branch"),
    ((Node("a"), Node("b", parents=("a", "a"))), "names a parent twice"),
])
def test_check_dag_refuses_a_join_that_cannot_hold(nodes, message):
    with pytest.raises(DagError, match=message):
        check_dag(nodes)


def test_on_is_read_only_and_nodes_stay_hashable():
    n = Node("refund", parents=("pay",), on={"pay": ["failed"]})
    assert n.on == {"pay": ("failed",)}
    assert hash(n) == hash(Node("refund", parents=("pay",), on={"pay": ("failed",)}))
    with pytest.raises(TypeError):
        n.on["pay"] = ("done",)


# ── declared loops ────────────────────────────────────────────────────


@pytest.mark.parametrize("nodes, message", [
    ((Node("a"), Node("b", parents=("a",), loop=Loop(to="c", max=1)), Node("c", parents=("a",))),
     "neither itself nor one of its ancestors"),
    ((Node("a"), Node("b", parents=("a",), loop=Loop(to="a", max=0))), "max >= 1"),
    ((Node("a"), Node("b", parents=("a",), loop=Loop(to="a", max=1, on=("omitted",)))),
     "fires on done, skipped or failed"),
    ((Node("a", choice=True, loop=Loop(to="a", max=1)), Node("b", parents=("a",))),
     "a choice and a loop"),
])
def test_check_dag_refuses_a_loop_that_cannot_hold(nodes, message):
    with pytest.raises(DagError, match=message):
        check_dag(nodes)


def test_a_loop_may_go_back_to_the_node_itself():
    check_dag((Node("a"), Node("b", parents=("a",), loop=Loop(to="b", max=2))))


@pytest.mark.parametrize("lease", ["soon", 0, "0s", -5])
def test_check_dag_refuses_a_lease_that_does_not_last(lease):
    with pytest.raises(DagError, match="lease"):
        check_dag((Node("a", lease=lease),))
