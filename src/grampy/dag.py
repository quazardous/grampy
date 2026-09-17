"""The graph: nodes, their parents, and the rule that says what can run.

    Node             a node: who it descends from, what it projects
    node             the named node, in THIS graph
    descendants      everything downstream of a node
    ancestors        everything upstream of a node
    claimable        can this node be taken, given what is recorded?
    claimable_nodes  every node that can be taken right now
    check_dag        refuse a graph that does not hold together

This module knows no graph and no storage. A caller declares its own
graph as a tuple of `Node`, and a *progress* — `{node: status}` for one
subject — is all the rule needs. A missing node has never started: that
is the third state, and it costs no column.

────────────────────────────────────────────────────────────────────────
THE RULE IS PURE, AND THAT IS THE POINT
────────────────────────────────────────────────────────────────────────

"All my parents are concluded, nobody holds me, and no descendant has
started" used to exist only as a thirty-line `INSERT … SELECT`. Testing
it needed a database, so few cases were written, so some were missed —
and the "never go backwards" guard silently disappeared once, found only
by an end-to-end test one migration later.

Here a case is two lines: an invented graph, a progress, an answer. A
storage driver becomes an IMPLEMENTATION of this rule, and the shared
driver contract confronts the two (`grampy.testing`).
"""
from __future__ import annotations

from dataclasses import dataclass, field


class DagError(ValueError):
    """A graph that does not hold together, or a node nobody declared."""


#: WHAT A NODE CAN BE WORTH TO ITS CHILDREN.
#:
#:     running   someone holds it — the lease, nothing more
#:     done      finished, it produced what it had to
#:     skipped   we stopped waiting for it (optional node, grace expired)
#:
#: `done` AND `skipped` BOTH SATISFY A CHILD, on purpose: a child does not
#: need to know WHY its parent is concluded, only that it will not be kept
#: waiting. The distinction is for MEASURING, not for deciding.
NODE_RUNNING = "running"
NODE_DONE = "done"
NODE_SKIPPED = "skipped"
#: THE THIRD END, AND IT SATISFIES NOBODY.
#:
#: `done` and `skipped` say "I will not keep you waiting"; `failed` says "I
#: did not produce what you expected". A child can NOT start on it — it
#: would work on an input that does not exist.
NODE_FAILED = "failed"
#: WHAT SATISFIES A CHILD. The only list a claim consults.
NODE_SATISFYING = (NODE_DONE, NODE_SKIPPED)
#: WHAT IS NEVER TAKEN AGAIN. Going back means FORGETTING the row.
NODE_CONCLUDED = (NODE_DONE, NODE_SKIPPED, NODE_FAILED)


@dataclass(frozen=True)
class Node:
    """A node of the graph: who it descends from, and what it projects.

    `parents` IS THE ONLY DECLARATION OF ORDER. Empty means it starts from
    the root — a freshly submitted subject. Parents are DIRECT, never
    transitive: a redundant declaration ends up diverging.

    `working` and `state` ARE PROJECTIONS, NOT COMMANDS. An application
    may mirror a simple label on its subject — the one it carries while
    the node runs, and the one it reaches when the node concludes — so
    that whatever counts and displays keeps reading one word. Nothing in
    this package reads them to decide what to claim: the graph decides.
    `None` for a node that does not move the subject.

    `optional` says a failure of this node degrades without blocking:
    once a grace delay has passed, the node is marked `skipped` and its
    children go on. Only a CLAIMABLE node may be skipped (its own parents
    concluded), otherwise its children would start before its parents.

    `once` says a REPLAY DOES NOT REDO IT. Its row is forgotten with the
    rest of the downstream — otherwise its parent could not be claimed
    again — but its guard lives elsewhere, in the application.
    """

    name: str
    parents: tuple[str, ...] = field(default_factory=tuple)
    working: str | None = None
    state: str | None = None
    optional: bool = False
    once: bool = False


def node(name: str, dag: tuple[Node, ...]) -> Node:
    """The named node. Raises rather than returning `None`: a caller that
    got `None` would put an empty name in a query, and the claim would
    simply return nothing."""
    for candidate in dag:
        if candidate.name == name:
            return candidate
    raise DagError(f"unknown node: {name!r}")


def descendants(name: str, dag: tuple[Node, ...]) -> set[str]:
    """Every node downstream of this one, directly or not.

    WHAT THEY ARE FOR: NEVER GOING BACKWARDS. A node must not be claimed
    once one of its descendants has started — the subject has moved PAST
    it, and taking it again would rewind its projection over newer work.
    A state-based queue gets this guard for free (`WHERE state = source`);
    a graph does not, so it is written here.
    """
    by_parent: dict[str, list[str]] = {}
    for n in dag:
        for parent in n.parents:
            by_parent.setdefault(parent, []).append(n.name)

    seen: set[str] = set()
    to_visit = list(by_parent.get(name, ()))
    while to_visit:
        current = to_visit.pop()
        if current in seen:
            continue
        seen.add(current)
        to_visit.extend(by_parent.get(current, ()))
    return seen


def ancestors(name: str, dag: tuple[Node, ...]) -> set[str]:
    """Every node upstream of this one, directly or not.

    The mirror of `descendants`, and together they answer a repair:

        descendants   what to TRUNCATE — downstream of the point reached
        ancestors     what to ADOPT    — what the reached state attests

    A sibling branch is NOT an ancestor, which is why this goes through
    the graph rather than a list: a subject can reach a state without a
    parallel branch ever having run, and adopting it would invent work.
    """
    by_name = {n.name: n for n in dag}
    node(name, dag)                      # raises on an unknown node

    seen: set[str] = set()
    to_visit = list(by_name[name].parents)
    while to_visit:
        current = to_visit.pop()
        if current in seen:
            continue
        seen.add(current)
        to_visit.extend(by_name[current].parents)
    return seen


def claimable(name: str, dag: tuple[Node, ...],
              progress: dict[str, str]) -> bool:
    """Can this node be taken, given what is already recorded?

    THREE REFUSALS, AND THEY DO NOT MEAN THE SAME THING:

        a row already exists      someone holds it, or it is concluded
        a descendant has started  the subject has moved PAST it
        a parent is not concluded its input does not exist yet

    `done` AND `skipped` CONCLUDE; `failed` DOES NOT.
    """
    node(name, dag)                      # raises on an unknown node
    if name in progress:
        return False
    if any(d in progress for d in descendants(name, dag)):
        return False
    return all(progress.get(p) in NODE_SATISFYING
               for p in node(name, dag).parents)


def claimable_nodes(dag: tuple[Node, ...],
                    progress: dict[str, str]) -> set[str]:
    """EVERY node that can be taken now. Empty means the subject is
    finished, blocked by a failure, or entirely in progress.

    This is what makes PARALLELISM visible: a fork returns two names, and
    reading it needs neither a database nor a worker.
    """
    return {n.name for n in dag if claimable(n.name, dag, progress)}


def check_dag(dag: tuple[Node, ...]) -> None:
    """Refuse a graph that does not hold together. Call it at startup.

    FAIL WHEN THE GRAPH IS DECLARED, not three weeks later as a quality
    statistic.
    """
    by_name: dict[str, Node] = {}
    for n in dag:
        if n.name in by_name:
            raise DagError(f"two nodes are named {n.name!r}")
        by_name[n.name] = n

    for n in dag:
        for parent in n.parents:
            if parent not in by_name:
                raise DagError(
                    f"node {n.name!r} descends from {parent!r}, "
                    f"which does not exist")

    # ── NO CYCLE, PROVEN BY A WALK ─────────────────────────────────
    #
    # A cycle is invisible to the eye past a handful of nodes, and it
    # would not raise: none of its nodes would ever become claimable, and
    # the queue would stop there, silently.
    seen: set[str] = set()
    in_progress: set[str] = set()

    def descend(name: str) -> None:
        if name in in_progress:
            raise DagError(f"cycle in the graph, through {name!r}")
        if name in seen:
            return
        in_progress.add(name)
        for parent in by_name[name].parents:
            descend(parent)
        in_progress.discard(name)
        seen.add(name)

    for n in dag:
        descend(n.name)

    # ── ONE ROOT ───────────────────────────────────────────────────
    #
    # Two parentless nodes would be two entry points, and nothing would say
    # which one a fresh subject takes.
    roots = [n.name for n in dag if not n.parents]
    if len(roots) != 1:
        raise DagError(
            f"the graph has {len(roots)} root(s): {roots}. "
            f"A fresh subject would not know where to enter.")

    # ── A PROJECTED STATE HAS ONE AUTHOR ───────────────────────────
    #
    # Two nodes posting the same state would make the label ambiguous: one
    # would read it without knowing which node wrote it, and a janitor
    # would take back the wrong one.
    posted_by: dict[str, str] = {}
    for n in dag:
        for state in (n.working, n.state):
            if state is None:
                continue
            if state in posted_by and posted_by[state] != n.name:
                raise DagError(
                    f"state {state!r} is posted by {posted_by[state]!r} AND by "
                    f"{n.name!r} — the label becomes ambiguous")
            posted_by[state] = n.name
