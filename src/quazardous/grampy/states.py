"""What the graph says about the projected states: replays and transitions.

    replay_targets       the states a replay can bring a subject back to
    replayed_after       what a replay from a node redoes
    to_undo              the nodes to forget when going back to a state
    source_state         the state a node takes its subjects from
    allowed_transitions  every state change the graph makes legitimate

All of it is DERIVED from `Node.working` / `Node.state`, never written by
hand. A second hand-written list of the same set is two truths that
diverge in silence — and the first to notice is a counter of "off-graph"
transitions shouting on a perfectly legitimate replay.
"""
from __future__ import annotations

from .dag import DagError, Node, ancestors, descendants, node


def replayed_after(name: str, dag: tuple[Node, ...]) -> tuple[str, ...]:
    """What a replay from this node REDOES: its downstream, minus `once` nodes."""
    return tuple(sorted(d for d in descendants(name, dag) if not node(d, dag).once))


def replay_targets(dag: tuple[Node, ...]) -> frozenset[str]:
    """THE REPLAY POINTS — the states a replay can bring a subject back to.

    A state is a replay point when it is PRODUCED by a node that has
    something to redo downstream. Going back to a state nothing produces —
    the entry state — means redoing everything, which is a requeue, not a
    replay.
    """
    return frozenset(n.state for n in dag
                     if n.state and replayed_after(n.name, dag))


def to_undo(state: str, dag: tuple[Node, ...]) -> tuple[str, ...]:
    """THE NODES DOWNSTREAM OF A REPLAY POINT — the rows to forget.

    A subject sent back to `state` must redo everything AFTER the node that
    produces it. Leaving a downstream row behind blocks the replay: a node
    is never claimed once a descendant has started.

    A state produced by NO node is the entry: everything is undone, in
    declaration order.
    """
    producer = {n.state: n.name for n in dag if n.state}
    start = producer.get(state)
    if start is None:
        return tuple(n.name for n in dag)
    return tuple(sorted(descendants(start, dag)))


def source_state(name: str, dag: tuple[Node, ...], *, entry: str) -> str:
    """The state a node takes its subjects FROM.

    It is the state of its nearest producing ancestor — `entry` for the
    root, or when nothing upstream produces a state. Parents that produce
    nothing are walked through: a branch that only enriches does not move
    the subject.

    A JOIN keeps only the most downstream producers: a producer that is
    the ancestor of another one has been overtaken. If two unrelated
    producers remain, the source is ambiguous and the graph is refused.
    """
    by_name = {n.name: n for n in dag}
    producers: set[str] = set()
    to_visit = list(node(name, dag).parents)
    seen: set[str] = set()
    while to_visit:
        current = to_visit.pop()
        if current in seen:
            continue
        seen.add(current)
        if by_name[current].state is not None:
            producers.add(current)
        else:
            to_visit.extend(by_name[current].parents)
    latest = {p for p in producers
              if not any(p in ancestors(other, dag) for other in producers)}
    states = {by_name[p].state for p in latest}
    if not states:
        return entry
    if len(states) > 1:
        raise DagError(
            f"node {name!r} takes its subjects from {sorted(states)} at once — "
            f"its source state is ambiguous")
    return states.pop()


def allowed_transitions(dag: tuple[Node, ...], *, entry: str,
                        exits: tuple[str, ...] = ()) -> frozenset[tuple[str, str]]:
    """Every `(from, to)` state change the graph makes legitimate.

        forward   source → working → state, for each node that moves the
                  subject (a node without `state` gives it back to its
                  source when done)
        return    working → source: a worker died, the batch goes back
        replay    final → replay point, a DELIBERATE step back
        exit      any non-exit state → each exit (failure, abandon)

    A FINAL state is one produced by a node below which nothing produces a
    state any more. Only a finished subject is replayed.
    """
    pairs: set[tuple[str, str]] = set()
    states: set[str] = {entry}
    for n in dag:
        if n.working is None and n.state is None:
            continue
        source = source_state(n.name, dag, entry=entry)
        target = n.state if n.state is not None else source
        states.update(s for s in (n.working, n.state) if s is not None)
        if n.working is not None:
            pairs.add((source, n.working))
            pairs.add((n.working, target))
            pairs.add((n.working, source))
        else:
            pairs.add((source, target))

    finals = {n.state for n in dag
              if n.state is not None
              and not any(node(d, dag).state is not None
                          for d in descendants(n.name, dag))}
    for final in finals:
        pairs.update((final, target) for target in replay_targets(dag))

    for start in states - set(exits):
        for exit_state in exits:
            pairs.add((start, exit_state))
    return frozenset((a, b) for a, b in pairs if a != b)
