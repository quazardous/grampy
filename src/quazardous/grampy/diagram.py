"""THE GRAPH, DRAWN — Mermaid and Graphviz, with live counts if given.

    to_mermaid   a Mermaid `flowchart` (GitHub, GitLab and most docs render it)
    to_state_diagram
                 a Mermaid `stateDiagram-v2`, the way a statechart reads
    to_dot       a Graphviz `digraph`
    overlay      the journal's counts per node, ready to be drawn

────────────────────────────────────────────────────────────────────────
EVERY MECHANISM HAS A SHAPE — NOTHING DECLARED IS LEFT UNDRAWN
────────────────────────────────────────────────────────────────────────

    choice           a diamond
    wait             a hexagon, with the event and its timeout
    lane             a trapezoid (Mermaid) or a house (DOT), with its merge,
                     place and timings
    optional         a dashed border, with its grace
    need=k           `k/n` on the node
    on failed        a dashed red edge labelled with the statuses it accepts
    loop             a dotted edge back to `to`, labelled `loop ≤max`
    retry            `retry ×limit` on the node
    rate             `rate limit/period` per band, `≤n at once`, `per policy`
    lease            `⏱ lease` on the node
    policies         `varies by policy` when a policy changes the node

A drawing that silently left out a loop or a failure edge would show a
workflow simpler than the one that runs; the tests confront every
mechanism of `dag.Node` with the drawing.

Counts (`overlay`) are an optional second layer: `⧖ waiting in a lane
▶ running  ⏳ scheduled  ✓ done  ↷ skipped  ✗ failed  ∅ omitted`, zeros left
out.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .dag import NODE_SATISFYING, Node
from .names import Merge, Outcome, Per, Status, WhileRunning

#: How a count is shown, in this order.
_COUNT_MARKS = ((Outcome.WAITING, "⧖"), (Status.RUNNING, "▶"), (Status.SCHEDULED, "⏳"),
                (Status.DONE, "✓"), (Status.SKIPPED, "↷"), (Status.FAILED, "✗"),
                (Status.OMITTED, "∅"))

_UNSAFE_ID = re.compile(r"[^A-Za-z0-9_]")


def overlay(journal: Any) -> dict[str, dict[str, int]]:
    """`{node: {status: count}}` for every node of the journal's graph."""
    return {n.name: journal.counts(n.name) for n in journal.dag}


def to_mermaid(graph: Any, counts: Mapping[str, Mapping[str, int]] | None = None,
               *, direction: str = "LR") -> str:
    """A Mermaid flowchart of the graph (`Graph` or tuple of nodes)."""
    nodes, varying = _nodes(graph)
    ids = _ids(nodes)
    lines = [f"flowchart {direction}"]
    for n in nodes:
        label = _escape_mermaid("<br/>".join(_label(n, varying, counts)))
        open_, close = _mermaid_shape(n)
        lines.append(f'    {ids[n.name]}{open_}"{label}"{close}')
    link = 0
    red: list[int] = []
    for n in nodes:
        for parent in n.parents:
            accepted = tuple(n.on.get(parent, NODE_SATISFYING))
            if parent in n.on:
                lines.append(f'    {ids[parent]} -.->|"{" / ".join(accepted)}"| {ids[n.name]}')
                if Status.FAILED in accepted:
                    red.append(link)
            else:
                lines.append(f"    {ids[parent]} --> {ids[n.name]}")
            link += 1
        if n.loop is not None:
            on = " / ".join(n.loop.on)
            lines.append(f'    {ids[n.name]} -.->|"loop ≤{n.loop.max} on {on}"| {ids[n.loop.to]}')
            link += 1
    optional = [ids[n.name] for n in nodes if n.optional]
    if optional:
        lines.append("    classDef optional stroke-dasharray: 5 5")
        lines.append(f"    class {','.join(optional)} optional")
    for index in red:
        lines.append(f"    linkStyle {index} stroke:#d33,color:#d33")
    return "\n".join(lines) + "\n"


def to_state_diagram(graph: Any, *, direction: str = "LR") -> str:
    """A Mermaid `stateDiagram-v2` of the graph (`Graph` or tuple of nodes).

    THE SAME GRAPH, AS A STATECHART READER EXPECTS IT: `[*]` enters the root
    and leaves from the nodes nobody follows; a choice is a `<<choice>>`
    pseudo-state, a fork and a join are `<<fork>>` and `<<join>>`; a join
    that needs only some parents says `need k/n` on its way out; an edge that
    accepts other statuses says which; a loop and a retry go back, labelled.
    What a node waits for, its lane, its lease and limits are notes.

    No counts: Mermaid cannot style the states of this diagram as it styles a
    flowchart's nodes — `to_mermaid(graph, counts)` draws them."""
    nodes, varying = _nodes(graph)
    ids = _ids(nodes)
    children: dict[str, list[Node]] = {}
    for n in nodes:
        for parent in n.parents:
            children.setdefault(parent, []).append(n)
    lines = ["stateDiagram-v2", f"    direction {direction}"]
    for n in nodes:
        lines.append(f'    state "{_escape_state(n.name)}" as {ids[n.name]}')
    # A pseudo-state must be declared before a transition names it, or
    # Mermaid draws an ordinary state of that name.
    for n in nodes:
        if len(n.parents) > 1:
            lines.append(f"    state {ids[n.name]}_join <<join>>")
    for n in nodes:
        if not n.parents:
            lines.append(f"    [*] --> {ids[n.name]}")
    for n in nodes:
        after = children.get(n.name, [])
        if not after:
            lines.append(f"    {ids[n.name]} --> [*]")
            continue
        target = {c.name: f"{ids[c.name]}_join" if len(c.parents) > 1 else ids[c.name]
                  for c in after}
        # Children taken on different outcomes are alternatives, not branches
        # running together: an outcome choice first, a fork only among the
        # children of one outcome.
        outcomes: dict[str, list[Node]] = {}
        for child in after:
            outcomes.setdefault(_edge_label(child, n.name), []).append(child)
        if n.choice or len(outcomes) > 1:
            split = f"{ids[n.name]}_choice"
            lines += [f"    state {split} <<choice>>", f"    {ids[n.name]} --> {split}"]
            for i, (label, group) in enumerate(outcomes.items()):
                shown = label or ("" if n.choice else Status.DONE)
                arrow = f" : {shown}" if shown else ""
                if len(group) == 1 or n.choice:
                    lines += [f"    {split} --> {target[c.name]}{arrow}" for c in group]
                else:
                    fork = f"{ids[n.name]}_fork{i}"
                    lines += [f"    state {fork} <<fork>>", f"    {split} --> {fork}{arrow}"]
                    lines += [f"    {fork} --> {target[c.name]}" for c in group]
        elif len(after) > 1:
            fork = f"{ids[n.name]}_fork"
            lines += [f"    state {fork} <<fork>>", f"    {ids[n.name]} --> {fork}"]
            lines += [f"    {fork} --> {target[c.name]}" for c in after]
        else:
            label = _edge_label(after[0], n.name)
            lines.append(f"    {ids[n.name]} --> {target[after[0].name]}"
                         + (f" : {label}" if label else ""))
    for n in nodes:
        if len(n.parents) > 1:
            join = f"{ids[n.name]}_join"
            need = f" : need {n.need}/{len(n.parents)}" if n.need is not None else ""
            lines.append(f"    {join} --> {ids[n.name]}{need}")
        if n.loop is not None:
            on = " / ".join(n.loop.on)
            lines.append(f"    {ids[n.name]} --> {ids[n.loop.to]} : loop ≤{n.loop.max} on {on}")
        if n.retry is not None:
            lines.append(f"    {ids[n.name]} --> {ids[n.name]} : failed, retry ×{n.retry.limit}")
    for n in nodes:
        notes = _state_notes(n, varying)
        if notes:
            lines.append(f"    note right of {ids[n.name]} : {_escape_state(' · '.join(notes))}")
    return "\n".join(lines) + "\n"


def _edge_label(child: Node, parent: str) -> str:
    if parent not in child.on:
        return ""
    return " / ".join(child.on[parent])


def _state_notes(n: Node, varying: set[str]) -> list[str]:
    notes = []
    if n.wait is not None:
        notes.append(f"waits {n.wait}" + (f", timeout {n.timeout}" if n.timeout else ""))
    if n.lane is not None:
        notes.append(_describe_lane(n))
    if n.optional:
        notes.append("optional" + (f", grace {n.grace}" if n.grace is not None else ""))
    if n.lease is not None:
        notes.append(f"lease {n.lease}")
    if n.rate:
        notes.append("rate " + " + ".join(f"{b.limit}/{b.period}" for b in n.rate))
    if n.concurrency is not None:
        notes.append(f"≤{n.concurrency} at once")
    if (n.rate or n.concurrency is not None) and n.per == Per.POLICY:
        notes.append("per policy")
    if n.once:
        notes.append("once")
    if n.name in varying:
        notes.append("varies by policy")
    return notes


def _escape_state(text: str) -> str:
    # A state label cannot hold a double quote, and a colon starts a label.
    return text.replace('"', "'").replace(":", "∶")


def to_dot(graph: Any, counts: Mapping[str, Mapping[str, int]] | None = None,
           *, direction: str = "LR") -> str:
    """A Graphviz digraph of the graph (`Graph` or tuple of nodes)."""
    nodes, varying = _nodes(graph)
    lines = ["digraph grampy {", f"    rankdir={direction};",
             '    node [fontname="Helvetica", shape=box, style=rounded];']
    for n in nodes:
        label = _escape_dot("\\n".join(_label(n, varying, counts)))
        attrs = [f'label="{label}"']
        if n.choice:
            attrs.append("shape=diamond")
        elif n.wait is not None:
            attrs.append("shape=hexagon")
        elif n.lane is not None:
            attrs.append("shape=house")
        if n.optional:
            attrs.append('style="rounded,dashed"')
        lines.append(f'    "{_escape_dot(n.name)}" [{", ".join(attrs)}];')
    for n in nodes:
        for parent in n.parents:
            edge = f'    "{_escape_dot(parent)}" -> "{_escape_dot(n.name)}"'
            if parent in n.on:
                accepted = " / ".join(n.on[parent])
                colour = ', color="#d33", fontcolor="#d33"' if Status.FAILED in n.on[parent] else ""
                edge += f' [style=dashed, label="{_escape_dot(accepted)}"{colour}]'
            lines.append(edge + ";")
        if n.loop is not None:
            on = " / ".join(n.loop.on)
            lines.append(f'    "{_escape_dot(n.name)}" -> "{_escape_dot(n.loop.to)}" '
                         f'[style=dotted, constraint=false, '
                         f'label="loop ≤{n.loop.max} on {_escape_dot(on)}"];')
    lines.append("}")
    return "\n".join(lines) + "\n"


# -- inside --------------------------------------------------------------------


def _nodes(graph: Any) -> tuple[tuple[Node, ...], set[str]]:
    """The nodes, and the names a policy changes."""
    nodes = tuple(getattr(graph, "nodes", graph))
    policies = getattr(graph, "policies", {}) or {}
    varying = {name for overrides in policies.values() for name in overrides}
    return nodes, varying


def _label(n: Node, varying: set[str], counts: Mapping[str, Mapping[str, int]] | None
           ) -> list[str]:
    lines = [n.name]
    badges = []
    if n.need is not None:
        badges.append(f"{n.need}/{len(n.parents)}")
    if n.wait is not None:
        badges.append(f"waits {n.wait}" + (f" ⏱ {n.timeout}" if n.timeout is not None else ""))
    if n.lane is not None:
        badges.append(_describe_lane(n))
    if n.grace is not None:
        badges.append(f"grace {n.grace}")
    if n.retry is not None:
        badges.append(f"retry ×{n.retry.limit}")
    if n.rate:
        badges.append("rate " + " + ".join(f"{b.limit}/{b.period}" for b in n.rate))
    if n.concurrency is not None:
        badges.append(f"≤{n.concurrency} at once")
    if (n.rate or n.concurrency is not None) and n.per == Per.POLICY:
        badges.append("per policy")
    if n.lease is not None:
        badges.append(f"⏱ {n.lease}")
    if n.once:
        badges.append("once")
    if badges:
        lines.append(" · ".join(badges))
    if n.name in varying:
        lines.append("varies by policy")
    if counts is not None and n.name in counts:
        shown = [f"{mark}{counts[n.name][status]}" for status, mark in _COUNT_MARKS
                 if counts[n.name].get(status)]
        if shown:
            lines.append(" ".join(shown))
    return lines


def _mermaid_shape(n: Node) -> tuple[str, str]:
    if n.choice:
        return "{", "}"
    if n.wait is not None:
        return "{{", "}}"
    if n.lane is not None:
        return "[/", "\\]"
    return "(", ")"


def _describe_lane(n: Node) -> str:
    lane = n.lane
    assert lane is not None
    if lane.merger is not None:
        head = f"lane: merged by {lane.merger}"
    elif lane.merge == Merge.ALL:
        head = f"lane: every version, up to {lane.max_size}"
    else:
        head = f"lane: {lane.merge} version"
    words = [f"{head}, place of the {lane.position}"]
    for label in ("cooldown", "delay", "max_wait"):
        if getattr(lane, label) is not None:
            words.append(f"{label.replace('_', ' ')} {getattr(lane, label)}")
    if lane.while_running == WhileRunning.SKIP:
        words.append("skips while running")
    return " · ".join(words)


def _ids(nodes: tuple[Node, ...]) -> dict[str, str]:
    """Mermaid ids: node names made safe, unique even when two names clean
    up to the same text."""
    ids: dict[str, str] = {}
    used: set[str] = set()
    for n in nodes:
        base = "n_" + _UNSAFE_ID.sub("_", n.name)
        candidate, i = base, 1
        while candidate in used:
            i += 1
            candidate = f"{base}_{i}"
        used.add(candidate)
        ids[n.name] = candidate
    return ids


def _escape_mermaid(text: str) -> str:
    return text.replace('"', "#quot;")


def _escape_dot(text: str) -> str:
    return text.replace("\\n", "\0").replace("\\", "\\\\").replace('"', '\\"').replace("\0", "\\n")
