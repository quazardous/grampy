"""THE GRAPH, DRAWN — Mermaid and Graphviz, with live counts if given.

    to_mermaid   a Mermaid `flowchart` (GitHub, GitLab and most docs render it)
    to_dot       a Graphviz `digraph`
    overlay      the journal's counts per node, ready to be drawn

────────────────────────────────────────────────────────────────────────
EVERY MECHANISM HAS A SHAPE — NOTHING DECLARED IS LEFT UNDRAWN
────────────────────────────────────────────────────────────────────────

    choice           a diamond
    wait             a hexagon, with the event and its timeout
    optional         a dashed border, with its grace
    need=k           `k/n` on the node
    on failed        a dashed red edge labelled with the statuses it accepts
    loop             a dotted edge back to `to`, labelled `loop ≤max`
    retry            `retry ×limit` on the node
    lease            `⏱ lease` on the node
    channels         `varies by channel` when a channel changes the node

A drawing that silently left out a loop or a failure edge would show a
workflow simpler than the one that runs; the tests confront every
mechanism of `dag.Node` with the drawing.

Counts (`overlay`) are an optional second layer: `▶ running  ⏳ scheduled
✓ done  ↷ skipped  ✗ failed  ∅ omitted`, zeros left out.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .dag import NODE_SATISFYING, Node

#: How a count is shown, in this order.
_COUNT_MARKS = (("running", "▶"), ("scheduled", "⏳"), ("done", "✓"),
                ("skipped", "↷"), ("failed", "✗"), ("omitted", "∅"))

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
                if "failed" in accepted:
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
        if n.optional:
            attrs.append('style="rounded,dashed"')
        lines.append(f'    "{_escape_dot(n.name)}" [{", ".join(attrs)}];')
    for n in nodes:
        for parent in n.parents:
            edge = f'    "{_escape_dot(parent)}" -> "{_escape_dot(n.name)}"'
            if parent in n.on:
                accepted = " / ".join(n.on[parent])
                colour = ', color="#d33", fontcolor="#d33"' if "failed" in n.on[parent] else ""
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
    """The nodes, and the names a channel changes."""
    nodes = tuple(getattr(graph, "nodes", graph))
    channels = getattr(graph, "channels", {}) or {}
    varying = {name for overrides in channels.values() for name in overrides}
    return nodes, varying


def _label(n: Node, varying: set[str], counts: Mapping[str, Mapping[str, int]] | None
           ) -> list[str]:
    lines = [n.name]
    badges = []
    if n.need is not None:
        badges.append(f"{n.need}/{len(n.parents)}")
    if n.wait is not None:
        badges.append(f"waits {n.wait}" + (f" ⏱ {n.timeout}" if n.timeout is not None else ""))
    if n.grace is not None:
        badges.append(f"grace {n.grace}")
    if n.retry is not None:
        badges.append(f"retry ×{n.retry.limit}")
    if n.lease is not None:
        badges.append(f"⏱ {n.lease}")
    if n.once:
        badges.append("once")
    if badges:
        lines.append(" · ".join(badges))
    if n.name in varying:
        lines.append("varies by channel")
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
    return "(", ")"


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
