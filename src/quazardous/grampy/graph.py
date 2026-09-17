"""THE GRAPH AS DATA — a workflow that can be written, read, stored, drawn.

    Document   who the graph is: dsl, namespace, name, version
    Graph      a document and its nodes, checked on construction
    to_dict / from_dict, to_json / from_json

────────────────────────────────────────────────────────────────────────
A DEFINITION IS DATA, AN EXECUTION IS NOT
────────────────────────────────────────────────────────────────────────

A graph declared in Python is only readable by Python. Written as plain
data, the same definition can be kept next to a migration, compared
between two versions, drawn, or translated for another tool — the way a
statechart definition is kept apart from the machine running it. The
journal still runs on `Graph.nodes`, the tuple it always ran on.

────────────────────────────────────────────────────────────────────────
ONE CANONICAL FORM
────────────────────────────────────────────────────────────────────────

`to_dict` writes what differs from a default and nothing else, nodes in
declaration order: two equal graphs give the same dict, and a diff between
two versions shows only what changed. `from_dict` is strict — an unknown
key or a wrong type is refused with the path to it, because a misspelled
option silently ignored is a workflow that does not do what it says.

The header's `dsl` names the format (`grampy/1`). A document written in a
format this version does not know is refused rather than half read.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .dag import Loop, Node, check_dag
from .timing import Retry

#: THE FORMAT THIS VERSION READS AND WRITES.
DSL = "grampy/1"

_NODE_FLAGS = ("optional", "once", "choice")
_NODE_LABELS = ("working", "state")


class GraphFormatError(ValueError):
    """A graph document that cannot be read — the message names the path."""


@dataclass(frozen=True)
class Document:
    """Who a graph is. `version` is the application's own, free text: two
    documents with the same name and different versions are two graphs."""

    name: str
    version: str = "0"
    namespace: str = "default"
    dsl: str = DSL


@dataclass(frozen=True)
class Graph:
    """A document and its nodes. `check_dag` runs on construction: a Graph
    that exists holds together."""

    document: Document
    nodes: tuple[Node, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))
        check_dag(self.nodes)

    # -- write ---------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        nodes: dict[str, Any] = {}
        for n in self.nodes:
            spec: dict[str, Any] = {}
            if n.parents:
                spec["parents"] = list(n.parents)
            for label in _NODE_LABELS:
                if getattr(n, label) is not None:
                    spec[label] = getattr(n, label)
            if n.on:
                spec["on"] = {parent: list(statuses) for parent, statuses in n.on.items()}
            if n.need is not None:
                spec["need"] = n.need
            if n.loop is not None:
                spec["loop"] = {"to": n.loop.to, "max": n.loop.max, "on": list(n.loop.on)}
            if n.retry is not None:
                spec["retry"] = {"limit": n.retry.limit, "delay": n.retry.delay,
                                 "backoff": n.retry.backoff}
                if n.retry.max_delay is not None:
                    spec["retry"]["max_delay"] = n.retry.max_delay
                if n.retry.jitter:
                    spec["retry"]["jitter"] = n.retry.jitter
            for flag in _NODE_FLAGS:
                if getattr(n, flag):
                    spec[flag] = True
            nodes[n.name] = spec
        d = self.document
        return {"document": {"dsl": d.dsl, "namespace": d.namespace,
                             "name": d.name, "version": d.version},
                "nodes": nodes}

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("indent", 2)
        return json.dumps(self.to_dict(), **kwargs)

    # -- read ----------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: Any) -> Graph:
        top = _mapping(data, "$", required=("document", "nodes"),
                       allowed=("document", "nodes"))
        head = _mapping(top["document"], "$.document", required=("name",),
                        allowed=("dsl", "namespace", "name", "version"))
        for key in head:
            _string(head[key], f"$.document.{key}")
        dsl = head.get("dsl", DSL)
        if dsl != DSL:
            raise GraphFormatError(
                f"$.document.dsl: {dsl!r} is not a format this version reads ({DSL!r})")
        document = Document(name=head["name"], version=head.get("version", "0"),
                            namespace=head.get("namespace", "default"), dsl=dsl)

        specs = top["nodes"]
        if not isinstance(specs, dict):
            raise GraphFormatError("$.nodes: expected an object of nodes by name")
        nodes = []
        for name, raw in specs.items():
            path = f"$.nodes.{name}"
            spec = _mapping(raw, path, required=(),
                            allowed=("parents", "on", "need", "loop", "retry",
                                     *_NODE_LABELS, *_NODE_FLAGS))
            parents = spec.get("parents", [])
            if not isinstance(parents, list):
                raise GraphFormatError(f"{path}.parents: expected a list of node names")
            for i, parent in enumerate(parents):
                _string(parent, f"{path}.parents[{i}]")
            for label in _NODE_LABELS:
                if label in spec and spec[label] is not None:
                    _string(spec[label], f"{path}.{label}")
            for flag in _NODE_FLAGS:
                if flag in spec and not isinstance(spec[flag], bool):
                    raise GraphFormatError(f"{path}.{flag}: expected true or false")
            on = spec.get("on", {})
            if not isinstance(on, dict):
                raise GraphFormatError(f"{path}.on: expected an object of statuses by parent")
            for parent, statuses in on.items():
                if not isinstance(statuses, list):
                    raise GraphFormatError(f"{path}.on.{parent}: expected a list of statuses")
                for i, status in enumerate(statuses):
                    _string(status, f"{path}.on.{parent}[{i}]")
            need = spec.get("need")
            if need is not None and (isinstance(need, bool) or not isinstance(need, int)):
                raise GraphFormatError(f"{path}.need: expected an integer")
            loop = None
            if "loop" in spec:
                raw_loop = _mapping(spec["loop"], f"{path}.loop", required=("to", "max"),
                                    allowed=("to", "max", "on"))
                _string(raw_loop["to"], f"{path}.loop.to")
                if isinstance(raw_loop["max"], bool) or not isinstance(raw_loop["max"], int):
                    raise GraphFormatError(f"{path}.loop.max: expected an integer")
                loop_on = raw_loop.get("on", ["failed"])
                if not isinstance(loop_on, list):
                    raise GraphFormatError(f"{path}.loop.on: expected a list of statuses")
                for i, status in enumerate(loop_on):
                    _string(status, f"{path}.loop.on[{i}]")
                loop = Loop(to=raw_loop["to"], max=raw_loop["max"], on=tuple(loop_on))
            retry = None
            if "retry" in spec:
                raw_retry = _mapping(spec["retry"], f"{path}.retry", required=("limit",),
                                     allowed=("limit", "delay", "backoff", "max_delay",
                                              "jitter"))
                try:
                    retry = Retry(**raw_retry)
                except (TypeError, ValueError) as exc:
                    raise GraphFormatError(f"{path}.retry: {exc}") from exc
            nodes.append(Node(name, parents=tuple(parents), loop=loop, retry=retry,
                              working=spec.get("working"), state=spec.get("state"),
                              optional=spec.get("optional", False),
                              once=spec.get("once", False),
                              on={p: tuple(v) for p, v in on.items()}, need=need,
                              choice=spec.get("choice", False)))
        return cls(document, tuple(nodes))

    @classmethod
    def from_json(cls, text: str) -> Graph:
        try:
            data = json.loads(text, object_pairs_hook=_no_duplicate)
        except json.JSONDecodeError as exc:
            raise GraphFormatError(f"$: not JSON — {exc}") from exc
        return cls.from_dict(data)


def _no_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """JSON lets a key appear twice and keeps the last one: two nodes of the
    same name would silently become one."""
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise GraphFormatError(f"$: the key {key!r} appears twice")
        out[key] = value
    return out


def _mapping(value: Any, path: str, *, required: tuple[str, ...],
             allowed: tuple[str, ...]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GraphFormatError(f"{path}: expected an object")
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise GraphFormatError(
            f"{path}: unknown key(s) {unknown} — expected among {list(allowed)}")
    missing = [k for k in required if k not in value]
    if missing:
        raise GraphFormatError(f"{path}: missing key(s) {missing}")
    return value


def _string(value: Any, path: str) -> None:
    if not isinstance(value, str) or not value:
        raise GraphFormatError(f"{path}: expected a non-empty string")
