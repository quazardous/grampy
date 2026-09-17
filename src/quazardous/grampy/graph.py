"""THE GRAPH AS DATA — a workflow that can be written, read, stored, drawn.

    Document   who the graph is: dsl, namespace, name, version
    Graph      a document, its nodes, and per-channel settings, all checked
    OVERRIDABLE  the node settings a channel may change
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
from dataclasses import dataclass, field, replace
from typing import Any

from .dag import DagError, Lane, Loop, Node, check_dag
from .timing import Rate, Retry

#: THE FORMAT THIS VERSION READS AND WRITES.
DSL = "grampy/1"

_NODE_FLAGS = ("optional", "once", "choice")

#: WHAT A CHANNEL MAY CHANGE ON A NODE: its settings, never its structure.
#: Parents, joins, choices, loops and waits are the workflow; how long to
#: wait, how often to retry, how long a lease lasts are how a source is
#: treated.
OVERRIDABLE = ("retry", "lease", "timeout", "grace", "rate", "concurrency", "lane")
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
    """A document, its nodes, and the settings some CHANNELS change.
    `check_dag` runs on construction, on the nodes and on every channel's
    variant: a Graph that exists holds together for every source.

        Graph(doc, nodes, channels={"partner-a": {"call": {"retry": Retry(5, "1m")}}})
    """

    document: Document
    nodes: tuple[Node, ...]
    channels: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "nodes", tuple(self.nodes))
        check_dag(self.nodes)
        names = {n.name for n in self.nodes}
        for channel, overrides in self.channels.items():
            for name, settings in overrides.items():
                if name not in names:
                    raise DagError(f"channel {channel!r} changes {name!r}, which does not exist")
                refused = sorted(set(settings) - set(OVERRIDABLE))
                if refused:
                    raise DagError(
                        f"channel {channel!r} changes {refused} on {name!r} — a channel "
                        f"changes only {list(OVERRIDABLE)}, never the structure")
                shared = sorted({"rate", "concurrency"} & set(settings))
                base = next(n for n in self.nodes if n.name == name)
                if "lane" in settings and (base.lane is None or settings["lane"] is None):
                    raise DagError(
                        f"channel {channel!r} changes the lane of {name!r}: a channel tunes "
                        f"a lane the node declares, it never adds or removes one")
                if shared and base.per != "channel":
                    raise DagError(
                        f"channel {channel!r} changes {shared} on {name!r}, whose budget is "
                        f"shared by every channel — declare per='channel' first")
            check_dag(self.variant(channel))

    def variant(self, channel: str | None) -> tuple[Node, ...]:
        """The nodes as `channel` sees them: its settings over the defaults.
        An unknown channel, or none, sees the defaults."""
        overrides = self.channels.get(channel, {}) if channel is not None else {}
        return tuple(replace(n, **overrides[n.name]) if n.name in overrides else n
                     for n in self.nodes)

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
            for key in ("lease", "wait", "timeout", "grace"):
                if getattr(n, key) is not None:
                    spec[key] = getattr(n, key)
            if n.retry is not None:
                spec["retry"] = _retry_to_dict(n.retry)
            if n.rate:
                spec["rate"] = [_rate_to_dict(band) for band in n.rate]
            if n.concurrency is not None:
                spec["concurrency"] = n.concurrency
            if n.per != "all":
                spec["per"] = n.per
            if n.lane is not None:
                spec["lane"] = _lane_to_dict(n.lane)
            for flag in _NODE_FLAGS:
                if getattr(n, flag):
                    spec[flag] = True
            nodes[n.name] = spec
        d = self.document
        out: dict[str, Any] = {"document": {"dsl": d.dsl, "namespace": d.namespace,
                                            "name": d.name, "version": d.version},
                               "nodes": nodes}
        if self.channels:
            out["channels"] = {
                channel: {name: {key: _setting_to_dict(key, value)
                                 for key, value in settings.items()}
                          for name, settings in overrides.items()}
                for channel, overrides in self.channels.items()}
        return out

    def to_json(self, **kwargs: Any) -> str:
        kwargs.setdefault("indent", 2)
        return json.dumps(self.to_dict(), **kwargs)

    # -- read ----------------------------------------------------------------

    @classmethod
    def from_dict(cls, data: Any) -> Graph:
        top = _mapping(data, "$", required=("document", "nodes"),
                       allowed=("document", "nodes", "channels"))
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
                            allowed=("parents", "on", "need", "loop", "retry", "lease",
                                     "wait", "timeout", "grace", "rate", "concurrency", "per",
                                     "lane",
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
            retry = _retry_from(spec["retry"], f"{path}.retry") if "retry" in spec else None
            for key in ("lease", "timeout", "grace"):
                _duration(spec.get(key), f"{path}.{key}")
            if "wait" in spec:
                _string(spec["wait"], f"{path}.wait")
            rate = _rates_from(spec["rate"], f"{path}.rate") if "rate" in spec else ()
            concurrency = _count(spec.get("concurrency"), f"{path}.concurrency")
            per = spec.get("per", "all")
            _string(per, f"{path}.per")
            lane = _lane_from(spec["lane"], f"{path}.lane") if "lane" in spec else None
            nodes.append(Node(name, parents=tuple(parents), loop=loop, retry=retry, lane=lane,
                              rate=rate, concurrency=concurrency, per=per,
                              lease=spec.get("lease"), wait=spec.get("wait"),
                              timeout=spec.get("timeout"), grace=spec.get("grace"),
                              working=spec.get("working"), state=spec.get("state"),
                              optional=spec.get("optional", False),
                              once=spec.get("once", False),
                              on={p: tuple(v) for p, v in on.items()}, need=need,
                              choice=spec.get("choice", False)))
        channels: dict[str, dict[str, dict[str, Any]]] = {}
        raw_channels = top.get("channels", {})
        if not isinstance(raw_channels, dict):
            raise GraphFormatError("$.channels: expected an object of channels by name")
        for channel, raw_overrides in raw_channels.items():
            if not isinstance(raw_overrides, dict):
                raise GraphFormatError(f"$.channels.{channel}: expected an object of nodes")
            for name, raw_settings in raw_overrides.items():
                path = f"$.channels.{channel}.{name}"
                settings = _mapping(raw_settings, path, required=(), allowed=OVERRIDABLE)
                parsed: dict[str, Any] = {}
                for key, value in settings.items():
                    if key == "retry":
                        parsed[key] = _retry_from(value, f"{path}.retry")
                    elif key == "rate":
                        parsed[key] = _rates_from(value, f"{path}.rate")
                    elif key == "concurrency":
                        parsed[key] = _count(value, f"{path}.concurrency")
                    elif key == "lane":
                        parsed[key] = _lane_from(value, f"{path}.lane")
                    else:
                        _duration(value, f"{path}.{key}")
                        parsed[key] = value
                channels.setdefault(channel, {})[name] = parsed
        return cls(document, tuple(nodes), channels)

    @classmethod
    def from_json(cls, text: str) -> Graph:
        try:
            data = json.loads(text, object_pairs_hook=_no_duplicate)
        except json.JSONDecodeError as exc:
            raise GraphFormatError(f"$: not JSON — {exc}") from exc
        return cls.from_dict(data)


def _retry_to_dict(retry: Retry) -> dict[str, Any]:
    out: dict[str, Any] = {"limit": retry.limit, "delay": retry.delay, "backoff": retry.backoff}
    if retry.max_delay is not None:
        out["max_delay"] = retry.max_delay
    if retry.jitter:
        out["jitter"] = retry.jitter
    return out


def _rate_to_dict(band: Rate) -> dict[str, Any]:
    out: dict[str, Any] = {"limit": band.limit, "period": band.period}
    if band.burst is not None:
        out["burst"] = band.burst
    return out


def _lane_to_dict(lane: Lane) -> dict[str, Any]:
    default = Lane()
    return {key: getattr(lane, key) for key in _LANE_KEYS
            if getattr(lane, key) != getattr(default, key)}


_LANE_KEYS = ("merge", "position", "cooldown", "delay", "max_wait", "while_running")


def _lane_from(value: Any, path: str) -> Lane:
    raw = _mapping(value, path, required=(), allowed=_LANE_KEYS)
    for key in ("merge", "position", "while_running"):
        if key in raw:
            _string(raw[key], f"{path}.{key}")
    for key in ("cooldown", "delay", "max_wait"):
        _duration(raw.get(key), f"{path}.{key}")
    return Lane(**raw)


def _setting_to_dict(key: str, value: Any) -> Any:
    if key == "lane":
        return _lane_to_dict(value)
    if key == "retry":
        return _retry_to_dict(value)
    if key == "rate":
        return [_rate_to_dict(band) for band in value]
    return value


def _rates_from(value: Any, path: str) -> tuple[Rate, ...]:
    if not isinstance(value, list):
        raise GraphFormatError(f"{path}: expected a list of bands")
    bands = []
    for i, raw in enumerate(value):
        spec = _mapping(raw, f"{path}[{i}]", required=("limit", "period"),
                        allowed=("limit", "period", "burst"))
        try:
            bands.append(Rate(**spec))
        except (TypeError, ValueError) as exc:
            raise GraphFormatError(f"{path}[{i}]: {exc}") from exc
    return tuple(bands)


def _count(value: Any, path: str) -> int | None:
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise GraphFormatError(f"{path}: expected an integer")
    return value


def _retry_from(value: Any, path: str) -> Retry:
    raw = _mapping(value, path, required=("limit",),
                   allowed=("limit", "delay", "backoff", "max_delay", "jitter"))
    try:
        return Retry(**raw)
    except (TypeError, ValueError) as exc:
        raise GraphFormatError(f"{path}: {exc}") from exc


def _duration(value: Any, path: str) -> None:
    if value is not None and (isinstance(value, bool)
                              or not isinstance(value, (int, float, str))):
        raise GraphFormatError(f"{path}: expected a duration (30s, 10m, 2h)")


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
