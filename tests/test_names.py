"""Grampy's words: members that are their strings, on every Python."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from quazardous.grampy import (
    NODE_DONE,
    Backoff,
    Graph,
    Lane,
    Loop,
    Merge,
    Node,
    Outcome,
    Per,
    Position,
    Reason,
    Retry,
    Status,
    WhileRunning,
)
from quazardous.grampy.graph import Document


def test_a_member_is_its_string():
    assert Status.DONE == "done" and "done" == Status.DONE
    assert hash(Status.DONE) == hash("done")
    assert {"done": 1}[Status.DONE] == 1
    assert Status("done") is Status.DONE
    assert NODE_DONE is Status.DONE, "the old constants are the members"


def test_str_and_format_give_the_value_on_every_python():
    # A bare (str, Enum) writes `Status.DONE` in an f-string from 3.12 on.
    assert str(Reason.RETRY) == "retry"
    assert f"{Reason.RETRY}" == "retry"
    assert f"{Reason.RETRY:>7}" == "  retry"
    assert json.dumps({"s": Status.FAILED}) == '{"s": "failed"}'


def test_strings_still_work_and_become_members():
    lane = Lane(merge="last", position="last", while_running="skip")
    assert lane.merge is Merge.LAST and lane.position is Position.LAST
    assert lane.while_running is WhileRunning.SKIP
    assert Lane(merge="last") == Lane(merge=Merge.LAST)
    assert hash(Lane(merge="last")) == hash(Lane(merge=Merge.LAST))
    assert Retry(3, "1s", backoff="linear").backoff is Backoff.LINEAR
    assert Loop(to="a", max=2).on == (Status.FAILED,)
    node = Node("b", parents=("a",), on={"a": ("failed",)}, per="policy",
                concurrency=1)
    assert node.on["a"] == (Status.FAILED,) and node.per is Per.POLICY


def test_an_unknown_word_is_refused_by_name():
    with pytest.raises(ValueError, match="unknown backoff 'cubic'"):
        Retry(3, "1s", backoff="cubic")


def test_a_merge_function_is_named_without_its_prefix_by_hand():
    lane = Lane(merge=Merge.fn("ends"))
    assert lane.merge == "fn:ends" and lane.merger == "ends"


def test_a_document_stays_plain_data():
    graph = Graph(Document("words"), (
        Node("in", lane=Lane.batch()),
        Node("call", parents=("in",), retry=Retry(2, "1s", backoff=Backoff.LINEAR)),
        Node("undo", parents=("call",), on={"call": (Status.FAILED,)}),
    ))
    data = graph.to_dict()

    def members(value):
        if isinstance(value, dict):
            return [m for v in value.values() for m in members(v)]
        if isinstance(value, list):
            return [m for v in value for m in members(v)]
        return [value] if type(value) is not str and isinstance(value, str) else []

    assert members(data) == [], "to_dict holds plain strings, as its JSON does"
    assert Graph.from_json(graph.to_json()) == graph


def test_grampy_writes_no_word_of_its_own_as_a_bare_string():
    """The core and the drivers name grampy's words through the members.
    A literal left behind would read as the application's own string."""
    words = sorted({m.value for kind in (Status, Reason, Outcome, Merge, Position,
                                         WhileRunning, Backoff, Per) for m in kind},
                   key=len, reverse=True)
    pattern = re.compile(
        r"(?:status|reason|merge|position|while_running|backoff|per)\s*[=!]=?\s*"
        r"[\"'](" + "|".join(words) + r")[\"']")
    root = Path(__file__).parent.parent / "src" / "quazardous" / "grampy"
    found = [f"{path.name}:{n}: {line.strip()}"
             for path in sorted(root.rglob("*.py")) if path.name != "names.py"
             for n, line in enumerate(path.read_text().splitlines(), 1)
             if pattern.search(line) and not line.lstrip().startswith(("#", '"', "`"))]
    assert found == []
