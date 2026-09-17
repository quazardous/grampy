"""The graph, drawn: every declared mechanism shows, counts overlay."""
from __future__ import annotations

import re
import shutil
import subprocess

import pytest

from quazardous.grampy import Document, Graph, Loop, Node, NodeJournal
from quazardous.grampy.diagram import overlay, to_dot, to_mermaid
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.timing import Retry

#: The brick sorter of the demo: every mechanism in one line.
SORTER = Graph(Document("brick-sorter"), (
    Node("scan", choice=True, lease="1m"),
    Node("colour", parents=("scan",)),
    Node("quarantine", parents=("scan",), wait="deminer.called", timeout="1h"),
    Node("defuse", parents=("quarantine",), retry=Retry(limit=3, delay="10s")),
    Node("reject", parents=("defuse",), on={"defuse": ("failed",)}),
    Node("check", parents=("colour",), optional=True, grace="5m",
         loop=Loop(to="colour", max=2)),
    Node("pack", parents=("colour", "defuse", "check"), need=2, once=True),
), channels={"supplier-b": {"defuse": {"retry": Retry(limit=5)}}})


def test_mermaid_draws_every_mechanism():
    text = to_mermaid(SORTER)
    assert text.startswith("flowchart LR\n")
    assert 'n_scan{"scan<br/>⏱ 1m"}' in text, "a choice is a diamond"
    assert 'n_quarantine{{"quarantine<br/>waits deminer.called ⏱ 1h"}}' in text
    assert "retry ×3" in text and "varies by channel" in text
    assert "2/3 · once" in text
    assert 'n_defuse -.->|"failed"| n_reject' in text, "a failure edge"
    assert 'n_check -.->|"loop ≤2 on failed"| n_colour' in text, "a loop edge"
    assert "class n_check optional" in text
    assert re.search(r"linkStyle \d+ stroke:#d33", text), "the failure edge is red"
    assert text.count("n_colour --> n_pack") == 1


def test_the_red_link_is_the_failure_edge():
    text = to_mermaid(SORTER)
    links = [line.strip() for line in text.splitlines()
             if "-->" in line or "-.->" in line]
    [index] = [int(m.group(1)) for m in re.finditer(r"linkStyle (\d+) stroke:#d33", text)]
    assert links[index] == 'n_defuse -.->|"failed"| n_reject'


def test_dot_draws_every_mechanism():
    text = to_dot(SORTER)
    assert text.startswith("digraph grampy {")
    assert '"scan" [label="scan\\n⏱ 1m", shape=diamond];' in text
    assert "shape=hexagon" in text and 'style="rounded,dashed"' in text
    assert '"defuse" -> "reject" [style=dashed, label="failed", color="#d33"' in text
    assert '"check" -> "colour" [style=dotted, constraint=false' in text


def test_counts_overlay_the_journal():
    journal = NodeJournal(MemoryDriver(), SORTER)
    lease = journal.claim("scan", 5, candidates=["a", "b", "c"])
    journal.conclude("scan", ["a"], token=lease.token, branch="colour")
    counts = overlay(journal)
    assert counts["scan"]["running"] == 2 and counts["quarantine"]["omitted"] == 1
    text = to_mermaid(SORTER, counts)
    assert "▶2 ✓1" in text
    assert "∅1" in text
    assert "✓0" not in text, "zeros are left out"


def test_names_are_made_safe_and_stay_unique():
    graph = (Node('a"b'), Node("a-b", parents=('a"b',)), Node("a b", parents=("a-b",)))
    text = to_mermaid(graph)
    ids = re.findall(r"^\s+(n_\w+)[\(\{]", text, flags=re.M)
    assert len(ids) == len(set(ids)) == 3
    assert "#quot;" in text
    dot = to_dot(graph)
    assert '"a\\"b" -> "a-b";' in dot


@pytest.mark.skipif(shutil.which("dot") is None, reason="graphviz is not installed")
def test_graphviz_accepts_the_dot():
    result = subprocess.run(["dot", "-Tsvg"], input=to_dot(SORTER), capture_output=True,
                            text=True, check=False)
    assert result.returncode == 0, result.stderr
