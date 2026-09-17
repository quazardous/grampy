"""THE DRIVER CONTRACT — one suite, every driver.

A driver is a second expression of the rule in `grampy.dag`. Two
expressions of one rule drift apart unless something confronts them, so
every driver runs the same tests, including a sweep of progresses — some
IMPOSSIBLE through the API — where the driver's claim must answer exactly
what `claimable_nodes` answers. That is where a forgotten `AND` would
never show otherwise.

Usage, in a test module (pytest is only needed here):

    class TestMyDriver(JournalContract):
        @pytest.fixture
        def harness(self):
            return MyHarness()

A harness provides:

    journal(dag, clock)          a NodeJournal on EMPTY storage
    candidates(subjects)         the driver's candidates, in this order
    seed(journal, subject, progress)
                                 write rows directly, bypassing the API
    parents_concluded(journal, name, subject) -> bool
"""
from __future__ import annotations

import itertools
from typing import Any

import pytest

from .dag import (
    NODE_DONE,
    NODE_FAILED,
    NODE_RUNNING,
    NODE_SKIPPED,
    DagError,
    Node,
    claimable_nodes,
)

#: A fork, a join, an optional branch — the diamond.
DIAMOND = (
    Node("start", working="starting", state="started"),
    Node("left", parents=("start",), working="lefting", state="lefted"),
    Node("right", parents=("start",), optional=True),
    Node("end", parents=("left", "right")),
)


class Clock:
    """A clock that only moves when told to."""

    def __init__(self, now: str = "2026-01-01T00:00:00+00:00") -> None:
        self.now = now

    def __call__(self) -> str:
        return self.now


def _progresses() -> list[dict[str, str]]:
    """Every progress of the diamond with at most two rows, plus a few
    deeper ones — many of them unreachable through the API."""
    statuses = (NODE_RUNNING, NODE_DONE, NODE_SKIPPED, NODE_FAILED)
    names = [n.name for n in DIAMOND]
    out: list[dict[str, str]] = [{}]
    for size in (1, 2):
        for chosen in itertools.combinations(names, size):
            for values in itertools.product(statuses, repeat=size):
                out.append(dict(zip(chosen, values, strict=True)))
    out += [{"start": NODE_DONE, "left": NODE_DONE, "right": NODE_SKIPPED},
            {"start": NODE_DONE, "left": NODE_DONE, "right": NODE_FAILED},
            {"start": NODE_DONE, "right": NODE_DONE, "end": NODE_RUNNING}]
    return out


class JournalContract:
    """The behaviour every `JournalDriver` must have. Subclass it and
    provide a `harness` fixture."""

    @pytest.fixture
    def clock(self) -> Clock:
        return Clock()

    @pytest.fixture
    def journal(self, harness: Any, clock: Clock) -> Any:
        return harness.journal(DIAMOND, clock)

    def _claim(self, harness, journal, name, subjects, limit=10, **kw):
        return journal.claim(name, limit, candidates=harness.candidates(subjects), **kw)

    # -- the rule, confronted ---------------------------------------------

    @pytest.mark.parametrize(
        "progress", _progresses(),
        ids=lambda p: ",".join(f"{k}={v}" for k, v in sorted(p.items())) or "empty")
    def test_the_driver_claims_exactly_what_the_rule_says(self, harness, clock, progress):
        expected = claimable_nodes(DIAMOND, progress)
        got = set()
        for n in DIAMOND:
            journal = harness.journal(DIAMOND, clock)
            harness.seed(journal, "s1", progress)
            if self._claim(harness, journal, n.name, ["s1"]):
                got.add(n.name)
        assert got == expected, f"{progress}: driver={sorted(got)} rule={sorted(expected)}"

    # -- claim ---------------------------------------------------------------

    def test_a_claim_takes_candidates_in_order_up_to_the_limit(self, harness, journal):
        # WHICH subjects are taken follows the candidates' order; the order of
        # the returned list is not part of the contract.
        taken = self._claim(harness, journal, "start", ["b", "a", "c"], limit=2)
        assert sorted(taken) == ["a", "b"]
        assert journal.progress("b") == {"start": NODE_RUNNING}
        assert journal.progress("c") == {}

    def test_a_held_node_is_not_taken_twice(self, harness, journal):
        assert self._claim(harness, journal, "start", ["s1"]) == ["s1"]
        assert self._claim(harness, journal, "start", ["s1"]) == []

    def test_a_child_waits_for_its_parent_to_conclude(self, harness, journal):
        self._claim(harness, journal, "start", ["s1"])
        assert self._claim(harness, journal, "left", ["s1"]) == []
        journal.conclude("start", ["s1"])
        assert self._claim(harness, journal, "left", ["s1"]) == ["s1"]

    def test_a_failed_parent_satisfies_nobody(self, harness, journal):
        self._claim(harness, journal, "start", ["s1"])
        assert journal.fail("start", ["s1"]) == 1
        assert self._claim(harness, journal, "left", ["s1"]) == []

    def test_a_started_descendant_closes_the_node(self, harness, journal):
        harness.seed(journal, "s1", {"start": NODE_DONE, "end": NODE_RUNNING})
        assert self._claim(harness, journal, "left", ["s1"]) == []

    def test_lifting_parents_keeps_the_two_other_guards(self, harness, journal):
        assert self._claim(harness, journal, "left", ["s1"], require_parents=False) == ["s1"]
        assert self._claim(harness, journal, "left", ["s1"], require_parents=False) == []
        harness.seed(journal, "s2", {"end": NODE_DONE})
        assert self._claim(harness, journal, "left", ["s2"], require_parents=False) == []

    def test_an_unknown_node_raises(self, harness, journal):
        with pytest.raises(DagError):
            self._claim(harness, journal, "nope", ["s1"])

    def test_no_candidate_takes_nothing(self, harness, journal):
        assert self._claim(harness, journal, "start", []) == []

    # -- conclude ------------------------------------------------------------

    def test_only_running_rows_are_concluded(self, harness, journal):
        self._claim(harness, journal, "start", ["s1"])
        assert journal.conclude("start", ["s1", "s1", "ghost"]) == 1
        assert journal.conclude("start", ["s1"]) == 0, "a duplicate report rewrites nothing"
        assert journal.progress("s1") == {"start": NODE_DONE}

    def test_an_unknown_status_raises(self, harness, journal):
        with pytest.raises(ValueError):
            journal.conclude("start", ["s1"], status=NODE_RUNNING)

    def test_concluding_nobody_is_zero(self, harness, journal):
        assert journal.conclude("nope", []) == 0

    # -- skip ----------------------------------------------------------------

    def test_only_an_optional_node_is_skipped(self, harness, journal):
        with pytest.raises(ValueError):
            journal.skip("left", candidates=harness.candidates(["s1"]))

    def test_a_skip_needs_concluded_parents_and_satisfies_children(self, harness, journal):
        harness.seed(journal, "s1", {"start": NODE_RUNNING})
        harness.seed(journal, "s2", {"start": NODE_DONE, "left": NODE_DONE})
        assert journal.skip("right", candidates=harness.candidates(["s1", "s2"])) == 1
        assert journal.progress("s2")["right"] == NODE_SKIPPED
        assert "right" not in journal.progress("s1")
        assert self._claim(harness, journal, "end", ["s2"]) == ["s2"]

    def test_a_skip_never_overwrites(self, harness, journal):
        harness.seed(journal, "s1", {"start": NODE_DONE, "right": NODE_FAILED})
        assert journal.skip("right", candidates=harness.candidates(["s1"])) == 0
        assert journal.progress("s1")["right"] == NODE_FAILED

    # -- adopt / forget / release -------------------------------------------

    def test_adopt_records_done_and_never_overwrites(self, harness, journal):
        harness.seed(journal, "s2", {"start": NODE_FAILED})
        assert journal.adopt("start", ["s1", "s1", "s2"]) == 1
        assert journal.progress("s1") == {"start": NODE_DONE}
        assert journal.progress("s2") == {"start": NODE_FAILED}
        assert journal.adopt("start", []) == 0
        with pytest.raises(DagError):
            journal.adopt("nope", [])

    def test_forget_makes_the_node_claimable_again(self, harness, journal):
        self._claim(harness, journal, "start", ["s1"])
        assert journal.forget("start", ["s1", "s2"]) == 1
        assert journal.progress("s1") == {}
        assert self._claim(harness, journal, "start", ["s1"]) == ["s1"]
        assert journal.forget("start", []) == 0

    def test_release_drops_only_old_running_leases(self, harness, journal, clock):
        clock.now = "2026-01-01T00:00:00+00:00"
        self._claim(harness, journal, "start", ["old", "done"])
        journal.conclude("start", ["done"])
        clock.now = "2026-01-01T01:00:00+00:00"
        self._claim(harness, journal, "start", ["young"])
        assert journal.release("start", "2026-01-01T00:30:00+00:00") == 1
        assert journal.progress("old") == {}
        assert journal.progress("done") == {"start": NODE_DONE}
        assert journal.progress("young") == {"start": NODE_RUNNING}

    # -- read ----------------------------------------------------------------

    def test_counts_cover_the_four_statuses(self, harness, journal):
        harness.seed(journal, "a", {"start": NODE_DONE})
        harness.seed(journal, "b", {"start": NODE_DONE})
        harness.seed(journal, "c", {"start": NODE_FAILED})
        assert journal.counts("start") == {NODE_RUNNING: 0, NODE_DONE: 2,
                                           NODE_SKIPPED: 0, NODE_FAILED: 1}

    def test_stages_measure_what_worked_in_order(self, harness, journal, clock):
        clock.now = "2026-01-01T00:00:00+00:00"
        self._claim(harness, journal, "start", ["s1"])
        clock.now = "2026-01-01T00:00:10+00:00"
        journal.conclude("start", ["s1"])
        journal.skip("right", candidates=harness.candidates(["s1"]))
        self._claim(harness, journal, "left", ["s1"])
        stages = journal.stages(["s1", "ghost"], at="2026-01-01T00:00:15+00:00")
        assert list(stages) == ["s1"]
        assert [[n, e, float(s), st] for n, e, s, st in stages["s1"]] == [
            ["start", "2026-01-01T00:00:10+00:00", 10.0, NODE_DONE],
            ["left", "2026-01-01T00:00:15+00:00", 5.0, NODE_RUNNING],
        ]

    def test_parents_concluded_reads_like_the_rule(self, harness, journal):
        harness.seed(journal, "s1", {"left": NODE_DONE, "right": NODE_SKIPPED})
        harness.seed(journal, "s2", {"left": NODE_DONE, "right": NODE_FAILED})
        assert harness.parents_concluded(journal, "end", "s1") is True
        assert harness.parents_concluded(journal, "end", "s2") is False
        assert harness.parents_concluded(journal, "start", "s3") is True, (
            "no parent, nothing required")

    def test_node_for_state_names_the_working_node(self, harness, journal):
        assert journal.node_for_state("lefting") == "left"
        assert journal.node_for_state("lefted") is None
