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

    journal(dag, clock, subject_type=str)
                                 a NodeJournal on EMPTY storage whose subjects
                                 are `str` or `int` (the storage's subject
                                 column typed accordingly)
    candidates(subjects)         the driver's candidates, in this order
    seed(journal, subject, progress)
                                 write rows directly, bypassing the API
    parents_concluded(journal, name, subject) -> bool
    store(dag, clock)            SHARED storage for the concurrency tests:
        .session()               opens a unit of work — `.journal`,
                                 `.candidates(subjects)`, `.commit()`,
                                 `.rollback()`; sessions may live in threads
        .close()                 drops what the store created
"""
from __future__ import annotations

import itertools
import threading
from typing import Any

import pytest

from .dag import (
    NODE_CONCLUDED,
    NODE_DONE,
    NODE_FAILED,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SATISFYING,
    NODE_SCHEDULED,
    NODE_SKIPPED,
    DagError,
    Loop,
    Node,
    accepts,
    claimable,
    claimable_nodes,
    descendants,
    joined,
    node,
    omitted_by,
)
from .timing import Retry, seconds, shift

#: A fork, a join, an optional branch — the diamond.
DIAMOND = (
    Node("start", working="starting", state="started"),
    Node("left", parents=("start",), working="lefting", state="lefted"),
    Node("right", parents=("start",), optional=True),
    Node("end", parents=("left", "right")),
)


#: An order: pay and reserve in parallel, ship on both, refund when the
#: payment went through and the reservation failed.
SAGA = (
    Node("order"),
    Node("pay", parents=("order",)),
    Node("reserve", parents=("order",)),
    Node("ship", parents=("pay", "reserve")),
    Node("refund", parents=("pay", "reserve"), on={"reserve": ("failed",)}),
)

#: Three engines, two are enough; the merge may be skipped.
QUORUM = (
    Node("scan"),
    Node("e1", parents=("scan",)),
    Node("e2", parents=("scan",)),
    Node("e3", parents=("scan",)),
    Node("merge", parents=("e1", "e2", "e3"), need=2, optional=True),
)

#: A choice between two routes, a step only one route has, a common end.
ROUTE = (
    Node("classify", choice=True),
    Node("publish", parents=("classify",)),
    Node("reject", parents=("classify",)),
    Node("notify", parents=("reject",)),
    Node("end", parents=("publish", "notify")),
)


#: A draft reviewed up to three times; past that, an escalation.
REVIEW = (
    Node("draft"),
    Node("review", parents=("draft",), loop=Loop(to="draft", max=2)),
    Node("publish", parents=("review",)),
    Node("escalate", parents=("review",), on={"review": ("failed",)}),
)


#: A call to a flaky service, retried twice with a doubling delay, then an alert.
FLAKY = (
    Node("prepare", lease="1h"),
    Node("call", parents=("prepare",), retry=Retry(limit=2, delay="10s")),
    Node("alert", parents=("call",), on={"call": ("failed",)}),
)


#: An onboarding: send, wait a week for the click, activate or remind; a
#: survey that nobody minds being skipped after a day.
ONBOARDING = (
    Node("send"),
    Node("clicked", parents=("send",), wait="email.clicked", timeout="7d"),
    Node("activate", parents=("clicked",)),
    Node("remind", parents=("clicked",), on={"clicked": ("failed",)}),
    Node("survey", parents=("send",), optional=True, grace="1d"),
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
    statuses = (NODE_RUNNING, NODE_DONE, NODE_SKIPPED, NODE_FAILED, NODE_OMITTED)
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
        lease = self._claim(harness, journal, "start", ["s1"])
        assert self._claim(harness, journal, "left", ["s1"]) == []
        journal.conclude("start", lease, token=lease.token)
        assert self._claim(harness, journal, "left", ["s1"]) == ["s1"]

    def test_a_failed_parent_satisfies_nobody(self, harness, journal):
        lease = self._claim(harness, journal, "start", ["s1"])
        assert journal.fail("start", ["s1"], token=lease.token) == 1
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
        lease = self._claim(harness, journal, "start", ["s1"])
        assert journal.conclude("start", ["s1", "s1", "ghost"], token=lease.token) == 1
        assert journal.conclude("start", ["s1"], token=lease.token) == 0, (
            "a duplicate report rewrites nothing")
        assert journal.progress("s1") == {"start": NODE_DONE}

    def test_an_unknown_status_raises(self, harness, journal):
        with pytest.raises(ValueError):
            journal.conclude("start", ["s1"], token=None, status=NODE_RUNNING)

    def test_concluding_nobody_is_zero(self, harness, journal):
        assert journal.conclude("nope", [], token=None) == 0

    # -- lease -----------------------------------------------------------------

    def test_every_claim_issues_its_own_token(self, harness, journal):
        first = self._claim(harness, journal, "start", ["s1"])
        second = self._claim(harness, journal, "start", ["s2"])
        empty = self._claim(harness, journal, "start", ["s1"])
        assert len({first.token, second.token, empty.token}) == 3

    def test_a_conclusion_needs_the_token_of_the_claim(self, harness, journal):
        first = self._claim(harness, journal, "start", ["s1"])
        second = self._claim(harness, journal, "start", ["s2"])
        assert journal.conclude("start", ["s1", "s2"], token=first.token) == 1
        assert journal.progress("s2") == {"start": NODE_RUNNING}
        assert journal.fail("start", ["s2"], token="forged") == 0
        assert journal.fail("start", ["s2"], token=second.token) == 1

    def test_a_released_lease_cannot_conclude_the_next_one(self, harness, journal, clock):
        clock.now = "2026-01-01T00:00:00+00:00"
        slow = self._claim(harness, journal, "start", ["s1"])
        clock.now = "2026-01-01T01:00:00+00:00"
        assert journal.release("start", "2026-01-01T00:30:00+00:00") == 1
        fresh = self._claim(harness, journal, "start", ["s1"])
        assert journal.conclude("start", ["s1"], token=slow.token) == 0, (
            "the slow worker came back after its lease went to another")
        assert journal.progress("s1") == {"start": NODE_RUNNING}
        assert journal.conclude("start", ["s1"], token=fresh.token) == 1

    def test_no_token_is_the_operators_override(self, harness, journal):
        self._claim(harness, journal, "start", ["s1"])
        assert journal.fail("start", ["s1"], token=None) == 1

    # -- joins as data --------------------------------------------------------

    def _run(self, harness, journal, name, subjects, **kw):
        """Claim then conclude, the way a worker does."""
        lease = self._claim(harness, journal, name, subjects)
        journal.conclude(name, lease, token=lease.token, **kw)
        return lease

    def test_a_failure_edge_claims_on_the_failure(self, harness, clock):
        journal = harness.journal(SAGA, clock)
        self._run(harness, journal, "order", ["s1", "s2"])
        self._run(harness, journal, "pay", ["s1", "s2"])
        lease = self._claim(harness, journal, "reserve", ["s1", "s2"])
        journal.fail("reserve", ["s1"], token=lease.token)
        journal.conclude("reserve", ["s2"], token=lease.token)
        assert self._claim(harness, journal, "refund", ["s1", "s2"]) == ["s1"]
        assert self._claim(harness, journal, "ship", ["s1", "s2"]) == ["s2"]

    def test_k_of_n_claims_and_skips_at_k(self, harness, clock):
        journal = harness.journal(QUORUM, clock)
        self._run(harness, journal, "scan", ["s1", "s2", "s3"])
        for engine in ("e1", "e2"):
            self._run(harness, journal, engine, ["s1", "s2"])
        self._run(harness, journal, "e1", ["s3"])
        assert sorted(self._claim(harness, journal, "merge", ["s1", "s3"])) == ["s1"]
        assert journal.skip("merge", candidates=harness.candidates(["s2", "s3"])) == 1
        assert journal.progress("s2")["merge"] == NODE_SKIPPED
        assert self._claim(harness, journal, "e3", ["s1", "s2", "s3"]) == ["s3"], (
            "s1 and s2 moved past e3; s3 has not")

    def test_a_choice_omits_the_other_route_in_the_same_write(self, harness, clock):
        journal = harness.journal(ROUTE, clock)
        lease = self._claim(harness, journal, "classify", ["s1", "s2"])
        assert journal.conclude("classify", ["s1"], token=lease.token, branch="publish") == 1
        assert journal.conclude("classify", ["s2"], token=lease.token, branch="reject") == 1
        assert journal.progress("s1") == {"classify": NODE_DONE, "reject": NODE_OMITTED,
                                          "notify": NODE_OMITTED}
        assert journal.progress("s2") == {"classify": NODE_DONE, "publish": NODE_OMITTED}
        assert self._claim(harness, journal, "reject", ["s1", "s2"]) == ["s2"]
        self._run(harness, journal, "publish", ["s1"])
        assert self._claim(harness, journal, "end", ["s1", "s2"]) == ["s1"]
        assert journal.stages(["s1"], at="2026-01-01T00:00:00+00:00")["s1"][0][0] == "classify"
        assert all(line[3] != NODE_OMITTED
                   for line in journal.stages(["s1"], at="2026-01-01T00:00:00+00:00")["s1"])

    def test_a_choice_must_name_a_branch_and_only_a_choice_may(self, harness, clock):
        journal = harness.journal(ROUTE, clock)
        lease = self._claim(harness, journal, "classify", ["s1"])
        with pytest.raises(ValueError, match="branch="):
            journal.conclude("classify", ["s1"], token=lease.token)
        with pytest.raises(DagError, match="not a branch"):
            journal.conclude("classify", ["s1"], token=lease.token, branch="end")
        with pytest.raises(ValueError, match="failed"):
            journal.fail("classify", ["s1"], token=lease.token, branch="publish")
        assert journal.fail("classify", ["s1"], token=lease.token) == 1
        assert journal.progress("s1") == {"classify": NODE_FAILED}, "a failure omits nothing"
        with pytest.raises(ValueError, match="not a choice"):
            journal.conclude("publish", ["s1"], token=None, branch="end")

    def test_a_choice_refused_by_its_token_omits_nothing(self, harness, clock):
        journal = harness.journal(ROUTE, clock)
        self._claim(harness, journal, "classify", ["s1"])
        assert journal.conclude("classify", ["s1"], token="stale", branch="publish") == 0
        assert journal.progress("s1") == {"classify": NODE_RUNNING}

    # -- history and loops ----------------------------------------------------

    def test_forget_and_release_archive_what_they_take_away(self, harness, clock):
        journal = harness.journal(DIAMOND, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        lease = self._claim(harness, journal, "start", ["s1", "s2"])
        journal.conclude("start", ["s1"], token=lease.token)
        clock.now = "2026-01-01T01:00:00+00:00"
        assert journal.release("start", "2026-01-01T00:30:00+00:00") == 1
        assert journal.forget("start", ["s1"]) == 1
        assert journal.history("s1") == [{
            "node": "start", "status": NODE_DONE, "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:00:00+00:00", "lease": lease.token,
            "archived_at": "2026-01-01T01:00:00+00:00", "reason": "forget"}]
        [released] = journal.history("s2")
        assert (released["status"], released["reason"]) == (NODE_RUNNING, "release")
        assert journal.history("ghost") == []

    def test_a_loop_goes_back_until_its_bound_then_the_failure_stands(self, harness, clock):
        journal = harness.journal(REVIEW, clock)
        for round_ in range(3):
            clock.now = f"2026-01-01T00:0{round_}:00+00:00"
            self._run(harness, journal, "draft", ["s1"])
            lease = self._claim(harness, journal, "review", ["s1"])
            assert journal.fail("review", ["s1"], token=lease.token) == 1
            if round_ < 2:
                assert journal.progress("s1") == {}, "sent back to draft"
                assert journal.passes("s1", "draft") == round_ + 1
        assert journal.progress("s1") == {"draft": NODE_DONE, "review": NODE_FAILED}
        assert self._claim(harness, journal, "escalate", ["s1"]) == ["s1"]
        reasons = [(e["node"], e["status"], e["reason"]) for e in journal.history("s1")]
        assert reasons == [("draft", NODE_DONE, "loop"), ("review", NODE_FAILED, "loop")] * 2

    def test_a_loop_does_not_fire_on_other_statuses(self, harness, clock):
        journal = harness.journal(REVIEW, clock)
        self._run(harness, journal, "draft", ["s1"])
        self._run(harness, journal, "review", ["s1"])
        assert journal.progress("s1") == {"draft": NODE_DONE, "review": NODE_DONE}
        assert journal.history("s1") == []

    def test_a_loop_refused_by_its_token_sends_nobody_back(self, harness, clock):
        journal = harness.journal(REVIEW, clock)
        self._run(harness, journal, "draft", ["s1"])
        self._claim(harness, journal, "review", ["s1"])
        assert journal.fail("review", ["s1"], token="stale") == 0
        assert journal.progress("s1") == {"draft": NODE_DONE, "review": NODE_RUNNING}
        assert journal.history("s1") == []

    # -- retries ---------------------------------------------------------------

    def test_a_failure_is_retried_when_due_then_stands(self, harness, clock):
        journal = harness.journal(FLAKY, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "prepare", ["s1"])
        lease = self._claim(harness, journal, "call", ["s1"])
        assert journal.fail("call", ["s1"], token=lease.token) == 1
        assert journal.progress("s1") == {"prepare": NODE_DONE, "call": NODE_SCHEDULED}
        assert journal.counts("call")[NODE_SCHEDULED] == 1
        assert self._claim(harness, journal, "call", ["s1"]) == [], "not due yet"
        assert self._claim(harness, journal, "alert", ["s1"]) == [], "a retry is not a failure"

        clock.now = "2026-01-01T00:00:10+00:00"
        lease = self._claim(harness, journal, "call", ["s1"])
        assert lease == ["s1"], "due after 10s"
        journal.fail("call", ["s1"], token=lease.token)
        clock.now = "2026-01-01T00:00:29+00:00"
        assert self._claim(harness, journal, "call", ["s1"]) == [], "second wait is 20s"
        clock.now = "2026-01-01T00:00:30+00:00"
        lease = self._claim(harness, journal, "call", ["s1"])
        journal.fail("call", ["s1"], token=lease.token)

        assert journal.progress("s1") == {"prepare": NODE_DONE, "call": NODE_FAILED}
        assert journal.retries("s1", "call") == 2
        assert [e["reason"] for e in journal.history("s1")] == ["retry", "retry"]
        assert self._claim(harness, journal, "alert", ["s1"]) == ["s1"]

    def test_expire_releases_what_each_node_allows_no_longer(self, harness, clock):
        journal = harness.journal(FLAKY, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._claim(harness, journal, "prepare", ["old"])
        clock.now = "2026-01-01T00:59:00+00:00"
        self._claim(harness, journal, "prepare", ["young"])
        assert journal.expire() == {}, "nothing held an hour yet"
        clock.now = "2026-01-01T01:00:01+00:00"
        assert journal.expire() == {"prepare": 1}
        assert journal.progress("old") == {}
        assert journal.progress("young") == {"prepare": NODE_RUNNING}
        assert [e["reason"] for e in journal.history("old")] == ["release"]

    def test_a_scheduled_row_closes_what_it_guards(self, harness, clock):
        journal = harness.journal(FLAKY, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "prepare", ["s1"])
        lease = self._claim(harness, journal, "call", ["s1"])
        journal.fail("call", ["s1"], token=lease.token)
        clock.now = "2026-01-01T01:00:00+00:00"
        assert journal.conclude("call", ["s1"], token=None) == 0, "a scheduled row is not held"
        assert self._claim(harness, journal, "prepare", ["s1"]) == []
        assert journal.forget("call", ["s1"]) == 1
        assert [e["status"] for e in journal.history("s1")] == [NODE_FAILED, NODE_SCHEDULED]

    def test_a_retry_refused_by_its_token_schedules_nothing(self, harness, clock):
        journal = harness.journal(FLAKY, clock)
        self._run(harness, journal, "prepare", ["s1"])
        self._claim(harness, journal, "call", ["s1"])
        assert journal.fail("call", ["s1"], token="stale") == 0
        assert journal.progress("s1")["call"] == NODE_RUNNING
        assert journal.history("s1") == []

    # -- waits, signals, grace -------------------------------------------------

    def test_a_signal_received_before_the_wait_still_settles_it(self, harness, clock):
        journal = harness.journal(ONBOARDING, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        assert journal.signal(["early"], "email.clicked", ref="click-1") == 1
        self._run(harness, journal, "send", ["early", "late"])
        with pytest.raises(ValueError, match="settled"):
            self._claim(harness, journal, "clicked", ["early"])
        assert journal.settle(harness.candidates(["early", "late"])) == {
            "clicked": {NODE_DONE: 1}}
        assert journal.progress("early")["clicked"] == NODE_DONE
        assert "clicked" not in journal.progress("late")
        assert self._claim(harness, journal, "activate", ["early", "late"]) == ["early"]
        [received] = journal.history("early")
        assert (received["node"], received["reason"], received["lease"]) == (
            "email.clicked", "signal", "click-1")

    def test_a_wait_fails_at_its_timeout_and_a_failure_edge_takes_over(self, harness, clock):
        journal = harness.journal(ONBOARDING, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "send", ["s1"])
        clock.now = "2026-01-07T23:59:59+00:00"
        assert "clicked" not in journal.settle(harness.candidates(["s1"])), "one second early"
        clock.now = "2026-01-08T00:00:00+00:00"
        assert journal.settle(harness.candidates(["s1"]))["clicked"] == {NODE_FAILED: 1}
        assert self._claim(harness, journal, "remind", ["s1"]) == ["s1"]
        journal.signal(["s1"], "email.clicked")
        assert journal.settle(harness.candidates(["s1"])).get("clicked") is None, (
            "too late: the wait has concluded")

    def test_grace_skips_an_optional_node_left_untaken(self, harness, clock):
        journal = harness.journal(ONBOARDING, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "send", ["idle", "busy"])
        self._claim(harness, journal, "survey", ["busy"])
        clock.now = "2026-01-02T00:00:00+00:00"
        assert journal.settle(harness.candidates(["idle", "busy"]))["survey"] == {
            NODE_SKIPPED: 1}
        assert journal.progress("idle")["survey"] == NODE_SKIPPED
        assert journal.progress("busy")["survey"] == NODE_RUNNING

    def test_a_signal_counts_again_only_after_the_wait_went_back(self, harness, clock):
        journal = harness.journal(ONBOARDING, clock)
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "send", ["s1"])
        journal.signal(["s1"], "email.clicked")
        clock.now = "2026-01-01T00:01:00+00:00"
        journal.settle(harness.candidates(["s1"]))
        clock.now = "2026-01-01T00:02:00+00:00"
        journal.forget("clicked", ["s1"])
        assert journal.settle(harness.candidates(["s1"])).get("clicked") is None, (
            "the old click was spent before the node went back")
        clock.now = "2026-01-01T00:03:00+00:00"
        journal.signal(["s1"], "email.clicked")
        assert journal.settle(harness.candidates(["s1"]))["clicked"] == {NODE_DONE: 1}

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
        lease = self._claim(harness, journal, "start", ["old", "done"])
        journal.conclude("start", ["done"], token=lease.token)
        clock.now = "2026-01-01T01:00:00+00:00"
        self._claim(harness, journal, "start", ["young"])
        assert journal.release("start", "2026-01-01T00:30:00+00:00") == 1
        assert journal.progress("old") == {}
        assert journal.progress("done") == {"start": NODE_DONE}
        assert journal.progress("young") == {"start": NODE_RUNNING}

    # -- read ----------------------------------------------------------------

    def test_counts_cover_every_status(self, harness, journal):
        harness.seed(journal, "a", {"start": NODE_DONE})
        harness.seed(journal, "b", {"start": NODE_DONE})
        harness.seed(journal, "c", {"start": NODE_FAILED})
        harness.seed(journal, "d", {"start": NODE_OMITTED})
        assert journal.counts("start") == {NODE_RUNNING: 0, NODE_SCHEDULED: 0, NODE_DONE: 2,
                                           NODE_SKIPPED: 0, NODE_FAILED: 1, NODE_OMITTED: 1}

    def test_stages_measure_what_worked_in_order(self, harness, journal, clock):
        clock.now = "2026-01-01T00:00:00+00:00"
        lease = self._claim(harness, journal, "start", ["s1"])
        clock.now = "2026-01-01T00:00:10+00:00"
        journal.conclude("start", ["s1"], token=lease.token)
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

    # -- the model, confronted at random ----------------------------------------

    @pytest.mark.parametrize("subject_type", [str, int], ids=["str-subjects", "int-subjects"])
    def test_the_driver_follows_the_model_on_random_graphs(self, harness, subject_type):
        """Random graphs, random sequences of claim, conclude, skip, adopt,
        forget and release; after every step the driver must hold exactly
        what a few lines of Python over `dag.claimable` say it should.

        The sweep above covers the claim rule on one graph; this covers the
        OPERATIONS, in orders nobody would think of writing — with string
        subjects and with integer subjects, the two kinds of id the journal
        promises to take as they are."""
        pytest.importorskip("hypothesis")
        from hypothesis import HealthCheck, settings
        from hypothesis.stateful import run_state_machine_as_test

        run_state_machine_as_test(
            _model_machine(harness, subject_type),
            settings=settings(max_examples=50, stateful_step_count=50,
                              deadline=None, derandomize=True,
                              suppress_health_check=list(HealthCheck)))

    # -- subject ids -----------------------------------------------------------

    def test_integer_ids_come_back_as_integers(self, harness, clock):
        """THE ID IS THE APPLICATION'S, taken and given back as it is: an
        integer never comes back as the string of its digits."""
        journal = harness.journal(FLAKY, clock, subject_type=int)
        big = 2**40
        lease = self._claim(harness, journal, "prepare", [3, big, 1])
        assert sorted(lease) == [1, 3, big]
        assert all(type(s) is int for s in lease)
        assert journal.conclude("prepare", [3, big], token=lease.token) == 2
        assert journal.progress(big) == {"prepare": NODE_DONE}
        lease = self._claim(harness, journal, "call", [big])
        assert lease == [big]
        journal.fail("call", [big], token=lease.token)
        assert journal.retries(big, "call") == 1
        assert journal.forget("prepare", [big]) == 1
        assert [e["node"] for e in journal.history(big)] == ["call", "prepare"]

    # -- concurrency -----------------------------------------------------------
    #
    # SAID IN SESSIONS, NOT IN LOCKS. A session is one unit of work on shared
    # storage: a transaction for a database, nothing at all for memory. The
    # tests below only open, act, commit — whatever a driver uses to stay
    # correct is its own business, and a driver that blocks where another
    # does not passes the same test.

    def test_a_claim_racing_a_forget_never_orphans_a_node(self, harness, clock):
        """A requeue forgets `start` and everything after it, in one session;
        a worker claims `left` meanwhile, from what it saw BEFORE the forget
        committed. Whatever the order, `left` must not end up held while
        `start`, its parent, is gone."""
        store = harness.store(DIAMOND, clock)
        try:
            setup = store.session()
            lease = setup.journal.claim("start", 1, candidates=setup.candidates(["s1"]))
            setup.journal.conclude("start", lease, token=lease.token)
            setup.commit()

            requeue = store.session()
            worker = store.session()
            for name in ("start", "left", "right", "end"):
                requeue.journal.forget(name, ["s1"])

            _race(lambda: worker.journal.claim(
                      "left", 1, candidates=worker.candidates(["s1"])),
                  worker.commit, requeue.commit)

            _assert_no_orphan(store, ["s1"])
        finally:
            store.close()

    def test_concurrent_claimers_never_take_a_subject_twice(self, harness, clock):
        subjects = [f"s{i:02d}" for i in range(40)]
        store = harness.store(DIAMOND, clock)
        try:
            taken: list[list[Any]] = [[] for _ in range(4)]

            def work(i: int) -> None:
                # Each worker walks the candidates in its own order, so that
                # claimers collide on different subjects at different times.
                order = subjects[i:] + subjects[:i] if i % 2 else subjects[::-1]
                while True:
                    session = store.session()
                    got = session.journal.claim(
                        "start", 7, candidates=session.candidates(order))
                    session.commit()
                    if not got:
                        return
                    taken[i].extend(got)

            _run_threads([lambda i=i: work(i) for i in range(4)])

            everyone = [s for batch in taken for s in batch]
            assert sorted(everyone) == subjects, "each subject taken exactly once"
        finally:
            store.close()

    def test_claims_and_requeues_interleaved_keep_every_parent(self, harness, clock):
        """A small storm: workers claim and conclude along the diamond while
        a janitor requeues subjects. At every commit, and at the end, no node
        is held without its parents."""
        subjects = [f"s{i}" for i in range(6)]
        store = harness.store(DIAMOND, clock)
        try:
            def worker(names: tuple[str, ...]) -> None:
                for _ in range(15):
                    for name in names:
                        session = store.session()
                        got = session.journal.claim(
                            name, 3, candidates=session.candidates(subjects))
                        session.journal.conclude(name, got, token=got.token)
                        session.commit()

            def janitor() -> None:
                for round_ in range(15):
                    session = store.session()
                    for name in ("start", "left", "right", "end"):
                        session.journal.forget(name, subjects[round_ % 3::3])
                    session.commit()

            _run_threads([lambda: worker(("start", "left")),
                          lambda: worker(("right", "end")),
                          lambda: worker(("left", "end", "start")),
                          janitor])

            _assert_no_orphan(store, subjects)
        finally:
            store.close()


def _model_machine(harness: Any, subject_type: type = str) -> Any:
    """A Hypothesis state machine: one random graph per run, one journal on
    it, and the MODEL — `{subject: {node: (status, started_at, lease)}}` —
    moved by the rule written as plainly as possible."""
    from hypothesis import strategies as st
    from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

    @st.composite
    def graphs(draw: Any) -> tuple[Node, ...]:
        size = draw(st.integers(min_value=1, max_value=6))
        specs: list[dict[str, Any]] = [{"name": "n0", "parents": ()}]
        for i in range(1, size):
            earlier = [spec["name"] for spec in specs]
            parents = tuple(sorted(draw(st.lists(
                st.sampled_from(earlier), min_size=1,
                max_size=min(3, len(earlier)), unique=True))))
            on = {p: tuple(draw(st.lists(st.sampled_from(NODE_CONCLUDED), min_size=1,
                                         max_size=2, unique=True)))
                  for p in parents if draw(st.integers(0, 3)) == 0}
            need = (draw(st.integers(1, len(parents)))
                    if len(parents) > 1 and draw(st.booleans()) else None)
            specs.append({"name": f"n{i}", "parents": parents, "on": on, "need": need})
        with_children = {p for spec in specs for p in spec["parents"]}
        by_name = {spec["name"]: spec for spec in specs}

        def upstream(name: str) -> list[str]:
            seen: list[str] = []
            todo = list(by_name[name]["parents"])
            while todo:
                current = todo.pop()
                if current not in seen:
                    seen.append(current)
                    todo.extend(by_name[current]["parents"])
            return sorted(seen)

        nodes = []
        for spec in specs:
            choice = spec["name"] in with_children and draw(st.integers(0, 3)) == 0
            loop = None
            if not choice and draw(st.integers(0, 3)) == 0:
                loop = Loop(to=draw(st.sampled_from([spec["name"], *upstream(spec["name"])])),
                            max=draw(st.integers(1, 2)),
                            on=tuple(draw(st.lists(st.sampled_from(
                                (NODE_DONE, NODE_SKIPPED, NODE_FAILED)),
                                min_size=1, max_size=2, unique=True))))
            retry = None
            if draw(st.integers(0, 3)) == 0:
                retry = Retry(limit=draw(st.integers(1, 2)), delay=draw(st.integers(0, 3)),
                              backoff=draw(st.sampled_from(("constant", "linear",
                                                            "exponential"))))
            wait = timeout = grace = None
            if (not choice and loop is None and retry is None and spec["parents"]
                    and draw(st.booleans())):
                wait = draw(st.sampled_from(("ev1", "ev2")))
                timeout = draw(st.one_of(st.none(), st.integers(1, 2), st.integers(1, 2)))
            optional = not choice and draw(st.booleans())
            if optional and draw(st.integers(0, 3)) > 0:
                grace = draw(st.integers(1, 2))
            nodes.append(Node(spec["name"], parents=spec["parents"],
                              on=spec.get("on", {}), need=spec.get("need"),
                              choice=choice, loop=loop, retry=retry, wait=wait,
                              timeout=timeout, grace=grace, optional=optional))
        return tuple(nodes)

    # Few subjects, so that they collide; ids given back EXACTLY as given.
    _SUBJECTS: tuple[Any, ...] = (
        ("s0", "s1", "s2", "s3") if subject_type is str else (0, 7, 42, 10**12))
    subjects = st.lists(st.sampled_from(_SUBJECTS), max_size=6)

    class Machine(RuleBasedStateMachine):
        @initialize(dag=graphs())
        def start(self, dag: tuple[Node, ...]) -> None:
            self.dag = dag
            self.clock = Clock("2026-01-01T00:00:00+00:00")
            self.ticks = 0
            self.journal = harness.journal(dag, self.clock, subject_type=subject_type)
            # (status, started_at, lease, finished_at)
            self.model: dict[str, dict[str, tuple[str, str, Any, Any]]] = {
                s: {} for s in _SUBJECTS}
            self.tokens: list[str] = []
            self.archived: dict[str, list[tuple[str, str, str, str]]] = {
                s: [] for s in _SUBJECTS}
            self.passes: dict[tuple[str, str], int] = {}
            self.retried: dict[tuple[str, str], int] = {}

        def _archive(self, s: str, name: str, reason: str) -> bool:
            row = self.model[s].pop(name, None)
            if row is None:
                return False
            self.archived[s].append((self.clock.now, name, row[0], reason))
            return True

        def _tick(self) -> None:
            self.ticks += 1
            self.clock.now = f"2026-01-01T00:{self.ticks // 60:02d}:{self.ticks % 60:02d}+00:00"

        def _node(self, data: Any) -> Node:
            return data.draw(st.sampled_from(self.dag))

        def _statuses(self, subject: str) -> dict[str, str]:
            return {n: row[0] for n, row in self.model[subject].items()}

        @rule(data=st.data(), candidates=subjects,
              limit=st.integers(min_value=0, max_value=4))
        def claim(self, data: Any, candidates: list[str], limit: int) -> None:
            self._claim(data, candidates, limit)

        def _claim(self, data: Any, candidates: list[str], limit: int,
                   node: Node | None = None) -> Any:
            n = node or self._node(data)
            self._tick()
            if n.wait is not None:
                with pytest.raises(ValueError):
                    self.journal.claim(n.name, limit, candidates=harness.candidates(candidates))
                return None
            got = self.journal.claim(n.name, limit, candidates=harness.candidates(candidates))
            assert got.token not in self.tokens, "a token is never issued twice"
            self.tokens.append(got.token)
            expected: list[str] = []
            for s in candidates:
                statuses = self._statuses(s)
                row = self.model[s].get(n.name)
                if row and row[0] == NODE_SCHEDULED and row[1] <= self.clock.now:
                    del statuses[n.name]          # due: as if absent
                if len(expected) < limit and claimable(n.name, self.dag, statuses):
                    if row and row[0] == NODE_SCHEDULED:
                        del self.model[s][n.name]   # replaced, not archived
                    self.model[s][n.name] = (NODE_RUNNING, self.clock.now, got.token, None)
                    expected.append(s)
            assert sorted(got) == sorted(expected), f"claim {n.name} {candidates}"
            return got

        @rule(data=st.data(), candidates=subjects,
              status=st.sampled_from((NODE_DONE, NODE_SKIPPED, NODE_FAILED)))
        def conclude(self, data: Any, candidates: list[str], status: str,
                     node: Node | None = None, token: Any = "draw") -> None:
            n = node or self._node(data)
            if token == "draw":
                token = data.draw(st.sampled_from([None, "forged", *self.tokens]))
            branch = None
            omit: tuple[str, ...] = ()
            if n.choice and status != NODE_FAILED:
                branch = data.draw(st.sampled_from(
                    [c.name for c in self.dag if n.name in c.parents]))
                omit = omitted_by(n.name, branch, self.dag)
            self._tick()
            expected = 0
            for s in dict.fromkeys(candidates):
                row = self.model[s].get(n.name)
                if row and row[0] == NODE_RUNNING and token in (None, row[2]):
                    self.model[s][n.name] = (status, row[1], row[2], self.clock.now)
                    retries = self.retried.get((s, n.name), 0)
                    if (n.retry is not None and status == NODE_FAILED
                            and retries < n.retry.limit):
                        self._archive(s, n.name, "retry")
                        due = shift(self.clock.now, n.retry.wait(retries + 1))
                        self.model[s][n.name] = (NODE_SCHEDULED, due, None, None)
                        self.retried[(s, n.name)] = retries + 1
                        expected += 1
                        continue
                    for other in omit:
                        self.model[s].setdefault(
                            other, (NODE_OMITTED, self.clock.now, None, self.clock.now))
                    loop = n.loop
                    if (loop is not None and status in loop.on
                            and self.passes.get((s, loop.to), 0) < loop.max):
                        for other in (loop.to, *sorted(descendants(loop.to, self.dag))):
                            if other == loop.to and other in self.model[s]:
                                self.passes[(s, loop.to)] = self.passes.get((s, loop.to), 0) + 1
                            self._archive(s, other, "loop")
                    expected += 1
            got = self.journal.conclude(n.name, candidates, token=token, status=status,
                                        branch=branch)
            assert got == expected, f"conclude {n.name} {candidates} token={token}"

        @rule(data=st.data())
        def advance(self, data: Any) -> None:
            """A worker's full turn on one node — claim then conclude done —
            so that sequences reach deep states (joins, waits, timeouts)."""
            workable = [x for x in self.dag if x.wait is None]
            if not workable:
                return
            n = data.draw(st.sampled_from(workable))
            got = self._claim(data, list(_SUBJECTS), 4, node=n)
            if got:
                self.conclude(data, list(got), NODE_DONE, node=n, token=got.token)

        @rule(data=st.data(), candidates=subjects)
        def skip(self, data: Any, candidates: list[str]) -> None:
            n = self._node(data)
            if not n.optional:
                return
            self._tick()
            expected = 0
            for s in dict.fromkeys(candidates):
                statuses = self._statuses(s)
                if n.name not in statuses and joined(n.name, self.dag, statuses):
                    self.model[s][n.name] = (NODE_SKIPPED, self.clock.now, None, self.clock.now)
                    expected += 1
            got = self.journal.skip(n.name, candidates=harness.candidates(candidates))
            assert got == expected, f"skip {n.name} {candidates}"

        @rule(data=st.data(), candidates=subjects)
        def adopt(self, data: Any, candidates: list[str]) -> None:
            n = self._node(data)
            self._tick()
            expected = 0
            for s in dict.fromkeys(candidates):
                if n.name not in self.model[s]:
                    self.model[s][n.name] = (NODE_DONE, self.clock.now, None, self.clock.now)
                    expected += 1
            assert self.journal.adopt(n.name, candidates) == expected

        @rule(candidates=subjects, event=st.sampled_from(("ev1", "ev2")))
        def signal(self, candidates: list[Any], event: str) -> None:
            self._tick()
            unique = list(dict.fromkeys(candidates))
            for s in unique:
                self.archived[s].append((self.clock.now, event, "received", "signal"))
            assert self.journal.signal(candidates, event) == len(unique)

        @rule(candidates=subjects, wait=st.integers(0, 5))
        def settle(self, candidates: list[Any], wait: int) -> None:
            for _ in range(wait):
                self._tick()
            now = self.clock.now
            expected: dict[str, dict[str, int]] = {}
            for n in self.dag:
                if n.wait is None and n.grace is None:
                    continue
                for s in dict.fromkeys(candidates):
                    statuses = self._statuses(s)
                    row = self.model[s].get(n.name)
                    if row and row[0] == NODE_SCHEDULED and row[1] <= now:
                        del statuses[n.name]
                    if not claimable(n.name, self.dag, statuses):
                        continue
                    times = [self.model[s][p][3] for p in n.parents
                             if statuses.get(p) in accepts(n, p)
                             and self.model[s][p][3] is not None]
                    since = max(times) if times else None
                    status = None
                    if n.wait is not None:
                        heard = [a for a, name, _, reason in self.archived[s]
                                 if name == n.wait and reason == "signal"]
                        back = [a for a, name, _, _ in self.archived[s] if name == n.name]
                        if heard and max(heard) >= max(back, default=""):
                            status = NODE_DONE
                        elif (n.timeout is not None and since is not None
                              and shift(since, seconds(n.timeout)) <= now):
                            status = NODE_FAILED
                    elif (n.grace is not None and since is not None
                          and shift(since, seconds(n.grace)) <= now):
                        status = NODE_SKIPPED
                    if status is None:
                        continue
                    self.model[s][n.name] = (status, now, None, now)
                    counts = expected.setdefault(n.name, {})
                    counts[status] = counts.get(status, 0) + 1
            assert self.journal.settle(harness.candidates(candidates)) == expected

        @rule(data=st.data(), candidates=subjects)
        def forget(self, data: Any, candidates: list[str]) -> None:
            n = self._node(data)
            self._tick()
            expected = sum(1 for s in dict.fromkeys(candidates)
                           if self._archive(s, n.name, "forget"))
            assert self.journal.forget(n.name, candidates) == expected

        @rule(data=st.data(), back=st.integers(min_value=0, max_value=5))
        def release(self, data: Any, back: int) -> None:
            n = self._node(data)
            older = max(self.ticks - back, 0)
            older_than = f"2026-01-01T00:{older // 60:02d}:{older % 60:02d}+00:00"
            self._tick()
            expected = 0
            for s in _SUBJECTS:
                row = self.model[s].get(n.name)
                if row and row[0] == NODE_RUNNING and row[1] < older_than:
                    self._archive(s, n.name, "release")
                    expected += 1
            assert self.journal.release(n.name, older_than) == expected

        @invariant()
        def the_journal_holds_the_model(self) -> None:
            if not hasattr(self, "journal"):
                return
            for s in _SUBJECTS:
                assert self.journal.progress(s) == self._statuses(s), s
                expected = [(name, status, reason) for _, name, status, reason
                            in sorted(self.archived[s], key=lambda a: (a[0], a[1]))]
                got = [(e["node"], e["status"], e["reason"]) for e in self.journal.history(s)]
                assert got == expected, f"history of {s}"

    return Machine


def _race(claim: Any, commit_claim: Any, commit_other: Any) -> None:
    """Run `claim` then `commit_claim` in a thread while the other session is
    still open. If the thread is still busy after a moment — the driver made
    it wait — commit the other session to let it through."""
    errors: list[BaseException] = []

    def run() -> None:
        try:
            claim()
            commit_claim()
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    thread = threading.Thread(target=run)
    thread.start()
    thread.join(timeout=0.5)
    commit_other()
    thread.join(timeout=30)
    assert not thread.is_alive(), "the claim never returned"
    if errors:
        raise errors[0]


def _run_threads(targets: list[Any]) -> None:
    errors: list[BaseException] = []

    def guard(target: Any) -> None:
        try:
            target()
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=guard, args=(t,)) for t in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=120)
    assert not any(t.is_alive() for t in threads), "a thread never returned"
    if errors:
        raise errors[0]


def _assert_no_orphan(store: Any, subjects: list[Any]) -> None:
    """No row of a node while one of its parents is not `done` or `skipped`."""
    session = store.session()
    try:
        for subject in subjects:
            progress = session.journal.progress(subject)
            for name in progress:
                missing = [p for p in node(name, DIAMOND).parents
                           if progress.get(p) not in NODE_SATISFYING]
                assert not missing, (
                    f"{subject}: {name} is {progress[name]} but its parent(s) "
                    f"{missing} are {[progress.get(p) for p in missing]}")
    finally:
        session.rollback()
