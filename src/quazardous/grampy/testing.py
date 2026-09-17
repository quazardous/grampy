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
    journal_on(journal, dag, clock)
                                 a second NodeJournal on the SAME storage as
                                 `journal`, for another graph (versions)
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
import json
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
    Lane,
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
from .graph import Document, Graph
from .journal import MigrationError
from .timing import Rate, Retry, seconds, shift

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


#: Listings that come back: each new version waits in a lane, an hour at
#: least after the last pass ended, then is scraped and published.
LISTING = (
    Node("arrive", lane=Lane.throttle(cooldown="1h")),
    Node("scrape", parents=("arrive",)),
    Node("publish", parents=("scrape",)),
)

T0 = "2026-01-01T00:00:00+00:00"


def _at(minutes: float) -> str:
    return shift(T0, minutes * 60)


def _listing(**lane: Any) -> tuple[Node, ...]:
    return (Node("arrive", lane=Lane(**lane)), *LISTING[1:])


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

    # -- policies ----------------------------------------------------------------

    def _policied(self):
        """ONBOARDING and FLAKY in one graph, where `slow` waits longer, retries
        more and gets a longer lease than the defaults."""
        nodes = (
            Node("send"),
            Node("clicked", parents=("send",), wait="email.clicked", timeout="7d"),
            Node("call", parents=("send",), retry=Retry(limit=1, delay="10s"), lease="1h"),
            Node("survey", parents=("send",), optional=True, grace="1d"),
        )
        return Graph(Document("policies"), nodes, policies={"slow": {
            "clicked": {"timeout": "30d"},
            "call": {"retry": Retry(limit=3, delay="1m"), "lease": "5h"},
            "survey": {"grace": "10d"},
        }})

    def test_a_subject_carries_its_policy(self, harness, clock):
        journal = harness.journal(self._policied(), clock)
        assert journal.enroll(["a", "b"], "slow") == 2
        assert journal.policy("a") == "slow"
        assert journal.policy("c") is None
        journal.enroll(["b"], None)
        assert journal.policy("b") is None
        assert journal.settings("call", "slow").lease == "5h"
        assert journal.settings("call", None).lease == "1h"

    def test_a_source_that_needs_an_extra_step_skips_it_elsewhere(self, harness, clock):
        """THE DOCUMENTED WAY to give one source a step the others do not
        take (docs/rules.md): the node is optional for everyone, and the
        policy that does not want it gives it a grace, so `settle` skips
        it — `skipped` satisfies what follows."""
        graph = Graph(Document("offers"), (
            Node("scrape"),
            Node("enrich", parents=("scrape",), optional=True),
            Node("publish", parents=("enrich",)),
        ), policies={"plain": {"enrich": {"grace": "1s"}}})
        journal = harness.journal(graph, clock)
        journal.enroll(["plain1"], "plain")
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "scrape", ["rich1", "plain1"])

        clock.now = "2026-01-01T00:00:05+00:00"
        assert journal.settle(harness.candidates(["rich1", "plain1"])) == {
            "enrich": {NODE_SKIPPED: 1}}, "only the policy with a grace"
        assert journal.progress("plain1")["enrich"] == NODE_SKIPPED
        assert "enrich" not in journal.progress("rich1"), "no grace: never skipped alone"

        # What follows goes on for the skipped one, and waits for the other.
        assert self._claim(harness, journal, "publish", ["plain1", "rich1"]) == ["plain1"]
        self._run(harness, journal, "enrich", ["rich1"])
        assert self._claim(harness, journal, "publish", ["rich1"]) == ["rich1"]

    def test_a_policy_changes_retries_leases_timeouts_and_graces(self, harness, clock):
        journal = harness.journal(self._policied(), clock)
        journal.enroll(["slow"], "slow")
        clock.now = "2026-01-01T00:00:00+00:00"
        self._run(harness, journal, "send", ["slow", "fast"])

        # retries: one for the default, three for `slow`
        for _ in range(2):
            lease = self._claim(harness, journal, "call", ["slow", "fast"])
            journal.fail("call", list(lease), token=lease.token)
            clock.now = shift(clock.now, 3600)
        assert journal.progress("fast")["call"] == NODE_FAILED
        assert journal.progress("slow")["call"] == NODE_SCHEDULED

        # leases: 1h by default, 5h for `slow`
        lease = self._claim(harness, journal, "call", ["slow"])
        assert lease == ["slow"]
        clock.now = shift(clock.now, 2 * 3600)
        assert journal.expire() == {}, "two hours is within the `slow` lease"
        clock.now = shift(clock.now, 4 * 3600)
        assert journal.expire() == {"call": 1}

        # timeouts and graces, measured from `send` concluding
        clock.now = "2026-01-08T00:00:00+00:00"
        settled = journal.settle(harness.candidates(["slow", "fast"]))
        assert settled["clicked"] == {NODE_FAILED: 1}
        assert settled["survey"] == {NODE_SKIPPED: 1}
        assert "clicked" not in journal.progress("slow")
        assert "survey" not in journal.progress("slow")

    # -- versions and migration --------------------------------------------------

    V1 = Graph(Document("line", version="1"), (
        Node("fetch"),
        Node("crop", parents=("fetch",)),
        Node("thumb", parents=("crop",)),
        Node("publish", parents=("thumb",)),
    ))
    #: v2 inserts `watermark` between `thumb` and `publish`, renames `crop` to
    #: `trim`, and drops nothing.
    V2 = Graph(Document("line", version="2"), (
        Node("fetch"),
        Node("trim", parents=("fetch",)),
        Node("thumb", parents=("trim",)),
        Node("watermark", parents=("thumb",)),
        Node("publish", parents=("watermark",)),
    ))

    def test_subjects_stay_on_the_version_they_started_on(self, harness, clock):
        v1 = harness.journal(self.V1, clock)
        v2 = harness.journal_on(v1, self.V2, clock)
        self._run(harness, v1, "fetch", ["old"])
        assert v1.pinned("old") == self.V1.document.identity
        assert self._claim(harness, v2, "fetch", ["old", "new"]) == ["new"]
        assert v2.pinned("new") == self.V2.document.identity
        assert self._claim(harness, v1, "crop", ["old", "new"]) == ["old"], (
            "v1 never touches a subject of v2")

    #: TWO WORKFLOWS, NOT TWO VERSIONS OF ONE: same version string, same node
    #: names, different documents. Nothing may leak between them.
    OFFERS = Graph(Document("offers", version="1"), (
        Node("fetch"), Node("publish", parents=("fetch",))))
    INVOICES = Graph(Document("invoices", version="1"), (
        Node("fetch"), Node("publish", parents=("fetch",))))

    def test_two_graphs_sharing_a_version_string_stay_strangers(self, harness, clock):
        """THE PIN IS THE WHOLE DOCUMENT, not the version alone. Were it the
        version, these two would each think the other's subjects were theirs."""
        offers = harness.journal(self.OFFERS, clock)
        invoices = harness.journal_on(offers, self.INVOICES, clock)
        self._run(harness, offers, "fetch", ["s1"])
        assert offers.pinned("s1") == "default/offers@1"

        assert self._claim(harness, invoices, "fetch", ["s1"]) == [], (
            "a subject of `offers` is none of `invoices`' business")
        assert invoices.progress("s1") == {}, "nor does it see its rows"
        assert self._claim(harness, offers, "publish", ["s1"]) == ["s1"], (
            "and `offers` still owns it")

    def test_a_compliant_subject_migrates_renamed_and_repinned(self, harness, clock):
        v1 = harness.journal(self.V1, clock)
        v2 = harness.journal_on(v1, self.V2, clock)
        self._run(harness, v1, "fetch", ["s1"])
        self._run(harness, v1, "crop", ["s1"])
        self._run(harness, v1, "thumb", ["s1"])
        assert v2.migrate(["s1"], self.V1, {"crop": "trim"}) == 1
        assert v2.pinned("s1") == self.V2.document.identity
        assert v2.progress("s1") == {"fetch": NODE_DONE, "trim": NODE_DONE, "thumb": NODE_DONE}
        assert self._claim(harness, v2, "watermark", ["s1"]) == ["s1"], (
            "the new node is next, as if the subject had started on v2")

    def test_migration_is_all_or_nothing(self, harness, clock):
        v1 = harness.journal(self.V1, clock)
        v2 = harness.journal_on(v1, self.V2, clock)
        for subject in ("early", "late"):
            for name in ("fetch", "crop", "thumb"):
                self._run(harness, v1, name, [subject])
        self._run(harness, v1, "publish", ["late"])
        with pytest.raises(MigrationError) as caught:
            v2.migrate(["early", "late"], self.V1, {"crop": "trim"})
        assert set(caught.value.problems) == {"late"}
        assert "publish" in caught.value.problems["late"]
        assert v2.pinned("early") == self.V1.document.identity, (
            "the compliant one did not move either")
        assert v1.progress("early")["crop"] == NODE_DONE

    def test_a_dropped_node_is_archived_unless_someone_holds_it(self, harness, clock):
        v1 = harness.journal(self.V1, clock)
        v3 = harness.journal_on(v1, Graph(Document("line", version="3"), (
            Node("fetch"), Node("thumb", parents=("fetch",)),
            Node("publish", parents=("thumb",)))), clock)
        self._run(harness, v1, "fetch", ["done", "held"])
        self._run(harness, v1, "crop", ["done"])
        self._claim(harness, v1, "crop", ["held"])
        with pytest.raises(MigrationError, match="held"):
            v3.migrate(["done", "held"], self.V1, {"crop": None})
        assert v3.migrate(["done"], self.V1, {"crop": None}) == 1
        assert v3.progress("done") == {"fetch": NODE_DONE}
        assert [(e["node"], e["reason"]) for e in v3.history("done")] == [("crop", "migrate")]

    def test_a_mapping_that_cannot_hold_is_refused(self, harness, clock):
        v1 = harness.journal(self.V1, clock)
        v2 = harness.journal_on(v1, self.V2, clock)
        with pytest.raises(ValueError, match="nowhere to go"):
            v2.migrate(["s1"], self.V1)
        with pytest.raises(ValueError, match="same node"):
            v2.migrate(["s1"], self.V1, {"crop": "trim", "fetch": "trim"})
        with pytest.raises(ValueError, match="does not have"):
            v2.migrate(["s1"], self.V1, {"ghost": "trim", "crop": "trim"})

    # -- rate and concurrency ------------------------------------------------------

    def test_concurrency_caps_what_runs_at_once(self, harness, clock):
        journal = harness.journal((Node("gpu", concurrency=2),), clock)
        first = self._claim(harness, journal, "gpu", ["a", "b", "c", "d"])
        assert len(first) == 2
        assert self._claim(harness, journal, "gpu", ["a", "b", "c", "d"]) == []
        journal.conclude("gpu", [first[0]], token=first.token)
        assert len(self._claim(harness, journal, "gpu", ["a", "b", "c", "d"])) == 1

    def test_rate_bands_let_through_their_burst_then_their_pace(self, harness, clock):
        journal = harness.journal((Node("call", rate=(Rate(3, "1m"), Rate(4, "1h"))),), clock)
        subjects = [f"s{i}" for i in range(10)]
        clock.now = "2026-01-01T00:00:00+00:00"
        assert len(self._claim(harness, journal, "call", subjects)) == 3
        assert self._claim(harness, journal, "call", subjects) == []
        clock.now = "2026-01-01T00:00:20+00:00"
        assert len(self._claim(harness, journal, "call", subjects)) == 1, "20 s buys one"
        clock.now = "2026-01-01T00:05:00+00:00"
        assert self._claim(harness, journal, "call", subjects) == [], (
            "the minute band is full again, the hour band is spent")

    def test_per_policy_gives_each_policy_its_own_budget(self, harness, clock):
        graph = Graph(Document("api"), (Node("call", concurrency=1, per="policy"),),
                      policies={"big": {"call": {"concurrency": 3}}})
        journal = harness.journal(graph, clock)
        journal.enroll(["b1", "b2", "b3", "b4"], "big")
        journal.enroll(["s1", "s2"], "small")
        taken = self._claim(harness, journal, "call", ["b1", "b2", "b3", "b4", "s1", "s2", "x"])
        assert sorted(taken, key=str) == ["b1", "b2", "b3", "s1", "x"], (
            "3 for big, 1 for small, 1 for the subjects without a policy")

    def test_a_claim_racing_another_never_exceeds_the_concurrency(self, harness, clock):
        """One claim takes the whole budget and has not committed yet; a second
        claim runs meanwhile. Whatever the storage blocks on, the second must
        not count the budget as free."""
        store = harness.store((Node("gpu", concurrency=3),), clock)
        subjects = [f"s{i:02d}" for i in range(10)]
        try:
            first = store.session()
            second = store.session()
            mine, theirs = first.candidates(subjects[:5]), second.candidates(subjects[5:])
            assert len(first.journal.claim("gpu", 3, candidates=mine)) == 3
            _race(lambda: second.journal.claim("gpu", 3, candidates=theirs),
                  second.commit, first.commit)
            check = store.session()
            try:
                assert check.journal.counts("gpu")["running"] == 3
            finally:
                check.rollback()
        finally:
            store.close()

    def test_concurrent_claimers_never_exceed_the_concurrency(self, harness, clock):
        store = harness.store((Node("gpu", concurrency=3),), clock)
        subjects = [f"s{i:02d}" for i in range(30)]
        try:
            def work(i: int) -> None:
                for _ in range(8):
                    session = store.session()
                    session.journal.claim("gpu", 2, candidates=session.candidates(subjects))
                    session.commit()

            _run_threads([lambda i=i: work(i) for i in range(4)])
            check = store.session()
            try:
                assert check.journal.counts("gpu")["running"] == 3
            finally:
                check.rollback()
        finally:
            store.close()

    # -- lanes -----------------------------------------------------------------

    def _settle(self, harness, journal, subjects):
        return journal.settle(harness.candidates(subjects)).get("arrive", {}).get("entered", 0)

    def _through(self, harness, journal, subjects):
        """One whole pass after the lane: scrape, then publish."""
        self._run(harness, journal, "scrape", subjects)
        self._run(harness, journal, "publish", subjects)

    def test_an_arrival_waits_in_its_lane_until_settled(self, harness, clock):
        journal = harness.journal(LISTING, clock)
        clock.now = T0
        assert journal.arrive("arrive", ["s1"], ref="v1") == {
            "queued": 1, "merged": 0, "skipped": 0}
        assert journal.progress("s1") == {}
        assert self._claim(harness, journal, "scrape", ["s1"]) == []
        assert journal.counts("arrive")["waiting"] == 1
        assert journal.arrival("s1", "arrive").ref == "v1"
        assert self._settle(harness, journal, ["s1"]) == 1
        assert journal.progress("s1") == {"arrive": NODE_DONE}
        assert journal.arrival("s1", "arrive") is None
        assert journal.counts("arrive")["waiting"] == 0
        assert self._claim(harness, journal, "scrape", ["s1"]) == ["s1"]
        [entered] = journal.history("s1")
        assert (entered["node"], entered["status"], entered["lease"], entered["reason"]) == (
            "arrive", "entered", "v1", "lane")

    def test_a_lane_is_never_claimed_and_only_a_lane_takes_arrivals(self, harness, clock):
        journal = harness.journal(LISTING, clock)
        with pytest.raises(ValueError, match="lane"):
            self._claim(harness, journal, "arrive", ["s1"])
        with pytest.raises(ValueError, match="not a lane"):
            journal.arrive("scrape", ["s1"])

    def test_throttle_keeps_the_last_version_at_the_place_of_the_first(self, harness, clock):
        journal = harness.journal(LISTING, clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        clock.now = _at(5)
        assert journal.arrive("arrive", ["s1"], ref="v2")["merged"] == 1
        waiting = journal.arrival("s1", "arrive")
        assert (waiting.ref, waiting.place, waiting.arrived_at) == ("v2", _at(0), _at(0))
        merged = [e for e in journal.history("s1") if e["status"] == "merged"]
        assert [e["lease"] for e in merged] == ["v2"]

    def test_dedupe_keeps_the_first_version(self, harness, clock):
        journal = harness.journal(_listing(merge="first"), clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        clock.now = _at(5)
        journal.arrive("arrive", ["s1"], ref="v2")
        assert journal.arrival("s1", "arrive").ref == "v1"

    def test_batch_keeps_every_version_in_the_order_they_came(self, harness, clock):
        journal = harness.journal(_listing(merge="all"), clock)
        for minute, ref in enumerate(("v1", "v2", "v3")):
            clock.now = _at(minute)
            journal.arrive("arrive", ["s1"], ref=ref)
        assert journal.refs("s1", "arrive") == ("v1", "v2", "v3")
        assert journal.arrival("s1", "arrive").ref == "v3", "the latest is still named"

    def test_a_batch_past_its_size_lets_the_oldest_go_and_says_so(self, harness, clock):
        journal = harness.journal(_listing(merge="all", max_size=2), clock)
        for minute, ref in enumerate(("v1", "v2", "v3")):
            clock.now = _at(minute)
            journal.arrive("arrive", ["s1"], ref=ref)
        assert journal.refs("s1", "arrive") == ("v2", "v3"), "the oldest was let go"
        dropped = [e for e in journal.history("s1") if e["status"] == "dropped"]
        assert [e["node"] for e in dropped] == ["arrive"], "and it left a trace"

    def test_a_named_function_decides_what_the_lane_keeps(self, harness, clock):
        """The graph holds the NAME; the journal holds the function."""
        seen = []

        def keep_the_ends(kept, arriving):
            seen.append((kept, arriving))
            whole = (*kept, arriving)
            return whole[:1] + whole[-1:] if len(whole) > 2 else whole

        journal = harness.journal(_listing(merge="fn:ends"), clock,
                                  mergers={"ends": keep_the_ends})
        for minute, ref in enumerate(("v1", "v2", "v3", "v4")):
            clock.now = _at(minute)
            journal.arrive("arrive", ["s1"], ref=ref)
        assert journal.refs("s1", "arrive") == ("v1", "v4")
        assert seen[0] == ((), "v1"), "asked with what waits and what arrives"
        assert seen[-1] == (("v1", "v3"), "v4")

    def test_a_lane_naming_a_function_the_journal_lacks_says_which(self, harness, clock):
        journal = harness.journal(_listing(merge="fn:nowhere"), clock)
        with pytest.raises(ValueError, match="nowhere"):
            journal.arrive("arrive", ["s1"], ref="v1")

    def test_debounce_waits_for_quiet_and_max_wait_ends_it(self, harness, clock):
        journal = harness.journal(_listing(position="last", delay="10m", max_wait="25m"), clock)
        for minute in (0, 5, 10):
            clock.now = _at(minute)
            journal.arrive("arrive", ["s1"], ref=f"v{minute}")
        assert journal.arrival("s1", "arrive").place == _at(10)
        clock.now = _at(19)
        assert self._settle(harness, journal, ["s1"]) == 0, "quiet since 10, not 10 minutes yet"
        clock.now = _at(15)
        journal.arrive("arrive", ["s1"], ref="v15")
        clock.now = _at(24)
        assert self._settle(harness, journal, ["s1"]) == 0
        clock.now = _at(25)
        assert self._settle(harness, journal, ["s1"]) == 1, "max_wait from the first arrival"
        assert journal.history("s1")[-1]["lease"] == "v15"

    def test_cooldown_runs_from_the_end_of_the_previous_pass(self, harness, clock):
        journal = harness.journal(LISTING, clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        assert self._settle(harness, journal, ["s1"]) == 1, "no previous pass, no cooldown"
        clock.now = _at(30)
        self._through(harness, journal, ["s1"])
        clock.now = _at(40)
        journal.arrive("arrive", ["s1"], ref="v2")
        clock.now = _at(89)
        assert self._settle(harness, journal, ["s1"]) == 0, "the pass ended at 30"
        assert journal.progress("s1") == {
            "arrive": NODE_DONE, "scrape": NODE_DONE, "publish": NODE_DONE}, (
            "the previous pass stays visible while the next version waits")
        clock.now = _at(90)
        assert self._settle(harness, journal, ["s1"]) == 1
        assert journal.progress("s1") == {"arrive": NODE_DONE}
        archived = sorted((e["node"], e["status"]) for e in journal.history("s1")
                          if e["reason"] == "arrival")
        assert archived == [("arrive", NODE_DONE), ("publish", NODE_DONE),
                            ("scrape", NODE_DONE)]
        assert self._claim(harness, journal, "scrape", ["s1"]) == ["s1"]

    def test_an_arrival_during_a_running_pass_waits_for_it(self, harness, clock):
        journal = harness.journal(_listing(), clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        self._settle(harness, journal, ["s1"])
        lease = self._claim(harness, journal, "scrape", ["s1"])
        journal.arrive("arrive", ["s1"], ref="v2")
        assert self._settle(harness, journal, ["s1"]) == 0, "scrape is running"
        journal.conclude("scrape", lease, token=lease.token)
        assert self._settle(harness, journal, ["s1"]) == 1
        assert journal.progress("s1") == {"arrive": NODE_DONE}

    def test_the_driver_never_lets_an_arrival_in_over_a_running_pass(self, harness, clock):
        """What the journal checks before, the driver holds on its own: a
        claim may land between the journal's read and the write."""
        journal = harness.journal(_listing(), clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        self._settle(harness, journal, ["s1"])
        self._claim(harness, journal, "scrape", ["s1"])
        journal.arrive("arrive", ["s1"], ref="v2")
        [entry] = [e for page in journal.driver.scan(
            harness.candidates(["s1"]), name=None, nodes=("arrive", "scrape", "publish"),
            parents=(), page=10, now=_at(0)) for e in page]
        assert journal.driver.enter("arrive", [("s1", entry.revision)],
                                    archive=("arrive", "scrape", "publish"), now=_at(0)) == []
        assert journal.driver.enter("arrive", [("s1", entry.revision + 1)],
                                    archive=("arrive",), now=_at(0)) == [], "stale revision"
        assert journal.progress("s1") == {"arrive": NODE_DONE, "scrape": NODE_RUNNING}
        assert journal.driver.enter("scrape", [("s1", entry.revision)],
                                    archive=("scrape",), now=_at(0)) == [], "nothing waits there"

    def test_skip_drops_an_arrival_during_a_running_pass(self, harness, clock):
        journal = harness.journal(_listing(while_running="skip"), clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        self._settle(harness, journal, ["s1"])
        self._claim(harness, journal, "scrape", ["s1"])
        assert journal.arrive("arrive", ["s1"], ref="v2") == {
            "queued": 0, "merged": 0, "skipped": 1}
        assert journal.arrival("s1", "arrive") is None
        assert [e["lease"] for e in journal.history("s1") if e["status"] == "skipped"] == ["v2"]

    def test_an_urgent_arrival_skips_the_cooldown_but_not_a_running_pass(self, harness, clock):
        journal = harness.journal(LISTING, clock)
        clock.now = _at(0)
        journal.arrive("arrive", ["s1"], ref="v1")
        self._settle(harness, journal, ["s1"])
        lease = self._claim(harness, journal, "scrape", ["s1"])
        journal.arrive("arrive", ["s1"], ref="v2")
        journal.arrive("arrive", ["s1"], ref="v3", urgent=True)
        assert journal.arrival("s1", "arrive").urgent
        assert self._settle(harness, journal, ["s1"]) == 0
        journal.conclude("scrape", lease, token=lease.token)
        clock.now = _at(1)
        assert self._settle(harness, journal, ["s1"]) == 1, "urgent: no hour of cooldown"

    def test_a_policy_tunes_its_lane(self, harness, clock):
        graph = Graph(Document("listings"), LISTING,
                      policies={"fast": {"arrive": {"lane": Lane.throttle(cooldown="5m")}}})
        journal = harness.journal(graph, clock)
        journal.enroll(["slow1"], None)
        journal.enroll(["fast1"], "fast")
        clock.now = _at(0)
        journal.arrive("arrive", ["slow1", "fast1"])
        assert self._settle(harness, journal, ["slow1", "fast1"]) == 2
        self._through(harness, journal, ["slow1", "fast1"])
        journal.arrive("arrive", ["slow1", "fast1"])
        clock.now = _at(5)
        assert self._settle(harness, journal, ["slow1", "fast1"]) == 1
        assert journal.arrival("slow1", "arrive") is not None
        with pytest.raises(DagError, match="never adds or removes"):
            Graph(Document("listings"), LISTING,
                  policies={"fast": {"scrape": {"lane": Lane()}}})

    def test_a_lane_lets_arrivals_in_by_place_within_its_rate(self, harness, clock):
        nodes = (Node("arrive", lane=Lane(), rate=(Rate(1, "1m"),)), *LISTING[1:])
        journal = harness.journal(nodes, clock)
        for minute, subject in ((0, "c"), (1, "a"), (2, "b")):
            clock.now = _at(minute)
            journal.arrive("arrive", [subject])
        clock.now = _at(3)
        assert self._settle(harness, journal, ["a", "b", "c"]) == 1
        assert journal.progress("c") == {"arrive": NODE_DONE}, "c arrived first"
        assert self._settle(harness, journal, ["a", "b", "c"]) == 0, "one a minute"
        clock.now = _at(4)
        assert self._settle(harness, journal, ["a", "b", "c"]) == 1
        assert journal.progress("a") == {"arrive": NODE_DONE}

    def test_a_lane_after_other_nodes_waits_for_its_parents(self, harness, clock):
        nodes = (Node("fetch"), Node("tag", parents=("fetch",), lane=Lane()),
                 Node("index", parents=("tag",)))
        journal = harness.journal(nodes, clock)
        journal.arrive("tag", ["s1"])
        assert journal.settle(harness.candidates(["s1"])) == {}
        self._run(harness, journal, "fetch", ["s1"])
        assert journal.settle(harness.candidates(["s1"])) == {"tag": {"entered": 1}}
        assert journal.progress("s1") == {"fetch": NODE_DONE, "tag": NODE_DONE}

    def test_a_migration_carries_the_arrivals_of_a_renamed_lane(self, harness, clock):
        v1 = Graph(Document("listings", version="1"), LISTING)
        v2 = Graph(Document("listings", version="2"),
                   (Node("inbox", lane=Lane.throttle(cooldown="1h")),
                    Node("scrape", parents=("inbox",)), Node("publish", parents=("scrape",))))
        old = harness.journal(v1, clock)
        new = harness.journal_on(old, v2, clock)
        old.arrive("arrive", ["s1"], ref="v1")
        assert new.migrate(["s1"], v1, {"arrive": "inbox"}) == 1
        assert new.arrival("s1", "inbox").ref == "v1"
        assert new.settle(harness.candidates(["s1"])) == {"inbox": {"entered": 1}}

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

    def test_the_driver_follows_the_model_through_lanes(self, harness):
        """The same model, every graph entered through a lane: arrivals,
        merges, cooldowns, delays and waits over and over again."""
        pytest.importorskip("hypothesis")
        from hypothesis import HealthCheck, settings
        from hypothesis.stateful import run_state_machine_as_test

        run_state_machine_as_test(
            _model_machine(harness, str, lane_root=True),
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

    def _lane_ready(self, store):
        """s1 went through `scrape`; `publish` is claimable; a second version
        waits in the lane, due."""
        setup = store.session()
        setup.journal.arrive("arrive", ["s1"], ref="v1")
        setup.journal.settle(setup.candidates(["s1"]))
        lease = setup.journal.claim("scrape", 1, candidates=setup.candidates(["s1"]))
        setup.journal.conclude("scrape", lease, token=lease.token)
        setup.journal.arrive("arrive", ["s1"], ref="v2")
        setup.commit()

    def test_two_versions_arriving_at_once_are_both_kept(self, harness, clock):
        """A lane that keeps every version cannot decide with one write: it
        reads what waits, merges, then writes. Two arrivals at the same
        moment would otherwise each start from the state before the other,
        and one would overwrite the other's version.

        The journal therefore merges under the driver's guard. Remove it and
        this fails on the storages that can really interleave — SQLite loses
        v1, PostgreSQL likewise. The memory driver runs both arrivals in one
        process, so it has no window to lose; the test holds there without
        proving anything, which is worth knowing when reading a green run."""
        store = harness.store(_listing(merge="all", max_size=10), clock)
        try:
            one, two = store.session(), store.session()

            def second() -> None:
                two.journal.arrive("arrive", ["s1"], ref="v2")

            one.journal.arrive("arrive", ["s1"], ref="v1")
            _race(second, two.commit, one.commit)

            check = store.session()
            try:
                assert check.journal.refs("s1", "arrive") == ("v1", "v2"), (
                    "both versions waited, neither overwrote the other")
            finally:
                check.rollback()
        finally:
            store.close()

    @pytest.mark.parametrize("first", ["worker", "door"])
    def test_a_lane_never_lets_a_version_in_over_a_claim(self, harness, clock, first):
        """The lane lets v2 in — archiving the pass — while a worker claims
        `publish` on the strength of that pass. Whoever writes first, a claim
        that was granted is never archived under the worker's feet, and
        `publish` never ends up held without its parent."""
        store = harness.store(_listing(), clock)
        try:
            self._lane_ready(store)
            worker, door = store.session(), store.session()
            granted: list[Any] = []

            def claim() -> None:
                granted.extend(worker.journal.claim(
                    "publish", 1, candidates=worker.candidates(["s1"])))

            def let_in() -> None:
                door.journal.settle(door.candidates(["s1"]))

            if first == "worker":
                claim()
                _race(let_in, door.commit, worker.commit)
            else:
                let_in()
                _race(claim, worker.commit, door.commit)
            _assert_no_orphan(store, ["s1"], _listing())
            check = store.session()
            try:
                if granted:
                    assert check.journal.progress("s1").get("publish") == NODE_RUNNING, (
                        "the lane archived a pass while a worker held publish")
                    assert check.journal.arrival("s1", "arrive") is not None
            finally:
                check.rollback()
        finally:
            store.close()

    @pytest.mark.parametrize("first", ["arrival", "door"])
    def test_a_version_arriving_as_the_lane_lets_one_in_is_never_lost(
            self, harness, clock, first):
        store = harness.store(_listing(), clock)
        try:
            setup = store.session()
            setup.journal.arrive("arrive", ["s1"], ref="v1")
            setup.commit()
            arrival, door = store.session(), store.session()

            def arrive() -> None:
                arrival.journal.arrive("arrive", ["s1"], ref="v2")

            def let_in() -> None:
                door.journal.settle(door.candidates(["s1"]))

            if first == "arrival":
                arrive()
                _race(let_in, door.commit, arrival.commit)
            else:
                let_in()
                _race(arrive, arrival.commit, door.commit)
            check = store.session()
            try:
                entered = [e["lease"] for e in check.journal.history("s1")
                           if e["status"] == "entered"]
                waiting = check.journal.arrival("s1", "arrive")
                kept = [*entered, *([waiting.ref] if waiting else [])]
                assert "v2" in kept, f"v2 lost: entered {entered}, waiting {waiting}"
            finally:
                check.rollback()
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


def _model_machine(harness: Any, subject_type: type = str, lane_root: bool = False) -> Any:
    """A Hypothesis state machine: one random graph per run, one journal on
    it, and the MODEL — `{subject: {node: (status, started_at, lease)}}` —
    moved by the rule written as plainly as possible. `lane_root` makes the
    root a lane in every graph, so that runs dwell on arrivals."""
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
            root_lane = lane_root and not spec["parents"]
            choice = (not root_lane and spec["name"] in with_children
                      and draw(st.integers(0, 3)) == 0)
            loop = None
            if not choice and not root_lane and draw(st.integers(0, 3)) == 0:
                loop = Loop(to=draw(st.sampled_from([spec["name"], *upstream(spec["name"])])),
                            max=draw(st.integers(1, 2)),
                            on=tuple(draw(st.lists(st.sampled_from(
                                (NODE_DONE, NODE_SKIPPED, NODE_FAILED)),
                                min_size=1, max_size=2, unique=True))))
            retry = None
            if not root_lane and draw(st.integers(0, 3)) == 0:
                retry = Retry(limit=draw(st.integers(1, 2)), delay=draw(st.integers(0, 3)),
                              backoff=draw(st.sampled_from(("constant", "linear",
                                                            "exponential"))))
            wait = timeout = grace = None
            if (not choice and loop is None and retry is None and spec["parents"]
                    and draw(st.booleans())):
                wait = draw(st.sampled_from(("ev1", "ev2")))
                timeout = draw(st.one_of(st.none(), st.integers(1, 2), st.integers(1, 2)))
            lane = None
            if root_lane or (not choice and loop is None and retry is None and wait is None
                             and draw(st.integers(0, 2)) == 0):
                lane = Lane(merge=draw(st.sampled_from(("first", "last", "all"))),
                            max_size=draw(st.integers(1, 3)),
                            position=draw(st.sampled_from(("first", "last"))),
                            cooldown=draw(st.one_of(st.none(), st.integers(5, 30))),
                            delay=draw(st.one_of(st.none(), st.integers(1, 4))),
                            max_wait=draw(st.one_of(st.none(), st.integers(2, 6))),
                            while_running=draw(st.sampled_from(("queue", "skip"))))
            optional = not choice and lane is None and draw(st.booleans())
            if optional and draw(st.integers(0, 3)) > 0:
                grace = draw(st.integers(1, 2))
            nodes.append(Node(spec["name"], parents=spec["parents"],
                              on=spec.get("on", {}), need=spec.get("need"),
                              choice=choice, loop=loop, retry=retry, wait=wait,
                              timeout=timeout, grace=grace, optional=optional, lane=lane))
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
            # (subject, lane) -> (ref, place, arrived_at, urgent, refs)
            self.waiting: dict[tuple[Any, str], tuple[Any, str, str, bool, str | None]] = {}

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
            if n.wait is not None or n.lane is not None:
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
            workable = [x for x in self.dag if x.wait is None and x.lane is None]
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

        def _pass(self, n: Node) -> tuple[str, ...]:
            return (n.name, *sorted(descendants(n.name, self.dag)))

        def _running(self, s: Any, n: Node) -> bool:
            return any(self.model[s].get(x, ("",))[0] in (NODE_RUNNING, NODE_SCHEDULED)
                       for x in self._pass(n))

        @rule(data=st.data(), candidates=subjects, ref=st.sampled_from((None, "r1", "r2")),
              urgent=st.integers(0, 5).map(lambda x: x == 5))
        def arrive(self, data: Any, candidates: list[Any], ref: Any, urgent: bool) -> None:
            lanes = [x for x in self.dag if x.lane is not None]
            if not lanes:
                return
            n = data.draw(st.sampled_from(lanes))
            lane = n.lane
            assert lane is not None
            self._tick()
            now = self.clock.now
            expected = {"queued": 0, "merged": 0, "skipped": 0}
            for s in dict.fromkeys(candidates):
                current = self.waiting.get((s, n.name))
                if lane.while_running == "skip" and self._running(s, n):
                    self.archived[s].append((now, n.name, "skipped", "lane"))
                    expected["skipped"] += 1
                elif current is None:
                    kept = (ref,) if lane.keeps_every_ref and ref is not None else ()
                    self.waiting[(s, n.name)] = (
                        kept[-1] if kept else ref, now, now, urgent, _refs_text(kept))
                    expected["queued"] += 1
                elif lane.keeps_every_ref:
                    # EVERY VERSION KEPT: read, merge, write — and what falls
                    # past `max_size` leaves a note behind.
                    was = _refs_of(current[4])
                    kept = ((*was, ref) if ref is not None else was)[-lane.max_size:]
                    for gone in [r for r in was if r not in kept]:
                        self.archived[s].append((now, n.name, "dropped", "lane"))
                        assert gone is not None
                    self.waiting[(s, n.name)] = (
                        kept[-1] if kept else None,
                        now if lane.position == "last" else current[1],
                        current[2], current[3] or urgent, _refs_text(kept))
                    self.archived[s].append((now, n.name, "merged", "lane"))
                    expected["merged"] += 1
                else:
                    self.waiting[(s, n.name)] = (
                        ref if lane.merge == "last" else current[0],
                        now if lane.position == "last" else current[1],
                        current[2], current[3] or urgent, current[4])
                    self.archived[s].append((now, n.name, "merged", "lane"))
                    expected["merged"] += 1
            assert self.journal.arrive(n.name, candidates, ref=ref, urgent=urgent) == expected

        @rule(data=st.data(), subject=st.sampled_from(_SUBJECTS),
              times=st.integers(4, 12))
        def keep_coming_back(self, data: Any, subject: Any, times: int) -> None:
            """One subject arriving again and again, a tick apart, then the
            janitor: where a moving place outruns its delay and only
            `max_wait` lets it through."""
            for _ in range(times):
                self.arrive(data, [subject], "r1", False)
            self.settle([subject], 0)

        def _let_in(self, n: Node, candidates: list[Any], now: str) -> int:
            lane = n.lane
            assert lane is not None
            due = []
            for index, s in enumerate(dict.fromkeys(candidates)):
                waiting = self.waiting.get((s, n.name))
                if waiting is None or self._running(s, n):
                    continue
                if not joined(n.name, self.dag, self._statuses(s)):
                    continue
                ref, place, arrived_at, urgent, _refs = waiting
                ready = [place]
                if lane.delay is not None:
                    ready.append(shift(place, seconds(lane.delay)))
                ended = [self.model[s][x][3] for x in self._pass(n)
                         if x in self.model[s] and self.model[s][x][3] is not None]
                if lane.cooldown is not None and ended:
                    ready.append(shift(max(ended), seconds(lane.cooldown)))
                late = (lane.max_wait is not None
                        and shift(arrived_at, seconds(lane.max_wait)) <= now)
                if urgent or max(ready) <= now or late:
                    due.append((place, index, s))
            for _, _, s in sorted(due):
                for x in self._pass(n):
                    self._archive(s, x, "arrival")
                ref, place, arrived_at, urgent, _refs = self.waiting.pop((s, n.name))
                self.archived[s].append((now, n.name, "entered", "lane"))
                self.model[s][n.name] = (NODE_DONE, arrived_at, None, now)
            return len(due)

        @rule(candidates=subjects, wait=st.integers(0, 5))
        def settle(self, candidates: list[Any], wait: int) -> None:
            for _ in range(wait):
                self._tick()
            now = self.clock.now
            expected: dict[str, dict[str, int]] = {}
            for n in self.dag:
                if n.lane is not None:
                    entered = self._let_in(n, candidates, now)
                    if entered:
                        expected[n.name] = {"entered": entered}
                    continue
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
                # Rows archived at the same second on the same node come in
                # no promised order.
                expected = sorted(self.archived[s])
                got = sorted((e["archived_at"], e["node"], e["status"], e["reason"])
                             for e in self.journal.history(s))
                assert got == expected, f"history of {s}"
                for n in self.dag:
                    if n.lane is not None:
                        waiting = self.journal.arrival(s, n.name)
                        assert (tuple(waiting) if waiting else None) == self.waiting.get(
                            (s, n.name)), f"{s} waiting in {n.name}"

    return Machine


def _refs_text(refs: tuple[Any, ...]) -> str | None:
    """What the journal stores for the versions a lane keeps — the model
    spells it out itself rather than importing the journal's own encoder,
    so a change there has to be a deliberate change here too."""
    return json.dumps(list(refs)) if refs else None


def _refs_of(stored: str | None) -> tuple[Any, ...]:
    return tuple(json.loads(stored)) if stored else ()


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


def _assert_no_orphan(store: Any, subjects: list[Any],
                      dag: tuple[Node, ...] = DIAMOND) -> None:
    """No row of a node while one of its parents is not `done` or `skipped`."""
    session = store.session()
    try:
        for subject in subjects:
            progress = session.journal.progress(subject)
            for name in progress:
                missing = [p for p in node(name, dag).parents
                           if progress.get(p) not in NODE_SATISFYING]
                assert not missing, (
                    f"{subject}: {name} is {progress[name]} but its parent(s) "
                    f"{missing} are {[progress.get(p) for p in missing]}")
    finally:
        session.rollback()
