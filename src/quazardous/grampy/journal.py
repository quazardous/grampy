"""THE NODE JOURNAL — who started what, and where it stands.

    claim     take a node for eligible subjects — a `Lease`, with its token
    conclude  finish it, or declare it failed — with the lease's token
    adopt     record work already done that the journal does not know
    forget    erase it — "never started", the initial state
    release   give back the leases of a dead worker
    expire    give back every lease held longer than its node allows
    enroll    give subjects a policy, whose settings then apply to them
    migrate   move subjects pinned to another graph onto this one
    signal    record that an awaited event happened, for subjects
    arrive    a subject comes back: it waits in a lane before running again
    settle    conclude waits, let due arrivals through their lane, and skip
              optional nodes past their grace
    history   every row forget, release or a loop took away, kept
    progress  what is recorded for ONE subject
    stages    what a BATCH went through, with durations

────────────────────────────────────────────────────────────────────────
THE LOGIC HERE, THE STORAGE IN A DRIVER
────────────────────────────────────────────────────────────────────────

The journal validates against the graph — the node exists, it is
optional before being skipped, a conclusion is a known status — and it
DECIDES: what a claim may take is `dag.claimable`, evaluated here, in
Python, on rows the driver read. The driver (`JournalDriver`) stores rows
and offers a few atomic operations; it never decides what is claimable.

ONE RULE, ONE PLACE. A driver that re-expressed the rule in its own
language — SQL, a script — would be a second copy to keep in step, and
every future rule (joins, choices, gates) would have to be written once per
storage. A driver MAY pre-filter what it reads, to read less; it may never
be the one that says yes.

────────────────────────────────────────────────────────────────────────
READ, DECIDE, WRITE — AND THE REVISION CLOSES THE GAP
────────────────────────────────────────────────────────────────────────

Deciding in Python means reading first and writing after, and something
may happen in between. The one thing that can turn a yes into a no is a
row DISAPPEARING: a `forget` takes the parent away, the child must not be
taken on its strength. (Rows appearing only ever close a node or satisfy
it later; a claim that missed them is a claim that ran a moment earlier.)

So every subject carries a REVISION. `forget` raises it, atomically with
the deletion. A claim reads the revision with the rows, and writes with
`insert_if_unchanged`: the row is inserted only if the revision is still
the one read. A claim that decided on rows since forgotten inserts nothing.

HOW a driver makes that atomic is its own affair — a lock for memory, a
row lock held to the end of the transaction for a database. The shared
concurrency tests (`grampy.testing`) hold every driver to the same
outcome, whatever it blocks on.

────────────────────────────────────────────────────────────────────────
A CONCLUSION PROVES IT HOLDS THE LEASE
────────────────────────────────────────────────────────────────────────

A worker can be slow rather than dead. Its lease released, the node
claimed again by another worker, the first one comes back and reports:
without a proof, it would conclude the SECOND worker's row. Every claim
therefore issues a token, unique to that claim, stored on the rows it
took; `conclude` and `fail` touch only the rows holding the token they
bring (a fencing token). `token=None` is the operator's override, written
on purpose, never a default.

────────────────────────────────────────────────────────────────────────
NOTHING IS LOST: A ROW TAKEN AWAY IS ARCHIVED
────────────────────────────────────────────────────────────────────────

The absence of a row still means "never started", and the claim still
reads only current rows. But a row does not vanish: `forget`, `release`
and a declared loop MOVE it to the history, with when and why. That is
where "how many times did this subject go through review", "who held it
before the lease was released" and the loop's bound are read.

────────────────────────────────────────────────────────────────────────
ELIGIBILITY COMES FROM OUTSIDE, AND THE JOURNAL DOES NOT READ IT
────────────────────────────────────────────────────────────────────────

A claim must stay ATOMIC across two things: the node rows and the
application's eligible subjects. Two calls would leave a window where two
workers take the same subject.

The journal therefore receives candidates as an OPAQUE value, in the
driver's own terms — an ordered iterable of subjects for the memory
driver, a `SELECT` for the postgres one. "Not terminal, sorted by
priority" is the application's sentence; it is written once, on its
side, and travels here unread.

────────────────────────────────────────────────────────────────────────
IT NEVER COMMITS
────────────────────────────────────────────────────────────────────────

A driver works on its owner's connection, so a node row and the
application's projection can be written in one transaction. The caller
decides where the transaction ends, because it knows what it put in it.
"""

from __future__ import annotations

import secrets
from datetime import datetime
from typing import Any, NamedTuple

from ._base import PAGE, _unique
from .dag import (
    NODE_CONCLUDED,
    Node,
    accepts,
    claimable,
    descendants,
    joined,
    node,
    omitted_by,
)
from .lanes import _Lanes
from .migration import MigrationError, _Migration
from .names import Outcome, Reason, Status
from .protocol import (
    CAPABILITIES,
    Arrival,
    CoreDriver,
    Entry,
    JournalDriver,
    Keyed,
    LaneDriver,
    Lease,
    LimitDriver,
    MissingCapability,
    Page,
    ReadingDriver,
    VersionDriver,
    _require,
    capability_methods,
    needed_capabilities,
    utc_now,
)
from .timing import seconds, shift, stamp

#: WHAT THIS MODULE HAS ALWAYS OFFERED — the journal and every name it held
#: before its parts moved to modules of their own (`protocol`, `limits`,
#: `lanes`, `migration`): an import from here keeps working.
__all__ = [
    "CAPABILITIES", "NODE_CONCLUDED", "PAGE", "Arrival", "CoreDriver", "Entry",
    "JournalDriver", "Keyed", "LaneDriver", "Lease", "LimitDriver", "MigrationError",
    "MissingCapability", "NodeJournal", "Page", "ReadingDriver", "VersionDriver",
    "capability_methods", "needed_capabilities", "utc_now",
]


def _ready_at(n: Node, entry: Entry) -> str | None:
    """WHEN THIS SUBJECT BECAME READY for a grouping node: the last of its
    parents to conclude. A node with no parent has no clock, so a group of
    those only ever goes when it is full."""
    ends = [entry.finished[p] for p in n.parents if p in entry.finished]
    return max(ends) if ends else None

class _Ready(NamedTuple):
    """A candidate a grouping node could take: what it is, what groups it,
    and since when it has been waiting for its group to fill."""

    subject: Any
    revision: int
    key: str | None
    ready_at: str | None
    policy: str | None


class NodeJournal(_Lanes, _Migration):
    """Progress per node, validated against ONE graph, stored by a driver."""

    # -- take --------------------------------------------------------------

    def claim(self, name: str, limit: int, *, candidates: Any,
              require_parents: bool = True) -> Lease:
        """Take up to `limit` candidates for this node. Return their subjects,
        as a `Lease` whose `token` the worker brings back to conclude them.

        THE RULE IS `dag.claimable` — parents concluded, nobody holding, no
        descendant started — decided here on the rows the driver read, and
        written with `insert_if_unchanged`. Two workers aiming at the same
        subject: one row, one winner. A `forget` in between: no row.

        `require_parents=False` — A NAMED EXCEPTION. A backfill over subjects
        older than the graph has no parent rows to show; requiring them would
        always return nothing. The caller lifting it must hold a proof outside
        the graph. The two other guards still hold, and so does the write.
        """
        n = node(name, self.dag)
        if n.wait is not None:
            raise ValueError(
                f"node {name!r} waits for {n.wait!r}: it is settled (`settle`), "
                f"never claimed")
        if n.lane is not None:
            raise ValueError(
                f"node {name!r} is a lane: subjects `arrive` in it and `settle` lets "
                f"them through, it is never claimed")
        limit = int(limit)
        token = secrets.token_hex(16)
        if limit <= 0:
            return Lease([], token)
        after = tuple(sorted(descendants(name, self.dag)))
        parents = n.parents if require_parents and not n.custom_join else ()
        now: str | None = None if self._clock_in_scan else self._clock()
        chosen: list[tuple[Any, int]] = []
        policy_of: dict[Any, str | None] = {}
        pinned: set[Any] = set()
        seen: set[Any] = set()
        # A GROUP IS GATHERED BEFORE IT IS JUDGED: the claim must read enough
        # candidates to know whether one is complete, so `limit` alone is not
        # how far it reads.
        enough = limit if n.group is None else max(limit, n.group.size)
        ready: list[_Ready] = []
        # A GROUPING CLAIM READS AND WRITES UNDER THE GUARD, taken before it
        # reads anything. Two claimers would otherwise each decide on a state
        # the other was about to change, and each win a piece of one group:
        # a bag of two and a bag of one, neither of them a group, and no row
        # looking wrong. Guarding the NODE rather than the key keeps it to one
        # lock, held over a read a grouping node makes rarely and in bulk.
        def gather() -> list[tuple[Any, int]]:
            """Read candidates until there is enough to decide."""
            nonlocal now
            for page in self.driver.scan(candidates, name=name,
                                         nodes=(name, *n.parents, *after),
                                         parents=parents, page=max(enough, PAGE),
                                         now=now, after=after):
                if now is None:
                    now = getattr(page, "now", None) or self._clock()
                for e in page:
                    # A SUBJECT LISTED TWICE COUNTS ONCE: it would otherwise
                    # take a place in the limit and be refused by the write.
                    if e.subject in seen or not self._mine(e):
                        continue
                    seen.add(e.subject)
                    if e.version is not None:
                        pinned.add(e.subject)
                    if not self._takable(n, after, _due_away(name, e, now),
                                         require_parents):
                        continue
                    if n.group is not None:
                        ready.append(_Ready(e.subject, e.revision, e.key,
                                            _ready_at(n, e), e.policy))
                        continue
                    chosen.append((e.subject, e.revision))
                    policy_of[e.subject] = e.policy
                    if len(chosen) >= limit:
                        return chosen
                if n.group is None and len(chosen) >= limit:
                    return chosen
            if n.group is None:
                return chosen
            if now is None:                 # no page at all: nothing to group
                return []
            picked = self._group(n, ready, now)
            policy_of.update({m.subject: m.policy for m in picked})
            return [(m.subject, m.revision) for m in picked]

        def write(group: list[tuple[Any, int]]) -> list[Any]:
            if not group or now is None:    # no page read, nothing chosen
                return []
            if self._limited(n):
                return self._within_limits(
                    n, group, policy_of, now,
                    lambda part: self.driver.insert_if_unchanged(
                        name, part, status=Status.RUNNING, now=now, lease=token))
            return self.driver.insert_if_unchanged(
                name, group, status=Status.RUNNING, now=now, lease=token)

        # ONE WRITE PER CLAIM. A storage that locks while writing takes its
        # locks in one go, in one order, and two claimers cannot hold each
        # other. The price: a subject refused by the write is not replaced —
        # under contention a claim may take fewer than `limit`, never a wrong
        # one.
        if n.group is None:
            taken = write(gather())
        else:
            with self.driver.guard([f"group|{name}"]):
                taken = write(gather())
        # PINNED ONCE: a subject the page showed pinned already is — to this
        # graph, `_mine` saw to it — so only a first write pins.
        self._pin([s for s in taken if s not in pinned])
        return Lease(taken, token)

    def _group(self, n: Node, ready: list[_Ready], now: str) -> list[_Ready]:
        """THE ONE GROUP THIS CLAIM MAY TAKE, or nothing.

        Candidates keep the order the application gave them, so the group
        that goes is the one whose members have waited longest. A group goes
        when it is FULL, or when its oldest member has been ready longer than
        `max_wait` — an incomplete group then goes as it is, and the worker
        sees how many it really got.

        Nothing is written while a group fills: a short group simply is not
        claimable, so there is no half-gathered state to repair after a
        crash. The price is that the claim reads its candidates again next
        time, which is the same price every claim already pays.
        """
        group = n.group
        assert group is not None
        gathered: dict[Any, list[_Ready]] = {}
        for member in ready:
            if group.per_key and member.key is None:
                # NO KEY IS NOT ONE KEY: grouping every keyless candidate
                # together would hand out groups nobody declared.
                raise ValueError(
                    f"node {n.name!r} groups by key, and a candidate carries none: "
                    f"name the key column `grampy_key` in SQL, pass Keyed(subject, "
                    f"key) otherwise, or give the adapter a group_of")
            gathered.setdefault(member.key if group.per_key else None, []).append(member)
        late = (shift(now, -seconds(group.max_wait))
                if group.max_wait is not None else None)
        for members in gathered.values():
            if len(members) >= group.size:
                return members[:group.size]
            # AN INCOMPLETE GROUP GOES ONLY WHEN IT HAS WAITED. `ready_at` is
            # None for a node with no parents: no clock, so no way to be late.
            oldest = [m.ready_at for m in members if m.ready_at is not None]
            if late is not None and oldest and min(oldest) <= late:
                return members
        return []

    def _takable(self, n: Node, after: tuple[str, ...], rows: dict[str, str],
                 require_parents: bool) -> bool:
        if require_parents:
            return claimable(n.name, self.dag, rows)
        return n.name not in rows and not any(d in rows for d in after)

    def parents_concluded(self, name: str, subject: Any) -> Any:
        """ALL PARENTS CONCLUDED — `done` or `skipped` — for this subject.

        Returned in the driver's terms: a boolean for the memory driver, a
        SQL expression for the postgres one, so an application can count
        what is waiting for a node in its own query.
        """
        _require(self.driver, "reading", "journal.parents_concluded")
        return self.driver.parents_concluded(node(name, self.dag).parents, subject)

    # -- conclude ----------------------------------------------------------

    def conclude(self, name: str, subjects: list[Any], *, token: str | None,
                 status: str = Status.DONE, branch: str | None = None) -> int:
        """Finish this node on these subjects. Return the count touched.

        ONLY RUNNING ROWS HOLDING `token` ARE TOUCHED: a duplicate report — a
        worker retrying after a network timeout — recounts nothing, and a
        worker whose lease went to someone else rewrites nothing. `token` is
        required; `None` concludes whoever holds the rows (an operator's act).

        A CHOICE CONCLUDES BY NAMING ITS BRANCH. `branch` is required to
        conclude a choice `done` or `skipped`, refused anywhere else; the
        other branches and what only they reach are `omitted` in the same
        write (`dag.omitted_by`). A failed choice omits nothing: its branches
        wait on it like any child on a failed parent.
        """
        if not subjects:
            return 0
        n = node(name, self.dag)
        if status not in NODE_CONCLUDED or status == Status.OMITTED:
            raise ValueError(
                f"unknown conclusion status: {status!r} — expected "
                f"{Status.DONE}, {Status.SKIPPED} or {Status.FAILED}")
        omit: tuple[str, ...] = ()
        subjects = _unique(subjects)
        now = self._clock()
        touched = 0
        retry_of = self._per_policy(name, "retry", subjects)
        if any(r is not None for r in retry_of.values()) and status == Status.FAILED \
                and branch is None:
            # FIRST, THE RETRIES — each subject under its policy's policy: under
            # the limit, the failure is archived and the node scheduled again,
            # later for later attempts.
            tries = self.driver.archived(subjects, name, Reason.RETRY)
            by_due: dict[str, list[Any]] = {}
            for s in subjects:
                policy = retry_of[s]
                if policy is not None and tries.get(s, 0) < policy.limit:
                    wait = policy.wait(tries.get(s, 0) + 1, self._rng)
                    by_due.setdefault(shift(now, wait), []).append(s)
            for due, group in by_due.items():
                touched += self.driver.conclude(name, group, status=status, now=now,
                                                lease=token, omit=(), reset=(),
                                                reschedule=due)
            retried = {s for group in by_due.values() for s in group}
            subjects = [s for s in subjects if s not in retried]
            if not subjects:
                return touched
        if n.loop is not None and status in n.loop.on:
            # THE WAY BACK, per subject: under the bound, the conclusion sends
            # it to `loop.to` in the same write; at the bound, it stands.
            reset = (n.loop.to, *sorted(descendants(n.loop.to, self.dag)))
            passes = self.driver.archived(subjects, n.loop.to, Reason.LOOP)
            back = [s for s in subjects if passes.get(s, 0) < n.loop.max]
            stay = [s for s in subjects if passes.get(s, 0) >= n.loop.max]
            if back:
                touched += self.driver.conclude(name, back, status=status, now=now,
                                                lease=token, omit=(), reset=reset,
                                                reschedule=None)
            if stay:
                touched += self.driver.conclude(name, stay, status=status, now=now,
                                                lease=token, omit=(), reset=(),
                                                reschedule=None)
            return touched
        if n.choice and status != Status.FAILED:
            if branch is None:
                raise ValueError(
                    f"node {name!r} is a choice: concluding it needs `branch=`, "
                    f"one of its children")
            omit = omitted_by(name, branch, self.dag)
        elif branch is not None:
            raise ValueError(
                f"`branch` given, but node {name!r} "
                f"{'failed' if n.choice else 'is not a choice'}")
        return touched + self.driver.conclude(name, subjects, status=status, now=now,
                                              lease=token, omit=omit, reset=(),
                                              reschedule=None)

    def fail(self, name: str, subjects: list[Any], *, token: str | None,
             branch: str | None = None) -> int:
        """This node did not produce. `failed` satisfies no child — unless a
        child's edge accepts it (`Node.on`). `branch` is accepted only to be
        refused: a failed choice names nothing."""
        return self.conclude(name, subjects, token=token, status=Status.FAILED,
                             branch=branch)

    def skip(self, name: str, *, candidates: Any, limit: int | None = None) -> int:
        """Mark this OPTIONAL node as given up, on the candidates.

        ONLY A CLAIMABLE NODE IS SKIPPED — parents concluded. Without that
        invariant, skipping a node on a subject not there yet would let its
        children start early, since they only name their DIRECT parents.

        `limit` BOUNDS A PASS: at most that many subjects, the first the
        candidates offer, and no more candidates read than needed to find
        them. A janitor calls again while a pass returns `limit`.
        """
        n = node(name, self.dag)
        if not n.optional:
            raise ValueError(
                f"node {name!r} is not optional — skipping it would move "
                f"the graph forward without its work")
        if limit is not None and int(limit) <= 0:
            return 0
        now = self._clock()
        # A DRIVER MAY SKIP IN ONE WRITE (`skip_where`, optional): the rule
        # for a plain join is SQL-shaped — no row, every parent satisfying —
        # and the contract proves the fast path equal to the loop below. A
        # custom join (`on`, `need`) always takes the loop.
        fast = getattr(self.driver, "skip_where", None)
        if fast is not None and not n.custom_join:
            # The limit is passed only when there is one: a `skip_where`
            # written before it existed keeps working unbounded.
            bound = {} if limit is None else {"limit": int(limit)}
            written = fast(name, candidates, parents=n.parents, now=now,
                           version=self.version, **bound)
        else:
            chosen: dict[tuple[Any, int], None] = {}
            for page in self.driver.scan(candidates, name=name,
                                         nodes=(name, *n.parents),
                                         parents=() if n.custom_join else n.parents,
                                         page=PAGE, now=now):
                for e in page:
                    if (self._mine(e) and name not in e.rows
                            and joined(name, self.dag, e.rows)):
                        chosen[(e.subject, e.revision)] = None
                        if limit is not None and len(chosen) >= limit:
                            break
                if limit is not None and len(chosen) >= limit:
                    break
            # One write, as for a claim.
            written = self.driver.insert_if_unchanged(
                name, list(chosen), status=Status.SKIPPED, now=now,
                lease=None) if chosen else []
        self._pin(written)
        return len(written)

    def adopt(self, name: str, subjects: list[Any]) -> int:
        """Record work ALREADY DONE that the journal does not know.

        For subjects older than the graph: with no row, they are neither
        claimable nor skippable downstream, and they wait without an error.
        The application ATTESTS the work — a reached state, a file — and
        the journal writes what is already true.

        NEVER OVERWRITES: a row that exists keeps its status, `failed`
        included.
        """
        node(name, self.dag)
        if not subjects:
            return 0
        count = self.driver.adopt(name, _unique(subjects), now=self._clock())
        self._pin(_unique(subjects))
        return count

    # -- undo --------------------------------------------------------------

    def forget(self, name: str, subjects: list[Any]) -> int:
        """Erase this node for these subjects — "never started".

        THE STEP BACK. A retry, a janitor, a replay: the subject goes back,
        and its node must become claimable again. The ABSENCE of a row is
        "never started"; a `to_redo` status would say the same thing while
        complicating the claim.
        """
        if not subjects:
            return 0
        node(name, self.dag)
        return self.driver.forget(name, _unique(subjects), now=self._clock())

    def release(self, name: str, older_than: str | datetime) -> int:
        """Give back the leases a dead worker has held for too long."""
        node(name, self.dag)
        return self.driver.release(name, older_than=stamp(older_than), now=self._clock(),
                                   version=self.version)

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        """What is recorded for ONE subject — exactly what `dag.claimable` reads.

        A JOURNAL ONLY SPEAKS ABOUT ITS OWN SUBJECTS: one pinned to another
        graph reads empty, as it claims empty. `migrate` deliberately looks
        past that, through the driver, since crossing is its whole job."""
        if self.version is not None:
            pinned = self.driver.versions([subject]).get(subject)
            if pinned is not None and pinned != self.version:
                return {}
        return self.driver.progress(subject)

    def expire(self) -> dict[str, int]:
        """Release, on every node declaring a `lease`, the rows held longer
        than it allows. Return `{node: count}` for the nodes that released
        something. Meant to be called on a schedule — a janitor — by the
        application: grampy runs no thread of its own."""
        now = self._clock()
        released: dict[str, int] = {}
        for n in self.dag:
            special = {policy: variant.lease for policy, variant in self._variants(n.name)
                       if variant.lease != n.lease}
            count = 0
            if n.lease is not None:
                count += self.driver.release(
                    n.name, older_than=shift(now, -seconds(n.lease)), now=now,
                    exclude=tuple(sorted(special)), version=self.version)
            for policy, lease in special.items():
                if lease is not None:
                    count += self.driver.release(
                        n.name, older_than=shift(now, -seconds(lease)), now=now,
                        only=(policy,), version=self.version)
            if count:
                released[n.name] = count
        return released

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        """Give these subjects a POLICY — their source. The graph's settings
        for that policy (retries, leases, timeouts, graces) then apply to
        them; the structure of the workflow stays the same for all."""
        if not subjects:
            return 0
        count = self.driver.enroll(_unique(subjects), policy)
        self._pin(_unique(subjects))
        return count

    def pinned(self, subject: Any) -> str | None:
        """The graph the subject is pinned to — a `Document.identity` — or
        None before its first write.

        IT IS WRITTEN ONCE. The first write pins the subject, and no later
        one moves it: only `migrate` changes it, on purpose."""
        return self.driver.versions([subject]).get(subject)

    def signal(self, subjects: list[Any], event: str, ref: str | None = None) -> int:
        """Record that `event` happened for these subjects — DURABLY, before
        any wait for it may have begun: a wait settles on a signal received
        since it last went back (a loop, a forget), however early."""
        if not subjects:
            return 0
        return self.driver.note(_unique(subjects), event, status=Outcome.RECEIVED,
                                reason=Reason.SIGNAL, now=self._clock(), ref=ref)

    def settle(self, candidates: Any, *, limit: int | None = None) -> dict[str, dict[str, int]]:
        """CONCLUDE WHAT NO WORKER DOES, on the candidates, for every node:

            wait      `done` when a signal was received since the node last
                      went back; `failed` once `timeout` has passed since
                      its parents concluded
            lane      a due arrival enters: the previous pass is archived and
                      the lane is `done` (`Lane`); reported as `entered`
            grace     an optional node still untaken `grace` after its
                      parents concluded is `skipped`

        Time runs from the LATEST accepted parent's conclusion; a node
        without parents has no clock and never times out. Return
        `{node: {status: count}}` for what was written. Meant for the
        application's janitor, like `expire`.

        `limit` BOUNDS A PASS: at most that many subjects written per node,
        the first in the order they would be taken — the candidates' order,
        a lane's places. A janitor calls again while a node's counts add up
        to `limit`."""
        if limit is not None and int(limit) <= 0:
            return {}
        now = self._clock()
        out: dict[str, dict[str, int]] = {}
        for n in self.dag:
            if n.lane is not None:
                entered = self._let_in(n, candidates, now, limit)
                if entered:
                    out.setdefault(n.name, {})[Outcome.ENTERED] = entered
                continue
            if n.wait is None and n.grace is None and all(
                    v.grace is None for _, v in self._variants(n.name)):
                continue
            after = tuple(sorted(descendants(n.name, self.dag)))
            entries = [e for page in self.driver.scan(
                           candidates, name=n.name, nodes=(n.name, *n.parents, *after),
                           parents=() if n.custom_join else n.parents, page=PAGE, now=now,
                           after=after)
                       for e in page]
            ready = list({e.subject: e for e in entries
                          if self._mine(e)
                          and claimable(n.name, self.dag, _due_away(n.name, e, now))}.values())
            if not ready:
                continue
            decided: dict[str, list[tuple[Any, int]]] = {}
            subjects = [e.subject for e in ready]
            heard: dict[Any, str] = {}
            went_back: dict[Any, str] = {}
            if n.wait is not None:
                heard = self.driver.latest(subjects, n.wait, Reason.SIGNAL)
                went_back = self.driver.latest(subjects, n.name, None)
            for e in ready:
                if limit is not None and sum(map(len, decided.values())) >= limit:
                    break
                since = _joined_since(n, e)
                seen_by = self.settings(n.name, e.policy)
                if n.wait is not None:
                    at = heard.get(e.subject)
                    if at is not None and at >= went_back.get(e.subject, ""):
                        decided.setdefault(Status.DONE, []).append((e.subject, e.revision))
                        continue
                    if (seen_by.timeout is not None and since is not None
                            and shift(since, seconds(seen_by.timeout)) <= now):
                        decided.setdefault(Status.FAILED, []).append((e.subject, e.revision))
                elif (since is not None and seen_by.grace is not None
                      and shift(since, seconds(seen_by.grace)) <= now):
                    decided.setdefault(Status.SKIPPED, []).append((e.subject, e.revision))
            for status, chosen in decided.items():
                written = self.driver.insert_if_unchanged(n.name, chosen, status=status,
                                                          now=now, lease=None)
                self._pin(written)
                if written:
                    out.setdefault(n.name, {})[status] = len(written)
        return out

    def history(self, subject: Any) -> list[dict[str, Any]]:
        """Every row taken away from ONE subject — by `forget`, `release` or
        a loop — oldest first, each with `archived_at` and `reason`."""
        return self.driver.history(subject)

    #: THE HISTORY ROWS A BOUND COUNTS — a retry limit, a loop's `max`.
    #: Pruning never touches them: they are the counters.
    COUNTED = (Reason.RETRY, Reason.LOOP)

    def prune_history(self, before: str | datetime) -> int:
        """KEEP THE HISTORY FROM GROWING FOREVER: delete what was archived
        before `before`, except the rows a bound counts (`COUNTED`).

        Forgets, releases, lane notes and signals go; the `retry` and `loop`
        rows stay, whatever their age. A subject pruned, then brought back —
        a replay, a requeue, a late result — keeps the retries and the loop
        passes it had used: a limit cannot be reset by a clean-up. Meant for
        the application's janitor, like `expire`. Return the count deleted.
        """
        return self.driver.prune_history(stamp(before), keep=self.COUNTED)

    def passes(self, subject: Any, name: str) -> int:
        """How many times a declared loop sent this subject back through
        `name` — what `Loop.max` bounds."""
        node(name, self.dag)
        return self.driver.archived([subject], name, Reason.LOOP).get(subject, 0)

    def retries(self, subject: Any, name: str) -> int:
        """How many times a failure of `name` was retried for this subject."""
        node(name, self.dag)
        return self.driver.archived([subject], name, Reason.RETRY).get(subject, 0)

    def stages(self, subjects: list[Any], *,
               at: str | datetime) -> dict[str, list[list[Any]]]:
        """What a BATCH went through — `{subject: [[node, end, seconds, status], …]}`.

        ONE READ FOR THE BATCH, not one per subject. ONLY NODES THAT WORKED:
        a `skipped` row never started. A node STILL RUNNING ends `at`.
        """
        _require(self.driver, "reading", "journal.stages")
        return self.driver.stages(list(subjects), at=stamp(at))

    def counts(self, name: str) -> dict[str, int]:
        """How many subjects stand where, for this node."""
        n = node(name, self.dag)
        _require(self.driver, "reading", "journal.counts")
        by_status = self.driver.status_counts(name)
        out: dict[str, int] = {status: int(by_status.get(status, 0))
                               for status in (Status.RUNNING, Status.SCHEDULED, *NODE_CONCLUDED)}
        if n.lane is not None:
            out[Outcome.WAITING] = int(self.driver.queued(name))
        return out

    def node_for_state(self, state: str) -> str | None:
        """The node whose WORKING state this is, if any.

        `check_dag` refuses two nodes posting the same state, so this returns
        at most one name. `None` is not an error: a state may belong to no
        node.
        """
        for n in self.dag:
            if n.working == state:
                return n.name
        return None


def _joined_since(n: Node, entry: Entry) -> str | None:
    """When the node became joined: the latest conclusion among the parents
    it accepts. None for a root, or when no conclusion time is known."""
    times = [entry.finished[p] for p in n.parents
             if entry.rows.get(p) in accepts(n, p) and p in entry.finished]
    return max(times) if times else None

def _due_away(name: str, entry: Entry, now: str) -> dict[str, str]:
    """The rows the rule reads: a `scheduled` row of `name` due by `now`
    counts as absent — the node may be taken again."""
    if entry.rows.get(name) == Status.SCHEDULED and entry.due.get(name, now) <= now:
        return {k: v for k, v in entry.rows.items() if k != name}
    return entry.rows
