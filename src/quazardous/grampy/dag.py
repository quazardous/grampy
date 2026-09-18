"""The graph: nodes, their parents, and the rule that says what can run.

    Node             a node: who it descends from, how it joins, what it projects
    node             the named node, in THIS graph
    accepts          the statuses of a parent this node accepts
    joined           are enough parents concluded the way this node accepts?
    omitted_by       what a choice leaves dead when it takes one branch
    Loop             a declared way back: which statuses send the subject where
    Lane             a declared way in: how a subject coming back waits
    Group            subjects worked together: a whole group or none
    descendants      everything downstream of a node
    ancestors        everything upstream of a node
    claimable        can this node be taken, given what is recorded?
    claimable_nodes  every node that can be taken right now
    check_dag        refuse a graph that does not hold together

This module knows no graph and no storage. A caller declares its own
graph as a tuple of `Node`, and a *progress* — `{node: status}` for one
subject — is all the rule needs. A missing node has never started: that
is the third state, and it costs no column.

────────────────────────────────────────────────────────────────────────
THE RULE IS PURE, AND THAT IS THE POINT
────────────────────────────────────────────────────────────────────────

"All my parents are concluded, nobody holds me, and no descendant has
started" used to exist only as a thirty-line `INSERT … SELECT`. Testing
it needed a database, so few cases were written, so some were missed —
and the "never go backwards" guard silently disappeared once, found only
by an end-to-end test one migration later.

Here a case is two lines: an invented graph, a progress, an answer. A
storage driver becomes an IMPLEMENTATION of this rule, and the shared
driver contract confronts the two (`grampy.testing`).
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from .names import MERGE_FN, Merge, Name, Per, Position, Status, WhileRunning
from .timing import Rate, Retry, canonical, seconds


class DagError(ValueError):
    """A graph that does not hold together, or a node nobody declared."""


#: WHAT A NODE CAN BE WORTH TO ITS CHILDREN.
#:
#:     running   someone holds it — the lease, nothing more
#:     done      finished, it produced what it had to
#:     skipped   we stopped waiting for it (optional node, grace expired)
#:     omitted   a choice took another branch: it will never run
#:
#: `done`, `skipped` AND `omitted` ALL SATISFY A CHILD BY DEFAULT, on
#: purpose: a child does not need to know WHY its parent is concluded, only
#: that it will not be kept waiting. The distinction is for MEASURING, not
#: for deciding. (A child of an omitted node never gets there: it is omitted
#: with it — see `omitted_by`.)
NODE_RUNNING = Status.RUNNING
#: WAITING TO BE TAKEN AGAIN — a retry's row: `started_at` is when it is
#: due. Until then it holds the node like a running row; once due, a claim
#: takes it as if the row were absent.
NODE_SCHEDULED = Status.SCHEDULED
NODE_DONE = Status.DONE
NODE_SKIPPED = Status.SKIPPED
NODE_OMITTED = Status.OMITTED
#: THE END THAT SATISFIES NOBODY — BY DEFAULT.
#:
#: `failed` says "I did not produce what you expected". A child can NOT
#: start on it — it would work on an input that does not exist — unless it
#: says so on that edge: a compensation, an alert (`Node.on`).
NODE_FAILED = Status.FAILED
#: WHAT SATISFIES A CHILD, unless the child's edge says otherwise.
NODE_SATISFYING = (NODE_DONE, NODE_SKIPPED, NODE_OMITTED)
#: WHAT IS NEVER TAKEN AGAIN. Going back means FORGETTING the row.
NODE_CONCLUDED = (NODE_DONE, NODE_SKIPPED, NODE_FAILED, NODE_OMITTED)


def _member(kind: type[Name], value: str) -> str:
    """The member for a string that names one; anything else as given, for
    `check_dag` to refuse with the node's name."""
    try:
        return kind(value)
    except ValueError:
        return value


@dataclass(frozen=True)
class Loop:
    """A DECLARED WAY BACK. When the node carrying it concludes with a status
    of `on`, the subject returns to `to` — an ancestor, or the node itself:
    `to` and everything after it are archived and become claimable again,
    in the same write as the conclusion. At most `max` times per subject;
    after that the conclusion stands, and a failure edge can take over
    (escalate, give up).

        Node("review", parents=("draft",), loop=Loop(to="draft", max=3))
    """

    to: str
    max: int
    on: tuple[str, ...] = (Status.FAILED,)

    def __post_init__(self) -> None:
        object.__setattr__(self, "on", tuple(_member(Status, s) for s in self.on))


#: HOW A LANE MERGES A NEW ARRIVAL INTO ONE ALREADY WAITING, and where the
#: merged arrival stands in the lane. A merge may also be `fn:<name>`, a
#: function the journal is given — see `Lane.merge`.
LANE_MERGES = (Merge.FIRST, Merge.LAST, Merge.ALL)
#: WHAT NAMES A MERGE FUNCTION rather than one of the modes above.
LANE_MERGE_FN = MERGE_FN
LANE_POSITIONS = tuple(Position)
#: WHAT AN ARRIVAL DOES WHILE A PASS OF THE SUBJECT IS STILL RUNNING.
LANE_WHILE_RUNNING = tuple(WhileRunning)


@dataclass(frozen=True)
class Lane:
    """A DECLARED WAY IN: the subject comes back — a new version of the same
    thing — and waits here before it runs through the graph again.

    An arrival (`journal.arrive`) waits in the lane; `journal.settle` lets it
    in once it is due, and the pass that went before — this node and
    everything after it — is archived in the same write (reason `arrival`).

        merge          first: the arrival waiting keeps its `ref`, later ones
                       are dropped; last: the latest `ref` wins; all: every
                       `ref` is kept, in the order they arrived, at most
                       `max_size` of them; `fn:<name>`: a function decides,
                       given to the journal as `mergers={"<name>": fn}`.
                       A `ref` NAMES WHAT CAME BACK — opaque to grampy, and
                       nothing to do with `document.version`, the graph's
        max_size       with `all`, how many refs are kept — beyond it the
                       OLDEST is let go, and noted in the history
        position       first: a merged arrival keeps the place of the first;
                       last: it goes to the back, as if it had just arrived
        cooldown       not before this long after the previous pass ended —
                       the same subject is not run twice within the window
        delay          not before this long after its place — quiet time
        max_wait       whatever the above, not later than this long after the
                       first arrival: a subject that keeps coming back still
                       gets through
        while_running  queue: an arrival during a running pass waits for it
                       to end; skip: it is dropped, and noted in the history

    Presets under the names other tools gave them:

        Lane.throttle(cooldown)   last ref, place of the first
                                  (Graphile Worker `preserve_run_at`)
        Lane.debounce(delay)      last ref, to the back (Lodash debounce,
                                  Graphile Worker `replace`)
        Lane.dedupe()             first ref, the rest dropped
        Lane.batch(max_size)      every ref, in order (the aggregator of
                                  Enterprise Integration Patterns)
    """

    merge: str = Merge.LAST
    position: str = Position.FIRST
    cooldown: float | int | str | None = None
    delay: float | int | str | None = None
    max_wait: float | int | str | None = None
    while_running: str = WhileRunning.QUEUE
    max_size: int = 100

    def __post_init__(self) -> None:
        for name in ("cooldown", "delay", "max_wait"):
            object.__setattr__(self, name, canonical(getattr(self, name)))
        object.__setattr__(self, "merge", _member(Merge, self.merge))
        object.__setattr__(self, "position", _member(Position, self.position))
        object.__setattr__(self, "while_running", _member(WhileRunning, self.while_running))

    @classmethod
    def throttle(cls, cooldown: float | int | str,
                 max_wait: float | int | str | None = None) -> Lane:
        return cls(merge=Merge.LAST, position=Position.FIRST, cooldown=cooldown,
                   max_wait=max_wait)

    @classmethod
    def debounce(cls, delay: float | int | str,
                 max_wait: float | int | str | None = None) -> Lane:
        return cls(merge=Merge.LAST, position=Position.LAST, delay=delay, max_wait=max_wait)

    @classmethod
    def dedupe(cls, cooldown: float | int | str | None = None) -> Lane:
        return cls(merge=Merge.FIRST, position=Position.FIRST, cooldown=cooldown)

    @classmethod
    def batch(cls, cooldown: float | int | str | None = None, *,
              max_size: int = 100, max_wait: float | int | str | None = None) -> Lane:
        return cls(merge=Merge.ALL, position=Position.FIRST, cooldown=cooldown,
                   max_wait=max_wait, max_size=max_size)

    @property
    def merger(self) -> str | None:
        """The name of the function this lane merges with, if it does."""
        return (self.merge[len(LANE_MERGE_FN):]
                if self.merge.startswith(LANE_MERGE_FN) else None)

    @property
    def keeps_every_ref(self) -> bool:
        """True when a merge has to read what waits before writing: the
        journal does those under the driver's guard."""
        return self.merge == Merge.ALL or self.merger is not None


@dataclass(frozen=True)
class Group:
    """SUBJECTS WORKED TOGETHER: the claim hands out a whole group or none.

    A node that groups is not claimed one subject at a time. The claim
    gathers `size` subjects sharing a key and hands them back under ONE
    lease, so the worker does one thing with all of them — a bag of five
    bricks of a colour, a feed file of ten thousand lines, one call to a
    service that charges per call.

        size       how many go together
        max_wait   past this long after the OLDEST member became ready, an
                   incomplete group goes anyway. Without it a rare key waits
                   for as long as it takes to fill — which is sometimes what
                   you want, and sometimes starvation; see `docs/rules.md`
        per_key    False gathers any subjects, whatever their key: a feed
                   file of ten thousand of anything

    THE KEY IS NOT GRAMPY'S. It travels with the candidates — a second
    column of your query, a `(subject, key)` pair in a plain iterable — and
    is compared, never read, like a lane's `ref` or a subject's policy.

    Nothing is stored while a group fills: a node whose group is short is
    simply not claimable. There is no half-gathered state to clean up.
    """

    size: int
    max_wait: float | int | str | None = None
    per_key: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_wait", canonical(self.max_wait))


@dataclass(frozen=True)
class Node:
    """A node of the graph: who it descends from, and what it projects.

    `parents` IS THE ONLY DECLARATION OF ORDER. Empty means it starts from
    the root — a freshly submitted subject. Parents are DIRECT, never
    transitive: a redundant declaration ends up diverging.

    `working` and `state` ARE PROJECTIONS, NOT COMMANDS. An application
    may mirror a simple label on its subject — the one it carries while
    the node runs, and the one it reaches when the node concludes — so
    that whatever counts and displays keeps reading one word. Nothing in
    this package reads them to decide what to claim: the graph decides.
    `None` for a node that does not move the subject.

    `optional` says a failure of this node degrades without blocking:
    once a grace delay has passed, the node is marked `skipped` and its
    children go on. Only a CLAIMABLE node may be skipped (its own parents
    concluded), otherwise its children would start before its parents.

    `once` says a REPLAY DOES NOT REDO IT. Its row is forgotten with the
    rest of the downstream — otherwise its parent could not be claimed
    again — but its guard lives elsewhere, in the application.

    ────────────────────────────────────────────────────────────────────
    HOW A NODE JOINS ITS PARENTS — data, not code
    ────────────────────────────────────────────────────────────────────

    `on` says, per parent, which of its statuses this node accepts;
    a parent not named accepts `NODE_SATISFYING`. A compensation that runs
    when a reservation failed and the payment went through:

        Node("refund", parents=("pay", "reserve"), on={"reserve": (Status.FAILED,)})

    `need` is how many parents must be accepted — all of them when `None`.
    `need=2` over three engines starts as soon as two agree; the third,
    not started yet, is closed by it (a descendant has started).

    `choice` marks a node that concludes by NAMING one of its children: the
    others, and whatever only they lead to, are `omitted` in the same
    write (exclusive choice). A join after the branches goes on, since
    `omitted` satisfies it.

    `loop` declares a way back (`Loop`): the graph stays acyclic, the cycle
    is an edge the journal takes, bounded, and every pass is kept in the
    history.

    `lease` is how long a worker may hold this node (`"10m"`): past it,
    `journal.expire()` gives the row back as if the worker had died. A
    fetch and an AI call do not deserve the same patience.

    `wait` makes the node an ATTENDED EVENT rather than work: no worker
    claims it; `journal.settle` concludes it `done` once a signal of that
    name was received for the subject — before the wait began included —
    or `failed` when `timeout` has passed since its parents concluded.

    `grace` gives an optional node that long, once its parents concluded,
    before `journal.settle` skips it.

    `rate` (bands of `timing.Rate`) and `concurrency` protect what the node
    uses: a claim takes no more than the bands let through, nor more than
    `concurrency` rows running at once. `per=Per.POLICY` gives each
    policy its own budget; `per=Per.ALL` shares one.

    `lane` makes the node a WAY IN for subjects that come back (`Lane`): no
    worker claims it; `journal.arrive` puts a subject in it, `journal.settle`
    lets it through, and each pass through what follows is archived when the
    next one enters.

    `retry` declares what a failure does first (`timing.Retry`): archived,
    and the node scheduled again after a delay, a bounded number of times.
    Only past the retries does the failure stand — and a loop or a failure
    edge see it.
    """

    name: str
    parents: tuple[str, ...] = field(default_factory=tuple)
    working: str | None = None
    state: str | None = None
    optional: bool = False
    once: bool = False
    on: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    need: int | None = None
    choice: bool = False
    loop: Loop | None = None
    retry: Retry | None = None
    lease: float | int | str | None = None
    wait: str | None = None
    timeout: float | int | str | None = None
    grace: float | int | str | None = None
    rate: tuple[Rate, ...] = ()
    concurrency: int | None = None
    per: str = Per.ALL
    lane: Lane | None = None
    group: Group | None = None

    def __post_init__(self) -> None:
        for name in ("lease", "timeout", "grace"):
            object.__setattr__(self, name, canonical(getattr(self, name)))
        object.__setattr__(self, "parents", tuple(self.parents))
        object.__setattr__(self, "rate", tuple(self.rate))
        object.__setattr__(self, "on", _Frozen(
            (parent, tuple(_member(Status, x) for x in statuses))
            for parent, statuses in dict(self.on).items()))
        object.__setattr__(self, "per", _member(Per, self.per))

    @property
    def custom_join(self) -> bool:
        """True when the node joins otherwise than "every parent satisfying"."""
        return bool(self.on) or self.need is not None


class _Frozen(dict):
    """A read-only, hashable mapping — a frozen dataclass field must hash."""

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(tuple(sorted(self.items())))

    def _refuse(self, *args: object, **kwargs: object) -> None:
        raise TypeError("Node.on is read-only")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _refuse  # type: ignore[assignment]


def node(name: str, dag: tuple[Node, ...]) -> Node:
    """The named node. Raises rather than returning `None`: a caller that
    got `None` would put an empty name in a query, and the claim would
    simply return nothing."""
    for candidate in dag:
        if candidate.name == name:
            return candidate
    raise DagError(f"unknown node: {name!r}")


def descendants(name: str, dag: tuple[Node, ...]) -> set[str]:
    """Every node downstream of this one, directly or not.

    WHAT THEY ARE FOR: NEVER GOING BACKWARDS. A node must not be claimed
    once one of its descendants has started — the subject has moved PAST
    it, and taking it again would rewind its projection over newer work.
    A state-based queue gets this guard for free (`WHERE state = source`);
    a graph does not, so it is written here.
    """
    by_parent: dict[str, list[str]] = {}
    for n in dag:
        for parent in n.parents:
            by_parent.setdefault(parent, []).append(n.name)

    seen: set[str] = set()
    to_visit = list(by_parent.get(name, ()))
    while to_visit:
        current = to_visit.pop()
        if current in seen:
            continue
        seen.add(current)
        to_visit.extend(by_parent.get(current, ()))
    return seen


def ancestors(name: str, dag: tuple[Node, ...]) -> set[str]:
    """Every node upstream of this one, directly or not.

    The mirror of `descendants`, and together they answer a repair:

        descendants   what to TRUNCATE — downstream of the point reached
        ancestors     what to ADOPT    — what the reached state attests

    A sibling branch is NOT an ancestor, which is why this goes through
    the graph rather than a list: a subject can reach a state without a
    parallel branch ever having run, and adopting it would invent work.
    """
    by_name = {n.name: n for n in dag}
    node(name, dag)                      # raises on an unknown node

    seen: set[str] = set()
    to_visit = list(by_name[name].parents)
    while to_visit:
        current = to_visit.pop()
        if current in seen:
            continue
        seen.add(current)
        to_visit.extend(by_name[current].parents)
    return seen


def accepts(child: Node, parent: str) -> tuple[str, ...]:
    """The statuses of `parent` that `child` accepts."""
    return tuple(child.on.get(parent, NODE_SATISFYING))


def joined(name: str, dag: tuple[Node, ...], progress: dict[str, str]) -> bool:
    """ENOUGH PARENTS CONCLUDED THE WAY THIS NODE ACCEPTS — `need` of them,
    all when `need` is None. A root has nothing to wait for."""
    n = node(name, dag)
    accepted = sum(1 for p in n.parents if progress.get(p) in accepts(n, p))
    return accepted >= (len(n.parents) if n.need is None else n.need)


def claimable(name: str, dag: tuple[Node, ...],
              progress: dict[str, str]) -> bool:
    """Can this node be taken, given what is already recorded?

    THREE REFUSALS, AND THEY DO NOT MEAN THE SAME THING:

        a row already exists      someone holds it, or it is concluded
        a descendant has started  the subject has moved PAST it
        not joined                its input does not exist yet (`joined`)

    By default `done`, `skipped` and `omitted` satisfy; `failed` does not.
    """
    node(name, dag)                      # raises on an unknown node
    if name in progress:
        return False
    if any(d in progress for d in descendants(name, dag)):
        return False
    return joined(name, dag, progress)


def omitted_by(choice: str, branch: str, dag: tuple[Node, ...]) -> tuple[str, ...]:
    """WHAT DIES WHEN `choice` TAKES `branch`: the other children of the
    choice, and every node that can only be reached through them.

    Reachability, not descendance: remove the branches not taken, and a
    node that can no longer be reached from the root is dead. A join that
    the taken branch also reaches stays alive — `omitted` satisfies it.
    Returned in declaration order.
    """
    n = node(choice, dag)
    if not n.choice:
        raise DagError(f"node {choice!r} is not a choice")
    children = [c.name for c in dag if choice in c.parents]
    if branch not in children:
        raise DagError(f"{branch!r} is not a branch of {choice!r} — expected one of {children}")
    cut = {c for c in children if c != branch}
    reachable: set[str] = set()
    grew = True
    while grew:                          # a fixpoint: no order assumed
        grew = False
        for c in dag:
            if c.name in cut or c.name in reachable:
                continue
            if not c.parents or any(p in reachable for p in c.parents):
                reachable.add(c.name)
                grew = True
    return tuple(c.name for c in dag if c.name not in reachable)


def claimable_nodes(dag: tuple[Node, ...],
                    progress: dict[str, str]) -> set[str]:
    """EVERY node that can be taken now. Empty means the subject is
    finished, blocked by a failure, or entirely in progress.

    This is what makes PARALLELISM visible: a fork returns two names, and
    reading it needs neither a database nor a worker.
    """
    return {n.name for n in dag if claimable(n.name, dag, progress)}


def check_dag(dag: tuple[Node, ...]) -> None:
    """Refuse a graph that does not hold together. Call it at startup.

    FAIL WHEN THE GRAPH IS DECLARED, not three weeks later as a quality
    statistic.
    """
    by_name: dict[str, Node] = {}
    for n in dag:
        if n.name in by_name:
            raise DagError(f"two nodes are named {n.name!r}")
        by_name[n.name] = n

    for n in dag:
        for parent in n.parents:
            if parent not in by_name:
                raise DagError(
                    f"node {n.name!r} descends from {parent!r}, "
                    f"which does not exist")
        if len(set(n.parents)) != len(n.parents):
            raise DagError(f"node {n.name!r} names a parent twice")

    # ── A JOIN SAYS WHAT IT CAN SAY ────────────────────────────────
    #
    # An edge setting that names no parent, a status that does not exist, a
    # `need` no count of parents can reach: each would make a node wait
    # forever, or start on nothing, without a word.
    for n in dag:
        for parent, statuses in n.on.items():
            if parent not in n.parents:
                raise DagError(f"node {n.name!r}: `on` names {parent!r}, not one of its parents")
            if not statuses:
                raise DagError(f"node {n.name!r}: `on` accepts nothing from {parent!r}")
            unknown = [x for x in statuses if x not in NODE_CONCLUDED]
            if unknown:
                raise DagError(
                    f"node {n.name!r}: `on` accepts unknown status(es) {unknown} from "
                    f"{parent!r} — expected among {list(NODE_CONCLUDED)}")
        if n.need is not None and not 1 <= n.need <= len(n.parents):
            raise DagError(
                f"node {n.name!r}: need={n.need} but it has {len(n.parents)} parent(s)")
        if n.choice and not any(n.name in c.parents for c in dag):
            raise DagError(f"node {n.name!r} is a choice without any branch")
        if n.choice and n.optional:
            raise DagError(
                f"node {n.name!r} is an optional choice: skipped, it would name no "
                f"branch and every branch would run")
        if n.loop is not None:
            if n.loop.to != n.name and n.loop.to not in _ancestors(n.name, by_name):
                raise DagError(
                    f"node {n.name!r} loops to {n.loop.to!r}, which is neither itself "
                    f"nor one of its ancestors")
            if n.loop.max < 1:
                raise DagError(f"node {n.name!r}: a loop needs max >= 1, got {n.loop.max}")
            if not n.loop.on or any(x not in (Status.DONE, Status.SKIPPED, Status.FAILED)
                                    for x in n.loop.on):
                raise DagError(
                    f"node {n.name!r}: a loop fires on done, skipped or failed, "
                    f"got {list(n.loop.on)}")
            if n.choice:
                raise DagError(f"node {n.name!r} is a choice and a loop: pick one")
        for label in ("lease", "timeout", "grace"):
            value = getattr(n, label)
            if value is None:
                continue
            try:
                if seconds(value) <= 0:
                    raise ValueError("a duration must last")
            except ValueError as exc:
                raise DagError(f"node {n.name!r}: {label} {value!r} — {exc}") from exc
        if n.wait is not None:
            if not n.wait:
                raise DagError(f"node {n.name!r}: wait needs an event name")
            unfit = [label for label in ("choice", "loop", "retry", "lease")
                     if getattr(n, label)]
            if unfit:
                raise DagError(
                    f"node {n.name!r} waits for {n.wait!r}: it is settled, never "
                    f"worked, so it cannot take {unfit}")
        elif n.timeout is not None:
            raise DagError(f"node {n.name!r}: a timeout needs a wait")
        if n.per not in tuple(Per):
            raise DagError(f"node {n.name!r}: per={n.per!r} — expected one of "
                           f"{[p.value for p in Per]}")
        if n.concurrency is not None and n.concurrency < 1:
            raise DagError(f"node {n.name!r}: concurrency {n.concurrency} — at least 1")
        if any(not isinstance(band, Rate) for band in n.rate):
            raise DagError(f"node {n.name!r}: rate takes Rate bands")
        if n.wait is not None and (n.rate or n.concurrency is not None):
            raise DagError(f"node {n.name!r} waits: it is never claimed, so it takes no "
                           f"rate or concurrency")
        if n.grace is not None and not n.optional:
            raise DagError(f"node {n.name!r}: grace skips an optional node — this one is not")
        if n.lane is not None:
            _check_lane(n)
        if n.group is not None:
            _check_group(n)


    # ── NO CYCLE, PROVEN BY A WALK ─────────────────────────────────
    #
    # A cycle is invisible to the eye past a handful of nodes, and it
    # would not raise: none of its nodes would ever become claimable, and
    # the queue would stop there, silently.
    seen: set[str] = set()
    in_progress: set[str] = set()

    def descend(name: str) -> None:
        if name in in_progress:
            raise DagError(f"cycle in the graph, through {name!r}")
        if name in seen:
            return
        in_progress.add(name)
        for parent in by_name[name].parents:
            descend(parent)
        in_progress.discard(name)
        seen.add(name)

    for n in dag:
        descend(n.name)

    # ── ONE ROOT ───────────────────────────────────────────────────
    #
    # Two parentless nodes would be two entry points, and nothing would say
    # which one a fresh subject takes.
    roots = [n.name for n in dag if not n.parents]
    if len(roots) != 1:
        raise DagError(
            f"the graph has {len(roots)} root(s): {roots}. "
            f"A fresh subject would not know where to enter.")

    # ── A PROJECTED STATE HAS ONE AUTHOR ───────────────────────────
    #
    # Two nodes posting the same state would make the label ambiguous: one
    # would read it without knowing which node wrote it, and a janitor
    # would take back the wrong one.
    posted_by: dict[str, str] = {}
    for n in dag:
        for state in (n.working, n.state):
            if state is None:
                continue
            if state in posted_by and posted_by[state] != n.name:
                raise DagError(
                    f"state {state!r} is posted by {posted_by[state]!r} AND by "
                    f"{n.name!r} — the label becomes ambiguous")
            posted_by[state] = n.name


def _check_group(n: Node) -> None:
    """A group gathers subjects a worker takes together; it cannot sit on a
    node no worker claims, nor on one that concludes by naming a branch."""
    group = n.group
    assert group is not None
    if not isinstance(group, Group):
        raise DagError(f"node {n.name!r}: group takes a Group")
    if group.size < 1:
        raise DagError(f"node {n.name!r}: group size={group.size!r} — a group of "
                       f"fewer than one subject is not a group")
    if group.max_wait is not None:
        try:
            if seconds(group.max_wait) <= 0:
                raise ValueError("a duration must last")
        except ValueError as exc:
            raise DagError(f"node {n.name!r}: group max_wait "
                           f"{group.max_wait!r} — {exc}") from exc
    unfit = [label for label in ("wait", "lane", "choice")
             if getattr(n, label) not in (None, False)]
    if unfit:
        raise DagError(
            f"node {n.name!r} groups its subjects, so a worker takes them "
            f"together: it cannot also take {unfit}")


def _check_lane(n: Node) -> None:
    lane = n.lane
    assert lane is not None
    if not isinstance(lane, Lane):
        raise DagError(f"node {n.name!r}: lane takes a Lane")
    for label, value, allowed in (("position", lane.position, LANE_POSITIONS),
                                  ("while_running", lane.while_running, LANE_WHILE_RUNNING)):
        if value not in allowed:
            raise DagError(f"node {n.name!r}: lane {label}={value!r} — expected one of "
                           f"{list(allowed)}")
    if lane.merge not in LANE_MERGES and not lane.merger:
        raise DagError(f"node {n.name!r}: lane merge={lane.merge!r} — expected one of "
                       f"{list(LANE_MERGES)}, or {LANE_MERGE_FN!r} and the name of a "
                       f"function the journal is given")
    if lane.merger == "":
        raise DagError(f"node {n.name!r}: lane merge={lane.merge!r} names no function")
    if lane.max_size < 1:
        raise DagError(f"node {n.name!r}: lane max_size={lane.max_size!r} — keeping "
                       f"fewer than one ref keeps nothing")
    for label in ("cooldown", "delay", "max_wait"):
        value = getattr(lane, label)
        if value is None:
            continue
        try:
            if seconds(value) <= 0:
                raise ValueError("a duration must last")
        except ValueError as exc:
            raise DagError(f"node {n.name!r}: lane {label} {value!r} — {exc}") from exc
    unfit = [label for label in ("wait", "choice", "loop", "retry", "lease", "grace",
                                 "optional", "concurrency")
             if getattr(n, label) not in (None, False)]
    if unfit:
        raise DagError(f"node {n.name!r} is a lane: it is entered, never worked, so it "
                       f"cannot take {unfit}")


def _ancestors(name: str, by_name: dict[str, Node]) -> set[str]:
    """Ancestors from a name index — check_dag's own walk, before `node()`
    can be trusted on a graph not yet checked. Assumes the parents exist."""
    seen: set[str] = set()
    to_visit = list(by_name[name].parents)
    while to_visit:
        current = to_visit.pop()
        if current in seen or current not in by_name:
            continue
        seen.add(current)
        to_visit.extend(by_name[current].parents)
    return seen
