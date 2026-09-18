"""SPEAK OBJECTS, NOT IDS — the canonical way to use grampy.

    class Bricks(Adapter):
        def id_of(self, brick):      return brick.id
        def policy_of(self, brick): return brick.crate
        def applies(self, brick, node):
            return node != "polish" or brick.crate == "factory"

    items = Items(journal, Bricks())
    items.admit(bricks)                          # policy read from the item
    lease = items.claim("sort", 10, candidates=query)
    for brick in lease:                          # OBJECTS, loaded in one call
        ...
    items.conclude("sort", lease)
    items.settle(candidates=waiting)             # the janitor, in items' terms

────────────────────────────────────────────────────────────────────────
WHERE THE LINE IS
────────────────────────────────────────────────────────────────────────

The core takes DECISIONS, never CRITERIA. It is given an id, a label, the
name of a branch — never a rule to evaluate, never a payload to look
inside. That is what keeps a workflow from learning what "source" means.

This layer is the translator: it holds the handlers that read the
application's own objects and turns their answers into the calls the
journal already understands. It runs in the application's process, where
the data is. The core never calls a handler, `dag` stays pure, and the
drivers are untouched.

It translates, it does not become a framework: no retry loop, no logging,
no worker lifecycle. Everything here is a call the id-based API could have
made by hand — the layer only spares the application from making it.

────────────────────────────────────────────────────────────────────────
ONE WORKFLOW, SEVERAL KINDS OF SUBJECT
────────────────────────────────────────────────────────────────────────

`applies(item, node)` is how one graph serves subjects that differ
slightly. An OPTIONAL node the item refuses is claimed and concluded
`skipped` in the same call, so nothing downstream waits for it and the
journal records that the decision was taken, rather than the step
silently never happening.

That spends a claim on a step not done. A hot path may prefer to leave
those subjects out of `candidates` in the first place — which subjects a
worker offers has always been the application's sentence — and keep a
policy `grace` as the safety net for the ones nobody takes.
"""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

from .dag import node
from .journal import Lease, NodeJournal
from .names import Status


def _are_items(candidates: Any) -> bool:
    """Your objects, rather than something a driver reads itself.

    Anything iterable is yours — a list, a tuple, a generator. A driver's
    query is not: a SQLAlchemy statement is not iterable, and grampy's
    `Query` is a named tuple, so it is told apart by the `sql` it carries.
    """
    if isinstance(candidates, (str, bytes)) or hasattr(candidates, "sql"):
        return False
    return isinstance(candidates, Iterable)


class Adapter:
    """HOW GRAMPY HOOKS ONTO ONE OF YOUR OBJECTS.

    One method is yours to write — `id_of`. The rest have answers that suit
    an application with nothing special to say, `inflate` included: it lets
    your objects straight through, so it only needs writing when ids come in.
    """

    # -- required ----------------------------------------------------------

    def id_of(self, candidate: Any) -> Any:
        """The subject id of a candidate — unique, stable, int or str.

        IT MAY ALREADY BE ONE. `inflate` runs first and hands on whatever it
        chose to let through, so an application that works in ids, or that
        mixes them with objects, writes this and nothing else:

            def id_of(self, candidate):
                return getattr(candidate, "id", candidate)

        The other handlers below are asked about the DATA, so they only ever
        see what `inflate` turned into an item.
        """
        raise NotImplementedError

    # -- required only when a driver's query names the candidates -----------

    def inflate(self, candidates: Sequence[Any]) -> Iterable[Any]:
        """WHATEVER THE CALLER HAD, AS ITEMS — in one call, your storage.

        The layer hands you the batch exactly as it received it and asks for
        objects back. Yours already? Let them through, which is what the
        default does. Ids, or a mix? Fetch what needs fetching, and only
        that: you are the one who can tell them apart.

            def inflate(self, candidates):
                thin = [c for c in candidates if isinstance(c, int)]
                fat = {b.id: b for b in Brick.objects.filter(id__in=thin)}
                return [fat.get(c, c) for c in candidates]

        Order does not matter, and a candidate nothing comes back for may be
        left out: it lands in `ItemLease.missing`.

        THIS IS ALSO WHERE IDS COME BACK FROM A DRIVER'S QUERY — the storage
        named the candidates, so the claim has ids and nothing attached. An
        adapter that never overrides this is told, rather than handed ids it
        would treat as objects.
        """
        return candidates

    # -- optional ----------------------------------------------------------

    def policy_of(self, item: Any) -> str | None:
        """The named set of operating settings this item runs under — how hard
        to push it, how long to wait, how often to retry. Your business word
        (a crate, a partner, a plan) becomes that name here; grampy never
        learns what it meant. `None` means the defaults."""
        return None

    def ref_of(self, item: Any) -> str | None:
        """What version of the item arrives in a lane — opaque to grampy,
        compared to nothing, handed back as it was given."""
        return None

    def branch(self, item: Any, node: str) -> str | None:
        """Which branch this item takes out of a `choice` node."""
        return None

    def applies(self, item: Any, node: str) -> bool:
        """Whether this OPTIONAL node is for this item at all. `False` gives
        it up rather than doing it — see the module docstring."""
        return True

    def group_of(self, item: Any) -> str | None:
        """What this item is grouped BY, for a node that works subjects
        together — a colour, a destination, a customer. Compared, never read.
        `None` puts everything in one group."""
        return None


class ItemLease(list):
    """The items a claim took — a plain list of YOUR objects — with the
    `token` to conclude them, and `missing`, the ids nothing loaded for."""

    def __init__(self, items: Iterable[Any], token: str,
                 missing: Sequence[Any] = ()) -> None:
        super().__init__(items)
        self.token = token
        self.missing = tuple(missing)


class Items:
    """A journal that speaks in your objects. Every call ends up in the
    id-based API, which stays exactly as it is."""

    def __init__(self, journal: NodeJournal, adapter: Adapter) -> None:
        self.journal = journal
        self.adapter = adapter

    # -- the door ----------------------------------------------------------

    def admit(self, items: Sequence[Any]) -> int:
        """Take these items in: record each one's policy, once. Items
        sharing a policy are enrolled together. Return the count."""
        by_policy: dict[str | None, list[Any]] = {}
        for item in items:
            by_policy.setdefault(self.adapter.policy_of(item), []).append(
                self.adapter.id_of(item))
        written = 0
        for policy, ids in by_policy.items():
            written += self.journal.enroll(ids, policy)
        return written

    def arrive(self, name: str, items: Sequence[Any], *,
               urgent: bool = False) -> dict[str, int]:
        """These items arrive in a lane, each bringing its own `ref_of`.

        One call per distinct ref, and the counts are ADDED — the journal
        reports `{Outcome.QUEUED: n, Outcome.MERGED: n, Outcome.SKIPPED: n}` per call, and what
        comes back here is the whole batch.
        """
        by_ref: dict[str | None, list[Any]] = {}
        for item in items:
            by_ref.setdefault(self.adapter.ref_of(item), []).append(
                self.adapter.id_of(item))
        outcome: dict[str, int] = {}
        for ref, ids in by_ref.items():
            for kind, count in self.journal.arrive(
                    name, ids, ref=ref, urgent=urgent).items():
                outcome[kind] = outcome.get(kind, 0) + count
        return outcome

    # -- take and finish ---------------------------------------------------

    def claim(self, name: str, limit: int, *, candidates: Any) -> ItemLease:
        """Take up to `limit` candidates and HAND BACK THE OBJECTS.

        `candidates` MAY BE ANYTHING YOU HAVE: your own objects, their ids, or
        a mix of the two, in any iterable. Objects travel with the claim and
        are handed straight back — a batch you already loaded is not loaded
        twice — and whatever was named by id alone is loaded with `inflate`.

        An id is an int or a string, which is what tells the two apart. An
        object `id_of` cannot read, and that could not be an id, is an adapter
        to fix, and says so.

        A DRIVER'S OWN QUERY is handed to the journal untouched, and the lease
        is loaded with `inflate`. That is the one place ids are unavoidable —
        the storage produces the candidates, and it has no Python objects to
        give. It buys something a list cannot: the driver may filter and page
        in the database, so a claim of ten out of a large backlog never ships
        the backlog to Python.

        The items this node does not apply to are concluded `skipped` in the
        same call and left out of the lease, so what comes back is what there
        is work to do on.
        """
        given, candidates = self._subjects(
            candidates, grouping=node(name, self.journal.dag).group is not None)
        lease = self.journal.claim(name, limit, candidates=candidates)
        # Objects handed in travel with the claim; anything known only by
        # its id is loaded, exactly as a driver's query would be.
        held = {s: given[s] for s in lease if s in given}
        rest = [s for s in lease if s not in held]
        if rest:
            held.update(self._loaded(Lease(rest, lease.token)))
        # THE LEASE'S OWN ORDER, whichever half each item came from.
        loaded = {s: held[s] for s in lease if s in held}
        keep: list[Any] = []
        give_up: list[Any] = []
        for item in loaded.values():
            (keep if self.adapter.applies(item, name) else give_up).append(item)
        if give_up:
            self.journal.conclude(name, [self.adapter.id_of(i) for i in give_up],
                                  token=lease.token, status=Status.SKIPPED)
        missing = [s for s in lease if s not in loaded]
        return ItemLease(keep, lease.token, missing)

    def conclude(self, name: str, items: Sequence[Any], *,
                 token: str | None = None, status: str = Status.DONE) -> int:
        """Finish this node on these items, asking `branch` for a choice.

        `items` is usually the `ItemLease` a claim gave back, and the token
        travels with it. Pass `token=` when the lease did not come along —
        a worker that took its job off a queue and holds only the proof.
        """
        return self._finish(name, items, token=token, status=status)

    def fail(self, name: str, items: Sequence[Any], *,
             token: str | None = None) -> int:
        """This node did not produce, on these items."""
        return self._finish(name, items, token=token, status=Status.FAILED)

    def _finish(self, name: str, items: Sequence[Any], *,
                token: str | None, status: str) -> int:
        token = token if token is not None else getattr(items, "token", None)
        # ONLY A CHOICE IS ASKED FOR A BRANCH. Anywhere else the core refuses
        # one, and rightly: there would be nothing to omit.
        asking = node(name, self.journal.dag).choice and status != Status.FAILED
        by_branch: dict[str | None, list[Any]] = {}
        for item in items:
            branch = self.adapter.branch(item, name) if asking else None
            by_branch.setdefault(branch, []).append(self.adapter.id_of(item))
        touched = 0
        for branch, ids in by_branch.items():
            touched += self.journal.conclude(name, ids, token=token,
                                             status=status, branch=branch)
        return touched

    # -- pass through, in items' terms -------------------------------------

    def signal(self, items: Sequence[Any], event: str, ref: str | None = None) -> int:
        """Record that `event` happened for these items."""
        return self.journal.signal([self.adapter.id_of(i) for i in items], event, ref)

    def progress(self, item: Any) -> dict[str, str]:
        """`{node: status}` for one item."""
        return self.journal.progress(self.adapter.id_of(item))

    def history(self, item: Any) -> list[dict[str, Any]]:
        """Every row forget, release or a loop took away from this item."""
        return self.journal.history(self.adapter.id_of(item))

    def skip(self, name: str, *, candidates: Any) -> int:
        """Give up this OPTIONAL node on the candidates that are at it — for
        items you already know refuse it, without claiming them first."""
        _, candidates = self._subjects(candidates)
        return self.journal.skip(name, candidates=candidates)

    def settle(self, candidates: Any) -> dict[str, dict[str, int]]:
        """The janitor's pass, in items' terms: waits concluded, due arrivals
        let through their lane, optional nodes past their grace skipped."""
        _, candidates = self._subjects(candidates)
        return self.journal.settle(candidates)

    def _subjects(self, candidates: Any, *,
                  grouping: bool = False) -> tuple[dict[Any, Any], Any]:
        """`({id: item}, what the journal gets)`. Items become their ids and
        stay in hand; a driver's query travels on untouched.

        A NODE THAT GROUPS gets `(id, key)` pairs instead, the key coming
        from `group_of` — the one place the application says what makes two
        subjects belong together.
        """
        if not _are_items(candidates):
            return {}, candidates
        # THE ADAPTER SEES THE BATCH AS IT CAME, and says what is an item.
        given = {self.adapter.id_of(i): i
                 for i in self.adapter.inflate(list(candidates))}
        if grouping:
            return given, [(s, self.adapter.group_of(i)) for s, i in given.items()]
        return given, list(given)

    def _loaded(self, lease: Lease) -> dict[Any, Any]:
        """`{id: item}` for a lease, in the lease's order, in ONE call."""
        if not lease:
            return {}
        if type(self.adapter).inflate is Adapter.inflate:
            raise NotImplementedError(
                f"{type(self.adapter).__name__} needs `inflate`: a driver's "
                f"query named the candidates, so the claim came back as ids "
                f"with no objects attached. Write it, or pass your items as "
                f"candidates and they travel with the claim")
        found = {self.adapter.id_of(i): i for i in self.adapter.inflate(list(lease))}
        return {s: found[s] for s in lease if s in found}
