"""LANES — subjects that come back wait in a lane, merge with the version
already waiting, and are let in by `settle` once due. See docs/rules.md,
"Lanes: subjects that come back".
"""
from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from ._base import PAGE, _unique
from .dag import (
    Lane,
    Node,
    descendants,
    joined,
    node,
)
from .limits import _Limits
from .names import Merge, Outcome, Reason, Status, WhileRunning
from .protocol import Arrival, Entry
from .timing import seconds, shift


def _encode_refs(refs: tuple[str, ...]) -> str | None:
    """The refs a lane keeps, as the text a driver stores. `None` when there
    is nothing to keep, so a lane that keeps one ref stores no list."""
    return json.dumps(list(refs)) if refs else None

def _decode_refs(stored: str | None) -> tuple[str, ...]:
    return tuple(json.loads(stored)) if stored else ()


class _Lanes(_Limits):
    """Arrivals, their merges, and the lane's door (within its limits)."""

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None = None,
               urgent: bool = False) -> dict[str, int]:
        """THESE SUBJECTS CAME BACK — a new version of each, `ref` naming it
        (opaque, optional) — and wait in the lane `name` to run again.

        An arrival for a subject already waiting is merged, as the lane says
        (`Lane.merge`, `Lane.position`); the merge is noted in the history.
        With `while_running=WhileRunning.SKIP`, an arrival for a subject whose pass is
        still running is dropped, and noted. `urgent` lets the arrival through
        at the next `settle` whatever its cooldown or delay — never over a
        running pass.

        Nothing runs here: `settle` lets due arrivals in. Return
        `{Outcome.QUEUED: n, Outcome.MERGED: n, Outcome.SKIPPED: n}` — keys that
        are also the strings `"queued"`, `"merged"`, `"skipped"`."""
        n = node(name, self.dag)
        if n.lane is None:
            raise ValueError(f"node {name!r} is not a lane: nothing arrives in it")
        out: dict[str, int] = {Outcome.QUEUED: 0, Outcome.MERGED: 0, Outcome.SKIPPED: 0}
        subjects = _unique(subjects)
        if not subjects:
            return out
        now = self._clock()
        lane_of = self._per_policy(name, "lane", subjects)
        after = (name, *sorted(descendants(name, self.dag)))
        progresses = self._progress_many(
            [s for s in subjects if lane_of[s].while_running == WhileRunning.SKIP])
        groups: dict[Lane, list[Any]] = {}
        skipped: list[Any] = []
        for subject in subjects:
            if any(progresses.get(subject, {}).get(x) in (Status.RUNNING, Status.SCHEDULED)
                   for x in after):
                skipped.append(subject)
                continue
            groups.setdefault(lane_of[subject], []).append(subject)
        if skipped:
            self.driver.note(skipped, name, status=Outcome.SKIPPED, reason=Reason.LANE,
                             now=now, ref=ref)
            out[Outcome.SKIPPED] += len(skipped)
        for lane, group in groups.items():
            if lane.keeps_every_ref:
                self._keep_every_ref(name, group, lane, ref, now, urgent, out)
                continue
            outcome = self.driver.arrive(name, group, ref=ref, now=now, merge=lane.merge,
                                         position=lane.position, urgent=urgent)
            merged = [s for s in group if outcome.get(s) == Outcome.MERGED]
            if merged:
                self.driver.note(merged, name, status=Outcome.MERGED, reason=Reason.LANE, now=now,
                                 ref=ref)
            out[Outcome.MERGED] += len(merged)
            out[Outcome.QUEUED] += sum(1 for s in group if outcome.get(s) == Outcome.QUEUED)
        self._pin(subjects)
        return out

    def _keep_every_ref(self, name: str, subjects: list[Any], lane: Lane, ref: str | None,
                        now: str, urgent: bool, out: dict[str, int]) -> None:
        """MERGE BY READING WHAT WAITS, THEN WRITING — under the driver's
        guard, on each subject's place in this lane.

        `first`, `last` and `dedupe` decide without looking, so one atomic
        write does them. Keeping every ref, or asking a function, cannot:
        two workers arriving at once would each start from the state before
        the other, and one would overwrite the other's version. The guard is
        the same one rate and concurrency take (`arrival|<node>|<subject>`).

        Many subjects, few writes: subjects that end up with the same refs
        are written together, and so are the notes of a same ref."""
        merger = self._merger(lane)
        dropped: dict[str, list[Any]] = {}
        merged: list[Any] = []
        with self.driver.guard([f"arrival|{name}|{s}" for s in subjects]):
            current = self.driver.arrivals(subjects, name)
            same: dict[tuple[str, ...], list[Any]] = {}
            for subject in subjects:
                waiting = current.get(subject)
                kept = _decode_refs(waiting.refs) if waiting is not None else ()
                wanted = tuple(merger(kept, ref))
                same.setdefault(wanted, []).append(subject)
                for gone in kept:
                    if gone not in wanted:
                        dropped.setdefault(gone, []).append(subject)
            for wanted, group in same.items():
                outcome = self.driver.arrive(
                    name, group, ref=wanted[-1] if wanted else None, now=now,
                    merge=Merge.SET, position=lane.position, urgent=urgent,
                    refs=_encode_refs(wanted))
                merged += [s for s in group if outcome.get(s) == Outcome.MERGED]
        # A REF LET GO IS STILL SAID: past `max_size`, or refused by the
        # function, it leaves a trace rather than vanishing.
        for gone, group in dropped.items():
            self.driver.note(group, name, status=Outcome.DROPPED, reason=Reason.LANE,
                             now=now, ref=gone)
        if merged:
            self.driver.note(merged, name, status=Outcome.MERGED, reason=Reason.LANE,
                             now=now, ref=ref)
        out[Outcome.MERGED] += len(merged)
        out[Outcome.QUEUED] += len(subjects) - len(merged)

    def _merger(self, lane: Lane) -> Callable[[tuple[str, ...], str | None], Any]:
        """The function this lane merges with: `all`'s, or the application's
        under the name the lane gives."""
        named = lane.merger
        if named is None:
            size = lane.max_size
            return lambda kept, arriving: (
                (*kept, arriving) if arriving is not None else kept)[-size:]
        try:
            return self._mergers[named]
        except KeyError:
            raise ValueError(
                f"lane merge {lane.merge!r} names a function the journal was not "
                f"given — pass it as NodeJournal(..., mergers={{{named!r}: fn}}); "
                f"it has {sorted(self._mergers)}") from None

    def arrival(self, subject: Any, name: str) -> Arrival | None:
        """What waits for this subject in the lane `name`, if anything."""
        node(name, self.dag)
        return self.driver.arrivals([subject], name).get(subject)

    def refs(self, subject: Any, name: str) -> tuple[str, ...]:
        """EVERY VERSION WAITING for this subject in the lane `name`, oldest
        first — what a lane keeping them all has gathered.

        A lane keeping one version gives that one; a subject with nothing
        waiting gives nothing."""
        arrival = self.arrival(subject, name)
        if arrival is None:
            return ()
        kept = _decode_refs(arrival.refs)
        return kept if kept else ((arrival.ref,) if arrival.ref is not None else ())

    def _let_in(self, n: Node, candidates: Any, now: str, limit: int | None = None) -> int:
        """THE LANE'S DOOR. An arrival enters when the subject's parents are
        joined, nothing of its previous pass runs, and it is due: urgent, or
        past both its `delay` (from its place) and its `cooldown` (from the
        end of the previous pass) — or past `max_wait` from its first arrival
        whatever the rest. Due arrivals enter in the order of their places,
        within the lane's `rate`."""
        after = tuple(sorted(descendants(n.name, self.dag)))
        pass_nodes = (n.name, *after)
        entries: dict[Any, Entry] = {}
        order: dict[Any, int] = {}
        # No pre-filter on the lane's own row: the previous pass holds one.
        for page in self.driver.scan(candidates, name=None, nodes=(*pass_nodes, *n.parents),
                                     parents=(), page=PAGE, now=now):
            for e in page:
                if e.subject not in entries and self._mine(e):
                    order[e.subject] = len(order)
                    entries[e.subject] = e
        if not entries:
            return 0
        waiting = self.driver.arrivals(list(entries), n.name)
        due: list[tuple[str, int, Any]] = []
        for subject, arrival in waiting.items():
            e = entries[subject]
            if not joined(n.name, self.dag, e.rows):
                continue
            if any(e.rows.get(x) in (Status.RUNNING, Status.SCHEDULED) for x in pass_nodes):
                continue
            lane = self.settings(n.name, e.policy).lane
            assert lane is not None
            if not arrival.urgent:
                ready = [arrival.place]
                if lane.delay is not None:
                    ready.append(shift(arrival.place, seconds(lane.delay)))
                ended = [e.finished[x] for x in pass_nodes if x in e.finished]
                if lane.cooldown is not None and ended:
                    ready.append(shift(max(ended), seconds(lane.cooldown)))
                late = (lane.max_wait is not None
                        and shift(arrival.arrived_at, seconds(lane.max_wait)) <= now)
                if max(ready) > now and not late:
                    continue
            due.append((arrival.place, order[subject], subject))
        chosen = [(subject, entries[subject].revision) for _, _, subject in sorted(due)]
        if limit is not None:
            chosen = chosen[:limit]
        if not chosen:
            return 0

        def write(group: list[tuple[Any, int]]) -> list[Any]:
            return self.driver.enter(n.name, group, archive=pass_nodes, now=now)

        if self._limited(n):
            return len(self._within_limits(
                n, chosen, {s: entries[s].policy for s, _ in chosen}, now, write))
        return len(write(chosen))
