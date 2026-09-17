"""The brick sorter — a factory the demo page animates, run by the real grampy.

Nothing here decides a workflow rule: the journal does. This module only
plays the factory around it — bricks arriving, workers taking a claim and
finishing it after a while, a defuser that sometimes fails, a bomb squad
that answers a call, sorted bricks sent back for another pass — and reports,
for each brick, where the journal says it stands, so the page can draw it
there.

    world = World(seed=7)
    world.tick(seconds=2)          # advance the factory, returns the state
"""
from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from quazardous.grampy import (
    NODE_DONE,
    NODE_FAILED,
    NODE_RUNNING,
    NODE_SCHEDULED,
    Document,
    Graph,
    Group,
    Lane,
    Node,
    NodeJournal,
    claimable_nodes,
)
from quazardous.grampy.diagram import overlay
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.items import Adapter, Items
from quazardous.grampy.timing import Retry

# --8<-- [start:graph]
#: The factory, as grampy sees it.
GRAPH = Graph(Document("brick-sorter", version="1", namespace="demo"), (
    Node("inbox", lane=Lane.throttle(cooldown="30s", max_wait="90s")),
    Node("scan", parents=("inbox",), choice=True, lease="30s"),
    Node("sort", parents=("scan",), lease="30s"),
    Node("quarantine", parents=("scan",), wait="bomb-squad.arrived", timeout="90s"),
    Node("defuse", parents=("quarantine",), lease="40s",
         retry=Retry(limit=3, delay="8s", backoff="exponential")),
    Node("reject", parents=("quarantine", "defuse"), need=1,
         on={"quarantine": ("failed",), "defuse": ("failed",)}),
    # TWO KINDS OF BRICK, ONE WORKFLOW. `polish` belongs to everyone and is
    # OPTIONAL: the adapter below says it is not for salvage bricks. The
    # policy's grace is the safety net, for a line running no polisher at
    # all. A policy changes settings, never the structure.
    Node("polish", parents=("sort",), optional=True),
    # FIVE OF A COLOUR MAKE A BAG. `pack` is not claimed one brick at a
    # time: the claim gathers a group sharing a key — here the colour, which
    # the adapter below hands over — and a worker bags them together.
    Node("pack", parents=("polish", "defuse"), group=Group(size=5)),
), policies={"salvage": {"polish": {"grace": "20s"}}})
# --8<-- [end:graph]


# --8<-- [start:adapter]
class Bricks(Adapter):
    """WHAT THE WORKFLOW NEEDS TO KNOW ABOUT A BRICK, and nothing more.

    grampy never looks inside a brick. It asks these questions, and the
    factory — which alone knows what a brick is — answers them.
    """

    def __init__(self, bricks):
        self.bricks = bricks

    def id_of(self, brick):
        return brick.id

    def inflate(self, candidates):
        """WHATEVER THE CALLER HAD, AS BRICKS. The factory is handed the
        batch as it came and alone knows what an id looks like here."""
        return [self.bricks[c] if isinstance(c, int) else c
                for c in candidates
                if not isinstance(c, int) or c in self.bricks]

    def policy_of(self, brick):
        """THE BUSINESS WORD BECOMES AN OPERATING ONE. The factory knows
        about crates; grampy only knows there are settings under this name.
        Mapping one to the other is the factory's job, right here."""
        return brick.crate

    def ref_of(self, brick):
        """Which version of the brick arrives in the lane."""
        return f"v{brick.version}"

    def branch(self, brick, node):
        """A TNT brick goes to quarantine; the others go straight to sorting."""
        return "quarantine" if brick.tnt else "sort"

    def group_of(self, brick):
        """WHAT MAKES A BAG UNIFORM: its colour. grampy compares this and
        never reads it — it has no idea what a colour is."""
        return brick.colour

    def applies(self, brick, node):
        """SALVAGE BRICKS ARE NOT POLISHED — the one difference between the
        two kinds, said once, here, instead of in every worker."""
        return node != "polish" or brick.crate == "factory"
# --8<-- [end:adapter]

#: Where each station stands on the floor, in layers left to right.
LAYOUT = {
    "inbox": (0, 1), "scan": (1, 1), "sort": (2, 0), "quarantine": (2, 2),
    "polish": (3, 0), "defuse": (3, 2), "pack": (4, 0), "reject": (4, 2),
}

#: Where a brick comes from. Salvage bricks are not polished — the policy
#: gives `polish` a grace, and the janitor skips it for them.
CRATES = ("factory", "salvage")

#: How long a sorted brick is kept back after its pass — the inbox lane's cooldown.
COOLDOWN = 30.0

#: One tray per colour at the end of the line; a TNT brick hides its colour until defused.
COLOURS = ("red", "orange", "yellow", "green", "blue", "pink")
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass
class Brick:
    id: int
    colour: str
    tnt: bool
    born: float
    version: int = 1
    crate: str = "factory"


@dataclass
class Job:
    node: str
    subject: int
    token: str
    done_at: float


@dataclass
class Settings:
    arrivals_per_minute: float = 20.0
    tnt_share: float = 0.2
    defuse_failure: float = 0.7
    squad_auto: bool = True
    squad_delay: float = 25.0
    returns_share: float = 0.15
    workers: dict[str, int] = field(default_factory=lambda: {
        "scan": 2, "sort": 2, "polish": 1, "defuse": 1, "pack": 2, "reject": 1})
    durations: dict[str, float] = field(default_factory=lambda: {
        "scan": 3.0, "sort": 4.0, "polish": 5.0, "defuse": 7.0, "pack": 4.0,
        "reject": 2.5})
    salvage_share: float = 0.4


class World:
    """The factory floor: bricks, workers, a clock, and one journal."""

    def __init__(self, seed: int = 7, settings: Settings | None = None) -> None:
        self.rng = random.Random(seed)
        self.settings = settings or Settings()
        self.elapsed = 0.0
        self.journal = NodeJournal(MemoryDriver(), GRAPH, clock=self._now, rng=self.rng)
        self.bricks: dict[int, Brick] = {}
        #: THE CANONICAL WAY IN: the factory talks in bricks, not in ids.
        self.items = Items(self.journal, Bricks(self.bricks))
        self.finished: dict[int, tuple[str, float]] = {}
        self.jobs: list[Job] = []
        self.calls: list[str] = []
        self.shipped = 0
        self.sorted: dict[str, int] = {colour: 0 for colour in COLOURS}
        self.exploded = 0
        self._next_id = 1
        self._arrival_debt = 0.0
        self._called: dict[int, float] = {}
        #: Sorted bricks off the line, kept so that they can be sent back.
        self.gone: dict[int, Brick] = {}
        #: THE BAGS, BY THE TOKEN OF THE CLAIM THAT MADE THEM. Every brick of
        #: one group shares that token — grampy writes it on their rows and
        #: keeps it after they conclude — so it is the bag's own name.
        self.bags: dict[str, dict[str, Any]] = {}
        self.bag_of: dict[int, str] = {}
        self.shipped_at: dict[int, float] = {}
        self._returns: dict[int, float] = {}

    # -- time ------------------------------------------------------------------

    def _now(self) -> str:
        return (EPOCH + timedelta(seconds=int(self.elapsed))).isoformat(timespec="seconds")

    def _log(self, call: str) -> None:
        self.calls.append(f"[{self._now()[11:19]}] {call}")
        del self.calls[:-60]

    # -- what visitors do -------------------------------------------------------

    def add_bricks(self, count: int, tnt: bool | None = None) -> None:
        added = []
        for _ in range(count):
            is_tnt = self.rng.random() < self.settings.tnt_share if tnt is None else tnt
            colour = self.rng.choice(COLOURS)
            crate = "salvage" if self.rng.random() < self.settings.salvage_share else "factory"
            brick = Brick(self._next_id, colour, is_tnt, self.elapsed, crate=crate)
            self.bricks[brick.id] = brick
            added.append(brick)
            self._next_id += 1
        if added:
            # The crate and the version are read off each brick, not passed.
            self.items.admit(added)
            self.items.arrive("inbox", added)
            self._log(f"items.admit({[b.id for b in added]})  # crates: "
                      f"{sorted({b.crate for b in added})}")
            self._log('items.arrive("inbox", bricks)')

    def send_back(self, brick_id: int) -> bool:
        """A sorted brick comes back as a new version: it waits in the inbox
        lane — merged with the version already waiting, if any — until its
        cooldown has passed. Only a shipped brick, or one already waiting,
        can be sent back."""
        brick_id = int(brick_id)
        waiting = self.journal.arrival(brick_id, "inbox") is not None
        shipped = self.finished.get(brick_id, ("",))[0] == "shipped" or brick_id in self.gone
        if not (waiting or shipped):
            return False
        brick = self.bricks.get(brick_id) or self.gone.pop(brick_id)
        brick.version += 1
        brick.tnt = False       # what comes back has been through the squad
        self.bricks[brick_id] = brick
        self.finished.pop(brick_id, None)
        self._returns.pop(brick_id, None)
        ref = f"v{brick.version}"
        outcome = self.journal.arrive("inbox", [brick_id], ref=ref)
        kind = "merged" if outcome["merged"] else "queued"
        self._log(f'journal.arrive("inbox", [{brick_id}], ref="{ref}")  # {kind}')
        return True

    def call_squad(self) -> None:
        waiting = [b.id for b in self.bricks.values() if self._where(b.id)[0] == "shelf"]
        if waiting:
            self.journal.signal(waiting, "bomb-squad.arrived")
            self._log(f'journal.signal({waiting}, "bomb-squad.arrived")')

    def configure(self, values: dict[str, Any]) -> None:
        for key, value in values.items():
            if hasattr(self.settings, key):
                setattr(self.settings, key, type(getattr(self.settings, key))(value))

    # -- one step of the factory -------------------------------------------------

    def tick(self, seconds: float) -> str:
        self.elapsed += seconds
        self._arrive(seconds)
        for brick_id, when in list(self._returns.items()):
            if when <= self.elapsed:
                self.send_back(brick_id)
        self._finish_jobs()
        self._squad()
        expired = self.journal.expire()
        if expired:
            self._log(f"journal.expire()  # {expired}")
        settled = self.journal.settle(self._active())
        if settled:
            self._log(f"journal.settle(candidates)  # {settled}")
        self._claim()
        self._retire()
        return json.dumps(self.state())

    def _active(self) -> list[int]:
        return sorted(b for b in self.bricks if b not in self.finished)

    def _arrive(self, seconds: float) -> None:
        self._arrival_debt += seconds * self.settings.arrivals_per_minute / 60.0
        whole = int(self._arrival_debt)
        if whole:
            self._arrival_debt -= whole
            self.add_bricks(whole)

    def _finish_jobs(self) -> None:
        due = [j for j in self.jobs if j.done_at <= self.elapsed]
        self.jobs = [j for j in self.jobs if j.done_at > self.elapsed]
        for job in due:
            brick = self.bricks[job.subject]
            if job.node == "defuse" and self.rng.random() < self.settings.defuse_failure:
                self.items.fail("defuse", [brick], token=job.token)
                self._log(f'items.fail("defuse", [{job.subject}])  # retries: '
                          f'{self.journal.retries(job.subject, "defuse")}')
            else:
                # No `branch=` by hand: for a choice, the adapter is asked.
                self.items.conclude(job.node, [brick], token=job.token)
                self._log(f'items.conclude("{job.node}", [{job.subject}])'
                          + (f'  # branch: {self.items.adapter.branch(brick, job.node)}'
                             if job.node == "scan" else ""))

    def _squad(self) -> None:
        if not self.settings.squad_auto:
            return
        for brick_id in self._active():
            if self._where(brick_id)[0] != "shelf":
                self._called.pop(brick_id, None)
                continue
            first = self._called.setdefault(brick_id, self.elapsed)
            if self.elapsed - first >= self.settings.squad_delay and self.rng.random() < 0.5:
                self.journal.signal([brick_id], "bomb-squad.arrived")
                self._log(f'journal.signal([{brick_id}], "bomb-squad.arrived")')
                self._called.pop(brick_id, None)

    def _claim(self) -> None:
        busy: dict[str, int] = {}
        for job in self.jobs:
            busy[job.node] = busy.get(job.node, 0) + 1
        # CANDIDATES ARE BRICKS, not ids: the factory never handles an id.
        candidates = [self.bricks[b] for b in self._active()]
        for node, workers in self.settings.workers.items():
            free = workers - busy.get(node, 0)
            if free <= 0 or not candidates:
                continue
            # ONE CALL FOR BOTH KINDS OF BRICK. The claim hands back bricks,
            # not ids, and the ones this station is not for — a salvage brick
            # at the polisher — are given up inside it, on the adapter's word.
            lease = self.items.claim(node, free, candidates=candidates)
            if not lease:
                continue
            self._log(f'items.claim("{node}", {free}, candidates=…)'
                      f'  # {[b.id for b in lease]}')
            if node == "pack":
                self.bags[lease.token] = {"colour": lease[0].colour,
                                          "size": len(lease), "born": self.elapsed}
                for brick in lease:
                    self.bag_of[brick.id] = lease.token
            for brick in lease:
                jitter = self.rng.uniform(0.7, 1.4)
                self.jobs.append(Job(node, brick.id, lease.token,
                                     self.elapsed + self.settings.durations[node] * jitter))

    def _retire(self) -> None:
        for brick_id in self._active():
            if self.journal.arrival(brick_id, "inbox") is not None:
                continue        # back in the inbox: its last pass is still on record
            progress = self.journal.progress(brick_id)
            if progress.get("pack") == NODE_DONE:
                self.finished[brick_id] = ("shipped", self.elapsed)
                self.shipped_at[brick_id] = self.elapsed
                self.shipped += 1
                self.sorted[self.bricks[brick_id].colour] += 1
                if self.rng.random() < self.settings.returns_share:
                    self._returns[brick_id] = self.elapsed + self.rng.uniform(4, 30)
            elif progress.get("reject") == NODE_DONE:
                self.finished[brick_id] = ("boom", self.elapsed)
                self.exploded += 1
        # A BAG OUTLIVES ITS BRICKS: they leave the floor, it stays in the bin.
        # Keep the last few, as a bin holds only so many.
        if len(self.bags) > 18:
            for token in sorted(self.bags, key=lambda t: self.bags[t]["born"])[:-18]:
                self.bags.pop(token, None)
                for bid, tok in list(self.bag_of.items()):
                    if tok == token:
                        self.bag_of.pop(bid, None)
        for brick_id, (kind, when) in list(self.finished.items()):
            if self.elapsed - when > 6:
                brick = self.bricks.pop(brick_id, None)
                self.finished.pop(brick_id, None)
                if kind == "shipped" and brick is not None:
                    self.gone[brick_id] = brick
        while len(self.gone) > 200:
            oldest = next(iter(self.gone))
            self.gone.pop(oldest)
            self._returns.pop(oldest, None)

    # -- what the page draws -------------------------------------------------------

    def _where(self, brick_id: int) -> tuple[str, str | None]:
        """(place, node): `inbox` waiting in the lane, `station` working,
        `retry` waiting to try again, `shelf` in quarantine, `queue` waiting
        for a worker, `shipped`, `boom`."""
        if brick_id in self.finished:
            return self.finished[brick_id][0], None
        if self.journal.arrival(brick_id, "inbox") is not None:
            return "inbox", "inbox"
        progress = self.journal.progress(brick_id)
        for name, status in progress.items():
            if status == NODE_RUNNING:
                return "station", name
        for name, status in progress.items():
            if status == NODE_SCHEDULED:
                return "retry", name
        ready = claimable_nodes(GRAPH.nodes, progress)
        if "quarantine" in ready:
            return "shelf", "quarantine"
        order = ["scan", "sort", "polish", "defuse", "reject", "pack"]
        for name in order:
            if name in ready:
                return "queue", name
        failed = [n for n, s in progress.items() if s == NODE_FAILED]
        return ("queue", "reject") if failed else ("queue", "scan")

    def _revealed(self, brick: Brick) -> bool:
        """A clean brick shows its colour; a TNT brick only once defused."""
        return not brick.tnt or self.journal.progress(brick.id).get("defuse") == NODE_DONE

    def state(self) -> dict[str, Any]:
        bricks = []
        for brick in self.bricks.values():
            place, node = self._where(brick.id)
            revealed = self._revealed(brick)
            entry = {
                "id": brick.id, "colour": brick.colour if revealed else None, "tnt": brick.tnt,
                "revealed": revealed, "place": place, "node": node, "version": brick.version,
                "crate": brick.crate,
                "retries": self.journal.retries(brick.id, "defuse") if brick.tnt else 0,
                "bag": self.bag_of.get(brick.id),
            }
            if place == "inbox":
                arrival = self.journal.arrival(brick.id, "inbox")
                assert arrival is not None
                entry["arrived"] = arrival.place
                shipped = self.shipped_at.get(brick.id)
                entry["cooldown"] = (max(0, int(COOLDOWN - (self.elapsed - shipped)))
                                     if shipped is not None else 0)
            bricks.append(entry)
        bricks.sort(key=lambda b: (b.get("arrived", ""), b["id"]))
        return {
            "clock": self._now()[11:19],
            "bricks": bricks,
            "counts": overlay(self.journal),
            "calls": self.calls[-25:],
            "shipped": self.shipped,
            "sorted": self.sorted,
            "bags": [{"id": token, **bag} for token, bag in self.bags.items()],
            "exploded": self.exploded,
            "settings": {
                "arrivals_per_minute": self.settings.arrivals_per_minute,
                "tnt_share": self.settings.tnt_share,
                "defuse_failure": self.settings.defuse_failure,
                "squad_auto": self.settings.squad_auto,
                "returns_share": self.settings.returns_share,
                "salvage_share": self.settings.salvage_share,
            },
            "returnable": sorted(self.gone)[-60:],
        }

    def detail(self, brick_id: int) -> str:
        brick = self.bricks.get(brick_id)
        if brick is None:
            return json.dumps(None)
        return json.dumps({
            "id": brick_id, "colour": brick.colour if self._revealed(brick) else None,
            "tnt": brick.tnt, "version": brick.version, "crate": brick.crate,
            "progress": self.journal.progress(brick_id),
            "history": self.journal.history(brick_id),
        })


def static() -> str:
    """What does not move: the graph as a document, and the floor layout."""
    return json.dumps({"graph": GRAPH.to_dict(), "layout": LAYOUT, "colours": COLOURS,
                       "cooldown": COOLDOWN})
