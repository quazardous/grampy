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
    Lane,
    Node,
    NodeJournal,
    claimable_nodes,
)
from quazardous.grampy.diagram import overlay
from quazardous.grampy.drivers.memory import MemoryDriver
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
    # One crate needs an extra step: `polish` belongs to everyone and is
    # OPTIONAL, and the crate that does not want it gives it a grace, so
    # `settle` skips it. A channel changes settings, never the structure.
    Node("polish", parents=("sort",), optional=True),
    Node("pack", parents=("polish", "defuse")),
), channels={"salvage": {"polish": {"grace": "2s"}}})
# --8<-- [end:graph]

#: Where each station stands on the floor, in layers left to right.
LAYOUT = {
    "inbox": (0, 1), "scan": (1, 1), "sort": (2, 0), "quarantine": (2, 2),
    "polish": (3, 0), "defuse": (3, 2), "pack": (4, 0), "reject": (4, 2),
}

#: Where a brick comes from. Salvage bricks are not polished — the channel
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
        added, salvage = [], []
        for _ in range(count):
            is_tnt = self.rng.random() < self.settings.tnt_share if tnt is None else tnt
            colour = self.rng.choice(COLOURS)
            crate = "salvage" if self.rng.random() < self.settings.salvage_share else "factory"
            self.bricks[self._next_id] = Brick(self._next_id, colour, is_tnt, self.elapsed,
                                               crate=crate)
            added.append(self._next_id)
            if crate == "salvage":
                salvage.append(self._next_id)
            self._next_id += 1
        if salvage:
            self.journal.enroll(salvage, "salvage")
            self._log(f'journal.enroll({salvage}, "salvage")')
        if added:
            self.journal.arrive("inbox", added, ref="v1")
            self._log(f'journal.arrive("inbox", {added}, ref="v1")')

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
            if job.node == "scan":
                branch = "quarantine" if brick.tnt else "sort"
                self.journal.conclude("scan", [job.subject], token=job.token, branch=branch)
                self._log(f'journal.conclude("scan", [{job.subject}], token=…, branch="{branch}")')
            elif job.node == "defuse" and self.rng.random() < self.settings.defuse_failure:
                self.journal.fail("defuse", [job.subject], token=job.token)
                self._log(f'journal.fail("defuse", [{job.subject}], token=…)  # retries: '
                          f'{self.journal.retries(job.subject, "defuse")}')
            else:
                self.journal.conclude(job.node, [job.subject], token=job.token)
                self._log(f'journal.conclude("{job.node}", [{job.subject}], token=…)')

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
        candidates = self._active()
        for node, workers in self.settings.workers.items():
            free = workers - busy.get(node, 0)
            # The polisher only takes factory bricks: WHICH subjects a worker
            # offers is the application's own sentence. The salvage channel's
            # grace is the safety net — the janitor skips the node for them,
            # so nothing waits forever.
            eligible = ([b for b in candidates if self.bricks[b].crate == "factory"]
                        if node == "polish" else candidates)
            if free <= 0 or not eligible:
                continue
            lease = self.journal.claim(node, free, candidates=eligible)
            if not lease:
                continue
            self._log(f'journal.claim("{node}", {free}, candidates=…)  # {list(lease)}')
            for subject in lease:
                jitter = self.rng.uniform(0.7, 1.4)
                self.jobs.append(Job(node, subject, lease.token,
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
