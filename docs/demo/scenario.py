"""The brick sorter — a factory the demo page animates, run by the real grampy.

Nothing here decides a workflow rule: the journal does. This module only
plays the factory around it — bricks arriving, workers taking a claim and
finishing it after a while, a defuser that sometimes fails, a bomb squad
that answers a call — and reports, for each brick, where the journal says it
stands, so the page can draw it there.

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
    Node,
    NodeJournal,
    claimable_nodes,
)
from quazardous.grampy.diagram import overlay
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.timing import Retry

#: The factory, as grampy sees it.
GRAPH = Graph(Document("brick-sorter", version="1", namespace="demo"), (
    Node("scan", choice=True, lease="30s"),
    Node("sort", parents=("scan",), lease="30s"),
    Node("quarantine", parents=("scan",), wait="bomb-squad.arrived", timeout="90s"),
    Node("defuse", parents=("quarantine",), lease="40s",
         retry=Retry(limit=3, delay="8s", backoff="exponential")),
    Node("reject", parents=("quarantine", "defuse"), need=1,
         on={"quarantine": ("failed",), "defuse": ("failed",)}),
    Node("pack", parents=("sort", "defuse")),
))

#: Where each station stands on the floor, in layers left to right.
LAYOUT = {
    "scan": (0, 1), "sort": (1, 0), "quarantine": (1, 2),
    "defuse": (2, 2), "pack": (3, 0), "reject": (3, 2),
}

#: One tray per colour at the end of the line; a TNT brick hides its colour until defused.
COLOURS = ("red", "orange", "yellow", "green", "blue", "pink")
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass
class Brick:
    id: int
    colour: str
    tnt: bool
    born: float


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
    workers: dict[str, int] = field(default_factory=lambda: {
        "scan": 2, "sort": 2, "defuse": 1, "pack": 2, "reject": 1})
    durations: dict[str, float] = field(default_factory=lambda: {
        "scan": 3.0, "sort": 4.0, "defuse": 7.0, "pack": 4.0, "reject": 2.5})


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

    # -- time ------------------------------------------------------------------

    def _now(self) -> str:
        return (EPOCH + timedelta(seconds=int(self.elapsed))).isoformat(timespec="seconds")

    def _log(self, call: str) -> None:
        self.calls.append(f"[{self._now()[11:19]}] {call}")
        del self.calls[:-60]

    # -- what visitors do -------------------------------------------------------

    def add_bricks(self, count: int, tnt: bool | None = None) -> None:
        for _ in range(count):
            is_tnt = self.rng.random() < self.settings.tnt_share if tnt is None else tnt
            colour = self.rng.choice(COLOURS)
            self.bricks[self._next_id] = Brick(self._next_id, colour, is_tnt, self.elapsed)
            self._next_id += 1

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
            if free <= 0 or not candidates:
                continue
            lease = self.journal.claim(node, free, candidates=candidates)
            if not lease:
                continue
            self._log(f'journal.claim("{node}", {free}, candidates=…)  # {list(lease)}')
            for subject in lease:
                jitter = self.rng.uniform(0.7, 1.4)
                self.jobs.append(Job(node, subject, lease.token,
                                     self.elapsed + self.settings.durations[node] * jitter))

    def _retire(self) -> None:
        for brick_id in self._active():
            progress = self.journal.progress(brick_id)
            if progress.get("pack") == NODE_DONE:
                self.finished[brick_id] = ("shipped", self.elapsed)
                self.shipped += 1
                self.sorted[self.bricks[brick_id].colour] += 1
            elif progress.get("reject") == NODE_DONE:
                self.finished[brick_id] = ("boom", self.elapsed)
                self.exploded += 1
        for brick_id, (_, when) in list(self.finished.items()):
            if self.elapsed - when > 6:
                self.bricks.pop(brick_id, None)
                self.finished.pop(brick_id, None)

    # -- what the page draws -------------------------------------------------------

    def _where(self, brick_id: int) -> tuple[str, str | None]:
        """(place, node): `station` working, `retry` waiting to try again,
        `shelf` in quarantine, `queue` waiting for a worker, `shipped`,
        `boom`."""
        if brick_id in self.finished:
            return self.finished[brick_id][0], None
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
        order = ["scan", "sort", "defuse", "reject", "pack"]
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
            bricks.append({
                "id": brick.id, "colour": brick.colour if revealed else None, "tnt": brick.tnt,
                "revealed": revealed, "place": place, "node": node,
                "retries": self.journal.retries(brick.id, "defuse") if brick.tnt else 0,
            })
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
            },
        }

    def detail(self, brick_id: int) -> str:
        brick = self.bricks.get(brick_id)
        if brick is None:
            return json.dumps(None)
        return json.dumps({
            "id": brick_id, "colour": brick.colour if self._revealed(brick) else None,
            "tnt": brick.tnt,
            "progress": self.journal.progress(brick_id),
            "history": self.journal.history(brick_id),
        })


def static() -> str:
    """What does not move: the graph as a document, and the floor layout."""
    return json.dumps({"graph": GRAPH.to_dict(), "layout": LAYOUT, "colours": COLOURS})
