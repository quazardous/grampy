"""RATE AND CONCURRENCY — a claim on a limited node takes what its bands
and caps allow, under the driver's guard. See docs/rules.md, "Rate limits
and concurrency".
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from ._base import _JournalBase
from .dag import (
    Node,
)
from .names import Per
from .timing import admit


class _Limits(_JournalBase):
    """A node's rate and concurrency limits, applied to a write."""

    def _limited(self, n: Node) -> bool:
        return bool(n.rate) or n.concurrency is not None or any(
            v.rate or v.concurrency is not None for _, v in self._variants(n.name))

    def _within_limits(self, n: Node, chosen: list[tuple[Any, int]],
                       policy_of: dict[Any, str | None], now: str,
                       write: Callable[[list[tuple[Any, int]]], list[Any]]) -> list[Any]:
        """RATE AND CONCURRENCY, decided here, kept by the storage's guard.

        Candidates are grouped by budget — one for the node, or one per
        policy with `per=Per.POLICY`. Under the guard of every budget's keys,
        each group is cut to what `concurrency` leaves free and what the rate
        bands let through (`timing.admit`), written by `write` — a claim, or
        a lane letting subjects in — and the bands advance by what was
        actually written."""
        groups: dict[str | None, list[tuple[Any, int]]] = {}
        for subject, revision in chosen:
            budget = policy_of.get(subject) if n.per == Per.POLICY else None
            groups.setdefault(budget, []).append((subject, revision))
        keys: dict[str | None, tuple[Node, list[str], str]] = {}
        for budget in groups:
            seen_by = self.settings(n.name, budget) if n.per == Per.POLICY else n
            prefix = f"{n.name}|{budget if budget is not None else '*'}"
            keys[budget] = (seen_by, [f"rate|{prefix}|{i}" for i in range(len(seen_by.rate))],
                            f"running|{prefix}")
        guarded = sorted({k for _, bands, lock in keys.values() for k in (*bands, lock)})
        instant = datetime.fromisoformat(now).timestamp()
        taken: list[Any] = []
        with self.driver.guard(guarded):
            stored = self.driver.limits([k for _, bands, _ in keys.values() for k in bands])
            advanced: dict[str, float] = {}
            for budget, group in sorted(groups.items(), key=lambda g: str(g[0])):
                seen_by, band_keys, _ = keys[budget]
                allowed = len(group)
                if seen_by.concurrency is not None:
                    busy = self.driver.running(
                        n.name, None if n.per != Per.POLICY else (budget,))
                    allowed = min(allowed, max(0, seen_by.concurrency - busy))
                if seen_by.rate:
                    allowed, _ = admit(seen_by.rate, [stored.get(k) for k in band_keys],
                                       instant, allowed)
                written = write(group[:allowed]) if allowed else []
                taken += written
                if seen_by.rate:
                    _, tats = admit(seen_by.rate, [stored.get(k) for k in band_keys],
                                    instant, len(written))
                    advanced.update(zip(band_keys, tats, strict=True))
            if advanced:
                self.driver.set_limits(advanced)
        return taken
