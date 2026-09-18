"""MIGRATION — subjects pinned to another version of the graph moved onto
this one, all or nothing. See docs/rules.md, "Versions and migration".
"""
from __future__ import annotations

from typing import Any

from ._base import _JournalBase, _unique
from .dag import (
    joined,
)
from .graph import Graph
from .names import Status


class MigrationError(ValueError):
    """Subjects that cannot move to the new version — `problems` says why,
    subject by subject. Nothing was written."""

    def __init__(self, problems: dict[Any, str]) -> None:
        self.problems = problems
        lines = "; ".join(f"{s!r}: {why}" for s, why in list(problems.items())[:10])
        more = f" (+{len(problems) - 10} more)" if len(problems) > 10 else ""
        super().__init__(f"{len(problems)} subject(s) not compliant — {lines}{more}")


class _Migration(_JournalBase):
    """Moving subjects between versions of a graph."""

    def migrate(self, subjects: list[Any], source: Graph,
                mapping: dict[str, str | None] | None = None) -> int:
        """MOVE SUBJECTS FROM `source` — another version of this graph — ONTO
        THIS ONE, or refuse them all.

        `mapping` names what became of each source node: another name, or
        `None` when it is gone (its rows are archived, reason `migrate`). A
        node left out keeps its name, and must exist here.

        A subject is COMPLIANT when its journal could have been written on
        this graph: every row, once renamed, stands where the rule of this
        graph lets a row stand — its parents joined (Rinderle, Reichert and
        Dadam's compliance criterion, on the current rows only, which are
        already the latest pass of any loop). A dropped node held by a worker
        makes a subject non-compliant too.

        ALL OR NOTHING: every subject is checked before anything is written;
        one failure raises `MigrationError` naming each non-compliant subject
        and why, and nothing moves. Subjects already on this version, or
        pinned to a third one, are refused the same way. Return the count
        migrated."""
        if self.graph is None or self.version is None:
            raise ValueError("migrate needs a journal built on a versioned Graph")
        if source.document.identity == self.version:
            raise ValueError(f"the source is already {self.version!r}")
        mapping = dict(mapping or {})
        here = {n.name for n in self.dag}
        there = {n.name for n in source.nodes}
        unknown = sorted(set(mapping) - there)
        if unknown:
            raise ValueError(f"mapping names nodes the source does not have: {unknown}")
        full = {name: mapping.get(name, name) for name in there}
        missing = sorted(name for name, target in full.items()
                         if target is not None and target not in here)
        if missing:
            raise ValueError(f"source nodes with nowhere to go on {self.version!r}: "
                             f"{missing} — map them to a node or to None")
        landed = [t for t in full.values() if t is not None]
        if len(landed) != len(set(landed)):
            raise ValueError("two source nodes are mapped onto the same node")

        subjects = _unique(subjects)
        pinned = self.driver.versions(subjects)
        progresses = self._progress_many(
            [s for s in subjects
             if pinned.get(s, source.document.identity) == source.document.identity])
        problems: dict[Any, str] = {}
        plans: dict[Any, tuple[dict[str, str], tuple[str, ...]]] = {}
        for subject in subjects:
            if pinned.get(subject, source.document.identity) != source.document.identity:
                problems[subject] = f"pinned to {pinned[subject]!r}"
                continue
            progress = progresses.get(subject, {})
            drop = tuple(sorted(name for name in progress if full.get(name, name) is None))
            held = [name for name in drop if progress[name] in (Status.RUNNING, Status.SCHEDULED)]
            if held:
                problems[subject] = f"{held} would be dropped while held or scheduled"
                continue
            moved: dict[str, str] = {}
            rename: dict[str, str] = {}
            for name, status in progress.items():
                target = full.get(name, name)
                if target is not None:
                    moved[target] = status
                    if target != name:
                        rename[name] = target
            stray = sorted(name for name in moved if name not in here)
            if stray:
                problems[subject] = f"rows on {stray}, which {self.version!r} lacks"
                continue
            unjoined = sorted(name for name in moved if not joined(name, self.dag, moved))
            if unjoined:
                problems[subject] = (f"{unjoined} could not have run on "
                                     f"{self.version!r}: their parents are not joined")
                continue
            plans[subject] = (rename, drop)
        if problems:
            raise MigrationError(problems)
        # Every renamed or dropped node is passed, rows or not: an arrival
        # waiting in a lane follows its node too.
        every_rename = {name: target for name, target in full.items()
                        if target is not None and target != name}
        every_drop = tuple(sorted(name for name, target in full.items() if target is None))
        now = self._clock()
        target_version: str = self.version
        many = getattr(self.driver, "rewrite_many", None)
        if many is not None:
            many(list(plans), rename=every_rename, drop=every_drop,
                 version=target_version, now=now)
        else:
            for subject in plans:
                self.driver.rewrite(subject, rename=every_rename, drop=every_drop,
                                    version=target_version, now=now)
        return len(plans)
