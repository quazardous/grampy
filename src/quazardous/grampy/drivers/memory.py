"""THE MEMORY DRIVER — node rows in a dict, for tests and single-process use.

Deterministic, no dependency. It is the reference the other drivers are
confronted with in `grampy.testing.JournalContract`, so it is written to
read like the rule, not to be fast.

`candidates` is an ordered iterable of subjects: the order is the
priority, the first ones are taken first.

ONE LOCK, AND EVERY CALL HOLDS IT. That is what makes `forget` — delete
and raise the revision — a single step for a claim running in another
thread: the claim either read before, and its revision no longer matches,
or after, and it saw no parent.
"""
from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..dag import (
    NODE_DONE,
    NODE_OMITTED,
    NODE_RUNNING,
    NODE_SATISFYING,
    NODE_SCHEDULED,
    NODE_SKIPPED,
)
from ..journal import Arrival, Entry, utc_now


@dataclass
class Row:
    """One subject's passage through one node."""

    status: str
    started_at: str
    finished_at: str | None = None
    lease: str | None = None


class MemoryDriver:
    """`(subject, node) → Row`, `subject → revision`. A lock makes each call
    atomic across threads."""

    def __init__(self) -> None:
        self.rows: dict[tuple[Any, str], Row] = {}
        self.revisions: dict[Any, int] = {}
        self.policy_of: dict[Any, str] = {}
        self.version_of: dict[Any, str] = {}
        self.limiter: dict[str, float] = {}
        self.archive: list[tuple[Any, dict[str, Any]]] = []
        self.waiting: dict[tuple[Any, str], Arrival] = {}
        self._lock = threading.RLock()

    def _take_away(self, subject: Any, name: str, *, now: str, reason: str) -> bool:
        """Move a row to the archive. Call with the lock held."""
        row = self.rows.pop((subject, name), None)
        if row is None:
            return False
        self.archive.append((subject, {
            "node": name, "status": row.status, "started_at": row.started_at,
            "finished_at": row.finished_at, "lease": row.lease,
            "archived_at": now, "reason": reason}))
        return True

    # -- write -------------------------------------------------------------

    def now(self) -> str:
        return utc_now()

    def scan(self, candidates: Any, *, name: str | None, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str) -> Iterator[list[Entry]]:
        """No pre-filter: every candidate is read, the journal decides."""
        subjects = list(candidates)
        for start in range(0, len(subjects), page):
            with self._lock:
                entries = []
                for subject in subjects[start:start + page]:
                    held = {n: self.rows[(subject, n)] for n in nodes
                            if (subject, n) in self.rows}
                    entries.append(Entry(
                        subject, self.revisions.get(subject, 0),
                        {n: row.status for n, row in held.items()},
                        {n: row.started_at for n, row in held.items()
                         if row.status == NODE_SCHEDULED},
                        {n: row.finished_at for n, row in held.items()
                         if row.finished_at is not None},
                        self.policy_of.get(subject), self.version_of.get(subject)))
            yield entries

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        taken: list[Any] = []
        with self._lock:
            for subject, revision in entries:
                current = self.rows.get((subject, name))
                if current is not None and not (
                        current.status == NODE_SCHEDULED and current.started_at <= now):
                    continue
                if self.revisions.get(subject, 0) != revision:
                    continue
                self.rows[(subject, name)] = Row(
                    status, now, None if status == NODE_RUNNING else now, lease)
                taken.append(subject)
        return taken

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        count = 0
        with self._lock:
            for subject in subjects:
                row = self.rows.get((subject, name))
                if row is None or row.status != NODE_RUNNING:
                    continue
                if lease is not None and row.lease != lease:
                    continue
                row.status, row.finished_at = status, now
                for other in omit:
                    self.rows.setdefault((subject, other), Row(NODE_OMITTED, now, now))
                if reset:
                    self.revisions[subject] = self.revisions.get(subject, 0) + 1
                    for other in reset:
                        self._take_away(subject, other, now=now, reason="loop")
                if reschedule is not None:
                    self._take_away(subject, name, now=now, reason="retry")
                    self.rows[(subject, name)] = Row(NODE_SCHEDULED, reschedule)
                count += 1
        return count

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        count = 0
        with self._lock:
            for subject in subjects:
                if (subject, name) in self.rows:
                    continue
                self.rows[(subject, name)] = Row(NODE_DONE, now, now)
                count += 1
        return count

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        with self._lock:
            for subject in subjects:
                self.revisions[subject] = self.revisions.get(subject, 0) + 1
            return sum(1 for subject in subjects
                       if self._take_away(subject, name, now=now, reason="forget"))

    def release(self, name: str, *, older_than: str, now: str,
                only: tuple[str, ...] | None = None, exclude: tuple[str, ...] = (),
                version: str | None = None) -> int:
        with self._lock:
            stale = [key for key, row in self.rows.items()
                     if key[1] == name and row.status == NODE_RUNNING
                     and row.started_at < older_than
                     and (only is None or self.policy_of.get(key[0]) in only)
                     and self.policy_of.get(key[0]) not in exclude
                     and (version is None
                          or self.version_of.get(key[0]) in (None, version))]
            for subject, n in stale:
                self._take_away(subject, n, now=now, reason="release")
            return len(stale)

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        with self._lock:
            for subject in subjects:
                if policy is None:
                    self.policy_of.pop(subject, None)
                else:
                    self.policy_of[subject] = policy
        return len(subjects)

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        with self._lock:
            return {s: self.policy_of[s] for s in subjects if s in self.policy_of}

    @contextmanager
    def guard(self, keys: list[str]) -> Iterator[None]:
        """Memory has no transaction: the lock is held for the block."""
        with self._lock:
            yield

    def limits(self, keys: list[str]) -> dict[str, float]:
        with self._lock:
            return {k: self.limiter[k] for k in keys if k in self.limiter}

    def set_limits(self, values: dict[str, float]) -> None:
        with self._lock:
            self.limiter.update(values)

    def running(self, name: str, policies: tuple[str | None, ...] | None) -> int:
        with self._lock:
            return sum(1 for (s, n), row in self.rows.items()
                       if n == name and row.status == NODE_RUNNING
                       and (policies is None or self.policy_of.get(s) in policies))

    def pin(self, subjects: list[Any], version: str) -> int:
        with self._lock:
            fresh = [s for s in subjects if s not in self.version_of]
            for s in fresh:
                self.version_of[s] = version
            return len(fresh)

    def versions(self, subjects: list[Any]) -> dict[Any, str]:
        with self._lock:
            return {s: self.version_of[s] for s in subjects if s in self.version_of}

    def rewrite(self, subject: Any, *, rename: dict[str, str], drop: tuple[str, ...],
                version: str, now: str) -> None:
        with self._lock:
            for name in drop:
                self._take_away(subject, name, now=now, reason="migrate")
                self.waiting.pop((subject, name), None)
            moved = {new: self.rows.pop((subject, old)) for old, new in rename.items()
                     if (subject, old) in self.rows}
            for new, row in moved.items():
                self.rows[(subject, new)] = row
            waiting = {new: self.waiting.pop((subject, old)) for old, new in rename.items()
                       if (subject, old) in self.waiting}
            for new, arrival in waiting.items():
                self.waiting[(subject, new)] = arrival
            self.version_of[subject] = version
            self.revisions[subject] = self.revisions.get(subject, 0) + 1

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        with self._lock:
            return {n: row.status for (s, n), row in self.rows.items() if s == subject}

    def history(self, subject: Any) -> list[dict[str, Any]]:
        with self._lock:
            entries = [dict(e) for s, e in self.archive if s == subject]
        return sorted(entries, key=lambda e: (e["archived_at"], e["node"]))

    def archived(self, subjects: list[Any], name: str, reason: str) -> dict[Any, int]:
        wanted = set(subjects)
        counts: dict[Any, int] = {}
        with self._lock:
            for s, e in self.archive:
                if s in wanted and e["node"] == name and e["reason"] == reason:
                    counts[s] = counts.get(s, 0) + 1
        return counts

    def note(self, subjects: list[Any], name: str, *, status: str, reason: str,
             now: str, ref: str | None) -> int:
        with self._lock:
            for subject in subjects:
                self.archive.append((subject, {
                    "node": name, "status": status, "started_at": now,
                    "finished_at": now, "lease": ref, "archived_at": now,
                    "reason": reason}))
        return len(subjects)

    def arrive(self, name: str, subjects: list[Any], *, ref: str | None, now: str,
               merge: str, position: str, urgent: bool,
               refs: str | None = None) -> dict[Any, str]:
        out: dict[Any, str] = {}
        with self._lock:
            for subject in subjects:
                current = self.waiting.get((subject, name))
                if current is None:
                    self.waiting[(subject, name)] = Arrival(ref, now, now, urgent, refs)
                    out[subject] = "queued"
                    continue
                # `set`: the journal worked out ref and refs under the guard.
                self.waiting[(subject, name)] = Arrival(
                    ref if merge in ("last", "set") else current.ref,
                    now if position == "last" else current.place,
                    current.arrived_at, current.urgent or urgent,
                    refs if merge == "set" else current.refs)
                out[subject] = "merged"
        return out

    def arrivals(self, subjects: list[Any], name: str) -> dict[Any, Arrival]:
        with self._lock:
            return {s: self.waiting[(s, name)] for s in subjects if (s, name) in self.waiting}

    def enter(self, name: str, entries: list[tuple[Any, int]], *,
              archive: tuple[str, ...], now: str) -> list[Any]:
        entered: list[Any] = []
        with self._lock:
            for subject, revision in entries:
                arrival = self.waiting.get((subject, name))
                if arrival is None or self.revisions.get(subject, 0) != revision:
                    continue
                if any(self.rows[(subject, x)].status in (NODE_RUNNING, NODE_SCHEDULED)
                       for x in archive if (subject, x) in self.rows):
                    continue
                self.revisions[subject] = revision + 1
                for x in archive:
                    self._take_away(subject, x, now=now, reason="arrival")
                del self.waiting[(subject, name)]
                self.archive.append((subject, {
                    "node": name, "status": "entered", "started_at": arrival.arrived_at,
                    "finished_at": now, "lease": arrival.ref, "archived_at": now,
                    "reason": "lane"}))
                self.rows[(subject, name)] = Row(NODE_DONE, arrival.arrived_at, now)
                entered.append(subject)
        return entered

    def queued(self, name: str) -> int:
        with self._lock:
            return sum(1 for (_, n) in self.waiting if n == name)

    def latest(self, subjects: list[Any], name: str,
               reason: str | None) -> dict[Any, str]:
        wanted = set(subjects)
        out: dict[Any, str] = {}
        with self._lock:
            for s, e in self.archive:
                if s in wanted and e["node"] == name and reason in (None, e["reason"]):
                    out[s] = max(out.get(s, ""), e["archived_at"])
        return out

    def status_counts(self, name: str) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for (_, n), row in self.rows.items():
                if n == name:
                    counts[row.status] = counts.get(row.status, 0) + 1
        return counts

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        wanted = {str(s) for s in subjects}
        with self._lock:
            lines = [(row.finished_at or at, n, str(s), row)
                     for (s, n), row in self.rows.items()
                     if str(s) in wanted
                     and row.status not in (NODE_SKIPPED, NODE_OMITTED, NODE_SCHEDULED)]
        out: dict[str, list[list[Any]]] = {}
        for ended, n, subject, row in sorted(lines, key=lambda x: (x[0], x[1])):
            seconds = round(_epoch(ended) - _epoch(row.started_at), 1)
            out.setdefault(subject, []).append([n, ended, seconds, row.status])
        return out

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> bool:
        with self._lock:
            return all(
                (subject, p) in self.rows
                and self.rows[(subject, p)].status in NODE_SATISFYING
                for p in parents)


def _epoch(moment: str) -> float:
    return datetime.fromisoformat(moment).timestamp()
