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
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..dag import NODE_DONE, NODE_RUNNING, NODE_SATISFYING, NODE_SKIPPED
from ..journal import Entry


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
        self._lock = threading.RLock()

    # -- write -------------------------------------------------------------

    def scan(self, candidates: Any, *, name: str, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int) -> Iterator[list[Entry]]:
        """No pre-filter: every candidate is read, the journal decides."""
        subjects = list(candidates)
        for start in range(0, len(subjects), page):
            with self._lock:
                yield [Entry(subject, self.revisions.get(subject, 0),
                             {n: self.rows[(subject, n)].status for n in nodes
                              if (subject, n) in self.rows})
                       for subject in subjects[start:start + page]]

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        taken: list[Any] = []
        with self._lock:
            for subject, revision in entries:
                if (subject, name) in self.rows:
                    continue
                if self.revisions.get(subject, 0) != revision:
                    continue
                self.rows[(subject, name)] = Row(
                    status, now, now if status == NODE_SKIPPED else None, lease)
                taken.append(subject)
        return taken

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None) -> int:
        count = 0
        with self._lock:
            for subject in subjects:
                row = self.rows.get((subject, name))
                if row is None or row.status != NODE_RUNNING:
                    continue
                if lease is not None and row.lease != lease:
                    continue
                row.status, row.finished_at = status, now
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

    def forget(self, name: str, subjects: list[Any]) -> int:
        with self._lock:
            for subject in subjects:
                self.revisions[subject] = self.revisions.get(subject, 0) + 1
            return sum(1 for subject in subjects
                       if self.rows.pop((subject, name), None) is not None)

    def release(self, name: str, *, older_than: str) -> int:
        with self._lock:
            stale = [key for key, row in self.rows.items()
                     if key[1] == name and row.status == NODE_RUNNING
                     and row.started_at < older_than]
            for key in stale:
                del self.rows[key]
            return len(stale)

    # -- read --------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        with self._lock:
            return {n: row.status for (s, n), row in self.rows.items() if s == subject}

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
                     if str(s) in wanted and row.status != NODE_SKIPPED]
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
