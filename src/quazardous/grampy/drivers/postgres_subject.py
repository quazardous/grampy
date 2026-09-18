"""A POSTGRESQL LAYOUT WITH ONE ROW PER SUBJECT — its progress in JSONB.

    PostgresSubjectDriver(execute, subjects, history, subject="subject",
                          limits=None, arrivals=None)

Same journal, same guarantees as `PostgresDriver`, other tables: an
application picks this layout knowingly, for what it gains and what it
costs (`docs/drivers.md`).

────────────────────────────────────────────────────────────────────────
THE TABLE
────────────────────────────────────────────────────────────────────────

    subjects    one row per subject, the subject as primary key:
                `revision` (integer, not null), `policy` and `version`
                (nullable text), and `nodes` (JSONB, not null) —
                `{node: {"status", "started_at", "finished_at", "lease"}}`,
                the same four fields a node row carries in the other layout.

`history`, `limits` and `arrivals` are the tables `PostgresDriver` takes,
unchanged. Rows are created on demand.

────────────────────────────────────────────────────────────────────────
WHAT THE LAYOUT BUYS, WHAT IT COSTS
────────────────────────────────────────────────────────────────────────

A page of candidates brings their whole progress in the same read, and a
claim writes in ONE statement: the revision sits on the row it guards, so
there is nothing to seed and nothing to lock besides. The price is that
every write to a subject rewrites its row, and two claims on two nodes of
ONE subject — a fork — queue behind each other's row lock. "No row for this
node" is not a plain index any more: a large table wants an expression index
per hot node, which is the application's schema, not this driver's.

────────────────────────────────────────────────────────────────────────
HOW `insert_if_unchanged` STAYS HONEST HERE
────────────────────────────────────────────────────────────────────────

One `INSERT … ON CONFLICT DO UPDATE … WHERE revision = read AND no row for
the node`: PostgreSQL locks the existing row and evaluates that condition on
its LATEST version, so a claim meeting a forget in flight waits for it and
finds the revision raised. A subject without a row is inserted at the
revision read, 0; a forget creating it meanwhile makes it a conflict with a
revision that no longer matches.

Operations that must remove rows and archive them lock the subjects first
(raising the revision when the protocol says so), then read, rewrite and
archive in one statement that sees what the lock waited for.
"""
from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB

from ..dag import (
    NODE_SATISFYING,
)
from ..names import Outcome, Reason, Status
from ..protocol import Entry, Page
from .postgres import PostgresCommon, _clock, _decoded, _epoch, _plain, _ranked

#: THE COLUMNS THE SUBJECTS TABLE MUST CARRY, besides the subject.
SUBJECT_COLUMNS = ("revision", "policy", "version", "nodes")
#: THE FIELDS OF ONE NODE, inside `nodes`.
FIELDS = ("status", "started_at", "finished_at", "lease")


def _row(status: str, started_at: str, finished_at: str | None,
         lease: str | None) -> dict[str, Any]:
    return {"status": status, "started_at": started_at, "finished_at": finished_at,
            "lease": lease}


class PostgresSubjectDriver(PostgresCommon):
    """One row per subject in `subjects`, its node rows as JSONB."""

    def __init__(self, execute: Callable[[Any], Any], subjects: sa.Table,
                 history: sa.Table, *, subject: str = "subject",
                 limits: sa.Table | None = None,
                 arrivals: sa.Table | None = None) -> None:
        missing = [c for c in (subject, *SUBJECT_COLUMNS) if c not in subjects.c]
        if missing:
            raise ValueError(
                f"table {subjects.name!r} lacks the column(s) {missing} — it needs "
                f"{subject!r} and {', '.join(SUBJECT_COLUMNS)}")
        super().__init__(execute, history, subject=subject, limits=limits,
                         arrivals=arrivals)
        self.subjects = subjects
        self._subject = subjects.c[subject]

    # -- the JSON, in SQL ----------------------------------------------------

    def _field(self, node: str, field: str, nodes: Any = None) -> Any:
        return sa.func.jsonb_extract_path_text(
            self.subjects.c.nodes if nodes is None else nodes, node, field)

    @staticmethod
    def _json(value: Any) -> Any:
        return sa.cast(sa.literal(value, JSONB), JSONB)

    def _merged(self, *parts: Any) -> Any:
        """`a || b || …` — on a key both hold, the right one wins."""
        out = parts[0]
        for part in parts[1:]:
            out = out.op("||", return_type=JSONB)(part)
        return out

    def _seed(self, subjects: list[Any]) -> None:
        """Create the rows missing, so that a lock can be taken on each."""
        self._execute(
            self._insert_rows(self.subjects, {self._key: sorted(subjects)}, revision=0, nodes={})
            .on_conflict_do_nothing())

    def _raise_revisions(self, subjects: list[Any]) -> None:
        """Locks the rows until the transaction ends — see the module."""
        s = self.subjects
        insert = self._insert_rows(s, {self._key: sorted(subjects)}, revision=1, nodes={})
        self._execute(insert.on_conflict_do_update(
            index_elements=[self._key], set_={"revision": s.c.revision + 1}))

    def _take_away(self, where: Any, names: list[str], *, now: str, reason: str,
                   row_filter: Callable[[Any], Any] | None = None) -> list[Any]:
        """Remove the rows of `names` from the subjects matching `where` —
        only those `row_filter` keeps, when given — and archive them, in ONE
        statement. Return the subjects touched, once per row archived."""
        s, h = self.subjects, self.history_table
        each = sa.func.jsonb_each(s.c.nodes).table_valued("key", "value").alias("e")
        conditions = [where, each.c.key.in_(names)]
        if row_filter is not None:
            conditions.append(row_filter(each.c.value))
        old = (sa.select(self._subject.label("subject"), each.c.key.label("node"),
                         each.c.value.label("row"))
               .select_from(s.join(each, sa.true()))
               .where(*conditions)
               .with_for_update(of=s)
               .cte("old"))
        # The subject's row loses every key archived for it, and nothing else.
        gone = (sa.select(old.c.subject, sa.func.array_agg(old.c.node).label("names"))
                .group_by(old.c.subject).cte("gone_keys"))
        update = (sa.update(s)
                  .where(self._subject == gone.c.subject)
                  .values(nodes=s.c.nodes.op("-", return_type=JSONB)(gone.c.names))
                  .cte("gone"))
        field = lambda f: sa.func.jsonb_extract_path_text(old.c.row, f)  # noqa: E731
        rows = self._execute(
            sa.insert(h).from_select(
                [self._key, "node", *FIELDS, "archived_at", "reason"],
                sa.select(old.c.subject, old.c.node, *[field(f) for f in FIELDS],
                          sa.literal(now), sa.literal(reason)))
            # PostgreSQL runs a data-modifying WITH whether or not it is
            # read; SQLAlchemy renders one only when told to.
            .add_cte(update)
            .returning(h.c[self._key])).fetchall()
        return [row[0] for row in rows]

    # -- write ---------------------------------------------------------------

    def scan(self, candidates: Any, *, name: str | None, nodes: tuple[str, ...],
             parents: tuple[str, ...], page: int, now: str | None,
             after: tuple[str, ...] = ()) -> Iterator[list[Entry]]:
        """Pre-filters in SQL, and brings the progress in the same read —
        and, when `now` is None, the server's clock."""
        s = self.subjects
        c = _ranked(candidates)
        columns = list(c.c)
        candidate = columns[0]
        grouped = c.c["grampy_key"] if "grampy_key" in c.c else sa.literal(None)
        clock = _clock() if now is None else sa.literal(now)
        query = (
            sa.select(candidate, c.c.grampy_rank, sa.func.coalesce(s.c.revision, 0),
                      s.c.policy, s.c.version, grouped.label("grampy_key"), s.c.nodes,
                      clock.label("grampy_now"))
            .select_from(c.outerjoin(s, self._subject == candidate))
            .order_by(c.c.grampy_rank)
            .limit(int(page)))
        if name is not None:
            query = query.where(sa.or_(
                s.c.nodes.op("->", return_type=JSONB)(name).is_(None),
                sa.and_(self._field(name, "status") == Status.SCHEDULED,
                        self._field(name, "started_at") <= clock)))
        for p in parents:
            query = query.where(self._field(p, "status").in_(NODE_SATISFYING))
        for d in after:
            query = query.where(s.c.nodes.op("->", return_type=JSONB)(d).is_(None))
        wanted = set(nodes)
        last = None
        while True:
            paged = query if last is None else query.where(c.c.grampy_rank > last)
            found = self._execute(paged).fetchall()
            if not found:
                return
            last = found[-1][1]
            entries = []
            for f in found:
                progress = {n: r for n, r in (_decoded(f[6]) or {}).items() if n in wanted}
                entries.append(Entry(
                    f[0], int(f[2]),
                    {n: r["status"] for n, r in progress.items()},
                    {n: r["started_at"] for n, r in progress.items()
                     if r["status"] == Status.SCHEDULED},
                    {n: r["finished_at"] for n, r in progress.items()
                     if r.get("finished_at") is not None},
                    f[3], f[4], f[5]))
            yield Page(entries, now=found[0][7])
            if len(found) < page:
                return

    def insert_if_unchanged(self, name: str, entries: list[tuple[Any, int]], *,
                            status: str, now: str, lease: str | None) -> list[Any]:
        """ONE statement — see the module."""
        if not entries:
            return []
        s = self.subjects
        row = _row(status, now, None if status == Status.RUNNING else now, lease)
        entries = sorted(entries, key=lambda e: e[0])
        insert = self._insert_rows(s, {self._key: [x for x, _ in entries],
                                       "revision": [v for _, v in entries]},
                                   nodes={name: row})
        rows = self._execute(insert.on_conflict_do_update(
            index_elements=[self._key],
            set_={"nodes": self._merged(s.c.nodes, insert.excluded.nodes)},
            where=sa.and_(
                s.c.revision == insert.excluded.revision,
                sa.or_(s.c.nodes.op("->", return_type=JSONB)(name).is_(None),
                       sa.and_(self._field(name, "status") == Status.SCHEDULED,
                               self._field(name, "started_at") <= now))))
            .returning(self._subject)).fetchall()
        return [r[0] for r in rows]

    def skip_where(self, name: str, candidates: Any, *, parents: tuple[str, ...],
                   now: str, version: str | None, limit: int | None = None) -> list[Any]:
        """`skip` IN ONE UPSERT, instead of reading every candidate page.

        The candidates are joined to their documents once, and the rule read
        there: no row for `name`, every parent satisfying, pinned to
        `version` or to none. The upsert's own condition — the revision
        still the one read, still no row for `name` — is checked by
        PostgreSQL on each document's latest version, as for a claim: a
        subject whose revision moved meanwhile is not skipped. Unranked
        unless `limit` asks for the first candidates by rank."""
        s = self.subjects
        c = _plain(candidates) if limit is None else _ranked(candidates)
        candidate = list(c.c)[0]
        mine = s.alias("mine")
        eligible = [mine.c.nodes.op("->", return_type=JSONB)(name).is_(None)]
        eligible += [self._field(p, "status", mine.c.nodes).in_(NODE_SATISFYING)
                     for p in parents]
        if version is not None:
            eligible.append(sa.or_(mine.c.version.is_(None), mine.c.version == version))
        read = (sa.select(candidate.label("subject"),
                          sa.func.coalesce(mine.c.revision, 0).label("revision"))
                .select_from(c.outerjoin(mine, mine.c[self._key] == candidate))
                .where(*eligible))
        if limit is None:
            read = read.distinct()
        else:
            read = (read.group_by(candidate, mine.c.revision)
                    .order_by(sa.func.min(c.c.grampy_rank)).limit(int(limit)))
        read = read.subquery("read")
        insert = postgresql.insert(s).from_select(
            [self._key, "revision", "nodes"],
            sa.select(read.c.subject, read.c.revision,
                      self._json({name: _row(Status.SKIPPED, now, now, None)}))
            .order_by(read.c.subject))
        rows = self._execute(insert.on_conflict_do_update(
            index_elements=[self._key],
            set_={"nodes": self._merged(s.c.nodes, insert.excluded.nodes)},
            where=sa.and_(s.c.revision == insert.excluded.revision,
                          s.c.nodes.op("->", return_type=JSONB)(name).is_(None)))
            .returning(self._subject)).fetchall()
        return [r[0] for r in rows]

    def conclude(self, name: str, subjects: list[Any], *, status: str,
                 now: str, lease: str | None, omit: tuple[str, ...],
                 reset: tuple[str, ...], reschedule: str | None) -> int:
        """The conclusion and its omissions are ONE update: the omitted rows
        are merged UNDER the subject's own, so a node that has a row keeps
        it."""
        s = self.subjects
        concluded_row = self._merged(
            s.c.nodes.op("->", return_type=JSONB)(name),
            self._json({"status": status, "finished_at": now}))
        parts = []
        if omit:
            parts.append(self._json({o: _row(Status.OMITTED, now, now, None) for o in omit}))
        parts += [s.c.nodes, sa.func.jsonb_build_object(name, concluded_row, type_=JSONB)]
        update = sa.update(s).where(self._in(self._subject, sorted(subjects)),
                                    self._field(name, "status") == Status.RUNNING)
        if lease is not None:
            update = update.where(self._field(name, "lease") == lease)
        concluded = [r[0] for r in self._execute(
            update.values(nodes=self._merged(*parts)).returning(self._subject)).fetchall()]
        if reset and concluded:
            self._raise_revisions(concluded)
            self._take_away(self._in(self._subject, sorted(concluded)), list(reset),
                            now=now, reason=Reason.LOOP)
        if reschedule is not None and concluded:
            self._take_away(self._in(self._subject, sorted(concluded)), [name],
                            now=now, reason=Reason.RETRY)
            self._execute(
                sa.update(s).where(self._in(self._subject, sorted(concluded)))
                .values(nodes=self._merged(s.c.nodes, self._json(
                    {name: _row(Status.SCHEDULED, reschedule, None, None)}))))
        return len(concluded)

    def adopt(self, name: str, subjects: list[Any], *, now: str) -> int:
        s = self.subjects
        insert = self._insert_rows(s, {self._key: sorted(subjects)}, revision=0,
                                   nodes={name: _row(Status.DONE, now, now, None)})
        return len(self._execute(insert.on_conflict_do_update(
            index_elements=[self._key],
            set_={"nodes": self._merged(s.c.nodes, insert.excluded.nodes)},
            where=s.c.nodes.op("->", return_type=JSONB)(name).is_(None))
            .returning(self._subject)).fetchall())

    def forget(self, name: str, subjects: list[Any], *, now: str) -> int:
        """Revision first — the lock — then the archive, which sees
        everything committed before the lock was granted."""
        subjects = sorted(subjects)
        self._raise_revisions(subjects)
        return len(self._take_away(self._in(self._subject, subjects), [name],
                                   now=now, reason=Reason.FORGET))

    def release(self, name: str, *, older_than: str, now: str,
                only: tuple[str, ...] | None = None, exclude: tuple[str, ...] = (),
                version: str | None = None) -> int:
        s = self.subjects
        where = sa.and_(self._field(name, "status") == Status.RUNNING,
                        self._field(name, "started_at") < older_than)
        if only is not None:
            where = sa.and_(where, s.c.policy.in_(list(only)))
        if exclude:
            where = sa.and_(where, sa.or_(s.c.policy.is_(None),
                                          s.c.policy.not_in(list(exclude))))
        if version is not None:
            where = sa.and_(where, sa.or_(s.c.version.is_(None), s.c.version == version))
        return len(self._take_away(where, [name], now=now, reason=Reason.RELEASE))

    def running(self, name: str, policies: tuple[str | None, ...] | None) -> int:
        s = self.subjects
        query = sa.select(sa.func.count()).select_from(s).where(
            self._field(name, "status") == Status.RUNNING)
        if policies is not None:
            named = [c for c in policies if c is not None]
            test: sa.ColumnElement[bool] = s.c.policy.in_(named)
            if None in policies:
                test = sa.or_(test, s.c.policy.is_(None))
            query = query.where(test)
        return int(self._execute(query).fetchone()[0])

    def pin(self, subjects: list[Any], version: str) -> int:
        """ONE upsert: a subject gets a version only when it has none."""
        s = self.subjects
        insert = self._insert_rows(s, {self._key: sorted(subjects)}, revision=0, nodes={},
                                   version=version)
        return len(self._execute(insert.on_conflict_do_update(
            index_elements=[self._key], set_={"version": insert.excluded.version},
            where=s.c.version.is_(None)).returning(self._subject)).fetchall())

    def versions(self, subjects: list[Any]) -> dict[Any, str]:
        s = self.subjects
        return {r[0]: r[1] for r in self._execute(
            sa.select(self._subject, s.c.version)
            .where(self._in(self._subject, list(subjects)), s.c.version.is_not(None))).fetchall()}

    def rewrite(self, subject: Any, *, rename: dict[str, str], drop: tuple[str, ...],
                version: str, now: str) -> None:
        self.rewrite_many([subject], rename=rename, drop=drop, version=version, now=now)

    def rewrite_many(self, subjects: list[Any], *, rename: dict[str, str],
                     drop: tuple[str, ...], version: str, now: str) -> None:
        s = self.subjects
        subjects = sorted(set(subjects))
        if not subjects:
            return
        self._raise_revisions(subjects)
        mine = self._in(self._subject, subjects)
        if drop:
            self._take_away(mine, list(drop), now=now, reason=Reason.MIGRATE)
        self._rewrite_arrivals(subjects, rename=rename, drop=drop)
        nodes = s.c.nodes
        if rename:
            # Each renamed key taken out and put back under its new name, in
            # the same UPDATE: the rows are locked since the revisions rose.
            # `-` of every old name first, so a chain of renames never
            # overwrites a key it still has to move.
            each = sa.func.jsonb_each(s.c.nodes).table_valued("key", "value").alias("e")
            moved = sa.func.coalesce(
                sa.select(sa.func.jsonb_object_agg(
                    sa.case(rename, value=each.c.key), each.c.value))
                .select_from(each).where(each.c.key.in_(sorted(rename)))
                .scalar_subquery(),
                sa.cast(sa.literal("{}"), JSONB))
            kept = nodes
            for old in sorted(rename):
                kept = kept.op("-", return_type=JSONB)(sa.literal(old))
            nodes = kept.op("||", return_type=JSONB)(moved)
        self._execute(sa.update(s).where(mine).values(version=version, nodes=nodes))

    def enroll(self, subjects: list[Any], policy: str | None) -> int:
        s = self.subjects
        rows = self._execute(
            self._insert_rows(s, {self._key: sorted(subjects)}, revision=0, nodes={},
                              policy=policy)
            .on_conflict_do_update(index_elements=[self._key], set_={"policy": policy})
            .returning(self._subject)).fetchall()
        return len(rows)

    def policies(self, subjects: list[Any]) -> dict[Any, str]:
        s = self.subjects
        return {r[0]: r[1] for r in self._execute(
            sa.select(self._subject, s.c.policy)
            .where(self._in(self._subject, list(subjects)), s.c.policy.is_not(None))).fetchall()}

    def enter(self, name: str, entries: list[tuple[Any, int]], *,
              archive: tuple[str, ...], now: str) -> list[Any]:
        """LOCK FIRST, CHECK AFTER — as in `PostgresDriver.enter`, the lock
        being the subject's own row."""
        a, s, h = self._need_arrivals(), self.subjects, self.history_table
        if not entries:
            return []
        wanted = dict(entries)
        subjects = sorted(wanted)
        self._seed(subjects)
        current = {r[0]: (int(r[1]), _decoded(r[2]) or {}) for r in self._execute(
            sa.select(self._subject, s.c.revision, s.c.nodes)
            .where(self._in(self._subject, subjects))
            .order_by(self._subject).with_for_update()).fetchall()}
        a_subject = a.c[self._key]
        waiting = {r[0]: (r[1], r[2]) for r in self._execute(
            sa.select(a_subject, a.c.ref, a.c.arrived_at)
            .where(a.c.node == name, self._in(a_subject, subjects))
            .order_by(a_subject).with_for_update()).fetchall()}
        entering = [
            x for x in subjects
            if x in current and current[x][0] == wanted[x] and x in waiting
            and not any(current[x][1].get(n, {}).get("status") in (Status.RUNNING, Status.SCHEDULED)
                        for n in archive)]
        if not entering:
            return []
        self._raise_revisions(entering)
        if archive:
            self._take_away(self._in(self._subject, entering), list(archive),
                            now=now, reason=Reason.ARRIVAL)
        self._execute(sa.delete(a).where(a.c.node == name, self._in(a_subject, entering)))
        self._execute(self._insert_rows(
            h, {self._key: entering, "started_at": [waiting[x][1] for x in entering],
                "lease": [waiting[x][0] for x in entering]},
            node=name, status=Outcome.ENTERED, finished_at=now, archived_at=now,
            reason=Reason.LANE))
        for x in entering:
            self._execute(sa.update(s).where(self._subject == x).values(
                nodes=self._merged(s.c.nodes, self._json(
                    {name: _row(Status.DONE, waiting[x][1], now, None)}))))
        return entering

    # -- read ----------------------------------------------------------------

    def progress(self, subject: Any) -> dict[str, str]:
        return self.progress_many([subject]).get(subject, {})

    def progress_many(self, subjects: list[Any]) -> dict[Any, dict[str, str]]:
        return {subject: {n: r["status"] for n, r in (_decoded(nodes) or {}).items()}
                for subject, nodes in self._execute(
                    sa.select(self._subject, self.subjects.c.nodes)
                    .where(self._in(self._subject, list(subjects)))).fetchall()}

    def status_counts(self, name: str) -> dict[str, int]:
        status = self._field(name, "status")
        return {r[0]: int(r[1]) for r in self._execute(
            sa.select(status, sa.func.count()).select_from(self.subjects)
            .where(status.is_not(None)).group_by(status)).fetchall()}

    def stages(self, subjects: list[Any], *,
               at: str) -> dict[str, list[list[Any]]]:
        """PER SLICE OF 400, as in `PostgresDriver.stages`."""
        s = self.subjects
        each = sa.func.jsonb_each(s.c.nodes).table_valued("key", "value").alias("e")
        status = sa.func.jsonb_extract_path_text(each.c.value, "status")
        started = sa.func.jsonb_extract_path_text(each.c.value, "started_at")
        ended = sa.func.coalesce(sa.func.jsonb_extract_path_text(each.c.value, "finished_at"),
                                 at)
        out: dict[str, list[list[Any]]] = {}
        for start in range(0, len(subjects), 400):
            chunk = list(subjects[start:start + 400])
            rows = self._execute(
                sa.select(self._subject, each.c.key, status, ended.label("ended"),
                          sa.func.round(_epoch(ended) - _epoch(started), 1)
                          .label("seconds"))
                .select_from(s.join(each, sa.true()))
                .where(self._in(self._subject, chunk),
                       status.not_in((Status.SKIPPED, Status.OMITTED, Status.SCHEDULED)))
                .order_by(sa.literal_column("ended"), each.c.key)).fetchall()
            for r in rows:
                out.setdefault(str(r[0]), []).append([r[1], r[3], r[4], r[2]])
        return out

    def parents_concluded(self, parents: tuple[str, ...], subject: Any) -> Any:
        """A SQL expression, correlated with `subject` (a column or a value):
        every parent has a satisfying row — true for no parent at all."""
        if not parents:
            return sa.true()
        s = self.subjects.alias("p")
        return sa.exists().where(
            s.c[self._key] == subject,
            *[self._field(p, "status", s.c.nodes).in_(NODE_SATISFYING) for p in parents])
