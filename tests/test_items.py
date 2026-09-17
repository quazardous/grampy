"""The items layer: objects in, objects out, handlers where data is needed."""
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from quazardous.grampy import Document, Graph, Node, NodeJournal
from quazardous.grampy.dag import NODE_DONE, NODE_OMITTED, NODE_SKIPPED
from quazardous.grampy.drivers.memory import MemoryDriver
from quazardous.grampy.items import Adapter, Items


@dataclass
class Brick:
    id: int
    crate: str = "factory"
    kind: str = "plain"
    stamp: str = "v1"


GRAPH = Graph(Document("line", namespace="demo"), (
    Node("scan", choice=True),
    Node("sort", parents=("scan",)),
    Node("burn", parents=("scan",)),
    Node("polish", parents=("sort",), optional=True),
    Node("pack", parents=("polish",)),
))


class Bricks(Adapter):
    """Everything the workflow needs to know about a brick, and no more."""

    def __init__(self, bricks):
        self.bricks = {b.id: b for b in bricks}
        #: Every call the layer made, to prove it asks once and loads once.
        self.calls: list[str] = field(default_factory=list)
        self.calls = []

    def id_of(self, brick):
        return brick.id

    def load(self, ids):
        self.calls.append(f"load({sorted(ids)})")
        return [self.bricks[i] for i in ids if i in self.bricks]

    def policy_of(self, brick):
        return brick.crate

    def ref_of(self, brick):
        return brick.stamp

    def branch(self, brick, node):
        self.calls.append(f"branch({brick.id},{node})")
        return "burn" if brick.kind == "scrap" else "sort"

    def applies(self, brick, node):
        self.calls.append(f"applies({brick.id},{node})")
        return node != "polish" or brick.crate == "factory"


@pytest.fixture
def world():
    bricks = [Brick(1), Brick(2, crate="salvage"), Brick(3, kind="scrap")]
    adapter = Bricks(bricks)
    journal = NodeJournal(MemoryDriver(), GRAPH)
    return bricks, adapter, Items(journal, adapter)


def test_candidates_are_items_too_and_are_not_loaded_twice(world):
    """Nothing here asks the caller to hold ids: what goes in is what comes
    out, and a batch already in hand is never fetched again."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=bricks)
    assert [b.id for b in lease] == [1, 2, 3]
    assert all(b is bricks[b.id - 1] for b in lease), "the very objects given"
    assert adapter.calls == [f"applies({i},scan)" for i in (1, 2, 3)], "no load"
    assert lease.token, "the lease still carries its proof"


def test_a_generator_of_items_works_like_a_list(world):
    """Any iterable of YOUR objects is items — nothing forces a list."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=(b for b in bricks))
    assert [b.id for b in lease] == [1, 2, 3]
    assert adapter.calls == [f"applies({i},scan)" for i in (1, 2, 3)], "no load"


def test_a_driver_query_is_handed_over_untouched_and_the_lease_is_loaded():
    """The one place ids are unavoidable: the storage itself produces the
    candidates, so the layer loads the lease afterwards, in one call."""
    import sqlite3

    from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema

    conn = sqlite3.connect(":memory:")
    for statement in schema():
        conn.execute(statement)
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO docs VALUES (?)", [(1,), (2,), (3,)])

    bricks = [Brick(1), Brick(2), Brick(3)]
    adapter = Bricks(bricks)
    items = Items(NodeJournal(SqliteDriver(conn), GRAPH), adapter)
    lease = items.claim("scan", 10, candidates=Query("SELECT id FROM docs ORDER BY id"))
    assert [b.id for b in lease] == [1, 2, 3]
    assert adapter.calls.count("load([1, 2, 3])") == 1, "one query for the batch"


def test_a_choice_takes_the_branch_the_handler_names(world):
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=bricks)
    assert items.conclude("scan", lease) == 3

    # Brick 3 is scrap: it burns, so `sort` is omitted for it alone. The
    # branch taken has no row yet — it is simply what comes next.
    assert items.progress(bricks[2])["sort"] == NODE_OMITTED
    assert "burn" not in items.progress(bricks[2]), "the branch taken is next"
    assert items.progress(bricks[0])["burn"] == NODE_OMITTED
    assert "sort" not in items.progress(bricks[0])

    # One call, two branches: each item went where its handler said.
    assert [b.id for b in items.claim("burn", 10, candidates=bricks)] == [3]
    assert [b.id for b in items.claim("sort", 10, candidates=bricks)] == [1, 2]


def test_an_optional_node_is_given_up_on_the_items_that_refuse_it(world):
    bricks, adapter, items = world
    items.conclude("scan", items.claim("scan", 10, candidates=bricks[:2]))
    items.conclude("sort", items.claim("sort", 10, candidates=bricks[:2]))

    lease = items.claim("polish", 10, candidates=bricks[:2])
    assert [b.id for b in lease] == [1], "the salvage brick is not in the lease"
    assert items.progress(bricks[1])["polish"] == NODE_SKIPPED, (
        "and the journal records that the decision was taken")

    items.conclude("polish", lease)
    # Nothing downstream waits for a step that was given up.
    assert [b.id for b in items.claim("pack", 10, candidates=bricks[:2])] == [1, 2]


def test_the_policy_and_the_ref_are_read_from_the_item(world):
    bricks, adapter, items = world
    assert items.admit(bricks) == 3
    assert items.journal.driver.policy_of[2] == "salvage"
    assert items.journal.driver.policy_of[1] == "factory"


def test_an_item_that_no_longer_loads_does_not_lose_the_claim(world):
    """On the query path, a row deleted between the claim and the load is
    named rather than dropped, and the rest of the lease still concludes."""
    import sqlite3

    from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema

    conn = sqlite3.connect(":memory:")
    for statement in schema():
        conn.execute(statement)
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO docs VALUES (?)", [(1,), (2,), (3,)])
    bricks, adapter, _ = world
    items = Items(NodeJournal(SqliteDriver(conn), GRAPH), adapter)
    del adapter.bricks[2]
    lease = items.claim("scan", 10, candidates=Query("SELECT id FROM docs ORDER BY id"))
    assert [b.id for b in lease] == [1, 3]
    assert lease.missing == (2,), "named, not silently dropped"
    assert items.conclude("scan", lease) == 2, "the others still conclude"


def test_the_adapter_is_asked_once_per_item_per_call(world):
    bricks, adapter, items = world
    items.claim("scan", 10, candidates=bricks)
    for brick in (1, 2, 3):
        assert adapter.calls.count(f"applies({brick},scan)") == 1


def test_the_handlers_have_answers_for_an_application_with_nothing_to_say():
    """An adapter that only knows how to name and load its items works."""

    class Bare(Adapter):
        def id_of(self, item):
            return item

        def load(self, ids):
            return list(ids)

    journal = NodeJournal(MemoryDriver(), (Node("a"), Node("b", parents=("a",))))
    items = Items(journal, Bare())
    lease = items.claim("a", 10, candidates=["x", "y"])
    assert sorted(lease) == ["x", "y"]
    assert items.conclude("a", lease) == 2
    assert items.progress("x") == {"a": NODE_DONE}


def test_a_duration_may_be_a_timedelta():
    """`timedelta` is how Python says a duration; it is stored as the short
    text a graph round-trips to JSON."""
    from datetime import timedelta

    from quazardous.grampy import Graph
    from quazardous.grampy.timing import Rate, Retry

    graph = Graph(Document("t"), (
        Node("a", lease=timedelta(minutes=2),
             retry=Retry(3, timedelta(seconds=10), max_delay=timedelta(minutes=5)),
             rate=(Rate(100, timedelta(minutes=1)),)),
        Node("b", parents=("a",), wait="x", timeout=timedelta(days=7)),
        Node("c", parents=("b",), optional=True, grace=timedelta(seconds=30)),
    ))
    assert graph.nodes[0].lease == "2m"
    assert graph.nodes[0].retry.delay == "10s"
    assert graph.nodes[0].retry.max_delay == "5m"
    assert graph.nodes[0].rate[0].period == "1m"
    assert graph.nodes[1].timeout == "7d"
    assert graph.nodes[2].grace == "30s"
    written = graph.to_json()
    assert Graph.from_json(written).to_json() == written, "still round-trips"
