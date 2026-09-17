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

    def inflate(self, candidates):
        """Objects pass; ids are fetched — the adapter alone can tell."""
        thin = [c for c in candidates if isinstance(c, int)]
        if thin:
            self.calls.append(f"inflate({sorted(thin)})")
        fat = {i: self.bricks[i] for i in thin if i in self.bricks}
        return [fat[c] if isinstance(c, int) else c
                for c in candidates if not isinstance(c, int) or c in fat]

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
    assert adapter.calls == [f"applies({i},scan)" for i in (1, 2, 3)], "nothing inflated"
    assert lease.token, "the lease still carries its proof"


def test_a_generator_of_items_works_like_a_list(world):
    """Any iterable of YOUR objects is items — nothing forces a list."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=(b for b in bricks))
    assert [b.id for b in lease] == [1, 2, 3]
    assert adapter.calls == [f"applies({i},scan)" for i in (1, 2, 3)], "nothing inflated"


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
    assert adapter.calls.count("inflate([1, 2, 3])") == 1, "one query for the batch"


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
    """On the query path, a row deleted between the claim and the inflate is
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
    """An adapter that only knows how to name and inflate its items works."""

    class Bare(Adapter):
        def id_of(self, item):
            return item

        def inflate(self, candidates):
            return list(candidates)

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


def test_the_janitor_pass_speaks_items_too(world):
    """`settle` and `skip` take items like everything else here."""
    bricks, adapter, items = world
    items.conclude("scan", items.claim("scan", 10, candidates=bricks[:2]))
    items.conclude("sort", items.claim("sort", 10, candidates=bricks[:2]))

    # `skip` gives up an optional node without claiming it first.
    assert items.skip("polish", candidates=bricks[1:2]) == 1
    assert items.progress(bricks[1])["polish"] == NODE_SKIPPED

    # `settle` reports per node, and takes items rather than ids.
    assert isinstance(items.settle(bricks), dict)
    assert [b.id for b in items.claim("pack", 10, candidates=bricks[:2])] == [2]


def test_arrivals_of_different_versions_are_counted_together():
    """One call per distinct ref, but ONE set of counts: adding them, not
    letting the last ref's counts replace the others'."""
    from quazardous.grampy import Lane

    graph = Graph(Document("lane"), (
        Node("inbox", lane=Lane.throttle(cooldown="1h")),
        Node("work", parents=("inbox",)),
    ))
    bricks = [Brick(1, stamp="v1"), Brick(2, stamp="v2"), Brick(3, stamp="v3")]
    items = Items(NodeJournal(MemoryDriver(), graph), Bricks(bricks))

    counts = items.arrive("inbox", bricks)
    assert counts["queued"] == 3, "three refs, three groups, three arrivals"
    assert sum(counts.values()) == 3, "nothing lost between the groups"


def test_ids_are_accepted_wherever_items_are(world):
    """Ids work everywhere items do — and mixed with them. What was handed
    over as an object is reused; what was named by id alone is loaded."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=[1, 2, 3])
    assert [b.id for b in lease] == [1, 2, 3]
    assert all(isinstance(b, Brick) for b in lease), "ids came back as objects"
    assert adapter.calls.count("inflate([1, 2, 3])") == 1

    items.conclude("scan", lease)
    assert items.settle([1, 2, 3]) is not None, "settle takes ids too"


def test_objects_and_ids_may_be_mixed_in_one_call(world):
    """Half a batch in hand, half named by id: neither is loaded twice."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=[bricks[0], 2, bricks[2]])
    assert [b.id for b in lease] == [1, 2, 3]
    assert lease[0] is bricks[0], "the object given is the object returned"
    assert adapter.calls.count("inflate([2])") == 1, "only the id was fetched"


def test_the_adapter_alone_decides_what_is_an_id(world):
    """The layer never guesses: it hands the batch over as it came, and what
    comes back is items. Here the adapter fetches ints and passes bricks."""
    bricks, adapter, items = world
    lease = items.claim("scan", 10, candidates=[bricks[0], 2])
    assert adapter.calls.count("inflate([2])") == 1, "only the id was fetched"
    assert [b.id for b in lease] == [1, 2]


def test_inflate_is_only_needed_when_a_query_names_the_candidates():
    """Items travel with the claim, so an adapter that never meets a driver
    query never needs `inflate` — and is told plainly when it does."""

    class NoLoad(Adapter):
        def id_of(self, brick):
            return brick.id

    import sqlite3

    from quazardous.grampy.drivers.sqlite import Query, SqliteDriver, schema

    conn = sqlite3.connect(":memory:")
    for statement in schema():
        conn.execute(statement)
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO docs VALUES (?)", [(1,), (2,)])

    bricks = [Brick(1), Brick(2)]
    items = Items(NodeJournal(SqliteDriver(conn), (Node("a"), Node("b")), ), NoLoad())
    assert [b.id for b in items.claim("a", 10, candidates=bricks)] == [1, 2]

    # A driver query hands back ids, and there is nothing to turn them into.
    fresh = Items(NodeJournal(SqliteDriver(conn), (Node("b"),)), NoLoad())
    with pytest.raises(NotImplementedError, match=r"NoLoad needs `inflate`"):
        fresh.claim("b", 10, candidates=Query("SELECT id FROM docs ORDER BY id"))


def test_an_adapter_may_be_one_tolerant_method():
    """An application that works in ids — or mixes them with objects —
    writes `id_of` and nothing else: `inflate` lets everything through."""

    @dataclass
    class Doc:
        id: int

    class DocsAdapter(Adapter):
        def id_of(self, candidate):
            return getattr(candidate, "id", candidate)

    items = Items(NodeJournal(MemoryDriver(), (Node("a"), Node("b"))), DocsAdapter())
    assert sorted(items.claim("a", 10, candidates=[1, 2])) == [1, 2]
    assert [d.id for d in items.claim("b", 10, candidates=[Doc(1), Doc(2)])] == [1, 2]
    mixed = items.claim("a", 10, candidates=[3, Doc(4)])
    assert [getattr(x, "id", x) for x in mixed] == [3, 4]
