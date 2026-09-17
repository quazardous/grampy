"""The demo's brick sorter runs on the real journal and reaches every place."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest

DEMO = Path(__file__).resolve().parents[1] / "docs" / "demo"


@pytest.fixture(scope="module")
def scenario():
    sys.path.insert(0, str(DEMO))
    try:
        import scenario as module
        yield module
    finally:
        sys.path.remove(str(DEMO))
        sys.modules.pop("scenario", None)


def test_the_factory_ships_rejects_retries_and_waits(scenario):
    world = scenario.World(seed=11)
    world.add_bricks(10)
    places = Counter()
    for _ in range(900):
        state = json.loads(world.tick(1.0))
        places.update(b["place"] for b in state["bricks"])
    assert world.shipped > 50
    assert world.exploded > 0, "a TNT brick must be rejected now and then"
    for place in ("queue", "station", "shelf", "retry", "shipped", "boom"):
        assert places[place], f"no brick ever stood at {place!r}"
    assert sum(world.sorted.values()) == world.shipped
    assert all(world.sorted.values()), "every colour tray gets bricks"


def test_a_tnt_brick_hides_its_colour_until_defused(scenario):
    world = scenario.World(seed=3, settings=scenario.Settings(
        arrivals_per_minute=0.0, defuse_failure=0.0, squad_delay=0.0))
    world.add_bricks(1, tnt=True)
    colours = set()
    for _ in range(200):
        [brick] = json.loads(world.tick(1.0))["bricks"] or [None]
        if brick is None:
            break
        colours.add(brick["colour"])
        assert brick["revealed"] == (brick["colour"] is not None)
    assert None in colours and colours - {None}, "hidden first, then revealed"


def test_the_page_gets_the_graph_and_a_detail(scenario):
    fixed = json.loads(scenario.static())
    assert set(fixed["layout"]) == set(fixed["graph"]["nodes"])
    assert len(fixed["colours"]) == 6
    world = scenario.World(seed=1)
    world.add_bricks(1, tnt=True)
    world.tick(5)
    detail = json.loads(world.detail(1))
    assert detail["tnt"] is True and "scan" in detail["progress"]
    assert detail["colour"] is None, "a TNT brick keeps its colour secret"
    assert json.loads(world.detail(999)) is None


def test_the_build_packs_grampy_and_the_scenario(tmp_path, monkeypatch, scenario):
    import zipfile
    sys.path.insert(0, str(DEMO))
    try:
        import build
        out = build.main()
    finally:
        sys.path.remove(str(DEMO))
    names = set(zipfile.ZipFile(out).namelist())
    assert "scenario.py" in names and "quazardous/grampy/journal.py" in names
    assert "quazardous/__init__.py" not in names, "quazardous stays a namespace"


def test_a_sorted_brick_sent_back_twice_runs_once_more_with_its_last_version(scenario):
    world = scenario.World(seed=5, settings=scenario.Settings(
        arrivals_per_minute=0.0, tnt_share=0.0, returns_share=0.0))
    world.add_bricks(1, tnt=False)
    for _ in range(60):
        world.tick(1.0)
        if world.finished.get(1, ("",))[0] == "shipped":
            break
    assert world.shipped == 1
    shipped_at = world.elapsed
    assert world.send_back(1) is True
    world.tick(1.0)
    assert world.send_back(1) is True, "a waiting brick merges its new version"
    assert world.send_back(999) is False
    state = json.loads(world.tick(1.0))
    [brick] = [b for b in state["bricks"] if b["id"] == 1]
    assert brick["place"] == "inbox" and brick["version"] == 3 and brick["cooldown"] > 0
    arrival = world.journal.arrival(1, "inbox")
    assert arrival.ref == "v3" and arrival.place == arrival.arrived_at, "throttle keeps its place"
    while world.elapsed - shipped_at < scenario.COOLDOWN - 1:
        state = json.loads(world.tick(1.0))
        assert [b for b in state["bricks"] if b["id"] == 1][0]["place"] == "inbox"
    for _ in range(80):
        world.tick(1.0)
        if world.shipped == 2:
            break
    assert world.shipped == 2, "one more pass, not two"
    history = world.journal.history(1)
    assert [e["lease"] for e in history if e["status"] == "entered"] == ["v1", "v3"]
    assert any(e["node"] == "pack" and e["reason"] == "arrival" for e in history)


def test_salvage_bricks_skip_the_polish_station(scenario):
    """The demo shows the documented way to give one source an extra step:
    `polish` is optional for everyone, and the salvage channel's grace makes
    the janitor skip it."""
    world = scenario.World(seed=2, settings=scenario.Settings(
        arrivals_per_minute=0.0, tnt_share=0.0, returns_share=0.0, salvage_share=1.0))
    world.add_bricks(1)
    assert world.journal.channel(1) == "salvage"
    for _ in range(200):
        world.tick(1.0)
        if world.journal.progress(1).get("pack") == "done":
            break
    assert world.journal.progress(1)["polish"] == "skipped"

    fresh = scenario.World(seed=2, settings=scenario.Settings(
        arrivals_per_minute=0.0, tnt_share=0.0, returns_share=0.0, salvage_share=0.0))
    fresh.add_bricks(1)
    assert fresh.journal.channel(1) is None
    for _ in range(200):
        fresh.tick(1.0)
        if fresh.journal.progress(1).get("pack") == "done":
            break
    assert fresh.journal.progress(1)["polish"] == "done", "a factory brick is polished"
