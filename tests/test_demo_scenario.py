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


def test_the_page_gets_the_graph_and_a_detail(scenario):
    fixed = json.loads(scenario.static())
    assert set(fixed["layout"]) == set(fixed["graph"]["nodes"])
    world = scenario.World(seed=1)
    world.add_bricks(1, tnt=True)
    world.tick(5)
    detail = json.loads(world.detail(1))
    assert detail["tnt"] is True and "scan" in detail["progress"]
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
