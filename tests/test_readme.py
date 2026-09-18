"""The README's examples run, as written.

They are the first code anyone copies, and nothing ran them: an adapter
there kept the old `inflate(self, ids)` of a renamed method, and crashed on
the very objects the same example hands in as candidates; the next one
called a loader the page never defined. This executes every python block of
the README, as is, so the page and the library cannot drift apart again.
"""
from __future__ import annotations

import re
import sys
import types
from pathlib import Path

README = Path(__file__).resolve().parent.parent / "README.md"


def _blocks() -> list[str]:
    return re.findall(r"```python\n(.*?)```", README.read_text(), re.S)


def _run(block: str) -> dict:
    """Run a block as the module a reader would paste it into: a dataclass
    looks its module up in `sys.modules`."""
    module = types.ModuleType("readme_block")
    sys.modules[module.__name__] = module
    try:
        exec(compile(block, str(README), "exec"), module.__dict__)
    finally:
        del sys.modules[module.__name__]
    return module.__dict__


def test_the_hello_world_runs_as_pasted(capsys):
    [hello] = [b for b in _blocks() if "class Orders(" in b]
    namespace = _run(hello)
    printed = capsys.readouterr().out
    assert "paying 1 30.0" in printed
    assert "{'pay': Status.DONE}" in printed, "the progress it shows is what it prints"
    assert namespace["items"].journal.progress(1) == {"pay": "done", "ship": "running"}


def test_the_adapter_example_runs_as_pasted():
    [example] = [b for b in _blocks() if "class DocsAdapter(" in b]
    namespace = _run(example)
    lease = namespace["lease"]
    assert sorted(doc.id for doc in lease) == ["s1", "s2"], "objects in, objects out"
    assert all(isinstance(doc, namespace["Doc"]) for doc in lease)
    assert namespace["items"].journal.progress("s1") == {"fetch": "done"}


def test_every_python_block_of_the_readme_is_run_by_a_test():
    assert len(_blocks()) == 2, "a new block needs a test of its own here"
