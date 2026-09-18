"""The README's example runs, as written.

It is the first code anyone copies, and nothing ran it: an adapter there
kept the old `inflate(self, ids)` of a renamed method, and crashed on the
very objects the same example hands in as candidates. This executes the
README's own text, so the page and the library cannot drift apart again.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

README = Path(__file__).resolve().parent.parent / "README.md"


def _example() -> str:
    """The README's python block that builds an `Items` layer."""
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), re.S)
    [block] = [b for b in blocks if "Items(" in b and "class " in b]
    return block


def test_the_readme_example_runs_as_written():
    @dataclass(frozen=True)
    class Doc:
        id: str
        scanned: bool = True

    library = [Doc("s1"), Doc("s2", scanned=False)]
    # `my_loader` is the one name the README leaves to the reader.
    namespace = {"my_loader": lambda: list(library)}
    exec(compile(_example(), str(README), "exec"), namespace)

    lease = namespace["lease"]
    assert sorted(doc.id for doc in lease) == ["s1", "s2"], "objects in, objects out"
    assert all(isinstance(doc, Doc) for doc in lease)
    journal = namespace["items"].journal
    assert journal.progress("s1") == {"fetch": "done"}, "the conclusion took"
