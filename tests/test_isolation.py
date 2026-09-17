"""The package imports what it says it imports — read from the source.

    the core                   the standard library and itself
    quazardous.grampy.drivers.postgres    + sqlalchemy
    quazardous.grampy.testing             + pytest, hypothesis
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "quazardous" / "grampy"

EXTRA = {
    "drivers/postgres.py": {"sqlalchemy"},
    "testing.py": {"pytest", "hypothesis"},
}


def top_level_imports(path: Path) -> set[str]:
    found: set[str] = set()
    for item in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(item, ast.Import):
            found.update(alias.name.split(".")[0] for alias in item.names)
        elif isinstance(item, ast.ImportFrom) and item.level == 0 and item.module:
            found.add(item.module.split(".")[0])
    return found


def test_every_module_imports_only_what_it_declares():
    files = sorted(PACKAGE.rglob("*.py"))
    assert len(files) >= 7, "the scan must actually see the package"
    allowed_everywhere = set(sys.stdlib_module_names) | {"quazardous"}
    for path in files:
        name = path.relative_to(PACKAGE).as_posix()
        extra = top_level_imports(path) - allowed_everywhere - EXTRA.get(name, set())
        assert not extra, f"{name} imports {sorted(extra)}"


def test_the_scan_sees_a_foreign_import(tmp_path):
    sample = tmp_path / "sample.py"
    sample.write_text("import os\nfrom somewhere.other import thing\n", encoding="utf-8")
    assert top_level_imports(sample) == {"os", "somewhere"}


def test_quazardous_stays_a_namespace():
    """No `__init__.py` in `quazardous/`: other distributions install their
    own package beside grampy under the same prefix (PEP 420)."""
    assert not (PACKAGE.parent / "__init__.py").exists()


def test_importing_the_core_loads_no_optional_module():
    """ONE PACKAGE, NOTHING FORCED: `import quazardous.grampy` brings the
    engine only. Drawings, drivers and the test contract load when asked
    for — and with them, their dependencies."""
    import subprocess

    probe = (
        "import sys, quazardous.grampy\n"
        "optional = ('quazardous.grampy.diagram', 'quazardous.grampy.drivers',\n"
        "            'quazardous.grampy.testing', 'sqlite3', 'sqlalchemy', 'pytest',\n"
        "            'hypothesis')\n"
        "print(sorted(m for m in sys.modules if m.startswith(optional)))\n"
    )
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True,
                         check=True).stdout.strip()
    assert out == "[]", f"importing the core loaded {out}"
