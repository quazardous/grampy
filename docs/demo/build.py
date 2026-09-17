"""Pack what the demo page loads into Pyodide: grampy's sources and the scenario.

    python docs/demo/build.py              # writes docs/demo/dist/grampy-demo.zip
    python -m http.server 8765 -d docs/demo   # then open http://localhost:8765

The page runs the grampy of this checkout, not a published release.
"""
from __future__ import annotations

import zipfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]


def main() -> Path:
    out = HERE / "dist" / "grampy-demo.zip"
    out.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        package = ROOT / "src" / "quazardous"
        for path in sorted(package.rglob("*.py")):
            archive.write(path, path.relative_to(ROOT / "src").as_posix())
        archive.write(HERE / "scenario.py", "scenario.py")
    return out


if __name__ == "__main__":
    print(main())
