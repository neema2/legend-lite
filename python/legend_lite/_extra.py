"""One package, two things (the user, 2026-10-15): ``pip install legend-lite`` is the compiler alone, which needs only
Python's standard library; ``pip install 'legend-lite[datacube]'`` is DataCube, with everything it needs -- duckdb (the
frames' database), pyarrow (their rows), and mini-racer's V8, which runs DataCube's own code (its rules, written once,
are asked of it: never copied into Python). Every module of DataCube's asks ``require_datacube()`` before it imports
any of them, so a compiler-only install that reaches for DataCube is told what to install, once, in one sentence."""

from __future__ import annotations

import importlib.util

# the datacube extra's packages, by the module each is imported as (python/BUILD.bazel, the wheel's extra_requires)
DATACUBE_MODULES = ("duckdb", "pyarrow", "py_mini_racer")


def require_datacube() -> None:
    """Nothing when the datacube extra is installed; refused, naming it, when any of its packages is missing."""
    missing = [m for m in DATACUBE_MODULES if importlib.util.find_spec(m) is None]
    if missing:
        raise ImportError(
            f"DataCube needs legend-lite's datacube extra ({', '.join(missing)} not installed): "
            "pip install 'legend-lite[datacube]'"
        )
