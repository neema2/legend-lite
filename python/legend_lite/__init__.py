"""legend-lite in Python: the Legend compiler as a native library (//native:compiler).

    from legend_lite import parse, plan
    tree = parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
    plan(model_text, tree, "trades::RT").sql

One package, two things: the compiler, on Python's standard library alone (``pip install legend-lite``), and DataCube
with everything it needs (``pip install 'legend-lite[datacube]'``: duckdb, pyarrow and mini-racer's V8, which runs
DataCube's own code). DataCube on a dataframe -- ``show(df)``, in ``legend_lite.datacube`` -- the dataframes as Legend
tables under it (``Frames``, in ``legend_lite.frames``) and the engine that serves them to DataCube (``Engine``, in
``legend_lite.engine``) are DataCube's, each loaded when first used, and without the extra each says to install it. In a
notebook, the cube under the cell (``DataCube``, in ``legend_lite.notebook``) also needs anywidget:
``pip install 'legend-lite[notebook]'``, which brings DataCube's packages too. A whole DataCube page built in
Python -- its grids, charts, sheets and stacks -- is ``Page``, in ``legend_lite.page``, shown with ``show(page)``.
``home()`` says where legend-lite keeps this user's things (the pages DataCube saves), and ``home(path)`` points it
elsewhere.
"""

from .compiler import (
    Column,
    HeldModel,
    LegendError,
    Plan,
    catalog_columns_sql,
    database_from_catalog,
    model_elements,
    parse,
    plan,
    plan_text,
    print_tree,
    relation_type,
    session_setup,
    table_model,
)

# Frames, Engine, show, DataCube and Page are not listed: `from legend_lite import *` would import duckdb
# through them.
# Nor is home: it is the whole package's (//python:legend_lite, beside the engine that keeps its pages), not the
# compiler's bindings'
__all__ = [
    'Column', 'HeldModel', 'LegendError', 'Plan', 'catalog_columns_sql', 'database_from_catalog', 'model_elements', 'parse',
    'plan', 'plan_text', 'print_tree', 'relation_type', 'session_setup', 'table_model',
]


def __getattr__(name: str):
    # Frames, Engine and show are DataCube's (the datacube extra: _extra.py), DataCube needs anywidget too; the compiler
    # needs none of them, so each loads only when asked for
    if name == 'Frames':
        from .frames import Frames
        return Frames
    if name == 'Engine':
        from .engine import Engine
        return Engine
    if name == 'show':
        from .datacube import show
        return show
    if name in ('query', 'studio'):
        # Legend Query and Studio served by this Python (E)
        from . import datacube
        return getattr(datacube, name)
    if name == 'DataCube':
        from .notebook import DataCube
        return DataCube
    if name == 'Page':
        from .page import Page
        return Page
    if name == 'home':
        from ._home import home
        return home
    raise AttributeError(f"module 'legend_lite' has no attribute {name!r}")
