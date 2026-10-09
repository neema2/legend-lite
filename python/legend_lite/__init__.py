"""legend-lite in Python: the Legend compiler as a native library (//native:compiler).

    from legend_lite import parse, plan
    tree = parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
    plan(model_text, tree, "trades::RT").sql

DataCube on a dataframe -- ``show(df)``, in ``legend_lite.datacube`` -- the dataframes as Legend tables under it
(``Frames``, in ``legend_lite.frames``) and the engine that serves them to DataCube (``Engine``, in
``legend_lite.engine``) need duckdb and pyarrow; the compiler alone needs only Python's standard
library, so each loads them when first used. In a notebook, the cube under the cell (``DataCube``, in
``legend_lite.notebook``) also needs anywidget: ``pip install 'legend-lite[notebook]'``.
"""

from .compiler import (
    Column,
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

# Frames, Engine, show and DataCube are not listed: `from legend_lite import *` would import duckdb through them
__all__ = [
    'Column', 'LegendError', 'Plan', 'catalog_columns_sql', 'database_from_catalog', 'model_elements', 'parse',
    'plan', 'plan_text', 'print_tree', 'relation_type', 'session_setup', 'table_model',
]


def __getattr__(name: str):
    # Frames, Engine and show need duckdb and pyarrow, DataCube anywidget too; the compiler needs none, so each loads
    # only when asked for
    if name == 'Frames':
        from .frames import Frames
        return Frames
    if name == 'Engine':
        from .engine import Engine
        return Engine
    if name == 'show':
        from .datacube import show
        return show
    if name == 'DataCube':
        from .notebook import DataCube
        return DataCube
    raise AttributeError(f"module 'legend_lite' has no attribute {name!r}")
