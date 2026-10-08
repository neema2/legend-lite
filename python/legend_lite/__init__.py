"""legend-lite in Python: the Legend compiler as a native library (//native:compiler).

    from legend_lite import parse, plan
    tree = parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
    plan(model_text, tree, "trades::RT").sql

Dataframes as Legend tables (``Frames``, in ``legend_lite.frames``) and the engine that serves them to DataCube
(``Engine``, in ``legend_lite.engine``) need duckdb and pyarrow; the compiler alone needs only Python's standard
library, so each loads them when first used.
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

# Frames and Engine are not listed: `from legend_lite import *` would import duckdb through them
__all__ = [
    'Column', 'LegendError', 'Plan', 'catalog_columns_sql', 'database_from_catalog', 'model_elements', 'parse',
    'plan', 'plan_text', 'print_tree', 'relation_type', 'session_setup', 'table_model',
]


def __getattr__(name: str):
    # Frames and Engine need duckdb and pyarrow; the compiler does not, so they load only when asked for
    if name == 'Frames':
        from .frames import Frames
        return Frames
    if name == 'Engine':
        from .engine import Engine
        return Engine
    raise AttributeError(f"module 'legend_lite' has no attribute {name!r}")
