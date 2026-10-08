"""legend-lite in Python: the Legend compiler as a native library (//native:compiler).

    from legend_lite import parse, plan
    tree = parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
    plan(model_text, tree, "trades::RT").sql

Dataframes as Legend tables (``Frames``, in ``legend_lite.frames``) need duckdb and pyarrow; the
compiler alone needs only Python's standard library, so ``Frames`` loads them when first used.
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

# Frames is not listed: `from legend_lite import *` would import duckdb through it
__all__ = [
    'Column', 'LegendError', 'Plan', 'catalog_columns_sql', 'database_from_catalog', 'model_elements', 'parse',
    'plan', 'plan_text', 'print_tree', 'relation_type', 'session_setup', 'table_model',
]


def __getattr__(name: str):
    # Frames needs duckdb and pyarrow; the compiler does not, so they load only when Frames is asked for
    if name == 'Frames':
        from .frames import Frames
        return Frames
    raise AttributeError(f"module 'legend_lite' has no attribute {name!r}")
