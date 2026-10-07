"""legend-lite in Python: the Legend compiler as a native library (//native:compiler).

    from legend_lite import parse, plan
    tree = parse("|#>{trades::DB.TRADES}#->groupBy(~[desk], ~[q: x|$x.qty : y|$y->sum()])")
    plan(model_text, tree, "trades::RT").sql
"""

from .compiler import (
    Column,
    LegendError,
    Plan,
    database_from_catalog,
    model_elements,
    parse,
    plan,
    plan_text,
    print_tree,
    relation_type,
)

__all__ = [
    'Column', 'LegendError', 'Plan', 'database_from_catalog', 'model_elements', 'parse', 'plan',
    'plan_text', 'print_tree', 'relation_type',
]
