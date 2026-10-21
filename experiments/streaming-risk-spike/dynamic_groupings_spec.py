"""The 16 groupings both dynamic_groupings.py (Feldera) and dynamic_deephaven.py ask for."""

DIMS = ["desk", "trader", "book", "sector", "ccy"]

# DataCube-shaped asks: levels, regroupings, an expanded group (a filter), finer grains
SUBS = [
    ("desk",), ("desk", "trader"), ("sector",), ("ccy",),
    ("desk", "sector"), ("book",), ("desk", "trader", "book"), ("sector", "ccy"),
    ("desk", "ccy"), ("trader",), ("trader", "sector"), ("desk", "trader", "book", "sector"),
    ("book", "ccy"), (), ("desk", "trader", "book", "sector", "ccy"), ("sector", "book"),
]
FILTERS = {4: {"f_desk": "Macro"}, 10: {"f_sector": "Tech"}, 11: {"f_desk": "Equities", "f_sector": "Energy"}}
