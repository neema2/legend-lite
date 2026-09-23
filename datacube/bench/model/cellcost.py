#!/usr/bin/env python3
"""Is the driver output CELLS (groups x columns) or columns alone?

The EXPLAIN shows a pivot is conditional aggregation: one FILTERed
aggregate state per pivot value, per group. If that is the cost model,
then holding columns fixed and raising the group count should cost the
same as holding groups fixed and raising columns -- what matters is the
product.

This decides whether the row axis needs windowing as badly as the
column axis does.
"""
import time

import duckdb

ROWS = 2_000_000
REPS = 3

# (groups, columns) pairs chosen so the product sweeps the same range
# two different ways.
CASES = [
    (25,    200),   # 5,000 cells
    (200,    25),   # 5,000 cells  -- same product, swapped
    (100,   1000),  # 100,000 cells
    (1000,  100),   # 100,000 cells -- same product, swapped
    (500,  2000),   # 1,000,000 cells
    (2000,  500),   # 1,000,000 cells -- same product, swapped
]


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


print(f"rows={ROWS:,}  threads=1  reps={REPS} (min)\n")
hdr = (f"{'groups':>7} {'cols':>6} {'cells':>11} {'ms':>8}"
       f" {'ns/cell':>9}")
print(hdr)
print("-" * len(hdr))

for g, c in CASES:
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"""
        CREATE TABLE wf AS
        SELECT ((i//{c})%{g}) AS grp, (i%{c}) AS pk,
               (i*7919)%100000/100.0 AS amount
        FROM range({ROWS}) t(i)
    """)
    keys = ",".join(str(i) for i in range(c))
    q = (f"CREATE OR REPLACE TABLE r AS PIVOT wf ON pk IN ({keys}) "
         f"USING sum(amount) GROUP BY grp")
    t = best(lambda: con.execute(q))
    cells = g * c
    print(f"{g:>7} {c:>6} {cells:>11,} {t:8.1f} {t * 1e6 / cells:9.1f}")
    con.close()

print("\nIf the two rows of each pair land close, the cost model is"
      "\ncells = groups x columns, and BOTH axes need windowing.")
