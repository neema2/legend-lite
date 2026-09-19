#!/usr/bin/env python3
"""Can we get narrow cost WITHOUT the pre-filter trap?

Pinning an IN list makes legend-lite pre-filter the source, which
changes row membership. A PROJECTION over a full-width pivot cannot
change row membership. The question is whether DuckDB's projection
pushdown prunes the unused FILTERed aggregates, giving narrow compute
for free.

Three arms at width 5000:
  A  full      : materialize every pivot column
  B  projected : full-width pivot, SELECT only 40 columns from it
  C  prefiltered: the pinned-IN static pivot (narrow, but drops rows)

If B ~ C, projection gives us narrow cost with correct rows.
If B ~ A, no pruning happens and the pivot computes everything anyway.
"""
import time

import duckdb

ROWS = 2_000_000
WIDTH = 5000
WINDOW = 40
REPS = 3


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


con = duckdb.connect()
con.execute("SET threads=1")
con.execute(f"""
    CREATE TABLE wf AS
    SELECT ((i//{WIDTH})%500) AS grp, (i%{WIDTH}) AS pk,
           (i*7919)%100000/100.0 AS amount
    FROM range({ROWS}) t(i)
""")

allk = ",".join(str(i) for i in range(WIDTH))
lo = WIDTH // 2
vis = list(range(lo, lo + WINDOW))
vink = ",".join(str(i) for i in vis)
viscols = ", ".join(f'"{i}"' for i in vis)

A = (f"CREATE OR REPLACE TABLE r AS "
     f"PIVOT wf ON pk IN ({allk}) USING sum(amount) GROUP BY grp")
B = (f"CREATE OR REPLACE TABLE r AS SELECT grp, {viscols} FROM "
     f"(PIVOT wf ON pk IN ({allk}) USING sum(amount) GROUP BY grp)")
C = (f"CREATE OR REPLACE TABLE r AS "
     f"PIVOT (SELECT * FROM wf WHERE pk IN ({vink})) "
     f"ON pk IN ({vink}) USING sum(amount) GROUP BY grp")

# Also the native-dynamic + projection variant, which needs no IN list
# at all and so needs no discovery pass.
D = (f"CREATE OR REPLACE TABLE r AS SELECT grp, {viscols} FROM "
     f"(PIVOT wf ON pk USING sum(amount) GROUP BY grp)")

for name, q in (("A full (all cols)", A),
                ("B projected from full", B),
                ("C prefiltered (narrow)", C),
                ("D dynamic + projection", D)):
    t = best(lambda q=q: con.execute(q))
    n = con.execute("SELECT count(*) FROM r").fetchone()[0]
    print(f"{name:>24}  {t:8.1f}ms   rows={n}")

print("\nrows= is the tell: any arm that drops rows is unusable as the"
      "\nrow-axis authority, no matter how fast it is.")
