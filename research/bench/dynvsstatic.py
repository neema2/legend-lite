#!/usr/bin/env python3
"""Separate the two effects that were previously conflated.

widepivot.py compared a STATIC full-width pivot against a STATIC
windowed pivot, so its 41x was a column-count effect only. Native
dynamic PIVOT was never measured.

Three arms at a fixed width isolate the variables:

  dynamic      PIVOT t ON pk            -- engine discovers the values
  static-full  PIVOT t ON pk IN (all)   -- same width, values supplied
  static-win   PIVOT t ON pk IN (40)    -- narrow

dynamic vs static-full  = the cost of value discovery
static-full vs static-win = the cost of output width
"""
import time

import duckdb

ROWS = 2_000_000
WIDTHS = [200, 1000, 5000]
REPS = 3
WINDOW = 40


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


print(f"rows={ROWS:,}  threads=1  reps={REPS} (min)\n")
hdr = (f"{'width':>6} {'dynamic':>9} {'static-full':>12} {'static-win':>11}"
       f" {'discovery':>10} {'width cost':>11}")
print(hdr)
print("-" * len(hdr))

for w in WIDTHS:
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"""
        CREATE TABLE wf AS
        SELECT (i%5) AS region, (i%100) AS country, (i%{w}) AS pk,
               (i*7919)%100000/100.0 AS amount
        FROM range({ROWS}) t(i)
    """)
    allk = ",".join(str(i) for i in range(w))
    wink = ",".join(str(i) for i in range(w // 2, w // 2 + WINDOW))

    def q(on):
        return (f"CREATE OR REPLACE TABLE r AS PIVOT wf ON {on} "
                f"USING sum(amount) GROUP BY region, country")

    t_dyn = best(lambda: con.execute(q("pk")))
    t_full = best(lambda: con.execute(q(f"pk IN ({allk})")))
    t_win = best(lambda: con.execute(q(f"pk IN ({wink})")))

    print(f"{w:>6} {t_dyn:9.1f} {t_full:12.1f} {t_win:11.1f}"
          f" {t_dyn - t_full:9.1f}ms {t_full / t_win:10.1f}x")
    con.close()

print("\n'discovery' = dynamic minus static-full (the extra scan).")
print("'width cost' = static-full over static-win (the real 41x driver).")
