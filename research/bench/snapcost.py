#!/usr/bin/env python3
"""What does a user-initiated "snap" actually cost?

A snap freezes the filtered source rows at drillable grain, so the user
can re-pivot and drill locally. Parquet is the container (it sidesteps
the apache-arrow JS version hazard entirely).

Measures, per row count:
  - time to materialize the snap to Parquet
  - the resulting file size (this is what has to live in a browser tab)
  - time to run a windowed pivot *from the Parquet file*
  - the same pivot against a native table, as the floor

Single-threaded; machine is contended, so figures are upper bounds and
we report the minimum of repeated runs.
"""
import os
import tempfile
import time

import duckdb

SIZES = [1_000_000, 5_000_000, 20_000_000]
REPS = 3
WINDOW = 40
PIVOT_CARD = 500          # distinct pivot keys in the source
ROWGRPS = 500             # region(5) x country(100)


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024.0


win_keys = ",".join(str(i) for i in range(PIVOT_CARD // 2,
                                          PIVOT_CARD // 2 + WINDOW))

print(f"threads=1  reps={REPS} (min)  pivot window={WINDOW} cols\n")
hdr = (f"{'rows':>12} {'snap ms':>9} {'parquet':>9} {'B/row':>7}"
       f" {'pivot@pq ms':>12} {'pivot@tbl ms':>13}")
print(hdr)
print("-" * len(hdr))

tmp = tempfile.mkdtemp(prefix="snapcost-")
for n in SIZES:
    con = duckdb.connect()
    con.execute("SET threads=1")
    # A realistic drillable source row: a few dimensions, a few measures,
    # a date, and a couple of text columns.
    con.execute(f"""
        CREATE TABLE src AS
        SELECT (i%5)   AS region,
               (i%100) AS country,
               (i%{PIVOT_CARD}) AS pk,
               DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
               'BOOK-'  || (i%250)  AS book,
               'TRADER-'|| (i%1200) AS trader,
               (i*7919)%100000/100.0   AS notional,
               (i*104729)%100000/100.0 AS pnl
        FROM range({n}) t(i)
    """)

    pq = os.path.join(tmp, f"snap_{n}.parquet")

    def snap():
        con.execute(
            f"COPY src TO '{pq}' (FORMAT parquet, COMPRESSION zstd)")

    t_snap = best(snap, reps=2)
    size = os.path.getsize(pq)

    pivot_pq = (
        f"CREATE OR REPLACE TABLE r AS PIVOT (SELECT * FROM '{pq}') "
        f"ON pk IN ({win_keys}) USING sum(notional) "
        f"GROUP BY region, country")
    pivot_tbl = (
        f"CREATE OR REPLACE TABLE r AS PIVOT src "
        f"ON pk IN ({win_keys}) USING sum(notional) "
        f"GROUP BY region, country")

    t_pq = best(lambda: con.execute(pivot_pq))
    t_tbl = best(lambda: con.execute(pivot_tbl))

    print(f"{n:>12,} {t_snap:9.0f} {human(size):>9} {size / n:7.1f}"
          f" {t_pq:12.1f} {t_tbl:13.1f}")
    con.close()
    os.remove(pq)

print(f"\nB/row is the number that sets the practical snap ceiling: a tab"
      f"\nbudget divided by bytes-per-row gives the max snappable rows.")
