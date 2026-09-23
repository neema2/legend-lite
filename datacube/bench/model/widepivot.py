#!/usr/bin/env python3
"""Wide-pivot benchmark.

Two questions:
  1. Does DuckDB's pivot time scale with the number of pivot COLUMNS?
  2. Does windowing the column axis (fetching only the ~40 visible
     pivot columns) make the total width irrelevant?

Single-threaded on purpose; the machine is contended, so we take the
minimum of repeated runs as the closest estimate of uncontended cost.
"""
import time

import duckdb

ROWS = 2_000_000
WIDTHS = [10, 10, 50, 200, 1000, 5000, 20000]
REPS = 3
WINDOW = 40
ROWGRPS = 500  # region(5) x country(100)


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


print(f"rows={ROWS:,}  threads=1  reps={REPS} (reporting min of {REPS})\n")
hdr = (f"{'width':>6} {'full ms':>9} {'window ms':>10} {'cells full':>11}"
       f" {'cells win':>10} {'full/win':>9}")
print(hdr)
print("-" * len(hdr))

rows = []
for w in WIDTHS:
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"""
        CREATE TABLE wf AS
        SELECT (i%5) AS region, (i%100) AS country, (i%{w}) AS pk,
               (i*7919)%100000/100.0 AS amount
        FROM range({ROWS}) t(i)
    """)

    all_keys = ",".join(str(i) for i in range(w))
    start = w // 2 if w > WINDOW else 0
    end = min(start + WINDOW - 1, w - 1)
    win_keys = ",".join(str(i) for i in range(start, end + 1))
    nwin = end - start + 1

    def full():
        con.execute(
            "CREATE OR REPLACE TABLE full_res AS "
            f"PIVOT wf ON pk IN ({all_keys}) USING sum(amount) "
            "GROUP BY region, country")

    def win():
        con.execute(
            "CREATE OR REPLACE TABLE win_res AS "
            f"PIVOT wf ON pk IN ({win_keys}) USING sum(amount) "
            "GROUP BY region, country")

    try:
        t_full = best(full)
        f_s = f"{t_full:9.1f}"
    except Exception as e:                              # noqa: BLE001
        t_full, f_s = None, f"{'ERR':>9}"
        print(f"  width={w} full pivot failed: {str(e)[:120]}")

    t_win = best(win)
    ratio = f"{t_full / t_win:8.1f}x" if t_full else f"{'-':>9}"
    print(f"{w:>6} {f_s} {t_win:10.1f} {ROWGRPS * w:>11,}"
          f" {ROWGRPS * nwin:>10,} {ratio}")
    rows.append((w, t_full, t_win))
    con.close()

print("\nInterpretation: if 'window ms' stays flat across widths, the column"
      "\naxis is bounded by the viewport and total pivot width stops mattering.")
