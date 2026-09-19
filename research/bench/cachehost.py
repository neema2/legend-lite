#!/usr/bin/env python3
"""Where should the snap live? Two measurable questions.

1. Does putting the snap on DISK instead of in memory cost anything?
   (decides whether a disk-backed cache lifts the size ceiling for free)
2. How much does losing MULTIPLE CORES cost?
   (a browser tab is effectively single-threaded for us: multi-core
   needs COOP/COEP and only single-thread bundles are registered, so
   this is the price of hosting the cache in a browser at all)

Four arms: {memory, file} x {1 thread, all threads}, over a 10M-row
snap running a realistic pivot.
"""
import os
import shutil
import tempfile
import time

import duckdb

ROWS = 10_000_000
REPS = 3
NCPU = os.cpu_count() or 8

tmp = tempfile.mkdtemp(prefix="cachehost-")

SRC = f"""
    SELECT ((i//500)%2000) AS book, (i%500) AS pk,
           DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
           'TRADER-' || (i%1200) AS trader,
           random()*1e6 AS notional,
           random()*1e6 - 5e5 AS pnl
    FROM range({ROWS}) t(i)
"""

keys = ",".join(str(i) for i in range(500))
PIVOT = (f"CREATE OR REPLACE TABLE r AS "
         f"PIVOT snap ON pk IN ({keys}) USING sum(notional) GROUP BY book")
SCAN = "SELECT count(*), sum(notional), avg(pnl) FROM snap"


def best(fn, reps=REPS):
    t = None
    for _ in range(reps):
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        t = dt if t is None else min(t, dt)
    return t * 1000.0


print(f"snap = {ROWS:,} rows x 6 cols   cpus={NCPU}   min of {REPS}\n")
hdr = (f"{'host':>10} {'threads':>8} {'build ms':>10} {'pivot ms':>10}"
       f" {'scan ms':>9} {'on-disk':>9}")
print(hdr)
print("-" * len(hdr))

results = {}
for host in ("memory", "file"):
    for threads in (1, NCPU):
        path = os.path.join(tmp, f"snap_{threads}.duckdb")
        if host == "file" and os.path.exists(path):
            os.remove(path)
        con = duckdb.connect(path if host == "file" else ":memory:")
        con.execute(f"SET threads={threads}")

        t_build = best(
            lambda: con.execute(f"CREATE OR REPLACE TABLE snap AS {SRC}"),
            reps=1)
        t_pivot = best(lambda: con.execute(PIVOT))
        t_scan = best(lambda: con.execute(SCAN).fetchall())

        con.close()
        size = (os.path.getsize(path) / 1024 / 1024
                if host == "file" and os.path.exists(path) else 0)
        sz = f"{size:8.0f}MB" if size else f"{'-':>9}"
        print(f"{host:>10} {threads:>8} {t_build:10.0f} {t_pivot:10.1f}"
              f" {t_scan:9.1f} {sz}")
        results[(host, threads)] = (t_pivot, t_scan)

print()
m1 = results[("memory", 1)][0]
mn = results[("memory", NCPU)][0]
f1 = results[("file", 1)][0]
print(f"cost of 1 thread vs {NCPU}:  {m1 / mn:.1f}x slower")
print(f"cost of disk vs memory (1 thread): {f1 / m1:.2f}x")
shutil.rmtree(tmp, ignore_errors=True)
