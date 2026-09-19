#!/usr/bin/env python3
"""Bracket the bytes-per-row of a snap between best and worst case.

The first snapcost run used modulo-patterned synthetic columns, which
Parquet's dictionary + RLE encodings compress unrealistically well
(2.6 B/row). Real data sits between that floor and a high-entropy
ceiling. This measures both ends with the same column count so the
comparison is apples to apples, plus a "realistic" middle where
dimensions are low-cardinality but measures and ids are not.
"""
import os
import tempfile

import duckdb

N = 5_000_000
tmp = tempfile.mkdtemp(prefix="snapentropy-")

CASES = {
    # Highly regular: every column a small modulo cycle.
    "regular (floor)": """
        SELECT (i%5) AS region, (i%100) AS country, (i%500) AS pk,
               DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
               'BOOK-'   || (i%250)  AS book,
               'TRADER-' || (i%1200) AS trader,
               (i*7919)%100000/100.0   AS notional,
               (i*104729)%100000/100.0 AS pnl
        FROM range({n}) t(i)
    """,
    # Realistic: dimensions repeat (that is what makes them dimensions),
    # but ids are unique and measures are genuinely continuous.
    "realistic": """
        SELECT (i%5) AS region, (i%100) AS country, (i%500) AS pk,
               DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
               'BOOK-'   || (i%250)  AS book,
               'TRADER-' || (i%1200) AS trader,
               random()*1e6 AS notional,
               random()*1e6 - 5e5 AS pnl
        FROM range({n}) t(i)
    """,
    # Worst case: high-cardinality ids and free text alongside measures.
    "high entropy (ceiling)": """
        SELECT (i%5) AS region, (i%100) AS country, (i%500) AS pk,
               DATE '2020-01-01' + INTERVAL (i%2000) DAY AS trade_date,
               uuid()::VARCHAR AS book,
               uuid()::VARCHAR AS trader,
               random()*1e6 AS notional,
               random()*1e6 - 5e5 AS pnl
        FROM range({n}) t(i)
    """,
}

print(f"{N:,} rows, 8 columns, zstd parquet, threads=1\n")
hdr = f"{'shape':>24} {'parquet':>10} {'B/row':>8} {'rows in 500MB':>15}"
print(hdr)
print("-" * len(hdr))

for name, sql in CASES.items():
    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"CREATE TABLE src AS {sql.format(n=N)}")
    pq = os.path.join(tmp, "s.parquet")
    con.execute(f"COPY src TO '{pq}' (FORMAT parquet, COMPRESSION zstd)")
    size = os.path.getsize(pq)
    bpr = size / N
    budget = int(500 * 1024 * 1024 / bpr)
    print(f"{name:>24} {size / 1024 / 1024:9.1f}MB {bpr:8.1f}"
          f" {budget:>15,}")
    os.remove(pq)
    con.close()

print("\n'rows in 500MB' is an indicative browser-tab budget for the"
      "\nsnap payload alone, before DuckDB's own working memory.")
