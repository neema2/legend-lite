# Step 1: DuckDB in the browser, measured

Every performance number in `../../research/bench/` was taken against
**native** DuckDB on Apple silicon. This directory answers the question
that was left open there: how much slower is DuckDB-WASM, and does it
move the snap ceiling?

It does. And it replaces the ceiling with a better one.

Toolchain: `@duckdb/duckdb-wasm` 1.33.1-dev57.0 (engine v1.5.4), run
through the blocking Node API, single-threaded, minimum of 3 runs.

## `wasm-penalty.mjs` — the penalty is ~2.1-2.4x

Mirrors `research/bench/cachehost.py` exactly (10M rows x 6 columns,
then a 500-column pivot over 2,000 groups) so the arms are comparable.

             operation   wasm ms  native ms  penalty
            build snap    1638.3      692.0    2.37x
      pivot (500 cols)     360.4      168.4    2.14x
             full scan      15.8       11.2    1.41x

Squarely inside the 2-4x that was predicted. The consequence is the
point: **that pivot takes 360ms in the browser, over the 300ms p95
target** for expand/sort/filter, where it was a comfortable 168ms
native.

A first run of this compared 2M-row WASM against the 10M-row native
reference and reported penalties below 1.0x -- i.e. WASM beating native,
which is nonsense. The sizes have to match; the numbers above do.

## `snap-ceiling.mjs` — but row count is barely the issue

        snap rows  build ms  pivot ms  sort ms   verdict
        1,000,000        99       206       29        OK
        2,000,000       149       232       28        OK
        5,000,000       375       267       27        OK
       10,000,000       665       334       25      OVER

Pivot time rises only **1.6x while rows rise 10x**. The reason is that
this sweep holds row groups x pivot columns constant at 1M cells, and
that product -- not the snap's size -- is what the pivot actually costs.
Re-sorting an existing result is ~27ms throughout and never a concern.

## `cell-budget.mjs` — the real ceiling is output cells

Rows fixed at 5M, cells swept, each product reached two ways:

     groups   cols      cells  pivot ms  ns/cell  verdict
        500     50     25,000        49     1966       OK
         50    500     25,000        57     2267       OK
       1000    200    200,000        81      405       OK
        200   1000    200,000        94      468       OK
       2000    500  1,000,000       202      202       OK
        500   2000  1,000,000       229      229       OK
       4000   1000  4,000,000       650      162     OVER

Equal-cell pairs land within 1.16x of each other, so the native cost
model from `research/bench/cellcost.py` holds in WASM as well, at
roughly **200ns per cell against native's ~100ns** -- consistent with
the 2.14x measured above. Falling ns/cell at low counts is the fixed 5M
row scan dominating.

## What this changes

The committed "10M row snap ceiling" came from payload size (a ~500MB
tab budget divided by bytes per row). That is still a valid **memory**
constraint. But it is not the **interaction** constraint, which is:

> **Keep a pivot under roughly 1,000,000 output cells**
> (row groups x pivot columns) to stay inside the 300ms budget.
> 4M cells is 650ms and feels broken.

The two constraints are independent and both apply. A 10M-row snap is
perfectly fine so long as the pivot run against it stays narrow enough.
This is also the better constraint to enforce, because both numbers are
known *before* the query runs: the column list comes from discovery, and
the group count is a cheap `COUNT(DISTINCT)`.

It also retires a worry. The full-width-pivot design chosen in
`research/bench/README.md` ("start simple") is safe precisely because
realistic pivots are narrow: 2,000 groups x 50 columns is 100k cells and
runs in well under 100ms. The cell cap is what tells us when the column
windowing escape hatch is actually needed -- and it triggers on cells,
not on column count alone.

## Notes for later

- The `duckdb-coi.wasm` bundle **is** shipped in the package, so
  multi-threading is available if the app serves COOP/COEP headers.
  Worth revisiting: native multi-threading was only worth 1.5x, so this
  is a modest win, not a rescue.
- `apache-arrow` resolved to exactly **17.0.0** here, pinned by
  duckdb-wasm's own `^17.0.0` dependency. Current published is 21.2.0,
  four majors ahead, and a mismatch makes Arrow inserts fail *silently*.
  Never add `apache-arrow` as a direct dependency without matching it;
  better still, ingest via Parquet and `registerFileBuffer`, which does
  not touch the JS Arrow package at all.
- OPFS persistence is still unmeasured -- it needs a real browser, not
  the Node API used here.

## Running

    npm install
    node bench/wasm-penalty.mjs [rows]
    node bench/snap-ceiling.mjs
    node bench/cell-budget.mjs

Machine load was 1.6-2.1 throughout, so these are upper bounds.
