# DataCube rewrite — measured evidence

Benchmarks run to settle design questions that the industry does not
publish numbers for. Machine was contended in every run (load ~4-6 on
10 cores), so all figures are **upper bounds**: single-threaded, minimum
of repeated runs.

## `bench.sql` — subtotals via `concatenate` of per-level `groupBy`

Question: can subtotals be computed by the engine, in one query, rather
than laid out client-side?

Answer: yes. `UNION ALL` of one `groupBy` per hierarchy level, ordered
with `NULLS FIRST` on each key, yields correct tree order directly.

    5,000,000 rows, threads=1  ->  97ms, 2,106 rows
                                   (1 grand total + 5 + 100 + 2,000)

This matters because it makes the single most-complained-about pivot
defect — wrong or unsortable subtotals — structurally impossible: a
subtotal is the same measure expression with a grouping column dropped,
not a second aggregation pass that can disagree with the first.

`concatenate` already exists in legend-lite (`builtin/Pure.java`) and
lowers to `SqlUnion`, and it exists in legend-engine, so this path is
available on both backends with no new IR.

## `widepivot.py` — does pivot width matter if the column axis is windowed?

Question: a pivot's hard axis is *width* (dynamic pivoted columns), and
nobody publishes benchmarks for it. Does total width drive cost, or does
fetching only the visible columns make width irrelevant?

Answer: windowing the column axis makes width irrelevant.

    rows=2,000,000  threads=1  min of 3

     width   full ms  window ms   cells full  cells win  full/win
    ------------------------------------------------------------
        50      19.8       20.8       25,000     12,500      1.0x
       200      19.9       16.6      100,000     20,000      1.2x
      1000      36.2       18.3      500,000     20,000      2.0x
      5000     119.9       18.2    2,500,000     20,000      6.6x
     20000     850.4       20.8   10,000,000     20,000     40.9x

`full` materializes every pivot column; `window` materializes only the
~40 visible ones via `PIVOT ... ON pk IN (<visible keys>)`.

Windowed cost is flat (16.6 - 20.8ms) while unwindowed cost grows 43x
over the same range. At 20,000 columns the unwindowed query takes 850ms,
already past the latency threshold Liu & Heer measured as harmful to
exploration; the windowed query fits inside a frame.

Consequence for the design: **window both axes, not just rows.** Then the
realistic worst-case column count is a test fixture, not an architectural
constraint.

Caveat, stated rather than hidden: the `width=10` case measures ~72ms in
repeated runs, slower than `width=50`. That is an artifact of correlated
synthetic keys — at width 10, `region = i%5` is a function of
`pk = i%10`, degenerating the grouping. It does not affect the trend and
nothing here depends on it.

## Reproducing

    duckdb -c ".read bench.sql"      # subtotal union
    python3 widepivot.py             # pivot width sweep

Both need only the `duckdb` CLI / Python module (tested on 1.4.4). Never
read these numbers off a loaded machine; re-run on an idle one before
quoting them as anything but upper bounds.

## `snapcost.py` — what a user-initiated snap costs

Question: if a user explicitly "snaps" a cache and explores it locally,
what does that cost to create, how big is the payload that has to live
in a browser tab, and how fast is it to query afterwards?

A snap freezes the *filtered source rows at drillable grain* — not the
aggregated result — because re-pivoting and drill-through both need the
underlying rows. Parquet is the container: it sidesteps the
apache-arrow JS version hazard (duckdb-wasm wants ^17, current is 21.2.0,
and a mismatch fails silently leaving the table uncreated).

    5M rows, 8 columns, zstd, threads=1

            rows   snap ms   parquet   B/row  pivot@pq ms  pivot@tbl ms
       1,000,000        90     2.5MB     2.6         14.1           7.1
       5,000,000       422    12.3MB     2.6         68.3          33.8
      20,000,000      1581    49.0MB     2.6        270.3         132.4

Two findings:

1. **Snap cost is linear at ~79ms per million rows** single-threaded, so
   a 10M-row snap is well under a second. That is a legitimate
   foreground "snapping..." moment, not a background job.

2. **Querying the Parquet file is consistently 2x slower than querying a
   native table** (14.1/7.1, 68.3/33.8, 270.3/132.4 — the decompression
   cost). So Parquet is the *transport* format: on arrival, load it into
   a DuckDB table and query that. Keeping the snap as a file and
   querying it in place silently halves throughput.

At 20M rows a windowed pivot from a native table is 132ms
single-threaded, inside the 300ms p95 budget, and a browser gets more
than one thread.

## `snapentropy.py` — the snap ceiling, bracketed honestly

The 2.6 B/row above is **not a usable planning number**. It is an
artifact of modulo-patterned synthetic columns, which Parquet's
dictionary and RLE encodings compress unrealistically well. Real data
sits higher, so the range was measured with the same column count at
three entropy levels.

    5,000,000 rows, 8 columns, zstd parquet, threads=1

                       shape    parquet    B/row   rows in 500MB
             regular (floor)      12.3MB      2.6     203,277,499
                   realistic      73.9MB     15.5      33,819,197
      high entropy (ceiling)     270.1MB     56.6       9,256,337

"realistic" keeps dimensions low-cardinality (that is what makes them
dimensions) while ids are unique and measures genuinely continuous.
"high entropy" replaces the text columns with UUIDs — the pessimistic
case for a financial dataset carrying trade ids and free text.

**Planning number: a ~500MB snap budget holds 10M rows across every
data shape measured, and 30M+ for realistic shapes.** Design the ceiling
at 10M rows with a pre-flight size estimate shown to the user, rather
than a row count that happens to work for compressible data.

## `dynvsstatic.py` — static is NOT much faster than dynamic

Correction to a claim made from `widepivot.py`. That benchmark compared
a **static full-width** pivot against a **static windowed** pivot — both
arms were `PIVOT ... ON pk IN (…)`. Native dynamic PIVOT was never
measured, so reading its 41x as a static-vs-dynamic result was wrong.

Measured properly, at fixed width, three arms:

    rows=2,000,000  threads=1  min of 3

     width   dynamic  static-full  static-win  discovery  width cost
       200      38.8         16.9        15.5      21.9ms        1.1x
      1000      55.2         31.0        15.8      24.2ms        2.0x
      5000     147.6        119.1        17.5      28.5ms        6.8x

**Dynamic vs static is worth a near-constant ~22-28ms** — the extra scan
to discover the distinct values. It does not scale with width.

**The real driver is output size**, which is orthogonal to
static-vs-dynamic. Dynamic is slower only because it *forces* full
width: "dynamic" means emit every discovered value, so it always sits at
the expensive end of the width curve. Static does not make the query
fast; it makes narrowness *expressible*.

## `cellcost.py` — the cost model is output cells, so window both axes

`EXPLAIN` on a static pivot shows the mechanism — conditional
aggregation, one FILTERed aggregate per pivot value, per group:

    PERFECT_HASH_GROUP_BY
      Groups: #0
      Aggregates: sum(#1) FILTER (WHERE #4)
                  sum(#3) FILTER (WHERE #5)
                  sum(#5) FILTER (WHERE #6)
                  sum(#7) FILTER (WHERE #7)

with one boolean predicate column per pivot value in the projection
below it. So the work is proportional to output cells = groups x
columns, each needing an aggregate state allocated, updated and
materialized.

Testing that by sweeping the same cell count two ways:

    rows=2,000,000  threads=1  min of 3

     groups   cols       cells       ms   ns/cell
         25    200       5,000     14.9    2982.6
        200     25       5,000     13.1    2626.9
        100   1000     100,000     32.2     322.1
       1000    100     100,000     21.7     217.0
        500   2000   1,000,000    125.0     125.0
       2000    500   1,000,000    101.2     101.2

Equal-product pairs land within 1.14-1.48x of each other, so **the cost
model is roughly `constant scan + (groups x columns) x ~100ns`**, with
columns modestly more expensive than rows. Falling ns/cell at small cell
counts is the fixed 2M-row scan dominating.

Methodological note: the first run of this used `grp = i%g` and
`pk = i%c`, which are correlated whenever `g` and `c` share factors — at
500x2000, `pk` fully determines `grp`, so most "cells" were structurally
empty and the pairs looked asymmetric (up to 2.0x). Independent keys
(`grp = (i//c)%g`) fixed it. The numbers above are the corrected run.

**Consequence: both axes need windowing, and the savings multiply.**
Neither axis is free, and neither dominates.

## `windowinvariant.py` — the witness for horizontal scrolling

Scrolling the column axis re-runs the cell query with a different pinned
IN list. That is only sound if a cell's value does not depend on which
window fetched it. Checked against a full-width pivot over deliberately
ragged data (groups whose keys sit only low, only high, or spanning):

    OK          W1 [1-5]  cells= 15  groups=3
    OK         W2 [6-10]  cells= 10  groups=2
    OK        W3 [11-15]  cells=  5  groups=1
    OK        W4 [16-20]  cells= 15  groups=3
    OK    overlap [4-12]  cells= 18  groups=2

    CELL VALUES window-invariant: YES

**Cell values are window-invariant.** `sum(x) FILTER (WHERE pk = k)`
reads only rows with `pk = k`, so pre-filtering the source to a window
containing `k` cannot change it. Every cell matched the full-width
truth, including under an overlapping window.

**Row membership is not**, and the magnitude is the point:

               window   groups
           full width        4
             W1 [1-5]        3
            W2 [6-10]        2
           W3 [11-15]        1
           W4 [16-20]        3

Scrolling from W1 to W3 would take the grid from three rows to one.

A first version of this test used DuckDB's raw `PIVOT t ON pk IN (…)`,
which *keeps* groups with no matching rows and therefore reported row
membership as invariant — testing DuckDB's semantics rather than
legend-lite's. `Pivots.lower` pre-filters the source when values are
pinned (for legend-engine parity), so the test now runs both forms and
the `prefilter=True` arm is the one that matches what legend-lite emits.

This belongs in the regression suite: it is the invariant that makes
column windowing sound, and the counter-case that makes the separate
row-axis query mandatory rather than merely tidy.

## `projectprune.py` — there is no safe middle path

Pinning an IN list buys narrow compute but makes legend-lite pre-filter
the source, which changes row membership. A PROJECTION over a full-width
pivot cannot change row membership, so if DuckDB's projection pushdown
pruned the unused FILTERed aggregates we would get narrow compute for
free and none of the complexity. It does not.

    rows=2,000,000  width=5000  window=40  threads=1  min of 3

                   A full (all cols)     275.0ms   rows=400
               B projected from full     257.6ms   rows=400
              C prefiltered (narrow)       3.6ms   rows=400
              D dynamic + projection     276.4ms   rows=400

B costs what A costs. The aggregates are computed whether or not the
columns are selected, so **the cost is computing groups x columns
aggregate states, not materializing them** — which corrects the cost
model in `cellcost.py`: it is compute-bound, not output-bound.

So the choice is binary: narrow compute with the pre-filter's
row-membership hazard, or full-width compute with one simple consistent
query. There is no third option at the SQL level.

Note this fixture cannot show the row-drop hazard — with
`grp = (i//WIDTH)%500` every group contains every pivot key, so arm C
keeps all 400 rows. `windowinvariant.py` uses deliberately ragged data
for that. The 76x gap between A and C here (vs 6.6x in `widepivot.py`
at the same width) shows how strongly the windowing payoff depends on
data shape: it ranges from ~1.2x on narrow pivots to ~76x on wide dense
ones.

## Where this lands: start simple

Collecting the width numbers in one place, because they decide how much
complexity is justified:

    pivot columns    full-width    windowed    payoff
              200        19.9ms      16.6ms      1.2x
             1000        36.2ms      18.3ms      2.0x
             5000       119.9ms      18.2ms      6.6x
            20000       850.4ms      20.8ms     40.9x

**Below roughly 1,000 columns, full-width is already inside the frame
budget and column windowing buys nothing measurable.** Every piece of
brittleness in the windowed design — the three-query split, the
row-membership invariant, column overscan, the window cache, uniform
column widths — exists solely to serve column windowing, and therefore
solely to serve pivots wider than about a thousand columns.

A month-by-three-measures pivot is 36 columns. Quarter by category by
two measures is about a hundred. Reaching five thousand takes pivoting
on something high-cardinality, like instrument or trader id.

So v1 should be **one query, full width, row-axis windowing only**.
Column windowing becomes a documented escape hatch with a measured
trigger, built as the three-query split if and when a real cube needs
it. Row windowing is kept because it always pays and carries no
correctness hazard: bounding the row window bounds the payload, since
payload = row window x columns.

What is worth keeping regardless, none of it brittle: keyset row
pagination, epoch-stamped discarding (forced anyway — `query()` cannot
be cancelled), the formatter cache (27ms -> 1ms, unrelated to
windowing), identity-keyed view state, subtotals via `concatenate`, and
snap mode.
