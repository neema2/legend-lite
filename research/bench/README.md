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
