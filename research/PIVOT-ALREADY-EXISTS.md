# The pivot pipeline is already built in legend-lite

This is the load-bearing tractability fact for the DataCube rewrite, so
it is recorded separately from the benchmarks.

DataCube compiles a snapshot to the pipeline
`extend → filter → select → sort → pivot → cast → groupBy → extend → sort → limit`.
`groupBy` and `concatenate` were already confirmed. The open question was
`pivot`/`cast` — specifically the **two-phase dynamic pivot** that
DataCube's cast-column discovery requires, which is the hardest part of
a pivot engine.

It exists, implemented, not merely declared.

## What is there

- **A first-class IR node**: `SqlSource.Pivot(source, on, in, usings, alias, outputs)`.
- **A dedicated lowering**: `lowering/Pivots.java`, extracted from the
  Lowerer at the 3,500-line shape boundary. Static pivot values pin the
  output columns via `PIVOT ... IN (v…)` — the same form the width
  benchmark used.
- **Two-phase staticization**: `exec/DynamicPivot.staticize(plan, dialect, connection)`.
  A `Pivot` whose `IN` list is empty has data-dependent output columns,
  so the key's `DISTINCT` values are discovered by a first query **on the
  same connection** and pinned as literals; the now-static pivot takes
  the ordinary emulation strategy.
- **A dialect capability flag**: `SqlDialect.needsStaticPivot`. Dialects
  with native dynamic PIVOT (DuckDB) pass through untouched; the
  engine's own H2 route gets the emulation.
- **Documented semantics**: values ascending, matching the reference
  target's dynamic-pivot column order; NULL keys skipped, because "a
  NULL never names an output column."
- **Correct layering**: the pre-pass runs at the *execution* seam, where
  a connection exists, never inside rendering. This is why
  `Compiler.plan()` legitimately returns no Connection —
  `StatementExecutor` applies staticization later.
- **Coverage**: 31 test methods matching `void test…[Pp]ivot`, including
  dedicated `integration/PivotCheckerTest.java` and
  `exec/DynamicPivotKeyLiteralTest.java`, plus witness tests named in
  the implementation comments (`testStaticPivot_SingleSingle_StringPivotValue`,
  `test_Static_Pivot_Filter`).

## Verification status

**Read, not run.** Maven is not installed in this environment and there
are no existing `core/target/surefire-reports`, so the 31 tests were not
executed. Treat "passing" as unverified until `tools/allgates.sh` runs.

The distinction that matters: this is implementation substance — an IR
node, a lowering file, a staticization pass, a capability flag — unlike
`IN_CASE_INSENSITIVE` / `NOT_IN_CASE_INSENSITIVE`, which are declared in
the function registry with no implementing class. Earlier in this work I
mistook the latter for working features; the check here was for a
lowering, not a declaration.

## Why it matters for sequencing

The client/snap plane's dependencies are now either measured or already
implemented, which makes it the most tractable part of the build:

- pivot, including two-phase dynamic discovery — **exists**
- subtotals via `concatenate` of per-level `groupBy` — exists, 97ms/5M rows
- DuckDB is legend-lite's default and most capable dialect — `needsStaticPivot` false, native passthrough
- column windowing, snap ceiling, Parquet transport — measured, see `bench/README.md`

The **server plane against real warehouses** is the unsized piece, and
it is genuinely separate work: `dialectOf` can currently only produce
`DuckDb` or a SQLite-flavored `AnsiSqlRenderer`; `AnsiSqlRenderer` throws
on `PlanParam`; and `SqlSelect.groupBy` is a flat `List<SqlExpr>`,
structurally incapable of expressing grouping sets. Snowflake/BigQuery
support therefore means new dialects plus a connection path plus
parameterized plans — real engine work that should be sized on its own
rather than smuggled into v1.

## The one number still missing

Every performance figure gathered so far is **native DuckDB on Apple
silicon**. DuckDB-WASM is typically slower, and only the `mvp` and `eh`
bundles are registered in the current setup — not `coi` — so there is no
`SharedArrayBuffer` and execution is effectively single-threaded. The
20M-row windowed pivot at 132ms native could plausibly land at 300-500ms
in the browser, which would move the 10M-row snap ceiling.

This is a day of work with a browser harness and it should happen before
the 10M number is committed to anywhere user-visible.
