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

## Verification status: RUN, and passing

**23 tests, 0 failures, BUILD SUCCESS in 19s** —
`PivotCheckerTest` (SingleColumnPivot, MultiColumnPivot, NullSemantics,
TypeParity, ChainedOperations, ComplexSourceChains,
FullPipelineWithNulls) plus `DynamicPivotKeyLiteralTest`.

An earlier version of this note said "read, not run — Maven is not
installed in this environment." **That was wrong.** JDK 21 (Temurin
21.0.11) and Maven 3.9.9 are both installed, under `~/jdk`, simply not
on a non-interactive shell's PATH. The check that produced the false
conclusion was a single `ls` over several candidate paths whose first
glob matched nothing; zsh aborts the whole command line on an unmatched
glob, so the Maven and SDKMAN probes in that same command never ran, and
the empty output read as "absent".

`tools/env.sh` now resolves the toolchain so this cannot recur:

    source tools/env.sh
    mvn -o -pl core test -Dtest='PivotCheckerTest,DynamicPivotKeyLiteralTest'

`sh tools/env.sh --show` prints what it resolved without sourcing.

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

---

# Correction: DuckDB uses native PIVOT; two-phase is the H2 emulation

The framing above overstated the two-phase pass. Verified in the code:
`SqlDialect.needsStaticPivot()` defaults to **false**, and **only `H2`
overrides it to true**. So on DuckDB, legend-lite emits native dynamic
`PIVOT` and `DynamicPivot.staticize` never runs; the two-phase discovery
is specifically the emulation for a backend with no native dynamic pivot.

This inverts a conclusion. **Native dynamic PIVOT emits *every* pivot
column** — that is the `full` column in the width benchmark, 850ms at
20,000 columns. The flat 20.8ms result came from
`PIVOT ... ON pk IN (<40 visible keys>)`, a **static** pivot. Native
dynamic pivot and column windowing are mutually exclusive: "dynamic"
means the engine discovers and emits all columns, which is exactly what
windowing declines to do.

So we want the two-phase *discipline* on DuckDB as well — not because
DuckDB lacks native dynamic pivot, but because **we do not want dynamic
pivot at all.** We want discovery, then a static pivot over the visible
slice.

## Windowing is expressible today

`Pivots.lower` builds the IN list from `pv.values()` — the Pure
`pivot()` call's own values argument — so a windowed pivot needs **no
engine change**. Values must be literals ("pivot values must be
literal"), which is fine: the visible window is a literal list computed
after discovery.

## The trap: pinning an IN list pre-filters the source

`Pivots.lower` lines 92-108 pre-filter the source to the pinned values,
deliberately, to match engine semantics — the engine drops out-of-list
rows while DuckDB's `PIVOT ... IN` keeps them as extra groups (witness
`testStaticPivot_SingleSingle_StringPivotValue`: 9 groups where the
engine produces 3).

Correct for parity, and fatal for naive column windowing. Demonstrated:

    CREATE TABLE t AS SELECT * FROM (VALUES
      ('A', 1, 10.0), ('A', 1, 20.0), ('B', 99, 30.0)) v(grp, pk, amt);

    PIVOT t ON pk IN (1) USING sum(amt) GROUP BY grp;
    -- A -> 30.0,  B -> NULL          (both groups present)

    PIVOT (SELECT * FROM t WHERE pk IN (1))
      ON pk IN (1) USING sum(amt) GROUP BY grp;
    -- A -> 30.0                      (group B is GONE)

If the visible column window drives the pinned values, then **the row
set becomes a function of the horizontal scroll position**: groups whose
keys all fall outside the window disappear, and any row total computed
this way silently becomes a total of the visible window only.

## Consequence: three queries, each windowed on the axis it owns

1. **Row axis** — `groupBy(rowDims)` plus row totals. No pivot, so no
   pre-filter and no column dependence. Windowed on rows only. This is
   the authority for which rows exist, the scrollbar extent, and row
   totals across *all* columns.
2. **Column axis** — `SELECT DISTINCT key ORDER BY key`, the ordered
   column list. The grid needs this independently anyway, to build its
   column model and know what the visible window is a window *of*.
   DataCube does the same thing today via its `getCastColumns` call.
3. **Cells** — the windowed static pivot, visible rows x visible
   columns. Values only; never the authority for which rows exist.

This also hands snap mode a real advantage: **while snapped, the
discovered column list is immutable**, so discovery runs once per snap
rather than once per query. Live mode must re-validate it.

## Benchmark caveat

`widepivot.py`'s row count was unaffected by the pre-filter only because
every `(region, country)` group in the synthetic data contains every
`pk`. The 41x figure remains valid as a **cell-value** measurement; it
does not measure, and must not be read as endorsing, a design where the
pivot query determines the row set.

## One compatibility confirmation

Multi-column pivots synthesize a composite key joined by `'__|__'`,
which is byte-identical to DataCube's
`PIVOT_COLUMN_NAME_VALUE_SEPARATOR` (`DataCubeQueryEngine.ts:311`). The
two sides already agree on the separator.
