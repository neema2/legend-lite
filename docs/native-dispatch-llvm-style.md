# Native Dispatch — LLVM-Style IR / Codegen Split

## Thesis

Pure → checker → **typed IR** (data) → **codegen** (dialect) → SQL string.

Mirror LLVM:

- **Frontend** (Pure parser + checker) emits typed IR.
- **IR** (`SqlExpr`, `SqlAggregate`, `SqlRelation`) is **pure data**. Records carry
  their structural fields. They have **no methods that emit SQL** and **no dependency
  on `SQLDialect`**. They can be inspected, optimized, transformed, dumped — as data.
- **Codegen** is the dialect. There is **exactly one** codegen entry per IR root:
  `SQLDialect.render(SqlExpr)`, `SQLDialect.render(SqlAggregate)`, and the
  printer's structural walk over `SqlRelation` (which calls back into
  `dialect.render(...)` for every embedded expression).
- A new dialect = one new class implementing `render(SqlExpr)` and friends. Nothing
  else changes.

## Binding rule (the rule that pins this in place)

> **IR is data. IR types do not import `SQLDialect`. IR types have no methods that
> return SQL strings. The only thing that produces SQL is the dialect's `render`
> family of methods.**

If an IR type needs `SQLDialect` for any reason, the design is wrong. Stop and reread
this rule.

## Why it failed before

The codebase grew with `SqlExpr.toSql(SQLDialect)` baked into every variant.
Implementations split: some hardcode SQL (`"(" + a + " + " + b + ")"`), some delegate
(`dialect.renderXxx(...)`). New variants picked whichever pattern looked nearby. The
plan said "typed records, dialect owns rendering" but never said "records have **no**
SQL methods", so contributors (including me) drifted.

## Phasing

### Phase 0 — IR/codegen split (mechanical, no behavior change) ✅

1. **`SqlExpr` interface**: removed `String toSql(SQLDialect)`. ✅
2. **All 64 `SqlExpr` records**: deleted `toSql` method bodies. ✅
3. **`SQLDialect` interface**: added `String render(SqlExpr)` as default method
   with a large pattern match over the sealed `SqlExpr` hierarchy. Variants that
   differ across dialects delegate to per-variant helper methods
   (`renderListExtract`, `renderStartsWith`, etc.). ✅
4. **`SqlRelationPrinter` deleted entirely.** All clause-level rendering
   (SELECT/FROM/WHERE/JOIN/GROUP BY/UNION/PIVOT/ASOF JOIN, plus structural
   helpers `renderAsStatement`/`renderAsFromItem`/`renderJoinBody`/
   `ensureFromAlias`) moved into `SQLDialect.render(SqlRelation)` as default
   methods. The `plan/printing/` package is gone. ✅
5. **Callers updated**: `PlanGenerator.generate()` calls `dialect.render(rel)`.
   No other entry points existed. ✅
6. **Verified**: test counts before and after Phase 0 are identical (886
   broken at the `78c1315` Phase 1+2 baseline; 886 broken after Phase 0).
   Pure refactor confirmed. ✅

**Acceptance criteria met:**
- `grep "toSql" engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java` → 0
- `grep "toSql" engine/src/main/java/com/gs/legend/plan/sql/SqlRelation.java` → 1 (javadoc invariant)
- `grep -rln "\\.toSql(dialect)" engine/src/main/java` → 0
- `engine/src/main/java/com/gs/legend/plan/printing/` directory → does not exist
- No `SqlRelationPrinter` references in main code

### Phase 1 — Native binding table for scalar (✅ landed in `78c1315`)

Already in place: `NativeBindingTable`, `NativeBindings`, typed
`BinaryArith`/`BinaryCompare`/`StringConcat`/`Negate`/`ListExtract`/`ListSlice`/`ListLength`
records. Phase 0 strips `toSql` from these records too — no other change.

### Phase 2 — Scalar list overloads (✅ landed in `78c1315`)

Already in place: head, first/1, first/2, last, tail, init, at, slice. Phase 0
strips `toSql` from `ListExtract`/`ListSlice`/`ListLength`.

### Phase 3.1 — Aggregates (typed `SqlAggregate`, redo cleanly)

1. New file `engine/src/main/java/com/gs/legend/sqlgen/SqlAggregate.java`. Sealed
   interface, pure-data records. **No `toSql`. No `SQLDialect` import.**
   Variants: `Sum, Avg, Min, Max, Count, Median, Mode, StdDev, StdDevSample,
   StdDevPopulation, Variance, VarianceSample, VariancePopulation, PercentileCont,
   PercentileDisc, StringAgg, Product`. No `Generic` escape hatch — every variant
   is named.
2. Each variant implements `mapArgs(UnaryOperator<SqlExpr>)` for structural
   rewrites (used by Pivot's `unqualify`). That's the only behavior on the type;
   it's a tree walk, not codegen.
3. `SQLDialect.render(SqlAggregate, boolean distinct)` — single dispatcher.
   Pattern-matches all variants. DISTINCT wrapping is dialect concern (only valid
   for unary aggregates).
4. `SqlRelation.Agg(alias, SqlAggregate aggregate, boolean distinct)` —
   restructure record. Delete `function: String, args: List<SqlExpr>` fields.
5. `AggregateBindingTable`, `AggregateBinding`, `AggregateBindings` — populate
   from `BuiltinRegistry`, keyed on `NativeFunctionDef` identity. No string
   matching.
6. `GroupByAggregateLowering.lowerAgg`: lookup binding, emit typed `SqlAggregate`.
7. `SqlRelationPrinter.renderAgg`: one line — `return dialect.render(a.aggregate(),
   a.distinct());`. No `aggSqlName` helper. No SQL strings.
8. Delete `mapAggFunctionName`. Delete aggregate cases from
   `DuckDBDialect.renderFunction` once verified unreachable.

### Phase 3.2 — Window aggregates / value functions

(deferred — same shape as 3.1 over `SqlExpr.WindowFunction` / `WindowCall`)

### Phase 3.3 — Arithmetic / comparison cleanup

(deferred — Phase 0 already removed `toSql` from `BinaryArith`/`BinaryCompare`/
`StringConcat`/`Negate`. Phase 3.3 deletes the remaining cases in
`NativeCallLowering.lower` legacy switch for `plus`/`minus`/`times`/`divide`/`equal`/
`notEqual`/`lt`/`le`/`gt`/`ge`.)

## Acceptance criteria

- `grep -rE 'toSql\\(' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java`
  returns 0 matches after Phase 0.
- `grep -rE 'implements SqlExpr' engine/src/main/java/com/gs/legend/sqlgen/`
  shows every variant has no `toSql` method.
- `grep -E 'import.*SQLDialect' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java
  engine/src/main/java/com/gs/legend/sqlgen/SqlAggregate.java` returns 0 matches.
- All currently-passing tests still pass after Phase 0 (pure refactor).

## Out of scope

- Optimizer passes over IR. The IR shape supports them now, but no pass is
  required for any current functionality.
