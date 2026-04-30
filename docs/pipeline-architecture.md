---
description: Holistic pipeline architecture — closed typed MIR, sharp layer boundaries
status: active
supersedes:
  - docs/native-dispatch-llvm-style.md
  - ~/.windsurf/plans/native-dispatch-table-v6_1-992f1a.md (archived)
---

# Pipeline Architecture — closed typed MIR

> **One plan, end-to-end. Replaces v6.1 + llvm-style. The layers are justified
> only if their boundaries are sharp. This document defines the boundaries and
> the migration path to enforce them.**

## 1. Pipeline

This document owns **midend + backend** (HIR → MIR → SQL). The frontend
(text → HIR) is owned by `frontend-architecture.md`. Full pipeline:

```
text                                                     [FRONTEND]
  → Parser              (text → AST)
  → NameResolver        (imports → FQN)
  → PureModelBuilder    (AST defs → CompiledElements)         ┐ Element Compiler
  → MappingNormalizer   (mappings → NormalizedMapping)        ┘
  → TypeChecker         (ValueSpec + model → HIR)             ┐ Expression Compiler
  → MappingResolver     (HIR + mapping → resolved HIR)        ┘
                                                         [MIDEND]
  → Lowerer             (HIR → MIR)               [plan/lowering/]
                                                         [BACKEND]
  → Dialect             (MIR → SQL)               [sqlgen/]
  → Executor            (SQL → results)           [executor/]
```

`PlanGenerator` is a thin orchestrator class wrapping the last three
calls into a `SingleExecutionPlan`. It is not its own layer. It stays —
no rename, no relocation. Its Javadoc documents it as an orchestration
shell with no logic.

**`ResultFormat.from(TypedSpec)`** is a parallel HIR walker that produces a
`ResultFormat` for the executor. It does not participate in lowering and
emits no MIR. It is not bound by the layer rules below.

## 2. Layer ownership (the contract)

| Layer | Owns | Forbidden |
|---|---|---|
| **Parser** | text → AST | type info, semantic decisions |
| **Compiler/Checkers** | AST → typed HIR; resolves overloads to `NativeFunctionDef`; computes types/multiplicities/store resolutions | emitting any IR node (HIR is its output, not MIR) |
| **Lowerer** | HIR → MIR via lowering rules + binding tables | naming SQL functions; SQL syntax; importing `SQLDialect`; `String` fields named `name`/`function`/`sqlName` flowing into MIR |
| **Dialect** | MIR → SQL via `render(SqlExpr)`, `render(SqlAggregate, distinct)`, `render(SqlRelation)` | inferring types; rewriting HIR; consulting the model |
| **Executor** | SQL string → results | everything above |

## 3. Core principle: the MIR is **closed**

> **The single carve-out**: `SqlExpr.Cast(expr, pureTypeName)` carries a
> Pure type name string. Pure type names are not SQL. The dialect maps
> them via `sqlTypeName()`. **This is the only string-typed dispatch
> surface in the entire MIR.** Anything else with a `String` field that
> encodes a SQL operation, function name, or operator is a bug — replace
> it with a typed variant.

The MIR (`SqlExpr`, `SqlAggregate`, `SqlRelation`, plus their record
hierarchies) is a **sealed, exhaustively-typed** intermediate representation.
Concretely:

1. **Every operation is its own record.** No `FunctionCall(String name, args)`
   catch-all. No `Binary(left, String op, right)`. No `WindowCall(String name, ...)`.
   If Pure has 7 overloads of `first`, the MIR has the variants required to
   distinguish their lowerings (often just one, since multiplicities collapse
   in the lowering decision).
2. **No String fields encode dispatch.** The single carve-out is
   `Cast(expr, pureTypeName)` where `pureTypeName` is a Pure type name
   (`"Integer"`, `"String"`) mapped by `dialect.sqlTypeName()`. This is
   documented in AGENTS.md and is the only string-typed dispatch surface.
3. **Pattern-match exhaustiveness is the test.** The dialect's
   `render(SqlExpr)` is a switch **expression** (not statement) over the
   sealed hierarchy. `SqlExpr`, `SqlAggregate`, `WindowAggregate`, and
   `SqlRelation` carry **explicit `permits` clauses** naming every
   variant; javac then enforces exhaustiveness and fails the build on a
   missing arm. **No `default ->` in any dialect render method.**
4. **Lowerings cannot leak SQL** because they have nowhere to put SQL. The
   types simply do not admit a SQL-name field. The compiler enforces the
   boundary.

This is the LLVM analogy taken seriously: HIR is the source IR (high-level,
rich semantics); MIR is the target-agnostic backend IR; Dialect is codegen.
Each IR boundary is a typed surface, not a stringly-typed gateway.

## 4. Current state (audit)

### 4.0 Test baseline

As of commit `78c1315` (Phase 1+2 completion): **2669 tests run, 1783
passing, 886 failing/erroring** (pre-existing). Each migration phase
must preserve passing-test count. New regressions are blocking; the
886 pre-existing failures are tracked separately and not in scope.

### 4.1 MIR records — 71 today
Mostly typed, but with three offenders:

- `SqlExpr.FunctionCall(String name, List<SqlExpr> args)` — catch-all, used in
  17 lowering sites
- `SqlExpr.Binary(SqlExpr left, String op, SqlExpr right)` — legacy, used in
  7 lowering sites; replaced by typed `BinaryArith` / `BinaryCompare` /
  `StringConcat` / `Negate` for some cases, but old constructor still alive
- `SqlExpr.WindowCall(String name, ...)` — stringly-typed window function

Plus orphans/redundancies needing cleanup:
- `WindowFunction`, `WindowSpec`, `WrappedWindowFunction` — legacy unused
  records (verify and delete in Phase A)
- `Unary` — legacy generic unary; should be replaced by typed variants

### 4.2 No `SqlAggregate` exists
`SqlRelation.Agg` currently uses a stringly-typed function name and arg list.
`GroupByAggregateLowering.mapAggFunctionName` maps Pure names to SQL function
names (`"sum"`, `"product"`, `"stddev"`) — this is the most blatant SQL leak
in the lowering layer.

### 4.3 `DuckDBDialect.renderFunction` — 168 case arms
Most arms are 1:1 name remaps; ~30 do structural decomposition (e.g.
`dateDiff`, `timeBucket`, `lpadSafe`, `listMinMaxBy`). After closure, this
switch shrinks to zero because `renderFunction` itself goes away — every Pure
name becomes a typed MIR variant rendered by a `case` in `render(SqlExpr)`.

### 4.4 Stringly-typed name mappers in lowering
- `GroupByAggregateLowering.mapAggFunctionName` — 10 cases
- `ExtendLowering.windowFuncName` — 4 cases
- `Bindings.fc(String sqlName)` — factory parameterized by SQL name
These all delete in Phase A/B.

## 5. The closed MIR — variants to introduce

Goal: replace every `FunctionCall(String, ...)` and `Binary(_, String, _)` and
`WindowCall(String, ...)` with typed variants. Concretely:

### 5.0 What's already typed and stays

- **Date/time**: `DateLiteral`, `TimeLiteral`, `TimestampLiteral`,
  `IntervalLiteral`, `DateAdd`, `CurrentDate`, `CurrentTimestamp` —
  kept. Only the 23 date-*extraction* cases (`year`, `month`,
  `dayOfMonth`, ...) collapse into a new `DatePart` variant.
- **Comparison**: `BinaryCompare(Op, left, right)` with enum `Op` —
  kept; expand the enum if needed.
- **Arithmetic**: `BinaryArith(Op, left, right)` with enum `Op` —
  **kept and extended**; we do **not** introduce per-op records
  (`Add`/`Subtract`/`Multiply`/`Divide`). Enum cases: `PLUS`, `MINUS`,
  `TIMES`, `DIVIDE`. `StringConcat` stays as a separate variant
  because Pure `plus` is overloaded on `String` and the binding picks
  `StringConcat` vs `BinaryArith.PLUS` based on the resolved
  `NativeFunctionDef`.
- **List**: `ListExtract`, `ListSlice`, `ListLength`, `ListContains`,
  `ListLambda`-shaped operations introduced in Phase C.
- **Structural**: `Column`, `Identifier`, `FieldAccess`,
  `QualifiedStar`, `Star` — kept.
- **Variant**: `VariantAccess`, `VariantTextAccess`,
  `VariantTextExtract`, `VariantIndex`, `ToVariant`, `VariantCast`,
  `VariantArrayCast`, `VariantScalarCast`, `VariantLiteral` — kept.
  New `VariantGet` joins them in Phase C.
- **Literals**: all literal records are kept.
- **Frame bounds**: `CurrentRowFrameBound`, `OffsetFrameBound`,
  `UnboundedFrameBound` — kept.

### 5.0a Lambdas in MIR

MIR carries lambdas as data. `SqlExpr.LambdaExpr(List<String> params,
SqlExpr body)` already exists. Lowering builds it by:

1. Bind each Pure lambda parameter to a fresh MIR identifier in a
   child `LoweringContext`.
2. Recursively lower the Pure lambda body via `Lowerer.lowerScalar`
   under the bound context.
3. Wrap the result as `LambdaExpr(boundParamNames, loweredBody)`.

No other shape is admissible. **MIR never holds a Pure AST node.**
Lambda body lowering is recursive scalar lowering — same machinery as
any other scalar.

When a typed MIR variant takes a lambda (`ListReduce`, `ListTransform`,
etc.), its lambda field is typed `SqlExpr` (constrained by convention
to be `LambdaExpr`). The dialect's render arm for that variant emits
the correct dialect-specific lambda syntax (DuckDB `(x, y) -> body`,
SQLite throws — see §9).

### 5.0b Binding-table registration shape

One example for each binding family. All bindings live in `populate()`
methods inside `NativeBindings` / `AggregateBindings` / `WindowBindings`
and are looked up by exact-identity `NativeFunctionDef`:

```java
// Scalar (NativeBindings.populate)
TABLE.bind(pick("first", "T[*]->Integer[1]"),
    (call, args, ctx) -> new SqlExpr.ListExtract(args.get(0),
        new SqlExpr.NumericLiteral(((Long) ((SqlExpr.NumericLiteral) args.get(1)).value()) + 1)));

// Aggregate (AggregateBindings.populate, Phase A)
AGG_TABLE.bind(pick("sum", "Number[*]"),
    (call, expr) -> new SqlAggregate.Sum(expr));

// Window (WindowBindings.populate, Phase B)
WIN_TABLE.bind(pick("first", "Relation<T>[1]->ColSpec<T>[1]->Window<T>[1]"),
    (call, args) -> new WindowAggregate.FirstValue(args.get(0)));
```

A missing binding throws `IllegalStateException("no binding for " + def)`.
There is no string-name fallthrough. Phase D removes the legacy switch
entirely.

### 5.0c WindowExpr arg migration

Today: `SqlExpr.WindowCall("lag", List.of(expr, offset, default), partitionBy, orderBy, frame)`.

After Phase B: `SqlExpr.WindowExpr(new WindowAggregate.Lag(expr, offset, default), partitionBy, orderBy, frame)`.

The `args` list disappears. Each `WindowAggregate` variant carries its
own structural fields with correct arity. `WindowBindings` registers
the `(NativeFunctionDef, ctor)` mapping per overload.

### 5.1 New `SqlExpr` variants
| Variant | Replaces | Lowering site |
|---|---|---|
| `WrapList(SqlExpr inner)` | `FunctionCall("wrapList", ...)` | NativeBindings, FoldLowering ×4 |
| `ListConcat(SqlExpr left, SqlExpr right)` | `FunctionCall("listConcat", ...)` | FoldLowering |
| `ListReduce(SqlExpr list, SqlExpr lambda, SqlExpr init)` | `FunctionCall("listReduce", ...)` | FoldLowering ×3 |
| `ListTransform(SqlExpr list, SqlExpr lambda)` | `FunctionCall("listTransform", ...)` | FoldLowering ×2, ControlFlowLowering |
| `VariantGet(SqlExpr variant, SqlExpr key)` | `FunctionCall("get", ...)` | NativeCallLowering |

Arithmetic (`+`, `-`, `*`, `/`) maps to existing `BinaryArith(Op, left, right)`
with enum `Op`; we do **not** introduce per-op records. Same for comparison
(existing `BinaryCompare(Op, ...)`) and string concat (existing `StringConcat`).
The legacy stringly-typed `Binary(_, String, _)` deletes in Phase E.

The 30 dialect-specific decomposition cases (`dateDiff`, `timeBucket`,
`lpadSafe`, etc.) become typed variants whose `render` arm in each dialect
emits the appropriate decomposition. Many are already DateAdd-style typed.
Audit list (built from DuckDBDialect lines 174-442):

- `year`, `month`, `dayOfMonth`, `hour`, `minute`, `second`, `quarter`,
  `quarterNumber`, `dayOfWeekNumber`, `firstDayOfMonth`, `firstDayOfYear`,
  `firstDayOfQuarter`, `firstHourOfDay`, `firstMillisecondOfSecond`,
  `dayOfWeek`, `dayOfYear`, `weekOfYear`, `extractDow`, `dateTruncDay`,
  `extractYear`, `extractMonth`, `extractDay`, `extractHour` — all become
  one typed `DatePart(Part, expr)` with a `Part` enum. ~23 cases collapse
  to one variant.
- `timeBucket`, `timeBucketScalar` — `TimeBucket(expr, count, unit, anchor?)`
- `lpadSafe`, `rpadSafe` — `PadSafe(side, expr, width, fill)`
- `decodeBase64`, `encodeBase64` — `Base64(direction, expr)`
- `indexOfFrom` — `IndexOfFrom(haystack, needle, start)`
- `joinStringsWithPrefixSuffix` — `JoinStringsWithAffixes(...)`
- `bitShiftRightSafe` — `BitShiftRight(expr, n)`
- `listSort`, `listSortWithKey`, `listFind`, `listZip`, `listMinMaxBy`,
  `listMinMaxByTopK`, `listMedian`, `listMode`, `listStdDev{Sample,Population}`,
  `listVariance{Sample,Population}`, `listCorr`, `listCovar{Sample,Population}`,
  `listPercentile{Cont,Disc}`, `arrayToString`, `listAppend`, `listReverse`,
  `listFilter`, `listSum`, `listExtract`, `listSlice`, `listLength`,
  `listConcat` — all typed variants, many are already there.
- `dateDiff` — typed `DateDiff(unit, a, b)` (replaces decomposition arm)
- `isDistinctFrom` — typed `IsDistinctFrom(left, right)`
- `currentUserId`, `SHA1`, `format`, `toString`, `toUpperFirstCharacter`,
  `toLowerFirstCharacter`, `jsonGet` — typed nullary/unary variants

Estimated final variant count: ~95 SqlExpr variants. Up from 71. Net +24
because the catch-all collapses many name-bearing call sites into typed nodes.

### 5.2 New `SqlAggregate` hierarchy
Replaces `SqlRelation.Agg(String funcName, List<SqlExpr> args, ...)`:

```
sealed interface SqlAggregate permits Sum, Count, CountStar, Avg, Min, Max,
    StdDev{Sample,Population}, Variance{Sample,Population}, Median, Mode,
    StringAgg, ArrayAgg, JsonObjectAgg, JsonArrayAgg, PercentileCont,
    PercentileDisc, BoolAnd, BoolOr, Product, First, Last, ... {}
```

`SqlRelation.Agg` becomes `Agg(SqlAggregate agg, boolean distinct, String alias)`.
`mapAggFunctionName` deletes.

### 5.3 New `WindowAggregate` hierarchy
Replaces `WindowCall(String name, ...)`:

```
sealed interface WindowAggregate permits RowNumber, Rank, DenseRank,
    PercentRank, CumeDist, Ntile, Lag, Lead, FirstValue, LastValue,
    NthValue, /* + all SqlAggregate-shaped wrappers */ ... {}
```

`WindowCall` becomes `WindowExpr(WindowAggregate fn, List<SqlExpr> partitionBy,
List<OrderByTerm> orderBy, WindowFrame frame)`. `windowFuncName` deletes.

## 6. Migration phases

Each phase is **atomic**: it adds typed variants, migrates lowering sites,
removes the legacy catch-all surface for one family, and at completion the
sealed hierarchy is exhaustive over that family. No phase leaves a half-typed
state.

### Phase 0 — Done ✅
SQL syntax stripped from MIR records and printer. `SqlRelationPrinter`
deleted, all rendering in `SQLDialect`.

### Phase A.0 — Pre-Phase-A: kill `Bindings.fc(String)`

Before Phase A starts, **delete `Bindings.fc(String sqlName)` entirely**.
Every existing call site either:
- Migrates to a typed-variant binding now (small per-site fix), or
- Stops registering and falls through to the legacy `NativeCallLowering`
  switch until its phase arrives.

This prevents new sites from picking up the bad pattern during
Phases A–C and keeps Layer 3 leak-free in the binding tables.

Acceptance: `grep -rE 'Bindings\.fc\(' engine/src/main/java/` returns 0;
`grep -rE 'String sqlName' engine/src/main/java/com/gs/legend/plan/lowering/` returns 0.

### Phase A — Aggregates closed
1. Introduce `SqlAggregate` sealed hierarchy with all variants.
2. Refactor `SqlRelation.Agg` to hold `SqlAggregate` instead of `(String, args)`.
3. Add `dialect.render(SqlAggregate, distinct)` arm; dialects override as
   needed.
4. Migrate `GroupByAggregateLowering` to emit typed `SqlAggregate`. Delete
   `mapAggFunctionName`.
5. Add `AggregateBindingTable` keyed by `NativeFunctionDef` → `SqlAggregate`
   factory. Populate for all aggregate overloads.
6. Greppable acceptance: `mapAggFunctionName` does not exist;
   `grep "Agg(.*\".*\"" SqlRelation.java` returns 0.

### Phase B — Window aggregates closed
1. Introduce `WindowAggregate` sealed hierarchy.
2. Replace `SqlExpr.WindowCall(String, ...)` with `WindowExpr(WindowAggregate, ...)`.
3. Add `dialect.render(WindowExpr)` (or arm in `render(SqlExpr)`).
4. Migrate `ExtendLowering.lowerWindowCol`. Delete `windowFuncName`.
5. Add `WindowBindingTable` keyed by `NativeFunctionDef`.
6. Greppable: `windowFuncName` does not exist; no `WindowCall` record.

### Phase C — List/fold ops closed
1. Add typed variants: `WrapList`, `ListConcat`, `ListReduce`, `ListTransform`,
   `VariantGet`.
2. Migrate `FoldLowering`, `ControlFlowLowering`, `NativeCallLowering`,
   `NativeBindings`. Each `FunctionCall("...", ...)` becomes a typed
   constructor.
3. Add corresponding `dialect.render` arms.
4. Greppable: `FunctionCall(".*Reduce|.*Transform|wrapList|listConcat|^get$"`
   in lowerings returns 0.

### Phase D — Date/string/misc surface closed
1. Introduce typed variants for the 30 decomposition cases in
   `DuckDBDialect.renderFunction` (DatePart, TimeBucket, PadSafe, Base64,
   etc., per §5.1).
2. Migrate `NativeCallLowering` default fallthrough — every Pure native that
   currently falls through to `FunctionCall(name, args)` either gets a
   binding to a typed variant or a binding that throws "unsupported."
3. Delete `SqlExpr.FunctionCall` entirely.
4. Delete `SQLDialect.renderFunction`.
5. Delete `Bindings.fc(String sqlName)`.
6. Greppable: `record FunctionCall` and `renderFunction` and
   `Bindings.fc(String` all return 0.

### Phase E — Binary cleanup
1. Migrate all `SqlExpr.Binary(_, String, _)` sites to `BinaryArith` /
   `BinaryCompare` / `StringConcat` / `Negate` / typed alternatives.
2. Delete `SqlExpr.Binary`. Delete `SqlExpr.Unary` (audit first; may
   already be unused).
3. Greppable: `record Binary(` and `record Unary(` removed.

### Phase F — Orchestrator and exhaustiveness cleanup
1. Audit `WindowFunction`, `WindowSpec`, `WrappedWindowFunction` — likely
   unused legacy. Delete.
2. **Keep** `PlanGenerator` as the public entry. Update its Javadoc to
   document it as an orchestration shell. **No rename.** (Renaming
   breaks downstream imports for zero architectural gain.)
3. Add explicit `permits` clauses to `SqlExpr`, `SqlAggregate`,
   `WindowAggregate`, `SqlRelation` listing every variant.
4. Convert every dialect `render` method to a switch **expression** with
   no `default ->`. Build fails on missing arms. Sealed-permits
   exhaustiveness becomes the structural guard.

## 7. The standard for "correctness" per phase

Each phase is a refactor. Two checks:

1. **Test pass count does not drop.** Baseline (§4.0) is the floor.
2. **Generated SQL stays human-readable.** "Byte-identical" is **not**
   the standard — sometimes a typed variant produces *better* SQL
   (e.g. inlining what was previously wrapped in a subquery, or
   eliminating a redundant projection). The standard is: SQL stays
   readable, expressions inline where they should, no gratuitous
   subquery wrapping. Reviewer eyeballs the diff in test snapshots and
   accepts changes that improve readability while preserving semantics.

Deliberate readability gains are welcome. Regressions where a previously
inlined expression now wraps in a subquery, or where a join condition
splits across a CTE for no reason, are blocking.

## 8. Acceptance criteria (greppable)

Run from repo root after the migration completes. All must return 0:

```bash
# No SQL in MIR records or in lowerings
grep -rE 'toSql\(' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java
grep -rE 'toSql\(' engine/src/main/java/com/gs/legend/sqlgen/SqlAggregate.java
grep -rE 'import.*SQLDialect' engine/src/main/java/com/gs/legend/plan/lowering/

# No stringly-typed dispatch in MIR
grep -E 'record FunctionCall' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java
grep -E 'record Binary\(.*String' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java
grep -E 'record WindowCall' engine/src/main/java/com/gs/legend/sqlgen/SqlExpr.java
grep -E 'String name' engine/src/main/java/com/gs/legend/sqlgen/SqlAggregate.java

# No SQL-name mappers in lowering
grep -rE 'mapAggFunctionName|windowFuncName|String sqlName' engine/src/main/java/com/gs/legend/plan/lowering/

# Dialect has no `default ->` in render
grep -nE 'default\s*->' engine/src/main/java/com/gs/legend/sqlgen/SQLDialect.java engine/src/main/java/com/gs/legend/sqlgen/DuckDBDialect.java engine/src/main/java/com/gs/legend/sqlgen/SQLiteDialect.java | grep -v sqlTypeName

# renderFunction removed
grep -rE 'renderFunction\(' engine/src/main/java/com/gs/legend/sqlgen/
```

Plus: full test suite. Each phase is a pure refactor; passing-test count must
not drop.

## 8. What this replaces / kills

- `~/.windsurf/plans/native-dispatch-table-v6_1-992f1a.md` — superseded (archived)
- `docs/native-dispatch-llvm-style.md` — superseded
- All earlier v1-v5 plans — already obsolete
- `SQLDialect.renderFunction` (after Phase D)
- `SqlExpr.FunctionCall`, `SqlExpr.Binary`, `SqlExpr.Unary`, `SqlExpr.WindowCall`,
  `SqlExpr.WindowFunction`, `SqlExpr.WindowSpec`, `SqlExpr.WrappedWindowFunction`
- `mapAggFunctionName`, `windowFuncName`, `Bindings.fc(String)`

## 9. SQLite scope

SQLite has 5 `case` arms today; full parity would mean ~95 render arms.
**Not a goal for this refactor.** As each phase migrates a family,
SQLite gets:
- An override **only** if a current SQLite test exercises that variant
  and the ANSI default in `SQLDialect` produces wrong output.
- Otherwise: SQLite inherits the ANSI default (often correct, sometimes
  wrong-but-untested).
- For variants where SQLite genuinely cannot express the operation
  (e.g. lambdas, list_reduce), the SQLite arm **throws**
  `UnsupportedOperationException("<variant> not supported by SQLite")`.
  This is fine for now; future SQLite parity work is tracked separately.

## 10. Risks and notes

- **Variant count grows** (~71 → ~95 in `SqlExpr`, +25 in `SqlAggregate`,
  +15 in `WindowAggregate`). This is intentional — explicit closure beats
  implicit dispatch. Files stay manageable because each variant is a
  single-line record.
- **Dialect surface grows** in absolute lines, shrinks in branchy logic.
  168-arm switch becomes a flat pattern match where each arm is a
  rendering, not a remap.
- **No `default ->` in `render(SqlExpr)`** — exhaustiveness check is the
  guard. If a contributor adds a variant without an arm, the build fails.
- **Migration order** ensures we never have a half-stringly state. Each
  phase closes a family; the legacy catch-all surface shrinks
  monotonically; no temporary types are introduced.
- **SQLite** scope is bounded by §9: throw on unsupported variants,
  override only what current SQLite tests need.
