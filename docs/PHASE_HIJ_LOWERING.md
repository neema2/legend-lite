# Phases H–J — Lowering: Typed HIR → Lean SQL (Design)

Status: DESIGN, pre-implementation. Sources studied (2026-07-03, three-agent mining;
reports summarized here — the behavioral inventories live in the session record):

1. **Current-branch `engine/plan/lowering`** — learnings, NOT canonical (its failures
   trace to `MappingResolverV2` co-evolving under a DO-NOT-COMMIT flag, not lowering).
2. **`docs/reference/plangen-legacy-pre-port.java.txt`** — the master PlanGeneration
   god class; behavioral gold standard (100% green on master). Own docstring: "minimize
   subquery wrapping".
3. **Real legend-engine `pureToSQLQuery.pure` (10.4k lines) + `sqlQueryToString/`** —
   canonical semantics AND canonical *shape*: ops fold into one `SelectSQLQuery`;
   subselect isolation only on collision.

## Tenet (user-set, confirmed canonical)

**Super lean, human-readable SQL.** One SELECT per fold-compatible run of operators;
subqueries only when an op genuinely collides with accumulated clauses; joins inlined.
Flatness is the IR's natural state, not a printer optimization (the current branch's
nested-IR + smart-printer approach is explicitly rejected).

## Architecture

```
TypedSpec (HIR, fully schema'd)
  → [H: StoreResolver — class sources → tables+joins via mappings]   (separate, later)
  → [I: Lowerer — per-construct rules + ONE fold policy] → SqlSelect IR
  → [J: Dialect renderer — DuckDB first]                 → SQL text
```

### The IR: one `SqlSelect`, sealed sources, data-only

Mirrors real legend's `SelectSQLQuery` (and fixes both lites' gaps):

- **`SqlSelect`** — THE node: projections, `from: SqlSource`, `where`, `groupBy`,
  `having`, `qualify`, `orderBy`, `limit/offset`, `distinct` flag, window columns.
  A run of foldable ops mutates/extends one `SqlSelect`.
- **`SqlSource`** (sealed): `Table`, `Values` (TDS/new), `Subselect(SqlSelect, alias)`,
  `JoinTree` (inlined joins incl. ASOF + association LEFT JOINs), `Union`, `Pivot`,
  `Unnest`, `SourceUrl`.
- **`SqlExpr`** (sealed, data-only): columns, literals, calls by SEMANTIC name,
  `WindowCall`, `Exists`, `Lambda` (list fns), `VariantAccess`/`VariantTextAccess`, Case.
- **`SqlAgg`** position-typed as in the current branch: `Reducer` vs `RankingFn`/`ValueFn`
  so "LAG in GROUP BY" is a compile error. KEEP.
- **Typed outputs everywhere**: every IR node carries its output columns WITH types —
  core's HIR has full schemas on `info()`, so this is free (current branch dropped
  types on most ops and flagged the retrofit as painful).
- **Data-only discipline**: no `toSql()` on IR nodes, no early string rendering
  (master's pivot/get/frame string-surgery is the anti-pattern). Rendering lives
  ONLY in the dialect.

### Result shape: a first-class axis (v1 learning)

The OUTPUT TYPE changes the whole back half — SQL envelope, execution-node result
format, and return structure. Classified TOTALLY from the HIR root's `ExprType`
(core's types are complete, so this is a closed switch, not heuristics):

| Root type | Shape | SQL envelope / return |
|---|---|---|
| `RelationType` | **TDS** | plain SELECT; columns+rows result |
| `ClassType[*]` / graphFetch / serialize | **GRAPH** | JSON envelope — `json_group_array(json_object(…))` (snapshot) or per-row `json_object` (streaming); nested to-many via correlated `json_group_array` subqueries |
| scalar `[1]` (incl. fold results, `write`→count) | **SCALAR** | single-value SELECT |
| scalar/class `[*]` collection | **COLLECTION** | UNNEST to N rows when result `isMany`; single LIST cell otherwise |

`ResultShape.of(TypedSpec)` is decided ONCE at the plan root and drives the
envelope; per-op lowering never branches on it (master mixed this in; the current
branch's `ResultFormat.from(hir)` had the right idea).

**The four shapes are ALREADY the downstream contract**: the K-layer
`ExecutionResult` is a sealed `Scalar/Collection/Tabular/Graph` quartet, and the
PCT adapter (`pct/.../ExecuteLegendLiteQuery.java`) converts each back to Pure
instances under the rule "all type information flows from Type on
ExecutionResult — no SQL type inspection." Lowering must emit results that keep
that contract: Pure types ride the result, consumers never sniff JDBC/SQL types.
(PCT also holds the protocol-reshaping details — TDS string format, date
subsecond trimming — those stay consumer-side, but they depend on typed results.)

### The fold policy: ONE authority (`Fold`)

Master's fatal flaw: each `generateXxx` re-derived inline-vs-wrap by poking builder
booleans, with per-op drift. Real legend centralizes per-op guards. Core gets ONE
policy class answering `Placement place(opKind, currentSelect)`; the rules (mirroring
real legend's guards, cross-checked against master's):

| Op | Folds when | Placement / else |
|---|---|---|
| filter | always places | WHERE if not aggregated; HAVING if groupBy/pivot; QUALIFY if predicate references a window column; isolate only if select was subquery-mutated |
| project/select | projected cols ⊇ groupBy cols ∧ ⊇ orderBy cols ∧ !distinct | replace projections / else isolate |
| extend (scalar) | (near-)always | append projections; isolate only over pivot |
| rename | always | alias-level; collapse chains (real legend `collapseTDSRename`) |
| sort | no set-op/pivot, orderBy unset | set orderBy / else isolate |
| limit/slice/drop | corresponding slot unset | set fromRow/toRow / else isolate |
| distinct | no groupBy/orderBy/limit | set flag / else isolate |
| groupBy | no existing groupBy/pivot/distinct ∧ orderBy ⊆ keys ∧ no window cols | set groupBy+aggs / else isolate |
| window extend | — | always isolates source (window over defined row set) |
| join/asOfJoin | — | JoinTree source; sides isolated only if not already table-shaped |
| pivot/union | — | structural sources |

**Mechanism (borrowed from Apache Calcite's `RelToSqlConverter`)**: the industry
formalization of exactly this problem is Calcite's `Clause` precedence enum
(`FROM < WHERE < GROUP_BY < HAVING < SELECT < SET_OP < ORDER_BY < FETCH`): an op
folds iff every clause it writes is unoccupied at its precedence position; wrap
otherwise. That is our `Fold` core; real legend's placement UPGRADES layer on top
(where Calcite would wrap, legend re-places: filter-over-agg → HAVING,
filter-over-window → QUALIFY, rename-chain collapse). SQLGlot's optimizer rules
(`merge_subqueries`, `eliminate_subqueries`, `qualify`) serve as the review
checklist for merge-safety edge cases (no merging into/out of
limit/offset/distinct/group inners; alias hygiene under correlation).

**Leanness has TWO dimensions, both test-pinned (engine's own FlatSql suites —
`RelationalMappingIntegrationTest` "FlatSql" tests, `RelationalMappingCompositionTest.countLeftJoins`):**
1. **SELECT flatness** — `assertEquals(1, count(sql, "SELECT"))` style with exact
   JOIN counts, plus boundary pins (filter-before-traverse = exactly 2 SELECTs;
   sort/filter between traverses breaks the chain). Port this assertion style as
   the golden-flatness suite for every fold rule and every isolation rule.
2. **Join elision** — joins that no consumed column needs are NOT emitted
   (`countLeftJoins == 0` when only main-table columns are used; master's
   `neededJoins` set). Association/traverse joins materialize on USE, not on
   declaration. **Placement: this lives in Phase H (StoreResolver), not the
   fold policy** — pure `#>{}#` relation queries write their joins EXPLICITLY
   (a user-written join is never elided; that would change semantics); elision
   applies only to MAPPING-implied joins — association navigation and class
   property access — where the resolver decides which hops the query actually
   consumes. The I-layer fold policy treats every join it receives as required.

### Lowering rules + dispatch

- Exhaustive sealed switch over `TypedSpec` (the `children()` spine + per-construct
  nodes were built for this). Per-construct rule classes mirroring the checker layout.
- Scalar natives: typed dispatch tables keyed by resolved `TypedFunction` IDENTITY
  (`TypedNativeCall.callee()`), never name strings. ONE table from day one — the
  current branch's dual typed+legacy path is rejected.
- Association navigation (Phase H): **filter position → scalar `EXISTS`** (Boolean
  composability under AND/OR/NOT/CASE — current branch's correct call);
  **projection/sort/groupBy positions → LEFT JOIN** discovery in the same walk that
  emits column refs (NavScope idea), designed for ALL nav kinds up front.
- Aliases: one shared counter, `t0..tN`, deterministic (no magic strings like
  master's `"proj_src"`/`"ext"`/`"grp"`).

### Dialect seam (J)

Interface with real legend's proven hook shape (subset): identifier quoting +
reserved words, literal rendering (string escape by quote-doubling, tz-aware dates),
semantic-function table, select assembly, feature flags (QUALIFY, native PIVOT,
ASOF JOIN). DuckDB first. IR carries semantic names (`listReduce`, `roundHalfEven`);
the dialect maps them.

## Semantics contract (MUST-honor; from real legend + master's test-pinned truths)

- `!=` null-aware expansion (`l<>r OR l IS NULL …`); null-safe equal helper;
  `not in` adds `OR l IS NULL`. `isEmpty`→`IS NULL` (scalar) / `list_length=0` (list).
- Division forces float: `((1.0 * a) / b)`; `divideRound`→`roundHalfEven`.
- `mod`→always-positive `mod(mod(x,n)+n,n)`; `rem`→plain `mod`. Banker's rounding
  (`roundHalfEven`) for round/divide-scale.
- 0↔1-based conversions: substring/at/slice/splitPart/take/drop +1 (with GREATEST
  clamps); indexOf −1.
- `toOne`/`eval`/`match`/`cast` erase to identity in SQL (except typed variant casts).
- Partial date literals (year / year-month) compare as STRINGS both sides;
  `adjust` re-wraps via strftime; `%latest`→CURRENT_DATE.
- Empty TDS → one all-NULL row + `WHERE 1=0` (schema preserved, zero rows).
- String concat via `plus` only when operand types say STRING (typed decision).
- Heterogeneous lists: per-element `::VARIANT`, Integer upcast to BIGINT first.
- join/asOfJoin prefix semantics as typed in Phase G (all right cols; our documented
  divergence). Real legend asserts no-duplicate-columns on join — we type-error it.
- Fold strategies → `list_concat` / `list_reduce` / `list_transform`+`list_reduce` /
  CollectionBuild unwrap — the `FoldStrategy` stamped by Phase G.
- Variant navigation: `get`→`->` / index; `to(@T)`→`->>` + CAST; `toMany`→CAST to array.
- Window: DESC→NULLS FIRST, ASC→NULLS LAST; frames ROWS/RANGE BETWEEN with
  PRECEDING/FOLLOWING/CURRENT ROW from signed offsets/unbounded.
- DuckDB shapes: `EXCLUDE` for rename/flatten, `PIVOT ... ON ... USING`, `ARG_MAX/MIN`,
  `QUALIFY`, `ASOF LEFT JOIN`, `json_group_array`/`json_object` envelopes.

## Sequencing (the anti-entanglement lesson)

Land lowering against STABLE inputs; mapping resolution comes after, separately:

1. **I+J skeleton**: SqlSelect/SqlSource/SqlExpr/SqlAgg IR + DuckDB renderer +
   golden flatness tests.
2. **Relation pipelines** (tableReference/TDS sources — no mappings needed):
   fold policy + per-construct rules + scalar dispatch. Port engine *CheckerTest
   SQL-string assertions.
3. **DuckDB execution harness** in tests; port the corpus execution assertions.
4. **Heavy constructs**: windows+frames, pivot(+cast idiom), joins/asOfJoin, folds,
   variant navigation, flatten.
5. **Phase H — StoreResolver**: class sources (`TypedGetAll`+mapping → table),
   association navigation joins, `navigate`/`legacyNavigate` (its deferred typing
   design lands here), `from` runtime binding, write/serialize envelopes.
6. Facade: `Compiler.compile(model, query, runtime)` becomes real.
