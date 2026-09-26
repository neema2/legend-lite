# Research line 4: the lowering half (design §1, §3.5, §3.6, §4; plan A2, C, E) against the code

Read-only audit, 2026-09-26. Paths under `core/src/main/java/com/legend/`.

## 1. What the lowering IS today
**F1. CONFIRMED — 158 bare registrations.** `grep -rn "nativeKeysAt(" core/src/main/java | grep -v Pure.java`
= 158 sites in 14 files. `Pure.nativeKeysAt(name)` (`builtin/Pure.java:698-746`) fans a bare name (or
exact FQN, `registeredAt` :815-818) to every overload's `signatureKey()`; index
`Index.REGISTERED_BY_BARE` (:600) / `KEYS_BY_NAME` (:602). Rules land in `Scalars.RULES : Map<String,Rule>`
(`lowering/Scalars.java:58`; 131 `RULES.put`/`family(` sites), `Aggregates.family(SqlAgg.Fn, "sum")`
(`Aggregates.java:36`), `Windows.fnKeys/aggregateKeys`, `FeatureRules.UNDER`. Plus 4 exact-overload
keys (`Pure.keyIn/keyInOptional/keyPlusString`).

**F2. REFUTED (partly) — lookup is already by the resolved overload.** `Scalars.lower` keys by
`call.callee().signatureKey()` (`Scalars.java:2550-2557`) = `TypedFunction.definition.signatureKey()`
(`compiler/element/TypedFunction.java:63-70`) = `qualifiedName(type:mult,…)` (`model/Function.java:51-64`).
Only registration is by bare name. TWO id spellings — `signatureKey()` and `FunctionId` — bridged by
a `catalogKey` map in `ImplementationTable.build` (~:55-60).

**F3. CONFIRMED — residual name-keyed dispatch inside the Lowerer.** `Lowerer.isFamily(n,"get"/
"equal"/"eq")` = `Pure.nativeNamed(bareName, signatureKey)` (`lowering/Lowerer.java:3406-3410`, sites
:2840, :2908) and `NativeFn.LowererForm.of(nc.callee().qualifiedName())` (:583) for
lateral/reduce/zScore/rowMapper/wavgRowMapper (`builtin/NativeFn.java:305-315`). These are the real
A2 targets.

**F4. REFUTED — the design's "Plan" is not new; the MIR is it.** `com.legend.sql` sealed roots:
`SqlExpr` ~45 variants (`sql/SqlExpr.java:270-1030`), `SqlExpr.Call(SqlFn fn, args)` with `SqlFn`
~200 ops (`sql/SqlFn.java`), `SqlAgg.Fn` ~35, `SqlSelect.SortKey.NullOrder` (`sql/SqlSelect.java:168`).
Null nodes: `IS_NULL, IS_NOT_NULL, COALESCE` (`SqlFn.java:18`), `IS_DISTINCT_FROM, NULL_SAFE_EQUAL,
NULL_SAFE_NOT_EQUAL` (:87-88). Missing: `NULLIF`. No `import com.legend.sql.dialect` in `lowering/`.

**F5. REFUTED — "`==` between two [0..1] is rendered as `is not distinct from`".** `equal`/`eq` lower
to bare `EQUAL` with a literal-empty ladder; comment "IS NOT DISTINCT FROM appears in no golden"
(`Scalars.java:83-88`, :92-130). `NULL_SAFE_EQUAL` only when BOTH operands are `SqlExpr.Column` AND both
`[0..1]` (`lowering/NullSemantics.java equalNullArms`), stripped back under `verbatim`/
`LEGACY_SQL_NULL_UNSAFE_EQUALS` (`Lowerer.java:1577, 2360, 3028, 3036`). The oracle is engine parity;
the engine emits bare `=`.

**F6. CONFIRMED with gaps — null semantics per use, not for aggregates.** `isEmpty` → `IS_NULL` or
`COALESCE(LIST_LENGTH,0)=0` (`Scalars.java:446-463`); `in` optional-needle → `COALESCE(in,false)`,
`in(x,[])` → `FALSE` (`Scalars.java:2390-2410`); `!=` arms (`NullSemantics.notEqualNullArms/
notEqualExpandedArms`); sort: Pure sorts stamp `NULLS LAST` asc / `NULLS FIRST` desc
(`lowering/Fold.java:386-410`, `Sorts.java:88-98`), engine TDS sorts bare with renderer nulls-low
(`sql/dialect/AnsiSqlRenderer.java:247-266`). `sum` is plain `SUM` (`Aggregates.java:36`), no COALESCE
(only `Lowerer.java:2977` in a TDS-cell context). Pure `[]->sum()` = 0 vs SQL NULL is NOT explicit.

**F7. PARTLY CONFIRMED — "rendering is data" ~half true.** `Spellings(Map<SqlFn,String>)`: 77 DUCKDB
rows, H2 = DUCKDB + 3 renames (`sql/dialect/Spellings.java:18-47`); `AnsiSqlRenderer.call()` 68 coded
arms (`AnsiSqlRenderer.java:649+`) with 27 `DialectCapability` throws; `H2.call` override
(`H2.java:191-350`); `EngineStyleH2` 94 arms (1,902 lines). Structural differences are MIR→MIR
`SqlRewriter` passes: H2 `LateralExplodeToUnion`, `H2AvgDelivers`, `SourceSpelling` (`H2.java:34-46`);
DuckDb `StableScanOrder, QualifyToSubselect, FoldToListReduce, CheckedDefectsToLists,
UnqualifyPivotArgs, SubstringClamp, QuantileOrder`. Only 1 UOE (EngineStyleH2); the real mechanism is
`DialectCapability extends IllegalStateException` at 34 render-time sites.

**F8. REFUTED — "generated from dynaFnToSql per dialect": 0%.** `builtin/DynaFn.java` = 232 rows
`(name, Resolution, fqns, Inference, Dialect…)`; Resolution PURE 159 / SHIM 9 / TRANSLATED 24 /
UNSUPPORTED 41. The `Dialect` column is MEMBERSHIP, never a spelling. Readers: `normalizer/
{RelOpTranslator,DynaFnArms,JoinChainEmission,GroupBySynthesis}` + `Pure.java` — zero in `lowering/`
or `sql/dialect/`. Every Spellings row is hand-written and H2-probed (`Spellings.java:21-31`).
Upstream's `dynaFnToSql` bodies are Pure functions (templates with arity logic).

## 2. Step A2
**F9. CONFIRMED — no `Pure.register*` API.** Registration = `Pure.nativeKeysAt(name[,arity|paramClassFqn])`
→ `RULES.put(key, rule)` (`Scalars.family` :69-78). A2 changes: (a) 158 fan-outs → explicit id lists;
(b) F3's two dispatches; (c) the `signatureKey` vs `FunctionId` split; (d) `Pure.nativeNamed`,
`Index.REGISTERED_BY_BARE/KEYS_BY_NAME` deleted.
**F10. CONFIRMED — many-to-one is the norm.** `family(SqlFn.LESS,"lessThan")` puts every overload on
one rule; `plus` = 5 overloads on one op with a String override; `StaticFold.FoldOp` groups 1–7 fqns
per op (`compiler/spec/StaticFold.java:403-428`); `DynaFn.AND` carries 2 fqns. `isVariadicRun` used
only by `normalizer/RelOpTranslator.java:727`. Expect ~500 id rows; a new catalog overload with no
rule becomes `Unimplemented` (loud, good); probe diff before the switch.

## 3. Step C vs "the database executes"
**F11. StaticFold is PRE-typing and scope-gated.** Walks untyped `ValueSpecification/AppliedFunction`
(`StaticFold.java:84-130`); invoked at two sites: inside a normalise-required body after substitution
(`Typer.java:1699`, gated by `requiresNormalization` :1600-1608) and on `.columns->map(...)`
(`Typer.java:722`). Folds PLUS on Longs/Strings, MINUS, IF, AND/OR, IN, CONTAINS, MAP-unroll, FILTER,
SORT_BY, JOIN_STRINGS… (:464-700). The plan's evaluator is POST-typing — different input; new code.
**F12. REFUTED — StaticFold is not in JavaEvalLedgerTest.** The ledger registers `LiteralFold`
(`core/src/main/java/com/legend/LiteralFold.java`: bare String/Boolean literal only; Integer/Float/Date
folding explicitly REFUSED; pinned by ConstantPlanParityTest).
**F13. The line.** Plan-time evaluation may compute only what determines the SHAPE of the SQL:
column lists/names, colspec sets, which `if`/`match` branch, which lambda body, unrolling `map` over
a static collection, type tokens, literal-empty detection. Never a value that becomes a SQL literal
SQL could compute — except where the value becomes an identifier (a column name in an NR body).
StaticFold crosses it marginally (`1+2` inside a filter lambda of an NR function becomes `3`,
`StaticFold.java:95-115`). MUST precede lowering: NR inlining, `.columns` folding, static `if` on
schema, colspec literalisation, `UserCallInliner` β (SQL has no call frame), `StatementInline`.

## 4. Dialect facts
**F14. H2 vs DuckDB differences are real and shape-dependent.** H2 lacks LATERAL →
`LateralExplodeToUnion` (183 lines, per-row LITERAL collections only), lacks QUALIFY →
`QualifyToSubselect`, AVG is DECFLOAT → `H2AvgDelivers`, absent functions deliberately unmapped
(`Spellings.java:21-31`); `needsStaticPivot()` read by `exec/DynamicPivot.java:43`; `rawH2IsNative()`
by `StatementExecutor.java:2970`; `lowering/GraphAggDecorrelate.java` (215 lines) dialect-independent.
**F15. UNPROVEN — "a rule a dialect lacks is a lowering error before SQL".** Capability failure is
render-time (`DialectCapability`, 34 sites: Ansi 27, H2 6, CarrierStrategies 1). The registry has NO
dialect axis: `platform/Implementation.java` `Intrinsic(positions, featureOverrides, families)`.
Achievable only as declared data per (SqlFn|structural node) × dialect consulted by a post-lowering
MIR walk — not a column on the declaration row (a rule can emit different SqlFns per argument shape).

## 5. Step E — the ownership mechanisms
| Mechanism | Where / size | Readers | Ownership row? |
|---|---|---|---|
| walled bodies | `platform/WalledBodies.java` (117 L, 8) | Registrations, PlatformRegistrations, SpecCompiler, UserCallInliner | YES → `Refused` (already) |
| walled natives | `Pure.WALLED_NATIVES` (`Pure.java:1585`, 6) | PlatformRegistrations | YES → `Refused` (already) |
| lite surface | `Pure.LITE_SURFACE` (`Pure.java:559-563`, 4) | `Pure.Index` only | NO — bind/visibility fact |
| lite internal set | `Pure.Lite` constants (:381+), `liteInternalNatives()` :567 | normalizer/lowering emitters | NO — namespace partition |
| legacy TDS vocabulary | `builtin/TdsLegacy.java` (68 L) | CoreFn, Typer, checkers, TdsErasure | NO — typing/desugar fact → `Form` rules |
| subsumed registry | `builtin/Subsumed.java` (2) | StatementExecutor, PlatformRegistrations, SeededStores, StoreEscapees | YES (already) → `Refused(MOOT)` |
| handler surface column | `engine-handlers.tsv` (838 rows: 830 engine + 6 lite; 169 empty fqn) | `compiler/BareNames`, `builtin/EngineHandlers` | NO — BIND fact; fqn column = declaration existence |
| family members | `NativeFn.families()` (`NativeFn.java:84-96`) | PlatformRegistrations → `Intrinsic.families`; `LowererForm.of(qualifiedName)` :583 | YES — a 5th lowering POSITION; the rule lives in the enum |
| dynafunction column | `DynaFn.Resolution` (232) | normalizer only | NO — translator rename table keyed by engine OPERATOR |
| claims ledger | `spec/src/test/java/com/legend/claims/{Claims,ClaimsGenerator}.java`; stale javadoc `Scalars.java:60` | tests | delete |

**F16. The table exists and its kinds match** (`spec/src/test/java/com/legend/generators/
ImplementationTableTest.java:109`: Intrinsic 664, Form 217, Refused 20, Body 2194, Unimplemented 71)
but is DERIVED from the rule maps (`lowering/PlatformRegistrations.java:16-22` reads
`RegistryKeys.scalarRules()` = `Scalars.ruleKeys()`). §4's "the row IS the rule" inverts ownership:
131+ lambdas into row values; `NoRule.explain` (`lowering/NoRule.java`) already reads the table.

## Recommended registry shape
1. Key: `FunctionId`, one row per declaration; retire `signatureKey` as a dispatch key.
2. Intrinsic row value: `Map<Position, RuleRef>`, Position ∈ {SCALAR, AGGREGATE, WINDOW,
   WINDOW_AGGREGATE, FORM(family)}; `RuleRef` a named, shared rule object (many ids → one rule).
3. Rules declared as `rule(RuleRef, FunctionId…)` lists — no bare names; a new overload with no rule
   is `Unimplemented`, loud.
4. Feature overrides stay a `(FunctionId, Feature) → RuleRef` overlay.
5. Rules emit the EXISTING MIR; add `NULLIF`; no new Plan IR.
6. Dialect capability = `SqlDialect.supports(SqlFn|StructuralNode)`, checked by one MIR walk after
   lowering, before render.
7. Renderings stay `Spellings` rows + coded arms; a `dynaFnToSql` extractor is a separate later job.
8. Family enums become rows with `Position.FORM`, dispatched by id; delete `LowererForm.of(name)`,
   `isFamily`.
9. Bind facts (handler surface, LITE_SURFACE, lite package) go to the DeclarationTable/World.
10. Pin: table kinds exact; rules-per-id count; zero `nativeNamed`/`nativeKeysAt` readers.

## Top 5 SQL-side risks
1. A new Plan IR beside the MIR (F4).
2. Null-semantics "made explicit" vs engine parity (F5, F6).
3. Evaluator scope creep (F11–F13).
4. Dialect capability as a declaration column (F15).
5. Identity split (F2): delete `signatureKey` dispatch in the same commit as A2.
