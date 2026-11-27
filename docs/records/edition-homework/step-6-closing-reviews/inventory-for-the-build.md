# The inventory for the build (of `268591b9c`): guards, conditions and equality matchers, lowerers with no planner, the order rule's callers

Read-only, by an independent reviewer (a subagent given the design and the rules, never the author's
conclusions), 2026-11-27. Kept verbatim as evidence; what was done with each finding is in
`step-6-closing-design-2026-11-27.md` (sections 5 and 6) and `step-6-closing-batch-2026-11-27.md`. Paths the report
writes under `/home/user/ll-audit...` are a read-only worktree of this repository.

---

All three inventories are below, measured at 268591b9c in /home/user/ll-audit3. Nothing was edited and no Bazel was run. Line counts come from Python ports of each guard's own counting rule.

# Inventory 1: guards the build will trip

Paths are relative to `core/src/test/java/com/legend/` unless they start with `core/` or `pct/`.

## Pins with under 10 lines or 1 entry of headroom (flagged)

| Guard (file:line) | What it pins | Pinned | Measured now | Headroom |
|---|---|---|---|---|
| `JavaEvalLedgerTest.javaEvaluationSurfaceOnlyShrinks`, `EVICT_SIZE` (JavaEvalLedgerTest.java:858) | stripped code lines of `planner/BodyPlanner.java` | 316 | 316 | **0, exact in both directions** (a shrink fails too, "re-pin") |
| `CodeShapeGuardrailTest.noMethodBeyondTheLimit` (:37, :757) | method length, limit 250, `METHOD_ALLOWLIST` empty | 250 | `Typer.applyFunction` 250 (Typer.java:623) | **0** |
| same | | 250 | `Lowerer.scalarStructural` 249 (Lowerer.java:3976) | **1** |
| same | | 250 | `Lowerer.scalarValueTailArms` 241 (:4352) | **9** |
| `CodeShapeGuardrailTest.deadPrivateMethodsOnlyShrink` (:553) | unreferenced private methods | 0 | 0 | **0**: a private method left unused fails |
| `ErrorShapeGuardrailTest` `BROAD_CATCH_COUNTS` (:45, default 0 at :322) | `catch (Exception/RuntimeException/Throwable)` per file | 0 for every target file (none is listed) | 0 in all 17 | **0** |
| `NativeCatalogGovernanceTest.theVerifiedSetsOnlyShrink` (builtin/…:198, :219, :244) | `Pure.INTERNAL_DESUGAR` / `ENGINE_VOCAB_SHIMS` / `LITE_SURFACE` | ≤40 / ≤12 / ≤10 | 40 / 12 / 10 | **0 each** (only matters if a native is added) |
| `SqlFnSortedTest.theSortIsMeasured` with `BOTH_WRITERS` (:197–440) | exact set of product writer classes per SqlFn | see the table below | exact | **0 entries**: a class newly referencing `SqlFn.X` (a comparison `== SqlFn.X` counts as a write; an anonymous class counts as its top class) fails |
| `ArchitectureTest.thePlannerEntriesAreTheMeasuredOnesExactly` (:1636) and `thePlannersAreReachedByTheirListedEntriesAlone` (:1161, rule :1662) | every access from outside planner/testplan/lowering into those packages, by full signature and caller | exact | exact | **0**: any new outside access needs a row; a row nobody reaches must go |
| `ArchitectureTest.anEditionsTableAloneNamesARule` (:3593, rule :3598) | any access to a `lowering.NamedRule` member outside NamedRule, NamedRules(\$…) and package `edge` | 0 | 0 | **0**: applies to Lowerer, Scalars, Equalities, EditionTable, HeldKinds and RootComparison |
| `CompatSpellingGuardTest` (:291, :429, :432) and `TerminalRowsFrozenTest` (:96, :105, :106) | `Scalars "+0000"` 1, `Scalars TDSNull` 2, `Typer TDSNull` 4 | exact | exact | 0 (only if those literals move) |
| `RaiseTextGuardTest` (:51) / `RawSqlLedgerTest` (:123) | `lowering/Scalars.java` 1 / `Lowerer.java` RawSql constructions 1 | 1 / 1 | not re-measured | not determined |

### SqlFn writer sets for the edited classes (`BOTH_WRITERS`)

Each row lists which edited classes are in that function's product-writer set. Any edited class not listed must be added before it may reference `SqlFn.X` for that function. RootComparison, ProductStatements, BodyPlanner, HeldKinds and EditionTable are in **no** set.

| SqlFn (line) | Edited classes in the set |
|---|---|
| COALESCE (:239) | Scalars, Equalities only (not Lowerer, SideEqualities, HeldKinds, RootComparison) |
| EQUAL (:276) | Lowerer, Scalars, Equalities, SideEqualities, NamedRules, NullSemantics, RelationPredicates |
| AND (:216) | Lowerer, Scalars, SideEqualities (not Equalities) |
| NOT (:394) | Lowerer, Scalars, SideEqualities (not Equalities) |
| OR (:410) | Scalars, SideEqualities |
| IS_NULL (:345) | Scalars, Equalities, SideEqualities (not Lowerer) |
| IS_NOT_NULL (:334) | Scalars, SideEqualities (not Lowerer, Equalities) |
| NOT_EQUAL (:405) | Scalars, NullSemantics |

NULL_SAFE_EQUAL, NULL_SAFE_NOT_EQUAL and IS_DISTINCT_FROM are not in `BOTH_WRITERS`, so product classes may reference them freely.

## Other pins on the named classes (headroom of 10 or more)

| Guard (file:line) | Item | Measured / limit |
|---|---|---|
| `noMethodBeyondTheLimit` | `Lowerer.scalarInner` (Lowerer.java:3739) | 233 / 250 (17) |
| | `Lowerer.lower(List)` (:585) | 23 |
| | `Lowerer.lower(TypedSpec)` (:870) | 79 |
| | `Scalars.lower` (Scalars.java:2391) | 49 |
| | `Lowerer.aggValue` | 233 |
| | `UserCallInliner.literalArms` | 233 |
| | `Typer.accessProperty` | 222 |
| | `UserCallInliner.rewriteSwitch` | 215 |
| | `Typer.synth` | 210 |
| | `OrderView.of` | 151 |
| | `BodyPlanner.prepare` | 118 |
| | `Equalities.equal` | 97 |

**`RootComparison.plan` is 80 lines (RootComparison.java:53–132) but the guard does not scan it.** The guard's signature regex (`SIG`, :466) excludes `@`, so `static @Nullable SqlQuery plan(` is never matched. The same blind spot hides `SideEqualities.kindedRows` (154 lines) and every other `@Nullable`-returning method.

| Guard (file:line) | What it pins for the edited classes |
|---|---|
| `CodeShapeGuardrailTest.mutableInstanceStateIsExplicit`, `MUTABLE_FIELD_ALLOWLIST` (:52–440) | Already lists `Lowerer.wholeComparisons` (:376) and many other `Lowerer.*` fields, plus `Typer.compared`, `Typer.comparedIn`, `UserCallInliner.spent/fresh/configMode/instantiations/lambdaDepth/frame`. It scans only 4-space fields with no `static`/`final`, and 8-space nested fields with a visibility keyword. |
| `JavaEvalLedgerTest.theFunnelPackagesAreClosedRegisters` (:2494) | `lowering` is **not** a register, so a new lowering file is free. The planner register `PLANNER_CLASSES` (:1558) lists BodyPlanner (:1632), ProductStatements (:1668) and RootComparison (:1674); a new planner `.java` file needs a row. The edge register lists EngineTable.java (:1967). |
| `ArchitectureTest.stageFunnelOnlyTheDriverDrivesResolverLoweringExec` (:1734) | `compiler..` must not depend on `lowering..`. So OrderView, EqualityClasses, LiteCrossTypeRule, Typer and UserCallInliner cannot reference `lowering.WholeComparisons` or any other lowering type. |
| `ArchitectureTest.loweringDependencySurfaceIsPinned` (:1796) | Lowering may depend only on: lowering, compiler.spec.typed, compiler.element, compiler.element.type, builtin, sql.., values, platform, error, java..; plus SourceInfo, model.FunctionId, base.RunScope, base.Sealed, base.Attribution and the nullness annotations. It may not use planner, compiler.spec (non-typed), or model types other than FunctionId. |
| `ArchitectureTest.loweringDoesNotTouchTheParserAst` (:279), `postNormalizationPhasesAreParserFree` (:338) | Lowering must not depend on parser, lexer, normalizer or ide. |
| `ArchitectureTest` `PLANNER_ENTRIES` rows already present | `Lowerer#<init>`, `#lower(List)`, `#withExistsJoinable`, `#withStyled`, `#withFloatLiteralKind` (callers Compiler and EnginePlans, :1400–1468); `#withInstanceKeys`, `#withObjectIdentity` (Compiler); `#withFeatures`, `#withOptions`, `#bindPlanParam` (EnginePlans); 14 `NamedRule#…` constants (EngineTable and UpstreamTies, :1420–1446). There is **no** row for `Lowerer#withWholeComparisons`. |
| `ProductStatementsTest.theLoweringsStepsArePackagePrivate…` (planner/ProductStatementsTest.java:166) | `ProductStatements.planned/lower/lowerer/configured/storeWrite` and `BodyPlanner.lowerAndPrepare/lowererFor/prepare/bare` must stay non-public. |
| `EdgeOnlyMembersTest` (OPEN / READ_API, exact) | A public product member that only the edge uses (for example a new EditionTable or lowering method called only by EngineTable) must be pinned. |
| `HookLambdasTest` | `LAMBDA_HOOKS` (:44) pins `lowering.Styled`. `CLASS_HOOKS` pins `planner.LiteEdition` → `planner.Edition` (:120). `TWO_FORM_CALLERS` pins `PlanEnv#edition()`. |
| `EdgeSeparationTest` | Hook-ruled edges `BodyPlanner → UpstreamPlans` (:132), `Lowerer → ResultEnvelopes` (:150), `Scalars → PureOnlyTexts` (:162). |
| `CompatConstantPoolTest` (:73–74) | `Lowerer \| TDSNull`, `Scalars \| TDSNull`. |
| `ParkedWorkLedgerTest` (:93, :96, :181) | Anchor regexes that must still be found in Typer.java. |
| `ArchitectureTest` (:4727–4730) | Only listed classes may make a temporal literal; Scalars is listed. |
| `IdentityGuardrailTest` `PINS` (:134–166), `PlatformNamesGuardrailTest` `BARE_NAME_ARMS_MAX`=188 (:189), `TenetRatchetTest` (:87) | Global ceilings over all main sources, not per class. **Current counts not measured.** |
| `pct/.../PctDisciplineTest.java` | Scans PCT sources only (`ADAPTER_MAX_LINES`=626). It names none of these classes. |

## Rules asked about by name

- **Which lowering types the planner may depend on:** I found **no** such allowlist in any test. The nearest rules are `PLANNER_ENTRIES_ONLY` (outside callers only), `loweringDependencySurfaceIsPinned`, `plannerIsExecutionFree` (ArchitectureTest.java:564) and the Bazel layer pin `tools/deps/core-layers.txt:99`. That pin gives `lowering`'s deps as: base builtin compiler compiler_element_type error model platform protocol sql values.
- **Anonymous classes:** no rule bans them.
  - `OpenClassMembersTest` pins anonymous classes only for its listed compat classes; RootComparison and Lowerer are not among them.
  - ArchUnit and the SqlFn census count nested and anonymous classes as their top-level class.
- **Public types added to lowering:** no count or allowlist exists.
- **Mutable fields and statics:** `staticFieldsAreFinal` (:4164) and `staticStateIsConstantOrRegistered` (:4278); a lambda constant is accepted.

**Would `lowering/WholeComparisons` plus an anonymous implementation in `planner/RootComparison` pass as written?**

Yes, under these conditions:

1. Its signatures use only allowed types, such as `TypedNativeCall`, `SqlQuery` and `@Nullable`.
2. Only planner and lowering classes reach it. Planner is inside "the planners", so no `PLANNER_ENTRIES` row is needed.
3. No compiler.spec class references it.
4. The anonymous class sits inside a method body, so its fields and methods are at 12 or more spaces and are scanned by neither the length rule nor the mutable-field rule.

It fails in these cases:

- **Compiler or EnginePlans calls `withWholeComparisons`:** a new `PLANNER_ENTRIES` row is needed, and the exact-match test enforces both directions.
- **The anonymous class references a `BOTH_WRITERS` function it is not listed for (for example COALESCE):** a `SqlFnSortedTest` row is needed, counted under `planner.RootComparison`.
- **The interface declares a constant without spelling `static final` (for example `WholeComparisons NONE = c -> null;`):** the 4-space `MUTABLE_FIELD` regex flags it, as the `TokenStreamCursor.IDENTIFIER_TOKENS` allowlist precedent shows (CodeShapeGuardrailTest.java:59).
- **The `Lowerer` field becomes a one-line declaration:** it would be scanned, but `Lowerer.wholeComparisons` is already allowlisted.

# Inventory 2: conditions, and what matches an equality's shape

## (a) Where a Boolean becomes a predicate rather than a kept value

**Only two sites mark a value as kept.** Both go through `keepingItsValue` (Lowerer.java:367) and `markKept` (:378):
- the scalar root, `scalarRoot` (:963);
- a computed column's lambda, `tryComputedColumns` (:2218).

`markKept` descends through `&&`, `||`, `!` and an `if`'s **branches**. It does not descend into an `if`'s condition, a lambda, or a `TypedRunTimeComparison`/`TypedCrossTypeComparison` wrapper. `scalar()` (:3699) then applies `Equalities.falseWhereASideIsEmpty` (Equalities.java:61) to marked nodes only. Every site below therefore lowers as a condition, unmarked.

| Position | Method (Lowerer.java unless noted) | Call used |
|---|---|---|
| WHERE / HAVING / QUALIFY of a filter | `filter` :2265 → `tryPredicate` :2416, `tryWindowPredicate` :2439, `predicateOrThrow` :2401; slot switch :2298–2308 (`mergeWhere` / `withHaving` / `withQualify`) | `scalar(LambdaBinding.last(lambda), …)` |
| JOIN ON | `join` :3000 → `sideCondition` :3031, :3036 | `scalar(LambdaBinding.last(lambda), …)` :3313 |
| As-of join match and condition | `asOfJoin` :3054 → `sideCondition` :3057, :3059; then `oneRowPerMatch` :3082 builds its own `WHERE … AND …` / `= 1` | `scalar` |
| Relation exists / forAll / isEmpty | `RelationPredicates` :175, :325, :329 → `whereLambda` :4966 → `tryPredicate` / `predicateOrThrow` | `scalar` |
| CASE WHEN for an `if` | `scalarInner` arm `case TypedIf` :4180–4190 | `scalar(i.condition(), columns)` |
| List filter | `scalarRelationalArms` `case TypedFilter` (non-relation) :4246–4261, or `TaggedLists.filter` | lambda via `case TypedLambda l` :4122, which calls `scalar(LambdaBinding.last(l), …)` |
| List exists / forAll | Scalars.java:459–472 (`LIST_EXISTS`/`LIST_FOR_ALL`, or `TaggedLists.exists/forAll`) | args already lowered; the lambda through :4122 |
| List find | Scalars.java:1975–1978 (`LIST_GET(LIST_FILTER…)`) | same |
| contains with a comparator | Scalars.java:3660–3672 | substituted lambda body |
| Delete / update predicate | `overRow` :711 (from :653, :665) | `tryPredicate` → `scalar` |
| Assert condition (database judge) | testplan/DatabaseJudge.java:428 → planner/SidePlanner.java:93 `over` → `Lowerer.over` :3687 | `scalar(condition, bound)`, unmarked |
| Test-body `if` holding asserts | testplan/AssertBranch.java:54–66; DatabaseAsserts.java:223 and HostAsserts.java:272 → `JudgePlans.plannedSide` (JudgePlans.java:224) | The condition is planned as a **side statement**, so plausibly `scalarRoot` and therefore **kept**. Inferred from the call chain, not traced to the end. |
| Aggregate FILTER | none exists: `SqlAgg.Reducer` (SqlAgg.java:67) has no filter field | n/a |
| Whole sides `==` / `hasSameElements` inside an expression | `wholeSides` :410, arms :4418–4430 | a ScalarSubquery, which `falseWhereASideIsEmpty` leaves alone because it is not an EQUAL call |

## (b) Code that inspects a lowered expression for being an equality

The last column assumes the equality arrives as `COALESCE(a = b, FALSE)`. None of these sites looks inside COALESCE.

| Site (file:line) | What it does today | If wrapped in `COALESCE(…, FALSE)` |
|---|---|---|
| lowering/Equalities.java:62 `falseWhereASideIsEmpty` | wraps only a plain `EQUAL` with exactly one optional side | idempotent guard: an already wrapped value is not an EQUAL, so it is not re-wrapped |
| lowering/NullSemantics.java:95 `verbatim` | `NULL_SAFE_EQUAL` becomes plain `=` for mapping and correlation joins | runs on `scalarInner`'s result before `scalar()` wraps, so it is unaffected; a wrapped value would not be rewritten |
| lowering/NullSemantics.java:154–165 `negate` (called from the NOT rule, Scalars.java:202) | `!(a=b)` gets the engine's null arms; `!NULL_SAFE_EQUAL` flips to `NULL_SAFE_NOT_EQUAL`; `!IN` gets `OR IS NULL` | falls to the default, so it becomes bare `NOT(COALESCE(a=b,FALSE))`. This already happens today for a kept `!(x == v)`, because the arg passes through `scalar()` before the rule. |
| lowering/KeyedCounts.java:211 | WHERE and inner-ON column=column equalities merge key equivalence classes | the pair is not seen: fewer keys, so `COUNT(*) OVER` is not folded to 1 |
| lowering/KeyedCounts.java:494 | WHERE `col = literal` narrows a row key | not narrowed, so not folded |
| lowering/KeyedCounts.java:560 | join ON equalities bind the key across sides | not bound, so not folded |
| lowering/GraphAggDecorrelate.java:91 | correlated `outer = inner` column equalities in WHERE become decorrelation keys | the conjunct is not a key; it stays an outside reference, and decorrelation keeps it or abandons |
| lowering/Lowerer.java:3108 `oneRowPerMatch` | as-of ON: an EQUAL / NULL_SAFE_EQUAL or ordering conjunct names the rank keys | returns `right` unchanged, so no deterministic one-row-per-match |
| lowering/EnumSourceValues.java:101–104 `sameDecodeTable` | two CASE decode tables with `src = lit` WHENs are recognised as the same | not the same, so no sharing |
| sql/ExistsJoins.java:195 `corrPair` | an `outer = inner` equality makes EXISTS rewritable to a LEFT JOIN on distinct keys | not a key: the correlated EXISTS is kept (its stated fallback) |
| sql/DecodeShapes.java:72, :114 | `CASE WHEN src = 'A' …` (or an OR of them) is read as a decode table | not a decode, so the CASE stays as written |
| sql/EnumValueTables.java:43, :51 | `decode = $enumParam` / `<>` becomes a value-table read | `e` is returned untouched, so the enum PlanParam stays as it is |
| sql/dialect/OuterJoinResidualsAsMatches.java:81 (used by ClickHouse) | a LEFT/RIGHT/FULL ON conjunct reading both sides that is not EQUAL counts as a residual and triggers the re-join rewrite (which itself writes NULL_SAFE_EQUAL, :108) | treated as a residual, so the rewrite fires |
| sql/dialect/DuckDb.java:45, :67 `joinCondition` | drops `IS NOT NULL x` when x is an operand of an EQUAL or other comparison in an outer join's ON | the comparison is unseen, so the IS NOT NULL is kept |
| sql/dialect/ClickHouse.java:295 | an ASOF ON with no EQUAL gets a synthetic hash equality | a synthetic key is added |
| sql/dialect/ClickHouse.java:396–407 | an ON made only of EQUAL / NULL_SAFE_EQUAL keys is written as `isNotDistinctFrom` keys | not all keys, so it takes the other path |
| sql/dialect/ClickHouse.java:614–623 `nullRejecting` | a comparison of outer columns counts as null-rejecting | not null-rejecting |
| sql/dialect/Snowflake.java:3266 | ASOF ON must be equalities plus one inequality | throws `DialectCapability` |
| sql/dialect/CarrierStrategies.java:536 (`asOfByWindow`, :481) | ON conjuncts: EQUAL gives key pairs, orderings give the match | `return null`, so the window emulation is not used |
| sql/dialect/CarrierStrategies.java:212, :1233 | internal idioms (dedup `LIST_POSITION = i`; the `typeof = 'DATE'` test) | n/a: these are the lowering's own constructions, not user equalities |
| sql/dialect/H2.java:1510 `booleanShaped` (used at :848 bool = text, :1412 cast to VARCHAR) | EQUAL, NULL_SAFE_EQUAL and the other comparisons count as Boolean-shaped | COALESCE is not, so `CAST(... AS VARCHAR)` loses the `CASE WHEN … THEN 'true'` spelling unless the type fact says BOOLEAN |
| Render arms that read `c.fn()` (MariaDb.java:2142, SQLite.java:1919, ClickHouse.java:2150, BigQuery.java:1874, :1882, Trino.java:927) | spelling of `=` and `<>` over composites | only the inner EQUAL is affected; the COALESCE renders normally |

## (c) How each dialect spells the null-safe comparisons

None of them throws for NULL_SAFE_EQUAL or NULL_SAFE_NOT_EQUAL. AnsiSqlRenderer's `nullSafe` (:1382) throws only for a function that is not null-safe.

| Dialect | NULL_SAFE_EQUAL | NULL_SAFE_NOT_EQUAL |
|---|---|---|
| DuckDb (:615), H2 (:698), Postgres (:807), SQLite (:1702), Snowflake (:1400), Databricks (:1078), Trino (:937) | `(a IS NOT DISTINCT FROM b)` via `nullSafe` | `(a IS DISTINCT FROM b)` |
| BigQuery (:1579, `nullSafeArm` :1881) | as ANSI; a composite side becomes `TO_JSON_STRING(a) = TO_JSON_STRING(b)` | `<>` over the same |
| MariaDb (:1951–1952), and MySql through `super` (MySql.java:124) | `(a <=> b)` | `(NOT (a <=> b))` |
| ClickHouse (:2009, :2013) | `coalesce(a = b, isNull(a) AND isNull(b))`; as a join key, `isNotDistinctFrom(a, b)` (:406) | `NOT coalesce(...)` |
| MongoDb (:1044, :1046) | `$eq` | `$ne` |

COALESCE has an arm in every dialect (DuckDb :652, H2 :730, Postgres :850, SQLite :1774, MariaDb :1972, ClickHouse :1977, Databricks :1143, BigQuery :1616, Snowflake :1454, Trino :993, MongoDb :1066).

## (d) The five targets the first empty-side attempt broke

Quote from docs/records/edition-homework/step-6-one-equality-2026-11-27.md:314–323:

> "**The empty-side fix, backed out (2026-11-27).** The user's condition was "fix for lite if small". It was built as a guard written by the comparison itself (`x IS NOT NULL AND x = v` wherever exactly one side may be empty), and the run of `261ee728d` showed it is not small: the same comparison is also a filter's, a join's and an `exists`'s condition, a `CASE WHEN`'s test, an enumeration mapping's decode and a union's route-key join, and the guard appeared in all of them (five targets failed: `JoinTortureTest`, `LowerRelationDuckDbTest`, an enumeration mapping's SQL, `UnionTargetLeanJoinTest.oneKeyPerArm`, a planner test). Stripping it again from a filter and a join (`d39d70540`) covers two of those places, not the rest. So the guard and the stripping are both removed; …"

The commit (`git show 261ee728d`) wrapped the guard in `SqlExpr.Group(AND(IS_NOT_NULL x, x = v))` inside `Equalities.equal`, so it applied in every position. Because of the Group node, every `instanceof Call && fn() == EQUAL` matcher in (b) stopped matching.

| Target | Code it most plausibly exercised |
|---|---|
| `lowering/JoinTortureTest` | `T_PERSON.FIRM` is nullable. A JOIN ON golden (JoinTortureTest.java:201–203, `ON t0.FIRM = t1.F_NAME`) goes through `Lowerer.join` → `sideCondition`. Also ExistsJoins `corrPair` and KeyedCounts. |
| `lowering/LowerRelationDuckDbTest` | Same nullable FIRM. A filter `$x.FIRM == 'ACME'` (:360) goes through `filter` → `tryPredicate` → WHERE. Join goldens (:630–659) go through `sideCondition`. |
| "an enumeration mapping's SQL" (test not named) | `sql/DecodeShapes` `conditionSource`/`codes` (:72, :114) or `EnumSourceValues.sameDecodeTable` (:101); possibly `EnumValueTables` (:43). **Which test this was: not determined.** |
| `integration/UnionTargetLeanJoinTest.oneKeyPerArm` (:120–128) | It asserts one ` ON `, no `OR`, no `coalesce`, and an ON matching `t0.ID = t\d+.__route0_0`: the resolver's union route-key join (`sideCondition` under verbatim equality). It would fail on the guard text directly, and would also fail any COALESCE wrap in an ON. It is built through the test helper `resolver/ResolvedSql` (a direct `new Lowerer`). |
| "a planner test" (not named) | **Not determined.** |

# Inventory 3: lowerers made with no planner, and the order rule's callers

## (a) Every construction of a Lowerer in main sources

| Site | Attaches `withWholeComparisons`? | Path it serves |
|---|---|---|
| core/src/main/java/com/legend/Compiler.java:962 (`lowerResolved`) | **no** | Lowering of an already-resolved query. Its main caller is planner/PlanAllocations.java:399, which builds column-lineage rows for a plan's activity (`ScanColumns`). Tests call it too. |
| core/src/main/java/com/legend/edge/EnginePlans.java:167 (`engineSql`) | **no** | legend-engine's plan text and toSQLString (EnginePlans :99; EngineAllocations.java:134). |
| core/src/main/java/com/legend/lowering/SeedableLets.java:39 | **no** | A probe asking "can this let seed?"; its tree is discarded. |
| core/src/main/java/com/legend/planner/ProductStatements.java:483 (`configured`) | not by itself | Two callers: `ProductStatements.lowerer` (:460–468) attaches it (`RootComparison.inside`); that is the product road, used by PlanMaker.java:596 and :632. **PlanMaker.java:954 (`count`) uses `configured` directly with no attachment**: a write's count statement, `lowerPlanParam` only. |
| core/src/main/java/com/legend/planner/BodyPlanner.java:154 (`lowererFor`) | **yes**, through `RootComparison.asking` (:178; RootComparison.java:213–219) | Test-body statements (`lowerAndPrepare`), the assert condition (`SidePlanner.over`, :93), and each whole side RootComparison lowers. |

Outside main: 9 test files construct a Lowerer directly, and none attaches it. They are server/PlantedLowering, lowering/LowerRelationDuckDbTest, lowering/ResultEnvelopeRefusalTest, lowering/JoinTortureTest, lowering/LowererLetScopeTest, resolver/ResolveNavigationTest, resolver/ResolveM2mTest, resolver/ResolvedSql and PipelineStageFailureTest. No construction was found in other modules' main sources (wasm, native, datacube).

## (b) Callers of `OrderView.of` and of `OrderedAsserts` in main sources

| Caller | Call | What it does with the answer |
|---|---|---|
| compiler/spec/OrderedAsserts.java:117 `unorderedRows` | `OrderView.of` | INCIDENTAL with rows means "no defined order" |
| testplan/HostJudge.java:335 | `OrderView.of` | INCIDENTAL actual makes a JSON judgment order-free |
| testplan/HostJudge.java:349 | `OrderView.of` | SORTED makes a rendered-form compare ordered (otherwise a multiset) |
| testplan/DatabaseJudge.java:1061 | `OrderView.of` | INCIDENTAL means a JSON golden's root elements are compared unordered |
| testplan/AssertVerdicts.java:1794 | `OrderView.of` | `makeString`/`joinStrings` over INCIDENTAL becomes `CSVJOIN:sep` (order-free) |
| testplan/AssertVerdicts.java:1662 | `ORDER_PRESERVING` set | wrapper detection |
| edge/ReplayGates.java:23 | `OrderView.of` | SORTED sets `ordered` and collects the sort keys; also uses `EXECUTE_FRAMES` (:86) and `ORDER_PRESERVING` (:91) |
| edge/UpstreamTestBodies.java:348 | `OrderView.of` | a String list that is INCIDENTAL under a join is rewritten order-free |
| edge/UpstreamPrints.java:265 | `OrderView.of` | INCIDENTAL printed relation means no row order |
| planner/RootComparison.java:312 `refuseUnordered` | `OrderedAsserts.unorderedSide` | ≥0 raises a TYPE error for `==` over a side with no order |
| testplan/TestPlanner.java:250 | `OrderedAsserts.refuse(lifted)` | refuses order-dependent assertEquals/assertNotEquals in a test body |
| testplan/BodyDriver.java:326 | `OrderedAsserts.rewrite(…, ::refused)` | the same refusal for an assert inside a helper |
| edge/UpstreamTestBodies.java:78 | `OrderedAsserts.rewrite(…, lambda)` | the engine door rewrites those asserts to assertSameElements twins or asRows |

`VerdictArm` and `HostJudgment` mention OrderedAsserts in comments only.

## (c) What reaches OrderView's DEFINED fallbacks

`OrderView.of` has **two** DEFINED fallbacks:
- inside its TypedNativeCall branch, for any native in none of the sets (OrderView.java, the `return OrderView.DEFINED` after `ORDER_COMBINING`);
- the final return.

**Typed node kinds that reach the final DEFINED and can carry many values or rows:** TypedFrameRef, TypedPlannedValue, TypedTableSwap, TypedViewRelation (a store view's rows), TypedSourceUrl, TypedFlatten, TypedCollectionRelation, TypedRecurse (a TypedRelationOp not in any arm), TypedIf, TypedMatch, TypedMatchRuntime, TypedEval, TypedUserCall (before inlining), TypedLet, TypedCollection (a literal list, which may hold store-derived elements), TypedFold, TypedTds, TypedJsonAccess, TypedJsonResult, TypedJoinSlot, TypedCast.

Correction: TypedCast is in fact handled (it descends to its source). It should not be in this list.

**Native count** (parsed from builtin/Pure.java: 915 `signature(...)` constants, first parameter `[*]`, `[1..*]`, `[0..*]`, `[m]`, a Relation or a TDS):
- 183 such native FQNs in total;
- **143 of them in none of** SORT_FQNS, ORDER_DESTROYING, ORDER_COMBINING, ORDER_PRESERVING or EXECUTE_FRAMES (sets matched by FQN).

The twenty of those most likely to be applied to store rows in a real query, by my judgement:
1. `collection::removeDuplicates`
2. `collection::removeDuplicatesBy`
3. `collection::reverse`
4. `collection::tail`
5. `collection::init`
6. `collection::zip`
7. `collection::paginated`
8. `collection::defaultIfEmpty`
9. `collection::add`
10. `lang::cast`
11. `multiplicity::toOneMany`
12. `meta::pure::tds::sort` (not in SORT_FQNS)
13. `relation::extend`
14. `relation::lateral`
15. `relation::asOfJoin`
16. `relation::variant::flatten`
17. `meta::pure::mapping::withMapping`
18. `meta::pure::mapping::withChainedMappings`
19. `meta::relational::milestoning::concatenateTemporalTdsQueries`
20. `meta::pure::tds::groupByWithWindowSubset`

Also worth noting: `legend::lite::without`, `joinWithPrefix`, `recurse`, `groupByComputedKeys` and `groupByOverInstances`.

# What I could not determine

- Current counts behind the global ceilings in IdentityGuardrailTest, PlatformNamesGuardrailTest and TenetRatchetTest.
- Whether the RaiseTextGuardTest and RawSqlLedgerTest counts are exact or ceilings, and their current values.
- Which enumeration-mapping test and which planner test failed under 261ee728d.
- Whether natives such as `tds::sort` or `relation::extend` are desugared into typed nodes before OrderView runs; if they are, they never reach the native fallback.
- The full lowering chain of `JudgePlans.plannedSide` for a test-body `if` condition; that it is a kept value is inferred from the call chain.

Scratch scripts (method-length and native-list ports): /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/mlen.py, /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/nat.py, and its output /tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/nat.out.
