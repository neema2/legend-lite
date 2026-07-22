# Corpus Runnability Plan — burn down to 100% runnable

**Goal (user-set):** every core_relational test must RUN through the platform.
FAILing on golden-SQL text or H2-dialect spelling is acceptable; not being able
to run a test at all is not.

## Where we are (homework)

Universe: **2538 tests** discovered and accounted (raw corpus carries ~2479
`<<test.Test>>` stamps; shared files counted per family variant; 129
`test.ToFix` excluded by engine-harness parity — the engine itself skips them).
No dark families: the previously-dark executionPlan (110) and postprocessor
(37) are discovered and classified. Plus 7 failed seed statements.

Current split (baseline `9f22c22e` + batch-4 in flight):

| bucket | count | % | runs? |
|---|---|---|---|
| PASS | 1193 | 47% | yes |
| FAIL (classified: order flappers, engine-side bugs, H2 text) | 55 | 2% | **yes** |
| SHAPE sql-only (executes rows; only golden-SQL asserts advisory) | 98 | 4% | **yes** |
| SHAPE "no execute() call" (body never reaches our engine) | 494 | 19% | **no** |
| SHAPE misc (assert forms, DB2 loop, wrapper shapes) | 26 | 1% | no |
| ERROR | 672 | 26% | **no** |

**Runnable today: 1346 / 2538 = 53%. The burn-down target: 1192 tests.**

## What the 1192 are

### ERROR (672) by cause cluster
- **Resolver/mapping gaps** (~220): property/class-not-mapped (80), Join-not-
  found cross-family (17), class-typed slot navigation (17), object-space
  TypedFilter/TypedMap/TypedDistinct vocabulary (27), multi-hop embedded (7),
  filter-unresolvable-column (11), modelJoin nested/milestoned (24), VIEW-
  backed union members (5), milestoning column walls (5), misc resolver walls.
- **graphFetch deep** (~126): non-association graph children (embedded/slot/
  otherwise/M2M, 21), cross-store JSON/CSV graph (19), union graph (13),
  qualifier/milestoned graph, plus the general graph family tail.
- **Platform-reflection vocabulary** (~140): tests that construct metamodel
  instances and call engine-internal functions — unknown functions (30:
  generateObjectReferences, compileLegendGrammar, …), serialize
  classCollection (15), RelationalDebugContext (10), ModelStore (6),
  ^Mapping (4), TabularDataSet/Table unknown types (17), router execute /
  evaluateAndDeactivate overloads (~43).
- **aggregationAware** (21): the aggregate-aware rewrite + its failed seed.
- **Dialect/driver** (~28): DB2 toSQLString (15), DuckDB binder errors (13).
- **Seeds** (7 statements): dropSchemaStatement, effectful-let setups, schema
  DDL spellings, connectionByElement 2-arg — cascade into family errors.

### SHAPE not-running (520)
- **executionPlan/tests** (100): assert over `executionPlan(...)` plan objects.
- **testDataGeneration** (68), **lineage** (55), **validation** (31): feature
  surfaces (existing tracks #44/#45/#46).
- **sqlDialectTranslation + sqlQueryToString + debugPrint** (~45): dialect
  translation API (pairs with the H2 advisory backend, #67).
- **Pure-code unit tests** (tests 39, tests/mapping/relation 43, router 18,
  pureToSQLQuery 11, helpers 7, modelJoins 5, m2m 5, extends 6, others):
  bodies are plain pure over the metamodel — need the called vocabulary
  evaluable, not execute().
- **Misc forms** (26): assertEmpty/1 (7), DB2 driver loop (8), wrapper-no-
  lambda (5), no-verifying-asserts (4), singles.

## The plan (phased burn-down)

**R0 — debt zero (user-ordered, BEFORE new construction).** Finish the
audit-23 backlog (~80 ranked suspects in task #75: SyntheticHeads identity
gates, temporal-family tail, freshening loops, liftTargetMerged crossed
routes, the LOW tail) so new R1-R5 code has no bad patterns to copy;
re-ground the golden-derived rules in engine SOURCE (negation constant,
sweep-date choice, self-assoc orientation) or ledger them explicitly;
harness slice D (endsInSort tails, tolerance instrumentation,
isExecuteCall/containsSqlText vocab gating); and the raw-SQL boundary
redesign — adaptRawSql's regex family moves OUT of the DuckDb dialect into
an explicit harness-boundary translator with a contract, with the real fix
(corpus raw H2 DDL/DML parsed into the SQL IR and rendered per dialect —
the standalone-SQL vision) planned as its own leg. RULE going forward: no
new regex-over-SQL anywhere; platform SQL is IR-rendered, corpus raw SQL
goes through the boundary translator only.

**R1 — seeds + runner vocabulary (cheap, ~40 tests).** Fix the 7 seed
statements (dropSchemaStatement native, effectful-let threading in setup,
schema DDL spelling, connectionByElement); assertEmpty/wrapper/no-assert SHAPE
forms; renameColumns non-literal pairs (6); single-expression-lambda walls
(5); bare-name stragglers. Each = honest execution, never a vacuous pass.

**R2 — resolver burn continuation (~220 tests).** The ongoing arc, highest
pass-yield per slice: union/subtype dispatch remainder, slot-chain navigation,
object-space vocabulary, the #72 INNER-filter isolation design, milestoning
long tail (#32), modelJoin nested predicates (with an engine-source check:
the engine's own modelJoins.pure restricts predicates to direct $this/$that
reads — the nested variants may classify as engine-unsupported).

**R3 — graphFetch deep (~126 tests).** H4b/H5: embedded/slot/otherwise graph
children, cross-store graph, union graph. A design leg + slices.

**R4 — platform-reflection vocabulary (~140 tests).** Register the metamodel
types/functions these bodies call (TabularDataSet, Mapping instances, debug
contexts, router overloads, serialize classCollection via meta-compile).
Policy: implement REAL thin behavior or classify loud — never stub an assert
into vacuity.

**R5 — the no-execute() surfaces (~494 tests).** Per-family feature tracks,
largest first: executionPlan object model over our lowered SQL (100);
testDataGeneration (#46, 68); lineage scans over resolved trees (#44, 55);
constraint validation (#45, 31); dialect-translation API + H2 advisory
backend (#67, ~45); then the pure-code unit-test vocabulary sweep (~195
across small families, censused per file).

**R6 — residual ledger.** DB2 asks (15+8): either a text-only DB2 renderer in
the EngineStyleH2 pattern or a classified dialect-residual ledger entry. The
55 FAILs stay classified as-is.

**Governance:** every slice through the full gate chain (core 0/0/0 → install
→ engine suite → FULL sweep → ledger diff with every delta classified → PCT →
commit/push). Corpus-vs-engine conflicts get classified against real engine
source, not goldens. The audit-#75 special-casing backlog continues
interleaved so new code doesn't recreate the debt.

**Expected end-state:** RUN rate from 53% → ~100% (modulo a small explicit
ledger: ToFix-excluded parity, DB2-only asks if we choose not to build the
renderer, and engine-unsupported shapes proven from engine source). PASS rate
rises opportunistically along the way — R2 alone should push passes well
above 1400.
