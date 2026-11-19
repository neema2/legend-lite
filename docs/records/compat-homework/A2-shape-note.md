# A2's shape note: legend-engine's SQL spellings deleted (plan slice 2.3a)

Census B2, 2026-11-19. Read from the code at `main` `b75d6c73a` (the product of `claude/census-b2`), by bytecode
where the question is reachability. The independent reviewer's condition is that A2 names its whole scope and lists
the tests that move; this note does both.

**Nothing moves in this homework:** no pin, ceiling, roster, ledger, golden or allowlist, and nothing merges.

The probes are in `A2-probes/`, run against `//core:product_jars` and `//core:edge`'s `libedge.jar`:
- `A2_Graph.java.txt`, a conservative method-level call graph;
- `A2_Reach.java.txt`, a forward walk with rapid type analysis and the style hooks' keys;
- `A2_Family.java.txt`, the class-level reference graph.

The roster is `A2-roster.tsv`.

**A2 is slice 2.3a.** It deletes legend-engine's SQL text printers and the statement shape that exists only for them:
`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`, `EngineShape`, the plan-text family's SQL spellings and
`RelationalRootForm.apply`. Slice 2.3b, after 2.2, holds the shapes that run in executed SQL (`existsJoin`,
`whereZones`) and `raw`.

## 1. The deletion list

### 1.1 Whole files: 4 files, 2,990 lines

| File | Lines | Who refers to it (the class-level graph, `A2-family-out.txt`) |
|---|---:|---|
| `core/src/main/java/com/legend/edge/EngineStyleH2.java` | 2,507 | `PureText` (the factories); `EngineStyleDB2` extends it |
| `core/src/main/java/com/legend/edge/EngineStyleDB2.java` | 365 | `PureText`; `EngineStyleComposite` extends it |
| `core/src/main/java/com/legend/edge/EngineStyleComposite.java` | 79 | `PureText` |
| `core/src/main/java/com/legend/edge/EngineTextDialect.java` | 39 | the printers' interface: `PureText`, `EnginePlans`, `EngineAllocations`, `PlanEnvelope`, `TestDataPlanText` |

**These four are a closed set.** Outside them, only five edge classes refer to any of the four, and no product class
does. The only `new EngineStyle*` sites in the bytecode are the two factories:
- `PureText.engineText`, lines 300-302;
- `PureText.enginePlanText`, lines 315-316.

So a printer's methods run only on an instance one of those two made. A renderer's virtual call into an override (the
printers extend lite's H2 dialect) is not a use.

### 1.2 Deletions inside files

| Site | Lines | Its callers |
|---|---|---|
| `PureText.engineText` (javadoc :295, method :298-308) and `enginePlanText` (javadoc :310, method :312-322) | about 28 | listed in §1.3 |
| `EdgeLoweringForms.engineSql` (:155-162) and `record EngineShape` (:186-272) | about 95 | `EnginePlans.engineSql` (`EnginePlans.java:174`, `.withStyled(EdgeLoweringForms.engineSql(env.styled()))`), which becomes `env.styled()` |
| `RelationalRootForm.apply`, both overloads (:44-161), and its helper `enumRawColumn` (:162-206) | about 160 | `EnginePlans.java:164`; **and `Compiler.lowerResolved` (`Compiler.java:962-964`), which the plan does not list** |

On `Compiler.lowerResolved`: its branch runs only when `relationalRootForm` is true, and every caller passes `false`:
`PlanAllocations.java:398` and the tests. So the branch is dead in the product, and the flag can go with `apply`. That
changes two signatures `PlannerSurfaceTest` pins (§5).

**`EngineStyleH2.raiseMessage` and `PureMessages.sentence`** (the compat owner's addition, from H13):
- `EngineStyleH2.raiseMessage` (`EngineStyleH2.java:409-416`) goes with its file.
- `PureMessages.sentence` (`edge/PureMessages.java:22`), legend-pure's raise sentence built in SQL, has **13** bytecode
  callers at `main`, not one (`A2_Callers.java.txt`):
  - `EngineStyleH2.raiseMessage:415`;
  - 12 `PureText` dialect passes' `raiseMessage`: DuckDb `:391`, H2 `:429`, Postgres `:475`, Databricks `:507`,
    Snowflake `:532`, BigQuery `:557`, SQLite `:590`, MariaDb `:615`, MySql `:640`, Trino `:672`, ClickHouse `:697`,
    MongoDb `:728`.

  The passes use it for raises whose sentence needs a computed operand (the interim that slice 1.2 deletes, G7).
  H13's "one caller left" holds once 1.2 has landed. So **`sentence` joins A2's deletion list on the condition that
  1.2 lands first**; otherwise it stays for 1.2 to delete.
- **Its tests:** none calls `sentence` directly. `exec/InterimEdgeSitesTest` pins the interim sites it serves (1.2's
  guard). `JavaEvalLedgerTest:1871` lists `PureMessages.java` among the files it reads; that stays, since the file
  keeps `compile` and `wordable`.
- **No census row names it:** `inventory.tsv:300` names `PureMessages.compile`.

**What stays of `RelationalRootForm`:** `primaryKeyColumns` (:207-255), `stripQ` and `dedup`. They are the product
resolver's key rule on executed paths: `ObjectReferenceDecode:133`, `ClassSources:797`, `CorrelatedSubselects:321`,
`:486` and `:1094`, `GraphEmission:533` and `:537`, `StoreResolver:3636`, `Substitution:1837`.

**The plan-text family is not printer-only.** The family is `EnginePlans` (1,011 lines), `PlanText` (1,003),
`PlanEnumForm` (295), `InProtocol` (240), `PlanVariables` (41), `EngineParams` (193), `EngineSpans` (225),
`EngineAllocations` (179), `PlanEnvelope` (60) and `TestDataPlanText` (116).
- The class-level fixpoint (`A2_Family`) grows no further than the four files. Every family class is referred to from
  outside the printers: the `UpstreamPlans` facet's implementation (`EnginePlans$1`), `TestDataRoutes`, `PureText$1`
  and the hosts.
- Their plan FORMAT stays, as slice 1.4's presenter (the plan, §2.3). What goes is each seam where a printer is
  handed in (§1.3), plus the spelling-only code behind those seams: template parameters, FreeMarker splices, the
  in-temp-table select, the select-list cut.
- Those seams are A2's second PR (§6).

### 1.3 Every call into the printers (bytecode, `A2-printer-call-sites.txt`)

| Caller | Factory | What it prints today | After A2 |
|---|---|---|---|
| `EnginePlans.toSqlString` (`EnginePlans.java:88`) | `engineText` | `toSQLString`'s text | lite's SQL in the target's dialect, or the refusal (§3) |
| `EnginePlans.activitySql` (`:298`) | `engineText` | an activity's `sql` (the `ActivityTexts` two-form) | the product's `ActivityTexts` form: lite's own SQL |
| `EnginePlans.ddlText` (`:971`, `:991`, `:1002`) | `engineText` | `toDDL`'s schema and table statements | lite's DDL (`SqlDialect.render(SqlDdl)`); some of it is EXECUTED, see §3 |
| `EnginePlans.planConnOf` (`:754`, `:767`) | `engineText` | a plan connection's `testDataSetupSqls` (`setUpDataSqlsText`) | lite's DML (`render(SqlDml)`); printed, never sent (S192) |
| `EnginePlans$1.engineSeedSql` (`:268`), `engineSeedSqlFromRecords` (`:276`) | `engineText` | `setUpDataSQLs`' statements (the `UpstreamPlans` facet) | lite's DML |
| `EnginePlans$1.generatedTestDataPlan` (`:283`) | `engineText` | the test-data generator's plan text | lite's SQL in the plan format |
| `EnginePlans.planToString` (`:440`, `:455`, `:463`), `sequencePlan` (`:700`, `:708`), `crossDbTdsPlan` (`:536`), `planModel` (`:843`) | `enginePlanText` | `planToString` and the plan's SQL nodes | lite's SQL inside legend-engine's plan format, or the refusal |
| `EngineAllocations.node` (`:133`) | `enginePlanText` | an allocation node's SQL in plan text | the same |
| `PlanEnvelope.emit` (via `EngineTextDialect.collectionSplice`, `inTempTableSelect`, `params`; `:34`, `:37`) and `TestDataPlanText.planText` (`:30`, `:49`, `:61`) | a printer handed in | FreeMarker splices, in-list temp selects, template parameters | gone with the interface; lite's text has no template parameters (F1's a3 class) |

The product's own planner asks for these through the style's hooks, and those call sites do not change. Only the
answer behind each one does:
- the `UpstreamPlans` facet: `BodyPlanner.java:311` and `:328` (`ddlText`), `PlanTextRoutines.java:27` (`routineText`),
  `PlanAllocations.java:113` (`planRows`), `Seeds.java:161`, `SeedSqlForms.java:39` and `:51`, and `BodyDriver.java:81`
  and `:855`;
- the `ActivityTexts` two-form: `PlanAllocations.activitySql`.

## 2. What stays, and `PureV1Routes.generatePlan`, by bytecode

**The question was whether `generatePlan` reaches the deleted code.** It builds its plan from lite's own:
`TypedQuery.executionPlan` → `PlanMaker.plan`. A forward walk from `PureV1Routes#generatePlan` (`A2_Reach`) asked
whether it can reach a printer factory. The walk uses rapid type analysis (a virtual call reaches only classes
instantiated in code already reached), and each style hook reaches only the form for the key its call site passes.

| Walk | Reaches a printer | The path (`A2-reach-*.txt`) |
|---|---|---|
| every hook | **yes** | `generatePlan` → `TypedQuery.executionPlan` → `PlanMaker.plan` → `programmed` → `planStatement` → `ProductStatements.prepare` → `BodyPlanner.prepare` (`:311`, a DDL native) → `facet(UpstreamPlans).ddlText` → `EnginePlans.ddlText` → `PureText.engineText` |
| `UpstreamPlans` cut | **yes** | … `ProductStatements.inline` → `ResultEnvelopeSplice.hook`, `relationalSqlRead` (a query reading an execute result's `activities[].sql`) → `ProgramFrames.Recorder.relationalActivitySql` → `PlanAllocations.activitySql` → `twoForm(ActivityTexts)` → `UPSTREAM`'s form (`EnginePlans::activitySql`) → `engineText` |
| `UpstreamPlans` and `ActivityTexts` cut | **no** (26,945 nodes walked) | |

**So "generatePlan reaches none of the deleted code" is false as worded.** Under `PURE_V1` every facet and two-form but
`LoweringForms` delegates to `UPSTREAM` (`PureText.java:218-279`). So a pure/v1 plan reaches legend-engine's printers
today, through two hooks:
- the `UpstreamPlans` facet, when the query holds a plan-text native: `toSQLString`, `planToString`, the DDL statement
  natives, or an `executionPlan` read;
- the `ActivityTexts` two-form, when the query reads an execute result's activity SQL.

An ordinary query reaches none. After A2, both paths answer lite's SQL, which is CP-4's promise: SQL text is not
promised, and a query that asks for SQL text gets lite's. The compat owner accepted this answer, 2026-11-19.

**None of pure/v1's golden-net requests takes either path.** Of the edge net's 15 requests at `claude/compat-s0`
`ef9e9bfa3` (14 at `main`), none calls a plan-text native or a DDL native, or reads an activity's SQL. Each is sent to
execute and to `generatePlan` (`PureV1EdgeGoldenTest.java:166`), so 0 of its 30 answers reach a printer. An ordinary
Studio "generate plan" is the same shape.

**What the walk cannot see:**
- A call on a JDK type (`Object.toString`, `Function.apply`) is not followed into project code, except through the
  `invokedynamic` that made a lambda.
- A hook's key is the nearest `ldc Class` before the call (within 12 instructions), or the caller's key when a
  delegating hook passes its first parameter on.
- Reflection is not followed.

**What stays:**
- the SQL-text appeal and the referee (ruled; slice 2.3);
- `PlanStyle.judgesEngineText` and the `EngineTestText` facet's judging, since every SQL-text assert still goes to the
  appeal;
- legend-engine's plan format (1.4's presenter);
- `PureText.dialect` and its passes;
- `RelationalRootForm.primaryKeyColumns`.

No `PlanStyle` method exists only for the printers: Phase 6 left `PlanStyle` naming no printer type. `Databases`
names none either. It is the refusal site for databases without a lite dialect (§3).

## 3. What each caller answers afterwards

**Where lite has a dialect, the caller answers lite's own SQL.** That covers H2, DuckDB, Postgres, SQLite, MariaDB,
MySQL, Trino, ClickHouse, Snowflake, Databricks, BigQuery and MongoDB. The text is the one the plan executes,
`PureText.dialect(type)`, so `toSQLString` prints the SQL lite runs.
- **A widening to report:** today `toSQLString` refuses every database but H2, DB2 and Composite ("toSQLString for
  DatabaseType.X — only the H2, DB2 and Composite engine-style renderers are built", `PureText.java:304-306`). Plan text
  likewise refuses all but H2 and the DB2 family (`:318-320`). After A2, both answer on all 12.

**Where lite has no dialect, the caller gets a refusal by name.** That covers DB2, Composite, MemSQL, Sybase, SybaseIQ,
SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle and Aurora.
- **The one site:** `Databases.dialect` (`core/src/main/java/com/legend/database/Databases.java:95-97`), which
  `PureText.dialect` already delegates to for these (`PureText.java:340-341`).
- **Its words:** `NotImplementedException`: "SQL dialect for database type 'DB2' is not implemented yet".
- DB2 and Composite are the two that answer today, through `EngineStyleDB2` and `EngineStyleComposite`. Their 23 corpus
  goldens already fail their text and have the appeal declined (F1's a1), so they stay in the "not promised" class.

**Executed statements need one decision.** `EnginePlans.ddlText` serves both the printed `toDDL` text (F1's a2) and the
statements 3 corpus tests EXECUTE (H8 §(e.3): `aggregationAware`'s setup, 57 statements). One method serves both
uses, so 2.3a cannot leave the executed half to 2.3b.
- **The measurement:** under F1's probe, `ddlText` printed lite's own H2 DDL, and the 3 tests passed on both lanes.
  They are not in the roster. On DuckDB, the H2 text goes through `raw`, as today.
- **The recommendation:** in 2.3a, write lite's H2 DDL (the measured shape), and keep `raw`'s translation for 2.3b's
  target-aware DDL.

`setUpDataSQLs` is never executed (S192's read-only reload), so it is printed text only.

## 4. The tests that move: `A2-roster.tsv`

This is one table, a test per row, with each lane's class and H12's attribution beside F1's. It reconciles F1's probe
(`census-b2-sqltext-probe-2026-11-19.tsv`: 147 on H2, 152 on DuckDB) with H12 (`claude/compat-translator-spike`
`326b5defe`, `h12/corpus/attribution-{h2,duckdb}.tsv`).

| Class | Why it moves | H2 | DuckDB |
|---|---|---:|---:|
| a1 | text alone: the text failed and the rows appeal declined (a DB2 or Composite golden, lite's rows underivable on the referee, a golden that does not run there) | 64 | 64 |
| a2 | text alone: a printed SQL, plan or DDL string compared or searched with no appeal | 20 | 22 |
| a3 | text alone: lite's printer has no form for the construct the test prints (untyped plan parameters, FreeMarker splices, a minimal-fraction date format, a mixed tagged value) | 52 | 53 |
| a4 | text alone: SQL read from legend-engine's plan (an SQL node's `sqlQuery`, an activity's `sql`) | 3 | 4 |
| b | a DIVERGED appeal: the rows differ, an answer the text hid (`joinStrings` twice; `firstDayOfWeek` on DuckDB) | 2 | 3 |
| c | legend-engine's plan structure (`supportsStream` on a `FunctionParametersValidationNode`) | 6 | 6 |
| **moves with A2 today** | | **147** | **152** |
| R4 | hidden until R4 lands: `testPlanForDateTimeConstantParameter{No,GMT,EST}TimeZone`, whose `indexOf(...) != -1` assert is vacuous under the old `indexOf` (H12) | 3 | 3 |
| **moves once R4 has landed** | | **150** | **155** |

**The reconciliation is exact.** H12 attributes all 147 and all 152 to lite's text:
- on H2: 142 text alone, 4 text with substring/indexOf, 1 text with another strand;
- on DuckDB: 146, 5 and 1.

No test of F1's probe falls outside H12's text attribution. The reviewer's "142/146 text-only" is H12's "text alone"
column. F1's a1-a4 is the same set, counted with H12's 4 and 5 text-and-substring rows and its 1 text-and-strand row
included. 155 distinct tests in all.

## 5. What else changes: tests, BUILD files, tools, and the guards A2 must move

**The tests that use a printer as a dialect under test** lose that column or case:
- `integration/DeclaredQuotedTableH2Test` (6 references), `integration/EdgeSpellingHookTest` (2);
- under `sql/dialect/`: `BinaryCollateTest` (6), `CatalogGridTest` (5), `EngineSeedRowTest` (3),
  `IdentifierEscapingTest` (2), `PreQuotedNamesTest` (4), `RaiseTextTest` (2), `SchemaDdlTextTest` (2),
  `SqlWriterTest` (2), `StoreWritesTest` (5), `StoredReadsTest` (2), `TempTableAsTest` (5), `TranslateTest` (6);
- `CapabilityMatrix` (1);
- the fixture `core/src/test/resources/com/legend/sql/dialect/pre-quoted-names.tsv`: its 192 `EngineStyleH2` rows;
- `planner/PlantedHostEdgeReach.java:18`, a planted fixture naming `EngineTextDialect`.

**BUILD files and tools:**
- `core/BUILD.bazel:3465`: `CapabilityMatrix`'s `{EDGE_DIALECTS}` root names `EngineStyleH2.java`. The generated
  `//docs:dialect-capability` pages regenerate.
- `tools/census/RenderCensus.java:9` and `:54` (it imports `EngineStyleH2` and renders with it), `render.sh`,
  `README.md`.

**The guards.** Each move needs the user's word through the orchestrator. The proposed justification for every row
reads: "2026-11-NN, slice 2.3a (A2): legend-engine's SQL spellings deleted under CP-4, upstream's SQL text never
promised (the user, 2026-11-19); `<entry>` named the deleted `<class>`". Per guard:

| Guard | Lines | The move | Its direction |
|---|---|---|---|
| `ArchitectureTest` | `:249-267` (the printers confined to the text natives), `:275-296` (no product class depends on `EngineTextDialect`), `:5088-5098` (`isAssignableTo(EngineTextDialect)` in a reach rule) | the rules' subjects are gone: delete the first two, drop the clause from the third | shrink |
| `CodeShapeGuardrailTest` | `:202`, `:205` (allowlist: `EngineStyleH2.frameDepth`, `anonDistinctDepth`) | remove both entries | shrink |
| `CompatSpellingGuardTest` | `:107-108` (the edge's names list), `:482-488` (a planted `new EngineStyleH2()` whose expected hit is `EngineStyleH2`) | drop the three names; replant the fixture on a surviving edge class | shrink, and a fixture |
| `DialectBoundaryTest` | `:186`, `:203` (the dialect-constructor ratchet, 17 since 2026-11-13 with the printers) | 17 → 14 | shrink |
| `DialectLeakRatchetTest` | `:59` (the dialect names regex), `:109` (the names list) | drop the three printers | shrink |
| `DialectOwnerExclusivityTest` | `:54` (`Crossing("…/EngineStyleH2.java", "DuckDB", 9)`), `:228` (asserts the dialects include `EngineStyleComposite`), `:300` (`DatabaseOwners.of(… "class EngineStyleDB2 extends EngineStyleH2")`), `:342` | remove the crossing; retarget the asserts | shrink |
| `JavaEvalLedgerTest` | `:1886`, `:1932` (the files the ledger reads: the three printers, `EngineTextDialect`) | drop the four names | shrink |
| `ParkedWorkLedgerTest` | `:147` (a parked pattern located in `EngineStyleH2.java`) | the parked row closes with its file | shrink |
| `PlannerSurfaceTest` | `:1066-1068`, `:1198-1200` (the two `Compiler#lowerResolved` signatures) | only if the dead `relationalRootForm` flag goes (§1.2): both signatures lose the `boolean` | a planner entry's signature |
| The corpus rosters `spec/src/test/resources/rcorpus/{h2,duckdb}-fail-roster.txt` | | +147 and +152 (or +150 and +155 after R4) by `A2-roster.tsv`; the ordering register −4 on H2 (F1) | grows: CP-4's "not promised" class, counted |
| The edge golden nets | `spec/src/test/resources/edge/corpus_{h2,duckdb}_host_judge-host.tsv`, `core/src/test/resources/edge/pure_v1_answers.tsv` (activities' SQL), `pct/src/test/resources/edge/*_answers.tsv` (relation activities) | re-recorded with the ruling cited | ruled moves |
| `spec/src/test/resources/rcorpus/lean-sql-baseline.tsv` | | may only shrink (the plan's check) | shrink |

The rest name the printers in comments only, and need edits but no moves: `DatabaseOwners:256`,
`EdgeSeparationTest:129` and `:326`, `HookRulingTest:66`, `SqlTextRatchetTest:116-117`. `HookLambdasTest:245` names
`BodyDriver$ExecEnv#engineText()`, the text collector the appeal reads. That is a different method, and it stays.

**The product's own sources** name the printers in comments and javadoc only, 11 mentions in 7 files: `Pure.java:416`,
`PlatformTypes.java:396`, `Fold.java:445`, `LiteralSpelling.java:548`, `AnsiSqlRenderer.java:661` and `:685`,
`DuckDbSpellings.java:14`, and `H2.java:17`, `:189`, `:962` and `:1484`. They are edits, not moves.

## 6. The slice plan: two PRs

**PR 1: the printers out, an adapter in.** This is F1's probe, made real.
- **Deletes:** the 4 files, 2,990 lines; the two factories; `EngineShape`; `RelationalRootForm.apply` and the dead
  `Compiler` flag.
- **Adds:** each §1.3 seam answers lite's SQL through one adapter over `PureText.dialect(type)`, about 80-120 lines,
  the shape F1's proxy measured (`LiteTextProbe`, 76 lines with its `PureText` hook). DB2, Composite and the rest get `Databases.dialect`'s refusal.
- **Moves:** the rosters and goldens by `A2-roster.tsv`; the guards in §5.
- **Measured:** 147 and 152 tests move (150 and 155 after R4). 3 DIVERGED become fails, the answer differences F1
  reported. No other answer moves.
- **Size:** about −3,300 main lines and +80 to +120; tests: 16 test files lose a printer column, and the fixture loses 192 rows.
  **2 to 3 days**, most of it the guard moves and re-recording the goldens.

- **PR 1's added tests** (the compat owner's call, 2026-11-19), on the pure/v1 route (`PureV1Routes.generatePlan` and
  execute):
  - one planted request of each hook's shape: a query calling `toSQLString` (or `planToString`, or a DDL native: the
    `UpstreamPlans` path), and one reading an execute result's `activities[].sql` (the `ActivityTexts` path). Each
    asserts lite's own SQL after the change;
  - one ordinary request, asserting its plan is byte-identical before and after.
- **PR 1's list of moves, for the user's word:**
  - the rosters' 147 and 152 (150 and 155 after R4);
  - the three nets re-recorded;
  - `toSQLString` and plan text widened to all 12 lite dialects: a move toward E-R2;
  - the dead `Compiler.lowerResolved` flag and its two `PlannerSurfaceTest` signatures;
  - each guard row in §5.
- **The calls of 2026-11-19:**
  - two PRs, as cut here;
  - `toDDL` writes lite's H2 DDL in PR 1, and `raw` stays for 2.3b;
  - the dead flag goes, listed for the user's word;
  - the widening is a listed move.

**PR 2: the plan-text family's spelling-only code out.**
- **Deletes:** the adapter's engine-only arms (template parameters, FreeMarker collection splices, the in-temp-table
  select, the select-list cut) and the code behind them in `PlanEnvelope`, `TestDataPlanText`, `EngineParams`,
  `EngineSpans` and `EnginePlans`' spelling paths.
- **Stays:** legend-engine's plan format, 1.4's presenter.
- **Moves:** nothing further. The check is that the corpus and both nets move no line beyond PR 1's. The a3 class has
  already failed in PR 1.
- **Size:** an estimate, not a measurement: about −800 to −1,200 lines across the 10 family files, about 3,300
  lines in all. **2 days.**

**Why not one PR:** PR 1's moves are the user's to read, 150-odd roster lines and three nets. PR 2 should be a pure
deletion against PR 1's recorded state, so that nothing hides in it. One PR would mix a ruled move with a
refactoring. Together, about 5 days, the plan's 2.3 estimate for 2.3a and 2.3b.
