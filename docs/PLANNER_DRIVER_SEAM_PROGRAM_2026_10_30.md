# The planner-driver seam program (2026-10-30): a test body as one plan the database executes

> **Status: PLANNED, NOT RATIFIED. No code.** This is the end-to-end plan the user asked for on 2026-10-30 ("Let's
> definitely do full detailed end to end plan - with homework and research, not guesses"), rewritten the same day
> after the user's correction ("Who said to copy upstream? That's not our execution model in lite ours is in
> database or host judge"): the first draft used legend-engine's host executor (its allocation node, its variable
> map, its text holes) as the design template. It is not the template. **Lite's execution model is the one in force:
> everything executes through a plan; the DATABASE holds the rows and computes the verdict; the host judge is the one
> kept exception (D7); a shape the planner cannot know is a plan step whose rows fill the next statement's regions
> (the user, 2026-10-20: the keys probe plan, register rows S106 to S110).** This program applies that model to the
> test body, which is the one place it does not yet hold.
>
> Every claim about lite's code is a measurement in `docs/planner-driver-seam-homework/CODE_MEASUREMENTS_2026_10_30.md`
> (cited as **CODE §n**). The upstream reading, `docs/planner-driver-seam-homework/UPSTREAM_READING_2026_10_30.md`
> (cited as **UPSTREAM §n**), is SPEC INPUT in the tenet's sense: what a let and an assert mean, and the evidence that
> planning needs no database for anything but one shape. It is not a model to copy. Nothing in either file was
> recalled from memory. The user rules on §6 before any step is built; each step is then planned with the user in its
> own brief, built, chained, audited, folded and pushed as the seam-and-wall program's steps were (the decisions log
> D70 to D73).
>
> **Prerequisite:** the seam-and-wall program (steps A, B1, B2, C; PR #82) lands first. This program starts on a
> fresh branch from `main` after it.

## 0. In plain words

A product query is a VALUE the planner makes once (`TypedQuery.plan` → `PlanMaker.plan` → one `ExecutionPlan`) and
the runtime sends to the database (`Execution.run` → `PlanRunner`). The planner never sees a session; the runtime
never sees the compiler (D72, the wall); the runner carries rows and computes nothing. That is lite's model, and for
a product query it is complete, lets and dynamic pivot included: a let is spliced at planning or staged as a table
the database holds (`PlanMaker.program`, CODE §1d); an uncast dynamic pivot is a `Keyed` plan, the keys read first
and the body's per-key regions filled by the runner from those rows (CODE §3e; S110).

A TEST BODY (a PCT test, a corpus test, a service test: lets, asserts, effects, then an answer) does not have that
model yet. The body driver (`com.legend.driver`) improvises a PROCEDURE: it takes the query's compiler off
`TypedQuery` (`specs()`, `resolved()`: CODE §1a, §1b) and, for every statement and every assert side, re-runs the
inliner, the store resolver, the lowerer and the renderer itself (14 inliner sites, 6 resolver uses, 2 lowerers, 20+
renders: CODE §2b), sends the statement, and plans the next one against what came back. Four probes read the live
session to finish planning, and their answers re-enter the COMPILER (a lowering callback, a resolver referee, a plan
edit, a DESCRIBE re-typing: CODE §3). One loop of 247 lines drives a 2,321-line driver and about 4,350 lines of
judges (CODE §5e).

**The program, in one sentence:** the body becomes a plan the planner makes once, in lite's own vocabulary (which
already holds `Sequence`, `Update`, `Effect`, `Check`, `TdsResult` and `Keyed`), with a verdict step for the asserts;
the database executes it statement by statement as it executes a write program today; and the driver becomes the
runner plus the judges at the verdict steps, so that after planning no compiler runs and nothing a probe reads
re-enters the compiler.

What the upstream reading contributes (UPSTREAM §3, §6): legend-engine plans a multi-statement lambda in one pass,
each later statement against the SHAPE of the lets before it, never their values, and reaches no database while
planning. That is the spec fact this program rests on: a body with lets needs no result to plan the next statement.
How legend-engine then EXECUTES (a Java node visitor, a variable map, text holes filled at run time, assertions
evaluated in Java) is its host executor and is not lite's model; lite's answer to the same needs is the staged table,
the compile-time splice, the `Keyed` regions and the in-database verdict.

## 1. The homework, in ten facts

Lite (CODE):

1. The driver reaches two of `TypedQuery`'s seven widened internals, `specs()` and `resolved()`; the other five
   (`lower`, `Resolved`, `root`, `resolve`, `lowerer`) are `PlanMaker`'s own (CODE §1h). Of `PlanMaker`'s four
   public statics, `plan` is the door's and `keyed`, `sql`, `serversFor` are the driver's (`JudgePlans`, one
   `BodyDriver` site; CODE §1i). The "driving of compiler phases through the internals" is, measured, the driver
   taking the compiler and the typed body off the query and re-running G½, H, I, J itself.
2. The driver compiles mid-run for eight measured reasons (CODE §2d): lets bound to earlier results (spliced frames,
   staged tables), helper bodies typed lazily at inlining, deferred TDS text, the metamodel system route, the four
   dynamic shapes read from the session, provisioning before planning (seeded tables' wire types), effects sent
   between statements, and engine-text plan asserts.
3. The four probes reach the database only through `PlanRunner.columns`/`describe` (CODE §3d), and re-enter
   PLANNING as: a callback into `lowering.Render.resolveAllDeferredTds` (ColumnProbe), a `SchemaReferee` into
   `resolver.RawGridSchema.stamp` (GridProbe), a plan edit after lowering (DynamicPivot), a re-typing of outputs
   after a DESCRIBE (WireProbe). This re-entry is the design flaw: a database answer flows back into the compiler.
4. The product path has a plan step for ONE of the four probe shapes: the dynamic pivot, as `ExecutionPlan.Keyed`
   run by `PlanRunner.keyed` (CODE §3e). A deferred TDS is REFUSED by the renderer there. A late-bound raw grid is
   NEITHER stamped NOR refused there: the plan carries zero columns (finding F1 below). A wire-decided kind is carried
   as the compiler's slot, neither probed nor refused.
5. The plan vocabulary is six node kinds with a runner arm each; `Keyed` inside a `Sequence` is a named refusal
   (`exec/PlanRunner.java:294-295`, `:361-362`; CODE §4).
6. The oracles: the corpus (`//gates:corpus_duckdb`, `corpus_h2`: the committed rosters
   `spec/src/test/resources/rcorpus/*-roster.txt`), channel B (`//gates:pct_channel_b`), and five core lanes (root,
   driver, exec, integration with its two twins, judge); the PCT main and heavy lanes already run the PRODUCT path
   (`PctPlanRoute`) and are untouched by this program (CODE §7a). A deferred TDS has no core test at all (CODE §7b).

Upstream, as spec input (UPSTREAM):

7. A multi-statement lambda is planned in ONE pass; each later statement is planned against the SHAPE of a let
   (`PlanSetPlaceHolder` with its TDS columns, `PlanVarPlaceHolder` with a type), never its value (UPSTREAM §3).
   Spec fact: no statement's result is needed to plan the next.
8. Planning never reaches a database: zero `executeInDb` calls in every plan-generation file (UPSTREAM §6).
9. The one plan-time-unknown shape is the dynamic pivot (UPSTREAM §4). Lite's ruled form for it is the keys probe
   plan (`Keyed`; S106 to S110) and stays.
10. Upstream never plans a test body: legend-pure interprets it, legend-engine evaluates assertions in Java as data
    (UPSTREAM §5). Lite's ruled form is the opposite by its execution tenet (no host interpreter; D29, D30): the body
    runs through plans and the verdict is the database's (or the host judge's, D7). This program keeps that form and
    changes WHO makes the plan and WHEN.

## 2. Findings posted to the user (defects and gaps the homework found)

- **F1 (invariant 4, the product path).** A raw SQL grid of unknown schema on the product path is neither stamped
  nor refused: the lowering emits `SqlSelect.starOf(RawSql(...))` with the outputs of a late-bound schema, which are
  EMPTY, and `PlanMaker.typed` builds a plan with zero columns (CODE §3e; `lowering/Lowerer.java:2386-2388`,
  `plan/PlanColumns.java:44-49`). No refusal names it anywhere in `planner/`, `TypedQuery` or `Compiler`. This is a
  defaulting branch by omission. Proposed: a refusal by name on the plan side (step S0), its own commit, before
  anything else. **The user's go is needed.**
- **F2.** A deferred TDS (`SqlExpr.DeferredTdsString`) has no test under `core/src/test`; its only oracle is the
  corpus and channel B (CODE §7b). Step H1 measures its origins before step S3a touches it.
- **F3.** `TypedQuery.planStreaming` has zero callers in main and test (CODE §6a). Dead public API; deleted in S5.
- **F4.** `GridProbe.probeNames` has no caller (CODE §3b). Deleted with GridProbe in S3c, or earlier as a nit.
- **F5.** `Keyed` inside a `Sequence` is refused by name, so a write program whose answer is an uncast dynamic pivot
  is refused on the product path today (CODE §4). S1 lifts the refusal: a pivot test body is a let, the pivot and an
  assert, so the body plan holds a `Keyed` step beside a `Verdict` step from the first pivot body onward (the user's
  question of 2026-10-30; H6 only counts how many).
- **F6.** The test-data generator's fold reads the session before planning the next statement (`foldCensus`, CODE
  §5a; the named residue D30 said to revisit as "a two-plan body"). Step S2j.

## 3. The target shape: lite's model applied to the body

### 3.1 The model, as ruled, and the body's gap against it

| what a body does | lite's ruled form (where it holds today) | the body driver today (the gap) |
|---|---|---|
| runs a statement | a plan step the runner sends; the database executes; typed rows to a sink (`TdsResult`, `Check`) | the same, but the step is made mid-run by the driver's own lowerer and renderer (CODE §2b) |
| binds a let | spliced into its readers at planning (`with(lets, stmt)`), or staged as a table the DATABASE holds (`Update` create-as, dropped at the end): `PlanMaker.program` (CODE §1d); never a value Java holds | `letPrefix`, `execFrames`, `queryLets`, `planRows` carried in the driver's environment (CODE §5b); frames run eagerly and re-planned per reader (CODE §2d.1) |
| asserts | the verdict is ONE CELL the database computes (`VerdictSql`, the verdict batch); the host judge over typed rows is the kept exception (D7, `com.legend.judge`); BOTH runs both and the agreement arm | the same judges, but each side planned mid-run by the driver (`planHostSide`, `planStaged`, `wrapSide`: CODE §5d) |
| performs an effect | an `Effect` step in place (`Update`, `Effect`, the setup steps on a `Target`) | a script collected per segment and sent before the next statement is planned, because planning may probe (CODE §2d.7) |
| needs a shape the planner cannot know | a plan step whose ROWS fill the next statement's REGIONS, executed by the runner (`Keyed`: the keys read first, the body's per-key regions filled from those rows; the user 2026-10-20, S110); a kind the plan cannot know is decoded by the wire at the row (D72: the runtime's row on the plan's column) | four probes whose answers re-enter the COMPILER (CODE §3) |
| reads the metamodel store | the step's `Target` is the system database with its setup (`PlanMakerTest.theMetamodelStoresTablesAreThePlansSetupInProcess...`) | the session switched per planned statement (`routeSystemDatabase`, CODE §2d.4) |

Nothing in the left column is new. The program is the right column becoming the middle one.

### 3.2 The plan vocabulary after (step S1)

Added to `executionplan/ExecutionPlan.java`, each a record with a runner arm and a JSON round trip, none with a
`default`, none holding a value the runner would compute:

- `Verdict`: an assert as a step, in the two ruled forms. `Verdict.Database(Sql verdict, Message facts)`: the one
  cell the database computes (today's verdict batch statement, as a step the runner sends and whose cell the
  listener receives). `Verdict.Host(TdsResult left, TdsResult right, Arm arm, Message facts)`: both sides' typed
  rows streamed to the host judge's lattice in `com.legend.judge` (the kept exception). Under
  `ExecuteOptions.JudgeMode.BOTH` a verdict carries both and the agreement arm.
- A **wire-decided column** fact on `TdsResult.columns`: a column whose declared kind is wire-decided
  (`Type.wireDecided`: String, Number, Any; `compiler/element/type/Type.java:693-696`) is decoded by the kind the
  wire reports, at the row (D72's carriage), recorded on `Column.slot`. No DESCRIBE round trip, no re-typing of the
  plan.
- The **raw grid as the pivot's pattern** (the user's questions of 2026-10-30, §3.5): a first step whose ROWS are
  the reported columns of the authored statement (name and wire type, as the seam reads them today: `columns`, a
  LIMIT 0 run, or `describe`, a prepared statement never run; which one is the CONNECTOR's, as the decode facts
  are, D72), then the body with a region per column (`Region`, `Binding.Key`, the existing expansion: a VALUES row
  per name for `columnNames`, a cell per column for `rows.values`), each cell decoded by the wire. Whether the step
  is `Keyed` itself with a "keys" step that reads columns, or a sibling record, is S1's brief; no runner capability
  is added beyond reading reported columns as rows. The same step names a deferred TDS's column list where H1 shows
  one outside a pivot. No step at all where nothing needs the names before the grid runs: a column read by the name
  the program wrote, a grid that is the answer or a judge's side (its columns read from the real run at the row).
- `Keyed` inside a `Sequence` (F5): required, lifted in S1.
- A step's `Target` may differ inside one `Sequence` where H7 shows bodies that mix the platform (a verdict's
  canonical rows) and a declared database; otherwise the one-target rule stays and the mixing refusal stays.

No allocation node, no variable map, no text hole: a let is a staged table or a splice (§3.1), and a step's rows go
to the database (a staged table) or to a sink (the judge, the caller), never into a Java-held environment the next
step reads.

### 3.3 The body planner (step S2): one pass, each statement a step

`planner/BodyPlanner.plan(TypedQuery, runtime, options) → ExecutionPlan` (the name is provisional), grown out of
`PlanMaker.program` (the write program's `Sequence`, CODE §1d), which already plans a program in one pass: each
statement resolved with the lets before it, a let bound to rows staged. The kinds, each a step, in the order H4's
census ranks them (provisional order here, by the driver's branches at `driver/BodyCompiler.java:71-176`, CODE §5a):

| kind (the driver's branch today) | the step after | the ruling it follows |
|---|---|---|
| a value statement (`prepareValue` + `runValue`) | `TdsResult`/`Check`, the last one the answer | the plan path program |
| a value let, a handle let (`letPrefix.add`, `PlanAllocations.registerHandlesIn`) | no step of its own: spliced into its readers at planning; a handle's lineage rows the plan's facts | the plan path program §5 (lets) |
| an alias let (a frame shared) | no step: a substitution at planning | the same |
| a staged let (`stage()`, `StagedRows.boundTo`) | `Update` (create as) naming the table, `Update` (drop) at the end, readers written against the table: `PlanMaker.program`'s form | S-rows of the plan path program (staged lets) |
| an effect (`executeInDb`, DDL natives, scripts; `collectEffect`/`sendScript`) | `Effect` steps in place; a segment's script one `Effect` of several statements where the dialect sends scripts | D65 (the routings) |
| an assert (`AssertVerdicts.tryAdjudicate`) | `Verdict.Database` / `Verdict.Host` / both | D7, D29 |
| a frame let (`execute(...)`, `legendQuery`; `buildFrame`, the splice hook) | its chain spliced into its readers at planning (today's "re-plan of a spliced chain", done once by the planner) or staged where its rows are read more than once; the eager run dropped where nothing reads its rows before the end (H4 counts; each dropped run posted, since it changes a lane's statement count) | the plan path program §5 |
| the metamodel system route (`routeSystemDatabase`) | the step's `Target` is the system database with its setup | the product path's form (CODE §7b) |
| an engine-text plan assert (`SqlTextVerdicts`, `engineSql`) | a `Verdict` over two constants, the text lowered at planning by the engine-text dialect (planning-side work, no session) | D30 (`planToString`, `toSQLString` plan-time values) |
| the generator's fold (`foldCensus`, D30) | S2j: a two-plan body: the generator's plan runs first through the runner, its facts handed to the body planner as DECLARED inputs (the one place a plan's answer feeds another plan's making; posted to the user as a divergence row if any shape must stay) | D30's "revisit as a two-plan body" |
| `print`, `$plan` navigation, `planToString`, `toSQLString`, `assertError` (D30's Java residue) | unchanged: plan-time values or carriage of an error; each stays pinned in `JavaEvalLedgerTest` | D30 |

The body planner is a function over immutable values: it takes the typed query and returns a plan. It holds no
session, no probe, no referee. `plannerIsExecutionFree` already forbids it to reach `exec`, `driver`, `judge`; a
new rule `theBodyPlannerHoldsNoSession` pins that no type of `exec` appears in its signatures.

`JudgePlans` (CODE §5c), which BUILDS the judges' plans, is planning work and moves into `planner/` (its `run`
helpers stay in the driver). `VerdictSql`/`VerdictQueries` already live in the compiler.

### 3.4 The driver after (step S4): the runner and the judges

`driver/BodyDriver.execute` becomes: `plan = BodyPlanner.plan(typed, runtime, options)`; `PlanRunner.sequence(plan,
session, listener, answer)`; at a verdict step the runner hands the cell (database judge) or the two sides' rows
(host judge) to `com.legend.judge`, and the listener gets the verdict; the last step's answer goes to the caller's
sink (`Execution.Answer`, unchanged). `BodyCompiler`, its `Segments`, `ExecEnv`'s `queryLets`/`planRows`/`frame`,
the four probes, the inliner/resolver/lowerer constructions and the 20+ renders leave the driver. What remains of
`BodyDriver` is the glue and D30's residue, each pinned.

After planning NOTHING compiles: a new ArchUnit rule `theDriverCompilesNothing` (no class of `com.legend.driver`
depends on `compiler.spec..`, `resolver..`, `lowering..`, `sql.dialect..`), and `tools/deps/core-layers.txt`'s driver
line loses `compiler`, `compiler_element_type`, `lowering`, `resolver`, `normalizer`, `parser`, `lineage`, `plan`,
`sql_dialect`, `validation`, `builtin`, `protocol` (the exact list is the measurement at S5; the target is "the
runtime's libraries plus `:planner`'s doors, `:judge` and `:execution_plan`").

### 3.5 The raw grid: the pivot's pattern, not a probe and not an oracle

The first draft asked whether the RAW GRID (the rows of a SQL string a test author typed, `executeInDb('select
...', $c)`: 2,252 calls in 86 of legend-engine's relational test files, the corpus input) needs a database answer
during planning, because a later statement may ask for its `columnNames` or flatten its `rows.values`, and offered
two forms: columns declared in the program, or a schema oracle door the planner owns. Both are withdrawn (the user's
questions of 2026-10-30: "Is this similar to dynamic pivot? Does it make sense to use that pattern?").

It is the dynamic pivot's situation in the one respect that matters: the compiler types the whole program WITHOUT
the names. A raw grid's cells are `Any[0..1]` and its names `String[*]` (`compiler/spec/typed/TypedRawSqlRelation`,
"columns are late-bound"; `RawGridSchema`'s stamp gives names, "cell types stay the database's own, decoded at
egress"), and a column read by a name the program wrote is the database's own error when absent, never the
compiler's. So the names are needed only to SPELL a later statement's text, exactly as a pivot's keys are, and the
pattern the user ruled for the pivot applies (S110: a plan step whose rows fill the next statement's regions):

1. a first step reads the authored statement's reported columns as rows (§3.2);
2. the body statement carries a region per column; `columnNames` is a VALUES list with a row per name, `rows.values`
   a row-major flatten with a cell per column, and `at`, `size`, a fold or a map over them ordinary SQL over that
   relation; each cell's kind is decoded by the wire at the row (D72);
3. no first step where nothing needs the names before the grid runs (a read by a written name, the grid as the
   answer or a side): the columns come from the real run, at the row, as legend-engine reads a dynamic pivot's.

The compiler never sees a database answer: `RawGridSchema.stamp`, its `SchemaReferee` and `GridProbe` are deleted at
S3c. Three differences from the pivot, each with its cost: the first step is a metadata read, not distinct values
(both exist in the seam; the plan step says WHAT, the connector HOW); the columns' types are unknown where the
pivot's are the aggregate's (the wire-decided decode covers it, and the judges already take tagged mixed-kind values
since track L); `columnNames` becomes `String[*]` where today it is rewritten into a fixed-size list of literals (a
multiplicity loosening; H2 shows whether any verdict depends on it, and each such case is posted). Declaring the
columns in the program (a typed `executeInDb` overload) stays as a convenience for an author who wants typed cells,
never as something the corpus must be rewritten to.

H2 is the homework that proves it: every downstream read of a raw grid in the corpus and channel B, each shown to
lower to SQL over the per-column region, and the count of grids that need the first step at all (a LIMIT 0 run
still plans the query on the database, and a SELECT with a side-effecting function would execute).

### 3.6 The boundary at the end (step S5)

The widenings step C paid for (D73's "cost") revert by construction: the driver no longer calls `specs()` or
`resolved()`; `keyed`, `sql`, `serversFor` are called by the body planner inside `planner/`; `TypedQuery`'s
internals move INTO `planner/` (`TypedQuery` becomes a thin door delegating to one public entry) so they are
package-private again. `PlanMaker` and `BodyDriver` keep one public entry each (`plan`, `execute`), which
`theDriverHasOneCaller` and `plannerIsExecutionFree` already pin. The JPMS question (a `module-info.java` exporting
`com.legend` only) is then about MECHANICAL guarantee, not about hiding anything: the user decides whether the one-day
spike (does rules_java 9.9 compile a module under Bazel without breaking the product jars?) is worth it (§6 Q4).

## 4. The steps, ordered and sized

Each step: its own brief agreed with the user, the full local chain, an independent read-only audit, the fold, the
records (GATES, IN_FLIGHT, this §8, the decisions log, register rows where a spelling or an outcome moves), one
commit, one push. **The oracle of every step is zero roster movement** on `corpus_duckdb`, `corpus_h2`, channel B
and the five core lanes, plus the statement-origin census (`StatementOrigin.PROBE` shrinking to zero by S3,
`WIRE_RETYPED`/`WIRE_SLOT_SKEW` equal before and after S3b). A step that moves a roster row stops and posts the row. **The heavy lanes run by hand at every S3 step and at S2d**
(the user's question of 2026-10-30): the local chain covers DuckDB, H2, SQLite and Postgres only; transaction brackets
around effects differ on MariaDB and MySQL, and the wire decode differs on Trino, ClickHouse, Snowflake, Databricks
and BigQuery, so a step that touches effects or the wire is not proven by the local chain alone (lane sessions measure
only; no credentials read, printed or copied).

| step | what | size | oracle and guards that move |
|---|---|---|---|
| **H** | the homework completions, read-only, lanes measure only: **H1** deferred TDS registrations by origin (pivot vs other) on the corpus and channel B; **H2** raw grids demanded vs undemanded, and whether each demanding body declares its columns; **H3** `WIRE_RETYPED` / `WIRE_SLOT_SKEW` per lane; **H4** a statement-kind census per body (the table in §3.3) and the frame lets whose rows are read before the end; **H5** bodies on the system route, bodies with the generator's fold; **H6** bodies whose non-last statement is an uncast dynamic pivot; **H7** bodies mixing the platform and a declared target | 2 to 3 days | none: measurements, posted as a report |
| **S0** | F1 closed inside the program (the user, 2026-10-30: "We will do the grid fix as part of the program"): the plan side refuses a late-bound raw grid by name until S3c gives it its real form, then plans it; built with S3c, not as its own commit | folded into S3c | a planner test; no roster moves (the product lanes never plan one) |
| **S1** | the vocabulary: `Verdict.Database`, `Verdict.Host`, the wire-decided column fact, the reported-columns-as-rows step with its per-column regions (`Keyed` or a sibling, S1's brief), `Keyed` inside a `Sequence` (F5, required), per-step targets if H7 says so; runner arms; JSON round trip (`testcases/PlanRoundTrip`); no caller yet | 3 days | `core_tests_planner`, `exec`, `testcases`; nothing in the driver changes |
| **S2a–S2j** | the body planner, kind by kind, as a strangler: the driver keeps its loop but, kind by kind, asks the body planner for the step and runs it through the runner; the driver's own branch for that kind is deleted in the same commit. Order from H4's frequencies; provisional: a (values), b (value, handle, alias lets), c (staged lets), d (effects), e (database-judge asserts), f (host-judge asserts, BOTH), g (frames), h (the system route), i (engine-text asserts), j (the generator's fold) | 1 to 3 days each, 2 to 3 weeks in all | each: zero roster movement; `BodyDriver`'s size pin shrinks (dated); the `JavaEvalLedgerTest` registers move by name; D30's residue list re-pinned |
| **S3a** | the dynamic pivot in bodies as `Keyed` (the product path's form, S110); `DynamicPivot.staticize` deleted; ColumnProbe deleted, the deferred TDS's column list a per-key region of the `Keyed` body (S110's form) where H1 shows the pivot is its origin, else the rows-fill-regions step of S1 | 3 to 5 days | zero roster movement; `PROBE` census drops by the pivots' count; `DynamicPivotKeyLiteralH2Test` becomes a planner test; `JavaEvalLedgerTest:217` (DynamicPivot's pin) deleted |
| **S3b** | wire-decided kinds decoded by the wire at the row (§3.2); `WireProbe.reconcile`/`staticized` deleted; `describe` has no caller in the driver | 3 to 5 days | `WIRE_RETYPED` before equals the wire-decoded count after; zero roster movement; `WireProbeTest` rewritten as a decode test in `exec` |
| **S3c** | raw grids as the pivot's pattern (§3.5): a read by a written name and a grid as the answer or a side with their columns from the real run; `columnNames` and `rows.values` as the reported-columns step plus per-column regions; `GridProbe`, `RawGridSchema.stamp` and `SchemaReferee` deleted; `RawSqlLedgerTest:122`'s one `RawSql` → 0 | 1 week | zero roster movement (a `String[*]` multiplicity loosening posted if any verdict reads it); `PROBE` census 0 |
| **S4** | the driver as the runner and the judges: `BodyCompiler` and `Segments` deleted; `BodyDriver.execute` = plan once, run the sequence, judge at the verdicts; `specs()`/`resolved()` no longer called | 1 week | zero roster movement; `theDriverCompilesNothing` added; the driver's size pins shrink to the residue |
| **S5** | the boundary: `TypedQuery`'s internals into `planner/` (package-private again), `PlanMaker`'s three driver statics package-private, `JudgePlans` in `planner/`, F3 and F4 deleted, `core-layers.txt`'s driver line cut, the registers closed; the JPMS spike if the user says so | 3 days (+1 for the spike) | `//tools/deps:core_layering_test`; `ArchitectureTest`; `JavaEvalLedgerTest` |
| **Z** | the final audit and the landing handed to the user | 2 days | CI on every OS on the branch; the PR; the user's word |

Total: six to eight weeks of sessions, the lanes' turnaround included. Steps H, S0, S1 are safe to start the moment
the user rules; S2 onward each need their own brief.

## 5. What stays, by name, and why

- **The two judges and the verdict batch** (`com.legend.judge`, D7, D71): the database computes the verdict; the host
  judge over typed rows is the kept exception; the program moves the judges' PLANS to the planner and leaves the
  judging where the rows arrive.
- **`Keyed`** (S106 to S110): the user's ruled form of a shape the planner cannot know; this program extends its
  pattern (rows fill the next statement's regions) to the body's other such shapes.
- **Compile-time splicing and staged tables for lets** (the plan path program §5): a let is never a value Java holds.
- **The engine-text dialect** (`EngineStyleH2`, `SqlTextVerdicts`'s expectations): plan-time text, planning-side.
- **D30's residue** (`assertError`, `print`, `$plan` navigation, `planToString`, `toSQLString`): named, pinned; the
  generator's fold is the one D30 said to revisit (S2j).

## 6. Decisions for the user, before any code -- RULED 2026-10-30

The user, 2026-10-30: "Yes I'll merge when green / Yes program is a go / We will do the grid fix as part of the
program / Fresh branch / Order sounds good / Yes pivot model unless you think that's bad/wrong". So: Q1 GO; Q2 the
defect F1 closed inside the program (S0 folded into S3c, which gives the raw grid its real form; no separate commit);
Q3 the pivot's pattern is the ruled form; Q4 answered below (the build already gives the compiler guarantee at the
library level; the four member-level rules are the named residue; no JPMS spike); Q5 a fresh branch from `main` after
PR #82 lands (the user merges when run 2280 is green); Q6 by H4's frequencies, asserts early. The questions as asked,
for the record:


- **Q1. The program as a whole:** §0 to §4 as the plan, or changes. (A yes starts H.)
- **Q2. F1 now:** the plan-side refusal of a late-bound raw grid (S0) as its own small commit, or held.
- **Q3. The raw grid (§3.5):** the pivot's pattern as the ruled form (recommended: a reported-columns step, per-column
  regions, cells decoded by the wire; no probe into the compiler, no oracle door), or another form.
- **Q4. JPMS:** spend one day on the `module-info.java` spike at S5, or rely on the ArchUnit rules and
  package-privacy. **Answered 2026-10-30 (the user: "Why when we are done with the program do we still not get
  compiler guarantee? ... what would still be crossing packages?"):** every LIBRARY-level edge is already refused by
  the build (a target compiles against its declared deps alone, rules_java's strict deps; the lists pinned by
  `tools/deps/core-layers.txt`): the planner cannot name a runtime or driver class, the runtime cannot name a planning
  class (D72), and after S5 the driver cannot name a compiler, resolver, lowering or dialect class. What the build
  cannot express is a MEMBER-level rule, "this one public entry is called by that one class", because Java's
  package-private, Bazel's deps and JPMS's exports all work at package, target or module grain, and the doors in the
  root package force their entries public. The residue after S5 is four such rules, each an ArchUnit test:
  `BodyDriver.execute` called by `Execution` alone; `BodyPlanner.plan` and `PlanMaker.plan` called by the doors and the
  driver alone; a session's `Statements` sent by the plan runner alone; the judges' entries reached from the verdict
  seam alone. JPMS would add nothing here: it exports packages, not methods, and the root package `com.legend` is split
  across the planner and driver targets, which a module system forbids outright. No spike.
- **Q5. The branch:** this program starts after PR #82 lands, on a fresh branch from `main` (recommended), or on top
  of the seam-and-wall branch.
- **Q6. The strangler order (S2a to S2j):** by H4's frequencies (recommended), or the user's order.

## 7. Rules of the program (in force, verbatim from the user)

"Never push to main, never open a PR or merge without my explicit word in your session"; "never change how any
output is spelled without asking me first, one case at a time, with the expression and the before and after"; "An
answer of mine to a question is not a go to build something else; ask"; "A guard pin or allowlist moves only with a
dated justification"; "Never read, print or copy credentials; lane sessions measure only"; "Run the start page's §6
checks before each commit; commit and push to the branch; records (GATES, IN_FLIGHT, the program's §9, register
rows) land with the code"; "Plan each step with the user and get agreement before writing code". The execution
tenet: Java orchestrates, the database executes; the judges in the database or the host judge; no host interpreter.
The upstream-fidelity tenet: upstream is read from the pinned sources for what an answer IS (a spelling, an outcome,
an error, a type, a rule), never recalled, and never copied as an execution model; every divergence in an answer is
posted, then a row of `docs/SEMANTICS_REGISTER.md`.

## 8. Status

- 2026-10-30: the homework done (two independent read-only agents: CODE, UPSTREAM); the plan written and posted; the
  user's correction the same day ("Who said to copy upstream? That's not our execution model in lite ours is in
  database or host judge"): the first draft's allocation node, variable map and "as upstream does it" framing
  withdrawn; this version states the design in lite's model (D75). The same day, from the user's questions ("What is
  a raw grid exactly?", "Is this similar to dynamic pivot?", "a metadata read or a limit 0 query?", "is this the right
  plan?"): the raw grid as the pivot's pattern and the oracle door withdrawn (§3.5), `Keyed` in a `Sequence` required
  (F5), the heavy lanes by hand at S2d and S3 (D76). The user's rulings on §6 the same day (D77): GO; the raw grid
  defect inside the program; the pivot's pattern; no JPMS spike; a fresh branch after PR #82; the order by frequency.
  **Step H started 2026-10-30** (read-only measurements; the report lands on the fresh branch). No code yet.
