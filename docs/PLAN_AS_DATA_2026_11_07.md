# The plan as data, totally (design for the user's review, 2026-11-07)

> **Status: PROPOSED, not ratified.** Nothing below is implemented. The user asked (2026-11-07): "should we fix the
> whole plan to be real data fully and totally in this step? So we don't have to revisit that later again? When you do
> plan it please show us what the plan has/looks like before we implement." This is that showing.

## 0. The one rule

**The run reads one thing: the plan, which is data, plus the rows the database returns.** No Java object made while
planning reaches the run: no closure, no exception object, no typed tree, no environment, no compiler type, no map
keyed by a Java object's identity. The proof is mechanical: every lane runs the plan *decoded from its own JSON* (the
run never sees the in-memory plan), and the run's libraries cannot compile against a planning library (Bazel deps).

This serves G4 (the plan is SQL and data only, never Java), G11 (the plan as portable data) and G1 (one planner: all
planning, the asserts' included, lives in the planner's libraries).

## 1. What runs on what today

| Road | What the run reads | Data? |
|---|---|---|
| A product query (`Execution.run`) | the `ExecutionPlan` alone (`PlanRunner.stream(plan, sessions, sink)`; the writers read the plan's columns) | **yes**, one wrinkle: the session's database is decided from the model (`Compiler.executesOn`) beside the plan, not read off the plan's target |
| A test body (`BodyRouter`) | the `ExecutionPlan` **plus a sidecar of Java objects** the driver keeps beside it (section 3) | **no** |

So the work is the test road: the sidecar moves into the plan, as data.

## 2. The plan today (already data)

`ExecutionPlan(parameters, root)`. A program's root is a `Sequence` of steps. Every node below is a record of
strings, numbers, booleans, lists and enums, written and read by `PlanJson` (with `MirJson` for the typed SQL tree),
and every lane already checks the round trip (`PlanJson.requireRoundTrip`).

```
ExecutionPlan
├─ parameters: [Parameter(name, type, multiplicity, enumValues)]
└─ root: Node
     Sequence(steps: [Node])
     TdsResult(shape: RELATION|VALUE|OBJECTS, columns: [Column], sql: Sql)        -- a query's rows
     Keyed(keys: Sql, keysOf, admitted, keyFields, outputs)                       -- rows whose columns the data names
     Constant(columns, cells)                                                     -- a folded answer
     Update(sql, count) / Check(sql) / Effect(sql, script) / Setup(key, steps, target)
     Branch(condition: Side, then: [Node], otherwise: [Node])
     Verdicts(verdicts: [Verdict], fused: TdsResult?, savepoint, rules: LITE|COMPILED_PURE_COMPAT)
       Verdict(name, arm, base: [Side], wrapper: Node?, message: Side?, raises)
       Side = Rows(TdsResult) | KeyedRows(Keyed) | Constant(columns, cells, folded)
Sql(statement, slots: [Slot(parameter, binding)], target: Target, tree: SqlQuery?, regions, stamp)
Column(name, type: "Integer" | "meta::x::Person" | ..., multiplicity, slot, store, precision)
Target(database: Declared(connection) | Platform(type) | System(type), session: [String], setup: [SetupStep])
```

What a `Verdict` does **not** say today: what to compare, how, and what to do when it fails. That is the sidecar.

## 3. The sidecar today: what the run reads that is not in the plan

Measured from the bytecode (a method-level call graph of the driver and judge, 2026-11-07): every method the run
reaches (the body's sink `BodyRouter$Moved`, the held appeals, the run's reader) and every planning type it touches.

| Holder (beside the plan) | What the run reads from it | Not data |
|---|---|---|
| `TestDoor.Planned` | each effect step's ledger texts (`Recorded(text, query)`), each seed step's ledger texts, the steps that are a let's eager run (`frameRuns`) | it is a Java map beside the plan, keyed by step index |
| the sink's own maps (`Moved`) | which steps are database-judge groups, the statement origin of a deferred text, the rules of each verdict (an identity map), the dialect and the engine-text recorder | identity maps; a `SqlDialect` object at run |
| **the host judge's planned asserts** (`HostAsserts.Planned` → `AssertVerdicts.HostAssert` → `HostSides.Check` + `HostJudgment`) | the listener name, family and polarity; the comparison (`HostJudgment`, 26 kinds); how each side's rows are shaped into a value (`JudgePlans.PlannedSide`) | `SideFacts.info` and `Rows.expected/actualInfo` are compiler `ExprType`s; `Keys` holds compiler `EqualityKeys`; `Raise`, `Messaged.unplanned`, `AssertError.caught` and `HostAssert.beforeTheArm` hold **exception objects**; `Unrolled`/`AssertError.inner` hold nested `HostAssert`s; `HostSides.Ask` holds a **typed tree, a splice closure and the environment** (inert at run, measured); `PlannedSide` holds `ExprType`, a `RelationType` schema, a `TypedSpec` root and a pre-answered `TypedSide` |
| **the database judge's planned batch** (`DatabaseAsserts.Segment` → `VerdictBatch.Planned`) | the roots in body order (each assert's listener name and its steps), each verdict's paired ruled row (`orElse`), the members the dialect could not spell and the dialect's words, each root's message side | `Pending` holds a `SqlQuery` and an **appeal closure**; `Resolved` holds an **exception object**; refusals are **`DialectCapability` objects**; every map is keyed by Java identity |
| the agreement judge's pairing (`AgreementAsserts.One`) | which database verdict pairs with which host verdict, and the host half's held verdict | identity maps between verdict objects |
| **the engine-text comparison** (`HostJudgment.EngineText` → `SqlTextVerdicts.Collected`) | the golden text, our text (or the side that reads it), whether they are equal, a decline reason | the **appeal closure** (`TextAppeal`: 4 construction sites) and the **environment** (`ExecEnv`) |
| **the held appeals** (`Appeals`) | each appeal, decided after the run | `Supplier` **closures**; inside them the referee, the planned row sides, the replay facts, and `BooleanSupplier` **fixture closures** from the listener |
| the batch's judge (`VerdictBatch.Judge`) | how a verdict row is read | a **method reference** (`DatabaseJudge.verdictOf`) wired at planning |

And the run's **value model** uses the compiler's type vocabulary: `TypedSide.Scalar/Tabular.returnType` is a compiler
`Type`, and `Equality`, `PureAsserts`, `TdsCompare` and `HostJudge` decide kinds with `Type.Primitive`,
`Type.ClassType`, `PrecisionDecimal`, `RelationType`. The planning types the run touches (from the bytecode):
`Type`, `ExprType`, `Multiplicity`, `EqualityKeys`, `PlatformTypes`, `AssertMessages`, `VerdictSql`,
`DialectCapability`, `RawSqlBoundary`, `SqlQuery`, `ExecEnv` and the test door's `Planned`/`Effect`/`Recorded`.

## 4. The proposal: the plan holds all of it

### 4.0 Two plan types: the product's, and a test plan around it

The user's question (2026-11-07): "Should we have a test plan type so the product plan is not polluted?" Yes. Today the
product is polluted in four places, used by nothing in the product (every user is the driver, the judges, `testplan`
or these four): the plan type (`ExecutionPlan` carries `Verdicts`, `Verdict`, `Side`, `Branch`, `Check`,
`JudgingRules` and the verdict `Savepoint`), the runtime (`exec`'s `PlanRunner.program` runs verdict groups and
branches; its `ProgramSink` has `verdict`, `sides` and `fused` hooks), the product planner (`PlanMaker.testProgram`)
and the product's options (`ExecuteOptions`' judge mode and judging rules).

By composition, as the libraries are layered:

```
ExecutionPlan (execution_plan: the product's)       TestPlan (test_plan: new, on top of execution_plan)
  parameters                                          parameters
  root: Sequence | TdsResult | Keyed | Update |       steps: [TestStep]
        Effect | Setup | Constant                       TestStep = Run(ExecutionPlan.Node)    -- a product step, as is
                                                                 | Verdicts(verdicts, fused, judge, rules, savepoint)
                                                                 | Branch(condition, then, otherwise)
                                                                 | FrameRun(Check)
                                                      (no compatibility part: the edge renders the test plan, 4.5)
```

- `planner` makes product plans only; `testplan` makes test plans (`PlanMaker.testProgram` moves there), composing the
  planner's product nodes.
- `exec` runs product plans, and offers one way to run a product step inside an open program (the program's
  sessions and transaction stay `exec`'s); `testexec` walks a test plan, hands each product step to `exec`, runs the
  verdict statements through `exec` and judges. The test half of `PlanRunner.program` (verdict groups, the host
  sides' reads, the verdict savepoints, branches) moves to `testexec`; `ProgramSink`'s verdict hooks are deleted.
- The judge mode and the judging rules leave `ExecuteOptions` for the test road's own options.
- `TestPlanJson` writes the test steps and reuses `PlanJson` for every product node.
- A test body with no assert IS a product plan, run by `exec` directly.

Everything below that says "added to the plan" means the TEST plan; the product plan gains nothing.

### 4.1 What is added to the plan

```
Verdicts(verdicts, fused, savepoint, rules,
         + judge: HOST | DATABASE | AGREEMENT)                   -- replaces the sink's databaseGroups set

Verdict(name, arm, base, wrapper, message, raises,
        + assertion: Assertion(listener, family?, wantEqual)    -- today HostAssert's fields
        + judgment: Judgment?                                   -- the host judge's comparison, as data (4.2)
        + shapes: [SideShape]                                   -- how each base side's rows become a value (4.3)
        + failure: Failure?                                     -- decided at planning, raised at its turn (4.4)
        + root: int                                             -- the statement-root assert it belongs to
        + orElse: int?                                          -- database judge: its ruled row's place
        + pairedWith: VerdictRef?)                              -- agreement judge: the other half's verdict

a read (a side, a query step) + facts: ReadFacts(order: (sorted, keys)?,   -- the neutral facts the edge renders
                                      provenance: (mapping, class, stores)?, -- from (4.5); no compatibility record
                                      page: (unpaged: Node)?)
Check(sql, + origin: LET?)                                      -- today TestDoor.Planned.frameRuns
(the effects' and seeds' ledger texts, today TestDoor.Planned.effects/seeds, are not stored: the edge renders them
 from the statements, 4.5)
Verdict ... + oursOrigin                                        -- today the sink's deferredTextOrigins
```

`TestDoor.Planned` becomes just the plan; the sink keeps only run state (the rows it has read, the verdicts told).

### 4.2 Judgment: the 26 comparisons, as data

Each `HostJudgment` kind keeps its name and its data fields; the non-data fields change form:

| Today | In the plan |
|---|---|
| `SideFacts(ExprType info, classFqn, KindClass kind, flags...)` | `SideFacts(type: TypeRef, classFqn, kind: Kind, flags...)` |
| `TypeRef` (new) | `{name: "Integer" \| "meta::x::Person" \| ..., multiplicity: [lower, upper], columns: [Column]?}` -- the plan's own type spelling, as `Column.type` already is |
| `KindClass` (`Primitive` enum, `Instance(fqn)`, `Element(fqn)`) | the same, as a tagged value |
| `Keys(EqualityKeys e, a, flags, byClass)` | `Keys(e: KeyTree?, a: KeyTree?, flags, byClass: {fqn: KeyTree})`, `KeyTree(classFqn, [Key(name, many, nested: KeyTree?, objectOnly, declaredClass)])` -- `EqualityKeys`' own fields, copied |
| `Rows(..., ExprType expectedInfo, actualInfo, ...)` | `TypeRef`s |
| `RowArg.Literal(Object value)` | a typed cell, as `Side.Constant` spells cells |
| `Unrolled(parts: [HostAssert], askCounts)` | `parts: [Assertion + Judgment]`, nested |
| `Messaged(inner, innerSides, RuntimeException unplanned)` | `unplanned: Failure?` |
| `Raise(RuntimeException)` | `Failure` |
| `AssertError(expected, line, column, RuntimeException caught, HostAssert inner)` | `caught: Failure?`, `inner: Assertion + Judgment` |
| `EngineText(Collected(golden, ours, textEqual, decline, TextAppeal, ExecEnv), routed)` | `EngineText(golden, ours?, textEqual, decline?, routed)`; the appeal is the verdict's `appeal` (4.5) |

The other 17 kinds (`Equals`' flags, `GridPair`, `Size`, `Condition`, `Decided`, `InstanceOf`, `Rendered`, ...) are
already strings, booleans and lists.

### 4.3 SideShape: how a side's rows become a value

Today `JudgePlans.shape` reads `PlannedSide(kind, wrapped.shapeInfo: ExprType, schema: RelationType, answered:
TypedSide)`. In the plan: `SideShape(kind: CONSTANT|GRAPH|TABULAR|KEYED|VALUES, type: TypeRef, schema: [Column]?)`;
a constant's value is its `Side.Constant` cells (already in the plan).

### 4.4 Failure: an error known at planning

`Failure(kind, message, unjudgedReason?)`, `kind` one of the error types the run may raise (`ASSERT_FAILED`,
`DATA_ERROR`, `NOT_IMPLEMENTED`, `ILLEGAL_STATE`, `DIALECT_CAPABILITY`, `TYPE_INFERENCE`, ... -- the closed list is
taken from what is raised today). The run raises that type with that message at the verdict's turn. Replaces every
exception object carried today: `Raise`, `Resolved`, `beforeTheArm`, `Messaged.unplanned`, `AssertError.caught`,
`SideAhead.failure`, `Sole.Decided`, the dialect's refusals. What is lost: the planning-time stack trace (the message,
the type and the assert's place are kept).

### 4.5 The edge is a renderer of the test plan (no appeal record, no third artifact)

The user's question (2026-11-07): "Are we sure the edge should not just be a renderer of the test plan instead of yet
another third artifact thing by ID?" Yes, it should: lite already treats legend-engine's text this way (the engine-text
printer, `EngineStyleH2`, is a render target of the one compiler, as a dialect renders the SQL tree). So the test plan
holds FACTS about its reads and never a compatibility record; renderers derive their views from it -- the product's
dialects lite's own text, the edge's engine style the engine texts and ledger texts, the edge's referee the appeal.

Each appeal input today, and where the renderer finds it:

| Appeal input (today, captured by a closure) | From the test plan |
|---|---|
| our engine text; the ledger texts | rendered in engine style from each statement's SQL tree (the plan carries the tree) |
| the golden; a plan golden's bindings | constants and parameters of the body |
| the in-list seed; extra temp tables the golden reads | the in-list literals in the tree; the temps from the golden text |
| our rows | the read's own step |
| ordered, and the sort keys | a neutral ORDER fact on the read (the planner's order view: sorted, keys) |
| the mapping, the class, the stores the read touches (the fixtures to provide) | a neutral PROVENANCE fact on the read (mapping, class, stores) |
| the page population (our read without its tail page) | a neutral PAGE fact on a paged read: its unpaged form, planned with it (deriving it from the SQL tree instead risks differing from re-planning; measured before choosing) |
| whether the read is a subset of the class extent (`extentSubset`) | a fact on the read, as the compiler reads it today |

**Said plainly (2026-11-07, on the user's "do you think it's actually a good idea or just went with it bc I asked?"):**
the texts are renderings outright, but three of these facts are the REFEREE'S needs and nothing else reads them -- a
paged read's unpaged form, `extentSubset`, and the stores a read touches (for the fixtures). They are true of the
query, and they ride the test plan under their own names, not as compatibility records; the alternative (an edge
attachment keyed by ids, with an edge planning hook in `testplan`) costs more and can drift. They are kept to the
minimum the appeal needs; making the referee need fewer of them (the page population derived from the SQL tree, no
extent check) could change appeal outcomes, so it is measured against upstream before it is chosen.

The facts are true of a query whatever the rules (legend-engine's own plans carry provenance), so the test plan is the
same under both rules but for the values a rule renders: what `sqlRemoveFormatting()` returns is rendered at planning
by the renderer the rules name and kept as a plain string. The edge depends on the test plan, never the reverse (G5).
Each appeal kind's inputs are checked one by one when this step is built; one that cannot be derived becomes a
neutral fact on the read, never an edge record.

### 4.6 The run's types: exec's way, no type model at run

`exec` already runs with no type vocabulary of its own: the planner decides every type question and writes the answers
into each column as facts -- `type` (the Pure type's name, a string, carried as a label), `slot` (what the database
answers with, the instruction for reading the cell) and `precision`/`store` -- and exec reads each cell by its slot
into a plain Java value. Only a few named rules read the label itself (`result/RowDecode.numericPure`, `ANY`,
`VARIANT`). The judges take the same road:

- a judged value is already a Java value paired with a declared type (`Equality.Typed(value, kind)`), and its carrier
  kind already comes from the Java value (`Long`, `BigDecimal`, `String`, a date literal); the declared half becomes
  the plan's type name (a string), not the compiler's `Type`;
- every decision a judge makes today from a declared type (the numeric family, whether a decimal's scale counts, a
  class's keys, a relation's columns) is made at planning and written into the judgment as a fact, the way `slot` is.

At run a judge reads facts and values; it classifies nothing (the user's question, 2026-11-07: "How does exec run
without its own Type vocabulary?").

### 4.7 Where the code lives afterwards: the product's shape, twice; no driver

The user's question (2026-11-07): "if planner pieces of driver move to planner should exec part move to exec under
testexec or something? Why is there anything in driver that would cross over between the two to have a third
package?" There is no reason, once the plan is data. The product already has the shape: `planner` plans, `exec`
runs, and the front door (`com.legend.Execution`) sequences the two and is the one place that sees both. A test body
takes the same shape, beside it:

| Product | Test body | Holds |
|---|---|---|
| `planner` | `testplan` (exists) | planning: for tests, the body's program (today), and ALL assert planning (moved in): the three judges' planners, the arms, `DatabaseJudge`, `SqlTextVerdicts`' planning half, `VerdictBatch`'s planning half, the planning environment (`BodyDriver.ExecEnv`) |
| `exec` | `testexec` (new; today's `judge` becomes it) | running: for tests, the body's sink, the judges' comparisons (`HostJudge`'s run half, `Equality`, `PureAsserts`, `TdsCompare`, the agreement rule, `assertError`'s judging), the database judge's root loop over the verdict rows. It depends on `exec`, never the reverse: the product runtime carries no test judging (D71) |
| `Execution.run` | `Execution.runProgram` | the front door: plans with `testplan`, runs with `exec` + `testexec`; what `BodyRouter` does today, in the front door as the product's is |
| `execution_plan` | `test_plan` (new, on `execution_plan`) | the plan types (4.0): the product's; the test's, with the judgments and the rest of 4.1 to 4.6 and the reads' neutral facts the edge renders from (4.5) |

Where each piece of today's `driver` goes:

| Today in `driver` | Goes to |
|---|---|
| `HostAsserts`, `DatabaseAsserts`, `AgreementAsserts`, `AssertVerdicts`' arms, `DatabaseJudge`, `HostSides`, `JudgePlans`, `AssertBranch`, `LiteralFold`, `SeedSqlForms`, `VerdictArm`, `BodyDriver` (its environment), `SqlTextVerdicts`' planning half, `BodyRouter`'s classify-and-plan | `testplan` |
| `BodyRouter$Moved` (the sink), `HostJudge`'s run half, `AgreementArm`, `AssertErrorNative`'s judging, `RunReader` (likely deleted: the runner reads every side off the plan) | `testexec` |
| `HostJudgment`, `KindClass` (as data) | `test_plan` |
| `SystemDatabase` (the system store's session) | `exec` |
| `Appeals`, `SqlTextVerdicts`' appeal and rows leg, `JudgeJson`'s compatibility grammar, the referee | the edge (P2) |
| `BodyRouter`'s sequencing | `Execution.runProgram` |

What both halves use is not a middle package; it is a LOWER library each depends on: the census counters
(`diagnostics`), the asserts' message texts, the strict JSON reader (`json`). Each layer is pinned in
`tools/deps/core-layers.txt`: `testplan` may not depend on `exec` or `testexec`; `testexec` may not depend on
`planner`, `testplan`, the compiler, lowering or the resolver. A run class that names a planning type does not
compile. ArchUnit R1-R4 become hard rules on top, with R3 read as "only the front door sees both".

### 4.8 The guard

Every lane runs **the plan decoded from its JSON**: the router writes the plan, reads it back, and hands the run only
the decoded copy. Anything the run needs that is not data then fails on every lane at once, not in review. Measured
cost first (one write and one read per body).

## 5. Decisions for the user

1. **The edge is a renderer of the test plan** (4.5, the user's correction): no compatibility record in any plan and
   no third artifact; the test plan carries neutral facts about its reads (order, provenance, page), and the edge's
   engine style and referee derive the engine texts, ledgers and appeals from it.
2. **A failure known at planning is data** (type and message); its planning-time stack trace is not kept.
3. **The judges take exec's way with types** (4.6): the plan's type names and planning-time facts replace the
   compiler's `Type` at run. This touches every comparison, so it lands first, alone, held to identical verdicts on
   every lane.
4. **Every lane runs the decoded plan** (4.8), permanently.
5. **Two plan types** (4.0): the product's `ExecutionPlan` loses every test node; a `TestPlan` composes product steps
   with the test's; `testexec` runs it through `exec`.

## 6. Order of work (each step compiles and keeps the fast loop green)

1. The judges' types the exec way (4.6): `TypedSide`, `Equality`, `PureAsserts`, `TdsCompare` off the compiler's
   `Type`, onto the plan's type names and planning-time facts.
2. Failures as data (4.4).
3. The two plan types (4.0): `TestPlan` around `ExecutionPlan`; the test nodes leave the product's plan, runtime,
   planner and options; `testexec` drives a test plan through `exec`.
4. Judgments and side shapes in the test plan (4.2, 4.3); `HostJudgment`, `HostSides.Planned`,
   `JudgePlans.PlannedSide` deleted from the run's view.
5. The database judge's root loop from the test plan (roots, `orElse`, refusals, messages); `VerdictBatch.Planned`'s
   identity maps deleted; the batch's judge a plain function.
6. The agreement pairing in the test plan.
7. The sidecar into the test plan (ledgers, origins, the judge of each group); `TestDoor.Planned` is the test plan.
8. The edge as a renderer (4.5): the reads' order, provenance and page facts in the test plan; the referee derives each
   appeal from the plan; the closures deleted (`TextAppeal`, `VerdictBatch.Appeal`, the fixtures' `BooleanSupplier`s).
9. Every lane runs the decoded plan (4.8); the move (4.7): all assert planning into `testplan`, the sink and the
   judging into `testexec`, the sequencing into `Execution.runProgram`, `driver` deleted, the deps pinned, R1-R4 hard.
10. All lanes and an audit (the P1 milestone), held to the baseline of 2026-11-07 (GATES).

## 7. An example: `projection::limit::testSimpleLimit`, as it is planned today and as proposed

The test (legend-engine's, the DuckDB corpus, compatibility rules):

```pure
let result = execute(|Person.all()->project([#/Person/firstName!name#])->limit(0), simpleRelationalMapping, ...);
assertEquals('select top 0 "root".FIRSTNAME as "name" from personTable as "root"', $result->sqlRemoveFormatting());
assertSize($result.values.rows, 0);
let result2 = ... ->limit(1) ...;
assertEquals('select top 1 ...', $result2->sqlRemoveFormatting());
assertSize($result2.values->at(0).rows, 1);
```

Printed with `-Dlegend.diagnostics=plan-json` (2026-11-07). The run plans three programs for it: the test's two setup
programs (80 KB and 100 KB of JSON: their DDL and inserts, each statement as a tree and as script text) and the body
(10 KB). Below, the body; each `Sql`'s `target` (connection and session) and `tree` (its typed SQL) are elided.

### 7.1 The host judge, today: the plan

```json
{"_type": "sequence", "steps": [
 {"_type": "check", "sql": {"statement": "SELECT t0.FIRSTNAME AS name\nFROM personTable AS t0\nLIMIT 0"}},
 {"_type": "verdicts", "rules": "compiledPureCompat", "fused": null, "verdicts": [
   {"name": "meta::pure::functions::asserts::assertEquals", "arm": "assertEquals", "wrapper": null, "base": [
     {"_type": "constant", "columns": [{"name": "value", "type": "String", "multiplicity": {"lower": 1, "upper": 1}}],
      "cells": [["select top 0 \"root\".FIRSTNAME as \"name\" from personTable as \"root\""]]},
     {"_type": "constant", "columns": [{"name": "value", "type": "String", "multiplicity": {"lower": 1, "upper": 1}}],
      "cells": [["select top 0 \"root\".FIRSTNAME as \"name\" from personTable as \"root\""]]}]}]},
 {"_type": "verdicts", "rules": "compiledPureCompat", "fused": null, "verdicts": [
   {"name": "meta::pure::functions::asserts::assertSize", "arm": "assertSize", "wrapper": null, "base": [
     {"_type": "constant", "columns": [{"name": "value", "type": "Integer", "multiplicity": {"lower": 1, "upper": 1}}],
      "cells": [["0"]]},
     {"_type": "rows", "result": {"_type": "tdsResult", "shape": "RELATION",
      "columns": [{"name": "name", "type": "String", "multiplicity": {"lower": 1, "upper": 1},
                   "slot": "VARCHAR", "store": "VARCHAR(200)"}],
      "sql": {"statement": "SELECT t0.FIRSTNAME AS name\nFROM personTable AS t0\nLIMIT 0"}}}]}]},
 ... the same three steps for result2 (LIMIT 1) ...
]}
```

The plan says *which* rows each assert reads. It does not say what to compare or how.

### 7.2 The host judge, today: the sidecar the driver keeps beside it

Printed by a probe (typed trees, environments and closures shown as markers):

```
assertSize   -> HostAssert(listenerName=...assertSize, family=assertSize, wantEqual=true,
                  check=Check(asks=[Ask(arg=<TypedSpec TypedCInteger>, hook=<closure>, ...),
                                    Ask(arg=<TypedSpec TypedPropertyAccess>, hook=<closure>, ...)],
                              judgment=Size(envelopeValuesRead=false)), beforeTheArm=null)
   side 0       PlannedSide(kind=CONSTANT, answered=Scalar(value=0, returnType=INTEGER))           -- a compiler Type
   side 1       PlannedSide(kind=TABULAR, wrapped=WrappedSide(plan=SqlSelect(...),
                  shapeInfo=ExprType(GenericType(Relation, [RelationType(columns=[...])]), [1..1]), ...), schema=...)
assertEquals -> HostAssert(..., judgment=EngineText(collected=Collected(name=assertEquals,
                  golden="select top 0 ...", ours="select top 0 ...", textEqual=true, decline=null,
                  appeal=<closure>, env=<ExecEnv>, oursOrigin=null), routed=true))
```

### 7.3 The host judge, proposed: one plan, nothing beside it

```json
{"_type": "verdicts", "judge": "host", "rules": "compiledPureCompat", "fused": null, "verdicts": [
  {"name": "meta::pure::functions::asserts::assertSize", "arm": "assertSize", "root": 1,
   "assertion": {"listener": "meta::pure::functions::asserts::assertSize", "family": "assertSize", "wantEqual": true},
   "judgment": {"_type": "size", "envelopeValuesRead": false},
   "base": [ ...the same two sides as today... ],
   "shapes": [
     {"kind": "CONSTANT", "type": {"name": "Integer", "multiplicity": [1, 1]}},
     {"kind": "TABULAR", "type": {"name": "meta::pure::metamodel::relation::Relation", "multiplicity": [1, 1]},
      "schema": [{"name": "name", "type": "String", "multiplicity": {"lower": 1, "upper": 1}}]}]}]}
```

```json
{"_type": "verdicts", "judge": "host", "rules": "compiledPureCompat", "fused": null, "verdicts": [
  {"name": "meta::pure::functions::asserts::assertEquals", "arm": "assertEquals", "root": 0,
   "assertion": {"listener": "meta::pure::functions::asserts::assertEquals", "family": "assertEquals",
                 "wantEqual": true},
   "judgment": {"_type": "engineText", "decline": null, "routed": true},
   "base": [ ...the golden and our text, the same two constants as today... ],
   "shapes": [{"kind": "CONSTANT", "type": {"name": "String", "multiplicity": [1, 1]}},
              {"kind": "CONSTANT", "type": {"name": "String", "multiplicity": [1, 1]}}]}]}
```

There is no appeal in the plan: under the compatibility rules, where the texts differ, the edge's referee derives
the appeal from the plan (4.5) -- here the golden and our text are constants of the plan and equal, so it has nothing
to decide. The `Ask`s' typed trees, the splice closure and the environment are planning's alone and are not in the
plan.

### 7.4 The database judge, today and proposed

Today the database judge's plan for this body is already almost all data: ONE step, a fused statement that computes
all four verdicts and their messages in the database (`WITH frame_result AS MATERIALIZED (... LIMIT 0), frame_result2
AS MATERIALIZED (... LIMIT 1), __p_0 AS (... the two texts ...), __p_1 AS (... COUNT ...) ... SELECT 0 AS __ix,
<verdict>, <expected>, <actual>, <unjudged>, <lenient>, <holds>, <message> FROM __p_0 UNION ALL ...`), and four
verdicts named only `assertEquals`, `assertSize`, `assertEquals`, `assertSize`. Beside it, `VerdictBatch.Planned`
holds the roots (which assert each verdict belongs to, in body order), identity maps from verdicts to pending rows,
and, under the compatibility rules, an appeal closure per engine-text row.

Proposed: each of the four verdicts carries its `assertion` and `root`; an engine-text row whose text may differ
is appealed by the edge from the plan (4.5); a member the dialect could not spell carries its `failure`. The
statement is unchanged.

### 7.5 Things the example shows that are not about data (noted, not in this step)

- The host judge's plan runs `SELECT ... LIMIT 0` twice: once as the let's `check` step and again as `assertSize`'s
  side. The database judge's reads it once (the frame). A G3 item.
- Every `Sql` repeats its whole `target` (the connection and the session statements): the body's 10 KB is mostly
  that and the typed trees. A plan-size item (G3), easy once the plan is the one artifact: targets by reference.
- The setup programs carry each statement twice (its tree and its script text).
