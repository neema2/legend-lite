# The plan as data, totally (the design, 2026-11-07/08)

> **Status: every decision RULED by the user (2026-11-08); a focused independent check of the three parts no review
> has seen (the runner's callback, the edge's extension point, the connection description) precedes step 0.** This
> document replaces the earlier drafts and their appendix; its history is the branch's commits (`83f68e44` to
> `982929bc`). Nothing below is implemented yet.

## 0. The rule, and how it is proven

**A test body's run reads one thing: its plan, which is data, plus the rows the database returns.** No Java object
made while planning reaches the run: no closure, no exception object, no typed tree, no environment, no compiler
type, no map keyed by a Java object's identity. The user (2026-11-08): *"the thing I really want to ensure is that
there is no code in the plan, it's all data."*

Proven mechanically (section 2.10): a test walks the plan's TYPES and requires each to be data, so no field can hold
code; every plan survives its JSON round trip; the run's libraries cannot compile against a planning library; one
lane runs the plan decoded from its JSON. Later, a planner-free runner binary (G11's own gate).

Serves G4 (the plan is SQL and data only), G11 (the plan as portable data), G1 (one planner, one runner) and G5 (the
edge depends on the product, never the reverse).

## 1. Today

| Road | What the run reads | Data? |
|---|---|---|
| A product query (`Execution.run`) | the `ExecutionPlan` alone (`PlanRunner.stream`); the writers read the plan's columns | yes, but the session's database is decided from the model (`Compiler.executesOn`) beside the plan |
| A test body (`BodyRouter`) | the `ExecutionPlan` plus a sidecar of Java objects the driver keeps beside it | no |

**The plan** (`ExecutionPlan`, written and read by `PlanJson`, round-tripped on every lane) is data: a `Sequence` of
`TdsResult`, `Keyed`, `Constant`, `Update`, `Check`, `Effect`, `Setup`, `Branch` and `Verdicts` steps; each
statement an `Sql(statement, slots, target, tree, regions, stamp)`. A `Verdict` says which rows an assert reads
(`base`), not what to compare or how. Test concepts sit in the product: the plan type (`Verdicts`, `Verdict`, `Side`,
`Branch`, `JudgingRules`, the verdict `Savepoint`), the runtime (`PlanRunner.program` runs verdict groups and
branches; `ProgramSink` has `verdict`, `sides`, `fused`), the product planner (`PlanMaker.testProgram`, and
`PlanMaker.program`'s `Door.TEST` branches) and the product's options (`ExecuteOptions`' judge mode, judging rules,
recorder).

**The sidecar** (measured from the bytecode and an independent review, 2026-11-07/08):

| Beside the plan | What the run reads | Not data |
|---|---|---|
| `TestDoor.Planned` | effects' and seeds' ledger texts, frame-run places | maps keyed by step index |
| the sink (`BodyRouter$Moved`) | which steps are database groups, deferred texts' origins, each verdict's rules, the dialect, the recorder | identity maps; a `SqlDialect` at run |
| the host judge's planned asserts (`HostAsserts.Planned` → `HostAssert` → `HostSides.Check` + `HostJudgment`, 23 kinds) | the assert's name, family, polarity; the comparison; each side's shaping (`JudgePlans.PlannedSide`) | compiler `ExprType`s, `EqualityKeys`, exception objects (`Raise`, `Messaged.unplanned`, `AssertError.caught`, `beforeTheArm`), nested `HostAssert`s, typed trees and the environment in `HostSides.Ask` (inert at run), a pre-answered `TypedSide` |
| the database judge's batch (`DatabaseAsserts.Segment` → `VerdictBatch.Planned`) | roots in body order, each row's ruled row, the dialect's refusals, messages | appeal closures, exception objects, `DialectCapability`s, identity maps |
| the agreement judge (`AgreementAsserts.One`) | host/database verdict pairing, the held host verdict | identity maps |
| the engine-text comparison (`SqlTextVerdicts.Collected`) | the golden, our text, equality, a decline | the appeal closure (`TextAppeal`, 4 sites) and the environment |
| the held appeals (`Appeals`) | each appeal, after the run | `Supplier` closures holding the referee, a separate read planned only for the appeal, replay facts, fixture closures |
| the batch's judge | how a verdict row is read | a method reference wired at planning |
| out-of-plan channels | the system store's session per graph (`SystemDatabase`, keyed by `ModelContext`); the harness listener's fixture registry (filled at planning, run in `beforeRun`); `Execution.PlannedProgram` closures | no plan field at all |

And the run's value model uses the compiler's types (`TypedSide.returnType`; `Equality`, `PureAsserts`, `TdsCompare`,
`HostJudge` decide kinds with `Type`).

## 2. The design

### 2.1 Two plan types

The product's `ExecutionPlan` keeps what a product query or service needs: `Sequence`, `TdsResult`, `Keyed`,
`Constant`, `Update`, `Check` (a product write program makes one per statement before its last), `Effect`, `Setup`.
It loses `Verdicts`, `Verdict`, `Side`, `Branch`, `JudgingRules` and the verdict `Savepoint`.

A new data library `test_plan` (on `execution_plan`) holds
`TestPlan(parameters, steps: [TestStep], extensionsByVerdict: {verdictId: [Extension]}, extensionsByStep: {step: [Extension]})`:

```
TestStep = Run(ExecutionPlan.Node)                    -- a product step, as is
         | Verdicts(verdicts: [Verdict], fused: TdsResult?, judge: HOST|DATABASE|AGREEMENT, rules, savepoint?)
         | Branch(condition: Read, then: [TestStep], otherwise: [TestStep])
         | FrameRun(check: ExecutionPlan.Check)       -- a let's eager run, its origin LET
Verdict  = (id, name, arm, assertion: (listener, family?, wantEqual), base: [Read], message: Read?,
            judgment: Judgment?, shapes: [SideShape], failure: Failure?, root, orElse?, pairedWith?, raises)
Read     = Rows(TdsResult) | KeyedRows(Keyed) | Constant(columns, cells)  -- today's Side
```

Every verdict has an `id`, unique in its plan, counted in body order from 1 (the database judge's members with equal
names are equal records today, so ids replace every identity map). `root` is the id of the statement-root assert's
first verdict; `orElse` the ruled row's verdict id; `pairedWith` the other half's verdict id under the agreement judge.
A step is named by its index in `steps`, as today.

### 2.2 Judgment: the 23 comparisons as data

Each `HostJudgment` kind keeps its name and data fields; the others change form:

| Today | In the test plan |
|---|---|
| `SideFacts(ExprType info, classFqn, KindClass kind, flags, typeName)` | `SideFacts(type: TypeRef, classFqn, kind, flags, typeName)` |
| `TypeRef` (new) | `(name, multiplicity: [lower, upper], columns: [Column]?)` in the plan's spelling, as `Column.type` is |
| `KindClass` (`Primitive`, `Instance`, `Element`, `Enum`) | the same, tagged |
| `Keys(EqualityKeys e, a, flags, byClass)` | `KeyTree(classFqn, [Key(name, many, nested?, objectOnly, declaredClass)])` per side and per class; `byClass` for an `Any` slot measured first (it can hold every keyed class) |
| `Rows(..., ExprType expectedInfo, actualInfo, ...)` | `TypeRef`s |
| `RowArg.Literal(Object)` | a typed cell (Float included: a constant side spells none today) |
| `Unrolled(parts: [HostAssert], askCounts)` | nested `(assertion, judgment)` parts |
| `Messaged(inner, innerSides, unplanned)`, `Raise`, `AssertError(..., caught, inner)` | `Failure`s (2.4), nested parts |
| `EngineText(Collected(...), routed)` | `EngineText(decline?, routed)`: the two texts are the verdict's reads |
| `Eq`'s side facts | dropped: never read at run |

### 2.3 SideShape

`SideShape(kind: CONSTANT|GRAPH|TABULAR|KEYED|VALUES, type: TypeRef, schema: [Column]?, listCarrier)`, replacing
`PlannedSide`'s `ExprType`, `RelationType` and pre-answered value; a constant's value is its cells. Where a dynamic
pivot's column types come from names read at run, the shape carries the template-to-type rule as data.

### 2.4 Failure: an error known at planning

`Failure(exceptionClass, message, position?: (source, line, column), phase?, unjudgedReason?, beforeTheArm, where)`:

- `exceptionClass` from a closed list of the exception classes raised today (names, not categories: messages print
  the simple class name, the agreement judge's message names it, catch-by-type decides counting);
- `position`: the cause's source position (`assertError` reads line and column); `phase`: a `ModelException`'s;
- `beforeTheArm`: raised with no census and no listener report, as today;
- `where`: the planning site, for debugging (ruled 2026-11-08), stable across runs.

The run raises that class with that message at the verdict's turn. Lost: the planning-time stack trace.

### 2.5 The judges' types: exec's way

`exec` runs with no type model: the planner writes each column's facts (`type` name, `slot`, `precision`, `store`)
and exec reads cells by slot into plain Java values. The judges do the same: a judged value pairs its Java value with
the plan's type name; every decision made today from a declared type is made at planning and carried as a fact.
Step 1 starts by completing this list (known so far): `Equality` (carrier and effective kinds, integer-vs-decimal,
numeric family, the fine numeric kind per side and per column), `PureAsserts.classFqnOf`, `JudgePlans` shaping
(reported type, relation columns, multiplicity, list carriers), `HostJudge.rowsOf` and `restrictOne` (key trees),
`AssertErrorNative`'s judging, dynamic-pivot column types, `TypedSide.returnType` everywhere, and the referee's
interface and the spec harness, which read compiler types from `TypedSide`.

### 2.6 The connection description, and the system store's key

The plan stops holding the model's `ConnectionDefinition`. It holds its own records (ruled 2026-11-08):

```
Database = Declared(key, type, spec: Spec, auth: AuthRef)   -- the model's record, copied once at planning
         | Platform(type)
         | System(type, graphKey)                            -- the graph's content key: which system store
Spec     = InMemory | LocalFile(path) | EmbeddedH2(name, directory, autoServer) | Static(host, port, database)
         | Trino(...) | Snowflake(...) | Databricks(...) | BigQuery(...) | Spanner(...) | ...   (the model's fields)
AuthRef  = None | VaultUserPassword(userRef, passwordRef) | KeyPairRef(...) | Kerberos(...) | OAuth(key, scope) | ...
```

- **Secrets stay references.** An authentication holding a literal secret is refused at planning, by name (no
  fallback); `ApiToken(apiToken)` is checked against upstream before this lands.
- **Exec's connector renders the URL at open**, as today: URL formats and driver settings are the connector's (D72:
  exec alone makes sessions). It fills what only the run knows: a private in-memory database's fresh name, an embedded
  server's port, a file path.
- **`key` and `kind` are the routing identity**: statements route and sessions share by which database a target is.
  `System(graphKey)` replaces `SystemDatabase`'s `ModelContext`-keyed sessions, so the system store's session is
  exec's, keyed by plan data.

### 2.7 The edge plans, under the compatibility rules, into a typed extension point

The edge cannot be a pure renderer (the independent review, confirmed): an appeal's rows are a SEPARATE read planned
only for the appeal from the typed body (`SqlTextVerdicts.java:713-719`: a `toSQLString` test never runs its query;
only the appeal does), the referee decodes enums with the model, the in-list seed comes from the typed query and its
lets, and the ledger texts are authored texts or the referee dialect's. So (ruled 2026-11-08):

- `test_plan` declares one interface, `Extension`; the test plan holds extensions by verdict id (an appeal's) and by
  step index (an effect's or a seed's ledger texts).
- Under `COMPILED_PURE_COMPAT` alone, `testplan` calls the edge's planning hook with the typed body; the edge plans its
  needs as its OWN typed records implementing `Extension`: the appeal's reads (product nodes), the replay facts
  (ordered, sort keys, extent subset, page population, provenance, in-list seed, temps, plan-golden bindings), the
  stores whose fixtures it provides, the ledger texts of effects and seeds.
- Under `LITE` nothing is planned for the edge (E3: the product does not change what it plans to serve compatibility).
- JSON: each extension record carries its type tag, read through a codec the edge registers (the `ServiceLoader`
  pattern `exec`'s plug-ins use). G5 holds: `test_plan` declares, the edge implements.
- After the run, the edge's referee decides each appeal from the plan, its extensions and the model (the edge may use
  the model; the run may not).
- Values a rule renders (what `sqlRemoveFormatting()` returns under the compatibility rules) are rendered at planning
  by the renderer the rules name and kept as plain strings.

A `Read` carries no fact for the referee: everything only the referee reads (order, sort keys, extent subset, page
population, provenance, the stores to provide) lives in the edge's extension, planned under the compatibility rules
alone.

### 2.8 The runner: exec is the one walker

Exec walks every program (G1: one runner) and calls back for the steps it does not know (the independent review's
recommendation, ruled 2026-11-08):

```
exec:      record Program(parameters, steps: [ProgramStep])
           ProgramStep = Node(ExecutionPlan.Node, origin?) | Callback(id, declared: [ExecutionPlan.Node])
           PlanRunner.program(Program, values, sessions, Callbacks, ProgramSink)
           interface Callbacks { void step(int id, OpenProgram open); }
           interface OpenProgram {
               Rows read(ExecutionPlan.Node read, origin?); -- on its routed session, inside the program
               void run(ExecutionPlan.Node step);           -- a product step inside the program (a branch's arm)
               <T> T guarded(Savepoint sp, Work<T> work);   -- under exec's Savepoint(set, rollbackTo, release)
               boolean insideATransaction();
               Map<String, Checked> values();               -- live values: trace stamps, write counts
           }
```

- `testexec` lowers a `TestPlan` to a `Program`: each `Run` a `Node`; each `FrameRun` a `Node` whose origin is `LET`;
  each `Verdicts` and `Branch` a `Callback` whose `declared` nodes are every statement it may send (the fused
  statement, wrappers, sides, messages, a branch's condition and arms). A statement's origin (today the sink's
  `originOf` and its deferred texts' origins) is the step's or the read's, passed to exec as data.
- Exec keeps everything that needs all statements or exec's internals: the program's shape (one target, the system
  session, the snapshot, the constants-alone path) computed over nodes and declared nodes; sessions and routing; the
  transaction bracket around every step, callbacks included; statement origins; write marking; the reader's guard,
  built from declared reads; the savepoint statements.
- `testexec`'s handler runs a verdict group (reads through the handle, judging, the root loop) and a branch (its
  condition through the handle, the taken arm's steps by `run` or its own handler).
- `ProgramSink`'s `verdict`, `sides` and `fused` hooks are deleted; the sink keeps the answer's columns and batches.
- A test body with no assert still runs through `testexec`: the answer's cardinality, the seeds and the ledger are
  the test road's.

### 2.9 The libraries

| Library | Holds | May not depend on |
|---|---|---|
| `execution_plan` | the product plan, its JSON, the connection description | any planning or runtime library |
| `test_plan` (new) | the test plan, its JSON, `Extension` | the same, and the edge |
| `planner` / `testplan` | planning: the product's / the test's: the body's program, ALL assert planning, the edge's hook | `exec`, `testexec` |
| `exec` / `testexec` (new; today's `judge` becomes part of it) | running: the product's, with the callback / the test's handler, the judges' comparisons, the root loop, the agreement rule | `testexec` may not depend on compiler, lowering, resolver, `planner`, `testplan` |
| the edge (P2's package, created here as far as the appeal and the ledgers need) | the hook, its extension records and codec, the referee, the engine-text renderer | the product never depends on it |
| `diagnostics`, `error`, `json` (lower) | what both halves use: the census counters (`Census` moves out of exec), the asserts' message texts, the strict JSON reader | |
| front doors | `Execution.run` (product) and `Execution.runProgram` (test) in separate targets | `server_lib` reaches the product door only |

Where today's `driver` goes:

| Today | Goes to |
|---|---|
| `HostAsserts`, `DatabaseAsserts`, `AgreementAsserts`, `AssertVerdicts`' arms, `DatabaseJudge` (planning), `HostSides`' planning, `JudgePlans`' planning, `AssertBranch`, `LiteralFold`, `SeedSqlForms`, `VerdictArm`, `BodyDriver` (the environment), `SqlTextVerdicts`' planning half, `VerdictBatch`'s planning half, `BodyRouter`'s classify-and-plan | `testplan` |
| `BodyRouter$Moved`, `HostJudge`'s judging, `AgreementArm`, `AssertErrorNative`'s judging, `DatabaseJudge.verdictOf`, `AssertVerdicts.judgedAtItsStep`/`heldAtItsStep`/`ok`/`fail`, `SqlTextVerdicts.judge`, `JudgePlans.shape`/`host`, `HostSides.shapedBase`, `VerdictBatch.Planned.judge` (the root loop) | `testexec` |
| `HostJudgment`, `KindClass` (as data) | `test_plan` |
| `SystemDatabase` | exec, keyed by `Database.System.graphKey` |
| `Appeals`, `SqlTextVerdicts`' appeal and rows leg, `JudgeJson`'s compatibility grammar, the referee, the recorder | the edge |
| `BodyRouter`'s sequencing | `Execution.runProgram` |
| `RunReader` | deleted (the runner reads every side off the plan) |

Pinned in `tools/deps/core-layers.txt`; ArchUnit R1-R4 hard (R3: only the front doors see both halves).

### 2.10 The guards (ruled 2026-11-08: A and B now, D later)

1. **The plan's types are data.** A test walks every type reachable from `ExecutionPlan`, `TestPlan` and every
   `Extension` implementation on the classpath (record components, sealed subtypes, the type arguments of `List`,
   `Set`, `Map`): each must be a primitive or its box, `String`, `BigDecimal`/`BigInteger`, an enum, or a record or
   sealed interface of the plan's own libraries, the edge's extension records, or the SQL tree (`com.legend.sql`,
   pure data by AGENTS.md 3a). It fails on any functional type, `Throwable`, `Object`, a wildcard, or a type of a
   planning or runtime library. Introduced with the two plan types (step 3), its violations pinned shrink-only.
2. **The round trip and the walls (A).** `PlanJson`/`TestPlanJson` round-trip every plan on every lane; `testexec`'s
   entry takes a `TestPlan` and sessions, and its library cannot reach a planning library.
3. **One lane on the decoded plan (B).** The DuckDB corpus lane runs only the plan read back from its JSON: in report
   mode from step 3, a hard check from step 9.
4. **Out-of-plan channels removed by name**, since A and B cannot see them: the `ModelContext`-keyed system sessions
   (2.6), the listener's fixture registry (the edge's extension names the stores), the `PlannedProgram` closures
   (plans), the environment inside `Collected` (gone with 2.7).
5. **Later, D:** a planner-free runner binary runs plans the planning pass wrote to files (G11's gate; the only form
   that also catches state shared through statics).

## 3. The rulings (the user, 2026-11-07/08)

1. Two plan types (2.1). 2. Failures known at planning are data, with `where` (2.4). 3. The judges take exec's way
with types (2.5). 4. The guards: A and B now, D later, "no code in the plan, it's all data" (2.10). 5. The edge plans,
under the compatibility rules, into a typed extension point (2.7; the pure renderer and the generic section were
withdrawn after the review). 6. The plan's own connection description, the URL rendered by exec at open (2.6). 7. Exec
is the one walker and calls back for test steps (2.8). 8. No `driver` package: `testplan`/`testexec` beside
`planner`/`exec` (2.9).

## 4. The order of work

Each step lands as one or more commits on the branch, held to the baseline of 2026-11-07 (GATES: every lane green at
`69ba09c2` with the H2 register fix).

0. **Preparation, no behaviour change**: verdict ids; `Census` to `diagnostics`; step 1's type-decision list completed
   (2.5); the `ApiToken` check against upstream.
1. **The judges' types the exec way** (2.5), alone, with the referee's interface and the spec harness.
2. **Failures as data** (2.4).
3. **Two plan types and the callback** (2.1, 2.8): `TestPlan`, `Program`/`Callbacks`/`OpenProgram` in exec, the
   test steps out of the product's plan, runtime, planner and options; guard 1 pinned; guard 3 in report mode.
4. **Judgments and side shapes in the test plan** (2.2, 2.3).
5. **The database judge's root loop from the test plan**; `VerdictBatch.Planned`'s identity maps deleted; the batch's
   judge a plain function.
6. **The agreement pairing** in the test plan.
7. **The sidecar into the test plan, and the plan-owned descriptions** (2.6): origins and each group's judge; the
   connection description; `System(graphKey)`.
8. **The edge's extension point** (2.7): the edge package created as far as needed; its hook, records and codec; the
   appeals' reads and facts by verdict, the ledger texts by step; the referee from the plan and its extensions; the
   closures deleted; the out-of-plan channels removed (2.10.4); `TestDoor.Planned` is the test plan.
9. **The guards complete** (2.10): guard 1 at zero; guard 3 hard on the DuckDB corpus lane.
10. **The move** (2.9): `testplan`, `testexec`, the edge's libraries; `driver` deleted; the front doors' targets split;
    deps pinned; R1-R4 hard.
11. **All lanes and an audit** (the P1 milestone).

## 5. Verification

- **Each change**: the touched core suites (driver and judge under the three judges, exec, integration, guardrails,
  census, architecture) and the fast list.
- **Each step**: the DuckDB corpus under both judges (every roster, register and failure line identical to the
  baseline); a step touching H2-specific, PCT or stress code also runs that lane.
- **Step 1 and step 3**, the riskiest: every lane before the next step starts.
- **The milestone (step 11)**: every lane one at a time, an independent audit, the GATES record. P1 lands through a
  PR, merged only on the user's word.

## 6. The risks, and what catches each

| Risk | Caught by |
|---|---|
| a type decision moved to planning changes a verdict (step 1) | every lane, identical verdicts, before step 2 |
| the callback changes a transaction, a routing or an origin (step 3) | every lane before step 4; the store-write, savepoint and system-routed core suites |
| a failure's class, message or position drifts (step 2) | the core suites that pin exception types and messages; the harness's printed class names |
| an `Any` slot's key trees make plans large (step 4) | measured at step 0; restricted to the classes the slot's values can be, if large |
| the edge's planned reads differ from today's appeal reads (step 8) | the DuckDB and H2 corpora's referee census and appeal outcomes, identical |

## 7. An example: `projection::limit::testSimpleLimit`

The test (legend-engine's, the DuckDB corpus, compatibility rules):

```pure
let result = execute(|Person.all()->project([#/Person/firstName!name#])->limit(0), simpleRelationalMapping, ...);
assertEquals('select top 0 "root".FIRSTNAME as "name" from personTable as "root"', $result->sqlRemoveFormatting());
assertSize($result.values.rows, 0);
let result2 = ... ->limit(1) ...;
assertEquals('select top 1 ...', $result2->sqlRemoveFormatting());
assertSize($result2.values->at(0).rows, 1);
```

Printed with `-Dlegend.diagnostics=plan-json` (2026-11-07): two setup programs (80 KB and 100 KB of JSON) and the
body (10 KB). Below, the body; each statement's `target` and `tree` elided.

**Today, the host judge's plan** -- which rows each assert reads, not what to compare:

```json
{"_type": "sequence", "steps": [
 {"_type": "check", "sql": {"statement": "SELECT t0.FIRSTNAME AS name\nFROM personTable AS t0\nLIMIT 0"}},
 {"_type": "verdicts", "rules": "compiledPureCompat", "fused": null, "verdicts": [
   {"name": "meta::pure::functions::asserts::assertEquals", "arm": "assertEquals", "wrapper": null, "base": [
     {"_type": "constant", "columns": [{"name": "value", "type": "String"}],
      "cells": [["select top 0 \"root\".FIRSTNAME as \"name\" from personTable as \"root\""]]},
     {"_type": "constant", "columns": [{"name": "value", "type": "String"}],
      "cells": [["select top 0 \"root\".FIRSTNAME as \"name\" from personTable as \"root\""]]}]}]},
 {"_type": "verdicts", "rules": "compiledPureCompat", "fused": null, "verdicts": [
   {"name": "meta::pure::functions::asserts::assertSize", "arm": "assertSize", "wrapper": null, "base": [
     {"_type": "constant", "columns": [{"name": "value", "type": "Integer"}], "cells": [["0"]]},
     {"_type": "rows", "result": {"_type": "tdsResult", "shape": "RELATION",
      "columns": [{"name": "name", "type": "String", "slot": "VARCHAR", "store": "VARCHAR(200)"}],
      "sql": {"statement": "SELECT t0.FIRSTNAME AS name\nFROM personTable AS t0\nLIMIT 0"}}}]}]},
 ... the same three steps for result2 (LIMIT 1) ...
]}
```

**Today, beside it** (a probe's print; typed trees, environments and closures shown as markers):

```
assertSize   -> HostAssert(listenerName=...assertSize, family=assertSize, wantEqual=true,
                  check=Check(asks=[Ask(arg=<TypedSpec TypedCInteger>, hook=<closure>, ...), Ask(arg=<TypedSpec ...>)],
                              judgment=Size(envelopeValuesRead=false)), beforeTheArm=null)
   sides        PlannedSide(kind=CONSTANT, answered=Scalar(value=0, returnType=INTEGER))
                PlannedSide(kind=TABULAR, wrapped=WrappedSide(plan=SqlSelect(...), shapeInfo=ExprType(...)), schema=...)
assertEquals -> HostAssert(..., judgment=EngineText(collected=Collected(golden="select top 0 ...",
                  ours="select top 0 ...", textEqual=true, appeal=<closure>, env=<ExecEnv>), routed=true))
```

**Proposed, the same verdicts in the test plan, nothing beside it:**

```json
{"_type": "verdicts", "judge": "host", "rules": "compiledPureCompat", "fused": null, "verdicts": [
  {"id": 2, "name": "meta::pure::functions::asserts::assertSize", "arm": "assertSize", "root": 2,
   "assertion": {"listener": "meta::pure::functions::asserts::assertSize", "family": "assertSize", "wantEqual": true},
   "judgment": {"_type": "size", "envelopeValuesRead": false},
   "base": [ ...the same two reads as today... ],
   "shapes": [{"kind": "CONSTANT", "type": {"name": "Integer", "multiplicity": [1, 1]}},
              {"kind": "TABULAR", "type": {"name": "meta::pure::metamodel::relation::Relation", "multiplicity": [1, 1]},
               "schema": [{"name": "name", "type": "String", "multiplicity": {"lower": 1, "upper": 1}}]}]}]}

{"_type": "verdicts", "judge": "host", "rules": "compiledPureCompat", "fused": null, "verdicts": [
  {"id": 1, "name": "meta::pure::functions::asserts::assertEquals", "arm": "assertEquals", "root": 1,
   "assertion": {"listener": "meta::pure::functions::asserts::assertEquals", "family": "assertEquals", "wantEqual": true},
   "judgment": {"_type": "engineText", "decline": null, "routed": true},
   "base": [ ...the golden and our text, the same two constants as today... ],
   "shapes": [{"kind": "CONSTANT", "type": {"name": "String", "multiplicity": [1, 1]}},
              {"kind": "CONSTANT", "type": {"name": "String", "multiplicity": [1, 1]}}]}]}
```

Here the two texts are constants and equal, so the edge plans nothing for the appeal; where our text differs or is
read at run, the edge's extension for verdict 1 carries the appeal's reads and replay facts (2.7).

**The database judge** today plans one step: a fused statement that computes all four verdicts and their messages in
the database (`WITH frame_result AS MATERIALIZED (... LIMIT 0), ... SELECT 0 AS __ix, <verdict>, <expected>,
<actual>, <unjudged>, <lenient>, <holds>, <message> FROM __p_0 UNION ALL ...`), its four verdicts named only
`assertEquals`, `assertSize`, `assertEquals`, `assertSize`. Proposed: each carries its `id`, `assertion` and `root`;
the statement is unchanged.

**Noted, not in this step** (G3, plan size): the host judge runs `SELECT ... LIMIT 0` twice (the let's check and
`assertSize`'s read; the database judge once); every statement repeats its whole target; the setup programs carry
each statement as tree and text.

## 8. The independent review (2026-11-08), and where each finding is answered

Three blockers: the edge as a pure renderer (answered by 2.7), the ledger texts (2.7: the edge plans them), the
runner split (2.8: exec walks and calls back; a body with no assert stays on the test road). Should-fix: `Check` is a
product node (2.1); `Failure`'s class, position, phase and flag (2.4); the layering snags (2.9: the mixed classes'
run halves named, `Census` moved, `SystemDatabase` keyed by plan data, the front doors' targets split); the system
store's identity (2.6); the run-time type decisions (2.5, step 0's list); the order (4: the edge before the move,
guard 3 in report mode from step 3); decision 4's blind spots (2.10.4). Notes folded in: 23 kinds, `KindClass.Enum`,
`Eq`'s unused facts, `ExecuteOptions.recorder`, `PlanMaker.program`'s test branches, the referee-only facts under
`LITE` (2.7: none planned).
