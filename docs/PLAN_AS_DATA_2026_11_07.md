# The plan as data, totally (the design, 2026-11-07/08)

> **Status: every decision RULED by the user (2026-11-08); a focused independent check of the three parts no review
> has seen (the runner's callback, the edge's extension point, the connection description) precedes step 0.** This
> document replaces the earlier drafts and their appendix; its history is the branch's commits (`83f68e44` to
> `982929bc`). Steps 0-8 are implemented (slices 1 and 2, 2026-11-08); section 10 records what each did.

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
Verdict  = (id, name, arm, assertion: (listener, family?, wantEqual), base: [Read], wrapper: Node?, message: Read?,
            judgment: Judgment?, shapes: [SideShape], failure: Failure?, root, orElse?, pairedWith?, raises)
Read     = Rows(TdsResult) | KeyedRows(Keyed) | Constant(columns, cells)  -- today's Side; a constant's cells are text
                                                                           of any type, decoded by testexec by type name
```

Every verdict has an `id`, unique in its plan, given as it is planned by one counter per body
(`judge/VerdictIds`, in the body's environment, shared by every planner of the body; built in step 0, 2026-11-08: the
database judge's members with equal names were equal records, so ids replace every identity map). A verdict rebuilt
from another -- a raising verdict's statement, a caller's message carried, the agreement judge's combined verdict --
keeps the id of the one it replaces. `root` is the id of the statement-root assert's
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
interface and the spec harness, which read compiler types from `TypedSide` (step 0, 2026-11-08: the referee's
interface takes `TypedSide` and the model -- the model stays, the edge may use it; the harness's `ReplayReferee`
reads a column's compiler type as `Type.EnumType` to decode an enum column, which becomes the plan's type name and a
lookup in the model).

### 2.6 The connection description, and the system store's key

The plan stops holding the model's `ConnectionDefinition` (ruled 2026-11-08) and holds its own copy, FAITHFUL: the same
variants under the same names with the same fields, `qualifiedName` included (every refusal names it, and the
connectors' refusal messages print the class names), `LocalH2` kept as its own specification (DuckDB and SQLite refuse
it, H2 opens it as in-memory), and every authentication kept distinct (`NoAuth` is not `DefaultH2`/`TestAuth`: Trino
builds its URL from the test user; Mongo's `UsernamePassword` has a literal user and a `kind:key` password reference
read at open). A reduced summary would lose these (the focused check, 2026-11-08).

```
Database = Declared(Connection)        -- the plan's own record: the model's fields, copied once at planning
         | Platform(type)
         | System(type, graphKey)      -- which system store
```

- **No secret in the plan**: authentication holds references, as upstream's does (`ApiToken(apiToken)` is a vault key
  upstream: `Vault.INSTANCE.getValue(authStrategy.apiToken)`; `UsernamePassword`'s password is a `kind:key`
  reference). Where and when a credential is refused stays as today (at open), so no error moves.
- **Exec's connector renders the URL at open**, as today (D72). The only parts filled at run today are private
  in-memory names (`plan_<hash>_<n>`, `private<n>`).
- **The identity of a database stays the WHOLE target** (its database, session statements and setup), as today:
  `sameTarget`, `UserBeside` and the shared sessions (`PlanSessions.openShared`, keyed by the target) compare it all, so
  runtimes with different seeds never share one database.
- **`graphKey` is a hash of the system store's planned seed steps, per engine** (the seed is every store table's rows
  in the engine's dialect, so equal content means an equal store). Exec owns the system store's sessions, keyed by it,
  with an EXPLICIT lifetime (today a `Cleaner` closes them when the graph is collected; a string key has no such
  signal): the sessions source holds them and closes them when it closes. Two graphs of equal content share one
  store: their concurrent use is checked before this lands. A server engine's system connections stay run
  configuration of the sessions source, not plan data.

### 2.7 The edge plans, under the compatibility rules, into a typed extension point

The edge cannot be a pure renderer (the independent review, confirmed): an appeal's rows are a SEPARATE read planned
only for the appeal from the typed body (`SqlTextVerdicts.java:713-719`: a `toSQLString` test never runs its query;
only the appeal does), the referee decodes enums with the model, the in-list seed comes from the typed query and its
lets, and the ledger texts are authored texts or the referee dialect's. So (ruled 2026-11-08), in three parts:

**At planning: the edge's hook, called by testplan's arms, with testplan's ids.**
- `test_plan` declares one interface, `Extension`; the test plan holds extensions by verdict id and part index (an
  appeal's: an `Unrolled` verdict can hold several engine-text parts) and by step index (an effect's or a seed's
  ledger texts).
- The arms keep computing an appeal's inputs where they do today (the route from the golden, the mapping, the facts,
  the population, the temps, the plan bindings, the `appealOn` environment: about 2,000 lines that stay in
  `testplan`); under `COMPILED_PURE_COMPAT` alone they hand the edge's hook a typed REQUEST with the verdict id and
  part, and the edge plans its own typed records implementing `Extension`: the appeal's reads (product nodes, with
  their failures and shapes as data), the replay facts, the stores whose fixtures it provides. Effect and seed steps
  hand the hook their ledger texts with their step index (consecutive effects merge into one step; established seeds
  are de-duplicated: the ids are testplan's, never derived from the typed body alone).
- Under `LITE` nothing is planned for the edge (E3).
- JSON: each extension record carries its type tag, read through a codec the edge registers (the `ServiceLoader`
  pattern `exec`'s plug-ins use). G5 holds: `test_plan` declares, the edge implements.

**At run: the run RECORDS, it does not decide.** Some of an appeal's inputs exist only at run: our text and a deferred
golden read at the verdict step, and under the database judge our text is the row's actual cell. So the hold that
`Appeals` does today splits: `testexec` keeps a RUN RECORD, as data -- each held appeal's verdict id and part, the
texts as read, whether they were equal, the order of the holds, and whether a write ran after the first hold -- and
keeps the listener's reports back as today (the `Keeping` listener). The run record is the run's output, never planned
and never fed back into a run.

**After the run: the edge decides** each appeal from the plan, its extensions, the run record and the model (the edge
may use the model; the run may not), in body order, the first failure the body's, as `Appeals.decide` does today.

**The ledger** (the referee's record of the statements a session ran): only its TEXTS can be planned. Its entries are
run state -- a seed skipped as already established, a seed's answered rows, a script that failed part-way, the
entries of earlier bodies on the same session, the harness's own fixture entries -- so the edge's recorder builds them
at run from exec's step events (`ProgramSink.seeded`, `effectRan`, `writing`, kept for it, 2.8) and the planned texts.
A recorder IMPLIES the compatibility rules (today it is set independently, but only the compatibility corpus records;
`BodyRouterParityTest` runs `LITE` with a recorder it no longer asserts on, and changes with this).

Values a rule renders (what `sqlRemoveFormatting()` returns under the compatibility rules) are rendered at planning by
the renderer the rules name and kept as plain strings. A `Read` carries no fact for the referee: everything only the
referee reads lives in the edge's extension, planned under the compatibility rules alone.

### 2.8 The runner: exec is the one walker

Exec walks every program (G1: one runner) and calls back for the steps it does not know (the independent review's
recommendation, ruled 2026-11-08; the focused check's fixes folded in):

```
exec:      record Program(parameters, steps: [ProgramStep])
           ProgramStep = Node(ExecutionPlan.Node, origin?) | Callback(id, declared: [ExecutionPlan.Node])
           PlanRunner.program(Program, values, sessions, Callbacks, ProgramSink)
           interface Callbacks { void step(int id); }                 -- called at the callback's place
           interface OpenProgram {                                    -- ONE per program, valid until it ends
               Rows read(ExecutionPlan.Node read, origin, bindings);  -- routed, inside the program; live values
                                                                      -- merged with the call's bindings
               Raised readRaising(ExecutionPlan.Node read, origin);   -- an assertError's side: its rows, or the
                                                                      -- database's error as DATA (message, line,
                                                                      -- column), never a JDBC type
               <T> T guarded(Savepoint sp, Work<T> work);             -- exec's Savepoint(set, rollbackTo, release):
                                                                      -- on failure rolls back, then rethrows the
                                                                      -- original exception unwrapped
               boolean insideATransaction();
           }
           ProgramSink keeps: columns, batches, seeded(step, answeredRows), effectRan(step), writing(step)
```

- `testexec` lowers a `TestPlan` to a `Program`: each `Run` a `Node`; each `FrameRun` a `Node` whose origin is `LET`;
  each `Verdicts` and `Branch` a `Callback` whose `declared` nodes are every statement it may send (the fused
  statement, every verdict's wrapper -- keyed ones included -- sides, messages, a branch's condition and its arms'
  statements).
- The handle is opened once per program and stays valid until the program ends: the database judge judges a segment
  at its last group (a later step) and reads earlier verdicts' messages then; the agreement judge holds a message
  reader across steps. The reader's guard covers every declared read of the whole program.
- Declared nodes add statements, the snapshot and the system target to the program's shape, never result steps (the
  shape's top-level rule would refuse a second result). Exec keeps the shape, sessions and routing, the transaction
  bracket around every step (callbacks included), statement origins, write marking, the savepoint statements.
- Origins keep today's layering (`enterIfUnmarked`): verdict and branch steps run under `BODY` only when the database
  judge alone judges; a branch's condition reads under `STATEMENT`; the origin is passed with each read as data.
- A constant read is decoded by `testexec` from its cells by type name (exec's `constantRows` decodes only strings and
  booleans and is not used for it).
- The constants-alone path (no target) folds into the main path: its on-demand reader is unused (every carried
  statement side gives the program a target), checked when step 3 lands.
- `ProgramSink`'s `verdict`, `sides` and `fused` hooks are deleted; `seeded`, `effectRan` and `writing` stay (the
  ledger and the run record need them).
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
| `SystemDatabase` | exec, keyed by `Database.System.graphKey`, its lifetime the sessions source's |
| `Appeals`' hold and the `Keeping` listener (the run record) | `testexec` |
| `Appeals.decide`, `SqlTextVerdicts`' rows leg, `JudgeJson`'s compatibility grammar, the referee, the recorder | the edge |
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
   (2.6), the listener's fixture registry (the edge's extension names the stores; whether to provide one stays the
   harness's run state), the `PlannedProgram` closures (plans), the environment inside `Collected` (gone with 2.7).
   What the run hands the edge after it (the run record, the ledger's entries) is the run's OUTPUT, data, never
   planned and never read back into a run.
5. **Later, D:** a planner-free runner binary runs plans the planning pass wrote to files (G11's gate; the only form
   that also catches state shared through statics).

## 3. The rulings (the user, 2026-11-07/08)

1. Two plan types (2.1). 2. Failures known at planning are data, with `where` (2.4). 3. The judges take exec's way
with types (2.5). 4. The guards: A and B now, D later, "no code in the plan, it's all data" (2.10). 5. The edge plans,
under the compatibility rules, into a typed extension point (2.7; the pure renderer and the generic section were
withdrawn after the review). 6. The plan's own connection description, the URL rendered by exec at open (2.6). 7. Exec
is the one walker and calls back for test steps (2.8). 8. No `driver` package: `testplan`/`testexec` beside
`planner`/`exec` (2.9).

## 3a. Ruled after the reviews (the user, 2026-11-08)

1. **`PlanRunner` split into its product half and its program half, the program half its own Bazel target
   `//core:exec_program`: ruled ("Yes on its own build target", 2026-11-08).** What the target adds over the file split
   is enforcement -- the product's binaries (everything depending on `//core:exec`)
   no longer carry the ~970 lines of test walking; `:exec` cannot compile against a program class; the layer pin
   names `testexec` as its only user. It stays in package `com.legend.exec` (as `mongodb_connector` does), its files
   under `src/main/java/com/legend/exec/` (the source-scanning guards hard-code that root); package-private across
   the split is enforced by the pin and ArchUnit, not by Java. The rejected alternative, the walker in `testexec` on a
   public exec toolkit, would reverse ruling 7 and G1's wording, rewrite ~970 lines against a new API, and still leave
   the statement-sending primitives in exec (only exec may send statements).
2. **Landing in three slices, stacked branches: ruled ("Slices yes stacked").** Three stacked branches, each cut from
   the one before, each with its lanes and an audit, each merged only on the user's word: slice 1 after step 3,
   slice 2 after step 8, slice 3 after step 10. **The stop rule**: a verdict change at step 1 that is not explained
   quickly stops the work and goes to the user.
3. **A recorder implies the compatibility rules: ruled** (2.7).
4. **D2 keeps the shape built** (ruled; the goals' D2 text now describes it).

## 4. The order of work

**How it runs (the user, 2026-11-08): "keep going don't stop until we are done, do fast dev loops, hold questions until
the end so you don't have to stop, make sure do audits and full gates at major milestones but batch things together."**
Each change: the touched suites and the fast list. Each step: the DuckDB corpus on both judges. Each slice's end
(steps 3, 8, 10): every lane one at a time, an independent audit, the GATES record. Questions that arise are written
to section 10 and asked at the end; a decision needed to proceed is taken by the design's own rules and recorded there.

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
8. **The edge's extension point** (2.7): the edge package created as far as needed; its hook, called by the arms with
   a typed request and testplan's ids; its records and codec; the run record in `testexec`; the edge's decision after
   the run from the plan, its extensions and the run record; the ledger from planned texts and exec's step events; a
   recorder implies the compatibility rules; the closures deleted; the out-of-plan channels removed (2.10.4);
   `TestDoor.Planned` is the test plan.
9. **The guards complete** (2.10): guard 1 at zero; guard 3 hard on the DuckDB corpus lane.
10. **The move** (2.9): `testplan`, `testexec`, the edge's libraries; `driver` deleted; the front doors' targets split;
    deps pinned; R1-R4 hard.
11. **All lanes and an audit** (the P1 milestone).

### Step 0's starting list for step 1 (the bytecode call graph, 2026-11-08)

Every method the run reaches that names a compiler type (calls, fields, casts, instanceof), from the driver's and the
judges' compiled classes; step 0 completes it with the referee's interface, the spec harness and the decisions the
review named (dynamic-pivot types, list carriers, numeric kinds per column, `Any` slots' keys, Float literals).

```
AssertErrorNative
    AssertErrorNative.judged  [Type$Primitive]
AssertVerdicts
    AssertVerdicts.ok  [Type$Primitive]
BodyRouter$Moved
    BodyRouter$Moved.holdingTheWalksContract  [Multiplicity$Bounded]
Equality
    Equality.carrierKind  [Type$Primitive]
    Equality.effectiveKind  [Type$PrecisionDecimal, Type$Primitive]
    Equality.grid  [Type]
    Equality.integerAndDecimal  [Type$Primitive]
    Equality.isNumericKind  [Type$Primitive]
    Equality.same  [Type$Primitive]
HostJudge
    HostJudge.lambda$restrictOne$0  [EqualityKeys$Key]
    HostJudge.messagedOf  [AssertMessages]
    HostJudge.restrictOne  [EqualityKeys, EqualityKeys$Key]
    HostJudge.rowsOf  [ExprType]
HostJudgment$Keys
    HostJudgment$Keys.of  [EqualityKeys]
HostJudgment$SideFacts
    HostJudgment$SideFacts.type  [ExprType]
JudgePlans
    JudgePlans.keyedShape  [ExprType, Type$Column, Type$RelationType]
    JudgePlans.lambda$keyedShape$0  [Type$Column]
    JudgePlans.reportedType  [Type$ClassType, Type$Primitive]
    JudgePlans.shape  [ExprType, Type$RelationType]
    JudgePlans.tabularShape  [ExprType, Multiplicity$Bounded, Type$Column, Type$RelationType]
    JudgePlans.valuesShape  [ExprType, Multiplicity, Multiplicity$Bounded, PlatformTypes, Type, Type$ClassType]
PureAsserts
    PureAsserts.classFqnOf  [Type$ClassType, Type$GenericType]
SqlTextVerdicts
    SqlTextVerdicts.actualText  [Type$Column, Type$RelationType]
    SqlTextVerdicts.ok  [Type$Primitive]
```

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
| an `Any` slot's key trees make plans large (step 4) | MEASURED at step 0 (2026-11-08, a print-only probe that did not land): 52 key trees on the DuckDB corpus under the host judge, the largest `DynaFunction`'s (159 classes, 357 keys; 9 verdicts) and `FunctionCall`'s and `SingleColumn`'s (69 classes, 176 keys; 8); 52 on PCT DuckDB and core's driver and integration suites, the largest `List`'s (45 classes; 4 verdicts); every other one small or empty. Carried as data they add tens of kilobytes to a handful of plans; step 4 chooses between carrying them and restricting them to the classes a slot's values can be, measured |
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

## 9. The focused check (2026-11-08) of 2.6, 2.7 and 2.8, and where each finding is answered

Verdict: all three executable with fixes, two of them changing the design itself (spot-checked by the author: the
raising verdict's bindings, `PlanRunner.java:900-914`; the shared sessions keyed by the target; the recorder set
independently of the rules).
- **The runner (2.8)**: the raising verdict needs bindings and a raising read returning the error as data; `Verdict`
  keeps its `wrapper`; one handle per program, its guard over every declared read; declared nodes join the shape as
  statements only; `seeded`/`effectRan`/`writing` stay; constant reads decoded by testexec; `guarded` rolls back and
  rethrows unwrapped; origins keep their layering; `run(node)` dropped (no caller: a branch's arms hold only verdict
  groups and branches).
- **The edge (2.7)**: appeals take inputs only the run reads -- the run RECORDS them as data and the edge decides
  after the run; the hook is called by the arms with testplan's ids (verdict and part, step), the arms keeping their
  computation; the ledger's entries are built at run from exec's step events, only its texts planned; a recorder
  implies the compatibility rules. Noted for later, unchanged here: under `LITE` an engine-text arm still requires a
  registered referee at planning.
- **The connection (2.6)**: the identity stays the whole target (key and kind alone would let different seeds share a
  database); the copy is faithful (`LocalH2`, class names, `qualifiedName`, every authentication); `graphKey` is a hash
  of the planned seed steps per engine with an explicit lifetime; refusal timing unchanged.

## 10. Questions held for the end, and decisions taken on the way (2026-11-08 on)


**Decisions taken on the way** (each by the design's own rules; none changes a ruling):

- **Step 1 (2026-11-08, `0a1edf63`)** left three reads for later steps, as the design orders them: `JudgePlans`' shaping
  and `HostJudge`'s `Rows` still read the compiler's `ExprType` at run (the side shapes and the judgments become data at
  step 4); `PureAsserts` reads `ClassLayouts`' synthetic type and id names (string constants of the compiler's layout;
  they move with the libraries at step 10). The referee's harness decides an enum column from the model by the
  column's type name at its builder (`ReplayReferee`), where it read the compiler's `Type.EnumType` from the side.
- **Step 1's lanes**: every lane run on step 1 alone before step 2 (it changes how numbers compare), not only at the
  slice's end.
- **The test plan's Java package is `com.legend.testexecutionplan`** (library `//core:test_plan`, created at step 2):
  `testplan` is the test's planner by the ruling, so the data takes the product's pattern (`executionplan` is the
  product plan's data, `planner` its planner, `exec` its runner).
- **Step 2's `Failure` has no position.** Planning sends no statement, so no planned error carries a database's raise
  and its source position (the position rides a cause only the database raises at run; `assertError`'s line and
  column checks read it from the run's own errors, unchanged). The record keeps `kind` (the closed list of classes),
  `message`, a model error's `phase`, a compile error's `element`, an `unjudgedReason` and `where`. `beforeTheArm`
  stays the assert's (a `Failure` there, as the exception was), not a flag inside `Failure`.
- **The closed list is checked when a failure is recorded**: a planning failure of a class not on it fails that
  planning by name (`Failures.of`), never recorded as a stand-in class; the lanes are the census of what is raised.
- **`NOT_A_CONSTANT` is never raised at run**: the engine-text arm's refusal is answered at planning (the body is
  refused by name); raising it is an invariant break, loud.
- **`DialectCapability` and `TypeInferenceException` are raised by the run** (a member or a message the dialect cannot
  spell, refused at its place; a side whose typing failed at planning): the run builds each from the failure's data. At
  step 10 `testexec` may not depend on the dialects or the compiler, so the two classes move to the `error` library then
  (a leaf every layer reaches).
- **The closed list, checked by its own refusal** (step 2's first corpus run, 2026-11-08): one class the list lacked,
  `TypeInferenceException` (12 tests: a side whose typing failed while planned), was named by the list's loud refusal --
  the counts unchanged, the failure lines naming the missing class. It is on the list since. Every lane at the slice's
  end is the rest of the census.
- **Step 3: the judge is the test plan's**, one per plan (`TestPlan.judge`: a body is planned by one judge), not a
  field of each verdict group as 2.1 drew it; a branch's steps run under BODY where the plan's judge is the database's
  alone, as the router's sink decided before.
- **Step 3: the planner never sees the test plan's types.** It holds a program's step as `planner.PlannedStep`: a
  product node, or the extension's step by its handle with the statements it reads (what the planner checks: the
  stamps bound, the system store seeded). The test door turns each handle back into the test plan's step. A frame let's
  eager run stays a product check inside the planner (its stamp logic reads checks) and becomes the test plan's
  `FrameRun` at the door, its LET origin a fact of the plan.
- **Step 3: the open program has no `readRaising`.** A raising verdict's base is read through `guarded(savepoint,
  read)`; the error the read raises is the run's own object, caught by the test run as the runner caught it, and its
  source position is read from its cause (exec's `RaisedErrors.Positioned`) for the database judge's parameters, as
  the runner read it. At step 10 `testexec` reads that position through a data accessor of exec's, so no `java.sql`
  subtype is named outside exec. A callback carries an origin as a product node does (2.8 passed one per read): the
  marks layer exactly as they did (`enterIfUnmarked`), the branch condition's STATEMENT inside a BODY step.
- **Step 3: a verdict's message reads the program's live values** (a write's count, a run's stamp bound so far), where
  the runner's reader read the declared values alone; a message reading a value bound by a step before it now reads
  it rather than failing unbound. No corpus or core test reads one.
- **Step 3: removed** -- the two program sinks nothing constructed (`HostSides.Sink`, `VerdictBatch.VerdictSink`), and
  the runner test "a verdict group is refused on the answer path" (the product plan cannot hold one: the type system
  refuses it). The product plan's JSON is version 27 (no verdict or branch tag); the test plan's is its own format,
  version 1, naming the product version it nests.
- **Guard 1's pins at its introduction (step 3)**: the model's `ConnectionDefinition` inside the product plan's
  database (step 7's connection description removes it) and the SQL tree's window-frame bound, an abstract `Number`
  (an Integer or a Long as the frame was written). Step 9 brings guard 1 to zero, so the bound becomes a concrete type
  there (a `Long`: a frame's row count); taken by the design's rule (the plan holds data of a named type), not a
  question.
- **Guard 3 in report mode (step 3)**: `-Dlegend.diagnostics=decoded-plan` runs every body on the plan read back from
  its JSON; the DuckDB corpus under it is step 3's report, hard from step 9.
- **Slice 1's independent audit (2026-11-08, read-only, over `6fdafffc..b33aecb0`) and what it changed:**
  - *Must-fix, fixed:* three places still compared verdicts by object identity (the database judge's segment end, the
    agreement judge's step end and its host/raising halves). Normal runs held (the plan's verdicts were the planner's
    objects); under guard 3's decoded plan a database-judged assert would have passed silently. They compare ids now.
  - *Should-fix, fixed:* a savepoint statement that fails (set, release, rollback to) is again the program's failure,
    never judged as the guarded read's: `OpenProgram.guarded` takes the caller's handling of the WORK's failure, as
    the runner sent its savepoint statements outside the read before the split. Postgres alone carries a verdict
    savepoint, so the DuckDB corpus could not see it.
  - *Should-fix, fixed:* the closed list gains the classes the audit named as raisable by planning
    (`SchemaInvariantException`, `NoSuchElementException`, `DateTimeException`, `UncheckedIOException`); a caller's
    message whose planning failed carries its TEXT alone (only its text is read, `AssertMessages.unevaluated`), so no
    class need be listed there. `ParseException` and the SQL translator's errors are not planning's (a body is parsed
    before it is planned) and stay off the list; `Resolvers.UnfoldableRef` is the lowering's own, never meant to escape.
  - *Found by the id fix, fixed:* under the agreement judge an `assertError` over an assert placed two verdicts with
    ONE id in one plan -- the host's own step and the database's raising copy, built "the same verdict, so the same
    id" for the database judge, where the copy replaces the host's step. Identity had told them apart. The agreement
    judge's copy takes an id of its own (`DatabaseAsserts.errorVerdictFor`); `VerdictIdsTest` pins it.
  - *Notes, fixed:* the referee's enum precheck back where it ran (its census label and order as before step 1); guard
    1 admits an enum only from the plans' libraries, the SQL tree's or the error vocabulary's; guard 3 decodes the
    programs planned ahead too; the product format's version test reads 27.
  - *Deviations the audit found unrecorded, recorded here:* `ExecuteOptions` still carries the test's judge mode,
    judging rules (now `TestPlan.JudgingRules`) and recorder -- the options split with the front doors at step 10; the
    constants-alone path is still a branch of `ProgramRunner` (no session opened), its reader gone as 2.8 said;
    `server_lib` reaches `exec_program` through the driver until the front doors split (step 10); `Failure.Kind` names
    its classes by their binary names (data), the run building each.
  - *Confirmed equivalent by the audit:* step 1's type-name decisions, step 2's failure fidelity (class, message,
    phase, element, unjudged reason), step 3's origins, shape, raising bindings, routing and the constants path.
- **Slice 1's lanes (2026-11-08, at `b33aecb0`)**: core (the root package's version test alone failing, fixed above),
  Postgres, the DuckDB and H2 corpus, PCT and stress green.
- **Step 4 lands in two commits.** 4a: the judgment's types are the test plan's data (`HostJudgment`, `KindClass`,
  `KeyTree`, `Assertion` in `//core:test_plan`); their planning facts read by `driver/JudgmentFacts`, the key tree a
  pair restricts to selected at run by `HostJudge.restrictKeys`. 4b: each host-judged verdict carries them, with its
  sides' shapes (`TestPlan.Host`), and the run reads them from the plan.
- **Step 4a's data forms**, by the design's rule (only what the run reads is carried): `SideFacts` drops the compiler's
  `ExprType` (no run reads it); `Rows` carries each side's class FQN (all its run reads of the types); a row
  assert's literal is its Java carrier and text (`String`, `Boolean`, `Double`, `Long`, `Integer`, `BigInteger`,
  read back to the same value); an unrolled quantifier's and an `assertError`'s inner assert are `Part`s (the
  assertion's facts, its judgment, how many sides it reads); the unused `KindClass.Fine` left with the move.
- **Step 4a: the engine-text comparison is data, its appeal held by number.** `EngineText` carries the arm's name,
  the golden and our text where the plan holds them, their equality, the decline, the origin our text is read under,
  and the NUMBER of its appeal: the appeal (harness Java, R3) and the environment it runs under are held beside the
  plan in the body's `EngineTextAppeals`, published for the body's run and read where a text fails. An out-of-plan
  channel, named, removed with the closures at step 8 (2.10's fourth guard).
- **Step 4b: `SideShape` holds the facts the shaping reads, by name** (2.3 drew a `TypeRef` and a schema): the declared
  type's name, a grid's declared columns, a keyed side's per-key templates (the pivot rule as data: a fixed column by
  its name, a generated one by the template its name ends with), a value side's scalar flag, lower bound, multiplicity
  text, null and list-carrier flags. A representation choice; nothing ruled changes. A constant side's text is read
  back at PLANNING and checked against the compiler's value there (it was checked at run); at run the constant is
  decoded from its cells by its type's name (2.8).
- **Step 4b: the test plan's own data has a hand-written codec** (`HostJson`). A first codec read the records by
  reflection; the guards refused it (F1.11 bans reflection in production: `ArchitectureTest
  .reflectionIsBannedInProduction`, `ErrorShapeGuardrailTest.reflectionSpellingsStayAtZero`), so every type is spelled
  out, one method each way, generated once from the record list and kept as source. A type the codec does not name
  cannot be written. The test plan's JSON is version 2 (3 at step 5).
- **Step 5 (2026-11-08): the database judge's root loop is ONE record of the plan** (`RootLoop`: the segment's roots
  in body order, each its rows by the verdict that reads them -- or the dialect's refusal where no statement could be
  spelled -- its ruled row's decision, its appeal by number and its outcomes decided at planning; its caller's message
  as a side or a refusal, and whether a verdict carries it; the fragment map the census's reason reads), carried on the
  segment's last verdict group (`Verdicts.roots`), not as fields spread over each verdict as 2.1 drew them (`root`,
  `orElse`, `failure`): the decided outcomes and the refused members have no verdict to carry them, and one record keeps
  the order the loop judges in. The run (`judge/RootLoops`) finds a verdict's segment from the plan's groups, judges
  after the segment's closing verdict, takes the row's judgment as a plain function (`DatabaseJudge.verdictOf`) and
  hears an appeal by its number (held beside the plan until step 8). The planned batch is data
  (`VerdictBatch.Planned(groups, loop)`); its maps keyed by object identity are gone, and the database judge's planner
  keeps no map for the run (each verdict's segment, the assertErrors' steps by verdict).
- **Step 5: under the database judge the run tells an assertError's two forms apart by the plan's facts**: its verdict
  row the database judges (S135) carries the host's facts (the assert it reports under) and a statement; its step the
  host judges (N3) carries the host's facts and no statement.
- **Step 6 (2026-11-08): the agreement judge's pairing is data on the group that closes the assert**
  (`Verdicts.agreement`: the verdict that carries the host's base, an assertError's raising verdict), the database
  half's root loop on the same group (`Verdicts.roots`, null where the database judge declines the assert). The run
  (`driver/AgreementRuns`) finds each assert's pairing from the plan's groups; the planner's `One` keeps no run state
  and the planner no map by verdict. The router reads which judge runs a plan from the plan (`TestPlan.judge`), never
  from which planner object it was handed. The test plan's JSON is version 4.
- **Step 7a (2026-11-08): the plan's own connection description, a FAITHFUL COPY.** `ExecutionPlan.Database.Declared`
  holds `executionplan.Connection` (the definition's fields: qualified name, store, database type, specification,
  authentication), and `Platform`/`System` the plan's `DatabaseType`; the plan's `ConnectionSpecification` (each variant,
  `LocalH2` kept) and `AuthenticationSpec` (each kept distinct, references never secrets) are copies of the model's --
  the same variants under the same names with the same fields (2.6) -- and the planner copies the model's definition
  variant by variant (`planner/ConnectionPlans`, an exhaustive switch each way). The plan format's codec
  (`ConnectionWire`) moved to the plan over the copy, its JSON unchanged. `execution_plan` and `exec` no longer reach
  `model`: exec opens a session from the plan's copy alone. Guard 1's two connection pins are gone; `Number` stays (step 9).
- **Step 7a, a first attempt reverted (2026-11-08):** the first cut MOVED the specification, authentication and database
  type records into the plan's library and had the model name them (one set of types). The guards refused it:
  ArchitectureTest's invariant 6j keeps the model depending on protocol, values and error alone. The copy the design
  names keeps 6j and is what landed; the model keeps its own vocabulary, and a caller handing the model's types to exec
  copies them (`ConnectionPlans.of`, `.type`).
- **Step 7b (2026-11-08): the system database names its store by its content's key.** `Database.System(type,
  graphKey)`: a hash of the store's content -- each table's schema, name and definition, and the graph's rows -- in no
  dialect's spelling (the seed text is per dialect, and the first planner to seed a graph's engine fixes it; the key must
  not depend on which asked first). The run finds the store's session by the plan's key. The session's OWNER is still
  the graph's holder (`SystemDatabase`, keyed by the compiled graph, closed by a `Cleaner`): 2.6's exec-owned sessions
  with an explicit lifetime, two graphs of equal content sharing one store, change every caller that runs a body (a
  holder each caller opens and closes) and need the concurrency check 2.6 names -- they go with step 8's removal of the
  out-of-plan channels (2.10.4 names this one). The product plan's JSON is version 28.
- **Step 8a (2026-11-08): the ledger texts are the edge's records in the test plan.** `test_plan` declares `Extension`
  (a record of the edge's) and `ExtensionCodec` (its JSON, found through the `ServiceLoader` seam); the test plan holds
  the edge's records by step (`extensionsByStep`) and by verdict (`extensionsByVerdict`, filled at 8b). The edge is a
  library of its own, `//core:edge` (`com.legend.edge`): its ledger records (`EffectTexts`, `SeedTexts`), its codec,
  and `CompatEdge`, which a harness hands a run in place of the bare ledger (`ExecuteOptions.recording(edge, ...)`).
  The test door plans each effect's and seed's ledger texts through the edge it is handed, by the step's index; the
  run tells the edge what each step did, and the edge records the referee's ledger. `TestDoor.Planned` and its maps
  beside the plan are gone: the test door's answer is the test plan.
- **Step 8a: the hooks the edge implements are `test_plan`'s (`Edge`)**, not the test planner's and the runner's apart
  (2.7 puts the planning hook in `testplan` and the run's in `testexec`): one object is handed for both, and the two
  libraries that call it share `test_plan` alone -- after the step 10 split neither may depend on the other.
- **Step 8a: an edge implies the compatibility rules** (ruled, 3a.3): `ExecuteOptions` refuses an edge under lite's
  own rules. `BodyRouterParityTest` hands the edge to its compatibility runs only. The census of an effect's
  statements is read from the plan's own effect step (its script), whatever the rules.
- **Step 8b (2026-11-08): an engine-text assert's appeal is data, decided after the run.** The appeal's closure
  (`TextAppeal`, four sites) and the database judge's row-appeal closures are gone: the arms plan an `Appeal` record
  (`test_plan`): a contract's failure words, the generator's refusal (S143), or the rows leg -- our rows' and the
  population's reads (each a statement and its shape, or the failure its planning raised), the referee's replay facts,
  the temp tables and in-list seed the golden reads, the stores whose fixtures the harness provides, and the target the
  referee replays on; for the database judge, the texts its verdict row compares. Each is numbered in its body; the
  plan carries those its verdicts name (`extensionsByVerdict`, under the compatibility rules alone). At the verdict's
  step the run RECORDS the appeal heard (its number and the texts read); after the run each is decided from the plan's
  record, the run's record, the referee and the model. `EngineTextAppeals` is a planning registry of these records, no
  longer published for the run.
- **Step 8b: the appeal's record is the test plan's own type, and its decision still runs in the test road** (2.7 has
  the edge plan its own records and decide them). The appeal's reads are planned by the arms' planning machinery
  (`JudgePlans.plannedSide`), and its decision reads planned sides, a target's session and the listener through the
  test road's run -- none of which the edge library can reach without the product exposing its run to it. Every input
  is data in the plan now (the user's rule: no code in the plan); moving the decision code into the edge library, on an
  interface the product declares, belongs with the package move (step 10), where testexec's run services get their
  public form.
- **Step 8c (2026-11-08): the out-of-plan channels removed by name (2.10.4)**, three of four:
  - *the harness's fixture registry, filled at planning*: an appeal's rows leg NAMES the stores it reads in the plan
    (`Appeal.Rows.stores`); the harness is handed them before the body's run (`AssertListener.beforeRun(stores)`) and
    provides each it can, and the decision asks again by name after the run (answered by the same provision). The
    arms no longer ask the harness while they plan.
  - *the planned program's closure*: `Execution.PlannedProgram` is its test plan, data; `Execution.runPlanned` runs it.
  - *the run's dialect and the collected comparison's environment*: the router's sink holds no `SqlDialect`, and an
    engine-text comparison holds no planning environment.
- **Step 8c: the system store's sessions stay keyed by the compiled graph** (2.6's fourth channel, held for the user,
  section 10's questions): the plan names the store by its content's key (7b), and the run finds the session by it, but
  the sessions' OWNER is still the graph's holder (`SystemDatabase`, closed by a `Cleaner` when the graph is collected).
  2.6's exec-owned sessions with an explicit lifetime need an owner every caller opens and closes (a corpus run holds
  thousands of graphs; a per-run owner re-seeds the store per body, the cost `CORE_SQL_SIZE` measured), and two graphs
  of equal content sharing one store need the concurrency check 2.6 asks for before it lands.
- **Slice 2's independent audit (2026-11-08, read-only, over `fbd29531..07258c9e`) and what it changed:**
  - *Confirmed equivalent by the audit:* the database judge's root loop (`RootLoops` against the old
    `VerdictBatch.Planned.judge`: order, census, refusals, ruled-row decisions, messages, appeals), the agreement
    judge's pairings (`AgreementRuns` against the old `AgreementAsserts`, the assertError and over-an-assert forms), the
    host judge's shaping and key selection from the plan, the engine-text judgment and its contract and TDG words, the
    router's census and its S135/N3 dispatch; every field of the hand-written codec written and read; no identity map
    and no thread-local planning object read by the run.
  - *Must-fix, fixed:* an appeal side's planning failure of a class off the closed list failed the whole body's
    planning (`Failures.of`), where it was kept until the appeal was heard. The appeal keeps such a failure as its
    words alone (`Appeal.Unlisted`: the class's simple name and its message, all the rows leg reads of it), so planning
    goes on and the decline (our rows) or the fault (the population) reads as before; a class on the list stays a
    `Failure`.
  - *Must-fix, fixed:* the stores the plan's appeals name were provided in verdict-id order; the harness's provision
    never overlays, so its order is the fixtures' order. They are provided in the appeals' number order, the order their
    arms were planned (`AppealsByVerdict.byNumber`). **Taken on the way:** only the stores of appeals the plan CARRIES are
    provided (before step 8c every planned appeal's, a discarded arm's among them); the run provides what its plan
    names, nothing a discarded plan asked for. The lanes measure it.
  - *Should-fix, fixed:* the class-level wall no longer lets exec reach the model (`EXEC_MAY_REACH`), and the MongoDB
    connector lost its unused model dependency; the R1 measurement covers the run classes steps 5, 6 and 8 made
    (`AgreementRuns`, `AppealsByVerdict`, `RootLoops`), what it found pinned; the dead code deleted (`HostAssert.enter`
    and its two `judge`s; `JudgePlans.shape(PlannedSide)` and its three helpers -- a second copy of the shaping rules,
    a planned constant decoding as it did; `EngineTextAppeals.all`); a failed text decided at planning under the
    compatibility rules is refused by name without numbering an appeal no plan carries; `RootLoops` accepts a verdict of
    no row of the loop only as a decided segment's carrier, refusing any other non-member as before step 5; the
    appeals' judgment walk is exhaustive (no `default`); the extension codecs load once; a guard keeps every
    `ExtensionCodec` provider in `com.legend.edge` (`ArchitectureTest.extensionCodecsAreTheEdges`); a round-trip test
    holds one of every host judgment, step, root-loop record, appeal read form and edge record
    (`TestPlanJsonRoundTripTest`, checked against the sealed list); stale comments corrected.
  - *Deviations the audit found unrecorded, recorded here:*
    - the plan's appeals are added by the router (`AppealsByVerdict.of`), not by the test door: a plan from
      `TestDoor.planned` names appeal numbers it does not carry until the router adds them, and under lite's own rules
      an engine-text judgment's number names an appeal no run hears. Step 10's one test-planning entry, returning the
      whole test plan, closes it.
    - `ExecuteOptions`' public three-argument constructor turns the compatibility rules on with an edge (`edge != null`),
      by ruling 3a.3 (a recorder implies the compatibility rules); before step 8a a recorder left them off there.
    - under lite's own rules the seed step's ledger check returns early (`Moved.seeded`): with no edge there are no
      ledger texts to count the statements against.
    - the edge's records by verdict are keyed by verdict id, each appeal by its number (2.7 drew "by verdict id and part
      index"): the same identification.
    - the system store's key hashes each table definition's record text (`toString`, the components' names and values
      by the Java language's definition) and the graph's rows, once per graph.
    - the rows leg refuses a missing referee by name before reading our rows (it failed later, on the referee's first
      use); PCT's channel B hands no referee and hears no appeal.
    - an appeal's constant rows are decoded from the plan's text by their type's name (a `Long`, a `BigInteger`), as a
      host side's constant is at its step, where the compiler's answered value was read.
- **Slice 2's lanes: a fix after the core lane.** The planner's WebAssembly build (`//wasm:planner`, TeaVM) has no
  `java.security`; step 7b's key used `MessageDigest`. The key is two 64-bit hashes in plain long arithmetic
  (`SystemRoutes.hashOf`), the same content; the planner builds for the browser again (`//pure-protocol:twins_test`
  with it).
- **Slice 2's lanes (2026-11-08, at `9de03396`, the audit's fixes in): every lane green.** The lanes were core,
  Postgres, the DuckDB and H2 corpus, PCT and stress. The DuckDB corpus run from the plan read back from its JSON
  (guard 3, report mode) is identical to the baseline on both judges: host 2436/108/7, database 2429/108/7.
- **Step 10's first attempt (2026-11-08) was abandoned before it reached the tree, and why.** It ran in a scratch copy
  compiled with plain `javac` while the lanes ran. The class-level cuts held: zero planning-to-run and zero
  run-to-planning references, measured with `jdeps`, kept as `docs/plan-as-data-homework/step-10-cuts-draft.patch.txt`
  with its own README. The package move then went wrong in three ways the user caught:
  - **It created `com.legend.testdoor`**, holding the router's sequencing, the system store's sessions and the appeal
    decider's interface. That is a renamed driver touching both halves. 2.9 and R3 allow exactly one thing that sees
    both: the test front door, thin, calling one `testplan` entry and one `testexec` entry and holding no logic.
  - **It widened visibility to whatever the compiler reported**, so the public surface became whatever happened to be
    called, not each library's chosen entry points.
  - **It would have pinned `testplan -> testdatagen -> exec/testexec`**, where strict separation splits the generator's
    planning entry from its run.

  **The corrected approach**, to settle with the user before any code:
  1. Write step 10 as a spec first: each library's few public entry points, and the thin door.
  2. Write its rules as ArchUnit checks that fail on today's code.
  3. Move code until they pass.

  Three things had no home in either half, and should have been questions instead of a package:
  - the router's sequencing, which goes in the thin door;
  - the system store's sessions, which the catalog design replaces (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md`);
  - the referee's read of the model to decode enum columns, whose facts can be planned into the appeal record.
- **Step 9 is ready as a draft, not landed:** `docs/plan-as-data-homework/step-9-draft.patch.txt`. A frame's offset
  becomes a closed type, so guard 1 is at zero, and guard 3 becomes hard on the DuckDB corpus lane. It compiles with core,
  spec and PCT, and has not been run under Bazel.
- **The system store's sessions (the held question of 2.6 and step 8c) have a design**, worked through with the user on
  2026-11-08: `docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md`, with its measurements and probes.
  - **One catalog per process**, the platform's rows written once.
  - **The product** registers a loaded project and commits.
  - **A test** registers its model inside its own transaction, only when a plan reads the catalog, and rolls back.
  - **`Class.all()`** is every class in the catalog the session sees.

  Its placement in this plan is open (its section 9). Until it lands, the model-keyed `SystemDatabase` stays the one
  recorded out-of-plan channel.

## 11. Closing the invariant: slices 3, 4 and 5 (the user's word, 2026-11-08)

Slice 2's invariant audit (GATES, 2026-11-08) found the invariant does not yet hold. It holds for the plan's types and
inside the walker and the judges' run halves, and fails at the boundary around them. This section replaces section 4's
steps 9 to 11. Its checklist is the audit's holes. **The product is in scope** (the user: "we need to close all holes
including product").

**The rule for every pin (the user, 2026-11-08):** "we just have to make sure we actually drive pins to zero -- we also in
this project have tendency not to drive to zero so if we do pins they better go to zero fully." Each guard below starts
as a ratchet: today's holes listed by name, shrink-only, a dated note on every removal. **A slice does not close while any
pin it owns is non-zero.** At the end of slice 5 every list is empty and every guard is hard. A pin is never moved to a
later slice to close one; that is a question for the user.

### The holes (the audit's numbering)

| # | Hole | Where today | Closed by |
|---|---|---|---|
| 1 | The appeal decision reads the compiled model (enum decoding), the referee's static mirror and the ledger | `BodyRouter` decider, `SqlTextVerdicts.rowsLegNow`, `SqlReplayReferee`, spec `ReplayReferee` | the enum and property decode facts planned into `Appeal.Rows`; the referee takes no model. The mirror and the ledger are the edge's after-run inputs, by 2.7, and are said so plainly |
| 2 | The system store's sessions are found through the compiled model, and the run writes into the model's cache | `SystemDatabase`, `BodyRouter`, `Execution.runPlanned(ctx)` | the catalog's test half (`docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md`), after its H2 DDL check |
| 3 | The router decides after the run whether the last verdict is the answer, from planner objects | `BodyRouter.execute` (`asserts.at(...)`, `typed.body()`) | a fact of the test plan |
| 4 | The listener carries code: `fixtureAhead`'s closure, and `beforeRun` makes the harness plan and run fixtures between the body's plan and its run | `AssertListener`, `PureTestRunner` | the listener is output only. The door returns the plan; the caller provides the fixtures the plan names (its own plans and runs); then it calls run |
| 5 | Under the database judge, planning reports declines the plan does not carry (attributed to the wrong verdict), and arm planning depends on whether a referee object exists | `SqlTextVerdicts.decide`/`declined`, the `refereed` checks | the decline carried in the plan and reported at its root's turn; planning gated on the judging rules |
| 6 | Three thread-locals carry run state | `Appeals.OPEN`, `RunReader.CURRENT`, `CallerMessage.DEPTH` | an explicit run record through the run's sink |
| 7 | Sessions are opened from the model, on the test road and the product's door alike; the plan's own connection is ignored for ordinary targets | `Execution.run`, `Execution.runProgram` (`sessions.open(executesOn(ctx, ...), ctx)`), `BodyRouter.sessionsByTarget` | sessions opened from the plan's targets (its connection description, 2.6); the `Source` contract takes no model |
| 8 | The run re-checks the plan's rules against the options | `Moved.rulesOf` | deleted: the plan's rules stand |
| D1 | Guard 1 deduplicates by type, so its `Number` pin also hides `Following.n` | `PlanTypesAreDataTest` | report by (type, path); the frame offset a closed type (the step 9 draft) |
| D2 | The plan is not self-contained: its appeal numbers point into a planner registry, and the router adds the appeals | `BodyRouter`, `TestDoor.planned` | test planning returns the whole test plan, appeals included. Under lite's own rules there are no dangling numbers |
| D3 | A CSV load's local file path in the plan | `options.resources()` at planning | to be designed in slice 4. A portability matter (G11) |
| A1 | Under the agreement judge with the compatibility rules, a failed text holds two appeals, one per half: the referee replays twice and its report is told twice (the outcome is right: both decide alike). Found by the slice 3 follow-up audit, pinned at 2 by `BodyRouterParityTest.engineTextPrograms` | `AgreementRuns` (the database half's `rowAppeals`), `SqlTextVerdicts.judge` | slice 4: one appeal per assert, heard by both halves |
| E | No thin door: `BodyRouter` makes about ten decisions; the mixed classes; the planning environment carries run resources | `driver/*`, `judge.VerdictBatch` | slice 5: the run record, then step 10 as a spec. Each library's entry points, the thin door, and ArchUnit rules first |

### The guards (ratchets in slice 3, hard by slice 5)

- **Guard 1** reports by (type, path). It goes to zero at slice 3 (the step 9 draft).
- **Guard 3** is hard on the DuckDB corpus lane from slice 3 (it already passes).
- **R1** measures every class, the mixed ones, `BodyRouter` and `SystemDatabase` included. It is pinned by name, and
  zero at slice 5.
- **A new check: no compiled model reaches the run.** No run class, door call or sink receives a `ModelContext` or
  `TypedQuery`. It is pinned per site, zero at slice 4.
- **A new check: the listener is output only.** No `AssertListener` method returns a value. Zero at slice 3.

### The slices

- **Slice 3:** the guards as ratchets; step 9 (the draft); holes 3, 4, 5, 8 and D2.
- **Slice 4:** holes 1, 2 and 7 (the product's door included), and D3.
- **Slice 5:** hole 6 and E. The run record, then step 10 as a spec reviewed by the user before code, the thin door and
  the package split. Every pin at zero, every guard hard.

Each slice closes with every lane and an invariant audit, briefed with the rule, not the step list.

### Slice 3's progress

- **Batch 1 (`99f4058c`):** step 9 (a window frame's offset a closed type), so guard 1 is at zero, judged per
  (type, path) (D1); guard 3 hard on the DuckDB corpus lane; hole 3 (the plan says whether its last statement answers
  a verdict: `TestPlan.answersLastVerdict`); hole 8 (the run's rules re-check deleted); D2 (the test door returns the
  whole plan, its appeals included, and a plan naming an appeal it does not carry is refused: `PlannedAppeals`).
- **Batch 2: holes 4 and 5, and the three guards.**
  - *Hole 5:* an arm's decline is its verdict row's data (`RootLoop.Row.decline`, the test plan's JSON version 7),
    reported by the root loop at the row's turn, before the row is judged. Until then the database judge told it to
    the listener while the assert was planned, so it was attributed to no verdict. A decline no plan carries is
    refused by name. Planning's environment holds no listener and no referee (`ExecEnv`'s two components deleted);
    the six "needs a replay oracle" walls are deleted, so planning depends on the judging rules alone, and the appeal
    decision refuses a missing referee (`rowsLegNow`).
  - *Hole 4:* the listener is output only. `AssertListener.fixtureAhead` and `beforeRun` are deleted. The test door
    has two halves, `Execution.planBody` (no session, nothing told to anyone) and `Execution.runBody`, and
    `runProgram` is the one composed of them. `PureTestRunner` plans the body, provides the fixtures of the stores its
    plan's appeals name (`PlannedAppeals.stores`, each planned only where it may be provided), then runs it.
    `Execution.runPlanned` is gone (a fixture program runs through `runBody`). The test runner's library reaches the
    test plan's (`tools/deps/core-layers.txt`).
  - *The guards* (`ArchitectureTest`, in `//core:guardrails`):
    - R1 measures every class of the driver, the judges and the edge but the planning-only ones named in
      `PLANNING_ONLY`. A measured class that reaches planning directly is MIXED and is itself a target. The first
      measure: 14 mixed classes, 266 crossings, pinned by source in `R1_CROSSINGS`, zero at slice 5.
    - `noCompiledModelReachesTheRun`: 13 sites (holes 1, 2 and 7), pinned in `MODEL_SITES`, zero at slice 4.
    - `theListenerIsOutputOnly`: hard, at zero. No listener method answers a value, and no planning class, planning
      entry or planning environment takes or holds a listener or a referee.
  - *Verified:* the DuckDB corpus on both judges equals the baseline (host 2436/108/7, database 2429/108/7, no line
    differs). One tally moved, as hole 5 intends: under the database judge `foreign-dialect:Composite` went from 9 to
    8, the host's count -- a decline planning had told for a row the run never reached is no longer counted.
  - *Found, not changed:* `BodyRouterParityTest.queryPrograms` checks only that the router planned each body. On H2,
    `[1, 2, 3]->map(x|$x * 2)` is refused at planning (`UNNEST reached a dialect without an unnest placement`), and
    the test does not notice. This predates the batch.
- **Slice 3 closed at `e8a932cb`** (2026-11-08): every lane green, two audits (the invariant audit and its
  follow-up), the record in GATES. Its own pins are at zero: guard 1, guard 3, the listener, the rule re-read.
  Marker branch `claude/plan-as-data-slice-3`.
