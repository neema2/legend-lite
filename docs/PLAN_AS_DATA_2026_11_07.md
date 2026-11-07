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
        + pairedWith: VerdictRef?                               -- agreement judge: the other half's verdict
        + appeal: Appeal?)                                      -- compatibility rules only (4.5)

Effect(sql, script, + ledger: [Recorded(text, query)])         -- today TestDoor.Planned.effects
Setup(key, steps, target, + ledger: [String])                   -- today TestDoor.Planned.seeds
Check(sql, + origin: LET?)                                      -- today TestDoor.Planned.frameRuns
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

### 4.5 Appeal: the compatibility rules' rows leg, as data

Today a closure capturing the referee, the planned row sides, the replay facts and fixture closures. In the plan:

```
Appeal(golden, goldenPlan?, planBindings: {name: [value]}, temps: [TempTable], inListSeed?: (kind, [value]),
       replaySql?, mapping?, classFqn?, facts: (extentSubset, ordered, sortKeys?),
       ours: Side, population: Side?, fixtures: [storeFqn])
```

The referee (the edge's, P2) decides it after the run from this data alone: it provides the named stores' fixtures,
reads `ours` and `population` as plans of their own, replays the golden, compares. Present only under
`COMPILED_PURE_COMPAT`.

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

### 4.7 Where the code lives afterwards (compile safety)

| Library | Holds | May not depend on |
|---|---|---|
| `execution_plan` | the plan's records (4.1 to 4.6), `PlanJson` | anything but base, json, sql records |
| `testplan` (the planner's test part) | ALL assert planning: the three judges' planners, the arms, `DatabaseJudge`, `SqlTextVerdicts`' planning, `VerdictBatch`'s planning | `exec`, `judge` |
| `judge` (the run) | the comparisons, the host judging (`HostJudge`'s run half), the database judge's root loop, the agreement rule | compiler, lowering, resolver, planner, testplan |
| `driver` | the router only: plans through `testplan`, runs through `exec` and `judge` (R3: the one place both halves meet) | -- |
| the edge (P2) | the referee, the appeals' decision, the engine-text printers | the product never depends on it |

`tools/deps/core-layers.txt` pins each, so a run class that names a planning type does not compile. ArchUnit R1-R4
become hard rules on top.

### 4.8 The guard

Every lane runs **the plan decoded from its JSON**: the router writes the plan, reads it back, and hands the run only
the decoded copy. Anything the run needs that is not data then fails on every lane at once, not in review. Measured
cost first (one write and one read per body).

## 5. Decisions for the user

1. **Compatibility data inside the plan** (the engine-text goldens, the appeals, the ledger texts), present only under
   the compatibility rules and read only by the edge, rather than a separate attachment beside the plan. Recommended:
   inside -- one artifact, one JSON, one round trip; the product run ignores it.
2. **A failure known at planning is data** (type and message); its planning-time stack trace is not kept.
3. **The judges take exec's way with types** (4.6): the plan's type names and planning-time facts replace the
   compiler's `Type` at run. This touches every comparison, so it lands first, alone, held to identical verdicts on
   every lane.
4. **Every lane runs the decoded plan** (4.8), permanently.

## 6. Order of work (each step compiles and keeps the fast loop green)

1. The judges' types the exec way (4.6): `TypedSide`, `Equality`, `PureAsserts`, `TdsCompare` off the compiler's
   `Type`, onto the plan's type names and planning-time facts.
2. Failures as data (4.4).
3. Judgments and side shapes in the plan (4.2, 4.3); `HostJudgment`, `HostSides.Planned`, `JudgePlans.PlannedSide`
   deleted from the run's view.
4. The database judge's root loop from the plan (roots, `orElse`, refusals, messages); `VerdictBatch.Planned`'s
   identity maps deleted; the batch's judge a plain function.
5. The agreement pairing in the plan.
6. The sidecar into the plan (ledgers, origins, the judge of each group); `TestDoor.Planned` is the plan.
7. Appeals as data (4.5); the closures deleted (`TextAppeal`, `VerdictBatch.Appeal`, the fixtures' `BooleanSupplier`s).
8. Every lane runs the decoded plan (4.8).
9. The move: all assert planning into `testplan`; `judge` and the sink run-only; the deps pinned; R1-R4 hard.
10. All lanes and an audit (the P1 milestone).

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
              {"kind": "CONSTANT", "type": {"name": "String", "multiplicity": [1, 1]}}],
   "appeal": null}]}
```

`appeal` is null here because both texts are constants of the plan and equal, so no rows leg can be needed. Where
our text is read at run, or differs, the verdict carries the appeal as data (4.5): the golden, our rows and the page
population as sides, the stores whose fixtures it needs, the replay facts. The `Ask`s' typed trees, the splice
closure and the environment are planning's alone and are not in the plan.

### 7.4 The database judge, today and proposed

Today the database judge's plan for this body is already almost all data: ONE step, a fused statement that computes
all four verdicts and their messages in the database (`WITH frame_result AS MATERIALIZED (... LIMIT 0), frame_result2
AS MATERIALIZED (... LIMIT 1), __p_0 AS (... the two texts ...), __p_1 AS (... COUNT ...) ... SELECT 0 AS __ix,
<verdict>, <expected>, <actual>, <unjudged>, <lenient>, <holds>, <message> FROM __p_0 UNION ALL ...`), and four
verdicts named only `assertEquals`, `assertSize`, `assertEquals`, `assertSize`. Beside it, `VerdictBatch.Planned`
holds the roots (which assert each verdict belongs to, in body order), identity maps from verdicts to pending rows,
and, under the compatibility rules, an appeal closure per engine-text row.

Proposed: each of the four verdicts carries its `assertion` and `root`; an engine-text row whose text may differ
carries its `appeal` as data; a member the dialect could not spell carries its `failure`. The statement is unchanged.

### 7.5 Things the example shows that are not about data (noted, not in this step)

- The host judge's plan runs `SELECT ... LIMIT 0` twice: once as the let's `check` step and again as `assertSize`'s
  side. The database judge's reads it once (the frame). A G3 item.
- Every `Sql` repeats its whole `target` (the connection and the session statements): the body's 10 KB is mostly
  that and the typed trees. A plan-size item (G3), easy once the plan is the one artifact: targets by reference.
- The setup programs carry each statement twice (its tree and its script text).
