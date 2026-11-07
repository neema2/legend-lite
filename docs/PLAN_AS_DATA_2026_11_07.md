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

### 4.6 The run's type vocabulary

The run stops using the compiler's `Type`. `TypedSide` carries a `TypeRef` (the plan's spelling); `Equality`,
`PureAsserts` and `TdsCompare` decide kinds from it through one small classifier in `execution_plan` (primitive
names, decimal precision, class and enum FQNs, relation columns), as the result writers already read `Column.type`.

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
3. **The run's own type vocabulary** (4.6) replaces the compiler's in the judges. This touches every comparison, so it
   lands first, alone, held to identical verdicts on every lane.
4. **Every lane runs the decoded plan** (4.8), permanently.

## 6. Order of work (each step compiles and keeps the fast loop green)

1. The run's type vocabulary (4.6): `TypedSide`, `Equality`, `PureAsserts`, `TdsCompare` off the compiler's `Type`.
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

## 7. An example: `projection::limit::testSimpleLimit`

(Filled in from the real plan, printed by `-Dlegend.diagnostics=plan-json`, once the lanes free the build: today's
plan and sidecar, and the same body's plan as proposed.)
