# Research: the comparison setting in function and mapping code (A), and stamping a corpus test body (B)

L = core/src/main/java/com/legend. Read only. Nothing was built or run. The tree moved while I read it: commit
`33a2358b` ("The editions, step 2a in progress") had landed the stamp field and a draft of the stamping. Line numbers
are from HEAD `a2c40dcb`.

## Summary

- **A, the comparison setting.** A flag on `Synthesized` is enough for the relational mapping language at whole-function
  level: a relational `$class$`, a relational `@Join` `$assoc$` and a `$view$` contain no user Pure. Three function kinds
  mix the two languages: `~func` class mappings, XStore column-space associations and ModelJoin associations. The finest
  unit that is purely relational is the single comparison call the normalizer writes. So the robust fix is per call,
  and there are three such places, not two:
  - `RelOpTranslator.comparisonFn` EQ/NEQ;
  - the enumeration decode at `MappingNormalizer.java:2490`;
  - the dynafunction pass-through, which turns a written `equal(...)`, `in(...)`, `contains(...)`, `max(...)` or
    `min(...)` into the marked Pure natives (`RelOpTranslator.java:561`, `DynaFnDecisions.java:68,96,122,148,152`).
- **A, exposure.** For legend-engine the setting is the plain call, so switching it on in bodies changes nothing for that
  audience by construction. The product and legend-pure do change. The product's biggest unmeasured exposure is the
  DataCube timeseries library (`datacube/src/pure-library-text.ts`, mirrored in `projects/timeseries/model.pure`). It
  holds about 105 integer-literal comparisons over value columns inside functions generic over relations. Those
  functions are typed at the call over the caller's real columns (`Overloads.java:128-132`), which today forces marking
  OFF. Other exposures:
  - `SalaryBandMapping`, in 2 test files (Decimal `<` Integer);
  - `TdsNullTypingPinTest`'s `f::r` (String `!=` SQLNull);
  - possibly the boot layer's own bodies.
- **B, a defect in the committed draft.** `UserCallInliner.stampedIfStoreFree` decides "reads no store" on the WRITTEN
  statement (`StatementEffects.reaches`, `UserCallInliner.java:226-236`). It does not see through `$result` to the frame
  the hook splices in. It then stamps the whole REDUCED tree, spliced frame chain included (`stampOutside`, 240-246).
  - Today this is dormant at the corpus door, which hands the product table (`PureText.java:138-141`).
  - Once the corpus's table stamps, every harness statement that reads a frame through a variable would stamp the
    frame's query code too. That is the wrong column.
  - The corpus needs a separate rule: stamp everything except the frame regions, keyed on node identity.
- **B, the recommended mechanism.** Do it inside the inliner. The recorder (and the judges' adapter) hands the inliner an
  identity set of every node of every frame chain it gave out. The inliner tracks a "frame depth": it raises the depth on
  entry to such a node, to a `TypedFrameRef`, or to the arguments of an `execute(...)` call. It stamps a native call only
  at depth 0. This survives the splice's re-wrapping (`executedExtent`, `readThroughExecute`, the CTE `From`), because
  the check is made on entry identity.

---

## PART A: the comparison setting inside functions and mapping code

### A0. What marks today

| Fact | Where |
|---|---|
| The marker is minted only where `t.marksCrossings()` is true | `L/compiler/spec/Overloads.java:143-149` |
| Marking is forced OFF for every function body (`inFunctionScope`) | `L/compiler/spec/Typer.java:147-154` |
| Marking is forced OFF for a relation-generic or NormalizeRequired body typed into its call | `Overloads.java:128-132`; `requiresNormalization` at `:270-281` (stereotype, `RelationGenerics.of(f)`, schema-erased params) |
| Marking is forced OFF for island output | `Typer.java:220`, `:256`; `SpecCompiler.java:390`, `:434` |
| Marking is ON only here | `SpecCompiler.typeQueryBodyMarked`, `SpecCompiler.java:324-326`, called once from `TypedQuery.body()` (`L/TypedQuery.java:92-99`) |
| Unmarked entries | `typeQueryBody` (`SpecCompiler.java:335`), `typeExpression` (`:296`, `:306`) |
| The marked comparisons, by declaration identity | `EqualityClasses.java:50-66`: `equal`, `eq`, `in`, `contains(Any[*],Any[1])`, `indexOf`, `<`, `<=`, `>`, `>=`, `compare`, 2-arg `max`/`min` (Number, Date). The Lite shims `meta::legend::lite::lessThan*`/`greaterThan*`/`notEqualAnsi`/`isDistinctFrom` are NOT in it. |
| The memo is per `SpecCompiler` | `SpecCompiler.java` field `memo`, used at `:84-114`. `TypedQuery` makes its own at `TypedQuery.java:36`. A body is typed once per query, under one rule. |

### A1. Every synthesis site: what it lowers, and its language

| Site | Hat | Source it lowers | Body language |
|---|---|---|---|
| `MappingNormalizer.java:632-644` `liftClassInline` | CLASS | clean-sheet class binding, Relational or Pure kind, inline realization | **Pure**: the user's expression verbatim, rooted at `#>{db.T}#` or a getAll. The binding kind (Relational) is NOT the language. |
| `MappingNormalizer.java:653-690` `liftAssocInline` | ASSOC | clean-sheet association predicate lambda | **Pure** |
| `MappingNormalizer.java:772-801` `synthesizeClassMapping` | CLASS | dispatch at `:782-788` (rows below) | per variant |
| ↳ `ClassMapping.Relational` → `synthRelational` (`:1303-1337`) | CLASS | legacy `###Mapping` relational set: `synthTableBackedParts` (`:1687-1876`), view-backed (`:1635-1656`), JSON-source (`:1575-1606`) | **Relational, whole function.** Every property mapping is a `RelationalOperation`. The `PropertyMapping` variants (`L/model/PropertyMapping.java:47-57`) have no embedded-Pure variant. `~filter` (`applyDirectFilter` `:2291`, join-mediated `:2324`), `~groupBy` (`GroupBySynthesis`) and `~distinct` (`:1822-1859`: lite glue `select`/`distinct`) are relational. Enum decode `:2396-2512` is lite-minted. The rest is lite glue (`map`, `new`, `tableReference`, casts in `DeclaredCoercions`). JSON source: lite glue, no comparisons. |
| ↳ aggregation-aware: views lifted as relational sets (`AggregateViewLift.java:34-48`) | CLASS | `~aggregateMapping` sets | **Relational.** The Pure `~modelOperation` lambdas (`groupByFunctions`, `mapFn`, `aggregateFn`) are NOT in any body. They ride `AggregateViewFacts` on the binding and are typed by the resolver with `specs.typeExpression(expr, bindings)` (`L/resolver/AggregationAwareRouting.java:118-126`, `SpecCompiler.java:306-314`), unmarked, and compared structurally. |
| ↳ `ClassMapping.Pure` → `synthM2M` (`:1131-1203`) | CLASS | M2M class mapping | **Pure**: `~filter` (`:1153-1157`) and property expressions (`m2mPropertyValue` `:1205-1233`) are the user's. Glue: `getAll`, `filter`, `map`, `new`, `NewInstanceCast`. The enum transformer is refused (`:1185-1194`). |
| ↳ `ClassMapping.RelationFunction` → `synthRelationFunction` (`:814-824`) | CLASS | `~func` class mapping | **MIX.** The pipeline is a user Pure function's body (`relationFunctionPipeline` `:905-931`) or an inline `~src` (Pure). Column bindings are Pure row expressions (`Col.bindSrc` `:890-893`). An enum-mapped column runs the lite-minted, mapping-language decode `translateEnumeratedSource` (`:897`, whose `equal` is at `:2490`) INSIDE this Pure function. |
| ↳ `ClassMapping.Union` / `Inheritance` → `UnionSynthesis` | CLASS | operation mappings | lite glue composing member functions. I found no comparison mints in `UnionSynthesis`. |
| `MappingNormalizer.java:943-1049` `synthesizeXStoreMapping` | ASSOC | XStore association over `~func` or relational ends (column-space route) | **MIX.** The condition is the user's Pure (`$this.id == $that.firmId`), rewritten by `RelationReads.xstore` to read the ENDS' COLUMNS (`:1009-1012`). So it compares column types, not property types. The end pipelines are a `~func` body (Pure) or a `tableReference` (`XStorePureEnds.java:72-140`). |
| `XStorePureEnds.java:258` (`synthesize`, `:200-264`) | ASSOC | XStore with a Pure-set or lossy-view end (property space) | **Pure**: the user's condition with reads renamed (`renameReads` `:271-304`) |
| `MappingNormalizer.java:1058-1124` `synthesizeModelJoinMapping` | ASSOC | ModelJoin association | **MIX**, like XStore: the user's Pure lambda over column reads (`RelationReads.rewrite` `:1100-1103`) |
| `AssociationSynthesis.java:544` (body built `:510-531`) | ASSOC | relational association `@Join` | **Relational, whole function**: `buildAssocPredicateBody` translates the join via `RelOpTranslator` |
| `LiftedViews.java:106` | VIEW | `###Relational` store view | **Relational, whole function** (`ViewRelation.viewRelationExpr`) |
| `L/compiler/DerivedProps.java:58` | PROP | derived property | **Pure** |
| `ModelNormalizer.java:337/346` | PRE/POST, CONSTRAINT_MSG | function pre/post conditions, their messages | **Pure** |
| `ModelNormalizer.java:380` (`liftDefaults`) | DEFAULT | property default value | **Pure** |
| `ModelNormalizer.java:409` | CONSTRAINT | class constraint | **Pure** |
| `ModelNormalizer.java:429` | CONSTRAINT_MSG | constraint `~message` | **Pure** |
| `ModelNormalizer.java:493` | QUERY | service query | **Pure** |

**Answer.** One flag on `Synthesized` ("this body is the relational language's") would correctly cover:
- the relational `$class$` (`MappingNormalizer.java:799` when `cm` is `ClassMapping.Relational`, aggregate views included);
- the relational `$assoc$` (`AssociationSynthesis.java:544`);
- `$view$` (`LiftedViews.java:106`).

It cannot cover three mixed kinds:
- `~func` `$class$`, where a Pure body holds the lite enum decode;
- XStore column-space `$assoc$`;
- ModelJoin `$assoc$`, where Pure conditions compare column-typed reads.

`ClassBinding`'s kind does not help either: clean-sheet Relational is Pure, and `AssociationBinding` has no kind
(trace2 §4).

**The finest pure-relational unit is the call the normalizer mints.** Recommendation: per-call Lite identities for the
relational comparisons (A2), with no flag needed. An optional belt is `Synthesized.relationalLanguage` set at the three
whole-relational sites. It would also cover the dynafunction pass-through without new shims, but it leaves `~func`'s
enum decode exposed.

Open semantic question, not lite's to decide here: XStore and ModelJoin column-space conditions. These are the user's
Pure, so the rule should apply. But the operands' types are the columns' SQL types, so a crossing can appear that the
property types would not make. That needs a ruling or a register row.

### A2. Every plain Pure call the normalizer writes for relational-language code

| # | Site | Mints | Marked today if typed with marking on? |
|---|---|---|---|
| 1 | `L/normalizer/RelOpTranslator.java:566-587` (Comparison arm), name from `comparisonFn` `:694-708` | `EQ` → `equal` (`:701`). `NEQ` → `not(equal)` (`:702`, `:584-586`). `<`, `<=`, `>`, `>=` → `Pure.Lite.*_ANY` (`:703-706`). | EQ/NEQ yes. The orderings no. |
| 2 | `L/normalizer/MappingNormalizer.java:2490` (`translateEnumeratedSource`, `:2413-2512`) | `equal(sourceRead, literal)` for each source value: String, Integer or EnumRef (`:2479-2489`), or'ed (`:2493`). Reached from `EnumeratedExpression` (`:2025-2030`), `EnumeratedColumn` (`:2036-2039`, `2396-2405`), an enum-mapped `JoinTerminalColumn` (`:2064-2067`), and `~func` columns (`:897`). | yes (an Integer column against a String source value, an EnumRef against a column) |
| 3 | `RelOpTranslator.java:561-564` (`operatorCall(dynaFnName(call), dynaFnCandidates(call), …)`) | A written dynafunction whose decision is PURE passes through under its own name (`DynaFnDecisions.java`): `contains` `:68`, `equal` `:96`, `in` `:122`, `max` `:148`, `min` `:152`. (`between` `:54`, `exists` `:97`, `startsWith` `:209`, `isEmpty`/`isNotEmpty` `:126-127` pass through too, but are not in `EqualityClasses`.) `greaterThan`/`lessThan`/…, `isDistinct` and `notEqualAnsi` are SHIMs (`:115-116`, `:125`, `:140-141`, `:162`). `notEqual` (`DynaFn.java:185`) has no decision, so it is UNSUPPORTED. | `equal`/`in`/`contains`/`max`/`min`: yes |
| 4 | `RelOpTranslator.java:597-602`, `:512-519` | `isEmpty`/`isNotEmpty` for IS (NOT) NULL | no (not a comparison) |
| 5 | `RelOpTranslator.java:371-389`, `:524-534` | `if` for `case`/`if` | no |
| 6 | `RelOpTranslator.java:542-546`, `:588-594` | `and`/`or` | no |
| - | `MappingNormalizer.java:725` `canonicalizeEqualOperands` | only READS `equal` by name (XStore direction agreement) | n/a |

**Cost of moving 1-3 to Lite identities.** Moving EQ/NEQ, the enum decode and the pass-through `equal`/`in`/`contains`/
`max`/`min` to Lite identities means each new identity needs:
- a lowering binding with `equal`'s rule (`L/lowering/Scalars.java` around `:115`);
- recognition by every structural matcher that keys on `equal`'s identity.

There are about 24 references in `resolver/` and `lowering/`, in:
- `ClassSources`, `ChainNormalizer`, `ClassWrites`, `TemporalFrame`, `SyntheticHeads`, `CorrelatedSubselects`,
  `Substitution`, `LiteralFolds`, `Callees`, `ForeignKeyIdentity`, `AssociationJoins`, `StoreResolver`;
- `Scalars`, `Lowerer`, `InstanceEquality`.

The equi-join key extractors (`CorrelatedSubselects.parentKeysLenient`/`collectEquiKeys`,
`ForeignKeyIdentity.sourceKeyColumn`) mostly see relational `@Join` conditions, so they MUST learn the shim or they
change plan shape or refuse (trace2 §1a). That cost is the strongest argument for the alternative, the
`Synthesized.relationalLanguage` flag at whole-function level plus ONE shim for the enum decode (the only relational
mint inside a Pure function). The flag route touches no matcher.

### A3. How the type-checker knows at `inFunctionScope`, and nesting

- `TypedFunction` (`L/compiler/element/TypedFunction.java:51-61`) exposes `definition()`, a `model.Function` and so
  possibly a `FunctionDefinition`, and from it `synthesizedFrom()`. That gives `hat`, `ownerFqn`, `memberName` and
  `forwardProperty` (`L/model/FunctionDefinition.java:157-200`). That is all there is. The hat cannot tell a relational
  `$class$` from an M2M or `~func` `$class$`.
- The withdrawn draft used `hat ∈ {CLASS, ASSOC, VIEW}` as "mapping code" (`withdrawn-function-body-door-draft.patch.txt`
  lines 212-219). That treats M2M and `~func` as unmarked, which the 2026-11-25 ruling then allowed ("don't refuse
  m2m/func for now").
- A flag would have to be added to `Synthesized`. Alternatively the typer can look up the binding kind from the
  `$class$` owner (`MappingDefinition.ClassBinding`), but that does not work for ASSOC.
- **Nesting stays correct.** `markingCrossings` saves and restores (`Typer.java:177-185`).
  - A lambda inside a body inherits the body's state.
  - A call to a user function is typed by signature only. `SpecCompiler.check` never recurses, and the callee's body is
    compiled separately through `compile` → `check` → `inFunctionScope` under its OWN rule (`SpecCompiler.java:84-114`,
    `199-201`).
  - The two forced-off sites inside a body behave as follows:
    - (a) `Overloads.java:131`, a relation-generic body typed into the call. It must change to apply the setting with the
      CALLEE's language, or the DataCube library stays silently unchecked.
    - (b) Islands (`Typer.java:220`, `:256`) keep their own rule.
  - `pretyped` arguments (`Typer.synth`, the `pretyped` map) keep the caller's typing, which is right: they are the
    caller's code.
  - Store resolution types more later: `ClassSources.compileSynthFn` → `specs.compile` (`L/resolver/ClassSources.java:191-200`)
    and `AggregationAwareRouting`'s `typeExpression`. These are on the same per-query `SpecCompiler`, so the one setting
    reaches them.
- **Per-audience answer at the mint** (design 2 of trace2 §6b):
  - product: throw a compile error at `Overloads.java:143-149`;
  - legend-engine: return `call` (today's unmarked path);
  - legend-pure: mint the marker only where PureComparisons can answer it at the mint or in the inliner's fold.
  - Then no marker ever reaches the resolver walkers that refuse it (trace2 §3).

### A4. The product's exposure in lite's own code and tests

I ran a scanner over `core/src/test`, `core/src/main/resources`, `datacube`, `python`, `warehouse`, `engine-client`,
`wasm`, `projects` and `docs/examples`. It looked for `$x.prop OP literal` where `prop`'s declared type in the same file
differs from the literal's class (scanner: `/tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/scan_crossings.py`,
hits: `.../scratchpad/hits.txt`, 21 lines). I then triaged by hand. I also read the existing probe
(`docs/records/edition-homework/probe-crossings-results.txt`).

| Where | Comparison | Kind of body | Confidence |
|---|---|---|---|
| `core/src/test/java/com/legend/integration/M2MIntegrationTest.java:264` (`SalaryBandMapping`; `salary: Decimal[1]` at `:57`) | `$src.salary < 50000`, `< 100000` (Decimal vs Integer) | M2M `$class$` | high (the probe measured it: 4 bodies) |
| `core/src/test/java/com/legend/integration/M2M2RTabularTest.java:147` | the same `salaryBand` mapping (Decimal vs Integer) | M2M `$class$` | high for the types. Not in the probe's list, so maybe not reached by its tests or not run then. |
| `core/src/test/java/com/legend/compiler/spec/TdsNullTypingPinTest.java:74-76` | `function f::r(v:String[0..1]):Boolean[1] { $v != meta::pure::tds::TDSNull }` (String vs `meta::relational::metamodel::SQLNull`, a class that `EqualityClasses.of` does not exclude, `EqualityClasses.java:122-123`) | user function | high (the probe: "user function f::r == STRING vs SQLNull") |
| `datacube/src/pure-library-text.ts` (5,263 lines), also `projects/timeseries/model.pure` | about 105 lines with `->toOne() OP <int literal>` over column reads, e.g. `:262` `$r->read($value)->toOne() <= 0`; `:265`, `:278`, `:325` `... == 0`; `:383` `$x.n->toOne() == 1`, `$x.s->toOne() == 0`; `:416`, `:425`, `:439` `$x.pk->toOne() == 0`; `:1739-1741` `$x.x->toOne() < 0`, `> 0`; `:590` `$x.var->toOne() < 0` | functions generic over relations (`ColSpec<V⊆T>` parameters), typed at the CALL over the caller's columns (`Overloads.java:128-132`) | **medium.** A crossing appears only where the caller's column is Float or Decimal; Number-typed lines (`value:Number`) are unmarked. Not measured: the probe did not run the DataCube lanes. B3 step 5a's own client regression (the `#TS{}#` islands refused) shows this library meets marking. |
| `core/src/test/java/com/legend/protocol/ModelComposerRoundTripTest.java:57` | constraint `positive: $this.quantity > 0` (`quantity: Float[1]`, `:68`) | constraint | high for the types. Low exposure: the test parses and composes and calls no compile entry. It surfaces only if `compileErrors` checks it. |
| `core/src/test/java/com/legend/server/CreateClassTest.java:86`, `:197` | `$x.notional > 100` | query body (already marked today) | not new |
| `AssociationIntegrationTest.java:1526`, `MetamodelMappingStoreTest.java:151-162`, `ElementParserTest.java:716`, `projects/regulatory-capital/model.pure:989` | (scanner false positives: queries, parse-only, or the `1` matched inside a name) | n/a | n/a |
| the boot layer's bodies (system metamodel functions, prelude derived properties) | unknown | functions typed at boot (`Compiler.java` `compileAllBodies` skips them, `:1003-1010` area) | **not checked.** Under the product rule these would throw, and they are reached by metamodel queries. |

Not covered by the scanner:
- property-to-property crossings, such as an Integer property compared with a Float property;
- calls through helper functions;
- Date/DateTime literals typed by inference;
- comparisons over relation columns whose types come from a database schema.

**Existing mechanical listers.**
- `Compiler.compileAllBodies` (`L/Compiler.java:993-1040`) already types every module body eagerly with one
  `SpecCompiler`.
- The withdrawn draft (`docs/records/edition-homework/withdrawn-function-body-door-draft.patch.txt`) added two things:
  - `Compiler.modelCheck(String, words)` (patch lines 34-53): `compileErrors` plus every crossing in a compiled body,
    gathered by an overload of `compileAllBodies` that takes a `crossings` sink (patch lines 12-29);
  - `SpecCompiler.crossingsOf(TypedFunction)` (patch lines 174-188): `CrossTypeComparisons.in(compile(fn).body())` as
    compile errors through the body's frame, skipping mapping code (`Typer.mappingCode`).
  - Both depend on marking being ON inside bodies (the draft's `Typer` change, patch lines 197-220). Revived as a tool,
    they list crossings in WRITTEN bodies. They do not list relation-generic bodies typed per call site (DataCube), which
    `compileAllBodies` types once with type variables (unmarked).
- `docs/records/edition-homework/probe-crossings-outside-the-query.patch.txt` plus `probe-crossings-classify.py.txt` is
  the run-time lister: it prints `PROBE-CROSS` at `Overloads.java:143` whenever a crossing is typed unmarked, with its
  scope. It catches per-call-site typing too. Run it on the DataCube lanes before switching the product rule on.

### A5. Where each door reaches the per-query `SpecCompiler`

The choke points that construct a `SpecCompiler`:
- `L/TypedQuery.java:36` (constructor `:33-38`), through `Compiler.query(ctx, String)` `L/Compiler.java:750-757`,
  `query(ctx, ValueSpecification)` `:761-763` and `queryResolved` `:768-770`;
- `Compiler.hasStatementEffects` `:915` (facts only, unmarked; keep unmarked);
- `Compiler.lowerResolved` `:946` (product-pinned, `Styled.PRODUCT`);
- `Compiler.compileAllBodies` `:995`.

Note that `body()` is typed lazily, and `admitted(...)` forces it. So the setting must be given at construction
(`Compiler.query*`/`queryResolved` take it) or by a `TypedQuery.with...` that replaces the `SpecCompiler` before
`body()` runs.

| Door | Entry, file:line |
|---|---|
| product API | `L/Execution.java:127`, `:138` |
| product lite/v1 HTTP | `L/server/LiteV1Planning.java:105`, `:123`, `:147`; compile route `:54` → `Compiler.compileErrors` → `compileAllBodies` |
| product server tools | `L/server/ModelChanges.java:108`, `:112`; `L/server/CreateClass.java:118` |
| product planner internals | `L/planner/OutputPlan.java:88`, `:120`; `L/testplan/GeneratedTestData.java:132`; `L/planner/PlanAllocations.java:398` → `lowerResolved` |
| pure/v1 (legend-engine) | `L/edge/PureV1Routes.java:216`, `:253`, `:271`, `:312`, `:355`; rewrite today `:333-338` (`EngineComparisons.asTheDatabases`, `PureMeaningRewrite`); compile route `:192` → `ModelInput.compileErrors` (`L/edge/ModelInput.java:43-44`, `66-67`) |
| legend-engine edge, other routes | `L/edge/HostAnswers.java:420`; `L/edge/TestDataRoutes.java:356`, `:395`, `:417`, `:481`; `L/edge/ServiceTestRunner.java:259`, `:286` (`queryResolved`); `:277` (`hasStatementEffects`) |
| wasm | `wasm/src/main/java/planner/Boundary.java:293`, `:330` (product); `wasm/src/edge/java/com/legend/edge/wasm/EdgeBoundary.java:37-38` → `PureV1Routes.route` (legend-engine) |
| DataCube tool | `datacube/tools/offer-facts/OfferFacts.java:82`, `:133`, `:210`, `:220` (product) |
| relational corpus | `L/test/PureTestRunner.java:231`, `:294`, `:358` (`audience.admitted(Compiler.queryResolved(...))`; the `Audience` interface is `L/test/Audience.java:18-62`, the corpus's at `spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java:597-634`, door `UpstreamTestBodies.admitted` `L/edge/UpstreamTestBodies.java:~53-57`); `:216` `hasStatementEffects`; `spec/src/test/java/com/legend/rcorpus/LeanSqlCorpusTest.java:61-62`; `MinimalCorpus.java:328`; `EagerCorpusCompileProbe.java:31` (`compileAllBodies`) |
| core's own Pure tests | `PureTestRunner` with `Audience.PRODUCT` (`L/test/Audience.java:43`) |
| PCT channel A (legend-pure / legend-engine / lite) | `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java:134`, audience switch `:144-148` |
| PCT channel B | `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:409-412` (`queryResolved` + `UpstreamTestBodies.admittedForPure`), `:417-424` |
| PCT probes | `pct/.../probe/BcDateProbeTest.java:45`; `pct/.../extension/EdgeNetPlantedTest.java:93` |
| IDE/LSP | `L/server/PureLspServer.java:147` calls only `Compiler.parseModel`: there is no typing entry |
| python | none: it reaches the server over HTTP (no Java entry found) |

---

## PART B: stamping a corpus test body

### B1. The roads, traced

**Program road (the test door's statements and the product's query statements).**
- `L/planner/PlanMaker.java:665-667` (`planStatement`, test body `Door.TEST`), `:585`, `:621` (staged lets), and `:125`
  and `:201` (the product query) → `ProductStatements.prepare` (`L/planner/ProductStatements.java:112-163`).
- `inline(...)` is at `:126-127`, defined `:184-198`. It builds a `UserCallInliner` with
  `ResultEnvelopeSplice.hook(recorder)` (`:186-187`), calls `inlineBody(statements, mark)` (`:188`), and then the
  optional second inlining with CTE reads (`:190-195`, `Recorder.again` at `L/planner/ProgramFrames.java:~273-292`).
- After that: `frameAnswered` (`:205-219`), the harness's own let terms read free into `env.queryLets` (`:135-139`), the
  frames' consumed lets (`:141-145`), `NativeDispatch.stage` (`:149-150`), and the resolver (`:151`).
- `mark` is the statement's position. It feeds `instantiatedAs(-(statement+1))` (`UserCallInliner.java:~288`): object
  identity per statement, nothing to do with the stamp.

**Frames.**
- `ProgramFrames.Frame(chain, relationRooted, source, …)` (`ProgramFrames.java:~29-43`) is registered by
  `TestKinds.frame` (`L/testplan/TestKinds.java:226-264`) from `FramePlanner.chain(ec, program.lets(), …)`
  (`L/planner/FramePlanner.java:86-…`).
- `FramePlanner.chain` → `ExecuteChainAssembly.chain` (`L/compiler/spec/ExecuteChainAssembly.java:520-575`) inlines the
  query lambda with its OWN `new UserCallInliner(specs)` (`:527-528`; no hook, no stamp). It wraps the chain in
  `new TypedFrom(chain, bound)` only when the chain has no From (`:530-570`).
- The recorder (`ProgramFrames.Recorder`, `:251-370`) hands out views:
  - `frame(name)` (`:~296-322`) returns `View(f.chain(), …)` (pasted) or a CTE view;
  - `inlineExecute(ec, …)` (`:~324-348`) builds `fc.chain()` per inline `execute`.

**The splice.** `ResultEnvelopeSplice.hook` (`L/compiler/spec/ResultEnvelopeSplice.java:148-154`) → `rewrite` (`:187-403`).
- `$r.values` → `valuesRead` (`:631-651`) → `executedExtent(f)` (`:676-690`, a NEW `TypedFrom` via `fr.withExecutedExtent()`,
  or rung 12's `new TypedFrom(root getAll, …)` `:681-685`) → `readThroughExecuteValues` (`:659-667`, a NEW
  `TypedSerialize` for graph fetch).
- `at`/`toOne`/`first` (`:326-354`):
  - over a relation frame, the call collapses to the chain;
  - over a class frame, it becomes a NEW `TypedNativeCall(w.callee(), [spliced], …)`: the harness's call, re-minted.
- A bare frame variable → `bf.spliced()` (`:398-401`): `From(TypedFrameRef)` for a CTE read (`:89-103`).
- `executeLegendQuery` → `frames.inlineExecute(lq).chain()` (`:192-204`): the envelope.
- `$r.activities` → `TypedPropertyAccess(f.sourceExec(), "activities")` (`:421-425`): the raw execute call.

**The inliner applies the hook.**
- `UserCallInliner.rewrite` (`L/compiler/spec/UserCallInliner.java:1398-1411`) runs `hook.apply(n, bound)` and, if it
  changed, `return rewrite(h)` (`:1402-1407`): the replacement is walked again.
- `executeLegendQuery` is re-offered after a reduction (`:1604-1611`).
- Frame chains keep identity through that re-walk in the normal case. This is relied on by `ProgramFrames.pastes`
  (`ProgramFrames.java:230-240`): "by identity: a pasted read places the frame's chain itself".

**Judge road (`BodyDriver`).**
- `planValue` (`L/testplan/BodyDriver.java:~716-735`) → `planBare` → `BodyPlanner.sideBody` (`L/planner/BodyPlanner.java:180-188`).
- `planHostSide` (`BodyDriver.java:~743-768`) → `BodyPlanner.inlinedSide` (`:191-205`) then `stagedSide` (`:208-214`).
- `inlinedSide` builds `new UserCallInliner(specs, hook)` (`:199-200`) over `letPrefix + value` (`:202`).
- The hook is `BodyDriver.spliceHook` (`:481-547`). It is an adapter whose `frame()` returns `View(f.chain(), …)` of an
  `ExecFrame` (`:~366`) and whose `inlineExecute` returns `buildFrame(ec, …).chain()` (`:~509-516`).
- `BodyPlanner.bare` (`:439-448`, a frame CTE on the judges' road) uses `sideBody(..., hook = null)`: frame code.

**Judges' minted nodes** (`L/compiler/spec/VerdictQueries.java`):
- `assertEqualsOf` (`:47-58`);
- `valuesRead` (`:64-67`);
- `predicateVector` (`:77-87`);
- `assertAsPredicate` (`:468-492`: `not`, `equal`, `abs`, `minus`, `lessThanEqual` via `native1`/`native2` `:507-518`);
- `unrolledElement` (`:343-366`, its own inliner at `:351-353`, with the hook).

**The committed draft.** `UserCallInliner.stampingStoreFreeValues` (`:208-211`) and `stampedIfStoreFree` (`:226-236`),
applied to each let (`:277`) and the last statement (`:284-285`). It is enabled from `ProductStatements.java:127` by
`env.styled().edition().stampsOutsideStore()`.
- `EngineTable.stampsOutsideStore()` is `true` (`L/edge/EngineTable.java:71-74`).
- It is reachable only through PURE_V1's `STAMP_PROBE` (`L/edge/PureText.java:177`, `238-242`).
- UPSTREAM, the corpus's style, hands the product table (`PureText.java:138-141`), so the corpus does not stamp today.

### B2. When the tree is complete, and how to find the frame regions

| Road | Users' functions expanded | Frame chain spliced | Where a stamp pass goes | What it must skip |
|---|---|---|---|---|
| program road, harness statement | inside `inlineBody` (`UserCallInliner.java:~277-289`, the reductions) | inside the same pass (hook at `:1402-1407`) | **in the inliner** (recommended), turned on from `ProductStatements.inline` `:184-198` with a corpus-mode flag and the recorder's frame identity set. Or a post-pass right after `:188`/`:194`, before `:135`. | (1) every node, by identity, of every chain the `Recorder` handed out (`frame()` `f.chain()`, `inlineExecute()` `fc.chain()`, the envelope for `executeLegendQuery`); (2) `TypedFrameRef`; (3) the arguments of any `execute`/`executeLegendQuery`/`executionPlan::execute` call left in place (orchestration-kept, `:1546-1556`, and `$r.activities`' re-rooted call) |
| program road, frame eager run / CTE | the frame's own inliner (`ExecuteChainAssembly.java:527`) | n/a | none: `TestKinds.java:356` (`eagerRun`) and `:367` (`cte`) must pass stamping OFF. With the corpus rule, today's draft flag would have to be off here. | everything |
| `frameAnswered` (last statement an `execute`) | yes | the root replaced by `view.chain()` (`ProductStatements.java:205-219`) | same as row 1. The root IS a frame, so it is unstamped. | the chain |
| judges, `planHostSide` / `planValue` | `BodyPlanner.inlinedSide` `:202` | the same pass (`spliceHook`) | in the inliner built at `BodyPlanner.java:199-200`, turned on by the caller (`inlinedSide` gains a flag; `sideBody` passes it; `bare` `:439-448` passes OFF). The identity set is collected by `BodyDriver.spliceHook`'s adapter (`:481-547`). | as row 1, with `ExecFrame.chain()` |
| judges, unrolled quantified element | `VerdictQueries.unrolledElement` `:351-353` | its hook | the same flag on that inliner | as row 1 |

**Why identity, checked on entry.**
- The splice re-wraps frame code in fresh nodes: `fr.withExecutedExtent()`, rung 12's `new TypedFrom(root, …)`,
  `sz.readThroughExecute()`, `fr.withSource(TypedFrameRef)`. So the chain's ROOT identity can be lost, but its
  descendants keep theirs, and the wrappers hold no native call of their own.
- A depth counter raised in `UserCallInliner.rewrite(n)` (`:1398`) when `n` is in the frame set or is a `TypedFrameRef`
  is robust even when the inliner then rebuilds the subtree (`reached()`, `LiteralUnroll.fold`), because the decision is
  taken before the rebuild.
- Do NOT raise the depth around the hook's whole replacement (`:1402-1407`). For a class frame, the replacement is the
  HARNESS's own `toOne`/`at` re-minted over the chain (`ResultEnvelopeSplice.java:350-352`), and it must be stamped.
- Stamp at depth 0 in the `TypedNativeCall` arm (`:1538-1612`) and on calls produced by the folds after `rewriteSwitch`
  (`:1410`).
- For the `execute` call itself, raise the depth across its arguments (the arm's orchestration branch `:1546-1556`
  already special-cases the runtime argument). A harness `let r = execute(|…)` is reduced at `:277`, and its query
  lambda must not be stamped.

**If a post-pass is used instead**, it must memoize old→new by identity. Otherwise two references to one shared subtree
(a let term placed at each read, a frame read twice before `again`) become two distinct nodes. That breaks:
- `ProgramFrames.pastes`;
- the inliner's `reducedTerms`;
- object identity (the inliner's comment at `:1597-1599`: "a gratuitous rebuild would re-mint a let-bound instance per
  side").

The committed `stampOutside` (`:240-246`) rebuilds through `mapChildren` without such a memo. It skips only
`reducedTerms`.

**The draft's rule, measured against the corpus.** `stampedIfStoreFree` asks `StatementEffects.reaches(written, isAStoreRead)`
(`L/compiler/spec/StatementEffects.java:154-189`) on the written statement. That walk does not follow a `TypedVariable`
to its let.
- So `assertEquals('Firm X', $result.values->toOne().legalName)` is judged store-free.
- Its REDUCED tree, which contains the spliced frame chain, is then fully stamped.
- At the pure/v1 door this cannot happen (no frames). At the corpus door it would stamp the frame's query code as
  outside the store.
- The corpus needs the frame-region rule above, not `stampedIfStoreFree`.

### B3. The judges' minted natives

| What | Natives | Touched by a table row? |
|---|---|---|
| `VerdictQueries` | `assertEquals` (`:47-58`), `not`/`equal`/`abs`/`minus`/`lessThanEqual` (`:468-518`), a `TypedMap` vector (`:77-87`), `TypedPropertyAccess` `.values` (`:64-67`) | No row today: none of substring, indexOf, toOne, round, parse*, raise |
| other natives named in `testplan/*.java` and `VerdictQueries.java` | `pair`, `equal`, `eq`, `type`, `abs`, `list`, `distinct`, `contains`, `at`, `not` | No row today |
| `edge/UpstreamTestBodies` | assert rewrites, e.g. `assertSameElements` via `EngineSubstitutes.call` (`L/edge/UpstreamTestBodies.java:~70-80`), and `typed.specs().typeExpression(...)` (`:263`) | assert identities only; no row |
| `edge/UpstreamPrints:293` | mints a print read | not a table row today |

`equal`/`eq`/`contains` matter only for the comparison setting (A), and they are minted as `TypedNativeCall` directly,
never through `Overloads`, so they are never marked.

**Where they would be stamped.** They enter the side before `planValue`/`planHostSide`, so the judge inliner
(`BodyPlanner.java:199-202`) stamps them at depth 0 if it is in corpus mode. That matches §2.3 ("the judges' own
calls"), and it is harmless while no row touches them.

The database judge's fused statements (`L/testplan/DatabaseAsserts.java:~466-468`) compose already-planned SQL and mint
no typed natives that need a stamp.

### B4. Seeded lets and frame query lets

| Lets | Side | Passes the stamp point? |
|---|---|---|
| a harness statement's own let terms (`inliner.queryLets()` = the reduced `scope.terms`, `UserCallInliner.java:~281`), read free into `env.queryLets()` (`ProductStatements.java:135-139`) | harness | yes, if stamping is inside the inliner: they are reduced at `:277`, depth 0. A post-pass over `inlined.body()` would MISS them, because they reach the lowering as top-level lets through `ProductStatements.seeded` (`:358-368`) → `SeedableLets.withSeedableLetPrefix` (`L/lowering/SeedableLets.java:31-57`). |
| a frame's consumed query lets (`fc.env().queryLets()` → `frames.consumed`, `TestKinds.java:244`; `ProgramFrames.consumedLets`; merged at `ProductStatements.java:141-145`; `ExecuteChainAssembly.java:529` `queryLetsSink.putAll`) | frame (engine) | no, correctly: they are inlined by the frame's own unstamped inliner and lowered at the statement's top (`Lowerer` `withQueryLets`, trace3 M3) |
| the judges' `env.queryLets()` from `inlinedSide` (`BodyPlanner.java:203`) | the side's, so harness lets plus any frame lets met while inlining | as row 1: stamped where reduced at depth 0 |

The seedability probe (`SeedableLets.java:43`) lowers with `Styled.PRODUCT` and discards the tree, so the stamp is
irrelevant there.

### B5. Corpus tests to watch the blend (`bazel run //spec:corpus_one -- duckdb host <test>`)

Roster status is from `spec/src/test/resources/rcorpus/duckdb-fail-roster.txt`. "ok" means not on the fail or skipped
rosters.

**Harness side** (the table's natives in the test's own code):

| Test | What it exercises | DuckDB |
|---|---|---|
| `meta::relational::tests::query::function::testFilterThroughAssociationUsingPlusFunction` | BOTH sides in one test. The query: `$e.firstName->toOne()`. The harness: `assertEquals('Firm X', $result.values->toOne().legalName)` over a CLASS frame (trace3 M1). | ok |
| `meta::pure::tds::tests::extensions::zScoreTest` | the one harness cross-number comparison, `abs(...) >= 3` | ok |
| `meta::relational::functions::toPostgresModel::tests::testConvertLiteral` | `substring` in the helper `parseSemiStructuredPathNavigation`, and the `getDynaFunctionConverterMap` crossing | ok |
| `meta::relational::functions::toPostgresModel::tests::testConvertSelectSQLQueryWithCTE` | same as above | ok |
| `meta::pure::executionPlan::m2m2r::tests::executeProjectWithNestedDerivedProperty` | harness `->toOne()` of `[0..*]` | ok |
| `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testAnnualizedDateRange` | harness `min()->toOne()->toString()` of a StrictDate | ok |
| `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testCmeDateRange` | same as above | ok |
| `meta::relational::tests::projection::aggregation::testDeepSubAggregation` | harness Float `toString` | ok |
| `meta::pure::tds::tests::extensions::columnValueDifferenceTest` | harness `makeString` over Date and Float | ok |
| `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeConstantParameterNoTimeZone` (and its 4 siblings) | harness `indexOf` over `$planAsString` | FAIL today: watch, do not gate |

**Query side** (inside `execute(|...)`; source: the pinned
`legend-engine-xts-relationalStore/.../core_relational/relational/tests`):

| Test | Query construct | Source line | DuckDB |
|---|---|---|---|
| `meta::relational::tests::query::association::toOne::testAssociationToOne` | `$p.firm->toOne().legalName == 'Firm X'` | `query/testAssociationToOne.pure:23` | ok |
| `meta::relational::tests::query::association::toOne::testTwoAssociationsToOneBranch` | two `toOne`s | `:46` | ok |
| `meta::relational::tests::query::association::toOne::testTwoAssociationsToOneDeep` | nested `toOne` | `:62` | ok |
| `meta::relational::tests::query::function::testFilterUsingPlusFunction` | `$p.firm->toOne().legalName + ' Great'` | `query/testWithFunction.pure:37` | ok |
| `meta::relational::tests::query::function::testFilterUsingStartsWithFunction` | `toOne` | `:54` | ok |
| `meta::relational::tests::query::function::testFilterUsingContainsFunction` | `toOne` | `:137` | ok |
| `meta::relational::tests::query::function::testFilterTimesWithManyOperands` | `$p.age->toOne()`, `$p.firm->toOne()` in project | `:436` | ok |
| `meta::relational::tests::query::eval::testAssociationToOneThroughEval` | `toOne` | `query/testWithEval.pure:23` | ok |
| `meta::relational::tests::query::function::round::testFilterUsingRoundFunction` | `$t.id->round() == 1` | `query/testWithFunction.pure:259` | ok |
| `meta::relational::tests::query::function::testFilterUsingSubstringFunction` | `substring(1, 5)` in the query | `:102` | FAIL today: watch, do not gate |

Also query-side and failing today: `testFilterUsingParseIntegerFunction` and `testFilterUsingParseDecimalFunction`
(`substring(...)->parseInteger()`/`parseDecimal()`, `:198`, `:205`).

---

## Recommended outline

### Part A
1. Do the relational side first, with one of two options:
   - **(a) Per call.** Lite identities `meta::legend::lite::equalAsSql` (EQ, and NEQ under `not`), used by
     `RelOpTranslator.java:701-702`, the enum decode `MappingNormalizer.java:2490`, and the PURE pass-through of
     `equal`/`in`/`contains`/`max`/`min` (`RelOpTranslator.java:561`). Each gets a lowering row equal to its Pure twin,
     and every equal-identity matcher in `resolver/` and `lowering/` (about 24 references) learns it.
   - **(b) Per function, cheaper.** `Synthesized.relationalLanguage` set at `MappingNormalizer.java:799` (Relational
     `cm`), `AssociationSynthesis.java:544` and `LiftedViews.java:106`, read in `Typer.inFunctionScope`. Plus ONE Lite
     identity for the enum decode used under `~func` (`MappingNormalizer.java:897` → `:2490`).
   - Either way, rule on the XStore/ModelJoin column-typed conditions.
2. Make the setting a constructor argument of `SpecCompiler`, read by `Typer`.
   - Every door constructs through `Compiler.query*`/`queryResolved` (`Compiler.java:750-770`) or `compileAllBodies`
     (`:995`).
   - `PureTestRunner`'s `Audience` gains the setting, and `admitted` stops rewriting comparisons.
   - PCT channel A's audience switch (`PctPlanRoute.java:144-148`) and channel B's `typed` (`ChannelB.java:409-412`)
     pass it.
   - `hasStatementEffects` and `lowerResolved` keep the plain call.
3. Apply the rule in three places:
   - in `typeQueryBodyMarked`;
   - in `inFunctionScope` for Pure-language bodies;
   - in `Overloads.inlineNormalized` (`:128-132`), with the callee's language.

   Keep islands off. Answer at the mint (`Overloads.java:143-149`):
   - product: throw;
   - legend-engine: the call;
   - legend-pure: the marker answered at the mint, or by the inliner's fold.
4. Before turning the product rule on, run the `PROBE-CROSS` probe on the DataCube, warehouse and wasm lanes, and check
   the boot layer's bodies. Decide on the DataCube timeseries library's integer literals (`0`, `1`, `2` against
   Float/Decimal columns).
5. Revive `Compiler.modelCheck`/`SpecCompiler.crossingsOf` as the product's compile route
   (`LiteV1Planning.java:54`). It is unnecessary once the product throws at the mint and `compileAllBodies` types under
   the product rule, but it is useful as a listing tool.

### Part B
1. Keep `stampedIfStoreFree` for pure/v1 only. Add a second mode, "outside the frames", for the corpus.
2. The `UserCallInliner` gains `stampingOutsideFrames(Predicate<TypedSpec> frameNode)` and an int `frameDepth`:
   - `rewrite(n)` (`:1398`): if `frameNode.test(n) || n instanceof TypedFrameRef`, raise the depth for the subtree;
   - the `TypedNativeCall` arm (`:1538`): for `execute*`, raise the depth over its arguments; at depth 0, return the call
     with `stampedOutsideStore(true)`, also after the folds at `:1410`;
   - let terms are stamped where reduced (`:277`), so `queryLets()` carries stamps.
3. `ProgramFrames.Recorder` (`:251-370`) collects an identity set of ALL nodes of every chain it hands out (`frame()`,
   `inlineExecute()`). `ProductStatements.inline` (`:184-198`) passes `recorder::isFrameNode` when the door's table asks
   for corpus stamping. `TestKinds.eagerRun`/`cte` (`:356`, `:367`) pass OFF.
4. On the judges' road:
   - `BodyDriver.spliceHook`'s adapter (`:481-547`) collects `ExecFrame.chain()` nodes the same way;
   - `BodyPlanner.inlinedSide`/`sideBody` gain the flag (`bare` `:439-448` OFF);
   - `VerdictQueries.unrolledElement`'s inliner (`:351`) gets it too.
5. Test with the B5 blend test `testFilterThroughAssociationUsingPlusFunction` first. Then the query-side `toOne` and
   `round` tests, and the harness `toOne`/`toString` tests.

---

## Not verified

- That frame-chain node identity survives `UserCallInliner.rewrite(h)` in every case. `ProgramFrames.pastes` relies on
  it, but the inliner's `reached()` and the folds may rebuild nodes. The on-entry depth design tolerates rebuilds below
  the entry; it does not tolerate the hook handing out a COPY of a chain rather than the chain.
- Whether `VerdictQueries.unrolledElement`'s output is re-inlined through `BodyPlanner.inlinedSide`, which would make
  its own inliner's stamping redundant.
- The DataCube timeseries crossings: whether callers' value columns are Float or Decimal (crossing) or Number (none). The
  count of about 105 is a regex count of integer-literal comparisons, not typed crossings.
- Whether the boot layer's bodies (the system metamodel functions, the prelude's derived properties and constraints)
  hold crossings.
- Whether `M2M2RTabularTest`'s `salaryBand` mapping is reached by its tests.
- Whether any relational mapping calls a user Pure function by name through the dynafunction pass-through
  (`RelOpTranslator.dynaFnName` returns the bare name for a non-dynafunction). Its body would be typed as Pure under the
  new rule.
- Whether UnionSynthesis, GroupBySynthesis, JoinChainEmission or ViewRelation mint any comparison outside
  `RelOpTranslator.translate`. A grep for `"equal"` and the comparison identities found none.
- Whether a harness let substituted into a frame's query at `ExecuteChainAssembly.chain` (`:526-528`, via `letPrefix`)
  should keep the harness's column. legend-engine evaluates such a let in its Java before the query, and the frame rule
  above gives it the engine column.
- The exact line ranges marked "~" moved during the session (a step-2a commit landed in `UserCallInliner` and
  `ProductStatements`); the rest were re-checked against HEAD `a2c40dcb`.
