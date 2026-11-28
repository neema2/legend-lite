# Trace 3: where an execute() frame's query meets the harness at lowering

L = core/src/main/java/com/legend. Everything below was read only. Nothing was built or run.

## The answer in brief

The rule cannot be used as stated. It holds for one case: a relation-rooted frame that gets its own outer `TypedFrom`. It breaks in four ways.

1. **Class-rooted frames lose their From.** The resolver eats the `TypedFrom` of every class-rooted frame that harness code reads (StoreResolver.java:2909-2913). By the time lowering runs, harness code and frame code are one chain with no From marker. This is the shape of the census's 382 `toOne` tests over class values.
2. **A query that writes its own `->from()` gets no outer From.** ExecuteChainAssembly.java:530 adds none, so the query's code above its own `->from()` sits outside any From.
3. **Some engine-meaning code is outside every From.** The frame's query lets are lowered at the statement's top (Lowerer.java:739-768). The `executeLegendQuery` envelope wraps the From from outside (FramePlanner.java:139-142).
4. **The pure/v1 door has no From to switch on.** Its queries get their runtime from the request, not from a `->from()` in the tree (PureV1Routes.java:282). The rule's "everything else is pure" would give those queries legend-pure's meaning. So the table for code outside a From has to be chosen by the door.

Three further points:
- **Separately planned frames** (eager runs, CTEs, activity SQL) should be given the engine table explicitly, never through a From in the tree.
- **Today the two edge styles differ only in their raises.** `EdgeLoweringForms.PCT` vs `.PURE_V1` (EdgeLoweringForms.java:27,30), and that choice is made per call site. So a field swap in the Lowerer is mechanically easy today. But the whole-statement passes and the memoized `whereZones` would mislabel as soon as the two tables differ anywhere else.
- **Tenet 6 ("no modes in the core") may forbid the Lowerer switch.** The door-rewrite precedent `PureMeaningRewrite` is the tenet-compliant alternative.

## 1. Every road by which a frame's query reaches `Lowerer.lower`

### (a) A frame lowered on its own

Every one of these gets the whole plan's one style.

**A1. Eager run at the let.** The test door's program road.
- Chain: TestKinds.frame (TestKinds.java:230-264) → eagerRun (:350-356) → ProductStatements.prepare(List.of(fc.runChain())) (ProductStatements.java:112-163) → BodyPlanner.prepare → ProgramState.lowered (ProgramState.java:130-135) → ProductStatements.lower (:343-351) → lowerer → PlanEnv.configure (:377-383).
- The same chain serves:
  - a frame run as a statement of its own (TestKinds.java:160-166);
  - `size(execute(...))` (TestKinds.java:312-321, eagerReadsIn).
- `env.frame()`:
  - BodyPlanner.prepare sets it from the first From in breadth-first order of the resolved body (BodyPlanner.java:240-244; ExecutionContext.froms, ExecutionContext.java:216-228).
  - It is non-null when a From survives resolution. The root From survives (StoreResolver.java:381-388).
  - It is null when the query carried its own inner From and the code above it consumed it (see 2).
- Style: `PlanEnv.styled()` from `program.style()` (PlanEnv.java:178-195).

**A2. A frame's CTE, when a statement reads the frame more than once.**
- Program road: TestKinds.cte (:362-372) → FramePlanner.cte (FramePlanner.java:222-279) → ProductStatements.prepare + ProgramState.lowered. It is then referenced by `FrameCtes.attach` (ProgramState.java:132-134).
- Judge road: BodyDriver.buildFrame (BodyDriver.java:436-437; only when eager and a verdict batch exists) → FramePlanner.cte(fc, letPrefix, specs) (:215-217) → BodyPlanner.bare (BodyPlanner.java:439-448) → sideBody → prepare (frame set as in A1) → lowerAndPrepare (:123-139) → lowererFor (:143-174) → env.configure.
- Which frames: relation-rooted with a static schema (FramePlanner.java:228-257), and plain class extents, rung 12 (:259-277).

**A3. The activity SQL** (`sql($r)`, `RelationalActivity.sql`, the string entry's activity SQL).
- ProgramFrames.Recorder.relationalActivitySql (ProgramFrames.java:355-369), BodyDriver.spliceHook (BodyDriver.java:522-546) and FramePlanner.activitySql (FramePlanner.java:184-187) → PlanAllocations.activitySql → `ActivityTexts` facet → EnginePlans::activitySql (PureText.java ActivityTexts arm) → EnginePlans.engineSql (EnginePlans.java:147-175).
- engineSql builds its own `Lowerer` with `.withStyled(env.styled())` (:171) and never calls BodyPlanner.prepare. So `env.frame()` is whatever the caller's environment holds, and there is no time zone from `configure`.

**A4. `frameAnswered`.** An `execute()` as a test program's last statement: the root is replaced by the chain and lowered as the statement (ProductStatements.java:199-213).

### (b) A frame spliced into a harness statement and lowered with it

**B1. The program road.**
- ProductStatements.inline with `ResultEnvelopeSplice.hook(recorder)` (ProductStatements.java:181-193). ProgramFrames.Recorder.frame (ProgramFrames.java:300-324) returns a pasted View, or a CTE View once the frame is read twice.
- Then the resolver (:150) → BodyPlanner.prepare (:163) → ProgramState.lowered → ProductStatements.lower.

**B2. The judges.**
- BodyDriver.planValue / planHostSide (BodyDriver.java:716-768), with hook = spliceHook (:481-547).
- Then BodyPlanner.sideBody / inlinedSide / stagedSide (BodyPlanner.java:180-214) → SidePlanner.staged → BodyPlanner.prepare (SidePlanner.java:38-41) → lowerAndPrepare (:71-76).
- The database judge's fused statements also use `env.plan().style()` (DatabaseAsserts.java:468).

**Which kinds of frame take which road**

| Frame | What the splice gives harness reads | Separate lowering |
|---|---|---|
| relation-rooted, planned | `From(TypedFrameRef)`: the reference inside the frame's own From (ResultEnvelopeSplice.java:89-103) | A2's CTE; FrameRefs.reference (FrameRefs.java:25) only projects it |
| relation-rooted, unplanned | the From-wrapped chain pasted (`View.spliced` returns `chain`, :92-93); `at(0)`/`toOne` collapse to it (:337-349) | A1 only |
| class-rooted | `executedExtent(f)` = `fr.withExecutedExtent()` (:676-690); a REAL `at`/`toOne` over it (:350-352) | A1 only |
| `extentFrame` (planned plain class) | `new TypedFrom(root getAll, ctx, true, cteName)` (:681-685): the From wraps only the getAll; filters are in the CTE | A2 (VerdictSql.classExtentRows, FramePlanner.java:270-275) |
| graph fetch / serialize | the From with `readThroughExecute` set inside it (:661-668) | A1 |
| `executeLegendQuery` | envelope `TypedJsonResult(chain)` built **around** the From (FramePlanner.java:136-143; EngineExecutedValues.java:25-42) | A1 on `runChain`; never a CTE |
| inline `execute(...).values` | as for its kind, via `inlineExecute` (ProgramFrames.java:327-352; BodyDriver.java:511-519) | activity only |
| class concatenate | the harness map is pushed into copies of the From (StoreResolver.java:519-521; ClassConcatenates.java:30-41) | A1 |

## 2. In the spliced case, is the From still there at lowering?

**Assembly.** ExecuteChainAssembly.chain wraps the inlined query in `new TypedFrom(chain, bound)` only if the query has no From of its own outside lambdas (ExecuteChainAssembly.java:530, 567; containsTypedFrom :592-604).
- A query written `|Person.all()->from(m,r)->project(...)`, or one under the empty-mapping sentinel (:531-535), keeps its own Froms wherever the user wrote them.
- So code above an inner `->from()` is frame code outside any From.

**Resolver.**
- A From is rebuilt intact only when `resolveNode` is called on the From itself (StoreResolver.java:381-388). The mapping code is then resolved inside it, under `fromContext` (:3649), and SubQueryLift lifts under that context (:384-385). This happens:
  - when the From is the statement's root;
  - for children of relation-space wrappers (`structural`, :715-717; arms :569-617), e.g. a harness `->filter`, `->map`, `->sort` over a relation frame;
  - for arguments of natives.
- In the object chain walk, an in-chain From is **consumed**: `context = chainContext = fromContext(fr, …); cur = fr.source()` (:2909-2913), and it is never re-emitted. This hits:
  - every class-rooted frame that harness code reads (`.name` → scalarReadAsProject :554-556; `map` :540-543 and objectNode :444-455; class `at`/`toOne`/`size` :470-477; RoutingContext.spineContext :97-104);
  - a product or frame query `Class.all()->from()->project()`.
  - The resolved chain then mixes harness code (at → slice, toOne, property reads, the harness map's project) with the frame's filters and caps, and all the mapping code they bring in, with no marker.
- Consequence: `env.frame()` is null for such statements. They lose the frame's time zone, post-processors and features (BodyPlanner.java:240-244 reads only surviving Froms). I did not test this.

**Harness code inside a From.**
- For relation frames, no. Harness ops stay above (`structural`), and `withSource` swaps only the query under the envelope (ResultEnvelopeSplice.java:100-102, 664).
- Exceptions:
  - the harness `map` that ClassConcatenates distributes into copies of the From (ClassConcatenates.java:30-41);
  - the harness `.values` read setting `readThroughExecute` on the serialize inside the From (:661-668).
- ChainNormalizer's `last()` flip does not pass through a From (ChainNormalizer.java:85-98).

**Frame code outside a From.**
- **Query lets.** The frame's consumed query lets go to `env.queryLets()` (ExecuteChainAssembly.java:528; FramePlanner.java:132, 173; ProgramFrames.consumed :123-125; ProductStatements.java:135-140). SeedableLets prefixes them as top-level `TypedLet`s (SeedableLets.java:31-57; ProductStatements.seeded :358-368; BodyPlanner.java:128-129). The Lowerer then lowers them in `withQueryLets` (Lowerer.java:739-768) before any From is entered. The seedability probe itself uses `Styled.PRODUCT` (SeedableLets.java:43).
- **SubQueryLift** mints Froms (SubQueryLift.java:238-246, 275-283, 319-327) but strips them after resolving, so none reaches lowering. It lifts frame subqueries only under the frame's From (:59-65).
- **The `executeLegendQuery` envelope** sits outside (see 1).

## 3. Lowerer entry points into a From

**There are three dispatch entries, not one:**
- `lower(TypedSpec)`: `if (spec instanceof TypedFrom from) return lower(from.source())` (Lowerer.java:807-809);
- `relation()`: `case TypedFrom fr -> relation(fr.source())` (:1336);
- `scalar()`: `case TypedFrom fr2 -> scalar(fr2.source(), columns)` (:4377-4378).

Two more sites only classify: `scalarRootShape` (:774-776) and CollectionLanes.valueLane (CollectionLanes.java:132). The planned frame arrives already lowered (`TypedFrameRef` arm, :1231). A save-and-restore at the three entries would cover every subtree the resolver keeps.

**Style reads that do not follow subtrees:**
- Whole-statement passes in `lower(List)`: `styled().forms().relations(...)` (:522), `keyedCounts` and `computedPruning` (:534-535). `LetBinding.bindRepeats` (:531) and `sharedLower` (:507-517) also operate on the whole statement.
- `whereZones` is memoized on first use (:2315-2321).
- `styled()` is read per call site everywhere else (Scalars.lower :2277, 2301, 3521, 4346, 4353; CastPolicy :1813, 1964, 4574; executeValues :819, 4475; existsJoin :2235; unionFrame :2578, 2612, 2962-3016).
- After lowering: post-processors from `env.frame()` act on the whole statement (BodyPlanner.java:132-135; ProductStatements.postProcessed :455-458).

**Existing per-From mechanisms:**
- **Per statement, not per subtree.**
  - `PlanEnv.withFrame` takes the first surviving From's context (BodyPlanner.java:240-273).
  - `configure` sets `withDbTimeZone(timeZone())` once for the whole Lowerer (PlanEnv.java:163-166, 190-195; field Lowerer.java:183, 198-201). A harness DateTime literal outside the From spells in the frame's zone. That is what the splice comment means by "the connection zone the side's literals spell in" (ResultEnvelopeSplice.java:96-99).
  - Table renames are merged per statement (BodyPlanner.java:248-258; FramePlanner.renames :289-320).
- **A true per-subtree precedent is in the edge, before planning.** PureMeaningCheck.Walk widens features inside a From (`case TypedFrom f -> here.addAll(c.features())`, PureMeaningCheck.java:333-343). It classifies store-bound code, including the mapping reach (:85-97), and PureMeaningRewrite rewrites it at the door (PureV1Routes.java:333).
- **Scoped-field precedents in the Lowerer:** try/finally around `sharing` (:507-517), `frame.lowering` (:328-332), `iterationPositions` (:452-458), `recursions` (:1101-1110), `enclosing` (:1146-1150), and the `reportInputs` counter (:2691-2695).
- **Guard file:** CodeShapeGuardrailTest has `Lowerer.styled` on its mutable-field allowlist (core/src/test/java/com/legend/CodeShapeGuardrailTest.java:372). Switching it per subtree changes what that row allows, and the row needs a dated justification (AGENTS.md, "Guard files are load-bearing").

## 4. Product queries with `->from(mapping, runtime)`

- **Same road.** Compiler.query → TypedQuery.plan → PlanMaker (:132, :212) → ProductStatements.prepare/lower. The resolver keeps a root From (StoreResolver.java:381) and consumes a mid-chain one (:2909-2913).
- **Product-only lowerings are pinned to `Styled.PRODUCT`:**
  - Compiler.lowerResolved (Compiler.java:~957-965);
  - PlanMaker.count (PlanMaker.java:955-957);
  - SeedableLets' probe (:43);
  - lowerTestDataSeed with `PlanStyle.PRODUCT` (ProductStatements.java:426-434).
- With PRODUCT both tables would be identical, so nothing changes.
- **The pure/v1 door shares the same road.** Its queries have no `->from()` in the tree; the runtime comes with the request (PureV1Routes.java:277, 282; EdgeServer.java:43; PCT's LEGEND_ENGINE audience, PctPlanRoute.java:97). The rule's "outside = pure" would be wrong there.

## 5. Where each door chooses the style

There is one style per plan everywhere:

| Door | Style | Where |
|---|---|---|
| pure/v1 | `PureText.PURE_V1` | PureV1Routes.java:277, 282; EdgeServer.java:43 |
| PCT | UPSTREAM, PURE_V1 or PRODUCT by audience; plus UPSTREAM and PRODUCT for the one-plan check | PctPlanRoute.java:94-99; :224, 232 |
| PctBackend | UPSTREAM | :73, :129 |
| corpus | UPSTREAM | MinimalCorpus.java:526, 614; LeanSqlCorpusTest.java:132 |
| PCT channel B | UPSTREAM | ChannelB.java:448 |
| product | `PlanStyle.PRODUCT` | as in 4 |

How the style flows:
- Test road: PlanningOptions.style → BodyDriver.ExecEnv.of → PlanEnv.of (BodyDriver.java:75-86).
- Program road: `program.style()` (TestKinds.java:163, 241, 318, 358, 369) → ProductStatements.env (:169-171).

Per-frame configuration today:
- `featuresOf` merges `env.frame().features()`, `env.features()` and `treeFeatures(body)` into one set (BodyPlanner.java:86-96). `lowererFor` (:164) and ProductStatements.lowerer (:381) hand that set to the whole Lowerer with `withFeatures`.
- So "per frame" means per statement, keyed on the first surviving From. A per-frame table on that road would be statement-wide, not subtree-scoped.
- (`treeFeatures` scans the resolved body, where the resolver has already erased `withFeatureFlags` (StoreResolver.java:372-376). I did not confirm whether it ever finds any.)

The style is also read outside the Lowerer:
- the resolver's graph-key text (BodyPlanner.java:72-73);
- staging (PlanTextRoutines.java:27);
- DDL text (BodyPlanner.java:311, 328);
- the envelope (FramePlanner.java:136);
- the activity SQL (PlanAllocations.java:113, 187, 257).

## 6. The judges

- **Which `Styled` they get.** Every judge side is lowered through BodyPlanner.lowererFor → `env.configure` (BodyPlanner.java:150), with the body environment's style (BodyDriver.java:77). That is the whole plan's one style. The database judge's fused statements get the same (DatabaseAsserts.java:468).
- **Judge-minted typed nodes:**
  - VerdictQueries.assertEqualsOf, valuesRead, predicateVector (VerdictQueries.java:~30-80);
  - pairsVector and unrolledElement (used at AssertVerdicts.java:1300-1322, 1500-1521).
  - Their SQL wrappers (SidePlanner.wrap, VerdictSql, CanonRider, SharedRelationsInPlace at BodyDriver.java:727) read no `Styled`. `Styled` appears in no testplan/ file.
- **Yes, one lowered statement mixes judge-minted calls, harness code and a spliced chain.**
  - The hook splices `$r.values` inside the side's value (planValue :716-735; planHostSide :743-768 via inlinedSide's hook).
  - A frame read as its CTE is defined on the verdict batch (BodyDriver.java:497-503).
  - Under the rule the judge nodes would be outside any From, so they get the pure table. That is right for the host judge (tenet 5).

## 7. The M7 census (docs/records/edition-homework/r1/m7-corpus-harness-census.md, m7_tables.txt)

The census covers 2,584 corpus tests that typed, and counts only harness code: it excluded the query arguments, the lets and helper parameters handed to them, and every `TypedFrom`.

| Construct | Tests | Reaching a verdict | Where |
|---|---:|---:|---|
| `substring` | 4 | 3 | all only in helpers: sqlQueryToString / pureToSqlQuery library code |
| `indexOf` | 7 | 7 | 5 in the body, 2 in helpers; all over `$planAsString`, a plan's text, not a frame chain |
| `toOne` of a possibly-empty value | 382 | 337 | 195 in the body; operands: class `[0..*]` 230 tests, class `[0..1]` 133 |
| Float / date text | 77 | 77 | `toString` 51, `makeString` 25, `format` 1 |
| any of these | 418 | 372 | |

(Census md:65-81; tables :3-12, 23-63.)

**The record does not say how many of these sit in a statement with a spliced frame chain.** The probe ran on the typed tree before any splice and skipped Froms (md:28-33). Its examples show many over `$result.values.rows` (md:101-124), which would share a statement with the spliced chain.

## Where the rule mislabels code, and the smallest fix for each

| # | Code | Rule's label | Should be | Where | Smallest fix |
|---|---|---|---|---|---|
| M1 | Class-rooted frame: its filters, caps and mapping code, merged with harness `at` / `toOne` / `.prop` / `map` | pure (no From survives) | engine for the frame part | StoreResolver.java:2909-2913; ClassConcatenates.java:30-41 | Re-emit a marker at the frame boundary in `collectOpChain`, or have the splice wrap the frame chain as a planned CTE (as rung 12 does for plain extents). Either way the harness ops lower over a reference. The real fix is to plan every class frame as its own statement and read it by reference. |
| M2 | Query with its own inner `->from()` or empty-mapping branches | code above the inner From is pure | engine | ExecuteChainAssembly.java:530-567 | Always wrap the whole query in an outer From. A second From is harmless: its context is already in the inner one. |
| M3 | Frame query lets | pure (lowered at top) | engine | Lowerer.java:739-768; SeedableLets.java:31-57; ProductStatements.java:135-140 | Key the consumed lets by their origin (frame vs harness), and lower a frame let's value under the engine table. Or inline them into the chain at assembly. |
| M4 | `executeLegendQuery` envelope (`TypedJsonResult`) | pure (outside the From) | engine (legend-engine's result format) | FramePlanner.java:136-143; EngineExecutedValues.java:25-42 | Mint the envelope inside the From (`fr.withSource(envelope)`), or treat `TypedJsonResult` as an engine-zone entry. |
| M5 | Separately planned frames (A1-A4) | depends on a surviving root From | engine | TestKinds.java:350-372; BodyPlanner.java:439-448; EnginePlans.java:171 | Pass the engine table explicitly at those call sites (a `PlanEnv.withStyle` before `configure` / `styled()`). Covers activity SQL, which never sees a From switch. |
| M6 | pure/v1 and PCT LEGEND_ENGINE queries (no From in the tree) | pure | engine | PureV1Routes.java:282; PctPlanRoute.java:97 | The door supplies two tables, (outside, inside). PURE_V1 = (engine, engine); corpus UPSTREAM = (pure, engine); PCT LEGEND_PURE = (pure, pure); PRODUCT = (product, product). |
| M7 | Whole-statement passes: `relations`, `keyedCounts`, `computedPruning`; post-processors | the outer table, over frame SQL too | engine for frame SQL | Lowerer.java:522, 534-535; BodyPlanner.java:132-135 | Identical in the two edge tables today (EdgeLoweringForms.java:96-116). Pin that equality in a test, or apply these passes to a From's SQL at the From boundary. |
| M8 | Memoized `whereZones` | whichever table first asked | per zone | Lowerer.java:2315-2321 | Memoize per table, or ask the current table at each merge. |
| M9 | `env.frame()` facts: time zone, features, renames | the first surviving From's, statement-wide; lost when no From survives | per frame | BodyPlanner.java:86-96, 240-258; PlanEnv.java:163-195 | Not part of the table split, but the same root cause as M1. Carry the facts on the frame's reference rather than reading them off the tree. |
| M10 | Style reads outside the Lowerer | the plan's one style | per zone | BodyPlanner.java:72-73, 311, 328; PlanTextRoutines.java:27; FramePlanner.java:136; PlanAllocations.java:113, 187, 257 | Each needs its zone decided where it reads the style. Graph-key text inside a frame chain is engine; DDL and plan texts are harness. |

**The constraint behind the whole design.**
- Tenet 6 says the core has no modes, and the switch rule allows only door switches, turned into a rewrite or refusal before the planner.
- A Lowerer that flips tables inside a From may not satisfy that; it needs a ruling.
- The compliant precedent is a door rewrite like PureMeaningRewrite. It walks store-bound code, mapping reach included (PureMeaningCheck.java:85-97, 327-343), and rewrites the engine-meaning calls into engine natives before planning (PureV1Routes.java:333). It would sidestep M1-M4 and M7-M8, because the labels are fixed on the typed tree before the resolver erases any From.

## Not verified

- That `env.frame()` is in fact null at run time for class-rooted harness statements (M9). This is inferred from the code; no test was run.
- Whether `treeFeatures` ever sees a feature flag on the resolved body.
- How many of the 382 `toOne` and 77 date/Float-text census tests are over a class-rooted frame (M1) and how many over a relation frame. The census does not say.
- DriverPkAppend and ImportDataFlowAppend (BodyPlanner.java:263-272): not read in detail. I assume they act inside the From.
- PlanTextRoutines and GraphKeyText: not traced to the nodes they produce.
- Compiler.java:957-965: from its surrounding text, the enclosing method is `lowerResolved`.
