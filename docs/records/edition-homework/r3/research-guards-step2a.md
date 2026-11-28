# Step 2a: what the draft will break or make stale

I checked branch `claude/edit-ahead` at 98233367 against `claude/modest-galileo-h6swr4`, 36 files under `core`. That count includes `core/src/test/java/com/legend/edge/EngineTableShadowTest.java`, which landed during the check. I only read code; nothing was built or run.

The draft deletes **six** members of `LoweringForms`, not five: `view`, `wireCast`, `relations`, `keyedCounts`, `computedPruning` and `unionFrame`.

## A. Compile errors

1. **`core/src/main/java/com/legend/planner/PlanMaker.java`: four calls still pass `style` to `sql(...)`.** No 4-argument overload is left. The only two are `sql(RenderedStatement, Target, SqlQuery)` at line 1020 and `sql(SqlQuery, SqlDialect, Target)` at line 1239.
   - **:232-233** `return sql(dialect.renderStatement(new SqlDml.CreateTableAs(schema, table, statement)), target, null, com.legend.database.PlanStyle.PRODUCT);` becomes `return sql(dialect.renderStatement(new SqlDml.CreateTableAs(schema, table, statement)), target, null);`
   - **:600-601** `... List.of(), true)), state.target(), null, state.style());` becomes `... List.of(), true)), state.target(), null);`
   - **:640-641** `... StagedRows.columns(rowsRoot.info()))), state.target(), null, state.style());` becomes `... ), state.target(), null);`
   - **:857-858** in `staging(...)`: `... Objects.requireNonNull(staged.queries().get(table), table))), target, null, style);` becomes `... ), target, null);`. Its `style` parameter is then unused. Dropping it is optional.

2. **`core/src/main/java/com/legend/compiler/spec/UserCallInliner.java:566`**: `new TypedNativeCall(call.callee(), pargs, call.info(), null, call.outsideStore())`. Here `call` is a `TypedUserCall` (`inlineCall(TypedUserCall call)`, line 550), and `TypedUserCall` has no `outsideStore()`. Use the 4-argument form `new TypedNativeCall(call.callee(), pargs, call.info(), null)`, or pass `false`. A user call becoming a native call has no stamp yet.

3. **Build failure: `core/edge_tests.bzl` does not list `EngineTableShadowTest.java`** (the list is `EDGE_TEST_SRCS`).
   - Because the file is missing from that list, the `core_tests_lib` glob compiles it (`core/BUILD.bazel:721`, `exclude = [...] + EDGE_TEST_SRCS`). That library has no `:edge` dependency, so strict deps fail.
   - Fix: after line 99 (`"src/test/java/com/legend/edge/PureMeaningEngineTest.java",`) add a dated comment and the entry:
     ```
     # the edition's table and the stamp in shadow beside A3's rewrite (the edition program's step 2a, 2026-11-25)
     "src/test/java/com/legend/edge/EngineTableShadowTest.java",
     ```
   - The `edge_test_classes()` target at `core/BUILD.bazel:1068` then selects it. The class is untagged, so `TaggedClassesHaveATargetTest` does not apply. `ArchitectureTest.productTestsReachNoEdge` reads the same list and is satisfied by the same line.

I found no other compile errors. I checked every rebuilt call site's receiver type, and every construction of `Styled` (2 sites), `TypedNativeCall`, `LoweringForms` implementers and `PlanStyle` implementers across core main/test, pct, spec, tools, wasm, datacube, engine-client, native and python.

## B. Guards that will fail, most certain first

1. **`core/src/test/java/com/legend/ArchitectureTest.java:1387`: `PLANNER_ENTRIES`** (exact, and on the shrink-only watched list). `thePlannerEntriesAreTheMeasuredOnesExactly` will fail.
   - The pinned row is `"lowering.SubselectPrune#prune(com.legend.sql.SqlQuery, boolean)", "com.legend.edge.EdgeLoweringForms",`.
   - It is stale because the edge's only call to it was `EdgeLoweringForms.computedPruning`, which the draft deletes. No other edge or root class calls it.
   - **SHRINK**: delete the row. Add a dated note in its place, for example `// (lowering.SubselectPrune#prune(SqlQuery, boolean) from EdgeLoweringForms: gone 2026-11-25 with the edition's E0c -- computedPruning deleted, the product's pruning written in Lowerer; a shrink)`.
   - Update the comment block above it (lines 1377-1382, "the subselect pruning (lean, or legend-engine's computed projections kept) and ...") so it names only the EXISTS sites.

2. **`core/src/test/java/com/legend/HookRulingTest.java:350-371`: `theTwoFormKindsAreAClosedListTheProductAnswersItsOwnForm`.** It pins `TWO_FORMS` as an exact `List.equals`.
   - **GROWTH**. After `com.legend.sql.dialect.DatabaseSettingsForm.class` (line 369) add:
     ```
     // EditionTable (the edition program's step 2, 2026-11-25, docs/records/edition-plan-to-finish-stage-b-2026-11-25.md
     // section 2.2: per native, the product rule a door's audience means; the product's has no row)
     com.legend.lowering.EditionTable.class
     ```
   - Optionally assert that `PlanStyle.PRODUCT.twoForm(EditionTable.class, LITE) == LITE`, and that `UPSTREAM` and `PURE_V1` hand back the product form while the probe is off.
   - Stale comment at line 365: `LoweringForms (... a raise, a view, a wire cast)`. Drop "a view, a wire cast".

3. **`core/src/test/java/com/legend/PlanStyleSurfaceTest.java:52`: `eachTwoFormKindMeetsCriterionA`.** `TWO_FORM_EXCEPTIONS = Map.of()` is watched.
   - Every `TWO_FORMS` kind must be an interface of bare signatures with no field. `EditionTable` declares the field `LITE`, so `breaksOfA` gives `"EditionTable#LITE (field)"`.
   - **Change the draft instead** (train 6's precedent: ProductLoweringForms, LiteDateLiteralTexts):
     - Move the product form into its own class, `core/src/main/java/com/legend/lowering/LiteEditionTable.java`: `public final class LiteEditionTable implements EditionTable { public static final LiteEditionTable INSTANCE = new LiteEditionTable(); private LiteEditionTable() {} ... }`.
     - Delete `EditionTable.LITE`.
     - Repoint the four users to `LiteEditionTable.INSTANCE`: `Styled.PRODUCT` (Styled.java:42), `PlanEnv.styled()` (PlanEnv.java:183), `PureOnlyTextsRefusalTest.java:52`, and the javadoc in `EditionTable`.
   - The alternative is pinning `"EditionTable#LITE (field)"` in `TWO_FORM_EXCEPTIONS` with a dated reason. That is a GROWTH that reopens what train 6 closed, so I don't recommend it.

4. **`core/src/test/java/com/legend/HookLambdasTest.java:110-113`: `CLASS_HOOKS`** (exact, watched). `theProductsHookClassesArePinned` will fail.
   - Every product class implementing a hook type is pinned, and `EditionTable` is now a hook type (through `TWO_FORMS`).
   - With the draft as written, the new implementer is the anonymous `com.legend.lowering.EditionTable$1`. With fix B3, it is `LiteEditionTable`.
   - **GROWTH** (needs a dated note). Next to the `ProductLoweringForms` entry, add:
     ```
     // the edition's table's product form, no row and no stamp (the edition program's step 2, 2026-11-25)
     Map.entry("com.legend.lowering.LiteEditionTable", Set.of("com.legend.lowering.EditionTable")),
     ```
     Use the key `"com.legend.lowering.EditionTable$1"` if B3 is not done.
   - Knock-on: `ProductFormsUseTheirArgumentsTest` iterates `CLASS_HOOKS`. `EditionTable` has no method taking a function, so the new form is skipped and needs no `RULED` row.

5. **`HookLambdasTest.java:56-61`: `TWO_FORM_CALLERS`** (exact, watched). `theTwoFormCallersArePinned` will fail, because `PlanEnv#styled()` now names one more class object and makes four `twoForm` calls.
   - **GROWTH**. Replace the entry with:
     ```
     Map.entry("com.legend.planner.PlanEnv#styled()", Set.of("com.legend.lowering.DateTimeTexts",
             "com.legend.lowering.ListTexts", "com.legend.lowering.PureOnlyTexts",
             // + LoweringForms (K5, train 5)
             "com.legend.lowering.LoweringForms",
             // + EditionTable (the edition program's step 2, 2026-11-25)
             "com.legend.lowering.EditionTable", "(twoForm reaches 4)")),
     ```

6. **`HookLambdasTest.java:177-179`: `FACET_CALLERS`** (exact; not on the watched list, but the dated-note rule still applies). `theFacetCallersAndForwardersArePinned` will fail: the entry lists every class object the method names, and `EditionTable.class` is a new one.
   - **GROWTH**. Change it to `Set.of("(facet reaches 1)", "com.legend.lowering.DateTimeTexts", "com.legend.lowering.ListTexts", "com.legend.lowering.EditionTable", "com.legend.lowering.LoweringForms", "com.legend.lowering.PureOnlyTexts")` and add a dated comment.

7. **`ArchitectureTest.java:4079`: `STATIC_STATE` / `staticStateIsConstantOrRegistered`.** It reads the edge's classes too (`CORE_AND_EDGE_CLASSES`).
   - `EngineTable.INSTANCE` is a static field holding an object whose final fields `asGiven` and `corrected` are `java.util.HashMap`s. `mutableObject` reports "an object com.legend.edge.EngineTable (its field asGiven: a collection java.util.HashMap)".
   - **Change the draft instead**: make the maps immutable. For example, build them in a static helper and assign `asGiven = Map.copyOf(built)` and `corrected = Map.copyOf(...)`. Registering them as a `StaticKind` would be wrong; they are constants.

8. **`core/src/test/java/com/legend/JavaEvalLedgerTest.java:1864+`: `FUNNEL_PACKAGE_REGISTERS["core/src/main/java/com/legend/edge"]`.** `theFunnelPackagesAreClosedRegisters` will report "NEW class in core/src/main/java/com/legend/edge: EngineTable.java".
   - **GROWTH**, with its tenet argument. After `"PureMeaningRewrite.java",` (line 1938) add:
     ```
     // the edition's table at legend-engine's door (the edition program's step 2, 2026-11-25): per native, which
     // product native's rule lowers a call, by its stamp and the statement's flags -- data, no SQL, no value computed
     "EngineTable.java",
     ```
   - No new file appears in any other funnel package (root, planner, exec, testexec, result, server, testplan, testdatagen).

9. **`core/src/test/java/com/legend/CodeShapeGuardrailTest.java:47`: `MUTABLE_FIELD_ALLOWLIST` / `mutableInstanceStateIsExplicit`.**
   - `    private boolean stampsStoreFreeValues;` in `UserCallInliner` is a non-final instance field, which the `MUTABLE_FIELD` pattern catches.
   - Option 1, **GROWTH**: next to `"UserCallInliner.configMode"` (line ~410), add `// 2026-11-25 (the edition program's step 2): whether the inliner stamps store-free statements' calls -- a builder flag set once by stampingStoreFreeValues before inlineBody, Lowerer.streamingGraphRoot's lifecycle` and the entry `"UserCallInliner.stampsStoreFreeValues",`.
   - Option 2 (better): make it `final` and set it through a constructor overload. That needs no entry.
   - The other new fields are fine: `storeReach`, `EngineTable`'s maps, `PureText.STAMP_PROBE` and `EngineTable.INSTANCE` are all final or static final.

## C. Stale but not failing (fix in the same pass)

- **`ProductFormsUseTheirArgumentsTest.java`**: the draft already removes the `relations` and `view` `RULED` rows with a dated note. The class javadoc at :36 still cites `computedPruning(pruned, kept)` as an example. The remaining `existsJoin ... arg 1.alias` value says "(view's arg 1's case)", which now points at a deleted row.
- **`testplan/PlanStyleFacets.java`** `TWO_FORMS` comment on `LoweringForms`: "a raise (B7's plain set), a view's nesting and a wire cast -- the product ... lowers a view flat and writes the cast". Two of the three are gone.
- **`edge/EdgeLoweringForms.java:26`** javadoc "every check written, views flat, wire casts written". There are also leftover blank lines where methods were removed.
- **`JavaEvalLedgerTest.java:1988-1990`** comment on `"EdgeLoweringForms.java"`: "a view nested and a wire cast left to the reader -- picks a lowered shape". Only the plain raises and existsJoin remain.
- **`EdgeOnlyDataTest.java`** `RULED_IN_MEANING` row comment "(ProductLoweringForms.view returns the body); its name read by the edge's view form". Both are gone.
- **`UpstreamBranchesTest.java:68`**, **`lowering/LowerRelationDuckDbTest.java:267`** ("ProductLoweringForms.computedPruning") and **`lowering/CastPolicy.java:51`** ("LoweringForms#wireCast"): comments only.
- **`lowering/CastPolicy.java:52-58`**: both branches are now `written(c, value, isMany, forms)`, so the `if` is dead.
- **`lowering/Fold.java:1635/1662`**: the `unionFrame` parameter is now always null at both Lowerer call sites.
- **`edge/PureText.java:171-177`**: `STAMP_PROBE`'s javadoc sits between `ENGINE_UNCHECKED`'s javadoc and its field, so `ENGINE_UNCHECKED` loses its doc (it becomes a dangling comment). Move `STAMP_PROBE` above that javadoc.
- **`compiler/spec/typed/TypedNativeCall.withArgs`**: public but called nowhere, although its javadoc says it is "the one way a pass rebuilds a call". No guard counts unused public methods. The 18 sites use the 5-argument constructor instead.
- **`ModeBranchCensusTest`** (report-only, does not fail): `java.lang.Boolean.getBoolean` gains one site, `PureText.<clinit>` for `legend.probe.stamp`. The report total goes up by 1.
- **`CodeShapeGuardrailTest.schemaAssertedOutputsOnlyShrink`**: one `outputsOf(` occurrence was in a deleted Lowerer comment, so the count drops. The test only checks `<= 23`, so it passes; note the slack.
- **Dated notes for the shrink-only check** (`tools/guards/shrink_only.py`): `CLASS_HOOKS` and `TWO_FORM_CALLERS` (HookLambdasTest) and `TWO_FORM_EXCEPTIONS` (PlanStyleSurfaceTest) are watched pins. Every added or raised entry needs a `YYYY-MM-DD` on its line or in the comment block directly above it. `PLANNER_ENTRIES` is watched too, but a removal needs no note there; add one anyway, per AGENTS.md.

## Probably fine (checked)

- **Package rules**:
  - `loweringDependencySurfaceIsPinned` admits `com.legend.platform` (so `Feature`) and `model.FunctionId` by name.
  - `core-layers.txt` and BUILD already give lowering `:model` and `:platform`, and give the edge `:lowering`, `:builtin`, `:model` and `:platform`.
  - testplan names `lowering.EditionTable` (it has `:lowering`), and the planner calls `Styled.edition()` and `UserCallInliner.stampingStoreFreeValues` (it has `:lowering` and `:compiler`).
  - The compiler names lowering only in javadoc, so `typedHirDoesNotDependOnCheckers` and the compiler deps are fine.
  - Invariant 7 (typed nodes built only by compiler, resolver, normalizer, lowering): every new construction is in those packages.
  - The edge implementing a lowering interface has EdgeLoweringForms as precedent.
- **`PLANNER_ENTRIES` gets no new rows.** `PureText`'s `EditionTable.class` is a class literal, not an ArchUnit access (the `LoweringForms.class` literals have no row either). `EngineTable` makes no access to planner or lowering members. `EngineTableShadowTest` is test code and is not imported.
- **`staticFieldsAreFinal`**: every new static is final.
- **`EdgeSeparationTest`**: `EditionTable` is not in `COMPAT_CLASSES`, so `PlanStyleFacets -> EditionTable` is no entry, and no product class reaches the edge. `theDatabaseJudgeReachesNoEdge` (transitive) is unaffected: `EditionTable` is in lowering.
- **`HookRulingTest`**:
  - `RULED`, the `everyImplementerOfARuledHook` implementer set and `HOOK_ENTRIES` don't change: `EditionTable` is a two-form kind, not a ruled hook.
  - `theFacetRegistryHoldsNothingButItsList` passes: `PlanStyleFacets`' fields are still `KINDS` and `TWO_FORMS`, and `EditionTable` is an interface.
  - `hookArgumentTypes` gains nothing: `FunctionId`, `boolean` and `Set` are not in the lowering package.
- **`OpenClassMembersTest`**: none of the touched classes is pinned, and `PlanStyleFacets`' members are unchanged.
- **`PlanStyleSurfaceTest`**: `OVERRIDERS`, `STYLE_METHODS` and `STYLE_SURFACE` are unchanged (no new anonymous class in `PureText`; `PlanStyle` itself is untouched). `theFacetTestsHoldEveryStyle` reads production jars only.
- **`HookLambdasTest.LAMBDA_HOOKS`**: unchanged. The new lambdas and method references (`UserCallInliner::isAStoreRead`, `this::stampOutside`) are a `Predicate` and a `UnaryOperator`. `PlanEnv`'s are `Function`s.
- **`EdgeOnlyMembersTest`**: every new public member has a product reader: `Styled#edition`, `TypedNativeCall#outsideStore`, `UserCallInliner#stampingStoreFreeValues`, `EditionTable.*`. Every `Pure` constant and `Feature` value `EngineTable` reads is also read in the product (Scalars, FeatureRules, StaticFold). `SubselectPrune#prune` was never in its `OPEN` or `READ_API`.
- **`EdgeOnlyDataTest`**: the new component `TypedNativeCall#outsideStore` is read by the product (`Scalars.lower`). `SqlSource$Subselect#frameName` is still read by Fold and the renderers.
- **`TypedSpecChildrenTest`**: the dummy builder reads record components and builds them reflectively; `boolean` is a value. `withChildren` keeps the stamp, so the round trip holds. No record-pattern deconstruction of `TypedNativeCall` or `Styled` exists anywhere, and no main code reads record components reflectively.
- **`JavaEvalLedgerTest`**: none of the 28 per-file `Map.entry("core/src/main/java/...", N)` pins (plus pct's `ValueBridge.java`) is a touched file. The literal-fold pin reads `typed/Literals.java`, which is untouched.
- **Other shape and census guards**:
  - `CodeShapeGuardrailTest`: `DEAD_PRIVATE_METHODS = 0` still holds. The three deleted Lowerer privates are gone, not dead, and every new private is called; `isAStoreRead` is reached via `::`, which the scan counts. No method passes 250 lines (`Scalars.lower` ends at 2862, `inlineBody` is about 40 lines).
  - `IdentityGuardrailTest`, `ObservabilityGuardrailTest` (getenv only), `CompatSpellingGuardTest` (product sources; the new literal is in the edge), `UpstreamBranchesTest.SITES` (empty, no `unionAlias` literal), `ParkedWorkLedgerTest` (no anchor in a touched file) and `TestLaneOrderGuardrailTest` (its `SWITCH_READ` matches `engineScanOrder` only): all fine.
  - `GuardNamesExistTest`: no string literal names a deleted member.
- **Generators and goldens**:
  - `//core:update_generated`'s generators read `meta::` tokens and the registries, and the draft adds none.
  - The edge golden nets (`edge_pure_v1`, `edge_cases`), the lean-SQL ladder and the capability matrix should not move. The six deleted forms answered identically in both implementations, and the product body is what was inlined. `computedPruning` was `SubselectPrune.prune(q, true)` on both sides. `STAMP_PROBE` is off by default, and every door's `EditionTable` is the product form while it is off.
- **NullAway** (production libraries): `lowersAs` is `@Nullable` on both implementations, `Scalars` null-checks the result, and `Subselect.frameName` and `Fold.tryProjectAll`'s frame parameter are `@Nullable`. Test libraries run with `nullaway = False`.

## Not verified

- I did not compile or run anything. B1-B9 are read from the guard code, not from a failing run. The exact strings a test prints, such as the `breaksOfA` key or the `CLASS_HOOKS` key `EditionTable$1`, assume javac's usual naming.
- What the draft does at run time, which no guard checks:
  - The stamp's placement.
  - `Scalars.lower` passing the original `call` (callee `substring`) to `substringAsGiven`'s rule.
  - Equality now including `outsideStore`. HIR caches keyed by equality rather than identity could split.
  - Whether `EngineTableShadowTest`'s nine queries actually agree.
- Whether `//tools/deps:edge_separation_closure_test` or the hosts' jar lists change. No BUILD target was added, so they shouldn't.
- Windows, pct, spec and wasm lanes beyond the source search for affected APIs.
