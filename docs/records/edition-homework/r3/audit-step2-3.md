# Independent review: edition steps 2-3 (c06ebbab..29f58b21), core/spec/tools

Read-only. Nothing was built or run. Every finding below comes from reading the code. PLAUSIBLE marks a finding that
needs a run (or legend-engine's runner) to confirm.

## Findings, most severe first

### F1. HIGH: a let whose value turns into a lambda only after reduction is stamped, and its body carries the stamp into the store query
`compiler/spec/UserCallInliner.java:142-147` (`stampedIfStoreFree`) skips stamping only when the WRITTEN value
`let.value()` is a `TypedLambda`. The reduced value is the thing that gets stamped, and it can be a lambda when the
written value is not one:
- `LiteralUnroll.fold` turns `[{..}]->toOne()` and `[{..}]->at(0)` into the lambda (LiteralUnroll.java:606-641).
- A user function that returns a lambda is inlined to the lambda.
- A `pair({..}, x)` read back through `.first` comes out as the lambda (UserCallInliner rewriteSwitch, the PAIR arm).

`stampOutside` then walks into that lambda's body and stamps every call in it. `Scope.put` registers the stamped
lambda. When the store query applies it, the `TypedEval` arm (UserCallInliner ~1442) gets the term back unchanged,
because it is in `reducedTerms`. It is a `TypedLambda`, so `reduceEval` β-reduces it, and the stamped calls end up
inside the extend's SQL.

Concrete query (EngineTablePlacementTest.MODEL, H2):
`{|let f = [{s: String[1]|$s->indexOf('x')}]->toOne(); #>{t::DB.PERSON}#->extend(~i: r|$f->eval($r.NAME->toOne()))->from(t::RT);}`
- Expected now: `indexOf` keeps the stamp, gets no row, and is lowered with Pure's 0-based, -1-on-a-miss meaning
  inside the SQL. legend-engine sends it to the database (1-based).
- Before: the deleted detector refused exactly this query as `unverified`
  (`PureMeaningCheckTest.anEvaluatedLetWhoseValueIsNotALiteralLambdaIsUnverified`).
- It is not among the three "newly answered" kinds the progress log and the test javadoc list. No remaining test
  pins it.

So a refusal has become a silent answer that differs from legend-engine's.

Fix direction: test the reduced term, not the written one. Also do not stamp, or refuse, wherever the stamped value
lets a lambda escape as a value: the term is a lambda, a collection of lambdas, or a structure holding one.

### F2. HIGH/MEDIUM: a model-data runtime counts as a checked database, so query-level calls get DuckDB's meaning where legend-engine runs Java
`planner/PlanEnv.java:1534-1538` builds the table for `Compiler.executesOn(ctx, runtime).type()`. For a runtime that
binds only model data (a JsonModelConnection), that is `Target.Platform`, whose `type()` is `Databases.PLATFORM =
DuckDB`. `PureText.engineAnswers(DuckDB)` is true, so every row applies to the query's own calls, which carry no stamp.

legend-engine runs a model-to-model query over a model connection in its own Java, with Pure's meaning, and
`EngineTable`'s own javadoc says so: "over a model connection ... legend-engine's own Java computes them". Only the
transforms are stamped (`ClassSources` with `transformsOutsideStore`). The query's own calls are not.

Example: `|t::Out.all()->project(~[s: o|$o.short->substring(0, 1)])->from(t::M2, t::JsonRT)`
- The outer `substring(0, 1)` is planned as DuckDB's `SUBSTRING(..., 0, 1)` as given. Pure's answer is the first
  character.
- Before: the old `PureMeaningCheck.target()` found no `ConnectionDefinition` on JsonRT, returned null, and the query
  was refused by name.
- Now it is a silent different answer, with no test and no register row.

`anUnchainedModelToModelTransformKeepsLegendPuresMeaning` covers only `$o.short`.

### F3. MEDIUM (inherited from the deleted detector, not a regression; but the stamp's javadoc claims more than the rule does): "reads no store" does not follow a variable to a let that reads a store
`StatementEffects.reaches` follows user calls but not `TypedVariable`s. A let or last statement that builds on an
earlier let reading a store, but not yet executed, is stamped as store-free. The reducedTerms guard then keeps only the
earlier term unstamped.

Example:
`{|let t = #>{t::DB.PERSON}#; let u = $t->extend(~a: r|$r.NAME->toOne()->substring(1, 3)); $u->from(t::RT);}`
- `u` is stamped, so its `substring` gets Pure's meaning inside the one SQL statement.
- legend-engine cannot compute `u` in Java. It sends it to the database or refuses (the old test's note says a
  relation let read elsewhere gets "No SQL translation exists for ... letFunction").
- The same applies to `{|let t = TABLE->from(t::RT); $t->extend(~a: ...substring...);}`: the last statement holds no
  store-read node, so it is stamped.

A rule that matches the cases the tests pin: a variable bound to a let that reads a store and has no `from()`/execution
makes its reader store-bound. A let that was executed (holds a `TypedFrom`, as in
`aCallOverAStoreReadCollectionLetIsHostSide`) stays host-side. Otherwise refuse, or add a register row.

### F4. MEDIUM (PLAUSIBLE): a graph fetch over a model-to-model mapping gets the database's meaning for code legend-engine runs in Java
In a chained graph fetch (`transformsOutsideStore(true, true)` is true) only `ClassSources.composeModelToModel`'s
pipeline and ctor are stamped. Code that enters the resolved tree some other way carries no stamp:
- the target class's qualified properties in the graph tree (`ClassSources.compileSynthFn` / GraphEmission's
  graph-leaf inlining);
- the query's own filter before `graphFetch`.

legend-engine runs an M2M graph fetch in Java (`storeContract.pure:89-91`, which the edition itself cites).

Old vs new:
- Before: the qualified-property case was refused (a site in a followed body could not be reached).
- Now: it is answered with the database's meaning. The "graph fetch's qualified property" kind was justified by E-M1,
  a relational probe.
- The query's-own-filter case was already the database's meaning before.

Also unchecked: user calls inside M2M transforms. `allStampedOutsideStore` stamps only the native calls present at
composition. If a transform's user call is β-reduced later, its body carries no stamp. See also the XStore/ModelJoin
association predicates (`AssociationJoins` ~1223, ~1854), which are compiled outside `composeModelToModel`.

### F5. MEDIUM (design/tenet): door-dependent branches are spread through the core
AGENTS.md tenet 6's edition exception says: "Only the typer reads it ... nothing after the typer can see it". This
change adds door-dependent decisions after the typer in four places:
- `ProductStatements.prepare`: `readsWrittenFlags` changes which feature flags a statement runs under. It also decides
  whether to stamp and how to stamp M2M.
- `UserCallInliner.inlineBody(..., stampStoreFree)`.
- `StoreResolver`, `ClassSources` and `SubQueryLift`: a boolean threaded through about 10 signatures.
- `Scalars.lower`: the table lookup.

It follows the existing two-form-kind pattern (`LoweringForms`, which the plan deletes in step 10) rather than
smuggling in something new. The lowering stays database-blind and SQL-free (invariants 2 and 3 hold).

Still, it is honest to say this widens the door-switch surface, and that the same query text's `withFeatureFlags`
is honoured at one door and ignored at another.

`EdgeOnlyMembersTest` unpins `PlanEnv#withFeatures` because "the product uses it again". It does, but only behind a
branch that only legend-engine's edition takes. The guard was relaxed to let edge-only behaviour run inside the
product's statement road.

The design record §2.3 says "Mapping code arrives later and is never stamped". The implementation stamps M2M
transforms. The progress log explains this, but §2.3 was not amended.

### F6. LOW/MEDIUM: legend-engine's meaning on the host plan halves now depends on the style the caller hands in
`edge/HostAnswers.java:423`: `hosted()` used to apply the rewrite whatever the style. Now `asLegendEngine` only does the
comparison rewrite, and the meaning comes from the `style` argument of `arrowPlan` and `jsonPlan`.
- Production (wasm `EdgeBoundary`) passes `PURE_V1`, so it is fine today.
- The javadoc (HostAnswers ~208) still says pure/v1's style is `PureText.UPSTREAM`.
- `PureV1ApiTest` and `UpstreamProtocolFieldsTest` call these methods with UPSTREAM, which now plans lite's meaning
  silently.

### F7. LOW: roads that plan at PURE_V1 get the rows but no stamp
`BodyPlanner.resolver(..., false)` and their own inliners without the stamp appear at:
- FramePlanner:262
- PlanAllocations:201-208
- BodyPlanner.resolvedToExecute
- EnginePlans:402, 412

At pure/v1 these plan in `PURE_V1` (`PlanEnv.styled` makes `EngineTable` rows), but never stamp. A host-side let there
gets the database's meaning. These are reachable only through plan-text natives (`executionPlan`/`planToString`) in a
pure/v1 request.

Related: `PlanAllocations:119` catches `NotImplementedException` and swallows it when `noLineageRows(e)` holds, which
is true for any non-raw-SQL refusal. So an unchecked-database refusal raised inside `upstream.planRows` becomes "no
rows" (PLAUSIBLE).

### F8. LOW: the seed is now lowered wholly in `PURE_V1` style, not only with the table
`ProductStatements.lowerTestDataSeed(query, runtime, PURE_V1)` also changes the seed's other forms:
- plain raises for `ENGINE_UNCHECKED`;
- legend-pure's texts (`PureOnlyTexts`, DateTime and list texts);
- `EdgeLoweringForms.parseDecimal`'s H2 `DECIMAL(5,2)`.

The commit names "the seed planned in the door's style", but none of these other effects is listed as a moved answer.
They are probably in legend-engine's direction.

### F9. LOW: behaviour moves that are not listed
- **toOne of empty is now judged after inlining.** `function f(x: Integer[*]): Integer[1] { $x->toOne() }` called
  with `[]` now becomes trustOne (null). The old detector saw `$x` as `[*]` in the function's body and left
  legend-pure's raise. This is arguably closer to legend-engine (it inlines too), but it is not recorded.
- **Written flags apply to the whole statement, host-side lets included.** `LEGACY_SQL_NULL_UNSAFE_EQUALS` now
  changes a stamped host-side let's `==` (the Lowerer's `legacyNullUnsafeEquals` is statement-wide). legend-engine
  computes that let in Java.
- **A refusal is no longer whole-query and static.** It names the first call lowered. A call in a column the pruning
  later drops is still refused (as before).
- **`executesOn` is now called inside `PlanEnv.styled()` at the door.** Its refusals (mixed databases, a connection's
  row mappers, an undefined runtime) can surface wherever a Lowerer or Styled is first built. That is before the
  resolver if `PlanTextRoutines` or the frames' recorder lower anything (PLAUSIBLE). "Every refusal keeps its order"
  is no longer guaranteed at this door.
- **The type check on substitutes is gone.** `EngineSubstitutes.checked` asserted that a substitute's callee and type
  were the typer's. Nothing now checks that a row's target native has the source's parameter and return types. The
  four rows are consistent today (String/String, Integer/Integer, T/T). A static check in `EngineTable`'s initializer
  would hold future rows to it.

## The items you asked about

1. **The stamp:** see F1 and F3.
   - Rebuild sites: I scripted a scan of every `new TypedNativeCall(` in core/src/main. The only constructions from a
     source call's `.callee()` that do not copy `outsideStore` are `UserCallInliner.java:566`, which builds from a
     `TypedUserCall` and is stamped afterwards anyway, and none on the four natives.
   - `TemporalFrame:859` (adjust) and `LineageTreeLines:110` rebuild other natives.
   - No guard was added for future rebuild sites, though design §2.3 promised one. `TypedNativeCall.withArgs`, which
     the javadoc says passes use, has no caller.
   - The `reducedTerms` handling in `stampOutside` does what its comment says (identity, the scope still open). It
     cannot help F3, because the variable's term is spliced and kept as it was bound.
   - `if` with a store-reading branch: the whole let goes unstamped (as in the old detector).
   - Graph-tree qualifier args read lets through `queryLets` (stamped terms): correct.
2. **Model-to-model:**
   - The statement-wide rule matches the old `mappingReach` predicate (`graphFetch || !chained`). Both are read
     statement-wide; the new one reads the inlined body, which also sees fetches inside inlined functions.
   - The memo is sound: `ClassSources` and `StoreResolver` are per-statement instances, nested resolvers inherit the
     flag, and `specs.compile` results are never mutated (`allStampedOutsideStore` copies).
   - Gaps: F2, F4, and the association predicates.
3. **The table:**
   - `toOne`: only `TO_ONE__T_MANY` (1-arg).
   - Overload pairing: the declaration order in `Pure.java:2704-2705` and `3092-3093` is 2-arg then 3-arg for all
     four lists, and `byPosition` checks the size.
   - The rerouted rules (substringAsGiven, substrAsGiven, STRPOS, trustOne in Coercions) ignore `n` and read only
     their arguments. `DecisionProbe` records the original callee (cosmetic).
   - Passes before `Scalars.lower`:
     - `StaticFold` runs at typing, before both designs, so it is the same.
     - `LiteralUnroll`'s toOne folds never fold an empty operand.
     - `ChainNormalizer.unwrapSinglePick`, `ChainDispatch`, `AggregationAwareRouting`, `Anchors` and `InnerDemand`
       go through `Pure.isToOneCall`, which treats toOne and trustOne alike.
     - The two Lowerer sites that exclude trustOne (`checkedToOne`, `requiredOneEgress`) need a relation or a MANY
       operand, which an always-empty operand never is.
     - `FeatureRules`' corrected substring is a no-op (PURE_SUBSTRING either way), and with the flag the row wins
       first.
     - `SubstringClamp` matches SUBSTRING, not SUBSTRING_AS_GIVEN.
   - I found no shape where the table is bypassed for a call the old rewrite covered.
4. **Refusals:** the Lowerer's catches (UnfoldableRef in `foldOrIsolate` and `Resolution.attempt`) do not catch
   `NotImplementedException`. `SeedableLets` lowers in `Styled.PRODUCT`, so it never meets the table. The resolver's
   broad catches run before lowering. `PlanAllocations` is the one swallow (F7).
5. **Feature flags:** F9. Also, unconsumed flags (`VARIANT_TYPE_AS_INPUT`, `USE_DB_NATIVE_IMPLICIT_NULL_ORDERING`)
   are now refused at pure/v1. The log says so.
6. **Roads:**
   - generatePlan, execute and executeArrow (`EdgeServer.runner`: PURE_V1), the seed (`loweredSeed`: PURE_V1) and
     wasm all plan in PURE_V1.
   - `HostAnswers` depends on its caller (F6). The plan-text roads stamp nothing (F7).
   - `withSeed`'s other queries plan no seed (unchanged).
7. **Tests:**
   - They would fail if every call were stamped (all of STORE_BOUND), if nothing were stamped (the AS_PURE
     host-side lets), if the M2M rule were inverted (both M2M cases), or if the refusal were removed.
   - `assertNotEquals` accepts any difference, so a row applied to one of two calls passes. "a user function the
     query calls" holds two calls and is only checked that way.
   - Pinned before, unpinned now:
     - the `[λ]->toOne()` let (F1);
     - a model-connection query's own calls (F2);
     - `aMappingTheModelDoesNotDeclareIsUnverified`;
     - the door's 500 on a `validate(...)` request (`aConstraintValidateTurnsIntoAFilterIsFlagged_andTheDoorRefusesAValidateRequest`):
       I could not tell what raised it.
8. **Guards:** every moved pin's dated comment is true against the code.
   - ArchitectureTest `lowerTestDataSeed`: one entry, `PureV1Routes`. The 2-arg overload is reached only from
     `testplan.GeneratedTestData`.
   - `BodyPlanner#resolver`: re-keyed, same caller.
   - `SubselectPrune#prune`: a shrink.
   - HookLambdasTest and HookRulingTest: one kind replaces one.
   - JavaEvalLedgerTest: `PLANNER_CLASSES` is an exact census, not shrink-only, and the edge list loses two files and
     gains one.
   - ClaimRegistryTest and native-claims.tsv agree with `EngineTable`'s references.
   - `EdgeOnlyMembersTest` is true but see F5: it is the one move that relaxes rather than records.

## Dead code, stale comments, workarounds
- **Dead code:**
  - `TypedNativeCall.withArgs`: no caller.
  - `Fold.tryProjectAll`'s frame parameter: always null.
  - `SubselectPrune.prune(q, computed, keptUnionFrame)` and union-frame handling, which E0c leaves without a writer.
    `prune(q, boolean)` is still public with no edge caller.
  - The unused `PlanStyle style` parameters left on `PlanMaker.staging`, `countOf` and others after `sql()` lost it.
  - Blank-line debris in `EdgeLoweringForms` and `ProductLoweringForms`.
  - The 2-arg `StoreResolver` constructor that defaults to false.
- **Stale comments:**
  - `UserCallInliner.java:254` cites `lowering.EditionTable.stampsOutsideStore`, which does not exist; it is
    `planner.Edition`.
  - `TypedNativeCall`'s javadoc says passes rebuild through `withArgs`.
  - `Edition.table`'s javadoc reads as if the method returns null.
  - `PureV1Routes` (~34) and `EdgeServer` (~16) headers say the routes plan in `PureText.UPSTREAM`, and so does
    `HostAnswers.arrowPlan`'s javadoc. That was harmless before; now the meaning depends on it.
  - Design record §2.3 says mapping code is never stamped.
- **Possible workaround:** `EngineTable` is both the door's `Edition` and the lowering's `EditionTable`, told apart by
  a boolean. The DOOR instance answers `lowersAs` by throwing. Two classes would let the type system rule out the
  mix-up.

## Not checked
- I ran nothing: no build, no tests, no legend-engine runner.
- Whether the regenerated native-claims.tsv matches the generator.
- The plan-handle id golden at pure/v1 with stamped trees.
- When user calls inside M2M transforms are inlined (F4), and the XStore/ModelJoin association predicates.
- `ExecuteChainAssembly`'s inlines.
- Frames or Result envelopes at pure/v1 (whether the recorder's identity checks, such as `ProgramFrames.pastes`
  `t == chain`, survive stamping's rebuilds).
- datacube and python hosts.
- Windows and other operating systems.
