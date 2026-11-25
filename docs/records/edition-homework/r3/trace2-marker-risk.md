# Keeping the cross-type marker until lowering: what breaks

L = core/src/main/java/com/legend. This was a read-only trace: nothing was built or run.

## 0. Facts the rest of the report depends on

- **The marker is transparent to generic walks.** `TypedCrossTypeComparison.children()` returns `[comparison]`, `withChildren` rebuilds the marker, and `info()`/`withInfo` delegate to the call (`L/compiler/spec/typed/TypedCrossTypeComparison.java:51-68`). So any walk built on `mapChildren` or `children()` sees through it.
- **Only two exhaustive switches have an arm for it:**
  - `Lowerer.scalar` at `L/lowering/Lowerer.java:3667` throws. Every scalar path ends up here: `windowScalar`'s default at 3555 calls `scalar`.
  - `CollectionLanes.java:137` is the other.
- **Every other hand-written switch over `TypedSpec` has a `default`.** javac therefore flags none of them. Section 3 lists them.
- **Today no marker reaches G½ or the resolver.** It is refused, or removed by a door rewrite, before planning:
  - refused at `PlanMaker.java:96`, `ProductStatements.java:430` and `TestPlanner.java:251`;
  - rewritten by `EngineComparisons.asTheDatabases`, called from `PureV1Routes.java:333` and `UpstreamTestBodies.java:54`, or by `PureComparisons.asPures`, called from `UpstreamTestBodies.java:48`.
- **Deleting the door rewrites widens the scope.** Every crossing in a *query or test body* would then also travel through G½ and the resolver, not only crossings in function bodies. Every risk below applies to query bodies too.
- **All mapping code is typed at one unmarked site.** `ClassSources.compileSynthFn` (`L/resolver/ClassSources.java:193-200`) calls `SpecCompiler.check` (`SpecCompiler.java:199`), which calls `Typer.inFunctionScope` (`Typer.java:147-154`). That covers `$class$` for relational and M2M bindings alike, plus `$assoc$`, `$prop$`, `$constraint$` and the rest.

## 1. Code that matches a comparison call structurally

Key to the "If wrapped" column:
- **REFUSE**: a NotImplemented or IllegalState exception.
- **WRONG?**: silently different output.
- **OPT**: only an optimisation is lost.
- **OK**: unaffected.

Reachability:
- **P**: reachable from Pure-language function bodies or mapping code (M2M, derived and qualified properties, Pure or XStore association conditions, constraints).
- **R**: reachable only from `###Relational` code, which stays unmarked.

### 1a. Matchers that sit one level above the comparison

These match the call one level down, through a parent such as `not(..)` or a lambda body. The marker hides the call from them.

| Site | What it matches, and why | If wrapped | Reach | Smallest fix |
|---|---|---|---|---|
| `resolver/Substitution.java:1176-1201` | `not(equal\|in\|contains(<to-many crossing read>))`. It reproduces the engine's null compensation (`NOT x OR read IS NULL`). Ordering operators pass through. | **REFUSE**: no `TypedNativeCall`, so it throws "negated 'TypedCrossTypeComparison' over a to-many crossing" (1196-1201). | P, and query bodies | Peel the marker in the `innerCmp`/`oc2` test, then re-wrap. |
| `resolver/ClassWrites.java:125-131` `equated` | An association condition's body is a single `==` of two columns. Used for writes. | **REFUSE**: returns null, then "writing it is not supported yet" (118-120). | P (Pure association) | Peel the marker. |
| `resolver/ForeignKeyIdentity.java:62-71` `sourceKeyColumn` | The condition's last statement is `==`. Used to detect a foreign-key identity. | **OPT** or a plan-shape change: returns null. | P and R | Peel the marker. |
| `resolver/CorrelatedSubselects.java:938-970` and `974-1000` (`parentKeysLenient`, `collectEquiKeys`) | Equi-join keys of an association condition, through `and`. | **REFUSE** at 471 ("requires a conjunctive equi-join"). Otherwise null, or another emission at 579. | Mostly R. P for Pure or XStore associations. | Peel the marker, in both functions. |
| `resolver/LiteralFolds.java:34-60` `staticBool`, used at `StoreResolver.java:335` and `:351` | Picks an `if` branch at planning time when the condition is `==`/`eq` over two literals. `literalEquals` uses SQL's rule: `1 == 1.0` is **true** (79-85). | **REFUSE** for a class-typed `if`: "class query under if() with a runtime condition" (`StoreResolver.java:352-354`). A value-typed `if` becomes a CASE, which is OK. | P, and query bodies | The answer depends on the audience, and it is needed *before* lowering. |
| `resolver/ChainDispatch.java:~277` `runtimeIfsAsUnions` | Same `staticBool == null` test. | **OPT**: a static branch becomes a union of both branches. The untaken branch now resolves too, and may hit its own refusals. | P | As for `staticBool`. |
| `compiler/spec/VerdictQueries.java:193-206` `rowLocal` | An assert predicate's shape, for the vector plan. | **OPT** or **REFUSE**: default false, so a different assert plan, or "the program road does not plan this body". | Test bodies | Add an arm that recurses. |
| `edge/AggAwareActivities.java:242-249` | Prints `==` in edge text. | **OPT**: returns false. | Edge only | Add an arm. |

### 1b. Matchers on the comparison node itself

These are reached by recursing *into* the marker's child, so they still fire, but only if the enclosing walk descends (section 3).

| Site | What it matches | If wrapped | Reach |
|---|---|---|---|
| `resolver/ChainNormalizer.java:61-75` | `equal(class row read, element ref)` becomes key equality. | **OK**: the walk uses `mapChildren` (54). | Metamodel queries |
| `resolver/Substitution.java:1085-1158` | `in`/`contains` over a bare to-many read becomes EXISTS. | **OK if** `Substitution.rewrite` gets a marker arm. Today it **REFUSES** (section 3). | P, and query bodies |
| `resolver/InnerDemand.java:779-800`, `856-880` | `in`/`contains` demand scans. | **OK**: they walk `children()` (82, 805, …). | P |
| `lowering/Lowerer.java:4148` `InstanceEquality.staticallyDisjoint` (`InstanceEquality.java:413-442`) | Answers FALSE statically for an enum against another enum or a non-String, a class instance against a primitive, and Variants. | **OK**: `scalar(inner)` reaches it. Under the legend-engine audience this gives Pure's FALSE, not the database's answer, exactly as today. | all |
| `lowering/Scalars.java:115, 200-203, 878, 1535, 2236, 2865-2868, 3476`; `InstanceEquality.java:62-127`; `NullSemantics.java:37, 108, 158`; `LambdaBinding.java:64-69`; `Comparators` | Rule tables and lowering keyed on the call itself. | **OK**, as long as the marker arm lowers `x.comparison()` through `scalar`, so the context flags (`verbatimEquality`) are kept. | all |
| `lowering/EnumSourceValues.java`, `KeyedCounts.java`, `Dedup.java` | Work on SqlExpr only. | **OK** | — |
| `planner/*`, `testplan/*` | No structural match on comparison ids. `HostJudge`/`AssertVerdicts` key on assert calls, and `VerdictQueries.java:481-482` *mints* an unmarked `equal` for `assertEquals`. | **OK**, apart from `rowLocal` above. | — |
| `TemporalFrame.java:870-1980`, `AssociationJoins.java:517, 2096`, `SyntheticHeads.java:177`, `Callees.java:72-82`, `ClassSources.java:1057`, `TemporalFrame.java:1535` | These *mint* equality and ordering calls, unmarked. | **OK** | — |
| `StoreResolver.java:1532-1533` | Unused constants. | — | — |

## 2. Folds that can answer a comparison between two different known types

The inliner's `rewrite` (`UserCallInliner.java:1347-1361`) calls `LiteralUnroll.fold` on every rewritten node. A marker reaches the switch's `default -> n.mapChildren(this::rewrite)` (1575), so **its child call is folded**. The result is `marker(false)`. This is verified.

| Fold | Cross-type behaviour | Runs on a marked node's child? |
|---|---|---|
| `LiteralUnroll.equalityFold` (`LiteralUnroll.java:385-420`), entered at 575 | Two element references compare by path. A reference against a scalar literal gives **false** (406-412). A TDS-null carrier against a scalar gives false (397-401); TDSNull is never marked (`EqualityClasses.java:115`). `neverEqual`, an enum against a spelled scalar of another kind, gives **false** (413-415, 427-431). A plain scalar fold needs the same class (416-419), so `1 == 1.0` is not folded. | Yes, giving `marker(false)` |
| `LiteralUnroll` `in` (596-600) | Compares values with Java `equals` (`scalar()` at 289-291 drops the type), so `1->in([1.0])` gives **false** because Long ≠ Double. It also applies `neverEqual`. | Yes |
| `LiteralUnroll` `contains` (472-485) | `neverEqual` elements count as unequal and the call folds to a boolean. | Yes |
| `LiteralUnroll` orderings (452-460) and `NormalizeFolds.foldInlined` `==` (`NormalizeFolds.java:95-104`, via `deepFoldInlined` at `UserCallInliner.java:1330-1344`) | Both sides must be `TypedCInteger`, so the two types are the same: no crossing. | n/a |
| `LiteralFolds.staticBool` / `literalEquals` (`LiteralFolds.java:52-58, 79-85`) | `1 == 1.0` gives **true** (SQL's rule); `'a' == 1` gives false. | **No.** It is called on the `if` condition itself, the marker gives null (59), and it never looks inside. |
| `StaticFold` (`StaticFold.java:608-611, 641-673`, `staticEquals` 877-890) | Answers EQUAL, IN, CONTAINS and INDEX_OF by Java equality, on *untyped* ValueSpecification inside `NormalizeRequiredFunction` bodies, before typing. | No marker can exist there yet. These crossings are answered before any marker could be minted, whatever the audience. |
| `InstanceEquality.disjointAnswer` (`Lowerer.java:4148`) | Static FALSE at lowering (section 1b). | Yes, through `scalar(inner)` |

**The real fold problem is the parent, not the child.** Several folds only fire when a child is a `TypedCBoolean`, and `marker(false)` is not one:
- the lazy `if` (`UserCallInliner.java:1114-1121`);
- `not`, `and` and `or` short-circuits (`LiteralUnroll.java:577-591`);
- the filter unroll over a spelled list (`UserCallInliner.java:1179`), which becomes `if(marker, e, [])`;
- `staticBool` and `runtimeIfsAsUnions`.

The consequence: **untaken branches are now rewritten** ("its walls are not ours", 1104-1107), and a class-typed `if` is refused.

Measured against today's behaviour, the child fold changes no answer:
- the product still refuses, because the marker survives;
- legend-pure's answer is false either way;
- legend-engine's answer was already this fold's, because its door unwraps the marker before G½.

It does mean lite already gives legend-engine's audience Pure's answer for `1->in([1.0])` and for spelled enum crossings. It also means the per-audience answer is not in one place today: `StaticFold`, `LiteralUnroll`, `LiteralFolds` and `InstanceEquality` each decide part of it.

## 3. Generic walks, and switches that do not know the marker

**Transparent (they use `mapChildren` or `children()`):**
- the inliner's default (1575) and `deepFoldInlined`;
- `ChainNormalizer.normalize` (54);
- `InnerDemand`'s scans;
- `SubQueryLift` (139-165);
- `ChainDispatch` (139, 275);
- `BodyClass.framesAsTheirQueries`;
- `EngineComparisons.rewrite`;
- `CrossTypeComparisons`.

**Hand-written walkers with no marker arm:**

| Site | Walker | Default behaviour | Who meets it |
|---|---|---|---|
| `resolver/Substitution.java:2121` | `rewrite`, object-space substitution | **REFUSE**: "object-space expression node … not substitutable" | Query filters and projections over mapped classes; inlined derived properties |
| `resolver/Substitution.java:2754` | `inlineParam`, the auto-map mapper | **REFUSE** | `->map(x \| $x.p == …)` |
| `resolver/ClassSources.java:1248` | `substituteSourceReads`, the **M2M binding** | **REFUSE**: "model-to-model binding node …" | M2M property expressions |
| `resolver/Pipelines.java:1266` | `rewriteRowReads` | **REFUSE**: "resolver bug: row-read rewrite hit …" | Association and join conditions, sub-rows (37 call sites across 16 files) |
| `resolver/Pipelines.java:1327` | `prefixColumns` | **REFUSE** | Association-leaf bindings |
| `resolver/AggregationAwareRouting.java:228, 312` | `path`, `canRewrite` | **REFUSE** | Aggregation-aware mappings |
| `resolver/StoreResolver.java:697` | `anchoredNode` | **REFUSE**: "class query under …". This happens when the marker wraps a class query, e.g. `Person.all()->size() == 1.0`. | Query bodies |
| `resolver/GraphEmission.java:3327` | `substVars` (used at 1509, 1600, 1661, 1670, 1673: qualifier and association conditions in graph fetch) | **SILENT**: returns `n`, so variables under the marker are **not substituted** | Graph fetch over qualified properties and XStore associations |
| `resolver/SyntheticHeads.java:902` | `descend` / `liftFilteredHeads` | **SILENT**: does not descend, so heads are not lifted | Filters through to-many navigations |
| `resolver/SyntheticHeads.java:1859` | `rebuildChildren` | **SILENT** | Same |
| `resolver/StackBuilder.java:740` | `projectable` | **OPT**: false | Projections |
| `resolver/Anchors.java:173` | `navigatesSource` | **OPT** or a shape change: false | Anchoring |
| `lowering/Lowerer.java:3555` | `windowScalar` | **OK**: falls to `scalar` | — |

The smallest fix is the same one-line arm in each walker: `case TypedCrossTypeComparison x -> x.mapChildren(k -> <this walk>(k, …))`. That is about ten arms, plus the peels from section 1a. A guard is also needed, because javac will not find these walkers: each has a `default`.

## 4. The `markingCrossings(false, …)` sites

| Site | What it covers |
|---|---|
| `Typer.java:152` `inFunctionScope` | **Every function body**: user functions, platform functions (legend-engine's library), and every synthesized body (`$class$` relational and M2M, `$assoc$`, `$prop$`, `$constraint$`, `$view$`, `$default$`, `$pre$`/`$post$`, `$query`; `SynthHat.java:21-44`). |
| `Overloads.java:131` | `NormalizeRequiredFunction` bodies typed into the call. `StaticFold` has already folded part of them, untyped. |
| `Typer.java:220` | Island output lifted ahead of its statement (`islandOutput`, 156-171). |
| `Typer.java:256` | `#SQL{}#` and other expression islands. |
| `SpecCompiler.java:390` | Island statement programs (Cypher updates, multi-statement SQL). |
| `SpecCompiler.java:434` | `SqlIslands.statements` sources. |

Marking is on only in `typeQueryBodyMarked` (`SpecCompiler.java:325`). Islands are cleanly separated already: they are their own syntax node, plus an identity set.

**Mapping code is not separable at the typing site.** Pure-language mapping code and `###Relational` code both reach `Typer.java:152` as one `$class$` function. The `Synthesized` provenance record carries only `hat`, `owner` and `member` (`FunctionDefinition.java:157`).

Two places can separate them:

- **The binding's kind.** `MappingDefinition.ClassBinding` is `Relational | Pure | Operation` (`MappingDefinition.java:244-245`). The typer could look it up from the `$class$` owner. `AssociationBinding(associationFqn, predicateFunctionFqn)` has **no kind** (`MappingDefinition.java:565`), so relational `@Join` predicates and Pure or XStore predicates look alike.
- **The relational translation itself** (better). `RelOpTranslator.comparisonFn` (`L/normalizer/RelOpTranslator.java:694-707`) already sends `<`, `<=`, `>` and `>=` to Any-typed Lite shims (`Pure.Lite.*_ANY`). Those are not in the `EqualityClasses` table (`EqualityClasses.java:50-64`), so they are never marked. Only `EQ` and `NEQ` use plain `equal` (701-702).

**Smallest change for the relational side:** send EQ/NEQ to a Lite `equal` shim with the same rule as `equal` (`Scalars.java:115`). Relational code then stays unmarked by construction, at function granularity and below, and `Typer.java:152` can simply mark. Not verified: whether DynaFn pass-through can also emit `collection::in` or `contains`.

**Synthesized equalities.** `JoinChecker.java:268, 321` synthesizes `equal` for joins by column name. In a marked body, a key-type mismatch is marked with an `==` the user never wrote. This already happens in query bodies today.

## 5. The measured crossings outside the query

The table in `docs/records/edition-homework/probe-crossings-results.txt` lists 18 "mapping: class" sites:
- 14 are relational (`$class$`, all `==`);
- 4 are the M2M `model::SalaryBandMapping` `<`, Decimal against Integer, at lines 113 and 218.

The other site kinds are 2 derived properties, 1 association, 1 platform function, 3 islands and 2 unscoped. The non-relational ones:

| Site | Path it takes | Works? |
|---|---|---|
| M2M `SalaryBandMapping` (`M2MIntegrationTest.java:258-265`): `salaryBand: if($src.salary < 50000, …)` | Typed at `Typer.java:152`. The inliner does not fold it (both sides would need to be `TypedCInteger`). Then `ClassSources.substituteSourceReads`: the `TypedIf` arm recurses into the condition and the default throws (1248). | **No: refused** for every audience until the arm is added. With the arm, it lowers. The product would then refuse 4 integration bodies that pass today, which is a semantic decision to post. |
| Derived `Money$prop$usdRate` (`==`, DateTime against StrictDate; 8 bodies per corpus lane) | β-inlined by the inliner; then `SyntheticHeads.liftFilteredHeads` (silent at 902), then `Substitution.rewrite` (throws at 2121, also inside a filter lambda's body). In graph fetch, `GraphEmission.inlineDerivedCalls`/`substVars` is silent at 3327. | **No: refused** (or silently unsubstituted in graph fetch). |
| Derived `m2m2r::Entitlement$prop$prop3` (`==`, Decimal against Float; 4 lines) | The same passes. It is an M2M-over-relational chain, so `ClassSources.substituteSourceReads` (1248) is likely too. | **No: refused** |
| Association `XStore::inMemoryAndRelational::crossMapping2$assoc$` (`>`, DateTime against StrictDate) | The association condition goes through `Pipelines.rewriteRowReads` (throws at 1266) and `GraphEmission.substVars` (silent at 3327) on the graph-fetch path (`GraphEmission.java:1650-1673`). Possibly also `ForeignKeyIdentity`/`CorrelatedSubselects` key extraction (an ordering conjunct is not an equi-key anyway). | **No: refused**, or wrong variables. |
| Platform `meta::relational::functions::toPostgresModel::getDynaFunctionConverterMap` (`==`, Expression against Boolean; 44 bodies per lane, under legend-engine's door) | Inliner: `equalityFold` does not fold an instance against a Boolean. If the comparison sits under a spelled `fold` or `if` that needs a literal, the marker blocks the unroll (section 2), and the untaken code gets rewritten. | **Not verified.** The source is not in this checkout. This is the largest single exposure: 88 corpus bodies. |

## 6. Conclusions

### (a) The real risks, ranked

1. **Resolver walkers with no marker arm refuse outright.**
   - Where: `Substitution.java:2121` and `:2754`; `ClassSources.java:1248` (all M2M); `Pipelines.java:1266` and `:1327`; `AggregationAwareRouting.java:228` and `:312`; `StoreResolver.java:697`.
   - Effect: every non-relational site in section 5 is refused for *every* audience. Because the door rewrite is deleted, so is any query-body crossing over a mapped class.
2. **Silent walkers.**
   - Where: `GraphEmission.java:3327` `substVars`; `SyntheticHeads.java:902` and `:1859`.
   - Effect: the subtree under the marker is not rewritten. It will most likely fail later; a wrong binding has not been ruled out.
3. **Parent folds blocked by `marker(false)`.**
   - Where: the lazy `if`, `and`/`or`/`not`, the filter unroll, `staticBool`, `runtimeIfsAsUnions`.
   - Effect: untaken branches are now rewritten, and may hit their own refusals. A class-typed `if` is refused (`StoreResolver.java:352`).
   - Why it matters for the design: unblocking these needs the audience's answer **in G½ and the resolver**, not at lowering. That contradicts "answer it at lowering".
4. **Parent-pattern matchers.**
   - `Substitution.java:1184`: a refusal, and its null compensation is itself a per-audience meaning.
   - `ClassWrites.java:129`: a refused write.
   - `CorrelatedSubselects.java:471`: a refusal.
   - `ForeignKeyIdentity.java:69`, `VerdictQueries.java:204`: plan-shape changes.
5. **The product's compile error stops being deterministic.** A crossing in a function body would surface only if lowering reaches it. It would not surface in untaken or folded branches, or in unused functions, and `compileErrors`/`compileAllBodies` would not report it. The three refusals at the planner entries also have to become product-only.
6. **The answer is already split.** `StaticFold`, `LiteralUnroll`, `LiteralFolds` (SQL's rule: `1 == 1.0` is true) and `InstanceEquality` each answer some crossings before lowering, by different rules. "Answer at lowering" would not be the only answer.
7. **Tenet 6 and the switch rule.** A lowering that branches on the audience is a core read of a door switch ("nothing in the core reads a switch … the lowering", `compat-rules-of-record-2026-11-19.md:62-64`). That is acceptable only under the v4 "override sets as data" reading (`edition-v4-direction-2026-11-25.md:34`), which the user has to rule on.
8. **Telling relational from Pure mapping code** needs a provenance change, not a site flag. The cheapest is the Lite `equal` shim for relational EQ/NEQ (section 4).

### (b) Opinion: design 2, not "the marker survives for every audience"

For legend-engine's audience, the typer should simply not mark inside function bodies and mapping code. Only the product and legend-pure's audience should mark.

- **For legend-engine the marker carries no information.** Its answer is the wrapped call unchanged, which is exactly what unmarked typing produces today. Marking for it only exposes the corpus (88 + 16 + 2 bodies measured, plus every query-body crossing now past the door) to risks 1-4, for zero change in its answer.
- **The product does not need the marker to survive.** Its answer is a refusal. Refuse where it is minted (`Overloads.java:143-149`), in function bodies too. The error is then deterministic, reported by `compileAllBodies`, and no pass downstream ever sees a marker in the product.
- **That leaves only legend-pure's audience with surviving markers.** PCT has no mappings, so the walker arms in section 3 matter little there. Its answers are decided by the types alone: false, -1, or the database's ordering. So they can be applied where the marker is minted, or in the inliner's fold. There is no reason to carry them to lowering, and the parent folds then work.
- **The cost of design 2 is one audience read in the typer**, in place of reads in the lowering plus the inliner plus the resolver. Both designs need the user to rule on the switch rule; design 2 keeps the read in one place.
- **Even under design 2,** add the one-line `mapChildren` arm to each walker in section 3, and a guard test (for example ArchUnit or a reflective census) that every `switch` over `TypedSpec` in `resolver/` has a marker arm or a reason it is unreachable.

### (c) Not verified

- The text of the `toPostgresModel::getDynaFunctionConverterMap` comparison and its enclosing fold or `if`. Its source is not in this checkout.
- Which exact path the `crossMapping2$assoc$` in-memory/relational condition takes: `Pipelines.rewriteRowReads`, `GraphEmission.substVars`, or `AssociationJoins`.
- Whether the silent walkers (`GraphEmission.java:3327`, `SyntheticHeads.java:902` and `:1859`) end in a refusal or could produce a wrong answer.
- How many query-body crossings in the corpus and PCT are rewritten by the doors today. That is the size of the newly exposed query-body population.
- Whether a relational DynaFn pass-through can emit `collection::in` or `contains` (marked ids) besides EQ/NEQ.
- Whether `StoreResolver.java:2955`, `TemporalFrame.java:1858` and `FlattenOps.java:153` (switches over operation nodes) can ever receive a marker. I judged them unreachable, since they switch over pipeline operations.
