# 16 — Architecture and design review

**Scope:** 30 files / 11,194 lines under `core/src/main/java/com/legend/normalizer/`, plus
`model/MappingDefinition.java` (449), `model/NormalizedModel.java` (82),
`compiler/ModelBuilder.java` (1,086), `compiler/element/PureModelContext.java` (667), `AGENTS.md`,
both `package-info.java` files, `docs/CLEAN_SHEET_INVERSION.md`, and
`core/src/test/java/com/legend/ArchitectureTest.java` (~1,050, read completely).

---

## Headline

The **artifact** design is good and in several places genuinely excellent — the compiled
`MappingDefinition` is a well-shaped, sealed, totally-stamped binding table, and `ResolvedMapping` is a
textbook "one object answers all closure questions."

The **package** design is not: the god class was shaved to fit a line-count guardrail rather than cut
at joints, and the result is a 144-symbol flat namespace with bidirectional call traffic between "the
class" and each of its "extractions." The stated invariant *"one owner per decision"* is true for about
half the decisions enumerable here and false for the other half, **including the most-copied one**
(set-id spelling: ~14 independent copies, in a method whose javadoc says *"The one rule"*).

---

## Q1 — Is the package decomposition right?

**No. The seams were cut by file size, and the code says so.** Nine of the 30 classes document their
own origin as a size extraction:

```
AssociationSynthesis:66   "Split from MappingNormalizer (the Doors split)."
JoinChainEmission:65      "Split from MappingNormalizer (the Doors split)."
UnionSynthesis:70         "Split from MappingNormalizer (the Doors split)."
GroupBySynthesis          "Split from MappingNormalizer (file-size seam, audit-19 window)."
DeclaredCoercions:26      "Split from MappingNormalizer at the shape limit"
ImplicitInheritance:19    "relocated from MappingNormalizer at the file guardrail"
RelationReads:26          "relocated from MappingNormalizer at the file guardrail"
ModelJoinNesting:32       "from MappingNormalizer (guardrail seam)."
SetDispatch:21            "(split from MappingNormalizer)"
```

The guardrail is real and mechanical: `CodeShapeGuardrailTest` has `FILE_LIMIT = 3500` with one
allowlist entry — `"MappingNormalizer.java", 3510`. **The file is now 2,910. The pressure came off at
3,500 and the shrinking stopped**; `METHOD_LIMIT` is 250 and the six largest methods in the package are
211, 205, 200, 190, 185, 182. **The ceiling, not the concept, is setting the shape.**

Four pieces of objective evidence that these are cuts, not seams:

**(a) The extracted classes carry the parent's entire import block.**

| file | unused / total imports |
|---|---|
| `UnionSynthesis` | **34 / 64** |
| `AssociationSynthesis` | **29 / 60** |
| `JoinChainEmission` | **27 / 59** |
| `MappingNormalizer` | 11 / 68 (leftovers from what moved out) |
| every other file | 0–3 |

`UnionSynthesis` imports `ParsedModel`, `NormalizedModel`, `PackageableElement`, `Realization`,
`MappingInclude`, `SynthHat`, `CInteger`, `CString`, `NewInstanceCast`… and uses none of them.
**That import list is `MappingNormalizer`'s, copied.**

**(b) The "extractions" call back into the parent.**

| class | calls into `MappingNormalizer.` | called from `MappingNormalizer` |
|---|---|---|
| `JoinChainEmission` | 10 | 15 |
| `AssociationSynthesis` | 12 | 4 |
| `UnionSynthesis` | 8 | 3 |
| `XStorePureEnds` | 3 | 9 |
| `ViewRelation` | 6 | 8 |
| `AggregateViewLift` | 4 | 1 |

`AggregateViewLift` (68 lines) is the clearest case: every line calls
`MappingNormalizer.synthesizeClassMapping`, `.declaredPrimaryKeyColumns`, `.relationalSourceOf`,
`.propertyPinsOf`. **It is a paragraph of `normalizeMapping` moved to another file.**

**(c) The god class's table of contents still advertises sections that are now empty.**
`MappingNormalizer.java:2036–2046` has three consecutive banners — *Hop emission — Pass 1 (structural)
and Pass 2 (nested JoinNav)*, *JoinNavigation collection (Pass 2 hoisting source)*, *PropertyMapping →
constructor field* — of which the first two have no body. `:2350–2356` has two more, both empty.
**The content left; the outline stayed.**

**(d) Encapsulation went to zero.** The package has **144 package-private static methods** (vs 142
private), 35 of them exported by `MappingNormalizer` alone. **Any of the 30 classes can call any of the
144.** This is not a decomposition; it is one procedure in 30 files with a shared global namespace.

It also **silently defeated an existing guard**: `CodeShapeGuardrailTest.deadPrivateMethodsOnlyShrink`
only scans `private` declarations, so every method promoted from `private` to package-private during the
split **left the dead-code guard's scope**. Five of them are in fact dead today (Q8).

### Concepts that SHOULD be classes and are not

| missing concept | currently scattered as | why it is a real noun |
|---|---|---|
| **`MainTable`** | `resolvedMainTable`, `inferMainTable`, `inferMainTableQuiet`, `canonicalTable`, `collectMainTables`, `collectExprTables`, `hasMainTable`, `mainTableDefOf`, `mainTableOf` — 9 methods (+ `inferViewMainTable`, still pinned at 5 sites) | The most-cited decision in the translator audit (T1); has a loud/quiet pair, an inference rule, a canonicalization rule and a root-selection rule. **It is a thing.** |
| **`SetId` / `SetIdentity`** | `ResolvedMapping.idOf`, `SetKeyFacts.setKey`, `ClassSources.setIdOf`, and ~11 more inline copies | See Q3 row 4 — the most duplicated rule in the codebase |
| **`RowProjection` / `TargetConstructor`** | `translatePmToField`, `CtorField`, `materializeEmbedded`, `materializeOtherwiseEmbedded`, `materializeInlineEmbedded`, `buildNewInstance`, `buildNewInstanceToOne`, `nullOfDeclaredType`, `nullOfPhysicalKind` | The `^Target(...)` terminus the package-info describes as one of the two halves of a synth body. **It has no class.** |
| **`ClassBindingBuilder`** | the 3-arm `Relational \| Operation \| Pure` ternary written **twice verbatim** at `MappingNormalizer.java:322–340` and `:386–406` | A copy-pasted 19-line constructor selection **is** the definition of a missing factory |
| **`BuildMode` (strict vs module)** | the sentinel `wallSink == null`, threaded through `normalize`, `normalizeMapping`, `MappingPrePass.run`, `MappingValidation`, `ModelNormalizer` | A two-valued policy encoded as "is this map null" and re-interpreted at 6 sites |
| **`PureSpecBuilder`** | `buildNewInstance`, `simpleTypeName`, `booleanizeCaseLiterals`, `canonicalizeEqualOperands`, `rootedAt`, `nameRefOrNull`, `PRIMITIVE_TYPE_NAMES` | Low-level `ValueSpecification` construction sitting inside the phase orchestrator (Q7) |

---

## Q2 — The noun test

| class | verdict | reasoning |
|---|---|---|
| `ResolvedMapping` | **NOUN — the best thing in the package.** | "A mapping resolved for synthesis." Real state, real identity, every closure question routed through it. **This is what the rest should look like.** |
| `Pipeline` | **NOUN.** | The accumulating emission frame. Mutable by nature; correctly identified as "the frame object" in the guardrail allowlist. Eleven fields is one or two too many but the concept is right |
| `MappingLedger` | **NOUN, overloaded.** | "What one mapping's synthesis learned" is a genuine thing, and replacing five shared-index write channels with it was the right move. But it accumulates six unrelated things and carries a read-only `mappedInClosure` set that has nothing to do with a ledger. Also `facts(surface, md, model)` at `:86` takes three parameters and **uses one** |
| `MappingClosures` | **Half.** | The nested `Closure` record is a noun. The outer class is a memo/factory whose plural name hides that. Its javadoc honestly admits there is no single closure rule: *"the walkers differed from one another (visible sets: a LATER include overrides an earlier one; operation sets per class: the FIRST include found wins; roots: includes first, then the mapping's own), and those differences are the current semantics."* **Four orders under one name** |
| `RelationReads` | **Borderline.** | Nominally a noun, actually the verb "rewrite `$this.p` into a column read." Small and coherent; acceptable |
| `MappingFacts` | **NOT a concept.** | Two static computations over the legacy surface. "Facts" is the least informative noun available. Each is a property of a `ClassBinding` and belongs on the thing it describes. Also imports `ModelBuilder`, `AssociationMapping`, `AssociationPropertyMapping` and uses none |
| `SetKeyFacts` | **NOT a concept — and it duplicates one.** | Five statics that build a `MappingDefinition.ClassBinding.DeclaredKeys`. **The noun already exists, in `model/`, and this is its constructor.** Should be `DeclaredKeys.of(rcm)`. Worse, `setKey()` is a *second, different* spelling of the set-identity rule (`setId ?: className`, **no** `::`→`_`) |
| `MilestoningFacts` | **NOT a concept.** | 30 lines, one method — a predicate on the knowledge kernel that belongs there. The class exists because `ShadowWalkerCensusTest` retired `isTemporalClass` to 0 and something had to hold the replacement |
| `MissProbe` | **NOT a class — an audit annotation wearing a class.** | `miss()` returns `null`. `knownMiss(o)` is `o.orElse(null)`. **The *program* behind it is excellent** (a full census of 33+37 `orElse(null)` sites, 23+16 of which became loud). But the artifact makes every call site less readable in exchange for greppability — **and no test greps it.** There is no ratchet on `MissProbe` anywhere. The census is a comment |
| `DynaFnArms` | **NOT a concept, and it is dead.** | Its own javadoc: *"public so the registry's test can hold the declarations against the code."* **No such test exists** — the only repo reference outside its own file is a javadoc sentence in `builtin/DynaFn.java:31`. A `public` production class in the compiler that exists to serve a guard that does not exist |
| `AggregateViewLift` | **NOT a concept.** | A verb, 68 lines, every line delegating back to `MappingNormalizer` |
| `ImplicitInheritance`, `StoreSubstitutionRewrite`, `GroupBySynthesis`, `UnionSynthesis`, `AssociationSynthesis`, `JoinChainEmission`, `XStorePureEnds`, `ModelJoinNesting`, `DeclaredCoercions`, `SetDispatch` | **Verbs — defensible only if they were passes.** | Pass names are fine *for passes*. **These are not passes:** none has a uniform `X -> Y` signature, all are static-method bags, and half call back into the caller. If they were real passes with `apply(ResolvedMapping) -> ResolvedMapping` or `emit(Pipeline)` shapes, the naming would be right **and the coupling would be visible** |

---

## Q3 — One owner per decision

**OK** = exactly one site decides. **SPLIT** = more than one site, possibly consistently.
**DIVERGENT** = more than one site *with different rules*.

| # | decision | owning site | other sites | verdict |
|---|---|---|---|---|
| 1 | Which table is a set's main table | `resolvedMainTable:1426` → `inferMainTable:1473` | `inferMainTableQuiet:1522` (same rule, `catch → null`); `ViewRelation.inferViewMainTable` (5 sites, pinned OWED) | **OK-ish.** One rule, but exception-as-control-flow gives it two answers and callers pick |
| 2 | Which set anchors an association predicate | `mainTableDefOf:2388` | nobody | **OK.** But the rule ("root set, else first declared") is inlined rather than delegated to #3 |
| 3 | Which set is root / sole | `ResolvedMapping.roots()` + `MappingClosures.Closure.ownRoots` | `MappingClosures:326`; `UnionSynthesis:116` (closure); `:308`; `MappingNormalizer:293` (own mapping only); `M2mRouteGuards:98`; `ModelBuilder:485` | **DIVERGENT.** Six sites, **each counting over a different scope**. Precisely where a bug would live, and nothing compares them |
| 4 | How a set id is spelled | `ResolvedMapping.idOf:83` — javadoc: *"The one rule."* | `SetKeyFacts.setKey:25` (**different** — no `::`→`_`); `AssociationSynthesis:309`; `M2mRouteGuards:133`; `ModelBuilder:495`, `:524`; `ClassSources.setIdOf:514` **and** `:432` inline (same file, ignoring its own helper); `ObjectReferenceDecode:156,158`; `GraphEmission:3168`; `MetamodelSeeds:280,321,738`; `ScanRelations:842` | **DIVERGENT — the worst row.** ~14 implementations, one of which omits the substitution. **The javadoc's claim is false** |
| 5 | How a set id resolves to a set | `ResolvedMapping.set(setId):86` | `ClassSources.findBindingBySetId:490` re-walks it over compiled bindings | **SPLIT by design** (two representations), but tie-breaks restated rather than shared |
| 6 | How the include closure orders | `MappingClosures.Closure` | admits **four** orders internally; then `StoreSubstitutionRewrite.resolveAllStores`, `MappingDefinition.classBindingsWithIncludes:330`, `ClassSources:490/517`, `AssociationJoins:643`, `GraphEmission:3188`, `MetamodelSeeds:203`, `SqlTextVerdicts:1534`, `PlanText:630`, `ScanRelations:2397` | **DIVERGENT.** ~10 walkers of one graph. The normalizer's own class says the divergence *"is the current semantics"* |
| 7 | How an include path resolves to a mapping FQN | `NameResolver:953` (Phase D) | `MappingClosures.surfaceOf:87` + package retry `:350`; `StoreSubstitutionRewrite.includeFqn`; `MappingDefinition.collectIncludedBindings:341`; `MetamodelSeeds.resolveIncludePath`; resolver sites use bare `findMapping(path)` with **no** package handling | **DIVERGENT.** Phase D already owns this; four more owners, three rules, and the resolver's version silently misses bare paths |
| 8 | When a cast/coercion is needed | `DeclaredCoercions` | nobody | **OK.** Clean single owner, class named for the decision |
| 9 | How a union member is ordered | declaration order of `Union.memberSetIds`, stamped by `MappingLedger.operationMembers` onto `ClassBinding.Operation.memberSetIds` | `StackBuilder:193/1741` and `ClassSources:709` **consume** the stamp; `ResolvedMapping.memberOrdinal:178` is the sole ordinal computation | **OK — exemplary.** Decided once at E, stamped, read verbatim. **The pattern the rest should follow** |
| 10 | What poisons a set, which reason wins | `MappingLedger.poisons` | Seven write sites; **three key shapes** and **three collision policies** (`put`, `putIfAbsent`, `merge`) | **DIVERGENT.** See Q4 |
| 11 | What makes a mapping/set invalid | `MappingValidation.run` | `ModelBuilder:480–535` independently validates one-root and distinct-set-ids, **also raising `Phase.NORMALIZE`**; `MappingPrePass.detectM2MCycles` | **SPLIT across packages.** Two validators in two packages both speaking as Phase E |
| 12 | Which dyna function a name denotes | `DynaFn.of(name)` (exact, case-sensitive) via `RelOpTranslator.dyna:62` | `JoinChainEmission.nullTolerant:1031–1043` uses `equalsIgnoreCase` against literals `"isNull"`, `"sqlNull"`, `"coalesce"`, `"ifnull"`, `"nvl"`, `"case"`, `"if"` — five of which are real `DynaFn` members | **DIVERGENT, and it changes rows.** This predicate gates the INNER-vs-LEFT+WHERE realization; a miss produces a silently wrong join shape. Case-insensitivity also accepts spellings `DynaFn.of` rejects |
| 13 | Which simple name an association mapping denotes | `NameResolver.resolveName:570` (wildcards → own package → core imports; **throws on ambiguity**) | `AssociationSynthesis.resolveAssociation:374` (exact → wildcards **first-wins** → same-package fallback) | **DIVERGENT.** Same tiers, **opposite ambiguity policy.** See Q5 |
| 14 | Which database a `[db]` ref denotes | `NameResolver.resolvePropertyMapping:1156` etc. | `StoreSubstitutionRewrite.qualifyStoreRefs:358` re-resolves, deliberately only for "SHADOWED cases" because unconditional qualification regressed the corpus | **DIVERGENT.** Phase E patching Phase D's output |
| 15 | Which overload a `~func` ref denotes | `SpecCompiler`/`Typer` (Phase G) | `MappingNormalizer.relationFunctionPipeline:930` calls `model.findFunction`, and **on empty falls back** to `compiler.spec.SignatureMangle.resolve` | **Phase E doing Phase G's job, plus a fallback** |
| 16 | A set's declared key text | `SetKeyFacts.declaredKeysOf` → `ClassBinding.Relational.declared`, read at 20 sites | nobody | **OK.** Stamped once, read verbatim |
| 17 | A set's physical source | `relationalSourceOf:1441` → `ClassBinding.Relational.source` (sealed, non-null) | nobody | **OK — exemplary.** *"A door that forgets to stamp does not compile"* |
| 18 | How a lifted function is named | `SynthFqn` (+ `SynthHat.segment()`), pinned by a runtime test | `UnionSynthesis.memberFunction:96` re-decides *which* naming variant by recomputing root-or-sole (row 3) | **OK for the spelling, DIVERGENT for the variant choice** |

**Score: 6 clean owners, 3 split, 9 divergent.**

> **The clean ones are all *stamped typed facts on the binding*. The divergent ones are all *rules
> recomputed from the surface at each point of use*. That correlation is the whole story of this
> subsystem.**

---

## Q4 — The stamped-facts design

`MappingDefinition.NormalizationFacts` is six `Map<String, ?>` fields, two constructors plus canonical,
and a `NONE`. Alongside it, **on the same record but not inside `facts`**, sit two more
Phase-E-learned maps: `routedTargetSets` and `resolvedStores`. **So "what Phase E learned" is actually
eight maps in two places with no principle separating them.**

**Is the shape right? No — and the evidence is that the shape predicts which facts survive.**

| fact | shape | readers at HEAD |
|---|---|---|
| `ClassBinding.Relational.source` | sealed `Table \| Json`, non-null | live, many |
| `declared` (`DeclaredKeys`) | typed record | 20 |
| `Operation.memberSetIds` | `List<String>` on its variant | 7 |
| `primaryKeyColumns` | typed component | 11 |
| `aggregateViews` / `propertyPins` | typed records on the binding | 3 / 1 |
| `facts().poisons` | `Map<String,String>` | 1 accessor, 3 call sites |
| `facts().mixedUnions` | `Map<String,List<String>>` | 1 |
| `facts().unionKeyThreads` | `Map<String,List<KeyThread>>` | 1 |
| `facts().unionMembers` | `Map<String,List<String>>` | 1 |
| `facts().routedTargetClasses` | `Map<String,Map<String,String>>` | 1 |
| `facts().nullableCensus` | `Map<String,Set<String>>` | **0 anywhere** |
| `routedTargetSets` (top level) | `Map<String,String>` | **0 anywhere** |

**Every *typed, per-binding* fact is consumed. Every *untyped string-map* fact has exactly one consumer
or none.** Two are entirely dead while the machinery that computes them (`SetDispatch`, 99 lines;
`RequiredNullableCensus`, 130 lines) is fully alive and runs on every compile.

**The key-shape problem is confirmed.** `poisons` is written with three grammars and read with exactly
one (`PureModelContext:368  md.facts().poisons().get(classFqn)`). **Nothing in the repo ever constructs
a `class[setId]` key to look one up. The `[setId]` writes are dead**, and the failure mode is silent.
A typed key would have made this a compile error.

### The right shape

```java
/** Why a binding is withheld. */
public sealed interface PoisonKey {
    record ForClass(String classFqn) implements PoisonKey {}
    record ForSet(String classFqn, String setId) implements PoisonKey {}
    record ForAssociation(String associationFqn) implements PoisonKey {}
}

public record NormalizationFacts(
        Poisons poisons,                       // Map<PoisonKey, Reason> + one documented merge rule
        Map<String, UnionFacts> unions,        // classFqn -> { memberSetIds, mixedKind, keyThreads }
        Map<String, RoutingFacts> routing)     // ownerClassFqn -> { perProperty targetClass }
```

Three changes carry the weight:

1. **Typed key.** Makes the two grammars distinguishable and makes "nobody reads `ForSet`" a
   compiler-visible fact. Cost ~40 lines; touches 7 write sites and 4 read sites.
2. **Group by subject, not by producer.** `mixedUnions`, `unionKeyThreads` and `unionMembers` are three
   maps keyed by the same class FQN describing the same union. **They are one record.** Today a reader
   must consult three maps and know they agree; nothing enforces that they do.
3. **Push per-binding facts onto the binding.** `routedTargetClasses` is per-owner-class-per-property;
   it belongs on `ClassBinding` next to `propertyPins`, which is already exactly that shape.
   `nullableCensus` is not a fact about a mapping at all — it is a compile-wide diagnostic register and
   should not be on the artifact (it should be deleted; Q8).

**What growth-by-appending costs, concretely:** `MappingDefinition` now has three constructors; the
7-arg one has **zero callers**, and `NormalizationFacts`' 4-arg convenience constructor has **zero
callers**. **Each new fact added a field, a null-guard branch, and a convenience overload to keep old
call sites compiling — which is exactly the mechanism by which a dead field survives, because nothing
ever forces a site to mention it.**

Contrast `ClassBinding.Relational`, whose javadoc records the opposite decision and its reason:
*"NO convenience constructors: one silently dropped `primaryKeyColumns` (the AssocJoin disease) — every
site spells every component."* **That lesson was learned on the binding and not applied one level up.**

---

## Q5 — Layering: is Phase E doing Phase D's (and G's) job?

**Yes, at four sites, and it is worse than redundancy — the rules differ.**

The package contract says Phase E *"treats name strings as opaque FQN tokens and propagates them
verbatim… it does not resolve, look up scopes, or invoke other phases."*

1. **`AssociationSynthesis.resolveAssociation:374–394`** implements a full name resolver. Phase D's
   `NameResolver.resolveNameMulti:653` already resolves association-mapping header names through the
   same tiers — **with one decisive difference: D collects all wildcard matches and throws
   `ResolutionException("ambiguous reference")`; E takes the first one silently.** So the duplication is
   not benign. AGENTS.md invariant 4 ("NO FALLBACKS") violated **with a semantic difference attached.**

   It also isn't understood: the test that covers it, `AssocSimpleNameProbeTest`, is titled *"simple-name
   AssociationMapping binds by FQN (same-package fallback)"* but its model is `Mapping a::M` with
   `import a::m::*` referencing `a::m::Driver` — **a wildcard match, not a same-package one. The
   same-package arm is untested.**

2. **`StoreSubstitutionRewrite.qualifyStoreRefs:358`** re-resolves `[db]` refs Phase D already resolved,
   and its comment states the cost outright: *"rewrite ONLY the SHADOWED cases… the propertyLevel
   family regressed wholesale under unconditional qualification."* **This is a name resolver that
   deliberately resolves *some* names, because resolving all of them breaks consumers keyed on the
   unresolved spelling.** That is the real cost: **the resolved/unresolved distinction has leaked into
   downstream keys**, so Phase D's output cannot be trusted to be resolved.

3. **`MappingNormalizer.relationFunctionPipeline:926–931`** does Phase G's overload resolution, with a
   fallback into `com.legend.compiler.spec` — **the literal negation of "does not… invoke other phases."**
   `MappingNormalizer:2860` reaches into `compiler.spec.CoreFn` for the same reason.

4. **`MappingClosures.of:59`** writes normalizer state into the shared model index:
   `return model.knowledge().derived(MappingClosures.class, MappingClosures::new);` — installing a
   mutable, normalizer-package object (two non-synchronized `HashMap`s) into `ModelBuilder`'s derived
   cache, **through a channel the guard does not cover** (Q6). It also means
   `MappingNormalizer.normalize(parsed, model)` is **not** the *"pure function of `(parsed, model)`"* its
   javadoc claims: called twice on one index, the second call reads memoized closures computed before
   the index changed.

**What it costs.** Two name-resolution algorithms for the same names with different ambiguity handling;
neither can be changed without checking the other; error messages differ in phase and wording; and
**Phase D's contract is no longer a guarantee anyone downstream can rely on, which is why
`qualifyStoreRefs` has to exist at all.** The fix is not to delete the E-side code — it is to make D
total for mapping bodies, then delete all four E-side resolvers and add the rule in Q6 that prevents
them coming back.

---

## Q6 — Guards: enforced vs asserted

### First, a correction to the premise

The observation about ArchUnit is right, and so is the state of the world
(`grep -c LegacyMappingDefinition core/src/main/java/com/legend/resolver` → **0**) — but **the invariant
IS mechanically guarded, by a different and in some ways stronger mechanism:**

`LegacyReachbackCensusTest.findLegacyMappingCallersArePinned` pins the exact per-file *occurrence
count* of `findLegacyMapping` across `core/src/main`, `pct`, `nlq`, `parser-equivalence`, with comments
stripped and a 250-file coverage floor:

```
normalizer/MappingClosures.java              2
MetamodelSeeds.java                          1
lineage/ScanRelations.java                   2
compiler/element/ModelContext.java           1
compiler/element/PureModelContext.java       4
compiler/ModelBuilder.java                   1
```

Any call added in `resolver/` fails the test as an unregistered key.

**Two doors it does not close:**

1. It matches the literal `findLegacyMapping` only. A **type-level** dependency on
   `LegacyMappingDefinition` acquired any other way is invisible.
2. **`NormalizedModel.legacySurfaces()`** — a `Map<String, LegacyMappingDefinition>` field added to the
   Phase-E output record. `MappingNormalizerTest:158` implements CLEAN_SHEET_INVERSION §7.4 guard 2
   (*"no `LegacyMappingDefinition` survives into the `NormalizedModel`"*) by iterating `elements` — **and
   the record grew a *second* field carrying exactly the banned type. The guard tests the letter; the
   record moved.**

### What IS mechanically enforced (relevant to Phase E)

| guard | what it pins |
|---|---|
| `ArchitectureTest.postNormalizationPhasesAreParserFree` | resolver/lowering/exec/typed cannot see `parser`, `lexer`, **`normalizer`**, `ide` |
| `ArchitectureTest.resolverNeverSeesTheUntypedAst` | resolver cannot see `protocol..` at all |
| `ArchitectureTest.modelIsPureData` | `model..` depends only on protocol/values/error/JDK |
| `ArchitectureTest.normalizerNeverWritesIntoTheModelIndex` | normalizer may not call `ModelBuilder.{add, retainLegacySurface, registerMappedClass}`; **no public field on `ModelBuilder`** |
| `ArchitectureTest.packageDependenciesAreAcyclic` | no top-level package cycles (compiler↔normalizer is currently one-way — verified) |
| `ArchitectureTest.typedNodesAreMintedOnlyByCompilerLayers` | only compiler/resolver/normalizer/lowering construct typed HIR |
| `LegacyReachbackCensusTest` | exact per-file `findLegacyMapping` occurrence counts |
| `ShadowWalkerCensusTest` | 19 shadow walkers' call-site counts under `normalizer/`, shrink-only |
| `CodeShapeGuardrailTest` | file ≤3500 / method ≤250 lines; dead **private** methods shrink-only; no static mutable state |
| `MappingNormalizerTest:158` | no `LegacyMappingDefinition` in `NormalizedModel.elements` |

### True-but-unguarded invariants, with the rules that would close them

| # | invariant | status | proposed rule |
|---|---|---|---|
| **U1** | The resolver never depends on the legacy surface **type** | true, unguarded at type level | `noClasses().that().resideInAnyPackage("com.legend.resolver..","com.legend.lowering..","com.legend.exec..","com.legend.compiler.spec..").should().dependOnClassesThat().belongToAnyOf(LegacyMappingDefinition.class, ClassMapping.class, PropertyMapping.class, AssociationMapping.class, RelationalOperation.class)` — **ban the whole legacy-surface vocabulary, not one accessor** |
| **U2** | No phase ≥ F branches on synthesis provenance (CSI §2.4: *"a compile error in review for any phase ≥ F to branch on it"*) | **VIOLATED** at `resolver/AssociationJoins.java:1236` (`fd.synthesizedFrom().hat() == SynthHat.PROP` gates β-inlining of derived properties in XStore conditions) | ban `SynthHat` / `FunctionDefinition.Synthesized` from those packages. **Fix first:** the inline-ability decision is a property of the call, not of where the function came from |
| **U3** | `NormalizedModel` carries no legacy surface | **VIOLATED** by the `legacySurfaces` field; the §7.4 test checks only `elements` | remove the field (move the archive to a separate `LineageArchive` element the lineage package owns), or extend the test to the whole record graph |
| **U4** | The normalizer does not reach into later phases | **VIOLATED** — `MappingNormalizer:930` (`compiler.spec.SignatureMangle`), `:2860` (`compiler.spec.CoreFn`) | `noClasses().that().resideInAPackage("com.legend.normalizer..").should().dependOnClassesThat().resideInAnyPackage("com.legend.compiler.spec..","com.legend.resolver..","com.legend.lowering..","com.legend.exec..","com.legend.sql..")` |
| **U5** | The normalizer writes nothing into the model index | guarded by a **method-name list**; `MappingClosures:59` installs mutable normalizer state through an uncovered channel | **structural fix instead of a bigger name list:** extract a read-only `Knowledge` interface (81 of ~140 normalizer reads already go through `knowledge()`), hand Phase E only that, and ban `ModelBuilder` from `normalizer`. The name-list rule then becomes unnecessary |
| **U6** | Set-id spelling has one owner | **FALSE** (~14 copies) | not an ArchUnit shape — a source ratchet: `assertEquals(1, occurrencesOf("replace(\"::\", \"_\")", mainSources()))` after the copies are folded into one `SetId.of(...)`. **The codebase already has exactly this idiom** (`LegacyReachbackCensusTest`) |
| **U7** | Include-path resolution has one owner | **FALSE** (4 rules, ~10 walkers) | same mechanism: a pinned count of `mappingPath()` call sites, plus an ArchUnit rule that only `NameResolver` may resolve them once D is total |
| **U8** | Dyna-function identity has one owner (`DynaFn.of`) | **FALSE** (`JoinChainEmission:1031–1043`) | a source ratchet on string literals equal to a `DynaFn` spelling outside `builtin/` — cheap, because `DynaFn.values()` supplies the list at test time |
| **U9** | AGENTS.md invariant 4, "NO FALLBACKS" | `[CONVENTION]` — nothing checks it; three live fallbacks are *documented in comments* (`AssociationSynthesis:389`, `StoreSubstitutionRewrite:240,371`, `MappingNormalizer:928`) | a `FallbackLedgerTest` in the shape of `JavaEvalLedgerTest`: a shrink-only register with a justification per row. **The codebase already trusts this mechanism for harder invariants** |
| **U10** | Stamps are read verbatim, never interpreted | prose in `MappingDefinition` javadoc only | not fully mechanizable; the cheap half is to force all fact reads through the four named `ModelContext` accessors so the read surface stays countable |
| **U11** | `ShadowWalkerCensusTest` walks the right directory | **no coverage floor** — it hardcodes `Path.of("src/main/java/com/legend/normalizer")` and never asserts how many files it found | one line: `GuardCoverage.assertFloor("shadow walkers", files.size(), 29)`. **This is exactly the rot `GuardCoverage` was written for, and this guard is the one that skipped it** |

---

## Q7 — Altitude

**Mixed, and the mixing is concentrated in `MappingNormalizer`.**

1. **`normalizeMapping:257–455` (200 lines).** Phase orchestration interleaved with: a 115-line `for`
   body (`:292`), the **same 19-line binding-construction ternary written twice** (`:322–340`,
   `:386–406`), string surgery for poison keys, and
   `String.valueOf(e.getMessage()).split("\n")[0]` at `:202` and `:218` —
   **first-line-of-a-stack-message extraction, in the top-level driver.**
2. **`AssociationSynthesis:309–316` — decoding a composite key with `startsWith`/`substring`:**
   ```java
   String sid = ResolvedMapping.idOf(rcm);
   String classId = rcm.className().replace("::", "_");
   String prop = key.startsWith(sid + "_")     ? key.substring(sid.length() + 1)
               : key.startsWith(classId + "_") ? key.substring(classId.length() + 1) : null;
   ```
   An `(setId, embeddedProperty)` pair encoded into one string and parsed back apart. **Any set id
   containing `_` — and `classId` is *built* by inserting `_` — can be mis-split.** A missing two-field
   record, in the middle of an association-injection pass.
3. **`JoinChainEmission.nullTolerant:1029–1046`** — `fc.name().equalsIgnoreCase("nvl")` inside a
   decision that determines whether an INNER join is realized as LEFT+WHERE. **High-stakes row
   semantics decided by case-insensitive string comparison, ten lines from a package
   (`builtin.DynaFn`) that exists to be the typed vocabulary for exactly this.**
4. **`MappingNormalizer:2781–2910` — "Low-level helpers"** is 130 lines of `ValueSpecification`
   construction living in the same class as `normalize(ParsedModel, ModelBuilder, Map)`. **Four orders
   of magnitude of abstraction in one file.**
5. **`inferMainTableQuiet:1522`** — exception-as-control-flow, and it is the version two callers
   actually use, **so the loud rule's diagnostics are discarded at the points that matter.**

**Counter-examples, to be fair:** `RelOpTranslator` is consistently at one altitude and reads well
despite 731 lines. `DeclaredCoercions`, `MappingValidation`, `MilestoningFacts` and `ResolvedMapping`
are all at one level throughout. **The altitude problem is not the package's; it is `MappingNormalizer`'s,
plus the two sites above.**

---

## Q8 — What I would delete (~550 production lines)

**Entirely dead — zero consumers anywhere in `core`, `pct`, `nlq`, `parser-equivalence`:**

| thing | lines | evidence |
|---|---|---|
| `SetDispatch` (whole class) + `MappingDefinition.routedTargetSets` + its 7-arg constructor + the `:453` stamping call | ~120 | `grep -rn routedTargetSets` → 7 hits: 5 declaration/copy, 1 construction, 1 definition. **No reader.** It walks the entire include closure on every compile to produce a map nobody opens |
| `RequiredNullableCensus` (whole class) + `MappingLedger.nullableCensus` + `census()` + `NormalizationFacts.nullableCensus` + `ModelContext.requiredNullableCensus()` + `PureModelContext:510` + the two hooks in `DeclaredCoercions` | ~180 | Zero readers repo-wide. Its javadoc promises *"the corpus harness AGGREGATES across its models and pins"* — **no such harness exists** |
| `DynaFnArms` (whole class) | 56 | `public` solely *"so the registry's test can hold the declarations against the code"*; **the only repo reference outside the file is a javadoc sentence** in `DynaFn.java:31` |
| `NormalizedModel.liftedByOwner()` | 20 | Zero production readers; only `ModelNormalizerTest` calls it. Its own javadoc: *"it exists for the incremental-invalidation layer"* — **a layer that does not exist.** Keeping it as a designed extension point is defensible; keeping it *and* documenting it as live is not |

**Dead members:** `MappingDefinition`'s 7-arg constructor (0 callers); `NormalizationFacts`' 4-arg
constructor (0 callers); `MappingLedger.facts(surface, md, model)` (`md`/`model` unused); **five dead
package-private statics the `deadPrivateMethodsOnlyShrink` guard cannot see** —
`GroupBySynthesis.isGroupByStep:228`, `JoinChainEmission.classTypedButUnmapped:738`,
`MappingNormalizer.nullOfDeclaredType:1360`, `MappingNormalizer.nullOfPhysicalKind:1335`,
`ViewRelation.relationExpr:527`; the `poisons` `class[setId]` write sites (`:312`, `:348`) — **but do
not just delete the writes; the *reads* are what is missing**; 11 unused imports in
`MappingNormalizer`, 34 in `UnionSynthesis`, 29 in `AssociationSynthesis`, 27 in `JoinChainEmission`;
five empty section banners.

**Concepts to fold (not remove functionality):**

- `MilestoningFacts` → one method on `KnowledgeLayer` beside `lineage()`.
- `SetKeyFacts` → `MappingDefinition.ClassBinding.DeclaredKeys.of(rcm)`, **killing the second
  set-identity spelling in the process.**
- `MappingFacts` → the two computations move onto the bindings they describe.
- `AggregateViewLift` → back into the binding builder it delegates to.
- `MissProbe` → **keep the *program*, drop the *class*.** The 23 loud sites are the win and they already
  `orElseThrow`. `MissProbe.miss()` as a `Supplier<null>` buys nothing without a ratchet; either add the
  ratchet (`MissProbeCensusTest`, in the `ShadowWalkerCensusTest` shape) or inline it back to
  `orElse(null)` and stop pretending.

**Stale documentation to correct (not delete):** `RequiredNullableCensus`' javadoc names a deleted
channel; `PureModelContext:104` says *"F+ compilation never reads"* the legacy surfaces, but `:421` and
`:461` do (existence probes only, but they do).

---

## What is genuinely well-designed

1. **`MappingDefinition.ClassBinding` is excellent.** Sealed by binding kind; `Relational.source` is a
   **non-null** component, so — in the javadoc's words — *"a door that forgets to stamp does not
   compile."* `RelationalSource` is sealed and **total**: `Table | Json`, with the decision to have *no*
   `Unknown` variant journaled and the pre-lift placeholder moved to a separate phase type. The explicit
   refusal of convenience constructors is **the right lesson drawn from the right incident.**
2. **`ResolvedMapping` is the model for the whole package.** One object, constructed once, answering
   every closure question. **Where the package follows this pattern the one-owner property holds; where
   it does not, it does not. The fix for most of Q3 is "more `ResolvedMapping`."**
3. **The stamp-and-read-verbatim pattern works, demonstrably.** Union member order is decided once,
   stamped, and read verbatim by `StackBuilder` — with the rule spelled out: *"recorded by the synthesis
   that emitted the body so no reader walks the body."* **Every typed stamp on the binding is alive and
   singly-owned. The design thesis is correct; it is the untyped half that rotted.**
4. **`NormalizedModel` as a phase type.** Re-normalization is impossible at the signature level — *"the
   duplicate-synth footgun dies at the signature."*
5. **`SynthHat` (CSI-12) and `SynthFqn` injectivity (CSI-10).** Two stringly-typed bugs found by
   adversarial self-review and fixed **structurally**: the hat became an enum whose `segment()` drives
   *both* the provenance tag and the FQN segment, pinned by a runtime test; and the lifted-mapping FQN
   embeds the full class FQN after a correct analysis showing the simple-name scheme is not injective.
   **Both include a written record of the rejected alternatives and why.**
6. **`MappingLedger` as a direction.** Replacing five write channels into a shared model index with a
   per-mapping accumulator stamped onto the artifact is exactly right, and
   `normalizerNeverWritesIntoTheModelIndex` locks the old doors.
7. **The guard culture is unusually strong.** `GuardCoverage.assertFloor` — *"Scope is a SILENT
   parameter… six of the audit's nine compliance-theater instances were guards whose scope rotted, not
   guards whose logic broke"* — is a better idea than most codebases have. `LegacyReachbackCensusTest`
   counting *occurrences per file* closes a hole the JDBC census left open. **AGENTS.md marking every
   invariant `[ENFORCED]` or `[CONVENTION]` and saying plainly "breaking it will not turn anything red,
   so it is on you" is rarer still, and it is what made this review possible.**
8. **The decision journal (CSI-1…CSI-14).** Each entry states the decision, the rejected alternatives,
   and the reason. CSI-7's index/sidecar test is a genuinely reusable rule, applied correctly.
9. **`MissProbe`'s program**, whatever one thinks of the class: a full census of 70 `orElse(null)` sites,
   split into 39 that never fired (now loud) and 31 that legitimately fire, **is real work and the right
   kind of work.**
10. **`StoreSubstitutionRewrite`'s exhaustiveness stance** — *"a new PM / `RelationalOperation` variant
    cannot be added without a rule here (the windowize stance)"* — and `DeclaredCoercions`' single
    ownership of the coercion decision.

---

## Prioritized: what I would change

### P0 — correctness, cheap

1. **Read the poison keys you write, or stop writing them.** Then introduce `PoisonKey` (Q4) so the
   question cannot recur, and pick **one** collision policy.
2. **Route `JoinChainEmission.nullTolerant` through `DynaFn.of`.** This predicate decides INNER vs
   LEFT+WHERE realization; deciding it with `equalsIgnoreCase` string literals next to a typed registry
   is a row-level bug waiting on a spelling. Check whether `"ifnull"`/`"nvl"` are reachable names at all
   — if not, those arms are dead.
3. **Add U2's guard and fix `AssociationJoins:1236`.** Phase H branching on `SynthHat.PROP` is a
   documented-forbidden coupling; **the inlinability of a call is a property of the call, not its provenance.**

### P1 — the one-owner debt, in order of blast radius

4. **`SetId.of(ClassMapping)` / `SetId.of(ClassBinding)`, one function, ~14 call sites folded** —
   including `SetKeyFacts.setKey`, which spells a *different* rule. Pin with a source ratchet (U6).
   **Half a day, and it retires the single most-copied decision in the codebase.**
5. **`Root.of(...)` — one answer to "is this set root-or-sole," with its counting scope an explicit
   parameter.** Six sites, three different scopes, zero comparison. **Fold them and the scope difference
   becomes visible instead of implicit.**
6. **Make Phase D total for mapping bodies, then delete Phase E's four resolvers** and add U4's rule.
   The hard part is the admitted downstream keying on unresolved spellings — **but that is the actual
   debt, and it will only get more expensive.**
7. **One include walker.** `MappingClosures` for the surface side, `classBindingsWithIncludes` for the
   compiled side, and nothing else. Where the four orders inside `MappingClosures` genuinely differ,
   make them four *named* methods **so the difference is a decision rather than a footnote.**

### P2 — the shape

8. **Delete the ~550 dead lines** before any further decomposition, so the next split is measured
   against live code.
9. **Extract a read-only `Knowledge` interface and hand Phase E only that** (U5). This makes
   `normalizerNeverWritesIntoTheModelIndex` unnecessary by construction and closes the `derived()`
   back door.
10. **Re-cut `MappingNormalizer` at joints, not at 3,500 lines.** In value order: `ClassBindingBuilder`,
    `MainTable`, `RowProjection`, `PureSpecBuilder`, `BuildMode`. **Acceptance test for a real
    extraction: the new file's import block is not the parent's, and the calls do not go both ways.**
11. **Reshape `NormalizationFacts`** per Q4 and delete the convenience constructors that let dead fields
    survive.
12. **Add `GuardCoverage.assertFloor` to `ShadowWalkerCensusTest`** (one line) and add the
    `FallbackLedgerTest` of U9. **AGENTS.md's most-cited invariant currently has no mechanical form at
    all, and this codebase already knows how to build one.**
