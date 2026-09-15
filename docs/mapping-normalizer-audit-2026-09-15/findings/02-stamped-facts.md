# 02 — The stamped-facts pipeline

**Scope read end to end:** `MappingPrePass` (261), `ResolvedMapping` (196), `MappingClosures` (431),
`MappingFacts` (117), `MappingLedger` (90), `MappingValidation` (93), `SetKeyFacts` (68),
`SetDispatch` (99), `StoreSubstitutionRewrite` (401), `model/MappingDefinition.java`,
`model/NormalizedModel.java`, `compiler/element/PureModelContext.java`, `ModelContext.java`.

---

## VERDICT

The claim — *"what Phase E learns is STAMPED on the compiled mapping artifact, never written into a
shared index, and never rediscovered downstream"* — is **two-thirds true**.

"Never written into a shared index" is genuinely, mechanically enforced. "Stamped on the artifact"
is true. **"Read as a fact downstream" is false for three of the stamped facts**: one whole stamped
map and one whole fact channel have *zero readers anywhere in the repo*, and a third has a key
language no reader can address.

---

## HIGH-1 — `MappingDefinition.routedTargetSets` is write-only: zero readers in the entire repo

**Written:** `MappingNormalizer.java:453` — `SetDispatch.routedTargetSets(md, model), resolvedStores,`

**Read:** nowhere. Repo-wide grep returns 7 hits: the `SetDispatch` producer, the one call site, and
5 lines inside `MappingDefinition.java` (component decl, convenience ctor, canonical-ctor copy).
No accessor call `.routedTargetSets()` exists in `core/`, `pct/`, `nlq/`, `parser-equivalence/`,
main **or** test.

**The javadoc claims a consumer that does not exist** — `SetDispatch.java:20-33`:

> *"the resolver materializes that set's binding; union targets dispatch through the union machinery instead"*

**Why it matters.** `SetDispatch` (99 lines) walks the full include closure of every mapping — class
PMs, association PMs, union/root classification, conflict detection — on every compile, and the
result is discarded. It is also the third convenience-constructor argument, so it looks load-bearing.

**Related:** `ClassSources.getForNav` (`:85-95`), documented as the consumer of the H5 dispatch hint,
takes a `head` parameter, **never uses it**, and delegates to `get(mappingFqn, classFqn, scope)`.
`NavMaterializer.java:103-104` repeats the claim. See `07-downstream-consumers.md` HIGH-1.

---

## HIGH-2 — `NormalizationFacts.nullableCensus` has no consumer: the whole census pipeline is a dead end

- **Written:** `MappingLedger.java:78-80` ← `RequiredNullableCensus.java:99, 110, 117` (130-line producer)
- **Stamped:** `MappingLedger.java:87`
- **Reader accessor:** `PureModelContext.java:508-518` — a memoized `derived(NullableCensus.class, …)`
  union across every compiled mapping
- **Callers of that accessor:** **none.** Repo-wide, `requiredNullableCensus` appears exactly 3
  times: the `ModelContext` default (`:259`), the `PureModelContext` override (`:510`), and a javadoc
  mention.

**The javadoc claims a pinning harness that does not exist** — `RequiredNullableCensus.java:42-46`:

> *"Rows accumulate on the COMPILE'S OWN ModelBuilder (`requiredNullableRows()`) … Readers go through
> `ModelContext.requiredNullableCensus()`; **the corpus harness AGGREGATES across its models and pins**."*

Both halves are false: `requiredNullableRows()` was deleted (the `ArchitectureTest.java:1004` rule
exists specifically to keep it deleted), and no harness — no test, no tool, no module — calls the
reader.

The class doc calls this "the dialect-split warning's firing list and nothing else." The firing list
fires into nothing.

---

## HIGH-3 — `poisons` is written with two key languages; the bracketed `"Class[setId]"` has no reader

**Write sites (7 total):**

| line | key shape |
|---|---|
| `MappingNormalizer.java:304` | `cm.className()` |
| `MappingNormalizer.java:312` | `cm.className() + "[" + idOf(cm) + "]"` |
| `MappingNormalizer.java:348` | `cm.className() + "[" + idOf(cm) + "]"` |
| `MappingNormalizer.java:356` | `cm.className()` |
| `MappingNormalizer.java:377` | `cm.className()` |
| `MappingNormalizer.java:427` | association FQN |
| `UnionSynthesis.java:343` | `rcm.className()` |

**The only reader** — `PureModelContext.java:366-369`:

```java
public java.util.Optional<String> mappingPoison(String mappingFqn, String classFqn) {
    return model.findMapping(mappingFqn)
            .map(md -> md.facts().poisons().get(classFqn));
}
```

**All three call sites pass a plain class FQN or an association FQN**, never a bracketed key:
`ClassSources.java:699`, `ClassSources.java:1519`, `AssociationJoins.java:1094-1095`.

**Independently confirmed:** a repo-wide grep for `+ "[" +` finds every bracketed-string
construction in `core/src/main`. The only two that touch `poisons` are the two write sites above.
Nothing anywhere builds that key to read it. No test asserts a bracketed poison key either.

**Why it matters.** Those two sites are the *per-set fault-isolation* arm — precisely the H5
multi-set path the team is actively building. Every genuine per-set synthesis failure
(`NotImplementedException`/`ModelException` from a non-root set) records its full message into a slot
no diagnostic can reach. `MappingNormalizer.java:369-375` promises the opposite:

> *"The full message rides on the poison and surfaces via StoreResolver's 0-binder error."*

For the bracketed half, it does not.

---

## MED-4 — MASKING confirmed: the generic multi-set reason is the only one a user can ever see

**Trace.** `MappingNormalizer.java:300-352`, non-union multi-set arm:

```java
if (!unionMember) {
    ledger.poisons.putIfAbsent(cm.className(),
            "class is mapped through multiple set IDs;"
          + " .all() over multi-set mappings (implicit union) is a roadmap feature");
}
String invalidSet = pp.invalid().get(cm);
if (invalidSet != null) {
    ledger.poisons.putIfAbsent(cm.className() + "[" + ResolvedMapping.idOf(cm) + "]", invalidSet);
    continue;
}
try { … } catch (NotImplementedException | ModelException e) {
    ledger.poisons.putIfAbsent(cm.className() + "[" + ResolvedMapping.idOf(cm) + "]",
            String.valueOf(e.getMessage()));
}
```

The generic class-level reason is written **first**, unconditionally, in the same iteration; the real
reason then lands under the unreadable key. `ModelBuilder.ingestLegacyMapping` (R2) guarantees
exactly one root set per multi-set class, so the root set takes the *other* branch (`:356`/`:377`)
which uses `put` — a real root-set failure does overwrite the generic. But when the **root succeeds
and a non-root set fails** — the normal H5 shape — the class key holds only the generic text and the
real text is unreachable.

**What the user sees** when navigating a route pinned to the failed set (`ClassSources.java:697-700`):

> `class 'w::Person' is not mapped in mapping 'w::M' (class is mapped through multiple set IDs; .all() over multi-set mappings (implicit union) is a roadmap feature)`

…when the actual cause might be, e.g., `JoinChainEmission.java:521` *"route 'x[s2]' …"*. The message
is not merely incomplete — it is **actively misleading**: it names `.all()` and "implicit union" for
a failure that has nothing to do with either.

For a **union-rooted** multi-set class the `!unionMember` guard skips the generic entirely, so a
failing member set produces *no readable reason at all*.

---

## MED-5 — The clean-sheet door stamps `NormalizationFacts.NONE` through a convenience constructor

`MappingNormalizer.java:531-532` (`cleanSheetToCanonical`, the Door 1/3 path):

```java
return new MappingDefinition(md.qualifiedName(), md.includes(),
        classBindings, assocBindings, md.enumerationMappings(), md.testSuitesSource());
```

That 6-arg overload (`MappingDefinition.java:47-56`) fills in `routedTargetSets = Map.of()`,
`resolvedStores = Map.of()`, `facts = NormalizationFacts.NONE`.

**Consequence:** every clean-sheet mapping compiles with *no* poisons, *no* `unionMembers`, *no*
`routedTargetClasses`, *no* `resolvedStores`. `ElementReferences.castTotalByRoute` (`:141`) and
`totalMembershipCast` (`:164`) silently take their fallback paths for Door-1 mappings, and
`MetamodelSeeds`' `mapping_store_resolutions` (`:172`) yields nothing. Nothing in the type system or
a test flags this.

**The overload ladder hides partial initialization, and there is a live caller doing it.** Of the 3
`MappingDefinition` constructors, the 7-arg one (`:59-69`) has **zero** callers, and the 4-arg
`NormalizationFacts` convenience constructor (`:96-102`) also has **zero** callers — `NONE` and the
canonical 6-arg form are the only ones used. Two dead rungs propping up a ladder whose one live
shortcut drops four facts.

See `15-m2m-json-enum.md` §5 for the full clean-sheet fact comparison (included enum mappings are
also dropped).

---

## MED-6 — `MappingFacts.unionMembers` resolves members by raw `setId`, not the effective id

`MappingFacts.java:42-48`:

```java
for (String sid : u.memberSetIds()) {
    String memberClass = null;
    for (ClassMapping m2 : surface.classMappings()) {
        if (sid.equals(m2.setId())) {          // raw setId
            memberClass = m2.className();
```

Every other resolution in the package goes through `ResolvedMapping.idOf` (`:82-84`), which falls
back to `className().replace("::", "_")` — "the engine's default. **The one rule.**"
`UnionSynthesis.synthUnion:378-381` resolves the *same* `memberSetIds` with `idOf`. So `synthUnion`
can bind a union that `unionMembers` reports as unresolvable, and the reader
(`ElementReferences.java:164`) then falls through to the `classBindingsWithIncludes` extent walk
instead of the declared-member rule.

Also: `unionMembers` resolves members **only within the mapping's own `classMappings()`** — no
include closure — while `synthUnion` resolves across `md.includedSets()`. An include-spanning union
yields no fact.

---

## MED-7 — "the FIRST union of the class" contradicts the engine rule this codebase documents as LAST

`MappingFacts.java:37-38`:

```java
if (!(cm instanceof ClassMapping.Union u) || out.containsKey(u.className())) {
    continue;   // the reader took the FIRST union of the class
}
```

But `MappingClosures.java:207-210`, in the same package:

> *"…the LAST found — `rootClassMappingByClass`'s `last()` (R1)."*

Three different "the union of a class" rules now coexist: `MappingFacts` = own-first;
`ResolvedMapping.unionOf:132-139` = own-first then closure; `MappingClosures.Closure.union:211-224`
= last-wins.

**Verdict:** `unionMembers` is a faithful, verified-by-diff reimplementation of the pre-`bbd0049c5`
reader (`git show bbd0049c5^:…/PureModelContext.java:304-330` — same first-wins, same all-or-nothing
on an unresolvable member), **but the preserved rule is a preserved bug**, not correct behaviour,
measured against the engine rule the team's own `MappingClosures` javadoc cites.

---

## MED-8 — `routedTargetClasses`' javadoc asserts a guarantee the code enforces only *within* one set

`MappingFacts.java:62-65` claims:

> *"owner class → property → **the ONE class every route of the property lands on**"*

The disagreement check (`:102-104`) is inside the per-set loop; `:110-112` returns on the **first**
set that yields any answer:

```java
if (routed != null && !routed.equals(m2.className())) {
    return null;                    // intra-set disagreement → no fact
}
…
if (routed != null) {
    return routed;                  // first set with an answer wins; later sets unexamined
}
```

So an owner class with sets `s1` (`prop[a] → ClassA`) and `s2` (`prop[b] → ClassB`) yields `ClassA`
as "the one class every route lands on." The consumer is a **cast-totality** decision —
`ElementReferences.castTotalByRoute:141` → `StoreResolver.java:2622` — where a false "total" elides
a runtime type filter. **Wrong rows, not an error.** This too is bug-compatible with the old reader
(`bbd0049c5^:…:339-367`, identical shape), so it is a *preserved* unsoundness; but the javadoc
overstates the guarantee, which is what makes it dangerous to build on.

---

## MED-9 — Unresolvable includes: three closure walkers skip silently, two throw a raw ISE no wall catches

`MappingClosures.java:191-194` (and identically `:265-268`):

```java
LegacyMappingDefinition included = surfaceOf(inc.mappingPath())
        .orElseThrow(() -> MissProbe.neverFired("MappingClosures#1"));
if (included == null) {
    continue;   // unresolvable include is its own loud problem elsewhere
}
```

The `null` check is **dead code after `orElseThrow`** (4 such sites: `:191`, `:265`, `:361`, `:407`),
and the comment describes behaviour the line above makes impossible. Meanwhile `walkMappings:152`,
`walkOps:218` and `walkRoots:299` use `.ifPresent(…)` and skip the same condition silently. Only
`walkEnums:350-357` handles the package-local bare-include form that `SymbolTable.resolveId`
(exact-FQN only, `:72-76`) cannot resolve — `walkSets`/`walkIds` do not.

`MissProbe.neverFired` returns an `IllegalStateException`, which `MappingNormalizer.withElement:245-257`
**deliberately does not wrap** ("genuine bugs stay RAW"). So this path escapes `MappingPrePass.run`'s
`catch (ModelException)` at `:71` and crashes the compile — falsifying `MappingPrePass.java:42-46`:

> *"A mapping whose pre-pass fails is walled under a tolerant build (and absent from the result)…"*

---

## MED-10 — The order-independence test is far weaker than the claim it is cited for

`MappingNormalizer.java:166-169` asserts *"a mapping's synthesis never depends on which mappings
normalized before it (T4.1 step 2, **verified item 1**)."* The verification is
`MappedInClosureTest.compiledMappingsIgnoreElementOrder:100-118`:

- it swaps two **mutually independent** mappings (`IMPLYING`, `UNRELATED` — disjoint classes, no
  include edge). `INCLUDER`, the only mapping in the fixture with an include, is **excluded** from
  the order test;
- it compares `classBindings().map(cb -> cb.classFqn()).sorted()` — **sorted**, so binding order,
  `functionFqn`, `RelationalSource`, `declared()` keys and `primaryKeyColumns` are never compared;
- it never compares the **lifted functions** (the actual mapping bodies) or
  `routedTargetSets`/`resolvedStores`;
- it does compare `facts()` in full — the one strong assertion.

### Q5 — but the claim itself is TRUE, for reasons the test does not check

Independently verified. The mechanism:

- `ModelBuilder.from(adopted)` (`Compiler.java:254-255`) indexes the *same element objects*
  `parsed.elements()` iterates, so `MappingClosures.surfaceOf` and `MappingPrePass`'s `authored` are
  identity-equal and the `surfaces`/`closures` memos (`MappingClosures.java:52-53`) cannot fork by
  arrival order;
- `MappingClosures` reads only **raw** index surfaces, so no mapping's pre-pass observes another's;
- `MappingPrePass.run` completes for all mappings before the synthesis loop
  (`MappingNormalizer.java:170`);
- `resolveAllStores` is Kahn-ordered over include edges, not element order.

Every `HashMap` traced is used for keyed lookup only, with two exceptions, both benign:

- `ImplicitInheritance.apply:40-48` builds `byClass` from `HashMap.values()` order, but
  `nearestMappedAncestor:210-218` requires `cands.size() == 1` and returns `null` on ambiguity, so
  order cannot decide;
- `MappingPrePass.detectM2MCycles:234` iterates `pureByTarget.values()` (a `HashMap`), which can vary
  the *rendered chain* in a cycle error message but not whether a cycle is detected.

---

## LOW-11 — `MappingLedger.facts()` takes two parameters it never uses

`MappingLedger.java:84-89`:

```java
MappingDefinition.NormalizationFacts facts(LegacyMappingDefinition surface,
        LegacyMappingDefinition md, com.legend.compiler.ModelBuilder model) {
    return new MappingDefinition.NormalizationFacts(
            poisons, mixedUnions, unionKeyThreads, nullableCensus,
            MappingFacts.unionMembers(surface), MappingFacts.routedTargetClasses(surface));
}
```

`md` and `model` are dead. The call site `MappingNormalizer.java:454` passes `md.raw()` and `model`,
which makes the signature read as though the facts are computed over the **pre-passed** record. They
are not — both surface facts come from `pp.surface()` (authored + JSON identity sets), pre-extends-
flatten, pre-implicit-ops, pre-multi-hop-injection. That is the correct bug-compatible choice, but
the signature actively misleads about it.

## LOW-12 — Stale javadoc on `LegacyMappingDefinition`

`LegacyMappingDefinition.java:74-78` claims `ModelBuilder.from()` cross-bakes the JSON identity sets.
It no longer does (grep: two comment mentions at `ModelBuilder:228`, `:331`, no code). That rewrite
moved to `MappingClosures.withJsonIdentitySets:91-121`, which says so at `:36-39`.

## LOW-13 — Performance is a non-issue; the numbers

`MappingFacts.routedTargetClasses` is the deepest nest: O(routedProps × sets × PMs × sets). Measured
over the full corpus (`core/src/test/resources` + the `legend-engine` checkout): **934 mapping
blocks; median 2 class mappings, p95 = 9, p99 = 19, max = 44.** Worst single mapping is
`propertyUnion.pure` (22 sets, 43 PMs, 40 routed PMs) at an **upper bound of ~832k inner
iterations** — low single-digit milliseconds, once per compile. `unionMembers` is negligible.
**No action warranted.**

---

## Q8 — The guard: the premise is right, the conclusion was refuted

`ArchitectureTest.java` contains **no** rule stopping the resolver from reading
`LegacyMappingDefinition`: `com.legend.model` is explicitly permitted (`modelIsPureData:288-300` is
about what *model* may depend on, not who may depend on it), and `ModelContext.findLegacyMapping`
(`:71-75`) is on the interface the resolver holds.

**But a different, non-ArchUnit mechanical guard exists and would catch it:**
`core/src/test/java/com/legend/LegacyReachbackCensusTest.java`. It walks all four source roots,
strips comments, and **counts `findLegacyMapping` occurrences per file**, asserting exact equality
against a pinned register (`:60-104`) of 5 files with per-file counts (`MappingClosures` 2,
`MetamodelSeeds` 1, `ScanRelations` 2, `ModelContext` 1, `PureModelContext` 4, `ModelBuilder` 1),
plus a 250-file coverage floor. A new resolver caller is a new map key → `assertEquals` fails.

**Scope and limits, honestly stated:**

- it pins *call sites*, not *reachability* — a helper inside an already-registered file
  (`PureModelContext` has 4 budgeted occurrences) could expose the surface to unlimited new callers;
- it is occurrence-counted per file, so a *replacement* (delete one reach, add another in the same
  file) is invisible;
- it covers only the `findLegacyMapping` name. Nothing pins `NormalizedModel.legacySurfaces()`
  (`:35-38`, whose own javadoc says "the compilation pipeline never reads it") — though today the
  sole consumer is `PureModelContext:104`, feeding `retainLegacySurface`.

**So: the guard exists, it is real, and it is better than an ArchUnit rule would be** (count-exact,
not package-shaped). It is not airtight, and the `ArchitectureTest` layering rules do not back it up.

---

## Census — stamped facts

| fact | written at | read at | reachable? |
|---|---|---|---|
| `poisons` — class-FQN key | `MappingNormalizer:304, 356, 377`; `UnionSynthesis:343` | `PureModelContext:366` → `ClassSources:699, 1519`; `AssociationJoins:1094` | **yes** |
| `poisons` — association-FQN key | `MappingNormalizer:427` | `AssociationJoins:1095` | **yes** |
| `poisons` — `"Class[setId]"` key | `MappingNormalizer:312, 348` | — | **NO — write-only (HIGH-3)** |
| `mixedUnions` | `UnionSynthesis:392` | `PureModelContext:352` → `ClassSources:685` | yes |
| `unionKeyThreads` | `UnionSynthesis:938` | `PureModelContext:359` → `ImportDataFlow:51`, `CastReRoot:90`, `StackBuilder:912` | yes |
| `nullableCensus` | `MappingLedger:79` ← `RequiredNullableCensus:99,110,117` | `PureModelContext:513` — **accessor has no callers** | **NO (HIGH-2)** |
| `unionMembers` | `MappingFacts:34` | `PureModelContext:336` → `ElementReferences:164` | yes (rule diverges — MED-6/7) |
| `routedTargetClasses` | `MappingFacts:66` | `PureModelContext:343` → `ElementReferences:141` → `StoreResolver:2622` | yes (guarantee overstated — MED-8) |
| `routedTargetSets` | `MappingNormalizer:453` ← `SetDispatch:34` | — | **NO — write-only (HIGH-1)** |
| `resolvedStores` | `MappingNormalizer:453` ← `StoreSubstitutionRewrite:257` | `MetamodelSeeds:172` | yes (empty for clean-sheet — MED-5) |
| `operationMembers` → `ClassBinding.Operation.memberSetIds` | `UnionSynthesis:136, 494` | `MappingNormalizer:336, 400` → `StackBuilder:192`, `ClassSources:706` | yes |
| `declaredKeys` → `ClassBinding.Relational.declared` | `MappingPrePass:90` ← `SetKeyFacts:30` | `ClassSources:626`; `MetamodelSeeds:294, 295, 378, 393-401` | yes |
| `propertyPins` | `MappingNormalizer:328, 393`; `AggregateViewLift:48` | `StackBuilder:1309` | yes |
| `aggregateViews` | `MappingNormalizer:393` ← `AggregateViewLift.facts` | `AggregationAwareRouting:197-198`; `ClassSources:1366` | yes |
| `strictErrors` (ledger, not stamped — correctly) | `MappingNormalizer:346, 369, 426` | `MappingNormalizer:192-193` (`get(0)` only) | yes |

**Q1 answer:** no stamped fact is *recomputed* downstream — no query-time re-derivation of anything
Phase E stamps was found. The rot is the opposite direction: three write-only channels. The one
arguable re-derivation, `ScanRelations:1700-1701` re-reading `Union.memberSetIds()` off the legacy
surface instead of `unionMembers`, is a registered `SURFACE CONTRACT` reach-back in an analysis-only
consumer — but see `08-lineage-rediscovery.md`, which shows that consumer is *not* analysis-only.

---

## What is genuinely good

- **"Never written into a shared index" is mechanically true and mechanically enforced.**
  `ArchitectureTest.normalizerNeverWritesIntoTheModelIndex:1010-1031` bans
  `add|retainLegacySurface|registerMappedClass` calls from `com.legend.normalizer..` **and** bans
  public fields on `ModelBuilder` — so the five deleted write channels cannot return as fields
  either. That second clause is the part most teams forget.
- **Order independence is real, and for structural reasons, not luck.** See MED-10 §Q5.
- **`MappingFacts` is a genuinely faithful reimplementation.** Diffed against
  `bbd0049c5^:PureModelContext.java:304-368`. Both functions preserve the old reader's rules exactly,
  including the all-or-nothing behaviour on an unresolvable member and the intra-set disagreement
  veto. The preserved quirks are honestly labelled *in the code* — which is how bug-compatibility
  should be done.
- **`ClassBinding` is sealed by binding kind with non-null stamps as record components**
  (`MappingDefinition.java:184-211`) and a deliberate no-convenience-constructor policy (`:164-166`:
  *"one silently dropped primaryKeyColumns (the AssocJoin disease) — every site spells every
  component"*). Exactly right — and it is the discipline `NormalizationFacts` and the outer
  `MappingDefinition` ladder abandoned (MED-5).
- **`StoreSubstitutionRewrite`'s recorder trick** (`:209-231`) — collecting database references by
  running the *same* exhaustive rewrite with a recording `Map` — makes drift between the rewriter and
  the collector structurally impossible. A better invariant than a test.
- **`MissProbe`** (the censused `orElse(null)` funnel) is a real instrument: 37 sites censused, 21
  that fire funnelled through `miss()`, 16 that never fired converted to loud `neverFired` throws.
  MED-9 is a defect in *where* one of them landed, not in the instrument.
- **`LegacyReachbackCensusTest`** is a better guard than an ArchUnit rule for this problem —
  occurrence-exact per file, with a coverage floor and a Windows path-separator fix showing it has
  actually been maintained.

---

## Doc/comment claims the code does not honour

| claim | at | reality |
|---|---|---|
| *"the resolver materializes that set's binding"* | `SetDispatch.java:20-33` | no resolver code reads `routedTargetSets` |
| *"the corpus harness AGGREGATES across its models and pins"* | `RequiredNullableCensus.java:45-46` | no caller of `requiredNullableCensus()` exists |
| *"Rows accumulate on … `requiredNullableRows()`"* | `RequiredNullableCensus.java:42` | that method was deleted; an ArchUnit rule keeps it deleted |
| *"The full message rides on the poison and surfaces via StoreResolver's 0-binder error"* | `MappingNormalizer.java:374-375` | true for class keys, false for the bracketed keys at `:312`/`:348` |
| *"the ONE class every route of the property lands on"* | `MappingFacts.java:62-63` | enforced within one set only; first answering set wins |
| *"unresolvable include is its own loud problem elsewhere"* | `MappingClosures.java:193` | dead code — `orElseThrow` on the line above already threw |
| *"A mapping whose pre-pass fails is walled under a tolerant build"* | `MappingPrePass.java:42-46` | a `MissProbe.neverFired` ISE escapes the wall by design |
| *"Used by `ModelBuilder.from()` to cross-bake synthetic ClassMappings from JsonModelConnection"* | `LegacyMappingDefinition.java:74-78` | that cross-bake now lives in `MappingClosures:91-121` |
| *"the LAST found — `rootClassMappingByClass`'s `last()` (R1)"* | `MappingClosures.java:207-210` | states the engine rule correctly — and `MappingFacts:37-38` uses first-wins instead |
