# FIXLIST — mapping-normalizer audit, 2026-09-15

Ordered, actionable. Every row has `file:line` and a stated acceptance test.
Audited at `ae16e5c46`. Grades: **PROVEN** (executed), **VERIFIED** (re-checked in source),
REPORTED (single audit, not re-checked).

**Before starting any code fix, do P0-0** — four instruments are currently blind, including the
only judge of real rows.

---

## P0-0 — Restore the oracle checkouts to the pins (blocks everything else)

`tools/oracle-pins.env` pins `legend-engine` 4.145.0 (`230c159196d6512486fd382556c5e9e4fb128ebb`)
and `legend-pure` 5.99.0 (`7fbc7d6e8d52e2488bdae67280e5f5dfed448d68`), moved 2026-09-11 by
`0a4a928c6`. On disk: **4.137.1-SNAPSHOT** (`943d38b3`) and **5.92.0-3** (`d00cfd5b`).

Blinded by this: `MinimalCorpusTest` (the corpus gate — the only row-level judge),
engine-citation verification, `com.legend.generators.*` parity (5 failing tests in the full
reactor), `DynaFnRegistryTest.registryMatchesTheCheckout`.

**Do:** move both checkouts to the pinned SHAs, then run `tools/allgates.sh` once.
**Acceptance:** `oracle_roots_check` passes; `MinimalCorpusTest.pinCensus` reports
`declared=2761, excluded=148, discovered=2613`.
**Note:** run gates via `tools/allgates.sh`, never a hand `mvn` — the hand path silently reads the
default checkout (documented in `docs/GATES.md`, and it caught this auditor).

---

## P0 — Correctness

### P0-1 — `MappingValidation`'s verdict is dropped when a mapping has a multi-hop association — **PROVEN**

`MappingPrePass.java:67` keys validation failures in an `IdentityHashMap<ClassMapping, String>`.
`ResolvedMapping.withMapping` carries it unchanged across the multi-hop injection — its own javadoc
names that rewrite — but `AssociationSynthesis.java:210-211` **replaces** the `ClassMapping`
objects, so `pp.invalid().get(cm)` at `MappingNormalizer.java:310` and `:354` returns null.

Probe output (module/tolerant build, same invalid mapping twice):

```
==== invalidOnly                   poisons={model::P=...references property not declared...}
                                   binding model::F          <-- model::P correctly withheld
==== invalidPlusMultiHopInjection  poisons={}                <-- poison GONE
                                   binding model::P          <-- invalid set BOUND anyway
```

**Fix:** key `invalid` by a value-stable key — `(classFqn, setId)` — not object identity.
This also fixes P0-2.
**Acceptance:** a test with an invalid PM *plus* a 2-hop AssociationMapping still poisons the class.

### P0-2 — Strict-build error selection is non-deterministic across JVM runs — **VERIFIED**

`MappingValidation.java:53` returns `new IdentityHashMap<>()`; `MappingPrePass.java:65` does
`invalid.values().iterator().next()`. That order derives from `System.identityHashCode`. The
comment two lines above says *"a strict build rejects the first"* — an `IdentityHashMap` has no
first. Before `12e36db45` the code threw inside the loop, in declaration order.

**Fix:** same key change as P0-1, plus iterate in `md.classMappings()` declaration order.
**Acceptance:** a mapping with two invalid class mappings reports the same error on repeated runs.

### P0-3 — Both cycle guards are broken, in mirror-image ways — **VERIFIED**

**M2M guard, `MappingNormalizer.java:1245`** — fires only on *non*-cycles. `m2mPropertyValue` never
recurses (it emits a `NewInstanceCast` and returns), so `cycleStack` can only trip when
`innerFqn.equals(ownClass)`. A legal `Person.manager: Person` is rejected as *"Cycle materializing
M2M class-typed property"*; the genuine `A.b: B, B.a: A` cycle compiles clean. It is a
`ModelException`, so it **fails a strict build outright**.

**Embedded guard, `MappingNormalizer.java:2165`** — can never fire. All three callers pass a fresh
set (`:2125`, `:2212`, `:2248`) and the recursion runs `materializeEmbedded` →
`translatePmToField` (`:2187`) → `materializeEmbedded` (`:2123`, fresh set again). A cyclic
`Inline[a]`→`Inline[b]`→`Inline[a]` model recurses to `StackOverflowError`, which `withElement`
deliberately lets escape unwalled.

**Fix:** thread `cycleStack` through `translatePmToField` into the embedded recursion; delete or
re-aim the M2M guard (self-reference is legal; the real cycle is a chain through `NewInstanceCast`
deferral and needs a different detector).
**Acceptance:** two tests — a self-referential M2M property compiles; a cyclic inline-embedded
model walls loudly. Neither exists today (`grep "Cycle materializing" core/src/test/` → nothing).

### P0-4 — `~distinct` silently returns too many rows — **PROVEN**

`MappingNormalizer.java:1882-1885` states the rule: *"DISTINCT over the MAPPED columns, not the raw
physical row (the table's unmapped PK would defeat the dedup)."* But `collectMappedColumns:1952`
has `default -> return false`, adding no column for `JoinTerminalColumn`, `Join`, `Embedded`,
`InlineEmbedded`, `OtherwiseEmbedded` — and the third branch at `:1916` emits a bare
`distinct(p.expr)` over the full physical row.

Probe (a `~distinct` class whose two properties are both join-terminal reads):

```
distinct( joinSlot( tableReference(db::DB,"PT"), ... ) ) -> map(row | ^model::P(...))
```

`PT.ID` is in that row and unique, so the dedup is a no-op and every row survives.

**Fix:** either collect columns for the missing PM kinds, or wall loudly. Do not leave the
raw-row branch silently reachable.
**Acceptance:** the probe shape asserts the emitted `distinct` carries a `ColSpecArray`, or walls.

### P0-5 — `~groupBy`'s "withhold the property" rule is not implemented — **PROVEN**

`GroupBySynthesis.java:181-188` deliberately emits no ColSpec for a per-row PM outside the key list
(*"WITHHOLD the property, keep the set"*). The terminal loop at `MappingNormalizer.java:1926-1933`
binds every PM anyway; `:2059-2062` returns a bare `$row.<propertyName>` read under `~groupBy`.

On the repo's own fixture (`MappingNormalizerTest.java:3800`), `groupByComputedKeys` projects
`[k, total]` while the constructor binds `extra = trustOne($row.extra)` — a column the grouped
relation does not produce.

**Fix:** skip withheld PMs in the terminal loop (the set of withheld names is already computed in
`GroupBySynthesis`; thread it out).
**Acceptance:** the existing `groupByPerRowFormulaOutsideKeyWithheld` currently asserts only
`poisonReasons(parsed).isEmpty()` — strengthen it to assert `extra` is **absent** from the
constructor. This is the test that let the bug through.

### P0-6 — Per-hop `(INNER)` join types are silently discarded — **VERIFIED**

`JoinChainElement.joinType` is parsed, validated (`MappingProtocolParser:1707,1726`), modelled,
copied through `StoreSubstitutionRewrite:92,107` and `NameResolver:1445,1469`, and re-emitted by
`ProtocolEmitter:357`. **`JoinChainEmission` never reads `hop.joinType()`.** Only the *filter's*
type reaches emission (`MappingNormalizer.java:1768`), and `MappingFromProtocol.java:554-557`
explicitly moves hop 0's type onto the filter and nulls it.

So `@A > (INNER) @B` on hops 1..n emits LEFT semantics, keeping parent rows the engine drops.
No poison, no exception.

**Fix:** honour the per-hop type, or wall loudly on a non-null `joinType` on hops 1..n.
A silent LEFT is the one option that must go.
**Acceptance:** a chained `(INNER)` hop either emits inner semantics or throws.

### P0-7 — Two slot minters share one namespace; only one checks collisions — **VERIFIED**

`JoinChainEmission.java:643-650`: `mintNavSlotAlias` puts its uniqueness loop **inside**
`if (collides)`, so on the non-colliding path it returns `propName` without consulting
`p.aliasToTargetTable`. Its sibling `uniqueSlotName` (`:654-664`) always checks.

A physical hop that already claimed slot `firm` (single-hop `@firm` chain → `String.join("__",
["firm"])` = `"firm"`) makes the class-typed `firm` property take the `continue` at `:344-349` —
no `legacyNavigate` emitted, `p.classSlots.add(...)` at `:453` never runs — and
`MappingNormalizer.java:2088-2090` then reads `navSlotByProp.getOrDefault(propName, propName)`,
binding `$row.firm`, the **physical sub-row**, where a `Firm` instance belongs.

Note the other branch of that ternary calls `slotFor(...)`, which *throws* on a miss. And this is
the failure mode `slotFor`'s own javadoc (`:673-680`) claims was eliminated in audit 18.

**Fix:** hoist the `while (aliasToTargetTable.containsKey(alias))` loop out of the `if (collides)`
branch; replace the `getOrDefault(propName, propName)` reader with a throwing lookup.
**Acceptance:** a mapping with a physical `@firm` chain *and* a class-typed `firm` property emits
two distinct slots.

---

## P1 — Facts that are written and never read

### P1-1 — Read the `class[setId]` poison keys, or stop writing them — **VERIFIED**

Written at `MappingNormalizer.java:312` and `:348` — the **per-set fault-isolation arm**. The sole
reader is `PureModelContext.java:366-369`, `md.facts().poisons().get(classFqn)`, a plain class FQN.
No site in the repo composes the bracketed key to read it. All three call sites
(`ClassSources.java:699`, `:1519`, `AssociationJoins.java:1094-1095`) pass a class or association FQN.

Consequence: a non-root set that fails synthesis records its reason where nothing can address it,
and the user gets a bare *"class X is not mapped in mapping M"* — three lines below a comment
promising *"the full message rides on the poison and surfaces via StoreResolver's 0-binder error."*

Worse for a **union member** set: the class-level poison at `:304` is explicitly skipped
(`if (!unionMember)`), so there is **no readable reason at all**. And for a non-union multi-set
class, the generic *"class is mapped through multiple set IDs; .all() … is a roadmap feature"* is
written **first** via `putIfAbsent`, masking the real cause.

**Fix:** introduce a sealed key and make the miss a compile error:

```java
public sealed interface PoisonKey {
    record ForClass(String classFqn) implements PoisonKey {}
    record ForSet(String classFqn, String setId) implements PoisonKey {}
    record ForAssociation(String associationFqn) implements PoisonKey {}
}
```

Then pick **one** collision policy — `put` (last wins, `:371`), `putIfAbsent` (first wins,
`:304,312,348,427`) and `merge` with `";"` (`UnionSynthesis.java:343`) are all in use on the same map.
**Acceptance:** a test where a non-root set fails synthesis and the *recorded reason* appears in
the user-facing error.

### P1-2 — Delete `SetDispatch` + `routedTargetSets`, or wire them — **VERIFIED**

`routedTargetSets` is produced by `SetDispatch` (99 lines, walks the full include closure of every
mapping on every compile), stamped at `MappingNormalizer.java:453`, and read by **nothing**
(7 grep hits: 5 declaration/copy, 1 construction, 1 definition). Its documented consumer,
`ClassSources.getForNav` (`:90`), takes a `head` parameter, **never uses it**, and delegates to
`get(...)` while its javadoc describes H5 set-id dispatch it does not perform.
`NavMaterializer.java:103-104` repeats the claim.

**Fix:** either implement `getForNav` against the table, or delete `SetDispatch`, the component,
the 7-arg `MappingDefinition` constructor (zero callers), and the misleading javadoc.

### P1-3 — Delete `RequiredNullableCensus` + `nullableCensus`, or build the harness — **VERIFIED**

130-line producer; accessor at `PureModelContext.java:510` has **zero callers** repo-wide. Its
javadoc claims *"Rows accumulate on … `requiredNullableRows()`"* (that channel was deleted; an
ArchUnit rule keeps it deleted) and *"the corpus harness AGGREGATES across its models and pins"*
(no such harness exists).

---

## P2 — One owner per decision

### P2-1 — `SetId.of(...)`, one function, ~14 call sites folded — **VERIFIED**

`ResolvedMapping.idOf:83` calls itself *"The one rule."* The same rule
(`setId != null ? setId : classFqn.replace("::","_")`) is independently implemented at:
`MetamodelSeeds.java:280,321,738`, `ModelBuilder.java:495,524`, `ClassSources.java:432,514`,
`ObjectReferenceDecode.java:156,158`, `GraphEmission.java:3168`, `ScanRelations.java:842`,
`M2mRouteGuards.java:133`, `MappingFromProtocol.java:265`, `MappingProtocolParser.java:2961`.

And `SetKeyFacts.setKey:25` implements a **different** rule — `setId ?: className`, with no
`::`→`_` substitution. They live in disjoint domains today, so nothing breaks; if they ever meet
they disagree.

Sharpest instance: `ClassSources.java:432` inlines the rule **82 lines above** its own helper
`ClassSources.setIdOf:514`.

**Fix:** one `SetId.of(ClassMapping)` / `SetId.of(ClassBinding)`; fold every site; pin with a
source ratchet in the `LegacyReachbackCensusTest` style (`assertEquals(1, occurrencesOf(...))`).
Half a day; retires the most-copied decision in the codebase.

### P2-2 — One "is this set root-or-sole" answer — REPORTED

Six sites, each counting over a **different scope**: `MappingClosures:326` (closure),
`UnionSynthesis:116` (closure), `UnionSynthesis:308` (querying mapping's own sets — see below),
`MappingNormalizer:293` (own mapping only), `M2mRouteGuards:98`, `ModelBuilder:485`.

Concrete divergence at `UnionSynthesis.java:307-310`: `set` is resolved **through the include
closure** (`md.set(...)`), but sole-ness is counted over `md.classMappings()` — the *querying*
mapping's own sets, not the owning one. Include `I` maps class `C` with one unmarked set; `M`
includes `I` and maps nothing for `C`; the count is 0, not 1, so a route to that set classifies
`PINNED_SINGLE` instead of a root route. `ResolvedMapping.roots()` already answers this correctly —
a third root determination in the same file.

**Fix:** one `Root.of(...)` with the counting scope an explicit parameter.

### P2-3 — Five "find the binding in the closure" implementations, four shadowing rules — REPORTED

| impl | rule |
|---|---|
| `ClassSources.findBinding:1348` | own-first; among >1 local: drop agg-views, `root()` wins, rootless ⇒ null; **across includes the LAST wins**; unknown include **throws** |
| `StackBuilder.findBinding:1680` | BFS, **shallowest/FIRST include wins**; no view filter; unknown include silently skipped |
| `RelationalRootForm.primaryKeyColumns:213` | **no include walk at all**; first binding with non-empty PKs |
| `MappingDefinition.classBindingsWithIncludes:326` | own-first DFS; consumed first-match |
| `GraphEmission.definingMapping0:3181` | **includes before own** — and its javadoc says the opposite |

**Load-bearing:** `ClassSources.buildRoutedUnionSource:386-402` uses `StackBuilder.leafSetIds` to
decide which union arms are **dead** and drops them to typed NULLs. For a class bound by two
included mappings, `build()` resolves the last include's binding while `leafSetIds` computes leaves
of the first's — arms silently dropped, wrong rows, no wall.

Also `MappingDefinition.classBindingsWithIncludes`'s javadoc (`:319-324`) claims it matches
`findBinding`; `findBinding`'s own comment (`:1400`) states the opposite rule across includes.

### P2-4 — `nullTolerant` decides INNER vs LEFT+WHERE with `equalsIgnoreCase` string literals — REPORTED

`JoinChainEmission.java:1031-1043` matches `"isNull"`, `"sqlNull"`, `"coalesce"`, `"ifnull"`,
`"nvl"`, `"case"`, `"if"` case-insensitively — five of which are real `DynaFn` members with
canonical spellings, ten lines from the typed registry that exists for exactly this.
A miss produces a silently wrong join shape.

**Fix:** route through `DynaFn.of`. Check whether `"ifnull"`/`"nvl"` are reachable names at all;
if not, those arms are dead.

---

## P3 — Capability gaps that reject legal input

### P3-1 — XStore: we reject shapes already in our own corpus manifest — **VERIFIED**

The engine's XStore model is **direction-scoped**: `XStoreAssociationImplementation` is an *empty*
class (`mapping.pure:174-175`), all semantics per-end in `XStorePropertyMapping.crossExpression`
(`:178-180`), and none of the engine's 8 XStore validations compares end A to end B. Ours requires a
single shared predicate (`MappingNormalizer.java:1032-1042`).

Four asymmetric engine fixtures are in `parser-equivalence/src/test/resources/corpus-manifest.tsv` —
they parse and round-trip, and are never normalized:

- `testModelJoinsToRelationalJoins.pure:399-400` — four *ordering* comparisons with operands flipped
  per side. `canonicalizeEqualOperands` (`:732-757`) swaps only for `equal`/`==` and sorts only
  `and`/`or`, so they canonicalize differently and the wall fires.
- `relationMappingSetup.pure:638-639` — genuinely asymmetric bodies, not inverses.
- `testMappingCrossStore.pure:239-242` — **four** property mappings across two set pairs; we read
  set ids from `propertyMappings2().get(0)` only (`:964-973`) and cannot represent the shape.
- `executionPlanTestSnowflake.pure:493-499` — a **one-ended** XStore; the engine accepts it.

**Also:** the rule is implemented **twice verbatim** — `MappingNormalizer.java:1032-1042` and
`XStorePureEnds.java:221-231`, plus the end-name check (`:1006-1013` / `:184-192`) and the
self-association orientation block (`:1017-1024` / `:196-202`). Neither copy has a test
(`grep -rn "direction-specific" core/src/test/` → nothing).

**Fix:** per-end predicates (the engine's model), set ids per property mapping, and one
implementation.

### P3-2 — Inline-embedded: missing ambiguity wall and subtype check — REPORTED

Engine asserts exactly one match (`mappingExtension.pure:266-272`, *"Found too many or not enough
matches"*); ours takes the first and `break`s (`MappingNormalizer.java:2229-2240`), so two sets
sharing an id across two included mappings resolve silently in closure order.

Engine rejects a referenced set whose class is not a subtype of the property's return type
(`RelationalInstanceSetImplementationValidator.java:130-141`); ours passes the class straight
through (`:2246-2248`).

Also: we scan only `ClassMapping.Relational`, but the engine flattens embedded sets into
`classMappings` (`RelationalCompilerExtension.java:367,520`), so `Inline[myBondMapping_issuer]`
resolves there and reports *"references unknown setId"* here.
Live engine test: `testInlineEmbeddedMappingWithAssociationFromRootMapping`.

**Receipt found** (T15 was listed as uncited): `helperFunctions.pure:432` +
`functions_Mapping.pure:66-72` (`_classMappingByIdRecursive` walks `$_this.includes` transitively).
The rule is engine-correct; cite these, not a corpus test.

### P3-3 — Relational association path has no direction-agreement wall — REPORTED

`AssociationSynthesis.java:428-432,449-456` takes the first property mapping, and the comment
admits the residual: *"two directions with genuinely NON-equivalent joins would still take the
first silently."* The identical hazard is a hard `NotImplementedException` on the XStore route.

Three `return null` paths (`:446`, `:462`, `:471`) withhold the binding **with no poison**, so the
query-side `.or()` reason chain finds nothing and the user gets a reasonless wall.

### P3-4 — `toString` dyna is a silent wrong translation — REPORTED

`TO_STRING` is `Resolution.PURE` with no translator arm, so it passes through to Pure's `toString`
(ISO form) where the engine renders `cast(%s as varchar)` — the database's format. No error, no
ledger row, different rows.

The codebase already knows: `RelOpTranslator.java:407-411` (the `concat` arm) says *"the DATABASE's
own formatting: '2014-01-01 06:30:00', not pure toString's ISO form — audit"* and works around it
there. The workaround exists in one arm and is missing where the name is spelled directly.

### P3-5 — `extractFromSemiStructured` rejects two legal engine types, mis-kinds `DECIMAL` — REPORTED

Engine's list (`dbExtension.pure:904-905`) includes `STRING` and `DATETIME`; ours
(`RelOpTranslator.java:194-207`) does not → `ModelException` on legal input. And `DECIMAL` maps to
`"Float"`, contradicting our own `RelationalKinds.java:33-34` (`Decimal`/`Numeric` → `"Decimal"`).

---

## P4 — SQL

### P4-1 — Union double-materialization — **measured 2.0×**

When an `exists` filter and an aggregate both read a union, the subtree is materialized twice:
4 table scans and 2 hash joins vs 2 and 1; **2.26 ms vs 1.12 ms** (best of 7) on 2,000 firms ×
200,000 people. Two aggregates already share one union correctly — the duplication is specific to
the `exists` filter building its own `DISTINCT` copy instead of reusing the grouped relation.

Also visible in that emission: a dead `WHERE TRUE`, and a `LEFT OUTER … WHERE … IS NOT NULL` pair
that is an `INNER JOIN` spelled long.

**Fix:** a CSE/CTE pass. None exists — `SqlWith` is only ever built by `SqlPostProcessors.extractCtes`,
an opt-in parity post-processor.

### P4-2 — `~groupBy` wraps and projects dead columns — the one parity regression

Ours emits 2 SELECTs with `acct`/`prod` projected and never read; the engine's golden for the
identical shape (`testGroupBy.pure:74-79`) is a single flat `SELECT … GROUP BY`.
Root cause: `SubselectPrune.java:33-36` refuses to prune grouped selects by rule — correct for
`DISTINCT`, unnecessary for `GROUP BY`. The flat form is reachable (a keys-only `~groupBy` emits it).
Zero measured cost on DuckDB; a text and parity defect.

### P4-3 — `AssociationMapping` + `~filter` isolates the root into `SELECT *` — REPORTED

Clean A/B, same model and rows: the direct property-mapping spelling emits one flat SELECT; the
`AssociationMapping` spelling emits two and a star. Witness:
`AssociationIntegrationTest.testAssociationWithMappingFilter` (`:660-708`).

### P4-4 — `SELECT *` frames (11 of 344) and per-hop chain nesting

`Lowerer.java:533` lowers a bare `TypedTableReference` to `SqlSelect.starOf(...)` and
`SubselectPrune.java:369` skips star projections by design. DuckDB prunes anyway; the cost is
portability. An N-hop **class** navigation yields N−1 nesting levels, pinning a right-deep join
tree — column-mapping chains do **not** suffer this.

**Structural root of P4-2..4:** `SubselectPrune` is the *only* post-lowering optimizer and it
prunes columns without ever collapsing a wrapper or touching a star. A conservative select-merge
pass would close most of the nesting census in one place.

---

## P5 — Guards, measures, hygiene

### P5-1 — Restore the `declaredPlatformKind` census row (AGENTS.md violation)

`6048acec2` **deleted** two `ShadowWalkerCensusTest.REGISTER` rows rather than ratcheting them to
zero. The inline justification covers `pureKindOf` only. `declaredPlatformKind` is still defined at
`DeclaredCoercions.java:74` and called at `:54, :102, :137`, with `RelationalKinds.pureKindOf` at
`:115, :145` — six live call sites inside the scanned directory, now unpinned.

### P5-2 — Ratchet `CodeShapeGuardrailTest`'s file ceiling down

`:47` pins `MappingNormalizer.java` at **3510**; the file is **2910**. Header says "SHRINK only";
nothing enforces shrink. 600 lines of slack on the file the guard exists to watch.

### P5-3 — Strengthen the order-independence test

`MappedInClosureTest.java:100-118` compares `classBindings().map(cb -> cb.classFqn()).sorted()` —
discarding binding order, set ids, root flags, function FQNs — and never compares the synthesized
function bodies. It swaps two mappings that share no classes; the only mapping with an include is
excluded from the order test.

**The claim it guards is TRUE** for structural reasons the test does not check (see
`02-stamped-facts.md` §Q5). Strengthen it to compare unsorted bindings **and** lifted bodies, and
add a permutation over the element list.

### P5-4 — Add tests for the three untested load-bearing rules

- **`~groupBy` stage 2** (`MappingNormalizer.java:1848-1870`) — delete the block today and
  `mvn -pl core test` stays green. Every `~groupBy` test is a flat single table with no `Join` PM;
  the near-miss uses a `JoinTerminalColumn`, which the stage-2 loop skips at `:1855`.
- **`(INNER)` `~filter` row explosion** (`JoinChainEmission.java:932-1020`) — the only `(INNER)`
  filter in tests is a grammar round-trip that never reaches the normalizer. Both `nullTolerant`
  decline branches (`:970`, `:1046`) unasserted.
- **`unionKeyThreads` ordinal naming** — zero assertions; the name appears in test sources only
  inside a comment.

Both of the first two are ~40-line fixtures in the existing style.

### P5-5 — Restore the three weakened view tests

`7da6acaa8` converted `MappingNormalizerTest.java:4364-4373`, `:4409-4418`, `:4453-4459` from exact
parent/child nesting to a `spineIndex(...)` forward search, and dropped
`assertEquals("select", select.function(), "~distinct dedups the MAPPED columns: select narrows first")`
entirely. The searching form accepts any number of unasserted operations between pinned points.
The `AGE` → `page` change in the same hunk is a legitimate behaviour change; the loosening is not.

### P5-6 — Give AGENTS.md invariant 4 ("NO FALLBACKS") a mechanical form

Currently `[CONVENTION]` — nothing checks it, and three live fallbacks are documented in comments
(`AssociationSynthesis:389`, `StoreSubstitutionRewrite:240,371`, `MappingNormalizer:928`).
Proposal: a `FallbackLedgerTest` in the shape of `JavaEvalLedgerTest` — a shrink-only register with
a justification per row. Likewise nothing ratchets `MissProbe`: the census lives in a javadoc, so
the zero can regrow silently.

### P5-7 — Add `GuardCoverage.assertFloor` to `ShadowWalkerCensusTest`

It hardcodes `Path.of("src/main/java/com/legend/normalizer")` and never asserts how many files it
found — the exact scope rot `GuardCoverage` was written for. One line.

---

## P6 — Delete (~550 production lines)

Verified zero consumers across `core`, `pct`, `nlq`, `parser-equivalence`:

| thing | lines |
|---|---|
| `SetDispatch` + `routedTargetSets` + the 7-arg `MappingDefinition` ctor + the stamping call | ~120 |
| `RequiredNullableCensus` + `nullableCensus` + `ModelContext.requiredNullableCensus()` + `PureModelContext:510` + the two `DeclaredCoercions` hooks | ~180 |
| `DynaFnArms` — `public` "so the registry's test can hold the declarations"; **no such test exists** | 56 |
| `NormalizedModel.liftedByOwner()` — zero production readers; documented for "the incremental-invalidation layer", which does not exist | 20 |

**Dead members:** `MappingDefinition`'s 7-arg ctor (0 callers); `NormalizationFacts`' 4-arg ctor
(0 callers); `MappingLedger.facts(surface, md, model)` — `md` and `model` unused; five dead
package-private statics the `deadPrivateMethodsOnlyShrink` guard cannot see (it scans `private`
only): `GroupBySynthesis.isGroupByStep:228`, `JoinChainEmission.classTypedButUnmapped:738`,
`MappingNormalizer.nullOfDeclaredType:1360`, `MappingNormalizer.nullOfPhysicalKind:1335`,
`ViewRelation.relationExpr:527`. Two dead `Pipeline` members: `backingView` (null at every
construction — `:1413`, `:1699`, `JoinChainEmission:947`, `Pipeline:90`) and `ownerSet` (write-only).

**Dead branches:** `MappingNormalizer.java:2339-2345` (unreachable — `synthTableBackedParts:1767`
intercepts first; the comment says the emission "was not built" and it **was**);
`MappingClosures.java:191-194`, `:265`, `:361`, `:407` (`if (x == null)` after `orElseThrow`);
`ModelJoinNesting.java:76-78`; `AssociationSynthesis.java:110-111`, `:647-650`;
`ImplicitInheritance.java:131-134`; `RelOpTranslator.java:415`.

**Unused imports:** `UnionSynthesis` 34/64, `AssociationSynthesis` 29/60, `JoinChainEmission` 27/59,
`MappingNormalizer` 11/68, `RelOpTranslator` 2. *(Evidence the "Doors split" was a block move.)*

**Orphaned / mis-attached javadoc (12 sites in `MappingNormalizer` + others):** `:407-410`
(truncated mid-clause by `b0e01458b` deleting `resynthesizeIncluded`, with a stray 9-space indent),
`:553-558`, `:693-699`, `:702-708`, `:1324`, `:1327`, `:1379`, `:1665` (**broken
`{@link #inferViewMainTable}`** — the method lives in `ViewRelation`), `:1702-1711` (orphan block,
no member under it), `:1985-1988`, `:2765-2770`, `:2772-2779`, `:2794-2813` (20-line javadoc for
`buildNewInstanceToOne` sitting on `simpleTypeName`); `ModelNormalizer.java:201-208`;
`JoinChainEmission.java:477-490`, `:598-606`, `:607-613`, `:1081-1084`;
`AssociationSynthesis.java:343-352`. Plus five empty section banners
(`MappingNormalizer:2036`, `:2040`, `:2044`-adjacent, `:2350`, `:2354`).

**Stale docs to correct, not delete:** `RequiredNullableCensus`'s javadoc names a deleted channel;
`PureModelContext:104` says "F+ compilation never reads" the legacy surfaces while `:421` and `:461`
do (existence probes); `LegacyMappingDefinition.java:74-78` claims `ModelBuilder.from()`
cross-bakes JSON identity sets (it moved to `MappingClosures:91-121`);
`MappingClosures`' class javadoc says "the FIRST include found wins" while `walkOps:232-244` is
last-wins; `synthViewBackedMapping`'s javadoc carries a 4-step description of the **deleted**
flattening algorithm.

---

## P7 — Design (bigger, do after P0–P2)

1. **Re-cut `MappingNormalizer` at joints, not at 3,500 lines.** In value order: `ClassBindingBuilder`
   (kills the 19-line ternary duplicated at `:322-340` and `:386-406`), `MainTable` (9 methods, one
   noun), `RowProjection` (the `^Target(...)` terminus), `PureSpecBuilder` (the 130-line
   "Low-level helpers"), `BuildMode` (replacing the `wallSink == null` sentinel).
   **Acceptance test for a real extraction:** the new file's import block is not the parent's, and
   the calls do not go both ways.
2. **Reshape `NormalizationFacts`** — typed key, group by subject (`mixedUnions` + `unionKeyThreads`
   + `unionMembers` are three maps keyed by the same class FQN describing one union), push
   per-binding facts onto the binding, and delete the convenience constructors that let dead fields
   survive.
3. **Make Phase D total for mapping bodies, then delete Phase E's four resolvers** —
   `AssociationSynthesis.resolveAssociation:374` (wildcard **first-wins** where D *throws* on
   ambiguity), `StoreSubstitutionRewrite.qualifyStoreRefs:358`, the `SignatureMangle` fallback at
   `MappingNormalizer:926-931`, the `findDatabase` leniency. Add the ArchUnit rule banning
   `normalizer → compiler.spec | resolver | lowering | exec | sql`.
   The hard part is the admitted downstream keying on *unresolved* spellings — that is the real debt.
4. **Extract a read-only `Knowledge` interface** and hand Phase E only that. 81 of ~140 reads
   already go through `knowledge()`. This makes `normalizerNeverWritesIntoTheModelIndex`
   unnecessary by construction and closes the `derived()` back door at `MappingClosures:59`.
5. **Give `ScanRelations` the stamped facts it needs.** Eight it re-derives are already on the
   binding. The genuinely missing ones — **per-set property-mapping shape**, the **ordered
   join-name list**, the **class-mapping `~filter`** — are the real gap in the stamped-facts
   design. Note `MappingClosures` is package-private with zero public members, so "just call it"
   is not available.

---

## Proposed new guards (from the architecture review)

| # | invariant | status | proposed rule |
|---|---|---|---|
| U1 | resolver never depends on the legacy surface **type** | true, guarded only by name-census | ban `LegacyMappingDefinition`, `ClassMapping`, `PropertyMapping`, `AssociationMapping`, `RelationalOperation` from `resolver`/`lowering`/`exec`/`compiler.spec` |
| U2 | no phase ≥ F branches on synthesis provenance | **VIOLATED** — `AssociationJoins.java:1236` gates β-inlining on `SynthHat.PROP` | ban `SynthHat` / `FunctionDefinition.Synthesized` from those packages |
| U3 | `NormalizedModel` carries no legacy surface | **VIOLATED** — the `legacySurfaces` field; the §7.4 test checks only `elements` | extend the test to the whole record graph |
| U4 | normalizer does not reach into later phases | **VIOLATED** — `MappingNormalizer:930`, `:2860` | ban `normalizer → compiler.spec..` etc. |
| U5 | normalizer writes nothing into the index | guarded by a **method-name list**; `MappingClosures:59` installs mutable state via `knowledge().derived(...)` | structural fix — the `Knowledge` interface (P7-4) |
| U6 | set-id spelling has one owner | **FALSE** (~14 copies) | source ratchet on `replace("::", "_")` occurrences |
| U8 | dyna identity has one owner | **FALSE** (`JoinChainEmission:1031`) | source ratchet on string literals matching `DynaFn.values()` outside `builtin/` |
| U9 | AGENTS.md invariant 4 (NO FALLBACKS) | `[CONVENTION]`, nothing checks it | `FallbackLedgerTest` (P5-6) |
| U11 | `ShadowWalkerCensusTest` walks the right directory | no coverage floor | `GuardCoverage.assertFloor` (P5-7) |
