# 07 — Downstream consumers (the resolver)

**Scope read end to end:** `ClassSources.java` (1,533), `StoreResolver.java` (3,493),
`AggregationAwareRouting.java` (335), `JsonSourceFrame.java` (281), `Anchors.java` (403),
`model/MappingDefinition.java`.

**Established before this audit began** (do not redo):

- The `com.legend.resolver` package (35,517 lines) contains **zero** code references to
  `LegacyMappingDefinition` or `PropertyMapping` — the only 6 grep hits are comments.
- `ModelContext.findLegacyMapping` is called only by `lineage/ScanRelations`, `MetamodelSeeds`,
  `normalizer/MappingClosures`, and existence checks in `PureModelContext`.
- ArchUnit `postNormalizationPhasesAreParserFree` (`:252`) and `resolverNeverSeesTheUntypedAst`
  (`:266`) forbid the resolver depending on `com.legend.normalizer..` and `com.legend.protocol..`;
  `LegacyMappingDefinition` lives in `com.legend.model`, which the resolver *is* allowed to use.
  (See `02-stamped-facts.md` Q8 — the invariant *is* guarded, by `LegacyReachbackCensusTest`.)

---

## Headline

The resolver genuinely never touches the legacy parse surface — but **it does not read the compiled
artifact's facts either**. Phase E's marquee "stamped fact" for the resolver, `routedTargetSets`, is
written by the normalizer and read by nobody, while two resolver comments claim it is the basis of
navigation dispatch. Alongside it, the mapping-closure walk is reimplemented five times with four
different shadowing rules.

---

## HIGH-1 — The H5 set-id dispatch table is stamped, documented as consumed, and dead

`ClassSources.java:85-95`:

```java
/** Navigate-target resolution with the H5 SET-ID DISPATCH hint baked
 * in: the head's sole routed set (mapping-closure table) selects the
 * set-discriminated binding when present; class-level serves otherwise ... */
ClassSource getForNav(String mappingFqn, String classFqn, String head,
        @com.legend.Nullable String scope) {
    // an un-routed navigation lands on the class ...
    return get(mappingFqn, classFqn, scope);
}
```

`head` is **never used**. No `setId` is ever passed. `NavMaterializer.java:103-104` repeats the claim
(*"a route naming a specific set of a (possibly rootless) multi-set target resolves through the
set-discriminated binding (`ClassSources.getForNav`)"*).

The table itself: `normalizer/SetDispatch.java:34` computes it, `MappingNormalizer.java:453` stamps
it into `MappingDefinition.routedTargetSets` — and `routedTargetSets()` has **zero** readers in
`core/src/main` or `core/src/test`.

**Why it matters:** this is the one place the claim "downstream consumes stamped facts" is asserted
in a comment and is **false in the code**; a stamped compile-time fact is carried through the
artifact for nothing, and the behaviour it describes is unimplemented.

---

## HIGH-2 — `NormalizationFacts` accessors don't close over includes, but `dispatch()` hands them includer mappings

`PureModelContext.java:335,343,352,358,367` are all
`findMapping(mappingFqn).map(md -> md.facts()....get(key))` — a **single-mapping** lookup. But
`ClassSources.dispatch` picks the mapping via `binds()`/`bindsIn` (`:1247`), which **does** walk the
closure. So a class whose union/poison/routed-target fact was recorded on an *included* mapping
resolves under the includer, and the fact comes back null.

Concretely `ClassSources.java:683`:

```java
List<String> mixed = setId == null ? ctx.mixedUnionMembers(mappingFqn, classFqn) : null;
if (mixed != null) { return mixedUnionSource(...); }
...
throw new MappingResolutionException("class '" + classFqn + "' is not mapped ...")
```

**A mixed-kind union declared in an included mapping falls straight through to "class is not
mapped."** The invariant "Phase E stamps, the resolver reads" is broken at the *fact-lookup* layer,
not the resolver layer — and the failure mode is a wrong wall, not a wrong answer, so it looks like
an unsupported feature.

The same asymmetry exists *within* one fact: `StackBuilder.keyThreads` (`:902-922`) walks the include
closure for `unionKeyThreads`; `CastReRoot.java:90` and `compiler/spec/ImportDataFlow.java:51` read it
directly. **Same fact, two disciplines.**

---

## HIGH-3 — Five "find the binding for C in the closure" implementations, four different shadowing rules

| impl | rule |
|---|---|
| `ClassSources.findBinding` `:1348` | own-first; among >1 local: drop agg-views, then `root()` wins, rootless ⇒ null; **across includes the LAST wins**; unknown include **throws** |
| `StackBuilder.findBinding` `:1680` | BFS, `cb.root() \|\| cb.setId()==null`, **shallowest/FIRST include wins**; no view filter; unknown include silently skipped |
| `RelationalRootForm.primaryKeyColumns` `:213-223` | **no include walk at all**; first binding with non-empty PKs; root-ness and setId ignored |
| `MappingDefinition.classBindingsWithIncludes` `:326` | own-first DFS list; consumed first-match by `AggregationAwareRouting.viewsOf/mainSetId` `:175,194` |
| `GraphEmission.definingMapping0` `:3181` | **includes before own** (see HIGH-4) |

**The `ClassSources` vs `StackBuilder` divergence is load-bearing.**
`stacks.leafSetIds(mapping, classFqn)` (`StackBuilder.java:1713`) calls `StackBuilder.findBinding`,
and `ClassSources.buildRoutedUnionSource:386-402` uses its answer to decide which routes are **DEAD**
and get dropped to typed NULLs:

```java
java.util.Set<String> rootLeaves = pinned.size() > 1 ? stacks.leafSetIds(mapping, classFqn) : null;
...
if (rootLeaves != null && sid != null && !rootLeaves.contains(sid)) { ... continue; }   // arm dropped
```

For a class bound by two included mappings, `build()` resolves the last include's binding while
`leafSetIds` computes leaves of the first one's — **arms silently dropped, wrong rows, no wall.**
This is the "one owner per decision" violation with a real wrong-answer path attached.

Also: `MappingDefinition.classBindingsWithIncludes`'s javadoc claims it matches `findBinding` —
*"OWN-FIRST … matching `ClassSources.findBinding`"* (`:319-324`). `findBinding`'s own comment says
the opposite rule applies across includes (*"the LATER include beats the earlier"*, `:1400`).

---

## HIGH-4 — `GraphEmission.definingMapping0`: javadoc says own-wins, code searches includes first

`GraphEmission.java:3172-3196`:

```java
/** ... own declarations win, else the first include that defines it. */
private static @Nullable String definingMapping0(ModelContext mc, String mappingFqn, String classFqn) {
    var m = mc.findMapping(mappingFqn).orElse(null);
    if (m == null) return null;
    for (var inc : m.includes()) {                       // <-- includes FIRST
        String r = definingMapping0(mc, inc.mappingPath(), classFqn);
        if (r != null) return r;
    }
    return m.classBindings().stream().anyMatch(cb -> cb.classFqn().equals(classFqn))
            ? mappingFqn : null;                          // <-- own LAST
}
```

A class bound both locally and in an include yields the *include's* name. It feeds `asorPrefix`
(`:3167`), i.e. the **user-visible objectReference protocol string**. No cycle guard either (every
sibling walker carries a `seen` set) — latent only because `StoreSubstitutionRewrite.resolveAllStores`
rejects include cycles at Phase E.

Same method, `:3168`: `String setId = cs.classFqn().replace("::", "_");` — the binding's own
`setId()` is discarded, so a named set's ASOR prefix carries the class-derived id.

---

## HIGH-5 — Poison surfacing: the `class[setId]` ledger is write-only, and the main wall doesn't walk includes

Two poison-key shapes are written: bare `className` (`MappingNormalizer.java:304,356,377,427`) and
`className + "[" + setId + "]"` (`:312,348`). Every read goes through one accessor
(`PureModelContext.java:366`), a plain-FQN `get`. All three resolver call sites
(`ClassSources.java:699`, `:1519`, `AssociationJoins.java:1094`) pass a bare class FQN or an
association FQN. **No caller ever composes the `[setId]` key.**

Worse, the two poison sites in the *same file* disagree on closure discipline. The dispatch-time one
walks includes with its own queue (`ClassSources.java:1512-1525`); the build-time one — **the actual
"class is not mapped" wall** — does not (`:697-700`):

```java
throw new MappingResolutionException("class '" + classFqn + "' is not mapped in mapping '" + mappingFqn + "'"
        + ctx.mappingPoison(mappingFqn, classFqn).map(r -> " (" + r + ")").orElse(""), classFqn);
```

**Answer to the question asked: yes, there are paths where a binding is missing and no reason is
surfaced.** Two of them: (a) the poison lives in an included mapping; (b) the poison is set-keyed.
Both produce a bare `class 'X' is not mapped in mapping 'M'` with nothing else — the exact silent
wall the ledger exists to prevent. The `AssociationJoins:1094-1097` site has the same include gap but
at least tries two keys, which is the best-quality message of the three.

---

## MED-6 — `resolvedStores` is stamped "never re-walked" and the resolver re-walks it — incorrectly

`MappingDefinition.java:144-148` documents `resolvedStores` as the engine's `Mapping.resolveStore`,
*"STAMPED at Phase E … in include order — never re-walked"*. Only `MetamodelSeeds.java:172` reads it.
Meanwhile `ClassSources.java:1282-1311` hand-rolls the same question:

```java
for (MappingInclude.StoreSubstitution sub : inc.substitutions()) {
    if ((sub.originalStore().equals(ra.store()) && sub.replacementStore().equals(rb.store()))
            || (sub.originalStore().equals(rb.store()) && sub.replacementStore().equals(ra.store()))) {
        return true;
```

This matches only a **single direct pair**, in either direction. A composed chain (inner include
`A->B`, outer include `B->C`) is missed, whereas `resolvedStores` composes it by construction. The
correct implementation is
`resolvedStores.getOrDefault(ra.store(), ra.store()).equals(resolvedStores.getOrDefault(rb.store(), rb.store()))`.

**Why it matters:** the miss silently skips the same-root-table subtype transplant at
`ClassSources.java:802-804`, so `subType(@F).prop` reads go un-synthesized (loud downstream) for a
legitimate model.

---

## MED-7 — `jsonSources` is resolver-wide mutable state, set on `from()` entry and never restored

`ClassSources.java:72-76` declares it; the only writer is `JsonSourceFrame.java:245-248`. **Nothing
clears it on scope exit.** The `Context` record *also* carries `jsonSources`
(`StoreResolver.java:263`), but `ClassSources.build:692` reads the mutable field, not the Context —
so the Context copy is only used to rebuild `ExecutionContext`s in `SubQueryLift.java:200,244`, and it
holds the **un-substituted** URLs while the field holds substituted ones.

**Answer to Q6:** `jsonSources` is not a stamped fact — it is execution-context data, so its
*presence* is legitimate; its *scoping* is not. After any `from()` carrying a JSON connection, a
later unmapped class in an outer/sibling scope whose FQN happens to be in that map silently resolves
to a JSON frame instead of walling. The class javadoc acknowledges only the memo-key collision, not
the leak.

---

## MED-8 — `mixedArmOrder` re-derives a compile-time fact at query time

`ClassSources.java:540-560`. `memberSetIds` comes from the stamped `facts().mixedUnions()`; the
resolver then re-sorts them by looking each binding up and testing `instanceof ClassBinding.Pure`.
The comment says the order *"pins"* golden output. **Query-independent → Phase E should stamp the
ordered list.** Note this loop uses a **fourth** binding lookup
(`findBinding(mdef, classFqn, memberId, …)`) per member.

---

## MED-9 — `RelationalRootForm.primaryKeyColumns` rediscovers three stamped facts on the hot path

`RelationalRootForm.java:213-247`, called from `StoreResolver.java:3452` for **every substitution**.
It (a) searches `mapping.classBindings()` with **no include walk**, (b) ignores `setId` and `root()`,
taking the first non-empty-PK binding, and (c) on miss walks the pipeline for a
`TypedTableReference` and re-reads the table's DDL PK flags — while `ClassBinding.Relational.source()`
already carries `(database, table)` and `primaryKeyColumns()` is already the extends-resolved key
(`MappingDefinition.java:177-183`).

**Contrast the correct pattern** at `StackBuilder.java:940-958`, which reads the binding fact first
and uses `rootTableOf` only as a fallback. `ClassSources.sameRootTable:1314` / `rootTableOf:1322` do
the pipeline walk with **no** fact consulted at all.

---

## MED-10 — Prefix→alias reverse parsing breaks on collision-renamed prefixes

`StoreResolver.java:1195-1198`:

```java
// the hop's step by its slot alias (the prefix is the alias plus "_")
String preAlias = pre.prefix().endsWith("_")
        ? pre.prefix().substring(0, pre.prefix().length() - 1) : pre.prefix();
ClassSource t = sources.navTarget(src, pre.targetClassFqn(), ClassSources.stepOf(src, preAlias), preAlias);
```

`AssociationJoins.prefixFor:596-607` produces `base + "_" + ordinal + "_"` on collision, so a renamed
prefix `firm_2_` reverse-parses to alias `firm_2`, which names no nav step. `stepOf` returns null,
`navTarget` silently falls to the route-less `getForNav` path, and `t.setId()` becomes the
class-level binding's — **a different set than the route pinned.**

---

## MED-11 — Four implementations of "a binding's set id", three answers

- `ClassSources.setIdOf:513` — `cb.setId() != null ? cb.setId() : cb.classFqn().replace("::","_")`
- `ClassSources.routeSetId:431-432` — the identical expression **inlined instead of calling `setIdOf`**
- `ObjectReferenceDecode.java:156` — adds `&& !cb.setId().isEmpty()`, so `setId==""` maps to the
  class-derived id here and to `""` in `setIdOf`
- `GraphEmission.java:3168` — class-derived only, `setId()` discarded

**On string-composed identity (Q4):** the `className + "[" + setId + "]"` composition survives only
in *error messages* (`StackBuilder.java:1213`, `GraphEmission.java:1929`) and in the **write-only**
poison keys — nothing parses it back in the resolver. What *is* parsed back is (a) the prefix→alias
reverse above, and (b) dotted chain keys: **30 `String.join(".", path)` composition sites vs 10
`lastIndexOf('.')` decomposition sites** (`StoreResolver.java:1725,1740,2278`,
`OccurrenceBundling.java:51`, `TemporalFrame.java:2248,2302`, `NavMaterializer.java:110`).

A typed `SetRef(classFqn, setId)` record is warranted — it would collapse MED-11 and is a
precondition for fixing HIGH-1 (the dispatch table is keyed by set id). A typed chain-key record
would be the larger win and is a separate job.

---

## MED-12 — Same closure, three unknown-include policies

`findBinding:1393` and `bindsIn:1258` `orElseThrow` on an unresolvable include (*"a silently-unresolved
include hid class bindings"*); `findBindingBySetId:500-503` and `findBindingByFunction:528-531` do
`.orElse(null); if (included == null) continue;`. A typo'd include path therefore walls loudly through
one door and produces *"route target … is not a set's function"* (`:471`) through the other.

---

## LOW

- **Dead code:** `ClassSources.stackLeaves:173` — zero callers repo-wide. Unused local `var optional`
  at `:266`.
- **`MappingInclude.mappingPath`** is documented "fully-qualified path" (`MappingInclude.java:17`),
  yet `MappingDefinition.collectIncludedBindings:342-347` implements a bare-name/package fallback and
  `ClassSources.findBinding:1393` throws on the same input. One of the two is wrong.
- **Over-long methods:** `StoreResolver.anchoredNode` ~250 (`:378-628`), `registerAssociationJoins`
  ~260 (`:2285-2545`), `resolveObject` ~250 (`:2868-3120`), `flattenNavSlot` ~245 (`:785-1030`),
  `collectOpChain` ~230 (`:2560-2790`), `foldAssociationJoins` ~190 (`:1974-2160`);
  `ClassSources.build` ~225 (`:663-888`), `composeModelToModel` ~117 (`:905-1022`).
- **Roadmap punts in scope are all *loud*** (`NotImplementedException` naming the construct + a ledger
  tag: H5b/H5c/#69/B3) — the right shape. The one soft spot is `StoreResolver.java:2225` — *"B3
  DEFERRED: a separate scalar pipeline regressed real value-leaf reads … stays data-dependent-loud,
  plumbing (`scalarPipeline`) in place"* — dormant plumbing kept alive for a reverted approach.

---

## Decision ownership table

| decision | owner(s) | duplicated? | legit query-time? |
|---|---|---|---|
| binding for class C (class-level, root rule) | `ClassSources:1348`, `StackBuilder:1680`, `RelationalRootForm:213`, `MappingDefinition:326`, `GraphEmission:3181` | **YES — 5 impls, 4 shadowing rules** | no — compile-time |
| binding for set id S / function F | `ClassSources:488`, `:516` | 1 each, but unknown-include policy differs | no |
| "does M bind C" | `ClassSources.bindsIn:1247` | 6th walk of the same shape | no |
| binding's set id | `ClassSources:513`, `:431`, `ObjectReferenceDecode:156`, `GraphEmission:3168` | **YES — 4 impls, 3 answers** | no |
| include-closure walk | 12 sites in `com.legend.resolver` | **YES** | no |
| store-substitution equivalence | `ClassSources:1282` vs stamped `resolvedStores` | **YES — stamp unread, walk is wrong** | no |
| set's main table | stamped `RelationalSource.Table` vs `ClassSources.rootTableOf:1322`, `RelationalRootForm:226` | **YES** (`StackBuilder:952` does it right) | no |
| set's primary keys | stamped `ClassBinding.primaryKeyColumns` vs `RelationalRootForm:238` (store DDL) | partial | no |
| mixed-union arm order | `ClassSources.mixedArmOrder:544` | rediscovery | no — pure function of the mapping |
| routed-set dispatch (`prop -> setId`) | stamped `routedTargetSets` — **no reader** | dead | n/a |
| poison reason | `ClassSources:699` (no closure), `:1519` (closure), `AssociationJoins:1094` (no closure) | **YES — 2 disciplines** | no |
| union key threads | `StackBuilder:902` (closure) vs `CastReRoot:90`, `ImportDataFlow:51` (direct) | **YES** | no |
| defining mapping (ASOR) | `GraphEmission:3181` | own impl, inverted rule | no |
| aggregation-aware view choice | `AggregationAwareRouting.chooseSet:86` | no | **yes** — query-dependent |
| routed-union arm construction | `ClassSources.buildRoutedUnionSource:360` | no | **yes** — routes are the navigator's |
| slot/nav/assoc demand + materialization | `StoreResolver` phases | no | **yes** |
| temporal context per hop | `TemporalFrame` | no | **yes** |
| M2M β-substitution | `ClassSources.substituteSourceReads:1101` | no | **yes** |

---

## What is genuinely good

- **The parse-surface separation is real.** No `LegacyMappingDefinition` / `PropertyMapping`
  reference and no pipeline-body re-parsing to recover mapping structure. Operation arms come from
  the binding fact (`ClassSources.java:707-712`: *"the binding's FACT names them … nothing reads the
  body"*), and the code matches the comment.
- **`AggregationAwareRouting` is the model citizen.** It reads `AggregateViewFacts` verbatim off the
  compiled binding, does a genuinely query-dependent match, and its `path`/`canRewrite` walks have
  loud `default ->` arms (`:247`, `:330`) rather than plausible fallbacks. `StoreResolver.java:2765-2771`
  uses its answer as a `setId` into the ordinary binding lookup — no special-casing downstream.
- **Loud-over-silent is consistently enforced, including against itself.** The correlated-predicate
  backstop at `StoreResolver.java:1841-1852` throws `IllegalStateException` when the augment walk
  misses a step, *because "proceeding would silently drop the correlation"*. The capture guard at
  `ClassSources.java:1202-1210`, the cycle guard at `:231-235`, and the resolver-bug asserts at
  `:181`, `:274`, `:743`, `:753` are all in this spirit. `StoreEscapees.check` as a post-condition
  (`StoreResolver.java:216-221`) is a real invariant, not a formality.
- **Per-key deferral in M2M composition** (`ClassSources.java:988-993`) is the right shape: the wall
  throws at *read* time via `ClassSource.throwIfDeferred`, so a query that never demands the broken
  property composes cleanly — with the regression it fixed named in the comment.
- **`Anchors` is a successful de-duplication** of exactly the class of defect this audit found
  elsewhere — its own javadoc records that two same-named copies with *"silently different descent
  rules"* previously lived in `StoreResolver` and `ClassSources` (`Anchors.java:20-23`). **The five
  binding-lookup copies are the same disease at an earlier stage; `Anchors` shows the team knows the cure.**
- **`StackBuilder.overTable:940-958`** is the correct stamped-fact pattern (binding fact first,
  pipeline walk only as fallback) and should be the template for MED-9 and the `sameRootTable` family.

---

## On the team's claim

*"Everything downstream consumes those stamped facts and never rediscovers mapping structure"* is
**half true**. The resolver never rediscovers from the *parse* surface — that part is solid and
structurally enforced. But it rediscovers freely from the *compiled* artifact: root selection,
include shadowing, main tables, primary keys, store substitution, and arm order are all recomputed at
query time, in multiple non-agreeing copies, while three of the facts Phase E actually stamps
(`routedTargetSets`, `resolvedStores`, the set-keyed poisons) have no reader at all.
