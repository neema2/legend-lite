# 14 — Exhaustive census: silent defaults, first-wins, swallows

**Scope:** all 30 files of `core/src/main/java/com/legend/normalizer/` (11,194 lines) plus
`model/MappingDefinition.java`, `ClassMapping.java`, `PropertyMapping.java`,
`LegacyMappingDefinition.java`, `NormalizedModel.java` (1,498).

**Purpose:** adjudicate two commit claims exhaustively rather than by sample —
`12e36db45` *"B5 every guard loud"* and `ae16e5c46` *"no first-wins on the union path"* — against the
standing requirement of **no fallbacks, no hacks, no shortcuts, no deferrals**.

---

## Category 1 — Silent defaults

| primitive | count | legit | suspect/bug |
|---|---|---|---|
| `orElseGet(MissProbe::miss)` (returns **null**) | 24 | 17 | 7 |
| `MissProbe.knownMiss(...)` (returns **null**) | 12 | 8 | 4 |
| `orElseThrow` (loud) | 41 (15 = `neverFired`) | 41 | 0 |
| `getOrDefault` | 17 | 14 | 3 |
| bare `.orElse(...)` | 7 | 4 | 3 |
| `List.of()` / `Map.of()` returns | 118 | 115 (record null-normalization) | 3 |
| `Optional.empty()` | 1 | 1 | 0 |

**36 null-returning "censused default" sites remain.** The funnel documents them; it does not remove
them. **The census criterion was empirical** ("fired on the 2026-09-15 corpus") **rather than
semantic — which inverts the risk**: the sites that *fire* are exactly the ones whose silence has
consequences, and those are the ones that stayed silent.

## Category 2 — First-wins

| primitive | count | correct rule | arbitrary |
|---|---|---|---|
| `putIfAbsent` | 14 | 9 (wall/scope sinks) | 5 |
| `break` on first match | 16 | 15 (ordered-list scans, documented shadowing) | 1 |
| `findFirst` | 2 | 2 | 0 |
| `computeIfAbsent` | 17 | 17 (all accumulator-append) | 0 |
| `merge` | 5 | 5 | 0 |

## Category 3 — Swallowed failures

| pattern | count | legit | suspect/bug |
|---|---|---|---|
| `catch` blocks | 15 | 12 (fault isolation w/ receipts) | 3 |
| `catch` → `return null` | 3 | 0 | **3** |
| `ifPresent` with no `else` | 11 | 1 | **10 (all include resolution)** |

## Category 4 — Quiet variants

Only **one** loud/quiet pair exists: `inferMainTable` / `inferMainTableQuiet`
(`MappingNormalizer.java:1473` / `:1522`), plus `nameRefOrNull` (`:715`, benign type probe).
7 callers of the quiet variant. Verdict: BUG-5.

## Category 5 — Deferral markers

| marker | count |
|---|---|
| `NotImplementedException` throws | 47 (67 refs) |
| `TODO` / `FIXME` / `XXX` | **0** |
| "roadmap" | 14 |
| "not supported yet" / "not built yet" / "for now" | 13 |

**Classification of the 47 throws: ~27 are correct loud rejections of user-model errors; ~20 are
shapes the real Legend Engine handles that this code bails on** — views as join targets
(`MappingNormalizer.java:2711`), multi-table joins (`:2736`), multi-hop Join PM under `~groupBy`
(`:1860`), `~filter` with explicit join type (`:2340`), non-plain views in joins (`:2629`),
join-mediated view `~filter` (`ViewRelation.java:132`), null-tolerant INNER filters
(`JoinChainEmission.java:969`, `:1044`), mixed-kind unions (`UnionSynthesis.java:393`), Pure-set
ModelJoin ends (`MappingNormalizer.java:1082`). **These are correctness gaps, not roadmap items** —
and see BUG-4 for why they do not fail a strict build.

## Category 6 — Nullability

**140** `@com.legend.Nullable` annotations in the normalizer (MappingNormalizer 30,
JoinChainEmission 24, RelOpTranslator 14, MappingClosures 8, Pipeline 9, rest ≤7). Conflation sites —
where one nullable return encodes *both* "not applicable" and "failed" — are BUG-5, BUG-3, SUSPECT-8,
SUSPECT-10.

## Category 7 — Order dependence

| structure | count | reaches ordered output? |
|---|---|---|
| `new LinkedHashMap` / `LinkedHashSet` | 88 | fine |
| `TreeMap` / `TreeSet` | 2 | fine |
| plain `HashSet` | 32 | **0** — every one is membership-only; none is iterated |
| plain `HashMap` | 11 | 2 iterated (`ImplicitInheritance:43,123`) — both order-safe by an `== 1` guard |
| `IdentityHashMap` | 2 | **1 — BUG-1** |

The `MappingNormalizer.java` mixed `HashMap`/`LinkedHashMap` import is clean: its 2 plain `HashSet`s
(`:278,:279`) and 1 plain `HashMap` (`:272`) are all keyed lookups, never iterated. `Pipeline`'s 9
output-bearing maps are all `LinkedHashMap`.

---

# Findings

## BUG-1 — HIGH — non-deterministic strict-build error selection

`normalizer/MappingPrePass.java:65`:

```java
Map<ClassMapping, ModelException> invalid = MappingValidation.run(r, model);
if (wallSink == null && !invalid.isEmpty()) {
    throw invalid.values().iterator().next();
}
```

`invalid` is an `IdentityHashMap` (`MappingValidation.java:53`). Its iteration order derives from
`System.identityHashCode` — **it varies between JVM runs.** When a mapping has two or more invalid
class mappings, **which error a strict build reports is not reproducible.**

**This is a regression introduced by `12e36db45` itself.** The pre-commit code threw inside the loop
(`if (!tolerant) { throw e; }`), i.e. the first invalid set in `md.classMappings()` declaration order —
deterministic. The commit message asserts *"MappingPrePass.run the first invalid set's"*; **an
`IdentityHashMap` has no first.** Note the sibling B4 site, `MappingNormalizer.java:192`
(`ledger.strictErrors.get(0)` on an `ArrayList`), *is* deterministic — **the two halves of the same
policy disagree.**

## BUG-2 — HIGH — validation guards self-disable on an unresolvable class

`normalizer/MappingValidation.java:72`:

```java
ClassDefinition cd = MissProbe.knownMiss(model.knowledge().hierarchyClass(rcm.className()));
if (cd == null) return;
```

If the class a mapping maps cannot be resolved, **all** property-name validation for that set is
skipped silently. The same shape at `:88` passes a null `tgt` into `M2mRouteGuards.requireBenignRoute`,
where `M2mRouteGuards.java:79` leaves `routedClass` null and the route guard **returns without checking
anything** — despite that method's own javadoc: *"including a set id that matches nothing … is a loud
wall, never a silent drop."* That promise holds only when `tgt != null`. **A mapping with a typo'd
class name loses both guards at once.**

## BUG-3 — HIGH — the enum typo guard is disabled by a bigger typo

`normalizer/MappingNormalizer.java:2502`:

```java
// An entry naming a NON-EXISTENT enum value is a COMPILE error —
// silently skipping it turned typos into NULL rows in [1] slots (audit).
List<String> knownValues = model.findEnum(em.enumName())
        .map(EnumDefinition::values).orElseGet(MissProbe::miss);
...
if (knownValues != null && !knownValues.contains(ev.enumValue())) { throw ... }
```

When the *enumeration itself* does not exist, `knownValues` is null and the `knownValues != null`
conjunct **switches the whole guard off.** Every `enumName()` consumer in `core/src/main/java` was
checked: **nothing validates that an `EnumerationMapping`'s enum exists** (`NameResolver.java:924`
resolves the name through the import scope but passes an unresolvable name through unchanged). **So the
exact defect the comment says was fixed — typos becoming NULL rows — is still reachable one level up.**

## BUG-4 — HIGH — roadmap gaps bypass the strict build, inconsistently

`normalizer/MappingNormalizer.java:345` (per-set) and `:368` (per-class):

```java
} catch (NotImplementedException | ModelException e) {
    if (e instanceof ModelException) { ledger.strictErrors.add(e); }
    ledger.poisons.put(cm.className(), String.valueOf(e.getMessage()));
    continue;
}
```

A `NotImplementedException` **never reaches `strictErrors`**, so it never fails a strict build —
confirmed by `MappingLedger.java:38` (*"a ROADMAP gap defers in both"*) and `MappingValidation.java:27`.
**The ~20 correctness gaps from Category 5 therefore compile green in every build mode**, surfacing
only if someone demands that specific class.

Meanwhile the per-**association** arm at `:427` does `ledger.strictErrors.add(e)` **unconditionally** —
so the identical `NotImplementedException` is strict on an association and non-strict on a class.
**One of the two is wrong.**

Against "no deferrals": **this is the deferral mechanism, and it is load-bearing.**

## BUG-5 — MED-HIGH — `inferMainTableQuiet` converts a model error into "unknown"

`normalizer/MappingNormalizer.java:1522`:

```java
static ...TableReference inferMainTableQuiet(ClassMapping.Relational rcm) {
    try { return inferMainTable(rcm); } catch (ModelException e) { return null; }
}
```

`inferMainTable` throws on two genuine model errors — *"Inconsistent database definitions"* and
*"property mappings span tables … Please specify a main table"*. The quiet variant returns null for
both, **indistinguishable from the legitimate null** (`refs.isEmpty()`, nothing to infer from).

Seven callers: `ImplicitInheritance.java:59, :212`, `UnionSynthesis.java:518, :520`,
`JoinChainEmission.java:526`, `AssociationSynthesis.java:656`, `MappingNormalizer.java:2362, :2399`.

**`ImplicitInheritance.java:212` is the worst:** an ancestor candidate whose main table is *ambiguous*
is silently filtered out of `cands`, which **changes which ancestor is selected** — a silent behaviour
change, not merely a deferred error.

## BUG-6 — MED — include-closure loudness is asymmetric across the seven walks

`normalizer/MappingClosures.java` — `walkSets` is loud (`:191`,
`orElseThrow(() -> MissProbe.neverFired("MappingClosures#1"))`). The other seven closure facts are silent:

```
:140  includes()    surfaceOf(fqn).map(...::includes).orElse(List.of())
:152  mappings()    surfaceOf(inc.mappingPath()).ifPresent(m -> walkMappings(...))
:167  walkMappings  ifPresent
:218  union()       ifPresent      <- the union path
:236  walkOps       ifPresent      <- the union path
:299  roots()       ifPresent
:311  walkRoots     ifPresent
:340  walkEnums     ifPresent
:381  walkPairs     ifPresent
:397  walkPairs     ifPresent
```

**Same question ("does this include resolve?"), two different answers depending on which fact is asked
first.** A mapping with an unresolvable include throws if `sets()` is asked but, if only `union()` is
asked, that include's Union operation **silently vanishes** from the closure. `:140` is the purest
Category-1 case: a caller cannot distinguish "this mapping has no includes" from "this mapping does not
exist."

**The "loud elsewhere" defence (`:193`) does not hold for the normalizer:** the only loud
unknown-include checks in `core/src/main/java` are `resolver/ClassSources.java:1251` and `:1400`, on
the query-resolution path — **they do not gate normalization.** Same silent skip again at
`UnionSynthesis.java:92, :725, :843`, `MappingNormalizer.java:830, :1563`,
`AssociationSynthesis.java:330`, and `model/MappingDefinition.java:352`.

## BUG-7 — MED — dead null-check and a comment that contradicts the code

`normalizer/MappingClosures.java:191-194`:

```java
LegacyMappingDefinition included = surfaceOf(inc.mappingPath()).orElseThrow(() -> MissProbe.neverFired("MappingClosures#1"));
if (included == null) {
    continue;   // unresolvable include is its own loud problem elsewhere
}
```

`orElseThrow` cannot return null — the branch and its comment are dead. **Left in place, the comment
reads as license for the silent-skip pattern at the other nine sites.**

## SUSPECT-8 — MED — unknown column silently skips declared-type coercion

`normalizer/DeclaredCoercions.java:114` and `:144`:

```java
DatabaseDefinition.ColumnDefinition cd = model.knowledge().column(db, col.table(), col.column()).orElseGet(MissProbe::miss);
String colKind = cd == null ? null : RelationalKinds.pureKindOf(cd.dataType());
if (colKind == null || colKind.equals(declared)) { return read; }
```

**"Column does not exist" and "column kind already matches the declared type" produce the same
outcome.** The comment defers loudness to the type checker; that checker sees the *uncoerced* read, so
the deferral is only sound if the checker independently rejects unknown columns.

## SUSPECT-9 — MED — swallowed `ModelException` in view substitution

`normalizer/JoinChainEmission.java:892-896`:

```java
try { tgtMain = MappingNormalizer.mainTableOf(md, targetClassFqn, model); }
catch (ModelException unmappedTarget) { return null; }
```

"Target class is not mapped" is reduced to "this view is not substitutable" — the reason is lost and
the caller proceeds down the non-substituted path.

## SUSPECT-10 — MED — `DeclaredKeys.NONE` conflation reaches a binding stamp

`normalizer/MappingNormalizer.java:325`, `:390`, `AggregateViewLift.java:45`:

```java
declaredKeys.getOrDefault(SetKeyFacts.setKey(rSrc), MappingDefinition.ClassBinding.DeclaredKeys.NONE)
```

A set missing from the pre-pass map is stamped **identically** to a set that genuinely declares no
keys — and this is an **output** (the compiled `ClassBinding`).

## SUSPECT-11 — LOW-MED — three conflict policies on one channel

`ledger.poisons` takes `putIfAbsent` (first-wins) at `MappingNormalizer.java:304, :312, :348, :427`,
plain `put` (last-wins) at `:371`, and `merge` with `";"` concatenation (accumulate) at
`UnionSynthesis.java:343`. **Three different answers to "what if this key is poisoned twice", one of
which silently discards a reason.**

## SUSPECT-12 — LOW-MED — store substitution first-wins across includes

`normalizer/StoreSubstitutionRewrite.java:308, :311` — `thisInclude.putIfAbsent(...)` /
`resolved.putIfAbsent(...)`: the first include that substitutes a given store wins. Deterministic
(include declaration order), **but not documented as a shadowing rule anywhere**, unlike the set/union
shadowing rules which are. Adjacent: `:297` `out.getOrDefault(incFqn, Map.of())` silently yields empty
substitutions for an unresolvable include.

## SUSPECT-13 — LOW — `ownSharedKeys` first-wins, on the union path

`normalizer/UnionSynthesis.java:868` — `out.put(en.getKey(), en.getValue().get(0));`, javadoc:
*"(db, canonical table, key) → the first member over it."* Deterministic (member-set-id order) and all
grouped members project an identical column, so it is a canonical-representative pick rather than a
semantic coin-flip — **but it is literal first-wins on the union path.**

## SUSPECT-14 — LOW — cycle-error text depends on `HashMap` order

`normalizer/MappingPrePass.java:228-236` — `pureByTarget` is a plain `HashMap`;
`for (... root : pureByTarget.values())` decides which node a cycle report starts from, and the message
is `String.join(" -> ", visiting)`. Deterministic in practice (`String.hashCode` is stable) but not
contractually, **and it reaches an output.**

## SUSPECT-15 — LOW — misleading exception type

`normalizer/AssociationSynthesis.java:400` — an association that fails to resolve leaves `ad0 == null`;
a `ModelJoin`/`Cross` then falls past both arms to `:412`'s
`NotImplementedException("Association mapping kind ... not supported")`. Loud, but **it reports a
roadmap gap for what is actually an unresolvable reference** — and per BUG-4 the exception type is
precisely what decides strict-build behaviour.

## SUSPECT-16 — LOW — `List.of()` as "could not determine"

`normalizer/UnionSynthesis.java:958` — `return List.of();   // a view-backed member: no physical key`.
The comment asserts one cause; **a genuinely missing table produces the same value.** Same shape at
`:725` (`tableKey` returns null for both "table missing" and "composite key").

---

**Totals: 7 BUG, 9 SUSPECT.** The remaining ~180 censused occurrences are legitimate — notably all 32
plain `HashSet`s (membership-only), all 17 `computeIfAbsent` (append-only accumulators), all 5
`ViewRelation` `findView` misses (the miss genuinely is the answer: "it's a table"),
`M2mRouteGuards.java:97` (null falls through to a throw), `RequiredNullableCensus.java:94,:108` (misses
are receipted via `ledger.census`), and the 15 `MissProbe.neverFired` conversions.

---

# Verdict on `12e36db45` — "B5 every guard loud"

**FALSE as a headline; partially true as a body.**

The commit's own body says it: *"37 bare orElse(null) sites … 21 fired and read through the documented
funnel MissProbe.miss, 16 never fired and are loud."* That is **16 of 37 made loud and 21
documented-but-still-silent** — the headline overstates the body. At `ae16e5c46` the silent population
has grown to **36** (24 `MissProbe::miss` + 12 `knownMiss`) against 15 `neverFired`.

Three concrete counter-examples, **all at sites the census recorded as *firing***:

- `MappingValidation.java:72` — an unresolvable class turns off property-name validation entirely (BUG-2).
- `MappingNormalizer.java:2502` — an unresolvable enumeration turns off the enum-value guard that was
  added to be loud (BUG-3).
- `MappingClosures.java:218/236` — an unresolvable include silently drops Union operations from the
  closure, while the same question is loud in `walkSets` (BUG-6).

**The deeper problem is the sorting criterion.** Sites were made loud because they *never fired on one
corpus run*, and left silent because they *did*. That is an argument about corpus coverage, not about
whether each silence is semantically correct — **and it systematically exempts the sites where silence
actually has consequences.**

Separately, BUG-4 shows the loudness that does exist is conditional: ~20 `NotImplementedException` gaps
never fail a strict build, and BUG-1 shows this same commit made the strict path non-deterministic.

# Verdict on `ae16e5c46` — "no first-wins on the union path"

**TRUE for the specific change; FALSE as a general statement.**

The named change is real and good. The diff replaces `ownerByProp.putIfAbsent(...)` at
`UnionSynthesis.java:184` and `:202` with `recordOwner` (`:239-251`), which **throws** a `ModelException`
naming both owners on conflict rather than keeping the first — a genuine first-wins → loud conversion,
and one of the cleanest guards in the package. The third `putIfAbsent` was in dead code, correctly deleted.

But first-wins remains on the union path:

- `UnionSynthesis.java:868` — `en.getValue().get(0)`, *"the first member over it"* (SUSPECT-13).
  Benign in effect, but it is the pattern the claim disavows.
- `UnionSynthesis.java:113` — `break` on the first closure mapping defining a member set, in
  `memberFunction`. **This one IS the spec** (own-record-first shadowing, and `ResolvedMapping.closure():102`
  puts `md` first by construction) — fair, and counted as correct.
- `MappingNormalizer.java:304, :312, :348` — `ledger.poisons.putIfAbsent`, first-wins on the reason a
  union member's binding was withheld, while `UnionSynthesis.java:343` **accumulates** with `merge` on
  the same map (SUSPECT-11).
- `MappingPrePass.java:65` — not first-wins but *arbitrary*-wins, and it can select which union
  member's validation error a strict build reports (BUG-1).

**The parenthetical "M4 → receipted skips only" holds where it could be checked:** the skip at
`UnionSynthesis.java:346-353` writes a poison and adds to `droppedRoutedProps` before `continue`, and
the skip at `:355` (`allMatch(ordinal == -1)`) is a genuine no-op case. The union *member resolution*
itself (`:369-412`) is loud throughout — missing member, Pure member, and non-subtype member all throw.
**That part of the union path is in good shape.**

*(`04-union-and-routes.md` finds two further uncounted quiet arms in the same file — `:313-321` and
`:957-960` — which would make M4 four rather than two.)*
