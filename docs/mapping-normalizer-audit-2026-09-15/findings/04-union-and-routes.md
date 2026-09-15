# 04 — Union synthesis and route classification

**Scope read end to end:** `UnionSynthesis.java` (976), `M2mRouteGuards.java` (159),
`ImplicitInheritance.java` (226), `SetDispatch.java` (99), `resolver/UnionHeads.java` (298),
`resolver/StackBuilder.java` (1,972). Engine/pure rules checked against the local checkouts.

---

## VERDICT on `ae16e5c46` — "no first-wins on the union path"

**Partially holds — true for the file it measured, false as stated for the union path.**

**What is true, and verified:** `grep putIfAbsent UnionSynthesis.java` returns **zero** hits. The
three named `putIfAbsent`s are gone. `recordOwner` (`:239-251`) is a genuine fix — a second,
different owner for a routed property name now throws `ModelException(NORMALIZE, …)` naming both
owners, and `MappingNormalizerTest.routedPropertyUnderTwoOwnersIsLoud` witnesses strict-throws /
module-walls. The dead per-ordinal key map really was dead. That part of the commit message is accurate.

**Where the claim overreaches:**

1. **Scope.** The title says "the union path"; the work and the M4 measure cover one normalizer file.
   The consumer half of the same path, `StackBuilder`, still carries four live first-one-wins
   `putIfAbsent`s (`:1032`, `:1040`, `:1067`, `:1383`), a first-fact-wins include walk (`:912-915`),
   a first-row-wins type pick (`:926-931`), a first-sibling-wins type pick (`:1936`), and — worse
   than a first-wins — a **last-wins that discards n−1 routes** at `:1169`. `UnionHeads` is clean.
2. **The M4 count of 2 is wrong even inside `UnionSynthesis`.** The gate entry asserts
   "M4 2 (both receipted skips; ZERO first-wins)". At least two further quiet arms sit in the same
   file and were not counted: the unreceipted route drop at `:313-321` (inside `classifyUnionRoutes`
   itself, 30 lines above one of the two receipts) and the view-backed-member key drop at `:957-960`.
3. **`memberFunction`'s `break` at `:113` survives in the very file the commit audited.** It is
   behaviourally safe — but only because `MappingValidation.java:47-52` rejects duplicate ids,
   matching the engine, *not* because of the shadowing rule the javadoc at `:94` claims.
4. **The sentinel scheme the commit left in place has an unhandled case** (finding 1 below).

Net: a real, verifiable improvement, and the headline is the right *direction*. But "no first-wins on
the union path" is a claim about a system, evidenced by a measure over one file, and the system does
not satisfy it.

---

## 1. HIGH — `PINNED_SINGLE` routes are silently excluded from the per-arm chain decision

`UnionSynthesis.java:777-780`:

```java
/** The union-member routes of a routed property (root/sole routes carry ordinal -1). */
static List<PropertyMapping.Join> memberJoins(List<UnionRoute> routes) {
    return routes.stream().filter(r -> r.targetOrdinal() >= 0)
```

`>= 0` drops **both** `-1` and `-2`. Every consumer of the shared-prefix-vs-per-arm split feeds on
`memberJoins`: `JoinChainEmission.java:260-261` (`perArmHops`) and `:507-508`
(`boolean perArm = !uniformChainedRoutes(memberJoins(routes))`). And `UnionSynthesis:751-753`:
`if (memberJs.isEmpty() …) return true;` — an all-pinned property yields `perArm = false`
unconditionally.

Then `JoinChainEmission:531` `boolean inArm = perArm && chain.size() > 1;` is false, so the
else-branch at `:562-585` builds the route condition from `prevTable` — the table reached by the
*emitted* prefix, which for a pinned route with a different chain is not the table its own mids reach.

`UnionSynthesis:329`'s own comment says several pinned routes are an expected shape (*"or several
(the queried mapping resolves the pins: engine R-target)"*), so this is reachable by design.

**Why it matters:** wrong join condition → wrong rows, silently. The doc at `:776` names only `-1`
and never mentions `-2` — the comment does not describe the code.

*(Same finding as `03-join-chain-emission.md` H5, from the other side of the seam.)*

## 2. MED-HIGH — first-arm-wins on a lift's target class (query side)

`StackBuilder.java:1032`, `:1040`, `:1067`: `targetClassByAlias.putIfAbsent(e.getKey(), tg.classFqn());`

The first arm that navigates an alias fixes the lift's target class for **all** arms; it flows into
`findBinding(mapping, targetClass)`, `leafSetIds`, `modeledColumns` and the slot's declared type
`new Type.ClassType(targetClass)` (`:1288`). Arms can legitimately disagree:
`JoinChainEmission.java:311-318` retargets a single-routed navigation to the routed set's *subclass*
when the declared class has no main table, so arm A can carry the subclass and arm B the declared
class under one alias. The later arm's class is discarded with no diagnostic.

## 3. MED — last-arm-wins: rule (c) discards every other arm's route

`StackBuilder.java:1155-1170`:

```java
if (allSets && ids.size() == entries.size() && shapes.size() == 1) {
    entries = List.of(entries.get(entries.size() - 1));
}
```

`n` arms each joining into their own private set collapse to the **last** one; arms 0..n−2 lose their
navigation entirely. The only justification is the class javadoc (`:86-88`, *"routes to the LAST
arm's set only"*) asserting engine behaviour — **no receipt, no test name, no engine citation in
code.** Per method discipline this is an unverified claim carrying real row-count consequences.

## 4. MED — an unreceipted quiet skip inside `classifyUnionRoutes` that M4 did not count

`UnionSynthesis.java:313-321`:

```java
} else if (memberIds != null && distinctPins > 1) {
    // ... the entry is DEAD ...
    continue;
```

A route is dropped with **no** ledger entry, no poison, no receipt — unlike the poison path 12 lines
below (`:341-347`) which does record.

---

## `memberFunction` (`:95`) — correct, wrongly justified

The `break` at `:113` is **not** a first-wins bug, but not for the reason the javadoc gives.

**Engine truth (verified in the checkouts):** `legend-pure .../functions_Mapping.pure:66-72`
`_classMappingByIdRecursive` collects **every** match across the closure and `classMappingById`
(`:74-79`) then calls `->toOne()`. There is no shadowing rule for set ids. Duplicate ids are a
**compile error** in both `legend-pure ValidatorState.java:68-81` ("Duplicate mapping found with id")
and `legend-engine MappingValidator.collectAndValidateClassMappingIds:162-194` ("Duplicated class
mappings found with ID").

legend-lite mirrors this: `MappingClosures.duplicateIds()` → `MappingValidation.java:47-52` throws.
So at most one mapping in the closure defines the id and the `break` finds *the* definer.

**LOW (method discipline) — the javadoc states a rule that does not exist.** `:94`: *"The own record
is searched first (a hoisted copy shadows the include's)."* The engine has no id-shadowing rule, and
legend-lite's own validation forbids the situation the comment describes. Note the engine's own list
order is **includes-first, own last** (`->concatenate($_this.classMappings...)`), so a first-hit rule
there would pick the *included* set — the opposite of what the comment asserts.

**MED — the two derivations of the same function name read different records.** The definition site
(`MappingNormalizer.java:272-275, 293, 802-805`) counts `md.classMappings()` on the **pre-passed**
record. `memberFunction` counts `m.classMappings()` over `md.closure()`, whose non-own entries are
**surfaces** (`MappingClosures.Closure.mappings():145-158`), and `UnionSynthesis.prePassedClosure:885-893`
exists precisely to get pre-passed records — and `memberFunction` does not use it. Today the only
record-growing rewrite is `ImplicitInheritance.implicitOpsForRoutedTargets:190-194`, which only
appends ops for classes with **zero** sets, so the counts cannot currently diverge. **A latent trap:**
any future pre-pass that adds a set to an already-mapped class silently emits a dangling function
reference.

**Engine parity worth noting (good):** the engine's `rootClassMappingByClass`
(`functions_Mapping.pure:61-64`) is `includes → own → ->last()`, i.e. own wins, later include wins,
no error on multiple roots. `MappingClosures.roots():291-315` + `ownRoots:320-330` reproduce exactly
that, including the sole-set auto-root inference scoped to the own mapping (matching
`MappingValidator.validateStar:420-451`).

---

## Order dependence — clean

**Ordinal assignment is deterministic and independent of hash iteration order.** Evidence:

- `HashMap` is *imported* into `UnionSynthesis` (line 60) and **never instantiated**. Every
  collection construction was enumerated: the only `HashSet`s are `:437` (`seenSets`, cycle guard)
  and `:609` (`seen`, visit guard), both membership-only. Everything feeding an ordered output is
  `LinkedHashMap` / `LinkedHashSet` / `List` (`:264, :265, :377, :422, :540, :547, :578, :818, :857,
  :869, :689`).
- Union ordinals come from `u.memberSetIds()` — the parsed `~operation` list. This matches the
  engine exactly: `router_operations.pure:33-36` `special_union` returns
  `$o.parameters.setImplementation` in declared order, preserved through `resolveOperation` into
  `buildUnion`, and `pureToSQLQuery_union.pure:136-157` `managePrimaryKeys` names columns
  `<col>_<setImpls->indexOf($s)>`. `ResolvedMapping.memberOrdinal:179` uses `indexOf`, matching the
  engine's first-occurrence semantics on a duplicated member.
- Inheritance ordinals come from `collectInheritanceMembers` → `LinkedHashSet chosen`, fed from
  `model.classes()` (`ModelBuilder.java:1017-1018` — *ingest order*) then `Pure.allNativeClasses()`
  (a fixed `List`).
- `ImplicitInheritance.java:40` and `:121` copy `visibleSets()` (a `LinkedHashMap`) into a
  **`HashMap`**, and `:42` builds `byClass` from `bySetId.values()`. Order-independent only by luck:
  every consumer is a `size()==1` test or a `Set` membership. **LOW** — gratuitous order destruction
  one refactor away from mattering.

**MED — inheritance-op ordinals are user-visible and are legend-lite's ingest order, not the
engine's.** `recordKeyThreads:923` mints `col + "_" + o`, and `ImportDataFlow.columns:70` surfaces
`t.name()` **verbatim as a result column**. For a `~operation` union the order is authored and
matches the engine; for an Inheritance op there is no authored list, and legend-lite's member order
is model-ingest order while the engine's is `getMappedLeafTypes`' traversal. **Result column names
differ.**

---

## Key threads — three silent assumptions

### MED — a `~primaryKey` the code cannot read is silently replaced by a different key

`UnionSynthesis.java:947-956`:

```java
for (RelationalOperation op : mr.primaryKey()) {
    if (op instanceof RelationalOperation.ColumnRef cr
            && canonicalTable(cr.table()).equals(canonicalTable(main.table()))) {
        declared.add(cr.column());
    }
}
if (!declared.isEmpty()) { return declared; }
```

A non-`ColumnRef` key expression, or one naming a joined table, is dropped from `declared` with no
`else`; `declared` then reads empty and the code falls through to the **table's** PRIMARY KEY — a
different identity, with no diagnostic. The engine's `resolvePrimaryKey`
(`helperFunctions.pure:439-460`) does not substitute.

### MED — a view-backed member silently contributes no row identity

`:957-960`: `if (td == null) { return List.of(); // a view-backed member: no physical key }`. That
member then has zero key threads, `ImportDataFlow` emits no column for it, and `CastReRoot`
(`:90`) has nothing to join on. A third quiet arm beyond the two M4 receipts.

### MED — the shared-key optimization changes the `importDataFlow` result shape

`ownSharedKeys:855-876` + `recordKeyThreads:919-921` suppress the per-ordinal threads whenever ≥2
members share `(db, canonicalTable, key)`, emitting one ordinal `-1` thread instead (`:929-937`).
`ImportDataFlow.java:58-60` then skips it (`if (t.shared()) continue;`). So a single-table hierarchy
yields **zero** member key columns, where `managePrimaryKeys` (`pureToSQLQuery_union.pure:136-157`)
always emits `<col>_<i>` for every set. The optimization is honestly documented as an H2 performance
cure (`:707-716`); its effect on the `importDataFlow` contract is not.

### Verified as claimed

*"un-routed members read NULL keys and nothing is assumed"* holds on the consumer side —
`StackBuilder.java:313-322` projects a thread only when `t.ordinal() == i` (or the arm is over the
shared table), leaving a typed NULL otherwise; `demandOnArm:1952-1954` pads with a typed empty
collection. `recordKeyThreadsOf:515-527` correctly substitutes the *inferred* main table for members
that declare none. `threadType:932-934` and `demandOnArm:1948-1951` are loud when no arm carries a column.

### MED — `threadType` takes the type from the wrong arm

`StackBuilder.java:925-931`:

```java
for (Type.RelationType r : rows) {
    Type.Column c = columnOf(r, t.column());
    if (c != null) { return c.type(); }
```

It scans **all** arm rows for a same-named column and returns the first hit, ignoring `t.ordinal()`,
which names the arm that actually owns the thread. Two members with a same-named key column of
different SQL kinds type the union column from arm 0. Same pattern at `:1929-1939`.

---

## The `PINNED_SINGLE = -2` / `-1` sentinel scheme

**MED — the int payload is entirely dead; only the three-way tag is read.** Full consumer census
(`grep targetOrdinal`, whole repo):

- `UnionSynthesis.java:349` — `allMatch(r -> r.targetOrdinal() == -1)`
- `UnionSynthesis.java:778` — `filter(r -> r.targetOrdinal() >= 0)`

That is all. `JoinChainEmission.routeList:502-596` — the one place that turns routes into emitted
Pure — **never reads the ordinal**; it names the set's own function via `memberFunction` (`:590`). So
the numeric ordinal in `UnionRoute` is written and never used; the record is a three-case tag encoded
as an `int`.

```java
sealed interface UnionRoute {
    record Member(int ordinal, Join j) implements UnionRoute {}
    record Root(Join j)                implements UnionRoute {}
    record Pinned(Join j)              implements UnionRoute {}
}
```

would make the compiler enforce exhaustiveness — which is exactly what is missing. **The missed case
is finding 1:** `memberJoins` handles MEMBER (`>=0`) and implicitly folds ROOT and PINNED together;
`classifyUnionRoutes:349` handles ROOT and folds MEMBER and PINNED together. **No consumer anywhere
distinguishes PINNED from ROOT**, even though `:84-87` defines them as distinct route kinds and
`:322-330` deliberately constructs them differently.

Secondary smell: `ResolvedMapping.memberOrdinal:194` also returns `-1`, meaning "**not** a member" —
the opposite of `UnionRoute`'s `-1`, "root route". The two meanings meet at `UnionSynthesis:303-311`
and are kept apart only by the `ord >= 0` guard.

---

## `collectInheritanceMembers` (`:575`) — half-retired

The hierarchy walk **is** on the kernel (`knowledge().subtree`, `directSubtypes`, `hierarchyClass`) —
that part of the census is honest. But `:606-648` hand-rolls a breadth-first *nearest-mapped-ancestor*
climb, reading `cd.superClasses()` raw at `:640`. `KnowledgeLayer.ancestorsAndSelf:146` already
answers this nearest-first, and `ImplicitInheritance.nearestMappedAncestor:204` uses it for the same
question. **LOW-MED — two implementations of one concept.** The remaining E-logic (stop at `base`,
stay inside `subtree`, recurse on a nested Inheritance op) is genuinely mapping-aware and justifies a
call site — but not a second ancestor walker.

## MED — the root lookup inside `classifyUnionRoutes` contradicts its own comment and the engine

`UnionSynthesis.java:307-310`:

```java
// engine rootClassMappingByClass: the * set, or the class's
// SOLE set (sole-ness judged in the OWNING mapping's scope)
boolean rootOrSole = set instanceof ClassMapping.Relational tr
        && (tr.root() || md.classMappings().stream()
                .filter(x -> x.className().equals(tr.className())).count() == 1);
```

`set` came from `md.set(...)`, which resolves **through the include closure**
(`ResolvedMapping.java:93-98`), but sole-ness is counted over `md.classMappings()` — the **querying**
mapping's own sets, not the owning one. The engine infers `root` per defining mapping
(`MappingValidator.validateStar:420-451`, own `_classMappings()` only).

Concretely: include `I` maps class `C` with exactly one unmarked set; `M` includes `I` and maps
nothing for `C`; the count is 0, not 1, so a route to that set is classified `PINNED_SINGLE` instead
of a root route. `ResolvedMapping.roots()` (which `collectInheritanceMembers:579` itself uses, 270
lines later in the same file) already answers this correctly — **a third root determination in one
file.**

---

## Fallbacks / dead code / duplication

**LOW (but large) — confirmed-dead code in `UnionSynthesis`, all zero-reference repo-wide:**

- `:653` `static final String MEMBER_WITNESS` — no reference anywhere.
- `:659` `private static final String FILTER_FORM` — no reference.
- `:661` `record Thread(ValueSpecification, List<ColSpec>)` — no reference, and it shadows
  `java.lang.Thread` inside the file.
- `:667-705` `record ScanSource` — `of()` is called only by `merged()`; `merged()` and `slotName()`
  have no caller in `core/src`. ~40 dead lines.
- `:792` `{@link #liftMidSteps}` — dangling javadoc link; no such method exists.
- **34 unused imports** (mechanically verified), including `java.util.HashMap`,
  `java.util.Collections`, `java.util.TreeSet`, `Realization`, `NewInstance`, `KeyExpression`,
  `ParsedModel`, `NormalizedModel`, and all six `C*` literal specs.

Leg 6g's commit message claims the dead lift-era cluster was removed; this residue was left behind in
the same file.

**MED — `collectRoutedJoins` misses routes inside an inline-embedded block naming an *included* set.**
`UnionSynthesis.java:211-222` hand-rolls a scan of the **own** class mappings with no fallthrough,
where `ResolvedMapping.set(ie.setId())` (three lines of the same file's own API, used everywhere
else) searches own-then-closure. An inline-embedded set defined in an include is silently skipped and
its routed joins never reach `unionRoutes`.

**LOW — the emitter mutates the normalizer's classification.** `JoinChainEmission.java:321`
`p.unionRoutes.remove(propName);` — a second, emission-time classifier overriding
`classifyUnionRoutes`. Order-dependent within a class mapping, and it means the "fact" is not immutable.

**LOW — residual first-wins outside `UnionSynthesis`, all silent:**

- `StackBuilder.java:912-915` `keyThreads` returns the first mapping in BFS order carrying a fact.
- `StackBuilder.java:1691-1697` `findBinding` — first root/class-level binding wins (own-first; this
  one matches the engine's `->last()` semantics and is fine).
- `StackBuilder.java:1383` `modeledOf`: `out.putIfAbsent(pa.property(), b.getKey())`.
- `ResolvedMapping.java:183-193` `memberOrdinal` — first member whose `extends` chain reaches the set
  wins the ordinal, silently.
- `M2mRouteGuards.java:95-97` `findFirst()` — safe only because `setIdMatches` is exact (`:130-133`).

**String-typed identity** is pervasive and load-bearing: set ids, class FQNs, slot aliases, key names
(`__route<g>_<k>`, `__s_<lift>_<entry>_<k>`, `emb__<path>__<sub>`, `<col>__pk_<table>`) are all raw
`String` with `startsWith`/`endsWith` discrimination (`StackBuilder.java:441`). A user property
literally named `emb__x` collides. Consistent and documented, but a naming convention doing a type's job.

---

## Does `StackBuilder` consume the stamped facts? — YES, cleanly

**This is the strongest part of the arc.** `StackBuilder.java` contains **zero** references to
`LegacyMappingDefinition`, `ClassMapping` records, or `ParsedModel` (verified by grep). It reads only
normalized facts:

- `operationMembers` → stamped at `UnionSynthesis.java:136` and `:494`, carried into the binding at
  `MappingNormalizer.java:336-337` / `:400-401`, consumed at `StackBuilder.java:192-193` and `:1741`
  (*"the arms are the binding's FACT (member order)"*).
- `unionKeyThreads` → stamped at `UnionSynthesis.java:938`, consumed at `StackBuilder.java:305, 912`,
  `CastReRoot.java:90`, `ImportDataFlow.java:51`. Names used verbatim (`t.name()`, `t.ordinal()`,
  `t.shared()`, `t.store()/table()`), never re-spelled.
- `propertyPins` → stamped at `MappingNormalizer.java:328/393`, consumed at `StackBuilder.java:1308-1309`.
- `mixedUnions` → correctly consumed by `ClassSources.java:684-686` (the mixed-kind union resolves at
  the resolver by design).

The only re-derivation is `leafSetIds`/`collectLeafSetIds` (`:1715-1748`), and it walks **bindings**,
not mapping text.

---

## What is genuinely good

- **Fact-flow discipline is real.** `StackBuilder` — 1,972 lines, the heart of the query-side union —
  does not touch a single parse record. Its class doc's claim *"Nothing here reads the mapping text:
  the arms' steps are the facts"* is one of the few large claims in this codebase that survives a grep.
- **Order determinism is engineered, not accidental.** The ordinal alignment invariant is enforced
  *by construction* — `inheritanceMembers` is the one enumeration shared by `synthInheritance` and
  route classification (`:531-536`), which is the right way to make "misalignment = silently wrong
  rows" unrepresentable rather than tested-for.
- **The root/include rules are genuine engine parity, not folklore.** `MappingClosures.roots()` +
  `ownRoots` reproduce `rootClassMappingByClass`'s `includes → own → last()` and `validateStar`'s
  own-mapping-scoped sole-set inference precisely; `duplicateIds` reproduces
  `collectAndValidateClassMappingIds`. Checked against the real Pure and Java sources; they match.
- **The `recordOwner` fix is the right shape.** Not "detect and pick one", not "log and continue" — a
  fact with one value, loud in strict, walled in module, with a named witness test. The correct
  treatment for the whole class of quiet arms this arc is chasing.
- **Loudness where it counts.** `threadType:932`, `demandOnArm:1948`, `renameReads:1554`,
  `addStc:1891`, `collapseOntoOneScan:545`, `keyReadMissing:360` all throw with diagnostic detail
  rather than defaulting. `MissProbe` is an unusually honest device.
- **`uniformChainedRoutes`** is the right architectural instinct — one predicate shared by the
  emitter and the inbound key collector so the two cannot drift. Its defect is in its *input*
  (`memberJoins`), not in the idea.
