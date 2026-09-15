# 06 — Associations and XStore

**Scope read end to end:** `AssociationSynthesis.java` (676), `XStorePureEnds.java` (303),
`ImplicitInheritance.java` (226), `resolver/AssociationJoins.java` (2,099), plus
`StoreSubstitutionRewrite.java` (401), `package-info.java`, `NameResolver`, `MappingClosures`,
`MappingLedger`, `ModelBuilder`, and `Pure`'s native signatures.

The 2026-09-15 self-audit (§4) explicitly excluded these files — *"same method, next pass."*
This is that pass.

---

## Q1 — The `AssociationSynthesis:389` layering violation: CONFIRMED, and provably inert

### HIGH — `resolveAssociation` performs import-scope + same-package name resolution inside Phase E

`AssociationSynthesis.java:375-394`:

```java
static java.util.Optional<AssociationDefinition> resolveAssociation(
        ModelBuilder model, ResolvedMapping md, AssociationMapping am) {
    String name = am.associationName();
    var direct = model.findAssociation(name);
    if (direct.isPresent() || name.contains("::")) { return direct; }
    for (String pkg : model.importsOf(md.qualifiedName()).wildcards()) {   // :383
        var hit = model.findAssociation(pkg + "::" + name);
        if (hit.isPresent()) { return hit; }
    }
    // same-package fallback (an unimported sibling)                        // :389
    int cut = md.qualifiedName().lastIndexOf("::");
    return cut < 0 ? Optional.empty()
            : model.findAssociation(md.qualifiedName().substring(0, cut) + "::" + name);
}
```

Against the contract at `normalizer/package-info.java:53-58`: *"it does not resolve, look up scopes,
or invoke other phases."* This reads `model.importsOf(...)` — a scope lookup — and reimplements
Phase D's two tiers.

**NameResolver already does this, for exactly this field.** `compiler/NameResolver.java:1097-1103`:

```java
case AssociationMapping.Relational r -> {
    String name = resolveName(r.associationName(), scope);
```

and all three kinds (`ModelJoin` `:1105`, `Cross` `:1111`) get the same call. `resolveName` →
`resolveNameMulti` (`:589-666`) walks **wildcards (`:617-624`) → own package (`:632-637`) →
CORE_IMPORTS (`:648-654`)** — a strict superset — over the same `knownFqns` universe. And every
`ModelNormalizer.normalize` call site is preceded by `NameResolver.resolve*` (`Compiler.java:236, 293, 406`).
**There is no path into Phase E that skipped Phase D.**

**Why it's there / what shape needs it:** nothing in-repo. Every `.pure` fixture containing
`AssociationMapping` (26 files) was searched for a simple-name association reference — **zero hits**;
all are spelled FQN. The javadoc cites an engine grammar shape that may exist in the external
`-Dlegend.pure.root` corpus.

**But it cannot help even if that shape exists — three independent proofs:**

1. **The pre-pass hard-fails first, with the exact lookup.** `MappingNormalizer.java:266` runs
   `injectMultiHopAssociationPMs` *before* any association synthesis and *outside any try/catch*.
   Inside, `AssociationSynthesis.java:110`:
   ```java
   AssociationDefinition ad = model.findAssociation(rel.associationName())
           .orElseThrow(() -> MissProbe.neverFired("AssociationSynthesis#1"));
   ```
   `findAssociation` is **exact-FQN only** (`ModelBuilder.java:654-656` → `symbols.resolveId`). So
   for any mapping carrying a `Relational` association mapping, a simple name kills the compile at
   `:110` before `:399` ever runs. **The lenient path at `:389` is unreachable for the Relational kind.**
2. **`IllegalStateException` escapes the walling.** `MappingNormalizer.java:196-201` catches only
   `ModelException`; `MissProbe.neverFired` returns `IllegalStateException`. So that failure sinks
   the whole module build rather than walling one mapping — the opposite of the stated policy.
3. **The Cross/ModelJoin kinds re-resolve strictly two lines later.**
   `synthesizeAssociationMapping:399` resolves leniently, then hands off to
   `MappingNormalizer.synthesizeXStoreMapping`, whose **first statement** is
   `model.findAssociation(xs.associationName()).orElseThrow()` (`MappingNormalizer.java:960`), and
   `synthesizeModelJoinMapping` likewise at `:1075`. A name only the fallback resolves dies there
   with a bare `NoSuchElementException` — again not caught by `:420`'s
   `NotImplementedException | ModelException`.

**Verdict:** name resolution in Phase E, redundant with Phase D, dead-or-harmful in every reachable
branch. **NameResolver should have handled it — and does.** Recommended fix: delete
`resolveAssociation` and call `model.findAssociation` directly; if a corpus witness for the bare
spelling turns up, fix it in `NameResolver`/`knownFqns`, not here.

### MED — `resolveAssociation` silently takes the first wildcard match where `NameResolver` raises an ambiguity error

`AssociationSynthesis.java:383-388` vs `NameResolver.java:573-579`:

```java
if (matches.size() > 1) throw new ResolutionException("ambiguous reference '" + name + "' ...");
```

Phase E's copy loops and `return`s on first hit. Two imported packages each defining `Assoc` → Phase
D errors, Phase E picks one silently. **This is the concrete semantic divergence created by having
two resolvers.**

### MED — a mapping's association binding and its poison can be keyed by a *simple name* the resolver can never look up

`MappingNormalizer.java:427-440`:

```java
ledger.poisons.putIfAbsent(
        AssociationSynthesis.resolveAssociation(model, md, am)
                .map(a -> a.qualifiedName())
                .orElse(am.associationName()),  // <- simple name when unresolved
```

…and the same `.orElse(am.associationName())` on the `AssociationBinding` key at `:437-439`. The
query side looks up `assoc.qualifiedName()` (`AssociationJoins.java:1089, 1094-1096`) — the real FQN
off the model. So **in exactly the case the fallback exists to serve, both the binding and the poison
land in a key space the resolver never probes.**

---

## Q2 — `StoreSubstitutionRewrite`: the leniency is gone, the comment describing it is stale, and its guard is dead

### MED — `StoreSubstitutionRewrite.java:371-376`'s "shadowed cases only" guard can never be false

```java
for (String r : raws) {
    if (r == null || r.contains("::") || model.hasDatabaseExact(r)) { continue; }   // :368
    // rewrite ONLY the SHADOWED cases: the raw spelling's lenient
    // simple-name resolution lands somewhere OTHER than the
    // scope-qualified candidate (or nowhere). When they agree the
    // raw spelling stays ...
    String current = model.findDatabase(r)
            .map(DatabaseDefinition::qualifiedName)
            .orElseGet(MissProbe::miss);                                            // :377-379
    for (String w : scope.wildcards()) {
        String cand = w + "::" + r;
        if (model.hasDatabaseExact(cand)) {
            if (!cand.equals(current)) { q.put(r, cand); }                          // :384
            break;
        }
    }
}
```

`r` is guaranteed bare (`:368`) and `ModelBuilder.findDatabase` is now **exact-FQN only**
(`ModelBuilder.java:793-809`: *"EXACT-FQN ONLY (NAME_RESOLUTION_BUG.md): the global suffix scan …
deleted"*). So `current` is **always null**, `!cand.equals(null)` is **always true**, and the rewrite
is unconditional. The `:240-241` javadoc — *"the lenient simple-name fallback still serves scope-less
models"* — **names a fallback that was deleted from `findDatabase`.** A comment claiming something
the code does not do, plus a dead conditional guarding against a regression the guard no longer prevents.

### MED — `qualifyStoreRefs` is a second, weaker store-name resolver in Phase E

`NameResolver.java:1422` (`resolveTableReference`), `:1156` (`PropertyMapping.Column`), `:1162`
(`EnumeratedColumn`), and the `JoinChainElement`/`FilterMapping`/`RelationalOperation` arms all call
`resolveName(db, scope)`. `qualifyStoreRefs` (`:358-400`) redoes it with: first-wildcard-wins
(`break` at `:386`), **no own-package tier**, **no CORE_IMPORTS tier**, **no ambiguity error**.

Its own comment states the cost outright: *"…the propertyLevel family regressed wholesale under
unconditional qualification."* **This is a name resolver that deliberately resolves *some* names,
because resolving all of them breaks consumers keyed on the unresolved spelling.** That is the real
debt: the resolved/unresolved distinction has leaked into downstream keys, so Phase D's output cannot
be trusted to be resolved, and every downstream reader must tolerate both.

### MED — store substitution never reaches association mappings pulled through an include

`StoreSubstitutionRewrite.java:188-201` defines `applyAssoc` (*"The same exhaustive walk over an
ASSOCIATION mapping's PM bodies"*). Its only two callers are `collectDatabases` (`:229`, the
recorder) and `qualifyStoreRefs` (`:398`, the mapping's *own* import qualification). The include path
applies substitution to **class mappings only** — `MappingClosures.java:201`:

```java
if (!inc.substitutions().isEmpty()) {
    local.replaceAll((k, v) -> StoreSubstitutionRewrite.apply(v, inc.substitutions()));
}
```

Meanwhile `MappingNormalizer.java:412` synthesizes only `md.associationMappings()` (own), and the
resolver reuses the included mapping's own binding
(`AssociationJoins.associationBindingInClosure:619-643`). **So under `include AMapping[db1->db2]`,
`AMapping`'s association join chains keep `db1`.** The class-mapping header at `:22-25` claims *"every
store reference pulled through the include re-points to the replacement"* — **association mappings
are an unstated exception.**

---

## Q3 — "no Any punt": true on the path the comment covers; an `Any` hatch exists on its sibling

**Good — the Relational association path is genuinely typed.** `AssociationSynthesis.java:500-512`
passes real relation refs, and `Pure.java:1732` declares:

```
legacyAssocPredicate<A,B,S,T>(a:A[1], b:B[1], src:Relation<S>[1], tgt:Relation<T>[1],
                              cond:Function<{S[1],T[1]->Boolean[1]}>[1]):Boolean[1]
```

Fully parametric. No `Any`, no bespoke checker. The `:502` claim holds. No `"Any"`, `type::Any`, or
erased return appears anywhere in `AssociationSynthesis`, `XStorePureEnds`, or `AssociationJoins`.

### MED — but the XStore property-space route (route A) *does* punt to `Any`, and the file says so

`Pure.java:1744`:

```
legacyLocalProperty(row:Any[1], prop:String[1]):Any[1]
```

emitted at `XStorePureEnds.java:280`. `XStorePureEnds.java:276-279`:

> *"NOTE (batch 110 probe): the marker is Any-typed, so an ORDERING comparison over a +prop on this
> route does not type (lessThan(Any, Integer)); a cast to the declared local type was tried and
> regressed six XStore Pure-end tests — the typed-local read is an open leg of route A"*

`XEnd` already carries `localTypes` (`:63`, populated at `:127`) — the declared type is in hand and
unused on this path. A real capability gap (loud, not silent), but **the statement "there is
genuinely no Any-typed escape hatch in the association path" is false for the XStore branch.**

### LOW — set identity on route A is String-typed and the route is recovered by sniffing an argument's runtime class

`Pure.java:1737` overload takes `srcSet:String[1], tgtSet:String[1]`; emitted as
`new CString(endA.setId())` (`XStorePureEnds.java:237-238`). The resolver recovers the route with
`call.args().get(2) instanceof TypedCString` (`AssociationJoins.java:1140`) and the target set id by
`reverse ? setA : setB` (`:1147-1150`). **Route kind and set identity are re-derived from emission
shape rather than carried as facts.**

---

## Q4 — Multi-hop injection: a surface mutation with no provenance and a silent-override collision

### MED-HIGH — an injected association PM silently overwrites a same-named user-authored class PM

`AssociationSynthesis.withInjectedPMs:280-300`:

```java
Set<String> have = new HashSet<>();
for (PropertyMapping pm : rcm.propertyMappings()) { have.add(pm.propertyName()); }
for (String id : lineage) {
    boolean own = id.equals(ResolvedMapping.idOf(rcm));
    ...
        for (PropertyMapping pm : forSet) {
            if (own || have.add(pm.propertyName())) { add.add(pm); }   // :290
        }
```

The `own ||` **short-circuits the shadow check**: for the set's own entries the injection is added
regardless of whether the class mapping already maps that property. They are then appended at the end
(`:334 pms.addAll(add);`), and the ctor builder keys by property name into a `LinkedHashMap` —
`MappingNormalizer.java:1925-1933`. **Last put wins, silently** — the association entry replaces the
user's PM with no wall, no poison, no census entry.

(Multiple same-name `Join` PMs *are* a legitimate shape when set-routed, so a blanket duplicate wall
is wrong; but the unrouted `targetSetId == null` collision has no guard at all.)

### MED — the injected PM carries no provenance, and the stamped facts describe the mutated surface as if authored

`PropertyMapping.Join(propertyName, database, joins, targetSetId)` has no origin field, and `:183-187`
rebuilds a plain `Join` with no marker. `MappingNormalizer.java:266` replaces the working surface
(`pp.withMapping(...)`), so everything downstream — `SetDispatch.routedTargetSets(md, model)`
(`:453`), `MappingFacts.routedTargetClasses(surface)`, the nullable census, `propertyPinsOf` — is
computed over the mutated tree with no way to distinguish an authored PM from a synthesized one.
**That is the mutation-based-shortcut signature: the rewrite is not reversible, not attributable, and
not visible to any later diagnostic.**

### LOW — the per-set injection matches on set id across *all* owner keys

`:286` iterates `bySet.values()` (every owner) and selects by set id alone. The comment at `:274-277`
justifies this ("set ids are unique in scope"), enforced by `MappingValidation.java:47-51` — so the
invariant holds, but the lookup is structurally a two-key map used as a one-key map.

---

## Q5 — XStore: the same rule implemented twice, verbatim

### MED — `MappingNormalizer.synthesizeXStoreMapping` and `XStorePureEnds.synthesize` are two copies of one ~50-line algorithm

Direction-agreement wall, `MappingNormalizer.java:1032-1042`:

```java
ValueSpecification canon0 = canonicalizeEqualOperands(cond, srcRow.name());
for (ValueSpecification c : conds) {
    if (!canonicalizeEqualOperands(c, srcRow.name()).equals(canon0)) {
        throw new NotImplementedException("XStore association '" + xs.associationName()
                + "' has direction-specific conditions; a single"
                + " shared predicate is required for now (mapping=" + md.qualifiedName() + ")");
```

`XStorePureEnds.java:220-232` — **character-identical** message and logic, only `MappingNormalizer.`-prefixed calls.

Also duplicated:

- the orientation loop with `selfAssoc ? (isProp1 ? tgtRow : srcRow) : (isProp1 ? srcRow : tgtRow)` —
  `MappingNormalizer.java:1006-1022` vs `XStorePureEnds.java:182-213`;
- the "matches neither end of association" `ModelException` — `:1008-1013` vs `:187-193`;
- the "has no property lines" `ModelException` — `:989-994` vs `:214-219`;
- the `legacyAssocPredicate(a, b, _, _, λ)` assembly and the whole `FunctionDefinition` construction —
  `:1043-1062` vs `:233-259`.

**The only real difference is one line:** `RelationReads.xstore(...)` (column space) vs
`renameReads(...)` (property space). The two routes should share one body parameterized by that
rewriter. Note the **asymmetry of the "no property lines" check**: `MappingNormalizer` performs it at
`:989`, *after* the route-A dispatch at `:973`, so route A only gets it via `XStorePureEnds:214` —
**the duplication is load-bearing, which is how this kind of copy rots.**

### LOW — the "for now" wall is a syntactic canon compared by record `.equals()`

`canonicalizeEqualOperands` (`MappingNormalizer.java:732-757`) normalizes `equal` operand order and
sorts `and`/`or` operands by `ps.get(0).toString().compareTo(...)` (`:751`). Conservative in the safe
direction (it over-walls, never under-walls), but the AND/OR ordering keys on record `toString()` — a
formatting change in any `ValueSpecification` record silently changes which XStore mappings compile.

---

## Q6 — Correctness

### HIGH — the Relational association path has *no* direction-agreement wall, while the XStore path does

`AssociationSynthesis.java:428-432, 449-456`:

```java
// Pick the FIRST property mapping as the primary; multi-PM
// disambiguation by [srcSetId, tgtSetId] could differentiate ...
AssociationPropertyMapping firstAm = rel.propertyMappings().get(0);
...
// First-PM-wins is RETAINED (audit 23 probed-and-reverted) ...
// Residual: two directions with genuinely NON-equivalent joins
// would still take the first silently.
```

**A self-declared silent-wrong-SQL residual, and it is the exact hazard the XStore sibling walls
loudly (Q5).** The asymmetry is the finding: the same risk is a hard `NotImplementedException` on one
route and an acknowledged silent default on the other. A `canonicalizeEqualOperands`-style comparison
over the *resolved join conditions* would close it without reinstating the direction-agreement wall
that broke tests (that wall compared *joins*; comparing *resolved conditions* is the XStore recipe).

### MED — three `return null` paths withhold the association binding with no poison

`AssociationSynthesis.java:446-448` (multi-hop), `:462-465` (no anchor table), `:471-474`
(Operation-mapped end). `MappingNormalizer.java:434` does `if (fn != null)` and moves on — **nothing
is written to `ledger.poisons`.** At query time the user gets `AssociationJoins.java:1089-1097`'s bare
*"association 'X' is not mapped in mapping 'Y'"*, and the `.or()` reason chain finds nothing. The
comment at `:459-461` claims *"NAVIGATING the association stays loud at resolve time"* — true, but
**reasonless**. The `:462` case in particular (an end class with NO `~mainTable`) is a user-model
shape, not a compiler invariant, and deserves the reason it already computed.

### Good — self-association direction is pinned consistently across the two phases

`AssociationSynthesis.java:478-489` computes `targetIsA` only for `!classA.equals(classB)` and
documents the same-class convention; `AssociationJoins.java:1137-1139` reverses by property name for
the self case.

**LOW edge:** the resolver's self-association test is `cs.classFqn().equals(targetClass)` — an exact
class equality. A *subclass* navigating a self-association (parent `B extends A`, both ends `A`)
falls into the `!parentIsA` branch instead, where `parentIsA` and `targetIsA` are both true and
orientation is decided by subtype rather than property name. The synthesis side's
`classA.equals(classB)` test has the same shape. Untested shape.

### Good — multiplicity is handled explicitly

`AssociationJoins.java:825-830` denies non-concrete ends; `toOneClassProp:797-806` gates bare-tail
demand on `[0..1]`; `parentNavSubquery:1337-1352` refuses a LIMIT-1 over a to-many hop unless an
explicit `->toOne()` declares it. That last one is genuinely careful work.

### Good — ends in different included mappings are handled on both sides

`AssociationSynthesis.injectMultiHopAssociationPMs:93-103` + the HOIST block at `:218-236`;
`anchorTableOf:634-663` walks `md.closure()`; `AssociationJoins.associationBindingInClosure:619-643`
does a BFS with "own definitions win, first match by declaration order".

---

## Q7 — `AssociationJoins` re-derives; the `.or()` chain is sound

**The association-keyed poison IS written** — `MappingNormalizer.java:427-431`, and
`MappingLedger.java:32-34` documents the key space as *"'class', 'class[setId]' or an association
FQN"*. So both arms of `AssociationJoins.java:1094-1096` can hit. **No bug here** — with the two
caveats already filed: the unresolvable-name case keys it by simple name (Q1), and the three
`null`-return paths write nothing at all (Q6).

### MED — the resolver re-derives association structure from the emitted predicate body, twice

`MappingDefinition.AssociationBinding` (`model/MappingDefinition.java:443`) carries exactly two
strings: `associationFqn`, `predicateFunctionFqn`. Everything else — orientation, route kind, pinned
target set id — is recovered by recompiling the predicate function and pattern-matching its body
(`AssociationJoins.predicateMaterial:1080-1153`), guarded by three `"resolver bug:"`
`IllegalStateException`s (`:1099, 1107, 1181`) that enforce the Phase-E↔Phase-H contract structurally
rather than by type.

Worse, the derivation is **duplicated inside the same file**:

| | `predicateMaterial` | `scanCondTargetReads` |
|---|---|---|
| compile the fn | `:1101 specs.compile(fns.get(0))` | `:1718 specs.compile(fnsE.get(0))` |
| classA fqn | `:1124` | `:1731` |
| `parentIsA` | `:1134` | `:1733` |
| `reverse` | `:1137-1139` | `:1734-1736` |
| `propSpace` | `:1140-1141` | `:1739-1740` |

Identical expressions, different variable suffixes. The only difference is that `predicateMaterial`
also computes `targetIsA` for its sanity guard, and throws where `scanCondTargetReads` silently
returns `null`. Both run on every association navigation (`:843` then `:916`). **The same class of
split-brain the file itself warns about at `:817-818`** (*"a separate index lookup was a split-brain
with findAssociationOf (audit blocker)"*) — caught once, reintroduced here.

---

## Q8 — Fallbacks, dead code, duplication, size

### MED — `buildAssocPredicateBody`'s 7-arg overload is dead, and its javadoc describes behaviour the code refuses

`AssociationSynthesis.java:529-545`:

```java
/**
 * ... Multi-hop joins chain conditions through intermediate
 * row bindings: each hop's predicate is conjoined with the next
 * via {@code and(...)}, with intermediate rows resolved by named
 * binding through the chain alias scope.
 */
static ValueSpecification buildAssocPredicateBody(...7 args...) {
    return buildAssocPredicateBody(join, ..., false);
}
```

`grep -rn buildAssocPredicateBody core/src/main core/src/test` → the only call is the 8-arg form at
`:490`. The 7-arg overload has **zero callers**, and the multi-hop chaining it promises is what
`:616-620` **throws on**.

### LOW — three dead `if (x == null)` branches immediately after `orElseThrow`

`AssociationSynthesis.java:110-111`, `ImplicitInheritance.java:131-134`,
`AssociationSynthesis.java:647-650`. Residue from the `orElse(null)` → `orElseThrow` census; the sweep
changed the call and left the guard.

### LOW — 28 of `AssociationSynthesis.java`'s 60 imports are unused

`NormalizedModel, ParsedModel, ComparisonOp, EnumerationMapping, FilterMapping, FilterPointer,
LogicalOp, MappingDefinition, MappingInclude, PackageableElement, Realization, RelationalDataType,
AppliedProperty, CBoolean, CFloat, CInteger, CString, ColSpec, ColSpecArray, EnumValue,
KeyExpression, NewInstance, NewInstanceCast, PackageableElementPtr, PureCollection, TypeAnnotation,
Collections, HashMap` — each appears exactly once (the import line). **Evidence the "Doors split"
from `MappingNormalizer` was a block move, not a refactor.**

### LOW — stacked/orphaned javadoc: a doc block above the wrong method

`AssociationSynthesis.java:343-352` — the *owner*-class doc is stacked directly above
`associationTargetClass`, which has its own doc immediately after; the real `associationOwnerClass`
(`:361`) has none. Same pattern at `AssociationJoins.java:276-284`, `:994-1004`, `:1044-1048`,
`:1471-1478`. **Javac keeps the last block, so the first is invisible** — a reader scanning the file
gets the wrong contract.

### LOW — over-long methods

`AssociationJoins.associationJoin` **250 lines** (`:821-1070`); `aggJoinMaterial` **129** (`:145-273`);
`corrPredOnJoinedRowCore` ~100 (`:1565-1665`). `AssociationSynthesis.injectMultiHopAssociationPMs`
**158** (`:81-238`); `synthesizeAssociationMapping` **132** (`:396-527`). `AssociationJoins` is 2,099
lines carrying ~9 responsibilities.

### LOW — `XStorePureEnds.java:163-167`'s error message is stale

*"resolves to no Relation or Relational set"* — the method also handles `ClassMapping.Pure`
(`:147-161`), so a class with no set of *any* of the three kinds gets a message naming only two.

### LOW — `resolveAssociation` is called up to 4× per association mapping

`AssociationSynthesis:399`, `:416`, `MappingNormalizer:428`, `:437`. Each is a fresh scope walk.

### MED — misleading diagnostic when an XStore/ModelJoin association name doesn't resolve

`AssociationSynthesis.java:399-415`: with `ad0 == null`, a `Cross` falls through to the third guard
and reports **"Association mapping kind Cross not supported"** — when the actual fault is an
unresolvable association name. The `Relational` arm gets the correct message at `:417-419`; the other
two kinds get a wrong one.

---

## What is genuinely good

- **The `legacyAssocPredicate` typing discipline is real.** Both native signatures
  (`Pure.java:1732, 1737`) are fully parametric; the Relational emission passes actual relation refs
  (`:505-512`) so the lambda's rows type through the ordinary kernel. Verified, not aspirational —
  and the kind of claim most rewrites fudge.
- **The landing-table check is a real invariant, not a comment.** `AssociationSynthesis:570-599`
  verifies the join actually lands on `classB`'s `~mainTable` and throws if not, with the view-target
  carve-out handled explicitly. The check that prevents a silently mistyped lambda.
- **The multi-hop interception is closed at both ends.** `synthesizeAssociationMapping:446-448`
  returns null, and `buildAssocPredicateBody:610-620` throws a compiler-invariant error if anything
  bypasses it.
- **`anchorTableOf` (`:628-665`) handles the embedded-set case correctly and documents *why*** —
  `:145-147` records that `hasMainTable` alone *"misjudged those and regressed 30 inheritance tests
  when tried, 2026-09-02"*. **Negative results captured at the site**, which is how you stop a fix
  from being re-broken.
- **`parentNavSubquery:1337-1352` refuses to silently compare one arbitrary element of a to-many
  hop** and requires an explicit `->toOne()`.
- **The capture/alpha-renaming discipline in `AssociationJoins` is thorough.** `freshenBinders:2054-2085`,
  `collectFreeVars:2027-2050` (genuinely shadow-aware, not global-name-collision), and the `:1418-1431`
  note explaining why the free set must be taken from the *original* pred. A class of bug most
  compilers ship with.
- **`StoreSubstitutionRewrite`'s exhaustive-switch stance works.** `pm` (`:111-149`) and `op`
  (`:151-186`) are total switches over sealed hierarchies, and `collectDatabases:209-231` rides the
  *same* rewrite with a recording map — so a new db-carrying node cannot be added without both the
  rule and the collector. The mechanism is excellent; the gap is that `applyAssoc` was never wired
  into the include path.
- **`ImplicitInheritance.nearestMappedAncestor:199-223`** returns `null` on ambiguity (*"ambiguous —
  stay loud downstream"*) rather than picking one, and refuses to inherit set-qualified routes (`:79-80`).
- **`resolveAllStores` (`StoreSubstitutionRewrite:257-333`)** is a proper Kahn topological pass with
  an explicit cycle error naming the mappings, and rejects duplicated includes before ordering so
  they can't masquerade as a cycle (`:271-277`).

---

## The two highest-value changes

1. **Delete `AssociationSynthesis.resolveAssociation` and `StoreSubstitutionRewrite.qualifyStoreRefs`'s
   scope walk.** Phase D already does both, better (ambiguity detection, own-package tier, core
   imports). Every downstream consumer re-resolves strictly anyway, so neither fallback can deliver
   its stated benefit — and `AssociationSynthesis:110` hard-fails on exactly the input they exist to
   rescue. **Restoring the package contract here is a deletion, not a rewrite.**
2. **Give the Relational association path the direction-agreement wall the XStore path already has**
   (Q6/HIGH), and **write a poison on the three `return null` paths** (Q6/MED) so the resolver's
   existing `.or()` reason channel actually carries a reason.
