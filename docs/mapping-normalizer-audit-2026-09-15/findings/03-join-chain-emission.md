# 03 — Join-chain emission

**Scope read end to end:** `JoinChainEmission.java` (1,085), `ModelJoinNesting.java` (202),
`RelationReads.java` (193), `DynaFnArms.java` (56), `MissProbe.java` (56), plus the `emitJoinChain`
callers in `MappingNormalizer` and `UnionSynthesis`.

---

## HIGH

### H1 — `mintNavSlotAlias` skips the collision check on its non-colliding path — **VERIFIED**

`JoinChainEmission.java:639-650`:

```java
boolean collides = model.knowledge().column(mainDb, tableName, propName).isPresent()
        || propName.equals("columns") || propName.equals(...ROWS_MARKER);
String alias = propName;
if (collides) {
    alias = propName + "_nav";
    while (p.aliasToTargetTable.containsKey(alias)) { alias = alias + "_"; }
}
```

The `while (p.aliasToTargetTable.containsKey(...))` guard lives **inside** the `if (collides)`
branch. When `collides` is false the method returns `propName` without ever consulting
`aliasToTargetTable` — the shared alias space physical hops also occupy. Physical hops mint through
`uniqueSlotName` (`:654-664`), which *does* check. **The two minters are asymmetric.**

The consequence is not a crash. At `:344-349`:

```java
if (emitNavigate) {
    if (p.aliasToTargetTable.containsKey(navAlias)) {
        prevTable = p.aliasToTargetTable.get(navAlias);
        prevAlias = navAlias; continue;
    }
}
```

A pre-existing physical slot named `firm` — a single-hop `@firm` chain from a `JoinTerminalColumn`
PM, a nested `JoinNavigation`, a `~filter` chain, or a `~groupBy` key, since
`String.join("__", ["firm"])` = `"firm"` — makes the class-typed `firm` PM take the `continue`:
**no `legacyNavigate` is emitted and `p.classSlots.add(slotAlias)` at `:453` never runs.**
Downstream, `MappingNormalizer.java:2087-2090`:

```java
String slot = targetIfMapped != null
        ? pipeline.navSlotByProp.getOrDefault(j.propertyName(), j.propertyName())
        : JoinChainEmission.slotFor(pipeline, j.joins());
```

binds the ctor field to `$row.firm` — the **physical sub-row**, where a `Firm` instance was expected.
Order-dependent: the reverse declaration order is safe, because `uniqueSlotName` would yield `firm__2`.

**Why it matters.** This is precisely the silent-misbinding failure the `slotFor` javadoc at
`:673-680` claims was eliminated (*"the old flattened-name fallback let the terminal read silently
bind through ANOTHER chain's slot (audit 18 finding 2)"*). The collision-proof discipline was applied
to the physical slot space only; the class slot space kept both the unchecked mint **and** a silent
`getOrDefault(propName, propName)` reader.

### H2 — The `OtherwiseEmbedded` fallback read ignores `navSlotByProp`, defeating the mint's purpose

Emission mints through the renamer (`:238-242` → `mintNavSlotAlias`), which for a colliding name
produces `<prop>_nav`. The reader does not — `MappingNormalizer.java:2212`:

```java
ValueSpecification fallback = new AppliedProperty(rowBind, oe.propertyName());
```

`navSlotByProp` has exactly two readers repo-wide (`JoinChainEmission:628`,
`MappingNormalizer:2089`); the Otherwise path is not one of them. So an `Otherwise(...)` on a
property whose name matches a main-table column — the exact case `Pipeline.java:29-31` documents:
*"differs when the property name collides with a physical main-table column — the milestoningmap
'exchange' case"* — emits the slot as `exchange_nav` and reads `$row.exchange`, the physical column.
`Pure.Lite.OTHERWISE(partial, <a column value>)`.

### H3 — Per-hop `(INNER)` / `(OUTER)` join-type annotations are silently discarded — **VERIFIED**

`JoinChainElement` carries the type (`model/JoinChainElement.java:12,25`), the parser validates it
(`MappingProtocolParser:1707,1726`), and the model deliberately hangs it on the hop it qualifies —
`MappingFromProtocol.java:549-551`:

```java
// ... the model hangs it on the FilterMapping itself ... and only a
// `> (INNER) @Next` type rides its own hop.
```

`JoinChainEmission` **never reads `hop.joinType()`** — grep over the whole normalizer returns only
`StoreSubstitutionRewrite` (copies it through) and the two `FilterMapping.JoinMediated.joinType()`
sites. Every hop emits the same `Pure.Lite.JOIN_SLOT` / `LEGACY_NAVIGATE` (LEFT semantics). An
`@A > (INNER) @B` chain therefore keeps parent rows the engine drops — silently, with no ledger
poison and no `NotImplementedException`.

This is the same semantics the file takes great care over one layer up: `innerFilteredSource` exists
solely to honour the *filter's* `(INNER)`. The discipline is present on one carrier of the annotation
and absent on the other.

### H4 — `innerFilteredSource`'s null-rejection test checks the condition's *shape*, never that it constrains the *joined side*

`JoinChainEmission.java:928-930` claims:

> *"the null-rejecting WHERE makes LEFT ≡ INNER row-for-row"*

and `:962-974` enforces it with `nullTolerant(fd.condition())`, which (`:1027-1059`) only walks for
`IsNull` / `isNull` / `sqlNull` and throws on `coalesce`/`ifnull`/`nvl`/`case`/`if`.

LEFT + WHERE ≡ INNER requires the WHERE to reject NULLs **on the joined side**. A filter condition
naming only main-table columns — e.g. `~filter [db] @personFirm | db.activeFirms` where
`activeFirms` is `ProductTable.STATUS = 'A'` — is not null-tolerant by this test, so it passes. Then
at `:977-987`:

```java
String terminalTable = p.aliasToTargetTable.get(terminalAlias);
if (terminalTable != null) scope.put(terminalTable, terminalRow);
scope.putIfAbsent(mainTable, r);
```

the condition binds through `r` (main row), the `terminalRow` binding is never referenced, and the
LEFT chain is undemanded — the exact failure the comment at `:978-982` says was fixed (*"binding
root-first silently self-filtered and left the chain undemanded (elided joins, one row per
parent)"*). Result: one row per parent, and parents with no chain match kept, where the engine's
INNER subselect drops them and explodes the rest. **The guard that would close this — "the
translated condition must read the terminal alias" — is not written.**

### H5 — `PINNED_SINGLE` routes are invisible to the shared-prefix/per-arm classifier `routeList` depends on

`UnionSynthesis.java:777-780`:

```java
static List<PropertyMapping.Join> memberJoins(List<UnionRoute> routes) {
    return routes.stream().filter(r -> r.targetOrdinal() >= 0)
            .map(UnionRoute::join).toList();
}
```

`PINNED_SINGLE = -2` (`UnionSynthesis:87`), so pinned-single routes are filtered out.
`uniformChainedRoutes` (`:750-752`) returns `true` on an empty list. `JoinChainEmission:507-508` and
`:260-262` both decide per-arm-vs-shared-prefix from that value.

So a property with two set-pinned routes (`prop[p2]`, `prop[p3]`, neither a union member nor root)
and **divergent multi-hop chains** classifies as "uniform shared prefix". `perArmHops` returns the
full chain of whichever PM was emitted first; that chain's prefix is emitted physically; then
`routeList`'s else-branch (`:562-586`) uses `chain.get(chain.size()-1)` of *each* route against
`prevTable` — the first PM's prefix terminus. Depending on the shapes this is either loud in the
wrong place (`determineTargetTable` → *"references multiple non-source tables"*) or silently the
wrong join condition. `classifyUnionRoutes:326-331` explicitly contemplates several such pins
(*"or several (the queried mapping resolves the pins: engine R-target)"*), so this is not a shape the
classifier may assume away.

---

## MED

### M1 — `routeList`'s `:521` throw lumps two engine-supported shapes with one genuine gap

`ClassMapping` permits `Relational, Pure, Union, RelationFunction, Inheritance`
(`model/ClassMapping.java:41-43`). `:520-523` fires for `Pure`, `Union`, and `Inheritance`:

```java
throw new NotImplementedException("route '" + propName + "[" + j.targetSetId()
        + "]' targets a set that is not Relational; mapping=" + md.qualifiedName());
```

- **`Pure` (M2M)**: genuine gap. A relational `@join` route into an M2M set is meaningless.
- **`Union` / `Inheritance`**: **engine-supported and reachable.** `classifyUnionRoutes` computes
  `memberIds` from the *target class's* union (`:280`), so a PM pinning the union/inheritance **set
  id itself** (`vehicles[vehicleUnion]`) gets `ord = -1`, fails `rootOrSole` (which requires
  `instanceof ClassMapping.Relational`, `:308-312`), and lands as `PINNED_SINGLE` → reaches this
  throw. The engine resolves such a pin through `rootClassMappingByClass` and dispatches the union.
  **A punt on a supported shape, reported as if it were a type error.**

`:528` (*"targets a set with no main table"*) — genuine: `inferMainTableQuiet` already ran.
`:533` (*"pushes a chain into a `~func` member"*) — genuine roadmap gap, honestly explained at `:515-518`.

All three build their messages by string concatenation of `propName` + `targetSetId()`.

### M2 — `plainClassViewCond` is NOT vestigial, but its gate is corpus-fitted by its own admission

The 2026-09-15 self-audit's read is wrong on the first half. The method is load-bearing for a
decision that survived the flattening deletion. In the navigate arm the emission passes *two* things
(`:412-431`): the slot thunk `getAll(targetClassFqn)` and `targetRows`, the row spelling the
condition lambda's `t` types against. When the join lands on a plain rename-only view `V` over table
`T`, and the target class is mapped over `T` (not `V`), the condition as written names `V`'s columns
while the class's row speaks `T`'s. Without the substitution at `:920-921` the condition would not
type. That decision did not die with the flattening fallback.

What *is* questionable is the gate, `:913-919`:

```java
if (sets.size() != 1 || !sets.get(0).groupBy().isEmpty()
        || sets.get(0).distinct()
        || MilestoningFacts.isTemporal(targetClassFqn, model)) {
    return null;
}
```

with the comment at `:908-912`: *"…those keep the expansion (sweep-proven: unionOfViews +
milestoned-view regressions under the looser gate)."*

`isTemporal` has no principled connection to whether the condition should speak view or physical
columns. **The code says plainly that these clauses were derived by narrowing until the corpus went
green, not from a rule.** That is a gate that will fail open or closed on the first shape outside the
sweep. Likewise `vPhys.equals(tgtMain)` at `:905` reduces a structured question to string equality on
table names.

Residue is present but confined to narration: `:899-901` still explains the rule by contrast with
*"the old flattening"*.

### M3 — The routed-navigate arm computes and discards an entire hop emission, and that dead work can throw

When `routes != null` at `:440`, the branch emits `routeList(...)` and never uses `targetRows`
(`:424-431`) or `navCond` (`:438`). But everything feeding them already ran unconditionally:
`findJoin` (`:367`), `resolveViewRefsInJoin` (`:377`), `declaredSpelling` (`:383`),
`plainClassViewCond` (`:391-395`), `hopTarget`/`determineTargetTable` (`:396`), and
`RelOpTranslator.translate` (`:408`). `determineTargetTable` throws `NotImplementedException` on a
multi-table condition and `ModelException` on a table-less one; `translate` throws on an
out-of-scope `ColumnRef`. **So a routed property can fail loudly on a hop condition the emitted
output never contains.** In the per-arm case this is the whole of the discarded work: `perArmHops`
truncated `hops` to `hops.get(0)` (`:256-262`) and `routeList` rebuilds that hop itself inside each
route (`:544-561`).

Also "just in case": `:468-470` registers `aliasToTargetTable` / `aliasToTargetColumns` for the nav
slot using the *non-routed* `targetTable` — a fiction for a routed navigate.

### M4 — The "named relation of a db as an expression" rule has four copies, one already extracted

`relationRef` (`:493-500`) is the extraction. Three sites don't use it: `:424-431` (navigate
`targetRows`), `:455-459` (physical `targetRel`), `:567-577` (inside `routeList`, with the `rfMember`
case folded in). All four are
`findView(db,n).isPresent() ? viewRelationExpr(...) : new AppliedFunction("tableReference", …)`.
`innerFilteredSource:947` uses `relationRef` correctly, which proves the seam works.

### M5 — `ModelJoinNesting:72-77`: unreachable guard whose comment describes behaviour the code no longer has

```java
AssociationDefinition nad = model.findAssociationOf(endCls, prop)
        .orElseThrow(() -> MissProbe.neverFired("ModelJoinNesting#1"));
if (nad == null) {
    continue;   // not an association hop — the rewrite's ordinary loud error names it
}
```

`orElseThrow` makes `nad == null` impossible. The comment describes a *graceful skip* for a
non-association two-hop read; the code now throws `IllegalStateException`. But `collectNestedHops`
(`:180-200`) collects **any** `$var.x.y`, not only association hops — so a ModelJoin body containing
a two-hop read through a non-association mid now dies with a census-provenance message instead of the
intended `RelationReads` error naming the property. Same unreachable-guard shape at
`JoinChainEmission:99, :127, :167, :1074` and `RelationReads:117, :145`.

### M6 — `Set<String[]>` cannot deduplicate

`ModelJoinNesting.java:58`: `Set<String[]> hops = new LinkedHashSet<>();` — arrays use identity
`equals`/`hashCode`, and `collectNestedHops` adds a fresh `new String[]{...}` per occurrence
(`:185`). The set is a list. The duplicate is caught only incidentally at `:67-70` by
`nestedCols.getOrDefault(var, Map.of()).containsKey(prop)`, which is populated at the *end* of the
loop body (`:143-144`) — so the masking depends on iteration order. A
`record NestedHop(String var, String prop)` makes the set do its job.

### M7 — `MappingNormalizer:2339-2345` is unreachable, and its comment claims a mechanism that now exists

See `01-mapping-normalizer-core.md` §3. `synthTableBackedParts:1767-1777` intercepts
`joinType() != null` *before* `applyFilter` is ever called. The row-exploding emission *was* built;
this is residue asserting it wasn't.

### M8 — Comment claims Pass 2 hoists chains from `OtherwiseEmbedded` fallback bodies; it does not

`MappingNormalizer:1808-1811` says Pass 2 hoists from *"…Embedded sub-PMs, **OtherwiseEmbedded
eager/fallback bodies**, groupBy keys…"*. `JoinChainEmission:767-768`:
`case PropertyMapping.OtherwiseEmbedded oe -> collectJoinNavigationsInPms(oe.embedded(), out, md);`
— `oe.fallback()` is never visited. No live consequence today (the fallback is constrained to a
`Join` at `:210`, which carries no nested `JoinNav`), but the doc overstates coverage.

---

## LOW / residue

- **L1 — 27 of ~50 imports unused** in `JoinChainEmission.java`: `SynthFqn, Multiplicity,
  NormalizedModel, ParsedModel, AssociationDefinition, AssociationMapping,
  AssociationPropertyMapping, ComparisonOp, EnumerationMapping, FunctionDefinition, LogicalOp,
  MappingDefinition, PackageableElement, Realization, RelationalDataType, SynthHat, CBoolean,
  CFloat, CInteger, EnumValue, KeyExpression, NewInstance, NewInstanceCast, TypeAnnotation,
  Collections, HashMap, HashSet`. Over half the import block is sediment.
- **L2 — Four orphaned javadocs.** Three stranded above the wrong method: `:477-490` (the 14-line
  `routeList` doc, separated from `routeList` at `:502` by `relationRef`); `:598-606`
  (`uniqueSlotName`'s doc; the method is at `:654`); `:607-613` (`mintNavSlotAlias`'s doc; the
  method is at `:626`). All three now attach to `recordNavSlotOwner` (`:618`). The fourth is worse —
  `:1081-1084` is a dangling javadoc at end-of-class with **no member after it**, documenting a
  deleted routed-navigation collapse.
- **L3 — Dead overload.** `collectJoinNavigationsInPms(List<PropertyMapping>, List<JoinNavSpec>)`
  (`:751-754`) has zero callers; every call site passes `md`.
- **L4 — Write-only field.** `Pipeline.ownerSet` is assigned at `:948` and `MappingNormalizer:1791`
  and read nowhere, while its javadoc (`Pipeline:60-62`) claims a live role.
- **L5 — Dead ternaries and an always-true guard.** `:309` and `:437` (`propName == null ? null : …`
  inside branches that imply `propName != null`); `:382` (`if (prevTable != null)` — `prevTable`
  starts as the non-nullable `mainTable`).
- **L6 — Double view-substitution over one condition.** `:377` resolves with
  `(backingView, backingView)` (PASS-1, unguarded); `hopTarget:847` resolves the *result* again with
  `(backingView, null)` (PASS-2, guarded). Two passes with different modes over the same expression,
  order-dependent, intermediate never named. *(Both are moot today — `backingView` is always null;
  see `01-mapping-normalizer-core.md` §3.)*

---

## On the engine citation for `innerFilteredSource`

`MappingNormalizer:1761-1763` cites *"getRelationalElementWithInnerJoin, pureToSQLQuery.pure:5077;
chosen at :5101"*. **The function and the choose-site are real; the line numbers cannot be verified
at the pinned version from this machine.** The local checkout is `legend-engine-4.137.0-36`, not the
pinned 4.145.0; at 4.137 the sites are **4745** and **4771**. The 24-line gap in the citation matches
the 26-line gap observed, consistent with the pair having drifted together. See
`10-engine-citations.md` for the full adjudication.

**The semantics the citation justifies are accurate.** At 4.137,
`getRelationalElementWithInnerJoin` builds a subselect rooted at the main table, applies the filter,
and projects **every main-relation column under its own name from the root alias**, with **no
DISTINCT** — exactly the shape `innerFilteredSource` emits (`:1019-1020`), including the row
explosion. The choose-site is as described. **The gap is not the shape — it is H4:** the engine gets
INNER from the join tree node, while lite gets LEFT + a WHERE whose null-rejection is only
shape-tested.

---

## What is genuinely good

- **The structured `pathToSlot` key.** `Pipeline:28` maps `List<String>` (ordered join names), not a
  flattened string, and `slotFor` (`:681-690`) throws on a miss instead of falling back. Chain
  `[A, B]` and a join literally named `A__B` stay distinct, and an unhoisted chain is loud. **This is
  the model the class-slot side should have copied (H1).**
- **Self-join handling via the `{target}` marker.** `determineTargetTable:2721-2724` returns
  `sourceTable` when the condition carries a `TargetColumnRef`; `emitJoinChain:407` then deliberately
  *omits* the `t` binding, so plain `ColumnRef`s bind to the source row and `{target}.COL` binds to
  `t` (`RelOpTranslator:238-245`). The two sides disambiguate **structurally** rather than
  positionally. A self-join with no `{target}` is loud, not guessed.
- **`seedAliasScope` uses `putIfAbsent`** (`MappingNormalizer:2569`), so `innerFilteredSource`'s
  deliberate terminal-first binding survives — the one ordering subtlety the comment calls out is
  actually enforced.
- **Ambiguous-table refusal.** `seedAliasScope:2560-2568` leaves a non-main table reached by two
  sub-rows *unbound* and records it; `RelOpTranslator:126`/`:229` throw a message telling the author
  how to fix it.
- **`nullTolerant`'s throw-on-unclassifiable arm** (`:1039-1048`): `coalesce`/`ifnull`/`nvl`/`case`/
  `if` are loud rather than being given a silent verdict. The *shape* of that guard is exactly right
  — its scope is what's wrong (H4).
- **Operand order is preserved.** `RelOpTranslator`'s `Comparison` arm translates `left` and `right`
  independently with no canonicalisation, and `translateArgs:162-167` keeps join-condition operands
  verbatim rather than applying the `toOne` conform that property expressions get.
- **`uniformChainedRoutes` is one predicate with one owner.** `UnionSynthesis:750` is called from
  both the emitter (`:260`, `:507`) and the inbound key collector, documented at `:339-345` as
  *"never allowed to drift"*. Its defect (H5) is in what it's fed, not in duplication.
- **`collectJoinNavigations` vs `collectJoinNavigationsInPms` are NOT duplicate owners.** They
  traverse different types — one the `RelationalOperation` tree, one the `PropertyMapping` tree — and
  mutually recurse at the boundary (`:763-770`). Correct decomposition; only the dead 2-arg overload
  (L3) is an issue.

---

## Method shape

`emitJoinChain` (182 lines) carries ten jobs: routed-set retargeting **with a mutation of shared
pipeline state** (`:321 p.unionRoutes.remove(propName)`), per-arm truncation, two different dedup
rules, two different naming rules, view resolution ×2, target-table determination, condition scoping,
and two emission arms that duplicate their `targetRows` construction. The tell is that the two arms
differ only in which of three near-identical relation expressions they build and which function
symbol they apply.

`emitHopsForStructuralPm` (127 lines) is better — one dispatch — but its `Embedded` /
`OtherwiseEmbedded` / `InlineEmbedded` arms (`:102-119`, `:130-148`, `:175-191`) are three copies of
the same twelve-line body (collision guard + `recordNavSlotOwner` + recurse) differing only in the
owner-class expression and the exception wording.
