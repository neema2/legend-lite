# 01 — MappingNormalizer core

**Scope read end to end:** `MappingNormalizer.java` (2,910), `Pipeline.java` (133),
`ModelNormalizer.java` (363), `package-info.java` (67).
**Five findings were proven by execution** against `core/target/classes` (probes written to a
scratch dir, nothing committed into the repo).

---

## 1. Correctness

### HIGH — `MappingValidation`'s verdict is silently discarded whenever a mapping has a multi-hop/routed association — **PROVEN**

`MappingNormalizer.java:266`, `:310`, `:354`

```java
266:  ResolvedMapping md = pp.withMapping(AssociationSynthesis.injectMultiHopAssociationPMs(pp, model));
...
310:  String invalidSet = pp.invalid().get(cm);      // cm comes from md.classMappings()
354:  String invalid    = pp.invalid().get(cm);      // cm comes from md.classMappings()
```

`pp.invalid()` is an **`IdentityHashMap`** keyed by the *pre-injection* `ClassMapping` objects
(`MappingPrePass.java:68`: `Map<ClassMapping, String> reasons = new java.util.IdentityHashMap<>();`,
filled from `MappingValidation.java:53-63`). `ResolvedMapping.withMapping` (`:52-54`) carries that
map across the rewrite unchanged — and its own javadoc names the operation that breaks it:
*"The same record over a rewritten mapping (a construction step, the multi-hop injection)."*
`AssociationSynthesis.java:211` rebuilds exactly the sets that receive injections:

```java
ClassMapping.Relational injectedCm = withInjectedPMs(cm, byClass, bySet, lineageOf(cm, view));
rewritten.add(injectedCm != null ? injectedCm : cm);
```

So for any class whose set gets an injected PM, the identity key no longer matches and
`pp.invalid().get(cm)` returns `null`.

**Proven.** Same invalid mapping, twice (module/tolerant build, `wallSink != null`):

```
==== invalidOnly                      (no association mapping)
MAPPING my::M poisons={model::P=PropertyMapping 'nope' references property not declared on class 'model::P'; mapping=my::M}
   binding model::F set=null                       <-- model::P correctly withheld

==== invalidPlusMultiHopInjection     (identical, + a 2-hop AssociationMapping on model::P)
MAPPING my::M poisons={}                            <-- poison GONE
   binding model::P set=null                        <-- invalid set BOUND anyway
   binding model::F set=null
```

**Why it matters.** This is the exact failure mode the poison ledger exists to prevent: a set the
validator rejected becomes a live binding with no recorded reason. Downstream it either
type-checks into a wrong shape or blows up far from the cause. It is also silently *conditional on
an unrelated feature* (whether the mapping happens to declare a multi-hop association), which makes
it nearly untestable by accident. Strict builds are spared only because `MappingPrePass.java:64-66`
throws first.

### HIGH — the M2M class-typed cycle guard is exactly backwards — **PROVEN**

`MappingNormalizer.java:1245-1249` (guard), `:1150`/`:1220-1222` and `:1253-1257` (the stack)

```java
1245:  if (!cycleStack.add(innerFqn)) {
1246:      throw new ModelException(LegendCompileException.Phase.NORMALIZE,
1247:              "Cycle materializing M2M class-typed property; class "
1248:            + innerFqn + " recurses. Stack=" + cycleStack);
1249:  }
1250:  try {
1253:      return new NewInstanceCast(innerFqn, List.of(), pb.expression(), pb.targetSetId());
1255:  } finally { cycleStack.remove(innerFqn); }
```

`m2mPropertyValue` **does not recurse** — it emits a `NewInstanceCast` and returns. `synthM2M` is
only ever entered with a fresh `HashSet` (`:795`). So `cycleStack` can hold at most
`{ownClass, innerFqn}`, and the guard can fire on one condition only: `innerFqn.equals(ownClass)` —
an ordinary self-referential property (`Person.manager: Person`, `Employee.boss: Employee`).

**Proven, both halves:**

```
==== m2mSelfRef    (tgt::TP.boss : tgt::TP[0..1])
THREW ModelException: Cycle materializing M2M class-typed property; class tgt::TP recurses. Stack=[tgt::TP]

==== m2mMutualRef  (tgt::TA.b : TB, tgt::TB.a : TA  — the actual cycle)
FN my::M$class$tgt::TA OK
FN my::M$class$tgt::TB OK
```

**Why it matters.** A commonplace M2M shape is walled with a misleading message, while the shape
the guard advertises passes clean.

> **Cross-reference / adjudication.** This audit originally concluded that `materializeEmbedded`'s
> guard at `:2165-2169` was *sound* ("wraps a genuinely recursive descent"). Audit 15 disagreed.
> **The call sites settled it in audit 15's favour:** all three callers pass a fresh
> `new HashSet<>()` (`:2125`, `:2212`, `:2248`), and the recursion runs `materializeEmbedded` →
> `translatePmToField` (`:2187`) → `materializeEmbedded` (`:2123`, fresh set again), so
> `cycleStack` is never threaded and the guard can never fire. The descent *is* recursive; the
> threading defeats the guard. See `15-m2m-json-enum.md` H6.

### HIGH — `~distinct` falls back to a raw-row `distinct()` that the surrounding comment says is wrong — **PROVEN**

`MappingNormalizer.java:1882-1917`

```java
1882: // Apply ~distinct. Engine semantics: DISTINCT over the MAPPED
1883: // columns, not the raw physical row (the table's unmapped PK would
1884: // defeat the dedup) — the source narrows to a select of exactly the
1885: // columns the PMs consume.
...
1915:  } else {
1916:      p.expr = new AppliedFunction("distinct", List.of(p.expr));
1917:  }
```

`mappedCols` is empty whenever no PM is a plain `Column`/`EnumeratedColumn`/`Expression`/
`LocalProperty` — `collectMappedColumns` (`:1942-1957`) sends `JoinTerminalColumn`, `Join`,
`Embedded`, `InlineEmbedded`, `OtherwiseEmbedded` to `default -> return false` **without adding a
column**.

**Proven.** A `~distinct` class whose two properties are both join-terminal reads:

```
distinct( joinSlot( tableReference(db::DB,"PT"), ... ) )
  -> map(row | ^model::P(name = $row.PF.LEGALNAME, firmName = $row.PF.LEGALNAME))
```

The `distinct` sits over the full physical row (`PT.ID` primary key, `PT.NAME`, `PT.FID`, plus the
slot). `PT.ID` is unique, so the dedup is a no-op and every `PT` row survives — precisely the
"unmapped PK defeats the dedup" case the comment warns against. **Silently too many rows**, no
wall, no poison.

### HIGH — `~groupBy`'s "withhold the property" rule is not implemented — **PROVEN**

`MappingNormalizer.java:1926-1933` + `:2059-2062`, contradicting `GroupBySynthesis.java:181-188`

`GroupBySynthesis.applyGroupBy` deliberately emits *no* ColSpec for a non-key, non-aggregate PM:

```java
// GroupBySynthesis.java:181-188
// a PER-ROW PM outside the key list ... WITHHOLD the property, keep the
// set — a read raises the ordinary not-mapped wall, loud.
continue;
```

But the terminal loop binds **every** PM regardless, and under `~groupBy` the translator returns a
bare row read named after the property:

```java
1926:  for (PropertyMapping pm : rcm.propertyMappings()) {
1927:      if (p.droppedRoutedProps.contains(pm.propertyName())) continue;   // the ONLY skip
1930:      CtorField cf = translatePmToField(pm, ..., !rcm.groupBy().isEmpty());

2059:  if (underGroupBy) {
2060:      return new CtorField(pm.propertyName(),
2061:              new AppliedProperty(rowBind, pm.propertyName()), false);
2062:  }
```

**Proven** on the exact model from the repo's own test (`MappingNormalizerTest.java:3800`,
`~groupBy(T.K)` with `k: T.K`, `extra: concat(T.K,'_x')`, `total: sum(T.QTY)`):

- `groupByComputedKeys` projects ColSpecs `[k]` and `[total]` — **no `extra`**.
- The `^model::P(...)` terminal binds `extra = trustOne($row.extra)`.

The existing test `groupByPerRowFormulaOutsideKeyWithheld` asserts only
`poisonReasons(parsed).isEmpty()` — it never checks that `extra` is absent from the constructor, so
it passes a body that reads a non-existent column. This is the "green check that only proves
well-formedness" pattern.

### MED — the `anySide` short-circuit is evaluated *after* a call that can throw

`MappingNormalizer.java:2613-2620`

```java
2613:  String phys = ViewRelation.inferViewMainTable(view, cr.table(), md, model, crDb);
2614:  if (anySide
2615:          ? (cr.table().equals(backingView)
2616:             || cr.table().equals(sourceTable)
2617:             || cr.table().equals(keepTargetView))
2618:          : !viewChainReaches(phys, sourceTable, crDb, md, model)) {
2619:      yield cr;
2620:  }
```

In `anySide` mode `phys` is **unused**, but `inferViewMainTable` throws `ModelException` on a view
with zero or >1 root tables (`ViewRelation.java:468-479`). A frame reference the `anySide` branch is
specifically written to yield verbatim therefore still walls if its view has multiple roots.
`phys` should be computed lazily inside the non-`anySide` arm.

### MED — strict-build policy differs between the per-class and per-association arms of one decision

`MappingNormalizer.java:343-347` / `:362-370` vs `:417-426`

Class arms record only user-model errors:

```java
343:  } catch (NotImplementedException | ModelException e) {
345:      if (e instanceof ModelException) { ledger.strictErrors.add(e); }
```

matching the stated rule at `:368-371` — *"only a MODULE build defers it, and a ROADMAP gap defers
in both."* The association arm records **everything**:

```java
417:  } catch (NotImplementedException | ModelException e) {
426:      ledger.strictErrors.add(e);
```

and `:192-194` throws `strictErrors.get(0)` in a strict build. So a roadmap gap
(`NotImplementedException`) in an *association* sinks a strict build while the same gap in a
*class* does not. The comment at `:418-425` argues both sides within seven lines. One owner, two answers.

### MED — per-SET poisons are written under a key nothing ever reads

`MappingNormalizer.java:312` and `:348`. See `02-stamped-facts.md` HIGH-3 for the full trace.
For a *union member* set the class-level poison at `:304` is explicitly skipped
(`if (!unionMember)`), so a member set that fails synthesis records its reason **only** under the
unreadable key — the reason is lost and the later resolve fails generically.

### LOW — `translatePmToField` bypasses the minted nav slot under `~groupBy`

`MappingNormalizer.java:2059-2062` vs `:2085-2094`. The non-grouped `Join` arm resolves through
`pipeline.navSlotByProp` (which mints an alias when the property name collides with a physical
column — the `milestoningmap 'exchange'` case named at `Pipeline.java:29-32`). The `underGroupBy`
early return at `:2060` reads `$row.<propertyName>` directly, so a grouped set with a colliding
Join property reads the wrong field.

### LOW — `navSlotByProp.getOrDefault(propName, propName)`

`MappingNormalizer.java:2088-2090` — if the navigate slot was never emitted this falls back to the
property name as a slot name rather than failing. A silent default in the middle of the
"no fallbacks" arc. (This is the reader half of `03-join-chain-emission.md` H1.)

---

## 2. Fallbacks / silent defaults

### MED — enum-value validation is skipped entirely when the enumeration can't be found

`MappingNormalizer.java:2498-2512`

```java
2498: // An entry naming a NON-EXISTENT enum value is a COMPILE error —
2501: // turned typos into NULL rows in [1] slots (audit).
2502:  List<String> knownValues = model.findEnum(em.enumName())
2503:          .map(EnumDefinition::values).orElseGet(MissProbe::miss);
2506:  if (knownValues != null && !knownValues.contains(ev.enumValue())) {
```

`MissProbe::miss` is literally `return null` (`MissProbe.java:45-47`). When the enumeration is
unknown the `knownValues != null` guard turns the whole strictness claim off — the exact
"typos become NULL rows" outcome the comment says was fixed, now conditional on a lookup miss.

### MED — `mainTableDefOf` falls back to the first-declared set when no set is root

`MappingNormalizer.java:2394-2413`. The comment (`:2390-2393`) diagnoses first-wins as the original
bug — *"taking the FIRST declared set bound predicates to the wrong table"* — and the body still
does it as the no-root fallback (`:2406-2408`, `:2411-2413`).

### LOW — unresolvable association binds under its raw declared name

`MappingNormalizer.java:427-431` and `:436-439` — the identical two-line
`resolveAssociation(...).map(...).orElse(am.associationName())` expression duplicated for the poison
key and the binding key. Whether those two keys agree is an accident of the same fallback firing twice.

### LOW — `inferMainTableQuiet` catches and drops a `ModelException`

`MappingNormalizer.java:1521-1529`. Honestly named and documented as a probe, but its six callers
(`AssociationSynthesis:656`, `JoinChainEmission:526`, `UnionSynthesis:518,520`,
`ImplicitInheritance:59,212`, `hasMainTable:2362`) then treat "ambiguous across tables" as
indistinguishable from "no table". See `14-guards-fallbacks-census.md` BUG-5.

### LOW — `resolvedStores.getOrDefault(..., Map.of())`

`MappingNormalizer.java:187-188` — `resolveAllStores` throws unless it produced an entry for every
mapping (`StoreSubstitutionRewrite.java:323-330`), so this default can never fire. Dead
defensiveness that reads as a permitted miss.

### Correctly distinguished (not findings)

`PropertyMapping.OtherwiseEmbedded.fallback()` (`:2003-2010`, `:2205-2215`) is the **domain** term
and is handled properly; `fallbackSetId` is `Objects.requireNonNull`-guarded in the record.

---

## 3. Dead code / residue

### MED — `Pipeline.backingView` is **always null**

`Pipeline.java:47-52` declares it; **every** construction site passes `null`:

- `MappingNormalizer.java:1413` → `synthTableBackedMapping(..., null, null)`
- `MappingNormalizer.java:1699` → `synthTableBackedMapping(..., /*backingView*/ null, viewSource)`
  — the *view-backed* path, the one case the field documents, explicitly passes null
- `JoinChainEmission.java:947` → `new Pipeline(..., null, ledger)`
- `Pipeline.java:90` (`forView`) → null

Consequences: `JoinChainEmission.java:376-378`'s `p.backingView == null ? jd.operation() : …` has a
permanently-dead else-branch whose comment claims *"PASS 1: the class's BACKING view always
substitutes to its physical expressions"*; `JoinChainEmission.java:848` always passes null; the
`backingView` parameter on `synthTableBackedMapping` and **both** `synthTableBackedParts` overloads
is dead plumbing; every `onlyView` branch in `resolveViewRefsInJoin` is unreachable.

### MED — `Pipeline.ownerSet` is write-only

`Pipeline.java:60-63` — assigned at `MappingNormalizer.java:1791` and `JoinChainEmission.java:948`,
**read nowhere**. Its javadoc claims it is *"the NAVIGATING set a routed navigation's link keys are
named by (B3.1b)"*. Two of `Pipeline`'s ten members are dead.

### MED — `mixedUnionRooted`: the mixed-kind-union rule computed twice, one copy discarded

`MappingNormalizer.java:279-290`

```java
279:  Set<String> mixedUnionRooted = new HashSet<>();
283:  // MIXED-KIND (route b): resolver arms need PER-SET bindings
284:  for (String sid : un.memberSetIds()) {
285:      if (md.set(sid) instanceof ClassMapping.Pure) {
286:          mixedUnionRooted.add(un.className());
287:          break;
288:      }
289:  }
```

`mixedUnionRooted` is **never read**. The real classification lives in `UnionSynthesis.java:385-392`
(`ledger.mixedUnions.put(...)`), which `PureModelContext.java:352` consumes — and which uses a
*wider* set lookup (`bySetId` seeded from `md.includedSets()`) than this dead copy's `md.set(sid)`.

### MED — five methods with zero callers repo-wide

Verified by grep across main + test:

- `nullOfPhysicalKind` — `:1330-1352` (23 lines + javadoc)
- `nullOfDeclaredType` — `:1354-1377` (24 lines)
- `collectColumnsOfTable` — `:1266-1294` (only self-recursive)
- `collectTargetColumns` — `:1296-1322` (only self-recursive)
- the 4-arg `synthTableBackedParts` overload — `:1737-1742`

~110 lines of unreferenced code carrying confident doc comments.

### MED — unreachable wall + comment describing an algorithm that was since built

`MappingNormalizer.java:2333-2345`

```java
2333: // The absorption theory (LEFT slot + WHERE ≡ INNER) was REFUTED ...
2338: // row-exploding emission is built.
2339:  if (jm.joinType() != null) {
2340:      throw new NotImplementedException("mapping ~filter with an"
2342:          + " row-explodes through to-many chains — not built yet; ...
```

`synthTableBackedParts:1767-1776` intercepts exactly `FilterMapping.JoinMediated` with a non-null
`joinType` **before** `applyFilter` is ever called (`:1841-1843` is the sole call site; `applyFilter`
is `private`), recursing with the filter nulled and `JoinChainEmission.innerFilteredSource`
(`:932`) as the source. The emission **was** built; the guard is unreachable and the comment is stale.

### MED — stale header comment on `~distinct`

`MappingNormalizer.java:1886-1887` — *"slot-carrying distinct mappings stay the H3-pending wall
downstream"* — while `:1900-1914` implements the slot-carrying case in full, with its own
contradicting explanation.

### MED — orphaned / mis-attached javadoc (12 sites)

Mechanically detected, each verified.

| file:line | residue |
|---|---|
| `MappingNormalizer.java:407-410` | truncated mid-clause: *"…whose set-routed property targets a"* then straight into `List<…> assocBindings`; stray 9-space indent on `:410` — botched deletion from `b0e01458b` removing `resynthesizeIncluded` |
| `:553-558` | javadoc for `liftClassInline` stranded 70 lines above it |
| `:693-699` | orphan — the target-side read rewriter, now in `UnionSynthesis` |
| `:702-708` | orphan — the member-ordinal helper, also moved to `UnionSynthesis` |
| `:1324`, `:1327` | two orphan one-liners — bitemporal/temporal predicates, now `MilestoningFacts` |
| `:1379` | orphan — *"The declared multiplicity of `prop` on `owner` (chain walk)"* |
| `:1665` | **broken `{@link #inferViewMainTable}`** — the method is `ViewRelation.inferViewMainTable` |
| `:1702-1711` | orphan `inferViewMainTable` javadoc, no member under it |
| `:1985-1988` | orphan — the `~primaryKey` column-names doc, stranded on `propertyPinsOf`'s javadoc |
| `:2765-2770` | orphan — the multi-path bare-column-ref note |
| `:2772-2779` | orphan — describes `RelOpTranslator.translate`, moved out |
| `:2794-2813` | **mis-attached**: a 20-line javadoc for `buildNewInstanceToOne` sits on `simpleTypeName` (`:2814`); the method it documents is at `:2823` |
| `ModelNormalizer.java:201-208` | orphan — the derived-property lift moved to `com.legend.compiler.DerivedProps` |

Plus **four empty section banners** with no code under them: `:688-691`, `:1260-1264`,
`:2036-2042` (two consecutive), `:2350-2352`. And `nameRefOrNull` (`:715-717`) sits under the
banner *"Pre-pass: inject multi-hop association ends"* while being a generic helper used only by
`AssociationSynthesis`.

### LOW — dead null-checks left by `orElse(null) → orElseThrow` conversions

- `ModelNormalizer.java:161-163` — `orElseThrow(...)` then `owner == null ? null : …`
- `MappingNormalizer.java:2467-2469` — same shape
- `MappingNormalizer.java:2436` — `p == null ? ... : p.view()`; the sole caller (`:2076`) always
  passes non-null

### LOW — unused parameters

`MappingLedger.java:84-89`: `facts(surface, md, model)` uses only `surface`; `md` and `model` are
ignored. Call site `MappingNormalizer.java:454` dutifully computes and passes `md.raw()` and `model`.

---

## 4. Typing vs string hacking

### MED — `MappingLedger.poisons` is one `Map<String,String>` carrying three key kinds

`MappingLedger.java:32-34` admits it: *`"class"`, `"class[setId]"` or an association FQN*. Writers:
`MappingNormalizer.java:304` (class), `:312`/`:348` (bracketed), `:356`/`:377` (class), `:427`
(association FQN). Readers do bare `get(classFqn)`. A sealed `PoisonKey` would have made the
unread-key defect a compile error.

### MED — AST nodes ordered by `toString()`

`MappingNormalizer.java:748-753`

```java
&& ps.get(0).toString().compareTo(ps.get(1).toString()) > 0
```

Canonical operand ordering for `and`/`or` — and therefore the XStore direction-agreement wall
(`:1034-1042`, `XStorePureEnds.java:221-225`) — depends on record `toString()`, a debugging
facility, not a stable structural key. A new record component or a reordered field silently changes
which mappings wall.

### MED — primitive-type identification by a hardcoded name set + FQN prefix string

`MappingNormalizer.java:2819-2849`

```java
2819: private static final Set<String> PRIMITIVE_TYPE_NAMES = Set.of(
2820:     "Integer", "String", "Float", "Boolean", "Decimal", "Number",
2821:     "StrictDate", "DateTime", "Date");
...
2847: : ptName.startsWith("meta::pure::metamodel::type::")
2848:     && PRIMITIVE_TYPE_NAMES.contains(simpleTypeName(ptName))
```

`simpleTypeName` (`:2814-2817`) is `lastIndexOf("::")` FQN parsing. `ModelNormalizer.java:272`
and `:292` show the typed alternative already exists (`Pure.BOOLEAN.qualifiedName()`). The comment
at `:2839-2841` correctly worries about a user class named `model::Integer` — and then guards it
with a string prefix.

### MED — an eight-parameter method with five mode knobs and a boolean that reverses its central test

`MappingNormalizer.java:2595-2601` — `resolveViewRefsInJoin(op, db, sourceTable, model, md,
backingView, onlyView, keepTargetView, anySide)`: four nullable `String`s each meaning a *different*
view role plus a boolean that swaps the entire source-side predicate (`:2614-2618`). Twelve
recursive call sites (`:2647-2673`) re-spell all eight arguments.

### LOW — `cand.enumName().replace("::", "_")`

`MappingNormalizer.java:2458-2459` — the implicit enum-mapping id derived by string substitution.
Cited as engine parity (`HelperMappingBuilder:348-351`), so defensible, but it is an identity
synthesized by `String.replace` and compared with `equals`.

### LOW — `hasSlots()` returns a constant

`Pipeline.java:107-109` returns `true` unconditionally; `RelOpTranslator.PipelineView.NONE` returns
`false`. A hand-rolled type discriminator on a two-implementation interface.

---

## 5. Duplication

### MED — the ClassBinding construction ladder written twice, 20 lines apart, drifting

`MappingNormalizer.java:319-342` (non-root, set-discriminated) and `:385-405` (root). Same three-arm
`Relational | isOperation | Pure` ternary, same `declaredKeys.getOrDefault(SetKeyFacts.setKey(rSrc),
…NONE)`, same `ledger.operationMembers.getOrDefault(idOf(cm), List.of())`. Differences: the `root`
flag, and `List.of()` vs `AggregateViewLift.facts(rSrc)` for aggregate facts. **Whether that second
difference is intentional or an omission is unknowable from the code** — which is the cost of the
duplication.

### MED — the 13-field `ClassMapping.Relational` rebuilt positionally, and the two copies disagree

15 sites repo-wide. Two are in scope, ~370 lines apart, both "copy with one field changed":

```java
1400:  rcm = new ClassMapping.Relational(rcm.className(), rcm.setId(),
1401:          rcm.extendsSetId(), rcm.root(), inferred, rcm.filter(),
1402:          rcm.distinct(), rcm.groupBy(), rcm.primaryKey(),
1403:          rcm.propertyMappings(), rcm.sourceUrl(),          // <-- passed through
1404:          rcm.propertyTargetSets(), rcm.aggregation());

1770:  ClassMapping.Relational noFilter = new ClassMapping.Relational(
1771:          rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
1772:          rcm.mainTable(), null, rcm.distinct(), rcm.groupBy(),
1773:          rcm.primaryKey(), rcm.propertyMappings(), null,   // <-- sourceUrl DROPPED
1774:          rcm.propertyTargetSets(), rcm.aggregation());
```

`:1770` is named `noFilter` and nulls **two** fields (positions 6 and 11). The `sourceUrl` null is
silent and unexplained. It happens to be harmless only because `synthRelational:1388` routes
`sourceUrl != null` away first — an invariant living three call frames up.
`ClassMapping.java:228-230` explicitly refuses a short overload (*"a defaulted `propertyTargetSets`
silently dropped `prop[setId]` routing"*), which is the right instinct; the missing piece is a
`with*` wither.

---

## 6. Method / class shape

- **`normalizeMapping` — `:257-455`, 199 lines, at least seven jobs**: multi-hop injection, per-class
  set counting, union/mixed-union classification, per-set synthesis + fault isolation, per-class
  synthesis + fault isolation, aggregate-view lifting, association synthesis + fault isolation,
  final `MappingDefinition` assembly. Nesting peaks at six levels with a **bare block** `{ … }` at
  `:299-351` whose comment (`:296-298`) explains what the *removed* `if` used to guard.
- **`synthTableBackedParts` — `:1751-1935`, 185 lines**: inner-filter interception + recursion,
  three emission passes, filter, groupBy, a deferred stage-2 navigate loop, `~distinct` with three
  branches, alias scoping, the terminal field loop. The three `~distinct` branches alone
  (`:1888-1918`) are a self-contained policy decision that wants its own method — and one of those
  branches is silently wrong (§1).
- **`resolveViewRefsInJoin` — `:2580-2675`**: a 6-arg overload delegating to an 8-arg one, twelve
  full-argument recursive call sites.
- **Overload ladder with a dead rung**: `synthTableBackedParts` 4-arg (`:1737`) has no caller.
- **Test-only production API**: `normalize(ParsedModel, ModelBuilder)` (`:144-146`) is called only
  from `MappingNormalizerTest.java:2471` and `ModelBuilderTest.java:262,290`.

---

## 7. Shortcuts / deferrals

Most are honest, loud, and well-argued. Three are not:

| site | status |
|---|---|
| `:2339-2345` — `~filter` with explicit join type "not built yet" | **Unreachable; it *was* built** |
| `:1886-1887` — slot-carrying `~distinct` "stays the H3-pending wall" | **Stale; the code handles it** |
| `GroupBySynthesis:181-188` — "WITHHOLD the property, keep the set" | **Not implemented** |
| `:304-308` — multi-set `.all()` is a roadmap feature | honest, class-keyed poison, readable |
| `:1195-1200` — M2M `prop*` explosion | honest `ModelException` |
| `:1202-1211` — M2M enum transformer | honest, correctly argues why dropping it would be silently wrong |
| `:1036-1041` — XStore direction-specific conditions "required for now" | honest, but rests on the `toString()` comparison |
| `:1859-1864` — multi-hop Join PM on `~groupBy` | honest |
| `:2629-2635` — joins over non-plain views | honest, sound reasoning |
| `:2708-2717` — views as join targets | honest, good rationale for walling at synth time |
| `:2736-2739` — multi-table joins | honest |
| `:1872-1880` — `~primaryKey` intentionally not lowered | **excellent** — explains the engine semantics and why lowering would *diverge* |
| `:269-271` — non-root sets await H5 | stale: `:299-351` now realizes them, contradicting the framing three lines above |

---

## What is genuinely good here

- **`Pipeline.pathToSlot` is a structural key, not a flattened string** — `Pipeline.java:22-28` uses
  `Map<List<String>, String>` and says why: *"so a chain [A, B] never collides with a single join
  literally named `A__B`"*. This is the typing discipline the rest of the file still owes.
- **`Pipeline.ledger()` throws rather than returning null for a view pipeline** (`:88-99`) — a
  `@Nullable` field with a loud accessor and a documented invariant.
- **`seedAliasScope` records ambiguity rather than resolving it arbitrarily** (`:2549-2571`).
  Counting sub-rows per table and refusing to bind an ambiguous one is the correct shape: the DSL
  genuinely cannot express the distinction, so the code declines to guess, and `Pipeline.java:39-46`
  even gives the user the workaround.
- **Exhaustive switches with no `default`, each arm stating a deliberate stance** — `collectMainTables`
  `:1531-1561` and `collectExprTables` `:1563-1600`, with *"exhaustive, no default — a new PM kind
  must state its main-table stance here"*. Makes the next `PropertyMapping` variant a compile error.
- **Database included in table identity** — `:1479-1493`. `[db1]T` and `[db2]T` are correctly
  different tables, with a note on why name-only dedup would have *"silently pick[ed] refs.get(0)'s
  database"*.
- **One canonicalization site** — `:1514-1519` delegates to `KnowledgeLayer.canonicalTable`, with
  the audit note that `RelOpTranslator` used to spell it independently.
- **`withElement` keeps genuine bugs raw** — `:245-254`: `NotImplementedException` /
  `MappingResolutionException` get element attribution so a module build can wall them, but
  *"Genuine bugs (NPE, ISE) stay RAW: they must fail the build, never silently wall an element away."*
- **Enum entries validated against the enumeration** — `:2498-2512`, with the audit finding that
  silently skipping them *"turned typos into NULL rows in [1] slots"*. Right, modulo the
  unknown-enum hole.
- **Per-PM union-route classification replacing the name-keyed map** — `:789-793` and
  `Pipeline.java:53-59`. Audit 11's finding that *"textual PM order silently decided the outcome"*
  is fixed at the root rather than papered over, and the dead pre-rewrite was removed.
- **Order independence is real at the top level** — `:164-170`: `MappingPrePass.run` completes for
  *every* mapping before any synthesis, and `mappingsPerClass`/`unionRooted` (`:272-291`) are
  computed over the full set before the synthesis loop.
- **`MissProbe` at least names every null default** — turns 37 anonymous `orElse(null)` sites into a
  censused, dated, numbered list and pushes 16 to `orElseThrow`. Honest bookkeeping that made these
  findings locatable.

---

## Verified docs-vs-code contradictions (consolidated)

1. `GroupBySynthesis.java:181-188` "WITHHOLD the property" — not implemented (`:1926-1933`). **PROVEN.**
2. `MappingNormalizer.java:1882-1885` "DISTINCT over the MAPPED columns" — the third branch (`:1916`)
   does the raw row. **PROVEN.**
3. `:1245-1249` "Cycle materializing M2M class-typed property" — fires only on non-cycles. **PROVEN.**
4. `:373-376` "fetching THIS class raises the recorded reason (loud at use, never silent)" — false
   for set-keyed poisons.
5. `:2338` "Loud until the row-exploding emission is built" — it was built; the guard is unreachable.
6. `:1886` "slot-carrying distinct mappings stay the H3-pending wall downstream" — handled at `:1900-1914`.
7. `:269-271` "non-root sets await the H5 set-ID dispatch story" — `:299-351` realizes them now.
8. `JoinChainEmission.java:372-375` "the class's BACKING view always substitutes" — `p.backingView`
   is always null; the branch never runs.
9. `Pipeline.java:60-63` `ownerSet` documented as load-bearing — never read.
10. `package-info.java:12-14` "composes the sub-slices as chained `ParsedModel -> ParsedModel`
    transforms" — `ModelNormalizer.normalize` (`:109-133`) does no such chaining. Also
    `package-info.java:9` gives the entry point as `normalize(ParsedModel)`; the real signature is
    `normalize(ParsedModel, ModelBuilder, Map)`.
11. `:418-425` argues both that an association roadmap gap "must not sink the whole mapping" and
    that strict must reject it; the code picks the latter, diverging from the per-class arm.
