# 05 — Views and `~groupBy`

**Scope read end to end:** `ViewRelation.java` (536), `GroupBySynthesis.java` (315),
`AggregateViewLift.java` (68), `DeclaredCoercions.java` (223), `resolver/ViewFrames.java`, plus
`synthViewBackedMapping` (`MappingNormalizer:1678`), `resolveViewRefsInJoin` (`:2595`), and `:2680-2720`.

---

## 1. Is the flattening fallback really gone?

**Verdict: the *property-mapping* flattening is genuinely gone; a second flattening — of view
references inside join and filter conditions — survives and is still live.**

**Confirmed deleted:** `layerMappingFilterPreMap`, `filterBelowAggregation`, `chainHasGroupBy`,
`ViewRelation.rewritePmThroughView` have **zero occurrences** in the tree (the only surviving
`filterBelowAggregation` hit is a *test method name*, `MappingNormalizerTest.java:4378`).

`synthViewBackedMapping` is now 22 lines (`MappingNormalizer.java:1679-1700`) and does exactly three
things: build the view relation, `throughFrame` the set, hand it to the table-backed synthesis with
the frame as `sourceOverride`. The set keeps the view's identity — `throughFrame`
(`ViewRelation.java:397`) reconstructs the `ClassMapping.Relational` with `rcm.mainTable()`
unchanged, so the pipeline's scope key stays the view name.

**And the frame path really does serve the shapes the audit called the `frameable` exclusions.**
`MappingNormalizerTest.java:4405-4418` has `~filter BigFilter` + `~groupBy([db::DB] T_SALE.REGION)` +
`amount: sum(T_SALE.AMOUNT)` over `~mainTable V_SALE` — mapping-level filter *and* groupBy *and* PMs
naming the physical root, all under a frame, asserting
`map → groupBy → filter(mapping) → project → filter(view) → tableReference`.

Of the census fixtures, the four `classMappingFilterWithInnerJoin` view tests and
`TestClassMappingsWithInnerFilterJoinedWithMilestoningDepthTwoNestedGeneration` appear in **neither**
lane's fail/skipped/accepted roster — they pass on both H2 and DuckDB. The `(INNER)` mapping
`~filter` those fixtures carry is intercepted at `MappingNormalizer.java:1767` and rebuilt view-aware
by `JoinChainEmission.innerFilteredSource` (`:932-1010`), which reads the frame's *declared* column
names at `:997-1002`.

**What is NOT gone:** `MappingNormalizer.resolveViewRefsInJoin` (`:2595-2675`) still substitutes
`<view>.<col>` by the view column's underlying physical expression in join conditions, recursing
through view-on-view (`:2646-2650`, comment: *"re-resolve so the chain flattens to the physical
root"*), and `ViewRelation.inlineViewRefs` (`:281-295`) does the same for a mapping `~filter`. Both
are the deleted fallback's idea in a different position, and F1's own sentence — *"The engine keeps a
view a subselect always"* — is the argument against them. They are fenced by a loud wall for
non-plain views (`:2629-2635`), so the damage is bounded, but the flattening worldview is not fully retired.

**Caveat on the census:** two of the five named mappings have no witness in the executed lanes.
`simpleRelationalMappingIncWithStoreFilter` is referenced only by the Snowflake and SybaseIQ
window-column tests in the engine checkout; and on the **H2 lane** the core view family is on the
committed fail roster — `query::view::testViewAll`, `testViewSimpleFilter`, `testAllWithJoinToView`,
`projection::view::testViewWithGroupBy`, `testUnionOnViewsMapping`, `testAssnToViewWithGroupBy`, plus
the whole `lineage::scanRelations` view family (`h2-fail-roster.txt:53,56,57,58,94,96,380,381,382,418,419,420`).
Those tests assert engine SQL text as well as rows. **So the frame path is row-correct where it is
exercised, and not at text parity for the plain view family.**

---

## 2. Findings

### HIGH — `Pipeline.backingView` is always null; a whole documented branch cannot execute

`MappingNormalizer.java:1787-1790` is the only pipeline constructor taking a `backingView`; its
inbound values are `null` at `:1413`, `null` at `:1699` (`/*backingView*/ null`), and the
pass-through recursion at `:1775`. The two other constructions pass null
(`JoinChainEmission.java:947`, `Pipeline.java:90`). Therefore `p.backingView == null` **always**, and
`JoinChainEmission.java:376-378`

```java
RelationalOperation joinCond = p.backingView == null ? jd.operation()
        : MappingNormalizer.resolveViewRefsInJoin(jd.operation(), hopDb, prevTable,
                model, md, p.backingView, p.backingView);
```

always takes the left arm. The comment above it states the rule as live: *"PASS 1: the class's
BACKING view always substitutes to its physical expressions"*. **It never runs.**

Consequently `onlyView` is never non-null (its only non-null producer is that dead call), so every
`onlyView` branch in `resolveViewRefsInJoin` is dead too: `:2604-2606`, `:2623`, and the ternary at
`:2646-2650` whose *"Pass-1 (onlyView) stays one-layer"* comment describes an unreachable mode.

Also dead in the same family: the 5-arg `synthTableBackedParts` overload (`:1737-1742`), no callers.

**Why it matters:** four method signatures and a `Pipeline` field thread a parameter that is always
null, and a reader reads the comments as the current rule.

### HIGH — the join/filter-condition view flattening survives the "one rule replaced it" claim

`MappingNormalizer.java:2637-2652` substitutes a view column reference by the view's own column
expression, recursively; `ViewRelation.java:281-295` does the same for filter conditions.
`ViewRelation.java:279` still carries the fallback's premise verbatim: *"the flattened set reads the
base tables, so its filter must too"* — a set that no longer exists. `inlineViewRefs` has exactly one
call site (`MappingNormalizer.java:2296`, the *set over a TABLE whose filter names a view* arm) and
no test or corpus fixture exercising that arm was found.

**Why it matters:** the headline of F1 is that one frame rule replaced the flattening. Two flatteners
are still live, one of them unreceipted.

### MED — stale documentation in `synthViewBackedMapping` (all three items confirmed)

`MappingNormalizer.java:1661-1677` still carries the 4-step ordered list of the deleted algorithm
behind the parenthetical *"(Historical shape of the deleted flattening fallback, for the record:)"*;
`:1665` contains `{@link #inferViewMainTable}`, a **broken javadoc link** — the method lives in
`ViewRelation`; and `:1702-1711` is an **orphaned javadoc block** with nothing under it (next real
token is the section banner at `:1715`). The body comment at `:1686-1695` then re-states the frame
rule a second time in prose, so **the method carries three overlapping descriptions, one of them for
deleted code.**

### MED — every engine line citation in scope is wrong against the *local* checkout

> **Read `10-engine-citations.md` before acting on this.** The local checkout is `943d38b3`
> (4.137.0-36), **not** the pinned 4.145.0, so this comparison is unsound as a verdict on the
> citations. It is recorded here because the *actual* 4.137 locations are useful.

Actual locations in `pureToSQLQuery.pure` at 4.137: `applyGroupBy` at **4998**,
`getRelationalElementWithInnerJoin` at **4745** (chosen at 4769-4771), the `ViewSelectSQLQuery`
construction at **4855**, `findTableForColumnInAlias` at **9244**. The code cites `:5187`
(`MappingNormalizer:1658`), `:5077 … chosen at :5101` (`:1760`), and `:5147-5157` for *"the engine
projects only the declared columns"* (`ViewRelation:253`) — all offset by ~330 lines.

Separately, `docs/audit-20a-overfit.md:195` cites `pureToSQLQuery.pure:5768-5776` for `applyGroupBy`
while `docs/TRANSLATOR_AUDIT_2026_09_15.md` cites `P:5331`. **Two internal docs, two numbers.**

The *rules* hold up where checked (at 4855: a `View` becomes `ViewSelectSQLQuery` wrapping a
subselect — a view never flattens; at 4784: `requiresAllProperties` when groupBy/distinct; at 4830:
distinct on the view's own select).

### MED — the engine's `applyGroupBy` does not say what `GroupBySynthesis` is cited as implementing

`applyGroupBy` (engine `:4998-5006`) is four lines: if the spec has a groupBy, set
`groupBy = <re-aliased key columns>` on the select. **It contains nothing about class-typed Join PMs,
nothing about aggregate decomposition, and nothing about navigating a grouped relation on group
keys.** The engine emits every property column bare (`:4784-4788`) and lets the database decide.

Our decomposition into `keyCols`/`aggCols` plus the withhold rules (`GroupBySynthesis.java:158-160`,
`:176-184`) and the grouped-navigation rename (`:83-130`) are **ours**, not the engine's.
`GroupBySynthesis.java:238-239` is honest about this (*"The engine itself decides nothing here"*);
the audit doc's table row T3 claiming "matches" against `P:5331` is not supported by the source.

### MED — the same shape gets two opposite answers in the same package

A per-row column that is not a group key: under a **view** `~groupBy` it becomes an implicit
`first()` aggregate (`ViewRelation.java:204-219`); under a **mapping** `~groupBy` the property is
silently **withheld** (`GroupBySynthesis.java:176-184`). Both cite engine leniency as justification.
**One of the two is wrong, and neither is the engine's behaviour** (which emits the bare column
either way).

### MED — `ViewRelation.java:205` "H2-LENIENT": not an ArchUnit breach, but a real leniency

**No layering violation:** `ViewRelation` imports nothing from `com.legend.sql.dialect`, the emission
is unconditional, and the dialect-specific part — unwrapping `ANY_VALUE` back to the bare expression
for engine-style text — lives where it belongs, at `EngineStyleH2.java:1362-1365`. The `first()` at
`:217` lowers via `Aggregates.java:48` (`family(SqlAgg.Fn.ANY_VALUE, "first")`).
`ArchitectureTest.compileSideLayersAreDialectBlind` (`:607`) is satisfied.

What *is* baked in is a **semantic leniency**: the engine's own SQL for this shape is a bare column,
which H2 1.x tolerates and DuckDB rejects; we emit `ANY_VALUE` on every dialect, so a model the
engine cannot run on a strict database runs here with an arbitrary witness row. Worth a receipt, not
a rule change.

### MED — `requireNonViewTarget`'s javadoc contradicts the code around it

`MappingNormalizer.java:2701-2717`: *"Views as join targets = roadmap slice"*, *"views as JOIN
TARGETS are a roadmap feature"*. But `JoinChainEmission.java:425, 456, 496, 574` expand exactly that
— a view join target becomes `ViewRelation.viewRelationExpr(...)`. The wall has one caller
(`AssociationSynthesis.java:588`) and restricts the *association* lane only. The blanket claim is stale.

### MED — `ViewFrames.frameNameOf` is not include-closure aware, where the compiler's view lookup is

`ViewFrames.java:60-67` scans `db.views()` then `db.schemas()` of the one database and returns `null`
otherwise. `ModelBuilder.findView` (`:973-996`) is explicitly *"Include-closure aware … an including
database resolves the included database's views"*. A class whose `~mainTable` is a view declared in
an **included** database therefore returns `null` here, and its five callers
(`StoreResolver.java:1075, 1114, 2031, 2071`, `CastReRoot.java:138`) treat a view-backed class as
physical-table-backed — exactly the identity the file's own javadoc says must be preserved
(*"its alias groups by the view's own name — `orderpnlview_0`, never the underlying physical table's"*).

Two further sharp edges in the same 40 lines: the loop `return`s inside the first matching class
binding (`:68`), so for a multi-set class the first binding decides — **first-wins identity of the
kind commit `ae16e5c46` was removing on the union path**; and `:47-49` re-implements
`KnowledgeLayer.canonicalTable` inline instead of calling it.

### MED — dead code that still reads as a live rule

- `ViewRelation.relationExpr` (`:524-535`): **zero callers**, and its javadoc — *"a physical table is
  a tableReference"* — is false for its own body:
  `model.findView(...).orElseThrow(() -> MissProbe.neverFired("ViewRelation#8"))` at `:530` throws on
  a physical table, so the `: new AppliedFunction("tableReference", ...)` branch at `:532` is
  unreachable. `JoinChainEmission.relationRef` (`:493-500`) is the live, correct implementation of the
  same rule, and `ViewRelation.sourceRefFor` (`:59-70`) is a third copy. **One rule, three
  implementations, one dead and mis-documented.**
- `MappingNormalizer.java:2339-2345`: the `jm.joinType() != null` `NotImplementedException` in
  `applyJoinMediatedFilter` cannot fire — `synthTableBackedParts` intercepts at `:1767-1776` before
  `applyFilter` (its only caller, `:1842`) is reached.
- `ViewRelation.java:509` (`if (found == null) return null;`) and `:516` (`if (jd == null) return
  null;`) are unreachable after `orElseThrow`. Related: `joinOnlyViewRoot`'s javadoc says *"null keeps
  the caller's loud wall"*, but a missing database or join now throws
  `IllegalStateException("F7.8: empty answer at ViewRelation#6/#7")`.

### MED — `inferViewMainTable`'s 3-arg overload is dead, and the census pin counts it

`ViewRelation.java:441-444` has no callers; all five live calls pass five arguments
(`ViewRelation.java:95`, `JoinChainEmission.java:889`, `MappingNormalizer.java:2613`, `:2696`) — the
fifth being `:443`, the dead overload's own delegation. `ShadowWalkerCensusTest.java:73` pins the
count at 5 as OWED debt, **so the ratchet-to-zero target is inflated by one phantom call.**

Of the four real ones, `:95` is legitimately needed by the frame path (the frame's source must be the
view's root relation; the engine reads the same fact from `$viewSpecification->mainRelation()`,
resolved at compile time rather than re-derived); `:2613` and `:2696` exist only to serve the
surviving join-condition flattening.

### LOW — `MissProbe` census note is stale for this file

`MissProbe.java` lists `ViewRelation#1–3` as the sites that fired; `ViewRelation` now has **five**
`orElseGet(MissProbe::miss)` sites (`:64, :104, :284, :352, :366`) and three `neverFired` sites
(`:508, :514, :530`), one of which (`#8`) is in dead code and can never fire.

### LOW — `frameRewrite`'s loudness is conditional on a heuristic

`ViewRelation.java:330-344`: the "column not in the view" error fires only if some *other* declared
column's expression is a bare `ColumnRef` of the same table (`readsTable`). A view whose columns are
all expressions or join navigations sets `readsTable = false`, and a PM naming the root table's
column falls through unchanged at `:345`. Not silent — `RelOpTranslator.java:232-236` raises
*"ColumnRef references table 'X' not in scope"* — but the diagnostic is the generic one, not the
frame rule's.

### LOW — shape

`viewRelationExpr` is one 189-line method (`ViewRelation.java:86-274`) mixing cycle detection,
view-on-view recursion, three join-hoisting loops, filter translation, groupBy decomposition and the
distinct fix-up. `resolveViewRefsInJoin` takes 9 parameters including three nullable `String` mode
flags plus a `boolean anySide`, re-threaded through 10 recursive call sites
(`MappingNormalizer.java:2655-2673`). `MappingNormalizerTest.java:4378`'s method name still
advertises the deleted `filterBelowAggregation`.

---

## 3. What is genuinely good

- **The frame is the only emission, and it is short.** 22 lines at `MappingNormalizer.java:1679-1700`,
  with `throughFrame` applying one rule uniformly to property mappings, `~groupBy` keys,
  `~primaryKey`, embedded sub-PMs and their PKs (`ViewRelation.java:383-439`). The `default -> pm`
  arm for Join PMs with its one-line reason (*"joins depart from the view by name"*) is the right call
  and matches the engine.
- **View-on-view is handled properly**, recursively and with a cycle guard that names the mapping
  (`ViewRelation.java:90-108`), instead of a depth limit or a silent stop.
- **The `~distinct` + synthetic-key interaction is genuinely thought through** (`:251-272`): distinct
  over rows carrying synthetic `k<i>` keys would remove nothing, so the declared columns are
  re-projected first. The kind of bug that normally ships.
- **`isGroupReducer` reads the dynafunction registry and the native catalog rather than a hand list**
  (`GroupBySynthesis.java:245-260`), with the receipt for *why* (a previous hand list carried `avg`
  and `stdDev`, neither an engine dynafunction).
- **Out-of-scope column references are loud** (`RelOpTranslator.java:232-236`), and the
  grouped-navigation rename refuses to bind a non-key column (`GroupBySynthesis.java:113-118`) rather
  than guessing.
- **The H2 leniency is confined to the renderer.** `EngineStyleH2.java:1362` is the only place that
  knows about it; the normalizer emits one semantic form.
- **`AggregateViewLift` is not a stub** — 68 lines is the right size for what it does: lift each
  aggregate view as an ordinary non-root set through the same `synthesizeClassMapping`, and stamp the
  specification facts on the main binding for `AggregationAwareRouting`
  (`resolver/AggregationAwareRouting.java:88, 115, 188` consume them). No aggregation logic is
  duplicated; the routing decision lives in one place downstream.
- **`DeclaredCoercions` carries the most honest comments in the scope**: `:46-50` names a *deliberate*
  divergence from the engine (`'Y'` → `parseBoolean` false) and says why loud beats
  silently-different; `:149-194` distinguishes conversions the engine's runtime actually performs
  from ones that would invent semantics, with the JDBC-fetch receipt for the Decimal→Float case and
  an explicit "Float over INTEGER stays identity" carve-out. **That is the standard the citation
  comments elsewhere in this scope fail to meet.**
