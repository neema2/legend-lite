# 12 — The arc's history, deletion claims, and ratchets

**Method:** every claim below was adjudicated against the code at HEAD (`ae16e5c46`), never against
the commit message that made it.

---

## 1. Arc map

**56 commits** in `d3cd6efc9..ae16e5c46`.

| class | count |
|---|---|
| touch `core/src/main/java/com/legend/normalizer/` | 24 |
| touch any `src/main/` | 33 |
| tests-only (test changed, no main) | **0** |
| docs-only (`.md` only) | 23 |

**Net lines (`git diff --shortstat d3cd6efc9..HEAD`)**

| path | files | + | − | net |
|---|---|---|---|---|
| `core/.../normalizer/` | 31 | 2,075 | 4,059 | **−1,984** |
| all `*/src/main/*` | 77 | 5,994 | 5,865 | **+129** |
| all `*/src/test/*` | 26 | 2,008 | 164 | **+1,844** |
| `*.md` | 8 | 3,178 | 2 | **+3,176** |

Per-package main: `resolver/` **+1,277**, `compiler/` **+781**, `model/` **+52**,
`normalizer/` **−1,984**.

> **The headline number of the arc — "UnionSynthesis 3,287 → 976 lines" — is real, but production
> code as a whole is net +129 lines.** A new 1,972-line `core/src/main/java/com/legend/resolver/StackBuilder.java`
> was created inside the arc. The published measure table
> (`docs/LEGACY_ROUTES_AS_COMPOSITION_2026_09_13.md:911`) counts M1 as `wc -l UnionSynthesis.java` and
> M2 as "count of places that build a `TypedConcatenate` of class arms" — **no measure counts the size
> of what replaced it.** Complexity moved across the normalizer/resolver seam; the metric only watches
> one side.

---

## 2. Deletion-claims verdict

| commit | claim | verdict | evidence at HEAD |
|---|---|---|---|
| `7da6acaa8` | `layerMappingFilterPreMap` deleted | **TRUE** | 0 hits repo-wide |
| `7da6acaa8` | `filterBelowAggregation` deleted | **TRUE** | only surviving hit is a *test method name*, `MappingNormalizerTest.java:4378` |
| `7da6acaa8` | `chainHasGroupBy` deleted | **TRUE** | 0 hits |
| `7da6acaa8` | `ViewRelation.rewritePmThroughView` deleted | **TRUE** | 0 hits |
| `7da6acaa8` | flattening fallback deleted (implies `inferViewMainTable`) | **PARTLY TRUE** | alive: `ViewRelation.java:441,446` + 5 pinned call sites. **Honestly disclosed:** `ShadowWalkerCensusTest:73` carries `Map.entry("inferViewMainTable", 5)` under an `// OWED:` comment. The commit ratcheted 6→5 rather than to 0 |
| `bbd0049c5` | the index's legacy walk deleted | **TRUE** | `routedTargetSetOf` 0 hits; `LegacyReachbackCensusTest` PureModelContext row 6→4 |
| `72f525969` | the nine include walkers become readers | **PARTLY TRUE** | the nine call sites now read `MappingClosures`, and `findLegacyMapping` sites in the normalizer fell 8→2. But (a) the nine *walks* still exist, moved inside `MappingClosures.java` (12 `includes()` loops, `:151-396`) with its own javadoc admitting *"Each accessor keeps the rule of the include walker it replaced, recursion shape included"*; (b) a **10th** independent include-graph walk survives outside it at `StoreSubstitutionRewrite.java:269,294` |
| `6048acec2` | `PhysicalTables` deleted | **TRUE** | file gone |
| `f829f9df3` | the callerless bitemporal twin deleted | **TRUE** | `isBitemporalClass` 0 hits |
| `4b38bde7a` | no reflection in the product, no pardons | **TRUE** | `ArchitectureTest.java:656` has **no `haveNameNotMatching` pardons** and now also bans `Class.forName/getRecordComponents/getDeclared*/getMethod*/getField*/getConstructor*/newInstance`. Repo-wide `src/main` grep: the only hit is the *word* inside a comment at `server/Json.java:386`, plus a non-product `experiments/backend-probes` file. Every `getSimpleName()` residue is inside error messages; `CodeShapeGuardrailTest.CLASS_NAME_LOGIC_SITES = 0` pins name-as-logic at zero |
| `12e36db45` | B5 every guard loud | **PARTLY TRUE** — see F3 |
| `ae16e5c46` | no first-wins on the union path | **PARTLY TRUE** — see F2 |

### `ae16e5c46` decoded

- **M4** = a published measure: "quiet arms on the union path", counted by a grep for `return; //`,
  `continue; //`, `putIfAbsent`, swallowed catch — **scoped to `UnionSynthesis.java` alone** (doc line 917).
- **"receipted skip"** = a `continue;` whose trailing comment states why the miss is the answer. At
  HEAD the two survivors are `UnionSynthesis.java:350` ("root routes = the un-routed navigation") and
  `:914` ("a Relation(~func) member has no key table"). M4 = 2. Measure satisfied.
- **Did first-wins go?** Inside `UnionSynthesis`, yes: two `putIfAbsent`s became `recordOwner()` which
  **throws `ModelException`** on a second distinct owner; the third was deleted with dead code.

---

## 3. Ratchet table

Every guard/ledger/allowlist change under any `src/test` in the arc:

| commit | guard | old → new | direction | dated justification naming the task? |
|---|---|---|---|---|
| `83c8b925c` | `ShadowWalkerCensusTest` (new file, 20 rows) | — → baseline | new pin | yes — "T4.1 step 3a, 2026-09-13" |
| `52b7205a7` | same, property family (6 rows) | 45,3,1,53,3,2 → 0 | **tightened** | yes — "step 3b, 2026-09-13" |
| `f829f9df3` | same, stereotype family | 2,3 → 0,0 | **tightened** | yes — "step 3c, 2026-09-13" |
| `6048acec2` | same, store family | 9,7,2,2 → 0 | **tightened** | yes — "step 3d, 2026-09-13" |
| `6048acec2` | same — **rows `pureKindOf` (3) and `declaredPlatformKind` (3) DELETED, not ratcheted** | 3,3 → *row removed* | **LOOSENED** | **partial / NO** — see F1 |
| `7da6acaa8` | same, `inferViewMainTable` | 6 → 5 | **tightened** | yes — "leg 6b" (commit body) |
| `72f525969` | `LegacyReachbackCensusTest` | MappingNormalizer 4 + UnionSynthesis 3 + AssociationSynthesis 1 → MappingClosures 2 | **tightened (−6)** | yes — "T4.1 step 4a, 2026-09-13" |
| `bbd0049c5` | same, PureModelContext | 6 → 4 | **tightened** | yes — "T4.1 step 4b, 2026-09-13" |
| `b9f272496` | `TransitionalShapesTest.VIEW_SITES` (new) | — → 5 sites, shrink-only + die-together assert | new pin | yes — B1 |
| `421063da7` | `TransitionalShapesTest` **deleted entirely** | pin → gone | **LOOSENED (legitimate)** | yes — the pin's own subjects (`MappedClasses`, `MappingView`) are both gone at HEAD, which is exactly what its `assertTrue(mappedClassesExists == viewExists)` demanded |
| `4b38bde7a` | `ArchitectureTest` F1.11 reflection pardons (2 name-regexes) | 2 → **0** | **tightened** | yes — "USER 2026-09-15: no pardons" |
| `4b38bde7a` | `ArchitectureTest.thePardonedReflectionClassesAreSiteCounted` **deleted** | site pins 2,4 → gone | **LOOSENED (legitimate)** | yes — the pardon list it policed no longer exists; the wholesale ban supersedes it |
| `4b38bde7a` | `CodeShapeGuardrailTest.CLASS_NAME_LOGIC_SITES` (new) | — → 0 | new pin | yes — 2026-09-15 |
| `435070647` | `NativeCatalogGovernanceTest` INTERNAL_DESUGAR ceiling | 16 → 18 | **LOOSENED** | yes — B3.1, 2026-09-13 |
| `b5076e4f0` | same | 18 → 16 | **tightened** | yes — B3.1b, USER review |
| `2fd76aa8b` | same | 16 → **17** | **LOOSENED** | yes — "composition step 1", 2026-09-13 |
| `2fd76aa8b` | `OwnCorpusConformanceTest` `LITE-DESIGN-mapping-as-function` | 20 → 21 | **LOOSENED** | yes — 2026-09-13, "REVIEWED" |
| `2fd76aa8b` | `OwnDialectCensusTest` file allowlist + per-file count | + `RoutedNavigateTest.java`, 0 → 1 | **LOOSENED** | yes — 2026-09-13, "REVIEWED" |
| `f34f3e03d`, `d2dde1451`, `a45aefd18`, `d539fc9b6`, `2fd76aa8b` | `JdbcSurfaceCensusTest` JDBC-opening-test allowlist | +5 entries | **LOOSENED ×5** | **task named, but UNDATED** — see F7 |
| `421063da7` | `OwnCorpusParityTest.MIN_MATCHED` | 2425 → **2422** | **LOOSENED (floor lowered)** | yes — "clean-sheet B3.3, 2026-09-13 … net −3" |
| 10 other commits | `OwnCorpusParityTest.MIN_MATCHED` | 2376 → 2379 → 2381 → 2392 → 2398 → 2405 → 2425 → 2429 → **2434** | **tightened** | yes — every step dated and named |

**Net:** the ratchets moved overwhelmingly in the tightening direction, and the project's own
discipline (dated inline justification, "REVIEWED" tags, *"SHRINKAGE means … ratchet the row down in
the same commit"*) was followed in all but the cases called out below.

---

## 4. Test-deletion audit

**Deleted test files: 1.**

- `core/src/test/java/com/legend/normalizer/TransitionalShapesTest.java` — deleted by `421063da7`.
  Reason stated: `MappedClasses` and `MappingView` both died in B3.3, and the test's own final
  assertion required them to die together. **Verified legitimate** — both files absent at HEAD.

**Deleted test methods inside surviving files: 4.**

| method | commit | stated reason | verdict |
|---|---|---|---|
| `ArchitectureTest.thePardonedReflectionClassesAreSiteCounted` | `4b38bde7a` | the pardon list it site-counted is gone | legitimate — superseded by a strictly stronger rule |
| `MappedClassesTest.explicitClassMappings` | `421063da7` | the graph-wide `MappedClasses` shape no longer exists | legitimate; coverage folded into `mappedIsClosureLocal` |
| `MappedClassesTest.implicitSetsAreMappedOrderIndependently` | `421063da7` | same rename/reshape | **order-independence coverage survives** as `MappedInClosureTest.compiledMappingsIgnoreElementOrder` (`:101`) — a weaker comparison, but pre-existing, not new |
| `TransitionalShapesTest.bareViewConstructionSitesArePinned…` | `421063da7` | file deletion above | legitimate |

**`@Disabled` / `@Ignore` added during the arc: 0.** `assumeTrue`/`Assumptions` added: 0. The
`SkipCensusTest` pins were untouched.

> **This arc did NOT delete the tests that would have caught its regressions.** Tests grew +1,844
> lines across 26 files, including five new row-verdict suites (`StackShapeWitnessTest`,
> `StackDesignWitnessTest`, `StackRatchetWitnessTest`, `RoutedNavigateTest`, `RoutedChainKeyTest`)
> that execute against real DuckDB.

---

## 5. The ten most assertive claims — verdicts

1. `7da6acaa8` **"the flattening fallback deleted"** — **PARTLY TRUE.** Four of five named symbols
   gone; `inferViewMainTable` alive with 5 call sites, openly pinned as OWED debt.
2. `bbd0049c5` **"the index's legacy walk deleted"** — **TRUE.**
3. `72f525969` **"the nine include walkers become readers"** — **PARTLY TRUE.** Call sites became
   readers; the nine distinct walks live on inside `MappingClosures`, and a 10th survives in
   `StoreSubstitutionRewrite`.
4. `6048acec2` **"PhysicalTables deleted"** — **TRUE.**
5. `f829f9df3` **"the callerless bitemporal twin deleted"** — **TRUE.**
6. `4b38bde7a` **"no reflection in the product, no pardons"** — **TRUE. The strongest-verified claim
   in the arc:** pardons removed, rule widened, residues actually rewritten (`ScanColumns` now walks
   `SqlExpr.children()`; `Json` names its array kinds; `FunctionBodyRows` switches on node kinds).
7. `12e36db45` **"B5 every guard loud"** — **PARTLY TRUE.** See F3.
8. `ae16e5c46` **"no first-wins on the union path"** — **PARTLY TRUE.** See F2.
9. `a45aefd18` **"B6 — one union builder"** — **TRUE under the published measure.** `UnionHeads.java`
   (298 lines) now constructs `StackBuilder.Arm` and delegates; `StoreResolver`'s remaining
   `TypedConcatenate`s distribute project/map over a *user-authored* concatenate, not class arms.
   Caveat: the one builder is 1,972 lines.
10. `3a264a325` **"the ambiguity wall deleted"** — **TRUE.** `ClassSources.findBinding` no longer
    throws *"class '…' is ambiguously mapped"*; it returns last-wins. **Notably the commit body
    itself discloses the blind spot:** *"0 wall hits, 0 duplicate ids, 0 first-vs-last differences —
    so the corpus cannot judge this batch"*. The change rests entirely on the new 114-line
    `IncludeRulesTest` plus engine-source citations.

**Bonus:** `87e4e2dfe` **"No memo"** — **TRUE in the code**
(`PureModelContext.resolutionUniverse()` returns a live `AbstractSet` view, no materialization),
though the `NameResolver.resolveQueryIn` javadoc it added calls the same thing *"the model context's
memoized union"* — a self-contradiction inside one commit.

---

## 6. Findings

### F1 — MEDIUM-HIGH — a census row was deleted instead of ratcheted, and the walker it counted is still in the counted directory

`6048acec2` removed two rows from `ShadowWalkerCensusTest.REGISTER`: `pureKindOf` (3) and
`declaredPlatformKind` (3). The inline justification covers only the first — *"RelationalKinds moved
to compiler as the one kind reader (its calls are not shadows and are no longer counted)"*.

**`declaredPlatformKind` got no justification at all**, and at HEAD it is still **defined and called
inside the census's own scan directory**: `core/src/main/java/com/legend/normalizer/DeclaredCoercions.java:74`
(definition), called at `:54, :102, :137`; plus `RelationalKinds.pureKindOf` at `:115, :145`.
**Six live shadow-walker call sites inside `normalizer/` are now unpinned and free to grow.**

The guard's own failure message demands the opposite discipline (*"SHRINKAGE means a family moved —
ratchet the row down in the same commit"*). **This is the one clear instance in the arc of an
allowlist row removed rather than driven to zero.**

### F2 — MEDIUM — "no first-wins on the union path" is scoped to one file; a structurally identical silent first-wins survives one file over

`ae16e5c46` replaced `ownerByProp.putIfAbsent(propertyName, ownerCls)` in `UnionSynthesis` with a loud
`recordOwner()` that throws on a second distinct owner. The *identical* pattern — owner class keyed by
property name, first writer wins, silently — is untouched at `JoinChainEmission.java:622`:

```java
p.navSlotOwner.putIfAbsent(j.propertyName(), ownerCls);
```

`recordNavSlotOwner`'s own javadoc says its purpose is to *"distinguish a same-owner routed SIBLING …
from a genuine cross-level clash (ledger cluster 66)"* — **the same collision class leg 6g declared a
model error.** It survives because **M4's grep is defined over `UnionSynthesis.java` alone** (doc line
917). **The measure, not the property, was satisfied.**

### F3 — MEDIUM — "every guard loud" is true for one construct in one package, and nothing mechanically holds it

The B5 census covered exactly `orElse(null)` inside `core/.../normalizer/`. That is genuinely at zero
(the only 3 residues are the `MissProbe` funnel's own definitions). But:

- **No guard test pins it.** Nothing under `core/src/test` references `MissProbe` or `neverFired`; the
  census exists only as a javadoc comment in `MissProbe.java`. **The zero can regrow silently** — the
  exact "a green check that only proves well-formedness" gap the harness rules exist to close.
- **41 bare `return null;` sites remain in the same package** (`JoinChainEmission` 9,
  `AssociationSynthesis` 8, `MappingNormalizer` 4, …) — the same silent-default shape, never censused.
- **A swallowed exception survives on the view path**: `JoinChainEmission.java:892-896` catches
  `ModelException` and `return null;` with no probe or receipt.
- ~100+ `orElse(null)` sites exist elsewhere in `core/src/main` (`compiler/spec/Typer.java` 16,
  `MetamodelSeeds` 14, …), untouched.

*(See `14-guards-fallbacks-census.md` for the exhaustive version.)*

### F4 — MEDIUM — a dead-code artifact from the mechanical loud-guard conversion

`MappingClosures.java:191-193`:

```java
LegacyMappingDefinition included = surfaceOf(...).orElseThrow(() -> MissProbe.neverFired("MappingClosures#1"));
if (included == null) {
    continue;   // unresolvable include is its own loud problem elsewhere
}
```

The null check is unreachable after `orElseThrow`. The former tolerance for an unresolvable include
was converted to a throw **without removing the arm it replaced** — so the comment now documents
behaviour the code no longer has.

### F5 — LOW-MEDIUM — row-verdict tests were re-pinned to a smaller result set

`d539fc9b6` ("leg 4a: ratchet witnesses; the builder thinned") changed three `RoutedNavigateTest` row
assertions:

- `sameShapeRoutesShareOneKey`: `["1|A","1|D","2|B","2|C"]` → `["1|A","2|B"]`
- `differentShapesKeepTheirOwnKeys`: `["1|A","2|B","2|C"]` → `["1|A","2|B"]`
- `existsThroughRoutes`: the probe switched from name `'D'` to `'A'`, and `'D'` re-pinned to an empty result
- `assertTrue(sql.contains("UNION ALL"))` → `assertFalse(...)`

**This is a mapping that now returns fewer rows than before.** It is *disclosed* (commit body: "R-a was
red … RoutedNavigateTest re-pinned to the receipt") and *receipted* —
`docs/LEG2_STACK_AUDIT_2026_09_14.md:84` cites `functions_Mapping.pure:66 _classMappingByIdRecursive`,
whose filter `$cm.id == $id` against the whole id list is true only for a single distinct id, so
several distinct pins fall back to the class root and non-root pins die.

**If that engine reading is right this is a fidelity *fix*; if it is wrong, three row-verdict tests
were rewritten to bless data loss. The engine citation is the single point of failure and cannot be
verified from this repo.** Recommend an independent read of that Pure function before the arc is
considered closed.

### F6 — LOW-MEDIUM — structural assertions weakened from adjacency to ordering

`7da6acaa8` rewrote three `MappingNormalizerTest` view tests from exact parent/child chains to a
`spine()` + `spineIndex(from)` search asserting only relative order. The
`assertEquals("select", select.function(), "~distinct dedups the MAPPED columns: select narrows first")`
assertion was **dropped entirely**, and one assertion flipped semantics
(`outerCols.contains("AGE")` → `contains("page")`). Justified by the frame rewrite, but the tests now
accept a strictly larger set of pipelines. *(Full detail: `09-test-quality.md` HIGH-2.)*

### F7 — LOW — five JDBC-allowlist entries carry a task name but no date

`JdbcSurfaceCensusTest` gained 5 rows across `2fd76aa8b`, `f34f3e03d`, `d2dde1451`, `a45aefd18`,
`d539fc9b6`. AGENTS.md requires *"a dated justification comment naming the task/incident"*; these name
the leg but omit the date, unlike the `NativeCatalogGovernanceTest` and `OwnDialectCensusTest` entries
in the same arc which are correctly dated.

### F8 — LOW — an internal contradiction in `MappingClosures`

Its class javadoc (written at step 4a) still states *"operation sets per class: the FIRST include found
wins"*, while `walkOps` (`:232-244`) is last-wins and the `union()` javadoc correctly says *"the LAST
found … (R1)"*. B2 changed the rule; the class-level comment was not updated.

### F9 — LOW — a behaviour-preserving claim that is asserted, not shown

`4b38bde7a` replaced `getSimpleName().startsWith("TypedC") && n.children().isEmpty()` with an explicit
11-arm switch in `FunctionBodyRows`. The new arms for `TypedColSpec`, `TypedColSpecArray` and
`TypedCsvCensus` **drop the `children().isEmpty()` guard**; the comment says *"the former
class-name-prefix rule classified them so; kept explicit"* — true only if those nodes never have children.

---

## Bottom line

The arc's deletion claims are **substantially honest**: eight of twelve verify cleanly, two are
honestly hedged in the code itself (`inferViewMainTable` is pinned as OWED debt in the census, not
hidden), and the reflection ban is the strongest-verified claim in the set. Guard files moved
overwhelmingly in the tightening direction with dated, task-named justifications, no tests were
disabled, and test code grew nine times faster than production code.

**The real weakness is not deletion-that-didn't-happen — it is measure scope.** Three of the arc's
headline properties ("no first-wins", "every guard loud", "union-synthesis lines") are defined by
greps over a single file or a single syntactic construct, and at HEAD each property holds *inside* its
grep and fails just outside it (F1, F2, F3). **Two of those measures have no guard test at all** —
M1–M4 live in a markdown table and B5's census lives in a javadoc. One ratchet row
(`declaredPlatformKind`, F1) was removed without justification while its walker still lives in the
counted directory, **which is the AGENTS.md violation proper.**
