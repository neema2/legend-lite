# 08 — `ScanRelations`: the biggest single duplication

**Scope read end to end:** `lineage/ScanRelations.java` (2,797), `ScanColumns.java` (329),
`ColumnLineageRows.java` (127), `LineageRows.java` (51), `PkInference.java` (152), plus
`normalizer/MappingClosures.java` (431) for the line-by-line comparison.

---

## Bottom line

The finding is real and **worse than "lineage re-reads the surface."** `ScanRelations` is not an
analysis-only sidecar:

- `ScanRelations.rootImpl` feeds the **execution-plan text** — `plan/PlanText.java:70`, `:188`,
  `StatementExecutor.java:996, 1026, 1160, 1851`, `PlanAllocations.java:85`
- `ScanRelations.relTree` feeds **test-data generation** — `TestDataGenerator.java:102, 196`
  (~40 call sites across that file)

So a private, string-matching re-implementation of the root-class-mapping rule decides **what the
user sees in `planToString`**, while the compiled path (`ClassSources.findBinding`) decides what
actually executes. **The two implementations disagree on four independent axes.**

The `PureModelContext:104-105` justification (*"ANALYSIS archive — F+ compilation never reads them"*)
is accurate about *compilation* and **misleading about scope**: plan text is an F+ product and it
goes through this file.

**Counter-evidence that the surface reach is not forced:** `ColumnLineageRows.java:67`, in the same
package, reads the compiled artifact —
`md.classBindingsWithIncludes(ctx::findMapping)` over `MappingDefinition.ClassBinding.Relational.source()`.
**The stamped path is reachable from `com.legend.lineage`. `ScanRelations` simply does not use it.**

---

## Reachability and test coverage (the stakes)

**Shipped, on three user-facing surfaces:**

| consumer | site | surface |
|---|---|---|
| `relationTreeAsString` | `Pure.java:1611-1612` natives, `SystemMetamodel.java:1147-1151`, rows via `LineageRows` | Pure `scanRelations` builtin |
| execution plan text | `PlanText.java:70,188` ← `ScanRelations.rootImpl` | `planToString` / `executionPlan` |
| test-data generation | `TestDataGenerator.java:102,196` ← `ScanRelations.relTree` | `generateTestData` (#46) |

**Tested — by goldens, not by units.**

- `docs/RELATIONAL_CORPUS.md:49`: `lineage/scanRelations | 49 | 49 | 0` — 49 corpus tests, all
  passing on the DuckDB lane.
- All 49 are on `spec/src/test/resources/rcorpus/h2-fail-roster.txt:48-96` (H2-lane capability gap).
- **The only unit test in `core/src/test/java/com/legend/lineage/` is `LineageScanTest` (104 lines,
  3 tests) and it exercises `ScanColumns` only.** `ScanRelations`, `PkInference`, `LineageRows`,
  `ColumnLineageRows` have **zero** unit tests. 2,797 lines with no unit test.
- **9 of the 49 "passing" tests verify nothing.** `docs/OUTSTANDING.md:14,326-334`:
  *"9 lineage/scanRelations :: harness-shape … sql-only: 1 advisory golden-SQL assert(s), no row
  verification"* — `testSameRelationsAtSameLevel`, `testTableToTdsWithCrossJoin`,
  `testTableToTdsWithJoin`, `testTableToTdsWithJoinAndUnion`, `testTableToTdsWithJoinToSameTable`,
  `testTableToTdsWithOLAPGroupBy`, `testTdsJoinConcatenateAndJoin`, `testUnionToSameTableWithDiffKeys`,
  `testUnionWithJoinToOneTable`. Green checks over a well-formedness assertion.
- **The engine's own `scanRelations.pure` — the spec — is deliberately excluded from the run.**
  `docs/GATES.md:422`: *"with `lineage/scanRelations/scanRelations.pure` admitted … the 49 tests
  resolve to IT instead of the platform's natives and wall on `openVariableValues` (DuckDB 2421 →
  2372). The exclusion STAYS."* Legitimate as policy, but **the differential oracle for this file is
  switched off.**
- **The duplication is registered, not unnoticed.** `LegacyReachbackCensusTest.java:89` pins
  `ScanRelations.java → 2` reaches under a *SURFACE CONTRACT* category. The census makes the fork
  conscious; it does not make it correct.

**Conclusion:** shipped feature, golden-only verification, 9 of 49 goldens verify nothing, spec
oracle disabled, zero unit tests. **The duplication matters a lot more than it would as debt.**

---

## The rule-duplication table

"AGREE?" = do the two implementations produce the same answer on the same model?

| # | rule | `ScanRelations` | other owner | agree? |
|---|---|---|---|---|
| 1 | **Include-closure walk** | `withIncludes:2381` / `collectIncludes:2389` — own-first preorder, `seen` on FQN, `ifPresent` = **silent skip**, no package-local bare-path resolution, no store substitutions, no JSON cross-bake | Phase E: `MappingClosures.Closure.mappings:145` / `walkSets:185` / `walkOps:233` / `walkRoots:307` (six walks, three rules). Post-E: `MappingDefinition.classBindingsWithIncludes:326` | **NO** |
| 2 | **Class-mapping lookup by class** | `rootClassMappings:1049` (all hits, `typeMatches` **tail-match**, sorted by setId, `root()` ignored) and `rootImplOrNull:831` (**first** hit wins, `root()` ignored) | `ClassSources.findBinding:1348` — exact FQN equality, **exactly one `root()`**, **LAST include wins**, unresolvable include **throws**. Phase E: `MappingNormalizer.relationalMappingsInClosure:2372` (exact) | **NO — 4 divergences** |
| 3 | **Set lookup by id** | `classMappingFor:2402` — scan closure, `setId.equals(r.setId())`, `hits.size()!=1` → throw | `ResolvedMapping.set:89`; id rule `ResolvedMapping.idOf:82` = `setId ?: className.replace("::","_")`; post-E `ClassSources.findBindingBySetId:489` | **NO** — ScanRelations never honours the derived id; and duplicate ids across sibling includes throw here but silently resolve there |
| 4 | **Join chain resolution** | `joinChain:2231` — per hop `ctx.findJoinDefinition`, "other table" = the one `!= at.table`, `{target}` ⇒ self-join | `JoinChainEmission.emitJoinChain:294` + `MappingNormalizer.determineTargetTable:2719` — same *idea*, but with a `>1 non-source table` **loud wall** (`:2735`) and a `0 non-source table` wall (`:2730`) | **PARTLY** — ScanRelations's third-table wall (`:2277`) is per-column-ref not per-condition, and it has no "references no table other than source" arm |
| 5 | **Main table** | `mainTableOf:2553` — `~mainTable` else **first `PropertyMapping.Column`**, returns `bare()` (strips *every* dotted prefix) | `MappingNormalizer.resolvedMainTable:1425` / `inferMainTable:1473`, **already stamped** as `RelationalSource.Table.table()`; collects from Column + EnumeratedColumn + Embedded + LocalProperty + OtherwiseEmbedded + Expression's direct refs; **loud on >1 database** (`:1489`); **loud on >1 table** (`:1496`); returns `canonicalTable()` (strips only `default.`) | **NO — 3 divergences** |
| 6 | **Property-mapping lookup** | `pmsFor:2181` — own PMs by name; if none, **any association PM with a matching property name**, owner class never checked, and `apm.sourceSetId()!=null && cm.setId()!=null` means a **null setId matches every source set** | `MappingClosures.Closure.ownPairs:402` — checks the association's owner class or a **real subtype** (`knowledge().isSubtype`, `:424-429`), keys by `sourceSetId`, stamps `targetSetId`. Inheritance merges: `MappingPrePass.flattenExtends:157`, `ImplicitInheritance.apply:37` | **NO** |
| 7 | **Embedded / `otherwise`** | `dispatchPms:1831` handles `Embedded` only; `OtherwiseEmbedded`, `InlineEmbedded`, `EnumeratedExpression`, `LocalProperty` all hit `default -> throw` (`:1957`) | `JoinChainEmission.emitHopsForStructuralPm:71` + `MappingNormalizer.materializeEmbedded:2145` / `materializeOtherwiseEmbedded:2205` / `materializeInlineEmbedded:2219` | **N/A — loud gap.** Lineage cannot describe an `otherwise` mapping at all. Loud, so not silently wrong |
| 8 | **Union member enumeration** | Three separate re-derivations: `rootClassMappings:1049` sorted by setId (`:1063-1066`); `unionSets:1494`; `srcUnionLabel:1694-1705` reading `Union.memberSetIds()` off the surface | `UnionSynthesis.synthUnion:364` / `inheritanceMembers:538`, **stamped** as `ClassBinding.Operation.memberSetIds` and `NormalizationFacts.unionMembers`; key columns as `unionKeyThreads` | **NO** — the stamp exists and is ignored. The set-id sort is golden-fitted, admitted in its own comment: *"declaration order fits only the first golden"* (`:1064-1065`) |
| 9 | **Filter handling** | `foldClassFilter:1289` + `assignFilter:1319` | `MappingNormalizer.applyFilter:2256` / `applyDirectFilter:2272` / `applyJoinMediatedFilter:2302`. Notably `:2339` **refuses** a `~filter` with an explicit join type | **PARTLY** — ScanRelations silently accepts the shape the normalizer walls |

---

## The include walk, line by line

```java
// ScanRelations.java:2378-2400
/** {@code md} plus its include closure, own-first (own definitions
 * shadow included ones, the mapping-include rule). */
private static List<LegacyMappingDefinition> withIncludes(ModelContext ctx,
        LegacyMappingDefinition md) {
    List<LegacyMappingDefinition> out = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();
    collectIncludes(ctx, md, out, seen);
    return out;
}

private static void collectIncludes(ModelContext ctx,
        LegacyMappingDefinition md, List<LegacyMappingDefinition> out,
        Set<String> seen) {
    if (!seen.add(md.qualifiedName())) { return; }
    out.add(md);
    for (com.legend.model.MappingInclude inc : md.includes()) {
        ctx.findLegacyMapping(inc.mappingPath()).ifPresent(in ->
                collectIncludes(ctx, in, out, seen));
    }
}
```

| axis | `ScanRelations:2389` | `MappingClosures.walkSets:185` | `MappingDefinition:336` | `ClassSources.findBinding:1348` |
|---|---|---|---|---|
| order | own-first preorder | includes only; deeper-first into a local map, then own overlaid | own-first preorder | own-first, then includes |
| shadowing between sibling includes | **FIRST include wins** | **LATER include wins** (`bySetId.putAll(local)`, `:203`) | first wins | **LAST include wins** (`:1413-1415`) |
| cycle | `seen` on mapping FQN | `seen` on **include path**, shared, never popped | `seen` on resolved path | `visited` on mapping path |
| missing include | **SILENT SKIP** (`ifPresent`, `:2397`) | **THROWS** (`:191`) | silent skip (`:352-355`) | **THROWS** `MappingResolutionException` (`:1409-1411`) |
| bare include path in includer's package | **NO** | only in `walkEnums:349-355` | **YES** (`:341-348`) | n/a |
| store substitutions | **NO** | **YES** (`:200-202`) | n/a | n/a |
| JSON identity sets | **NO** | **YES** (`surface:78` / `withJsonIdentitySets:91`) | n/a | n/a |

**Direct consequence.** On a model with an unresolvable include path, Phase E **refuses to compile**
while `relationTreeAsString` happily prints a smaller tree. And the bare-path case is worse, because
it is **not a broken model**: `include myMapping` (unqualified, same package) resolves for
`classBindingsWithIncludes` and for `walkEnums`, and silently resolves to **nothing** in
`ScanRelations` — a whole included mapping vanishes from the lineage tree and from the plan's
`setImplementation` block, with no error.

---

## Findings

### HIGH-1 — `rootImpl` ignores `root()`; it returns the first declared set

`ScanRelations.java:831-849` (`rootImplOrNull`):

```java
for (LegacyMappingDefinition m : withIncludes(ctx, md)) {
    for (ClassMapping.Relational r : allClassMappings(m)) {
        if (typeMatches(r.className(), classFqn)) {
            ...
            return new String[]{name, setId, mainDbOf(r), mainTableOf(r)};
```

`ClassSources.findBinding:1377-1387` for the same question:

```java
List<...ClassBinding> roots = local.stream().filter(ClassBinding::root).toList();
if (roots.size() == 1)      { local = roots; }
else if (roots.isEmpty())   { local = List.of(); }   // rootless multi-set: 0-binder + poison
else { throw new MappingResolutionException("class '" + classFqn + "' has " + roots.size() + " ROOT set bindings…"); }
```

**The normalizer already learned this lesson and wrote it down** —
`MappingNormalizer.mainTableDefOf:2390-2393`: *"The ROOT set's table — with multiple set IDs …
taking the FIRST declared set bound predicates to the wrong table whenever a non-root set was
declared first (audit)."* **The identical defect is still live in `ScanRelations`, on the plan-text path.**

For a class with a non-root set declared before its `*` set, `planToString` prints the wrong
`setImplementation` id, database and main table, next to SQL generated from the *other* set.

### HIGH-2 — Four-way divergence in root-class-mapping resolution feeding plan text

Beyond `root()`:

1. **Class identity.** `typeMatches:2535` tail-matches unqualified names; `ClassSources.findBinding:1354`
   and `relationalMappingsInClosure:2378` use `equals`. `Person` written in a mapping matches *every*
   `x::y::Person` here and exactly one there.
2. **Include precedence.** First-wins here (`:833-841`), **last-wins** there (`:1413-1415`), with the
   comment at `:1400-1402` explicitly naming the engine rule as *"the LATER include beats the earlier."*
3. **Multi-set handling.** `rootClassMappings:1049-1068` returns *all* hits and builds one root node
   per hit — treating any multi-set class as a union. `findBinding` returns exactly one or refuses.
4. **Missing include.** Silent skip vs `MappingResolutionException`.

### HIGH-3 — `mainTableOf` is a lossy re-derivation of a stamped fact

`ScanRelations.java:2553-2566`:

```java
private static @Nullable String mainTableOf(ClassMapping.Relational cm) {
    if (cm.mainTable() != null) { return bare(cm.mainTable().table()); }
    for (PropertyMapping pm : cm.propertyMappings()) {
        if (pm instanceof PropertyMapping.Column c) { return bare(c.table()); }
    }
    throw new NotImplementedException("scanRelations: set '" + cm.className() + "' has no main table …");
}
```

against `MappingNormalizer.inferMainTable:1473-1511`, whose result is **already stamped** as
`RelationalSource.Table.table()` with the doc *"resolved main table (explicit `~mainTable` or the
engine-parity inference — the SAME call the synthesis makes)"* (`MappingDefinition.java:404-407`).

Three concrete divergences:

- **First-wins, no ambiguity wall.** A set whose column PMs span two tables: the normalizer throws
  *"property mappings span tables … Please specify a main table"* (`:1496`); `ScanRelations` silently
  picks whichever `Column` PM was written first.
- **Contributor set.** The normalizer collects from `EnumeratedColumn`, `Embedded`, `LocalProperty`,
  `OtherwiseEmbedded` and `Expression` (`collectMainTables:1531-1560`). `ScanRelations` sees only
  `Column`. **A set that maps its identity through an embedded block or an enumerated column throws
  "has no main table" in lineage while compiling fine.**
- **`bare()` ≠ `canonicalTable()`.** `ScanRelations.bare:2579` strips *everything* before the last `.`;
  `KnowledgeLayer.canonicalTable:350-352` strips **only** `"default."`. So a
  `~mainTable [db]Schema1.personTable` is `personTable` in the lineage tree and `Schema1.personTable`
  in the stamp — **the plan prints an unqualified table the SQL qualifies.** `mainDbOf:2567`
  compounds it: it returns `c.database()` from the first Column PM, legitimately null for scope-block
  refs, and every call site wraps it in `Objects.requireNonNull` — an NPE-shaped wall on a valid model.

### HIGH-4 — `pmsFor` attributes association property mappings without checking the association's owner class

`ScanRelations.java:2181-2212`:

```java
for (AssociationMapping am : ams) {
    for (AssociationPropertyMapping apm : am.propertyMappings()) {
        if (!apm.propertyName().equals(prop)) { continue; }
        if (apm.sourceSetId() != null && cm.setId() != null
                && !apm.sourceSetId().equals(cm.setId())) { continue; }
        out.add(apm.body());
    }
}
```

The owner is `MappingClosures.Closure.ownPairs:402-429`:

```java
String owner = AssociationSynthesis.associationOwnerClass(ad, apm.propertyName());
if (owner == null || !(owner.equals(classFqn) || model.knowledge().isSubtype(classFqn, owner))) { continue; }
```

Two bugs, both silent:

- **No owner-class check.** Any association anywhere in the closure with a property named `firm` is
  attributed to *any* class mapping asked for `firm`. The tree then joins through a chain belonging
  to a different class.
- **Null-setId over-match.** When `cm.setId()` is null (an unnamed set — very common), the guard
  short-circuits and **every** `sourceSetId` matches. `ownPairs` never has this hole.

### MED-1 — Silent-skip include walk

`ScanRelations.java:2397`, `ifPresent`. Divergent from `MappingClosures.walkSets:191` (throws) and
`ClassSources.findBinding:1409` (throws), and missing the package-local bare-path resolution that
`MappingDefinition.collectIncludedBindings:341-348` has.

### MED-2 — Three private store-table lookups, none of which follows database includes

`pkCols:1793-1810`, `tableHasCol:1812-1828`, `milestoningCols:1119-1170` each build
`new ArrayList<>(db.tables())` plus `db.schemas()` tables and scan. **None follows `db.includes()`.**

Compare: `PureModelContext.findTableDefinition:622` → `tableDefWithIncludes:627-645` (follows
includes, cycle-safe); `KnowledgeLayer.table:360` (*"schema-aware … the database's own tables first
then its includes, transitively"*). `ScanRelations` uses the include-following
`ctx.findTableDefinition` at `:742` and its own scan everywhere else.

**Why it matters:** a table declared in an included database yields **empty** PK columns from
`pkCols`, so the union-arm PK demand that `buildRoots:1035` and `walk:1447` add for instance identity
is silently absent from the tree. `tableHasCol:1812` is the gate on union route-key columns at
`:1626` — an included-database table drops every route key. **The lineage tree omits join keys on a
perfectly ordinary model.**

These lookups are also schema-blind while `bare()` has already discarded the schema, so `Schema1.T`
and `Schema2.T` are the same table to `pkCols` — the exact confusion the schema-aware
`findView:1370-1374` overload was added to fix.

### MED-3 — `isView`/`viewDef`/`viewExpansion` still use the schema-blind view lookup the comment says was wrong

`ScanRelations.java:1364-1366` — the schema-aware overload at `:1370` carries the fix note, and only
`flatten:1070` calls it. `isView:875`, `viewDef:882`, `viewExpansion:934` — the whole
`relTree`/test-data-gen path — go through the blind form. **The documented bug is still live on the
test-data-generation path.**

### MED-4 — No subtype knowledge: `subType(@X)` narrowing is exact-class-only

`typeMatches:2535-2547` is pure string matching. It is the `subType` gate at `dispatchPms:1851` and
`:1866`, and the union-sibling gate at `unionSiblings:1989`. `MappingClosures.ownPairs:425` and
`UnionSynthesis.synthUnion:410` both use `knowledge().isSubtype(...)` for the same question.
`->subType(@Vehicle)` over a set mapping `Car` (a `Vehicle`) is pruned by `ScanRelations` and kept by
the engine — **a missing branch, not a loud wall.**

### MED-5 — Nine catch sites convert loud walls into silent fallbacks; the guardrail counts one

`ErrorShapeGuardrailTest.java:89` pins `ScanRelations.java → 1`, because it counts only
`catch (Exception`. Actual census:

| line | catch | effect |
|---|---|---|
| 1186 | `NotImplementedException` | join label falls back to the join name (documented, benign) |
| 1501 | `NotImplementedException` | `unionSets` → `List.of()` — an *ambiguous class mapping* becomes "not a union" |
| 1716 | `NotImplementedException` | `srcUnionLabel` → null |
| 1978 | `NotImplementedException` | `unionSiblings` → return — same swallow as 1501 |
| 1998 | `NotImplementedException` | `mainTableOf` failure → skip the sibling set |
| 2483 | `NotImplementedException` | `mainTableOf` failure → skip the candidate |
| 2604, 2613 | `IllegalStateException ignore` | **exception-driven control flow** in `rootClassFqn` |
| 2793 | **`RuntimeException`** | `rootFor` → `Optional.empty()` — the broadest |

Lines 1501/1978 are the dangerous pair: `rootClassMappings` throws *"no class mapping for X"* **and**
is the only caller, so "class genuinely unmapped" and "class ambiguously mapped" both collapse to
"not a union" and the tree silently loses its union fork.

### MED-6 — Aggregation-aware routing is invisible to lineage

`ScanRelations` never reads `ClassMapping.Relational.aggregation()` (grep: zero hits).
`ClassBinding.Relational.aggregateViews` is stamped precisely so the router can pick a view. When the
router rewrites onto an aggregate view, **the lineage tree still names the detail table.**
*(Lower confidence — the router's rewrite was not traced end to end. But the fact is stamped and unread.)*

### MED-7 — String-typed structural identity in the node keys drops schema and database

- `joinChain:2252` — `other + "(" + el.joinName() + ")" + keySuffix`. The node is constructed with
  `otherSchema` (`:2255`) but the **key omits it and omits `otherDb`** — two same-named tables in
  different schemas or databases, reached by the same join name, merge into one node and pool their columns.
- `unionNavigate:1607` — `"union#" + ts.setId()`; `setId` may be null → key `"union#null"` collides.
- `attachTdsJoinNamed:433` / `attachTdsJoin:513` — `String.format("%03d", 999 - parent.children.size())`,
  a descending counter that collides past 999 children.
- `unionSiblings:2004` — `child.table + "(" + child.joinName + ")[set" + k + "]"`.

**Honest correction:** the brief flagged `:2517` (`h.className() + "[" + h.setId() + "]"`) as
string-typed identity. It is inside an **error message** only, and a good one. Not a finding.

### LOW-1 — Method length and in-file duplication

93 methods / 2,797 lines. Longest: `dispatchPms` 135 (`:1831`), `unionNavigate` 95 (`:1518`),
`tableToTdsRoots` 80 (`:268`), `walk` 74 (`:1422`), `joinChain` 73 (`:2231`), `attachTdsJoin` 71
(`:450`), `parseTdsSource` 69 (`:540`), `srcUnionLabel` 65 (`:1692`), `targetCm` 63 (`:2459`),
`orUnionLabel` 61 (`:1622`).

**Verbatim triplication:** the "take the last join, `columnRefs` it, split into `sCol`/`tCol`/`tTab`
by comparing `bare(r.table())` against `srcMain`" block appears three times — `unionNavigate:1543-1560`,
`orUnionLabel:1650-1670`, `srcUnionLabel:1730-1750`. The "peel post-join wrapper ops down to the join
spine" loop appears twice — `:271-280` and `:375-385`. The store-table scan appears three times (MED-2).

### LOW-2 — Comments that claim what the code does not do

- `:2440-2446` — the javadoc describing `targetCm` is attached to `propertyTargetClass` (two stacked
  `/** */` blocks; Java keeps the second). Same at `:97-113` and `:2693-2699`.
- `mainTableOf:2553` and `mainDbOf:2567` are annotated `@Nullable`; `mainTableOf` **never returns
  null** (it throws), which is why every call site wraps it in `Objects.requireNonNull` with a message
  about a different failure.
- `withIncludes:2378` says *"own definitions shadow included ones, the mapping-include rule."* That is
  half the rule; `ClassSources.findBinding:1400-1402` calls the engine rule *"the LATER include beats
  the earlier"* between includes.

### LOW-3 — Miscellaneous

- `rootImplOrNull:849` — `depth < 4` magic hop limit (*"max 4 hops, the corpus chains twice"*); at hop
  5 it returns null and `rootImpl:812` throws "no class mapping", blaming the wrong thing.
- `:1019` and `:2749` — `System.getenv("LL_LINEAGE_DEBUG")` + `System.err.println` in production code.
- `scanRoots:162` and `tableToTdsRoots:322,337` — `Comparator.comparing(nd -> nd.table)` over a
  `@Nullable String table`; NPE on a null-table node.
- `walk:1470` — an empty property-mapping list is a silent `return`. Documented as engine parity, and
  it also swallows a genuinely misspelled property.
- `expandView:1234-1252` — first plain `ColumnRef` seeds the root table; disagreement across tables is
  loud (`:1245`), first-wins within a table.
- `assignOne:1350` — DFS first-match; on a self-join (two nodes, same table) a filter column lands on
  the parent only.
- `PkInference.infer:53-91` — `ps.get(0)`/`ps.get(1)` with no arity guard on the
  `filter/limit/drop/slice/sort/extend/from` and `join` arms; a malformed call is an
  `IndexOutOfBoundsException`, not a wall. The `default` composition arm (`:95-111`) ignores argument
  substitution — conservative, and undocumented as such.
- `LineageRows:40-41` — `row.add(l.name())` / `row.add(l.label())` add nulls for the `root` line;
  downstream SQL must tolerate NULL. Fine today, undocumented.

---

## What would have to be stamped for lineage to stop walking the surface

Verified against the actual stamped API.

**Already stamped and simply unused — free wins, no design work:**

| need | existing stamp | used by |
|---|---|---|
| main table + database, canonically spelled | `ClassBinding.Relational.source()` → `RelationalSource.Table{database, table}` | `ColumnLineageRows:67-71` already reads exactly this |
| root set per class, include-closed, `root()`-correct | `classBindingsWithIncludes:326` + `ClassBinding.root()`; or `ClassSources.findBinding:1348` | the compiler itself |
| set id (incl. derived) | `ClassBinding.setId()` + `ClassSources.setIdOf:512` | resolver |
| union member set ids | `ClassBinding.Operation.memberSetIds` (`:285`) and `NormalizationFacts.unionMembers` | `MappingFacts`, resolver |
| union route key columns + ordinals | `NormalizationFacts.unionKeyThreads` → `KeyThread(name, kind, column, ordinal)` | union stack builder |
| property → routed target set | `ClassBinding.Relational.propertyPins` (`:194`) + `routedTargetSets()` + `routedTargetClasses()` | `targetCm:2459` re-derives this from join conditions |
| declared `~primaryKey` / `~groupBy` / `~distinct` / mapped columns / own properties | `ClassBinding.DeclaredKeys` (`:249`) | Phase E |
| extends chain | `ClassBinding.extendsSetId()` | resolver |

Replacing those eight would retire `rootImpl`, `rootClassMappings`, `classMappingFor`, `mainTableOf`,
`mainDbOf`, `unionSets`, `hasUnionOperation`, `targetCm` and both `withIncludes`/`collectIncludes` —
roughly the whole "Lookups" section, `:2350-2590`, plus `:831-870`.

**Genuinely missing — the real gap in the stamped-facts design.** To retire the *tree walk*
(`walk`/`dispatchPms`/`joinChain`/`unionNavigate`):

1. **Per-set property-mapping shape.** `ClassBinding` carries *no* property→mapping information
   beyond `propertyPins` and `DeclaredKeys.ownProperties` (names only). Needed:
   `Map<String, List<PropertyMappingFact>>` where `PropertyMappingFact` is a sealed kind —
   `Column{database, table, column}` / `Enumerated{…, enumMappingId}` / `Expression{columnRefs}` /
   `Join{database, orderedJoinNames, targetSetId}` / `JoinTerminal{…, terminalColumn}` /
   `Embedded{nested facts}` / `Otherwise{partial, fallbackJoin, fallbackSetId}`.
   **The single biggest missing fact** — what `pmsFor:2181` + `dispatchPms:1831` exist to recover.
2. **Association ends merged into the set, owner-resolved.** `MappingClosures.ownPairs:402` computes
   exactly this and then throws it away — nothing on `MappingDefinition` carries it. Stamping its
   output would close HIGH-4 outright.
3. **The ordered JOIN-NAME list per property hop.** This is the census's stated reason the reach
   cannot be closed (`LegacyReachbackCensusTest:34-40`: *"join-NAME vocabulary exists only on the
   authored surface"*). It is correct that the lifted function does not carry it — **and it is 12
   characters of metadata.** Stamping `List<JoinChainElement>` (name, type, database) per Join
   property fact removes the entire justification for the fork.
4. **Class-mapping `~filter`.** `DeclaredKeys` carries distinct/groupBy/primaryKey but **not** the
   filter. Needed: `FilterFact{kind: direct|joinMediated, db, filterName, joinNames}` —
   `foldClassFilter:1289` / `assignFilter:1319` exist only for this.
5. **Inheritance/union `root()` disposition as a fact, not a re-decision.** Stamping the *chosen*
   class-level binding id per class (post-include-closure) gives every consumer one answer instead of
   three implementations.
6. **Store-level facts as a service contract, not a mapping stamp.** PK columns, column existence,
   milestoning windows and views are database facts. `KnowledgeLayer.table:360` and
   `ModelContext.findTableDefinition:251` already provide include-following, schema-aware answers.
   Nothing needs stamping — `ScanRelations` needs to *call* them.

**Blocking structural note:** `MappingClosures` is **package-private with zero `public` members**
(`final class MappingClosures`, `:49`; `grep -n "public "` → 0 hits). Nothing outside
`com.legend.normalizer` can ask it anything. So *"just call `MappingClosures`"* is not available to
`ScanRelations` as the code stands — the only two ways out are the stamped `MappingDefinition`
(items 1–5) or a re-walk (today's choice). **Worth saying plainly in any remediation plan.**

---

## `PkInference` verdict: not a duplicate — verified, not assumed

`PkInference` (`:43-115`) is a **parse-space** walk over relation-function chains rooted at
`#>{db.TABLE}#` (`tableReference`, `:50`). It **never reads a mapping** — no `findLegacyMapping`, no
`findMapping`, no `ClassMapping`, no `~primaryKey`. Its output is the auto-inferred PK of a relation
expression through `filter/rename/select/distinct/groupBy/join/…`.

`MappingNormalizer.declaredPrimaryKeyColumns:2021` reads `ClassMapping.Relational.primaryKey()`.
`UnionSynthesis.memberPrimaryKey:944` reads the same, **filtered to the member's main table**
(`:948-951`), falling back to the physical table's PK columns (`:956-963`).

Three different inputs, three different questions. **Confirmed genuinely different features.**

**But there is a real overlap the brief did not ask about:** the *store-table-PK* lookup is spelled
**three** times.

| site | lookup | follows `db.includes()`? | schema-aware? |
|---|---|---|---|
| `UnionSynthesis.memberPrimaryKey:956` | `model.knowledge().table(db, table)` | **yes** | **yes** |
| `PkInference.tablePk:128` | `ctx.findTableDefinition(db, table)` after stripping to the last `.` (`:124-127`) | **yes** | no |
| `ScanRelations.pkCols:1793` | hand-rolled scan of `db.tables()` + `db.schemas()` | **no** | no |

`pkCols` is the odd one out on both axes — that is MED-2, and it is a live bug.

**One more divergence spotted in passing:** `declaredPrimaryKeyColumns:2026-2029` accepts
`~primaryKey` column refs **regardless of table**, while `memberPrimaryKey:948-951` accepts only those
on the main table. A set whose `~primaryKey` names a joined table stamps a
`ClassBinding.primaryKeyColumns` that its own union key threads will not agree with.
