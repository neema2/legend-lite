# 10 — Engine citations, independently re-censused

**Task:** re-run and extend the citation census in `docs/TRANSLATOR_AUDIT_2026_09_15.md` §1, and
verify the *substance* of each citation — not just that the file exists, but that the cited line says
what the comment claims.

---

## The headline problem: the reference checkouts are not at the pins

| | claimed pin | **actually checked out** | last updated |
|---|---|---|---|
| `$HOME/legend/legend-engine` | 4.145.0 | **`943d38b3dc21` = `legend-engine-4.137.0-36`** | 2026-08-06 |
| `$HOME/legend/legend-pure` | 5.99.0 | **`d00cfd5ba664` = `legend-pure-5.92.0-3`** | 2026-08-05 |

**Neither claimed pin exists on this machine.** `git tag --list '*4.145*'` and `'*5.99*'` are both
empty; the highest tags present are 4.138.1 and 5.92.0. There is no 4.145.0 artefact anywhere under
`$HOME` or `~/.m2`. The only other engine copy on disk is a jar extract at
`/Users/neemsandv/tmp-audit.KKMcZn/allscan/…-4.138.2/`.

The self-audit doc (line 8) says *"Engine tree: `$HOME/legend/legend-engine` at the pin (4.145.0)"*.
That tree was last pulled **2026-08-06, forty days before the 2026-09-15 audit**, and is at 4.137.0.
**So the audit's stated evidence source cannot be reproduced.**

---

## Census table

Verified against 4.137.0-36 / 5.92.0-3 (cross-checked against the 4.138.2 extract where it mattered).
"our file:line" is in `core/src/main/java/com/legend/normalizer/`.

| # | citation | cited at | resolves? | engine actual | engine code matches our claimed rule? | verdict |
|---|---|---|---|---|---|---|
| 1 | `core_functions_unclassified/hash/hash.pure` | RelOpTranslator:53 | yes | path exists | n/a | **OK** |
| 2 | `testMerge.pure:121` (or/and variadic) | RelOpTranslator:522 | yes | `:121` = `or(` with **3** args | yes — variadic confirmed | **OK (exact)** |
| 3 | `pureToSQLQuery.pure resolveJoinElement` | RelOpTranslator:630 | yes | `:1535` | yes | **OK** (no line cited) |
| 4 | `DynaFunction` | RelOpTranslator:633 | yes | `meta::relational::metamodel::DynaFunction` | yes | **OK** |
| 5 | `resolvePrimaryKey` | SetKeyFacts:15 | yes | see #18 | yes | **OK** |
| 6 | `scanRelationsTests.pure` | ViewRelation:100 | yes | path exists | n/a | **OK** |
| 7 | `pureToSQLQuery.pure:5147-5157` (view projects only declared cols) | ViewRelation:253 | file yes, line no | at −332 → 4815-4825 = the `merged`/`fks` block, **not** the projection. Projection is `requiresAllProperties`/`properties` at **4784-4788** | weak — lands on the wrong block even after offset | **DRIFTED** |
| 8 | `OtherwiseEmbedded…` | JoinChainEmission:200 | yes | `OtherwiseEmbeddedRelationalInstanceSetImplementation` | yes | **OK** |
| 9 | `testReprocessGroupByAlias` | JoinChainEmission:904 | yes | `testModelGroupBy.pure:1046` | yes | **OK** |
| 10 | `getRelationalElementWithInnerJoin` | JoinChainEmission:929 | yes | `pureToSQLQuery.pure:4745` | yes | **OK** (no line cited) |
| 11 | `ViewSelectSQLQuery` | JoinChainEmission:946 | yes | `pureToSQLQuery.pure:4855` | yes | **OK** |
| 12 | `relationalModelJoins.pure` | XStorePureEnds:112, :137 | yes | 419-line file | n/a | **OK** |
| 13 | `router_operations.pure getMappedLeafTypes` | UnionSynthesis:479, :583; ImplicitInheritance:101 | yes | `:39` (+ `_recursive` at `:44`) | yes | **OK** |
| 14 | `pureToSQLQuery_union.pure:140–150` | UnionSynthesis:896 | yes | **`:140` = `let key = $q.columns->slice(0, $setImpl->resolvePrimaryKey()->size());`**, `:140-150` = the importDataFlow pk aliasing | yes, exactly | **OK (exact)** |
| 15 | `resolvePrimaryKey` `helperFunctions.pure:439–454` | UnionSynthesis:897 | yes | **`:439`** and **`:454`** are the two `resolvePrimaryKey` overloads | line exact — but both are **dispatchers** (`match` on ersi/rrsi), not the rule | **OK (exact) / see F1** |
| 16 | `resolvePrimaryKey` | UnionSynthesis:943 | yes | as above | yes | **OK** |
| 17 | `platform_store_relational functions.pure:143-167` (`resolveDistinct`) | MappingPrePass:205 | yes | `resolveDistinct` is at **167–176**; **143 = `resolveFilter`**, 155 = `resolveGroupBy` | the range spans three sibling functions and *ends* where `resolveDistinct` *begins* | **DRIFTED** (should be `:167–176`) |
| 18 | same file `:190-214` (pk priority ladder) | MappingPrePass:206 | yes | **`:191` = `resolvePrimaryKey(RootRelationalInstanceSetImplementation)`, `:206-214` = `primaryKeyLogicalOrder`** (groupBy → distinct → userDefined → declared) | **yes, exactly the ladder** | **OK** |
| 19 | `testExtendsWithStoreSubstitution.pure` | StoreSubstitutionRewrite:25 | yes | path exists | n/a | **OK** |
| 20 | `Mapping.resolveStore` (legend-pure `functions_Mapping.pure`) | StoreSubstitutionRewrite:244 | **yes, in legend-pure** | `:110` = `meta::pure::mapping::resolveStore`; `:66` = `_classMappingByIdRecursive` | yes | **OK (exact)** |
| 21 | `HelperRelationalBuilder`, `RelationalCompilerExtension`, `pureToSQLQuery.pure` | MappingNormalizer:76-77 | yes | all three exist | yes | **OK** |
| 22 | `RelationalCompilerExtension` collects all aliases, errors on >1 | MappingNormalizer:1469 | yes | **`:315–362`** (audit cites 316–360) | **yes** — `tableAliasesMap` → `tables.size() > 1` → *"Can't find the main table … Please specify a main table"*; second check on `databases` | **OK** |
| 23 | `HelperRelationalBuilder.java:1172` (alias map takes every direct `TableAliasColumn`) | MappingNormalizer:1543 | file yes, **line no** | actual **`:835`** `aliasMap.getIfAbsentPut(...)`; dyna recursion with the *same* map at **`:854`** | **substance exactly right** | **WRONG LINE** |
| 24 | `HelperRelationalBuilder.java:1182` (join terminal gets a fresh map) | MappingNormalizer:1545 | file yes, **line no** | actual **`:850`**: `processRelationalOperationElement(elementWithJoins.relationalElement, context, **Maps.mutable.empty()**, selfJoinTargets)` | **substance exactly right** — literally a fresh empty map | **WRONG LINE** |
| 25 | `pureToSQLQuery.pure:5187 ViewSelectSQLQuery` | MappingNormalizer:1658 | file yes, **line no** | actual **`:4855`** `^ViewSelectSQLQuery(view=$v, …)` under `v:View[1]` in `processRelation` | **substance right** | **WRONG LINE** |
| 26 | "ViewSelectSQLQuery extends TABLE; a view never flattens" | MappingNormalizer:1689 | yes | **legend-pure `platform_store_relational/grammar/relational.pure:270`: `Class meta::relational::metamodel::relation::ViewSelectSQLQuery extends Table{`** | **yes — verbatim.** And no flattening path exists in `processRelation` | **OK** |
| 27 | `HelperRelationalBuilder.java:521–565` (view main table) | MappingNormalizer:1705 | yes | `resolveMainTable` **517**, `findMainTable` **529**/**544**, `identifyMainTable` **551**, "contains multiple main tables" **560**, ends 563 | **yes** — explicit `~mainTable` else root table of every column mapping, exactly one | **OK** (±4) |
| 28 | `getRelationalElementWithInnerJoin`, `pureToSQLQuery.pure:5077`; chosen at `:5101` | MappingNormalizer:1759-1760 | file yes, **line no** | actual **`:4745`** (def) and **`:4769`** (`if ($innerJoinFilterExists,`). **Relative gap 24 preserved exactly** | **substance right** — `columns = mainRelation().columns->map(c \| ^Alias(name = $c.name, …))`, i.e. original name | **WRONG LINE** |
| 29 | `HelperRelationalBuilder.processRelationalClassMapping` | MappingNormalizer:1876 | yes | `:1156` | yes | **OK** |
| 30 | `resolvePrimaryKey` | MappingNormalizer:1985 | yes | see #18 | yes | **OK** |
| 31 | `HelperMappingBuilder:348-351 getEnumerationMappingId` | MappingNormalizer:2454 | yes | **`:348` = `public static String getEnumerationMappingId`** | yes | **OK (exact)** |
| 32 | `HelperMappingBuilder.processEnumMapping` | MappingNormalizer:2500 | yes | `:215` | yes | **OK** |
| 33 | `ExecuteInDb:81 Types.DECIMAL->Float` | DeclaredCoercions:183 | **yes, in legend-pure** | **`…/interpreted/natives/ExecuteInDb.java:81` = `.withKeyValue(Types.DECIMAL, M3Paths.Float)`** | **yes — verbatim** | **OK (exact)** |
| 34 | `ResultSetValueHandlers` | DeclaredCoercions:184 | yes (legend-pure) | class exists | paraphrase, no line | **OK** |

---

## The offset pattern — and why it is not a fabrication

Every wrong line number lands in exactly two files, `pureToSQLQuery.pure` and
`HelperRelationalBuilder.java`, and the error is a **monotonically growing** offset:

| region | cited | actual | offset |
|---|---|---|---|
| P `navigateToOtherwiseMapping` (T10) | 927 | **720** | +207 |
| P otherwise dispatch (T10) | 1912 | **1705** | +207 |
| P `getRelationalElementWithInnerJoin` | 5077 | **4745** | +332 |
| P `innerJoinFilterExists` | 5101 | **4769** | +332 |
| P `requiresAllProperties` | 5116 | **4784** | +332 |
| P `distinct = getDistinct()` | 5162 | **4830** | +332 |
| P `ViewSelectSQLQuery` | 5187 | **4855** | +332 |
| P `applyGroupBy` | 5331 | **4998** | +333 |
| P `processGroupBy` | 6273 | **5936** | +337 |
| H alias map fill | 1172 | **835** | +337 |
| H fresh map | 1182 | **850** | +332 |
| H view main table | 521–565 | **517–563** | ~0 |

**A constant-offset clerical error cannot produce 0 → 207 → 332 → 337 in one file**, and the
*internal* gaps are preserved to the line (5101−5077 = 24, actual 4769−4745 = 24; 5187−5077 = 110,
actual 4855−4745 = 110). **These numbers came from reading a real, later `pureToSQLQuery.pure`** —
one that is not on this machine. The 4.138.2 extract puts `getRelationalElementWithInnerJoin` at 4744,
so the drift is not between 4.137 and 4.138 either.

**So: the substance is sound, the line numbers are unverifiable here, and the project cannot today
re-check its own citations against the tree it claims to cite. That is the single most important
finding of this dimension.**

---

## Findings

### F1 — HIGH — the self-audit's `functions.pure:190` "stale" verdict is itself wrong, and the "fix" made the citation worse

The audit's §1 table says `functions.pure:190 (resolvePrimaryKey)` is stale and the real site is
`HF:439–454`. **Both halves are wrong:**

- `platform_store_relational/functions.pure:191` **is**
  `resolvePrimaryKey(RootRelationalInstanceSetImplementation)`, and `:206–214` is the actual
  **primary-key priority ladder** (`_thisHasGroupBy` → `superHasGroupBy` → `_thisHasDistinct` →
  `superHasDistinct` → `_thisHasUserDefinedPrimaryKey` → `superHasUserDefinedPrimaryKey` →
  declared). **That is the rule.**
- `helperFunctions.pure:439–454` is a two-line `match` **dispatcher** that delegates to exactly that
  function. Citing it instead of `:191` points at a signpost rather than the rule.

The code itself is fine — `MappingPrePass.java:206` correctly cites `(:190-214)`. It is the **audit
doc's** §1 and T4 rows that are wrong. **Severity is high because it is the one case where "fixing a
citation" replaced a substantive receipt with a hollow one** — precisely the failure mode the
citation discipline exists to prevent.

### F2 — HIGH — `H:1564 processRelationalPrimaryKey` (audit T4 receipt) does not exist

`grep -rn "processRelationalPrimaryKey"` over all of legend-engine returns nothing. Line 1563 of
`HelperRelationalBuilder.java` is `validatePropertyMappings`. This citation is in the audit doc only,
not in the normalizer code.

### F3 — MED — `H:1511` (audit T12, local `+prop` mapping properties) is wrong

Line 1511 is `default: { return null; }` inside a switch; 1513 begins `processFilterMapping`.
Real sites below.

### F4 — MED — `mergeJoinTreeNodes` (audit T17) does not exist anywhere in legend-engine

Real site below.

### F5 — MED — four line-citations in shipped code point at the wrong line

Items 23, 24, 25, 28: `HelperRelationalBuilder.java:1172`, `:1182`, `pureToSQLQuery.pure:5187`,
`:5077`/`:5101`. **Every one is substantively correct at the function level; none resolves at any
engine copy on this machine.**

### F6 — LOW — `platform_store_relational functions.pure:143-167` (MappingPrePass:205)

Cites `resolveDistinct` but the range spans `resolveFilter` (143) → `resolveGroupBy` (155) → and stops
at `resolveDistinct`'s opening line (167). Should be `:167–176`.

### F7 — LOW — `pureToSQLQuery.pure:5147-5157` (ViewRelation:253)

*"the engine projects only the declared columns"*. Even after the +332 correction this lands on the
`merged`/`fks` block, not the projection. The claim's real support is `requiresAllProperties` +
`properties` at `:4784–4788` and `distinct = getDistinct()` at `:4830`.

---

## Hypotheses tested, with verdicts

- **`PureModelBuilder.inferViewMainTable` / `PureModelBuilder.addRuntime` name a nonexistent class** —
  **CONFIRMED but already fixed.** `PureModelBuilder` appears nowhere in the normalizer package at
  `ae16e5c46`; `MappingNormalizer.java:1665` now reads `{@link #inferViewMainTable}`, our own method.
  Commit `49d4eeaad` is an ancestor of HEAD. *(Note: that `{@link}` is itself broken — the method is
  in `ViewRelation`. See `01-mapping-normalizer-core.md`.)*
- **`com.gs.legend.compiler.MappingNormalizer` at MappingNormalizer.java:75** — **CONFIRMED gone.**
  No `com.gs.legend` anywhere in the package now.
- **`functions_Mapping.pure` resolves in legend-pure, not legend-engine** — **CONFIRMED.**
  `legend-pure-dsl/…/platform_dsl_mapping/functions_Mapping.pure`, and `:66`/`:110` are both **exact**.
- **`pureToSQLQuery.pure:5061-5074` → `:5077`** — **the fix is right relative to the file the auditor
  read** (the def is exactly 332 above the actual 4745, like every other P citation), **but
  unverifiable at any tree present here.**
- **T1 alias-map rule** — **FULLY CONFIRMED in substance.** Direct `TableAliasColumn` →
  `aliasMap.getIfAbsentPut` (H:835); `DynaFunc` → recurses with the **same** map (H:854);
  `ElementWithJoins` → recurses with `Maps.mutable.empty()` (H:850). **Joined tables genuinely never
  count.** `RelationalCompilerExtension:315-362` then errors on >1 table or >1 database.
- **T3 groupBy** — **CONFIRMED.** `applyGroupBy(base, viewSpecification, nodeId)` (P:4998) reads
  `$viewSpecification->getGroupBy()` and realiases onto `findLastJoinTreeNode()`.
  `requiresAllProperties` (P:4784) is `addAllColumns || groupBy non-empty || distinct` — a grouped set
  does project every property.
- **`processGroupBy` is a different, query-side function** — **CONFIRMED.** P:5936 takes a
  `FunctionExpression` and is registered in the `PureFunctionToRelationalFunctionPair` table
  (P:10237, 10312-10317) against `meta::pure::tds::groupBy` and
  `meta::pure::functions::relation::groupBy`. Unrelated to `applyGroupBy`.
- **T7 "the engine never flattens a view"** — **CONFIRMED, strongly.** `processRelation`'s `v:View[1]`
  arm wraps unconditionally in `^ViewSelectSQLQuery`, and `ViewSelectSQLQuery extends Table` verbatim
  at legend-pure `relational.pure:270`. **No flattening arm exists.**
- **T10 `P:927` / `:1912`** — file right, lines wrong (actual 720 / 1705, offset +207); substance right.

---

## New work — the "line TBD" / "cite" items, now found

### T11 (enum processing)

Two distinct sites, both in `pureToSQLQuery.pure`:

- **`:4892–4893`** — runtime push-down:
  `if($state.pushDownEnumTransformations && $mapping.transformer->instanceOf(EnumerationMapping) && !$state.inFilter, | let caseParams = …enumValueMappings)` → a SQL `CASE`.
- **`:5627` and `:5644`** — filter side:
  `getEnumPropMappingTransformer()->cast(@EnumerationMapping<Any>)->toSourceValues($literal.value)`,
  for `Literal` and `LiteralList` respectively.
- Compile side already correctly cited: `HelperMappingBuilder.java:215` (`processEnumMapping`) and
  `:348` (`getEnumerationMappingId`).
- Guard worth citing too: **`:8019`** asserts *"Missing an EnumerationMapping for the enum property"*.

### T12 (local `+prop` mapping properties)

**Not `H:1511`.** Two real sites in `HelperRelationalBuilder.java`:

- **`:1068`, `:1076–1079`** — `processRelationalPropertyMapping` sets `_localMappingProperty(true)`,
  `_localMappingPropertyType`, `_localMappingPropertyMultiplicity`.
- **`:1160–1186`** — `processRelationalClassMapping` collects the local properties, synthesises a
  `MappingClass` named `<Class>_<parent>_<id>`, generalises it to the mapped class, and hangs the
  local properties off it as real `Property` instances. **That second block is the one that makes
  them "bind like class properties".**

### T15 (inline-embedded set ids across the include closure)

The chain is **cross-repo**, which is why it was never found:

- `mappingExtension.pure:266–272` — `meta::pure::router::routing::inlineEmbeddedMapping` calls
  `$m->classMappingById($i.inlineSetImplementationId)` and asserts exactly one match.
- legend-pure `functions_Mapping.pure:74` — `classMappingById` delegates to `_classMappingByIdRecursive`.
- legend-pure `functions_Mapping.pure:66–71` — `_classMappingByIdRecursive` maps over
  **`$_this.includes`** recursively before concatenating the local `classMappings`. **That is the
  include-closure walk. Our T15 rule is the engine's.**
- Note: the relation-function sibling at `mappingExtension.pure:277` calls `_classMappingByIdRecursive`
  directly. **Citing `mappingExtension.pure` alone (as the audit proposed) is only half the receipt** —
  the closure walk lives in legend-pure.

*(See also `15-m2m-json-enum.md`, which independently found `helperFunctions.pure:432` for the
embedded-side of the same rule.)*

### T17 (join-tree node dedup)

`mergeJoinTreeNodes` **does not exist**. The real function is:

- **`pureToSQLQuery.pure:8473`** —
  `meta::relational::functions::pureToSqlQuery::merge(parentTargetTreeNode:MergeResultContainer[1], sourceTreeNode:RelationalTreeNode[1], …)`.
- The dedup key is at **`:8480`**:
  `$a.node->children()->cast(@JoinTreeNode)->filter(jtn | eq($jtn.join.name, $childNode.join.name))`
  — i.e. **the engine merges siblings by join NAME**, not by property name. Driver is
  `mergeSQLQueryData` at **`:8595`**.
- **Worth checking our "class hops by property name" half against this**, since the engine's key here
  is the join name.

---

## Did the census of 35 undercount?

**Yes.** A sweep of the package finds ~34 substantive engine citations plus doc/class-name mentions;
the audit's §1 table discusses only 6 and its rule table names ~20. Citations present in the code that
appear **nowhere** in the audit's census or rule table:

`ExecuteInDb:81` and `ResultSetValueHandlers` (DeclaredCoercions:183-184) ·
`core_functions_unclassified/hash/hash.pure` (RelOpTranslator:53) · `testMerge.pure:121`
(RelOpTranslator:522) · `resolveJoinElement` (RelOpTranslator:630) · `relationalModelJoins.pure` ×2
(XStorePureEnds:112,137) · `scanRelationsTests.pure` (ViewRelation:100) ·
`pureToSQLQuery.pure:5147-5157` (ViewRelation:253) · `pureToSQLQuery_union.pure:140–150`
(UnionSynthesis:896) · `HelperMappingBuilder:348-351` and `HelperMappingBuilder.processEnumMapping`
(MappingNormalizer:2454, 2500) · `testReprocessGroupByAlias` (JoinChainEmission:904) ·
`platform_store_relational functions.pure:143-167` and `:190-214` (MappingPrePass:205-206) ·
`router_operations getMappedLeafTypes` at ImplicitInheritance:101.

**Notably, several of the uncensused ones are the most precise citations in the whole package** —
`ExecuteInDb:81`, `pureToSQLQuery_union.pure:140`, `HelperMappingBuilder:348`,
`functions.pure:190-214` are all exact to the line at the tree actually on disk. **The census sampled
the citations it already doubted rather than sweeping the package.**

---

## Bottom line

The citation discipline is working **better than the self-audit's own numbers suggest**: every rule
that could be checked is substantively the engine's rule, including the three strongest claims (the
fresh alias map for join terminals, `ViewSelectSQLQuery extends Table`, `processGroupBy` being
query-side). Every citation into legend-pure is exact to the line.

The defects are (a) four wrong line numbers in shipped comments, all into two files, all pointing at
real code one function-region away; (b) four wrong or nonexistent citations in the audit doc's rule
table; and (c) one case where "fixing a citation" swapped the real rule for a dispatcher.

**The structural problem is bigger than any of those:** the reference checkouts are 8 minor versions
behind the pin the citations are written against, and that pin exists nowhere on this machine. Until
`$HOME/legend/legend-engine` is actually at 4.145.0, *"a citation is a file and a line at the pin"* is
a rule the project **cannot enforce or re-verify** — including the parts of it that are correct.

**Secondary recommendation:** citations are bare `file:line` with no anchoring text, and **no test
verifies any of them**. Adding the cited symbol name to each citation (as items 14, 18, 31, 33 already
do) would make them self-repairing under drift; a generated check against the pinned tree would make
them enforceable.
