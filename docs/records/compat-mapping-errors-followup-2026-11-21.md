# Follow-up: mapping errors upstream raises at run time, as codes in upstream's words

Written 2026-11-21 (stage B's BE, the one error model). **Status: a follow-up, not started.** The user's ruling the
same day: "Okay put this as a follow up and write down the research and then let's go with option 1".

## Why this exists

The user's ruling "D" (2026-11-21): a test body's planning error stays in the test plan, raised at its turn, only if
upstream raises it when the expression RUNS; anything else (a compile error upstream, lite's own gap, a defect) refuses
the body at planning. Each run-time error is a code whose sentence is upstream's (the one catalog), decided per code and
never by the exception's Java class (docs/records/compat-error-model-2026-11-21.md §4).

D as first built kept every `MappingResolutionException` at its turn by its class (`Failures.atItsTurn`). The BE audit
(finding 3) caught it, and the research below shows the class mixes three things: about 14 of its 73 raise sites are
errors legend-engine's router raises when the query runs, about 28 are compile errors upstream, and about 31 are lite's
own gaps or lite-only functions (8 of them not yet settled).

**Option 1, ruled 2026-11-21:** every mapping error refuses the test body at planning, in lite's own words, as a
compile error or a refusal does. Recorded as a divergence in docs/SEMANTICS_REGISTER.md (the row "a mapping error
upstream raises at run is refused at planning"). The other options put to the user: the codes inside BE now (days,
divergences posted case by case), or the class rule kept as a ledgered exception.

**What option 1 costs, until this follow-up lands:** for the ~14 run-time cases, a failing test fails before the asserts
ahead of the error report (pass/fail unchanged: upstream raises too), and an `assertError` that expects one of these
errors fails on lite where upstream passes (it most likely failed already: lite's words are not upstream's). Product
queries are unaffected: a query with a mapping error was always refused at planning.

## The follow-up, in order

1. **The codes.** One run-time code per distinct upstream sentence (§3 below), byte for byte from the pinned sources,
   each stating `Raised.RUNNING`. Where a sentence is compile-time or run-time by the site's context (`LATEST_IN_RANGE`,
   both date-format kinds, `TYPE_NOT_SUPPORTED_YET`), split it into two codes, one per phase, so each code still states
   one phase (the recommendation; the alternative is a site choosing the kind).
2. **The sites.** Each RUN site (§1) raises its code; the three split sites (`ClassSources:716`, `Substitution:1639`,
   `TemporalFrame:1924`) are split by the condition their rows name; every other site keeps refusing at planning.
3. **The operands lite lacks.** Upstream's sentences name facts lite does not carry at those sites today: the set id
   (explicit, else the class path with `::` as `_`, an embedded set's `parentId_property`), the mapping that DECLARES
   the set (it can be an include, not the query's mapping), a table's schema, the milestoning dimension. Each one is
   computed, or, where that is too costly, the closest thing is posted to the user before it lands (the fidelity rule).
4. **Upstream's exemption: an unmapped OPTIONAL property answers empty.** legend-engine's router raises "No property
   mapping found" only for a required property that is not auto-mapped and whose set is not part of a merge
   (`builder.pure:392`, `modelToModel.pure:120`); an optional one yields empty. Lite raises today. This changes what a
   PRODUCT query returns (empty instead of an error), so it is reviewed on its own, not folded into an error change.
5. **The 8 UNSURE sites** (§1): read upstream further and settle each (chain dispatch, `getAllForEachDate`, class-typed
   and association properties upstream marks VOID, the missing runtime).
6. **The probes.** Six places catch `MappingResolutionException` as a probe (§1's last note): each keeps catching
   whatever the error becomes.
7. **The two lowering raises (§2), the same shape:** `TYPE_NOT_SUPPORTED_YET` is upstream's run-time raise only for a
   class-typed RELATION COLUMN (move the raise to the column's lowering; a `to(@C)` anywhere else refuses at planning:
   today lite marks every one RUNNING, so a body that upstream answers with an instance rides the plan); a date format
   that never parses is compile-time for a LITERAL format string and run-time for a computed one (today lite refuses
   both at planning, under no code: right for a literal).
8. **Measure** (core, the corpus, PCT, channel B), list every moved pin for the user, audit, and land.

## The research (read-only, 2026-11-21, lite at fc32c272 on claude/compat-error-model)

Classes: **RUN** means upstream raises it when the expression is routed or executed. **COMPILE** means upstream's
compiler (or grammar) raises it. **GAP** means upstream does it and lite refuses (this includes lite-internal invariant
walls that should really be `IllegalStateException`). **LITE** means a lite-only function, so upstream has no answer
and the error is not RUN. **UNSURE** means no upstream sentence was found (what was searched is noted). Lite paths are
relative to `core/src/main/java/com/legend/`; line numbers are at fc32c272 and drift.

Abbreviations for the pinned sources:

| Short | Path |
|---|---|
| ENG | the pinned legend-engine source archive (Bazel repository `+http_archive+legend_engine_src`) |
| PURE | the pinned legend-pure source archive (Bazel repository `+http_archive+legend_pure_src`) |
| ROUTER | `ENG/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router` |
| GFETCH | `ENG/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/graphFetch` |
| ENGC | `ENG/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph` |
| RELC | `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph` |
| RELG | `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-grammar/src/main/antlr4/org/finos/legend/engine/language/pure/grammar/from/antlr4` |
| MILES | `ENG/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/milestoning/milestoning.pure` |
| P2SQL | `ENG/legend-engine-xts-relationalStore/.../legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure` |
| PVAL | `PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/compiler/validation` |

## 0. The upstream sentences this file relies on (quoted from the pinned code)

| Id | Where (file:line) | Code, verbatim |
|---|---|---|
| U1 | ROUTER/store/builder.pure:395 | `assert(!$pMapping->isEmpty(), \|'No property mapping found for: "'+$propertyName+'" in "'+$setImpl.id+'" in the mapping "'+$setImpl.parent->elementToPath()+'"');` |
| U1x | ROUTER/store/builder.pure:392 (the guard before U1) | `if($pMapping->isEmpty() && ($instanceSetImpl->isPropertyAutoMapped(...) \|\| $instanceSetImpl->isNoMappingDefaultToEmpty($property) \|\| $instanceSetImpl->isPartOfMerge()), \| [], ...` (no error). `isNoMappingDefaultToEmpty` = `!$property.multiplicity->hasLowerBound() && $property.owner->instanceOf(Class)` (ENG/.../core/pure/mapping/modelToModel.pure:120-123), so an unmapped **optional** property yields empty, with no error. |
| U2 | ROUTER/store/builder.pure:411 (operation/union member) | `assert(!$pMapping->isEmpty(), \|'No property mapping found for: '+$propertyName+' in mapping '+$resolvedImpl.class.name->toOne()+'['+$resolvedImpl.id+']');` |
| U3 | ROUTER/store/routing.pure:483 (a DataType property read off a class with no class mapping) | `assert($resolvedClassMappings->isNotEmpty(), \| 'The class \''+$sourceClass.name->toOne()+'\' can\'t be found in the mapping \''+$mapping.name->toOne()+'\'');` (simple names) |
| U4 | ROUTER/routing/router_routing.pure:584 (a lambda parameter's class has no routed set) | `assert(!$map->isEmpty(),\| 'Error mapping not found for class '+$class.name->toOne()+' cache:\''+$routed->cast(@...StoreMappingRoutedValueSpecification).sets.class.name->joinStrings(', ')+'\'');` |
| U5 | GFETCH/graphExtension.pure:568 (M2M graphFetch, unmapped property) | `assert($setImplementation->isNoMappingDefaultToEmpty($requiredProperty->toOne()) \|\| $setImplementation->isPartOfMerge(), \|'No mapping found for property \'' + $tgtPgft.property.name->toOne() + '\'');` |
| U6 | GFETCH/graphExtension.pure:456 (graphFetch root class unmapped) | `assert($rootSetImplementation->isNotEmpty(), \|'Mapping ' + $mapping->elementToPath() + ' does not map class from tree: ' + $tree.class->elementToPath());` |
| U7 | MILES:421 | `assert(!$date->isLatestDate(), '%latest not a valid businessDate for TemporalSnapshotMilestoning');` |
| U8 | MILES:661 (twin at :645) | `assertNotEmpty($infinityDate,\| '%latest usage for temporal parameter: '+$t->temporalColumnAliasProperties().first+' Table: '+$table.schema.name+'.'+$table.name+' requires it to specify a milestoning \'INFINITY_DATE\'');` where `.first` is `'processingDate'` or `'businessDate'` (MILES temporalColumnAliasProperties) |
| U9 | MILES:451 (snapshot twin :459) | `assert(!$start->isLatestDate() && !$end->isLatestDate(), '%latest not a valid parameter for allVersionsInRange');` |
| U10 | MILES:964 | `assertFalse($isGenProp && $dateArgs->isEmpty() ,\|'No-Arg milestoned property: \''+$fe.func->...printFunctionSignature()+'\' must either be supplied with dates or be called in a Milestoned Context');` |
| U11 | ROUTER/routing/router_routing.pure:151 | `\| fail('RelationStoreAccessor should be used with a from(Runtime) expression.'); $state;,` |
| C1 | ENGC/PureModel.java:1258 | `"Can't find mapping '" + fullPath + "'"` (CompileContext.resolveMapping, CompileContext.java:387; includes via handlers/MappingIncludedMappingHandler.java:30, called at MappingCompilerExtension.java:100) |
| C2 | ENGC/PureModel.java:1304 | `"Can't find runtime '" + fullPath + "'"` (CompileContext.java:407; HelperRuntimeBuilder.java:303) |
| C3 | ENGC/PureModel.java:1345 | `"Can't find connection '" + fullPath + "'"` (HelperRuntimeBuilder.java:151) |
| C4 | ENGC/PureModel.java:821 | `"Can't find the packageable element '" + ... + "'"` (a lambda's element pointer, ValueSpecificationBuilder.java:185). legend-pure says `idOrPath + " has not been defined!"` (PURE/.../m3/exception/PureUnresolvedIdentifierException.java:76) |
| C5 | ENGC/ClassMappingSecondPassBuilder.java:97 | `"Can't find class mapping '" + classMappingId + "' in mapping '" + ... + "'"` (an operation's member set id) |
| C6 | ENGC/ClassMappingThirdPassBuilder.java:109 | `"Can't find class mapping '" + p._targetSetImplementationId() + "'"` (a property mapping's target set id) |
| C7 | ENGC/MappingCompilerExtension.java:162 | `"Class '" + ... + "' is mapped by " + n + " set implementations and has " + rootCount + " roots. There should be exactly one root set implementation for the class, and it should be marked with a '*'"` (legend-pure twin: PURE/legend-pure-dsl/.../validator/MappingValidator.java:448) |
| C8 | PVAL/functionExpression/GetAllValidator.java:58 | `"The type " + name + " is  " + stereotypes + ", " + temporalPropertyNames.makeString("[", ",", "]" + " should be supplied as a parameter to all()")` |
| C9 | PVAL/functionExpression/GetAllValidator.java:62 | `"The type " + name + " is not Temporal, Dates should not be supplied to all()"` |
| C10 | PVAL/functionExpression/GetAllVersionsInRangeValidator.java:55 | `".allVersionsInRange() is applicable only for businessTemporal and processingTemporal types"` (bitemporal included in the refusal) |
| C11 | PVAL/functionExpression/GetAllVersionsInRangeValidator.java:63; property form PVAL/validator/MilestoningFunctionExpressionValidator.java:57 | `"%latest not a valid parameter for .allVersionsInRange()"`; `"%latest not a valid parameter for AllVersionsInRange()"` |
| C12 | PVAL/validator/MilestoningFunctionExpressionValidator.java:67 | `"No-Arg milestoned property: '" + name + "' must be either called in a milestoning context or supplied with " + names.makeString("[", ", ", "]") + " parameters"` |
| C13 | RELC/HelperRelationalBuilder.java:1320 | `String.format("Milestone column '%s' not found on table definition", colName)` |
| C14 | RELG/RelationalParserGrammar.g4:109-110, 124-125 | `businessMilestoningFromThru: BUSINESS_MILESTONING_FROM EQUAL identifier COMMA BUSINESS_MILESTONING_THRU EQUAL identifier`; `processingMilestoningInOut: PROCESSING_MILESTONING_IN ... COMMA PROCESSING_MILESTONING_OUT ...` (a half block does not parse) |

These validators (C8-C12) run during compile, from PVAL/validator/FunctionExpressionValidator.java:99-111. Set ids
default to the class path with `::` replaced by `_` (ENGC/HelperMappingBuilder.java:355). An embedded set's id is
`sourceId + "_" + property` (RELC/HelperRelationalBuilder.java:1497).

## 1. Every `new MappingResolutionException(` in core/src/main/java (75 hits; 73 raise sites)

`testexecutionplan/Failures.java:97` and `:119` rebuild a planned failure at its turn, so they are not raise sites.

| Site | Lite's message (as built) | Class | Upstream (citation) |
|---|---|---|---|
| Compiler.java:798 | `NO_RUNTIME` = "a query executes on its runtime's declared connection, and none was given: add ->from(mapping, runtime) or supply a runtime" | UNSURE (split) | Class query: **GAP**, because legend-pure's `getAll` native answers in memory (`context.getClassifierInstances`, PURE/legend-pure-runtime/.../interpreted/natives/grammar/lang/all/GetAll.java:45). Relation store accessor with no `from()`: candidate **RUN** U11. I did not confirm that a test body with no `from()` is routed at all. |
| Compiler.java:800 | "runtime '%s' is not defined" | COMPILE | C2, or C4 for a lambda's pointer |
| Compiler.java:818 | "connection '%s' of runtime '%s' is not defined" | COMPILE | C3 |
| planner/PlanMaker.java:1236 | "a query naming the runtimes %s in its from()s, and given none: which one it runs on is the caller's to say" | GAP | Upstream routes each `from()` on its own runtime: ROUTER/store/routing.pure:588-605 (`from_T_m__Runtime_1__T_m_` → `routingStrategy = getRoutingStrategyFromRuntime($fromRuntime)` for that subtree) |
| resolver/ClassSources.java:279 | "unknown mapping '%s'" | COMPILE | C1 / C4 (defensive; the element was already resolved) |
| resolver/ClassSources.java:387 | "unknown mapping '%s'" | COMPILE | C1 / C4 |
| resolver/ClassSources.java:430 | "every route of a navigation into '%s' pins a set outside the class's root under mapping '%s'" | GAP | Lite-internal route-stack invariant. Upstream prunes impossible permutations as VOID (ROUTER/store/builder.pure:295-297) and does not raise. |
| resolver/ClassSources.java:477 | "unknown mapping '%s'" | COMPILE | C1 / C4 |
| resolver/ClassSources.java:482 | "route target '%s' is not a set's function in mapping '%s' or its includes" | GAP | Lite-internal: the function is lite's synthesized set function. No upstream counterpart. |
| resolver/ClassSources.java:679 | "unknown mapping '%s'" | COMPILE | C1 / C4 |
| resolver/ClassSources.java:716 | "class '%s'[' set '%s] is not mapped in mapping '%s'[ (%s)]" (the suffix is the normalization poison) | **split** | (a) `setId != null`: COMPILE (C6, or C5 for an operation member). (b) a poison reason is present: GAP, because lite's normalizer failed where upstream compiled (COMPILE only if upstream's compiler rejects the same text). (c) class-level, no reason: **RUN**. The sentence depends on the consumer: a lambda parameter of the class → U4; a DataType property read outside a lambda → U3; an M2M graphFetch root → U6; a bare `getAll` → UNSURE. For (c), ROUTER/store/routing.pure:381-398 `processClass` returns the state unrouted ("Possible when the mapping is 'embedded'"), and the error surfaces at U3, U4 or U6. |
| resolver/ClassSources.java:1083 | "whole-source child '%s' names set '%s' of '%s', which mapping '%s' does not bind" | COMPILE | C6 |
| resolver/ClassSources.java:1282 | "mapping '%s' includes unknown mapping '%s' (a silently-unresolved include hid class bindings)" | COMPILE | C1 (the include path) |
| resolver/ClassSources.java:1442 | "class '%s' has %d ROOT set bindings in mapping '%s'" | COMPILE | C7. Lite's phase F already enforces this (`compiler/ModelBuilder.java:582`), so this site is likely unreachable. |
| resolver/ClassSources.java:1467 | "mapping '%s' includes unknown mapping '%s'" | COMPILE | C1 |
| resolver/ClassSources.java:1508 | "class '%s' is not mapped in '%s' and its ModelChainConnection %s has %d binders — chain dispatch needs exactly one" | UNSURE | Lite's exactly-one-binder rule. I searched ENG/.../core/pure/mapping/modelToModel.pure; its chain asserts (:291, :297, :299) are about connections, not binders. No match. |
| resolver/ClassSources.java:1521 | "self-sourced class '%s' needs an upstream layer, but the context names only mapping '%s' (no runtime candidate set)" | UNSURE (likely GAP) | No counterpart found (same search) |
| resolver/ClassSources.java:1542 | "class '%s' chain dispatch over %s has %d binders — needs exactly one" | UNSURE | As at 1508 |
| resolver/ClassSources.java:1556 | "unknown runtime '%s'" | COMPILE | C2 / C4 |
| resolver/ClassSources.java:1583 | "runtime '%s' has %d mappings binding class '%s' (of %d candidates); class-query dispatch needs exactly one[; '%s' failed to normalize this class: %s]" | GAP / UNSURE | This is lite's own dispatch by a runtime's mapping list. Upstream's `from(runtime)` builds the strategy from the runtime (routing.pure:588-605). I found no mapping-list dispatch. The poison suffix makes it GAP. |
| resolver/UnionHeads.java:111 | "unknown mapping '%s'" | COMPILE | C1 / C4 |
| resolver/UnionHeads.java:249 | "property '%s' of class '%s'[ (through '%s')] is not mapped" | **RUN** (required property only) | U2. For an optional property, U1x applies to union members too (builder.pure:408): upstream yields empty, so lite is GAP there. |
| resolver/ClassWrites.java:90 | "%s: the end %s of %s names no column of the pairs %s" | LITE | `meta::legend::lite::link`/`unlink` (`builtin/Pure.java:556-560`, PKG `meta::legend::lite::`). No upstream function. |
| resolver/ClassWrites.java:93 | "%s: %s is no association end of %s" | LITE | as above |
| resolver/ClassWrites.java:191 | "writing %s: the rows' column %s is no property its mapping %s maps" | LITE | `writeInstances`/`mergeInstances`/`deleteInstances` |
| resolver/ClassWrites.java:198 | "writing %s: %s and %s are one column, %s" | LITE | as above |
| resolver/ClassWrites.java:275 | "%s is not writable: its mapping %s reads its instances through %s, not one table's rows (...)" | LITE | as above |
| resolver/ClassWrites.java:289 | "%s is not writable: its mapping %s gives it %d key columns, a write needs one (...)" | LITE | as above |
| resolver/ClassWrites.java:326 | "%s is not writable: its mapping %s computes %s (%s), not one column's value (...)" | LITE | as above |
| resolver/TemporalFrame.java:279 | "navigation to bi-temporal class '%s' requires processing and business dates" (one of the two dates given) | COMPILE (sentence UNSURE) | Lite's own javadoc says "the engine compile-rejects". The candidates are C12, or no matching milestoned-property overload (PURE PureUnmatchedFunctionException.java:35 "The system can't find a match for the function: "). I did not trace legend-pure's bitemporal overload generation (MilestoningPropertyProcessor). |
| resolver/TemporalFrame.java:672 | "milestoned fetch of '%s': the class declares no temporal stereotype" | COMPILE | C9 |
| resolver/TemporalFrame.java:819 | "bitemporal window column '%s' is not on the join row" | COMPILE if the table lacks it, else GAP (defect) | C13 is the model case. Otherwise it is lite's row shape. |
| resolver/TemporalFrame.java:1248 | "milestoning column '%s' is not on the join target row" | COMPILE / defect | C13 |
| resolver/TemporalFrame.java:1257 | "outer milestoning date column '%s' is not on the join source row" | COMPILE / defect | C13 |
| resolver/TemporalFrame.java:1408 | "bi-temporal class fetch of '%s' is not supported yet" | COMPILE when reached by a one-date `getAll` of a bitemporal class, else GAP | C8 (2 temporal properties, 1 date) |
| resolver/TemporalFrame.java:1419 | "milestoning block of '%s' declares only one of FROM/THRU — a half-declared block cannot filter versions" | COMPILE (parse) | C14 |
| resolver/TemporalFrame.java:1452 | "milestoning column '%s' is not on the pipeline row of '%s'" | COMPILE / defect | C13 |
| resolver/TemporalFrame.java:1472 | "%latest over a SNAPSHOT-milestoned table is not supported yet" | **RUN** | U7. `%latest` as a `getAll` argument passes legend-pure's compile (MilestoningFunctionExpressionValidator.java:49-54 allows it for getAll). |
| resolver/TemporalFrame.java:1499 | "snapshot fetch date '%s' has no day component — a full date is required" | GAP | Upstream does not refuse. MILES:419-431 `getSnapshotMilestoneFilter` writes `datePart($date)` for a non-StrictDate literal. |
| resolver/TemporalFrame.java:1542 | "%latest usage for temporal fetch of '%s' requires table '%s' to specify a milestoning 'INFINITY_DATE'" | **RUN** | U8. Lite lacks two of upstream's operands: the dimension and the schema. |
| resolver/TemporalFrame.java:1782 | "temporal join-target filtering through %s is not supported yet" | GAP | |
| resolver/TemporalFrame.java:1858 | "milestone filter pushdown through %s is not supported yet" | GAP | |
| resolver/TemporalFrame.java:1877 | "allVersionsInRange of '%s': the class declares no temporal stereotype" | COMPILE | C10 |
| resolver/TemporalFrame.java:1918 | "bi-temporal allVersionsInRange of '%s' is not supported yet" | COMPILE | C10 refuses bitemporal at compile. Lite's "not supported yet" wording is wrong here. |
| resolver/TemporalFrame.java:1924 | "%latest is not a valid parameter for allVersionsInRange" | COMPILE (literal) / RUN (non-literal) | A literal `%latest` is C11. A `%latest` that reaches the call through a variable is U9 at run. Lite's test is `instanceof TypedCLatestDate` after G½, so it cannot tell the two apart. Lite's text also adds "is". |
| resolver/TemporalFrame.java:1947 | "milestoning column '%s' is not on the pipeline row of '%s'" | COMPILE / defect | C13 |
| resolver/TemporalFrame.java:2256 | "navigation '%s' to bi-temporal class '%s' requires processing and business dates" | COMPILE (sentence UNSURE) | as at 279 |
| resolver/TemporalFrame.java:2330 | "navigation '%s' to temporal class '%s' requires a milestoning date (property function argument, or a propagated temporal context through temporal parents)" | COMPILE | C12. The engine's run-time twin U10 fires only for a context that legend-pure's compile propagation saw and routing lost. |
| resolver/TemporalFrame.java:2740 | "getAllForEachDate of '%s': the class declares no temporal stereotype" | UNSURE | `getAllForEachDate` is engine Pure, `fail('not implemented')` (ENG/.../core/pure/corefunctions/collectionExtension.pure:230-234). There is no validator (PVAL only covers `getAll_Class_1__*`), and P2SQL:4934-4980 `processGetAllForEachDate` asserts nothing. |
| resolver/TemporalFrame.java:2745 | "getAllForEachDate of the bi-temporal class '%s' is not supported yet" | GAP / UNSURE | P2SQL:4956 takes `currentMilestoningStrategy->at(0)` and does not refuse |
| resolver/TemporalFrame.java:2751 | "getAllForEachDate of '%s': the dates argument must resolve to a ONE-column relation" | GAP (defect) | Lite's representation (upstream's `dates:Date[*]`) |
| resolver/TemporalFrame.java:2790 | "getAllForEachDate of '%s': the mapped table declares no milestoning block" | UNSURE | No assert found in P2SQL:4934-4980 |
| resolver/CorrelatedSubselects.java:1114 | "property '%s' of class '%s' has no binding in mapping '%s' (aggregated navigation leaf)" | **RUN** (required leaf) | U1 on the hop target's set. Optional leaf: U1x applies, so GAP. |
| resolver/Substitution.java:1309 | "property '%s' of nested navigation '%s' is not mapped in mapping '%s'" | **RUN** (required) | U1 (target set) |
| resolver/Substitution.java:1530 | same text, chainKey | **RUN** (required) | U1 |
| resolver/Substitution.java:1639 | "property '%s' of class '%s' has no binding in mapping '%s' (unmapped, or routed to a non-root mapping set — multi-set union dispatch is a roadmap feature)" | **split** | Truly unmapped and required → RUN U1. "Routed to a non-root set" → GAP (upstream dispatches it). Lite does not tell these apart today. |
| resolver/Substitution.java:2466 | "property '%s' of embedded '%s' on class '%s' is not mapped in mapping '%s'" | **RUN** (required) | U1. An embedded set is an InstanceSetImplementation; its id is `parentId_head` (RELC:1497). |
| resolver/Substitution.java:2571 | "property '%s' of class '%s' is not mapped in mapping '%s'" (association target leaf) | **RUN** (required) | U1 |
| resolver/Substitution.java:2808 | "property '%s' of class '%s' has no binding in mapping '%s' (membership crossing leaf)" | **RUN** (required) | U1 |
| resolver/GraphEmission.java:349 | "property '%s' of class '%s' is not mapped in mapping '%s'" (graphFetch, required) | **RUN** | M2M (Pure) set: U5. Relational: U1, inferred (graphFetch_routing.pure:43-70 consumes a property the store router already routed; I did not trace it end to end). Lite already serves an optional property as null, which matches U1x/U5. |
| resolver/GraphEmission.java:1959 | "property '%s' of embedded '%s' on class '%s' is not mapped in mapping '%s' [otherwise fallback=%s]" | **RUN** (required) | U1 on the embedded set (inferred, as above). The `[otherwise fallback=..]` suffix is lite debug text. |
| resolver/StackBuilder.java:197 | "operation '%s' names member set '%s', which mapping '%s' and its includes do not bind" | COMPILE | C5. Lite already has the sentence: `CompileKind.MISSING_CLASS_MAPPING`. |
| resolver/StackBuilder.java:360 | "route condition reads '%s', which no target set's rows carry (class '%s', mapping '%s')" | GAP (defect) | Lite-internal |
| resolver/StackBuilder.java:933 | "union key thread '%s' reads column '%s', which no arm's row carries" | GAP (defect) | Lite-internal |
| resolver/StackBuilder.java:1239 | "lift '%s': arm %s[%s] reads '%s', which its rows do not carry (row %s)" | GAP (defect) | Lite-internal |
| resolver/StackBuilder.java:1315 | "lift '%s': the route condition reads '%s', which the target rows do not carry (condition typed %s)" | GAP (defect) | Lite-internal |
| resolver/AssociationJoins.java:871 | "property '%s' of class '%s' is not mapped in mapping '%s'" (a class-typed head that is no association end) | UNSURE | Upstream marks an unmapped class-typed property VOID (ROUTER/store/builder.pure:290-297), and ROUTER/store/routing.pure:505-508 passes it on unrouted. No sentence found. |
| resolver/AssociationJoins.java:1207 | "association '%s' is not mapped in mapping '%s'[ (%s)]" | UNSURE; GAP with a poison suffix | Same VOID path (builder.pure:239-247 association lookup, no assert) |
| resolver/StoreResolver.java:1346 | "elementId: its argument must be an instance of a class, not %s" | LITE | `meta::legend::lite::elementId` (`builtin/Pure.java:428`). No upstream function. |
| resolver/StoreResolver.java:1354 | "elementId: the class %s has no identity under the mapping %s: ..." | LITE | as above |
| resolver/StoreResolver.java:1667 | "class query requires an execution context: add ->from(mapping, runtime) or supply a runtime" | GAP | legend-pure GetAll.java:45 answers from the in-memory instances |
| resolver/StoreResolver.java:2967 | "class fetch of '%s' with %d milestoning arguments is not supported" | COMPILE | `getAll` natives take only 1 or 2 dates (PURE/.../platform/pure/grammar/milestoning.pure:57-58), so a call with 3 matches nothing: PureUnmatchedFunctionException.java:35 |
| resolver/StoreResolver.java:2976 | "fetch of temporal class '%s' requires a milestoning date argument (use allVersions() for the unfiltered extent)" | COMPILE | C8 |

Count over the 73 raise sites:

| Class | Count | Sites |
|---|---|---|
| RUN | 11 | UnionHeads:249; TemporalFrame:1472, 1542; CorrelatedSubselects:1114; Substitution:1309, 1530, 2466, 2571, 2808; GraphEmission:349, 1959 |
| Split, RUN in part | 3 | ClassSources:716; Substitution:1639; TemporalFrame:1924 |
| COMPILE | 23 | |
| COMPILE, or a defect | 5 | the milestoning-column rows |
| LITE | 9 | |
| GAP | 14 | |
| UNSURE | 8 | includes Compiler:798 |

**This exception's class does not make an error RUN.** Every RUN row above is RUN only for a required property (or
another condition its row states).

MRE is also caught as a probe, so a code change must keep these catching whatever the class becomes:
`resolver/CorrelatedSubselects.java:2234, 2280`, `resolver/ElementReferences.java:136, 159`,
`resolver/AssociationJoins.java:724` and `normalizer/MappingNormalizer.java:250`.

## 2. The two lowering raises

### 2a. `lowering/CastPolicy.java:131-143` (and its twin `lowering/Lowerer.java:3862-3871`), `TYPE_NOT_SUPPORTED_YET`

Today lite raises `CompileKind.TYPE_NOT_SUPPORTED_YET` ("The type %1$s is not supported yet!", `Raised.RUNNING`) on
every single-valued, materialized `to(@C)` of a Variant, where C is a class that is not Variant or Any. The twin
raises the same for a class-typed leaf of a property chain over `to(@C)`.

| Case | Upstream | Class |
|---|---|---|
| The `to(@C)` value **is a relation column's value**: the result of a col-spec function in project, extend or groupBy keys, or of an agg spec's map or reduce | engine compiled: `ENG/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-runtime-java-extension-compiled-functions-relation/.../relation/compiled/RelationNativeImplementation.java:444-503`. `performExtend` dispatches on `colFuncSpecTrans.functionType._returnType()._rawType()` (String, Integer, Float, Decimal, Variant, Date, Boolean, Number) and otherwise does `throw new PureExecutionException("The type " + PackageableElement.getUserPathForPackageableElement(colFuncSpecTrans.functionType._returnType()._rawType()) + " is not supported yet!")`. Interpreted: `.../interpreted/natives/shared/AggregationShared.java:536` (`processFuncColSpec`, same text over `type`) and `:109` (`processOneAggColSpec`, the reduce's return type). PCT: `.../core_functions_relation/relation/tests/composition.pure:1445-1456` `testVariantColumn_modelOutputNotSupported`, `#TDS...#->project(~[name: x \| $x.payload->to(@...Person)])` inside `assertError(..., 'The type meta::pure::functions::relation::tests::composition::Person is not supported yet!')`. The operand is the full path, which matches lite's `tc.fqn()`. | **RUN** |
| The `to(@C)` value is anything else: the expression's result, a let, an element of a collection, an argument to a non-relation function, or an intermediate value inside a column whose column type is not C (e.g. `...->to(@C)->someFn()` returning a String) | Upstream answers with an instance. `ENG/.../legend-engine-pure-code-functions-variant/legend-engine-pure-functions-variant-pure/src/main/resources/core_functions_variant/functions/convert/to.pure:188-192` `testToClass`: `$f->eval(\|fromJson('{"firstName": "John", "lastName": "Smith"}')->to(@Person))` equals `^Person(firstName = 'John', lastName = 'Smith')`. The native is `.../compiled-functions-variant/.../compiled/natives/To.java:288-297` (a JSON object goes through `JsonGen.fromJson` into the class) and interpreted `.../interpreted/natives/AbstractTo.java:203`. A non-object JSON to a class instead raises at run `GenericType.print(...) + " is not managed yet!"` (To.java:306, AbstractTo.java:223), which is a different sentence. | **GAP** (refuse at planning) |

**The separating condition.** Upstream's raise is about the **column's type**, not the cast. It fires when a relation
column function's declared return type is a class (anything not a subtype of String, Integer, Float, Decimal, Variant,
Date, Boolean or Number). That covers `to(@C)`, a class-typed property leaf over it, and any other class-valued column,
such as one whose value is `^C(...)`.

So the test belongs where a column is lowered, on the typed column (TypedFuncCol, or an agg col's map or reduce
lambda) whose return type is a ClassType that is not Variant or Any. Raise RUN there, with the column type's path.
CastPolicy and the Lowerer leaf arm cannot see that context, so a `to(@C)` reached anywhere else must refuse at
planning instead of carrying a RUN kind.

### 2b. `lowering/DateFormats.java:50-52` and `:138-139`

| Lite site | Lite text | Upstream sentence (verbatim) and where |
|---|---|---|
| DateFormats.java:51-52 (`javaDateToParts`) | "Missing closing quote in format string: " + pattern | `PURE/legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/coreinstance/primitive/date/DateFormatPattern.java:2273` (`parseQuotedText`): `throw new IllegalArgumentException("Missing closing quote in format string: " + text());`. `text()` is `formatString.substring(this.start, this.end)` (:2287-2290), the whole pattern between the braces **including** a leading `[zone]`. Lite passes the pattern after the zone has been stripped (`Scalars.dateWithPattern`), so the operand differs in the zone case. (:2126 has the same text for a sub-second fill.) |
| DateFormats.java:138-139 (`endOfPattern`) | "Could not find end of date format starting at index " + open + " of: " + format | `PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/tools/FormatTools.java:192`: `throw new IllegalArgumentException("Could not find end of date format starting at index " + start + " of: " + formatString);`. `start` is the index of `{` (FormatTools.java:153-156), as is lite's `open`. Note that this site is line 139, not "Missing closing quote". |

**Compile or run? It depends on whether the format string is a literal at the call.**

- **COMPILE**: legend-pure (pinned) has `PVAL/functionExpression/FormatValidator.java:46-70`, called for
  `format_String_1__Any_MANY__String_1_` from `PVAL/validator/FunctionExpressionValidator.java:91-94`. When parameter 0
  is an `InstanceValue` holding one String, it calls `FormatTools.validate(...)` (:64), and that reaches
  `DateFormat.validate` → `DateFormatPattern.parse` (FormatTools.java:84-94; DateFormat.java:226-229). It rethrows
  `new PureCompilationException(..., e.getMessage(), e)` (:68): same words, raised at compile. Its javadoc (:30-37)
  says "a format string that could never be used is a compilation error ... Only a literal can be checked here ...
  which is why the run-time check stays".
- **RUN**: a computed format string, i.e. a variable or let, or a function parameter bound by a caller. The path is
  `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/.../natives/essentials/string/toString/Format.java:121-128`
  (`findEndOfDateFormatString`, then `date.format(...)` → DateFormat.java:196 → `DateFormatPattern.parse`). Format.java:207-209
  rewraps the IllegalArgumentException as `PureExecutionException(..., e.getMessage(), ...)`: same words, at run.
- **Lite's condition**: `lowering/Scalars.java:2366-2367` lowers only when `spread.get(0) instanceof SqlExpr.StringLit`.
  That is a SQL literal, which may have been a let or parameter that G½ inlined. The split must be read on the call as
  written, before G½: `format`'s argument 0 a `TypedCString` means COMPILE; anything else means RUN. The same applies
  to TemporalFrame:1924's `%latest`.

## 3. Proposed codes: one per distinct upstream run-time sentence

Templates are upstream's, byte for byte, with `%n$s` slots. "Lite has" lists the operands each site holds today, in
its order. A missing operand has to be computed.

| Code | Template | Upstream | Sites | Operands (upstream order) / lite has |
|---|---|---|---|---|
| `NO_PROPERTY_MAPPING` | `No property mapping found for: "%1$s" in "%2$s" in the mapping "%3$s"` | U1 builder.pure:395 | Substitution:1309, 1530, 1639 (unmapped arm), 2466, 2571, 2808; CorrelatedSubselects:1114; GraphEmission:349 (relational), 1959 | 1 property name, 2 set id (explicit, else class path `::`→`_`, embedded `parentId_prop`), 3 the set's **declaring** mapping path (`setImpl.parent`, which can be an include and is not the query's mapping). Lite has (property, class FQN, query mapping FQN), plus the hop path at 1309/1530 and the embedded head at 2466/1959. It must add the ClassSource's set id and the declaring mapping. **Precondition:** the property is required (lower bound ≥ 1), not auto-mapped, and the set is not part of a merge (U1x). Otherwise upstream yields empty and the site is GAP. |
| `NO_PROPERTY_MAPPING_IN_SET` | `No property mapping found for: %1$s in mapping %2$s[%3$s]` | U2 builder.pure:411 | UnionHeads:249 | 1 property, 2 the member set's class **simple** name, 3 member set id. Lite has (leaf, class FQN, via). Same precondition (builder.pure:408). |
| `CLASS_NOT_IN_MAPPING` | `The class '%1$s' can't be found in the mapping '%2$s'` | U3 routing.pure:483 | ClassSources:716(c), when a DataType property is read off the class's instances outside a lambda | 1 class **simple** name, 2 mapping **simple** name (`$mapping.name`). Lite has (class FQN, set id?, mapping FQN). |
| `MAPPING_NOT_FOUND_FOR_CLASS` | `Error mapping not found for class %1$s cache:'%2$s'` | U4 router_routing.pure:584 | ClassSources:716(c), when the class is a lambda parameter's type | 1 class **simple** name, 2 the classes of the sets routed so far for the call, simple names joined by `, ` (usually empty). Lite has (class FQN, mapping FQN). |
| `GRAPH_FETCH_NO_PROPERTY_MAPPING` | `No mapping found for property '%1$s'` | U5 graphExtension.pure:568 | GraphEmission:349 (M2M/Pure set) | 1 property name. Same optional/merge exemption. |
| `GRAPH_FETCH_CLASS_NOT_MAPPED` | `Mapping %1$s does not map class from tree: %2$s` | U6 graphExtension.pure:456 | ClassSources:716(c) from an M2M graphFetch root (reachability UNSURE) | 1 mapping path, 2 class path |
| `LATEST_SNAPSHOT_DATE` | `%latest not a valid businessDate for TemporalSnapshotMilestoning` | U7 MILES:421 | TemporalFrame:1472 | none (one fixed sentence for both dimensions) |
| `LATEST_NEEDS_INFINITY_DATE` | `%latest usage for temporal parameter: %1$s Table: %2$s.%3$s requires it to specify a milestoning 'INFINITY_DATE'` | U8 MILES:661 (:645) | TemporalFrame:1542 | 1 `processingDate` or `businessDate` (the dimension), 2 schema name, 3 table name. Lite has (class FQN, table). It must add the dimension and the schema. (The union variant MILES:649/665 is a separate fixed sentence, not on an MRE site.) |
| `LATEST_IN_RANGE` | `%latest not a valid parameter for allVersionsInRange` | U9 MILES:451 (snapshot :459) | TemporalFrame:1924, **only** when `%latest` is not a literal at the call. A literal is COMPILE (C11), with a different sentence. | none |
| (candidate) `RELATION_ACCESSOR_NEEDS_FROM` | `RelationStoreAccessor should be used with a from(Runtime) expression.` | U11 router_routing.pure:151 | Compiler:798, for a relation store accessor with no `from()` | none. Reachability UNSURE (see the row) |
| (candidate) `NO_ARG_MILESTONED_PROPERTY` | `No-Arg milestoned property: '%1$s' must either be supplied with dates or be called in a Milestoned Context` | U10 MILES:964 | none today. TemporalFrame:2330 is COMPILE (C12), with a different sentence. Only needed if lite finds a missing context that legend-pure's compile propagation would have seen. | 1 the property's printed signature |

Outside this class (§2):

| Code | Template | Upstream | Note |
|---|---|---|---|
| `TYPE_NOT_SUPPORTED_YET` (exists) | `The type %1$s is not supported yet!` | RelationNativeImplementation.java:503; AggregationShared.java:109, :536 | RUN only for a class-typed **relation column**. The raise has to move to the column's lowering; elsewhere it is GAP. |
| `DATE_FORMAT_UNCLOSED_QUOTE` | `Missing closing quote in format string: %1$s` | DateFormatPattern.java:2273 (:2126) | 1 the whole `%t{...}` pattern including `[zone]`. Each site decides: a literal format string is COMPILE (FormatValidator.java:68), a computed one is RUN. |
| `DATE_FORMAT_UNTERMINATED` | `Could not find end of date format starting at index %1$s of: %2$s` | FormatTools.java:192 | 1 index of `{`, 2 the whole format string. Same literal/computed split. |

Two points for the codes' design:

1. `CompileKind.Raised` is fixed per kind, but `LATEST_IN_RANGE`, both DATE_FORMAT kinds and `TYPE_NOT_SUPPORTED_YET`
   are COMPILE or RUN (or GAP) depending on the site's context. Either split each into two kinds, or have the site
   decide and pick the kind.
2. Every `NO_PROPERTY_MAPPING*` site must first apply upstream's exemption: an optional property owned by a class, an
   auto-mapped property, or a merge member. There upstream answers empty, so raising is a GAP and RUN would be wrong.
