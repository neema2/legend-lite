# Mapping Feature Checklist

Single source of truth for all mapping features. Reconciled from LEGEND_MAPPING_PATTERNS, MAPPING_STORE_GAP_ANALYSIS, MAPPING_UNIFICATION_PLAN, and PHASE_5_6_IMPLEMENTATION_PLAN.

Status key: ✅ = working E2E, ⚠️ = partial, ❌ = not implemented, 🔧 = needs grammar

---

## A. Property Mapping Types

| # | Feature | Syntax | Status | Test(s) | Rosetta |
|---|---------|--------|--------|---------|---------|
| A1 | Simple column | `prop: [db]T.col` | ✅ | many | [property-mappings](rosetta/property-mappings.md) |
| A2 | Join chain + column | `prop: @J1 > @J2 \| T.col` | ✅ | testJoinChainPropertyMapping | [join-navigation](rosetta/join-navigation.md) |
| A3 | DynaFunction | `prop: concat(T.A, ' ', T.B)` | ✅ | testDynaFunctionConcat + 5 more | [property-mappings](rosetta/property-mappings.md) |
| A4 | DynaFunction + join | `prop: upper(@J \| T.col)` | ✅ | DynaFunctionIntegrationTest | [property-mappings](rosetta/property-mappings.md) |
| A5 | Enum transformer | `EnumerationMapping M: T.code` | ✅ | EnumIntegrationTest | [enum-mappings](rosetta/enum-mappings.md) |
| A6 | Embedded | `prop() { sub: T.col }` | ✅ | testEmbeddedPropertyAccess | [embedded-otherwise-inline](rosetta/embedded-otherwise-inline.md) |
| A7 | Otherwise embedded | `prop() { ... } Otherwise(...)` | ✅ | OtherwiseMappings (8 tests) | [embedded-otherwise-inline](rosetta/embedded-otherwise-inline.md) |
| A8 | Inline embedded | `prop() Inline[setId]` | ✅ | testInlineMapping | [embedded-otherwise-inline](rosetta/embedded-otherwise-inline.md) |
| A9 | Binding transformer | `Binding B: T.json` | ❌ | — | [binding-transformer](rosetta/binding-transformer.md) |
| A10 | Source/Target IDs | `prop[src, tgt]: @J` | ❌ | — | [xstore-crossdb](rosetta/xstore-crossdb.md) |
| A11 | Cross-DB reference | `[otherDb]T.col` | ❌ | — | [xstore-crossdb](rosetta/xstore-crossdb.md) |

## B. Class Mapping Directives

| # | Feature | Syntax | Status | Test(s) | Rosetta |
|---|---------|--------|--------|---------|---------|
| B1 | ~mainTable | `~mainTable [db]T` | ✅ | many | [class-directives](rosetta/class-directives.md) |
| B2 | ~filter | `~filter [db]FilterName` | ✅ | testMappingFilter | [class-directives](rosetta/class-directives.md) |
| B3 | ~filter via join | `~filter [db]@J [db]Filter` | ❌ | — | [class-directives](rosetta/class-directives.md) |
| B4 | ~distinct | `~distinct` | ✅ | testMappingDistinct | [class-directives](rosetta/class-directives.md) |
| B5 | ~groupBy | `~groupBy(T.col)` | ✅ | testMappingGroupBy | [class-directives](rosetta/class-directives.md) |
| B6 | ~primaryKey | `~primaryKey(T.col)` | ❌ | — | [class-directives](rosetta/class-directives.md) |
| B7 | Root marker | `*ClassName[id]` | ✅ | testSetIdsAndRoot | [identity-extends](rosetta/identity-extends.md) |
| B8 | Mapping ID (set ID) | `[myId]` | ⚠️ extracted, not queryable by ID | — | [identity-extends](rosetta/identity-extends.md) |
| B9 | Extends | `extends [parentId]` | ❌ | testMappingExtends (disabled) | [identity-extends](rosetta/identity-extends.md) |
| B10 | Scope blocks | `scope([db]T) (...)` | ❌🔧 | testScopeBlock (disabled) | [class-directives](rosetta/class-directives.md) |

## C. Mapping-Level Features

| # | Feature | Syntax | Status | Test(s) | Rosetta |
|---|---------|--------|--------|---------|---------|
| C1 | Mapping includes | `include other::Mapping` | ✅ | testMappingInclude | [includes-substitution](rosetta/includes-substitution.md) |
| C2 | Store substitution | `include M [DB1 -> DB2]` | ❌ | testStoreSubstitution (disabled) | [includes-substitution](rosetta/includes-substitution.md) |
| C3 | Association mapping | `Assoc: AssociationMapping(...)` | ✅ | testExplicitAssociationMapping | [association-mappings](rosetta/association-mappings.md) |
| C4 | Enumeration mapping | `Enum: EnumerationMapping {...}` | ✅ | EnumIntegrationTest | [enum-mappings](rosetta/enum-mappings.md) |
| C5 | Local properties | `+prop: Type[m]: T.col` | ❌ | testLocalProperty (disabled) | [local-properties](rosetta/local-properties.md) |

## D. Database Objects

| # | Feature | Syntax | Status | Test(s) | Rosetta |
|---|---------|--------|--------|---------|---------|
| D1 | Table | `Table T (col TYPE)` | ✅ | many | [database-objects](rosetta/database-objects.md) |
| D2 | Join (simple) | `Join J(T1.C = T2.C)` | ✅ | many | [join-navigation](rosetta/join-navigation.md) |
| D3 | Join (complex) | `Join J(T1.A = T2.A and ...)` | ✅ | testMultiColumnJoin | [join-navigation](rosetta/join-navigation.md) |
| D4 | Join (function) | `Join J(concat(...) = T.C)` | ✅ | testFunctionInJoin | [join-navigation](rosetta/join-navigation.md) |
| D5 | Self-join | `Join J(T.pid = {target}.id)` | ✅ | testSelfJoin | [join-navigation](rosetta/join-navigation.md) |
| D6 | Filter | `Filter F(T.col = 1)` | ✅ | testMappingFilter uses Filter | [database-objects](rosetta/database-objects.md) |
| D7 | View | `View V (col: expr)` | ✅ | testViewAsDataSource, testViewAllFeatures, testViewJoinPruning, testViewEndToEnd, testViewWithFilterDistinctJoinDyna, testViewMultiJoinChains, testViewDynaWithTwoJoinChains, testViewWithGroupBy | [database-objects](rosetta/database-objects.md) |
| D8 | Schema | `Schema S (Table ...)` | ✅ | testSchemaTable | [database-objects](rosetta/database-objects.md) |
| D9 | Database include | `include otherDb` | ✅ | testDatabaseInclude | [database-objects](rosetta/database-objects.md) |
| D10 | TabularFunction | `TabularFunction F(...)` | ❌ | — | [database-objects](rosetta/database-objects.md) |
| D11 | MultiGrainFilter | `MultiGrainFilter F(...)` | ❌ | — | [database-objects](rosetta/database-objects.md) |

## E. Class Mapping Types

| # | Feature | Syntax | Status | Test(s) | Rosetta |
|---|---------|--------|--------|---------|---------|
| E1 | Relational | `Class: Relational { ... }` | ✅ | many | [property-mappings](rosetta/property-mappings.md) |
| E2 | Pure (M2M) | `Class: Pure { ~src S, ... }` | ✅ | M2MIntegrationTest | [m2m-mapping](rosetta/m2m-mapping.md) |
| E3 | XStore | `Assoc: XStore { ... }` | ❌🔧 | testXStore (disabled) | [xstore-crossdb](rosetta/xstore-crossdb.md) |
| E4 | AggregationAware | `Class: AggregationAware { ... }` | ❌🔧 | testAggregationAware (disabled) | [aggregation-aware](rosetta/aggregation-aware.md) |
| E5 | Relation | `Class: Relation { ~func ... }` | ❌🔧 | testRelationClassMapping (disabled) | [relation-class-mapping](rosetta/relation-class-mapping.md) |
| E6 | Union | `operation: union(s1, s2)` | ❌ | — | [union-merge](rosetta/union-merge.md) |
| E7 | Merge/Intersection | `operation: intersection(...)` | ❌ | — | [union-merge](rosetta/union-merge.md) |

## F. Known Bugs

| # | Description | Status | Test(s) |
|---|-------------|--------|---------|
| F1 | Computed from 2 joins: `plus(@J1\|col, @J2\|col)` only resolves 1st | ✅ | testComputedFromTwoJoins |

---

## Implementation Order

Ordered by: dependencies, impact, difficulty. Each item is independently testable.

### Tier 1: Composition Quick Wins ✅

All composition tests pass (sections 1–21 in RelationalMappingCompositionTest):
- Self-join, complex join, association + multi-hop chain — all verified
- Class-source groupBy with association key fixed (fb12a84)

### Tier 2: Core Missing Features

| Step | Feature | Ref | Depends on | Effort |
|------|---------|-----|------------|--------|
| 2 | ~~~Computed from 2 joins (F1)~~~ | — | — | Done |
| 3 | Set ID lookup by ID (B8) | Group F | — | Low |
| 4 | Mapping extends (B9) | Group F | B8 | Medium |
| 5 | Store substitution (C2) | Group G | C1 | Medium |
| 6 | Scope blocks (B10) | Group B | Grammar | Medium |
| 7 | Local properties (C5) | Group J | — | Medium |

### Tier 3: Database Features

| Step | Feature | Ref | Effort |
|------|---------|-----|--------|
| 8 | ~~~Views as data source (D7)~~~ | — | Done |
| 9 | ~~~groupBy (B5)~~~ | — | Done |
| 10 | ~primaryKey (B6) | — | Low |
| 11 | ~filter via join (B3) | — | Medium |
| 12 | ~~~Schema support (D8)~~~ | — | Done |

### Tier 4: Advanced / Grammar Additions

| Step | Feature | Ref | Effort |
|------|---------|-----|--------|
| 13 | Union/Merge (E6, E7) | Group N | Medium |
| 14 | Binding transformer (A9) | Group L | Medium |
| 15 | Source/Target IDs (A10) | — | Low |
| 16 | XStore (E3) | Group K | Hard🔧 |
| 17 | AggregationAware (E4) | Group K | Hard🔧 |
| 18 | Relation mapping (E5) | Group K | Medium🔧 |
| 19 | Milestoning | Group M | Hard |
| 20 | Cross-DB refs (A11) | — | Medium |
| 21 | TabularFunction (D10) | — | Low |
| 22 | MultiGrainFilter (D11) | — | Low |

---

## Composition Tests (verify after each tier)

These are stubs in GapComposition — fill in and enable as prerequisite features land:

| Test | Requires |
|------|----------|
| Set IDs + filter disambiguation | B8 |
| Extends + filter inheritance | B9 |
| Mapping include + join navigation | C1 (done) |
| Store substitution + query | C2 |
| View + join + filter | D7 (done) |
| DB filter + mapping filter stacking | B2 (done), D6 (done) |
| Local property + join + filter | C5 |
| Association mapping + multi-table chain | C3 (done) |
| Scope block + embedded + filter | B10 |
| AggregationAware + join | E4 |
| Self-join + filter + sort | D5 (done) |
| Complex join + aggregation | D3 (done) |
