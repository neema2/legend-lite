# Rosetta Implementation Checklist

Filtered view of all ❌/⚠️ features that need implementation, ordered by priority tier.
Each links to its Rosetta Stone doc with the full proposed Relation API design.

Consolidates details from: [MAPPING_FEATURE_CHECKLIST](MAPPING_FEATURE_CHECKLIST.md),
[MAPPING_UNIFICATION_PLAN](MAPPING_UNIFICATION_PLAN.md), [LEGEND_MAPPING_PATTERNS](LEGEND_MAPPING_PATTERNS.md),
and [MAPPING_STORE_GAP_ANALYSIS](MAPPING_STORE_GAP_ANALYSIS.md).

Status key: ❌ = not implemented, ⚠️ = partial, 🔧 = needs grammar addition

---

## 1. Architecture Progress

The pipeline refactoring from the v5 Mapping Unification Plan. These are **cross-cutting
improvements** that enable feature work — not features themselves.

```
PureParser  →  TypeChecker  →  MappingResolver  →  PlanGenerator  →  Dialect
 (Pass 1)       (Pass 2)        (Pass 3)            (Pass 4)         (Pass 5)
 Pure→AST       types only      mapping→store        SqlExpr gen      SQL render
```

| Phase | Goal | Status | Key Outcomes |
|-------|------|--------|--------------|
| **Phase 0** — Clean TypeChecker | Function dispatch → Checker classes; delete `table()`/`class()` | ✅ Done | 33 checkers extracted, clean `switch` dispatch, `PureFunctionRegistry` → `BuiltinFunctionRegistry` + `ModelContext` for user fns |
| **Phase 1** — MappingResolver | Remove mapping from TypeChecker; TypeInfo has no `ClassMapping` | ✅ Done | TypeInfo record has zero mapping fields; MappingResolver owns all store resolution; `StoreResolution` sidecar for PlanGenerator |
| **Phase 2** — Unify PlanGenerator | One codepath for class-source and Relation-source queries | ✅ Done | `generateGetAll` deleted; both paths produce same output via StoreResolution |
| **Phase 3** — DynaFunction + Mapping Features | Full `MappingNormalizer` → `extend()` pipeline | ✅ Done | All computed properties (dynaFunc, enum, expression access) synthesized as `extend()` ColSpecs with 1-param lambdas |
| **Phase 4** — Mapping as Relation DSL | `###Mapping` desugars entirely to Relation ops | 🔲 Future | Endgame: mapping becomes thin syntax sugar over `#>{db.T}#->extend()->join()->filter()` chains |

### Key architectural invariant: `extend()` takes `Relation`, not just table ref

The `extend()` function in our pipeline works on **any Relation-typed source** — table accessors,
subqueries, M2M intermediates, or composed Relation chains. This is the foundation that makes
mapping-as-Relation-DSL possible:

```
Mapping syntax:          fullName: concat([db]T.first, ' ', [db]T.last)
MappingNormalizer:       →  extend(~fullName: {row| $row.first + ' ' + $row.last})
                            ↑ operates on whatever Relation the sourceRelation produces
Target Relation DSL:     #>{db.PersonTable}# ->extend(~fullName: r | $r.first + ' ' + $r.last)
```

### Infrastructure already completed

These foundational pieces are **done** and enable all feature work below:

- **Definition records**: `RelationalOperation` sealed interface (ColumnRef, TargetColumnRef, Literal, FunctionCall, Comparison, BooleanOp, IsNull, IsNotNull, Group, ArrayLiteral, JoinNavigation, ScopeBlock, EnumTransform, BindingTransform)
- **PropertyMappingValue** sealed interface: ColumnMapping, JoinMapping, ExpressionMapping, EmbeddedMapping, InlineMapping, OtherwiseMapping
- **Model objects**: Join with full expression tree, View, Filter, Schema — all registered
- **Builder wiring**: Zero regex, zero raw-string fallbacks — `extractDbJoin` regex replaced by proper AST walking
- **MappingNormalizer**: Synthesizes ALL property types as `sourceRelation` extend chain — single source of truth
- **MappingResolver**: Single-pass walker producing `StoreResolution` (PropertyResolution: Column | DynaFunction | M2MExpression | EmbeddedColumn)
- **Join conditions**: Pre-converted from `RelationalOperation` → `ValueSpecification` in MappingNormalizer; MappingResolver stamps TypeInfo; PlanGenerator renders via `generateScalar()` — no `RelationalOperation` in PlanGenerator
- **SymbolTable**: Canonical `extractSimpleName()` / `extractPackagePath()` — zero string hacking for qualified names
- **Type system**: 140+ functions registered in `BuiltinFunctionRegistry`, Pure type names throughout (Dialect maps to SQL)

---

## 2. Remaining Features

### Tier 2: Core Missing Features

| # | Feature | Status | Relation Translation | What to Build | Rosetta |
|---|---------|--------|---------------------|---------------|---------|
| B8 | Set ID lookup | ⚠️ | Query routing metadata — `findBySetId(id)` in MappingRegistry | MappingRegistry: `findBySetId(String)`, `findRootMapping(String)`, support multiple mappings/class | [identity-extends](rosetta/identity-extends.md) |
| B9 | Extends | ❌ | `parentRel->extend(~childProp: ...)` — child inherits parent's Relation expression + adds columns | Builder processes `extends [parentId]`; MappingNormalizer merges parent property mappings; `redefine()` for overrides | [identity-extends](rosetta/identity-extends.md) |
| C2 | Store substitution | ❌ | Replace `#>{Db1.T}#` → `#>{Db2.T}#` in included mapping's Relation expression | Transitive include resolution at model build time; parameterized store accessor `#>{$store.T}#` | [includes-substitution](rosetta/includes-substitution.md) |
| B10 | Scope blocks | ❌🔧 | Pure syntax sugar — `scope([DB]T)(p1:c1, p2:c2)` desugars to `->project([~c1->as(~p1), ~c2->as(~p2)])` | Grammar rule for `scope` keyword; parse-time desugaring in Builder (no runtime impact) | [class-directives](rosetta/class-directives.md) |
| C5 | Local properties | ❌ | `->extend(~localProp: r \| expr)` — mapping-only column not in class model | Builder recognizes `+prop` prefix; MappingNormalizer emits `extend()` ColSpec; property not in class model. **Depends on E3** — without XStore, local properties are dead code (filtered out of execution, invisible to user queries) | [xstore-crossdb](rosetta/xstore-crossdb.md) |

### Tier 3: Database Features

| # | Feature | Status | Relation Translation | What to Build | Rosetta |
|---|---------|--------|---------------------|---------------|---------|
| B6 | ~primaryKey | ❌ | Column metadata / `->select(~id)` — PK annotation on Relation | Parser: `#>{db.T \| [pk]}#` syntax; metadata propagation through pipeline | [class-directives](rosetta/class-directives.md) |
| B3 | ~filter via join | ❌ | `->join(otherRel, ...) ->filter(pred)` — join to related table then filter | MappingNormalizer follows join path from filter; PlanGenerator emits EXISTS subquery or JOIN+WHERE | [class-directives](rosetta/class-directives.md) |

### Tier 4: Advanced / Grammar Additions

| # | Feature | Status | Relation Translation | What to Build | Effort | Rosetta |
|---|---------|--------|---------------------|---------------|--------|---------|
| E6 | Union | ❌ | `rel1->concatenate(rel2)` — UNION ALL of multiple class mapping Relations | Parse `Operation { union(set1, set2) }`; each set ID → its Relation expression; concatenate results | Medium | [union-merge](rosetta/union-merge.md) |
| E7 | Merge | ❌ | `rel1->join(rel2, ...) + coalesce()` — merge with conflict resolution lambda | Parse `Operation { merge(s1, s2) }`; join on PK; merge lambda resolves conflicting rows | Medium | [union-merge](rosetta/union-merge.md) |
| A9 | Binding | ❌ | `->extend(~prop: r \| $r.json_col->toVariant()->get('key'))` — JSON/XML deserialization | `toVariant()` + `get()` for field extraction; Dialect renders `CAST AS JSON` + `->>` | Medium | [binding-transformer](rosetta/binding-transformer.md) |
| A10 | Source/Target IDs | ❌ | Named functions resolve this — `prop[srcId, tgtId]: @J` → explicit set ID pairs on association | Builder extracts `sourceAndTargetMappingId`; MappingResolver uses for association disambiguation | Low | [xstore-crossdb](rosetta/xstore-crossdb.md) |
| E3 | XStore | ❌🔧 | Cross-source: execute each store's Relation, join in-memory via temp tables | Grammar: `classMappingType` needs `XSTORE` variant; `$this.prop == $that.prop` lambda compilation; federation executor | Hard🔧 | [xstore-crossdb](rosetta/xstore-crossdb.md) |
| E4 | AggregationAware | ❌🔧 | Query analyzer macro selecting best pre-aggregated Relation based on query shape | Grammar: `AGGREGATION_AWARE` variant + view/mainMapping sub-grammar; `~canAggregate`, `~groupByFunctions`, `~aggregateValues` directives; optimizer intercepts `groupBy()` calls | Hard🔧 | [aggregation-aware](rosetta/aggregation-aware.md) |
| E5 | Relation mapping | ❌🔧 | `~func` returns `Relation<Any>[1]` — property mappings are bare column renames | Grammar: `RELATION` variant + `~func` directive; property → `->rename(~COL, ~prop)` chain; no DynaFunc/joins (transforms in `~func` body) | Medium🔧 | [relation-class-mapping](rosetta/relation-class-mapping.md) |
| G | Milestoning | ❌ | `->filter(r \| $r.bus_from <= $asOf && $r.bus_thru > $asOf)` — auto-injected temporal predicates | Table metadata: `~businessFrom`/`~businessThru`/`~processingIn`/`~processingOut`; `->asOf()` function; traverse propagation through joins | Hard | [milestoning](rosetta/milestoning.md) |
| A11 | Cross-DB refs | ❌ | Multi-source: `#>{otherDb.T}#` + cross-join via federation | Same infrastructure as XStore — federation executor needed | Medium | [xstore-crossdb](rosetta/xstore-crossdb.md) |
| D10 | TabularFunction | ❌ | `#>{db.func(args)}#` — parameterized store accessor | Parameterized function in RelationStoreAccessor; PlanGenerator emits function call in FROM clause | Low | [database-objects](rosetta/database-objects.md) |
| D11 | MultiGrainFilter | ❌ | `->filter(r \| discriminator_pred)` — just a filter with discriminator column | Trivial — same as regular filter with named discriminator | Low | [database-objects](rosetta/database-objects.md) |

---

## 3. Mapping → Relation Translation Reference

Every mapping feature maps to Relation operators. This is the core thesis from the v5 plan:

### Class-level features

| Mapping Syntax | Relation Equivalent | Status |
|----------------|-------------------|--------|
| `~mainTable [db]T` | `#>{db.T}#` | ✅ |
| `~filter [db]MyFilter` | `->filter(pred)` | ✅ |
| `~filter [db]@J1 [db]MyFilter` | `->join(...)->filter(pred)` | ❌ |
| `~distinct` | `->distinct()` | ✅ |
| `~groupBy([db]T.dept)` | `->groupBy(~dept, ~aggs)` | ✅ |
| `~primaryKey([db]T.id)` | Column metadata annotation | ❌ |
| `*ClassName[id]` | Routing metadata (not a data op) | ⚠️ |
| `extends [parentId]` | `parentRel->extend(~childCols)` | ❌ |

### Property-level features

| Mapping Syntax | Relation Equivalent | Status |
|----------------|-------------------|--------|
| `prop: [db]T.col` | `->rename(~col, ~prop)` or `->project(~prop: r \| $r.col)` | ✅ |
| `prop: [db]@J \| T.col` | `->join(#>{db.T}#, ...) ->project(~prop: r \| $r.col)` | ✅ |
| `prop: @J1 > (INNER) @J2 \| T.col` | `->join(...)->join(...)` with explicit join type | ✅ |
| `prop: concat([db]T.a, ' ', [db]T.b)` | `->extend(~prop: r \| $r.a + ' ' + $r.b)` | ✅ |
| `EnumMapping M: [db]T.code` | `->extend(~prop: r \| if(eq($r.code,'P'),'PENDING',...))` | ✅ |
| `Binding B: [db]T.json` | `->extend(~prop: r \| $r.json->toVariant()->get('key'))` | ❌ |
| `prop() { sub: T.col }` | Inline column access (same alias, no JOIN) | ✅ |
| `prop() {...} Otherwise([id]:@J)` | Embedded + fallback join | ✅ |
| `prop() Inline[setId]` | Import referenced mapping's Relation expression | ✅ |
| `+prop: Type[m]: T.col` | `->extend(~prop: r \| expr)` (mapping-only) | ❌ |
| `prop[src, tgt]: @J` | Association with explicit set ID pairs | ❌ |

### Structural features

| Mapping Syntax | Relation Equivalent | Status |
|----------------|-------------------|--------|
| `operation: union(s1, s2)` | `rel1->concatenate(rel2)` | ❌ |
| `operation: merge(s1, s2)` | `rel1->join(rel2, ...) + coalesce` | ❌ |
| `include my::OtherMapping` | Compose: import other mapping's Relation expressions | ✅ |
| `include M [Db1 -> Db2]` | Replace `#>{Db1.T}#` → `#>{Db2.T}#` in included expressions | ❌ |
| `Assoc: AssociationMapping(@J)` | Explicit `->join(otherRel, type, pred)` | ✅ |
| `Assoc: XStore { $this.id == $that.id }` | Cross-source federation join | ❌ |
| `Child extends [parent]` | `parentRel->extend(~childProp: ...)` | ❌ |

---

## 4. Key Technical Design Decisions

These decisions are documented across the v5 planning docs and should be preserved:

### 4.1 `sourceRelation` is the single source of truth

`MappingNormalizer` synthesizes ALL computed properties (dynaFunction, enum, expression access)
as `sourceRelation` extend ColSpecs with 1-param lambdas. `MappingResolver` walks the extend
chain in a single pass to derive `PropertyResolution`. `StoreResolution` is **derived from**
sourceRelation, not built independently.

### 4.2 TypeChecker never sees mappings

TypeChecker resolves types using **only class/association model data**. `ctx.getMapping()` was
deleted from TypeInfo. Types come from class definitions; physical column/table resolution comes
from MappingResolver as a separate pass. This is the same architecture as legend-engine's
`Compile → Route → Plan → Execute`.

### 4.3 PlanGenerator does no type inference

PlanGenerator reads the **annotated AST** (structure from AST, types from TypeInfo, store from
StoreResolution). It emits `SqlExpr` nodes — never raw SQL strings. All SQL-specific decisions
(type names, function names, decompositions) live in the Dialect.

### 4.4 Join conditions are ValueSpecifications

Join conditions are pre-converted from `RelationalOperation` → `ValueSpecification` in
MappingNormalizer. MappingResolver stamps TypeInfo on every join condition node. PlanGenerator
renders them via `generateScalar()` — PlanGenerator **never touches `RelationalOperation`**.

### 4.5 No regex, no string hacking, no fallbacks

Every grammar construct has a proper AST node. The old `extractDbJoin` regex is dead. Raw
expression string fallbacks in `visitRelationalPropertyValue` are replaced by structured
`PropertyMappingValue` subtypes. If a type is unknown, the Compiler **fails** — no defaults.

### 4.6 DynaFunction names: Pure, not SQL

All function names in the AST use Pure names (`plus`, `toUpper`, `substring`), not DuckDB names
(`concat`, `UPPER`, `SUBSTR`). The Dialect's `renderFunction()` maps Pure → SQL at render time.
DynaFunction names in mapping syntax (`concat`, `toLower`) are relational operations that get
converted to Pure equivalents in `RelationalMappingConverter`.

---

## 5. Summary

| Category | ✅ Done | ❌/⚠️ Remaining |
|----------|---------|-----------------|
| A. Property Mappings | 8 | 3 (A9, A10, A11) |
| B. Class Directives | 5 | 4 (B3, B6, B9, B10) + 1 partial (B8) |
| C. Mapping-Level | 3 | 2 (C2, C5) |
| D. Database Objects | 9 | 2 (D10, D11) |
| E. Class Mapping Types | 2 | 5 (E3, E4, E5, E6, E7) |
| G. Milestoning | 0 | 1 |
| **Total** | **27** | **17** (+1 partial) |

### Key dependency chains

- **B9 (extends)** depends on B8 (set ID lookup) — child references parent by set ID
- **C2 (store substitution)** depends on transitive include resolution at model build time
- **C5 (local properties)** depends on E3 (XStore) — local properties exist only as XStore join keys; dead code without XStore
- **E3/A11 (xStore/cross-DB)** both need executor federation infrastructure
- **E4 (AggregationAware)** is a query optimizer — highest complexity; needs query shape analysis
- **E6/E7 (union/merge)** depend on B8 (set ID) — operation references set IDs
- **G (milestoning)** is cross-cutting: table metadata + auto-filter injection + traverse propagation

### Architecture phases: 3 of 4 done

| Phase | Status | Impact |
|-------|--------|--------|
| Phase 0-1: TypeChecker cleanup + MappingResolver | ✅ | Checkers are mapping-free; StoreResolution sidecar |
| Phase 2-3: Unified PlanGen + DynaFunction pipeline | ✅ | One codepath; `extend()` on Relation |
| Phase 4: Mapping as Relation DSL | 🔲 | Endgame — depends on remaining features above |

### Object construction gap

The fundamental gap between Mappings and Relations is **not** about transforms or joins — those
all translate cleanly. The gap is **object construction**: mappings produce typed domain objects
with nested properties (`Person` with `Firm` with `Address`), while Relations produce flat rows.
This requires an unflatten/hydration post-processing layer and is a **presentation-layer concern**,
not a gap in the relational algebra.
