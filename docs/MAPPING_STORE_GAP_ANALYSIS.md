# Mapping & Store — Comprehensive Gap Analysis and Implementation Plan

> **Goal**: 100% backward compatibility with legend-engine/legend-pure mapping and store features.
> **Final endgame**: `RelationalMapping` becomes a thin DSL on top of `Relation` constructs.
> **Approach**: Proper compiler-grade implementation. Zero regex. Zero string hacking.

---

## 1. Current State Summary

### What Works (Grammar → Definition → Model → Resolver → Plan → SQL)

| Feature | Grammar | Definition | Model | Resolver | PlanGen |
|---------|---------|------------|-------|----------|---------|
| Simple relational mapping (`Class : Relational { prop : [DB]T.C }`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Pure M2M mapping (`Class : Pure { ~src S, prop : $src.x }`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Single equi-join (`Join J(T1.C = T2.C)`) | ✅ | ✅ (regex!) | ✅ | ✅ | ✅ |
| Single-hop join property (`prop : [DB]@JoinName`) | ✅ | ✅ | ✅ | ✅ | ✅ |
| Enumeration mapping | ✅ | ✅ | ✅ | ✅ | ✅ |
| `~mainTable` | ✅ | ✅ | ✅ | ✅ | ✅ |
| Mapping test suites | ✅ | ✅ | — | — | — |

### What's Parsed but Dropped on the Floor

| Feature | Grammar | Definition | Model | Notes |
|---------|---------|------------|-------|-------|
| **Set IDs** (`Class[myId] : Relational`) | ✅ parsed | ❌ ignored | ❌ | Builder ignores `[id]` bracket |
| **Root marker** (`*Class[id]`) | ✅ parsed | ❌ ignored | ❌ | Builder ignores `*` |
| **Extends** (`Class[c1] extends [parent]`) | ✅ parsed | ❌ ignored | ❌ | Builder ignores `extends` clause |
| **Filter** (`~filter [DB]@J1\|[DB]myFilter`) | ✅ parsed | ❌ ignored | ❌ | Builder skips `mappingFilter` |
| **Distinct** (`~distinct`) | ✅ parsed | ❌ ignored | ❌ | Builder skips `mappingDistinct` |
| **GroupBy** (`~groupBy(...)`) | ✅ parsed | ❌ ignored | ❌ | Builder skips `mappingGroupBy` |
| **PrimaryKey** (`~primaryKey(...)`) | ✅ parsed | ❌ ignored | ❌ | Builder skips `mappingPrimaryKey` |
| **Scope blocks** (`scope([DB]T) (...)`) | ❌ GRAMMAR GAP | ❌ | ❌ | `SCOPE` keyword in lexer but no grammar rule in `relationalClassMappingBody`. Legend-engine has `propertyMappingWithScope` rule. |
| **Multi-hop join chains** (`@J1 > (INNER) @J2`) | ✅ parsed | ❌ single only | ❌ | Builder only extracts first hop |
| **Join types** (`(INNER)`, `(LEFT OUTER)`) | ✅ parsed | ❌ ignored | ❌ | Not captured anywhere |
| **Complex join conditions** (`and`/`or`/functions) | ✅ parsed | ❌ regex! | ❌ | `extractDbJoin` uses regex, only finds first `T.C = T.C` |
| **Self-joins** (`{target}`) | ✅ parsed | ❌ ignored | ❌ | Not modeled |
| **DynaFunction expressions** (`concat(T.A, ' ', T.B)`) | ✅ parsed | ❌ fallback | ❌ | Falls back to raw expression string |
| **Embedded mappings** (`prop (legalName : T.C)`) | ✅ parsed | ❌ fallback | ❌ | Stored as raw expression string |
| **Inline mappings** (`prop () Inline[id]`) | ✅ parsed | ❌ fallback | ❌ | Stored as raw expression string |
| **Otherwise** (`Otherwise ([id]:[DB]@J)`) | ✅ parsed | ❌ fallback | ❌ | Stored as raw expression string |
| **Association mappings** (`Assoc : AssociationMapping (...)`) | ✅ parsed | ❌ not visited | ❌ | `visitMapping` skips them entirely |
| **Mapping includes** (`include my::BaseMapping`) | ✅ parsed | ❌ not visited | ❌ | `visitMapping` skips them entirely |
| **Store substitution** (`include M [DB1 -> DB2]`) | ✅ parsed | ❌ not visited | ❌ | `visitMapping` skips them entirely |
| **Local properties** (`+prop : Type[m] : T.C`) | ✅ parsed | ⚠️ partial | ❌ | Property name extracted, `+` semantics lost |
| **Binding transformer** (`Binding b : T.C`) | ✅ parsed | ❌ fallback | ❌ | Stored as raw expression string |
| **Enum transformer on joins** | ✅ parsed | ❌ | ❌ | |
| **Source/target mapping IDs** (`prop[src, tgt]`) | ✅ parsed | ❌ ignored | ❌ | Builder ignores `sourceAndTargetMappingId` |
| **Views** (`View V (col : T.C PRIMARY KEY)`) | ✅ parsed | ❌ not visited | ❌ | `visitDatabase` skips views entirely |
| **Filters** (`Filter F(T.C = 1)`) | ✅ parsed | ❌ not visited | ❌ | `visitDatabase` skips filters entirely |
| **Schemas** (`Schema S (Table T (...))`) | ✅ parsed | ⚠️ partial | ❌ | Tables extracted, schema info lost |
| **Database includes** (`include my::OtherDB`) | ✅ parsed | ❌ not visited | ❌ | `visitDatabase` skips includes |
| **TabularFunction** | ✅ parsed | ❌ not visited | ❌ | |
| **MultiGrainFilter** | ✅ parsed | ❌ not visited | ❌ | |
| **Milestoning spec** on tables | ✅ parsed | ❌ ignored | ❌ | |
| **Variant access** (`col->get('key', @Type)`) | ✅ parsed | ❌ fallback | ❌ | Falls back to raw expression string |

### What's Not in the Grammar At All

| Feature | In legend-engine | Priority |
|---------|-----------------|----------|
| **XStore association mapping** (`Assoc : XStore { ... }`) | ✅ | Medium |
| **AggregationAware mapping** (`Class : AggregationAware { Views: [...], ~mainMapping: ... }`) | ✅ | Medium |
| **Relation class mapping** (`Class : Relation { ~func fn }`) | ✅ | Medium |
| **FlatData class mapping** | ✅ | Low (different store) |
| **ServiceStore class mapping** | ✅ | Low (different store) |
| **Union/Merge set implementation** | ✅ | Medium |

---

## 2. Model Object Gaps

### 2.1 Join — Single Biggest Structural Gap

**Current**: `Join(name, leftTable, leftColumn, rightTable, rightColumn)` — single equi-join only.

**Needed**: Full expression tree matching grammar's `dbOperation` rule:

```
Join J1(T1.A = T2.B and T1.C is not null)
Join J2(concat('prefix_', T1.name) = T2.prefixed_name)
Join SelfJoin(T1.parent_id = {target}.id)
```

**Solution**: A `RelationalOperation` sealed interface hierarchy:

```java
sealed interface RelationalOperation {
    record ColumnRef(String table, String column) implements RelationalOperation {}
    record TargetColumnRef(String column) implements RelationalOperation {}  // {target}.col
    record Literal(Object value) implements RelationalOperation {}          // 'text', 42, 3.14
    record FunctionCall(String name, List<RelationalOperation> args) implements RelationalOperation {}
    record Comparison(RelationalOperation left, String op, RelationalOperation right) implements RelationalOperation {}
    record BooleanOp(RelationalOperation left, String op, RelationalOperation right) implements RelationalOperation {}  // and/or
    record IsNull(RelationalOperation operand) implements RelationalOperation {}
    record IsNotNull(RelationalOperation operand) implements RelationalOperation {}
    record Group(RelationalOperation inner) implements RelationalOperation {}
    record ArrayLiteral(List<RelationalOperation> elements) implements RelationalOperation {}  // for in(col, [1,2,3])
}
```

**Join** becomes:

```java
record Join(String name, RelationalOperation condition) { ... }
```

### 2.2 View — Currently Missing

**Needed**: Views are named queries inside a Database definition, usable like tables in mappings.

```java
record View(
    String name,
    RelationalOperation filterCondition,      // optional ~filter
    List<RelationalOperation> groupByColumns,  // optional ~groupBy
    boolean distinct,                          // optional ~distinct
    List<ViewColumnMapping> columnMappings     // col : dbOperation
) {
    record ViewColumnMapping(String name, RelationalOperation expression, boolean primaryKey) {}
}
```

### 2.3 Filter — Currently Missing

```java
record Filter(String name, RelationalOperation condition) {}
```

### 2.4 Schema — Currently Missing

```java
record Schema(String name, List<Table> tables, List<View> views) {}
```

### 2.5 Database — Needs Expansion

**Current**: `DatabaseDefinition(name, tables, joins)` — no views, filters, schemas, includes.

**Needed**:
```java
record DatabaseDefinition(
    String qualifiedName,
    List<String> includes,                    // included database paths
    List<SchemaDefinition> schemas,           // named schemas
    List<TableDefinition> tables,             // top-level tables
    List<ViewDefinition> views,               // views
    List<JoinDefinition> joins,               // joins with full expression trees
    List<FilterDefinition> filters,           // named filters
    List<FilterDefinition> multiGrainFilters, // multi-grain filters
    List<TabularFunctionDefinition> tabularFunctions
)
```

### 2.6 ClassMappingDefinition — Needs Major Expansion

**Current**: Only `className`, `mappingType`, `mainTable`, `propertyMappings`, `sourceClassName`, `filterExpression`, `m2mPropertyExpressions`.

**Needed**:
```java
record ClassMappingDefinition(
    String className,
    String qualifiedClassName,     // NEW: preserve full qualified name
    String mappingType,            // "Relational" | "Pure" | "Relation" | "AggregationAware"
    String setId,                  // NEW: explicit [id] or null for default
    boolean isRoot,                // NEW: * prefix
    String extendsSetId,           // NEW: extends [parentId]
    TableReference mainTable,
    MappingFilter filter,          // NEW: structured ~filter (not raw string)
    boolean distinct,              // NEW: ~distinct flag
    List<MappingOperation> groupBy,   // NEW: ~groupBy expressions
    List<MappingOperation> primaryKey, // NEW: ~primaryKey expressions
    List<PropertyMappingDefinition> propertyMappings,
    // M2M-specific
    String sourceClassName,
    Map<String, String> m2mPropertyExpressions,
    // Relation-specific
    String funcReference           // NEW: ~func for Relation mapping type
)
```

### 2.7 PropertyMappingDefinition — Needs Restructuring

**Current**: A single record with `columnReference | joinReference | expressionString` — uses raw expression strings as fallback for everything complex.

**Needed**: A proper sealed interface:

```java
sealed interface PropertyMappingValue {
    record ColumnMapping(ColumnReference column) implements PropertyMappingValue {}
    record JoinMapping(List<JoinChainElement> joinChain, PropertyMappingValue terminal) implements PropertyMappingValue {}
    record ExpressionMapping(MappingOperation expression) implements PropertyMappingValue {}
    record EmbeddedMapping(List<MappingOperation> primaryKey, List<PropertyMappingDefinition> properties) implements PropertyMappingValue {}
    record InlineMapping(String targetSetId) implements PropertyMappingValue {}
    record OtherwiseMapping(EmbeddedMapping embedded, String fallbackSetId, JoinMapping fallbackJoin) implements PropertyMappingValue {}
}

record JoinChainElement(String databaseName, String joinName, String joinType) {}  // joinType: INNER, LEFT_OUTER, etc.

record PropertyMappingDefinition(
    String propertyName,
    boolean isLocal,                // + prefix
    String localType,               // type for local properties
    String localMultiplicity,       // multiplicity for local properties
    String sourceSetId,             // [sourceId, targetId] first
    String targetSetId,             // [sourceId, targetId] second
    String enumMappingId,           // EnumerationMapping transformer
    String bindingReference,        // Binding transformer
    PropertyMappingValue value
)
```

### 2.8 Association Mapping — Currently Missing

```java
record AssociationMappingDefinition(
    String associationName,
    String mappingType,             // "Relational" | "XStore"
    List<AssociationPropertyMapping> properties
) {
    record AssociationPropertyMapping(
        String propertyName,
        String sourceSetId,
        String targetSetId,
        // For Relational:
        List<JoinChainElement> joinChain,
        // For XStore:
        String crossExpression      // $this.prop == $that.prop
    ) {}
}
```

### 2.9 Mapping Include — Currently Missing

```java
record MappingInclude(
    String includedMappingPath,
    List<StoreSubstitution> storeSubstitutions
) {
    record StoreSubstitution(String originalStore, String substituteStore) {}
}
```

### 2.10 MappingOperation — Structured Expression Tree for Mapping Contexts

Used in `~groupBy`, `~primaryKey`, and DynaFunction property expressions. Mirrors `RelationalOperation` but in the mapping context (includes database pointers and mapping-specific features):

```java
sealed interface MappingOperation {
    record ColumnRef(String databaseName, String tableName, String columnName) implements MappingOperation {}
    record JoinNavigation(String databaseName, List<JoinChainElement> joinChain, MappingOperation terminal) implements MappingOperation {}
    record FunctionCall(String name, List<MappingOperation> args) implements MappingOperation {}
    record Comparison(MappingOperation left, String op, MappingOperation right) implements MappingOperation {}
    record BooleanOp(MappingOperation left, String op, MappingOperation right) implements MappingOperation {}
    record Literal(Object value) implements MappingOperation {}
    record Group(MappingOperation inner) implements MappingOperation {}
    record ScopeBlock(String databaseName, String tableName, List<PropertyMappingDefinition> properties) implements MappingOperation {}
}
```

---

## 3. Implementation Plan

### Build Order Rationale

**Hybrid: layer-first for foundation, then feature-by-feature for wiring.**

Layers 1-2 build the definition records and model objects that everything depends on. Once those are solid, Layers 3-5 wire them up feature-by-feature with testable increments.

This avoids repeatedly touching the same files (which happens with pure feature-by-feature), while still giving us testable increments for the complex wiring.

---

### Phase 1: Foundation — Definition Records (3-4 days)

**Goal**: Every grammar construct captured in a typed definition record. Zero raw-string fallbacks.

#### Step 1.1: `RelationalOperation` sealed interface
- New file: `model/def/RelationalOperation.java`
- Sealed interface hierarchy for join/filter/view expressions
- Variants: `ColumnRef`, `TargetColumnRef`, `Literal`, `FunctionCall`, `Comparison`, `BooleanOp`, `IsNull`, `IsNotNull`, `Group`, `ArrayLiteral`

#### Step 1.2: `MappingOperation` sealed interface
- New file: `model/def/MappingOperation.java`
- Sealed interface hierarchy for mapping property expressions, `~groupBy`, `~primaryKey`
- Variants: `ColumnRef`, `JoinNavigation`, `FunctionCall`, `Comparison`, `BooleanOp`, `Literal`, `Group`, `ScopeBlock`

#### Step 1.3: Expand `DatabaseDefinition`
- Add: `includes`, `schemas`, `views`, `filters`, `multiGrainFilters`, `tabularFunctions`
- `JoinDefinition` becomes `JoinDefinition(String name, RelationalOperation condition)`
- New nested records: `ViewDefinition`, `FilterDefinition`, `SchemaDefinition`, `TabularFunctionDefinition`

#### Step 1.4: Expand `MappingDefinition`
- `ClassMappingDefinition` gets: `qualifiedClassName`, `setId`, `isRoot`, `extendsSetId`, `filter` (as `MappingFilter`), `distinct`, `groupBy`, `primaryKey`, `funcReference`
- New `MappingFilter` record: `MappingFilter(String databaseName, List<JoinChainElement> joinPath, String filterName)`
- New `PropertyMappingValue` sealed interface replaces the current union-of-nulls pattern
- New `JoinChainElement` record for multi-hop join chains
- New `AssociationMappingDefinition` record
- New `MappingInclude` record with `StoreSubstitution`

#### Step 1.5: Update `MappingDefinition` top-level
- Add: `List<AssociationMappingDefinition> associationMappings`
- Add: `List<MappingInclude> includes`

---

### Phase 2: Foundation — Model Objects (2-3 days)

**Goal**: Rich model objects that the resolver and plan generator work with.

#### Step 2.1: `Join` gets full expression tree
- `Join(String name, RelationalOperation condition)` replaces current 5-field record
- Add helper methods: `involvesTable(String)`, `getSimpleEquiJoinColumns()` (backward compat for simple cases)
- All downstream code updated to use expression tree

#### Step 2.2: New model objects
- `View(name, filter, groupBy, distinct, columnMappings)` — usable like a `Table` in mappings
- `Filter(name, condition)` — referenced from class mappings
- `Schema(name, tables, views)` — namespace within Database

#### Step 2.3: Expand `Table`
- Add optional `schemaName` field
- Add milestoning metadata (business/processing milestoning fields)

#### Step 2.4: `PropertyMapping` expansion
- Support for join chains (not just single column)
- Support for expression-based mappings (DynaFunction expressions)
- Support for embedded, inline, otherwise references

---

### Phase 3: Builder Wiring (4-5 days)

**Goal**: `PackageableElementBuilder` properly extracts ALL grammar constructs into definition records. Zero regex. Zero raw-string fallbacks.

#### Step 3.1: `RelationalOperation` builder from `dbOperation` parse tree
- New method: `RelationalOperation visitDbOperation(DbOperationContext ctx)`
- Recursively converts `dbBooleanOperation`, `dbAtomicOperation`, `dbFunctionOperation`, `dbColumnOperation`, `dbJoinOperation`, `dbConstant`, `{target}` into `RelationalOperation` tree
- **Replaces the regex** in `extractDbJoin`

#### Step 3.2: `MappingOperation` builder from `mappingOperation` parse tree
- New method: `MappingOperation visitMappingOperation(MappingOperationContext ctx)`
- Handles: column refs, join navigation, function calls, comparisons, boolean ops, constants, scope blocks

#### Step 3.3: Database definition extraction
- `visitDatabase` extracts: includes, schemas (with tables/views), views, filters, multi-grain filters, tabular functions
- All use `visitDbOperation` for expression trees

#### Step 3.4: Mapping definition extraction — class mappings
- `visitClassMappingElement` extracts: `setId`, `isRoot`, `extendsSetId`, `qualifiedClassName`
- `visitRelationalClassMappingBody` extracts: `filter`, `distinct`, `groupBy`, `primaryKey`
- `visitRelationalPropertyValue` returns proper `PropertyMappingValue` subtypes (embedded, inline, otherwise) instead of raw strings

#### Step 3.5: Mapping definition extraction — associations, includes
- `visitMapping` processes `associationMappingElement` → `AssociationMappingDefinition`
- `visitMapping` processes `includeMapping` → `MappingInclude` with store substitutions

#### Step 3.6: Multi-hop join chains
- `visitMappingJoinSequence` returns `List<JoinChainElement>` with join types
- Used in property mappings, association mappings, and filter join paths

---

### Phase 4: Model Builder Wiring (3-4 days)

**Goal**: `PureModelBuilder` creates rich model objects from the expanded definition records.

#### Step 4.1: Database registration
- `addDatabase` creates `Join` with full `RelationalOperation` condition
- Registers `View` objects (usable as tables in mappings)
- Registers `Filter` objects
- Resolves `include` directives (merge tables/joins/views from included databases)
- Handles `Schema` namespacing

#### Step 4.2: Mapping registration
- `addMapping` processes includes first (transitive closure)
- Registers class mappings with set IDs, root markers, extends chains
- Registers association mappings (relational and XStore)
- Resolves store substitutions on includes

#### Step 4.3: MappingRegistry expansion
- Support set ID-based lookup: `findBySetId(String id)`
- Support root mapping lookup: `findRootMapping(String className)`
- Support multiple mappings per class (different set IDs)
- Association mapping registration and lookup

---

### Phase 5: Core Features End-to-End (5-7 days)

**Goal**: Each core feature works parse → model → resolver → plan → SQL.

#### Step 5.1: Join chains + join types
- MappingResolver follows multi-hop join chains
- PlanGenerator emits multi-table JOINs with correct join types
- SQLGenerator renders `LEFT OUTER JOIN`, `INNER JOIN`, etc.

#### Step 5.2: DynaFunction expressions in property mappings
- Property expressions like `concat(T.first, ' ', T.last)` compiled via `MappingOperation` tree
- PlanGenerator translates `MappingOperation.FunctionCall` → IR `FunctionExpression`
- Leverages existing PureFunctionRegistry for function→SQL mapping

#### Step 5.3: Scope blocks
- `scope([DB]T) (prop : col, ...)` desugared during builder extraction
- Each property in scope block gets the table prefix automatically
- No runtime impact — pure parse-time desugaring

#### Step 5.4: Embedded mappings
- PlanGenerator handles `EmbeddedMapping` by inlining sub-property column access
- Nested properties resolved from same table (denormalized data pattern)
- `~primaryKey` in embedded block used for deduplication

#### Step 5.5: Inline mappings
- MappingResolver resolves `Inline[setId]` by looking up the referenced class mapping
- Property mappings from the referenced mapping substituted in place

#### Step 5.6: Otherwise mappings
- Combines embedded mapping with join fallback
- PlanGenerator emits `COALESCE(embedded_col, join_col)` pattern or equivalent

#### Step 5.7: Association mappings
- Registered in MappingRegistry with source/target set ID pairs
- MappingResolver uses association mappings to resolve property navigation across mapped classes
- Generates proper JOIN conditions from association join definitions

#### Step 5.8: Mapping filters
- `~filter` on class mapping adds WHERE clause to generated SQL
- Filter-via-join navigates join path then applies named filter condition
- PlanGenerator emits filter as additional predicate in SelectSQLQuery

#### Step 5.9: Set IDs, root markers, extends
- MappingResolver uses set IDs for disambiguation
- Root marker determines default mapping when multiple exist for same class
- Extends resolves property mapping inheritance (child inherits parent's property mappings)

#### Step 5.10: Mapping includes + store substitution
- Transitive closure of included mappings resolved at model build time
- Store substitution replaces database references in included mappings
- All set IDs from included mappings visible in including mapping

---

### Phase 6: Advanced Features (5-8 days, can be deferred)

#### Step 6.1: Views as data sources
- Views usable in `~mainTable` and column references
- View's internal query (filter, groupBy, column expressions) materialized as subquery
- View columns typed from their defining expressions

#### Step 6.2: Self-joins (`{target}`)
- `TargetColumnRef` in `RelationalOperation` represents `{target}.column`
- SQLGenerator aliases the same table twice with different aliases

#### Step 6.3: Complex join conditions
- Multi-column joins (`T1.A = T2.A and T1.B = T2.B`)
- Function-based joins (`concat('prefix_', T1.name) = T2.prefixed`)
- `in()` conditions in joins
- Full `RelationalOperation` tree rendering to SQL

#### Step 6.4: Local mapping properties
- `+prop : Type[mult] : column_expr` creates a mapping-only property
- Available via `$this`/`$that` in XStore expressions
- Not visible in domain model queries

#### Step 6.5: XStore association mapping ⚠️ GRAMMAR NEEDED
- **Requires grammar addition**: `classMappingType` needs `XSTORE` variant
- Association with Pure model expressions (`$this.prop == $that.prop`)
- Cross-store join via temp tables / parameter passing
- Lambda compilation with `$this`/`$that` bindings

#### Step 6.6: AggregationAware mapping ⚠️ GRAMMAR NEEDED
- **Requires grammar addition**: `classMappingType` needs `AGGREGATION_AWARE` variant + view/mainMapping sub-grammar
- Query routing: redirect to pre-aggregated tables when query matches
- `~modelOperation` with `~canAggregate`, `~groupByFunctions`, `~aggregateValues`
- Falls back to `~mainMapping` when no aggregate view matches

#### Step 6.7: Relation class mapping ⚠️ GRAMMAR NEEDED
- **Requires grammar addition**: `classMappingType` needs `RELATION` variant + `~func` directive
- Maps class to a zero-argument Pure function returning `Relation<Any>[1]`
- Column names validated against relation type parameter
- Only primitive properties supported (no joins)

#### Step 6.8: Binding transformer
- `Binding qualifiedName :` prefix on property value
- Deserializes column value via external format binding (JSON, XML, etc.)

#### Step 6.9: Milestoning
- Business milestoning: `~businessFrom`, `~businessThru`
- Processing milestoning: `~processingIn`, `~processingOut`
- Auto-injected WHERE clauses for temporal date ranges
- `~busSnapshotDate`, `~processingSnapshotDate` for point-in-time queries

#### Step 6.10: Union/Merge set implementation
- Multiple mappings for same class combined via UNION ALL
- Merge strategy determines how conflicting rows are resolved

---

### Phase 7: RelationalMapping as Relation DSL (Endgame, 5-10 days)

**Goal**: `###Mapping` syntax desugars entirely into `Relation` algebra operations.

#### Translation Table

| Mapping Feature | Relation Equivalent |
|----------------|-------------------|
| `~mainTable [DB]T` | `#>{DB.T}#` (table accessor) |
| `prop : T.col` | `->project(~col)` |
| `prop : concat(T.A, T.B)` | `->extend(~prop : x \| $x.A + $x.B)` |
| `~filter [DB]myFilter` | `->filter(filterCondition)` |
| `~distinct` | `->distinct()` |
| `~groupBy(T.A, T.B)` | `->groupBy(~A, ~B)` |
| `@JoinName` | `->join(otherRelation, JoinKind, condition)` |
| `scope([DB]T) (p1 : c1, p2 : c2)` | `->project([~c1->as(~p1), ~c2->as(~p2)])` |
| Embedded mapping | `->project(~col1->as(~nested.prop1))` + object construction |
| Association mapping | `->join(targetRelation, JoinKind, condition)` |

#### Object Construction Gap

The fundamental gap: Relations produce flat rows, mappings produce nested objects. This requires:
- **Hydration layer**: post-processing step that takes flat rows and constructs object graphs
- **Graph fetch**: tree-shaped query that fetches nested objects efficiently
- This is a **presentation-layer concern**, not a relational algebra concern

---

## 4. Dependency Graph

```
Phase 1 (Definition Records)
    ├── Step 1.1: RelationalOperation
    ├── Step 1.2: MappingOperation
    ├── Step 1.3: DatabaseDefinition expansion
    ├── Step 1.4: MappingDefinition expansion
    └── Step 1.5: Top-level MappingDefinition
           │
           ▼
Phase 2 (Model Objects)
    ├── Step 2.1: Join expression tree
    ├── Step 2.2: View, Filter, Schema
    ├── Step 2.3: Table expansion
    └── Step 2.4: PropertyMapping expansion
           │
           ▼
Phase 3 (Builder Wiring)
    ├── Step 3.1: RelationalOperation builder (replaces regex!)
    ├── Step 3.2: MappingOperation builder
    ├── Step 3.3: Database extraction
    ├── Step 3.4: Class mapping extraction
    ├── Step 3.5: Association + Include extraction
    └── Step 3.6: Join chain extraction
           │
           ▼
Phase 4 (Model Builder Wiring)
    ├── Step 4.1: Database registration
    ├── Step 4.2: Mapping registration
    └── Step 4.3: MappingRegistry expansion
           │
           ▼
Phase 5 (Core Features E2E) ─── can start individual features as Phase 3-4 land
    ├── 5.1: Join chains
    ├── 5.2: DynaFunction expressions
    ├── 5.3: Scope blocks (parse-time only)
    ├── 5.4: Embedded mappings
    ├── 5.5: Inline mappings
    ├── 5.6: Otherwise mappings
    ├── 5.7: Association mappings
    ├── 5.8: Mapping filters
    ├── 5.9: Set IDs / root / extends
    └── 5.10: Includes + store substitution
           │
           ▼
Phase 6 (Advanced) ─── can be deferred
    ├── 6.1: Views as data sources
    ├── 6.2: Self-joins
    ├── 6.3: Complex join conditions
    ├── 6.4: Local mapping properties
    ├── 6.5: XStore (grammar + implementation)
    ├── 6.6: AggregationAware (grammar + implementation)
    ├── 6.7: Relation mapping (grammar + implementation)
    ├── 6.8: Binding transformer
    ├── 6.9: Milestoning
    └── 6.10: Union/Merge
           │
           ▼
Phase 7 ─── Endgame: Mapping as Relation DSL
```

---

## 5. Estimated Timeline

| Phase | Effort | Risk | Dependencies |
|-------|--------|------|--------------|
| Phase 1: Definition Records | 3-4 days | Low | None |
| Phase 2: Model Objects | 2-3 days | Low | Phase 1 |
| Phase 3: Builder Wiring | 4-5 days | Medium | Phases 1-2 |
| Phase 4: Model Builder | 3-4 days | Medium | Phases 1-3 |
| Phase 5: Core Features | 5-7 days | Medium | Phases 1-4 |
| Phase 6: Advanced | 5-8 days | High | Phase 5 |
| Phase 7: Relation DSL | 5-10 days | High | All |
| **Total** | **27-41 days** | | |

---

## 6. Key Design Decisions

### 6.1 Hybrid build order
Layer-first for foundation (Phases 1-2), then feature-by-feature (Phases 5-6). This avoids repeatedly touching the same definition/model files while still giving testable increments for complex wiring.

### 6.2 Unified expression tree
Single `RelationalOperation` sealed interface used in both `###Relational` (Database) and `###Mapping` contexts. `ColumnRef` has an optional `databaseName` (null in DB context, present in mapping context). Mapping-specific variants (`ScopeBlock`, `EnumTransform`, `BindingTransform`) are additional record types in the same sealed hierarchy. This avoids duplication between two nearly-identical tree types.

### 6.3 No regex, no string hacking
Every grammar construct gets a proper AST node. The `extractDbJoin` regex is the first thing to die. Raw expression string fallbacks in `visitRelationalPropertyValue` are replaced by structured `PropertyMappingValue` subtypes.

### 6.4 Backward-compatible model changes
Existing `Join`, `Table`, `DatabaseDefinition`, `MappingDefinition` records are expanded (not replaced). Existing constructor signatures are preserved where possible via secondary constructors.

### 6.5 XStore/AggregationAware/Relation require grammar additions
These three mapping types are not currently in the grammar. They need:
- New `classMappingType` alternatives (or a different dispatch mechanism)
- New sub-grammar rules for their bodies
- New definition records

---

## 7. Files Affected

### New Files (~10)
- `model/def/RelationalOperation.java` — sealed interface for DB expressions
- `model/def/MappingOperation.java` — sealed interface for mapping expressions
- `model/def/AssociationMappingDefinition.java` — association mapping record
- `model/def/MappingInclude.java` — mapping include record
- `model/def/PropertyMappingValue.java` — sealed interface for property mapping types
- `model/def/JoinChainElement.java` — join chain element record
- `model/store/View.java` — view model object
- `model/store/Filter.java` — filter model object
- `model/store/Schema.java` — schema model object

### Modified Files (~15)
- `antlr/PureParser.g4` — add XStore, AggregationAware, Relation mapping types (Phase 6)
- `antlr/PackageableElementBuilder.java` — major: replace regex, extract all grammar constructs
- `model/def/DatabaseDefinition.java` — expand with views, filters, schemas, includes
- `model/def/MappingDefinition.java` — expand ClassMappingDefinition, PropertyMappingDefinition
- `model/store/Join.java` — replace with expression tree
- `model/store/Table.java` — add schema, milestoning
- `model/mapping/MappingRegistry.java` — expand for set IDs, association mappings
- `model/mapping/RelationalMapping.java` — support embedded, DynaFunction property mappings
- `model/PureModelBuilder.java` — wire all new model objects
- `compiler/MappingResolver.java` — handle all new features
- `plan/PlanGenerator.java` — handle all new features
- `plan/SQLGenerator.java` — render complex joins, expressions
