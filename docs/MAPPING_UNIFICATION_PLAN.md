# Mapping Unification Plan

> Four-pass compiler architecture for legend-lite:
> **Parse → Type → Map → Generate**
>
> Proven by legend-engine's `Compile → Route → Plan → Execute` pipeline.
> Every claim below is verified against actual codebase with line references.

---

## 1. Target Architecture

```
PureParser  →  TypeChecker  →  MappingTranslator  →  PlanGenerator
 (Pass 1)       (Pass 2)         (Pass 3)             (Pass 4)
 Pure→AST       types only       mapping→store         SQL gen
```

| Pass | Input | Output | Uses | Doesn't Touch |
|------|-------|--------|------|----------------|
| **Parse** | Pure source text | AST + Classes + Mappings + Tables + Joins | Grammar rules | Types, stores, SQL |
| **Type** | AST + class/assoc defs | Typed AST (`$p.name → String`) | Class model only | Mappings, tables, SQL |
| **Map** | Typed AST + mapping defs | Resolved AST (tables, columns, joins) | Mapping model | SQL syntax |
| **Generate** | Resolved AST | SQL string | SQL dialect | Types, mappings |

### No Chicken-and-Egg — Proven by Code

`compileProperty` ([TypeChecker.java:1090-1175](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/TypeChecker.java#L1090-L1175))
resolves types using **only class/association model data**:

```java
// Line 1112-1118: Class property type from model
modelContext.findClass(qualifiedName)...findProperty(ap.property())
→ GenericType.fromType(propOpt.get().genericType())

// Line 1125-1139: Association-injected property from model
modelContext.findAssociationByProperty(qualifiedName, ap.property())
→ GenericType.ClassType(nav.targetClassName())
```

**`ctx.getMapping()` is never called by `compileProperty`.** It is defined at line 1404
but only consumed by `resolveAssociations` — the join-resolution code that moves to
MappingTranslator.

### Confirmed by Legend-Engine

Legend-engine uses the same architecture ([router_main.pure](file:///Users/neema/legend/legend-engine/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/router/router_main.pure)):
Router runs after compilation, before plan generation. ~1,200 lines across 6 files.

---

## 2. Current State — Precise Inventory

### 2.1 Checker Classification (Verified by Grep)

**6 ACTIVE checkers** — call `resolveAssociations` using `source.mapping()`:

| Checker | What it does with mapping | Lines |
|---------|--------------------------|-------|
| [GetAllChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/GetAllChecker.java) | Discovers mapping, resolves M2M chains, builds virtual schema | ~200 |
| [ProjectChecker:40,91](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/ProjectChecker.java#L40-L104) | `mapping = source.mapping()` → `env.resolveAssociations(bodies, mapping)` | ~15 |
| [FilterChecker:42](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/FilterChecker.java#L42) | `resolveAssociationsFromParams(params, source)` | ~5 |
| [SortChecker:210,238](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/SortChecker.java#L210-L242) | `resolveAssociationsFromParams` (2 call sites) | ~10 |
| [MapChecker:84-85](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/MapChecker.java#L84-L91) | `if (source.mapping() != null)` → `env.resolveAssociations(lambda.body(), source.mapping())` | ~5 |
| [ScalarChecker:83](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/ScalarChecker.java#L83) | `resolveAssociationsFromParams(params, source)` | ~5 |

**10 PASSTHROUGH checkers** — only propagate via `.mapping(source.mapping())`:

> GroupByChecker, FromChecker, AggregateChecker, ExtendChecker, DistinctChecker (×3),
> SelectChecker (×3), RenameChecker, SlicingChecker, FlattenChecker (×2), PivotChecker

**1 INFRASTRUCTURE** — [AbstractChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/AbstractChecker.java):
- `resolveAssociationsFromParams` ([lines 939-953](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/AbstractChecker.java#L939-L953)): collects lambda bodies → calls `env.resolveAssociations`
- `bindLambdaParam` ([lines 747-788](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/AbstractChecker.java#L747-L788)): stamps `ctx.withMapping(paramName, source.mapping())` for 3 type branches

### 2.2 Association Resolution — Two Parts

Today `resolveAssociations` does TWO things in one pass:

| Part | What | Source of truth | Where it goes |
|------|------|-----------------|---------------|
| **Type resolution** | `$p.address → Address[0..1]` | `modelContext.findAssociationByProperty()` (class/assoc defs) | Stays in TypeChecker |
| **Join resolution** | `$p.address → JOIN(Person.ID = Address.PERSON_ID)` | `MappingRegistry.findJoin()` (mapping defs) | Moves to MappingTranslator |

### 2.3 PlanGenerator — Mapping-Reading Inventory

PlanGenerator reads mapping info at 3 key points:

**Point 1 — `resolveColumnExpr`** ([lines 837-907](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/plan/PlanGenerator.java#L837-L907), 75 lines):
```java
// Core property→column resolver. 3 cases:
if (mapping instanceof PureClassMapping pcm)     // M2M: compile expression AST to SQL
if (!(mapping instanceof RelationalMapping rm))  // Error
rm.getPropertyMapping(propertyName)              // Relational: column/enum/expression
```
Called from 8 sites: generateProject (×2), generateGraphFetch, generateSort,
generateFilter, resolveAssociationRefs, generateScalar.

**Point 2 — Association JOIN generation** ([lines 975-1010](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/plan/PlanGenerator.java#L975-L1010)):
```java
assocTarget.targetMapping().sourceTable().name()  // target table
mapping.sourceTable().name()                       // source table
join.getColumnForTable(srcTableName)               // FK columns
```
Called from: resolveAssociationRefs, generateProject (step 5), generateSort.

**Point 3 — `info.tableName()`** (2 call sites):
```java
// Line 433: generateGetAll entry point
String tableName = info != null ? info.tableName() : null;
// Line 547: generateGraphFetch entry point
String tableName = info != null ? info.tableName() : null;
```

### 2.4 PropertyMapping Model Capabilities

| Mode | Example | Supported |
|------|---------|-----------|
| Simple column | `firstName → FIRST_NAME` | ✅ |
| Semi-structured | `price → PAYLOAD→get('price', @Integer)` | ✅ |
| Enum transform | `gender → GENDER_CODE (CASE WHEN)` | ✅ |
| DynaFunction | `fullName → concat(T.first, ' ', T.last)` | ❌ |
| Database View | `CREATE VIEW FullNameView AS ...` | ❌ |
| Local property (+) | `+fullName: self.first + ' ' + self.last` | ❌ |

---

## 3. MappingTranslator Design

### 3.1 Output Format

MappingTranslator doesn't create a new AST. It produces a **`StoreResolution`** record
that PlanGenerator reads alongside the typed AST:

```java
public record StoreResolution(
    // Per-node resolution: AST node → its resolved store info
    Map<ValueSpecification, NodeResolution> nodeResolutions,
    // All association joins discovered in the expression tree
    Map<String, AssociationJoin> associationJoins
) {
    // For each AST node that references a class/property:
    public sealed interface NodeResolution {
        record TableAccess(String tableName, String alias,
                           RelationalMapping mapping) implements NodeResolution {}
        record ColumnRef(String tableName, String columnName,
                         PropertyMapping pm) implements NodeResolution {}
        record AssociationNav(String joinName, String targetTable,
                              String leftCol, String rightCol,
                              boolean isToMany) implements NodeResolution {}
    }

    public record AssociationJoin(
        String propertyName, Join join,
        RelationalMapping targetMapping, boolean isToMany
    ) {}
}
```

### 3.2 How PlanGenerator Consumes It

```java
// Before (reads mapping from TypeInfo sidecar):
ClassMapping mapping = info.mapping();
String tableName = info.tableName();
SqlExpr col = resolveColumnExpr(propName, mapping, alias);

// After (reads from StoreResolution):
var resolution = storeResolution.nodeResolutions().get(astNode);
switch (resolution) {
    case TableAccess ta -> builder.from(ta.tableName(), ta.alias());
    case ColumnRef cr   -> new SqlExpr.Column(alias, cr.columnName());
    case AssociationNav nav -> addJoin(nav);
}
```

### 3.3 The Single-Pass Walk

```java
public class MappingTranslator {
    public StoreResolution translate(
            Map<ValueSpecification, TypeInfo> typedAST,
            MappingRegistry registry) {

        var resolutions = new HashMap<ValueSpecification, NodeResolution>();
        var joins = new HashMap<String, AssociationJoin>();

        for (var entry : typedAST.entrySet()) {
            ValueSpecification node = entry.getKey();
            TypeInfo type = entry.getValue();

            if (node instanceof AppliedFunction af && "getAll".equals(af.function())) {
                // Resolve class → table
                String className = extractClassName(af);
                ClassMapping mapping = registry.findAnyMapping(className).orElseThrow();
                resolutions.put(node, new TableAccess(
                    mapping.sourceTable().name(), "t0", (RelationalMapping) mapping));
            }

            if (node instanceof AppliedProperty ap) {
                // Check if this is an association navigation
                // (type was already resolved by TypeChecker to the target class)
                // Now resolve the JOIN from the mapping
                scanForAssociationJoins(ap, type, registry, joins);
            }
        }
        return new StoreResolution(resolutions, joins);
    }
}
```

---

## 4. Phased Implementation

### Phase 1: Build MappingTranslator + Remove Mapping from TypeChecker

**Concrete changes**:

#### [NEW] `MappingTranslator.java`
- Single-pass walk over typed AST
- Resolves: getAll → table, properties → columns, associations → joins
- Produces `StoreResolution` record

#### [NEW] `StoreResolution.java`
- Record holding per-node resolutions + association joins

#### [MODIFY] 10 PASSTHROUGH checkers
- Delete `.mapping(source.mapping())` from each (1-line change each)

#### [MODIFY] 6 ACTIVE checkers
- Delete `resolveAssociations` calls (Project, Filter, Sort, Map, Scalar)
- GetAllChecker: remove M2M chain resolution, keep only type resolution (~200 lines → ~25 lines)

#### [MODIFY] `AbstractChecker.java`
- Delete `resolveAssociationsFromParams` (lines 939-953)
- Remove `ctx.withMapping()` from `bindLambdaParam` (lines 759, 769, 777)

#### [MODIFY] `TypeInfo.java`
- Remove `mapping` field and `associations` field from TypeInfo record
- Remove `Builder.mapping()` and `Builder.associations()` methods
- Remove `tableName()`, `hasAssociations()` convenience methods

#### [MODIFY] `PlanGenerator.java`
- `generateGetAll`: read table from `StoreResolution` instead of `info.tableName()`
- `generateProject`: read associations from `StoreResolution` instead of `info.associations()`
- `generateSort`: read mapping from `StoreResolution` instead of `info.mapping()`
- `resolveColumnExpr`: accept `StoreResolution` instead of `ClassMapping`

**Test impact**: All existing tests should pass — behavior is unchanged,
just responsibility moves from TypeChecker to MappingTranslator.

**Estimate**: 5-7 days. Risk: Medium (largest change, but mostly mechanical deletion).

---

### Phase 2: Unify PlanGenerator to One Codepath

**Goal**: Delete `generateGetAll`. MappingTranslator produces resolved metadata
that PlanGenerator processes uniformly.

**Concrete changes**:
- Delete `generateGetAll` method (~15 lines)
- Delete `generateGraphFetch`'s separate table resolution (~15 lines)
- All paths use `StoreResolution.nodeResolutions()` for table/column/join info
- `resolveColumnExpr` simplified: no longer handles M2M case inline (MappingTranslator pre-resolves)

**Estimate**: 3-5 days. Risk: Medium.

---

### Phase 3: DynaFunction Transforms

**Goal**: Support `fullName: concat(T.first, ' ', T.last)` in property mappings.

**Concrete changes**:
- Parser: handle DynaFunction expressions in property mapping RHS
- `PropertyMapping`: add `Optional<ValueSpecification> expression()` for parsed AST
- `MappingTranslator`: translate DynaFunction → SQL via existing `BuiltinFunctionRegistry` (~225 fns)

**Estimate**: 3-5 days. Independent — can start after Phase 1.

---

### Phase 4: Mapping as Relation DSL (Endgame)

**Goal**: `###Mapping` → syntactic sugar expanded by MappingTranslator into Relation ops.

**Estimate**: 5-10 days. Depends on Phases 1-3.

---

## 5. What PlanGenerator Loses (Line Inventory)

| PlanGenerator code | Lines | What happens |
|-------------------|-------|--------------|
| `resolveColumnExpr` (3-way dispatch) | 75 | Simplified: reads pre-resolved columns from StoreResolution |
| `resolveAssociationRefs` (recursive) | ~40 | Reads pre-resolved joins from StoreResolution |
| `generateGetAll` (class-based entry) | ~15 | Deleted entirely |
| `info.tableName()` reads | 2 sites | Reads from StoreResolution instead |
| `info.mapping()` reads | ~10 sites | Reads from StoreResolution instead |
| `info.associations()` reads | ~5 sites | Reads from StoreResolution instead |
| **Est. total reduction** | **~150 lines** | Replaced by StoreResolution reads |

---

## 6. Risk Assessment

| Concern | Risk | Mitigation |
|---------|------|------------|
| Breaking 16 checkers | Low | 10 are 1-line deletions, 6 are small refactors |
| MappingTranslator misses edge case | Medium | Same test suite runs against new path |
| PlanGenerator reads wrong resolution | Medium | StoreResolution is type-safe (sealed interface) |
| M2M chain resolution in wrong pass | Low | M2M types resolved by TypeChecker (class defs), M2M joins by MappingTranslator |
| No one-way doors | ✅ | All phases reversible |

## 7. Timeline

```
Phase 1 ─── MappingTranslator + remove mapping from TypeChecker
│            5-7 days
▼
Phase 2 ─── Unify PlanGenerator codepaths
│            3-5 days
├──────────────────────┐
▼                      ▼
Phase 3 (parallel) ── DynaFunction transforms
│                      3-5 days
▼
Phase 4 ─── Mapping as Relation DSL
             5-10 days
```

**Total: ~18-27 days**
