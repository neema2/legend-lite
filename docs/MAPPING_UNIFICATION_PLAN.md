# MappingExpander Architecture — End-to-End Plan

> Evidence-based plan for centralizing mapping resolution into a MappingExpander layer,
> creating a clean path from Mapping-DSL → Relation → SQL.

---

## 1. Current State — What Exists Today

### 1.1 The Pipeline

```
PureModelBuilder.addSource()  →  TypeChecker.check()  →  PlanGenerator.generate()
     (parse + model)              (type-check + route)     (AST → SQL)
```

TypeChecker does **two jobs**: pure type-checking AND store-specific mapping resolution.
This creates scattered mapping logic across 8+ files.

### 1.2 Where Mapping Logic Lives Today

| File | What It Does | Lines |
|------|-------------|-------|
| [GetAllChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/GetAllChecker.java) | Discovers mapping, resolves M2M chains, builds virtual schema | ~200 |
| [TypeChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/TypeChecker.java#L910-L992) | `resolveAssociationsInBody` + `scanForAssociationPaths` + `resolveAndStore` | ~80 |
| [ProjectChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/ProjectChecker.java#L84-L97) | Calls `resolveAssociations`, merges with source | ~15 |
| [FilterChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/FilterChecker.java#L42) | `resolveAssociationsFromParams` | ~5 |
| [SortChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/SortChecker.java#L210) | `resolveAssociationsFromParams` (2 call sites) | ~10 |
| [MapChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/MapChecker.java#L85) | `resolveAssociations` | ~5 |
| [ScalarChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/ScalarChecker.java#L83) | `resolveAssociationsFromParams` | ~5 |
| [AbstractChecker](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/AbstractChecker.java#L935-L953) | `resolveAssociationsFromParams` helper | ~20 |
| **Total** | | **~340 lines scattered across 8 files** |

### 1.3 PropertyMapping Model — Current Capabilities

From [PropertyMapping.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/model/store/PropertyMapping.java):

```java
// 3 modes:
PropertyMapping.column("firstName", "FIRST_NAME");          // Simple column
PropertyMapping.expression("price", "PAYLOAD",              // Semi-structured ->get()
    "PAYLOAD->get('price', @Integer)");
PropertyMapping.enumColumn("gender", "GENDER_CODE",         // Enum transform
    "Gender", Map.of("M", List.of("M","MALE"), ...));
```

> [!WARNING]
> **No DynaFunction support.** The doc (§4) shows `concat([db]T.first, ' ', [db]T.last)` in property
> mappings, but our `PropertyMapping` only handles column references, `->get()` expressions, and
> enum mappings. DynaFunction transforms are NOT parsed or represented.

### 1.4 Store Model — What We Have

From [/model/store/](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/model/store):

| Model class | What it represents |
|------------|-------------------|
| `Table(name, columns)` | Database table |
| `Column(name, sqlType, nullable)` | Table column |
| `Join(name, leftTable, leftCol, rightTable, rightCol)` | Single-column equi-join |
| `SqlDataType` enum | Column types |
| `PropertyMapping` | Property→column mapping (3 modes above) |

> [!WARNING]
> **No Views.** The doc (§3.8) shows Views as precomputed relations, but we have no `View` concept.
> **No multi-column joins.** Our `Join` only supports single-column equi-joins.
> **No Filters in DB definitions.** Named filters in `###Relational` are not supported.

### 1.5 PlanGenerator — Two Codepaths

From [PlanGenerator.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/plan/PlanGenerator.java):

```java
// Class-based path (line 431-441):
private SqlBuilder generateGetAll(AppliedFunction af) {
    TypeInfo info = unit.typeInfoFor(af);
    String tableName = info.tableName();         // ← reads from mapping
    return new SqlBuilder().selectStar().from(tableName, alias);
}

// Relation-based path:
private SqlBuilder generateTable(AppliedFunction af) { ... }  // #>{db.table}#
```

Association JOINs are emitted in `generateProject` (lines 783-812) and `generateFilter` by reading
`TypeInfo.associations()` from the sidecar.

---

## 2. Target Architecture

```
PureModelBuilder  →  TypeChecker  →  MappingExpander  →  PlanGenerator
  (parse + model)    (pure types)    (class→Relation)     (Relation→SQL)
```

### 2.1 What Each Layer Does

| Layer | Responsibility | Does NOT do |
|-------|---------------|-------------|
| **PureModelBuilder** | Parse Pure → registries (Classes, Mappings, Joins, Tables) | No type-checking |
| **TypeChecker** | Pure type inference: `getAll(Person) → Person[*]`, `$p.name → String` | No mapping lookup, no association resolution |
| **MappingExpander** | Rewrites class-based expression tree into Relation expression tree using the mapping | No type inference, no SQL |
| **PlanGenerator** | Converts Relation expression tree → SQL | No mapping logic, one codepath |

### 2.2 What MappingExpander Produces

**Input**: Type-checked AST + Mapping

```pure
// getAll returns bare ClassType:
Person.all()->project([p | $p.firstName, p | $p.address.city])->from(MyMapping, MyRuntime)
```

**Output**: Rewritten as Relation operations:

```pure
#>{db.PersonTable}#
  ->join(#>{db.AddressTable}#, LEFT, {a,b | $a.PERSON_ID == $b.ID})
  ->project(~[firstName: r | $r.FIRST_NAME, city: r | $r.CITY])
```

---

## 3. Phased Implementation Plan

### Phase 1: Externalize Mapping Discovery (Current Scope)

**Goal**: getAll() receives mapping instead of scanning for it.

**Changes**:

#### [MODIFY] [GetAllChecker.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/checkers/GetAllChecker.java)

Current (line 48):
```java
ClassMapping mapping = registry().findAnyMapping(className)
    .orElseThrow(...);
```

Proposed — inject mapping via a `MappingResolver`:
```java
ClassMapping mapping = env.resolveMapping(className);
// MappingResolver can be:
//   - auto-discover (single mapping per class) → current behavior
//   - explicit       (user-provided mapping)    → legend-engine pattern
```

#### [MODIFY] [TypeCheckEnv.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/TypeCheckEnv.java)

Add: `ClassMapping resolveMapping(String className)`

#### [MODIFY] [TypeChecker.java](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/compiler/TypeChecker.java)

Implement `resolveMapping` — delegates to auto-discovery or explicit injection.

**Risk**: Low. Behavioral change is minimal — auto-discovery produces same result.

---

### Phase 2: Centralize Association Resolution

**Goal**: Extract `resolveAssociations` from 6 checkers + TypeChecker into one place.

**What moves**:

| From | Lines Removed |
|------|--------------|
| TypeChecker `resolveAssociationsInBody` | ~80 lines |
| AbstractChecker `resolveAssociationsFromParams` | ~20 lines |
| ProjectChecker association block | ~15 lines |
| FilterChecker association call | ~5 lines |
| SortChecker association calls (×2) | ~10 lines |
| MapChecker association call | ~5 lines |
| ScalarChecker association call | ~5 lines |

**Where it goes**: A new `AssociationResolver` (or part of MappingExpander):

```java
public class AssociationResolver {
    private final ModelContext model;
    
    /**
     * Scans an expression tree for multi-hop property paths and resolves
     * associations using the mapping. One place, one implementation.
     */
    public Map<String, TypeInfo.AssociationTarget> resolve(
            List<ValueSpecification> body, ClassMapping mapping) {
        // Current logic from TypeChecker.resolveAssociationsInBody
        // + scanForAssociationPaths + resolveAndStore
    }
}
```

Each checker changes from:
```java
// Before (in each checker):
associations = env.resolveAssociations(bodies, mapping);
```
to:
```java
// After (in each checker):
associations = env.associationResolver().resolve(bodies, mapping);
```

> [!NOTE]
> Phase 2 is pure refactoring — no behavioral change. The logic moves but the output is identical.

**Risk**: Low. Each checker still gets the same `Map<String, AssociationTarget>`.

---

### Phase 3: Move M2M Resolution into MappingExpander

**Goal**: GetAllChecker becomes ~30 lines. M2M chain resolution moves to MappingExpander.

**What moves from GetAllChecker**:

| Method | Lines | Destination |
|--------|-------|------------|
| `compileM2MGetAll` | ~35 | MappingExpander |
| `resolveSource` | ~12 | MappingExpander |
| `resolveM2MChain` | ~23 | MappingExpander |
| `buildSourceContext` | ~25 | MappingExpander |
| `resolvePropertyType` | ~7 | MappingExpander |
| `resolveJoinReferences` | ~25 | MappingExpander |

**GetAllChecker after Phase 3**:
```java
public TypeInfo check(AppliedFunction af, TypeInfo source,
                      TypeChecker.CompilationContext ctx) {
    resolveOverload("getAll", params, null);
    String className = TypeInfo.simpleName(fullPath);
    ClassMapping mapping = env.resolveMapping(className);
    
    // Just return ClassType[*] — MappingExpander handles the rest
    return TypeInfo.builder()
        .mapping(mapping)
        .expressionType(ExpressionType.many(new GenericType.ClassType(fullPath)))
        .build();
}
```

**Risk**: Medium. PlanGenerator currently reads M2M virtual schema from getAll's TypeInfo.
The MappingExpander needs to stamp this info in a way PlanGenerator can still find it.

---

### Phase 4: Unify PlanGenerator Codepaths

**Goal**: PlanGenerator only has the Relation path. Class-based `generateGetAll` is removed.

**What changes**:

```java
// Before (two paths):
case "getAll" -> generateGetAll(af);      // reads TypeInfo.tableName()
case "#>{...}#" -> generateTable(af);      // reads table from AST

// After (one path):
// MappingExpander has already rewritten getAll(Person) → #>{PersonTable}#
// PlanGenerator only sees Relation operations
```

**Risk**: High. This requires MappingExpander to produce AST nodes that PlanGenerator can process.
The AST representation needs to be rich enough to carry table references, join conditions, etc.

> [!IMPORTANT]
> Phase 4 is where the Mapping→Relation translation actually happens.
> The MappingExpander would emit Relation AST nodes, not just TypeInfo metadata.

---

### Phase 5: DynaFunction Transforms in Property Mappings

**Goal**: Support `fullName: concat([db]T.first, ' ', [db]T.last)` in property mappings.

**What's needed**:

1. **Parser**: Parse DynaFunction expressions in property mapping RHS
2. **PropertyMapping model**: Store parsed AST instead of string column name
3. **MappingExpander**: Translate DynaFunction to Relation `extend()` expression

The doc (§4.4) shows transforms can live in 3 places:

| Place | Our support | What to add |
|-------|------------|-------------|
| **Property mapping RHS** | ❌ Only column refs | Parse DynaFunction → store as AST → emit `extend()` |
| **Database View** | ❌ No View concept | Add View to store model → treated as named Relation function |
| **Local property (+)** | ❌ Not supported | Parse +prop → store as computed column → emit `extend()` |

**Risk**: Medium. Parser changes needed. But DynaFunctions map 1:1 to our existing
`BuiltinFunctionRegistry` (~225 functions). The MappingExpander just translates the
mapping's DynaFunction tree into a Relation `extend()` lambda.

---

### Phase 6: Mapping as Relation DSL (Endgame)

**Goal**: `###Mapping` becomes syntactic sugar that MappingExpander expands into Relation ops.

At this point, MappingExpander does a single-pass tree rewrite:

```java
// For each ClassMapping in the Mapping:
Relation expandClassMapping(RootRelationalClassMapping cm) {
    // 1. Start with main table
    Relation rel = tableAccess(cm.mainTable());       // #>{db.Table}#
    
    // 2. Apply filter
    if (cm.filter != null)
        rel = rel.filter(translatePredicate(cm.filter));
    
    // 3. Process property mappings
    for (PropertyMapping pm : cm.propertyMappings) {
        if (pm.isSimpleColumn())
            rel = rel.project(pm.propertyName(), pm.columnName());
        else if (pm.isDynaFunction())
            rel = rel.extend(pm.propertyName(), translateDynaFunction(pm.expression));
        else if (pm.isJoinChain())
            rel = rel.join(resolveJoinTarget(pm), pm.joinType())
                      .project(pm.propertyName(), pm.targetColumn());
    }
    
    // 4. Distinct / GroupBy
    if (cm.isDistinct()) rel = rel.distinct();
    if (cm.groupBy != null) rel = rel.groupBy(translateGroupBy(cm.groupBy));
    
    return rel;
}
```

This pseudocode mirrors the doc's §7.2 translation. The key insight: when mappings
become Relations, this entire method becomes "just inline the user's Relation expression."

---

## 4. Evidence: What Translates Cleanly

From the doc §8.1, cross-referenced with our codebase:

| Feature | Doc Assessment | Our State | Phase |
|---------|---------------|-----------|-------|
| Simple column mapping | 🟢 Trivial | ✅ Working | Current |
| Enum transforms | 🟡 Medium | ✅ Working (PropertyMapping.enumColumn) | Current |
| Join-based properties | 🟢 Easy | ✅ Working (AssociationTarget + PlanGenerator JOINs) | Current |
| Multi-hop joins | 🟡 Medium | ✅ Working (scanForAssociationPaths) | Current |
| Semi-structured `->get()` | 🟢 Easy | ✅ Working (PropertyMapping.expression) | Current |
| DynaFunction transforms | 🟢 Easy per fn | ❌ Not in mappings | Phase 5 |
| Database Views | 🟢 Easy | ❌ No View concept | Phase 5 |
| Local properties (+) | 🟢 Easy | ❌ Not supported | Phase 5 |
| Embedded mappings | 🟡 Medium | ❌ Not supported | Phase 6 |
| Union mapping | 🟢 Easy | ❌ Not supported | Phase 6 |
| xStore mapping | 🔴 Hard | ❌ Not supported | Future (Router) |
| Relation class mapping | 🟢 Trivial | ❌ Not supported | Phase 6 |

## 5. Risk Assessment — One-Way Doors

| Concern | Is it a One-Way Door? | Why |
|---------|----------------------|-----|
| No Router | ❌ Two-way | MappingExpander can be extended with clustering without rewrite |
| M2M in GetAllChecker | ❌ Two-way | Clean extraction path to MappingExpander in Phase 3 |
| Two PlanGenerator paths | ❌ Two-way | Unification is additive (Phase 4) |
| PropertyMapping lacks DynaFunction | ❌ Two-way | Add `ValueSpecification` field to PropertyMapping, backward-compat |
| No View in store model | ❌ Two-way | Views = named Relation functions, additive |
| AssociationTarget in TypeInfo sidecar | ⚠️ **Watch** | If too many PlanGenerator paths depend on this format, changes get expensive. Keep association format stable or migrate PlanGenerator first. |

## 6. Recommended Ordering

```
Phase 1 (Now)  ─── Externalize mapping discovery
     │              1-2 days · Risk: Low
     ▼
Phase 2 (Next) ─── Centralize association resolution
     │              2-3 days · Risk: Low · Pure refactoring
     ▼
Phase 3 (Then) ─── M2M → MappingExpander
     │              2-3 days · Risk: Medium
     ▼
Phase 4 (Later) ── Unify PlanGenerator paths
     │              3-5 days · Risk: High
     ▼
Phase 5 (After) ── DynaFunction transforms
     │              3-5 days · Risk: Medium
     ▼
Phase 6 (End)  ─── Mapping as Relation DSL
                    5-10 days · Risk: High
```

> [!IMPORTANT]
> Phases 1-3 are **cleanup and refactoring** — no new features, just architecture.
> Phase 4 is the **critical pivot** — after this, PlanGenerator is unified.
> Phases 5-6 are **feature work** building on the clean foundation.
