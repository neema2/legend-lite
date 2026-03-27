# Mapping Unification Plan

> Four-pass compiler architecture for legend-lite:
> **Parse → Type → Resolve → Generate**
>
> Proven by legend-engine's `Compile → Route → Plan → Execute` pipeline.
> Every claim is verified against actual codebase with line references.

---

## 1. Target Architecture

```
PureParser  →  TypeChecker  →  MappingResolver  →  PlanGenerator
 (Pass 1)       (Pass 2)        (Pass 3)            (Pass 4)
 Pure→AST       types only      mapping→store        SQL gen
```

| Pass | Input | Output | Uses | Doesn't Touch |
|------|-------|--------|------|----------------|
| **Parse** | Pure source text | AST + Classes + Mappings + Tables + Joins | Grammar rules | Types, stores, SQL |
| **Type** | AST + class/assoc defs | Typed AST (`$p.name → String`) | Class model only | Mappings, tables, SQL |
| **Resolve** | Typed AST + mapping defs | Resolved AST (tables, columns, joins) | Mapping model | SQL syntax |
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

**`ctx.getMapping()` is never called by `compileProperty`.** Defined at line 1404,
only consumed by `resolveAssociations` — join-resolution code that moves to MappingResolver.

### Confirmed by Legend-Engine

Legend-engine uses the same architecture. Router runs after compilation, before plan
generation. ~1,200 lines across 6 files in `core/pure/router/`.

---

## 2. Entry Point Inventory

### Pipeline Roots (6 real, 2 fake to delete)

**Family 1 — Class Sources** (need mapping):

| Root | AST Type | Checker | Mapping Source |
|------|----------|---------|----------------|
| `Person.all()` | `AppliedFunction("getAll")` | GetAllChecker | Registry lookup |
| `^Person(name='J')` | `ClassInstance("instance")` | InstanceChecker *(new)* | Identity (built on fly) |

`[^P(..), ^P(..)]` is a `PureCollection` of instances → `compileCollection` iterates,
each `^P()` goes through InstanceChecker. Collection wrapping stays inline.

**Family 2 — Relation Sources** (no mapping):

| Root | AST Type | Checker |
|------|----------|---------|
| `#>{db.PersonTable}#` | `ClassInstance("relation")` | RelationAccessChecker *(new)* |
| `TDS [col1:String]` | `ClassInstance("tdsLiteral")` | TdsLiteralChecker *(new)* |

**Family 3 — Scalar / Lambda** (no relation, no mapping):

| Root | AST Type | Handler |
|------|----------|---------|
| `\|1+1`, `{x\|body}` | `LambdaFunction` | `compileLambda` (inline) |

**FAKE — Delete**:

| Root | Why delete |
|------|-----------|
| `table('db.T')` | Not a real Pure function. Raw table access → use `#>{db.T}#` |
| `class('M::P')` | Redundant with `Person.all()` — same mapping lookup |

### Core Compiler Mechanics (stay inline in TypeChecker)

| Mechanic | Why inline |
|----------|-----------|
| `compileLambda` | Scope management (let bindings, multi-statement bodies) |
| `compileVariable` | Variable lookup (lambda params, let bindings) |
| `compileProperty` | Property resolution (class model, relation schema, associations) |
| `compileCollection` | Element type unification |
| Literals | Trivial one-liners |

### Why TypeChecker Doesn't Need Mappings

| What TypeChecker checks | Source of truth | Needs mapping? |
|------------------------|-----------------|----------------|
| `Person.all()` → `Person[*]` | Class exists? → class definition | **No** |
| Available properties | Person's property list → class definition | **No** |
| `$x.name` → `String` | Property type → class definition | **No** |
| `$x.address.city` → `String` | Association navigation type → association definition | **No** |

What mapping adds (resolved by MappingResolver, not TypeChecker):
- `name` → column `FIRST_NAME` (physical column)
- `address` → `JOIN Person.ID = Address.PERSON_ID` (physical join)

---

## 3. Phased Implementation

### Phase 0: Clean TypeChecker Architecture

**Goal**: Eliminate boilerplate, make TypeChecker a clean generic dispatcher,
unify function resolution.

#### Clean `compileFunction` — one pattern, no boilerplate

Today: 25 wrapper methods + 40-case switch. After: **one method, no wrappers**.

```java
private TypeInfo compileFunction(AppliedFunction af, CompilationContext ctx) {
    String funcName = simpleName(af.function());

    // Common: compile first arg (source) — harmless for source-less functions
    TypeInfo source = !af.parameters().isEmpty()
        ? compileExpr(af.parameters().get(0), ctx) : null;

    // Switch is pure name → checker, one line each
    var info = switch (funcName) {
        case "getAll"   -> new GetAllChecker(this).check(af, source, ctx);
        case "filter"   -> new FilterChecker(this).check(af, source, ctx);
        case "project"  -> new ProjectChecker(this).check(af, source, ctx);
        case "sort", "sortBy", "sortByReversed"
                        -> new SortChecker(this).check(af, source, ctx);
        case "map"      -> new MapChecker(this).check(af, source, ctx);
        // ... all 30+ functions, one line each
        default -> compileUnknownFunction(af, funcName, source, ctx);
    };

    // Common: stamp TypeInfo
    types.put(af, info);
    return info;
}
```

Source-less functions (`getAll`, `match`, `if`, `eval`, `let`) simply ignore the
pre-compiled `source` and access `af.parameters()` directly. Pre-compiling
`PackageableElementPtr("Person")` or `CString("x")` is harmless (returns scalar).

#### ClassInstance dispatch — extract to checkers

```java
case ClassInstance ci -> switch (ci.type()) {
    case "instance"    -> new InstanceChecker(this).check(ci, ctx);
    case "relation"    -> new RelationAccessChecker(this).check(ci, ctx);
    case "tdsLiteral"  -> new TdsLiteralChecker(this).check(ci, ctx);
    default -> passthrough(ci);  // colSpec, colSpecArray, graphFetchTree
};
```

#### Delete `table()` and `class()` — fake functions
- `table('db.T')` → not a real Pure function. Use `#>{db.T}#` syntax
- `class('M::P')` → redundant with `Person.all()` — same mapping lookup
- Remove `compileTableAccess` method (~25 lines)
- Update any tests that use `table()` to use `#>{db.T}#` syntax

#### Fold `PureFunctionRegistry` into `ModelContext`

Today: user-defined functions live in a separate static
[PureFunctionRegistry](file:///Users/neema/legend/legend-lite/engine/src/main/java/com/gs/legend/model/PureFunctionRegistry.java)
(119 lines, 4 hardcoded PCT test helpers). User functions should be model elements
just like classes, mappings, stores.

- Move `registerPure()` / `getFunction()` into `ModelContext`
- 4 hardcoded test functions (lines 90-116) → defined in test Pure model strings
- `compileUnknownFunction()` checks `modelContext.findFunction(name)` first,
  then falls back to `BuiltinFunctionRegistry`
- Keep `inlineUserFunction()` mechanism (text substitution + re-parse)

#### Concrete changes
- [MODIFY] TypeChecker.java: delete 25 compileX wrappers → clean switch
- [NEW] InstanceChecker.java: extract from `compileInstanceLiteral` (96 lines)
- [NEW] RelationAccessChecker.java: extract from `compileRelationAccessor` (5 lines)
- [NEW] TdsLiteralChecker.java: extract from `compileTdsLiteral` (32 lines)
- [MODIFY] ModelContext: add `registerFunction()` / `findFunction()` from PureFunctionRegistry
- [DELETE] PureFunctionRegistry.java (119 lines → absorbed into ModelContext)
- [DELETE] `compileTableAccess` method (~25 lines)

**TypeChecker shrinks**: ~1,491 → ~1,000 lines.

**Estimate**: 2-3 days. Low risk — pure refactoring.

---

### Phase 1: Explicit Mapping Injection

**Goal**: Mapping enters the pipeline at ONE place, explicitly. No auto-discovery
hidden inside checkers.

#### The pattern

```java
// In TypeChecker (or top-level entry point):
Optional<ClassMapping> mapping = MappingScanner.findExactlyOne(registry, className);
// If 0: throw "No mapping found for class 'Person'"
// If >1: throw "Ambiguous: multiple mappings found for class 'Person': [M1, M2]"
// If 1: pass it explicitly

getAllChecker.check(af, source, ctx, mapping.get());
```

#### MappingScanner (new, ~30 lines)

```java
public class MappingScanner {
    /** Finds exactly one mapping for a class. Throws if 0 or >1. */
    public static ClassMapping findExactlyOne(MappingRegistry registry, String className) {
        var mappings = registry.findAllMappings(className);
        if (mappings.isEmpty())
            throw new PureCompileException("No mapping for class '" + className + "'");
        if (mappings.size() > 1)
            throw new PureCompileException("Ambiguous: " + mappings.size()
                + " mappings for '" + className + "': " + mappings);
        return mappings.get(0);
    }
}
```

For `^Person()` / `[^Person()]`: InstanceChecker builds an identity mapping and
passes it explicitly — same explicit pattern, different mapping source.

#### What changes
- [MODIFY] GetAllChecker: accept `ClassMapping` parameter instead of auto-discovering
- [NEW] MappingScanner.java: single-mapping discovery + validation
- [MODIFY] TypeChecker `compileGetAll` (or checker registry dispatch): call
  `MappingScanner.findExactlyOne()` before delegating to GetAllChecker
- [MODIFY] InstanceChecker: build identity mapping, pass explicitly

**Estimate**: 1-2 days. Low risk.

---

### Phase 2: Remove Mapping from TypeInfo + Build MappingResolver

**Goal**: TypeChecker stops propagating mapping. MappingResolver handles all mapping
resolution as a separate pass.

#### Remove mapping from 16 checkers

**10 PASSTHROUGH checkers** — delete `.mapping(source.mapping())` (1-line each):
> GroupBy, From, Aggregate, Extend, Distinct, Select, Rename, Slicing, Flatten, Pivot

**6 ACTIVE checkers** — delete `resolveAssociations` calls:
> Project, Filter, Sort, Map, Scalar, GetAll

**AbstractChecker** — delete `resolveAssociationsFromParams` (lines 939-953),
remove `ctx.withMapping()` from `bindLambdaParam` (lines 759, 769, 777).

**TypeInfo** — remove `mapping` field, `associations` field, `tableName()`,
`hasAssociations()`.

#### MappingResolver (new, ~200-300 lines)

Single-pass walker over typed AST. Produces `StoreResolution`:

```java
public record StoreResolution(
    Map<ValueSpecification, NodeResolution> nodes,
    Map<String, AssociationJoin> joins
) {
    sealed interface NodeResolution {
        record TableAccess(String table, String alias, RelationalMapping mapping)
            implements NodeResolution {}
        record ColumnRef(String table, String column, PropertyMapping pm)
            implements NodeResolution {}
        record AssociationNav(String joinName, String targetTable,
            String leftCol, String rightCol, boolean isToMany)
            implements NodeResolution {}
    }
}
```

#### PlanGenerator reads StoreResolution instead of TypeInfo.mapping()

```java
// Before: ClassMapping mapping = info.mapping();
// After:  var res = storeResolution.nodes().get(astNode);
```

**Estimate**: 5-7 days. Medium risk.

---

### Phase 3: Unify PlanGenerator to One Codepath

**Goal**: Delete `generateGetAll`. Both class and relation paths produce the same
output from MappingResolver. PlanGenerator has one path.

**Estimate**: 3-5 days.

---

### Phase 4: DynaFunction + Advanced Mapping Features

**Goal**: Support `fullName: concat(T.first, ' ', T.last)` in property mappings.

**Estimate**: 3-5 days. Independent — can start after Phase 1.

---

### Phase 5: Mapping as Relation DSL (Endgame)

**Goal**: `###Mapping` → syntactic sugar expanded by MappingResolver into Relation ops.

**Estimate**: 5-10 days. Depends on Phases 1-4.

---

## 4. Current Checker Architecture

### TypeChecker Breakdown (1,491 lines → ~1,050 after Phase 0)

| Section | Lines | After Phase 0 |
|---------|-------|---------------|
| Core infra (compileExpr switch, dispatch) | 170 | 50 (registry) |
| 25 boilerplate wrappers | 125 | 0 (deleted) |
| compileTableAccess | 25 | 0 (deleted) |
| compileInstanceLiteral | 96 | 0 (→ InstanceChecker) |
| compileTdsLiteral | 32 | 0 (→ TdsLiteralChecker) |
| compileRelationAccessor | 5 | 0 (→ RelationAccessChecker) |
| compileRegistryOrUserFunction + inline | 60 | 60 |
| compileLambda | 58 | 58 |
| compileVariable | 31 | 31 |
| compileProperty | 155 | 155 |
| compileCollection | 23 | 23 |
| Association resolution (→ Phase 2) | 80 | 80 (moves in Phase 2) |
| CompilationContext record | 60 | 40 (remove mapping) |
| Type helpers + registration | 115 | 115 |
| Comments/imports/blank | 456 | ~440 |

### Checker Inventory (33 files, 4,727 lines)

- **AbstractChecker** (1,088 lines): shared infra (overload resolution, lambda compilation, bindings)
- **32 concrete checkers** (3,639 lines): function-specific type validation

---

## 5. Timeline

```
Phase 0 ─── Checker registry + delete fake functions + extract InstanceChecker etc.
│            2-3 days · LOW RISK
▼
Phase 1 ─── Explicit mapping injection + MappingScanner
│            1-2 days · LOW RISK
▼
Phase 2 ─── MappingResolver + remove mapping from TypeInfo/checkers
│            5-7 days · MEDIUM RISK
▼
Phase 3 ─── Unify PlanGenerator codepaths
│            3-5 days · MEDIUM RISK
├──────────────────────┐
▼                      ▼
Phase 4 (parallel) ── DynaFunction transforms
│                      3-5 days
▼
Phase 5 ─── Mapping as Relation DSL
             5-10 days
```

**Total: ~20-32 days**

> [!IMPORTANT]
> No one-way doors. Every phase is reversible. The architecture is proven by
> legend-engine's 10+ years of production use.
