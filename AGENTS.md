# Legend Lite — Architectural Invariants

> **This file is read by AI coding assistants. Follow these rules strictly.**

## Pipeline

```
Parser → Compiler → PlanGenerator → Dialect → Execute
```

## Invariants

### 1. Compiler Does ALL AST Walking and Typing

- The Compiler (`CleanCompiler`) is the **single source of truth** for types
- It MUST produce TypeInfo for **EVERYTHING** — every expression, every function call
- It MUST resolve all variant access patterns, cast targets, list types, etc.
- If TypeInfo is missing for something, **the Compiler has a bug — fix the Compiler**

### 2. PlanGenerator Does No Type Inference

PlanGenerator reads the **annotated AST** — structure from AST nodes, types and resolved metadata from TypeInfo sidecar.

PlanGenerator MUST NOT:
- Infer or resolve types (no model lookups, no type compatibility checks)
- Validate correctness (that's the Compiler's job)
- Inspect AST node types for **type** dispatch (no `instanceof CInteger` to decide SQL type)
- Parse function names to guess types (no `funcName.contains("Int")`)
- Contain any SQL dialect-specific code (no DuckDB, JSON, INTEGER, BIGINT, VARCHAR, TIMESTAMP_NS)
- Hardcode SQL type strings — use Pure type names; the Dialect maps them

PlanGenerator MAY:
- Read AST structure: function names, parameters, lambda bodies, ColSpec names, nesting
- Read TypeInfo annotations: expression types, store resolutions, sort specs
- Pattern-match on AST node kinds for **structural** dispatch (e.g., `AppliedFunction` → `generateFunction`, lambda body → compile body)

PlanGen emits `SqlExpr` nodes — never raw SQL strings.

### 3. Dialect Owns All SQL Rendering

- The Dialect (`SQLDialect`) owns **all** SQL-specific decisions
- Type name mapping: `sqlTypeName("Integer")` → `BIGINT`
- Function name mapping: `renderFunction("dateDiff", args)` → `DATE_DIFF(...)`
- Dialect-specific decompositions (WEEK diff, lpad safe, timeBucket) go **HERE**, not in PlanGen

### 4. NO FALLBACKS. NO DEFAULTING.

- The **WHOLE POINT** of the Compiler is to catch mistakes early
- If a type is unknown, **FAIL** — do not guess, do not default, do not fall back
- If TypeInfo is missing, **fix the Compiler** — do not work around it in PlanGen
- Every defaulting branch is a bug hiding behind a safety net

### 5. Lazy Loading of User Packageable Elements

Cross-project dependencies must not force-load the whole transitive graph. When project A references a class in project B, compiling A must not require loading every class in B. **`NameRef`, `Type.ClassType(fqn)`, `Type.EnumType(fqn)`, and `PureClass.superClassFqns` exist specifically to carry FQN references without forcing the target to load.**

**Split by origin:**

- **Platform types** — `LClass`, `Primitive`, platform enums, everything declared in `BuiltinClassRegistry`. Always loaded at JVM start (bootstrap). **Safe to eagerly classify and dispatch.** `NameResolver` promoting `NameRef("Relation")` → `LClass.RELATION` is correct because `LClass` is always in scope.
- **User types** — user-declared classes, enums, functions. Referenced by FQN. **Must not be eagerly dereferenced into resolved Java objects.** A `Type.ClassType("my::app::Person")` is just an FQN string in a typed wrapper; it does NOT imply `my::app::Person` is loaded.

**The lazy contract:**

- Structural access (`findProperty`, `allProperties`, `isClassSubtype`, superclass-chain walks) MUST go through `ModelContext.findClass(fqn)` / `findEnum(fqn)` / `findFunction(fqn)`. These methods are the sole layer that owns load triggering.
- Long-lived fields hold **FQN strings** or FQN-wrapping types (`ClassType`, `EnumType`, `NameRef`), never resolved `PureClass` / `PureEnum` / `FunctionDefinition` references. `PureClass.superClassFqns: List<String>` is the canonical example.
- Classification (`NameRef` → `ClassType` / `LClass` / `Primitive`) is a controlled boundary at property-build time (`PureModelBuilder.substituteTypeVarsAndClassify`) and at expression type-checking. Outside those boundaries, keep `NameRef` unresolved.

**Never:**

- Store `PureClass` / `PureEnum` / `FunctionDefinition` as fields of another `PureClass`, `PropertyBuilder`, or long-lived container. Store the FQN and resolve lazily through `ModelContext`.
- Walk a dependency graph manually (e.g., collect all classes via field access). Go through `ModelContext.findClass(fqn)` every step.
- Mix platform and user handling in an eager-resolution path that always loads the target. Guard platform-only promotion with `LClass.findByFqn(fqn).isPresent()` or equivalent so user FQNs fall through unchanged.
- Force user-type load just to dispatch on shape (e.g., "is this a list?"). Dispatch on the `Type` identity (`LClass`, `Primitive`, `ClassType.qualifiedName()`), not on the resolved object.

**When you genuinely need a user class's structure**, call `modelContext.findClass(fqn)` at the exact use site and let the context decide whether to lazy-load. Don't cache the result across unrelated operations.

## SqlExpr Rules

- `Cast(expr, pureTypeName)` — Pure type name, Dialect maps via `sqlTypeName()`
- `FunctionCall(name, args)` — semantic name, Dialect maps via `renderFunction()`
- `VariantTextExtract` — not `JsonAccess` (dialect-agnostic naming)
- `ToVariant` — for `toVariant()` Pure function; Dialect renders to `CAST AS JSON` or `VARIANT`
- NO `RawCast` — removed because it bypassed dialect mapping

## Common Mistakes (Don't Repeat)

1. **`(int) longValue`** — Java boxed `Long` can't raw-cast to `int`. Use `.intValue()`
2. **Hardcoding SQL types in PlanGen** — Use Pure type names; Dialect maps them
3. **Moving dialect logic into PlanGen** — put it in the Dialect, always
4. **Adding normalizations instead of fixing root cause** — fix the actual generation
5. **Adding fallbacks/defaults** — fail loudly; fix the Compiler instead
6. **Fixing fallbacks by changing the default** — if you find a fallback/default branch being hit, do NOT change what it defaults to. Instead: (a) make it throw, (b) find why the Compiler produced null/missing TypeInfo, (c) fix the Compiler. The fallback existing at all is the bug.
7. **Doing type inference in PlanGenerator** — PlanGenerator may read AST for structure (property names, function names, lambda bodies) but must NEVER infer or resolve types. If PlanGenerator needs a **type** (e.g., "is this a list?", "what class is this?"), it must come from TypeInfo. Structural information (e.g., ColSpec names, nesting) can be read directly from the AST.
8. **Making the Compiler lenient on missing model elements** — the Compiler MUST throw if a referenced class, property, or type is not found in the model context. This is literally the compiler's job. If a test fails because a class isn't found, **fix the test to set up the model correctly** (add the class to `commonClassDefs()` or `TypeEnvironment`). NEVER make the Compiler silently degrade (e.g., fall back to `ANY`, skip compilation). Silent degradation hides bugs and causes downstream issues (e.g., PlanGenerator routing `contains` to `STRPOS` because the struct array wasn't tagged as a list).
