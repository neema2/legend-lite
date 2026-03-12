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

### 2. PlanGenerator Does NO AST Walking or Typing

- PlanGen MUST NOT inspect AST node types for type dispatch (no `instanceof CInteger` to decide behavior)
- PlanGen MUST NOT parse function names to guess types (no `funcName.contains("Int")`)
- PlanGen MUST NOT contain any SQL dialect-specific code (no DuckDB, JSON, INTEGER, BIGINT, VARCHAR, TIMESTAMP_NS)
- PlanGen MUST NOT hardcode SQL type strings — use Pure type names; the Dialect maps them
- PlanGen reads TypeInfo from `CompilationUnit` — that is the ONLY source of type knowledge
- If PlanGen needs type queries, **add utility methods to TypeInfo** (e.g., `isList()`, `isString()`, `isDate()`) — never leak type or AST work into PlanGen
- PlanGen emits `SqlExpr` nodes — never raw SQL strings

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
