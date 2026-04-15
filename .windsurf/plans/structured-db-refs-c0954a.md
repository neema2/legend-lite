# Structured DB Refs: Eliminate FQN String Concatenation

## Problem

Database-scoped elements (tables, joins, filters, views) are modeled with separate `(db, name)` components at the source (`JoinChainElement`, `Table`, `Join`, `Filter`), but get collapsed into concatenated `"db.name"` strings at API boundaries. Downstream code then either:
- Passes the concatenated string to 1-arg lookup methods (`findJoin(fqn)`, `findTable(fqn)`) — no compile-time guarantee that db was provided
- Has to split the string back apart (MappingResolver line 727: extracts bare table name from FQN)

This violates the NO FALLBACKS principle: a caller can pass a bare name to `findTable("T_PERSON")` and get silent null instead of a compile error.

## Root Cause

`PropertyMapping.joinChain` is `List<String>` instead of a structured type. The `tableReference()` AST function takes a single `CString(fqn)` instead of 2 args. These force all producers to concatenate and all consumers to accept concatenated strings.

## Concatenation Sites (producers)

| # | File | Line | Pattern | What it produces |
|---|---|---|---|---|
| P1 | `PureModelBuilder.qualifyJoinNames` | 603 | `db + "." + jce.joinName()` | `PropertyMapping.joinChain` entries |
| P2 | `MappingNormalizer.resolvePropertyMappingsThroughView` | 632,662,679 | `jce.databaseName() + "." + jce.joinName()` | Same — view-resolved join chains |
| P3 | `MappingNormalizer.synthesizeSourceSpec` | 308 | `rm.table().qualifiedName()` | `CString` arg to `tableReference()` |
| P4 | `MappingNormalizer.buildTraverseChain` | 878 | `join.db() + "." + targetTable` | `CString` arg to `tableReference()` |
| P5 | `PureModelBuilder.resolveJoinByChainElement` | 613 | `db + "." + jce.joinName()` | SymbolTable key for `resolveId` |
| P6 | `PureModelBuilder.resolveJoinFromReference` | 625 | `db + "." + firstHop.joinName()` | Same |
| P7 | `PureModelBuilder.addMapping` (filter) | 825 | `f.databaseName() + "." + f.filterName()` | `filterFqn` string |

## Consumption Sites (consumers of concatenated strings)

| # | File | Line | What it receives |
|---|---|---|---|
| C1 | `MappingNormalizer.buildTraverseChain` | 858 | `model.findJoin(joinName)` — FQN from `pm.joinChain()` |
| C2 | `TableReferenceChecker.resolveTable` | 42 | `modelCtx.findTable(tableRef)` — FQN from `CString` |
| C3 | `MappingResolver` | 727 | Splits FQN back: `fqn.substring(fqn.indexOf('.') + 1)` |
| C4 | `MappingNormalizer.addTraverseExtends` | 455 | Groups PMs by `pm.joinChain()` (uses `List<String>` equality) |

## Fix: 3 Phases

### Phase 1: `JoinRef` record + `PropertyMapping` refactor

New record in `com.gs.legend.model.store`:
```java
public record JoinRef(String db, String name) {
    public JoinRef {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(name, "name");
    }
}
```

Changes:
- `PropertyMapping.joinChain`: `List<String>` → `List<JoinRef>`
- `PropertyMapping.multiJoinChains`: `List<List<String>>` → `List<List<JoinRef>>`
- `PropertyMapping.joinChain(...)`, `dynaFunctionWithJoin(...)`, `dynaFunctionWithMultiJoin(...)` factory methods: accept `List<JoinRef>`
- `qualifyJoinNames` → returns `List<JoinRef>` (no concatenation — wraps `JoinChainElement` into `JoinRef`)
- `resolvePropertyMappingsThroughView` (3 sites) → same
- `MappingNormalizer.addTraverseExtends` grouping (C4): groups by `List<JoinRef>` equality (record equals works)
- `MappingNormalizer.buildTraverseChain`: signature changes to `List<JoinRef>`, calls `model.findJoin(jr.db(), jr.name())`

**Estimated edits**: ~20

### Phase 2: `findJoin` / `findTable` → 2-arg

- `PureModelBuilder.findJoin(String db, String name)` — replaces 1-arg
- `PureModelBuilder.findTable(String db, String name)` — replaces 1-arg
- `ModelContext.findTable(String db, String name)` — interface change
- `MappingNormalizer` ModelContext adapter (line 59): delegate 2-arg
- `TypeCheckerTest` stub: update to 2-arg
- Delete 1-arg `findJoin`, `findTable` from `PureModelBuilder`
- Internal helpers (`resolveJoinByChainElement`, `resolveJoinFromReference`): call 2-arg directly with `(jce.databaseName(), jce.joinName())` — no concatenation

**Estimated edits**: ~15

### Phase 3: `tableReference()` → 2-arg AST

Change `tableReference` function from 1 arg (concatenated FQN) to 2 args (db, name):
```java
// Before:
new AppliedFunction("tableReference", List.of(new CString("store::DB.T")), false)
// After:
new AppliedFunction("tableReference", List.of(new CString("store::DB"), new CString("T")), false)
```

Changes:
- `MappingNormalizer.synthesizeSourceSpec` (P3): 2 `CString` args
- `MappingNormalizer.buildTraverseChain` (P4): 2 `CString` args from `join.db()` and `targetTable`
- `TableReferenceChecker`: reads `args.get(0)` as db, `args.get(1)` as name, calls `findTable(db, name)`
- `MappingResolver` (C3): reads both args — no more FQN splitting
- `ExtendChecker`: `resolvedTableName` in TypeInfo stays as-is (it's the bare SQL name for alias generation)
- `TypeInfo.resolvedTableName` semantics: currently stores the FQN. Evaluate whether it should store bare name or structured. Likely keep as bare SQL name (used for SQL alias, not lookup).

**Estimated edits**: ~10

### Phase 4: Cleanup

- Delete `qualifiedName()` methods on `Table`, `Join`, `View`, `Filter` — or keep only for debugging/logging (not for lookups)
- Verify no remaining `db + "." + name` concatenation for lookup purposes
- `PureModelBuilder.getTable(String db, String name)` / `getJoin(String db, String name)` — already done
- Delete `MappingRegistry.findJoin(String)` — dead code per plan

## Execution Order

Phase 1 → Phase 2 → Phase 3 → Phase 4. Each phase is independently testable (run full suite after each).

## Verification

`mvn test -pl engine` — target: 2412 tests, 0 failures, 0 errors, 19 skipped.
