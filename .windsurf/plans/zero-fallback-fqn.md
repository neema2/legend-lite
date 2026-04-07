# Zero-Fallback FQN Plan

## Problem
`SymbolTable.resolveId()` has a 2-step lookup: try FQN, fall back to simple name.
This fallback is a silent correctness bug hiding behind a safety net:
- Two classes with the same simple name → first-registered wins, no error
- Callers don't know if they got the right class
- Violates AGENTS.md: NO FALLBACKS. NO DEFAULTING.

## Current State (after Phase 2 fix)
- All 10 `ClassType` creation sites produce FQN ✅
- All TypeChecker/MappingResolver side-tables keyed by FQN ✅
- All PureModelBuilder internal maps keyed by integer ID ✅
- `resolveId` simple-name fallback still exists but never hit in practice

## Design: Two Tiers of Resolution

### Tier 1: Build-time (Parser → PureModelBuilder)
Parser output contains simple names from user source (e.g., `firm: Firm[1]`).
PureModelBuilder resolves these to FQN **at registration time** via `resolveToFqn()`.
This is the **only** place simple-name-to-FQN resolution should happen.

Add **ambiguity detection**: if `simpleToAllFqns.get(name).size() > 1`, throw:
```
Ambiguous class reference 'Person' — matches: model::Person, hr::Person. 
Use fully qualified name or add import.
```

### Tier 2: Runtime (Compiler, MappingResolver, PlanGenerator)
Everything post-build is FQN-only. Split `resolveId` into two methods:

```java
// Strict FQN-only lookup — used by findClass, findMappingExpression, etc.
public int resolveIdStrict(String fqn) {
    Integer id = fqnToId.get(fqn);
    if (id == null) return -1;  // not found (may be primitive, etc.)
    return id;
}

// Build-time lookup — resolves simple names via simpleToId (with ambiguity check)
public int resolveId(String name) {
    Integer id = fqnToId.get(name);
    if (id != null) return id;
    // Simple name fallback — only valid at build time
    id = simpleToId.get(name);
    if (id != null) {
        List<String> allFqns = simpleToAllFqns.get(name);
        if (allFqns != null && allFqns.size() > 1) {
            throw new PureCompileException(
                "Ambiguous reference '" + name + "' — matches: " + allFqns);
        }
        return id;
    }
    return -1;
}
```

### Tier 3: Import-aware resolution (Phase 3, future)
Replace `simpleToId` fallback with proper import resolution:
```java
public int resolveWithImports(String name, List<String> imports) {
    // 1. Already FQN? Direct lookup
    Integer id = fqnToId.get(name);
    if (id != null) return id;
    // 2. Try each import prefix
    for (String imp : imports) {
        id = fqnToId.get(imp + "::" + name);
        if (id != null) return id;
    }
    return -1;
}
```

## Steps

### Step 1: Add `resolveIdStrict` to SymbolTable
- New method: FQN-only, single `fqnToId.get()`
- Add ambiguity check to existing `resolveId` (throw if >1 FQN for simple name)
- No callers changed yet — just adding the new method

### Step 2: Migrate runtime callers to `resolveIdStrict`
Switch all post-build lookups to `resolveIdStrict`:
- `PureModelBuilder.findClass()`, `findEnum()`, `hasEnumValue()`
- `PureModelBuilder.findAssociationByProperty()`, `findAllAssociationNavigations*()`
- `PureModelBuilder.findAllPropertyJoins()`
- `PureModelBuilder.resolveType()` 
- `PureModelBuilder.getClass()`, `getEnum()`, `getAssociation()`, `getService()`
- `PureModelBuilder.resolveMappingNames()`, `resolveConnection()`, `resolveDialect()`
- `NormalizedMapping.findClassMapping()`, `findMappingExpression()`, `findSourceRelation()`
- `MappingRegistry.findPureClassMapping()`
- `MappingNormalizer` — targetClassName, sourceClassName, nav.targetClassName lookups

### Step 3: Audit build-time callers that still use `resolveId`
These are legitimate build-time resolution sites where parser names may be simple:
- `PureModelBuilder.resolveSuperclasses()` — `symbols.resolveId(superClassName)`
- `PureModelBuilder.resolveDatabaseIncludes()` — `symbols.resolveId(includedPath)`
- `PureModelBuilder.addRelationalClassMappings()` — `symbols.resolveId(classMapping.className())`
- `PureModelBuilder.resolveToFqn()` — `symbols.resolve(name)`

Verify each gets simple names from the parser. These stay on `resolveId` (with ambiguity check).

### Step 4: Run full test suite, verify 0 failures
All 2125 tests should pass. Any failure = a caller was passing simple name at runtime.

### Step 5: Delete `resolveToFqn` fallback callers that are now redundant
After Step 2, the `resolveToFqn()` calls in `findAssociationByProperty`, 
`findAllPropertyJoins`, etc. are redundant (input is already FQN). Remove them.

## Validation
- 2125 tests, 0 failures
- `resolveIdStrict` has 0 fallback paths
- `resolveId` has ambiguity check (throws on collision)
- grep confirms: no `resolveId` calls outside SymbolTable + PureModelBuilder build methods
