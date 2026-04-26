# Query-Demand Pruning — Design Note

Status: proposed (follow-up to Phase C.1, commit `38c6773`).

## Problem

`TypeChecker` exposes two maps via `CompiledDependencies`:

```java
Map<String, Set<String>> classPropertyAccesses;   // class -> properties accessed
Map<String, Set<String>> associationNavigations;  // class -> assoc props navigated
```

`MappingResolver.stampExtendOverrideIfNeeded` reads them as proxies for
"what does the query need?" and prunes synth-body extends accordingly.
This is correct for relational mappings (whose bodies use physical column
refs that don't fire `resolvePropertyOnClass`) but breaks for M2M.

### Concrete failure

```
StaffWithDept.all()->graphFetch(#{ StaffWithDept { fullName } }#)
```

Where `StaffWithDept` is M2M-mapped from `Employee`, with property bodies:

- `fullName: src | $src.firstName + ' ' + $src.lastName`
- `department: src | $src.dept.name`

`compileMappingFunctionFor("StaffWithDept")` walks the WHOLE mapping
function body — both ColSpec lambdas. `department`'s lambda navigates
`$src.dept`, stamping `associationNavigations[Employee] += dept` even
though the query graph-fetches only `fullName`. Pruner reads the
polluted map and keeps the JOIN.

12 tests in `M2MChainIntegrationTest` and
`RelationalMappingCompositionTest` exhibit this.

## Root cause

`associationNavigations` (and `classPropertyAccesses`) conflate two
concerns:

1. **Static deps per `PackageableElement`** — Bazel-style: "what does
   this class/function/mapping body reference?" Used for incremental
   builds, IDE navigation, etc. Wants exhaustive closure.
2. **Per-query demand** — "what does THIS query at runtime need?"
   Used by `MappingResolver` pruning. Wants demand-driven scope.

Today both are aggregated into the same shared mutable maps on
`TypeChecker`. Every `check(...)` call appends. The result is a
type-check-coverage view, not a query-demand view.

## Why pruning needs demand, not type-check coverage

Pruning has to follow demand transitively — what the query needs,
INCLUDING what every function/mapping it invokes needs — but ONLY the
parts actually invoked.

For a user function with 12 extends where the query reads 1 col, only
the extend producing that col should be retained. Mapping function with
N ColSpecs where graph-fetch picks 1 is the same shape: only that
ColSpec's body contributes to demand.

Type-check walk is exhaustive: every body's stamps are accumulated.
Query demand is selective: only invoked ColSpec / lambda bodies count.

## Proposed redesign

### Split the producer side

```java
class CompiledClass     extends CompiledElement { StaticDeps staticDeps; }
class CompiledFunction  extends CompiledElement { StaticDeps staticDeps; }
class CompiledMapping   extends CompiledElement {
    Map<String, StaticDeps> perPropertyDeps;          // per-ColSpec
    StaticDeps staticDeps() { return ⋃ perPropertyDeps.values(); }
}
class CompiledExpression {  // ValueSpec
    StaticDeps staticDeps;       // direct refs from the query body
    DemandDeps demandDeps;       // transitive query-demand closure
}
```

- `check(PackageableElement)` returns a `CompiledX` carrying its OWN
  static deps. No global aggregation.
- `check(ValueSpec)` returns a `CompiledExpression` whose `demandDeps`
  is computed via a demand-driven worklist (see below).

### Compile mapping functions per-ColSpec

To make per-property deps storable, `compileMappingFunctionFor` walks
each ColSpec lambda independently, capturing its own deps. Each
ColSpec's deps land in `perPropertyDeps[colName]`.

This requires distinguishing "compiling a mapping ColSpec lambda" from
generic lambda compilation — passed via `CompilationContext`
(`Optional<String> owningMappingClass`) set by `compileMappingFunctionFor`
at entry.

### Two pass-2 closures sharing one skeleton

Today's `compileNeededAssociationTargets` is a worklist closure over
the assoc graph driven by globally aggregated stamps. Refactor it into
a reusable closure routine with two invocations:

- **Element pass-2** — runs inside `check(PackageableElement)`. Seeded
  with the element body's direct refs. Closes exhaustively over the
  reachable graph. Output: `CompiledX.staticDeps`.
- **Query pass-2** — runs inside `check(ValueSpec)`. Seeded with the
  query AST's direct stamps. Closes over query demand: when seeing
  `(class, prop)` for a mapped class, pull
  `CompiledMapping.perPropertyDeps[prop]` and recurse with its stamps.
  When seeing a `TypedUserCall(fn)`, pull `CompiledFunction.staticDeps`
  (whole-body, since user-fn calls invoke whole body). Output:
  `CompiledExpression.demandDeps`.

### Consumer side

- **Bazel / build / IDE**: walks compiled forest, unions `staticDeps`.
  Same answer as today, just stored per-element.
- **`MappingResolver` pruning**: reads `compiledExpression.demandDeps`.
  No global map consultation. M2M pollution gone by construction.

## Correctness sketch

### M2M case

Query `StaffWithDept.all()->graphFetch(#{StaffWithDept{fullName}}#)`:

1. Query AST direct stamps: `(StaffWithDept, fullName)` from graph-fetch.
2. Query pass-2 worklist:
   - `(StaffWithDept, fullName)` → look up
     `compiledMapping[StaffWithDept].perPropertyDeps[fullName]` =
     `{ (Employee, firstName), (Employee, lastName) }`. Add to worklist.
   - `(Employee, firstName)` → Employee is relationally mapped,
     `perPropertyDeps[firstName]` = `{}` (terminal column ref). Done.
   - `(Employee, lastName)` → similar. Done.
3. `demandDeps` = `{ (StaffWithDept, fullName), (Employee, firstName),
   (Employee, lastName) }`.
4. Pruning Employee's source spec: `dept` is NOT in demand → cancelled.
   `firstName`/`lastName` ARE in demand → kept. SQL has no spurious
   JOIN to `T_DEPARTMENT`.

### User function with N extends, query reads 1 col

Query `Person.all()->project(~name | f($row).colA)` where `f` returns a
relation with cols A, B, C, ..., N.

1. Direct stamps: `(Person, *fields used by f's args)`, plus user-call
   to `f`.
2. Query pass-2:
   - `f` user-call → pull `compiledFunction[f].staticDeps` (whole-body
     deps because user-fn calls invoke the whole body).
3. `demandDeps` includes everything `f`'s body touches (it must, because
   when `f` runs at SQL time, all its extends materialize).

For partial pruning of `f`'s body based on which output cols downstream
reads, that's a deeper optimization (column-projection pushdown). The
proposed redesign doesn't deliver it but doesn't preclude it.

## Migration plan

1. **Land per-element static deps** — add `staticDeps` fields to
   `CompiledClass`/`CompiledFunction`/`CompiledMapping`. Populate from
   per-`check(PackageableElement)` walks. Don't remove globals yet.
2. **Per-ColSpec mapping deps** — refactor mapping function compilation
   to walk ColSpec lambdas independently. Populate
   `perPropertyDeps`. Don't remove globals yet.
3. **Demand-driven query closure** — add `demandDeps` to
   `CompiledExpression`. Populate via the new query pass-2.
4. **Switch consumers**:
   - Bazel readers: `staticDeps` (per-element union).
   - `MappingResolver.stampExtendOverrideIfNeeded`: `demandDeps` from
     the query's `CompiledExpression`.
5. **Remove globals**: delete `classPropertyAccesses` /
   `associationNavigations` instance fields and their leak through
   `CompiledDependencies`. Keep only what TypeChecker needs internally
   for compile-loop control (or eliminate that too if no longer used).

## Test target

12 tests currently failing under Phase C.1:

- `M2MChainIntegrationTest$DeepFetch.testDeepFetchRootOnlyNoJoin`
- `M2MChainIntegrationTest$DeepFetch.testDisjointRootOnly`
- `M2MChainIntegrationTest$JoinChainTests.testGraphFetchSingleHopTraverse`
- `M2MChainIntegrationTest$L1SingleHop.testGraphFetch`
- `M2MChainIntegrationTest$L1SingleHop.testGraphFetchWithFilter`
- `M2MChainIntegrationTest$L2TwoHop.testGraphFetchTwoHop`
- `M2MChainIntegrationTest$L2TwoHop.testGraphFetchTwoHopWithFilter`
- `M2MChainIntegrationTest$L3ThreeHop.testGraphFetchThreeHop`
- `M2MChainIntegrationTest$L4FourHop.testGraphFetchFourHop`
- `RelationalMappingCompositionTest$FullCombo.filterAssocProjectChain`
- `RelationalMappingCompositionTest$FullCombo.filterChainAndAssoc`
- `RelationApiIntegrationTest$CombinedOperationsTests.testComplexNavigation`

Should all pass once the redesign lands.

## Notes / open questions

- User-function partial-extend pruning (the "12-extend user fn, query
  reads 1 col" case) is column-projection pushdown — out of scope here
  but enabled by the same per-property storage if user functions also
  store per-output-col deps.
- TypeChecker's pass-2 worklist (`compileNeededAssociationTargets`)
  currently serves both type-check coverage AND mapping-function
  compile triggering. Both responsibilities can be served by the
  exhaustive element pass-2 in the redesign.
- Memo behavior must be preserved: each `CompiledX` is computed once
  per element FQN; demand-deps are per-query (not memoizable across
  queries).
