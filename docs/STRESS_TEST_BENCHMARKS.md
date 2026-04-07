# Stress Test Benchmarks

Measured on MacOS, Apple Silicon, JDK 21, single-threaded.
Commit: `f74cbef` (strict FQN resolution).

## Test Classes

All stress tests live in `engine/src/test/java/com/gs/legend/test/`:

| Test | Model | Queries | Focus |
|---|---|---|---|
| `StressTest.java` | 1K hub-spoke | 100 | Baseline scaling |
| `StressTest10K.java` | 10K hub-spoke | 100 | Medium model |
| `StressTest100K.java` | 100K hub-spoke | 100 | Large model scaling |
| `StressTestDense.java` | 10K dense (~10 links/hub) | 100 | Connectivity density |
| `StressTestComplexQueries.java` | 1K dense | 10 | Per-query phase breakdown |
| `StressTestChaotic.java` ⚠️ | 100K non-uniform (1-50 props, all types) | 200 | Heterogeneity at scale |

⚠️ `StressTestChaotic` is tagged `@Tag("heavy")` and excluded from the default test suite.
It requires `-Xmx16g` due to ANTLR parse tree size for wide classes.

Reproduce (standard suite):
```bash
mvn test -pl engine -Dtest="StressTest,StressTest10K,StressTest100K,StressTestDense,StressTestComplexQueries"
```

Reproduce (chaotic only):
```bash
mvn test -pl engine -Dtest="StressTestChaotic" -DargLine="-Xmx16g"
```

## Model Size Scaling

Same 10 query patterns (simple project, filter, sort, 1-hop, 2-hop association navigation).

### Cold (separate JVM per test)

| Phase | 1K | 10K | 100K | Scaling |
|---|---|---|---|---|
| Generate source | 1 ms | 10 ms | 55 ms | Linear |
| Parse + build | 228 ms | 818 ms | 5,246 ms | ~Linear |
| Normalize | 12 ms | 69 ms | 484 ms | ~Linear |
| 100 queries | 85 ms | 78 ms | 113 ms | **Flat (O(1))** |
| **Total** | **408 ms** | **1,073 ms** | **5,905 ms** | ~Linear |

### Hot (all tests in same JVM, JIT-warmed)

| Phase | 1K | 10K | 100K | Scaling |
|---|---|---|---|---|
| Generate source | 0 ms | 9 ms | 74 ms | Linear |
| Parse + build | 29 ms | 474 ms | 5,525 ms | ~Linear |
| Normalize | 1 ms | 40 ms | 414 ms | ~Linear |
| 100 queries | 21 ms | 28 ms | 26 ms | **Flat (O(1))** |
| **Total** | **55 ms** | **555 ms** | **6,041 ms** | ~Linear |

Queries are constant-time regardless of model size — demand-driven compilation means
only the classes a query touches are compiled and resolved.

## Density Scaling (10K classes)

Same queries, varying hub-to-hub connectivity.

### Cold

| Phase | Sparse (~2 per hub) | Dense (~10 per hub) |
|---|---|---|
| Associations | 10,100 | 19,000 |
| Parse + build | 818 ms | 966 ms |
| Normalize | 69 ms | 72 ms |
| 100 queries | 78 ms | 91 ms |
| **Total** | **1,073 ms** | **1,237 ms** |

### Hot

| Phase | Sparse (~2 per hub) | Dense (~10 per hub) |
|---|---|---|
| Parse + build | 474 ms | 748 ms |
| Normalize | 40 ms | 54 ms |
| 100 queries | 28 ms | 53 ms |
| **Total** | **555 ms** | **882 ms** |

Density adds moderate overhead. Per-query cost stays under 1ms hot.

## Query Complexity (dense 1K model, ~10 connections per hub)

| Query Pattern | Parse | TypeCheck | Resolve | PlanGen | Total |
|---|---|---|---|---|---|
| Simple baseline (3 cols) | 504us | 2,157us | 65us | 84us | ~3ms |
| 2-hop ring (H0→H1→H2) | 525us | 4,769us | 209us | 132us | ~6ms |
| 3-hop ring (H0→H1→H2→H3) | 339us | 5,711us | 233us | 121us | ~6ms |
| 2-hop mixed (skip+ring) | 312us | 10,302us | 369us | 147us | ~11ms |
| Fan-out 5 assocs | 7,384us | 95,334us | 3,791us | 7,744us | ~114ms |
| Fan-out 8 assocs | 1,527us | 22,484us | 1,099us | 518us | ~26ms |
| Assoc + filter | 1,495us | 5,032us | 169us | 6,119us | ~13ms |
| Assoc + sort | 819us | 5,363us | 138us | 654us | ~7ms |
| Assoc + filter + sort + limit | 1,071us | 7,876us | 189us | 191us | ~9ms |
| Sat→Hub→Hub (2-hop) | 364us | 26,877us | 160us | 294us | ~28ms |

All query patterns under 120ms. TypeChecker dominates for fan-out queries
(compiling source relations for each target class).

## Chaotic 100K (non-uniform classes)

100K classes with 1-50 properties each, all 7 Pure types (String, Integer, Boolean,
Date, DateTime, Float, Decimal), enum mappings, DynaFunc concat, mapping filters,
random association graph.

| Metric | Value |
|---|---|
| Classes | 100,000 (40K tiny, 25K small, 20K med, 10K large, 5K huge) |
| Property range | 1–50 per class |
| Pure types | String, Integer, Boolean, Date, DateTime, Float, Decimal |
| Enum classes | 4,362 |
| Filtered classes | 8,309 |
| Concat DynaFunc | 2,431 |
| Associations | 100,000 |
| Pure source | 115 MB |
| Parse + build | 13,531 ms |
| Normalize | 529 ms |
| 200 queries | 175 ms |
| **Total** | **14.7 s** |
| Heap required | 16 GB |

16 query patterns: simple projects, all-column projects, filter on Integer/Boolean/Float,
1-hop and 2-hop associations, DynaFunc computed columns, sorts, limits, slices,
assoc+filter combos, Date column projects.

Queries remain O(1) — 200 queries in 175ms regardless of 100K model size.

## Key Optimizations

1. **Demand-driven compilation** (TypeChecker pass 2): only compile source relations
   for classes the query navigates — not the entire model graph.

2. **Demand-driven resolution** (MappingResolver): only resolve association joins
   the query actually uses — skip all others.

3. **Association index** (PureModelBuilder): `className → List<Association>` index
   for O(1) lookup instead of O(N) scan.

4. **SymbolTable int-keyed maps**: all class/mapping/association lookups use integer IDs
   instead of string keys — eliminates hash collisions at scale.
