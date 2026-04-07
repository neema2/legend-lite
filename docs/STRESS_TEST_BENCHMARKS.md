# Stress Test Benchmarks

Measured on MacOS, JDK 21, single-threaded. Times include JIT warmup (first query is always slower).

## Model Size Scaling

Same 10 query patterns (simple project, filter, sort, 1-hop, 2-hop association navigation).

| Phase | 1K classes | 10K classes | 100K classes | Scaling |
|---|---|---|---|---|
| Pure source | 600 KB | 6 MB | 65 MB | Linear |
| Parse + build | 211ms | 814ms | 5,146ms | ~Linear |
| Normalize | 23ms | 128ms | 921ms | ~Linear |
| 100 queries | 155ms | 343ms | 347ms | **Flat (O(1))** |
| **Total** | **550ms** | **1,402ms** | **6,662ms** | ~Linear |

Queries are constant-time regardless of model size — demand-driven compilation means
only the classes a query touches are compiled and resolved.

## Density Scaling (10K classes)

Same queries, varying hub-to-hub connectivity.

| Phase | Sparse (~2 per hub) | Dense (~10 per hub) |
|---|---|---|
| Associations | 10,100 | 19,000 |
| Parse + build | 814ms | 949ms |
| Normalize | 128ms | 191ms |
| 100 queries | 343ms | 817ms |
| **Total** | **1,402ms** | **2,076ms** |

Density adds moderate overhead. Per-query cost stays under 10ms.

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

## Key Optimizations

1. **Demand-driven compilation** (TypeChecker pass 2): only compile source relations
   for classes the query navigates — not the entire model graph.

2. **Demand-driven resolution** (MappingResolver): only resolve association joins
   the query actually uses — skip all others.

3. **Association index** (PureModelBuilder): `className → List<Association>` index
   for O(1) lookup instead of O(N) scan. Reduced normalize from 21s → 128ms at 10K.
