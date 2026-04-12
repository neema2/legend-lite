# Stress Test Benchmarks

Measured on MacOS, Apple Silicon, JDK 21, single-threaded.
Commit: `c0997b5` (hand-rolled parser + zero-alloc textEquals + ArrayList int-indexed maps).

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

Reproduce (standard suite):
```bash
mvn test -pl engine -Dtest="StressTest,StressTest10K,StressTest100K,StressTestDense,StressTestComplexQueries"
```

Reproduce (chaotic only):
```bash
mvn test -pl engine -Dtest="StressTestChaotic" -Dsurefire.excludedGroups=""
```

## Model Size Scaling

Same 10 query patterns (simple project, filter, sort, 1-hop, 2-hop association navigation).

### Cold (separate JVM per test)

| Phase | 1K | 10K | 100K | Scaling |
|---|---|---|---|---|
| Generate source | 2 ms | 15 ms | 124 ms | Linear |
| Parse + build | 90 ms | 266 ms | 1,496 ms | ~Linear |
| Normalize | 11 ms | 79 ms | 327 ms | ~Linear |
| **Total** | **328 ms** | **531 ms** | **2,115 ms** | ~Linear |

vs previous (`f74cbef`, ANTLR parser):

| Phase | 1K | 10K | 100K | Speedup |
|---|---|---|---|---|
| Parse + build | 228→90 ms | 818→266 ms | 5,246→1,496 ms | **2.5–3.5x** |
| **Total** | 408→328 ms | 1,073→531 ms | 5,905→2,115 ms | **1.2–2.8x** |

Queries are constant-time regardless of model size — demand-driven compilation means
only the classes a query touches are compiled and resolved.

## Density Scaling (10K classes)

Same queries, varying hub-to-hub connectivity.

### Cold

| Phase | Sparse (~2 per hub) | Dense (~10 per hub) |
|---|---|---|
| Associations | 10,100 | 19,000 |
| Parse + build | 266 ms | 307 ms |
| Normalize | 79 ms | 66 ms |
| **Total** | **531 ms** | **616 ms** |

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
| Pure source | ~115 MB |
| Generate source | 432 ms |
| Parse + build | 2,095 ms |
| Normalize | 468 ms |
| **Total** | **3,250 ms** |
| Heap required | 4 GB |

vs previous (`f74cbef`, ANTLR parser): Parse+build **9,611→2,095 ms (4.6x)**, Total **11.1→3.3 s (3.4x)**, Heap **6→4 GB**.

16 query patterns: simple projects, all-column projects, filter on Integer/Boolean/Float,
1-hop and 2-hop associations, DynaFunc computed columns, sorts, limits, slices,
assoc+filter combos, Date column projects.

Queries remain O(1) — 200 queries in <200ms regardless of 100K model size.

## Multi-Domain Realistic Model (StressDomainTest)

20 financial domains (products, refdata, counterparty, org, positions, trading, pnl,
risk, settlement, ops, collateral, sales, regulatory, marketdata, funding, accounting,
clearing, tax, research, prime) with realistic class names, properties, and cross-domain
associations — a real capital-markets data model.

| Metric | Value |
|---|---|
| Domains | 20 |
| Classes | 200 (10 per domain) |
| Tables | 200 |
| Intra-domain joins | ~130 |
| Cross-domain joins | 44 |
| Mappings | 20 |
| Properties | ~2,400 total |
| Pure source files | 41 (.pure) |

### Pipeline Timings

| Phase | Time |
|---|---|
| Load 41 files | 12 ms |
| Parse + build model | 194 ms |
| Normalize 20 mappings | 8 ms |
| 12 queries | 176 ms |
| **Total** | **396 ms** |

### Per-Query Breakdown (μs)

| Query | Hops | JOINs | Props | SQL | Total | Parse | TypeCheck | Resolve | PlanGen |
|---|---|---|---|---|---|---|---|---|---|
| q0: Trade→Instr→Sector+Cpty+Trader+Book | 3 | 5 | 48 | 2,127 | 66,763 | 10,535 | 47,361 | 2,413 | 6,449 |
| q1: Trade→Book→Desk+Instr+Trader | 3 | 6 | 50 | 2,152 | 11,900 | 9,000 | 2,194 | 223 | 482 |
| q2: PnL→Book→Desk+Trader | 3 | 4 | 45 | 1,801 | 8,637 | 6,733 | 1,514 | 158 | 230 |
| q3: Settlement→Trade→Instr+Cpty→Country | 4 | 6 | 52 | 1,902 | 8,567 | 6,659 | 1,526 | 216 | 165 |
| q4: Confirm→Trade→Instr→Sector+Cpty | 4 | 6 | 40 | 1,699 | 8,833 | 7,141 | 1,316 | 187 | 187 |
| q5: SalesCredit→Trade→Book→Desk+Instr+Cpty | 4 | 5 | 42 | 1,752 | 12,274 | 9,501 | 2,241 | 206 | 322 |
| q6: Greeks→Position→Instr→Sector+Instr | 3 | 5 | 44 | 1,600 | 6,814 | 5,704 | 840 | 124 | 144 |
| q7: TradeReport→Trade→Instr+Book→Desk+Cpty | 4 | 6 | 46 | 1,933 | 9,167 | 7,657 | 1,161 | 172 | 174 |
| q8: CollateralAgmt→Cpty→Country | 3 | 2 | 40 | 1,657 | 6,789 | 5,930 | 620 | 89 | 147 |
| q9: Position→Instr→Sector+Currency | 3 | 3 | 40 | 1,611 | 6,206 | 5,388 | 600 | 103 | 113 |
| q10: Trade filter+sort+limit+nav | 3 | 4 | 21 | 1,029 | 13,104 | 6,613 | 3,468 | 157 | 2,863 |
| q11: **Trade→9 disjoint JOINs** | 2 | **9** | **60** | **2,933** | 12,546 | 10,735 | 1,364 | 208 | 238 |

q0 is the cold-start query (JIT warming: 47ms TypeCheck). After warmup, TypeCheck
drops to 0.6–2ms. Parse dominates at 5–11ms (ANTLR overhead on large query text).
Resolve + PlanGen are consistently **100–400μs combined**, even for the 9-JOIN monster.

### q11 Join Tree (9 disjoint paths, 6 domains)

```
TRADE (t0)
├─ INSTRUMENT (j1)     ← Trade_Instrument
│  ├─ SECTOR (j2)      ← Instrument_Sector
│  ├─ CURRENCY (j3)    ← Instrument_Currency
│  └─ EXCHANGE (j4)    ← Instrument_Exchange
├─ BOOK (j5)           ← Trade_Book
│  └─ DESK (j6)        ← Book_Desk
├─ TRADER (j7)         ← Trade_Trader
└─ COUNTERPARTY (j8)   ← Trade_Counterparty
   └─ COUNTRY (j9)     ← Counterparty_Country
```

60 columns from 10 tables across 6 domains (trading, products, refdata, positions,
org, counterparty), 2.9KB SQL, generated in 238μs of plan generation.

## Key Optimizations

1. **Hand-rolled recursive descent parser** (PureModelParser + PureLexer2): replaces
   ANTLR parse tree + visitor with direct token-to-object construction. Zero parse
   tree allocation, zero visitor dispatch.

2. **Zero-allocation string comparison** (textEquals): compares lexer token bytes
   directly against keyword strings without creating intermediate String objects.

3. **ArrayList int-indexed maps** (PureModelBuilder, MappingRegistry): all
   class/mapping/association lookups use `ArrayList<T>` indexed by primitive int IDs
   from SymbolTable — eliminates Integer boxing, HashMap$Node allocations, and hash
   computation. Direct `array[id]` access instead of `hashCode()` + bucket walk.

4. **Demand-driven compilation** (TypeChecker pass 2): only compile source relations
   for classes the query navigates — not the entire model graph.

5. **Demand-driven resolution** (MappingResolver): only resolve association joins
   the query actually uses — skip all others.

6. **Association index** (PureModelBuilder): `className → List<Association>` index
   for O(1) lookup instead of O(N) scan.
