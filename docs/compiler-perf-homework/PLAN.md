# Compiler performance: the measurement plan (2026-11-13)

Phase 1 of the compiler performance investigation (branch `claude/compiler-perf`, from `068dbf199`). The scope is
the whole compiler pipeline and its host-side costs: A to J, plan making and PlanJson, and the host half of K (decode
and the writers). It does **not** cover the SQL's shape or size (stream E), the database's own time, or DataCube
rendering. The report will be `docs/COMPILER_PERFORMANCE_2026_11_13.md`. Until the user rules, this investigation
measures only and fixes nothing.

## 0. What already exists, and what this plan reuses

Nothing new is built to measure where a tool already does the job:

| Tool | What it gives | Database? |
|---|---|---|
| `//core:compile_latency` (`CompileLatency.java`) | per-query names / type / lower / render times (p50, p95, p99, max) and the model's parse+build, over the stress corpus or any model + query TSV; a cold pass and warm passes | no |
| `//core:scale -- 10k\|100k\|dense\|complex\|chaotic\|profile` | generated hub-spoke models (1K, 10K and 100K classes) and the chaotic 100K: generate, parse+build, normalize, 100-200 queries | connection opened, nothing run |
| `//core:stress_tool` | the stress corpus executed (4,736 service tests), on DuckDB, with shared or fresh sessions | yes |
| `//spec:eager_corpus_compile` | every body of the corpus world typed (`compileAllBodies`), about 9.9K bodies | no |
| `bazel run //wasm:startup` | the TeaVM planner in Node: load, boot, first plan, warm p50 | no |
| `tools/census:render_census` + `-Dlegend.diagnostics=pct-cases` | PCT channel A's cases recorded once, then lowered and rendered with no database | no (replay) |
| `docs/build-inventory/program/evidence/compiler/jfr_*.py` | JFR samples attributed to the phases (the first phase found on the stack) | — |

Prior numbers to compare against (history, other machines): the 2026-10-08 baseline (stress compile-only p50 0.38 ms,
p95 1.02 ms; lower is 0.30 ms of the 0.38 ms p50; the model's parse+build 1.5 to 6.5 s), and the 2026-10-07 and
2026-10-08 JFR profiles (names 12-17%, store-resolve 12-15%, `BareNames.catalogTiered`, `MappingDefinition.withIncludes`,
`SymbolTable.resolveId`, and HashMap churn).

**A correction to the brief.** `stress_tool` cannot scale a model. The 1x / 10x / 100x scaling comes from `//core:scale`'s
1K, 10K and 100K hub-spoke generators (`StressTest`, `StressTest10K`, `StressTest100K`), whose sizes are hard-coded.
For more points (for example 1K, 3K, 10K, 30K, 100K) and a fixed query set, a throwaway generator in the scratchpad
writes a `.pure` model and a query TSV, and `compile_latency --model/--queries` times them. Nothing is committed but
the generator's source, as `.txt`.

## 1. Workloads

| # | Workload | Driver | What it isolates |
|---|---|---|---|
| W1 | Stress corpus, compile only (7,610 elements, 4,736 queries) | `compile_latency --corpus stress --passes 6` | the model build (A-F) once, then G to J per query, cold (pass 1) and warm (passes 2-6) |
| W2 | Stress corpus, executed on DuckDB with shared sessions | `stress_tool` under JFR | the host side of K (decode, the writers, `ServiceTestRunner`) against planning. Database time appears only as native time, which the Java samples do not see |
| W3 | Model scaling at 1x, 10x and 100x, plus intermediate points | `//core:scale 10k/100k`, and the scratchpad generator at 1K, 3K, 10K, 30K, 100K classes through `compile_latency --model` | superlinear growth in A-F, and whether per-query G-J stays flat as the model grows (invariant 5: a query should pay only for what it touches) |
| W4 | The chaotic 100K and the dense 10K | `//core:scale chaotic`, `dense` | heterogeneity and connectivity at scale; peak heap |
| W5 | Deep and wide queries | scratchpad query generator over one fixed model, depth d = 1, 2, 4, 8, 16, 32 for each shape: chained filters, nested lets, association paths of length d, d-way fan-out, projections d columns wide, nested groupBy / join / subselect, and `if` / `case` towers; also `//core:scale complex` | superlinear growth in G, G½, H and I per construct. If J grows only because the SQL text grows, the report says so and stops there (stream E's area) |
| W6 | PCT channel A planning | record the cases once (`-Dlegend.diagnostics=pct-cases` on `pct_duckdb_*`), then time a plan-only replay (`compile_latency --model/--queries`, or a throwaway timer around `RenderCensus`'s loop) | the per-case model compile, which `PctPlanRoute` repeats for every case (`Compiler.compileModel` per call), against the query's own cost |
| W7 | Relational corpus | `//spec:eager_corpus_compile` (the planning half: about 9.9K bodies) under JFR; `bazel run //spec:corpus_one -- duckdb host` timed per test with `-Dlegend.diagnostics=progress` | the whole-world model and typing, and the host pass's planning share. The database's share is reported, not analysed |
| W8 | Core suites | `//core:core_tests` wall time per package and JFR on `core_tests_integration` | whether the suites' time is the compiler (repeated model compiles of the same fixture) or the harness |
| W9 | Server cold start and first request | `//core:server` started from `bazel-bin` under `-Xlog:startuptime`-style timing (process start to "Backend Ready", then the first compile request and the next 50) | static initialisation (the prelude, the system metamodel, the boot layer) and the absence of a model cache in the server |
| W10 | WebAssembly planner | `bazel run //wasm:startup`, 5 runs | the JVM-to-WASM ratio for the same plan. Only if the profile points at our code, not at TeaVM |
| W11 | Python native library | if `//native:compiler` builds in this container's disk and time budget: `lite_plan_text` timed over the wasm corpus from `python/tests` (cold load, first plan, warm p50). Otherwise reported as not measured, with the reason | native-image cold and warm costs against the JVM |

**Cold versus warm** throughout: cold means a new JVM and its first model and first query. Warm means the same work
again in the same process after it has been JIT-compiled (the last of five or more passes). Both are reported.

## 2. Metrics

- **Time per stage.** A lex, B/C parse, D names, E normalize, F build and integrity, G type, G½ inline, H store-resolve,
  I lower, J render, plan making and PlanJson (the plan built and serialised), and the host half of K.
  - `compile_latency` already splits G to J, as type / lower (G½+H+I) / render.
  - A throwaway probe in the scratchpad (never committed) splits A to F and G½ / H / I with the public entry points
    `Compiler.parseSources`, `buildModel` and `TypedQuery`'s stages. It splits further only where those points allow;
    otherwise JFR's per-phase sample attribution (`jfr_phases.py`) gives the shares.
- **Allocation per stage.** `com.sun.management.ThreadMXBean.getThreadAllocatedBytes` around each stage in the
  probe, plus JFR's `ObjectAllocationSample` by top frame.
- **Peak heap and GC.** `-Xlog:gc*:file=…`: collection count, total and longest pause, peak heap after collection.
  Also `MemoryPoolMXBean` peak usage at exit.
- **Planning time per query** (p50, p95, p99, max) and **per model** (A to F), and per body for W7.
- **Growth exponents.** For W3 and W5, the slope of time against size on a log-log fit across the points. A slope near
  1 is linear; anything clearly above 1 is a finding, and gets a profile at the largest point.
- **Determinism check, as a sanity measure (G10).** Each repeat's per-case output (the rendered SQL, or the plan's
  JSON, hashed) must be identical across repeats. Otherwise the repeats are not measuring the same work, and that is
  itself reported.

## 3. Tools

- **JFR**, `settings=profile` (sampling every 10-20 ms) with a writable repository folder, as the earlier profiles did.
  For W1 and W3, also a custom `.jfc` with a 1 ms Java sample period, so that the short per-query work collects
  enough samples.
- **`-Xlog:gc*`** and `-Xlog:safepoint` for GC and pauses.
- **Aggregation:** the existing `jfr_phases.py`, `jfr_callers.py` and `jfr_other.py` (read in place; changes to them,
  if any, are copied into the homework folder as `.txt`).
- **A throwaway timing probe** in the scratchpad, compiled against `bazel-bin`'s `core_tests_lib` jar. Never committed;
  its source is copied into the homework folder as `.txt` so the numbers can be reproduced.
- **async-profiler** only if its release archive downloads and loads cleanly (GitHub release assets work through the
  proxy). It is used for allocation flame graphs and a wall-clock view. If it does not load, JFR alone.
- **No product code is changed.** No guard file, ratchet or ledger is touched.

## 4. Noise control

- **The machine.** This container: 4 vCPU (Xeon @ 2.10 GHz), 15 GB RAM, JDK 21.0.12, shared cloud hardware. Every
  receipt records `nproc`, the load average and the CPU steal counter from `/proc/stat`, taken before and after the
  run. A run whose steal grew by more than 2% of its CPU time is flagged in the receipt, not silently dropped.
- **Nothing else running.** The Bazel server is shut down (`bazel shutdown`) after the build. Every timed run starts
  a built binary straight from `bazel-bin` with fixed JVM flags, not through `bazel run`, so neither Bazel's JVM nor
  its sandbox competes. No other session's work runs in this container.
- **Fixed JVM settings.** `-Xms` equal to `-Xmx` (2 GB for W1, W2, W5 and W6; 6.5 GB for W3 and W4, as `//core:scale`
  sets), default GC (G1, the product's), `-Duser.timezone=GMT`. One configuration for every run of a workload.
- **Warm-ups and repeats.**
  - Warm numbers come from the last pass after at least 4 warm-up passes in the same JVM.
  - Cold numbers come from at least 5 separate JVMs.
  - Each workload is repeated at least 5 times. The report gives the median, the minimum and maximum, and the IQR.
    Per-query numbers are percentiles within a run, and their medians across runs.
  - The order of workloads is interleaved across repeats, so that a slow period of the machine does not fall on one
    workload.
- **A kill criterion for hypotheses.** A suspected hotspot is a finding only if it shows up in JFR samples, survives
  the repeats, and (where the probe can isolate it) its stage's time scales the way the hypothesis says. Hypotheses
  that measurement kills are recorded, as `PERFORMANCE_AUDIT.md` §5 does.

## 5. Hypotheses carried in from the earlier audits, to confirm or kill

1. Name resolution (`BareNames.catalogTiered` / `catalog` / `tiered`, `ResolvedNames.referents`) rebuilds catalogs
   per call: 24-40% of the whole-world compile on 2026-10-07.
2. `MappingDefinition.withIncludes` recomputes the include closure on every lookup (`MappingDefinition.java:447`;
   2.5% then).
3. `SymbolTable.resolveId` and `ModelBuilder.findMapping` look up by the FQN string on every call.
4. `Pipelines.closeOverConditions`: an O(k³) fixpoint in join-slot count (`PERFORMANCE_AUDIT.md` §5.1; still called
   from `OccurrenceBundling`, `ChainedExists` and `CorrelatedSubselects`). W5's join and association depth is its test.
5. Per-query lowering is most of a warm query (0.30 ms of the 0.38 ms p50 on 2026-10-08). Where in G½, H and I it
   goes is not yet known.
6. Every product door compiles the model per request (`PctPlanRoute`, `PureV1Api`, `ModelInput`): no model is held
   between requests, outside the boot layer's process cache. W6 and W9 measure what that costs. Any fix has to
   respect "no cross-run state" and G10.
7. HashMap churn (`getNode`, `putVal`, `resize`) dominates the self time, which suggests string-keyed maps rebuilt
   per call rather than one bad algorithm.

## 6. Deliverables of phase 2 (the baseline)

`docs/compiler-perf-homework/`:

- `README.md`: how to reproduce every number;
- `scripts/*.txt`: the run scripts, the generator and the probe's source, all as `.txt`;
- `raw/`: every run's raw output (TSV and logs), plus the JFR summaries. The `.jfr` files themselves are kept
  outside the repository, because of their size.
- `BASELINE.md`: the medians and spreads per workload and per stage.

## 7. Risks to the plan itself

- **Disk.** Bazel's LLVM toolchain is 11 GB unpacked, and this container has about 30 GB in all. I have turned off
  the repository-contents and disk caches (`~/.bazelrc`) to fit. The native image (W11) and the reference checkouts
  (W7's eager compile reads the upstream corpus) may not fit beside it. If they do not, I say so and skip them; I do
  not delete the LLVM tarball.
- **A shared machine.** A noisy neighbour shows up as steal and as spread across repeats. It is reported as found, and
  the results are only stated as firmly as the spread allows.
- **The time budget.** The order is W1, W3, W5, W2, W6, W9, W8, W7, W10, W11: the core compiler first, then the
  edges.
