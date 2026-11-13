# Compiler performance: the baseline (phase 2), 2026-11-13

Measured on branch `claude/compiler-perf` at 068dbf199 (its core/, wasm/ and native/ trees are identical to main's merge 981a24b87). The workloads, metrics and noise rules are in [PLAN.md](PLAN.md).
The scripts are in `scripts/` (as `.txt`), and the raw numbers and logs are in `raw/`. The diagnosis that reads these
numbers is `docs/COMPILER_PERFORMANCE_2026_11_13.md`.

**Machine.** A cloud container with 4 vCPUs (Xeon @ 2.10 GHz) and 15 GB of RAM. Steal was 0.3–1.9% per run, and every
log line records it. The JDK is the product's own: Zulu 25.0.4 (Bazel's `remotejdk_25`) with `JAVA_TOOL_OPTIONS`
cleared. The heap is fixed (`-Xms = -Xmx`). Each workload ran several times as separate JVMs, round-robin; the spread is
reported, not hidden. In-process times come from `System.nanoTime` and allocation from
`ThreadMXBean.getCurrentThreadAllocatedBytes`. Profiles are JFR at 1 ms sampling (`fine.jfc`), with
`-XX:+DebugNonSafepoints` on the line-level ones.

**The probe.** `PerfProbe` is throwaway code and is not committed as Java. It calls the production seams one by one,
the same calls `//core:compile_latency` makes:
- `Compiler.parseSources` (A–C) and `Compiler.buildModel` (D–F);
- per query: names (`Compiler.query`/`queryResolved`), type (`TypedQuery.body`, G), plan
  (`TypedQuery.executionPlan`: G½ + H + I + J + plan assembly) and json (`PlanJson.write`);
- a SHA-256 of each plan's JSON, for the determinism check.

## W1: the stress corpus, plan only (5 JVMs × 5 model builds × 6 passes)

The corpus is 9,106 elements and 6,085 cases. 5,800 plan; 285 fail at names or type: unresolved `getAll` in the
typedvalues/trading sets, and a runtime with no mappings in m2m/outer. Those failures are the same in every pass.

| | median | min–max | allocated |
|---|---|---|---|
| model parse, cold (1st build) | 1,347 ms | 1,336–1,450 | 1.22 GB |
| model parse, warm (builds 2–5) | 625 ms | 569–961 | 1.21 GB |
| model build, cold | 2,246 ms | 2,136–2,284 | 1.06 GB |
| model build, warm | 753 ms | 685–925 | 0.90 GB |
| pass 1 (cold JIT), all cases | 13,959 ms | 13,670–14,181 | |
| pass 6 (warm), all cases | 8,178 ms | 7,991–8,292 | |
| — names | 591 ms (7.2%) | 586–602 | 0.39 GB |
| — type | 619 ms (7.6%) | 608–631 | 0.26 GB |
| — plan (G½–J + assembly) | 5,989 ms (73.2%) | 5,840–6,099 | 4.64 GB |
| — PlanJson.write | 859 ms (10.5%) | 853–872 | 1.24 GB |

- **Per query** (pass 6, 5,800 cases): plan p50 0.70 ms, p95 2.02 ms, max 7.0 ms (`D_PaymentDense`). The top 10% of
  cases take 21% of plan time, so no single pathological query dominates.
- **`//core:compile_latency`** (5 runs, same corpus):
  - total p50 0.79–0.84 ms, p95 2.15–2.37 ms, p99 2.78–3.31 ms;
  - by stage, `lower` p50 0.64–0.67 ms (its "lower" covers G½–I, ~84% of the total);
  - model parse+build 3.8–4.1 s; cold first case 246–275 ms.
- **Determinism (G10):** 0 of 5,800 plan-JSON hashes differ across all 30 warm passes (5 JVMs × 6).
- **Memory:** peak heap 1.63–1.68 GB; ~50 GCs per run, 1.2–1.3 s total.

## W3: model and mapping scale (hub-spoke generator; 5 JVMs × 3 builds × 6 passes of 100 queries)

`gen_scale.py` writes the generator of `StressTest10K` with parameters: H hubs, 9 satellites each, a ring, cross-links,
views and filters. `m*` splits the class mappings across K mappings that include each other (chain or flat). `s*`
splits the tables across S stores in an include chain. Every model plans 100/100 queries.

| model | MB | elements | parse cold ms | parse warm ms | parse MB | build cold ms | build warm ms | build MB | 100 queries warm ms (of which plan) | peak heap MB |
|---|---|---|---|---|---|---|---|---|---|---|
| h100 | 0.64 | 2,014 | 379 | 62 | 26 | 1,335 | 203 | 168 | 187 (141) | 372 |
| h300 | 1.97 | 6,034 | 623 | 230 | 79 | 1,735 | 550 | 496 | 184 (140) | 921 |
| h1000 | 6.62 | 20,104 | 2,337 | 1,821 | 262 | 2,964 | 1,700 | 1,627 | 233 (179) | 2,753 |
| h3000 | 20.31 | 60,304 | 15,250 | 16,583 | 776 | 6,300 | 4,553 | 4,884 | 213 (167) | 4,125 |
| h10000 | 68.28 | 201,004 | 165,654 | 175,038 | 2,557 | 19,588 | 13,171 | 16,269 | 226 (177) | 8,841 |
| m10chain | 6.62 | 20,113 | 2,642 | 1,764 | 262 | 3,285 | 2,001 | 1,707 | 229 (178) | 3,269 |
| m100chain | 6.62 | 20,203 | 2,390 | 1,723 | 263 | 4,338 | 2,648 | 3,126 | 211 (169) | 3,341 |
| m100flat | 6.62 | 20,203 | 2,426 | 1,726 | 263 | 2,991 | 1,718 | 1,644 | 235 (187) | 2,871 |
| s10 | 6.68 | 20,113 | 2,321 | 1,697 | 262 | 3,026 | 1,790 | 1,695 | 250 (191) | 2,989 |
| s100 | 6.75 | 20,203 | 2,460 | 1,715 | 261 | 4,118 | 3,019 | 2,476 | 269 (202) | 4,185 |

Reading the table:
- **Parse is superlinear in one file's size.** It is ~quadratic from 2 MB up (×3.3 size → ×7.9, ×9.1, ×10.6 time),
  while its allocation stays linear.
- **Build is roughly linear in elements,** at ~80 KB allocated per element.
- **Per-query planning is flat across model size,** 2K to 201K elements (invariant 5 holds).
- **A chain of mapping includes or store includes makes the build superlinear** in chain depth (m100chain, s100).
  Flat includes do not (m100flat).
- "peak heap" is the sum of each pool's peak, so it can exceed `-Xmx`.

**The include reproduction** (`winc.sh`): the same 2,000-class model split across K mappings, 3 JVMs each.

| K | 1 | 10 | 50 | 100 | 200 |
|---|---|---|---|---|---|
| chain: build MB | 488 | 507 | 609 | 810 | 1,436 |
| flat: build MB | 489 | 493 | 494 | 494 | 495 |
| chain: plan ms / 100 queries (median of passes 6–10, 3 runs) | 120 121 120 | 131 119 121 | 135 137 150 | 157 151 130 | 122 127 136 |

Per-query planning does not move beyond its noise (±15%) with K in this reproduction.

## W5: deep and wide queries (9 shapes × depth 1–32 over h100; 5 JVMs, pass 8)

Median ms per query (names + type + plan + json), and the growth exponent from depth 8 to depth 32:

| shape | d=1 | d=2 | d=4 | d=8 | d=16 | d=32 | exponent |
|---|---|---|---|---|---|---|---|
| filters | 1.38 | 1.19 | 1.43 | 1.71 | 2.66 | 3.62 | 0.54 |
| extend | 1.22 | 1.28 | 1.59 | 2.14 | 3.90 | 7.48 | 0.90 |
| path (association hops) | 1.91 | 3.03 | 3.31 | 5.77 | 12.11 | 33.38 | 1.27 |
| fanout | 2.12 | 2.81 | 3.56 | 5.63 | 9.25 | 14.50 | 0.68 |
| wide | 1.18 | 1.46 | 1.41 | 1.76 | 2.37 | 3.56 | 0.51 |
| groupby | 1.44 | 1.77 | 1.89 | 2.75 | 3.48 | 5.93 | 0.56 |
| ifs | 1.23 | 1.29 | 1.49 | 1.98 | 2.40 | 3.88 | 0.49 |
| joins | 2.34 | 2.71 | 3.92 | 5.61 | 10.17 | 22.30 | 1.00 |
| ands | 1.25 | 1.39 | 1.53 | 1.89 | 2.31 | 4.00 | 0.54 |

No shape's compile time is superlinear in depth, except `path`. Its plan JSON at depth 32 is 866 KB, against 17–210 KB
for the other shapes, so its cost is the plan's size. That is stream E's (lean SQL), not the compiler's.

## W9: a server request split (in process, `PureV1Api.generatePlan`; 2 JVMs each; ms per request)

| model | compileModel alone | the route as served (model compiled per request) | the route over a HELD model | one query + one executionPlan, held |
|---|---|---|---|---|
| wasm corpus (768 B) | 3.9–4.1 | 7.2 | 1.70–1.74 | 0.88 |
| h100 (0.64 MB) | 260–300 | 232–234 | 35.6–36.3 | 2.0 |
| h1000 (6.6 MB) | 4,432–5,560 | 4,155–4,171 | 1,474–1,495 | 2.1–2.4 |

The server holds no model: the JVM server uses `Models.COMPILED` on every route. Even over a held model, the route
costs far more than the query: it re-parses the model text (`ModelInput.Text.records()`) and plans twice (diagnosed in
the report). The model text's hash costs 0.04 / 10–22 / 53–56 ms.

## W10: the TeaVM planner against the JVM (same boundary calls, held model; 3 runs each, 15 reps per query)

| input | WASM ms/query | JVM ms/query | WASM/JVM per query (p10 / p50 / p90) | model compile per call, WASM vs JVM | answers identical |
|---|---|---|---|---|---|
| wasm corpus (69 queries) | 0.95–0.96 | 0.98–1.03 | 0.57 / 0.99 / 2.76 | 10.9–16.4 vs 7.3–8.3 ms | 69/69 |
| W5 deep (54 queries, h100) | 8.1–8.5 | 3.9–4.4 | 0.84 / 1.62 / 2.73 | 805–875 vs 169–207 ms | 54/54 |

- Holding the model costs 606–634 ms in WASM vs 1,110–1,308 ms on the JVM (corpus), and 1,443–1,699 vs 1,539–1,664 ms
  (h100).
- The JVM side is only lightly warmed (16 calls per query), so its ratio is conservative.
- `wasm/README.md`'s "warm plan p50 9.6–13.9 ms, 4.0–4.6× the JVM" timed `planOrError`, which compiles the model on
  every call: about 90% of it is model compile.

## W2: the stress corpus executed (`StressTool`, DuckDB, platform backend)

- **Results:** pass 5,700, fail 339, skipped 48.
- **Wall time:** 325.7 s plain; 368.8 s under JFR. User CPU is ~30% of 4 cores, so about one core is busy.
- **JFR (Java samples only; DuckDB's native time is not sampled):**
  - 96% of samples are in test planning (`TestPlanner.plan`).
  - Planning each test's DATA seed is 50.8% (`TestKinds.seed`: CSV parsing, INSERT rendering, SHA-256).
  - The test plan's JSON round-trip (`TestPlanJson.requireRoundTrip`) is 36%.
  - Query compilation proper (names, type, store-resolve, lower, render) is ~8–9%.
  - Exec, results and JDBC together are under 1%.

## Not run

- **W6 (PCT plan-only replay), W7 (corpus), W8 (core suites) and W11 (the native library)** were not run. W2 already
  answers the question they would have asked: what share of a test road's time is the compiler (8–9%), and what takes
  the rest. The time went to the diagnosis instead.
- **Disk:** about 15 GB of the allowance remained.
- **Runnable later:** each has its runner named in PLAN.md §2.
