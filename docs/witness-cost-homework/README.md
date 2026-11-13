# The witness's cost on the product path (2026-11-13)

Evidence for item 5 of the "separation made foolproof" queue: what the standing witness costs a query, measured,
because the user asked. The witness is the code that refuses a crossing between planning and running:
- Java: `RunScope` (`base/.../RunScope.java`). Its `planning` checks sit at the planning seams, the A–F phase checks
  included. Its run side is `enter`/`close`, `statement`, `opening` and the call-back scope.
- Python: `Library.call`'s planner check (`_library.py`) and the run frame (`_runframe.runs`).
- TS: `run-frame.ts`. Under Node it is AsyncLocalStorage. In a browser or the headless V8 no frame is kept.

These are measurements only, except for one product change they led to: the Python frame's direct set and reset
(d74e94306), measured before and after below. The scripts are in `scripts/`, kept as `.txt` and never built by the
repository. The reader outputs and short logs are in `results/`; the raw JVM logs are not committed.

## Machine and toolchains

- A cloud container with 4 vCPUs (Xeon @ 2.10 GHz) and 15 GB of RAM.
- The repository's own toolchains:
  - Zulu 25.0.4 (Bazel's `remotejdk_25`), `JAVA_TOOL_OPTIONS` cleared;
  - Node 22.16.0 (`rules_nodejs`);
  - Python 3.12.13 (`rules_python`), with its duckdb 1.5.5, pyarrow and numpy wheels.
- The native library `//native:compiler` and the deploy jars of `//core:compile_latency` and `//core:stress_tool` were
  built from the branch at d00692eee–7f872ef24 (7f872ef24 changed none of their inputs).
- The container restarted once mid-measurement. The second W1 A/B kept 8 of its 12 JVMs (4 ON, 4 OFF, one host).
  Every later figure was measured on the new host.

## Method

"ON" is the code as shipped. "OFF" is the same code with the witness swapped for a no-op:
- Java: a stub `RunScope` (`scripts/RunScopeOff.java.txt`) first on the classpath.
- Python: `Library.call` without its check, and `Frames.run` unwrapped (its `__wrapped__`).
- TS: `run-frame.ts` with no frame kept (`scripts/make_run_frame_off.py.txt`).

Every figure is measured two ways:
1. **Counts times per-call cost.**
   - The checks per query are counted on the real path: Java's COUNT variant of `RunScope`
     (`scripts/make_runscope_count.sh.txt`), Python's counting frame (`frames_count.py`), and Node's promises through an
     async hook (`count-promises.mjs`).
   - Each per-call cost is measured ON vs OFF: `WitnessMicro`, `timeit`, `bench.mjs`. For Python's frame it is also
     measured around a real DuckDB statement, in 200 alternating paired batches (`pyduck*.py`).
2. **A whole-path A/B, ON vs OFF:**
   - Java's W1 (the stress corpus, plan only, `PerfProbe` from `docs/compiler-perf-homework`, with a counts readout);
   - Java's product path (`RunProbe`: `Compiler.query`, then `Execution.run` on DuckDB);
   - Python's `Frames` `Table.execute` (`pywhole.py`);
   - TS's WASM planner plus `runPlan` on DuckDB-WASM (`whole-bench.ts`).

For all three languages the whole-path A/B is noise-bound: ±5–10% between JVMs, ±20% between Node processes, and about
±35 µs for Python's paired batches. So the per-query figures come from method 1, and the A/Bs are kept as the check
that nothing larger hides there.

## Results: the witness's cost per query

| | before | now | a typical query (plan + run) | share now |
|---|---|---|---|---|
| Java | ≤ 0.1 µs (11 ns resolved) | the same | 2.4–3.3 ms (trades corpus, DuckDB, 1,000 rows) | ≤ 0.004% |
| Python | ≈ 5.7 µs | ≈ 1.4–2.2 µs (d74e94306) | 2.3 ms (1,000 rows) / 5.7 ms (100k rows), `Frames` `Table.execute` | ≈ 0.08% / 0.03% |
| TS on Node 22.16, as pinned | 1.0–1.1 µs | the same | 2.1–3.1 ms (WASM planner 1.0–1.3 ms + `runPlan` 1.2–1.8 ms) | ≈ 0.04% |
| TS on Node 22.16 with `--experimental-async-context-frame` | — | ≈ 0.03–0.06 µs (measured only, not adopted) | the same | ≈ 0.002% |
| TS in a browser or the headless V8 | 0 | 0 (and no witness: `frameKept` is false) | | 0 |

### Java

- **Checks per query:**
  - W1, plan only: 45 `planning` and 1 `plans` (274,635 checks per pass of 6,085 cases). Each model build makes about
    9,950 checks (phases A–F).
  - The product path: about 38 `planning`, 1 `plans`, 1 run, 1 `opening`, 3 `statement`, 1 call-back and
    1 `planningWhat`.
- **Per call** (`results/java-micro.txt`):
  - a run's `enter` + `statement` + `close`: +4.0 ns inlined, +6.7 ns out of line;
  - each further `statement`: +1.65 ns;
  - a `plans` scope: +3.8 ns out of line;
  - `planning`, `opening`, the call-back and `planningWhat`: below the micro's resolution (ON−OFF within ±2 ns, at times
    negative).
- **Per query:** the resolved terms add up to 11 ns. With ±2 ns allowed for each of the 41 unresolved calls, the bound
  is ≤ 0.1 µs. W1 plan only: ≤ 0.1 µs of about 0.86 ms of planning. A model build: ≤ 20 µs of about 550 ms.
- **A/B** (`results/java-ab.txt`):
  - W1: by each JVM's best warm pass, ON−OFF +1.6%; the no-check control (`PlanJson.write`) moved +1.7%. The JVMs fall
    into a fast mode (~4.7 s) and a slow mode (~6 s) whichever variant they run.
  - The product path: −6.5% by mean, +2.0% by p50.

### Python

- **Checks per query:** 2 planner calls (`lite_pure_v1`, `lite_plan_json_held`) and 1 frame (`'a plan'`) per
  `Table.execute`.
- **The check:** 36 ns per call. A real `lite_plan_json_held` takes about 230 µs.
- **The frame, before:**
  - 0.71 µs around an empty function, but 5.2–5.8 µs around a real DuckDB `SELECT 1` (paired, three runs, the same
    with GC off).
  - Holding the variable around a whole batch costs nothing (−0.1 to +0.7 µs, intervals spanning 0). So the cost was
    entering and leaving contextlib's generator context manager on cold caches, right after the database's work.
  - In the same harness, a plain do-nothing wrapper cost 0.7–1.0 µs and a direct set/reset wrapper 1.9–2.6 µs.
- **The frame, now** (d74e94306, `runs` sets and resets the variable directly): +1.3 to +2.1 µs per run beside the old
  frame's +5.2 to +5.8 µs, in the same runs; 137 ns around an empty function. Same semantics. Two tests were added, each
  red with the reset taken out.
- **Per query, now:** 2 × 36 ns + 1.3–2.1 µs ≈ 1.4–2.2 µs.
- **A/B, before the change** (`pywhole.py`): +16 µs [−18, +45] at 1,000 rows and +18 µs [−16, +55] at 100k rows.
  Consistent with the old frame, but it cannot resolve it.

### TS on Node

- **Per query:** 1 `planning` check (in `answer()`), 1 `inRun` (`runPlan`), and 21 promises (async-hook count, 10 passes
  of 18 queries; the same count with the flag).
- **Per call:**
  - `planning`: 0.4 ns;
  - `inRun`: +30–45 ns per run.
- **The process-wide promise tax.** On Node 22.16 as pinned, once any `inRun` has run, every promise in the process pays
  +41–50 ns: an await loop goes from 40 to 119–141 ns per iteration, at 2 promises an iteration, inside a run or outside
  one. Node 22's AsyncLocalStorage turns its promise hooks on at first use and keeps them on. The tax falls on all the
  process's async work, not only queries; a promise-tight loop runs about 3× slower.
- **Per query:** 21 × 41–50 ns + 30–45 ns ≈ 1.0–1.1 µs.
- **With `--experimental-async-context-frame`** on the same Node 22.16.0:
  - the tax is gone: 39–48 ns per await iteration ON vs 39–45 ns OFF, outside a run and inside one;
  - `inRun` +31 ns per run (77 vs 46 ns);
  - `planning` 1.0 ns (from 0.4).
  - Per query: about 0.03–0.06 µs.
  - The run frame's suites pass with the flag on (`NODE_OPTIONS`, checked by a preload):
    `//engine-client:run_frame_test` 5/5, `//engine-client:plan_runner_test` 43/43, `//datacube:lite_planner_test`
    29/29.
  - Measured only: no toolchain or flag changed in the repository.
- **A/B** (`whole-bench.ts`, 5 processes each): ON came out 9–19% faster. The planner, which the frame does not touch,
  moved just as much: process noise. Only 18 of the corpus's 67 parsed queries run end to end in this harness. The host
  runner writes integer, text and boolean cells only, and refuses the rest by name.

## Caveats

- Java's ≤ 0.1 µs is a bound, not a value: 41 of its per-query calls are below the micro's resolution, and each is
  allowed ±2 ns rather than guessed.
- The typical queries are the trades corpus's (`wasm/corpus`) and five `Frames` shapes. A query that sends more
  statements pays more where the cost is per statement:
  - Java: about 1.65 ns per statement check.
  - Python: nothing more (one frame per run).
  - TS on Node as pinned: 41–50 ns per extra promise its awaits make; nothing with the flag.

## To rerun

1. Build the inputs:
   - `bazel build //core:compile_latency_deploy.jar //core:stress_tool_deploy.jar //native:compiler //engine-client:plan_runner_test`.
   - `$OUTPUT_BASE` in the scripts is `bazel info output_base`, which holds the JDK, Node and Python toolchains.
   - `$SP` is a scratch directory.
2. Java:
   - compile `RunScopeOff.java.txt` (as `com/legend/base/RunScope.java`) into `$SP/off`;
   - make the COUNT variant with `make_runscope_count.sh.txt` and compile it into `$SP/count`;
   - compile `PerfProbe`, `WitnessMicro`, `RunProbe` and `CountRun` against the deploy jars (`javac --release 21`);
   - run `ab.sh`/`ab2.sh` (W1), `abrun.sh 1000 30` (the product path) and `WitnessMicro <mode> 50000000` per mode, ON
     (`-cp $SP/on:...`, an empty directory) and OFF (`-cp $SP/off:...`).
3. Python: with `LEGEND_LITE_LIBRARY` and `PYTHONPATH` (python/ plus the toolchain's site-packages), run `pybench.py`,
   `pyduck3.py` … `pyduck6.py`, `pywhole.py <rows> 200` and `frames_count.py`.
4. TS:
   - copy `//engine-client:plan_runner_test`'s runfiles `_main` (src, test, node_modules, pure-protocol, tools/js;
     duckdb-extensions and wasm/planner_dir linked) to `$SP/ts/tree-on` and `$SP/ts/tree-off`;
   - put the no-frame `run-frame.ts` in the OFF tree, and `whole-bench.ts` into each tree's `engine-client/test`;
   - run `prep_lambdas.py`, then `abts.sh 1000 20`;
   - run `bench.mjs <on|off> <per-call|runs|awaits-outside|awaits-inside>`, with and without
     `--experimental-async-context-frame`.
