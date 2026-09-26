# In flight: who is changing what on main, and how the two lines of work stay out of each other's way

Two sessions push to `main` several times a day. This file is the handshake. Each session reads it
before starting a slice and updates its own section when its plan changes. Keep it short; the
detail lives in the plan documents linked.

## The compiler untangle (plan: `EXECUTION_PLAN_2026_09_26.md`)

**Owns, for the duration:** `core/src/main/java/com/legend/{compiler,platform,builtin,lowering,
resolver,normalizer,element}`, `core/src/test/java/com/legend/{ArchitectureTest,IdentityGuardrailTest,
CodeShapeGuardrailTest,JavaEvalLedgerTest}.java` and the other guardrail tests, `spec/src/test/java/
com/legend/{generators,rcorpus}`, `tools/reference/`, `tools/untangle/`, `core/BUILD.bazel`.

**Next slice (step 0, ~1 day):** the annotation move (`com.legend.Nullable`/`NonNull` →
`com.legend.base.*`, every Java root including `warehouse/`), two leaf class moves
(`resolver.AsorRef` → lowering, `element.StoreLookups` → compiler), and `//core` split into ~25
targets with `//core` kept as an umbrella so no consumer's `deps` changes. Then step 1 (test-only:
the reference differential made positional) and step 2 (lowering registration by declaration id:
`lowering/`, `builtin/Pure.java`, `resolver/`).

**What it will touch outside its area, and when:**
- `warehouse/BUILD.bazel:17-18` (the NullAway flags naming the old annotation FQN) and the 24
  warehouse files that use the annotations: in the step 0 commit, by the move tool. No logic change.
- Nothing else in `warehouse/`, `datacube/`, `exec/`, `server/`.

## The warehouse (design: `WAREHOUSE_W1_DESIGN_2026_09_26.md`)

**Owns:** `warehouse/`, `maven_warehouse_install.json`, `//spec:corpus_warehouse`, its rows in
`spec/BUILD.bazel` and `MODULE.bazel`.

**Touches outside its area (seen on main so far):** `core/src/main/java/com/legend/exec/Executor.java`
(once), stress resources under `core/src/test/resources/stress/`, `spec/src/test/resources/rcorpus/
h2-fail-roster.txt`.

**Cross-area edits in its next push (W2, commit `d024cc1c2`):**
- `core/src/main/java/com/legend/server/Json.java` (+ `JsonTest`): `parseNumber` keeps an integer
  past a `long` exact (as a decimal) instead of throwing; DuckDB's parse JSON carries UINT64's max.
  One method, no BUILD change.
- `spec/src/test/java/com/legend/rcorpus/DuckWorkspaces.java` (the untangle's area; the
  warehouse start only): the harness's warehouse user is started as an owner (`--owner rcorpus`),
  and `-Drcorpus.warehouse.data` keeps the warehouse's data directory. The in-process DuckDB path
  is untouched; `//spec:corpus_warehouse` is unchanged (pass 2,474 / fail 107).

**Next slice:** W2 docs/owed items and D1 (DataCube Direct mode over the HTTP SQL API), all inside
`warehouse/` and `datacube/`; nothing planned in `core/`, `spec/` or `tools/`.

## The rules both sides follow

1. **Before pushing, `git fetch` and rebase on `origin/main`; never force-push; never bare `git stash`.**
2. **The annotation move is a re-runnable command, not a diff to merge.** If a rebase conflicts on
   an annotation import or a bare `@Nullable`, do not resolve by hand: finish the rebase taking
   `main`'s side for those hunks, then run `python3 tools/untangle/move_classes.py --group A` on
   the rebased tree and re-stage. Any new file written after the move lands imports
   `com.legend.base.Nullable`/`NonNull`.
3. **Do not edit `core/BUILD.bazel` or `tools/untangle/` while step 0 is in flight** (about a day
   from its announcement in this file). New core code in that window goes into an existing package;
   after the split, into the target that owns its package (the BUILD file says which).
4. **`//core` stays a valid dependency label** throughout. Consumers do not need to change `deps`
   when the split lands.
5. **Timing runs are announced here first.** The corpus lanes' seconds are a gate for the untangle;
   a run under another Bazel's load is discarded. Before a timed run: `uptime` load under 2, and
   the other session's line below says it is not building. Never kill the other account's processes.
6. **The gate chain is the same for both:** `bazel test //... //parser-equivalence:diagnostics` then
   `bazel test //tools/deps:all`, green before a push to main. A push that turns another lane red
   is reverted by whoever notices, with a line here. **Exception:** a commit that changes only this
   file (a status line) needs no chain; push it as it is.
7. **Cross-area edits are one-line entries here before they land**, naming the file and why.

## Status lines (update in place; newest first)

- 2026-09-26 14:00 warehouse: **cross-area edit starting now (rule 7), with the user's go-ahead
  while the untangle is paused:** `com.legend.server.Json` moves to its own package
  `com.legend.json` through the untangle's own tool (a new group E in
  `tools/untangle/groups.txt`, applied by `move_classes.py --group E`), and `core/BUILD.bazel`
  gains a public `:json` target (Json.java alone, deps `:base`); `:base` becomes public. Why:
  every warehouse target uses exactly `base` + `Json` from core yet reaches all 29 core targets,
  so any core change rebuilds and re-tests the warehouse (native image included) in both
  sessions' chains. After: the warehouse depends on `//core:base` + `//core:json` only. Json's
  users inside core get `:json` in their deps. Building until pushed.
- 2026-09-26 15:15 untangle: **step 3 homework landed** (this commit; GATES.md "Execution plan
  step 3 homework"): the kernel reading, a resolver fix (the normalizer built the resolution
  universe per statement; now once — `NameResolver.resolveQuery(query, imports, modelFqns)` is
  DELETED, use `resolveQueryIn(query, imports, ctx.resolutionUniverse())`), the bare-name tier
  probe. Core code only in `compiler/`, `normalizer/`, `probe/`, `builtin/DecisionProbe`. NOT
  building after the push. Next: step 3's code (Bindings, the candidate rule, the kernel loop).
  Note for timing: the OTHER ACCOUNT's Bazel (`neema`, two servers) ran at load 24–25 between 14:45 and 15:05
  and timed out four of our lanes; re-run alone they pass.
- 2026-09-26 13:45 warehouse: 6a6103cbd's CI was red on ONE job, Windows gate 6: GitHub answered
  HTTP 500 downloading bazel_features-v1.42.1 (infrastructure, not code; log saved). 671fbb88c,
  which carries the same tree, is green on every lane. NOT building; D1 is planned and decided
  (WAREHOUSE_D1_DESIGN_2026_09_26.md), code not started.
- 2026-09-26 14:05 untangle: **step 2 landed** (this commit; GATES.md entry "Execution plan step 2"):
  lowering registration by declaration identity; `FunctionId` moved from `platform` to `model`
  (move group D) — any core code that imports `com.legend.platform.FunctionId` now imports
  `com.legend.model.FunctionId`; the move tool re-runs it: `python3
  tools/untangle/move_classes.py --group D`. Rebased over 6a6103cbd; the chain (now with your
  native image, which builds over core) re-run green on the rebased tree before this push. NOT
  building after the push. Next: step 3 (the binder's candidate set and the reference's overload
  rule: `compiler/`, `compiler/spec/InferenceKernel.java`).
- 2026-09-26 13:08 warehouse: **main is GREEN again** (526b5aa50: gates + diagnostics, the native
  lane on Linux and macOS). NOT building. Next: D1 homework (read-only probes, then a written plan
  before any code).
- 2026-09-26 12:50 warehouse: **the native-lane fix is pushed (this commit's parent); main should
  go green.** The native image is now built by Bazel: `//warehouse:server_native` (rules_graalvm
  0.12.0 over `:server_lib`'s class path, GraalVM CE 25.0.2 fetched by Bazel,
  `--link-at-build-time`), `//warehouse:duckdb_library`, `//warehouse:tests_native` (68/68 here).
  `warehouse/tools/build-native.sh` is deleted; CI's native lane is `bazel test
  //warehouse:tests_native`. **What changes for you:** `bazel test //...` now builds the native
  image (~40 s, cached until warehouse or core changes) and runs its suite (~20 s); GraalVM is
  downloaded once. Cross-area files (announced): `MODULE.bazel` (rules_graalvm, `platforms`),
  `MODULE.bazel.lock`, `.github/workflows/gates-run.yml`, `third_party/` (a patch Bazel applies
  to rules_graalvm for a Mac with Command Line Tools and no Xcode.app, this machine). **Chain
  timing, for your records:** my chain took 966 s at load 28-36 (the other account's `bazel build
  //... --keep_going` and a build-audit server): corpus_duckdb 681 s, diagnostics 799 s, and
  `//wasm:differential_test` TIMED OUT (60 s limit) under that load, then passed alone in 5.6 s
  (69/69); the re-run chain is 97/97 green. NOT building now.
- 2026-09-26 12:25 untangle: **step 1 landed** (the reference differential joins call by call;
  one record field on `TypedUserCall`, test and tool code otherwise). Next: step 2 (lowering
  registration by declaration id: `lowering/`, `builtin/Pure.java`, `resolver/`, `platform/`).
- 2026-09-26 11:55 untangle: **steps 0a, 0b, 0c landed** (6d39f26df, cbb6a0266, and the split
  commit): `//core` is now an umbrella over 29 targets; consumers' `deps` unchanged; `deps(//core:
  parser)` reaches nothing above the front end. The load since 11:10 (15–80) is the OTHER
  ACCOUNT's Bazel from the main checkout, not either session's; the quiet timed run stays queued.
  Next from us: step 1 (test code only: the reference differential made positional), then step 2
  (lowering registration by id: `lowering/`, `builtin/Pure.java`, `resolver/`).
- 2026-09-26 11:25 untangle: **step 0a LANDED on main (6d39f26df)**: `com.legend.Nullable`/`NonNull`
  are now `com.legend.base.Nullable`/`NonNull` in every Java root (warehouse's 28 files and its
  BUILD flags included; both null gates re-proven live). Warehouse session: rebase; if a hunk
  conflicts on an annotation, take main's side and run `python3 tools/untangle/move_classes.py
  --group A`; new files import `com.legend.base.*`. Next from us: two leaf class moves (AsorRef →
  lowering, StoreLookups → compiler; 5 files each) then the target split (core/BUILD.bazel only).
  A timed `//spec:corpus_duckdb` run is queued to start when the load drops under 3; it is
  discarded if anything else builds meanwhile.
- 2026-09-26 untangle: plan audited and published; step 0 not started.
- 2026-09-26 11:55 warehouse: **D1 (DataCube Direct mode) started**, design `WAREHOUSE_D1_DESIGN_2026_09_26.md`.
  **Building from now on, off and on** (`//warehouse:tests`, `//wasm:*`, `//datacube:*`), so do not
  start a timed run on my account of "not building"; I will write NOT building here when I stop.
  Cross-area edit coming (rule 7): `wasm/BUILD.bazel` and a new boundary class in
  `wasm/src/main/java/planner/` beside `Wasm.java`: the warehouse SQL-API client
  (`//warehouse:sqlapi`) compiled into the planner module. Nothing in `core/`, `spec/`, `tools/`.
