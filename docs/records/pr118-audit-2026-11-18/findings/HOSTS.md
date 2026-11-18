# HOSTS: verdict INCOMPLETE

## Summary (at most 12 lines; answer your brief's questions first)

1. **Edge off.** Every shipped Java root now starts at the product: `//core:server` (main `LegendHttpServer`), `//wasm:planner` (`planner.TabExports` -> `:tab` -> `:boundary` -> `//core:plan_side`), `//native:compiler` (`:entry`) and `//query-store:page_entry`. No product source names an edge class by string or reflection, and `configured(null)` never touches `ServiceLoader`. The leaks are in Python and in what the guard cannot see: the product engine still serves legend-engine's `/api/server/v1/currentUser` (HOSTS-3); the product loader keeps the engine library's entry table and does an optional `import legend_lite_engine` (HOSTS-4); the product wheel ships `legend_lite.edge` (HOSTS-5); and `//:sites` ships DataCube's edge bundle. `ShippedArtifactRootsTest` covers Java targets only (HOSTS-8).
2. **Edge on.** Each host has one declared mechanism: `--engine legend-engine`, `loadEngineModule('legend-engine')`, `legend_lite.edge.engine()` plus the `[engine]` extra, and DataCube's `?planner=engine`. Each also has a bypass (`EdgeServer.main`, `Engine(edge=...)`) (HOSTS-7). DataCube's shipped app lost its engine plane (HOSTS-6). No shipped TS app wires the engine module. As far as reading goes, the Java routes match `main`.
3. **`transacted`.** Still present at the head: it commits outside its `try` (`plan-runner.ts:427`). The PR changed no host commit path, and its new `runPlanRows` path inherits the defect (HOSTS-2).
4. **Plan-format copies.** Version 58 agrees in Java, TS and Python, and is guarded. No host reads `upstream` or `decimal`. The PR did add hand-kept rules: three parameter checkers (HOSTS-10), three raise readers that disagree on two inputs (HOSTS-11), and four copies of the store's refusal mapping (HOSTS-9).
5. **Blocker.** The hosts' K2 proofs run in no CI lane, and the PR's own CI is red because of it (HOSTS-1).

## Blockers (if any; each one also appears in Findings)

- **HOSTS-1**: CI fails at the PR head on the lane guard (`linux / product`, job 134886148948). Eight tests are in no lane, five of them the hosts' K2 and conformance proofs: `//wasm:engine_routes_test`, `//wasm:product_module_test`, `//python:edge_test`, `//engine-client:engine_module_test` and `//engine-client:lite_api_server_test`. D's heavy round of 41 targets does not list them either, so the edge-on and edge-off runtime proofs for the tab and Python are not shown to have run on the final commit.

## Strengths

- **The server opt-in is clean.** `LegendHttpServer.main` -> `configured(engineOf(args))` (`core/src/main/java/com/legend/server/LegendHttpServer.java:586-628`).
  - With no `--engine`, it returns `LITE` and never calls `ServiceLoader`.
  - An unknown engine is refused by name, with the list of engines the server carries, and the server exits with status 2.
  - The route set with the engine on matches `main`'s, read side by side: `/api/pure/v1/` POST-only with a 405, `/api/pure/v1/query` on the raw path, and `/api/server/v1/currentUser` (`EdgeServer.java:30-35, 49-81`).
  - `ServerBootTest` covers off, on and unknown.
- **The wasm split has the right shape.** The product module's closure is `:tab` -> `:boundary` -> `//core:plan_side` (`wasm/BUILD.bazel:39-96`). The engine module has its own entry under `src/edge` (`EngineExports.java`) and depends on the product, never the reverse. The build-time twin `//tools/deps:wasm_planner_closure_test` is in the `checks` lane and passed on CI.
- **The native split mirrors it.** `//native:compiler` deps `[":entry"]` and `//native:engine` deps `[":edge_entry"]` (`native/BUILD.bazel:111, 178`). The Python arity tables match the native entry points: checked by script, no arity mismatch, and the only names in neither table are the free helpers.
- **The Python seam refuses by name and never defaults.** `_edge_seam.NO_EDGE_ROUTES` answers 404 and `edge_routes()` refuses `None` (`python/legend_lite/_edge_seam.py:38-54`). The layer rule has planted examples that it catches (`tests/test_run_closure.py:465-471, 607-612`).
- **DataCube's S23 move was a redesign, not a copy.** legend-engine's TinyInt-for-BIT reading left the product's `relation-type.ts`. The edge now reads the model's BIT columns lazily, only when an answer carries TinyInt (`datacube/src/edge/engine-booleans.ts`, `upstream-planner.ts` `#engineBooleans`; diff against `main`'s `planner.ts` `UpstreamPlanner`).
- **The product apps speak lite's API only.** No `pure/v1` or `server/v1` path literal remains in Query's, Studio's, the query store's or engine-client's doors' code. The clients are `query-store/src/client.ts:95,104`, `engine-client/src/doors/lite-server.ts:41` and `datacube/src/services/lite-server.ts`.
- **The plan version is guarded.** It is 58 in `PlanJson.java:171`, `plan-runner.ts` and `_runs.py:27`, and `PlanVersionsAgreeTest` reads both hosts. v58's removal of `upstream` is consistent: no host reads it.

## Findings

### HOSTS-1 [BROKEN-RULE] [BLOCKER] The hosts' K2 proofs and the lite API conformance readers are in no CI lane; the PR head's CI is red
- Where: `wasm/BUILD.bazel:251` (`product_module_test`), `wasm/BUILD.bazel:259` (`engine_routes_test`), `python/BUILD.bazel:533` (`edge_test`), `engine-client/BUILD.bazel:561` (`engine_module_test`), `engine-client/BUILD.bazel:576` (`lite_api_server_test`). None appears in any `test_suite` of `gates/BUILD.bazel`, nor in `//engine-client:tests` (`engine-client/BUILD.bazel:660-672`).
- What: CI's lane guard (`gates-run.yml`: `tests(//...) except tests(//gates:lanes) except tests(//gates:heavy)`) fails at `68f20eb4d`. The `linux / product` log, lines 729-737, lists:
  - `//core:is_numeric_probe_test`
  - `//core:lite_api_conformance_test`
  - `//engine-client:engine_module_test`
  - `//engine-client:lite_api_server_test`
  - `//python:edge_test`
  - `//studio:run_test`
  - `//wasm:engine_routes_test`
  - `//wasm:product_module_test`

  The handoff's list of D's 41 heavy-round targets names none of them.
- Why it matters:
  - These are the only runtime proofs of K2 in the hosts: edge off for the tab module, edge on byte-exact for the tab and Python, and the engine-module word check. Two of them are the server and browser readers of the shared lite API conformance suite (`lite-api-cases.json`).
  - As things stand, a regression in any of them reaches `main` unseen. The PR cannot merge green.
  - The handoff's §7.1 names `//wasm:engine_routes_test` and "the Python edge tests" as the edge-on proof. Nothing documented ran them at the head.
- Instead: add the five host tests to lanes, for example `//wasm:*` and `//engine-client:*` to `datacube` or `ui`, and `//python:edge_test` to `warehouse` beside `//python:engine_test`. Run them once at the head and only then claim K2.
- Confidence: verified (the CI log was fetched with `gh api .../actions/jobs/134886148948/logs`, and the gates suites were read).

### HOSTS-2 [WRONG-ANSWER] [FIX-AFTER] TS `transacted` still commits outside its `try`; a failed COMMIT leaves the session in an aborted transaction
- Where: `engine-client/src/plan-runner.ts:412-429`. The commit loop is at :427, after the `try`/`catch` that alone rolls back (:417-426).
- What: unchanged from `main` (`main:engine-client/src/plan-runner.ts:255`). The PR changed no host transaction or commit code: grep over the host diffs for commit, rollback or transact finds only a doc line. The new `runPlanRows` -> `runAnswer` -> `inTransaction` -> `transacted` path (`plan-runner.ts:729-842`) inherits the defect. Python's `run_write` commits inside its `try` (`python/legend_lite/_runs.py:151-163`). So does TS's own `stream()` (`plan-runner.ts:540-567`).
- Why it matters: when a write plan's COMMIT fails (for example DuckDB's "TransactionContext Error: Failed to commit" on a write-write conflict), no ROLLBACK is sent. The held tab session stays in an aborted transaction, and the next plan on it fails. Python, by contrast, rolls back. I did not run a reproduction.
- Instead: move the commit loop inside the `try` (the same shape as `stream()`), and add a test with a session whose commit statement throws.
- Confidence: verified (read); impact suspected.

### HOSTS-3 [WRONG-DESIGN] [FIX-AFTER] Python's product engine still serves legend-engine's `/api/server/v1/currentUser` with the edge off
- Where: `python/legend_lite/engine.py:150` (`_CURRENT_USER = '/api/server/v1/currentUser'`) and `:613-615` (answered before any lite route). It is pinned by product tests: `python/tests/test_engine.py:1211-1242` and `tests/test_wheel.py:133-134`.
- What:
  - Java moved this route to the edge's mount (`core/src/main/java/com/legend/edge/EdgeServer.java:68-81`) and gave the product `/api/lite/v1/user`.
  - Python serves lite's `user` too (`compiler.py:141`), and keeps upstream's route in the product.
  - Python's `EngineRoutes.serves` does not claim the route (`edge/_pure_v1.py:162-165`), so it is the product that answers it, whether the edge is on or off.
- Why it matters: this is an upstream route spelling in a shipped product artifact with the edge off (the brief's Q1), and the hosts disagree. Product tests keep it alive, so nothing will remove it.
- Instead: move the route into `EngineRoutes` (`serves` plus `answer`), point the product tests' cookie checks at `POST /api/lite/v1/user`, and add the route to the edge's tests.
- Confidence: verified.

### HOSTS-4 [WRONG-DESIGN] [FIX-AFTER] Python's product loader holds the engine library's entry table and an optional import of the opt-in package
- Where: `python/legend_lite/_loader.py:62-75` (`ENGINE_ARITY`, `ENGINE_NOT_PLANNING`, which name legend-engine's 8 entry points) and `:201-237` (`EngineMissing`, `_find_engine`, `engine_loaded`, with `import legend_lite_engine` at :220).
- What:
  - The product module `_loader.py` (in `_LOADER_SRCS`) knows the edge library's symbol names and where the opt-in wheel installs it.
  - The layer rule flags only an import named `edge` (`tests/test_run_closure.py:465`), so `import legend_lite_engine` passes unseen. This is the "optional import in Python" blind spot.
  - `EdgeOff.test_the_product_library_exports_no_edge_entry` (`tests/test_engine.py:778-785`) checks the product library against that same hand-kept list. Its docstring says it is "never a list kept by hand".
- Why it matters: compatibility knowledge lives in the product (the meaning rule). A new edge entry added to `EngineEntries` but not to `ENGINE_ARITY` escapes the EdgeOff check.
- Instead: move `ENGINE_ARITY`, `_find_engine` and `engine_loaded` into `legend_lite/edge/_engine.py`. `Loaded(path, arity, not_planning, setting)` already takes them. Then extend the layer rule to `legend_lite_engine`, and derive EdgeOff's list from the engine library's exports, not a constant.
- Confidence: verified.

### HOSTS-5 [WRONG-DESIGN] [FIX-AFTER] The product wheel ships the edge's Python; the `[engine]` extra adds only the native half
- Where: `python/BUILD.bazel:896-905` (`wheel_package` deps `:edge`) and `:944-946` (extra `engine` = `legend-lite-engine==0.1.0`). The edge code is `python/legend_lite/edge/_pure_v1.py`, for example `API = '/api/pure/v1/'` at :38 and upstream's header `x-legend-response-format: FormatNotSet` at :220.
- What: every `pip install legend-lite` carries legend-engine's routes and spellings under `legend_lite.edge`. They are inert until imported, and the door then refuses by name without the native library.
- Why it matters: "Product root, edge opt-in" holds at runtime but not in the artifact. Java ships the edge in its one jar by ruling 6, but the Python split was made precisely so that the opt-in is a separate wheel, and that wheel holds only the `.so`. `ShippedArtifactRootsTest` cannot see this (HOSTS-8).
- Instead: ship the edge's Python in `legend-lite-engine` (for example `legend_lite_engine/edge`, with `legend_lite.edge` as a thin import shim, or no shim), so the product wheel holds none of upstream's spellings.
- Confidence: verified.

### HOSTS-6 [WRONG-ANSWER] [FIX-AFTER] DataCube's shipped app lost its legend-engine plane: the bundle it imports by URL is not in `//datacube:dist`
- Where:
  - `datacube/demo/planners.ts:33` and `:93` import `./legend-engine.js` by URL.
  - `datacube/demo/boot.ts:55` offers the `engine` plane in the picker.
  - `datacube/demo/make-dist.mjs:57-58` copies only `bundle` and `bundle-engine` (plus the worker, vendor and widget files).
  - `datacube/BUILD.bazel:2474-2475`: `//datacube:app` uses `site = ":dist"`.
- What:
  - On `main`, `UpstreamPlanner` was in `bundle.js` (`main:datacube/demo/planners.ts:23,75`), so the plane worked in the app.
  - At the head, with `?planner=engine&engine=http://localhost:6300` (`page-config.ts:122`) and a running legend-engine, the page passes its health check and then fails `import()` with a module-fetch TypeError. It neither plans nor refuses by name.
  - The one-origin `//site:dist` does carry the file, because its copy from `datacube:site` does not leave it out (`site/make-dist.mjs:47-51`).
- Why it matters: the opt-in in one shipped artifact is broken, and with an unnamed error (G7: say it, never fail obscurely). No test loads `legend-engine.js`: grep finds only the BUILD entry, the import and the allowlist.
- Instead: copy `legend-engine.js` (and its chunks, if it splits) in `datacube/demo/make-dist.mjs`, or drop the `engine` plane from the app's picker. Add a dist test that resolves every URL `planners.ts` imports.
- Confidence: verified by reading; not run.

### HOSTS-7 [BROKEN-RULE] [FIX-AFTER] A second, undeclared way to legend-engine's routes on the server (and in Python)
- Where:
  - `core/src/main/java/com/legend/edge/EdgeServer.java:92-95`: `public static void main` serves `UPSTREAM` with no `--engine`. It ships in `//core:server_deploy.jar` via `:edge_server`.
  - Only the planted guard targets name it as a main (`core/BUILD.bazel:2849, 2860`).
  - Python's product `Engine(frames, edge=...)` takes `EngineRoutes()` directly (`python/legend_lite/engine.py`, `edge_routes` at `_edge_seam.py:49-54`), which bypasses the door's `engine_library()` pre-check (`edge/__init__.py:30-33`).
- What: `java -cp server_deploy.jar com.legend.edge.EdgeServer` mounts legend-engine's API without the declared opt-in. In Python, a direct composition degrades into 500 responses per call (`_api` -> `refusal` -> `EngineMissing`) instead of being refused at composition.
- Why it matters: this breaks the program's own rule that legend-engine's routes load only through the declared opt-in.
- Instead: delete `EdgeServer.main`, giving the planted targets a planted main of their own; keep `loopback` for tests. Make the edge door the only public way to compose `EngineRoutes`, for example a private sentinel, or a check in `Engine.__init__` that the routes came from the door.
- Confidence: verified.

### HOSTS-8 [WRONG-DESIGN] [FIX-AFTER] K2's mechanical check covers Java only; the TS and Python edge targets are outside its target list
- Where: `core/BUILD.bazel:2718-2719` (`_EDGE_TARGETS`) and `:2802-2803` (`_EDGE_TARGET_LIST`). These list core, native and wasm targets only. `//engine-client:edge`, `//datacube:edge_entry` and `//python:edge` are absent. Also `datacube/BUILD.bazel:2150-2152`: every DataCube bundle, including shipped `bundle_bundle`, lists `:edge_entry` in `srcs`.
- What:
  - `REACH_OPEN` and `ROOTS_OPEN` are empty partly because the query cannot see `//python:wheel` -> `:edge`, or `//:sites` -> `//datacube:site` -> `bundle_legend_engine` -> `:edge_entry` -> `//engine-client:edge`.
  - Adding them would not be enough. Every DataCube bundle declares `:edge_entry` as an input, so the build graph over-approximates, and whether a product bundle is edge-free is left to the source import scanner (`tools/deps/app-runs.test.ts`, with an allowlist entry for the URL import at :83).
- Why it matters: "a census of 0 proves only that the census's rules match nothing". Here the rules do not cover two of the four hosts.
- Instead: add the TS and Python edge targets to the K2 lists and declare their opt-ins (the site's `legend-engine.js`, the engine wheel). Give `src/edge/engine-plane.ts` its own esbuild target with `:edge_entry`, and take `:edge_entry` out of the product bundles' `srcs`, so the build graph proves the product bundles edge-free.
- Confidence: verified.

### HOSTS-9 [WRONG-DESIGN] [FIX-AFTER] Four hand-kept copies of the store's refusal-to-lite-shape rule; Python's bytes differ
- Where:
  - `core/src/main/java/com/legend/server/LegendHttpServer.java:249-298` (`MISSING`, `storeRoutes`)
  - `python/legend_lite/engine.py:419-440` (`_MISSING`, `_lites_refusal`)
  - `query-store/src/wasm-server.ts:73-89` (`MISSING`, `stored`)
  - `datacube/src/cube-store.ts:201-219` (`MISSING`, `lite`)
- What:
  - The Java store (`SavedQueries.answer`, `SavedCubes.answer`, compiled into the page module and the native library) answers its raw `{message, kind}`. Each host's lite mount then re-derives lite's `{kind, message}` with the same set of three kind names and the same status rule.
  - Python writes `json.dumps(...)` with default separators and `ensure_ascii`, so its refusal bytes (`{"kind": "notFound", ...}`) differ from the server's and the tab's compact JSON.
- Why it matters: one rule lives in four places, in three languages. A new missing-record kind added in Java will be wrong on three hosts. The brief asked about exactly this kind of new hand-kept copy.
- Instead: have `SavedQueries` and `SavedCubes` answer lite's shape themselves (a `LITES_SHAPE` answer beside `LITES_WORDS`), so the page module, the native `lite_store_*` entries and the server all pass it through.
- Confidence: verified.

### HOSTS-10 [WRONG-DESIGN] [FIX-AFTER] Three hand-kept parameter-binding rules, unbacked by shared cases and already divergent
- Where:
  - Java `core/src/main/java/com/legend/exec/PlanParameters.java:150-166` (typed Java values)
  - TS `engine-client/src/plan-runner.ts:41-195` (`checkParameters`, `parameterValue`)
  - Python `python/legend_lite/_params.py:63-219`
- What:
  - `_params.py:6-7` claims "the shared cases ... hold the three to one outcome". `lite-api-cases.json` has 2 cases with `params`, bindings server and python only, and neither has a wrong value.
  - Divergence 1, verified by running the regexes: Python's `\d` matches non-ASCII digits (`_params.py:24-26`), so StrictDate `"٢٠٢٢-٠١-٠١"` and Decimal `"١٢٣"` are accepted and sent to the database. TS (and JS regexes generally) refuse them by name.
  - Divergence 2: a TS `Number` parameter binds an unsafe whole JS number as a double (`plan-runner.ts:186`), while its `Integer` refuses the same value as "digits already lost".
  - Divergence 3: a Python `Float` accepts a Python `int`.
- Why it matters: this is new plan-binding knowledge copied into each host, the brief's Q4. A caller gets a database error on one host and a `parameters` refusal on another.
- Instead: one shared case file for parameter checking, read by every binding with refusals included, and `re.ASCII` (or `[0-9]`) in `_params.py`. Longer term, generate the rule's tables from Java, as `lite-facts` and `lite-raise` are.
- Confidence: verified.

### HOSTS-11 [WRONG-ANSWER] [NOTE] The three raise readers disagree on two inputs the shared fixture does not cover
- Where: TS `engine-client/src/raised.ts:47` (`.split(POSITION_SEP).pop()`, the text after the LAST separator) and `:49` (`/^[1-9]\d{0,8}$/`). Java `core/src/main/java/com/legend/exec/RaisedErrors.java:72` (`indexOf`, the FIRST) and `core/src/main/java/com/legend/sql/RaiseKind.java:70-81` (accepts `LG#01`). Python `python/legend_lite/_raised.py:24, 85-87` (FIRST separator, accepts `01`).
- What: these readers are new in this PR (the hosts read no raises on `main`). Measured with node: the message `x \u001f3:7\u001ea\u001eb\u001f` reads `"b"` in TS and `"a\u001eb"` in Java and Python. `\u001f\u001cLG#01\u001f` is site 1 in Java and Python and a plain text in TS. `raise-envelopes.json` has neither case.
- Why it matters: the tab could show a different `fail` message from the server's. This is reachable only if a user's text carries U+001E or U+001C (suspected), hence NOTE.
- Instead: take the first separator in TS and accept leading zeros, as Java does. Add both cases to `raise-envelopes.json`.
- Confidence: verified (the divergence); suspected (reachability).

### HOSTS-12 [COSMETIC] [FIX-AFTER] Stale "rooted at the edge" claims after K2, and stale route comments
- Pattern 1: 9 sites still say the shipped artifacts are rooted at the edge. Examples:
  - `core/src/main/java/com/legend/edge/EdgeServer.java:14` ("THE SHIPPED SERVER'S COMPOSITION ROOT")
  - `core/src/main/java/com/legend/server/LegendHttpServer.java:59, 187, 633` ("the shipped binary's root, the edge's")
  - `native/BUILD.bazel:55`, `native/src/edge/.../EngineEntries.java:18`, `wasm/BUILD.bazel:56`, `wasm/src/edge/.../EdgeBoundary.java:12` ("rooted at the edge as the server is")
- Pattern 2: `EdgeBoundary.java:8` has `{@link TabExports}`, a class that does not exist in `com.legend.edge.wasm`.
- Pattern 3: product files describe routes they no longer call, for example `datacube/src/services/python-host.ts:29, 642, 656, 754` (`/api/pure/v1/query`). The code calls `/api/lite/v1/queries` through the clients.
- Confidence: verified.

### HOSTS-13 [WRONG-DESIGN] [NOTE] Every TS test of the shipped server runs it with the edge on
- Where: `query-store/test/lite-server.ts:35` (used by `//query-store:lite_test`, DataCube's saved-pages server test and `//engine-client:lite_api_server_test`) and `query/demo/verify.mjs:43` (Query's end-to-end test). Both pass `--engine legend-engine`. Query needs only lite's routes; product app code has no `pure/v1` literal.
- What: the shipped default (edge off) is exercised only by `ServerBootTest.withNoEngineTheEdgeIsNotLoaded` (one `model/check` and one 404). Despite its name, that test does not check class loading.
- Instead: start the server edge-off for every lite-only suite (Query's verify, lite API conformance, saved pages), keep edge-on for the ENGINE dialect only, and rename or strengthen the boot test.
- Confidence: verified.

### HOSTS-14 [WRONG-DESIGN] [NOTE] Option A's costs, and parts of the opt-in that are not exercised
- Each opt-in module embeds the whole product compiler. `//wasm:engine_planner` goes through `:edge_tab` -> `:boundary` and `:tab`; `//native:engine` goes through `:edge_entry` -> `:entry`. So with the edge on, Python loads two GraalVM isolates and the tab two TeaVM modules, and each compiles its own models (`EdgeBoundary.HELD`, `EdgeBoundary.java:49-55`).
- The engine module's `pureV1OrError` also answers lite's own `/api/lite/v1/*` (`EdgeBoundary.java:33-41`, pinned as "lite API v1 model/check" in `wasm/engine-routes.json`). That contradicts `EngineExports`' "exports nothing else".
- No shipped app wires `engine: 'legend-engine'` (`BUILD.bazel:63-67`); only tests call `loadEngineModule`.
- No test or CI job builds or installs `//python:engine_wheel` (grep for `engine_wheel` in BUILD files and workflows finds only its definition), so `pip install legend-lite[engine]` is unproven.
- `engine-routes.json`'s pins were captured inside this PR (at `1f2842680e` and `1a96dbd2df`), so they prove the split moved no byte, not that the edge-on answers match `main`. That falls to the edge golden net.
- Confidence: verified.

### HOSTS-15 [COSMETIC] [NOTE] The Python edge reaches into product privates; there are edge-only product helpers
- Where: `python/legend_lite/edge/_pure_v1.py:30` (`_on_compiler_threads`, `_workers`), `:108, 180` (`_said`), `:206, 229` (`engine._noted`). `python/legend_lite/compiler.py:95-98`: `folded()` exists only for the edge.
- Why it matters: the edge is coupled to underscore internals of `engine.py` and `_pool.py`, which no layer rule pins.
- Instead: a small named product API the edge may use (answer, refuse, on-compiler-thread), listed as Python's `READ_API`.
- Confidence: verified.

## Cross-area observations

- **GUARDS:** at the PR head, `linux / checks` is red: `//core:guardrails_edge TIMEOUT in 300.5s` (job 134886149048). Its log line before the timeout shows the census at "lines left: 0".
- **PRODUCT/TESTS:** three further unlaned tests are in the same CI failure: `//core:lite_api_conformance_test` (the browser binding of lite API v1's shared suite), `//studio:run_test` and `//core:is_numeric_probe_test`. So in CI only Python's binding of the shared lite API suite runs.
- **EDGE:** `core/src/main/edge/META-INF/services/com.legend.testexecutionplan.ExtensionCodec` -> `com.legend.edge.EdgeCodec` ships in `//core:server`'s jar. It is a second edge `ServiceLoader` registration besides the declared `MountProvider`. It is inert with the edge off, because no server route reaches `TestPlanJson` (grep of `com/legend/server`).
- **PRODUCT:** `lite-api-cases.json` says "the words are the product's", and the hosts word lite's own refusals differently. For example a user request holding a key reads "a user request has no key who (it holds none)" in Java and TS (`LiteV1Api.java:443`, `wasm-server.ts`) and "a user request holds no key (it holds who)" in Python (`compiler.py`). This is legitimate under the rule, but worth knowing.
- **Warehouse:** its changes (parameter binding: `ApiJson.parameters`, `Conn.execute` double and boolean binds, `Statements` refusing parameters for grants and attached catalogs) are a product feature unrelated to the edge. They read correctly: unknown value types are refused by name.

## Measurements (numbers with the command that produced them)

- **Unlaned tests at the head: 8,** 5 of them the hosts' K2 and conformance proofs. Command: `gh api repos/neema2/legend-lite/actions/jobs/134886148948/logs`, lines 729-737. The log is saved under the scratchpad at `audit/verify/hosts_ci/product.log`.
- **CI at `68f20eb4d` when read:** `linux / product` failure and `linux / checks` failure; every other completed lane a success; the macOS and Windows lanes still running. Command: `gh api repos/neema2/legend-lite/commits/68f20eb4d.../check-runs`.
- **Host edge code:**

  | Host | Lines | Files |
  |---|---|---|
  | engine-client TS edge | 1,181 | 11 files |
  | DataCube edge | 300 | `datacube/src/edge/*.ts` |
  | Python edge | 338 | `legend_lite/edge/*.py` plus `legend_lite_engine` |
  | Java host edge | 509 | `wasm/src/edge`, `native/src/edge`, `EdgeServer`, `EngineMount` |

  Command: `wc -l`.
- **Stale edge-root claims after K2: 9.** Command: `grep -rn "rooted at the edge as the server\|shipped binary's root, the edge\|SHIPPED SERVER'S COMPOSITION ROOT\|THE SHIPPED SERVER'S ROOT (phase D\|mounted by the shipped binary's root"`.
- **Copies of the store refusal mapping: 4.** Command: `grep -rn "query_not_found\|QUERY_NOT_FOUND.name"` over core main, python, datacube/src, query-store/src.
- **Shared parameter cases with `params`: 2, refusals of a bad value: 0.** Command: a Python read of `lite-api-cases.json`.
- **Python vs JS digit regex:** `python3 -I -c` with `re.compile(r'(\d{4})-(\d{2})-(\d{2})').fullmatch('٢٠٢٢-٠١-٠١')` returns a match, year 2022; `node -e` with `/^-?\d+(\.\d+)?$/.test('١٢٣')` returns false.
- **Raise reader divergence:** `node -e` with `raiseIn` copied from `raised.ts` gives `{"text":"b"}` and `{"text":"\u001cLG#01"}`; the equivalent of Java's `indexOf` in Python gives `'a\x1eb'`.
- **Python arity tables vs native entry points:** a script over `native/src/{main,edge}/java/**`'s `@CEntryPoint` and `_loader.py` found no arity mismatch. The only names in neither table are `lite_free` and `lite_unfreed`; the four that looked missing were the regex missing multi-line signatures.
