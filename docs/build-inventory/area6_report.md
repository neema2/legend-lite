# Area 6: query, studio, site, engine-client, query-store, pure-protocol, sdlc-client, depot-client, tools/js, tools/browser

Repo: `the bazel/exec checkout` at `6f1d9aa9a` (branch bazel/exec). All 109 targets in
`runs/inventory/area6_targets.tsv` are covered below. Only read-only commands were run: file reads, git,
`bazel query` and `bazel aquery`.

## How to read the columns

**What makes it run TODAY** uses these codes:
- **B**: `bazel build //...`, CI's `build` lane (`.github/workflows/gates-run.yml:63`, `:163-164`). Every
  non-manual target is built there. For a test, that means it is built but never run.
- **L**: in `//gates:local` (`gates/BUILD.bazel:11-79`), either directly or through a suite.
- **checks / misc / app / browser**: CI lanes, `gates-run.yml:51`, `:62`, `:61`, `:65`.
- **gen**: through `//:generated` (`BUILD.bazel:23-40`), which runs in the checks lane and in L.
- **manual-internal**: a macro's internal target, tagged `manual`. `//...` leaves it out, but its parent pulls it
  into the build.

**Node?** (added at the coordinator's request) uses these codes:
- **none**: no Node process runs. Examples: js_library, filegroup, alias, copy_to_directory (bazel_lib's copy
  action), launchers.
- **(a)**: Node only launches a native binary.
- **(b)**: our JS/TS runs under Node.
- **(c)**: the code needs a Node-only API or package, which is named.

Node is pinned at `node.toolchain(node_version = "22.16.0")` (`MODULE.bazel:32-33`).

Verdict words are exactly the brief's.

### Shared facts (cited once, used by many rows)

**F1. esbuild 0.28.2 is a Node shim over a native binary.**
- `esbuild` npm `bin/esbuild` starts with `#!/usr/bin/env node`. It finds the platform package with
  `require.resolve` and runs it with `child_process.execFileSync(binPath, ...)`.
- Source: the `package.tgz` in `@npm_query__esbuild__0.28.2`, extracted. `ESBUILD_BINARY_PATH` overrides the lookup
  (bin/esbuild:30, :111-116).
- The native packages are in every lock, for example `query/pnpm-lock.yaml:127` (`@esbuild/darwin-arm64@0.28.2`).

**F2. typescript 7.0.2 `tsc` is the same pattern.**
- `bin/tsc` is `#!/usr/bin/env node` + `import "../lib/tsc.js"`.
- `lib/tsc.js:3-19` takes `getExePath()` (`@typescript/typescript-<platform>-<arch>`) and runs
  `execFileSync(exe, ...)`.
- Native packages: `query/pnpm-lock.yaml:304,310`, `studio/pnpm-lock.yaml:210,216`.

**F3. `node_test` adds Node-only plumbing to every js_test.** The macro (`tools/js/defs.bzl:29-63`) adds:
- `--experimental-strip-types`;
- the `--import` preload `tools/js/zone.mjs`;
- reporters `spec` + `tools/js/strict-reporter.mjs`;
- `//tools/js:{runfiles,strict_reporter,zone}`;
- with `wasm=True`, `//wasm:planner_dir` + `WASM_PLANNER` + `--experimental-wasm-exnref`.

**F4. `browser_test` = node_test + pinned Chromium.** It adds `//tools/browser:{chromium_headless_shell,
_executable, pinned_chromium}`, `PINNED_CHROMIUM` and the tag `browser`, and is compatible only with the 5 Chrome
for Testing platforms (`tools/browser/defs.bzl:18-47`).

**F5. Type checking happens only in tests.**
- esbuild strips types and does not type-check.
- The only type checks are the `tsc_test` targets (`//query:typecheck_test`, `//studio:typecheck_test`) and
  DataCube's (area 5).
- A `bazel build //...` (B) builds a tsc_test's runfiles but does not run tsc, so **the build lane is green with
  type errors**.

**F6. Who type-checks which package** (tsconfig `include` lists):

| Package files | Checked by |
|---|---|
| engine-client/src | query (`query/tsconfig.json:24`) and datacube (`datacube/tsconfig.json`) |
| pure-protocol/src | query, datacube |
| pure-protocol/test | datacube only |
| query-store/src | query, datacube |
| query-store/test | datacube only |
| sdlc-client/src, depot-client/src | studio (`studio/tsconfig.json:24`) |
| sdlc-client/test, depot-client/test | **no target** |

- `sdlc-client/tsconfig.json` (`include: ["src","test"]`) is referenced by no target (`git grep -n
  "sdlc-client/tsconfig"` is empty).
- No tsconfig has `allowJs`/`checkJs`. So none of these `.mjs` files is type-checked:
  - `query/demo/{serve,verify}.mjs`, `query/tools/icons.mjs`;
  - `studio/demo/{serve,verify}.mjs`, `site/*.mjs`;
  - `tools/js/{zone,strict-reporter}.mjs`, `tools/browser/revision_test.mjs`.
- `datacube/tsconfig.mjs.json:13` covers only datacube's own `demo/*.mjs` and `bench/*.mjs`.

**F7. The WASM runtime branches on Node.** The TeaVM runtime the tests load
(`bazel-bin/wasm/planner/wasm-gc-module-runtime.js:1108-1116`) uses `fetch(location)` in a browser and
`importNodeFs().readFile` under Node. The same module therefore runs in the page as the apps run it.

---

## Part A: one row per target (or per proven-identical group)

### depot-client (5)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//depot-client:all_files` | filegroup (guards_package) | package's files for G0 (`tools/guards/defs.bzl:109-116`) | glob `**` | file set | `//tools/guards:repository_files` → `inventory_test` (`tools/guards/BUILD.bazel:9-22`) | any file add/remove | B; checks, L via inventory_test | WIRING | none | identical in all 10 packages, see the group row below |
| `//depot-client:depot_client` | js_library | Depot client TS sources (`depot-client/BUILD.bazel:8-13`) | `src/*.ts`; dep `//sdlc-client:sdlc_client` (imports `sdlc-client/src/wire.ts`: `depot-client/src/wire.ts:4`, `client.ts:6`) | sources provider | `//studio:src`, studio tests/typecheck, own tests | n/a | B | WIRING | none | no type check of its own (F6) |
| `//depot-client:wasm_test` | js_test (node_test) | conformance suite against depot-server's rules compiled to WASM (`BUILD.bazel:16-29`) | `//sdlc-server:page_dir` (TeaVM; reaches all 33 core/base/json libs, see B6) | verdict | `:tests` | depot-client/src, sdlc-client/src, sdlc-server page/rules | built B; run misc, L | TEST-INTEGRATION | (b) node:test/assert; loads the runtime by file URL (`test/wasm.test.ts:4,11-12`) | sets `--experimental-wasm-exnref` by hand instead of `wasm=True` (`BUILD.bazel:28`) |
| `//depot-client:server_test` | js_test (node_test, medium) | same suite against `//sdlc-server:server` over HTTP (`BUILD.bazel:32-46`) | `//sdlc-client:sdlc_server`, `:server_helper`, JVM server (31 core libs) | verdict | `:tests` | depot-client/src, sdlc-server HTTP | built B; run misc, L | TEST-INTEGRATION | (c) child_process/net via server_helper (`sdlc-client/test/sdlc-server.ts:5-8,22,42,71`) | |
| `//depot-client:tests` | test_suite | both tests | | | `//gates:local:74` | | L; misc | WIRING | none | still used |

### engine-client (7)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//engine-client:all_files` | filegroup | as depot-client | | | repository_files | | B | WIRING | none | |
| `//engine-client:engine_client` | js_library | where a query runs (DuckDB-WASM, warehouse, pure/v1) (`engine-client/BUILD.bazel:17-33`) | `src/**/*.ts` incl. committed `src/generated/lite-facts.ts`; `//pure-protocol:pure_protocol`; npm `@duckdb/duckdb-wasm`, `apache-arrow` (own lock `npm_engine_client`, `MODULE.bazel:67-72`) | sources | `//query:src`, `//query:typecheck_test`, `//datacube:src_base` and 3 datacube checks | n/a | B | WIRING | none | visibility names `//studio` (`:23`) but studio imports nothing from engine-client (`grep -rln engine-client studio/src studio/demo studio/test` is empty); no test or typecheck of its own (F6) |
| `//engine-client:type_facts_main` | java_library (legend_java_library, nullaway=False) | `tools/typefacts/TypeFacts.java` (`BUILD.bazel:39-45`) | `deps = ["//core"]`; `//core:core` exports every core library (`core/BUILD.bazel:232-236`) | `libtype_facts_main.jar` | `:lite_facts` | TypeFacts.java, or the ABI of `Type`/`PlatformTypes` | B (javac on ijars of all core libs) | TOOL | n/a | uses only `com.legend.compiler.element.type.{Type,PlatformTypes}` (`TypeFacts.java:6-7`), which `//core:compiler_element_type` holds (`core/BUILD.bazel:93-97`); its Javadoc is stale: "Built by `//datacube:type_facts`... `bazel run //datacube:update_generated`" (`TypeFacts.java:26-27`) |
| `//engine-client:lite_facts` | _java_run | runs TypeFacts and writes `generated/lite-facts.gen.ts` (`BUILD.bazel:47-53`) | **every transitive runtime jar** of `//core` (`tools/java_run/defs.bzl:46,110`). `bazel aquery //engine-client:lite_facts` shows 31 core jars (incl. `libserver_lib`, `libtest`, `libtestdatagen`, `libexec`), `libbase`, `libjson`, plus the JDK | `lite-facts.gen.ts` (type-family table + `PIVOT_SEPARATOR`) | `:update_generated`, `:update_generated_test`, `:guard_classpaths` | TRUE trigger: a change to `Type.Primitive` / `PlatformTypes.VARIANT`/`ANY` / `Type.RelationType.PIVOT_SEPARATOR` (= `core/.../compiler/element/type/`), or to TypeFacts.java | **any byte change in any of 33 jars**: B, L+checks via gen | GEN-COMMITTED | n/a (JVM) | Committed output changed 4 times (`git log --follow engine-client/src/generated/lite-facts.ts`: b02032e69, 9919f90b9, be3fc8d7d, 299e7c359), each time from a generator or header edit. Since 2026-09-27, 73 commits touched `core/src/main` and 2 touched `compiler/element/type` (`git log --oneline --since=2026-09-27 -- <dir> \| wc -l`). So yes, every engine edit reruns it today. |
| `//engine-client:update_generated` | _write_source_file | `bazel run` writer of `src/generated/lite-facts.ts` (`BUILD.bazel:55-62`) | `:lite_facts` | writes the checkout | `//:update_generated` (`BUILD.bazel:59`); humans (`engine-client/README.md:14`) | same as lite_facts | B (builds lite_facts) | GEN-COMMITTED | none | writer half of the pair |
| `//engine-client:update_generated_test` | _diff_test | committed copy vs `:lite_facts` | | verdict | `:update_generated_tests` | same as lite_facts | built B; run checks + L via gen | CHECK-DIFF | none | |
| `//engine-client:update_generated_tests` | test_suite | | | | `//:generated` (`BUILD.bazel:32`) | | gen | WIRING | none | |

### pure-protocol (4)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//pure-protocol:all_files` | filegroup | as above | | | repository_files | | B | WIRING | none | |
| `//pure-protocol:pure_protocol` | js_library | V1 lambda JSON builders, no npm deps (`pure-protocol/BUILD.bazel:8-12`) | `src/*.ts` | sources | engine_client, `//query:src`, `//datacube:src_base`, query tests/typecheck, twins_test | n/a | B | WIRING | none | |
| `//pure-protocol:twins_test` | js_test (node_test wasm=True) | each builder's JSON byte-equal to the WASM grammar's (`BUILD.bazel:16-26`) | `//wasm:planner_dir` (TeaVM over 26 core/base/json libs, `bazel query 'kind(java_library, deps(//wasm:boundary)) intersect (...)' \| wc -l` = 26) | verdict | L (`gates/BUILD.bazel:68`), misc | pure-protocol/src or the planner's grammar/printer | built B; run misc, L | TEST-INTEGRATION | (b) node:test/assert + runtime by file URL (`test/lite.ts:4,14,20`) | |
| `//pure-protocol:typescript_sources` | js_library | src + test for DataCube's typecheck (`BUILD.bazel:30-34`) | `src/**`, `test/**` | sources | 24 datacube targets (typecheck plus 22 node tests, per inventory.json) | n/a | B | WIRING | none | used widely beyond typecheck: 22 datacube unit tests take it as data |

### query-store (7)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//query-store:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//query-store:query_store` | js_library | saved-query client + in-page store (`query-store/BUILD.bazel:9-13`) | `src/*.ts` | sources | `//query:src`, `//datacube:src_base`, query tests/typecheck, own tests | n/a | B | WIRING | none | |
| `//query-store:local_test` | js_test (node_test) | conformance vs `localQueryServer` + MemoryRecords (`BUILD.bazel:16-25`) | sources only | verdict | L (`:69`), misc | query-store/src | built B; run misc, L | TEST-UNIT | (b) node:test/assert only (`test/local.test.ts:4-5`); global fetch | pure browser code; could run in-page |
| `//query-store:share_test` | js_test (node_test) | share-link round trip over fixture records (`BUILD.bazel:28-38`) | `//fixtures/saved-queries:records` | verdict | L (`:70`), misc | query-store/src/share.ts, fixtures | built B; run misc, L | TEST-UNIT | (b)+(c) node:fs readFileSync of a runfile (`test/share.test.ts:4,11`) | |
| `//query-store:lite_test` | js_test (node_test, medium) | same conformance against legend-lite's server (`BUILD.bazel:41-54`) | `:legend_server` → `//core:server` (31 core libs, `bazel query 'kind(java_library, deps(//sdlc-server:server)) intersect //core/...'` gives the same 31) | verdict | L (`:58`), app lane (`gates-run.yml:61`) | query-store/src or the server's `/api/pure/v1/query` routes | built B; run app, L; **reruns on every core edit** (depends on the full server) | TEST-INTEGRATION | (c) child_process.spawn, fs.mkdtempSync, os.tmpdir (`test/lite.test.ts:4-7,23,27`) | |
| `//query-store:typescript_sources` | js_library | src + test for typechecks (`BUILD.bazel:57-64`) | | | `//datacube:typecheck_test`, `typecheck_mjs_test` | | B | WIRING | none | visibility names `//query`, but no query target uses it (inventory.json users) |
| `//query-store:legend_server` | executable_of (testonly) | the `//core:server` launcher as one file (`BUILD.bazel:67-71`) | `//core:server` | launcher | `:lite_test` | | B | WIRING | none | |

### query (40)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//query:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//query:src` | js_library | all of `query/src` (`query/BUILD.bazel:26-38`) | `src/**/*.ts,css`; deps `pure_protocol`, `query_store`, **`//datacube:src`** (imports `datacube/src/embed.ts`: `query/src/app/cube.ts:11`, `ui/results.ts:7`, `backend/cube-planner.ts:10`), `engine_client` | sources | both bundles + 3 node tests | n/a | B | WIRING | none | one library for the whole app: every query test carries all of datacube:src as runfiles |
| `//query:bundle_bundle` | _run_binary (esbuild) | `demo/main.ts` → `demo/bundle.js` (`BUILD.bazel:41-59`) | `demo/**/*.ts`, `:src` (whole closure incl. datacube), `node_modules/@duckdb/duckdb-wasm` | `demo/bundle.js` | `:site` → `//site:dist`, `:serve`, `:verify` | query/src, demo/*.ts, datacube/src, engine-client, pure-protocol, query-store, lock | B | COMPILE | (a) Node runs the esbuild shim, F1 | no type check (F5); does NOT reach wasm (reaches = npm_any only) |
| `//query:bundle_planner_worker` | _run_binary (esbuild) | `src/backend/planner-worker.ts` → `demo/planner-worker.js` | as above | `demo/planner-worker.js` | `:site` | as above | B | COMPILE | (a) F1 | `srcs` is the same as bundle_bundle (whole `:src` + duckdb-wasm), so it re-runs on every query/datacube edit even though a worker needs little of it (OPEN: the worker's real import closure; settle with `esbuild --metafile`) |
| esbuild internals ×10: `//query:bundle_bundle{__entry_point,__js_binary,_copy_srcs_to_bin,_js_info_files,_runfiles}`, `//query:bundle_planner_worker{same 5}` | directory_path / js_binary / _copy_to_bin / js_info_files / filegroup | expansion of `esbuild.esbuild` from `@npm_query//query:esbuild/package_json.bzl` (`BUILD.bazel:9,46`); every member has macro=esbuild, manual=1, same deps pattern (area6_targets.tsv) | `:src` | the esbuild launcher and staged srcs | their parent bundle | with parent | manual-internal | WIRING | `__js_binary`: (a) F1; the rest none | identical in shape, 5 per bundle |
| `//query:vendor` | copy_to_directory | `demo/vendor`: planner WASM, DuckDB WASM + workers, fonts (`BUILD.bazel:63-96`) | `//wasm:planner` (TeaVM, all core), npm duckdb-wasm, @fontsource ×3 | dir | `:site` | wasm planner rebuild, lock | B | WIRING | none | packaging, but it pulls the planner compile into the site |
| `//query:cube_styles` | copy_to_directory | DataCube CSS as `demo/cube` (`BUILD.bazel:99-104`) | `//datacube:styles` | dir | `:site` | datacube CSS | B | WIRING | none | |
| `//query:site` | filegroup | everything the browser fetches (`BUILD.bazel:107-119`) | html/json/css/models + 2 bundles + vendor + cube_styles | file set | `//site:dist`, `:serve`, `:verify` | | B | WIRING | none | this is the shipped Query app |
| `//query:serve` | js_binary | static loopback server for the site (`BUILD.bazel:121-126`) | `:site` | process | humans, `query/README.md:11-12` | by hand | B (users = 0) | TOOL | (c) node:http createServer, node:fs readFile/stat (`demo/serve.mjs:6-9,34,43,49`) | not DEAD: a README tells a human to run it |
| `//query:verify` | js_binary (tag `browser-ci`) | Playwright end to end in 3 planes: browser, legend-lite server, native warehouse (`BUILD.bazel:128-148`) | `:site`, `node_modules/playwright`, `//core:server`, `//warehouse:server_native` (native image), `//warehouse:duckdb_library` | exit code; screenshots in **`$BUILD_WORKSPACE_DIRECTORY/.scratch/verify`** (`demo/verify.mjs:29-31`) | CI browser lane by **`bazel run`** (`gates-run.yml:178-195`, query `attr(tags,"browser-ci", //query:* + //site:*)`); humans (`query/README.md:42`) | query app, any engine/warehouse change | B builds it (pulling server_native); runs only as `bazel run` in browser lane, Linux only (`gates-run.yml:67-69`) | TEST-BROWSER | (c) Playwright, node:child_process (spawns server+warehouse), node:http, node:fs (Part E) | uses **Playwright's own Chromium**, not the pinned one (no `pinned-chromium.mjs` import; `chromium.launch()` at `:442`), so it needs CI's `//datacube:install_browser` step (`gates-run.yml:149-151`); fixed ports `18090+rand` (`:25-26`). Planned as a test in workplan P4-05 (not done). |
| `//query:build_test`, `//query:load_test`, `//query:saved_queries_test` | js_test (node_test wasm=True), one expansion of `BUILD.bazel:164-182` | builder → lambda; lambda → builder state; saved records round trip, each over the real WASM grammar | `:src` (whole app + datacube), `//wasm:planner_dir`, `:demo_models`, `//fixtures/saved-queries:records`, **`:node_modules/jsdom`** | verdicts | `:tests` | query/src/builder, app; planner grammar; fixtures | built B; run misc, L | TEST-INTEGRATION | (b) node:test/assert; `test/lite.ts` uses node:fs readFileSync + runtime by file URL (`:4-5,23,26,55`); saved-queries: node:fs (`:8,24`) | **jsdom is in data but unused**: no file under `query/` mentions jsdom (`grep -rln jsdom query/test query/src query/demo query/tools` returns nothing, exit 1); added in 4a1106601 |
| `//query:typecheck_test` | js_test (tsc_test) | strict tsc over query + engine-client + pure-protocol + query-store src (`BUILD.bazel:184-205`; `tsconfig.json:24`) | the ts files, datacube:src, npm types, **@types/jsdom + jsdom (unused)** | verdict | `:tests` | any of those TS files, lock | built B (not run, F5); run misc, L | COMPILE | (a) Node runs the tsc shim, F2 | the only type check of query; expressed as a test |
| `//query:typecheck_test__entry_point` | directory_path | tsc_test internal (manual) | | | typecheck_test | | manual-internal | WIRING | none | |
| `//query:tests` | test_suite | 3 node tests + typecheck (`BUILD.bazel:207-210`) | | | L (`gates/BUILD.bazel:71`), misc (`gates-run.yml:62`) | | | WIRING | none | |
| `//query:demo_models` | filegroup | `demo/models/*.pure` for tests (`BUILD.bazel:159-162`) | | | 3 node tests | | B | WIRING | none | |
| `//query:icons_tool` | js_binary | `tools/icons.mjs`: SVG strings from react-icons (`BUILD.bazel:213-216`) | | | `:icons_gen` | | B | TOOL | (b)+(c) node:fs existsSync/readFileSync (`tools/icons.mjs:7,61-62`) | |
| `//query:react_icons` | copy_to_directory | 5 `index.mjs` sets from `@react_icons` (`BUILD.bazel:220-232`; pin `MODULE.bazel:359-367`, react-icons 5.5.0 by sha512) | upstream archive | dir | `:icons_gen` | react-icons bump | B | WIRING | none | |
| `//query:icons_gen` | _run_binary (js_run_binary) | runs icons_tool, stdout `icons.gen.ts` (`BUILD.bazel:234-240`) | `:react_icons`, `tools/icons.mjs` | `icons.gen.ts` | `:update_generated(_test)` | TRUE trigger: react-icons pin bump or the ICONS table in icons.mjs | B, gen; **already matches its true trigger** (no core in its closure; reaches is empty) | GEN-COMMITTED | (b) F above | introduced e35e32871 (P2-07) |
| icons_gen internals ×3: `//query:icons_gen_{copy_srcs_to_bin,js_info_files,runfiles}` | _copy_to_bin / js_info_files / filegroup | js_run_binary expansion, manual | | | icons_gen | | manual-internal | WIRING | none | |
| `//query:update_generated` | _write_source_file | writes `src/ui/icons.ts` (`BUILD.bazel:242-247`) | `:icons_gen` | checkout | `//:update_generated:65`; humans (`src/ui/icons.ts:2`) | as icons_gen | B | GEN-COMMITTED | none | |
| `//query:update_generated_test` | _diff_test | | | | `:update_generated_tests` | as icons_gen | gen | CHECK-DIFF | none | |
| `//query:update_generated_tests` | test_suite | | | | `//:generated:37` | | gen | WIRING | none | |

### sdlc-client (7)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//sdlc-client:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//sdlc-client:sdlc_client` | js_library | SDLC client + WASM-server adapter (`sdlc-client/BUILD.bazel:9-13`) | `src/*.ts` | sources | studio src/typecheck, depot_client, tests | n/a | B | WIRING | none | `sdlc-client/tsconfig.json` is orphaned (F6) |
| `//sdlc-client:wasm_test` | js_test (node_test) | conformance vs sdlc-server rules in WASM (`BUILD.bazel:32-44`) | `//sdlc-server:page_dir` (all 33 libs, B6) | verdict | `:tests` | sdlc-client/src, sdlc-server rules | built B; run misc, L | TEST-INTEGRATION | (b) node:test/assert, runtime by file URL (`test/wasm.test.ts:4-6,14,17`) | |
| `//sdlc-client:server_test` | js_test (node_test, medium) | conformance + origin/host/path guards vs the JVM server (`BUILD.bazel:16-29`) | `:sdlc_server`, `:server_helper`, `//sdlc-server:server` | verdict | `:tests` | sdlc-client/src, sdlc-server HTTP | built B; run misc, L; reruns on every core edit | TEST-INTEGRATION | (c) node:http raw `request` with Origin/Host headers that "fetch will not let a test set" (`test/server.test.ts:7,27-38`); node:fs canary write/read (`:6,61,69`) | |
| `//sdlc-client:sdlc_server` | executable_of (testonly) | sdlc-server launcher as one file (`BUILD.bazel:47-55`) | `//sdlc-server:server` | launcher | sdlc/depot server_test, `//studio:verify_test` | | B | WIRING | none | |
| `//sdlc-client:server_helper` | js_library (testonly) | `test/sdlc-server.ts`: start/stop the model-home server (`BUILD.bazel:58-66`) | | | sdlc/depot server_test, studio verify_test | | B | TOOL | (c) child_process spawn/spawnSync(taskkill), net.createServer free port, fs.mkdtempSync, os.tmpdir, process.env/platform (Part E) | |
| `//sdlc-client:tests` | test_suite | | | | L (`:75`), misc | | | WIRING | none | |

### site (4)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//site:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//site:dist` | copy_to_directory | both apps under one origin, for any static host (`site/BUILD.bazel:9-18`) | `index.html`, `//datacube:site`, `//query:site` | `dist/` | `:serve`, `:verify`; humans per the BUILD comment | any app change | B | WIRING | none | the shipped static product; no deploy workflow (`.github/workflows` = diagnostics, gate, gates-run) |
| `//site:serve` | js_binary | loopback server for dist (`BUILD.bazel:20-25`) | `:dist` | process | humans (BUILD comment, `serve.mjs:1`) | by hand | B (users = 0) | TOOL | (c) node:http, node:fs (`serve.mjs:4-7,35,44,46`) | only its own comment documents it, no README |
| `//site:verify` | js_binary (tag `browser-ci`) | saved in Query, opened in DataCube, one origin; share links; remote file (`BUILD.bazel:27-36`) | `:dist`, `//datacube:node_modules/playwright` | exit code | CI browser lane by **`bazel run`** (`gates-run.yml:178-195`) | either app | B; browser lane by `bazel run` | TEST-BROWSER | (c) Playwright via `createRequire('../datacube/node_modules/')` (`verify.mjs:15`), node:http, node:fs, Buffer (Part E) | **Playwright's own Chromium** (`chromium.launch()` at `:48`), so it needs install_browser; reaches across packages into `../datacube/node_modules` (workplan P4-05, HN-N3) |

### studio (33)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//studio:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//studio:src` | js_library | Studio sources (`studio/BUILD.bazel:17-24`) | `src/**`; `//depot-client:depot_client`, `//sdlc-client:sdlc_client` | sources | 3 bundles, 2 node tests | n/a | B | WIRING | none | |
| `//studio:bundle` | _run_binary (esbuild) | `demo/main.ts` → `bundle.js` + `bundle.css`, Monaco inlined, `.ttf` as dataurl (`BUILD.bazel:27-41`) | `demo/**/*.ts`, `:src`, `node_modules/monaco-editor` | 2 files | `:site` | studio/src, demo, sdlc/depot-client, lock | B | COMPILE | (a) F1 | |
| `//studio:bundle_planner_worker`, `//studio:bundle_editor_worker` | _run_binary (esbuild), one comprehension `BUILD.bazel:43-59` | compiler worker; Monaco's editor worker | `:src`, monaco-editor | `planner-worker.js`, `editor.worker.js` | `:site` | as above | B | COMPILE | (a) F1 | both take all of `:src` + monaco as srcs (same OPEN as query's worker) |
| esbuild internals ×15: `//studio:bundle{__entry_point,__js_binary,_copy_srcs_to_bin,_js_info_files,_runfiles}` and the same 5 for `bundle_planner_worker` and `bundle_editor_worker` | as query | `esbuild.esbuild` expansion (macro=esbuild, manual=1, tsv) | | | parent | with parent | manual-internal | WIRING | `__js_binary`: (a); rest none | identical shape |
| `//studio:vendor` | copy_to_directory | planner WASM, **SDLC page WASM**, fonts (`BUILD.bazel:63-87`) | `//wasm:planner`, `//sdlc-server:page` (both TeaVM over core) | dir | `:site` | wasm rebuilds | B | WIRING | none | |
| `//studio:site` | filegroup | everything the browser fetches (`BUILD.bazel:90-99`) | | | `:serve`, `:verify_test` | | B | WIRING | none | not part of `//site:dist` |
| `//studio:serve` | js_binary | loopback server (`BUILD.bazel:101-106`) | `:site` | process | humans, `studio/README.md:8` | by hand | B (users = 0) | TOOL | (c) node:http, node:fs (`demo/serve.mjs:7-10,35,44,50`) | |
| `//studio:verify_test` | js_test (browser_test, large) | whole loop in **pinned** Chromium at level 0 (page) and level 1 (sdlc-server) (`BUILD.bazel:108-122`) | `:site`, playwright, `//sdlc-server:server`, `:server_helper`, pinned chromium (F4) | verdict, screenshots in TEST_UNDECLARED_OUTPUTS_DIR (`demo/verify.mjs:23`) | browser lane (`gates-run.yml:65`), `studio/README.md:12` | studio, sdlc/depot-client, sdlc-server, planner | built B; run browser lane (Linux); not in L, not in `:tests` | TEST-BROWSER | (c) Playwright + node:http + node:fs + server_helper (Part E) | the only browser test in this area that is a real test |
| `//studio:demo_test`, `//studio:workspace_test` | js_test (node_test wasm=True), one comprehension `BUILD.bazel:125-142` | demo projects published through the page SDLC; workspace model over both WASM modules | `:src`, `:demo_projects`, `//sdlc-server:page_dir`, `//wasm:planner_dir`, depot_client | verdicts | `:tests` | studio/src, sdlc/depot-client, planner or SDLC page | built B; run misc, L | TEST-INTEGRATION | (b) node:test/assert; demo: node:fs readFile (`test/demo.test.ts:6,17-18`); modules.ts: runtime by file URL (`:5,16,35,54`) | |
| `//studio:demo_projects` | filegroup | `demo/projects/**` (`BUILD.bazel:145-148`) | | | 2 node tests | | B | WIRING | none | |
| `//studio:typecheck_test` | js_test (tsc_test) | strict tsc, studio + sdlc/depot-client src (`BUILD.bazel:150-167`) | | | `:tests` | those TS files, lock | built B; run misc, L | COMPILE | (a) F2 | does not cover sdlc/depot-client tests (F6) |
| `//studio:typecheck_test__entry_point` | directory_path | internal (manual) | | | | | manual-internal | WIRING | none | |
| `//studio:tests` | test_suite | demo, typecheck, workspace (`BUILD.bazel:170-177`) | | | L (`:77`), misc | | | WIRING | none | |

### tools/browser (5)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//tools/browser:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//tools/browser:chromium_headless_shell` | alias (platform_select) | every file of the pinned shell (`tools/browser/BUILD.bazel:24-29`); repos from `extensions.bzl:44-67`, pin rev 1243 / CfT 153.0.8010.12 (`MODULE.bazel:389-401`) | `@chromium_headless_shell_<plat>//:files` | files | 11 datacube browser tests + `//studio:verify_test` | pin bump | B (fetch + analysis) | WIRING | none | |
| `//tools/browser:chromium_headless_shell_executable` | alias | the executable alone (`BUILD.bazel:31-36`) | | | same 12 | | B | WIRING | none | |
| `//tools/browser:pinned_chromium` | js_library | `pinned-chromium.mjs`: points Playwright at the pinned shell, moves TMPDIR into TEST_TMPDIR, Windows MAX_PATH guard (`pinned-chromium.mjs:1-50`) | | | 25 targets: studio verify_test plus datacube harnesses/tests (inventory.json) | | B | TOOL | (c) node:fs existsSync/realpathSync, process.env (13 uses), process.platform; exists only because Playwright is a Node library | |
| `//tools/browser:revision_test` | js_test (node_test, size unset) | each lock's playwright-core `browsers.json` revision/version equals the pin; every lock in the inventory is listed (`BUILD.bazel:43-79`, `revision_test.mjs:1-11`) | 3 `node_modules/playwright`, 4 locks, `@chromium_pin//:pin.json`, `@repo_inventory//:files.txt` | verdict | checks (`gates-run.yml:51`), L (`:26`) | lock or pin change, a new lock | built B; checks, L | CHECK-GUARD | (c) node:module createRequire, node:fs (`revision_test.mjs:12-14,40-43`) | there is no "install" target here: the install step is `//datacube:install_browser` (area 5), used by CI for query/site verify |

### tools/js (5)

| target | kind | what it is | reads | produces | used by | SHOULD run when | TODAY | verdict | Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `//tools/js:all_files` | filegroup | | | | repository_files | | B | WIRING | none | |
| `//tools/js:runfiles` | js_library | `runfiles.mts`: rlocation lookup through `JS_BINARY__RUNFILES` (`runfiles.mts:1-30`) | | | 162 targets (every node_test via F3, plus typechecks) | | B | TOOL | (c) node:fs/path/url, process.env | needed only because tests read inputs from disk |
| `//tools/js:strict_reporter` | js_library | node:test reporter that sets the exit code on any `test:fail` (`strict-reporter.mjs:1-20`) | | | 138 targets (every node_test) | | B | TOOL | (c) node:test reporter API, process.exitCode | works around Node 22 exiting 0 when a suite throws (its own comment) |
| `//tools/js:zone` | js_library | preload: `TZ = LEGEND_TZ` (`zone.mjs`) | | | 138 | | B | TOOL | (c) process.env | works around Windows' MSYS launcher dropping TZ (117179fa8) |
| `//tools/js:lock_matches_package_json_test` | js_test (node_test) | each of 4 package.json files vs its pnpm lock (`BUILD.bazel:26-56`) | datacube/query/studio/engine-client package.json + lock | verdict | checks (`:51`), L (`:29`) | a package.json or lock edit | built B; checks, L | CHECK-GUARD | (b)+(c) node:fs readFileSync (`lock-matches-package-json.test.ts:7,40,43`); text comparison only | |

### guards_package group row (proof that the 10 `all_files` are identical)

These 10 targets all come from `guards_package()` with the same deps pattern (0 deps, 1 user =
`//tools/guards:repository_files`, per inventory.json): `//depot-client:all_files`, `//engine-client:all_files`,
`//pure-protocol:all_files`, `//query-store:all_files`, `//query:all_files`, `//sdlc-client:all_files`,
`//site:all_files`, `//studio:all_files`, `//tools/browser:all_files`, `//tools/js:all_files`.

### Verdict counts (109)

| verdict | n | members |
|---|---|---|
| WIRING | 68 | 10 all_files; 9 source js_libraries; 2 launchers; 6 test_suites; 25 esbuild internals; 3 icons_gen internals; 2 tsc entry points; 9 copies/filegroups (query vendor/cube_styles/react_icons/site/demo_models, site:dist, studio vendor/site/demo_projects); 2 chromium aliases |
| COMPILE | 7 | query bundle_bundle, bundle_planner_worker; studio bundle, bundle_planner_worker, bundle_editor_worker; query/studio typecheck_test |
| TOOL | 10 | type_facts_main, icons_tool, query/studio/site serve, server_helper, runfiles, strict_reporter, zone, pinned_chromium |
| GEN-COMMITTED | 4 | engine-client lite_facts + update_generated; query icons_gen + update_generated |
| CHECK-DIFF | 2 | engine-client, query update_generated_test |
| CHECK-GUARD | 2 | revision_test, lock_matches_package_json_test |
| TEST-UNIT | 2 | query-store local_test, share_test |
| TEST-INTEGRATION | 11 | depot wasm/server; pure-protocol twins; query-store lite; query build/load/saved_queries; sdlc wasm/server; studio demo/workspace |
| TEST-BROWSER | 3 | query:verify, site:verify (both `bazel run` harnesses), studio:verify_test |
| DEAD / OPEN | 0 / 0 | the three `serve` binaries have 0 users but are documented (query and studio READMEs, site's BUILD comment), so they are not DEAD |

---

## Part B: the problems, with evidence

**B1. `lite_facts` reruns on every engine edit; its true trigger is one directory.**
- `type_facts_main` deps `//core`, which exports all core libraries (`engine-client/BUILD.bazel:44`,
  `core/BUILD.bazel:232-236`).
- `java_run` takes **full transitive runtime jars** as inputs (`tools/java_run/defs.bzl:46,110`).
- `bazel aquery //engine-client:lite_facts` lists 31 core jars + base + json, including `libserver_lib`,
  `libtest`, `libtestdatagen`, `libexec` and `libplanner`.
- The program reads only `Type.Primitive`, `PlatformTypes.VARIANT/ANY` and `Type.RelationType.PIVOT_SEPARATOR`
  (`TypeFacts.java:39-53,80`). All of these are in `//core:compiler_element_type`, whose closure is 11 in-repo
  libraries plus json (`bazel query 'kind("java_library", deps(//core:compiler_element_type))'`).
- History: 73 core commits since 2026-09-27, 2 of them in that directory; the committed output never changed
  because of core.
- It runs in B, in L and in checks (through `//:generated`).

**B2. The build lane does not type-check TypeScript** (F5).
- esbuild bundles without types.
- `typecheck_test` runs only in misc and L.
- A type error passes `bazel build //...`.

**B3. Type-check coverage has holes and duplicates** (F6).
- **Holes:** sdlc-client/test and depot-client/test are checked by no target; `sdlc-client/tsconfig.json` is
  orphaned; the harness `.mjs` files are unchecked.
- **Duplicates:** engine-client/src, pure-protocol/src and query-store/src are each checked twice (query and
  datacube), under different `lib` settings (`datacube/tsconfig.json` has no `DOM.Iterable`; query has it). So a
  library's correctness depends on its consumers' configs.
- **No check of their own:** engine-client, pure-protocol, query-store and depot-client have none.

**B4. Two browser harnesses run by `bazel run` in CI, not as tests.**
- The step runs the query `attr(tags,"browser-ci", //query:* + //site:*)` and calls `bazel run` on each result
  (`gates-run.yml:178-195`).
- Both use Playwright's own Chromium, so they need the `install_browser` step (`gates-run.yml:149-151`). `query:verify`
  imports `playwright` directly (`demo/verify.mjs:18`); site reaches into DataCube's node_modules
  (`site/verify.mjs:15`).
- `query:verify` writes into the checkout (`:29-31`), uses fixed ports (`:25-26`) and uses `../..` runfiles
  arithmetic (`:21-24`).
- Neither runs in L. Neither is cached. A human gets no `bazel test` verdict.
- Workplan P4-05/P4-09 plans the fix (`BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:2181-2192,2235-2242`).
  `BAZEL_EXECUTION_LOG.md` has no P4-05 entry yet.
- `docs/GATES.md:35` still says "every `//datacube`... target tagged browser-ci", but CI's query covers only
  `//query` and `//site`.

**B5. `bazel build //...` builds the native warehouse because of `query:verify`.**
- The js_binary is non-manual, and its data includes `//warehouse:server_native` and `//core:server`
  (`query/BUILD.bazel:130-148`). (Area 4 also builds server_native through `//warehouse:*`, so the extra cost may
  be zero.)
- `site:verify` and the `serve` binaries likewise pull the whole site (the planner WASM, so all of core via TeaVM)
  into B.

**B6. Wasm and server tests here rerun on every core edit, because their WASM or server inputs depend on all of core.**
- `//sdlc-server:page` closes over all 33 libraries: `bazel query 'kind(java_library, deps(//sdlc-server:page_boundary)) intersect (//core/...+//base/...+//json/...)'`
  lists `//core:core`, `server_lib`, `test`, `testdatagen`, and so on.
- So `sdlc-client:wasm_test`, `depot-client:wasm_test`, `studio:{demo,workspace}_test`, and Studio's vendor and
  site, rebuild and rerun on any core change.
- `//wasm:planner` closes over 26 libraries.
- The server tests use `//core:server` (31) or `//sdlc-server:server` (31).
- Deciding these closures is area 3 (wasm/TeaVM) and area 4 (servers). It is recorded here because it decides
  when this area's 11 integration tests run.

**B7. `query:src` is one library over the whole app plus all of `//datacube:src`.**
- The 3 query node tests take `:src` as data (`query/BUILD.bazel:169`), so any datacube/src edit reruns them.
- The worker bundles take the same `srcs` as the page bundle (`query/BUILD.bazel:48`, `studio/BUILD.bazel:45`).
- OPEN: whether a test's import closure reaches datacube. Settle it with `esbuild --metafile` on each test entry,
  or with a tsc `--listFiles` per entry.

**B8. Unused and stale inputs.**
- query node tests and the query typecheck take `jsdom` and `@types/jsdom`, which nothing imports (B-row evidence).
- engine-client's visibility for `//studio` is unused.
- `//query-store:typescript_sources`'s visibility for `//query` is unused.
- TypeFacts' Javadoc names `//datacube:type_facts` (`TypeFacts.java:26-27`).
- `revision_test` has no `size`.

**B9. The test-support code exists only to make Node behave.**
- `strict_reporter` works around node:test's exit code.
- `zone` works around the MSYS launcher dropping TZ.
- `runfiles.mts` exists because tests read disk.
- `pinned_chromium` exists because Playwright is a Node package.
- Each is a dependency of 138-162 targets.

---

## Part C: the right shape (proposals, each with its evidence)

1. **engine-client facts.**
   - Change `type_facts_main` deps to `//core:compiler_element_type` (B1).
   - Keep `lite_facts` and its diff test in a "generated" suite outside the everyday build (tag `manual`, listed in
     `//:generated`). It then runs only when a file in `core/.../compiler/element/type/` or TypeFacts.java changes,
     and the action cache keys on 13 jars, not 33.
   - Fix the Javadoc.
   - Evidence: B1. Depends on area 1 (core) keeping `compiler_element_type` a stable small target.
2. **TS "compile" is the type check, as a build action.**
   - Replace each `tsc_test` with a `js_run_binary` (or a native-binary run, see Part D) that runs `tsc -p ... --noEmit`
     and declares a stamp output in the package's default outputs. Then `bazel build //...` fails on a type error.
   - Give every library package its own strict tsconfig plus check: engine-client, pure-protocol, query-store,
     sdlc-client (using its existing orphan tsconfig), depot-client, each covering `src` and `test`.
   - The app checks then cover only their own dirs, which closes B3's holes and removes the duplicates.
   - Evidence: F5, F6, B2, B3.
3. **Bundles stay COMPILE.** Five esbuild runs. Narrow a worker's `srcs` once its import closure is known (OPEN in
   B7).
4. **Split `query:src`.**
   - Use at least `builder` (pure, no datacube) and `app/ui` (datacube embed). Then the 3 node tests depend on the
     builder only.
   - Drop jsdom from tests and typecheck.
   - Evidence: B7, B8.
5. **Tests by kind, with suites that say so.**
   - unit (no wasm, no server): `query-store:{local,share}_test`.
   - wasm-integration (trigger: package src or planner/SDLC page): twins, query ×3, studio ×2, sdlc/depot wasm.
   - server-integration (trigger: package src or the server's routes): query-store lite, sdlc/depot server.
   - browser: studio verify_test, plus query:verify and site:verify converted to `browser_test` on the pinned
     Chromium (P4-05), with ports at 0, TEST_TMPDIR, and no `../datacube/node_modules`.
   - Then delete the `bazel run` loop and `install_browser` (P4-09).
   - Evidence: B4, F4.
6. **The B6 cross-area items.**
   - Area 3 decides `//sdlc-server:page`'s closure (it carries `server_lib`, `test`, `testdatagen`).
   - Area 4 decides whether the servers need all of core.
   - Until then, the 11 integration tests rerun on every engine edit.
7. **Keep the guards** (`revision_test`, `lock_matches_package_json_test`) in checks, with trigger = lock or
   package.json change. They already depend on nothing else (data lists in `tools/browser/BUILD.bazel:47-58`,
   `tools/js/BUILD.bazel:30-39`).
8. **Remove stale visibility** (engine-client → studio, query-store typescript_sources → query) and set
   `revision_test` size.

---

## Part D: what these packages need Node for, and what each use would take to remove

| use | targets | class | evidence | what removal takes |
|---|---|---|---|---|
| esbuild launcher | 5 bundles (+ their `__js_binary`) | (a) | F1: Node shim then `execFileSync` of `@esbuild/<platform>` | run the platform package's binary directly from a `run_binary`/genrule (select over the 5 platform packages already in each lock), or set `ESBUILD_BINARY_PATH` (shim line 30). esbuild reads `node_modules` as plain files, so no Node is needed at bundle time. |
| tsc launcher | 2 tsc_tests (and datacube's) | (a) | F2: `lib/tsc.js` runs `getExePath()` then `execFileSync` | invoke `@typescript/typescript-<plat>-<arch>`'s exe directly. It resolves `@types/node` only because `types: ["node"]` is in each tsconfig, and that disappears once tests stop using node:* APIs. |
| unit/integration tests | the 11 node_tests that start no server (15 node_tests in all) | (b) | node:test + node:assert everywhere; plus node:fs (runfile reads), file-URL WASM loading (Part E) | the code under test is browser code: query-store's local server is IndexedDB/fetch based; the sdlc WASM server persists to IndexedDB (`sdlc-client/src/wasm-server.ts`, `records.ts`); the TeaVM runtime has a browser branch (F7). In Chromium it needs: (1) a test runner and assert in the page (node:test/assert are unavailable); (2) inputs served over HTTP instead of read with node:fs/runfiles; (3) a driver to start Chromium, collect results and exit with a code. The driver is the CDP client the user chose. |
| tests that start servers | query-store lite, sdlc/depot server, studio verify (level 1), query:verify | (c) child_process, net, os, fs.mkdtemp | `query-store/test/lite.test.ts:4-7,27`; `sdlc-client/test/sdlc-server.ts:5-8,42,71` | the driver (not the page) must start, stop and health-check JVM and native servers, make temp dirs and Windows tree-kill. That is privileged work, and it stays in the driver. |
| raw HTTP with forbidden headers | sdlc-client server_test (origin/host guards) | (c) node:http | `server.test.ts:27-38` sends `Origin` and `Host` | a page cannot set these headers (the test's own comment, `:27`). The case moves to the driver or to a JVM test of sdlc-server. |
| filesystem assertions | sdlc server_test canary | (c) node:fs | `server.test.ts:61,69` | driver-side or JVM test |
| static servers | query/studio/site serve, plus the in-harness servers in 3 verify scripts | (c) node:http + node:fs | serve.mjs files; Part E | any static server the driver or a server already in the repo provides. `site:dist` is already a plain folder. |
| browser automation | query:verify, site:verify, studio:verify_test, pinned_chromium | (c) Playwright | Part E | the CDP client must cover Part E's union. pinned-chromium.mjs and revision_test exist only for Playwright and go with it. |
| generator | icons_tool | (b)+(c) node:fs | `tools/icons.mjs:7,61-62` | GEN-COMMITTED, triggered only by a react-icons bump. Rewrite it in the repo's JVM generator style (java_run), or run it in the page through the driver with the react-icons files served. |
| guards | lock_matches_package_json_test, revision_test | (c) node:fs, node:module | lines above | pure text/JSON reads: rewrite as JUnit guards like `//tools/guards:*`. revision_test is moot once Playwright is gone. |
| test plumbing | runfiles, strict_reporter, zone | (c) | B9 | gone with Node tests. Inputs become served URLs, and the exit code comes from the driver. |
| pnpm/npm repos | `npm_query`, `npm_studio`, `npm_engine_client` (`MODULE.bazel:48-72`) | — | | still needed for the files: duckdb-wasm, apache-arrow, monaco, fonts, and the esbuild/tsc native packages. Node itself is not needed to read them. |

---

## Part E: every Playwright and Node use, per harness and per js_test

Method: the counts come from a regex scan of each file (script in the session scratchpad). Comment lines are
skipped. A count is call sites; file:line lists are exact. Selector features count Playwright-only selector
syntax inside selector strings.

### E1. `query/demo/verify.mjs` (run by `bazel run` in CI's browser lane)

**Playwright:**
- Launch and lifecycle:
  - `chromium.launch` 1 [442]
  - `newContext({viewport})` 1 [139]
  - `newPage` 1 [142]
  - `goto` 15 [166,175,181,190,216,226,231,239,263,297,325,346,359,398,427]
  - `reload` 2 [207,286]
  - `page.goto`/`reload` wrapped so that every load re-answers sign-in [144-148]
  - `close` 3 [161,437,448]
  - `on('pageerror')` 1 [151]
- Input:
  - `click` 50 (from 109 to 421)
  - right-click `{button:'right'}` 6 [195,242,275,305,410,418]
  - `dblclick` 12 [192,241,265,299,300,329,348,350,361,382,400,429]
  - `hover` 1 [411]
  - `fill` 15 [128,129,203,218,249,256,269,271,283,302,308,321,343,349,376]
  - `press(selector,'Tab')` 7 [219,257,272,303,309,322,344]
  - `selectOption` 7 [169,197,244,250,270,277,384]
  - `check` 1 [334]
- Waits:
  - `waitForSelector` 24 [167,170,176,182,191,208,217,221,224,227,232,240,255,264,274,287,298,326,347,360,399,420,422,428]
  - `waitForFunction` 7 [110,184,205,285,292,404,413]
- Reads:
  - `textContent` 13
  - `inputValue` 2 [171,209]
  - `locator(...).count` 3 [183,210,406]
  - `$` (ElementHandle) 1 [112]
  - `$$eval` 2 [118,123]
  - `evaluate` 1 [206]
- Artifacts: `screenshot` 1 [159]
- Playwright selector syntax:
  - `:has-text()` 36
  - `:text-is()` 7 [291,293,403,411,412,419,421]
  - `text=` 1 [182]
  - `>> nth=` 9 [250,301,371,374,386,410,418]
  - `:visible` 6 [406,420]
  - `:has()` 3 [411,412,419]
  - `:text()` 1 [224]

**Node:**
- `node:child_process` spawn 2: legend-lite server [34] and native warehouse [47], with stdout/stderr parsing
  [39-40,53-58] and `kill` [450-451]
- `node:http` createServer 1 [66], which also rewrites the config JSON on the fly [69-80]
- `node:fs` readFile 3 [70,76,85], stat 1 [84], mkdirSync/mkdtempSync 2 [30,31]
- `node:path`, `node:url`, `node:assert`
- `process.env` 4 [29,35,47,49]
- `process.exit` [456]
- global `fetch` health poll [94]

### E2. `site/verify.mjs` (run by `bazel run` in CI's browser lane)

**Playwright:**
- Launch and lifecycle:
  - `chromium.launch` 1 [48]
  - `newContext` 5 [49,109,178,207,239]: separate contexts stand for "another browser" with empty storage. One
    has `permissions: ['clipboard-read','clipboard-write']` [49].
  - `newPage` 8 [57,68,112,129,150,180,209,241]
  - `goto` 9
  - `reload` 1 [256]
  - `close` 5
  - `on('pageerror')` 8
  - `bringToFront` 3 [86,103,140]
- Input:
  - `click` 36
  - right-click 3 [73,170,231]
  - `dblclick` 1 [72]
  - `hover` 6 [62,142,163,171,215,232]
  - `fill` 4 [80,192,227,252]
  - `keyboard.press('Escape')` 3 [148,188,238]
  - `selectOption` 1 [75]
- Waits:
  - `waitForSelector` 11, of which 2 use `{state:'detached'}` [229,269]
  - `waitForFunction` 11
  - `locator.waitFor` 8, of which 3 are detached [93,195,254]
- Locators:
  - `locator` 48
  - `{has/hasText/hasNot}` options 18
  - `first` 9, `last` 2 [94,263], `nth` 2 [170,231]
  - `filter` 4 [172,233]
  - `count` 2 [134,155]
- Reads:
  - `textContent` 4
  - `allTextContents` 1 [96]
  - `inputValue` 4 [123,177,237,261]
  - `evaluate` 4 [107,136,147,262]: 2 of them read `navigator.clipboard.readText()` in the page [107,147]
  - `evaluateAll` 1 [97]
  - `$$eval` 2 [78,120]
- Selector syntax: `:has-text()` 7, `:text-is()` 8, `:scope` 8

**Node:**
- `node:module` createRequire into `../datacube/node_modules` [15]
- `node:http` createServer 1 [25], which serves `dist/` and a fake remote CSV that can be told to refuse (403) [22-32,255,267]
- `node:fs` readFile [40], stat [38]
- `Buffer.byteLength` [29]
- `process.exit` [287]

### E3. `studio/demo/verify.mjs` (`//studio:verify_test`, a real test on the pinned Chromium)

**Playwright:**
- Launch and lifecycle:
  - imports `pinned-chromium.mjs` first [8]
  - `chromium.launch` 1 [107]
  - `newContext` 2 [111,114], each with its own IndexedDB
  - `newPage({viewport})` 3 [48,111,114]
  - `goto` 1 [55]
  - `close` 2
  - `on('pageerror')` 1 [50]
- Input:
  - `click` 16
  - `fill` 5 [63,69,81,87,92]
  - `keyboard.press('ControlOrMeta+A')` 1 [72] and `keyboard.type(...)` 1 [73], both into Monaco
- Waits: `waitForSelector` 3 [65,84,89], `waitForFunction` 2 [53,94]
- Locators: `locator` 10, `getByTestId` 14
- Reads: `textContent` 3 [52,90,96]
- Artifacts: `screenshot` 1 call site [51], used 4 times [60,78,95,100]

**Node:**
- `node:http` createServer [30]
- `node:fs` readFile/stat [36-37], mkdirSync [24]
- `node:os` tmpdir
- `process.env` (TEST_UNDECLARED_OUTPUTS_DIR, TEST_TMPDIR) [23]
- `startSdlcServer` from server_helper [18,113,120]

### E4. `sdlc-client/test/sdlc-server.ts` (`//sdlc-client:server_helper`)

No Playwright.

**Node:**
- `child_process.spawn` (the launcher, with RUNFILES_DIR) [42]
- `spawnSync(taskkill /t /f)` on Windows [71]
- `net.createServer` free-port probe [22]
- `fs.mkdtempSync` [41], `os.tmpdir`
- `process.env` 6 [16,39,41,43]
- `process.platform` [64]
- global `fetch` health [53,80]
- `runfileFromEnv` [42]

### E5. js_tests (none use Playwright)

| target | Node APIs (file:line) |
|---|---|
| `//query:build_test` | node:test, node:assert (`build.test.ts:4-5`); through `test/lite.ts`: node:fs readFileSync [55], node:url fileURLToPath [5,26], dynamic `import()` of the runtime by file URL [23], runfiles [17,55] |
| `//query:load_test` | node:test/assert (`load.test.ts:4-5`) + `lite.ts` as above |
| `//query:saved_queries_test` | node:test/assert, node:fs readFileSync (`saved-queries.test.ts:7-9,24`), runfiles [24] + `lite.ts` |
| `//studio:demo_test` | node:test/assert, node:fs/promises readFile, node:url (`demo.test.ts:5-8,16-18`); `modules.ts`: node:url, runtime import [5,16], runfiles [35,54] |
| `//studio:workspace_test` | node:test/assert (`workspace.test.ts:5-6`) + `modules.ts` |
| `//query-store:local_test` | node:test/assert (`local.test.ts:4-5`), global fetch [34]; `conformance.ts` node:test/assert [6-7] |
| `//query-store:share_test` | node:test/assert, node:fs readFileSync (`share.test.ts:3-5,11`), runfiles [11] |
| `//query-store:lite_test` | node:test, child_process.spawn [4,27], fs.mkdtempSync [5,23], os.tmpdir [6], path [7], process.env 4 [14,23,28], fetch [44,66] |
| `//pure-protocol:twins_test` | node:test/assert (`twins.test.ts:5-6`); `lite.ts`: node:url, runtime import, runfiles [4,14,20] |
| `//sdlc-client:wasm_test` | node:test/assert, node:url (`wasm.test.ts:4-6`), runtime import [17], runfiles [14], fetch 5 [36,39,44,149,150] |
| `//sdlc-client:server_test` | node:test/assert, node:http request [7,32], node:fs writeFileSync/readFileSync [6,61,69], node:path [8], plus server_helper (E4) |
| `//depot-client:wasm_test` | node:url, runtime import [4,12], runfiles [11]; `conformance.ts` node:test/assert, fetch 3 [25,41,52] |
| `//depot-client:server_test` | node:test [4] + server_helper (E4) |
| `//tools/js:lock_matches_package_json_test` | node:test/assert, node:fs readFileSync [7,40,43], node:path [8], runfiles [30,31] |
| `//tools/browser:revision_test` | node:module createRequire [12,40,42] (resolves `playwright-core`), node:fs readFileSync 4 [19,24,34,43], process.env [18], process.exitCode [50] |
| `//query:typecheck_test`, `//studio:typecheck_test` | Node only launches the tsc exe (F2) |
| every node_test (F3) | preload `zone.mjs` (process.env [5-6]); reporter `strict-reporter.mjs` (process.exitCode [15]); `runfiles.mts` (node:fs/path/url, process.env [8-10,13,30,37,65]) |

### E6. The union, and where each operation can live

**What a CDP driver must provide.** This is the union over E1-E3 (CDP domain names in parentheses are the
standard mapping, not repo evidence):

1. **Launch and lifecycle.**
   - Start the pinned chromium-headless-shell (`--remote-debugging-pipe`).
   - Open several isolated browser contexts with separate storage. site uses 5, studio 2, query 1, and each
     context stands for "another browser" (`Target.createBrowserContext`).
   - Open pages in a context with a viewport (`Target.createTarget`, `Emulation.setDeviceMetricsOverride`).
   - Close pages, contexts and the browser.
2. **Navigation.** goto, reload and bring-to-front (site [86,103,140]). Also tell a hash-only navigation from a
   real load (query wraps goto/reload on the `null` response, [144-148]).
3. **Page errors.** Collect uncaught page errors; all three harnesses assert none (`Runtime.exceptionThrown`).
4. **Run script in the page.**
   - Evaluate a function with arguments and get JSON back. This covers evaluate, `$$eval`, evaluateAll,
     waitForFunction, textContent, inputValue and count.
   - Poll for a condition with a timeout.
5. **Trusted input.**
   - Mouse click, double-click and right-click at an element's position, and hover. Hover opens nested menus:
     site [62,142,171,215,232], query [411].
   - Key presses: Tab, Escape, `ControlOrMeta+A`.
   - Text typing into Monaco (studio [72-73]).
   - Input dispatch goes through `Input.dispatchMouseEvent`/`dispatchKeyEvent`/`insertText`.
6. **Screenshots** to a file (query [159], studio [51]).
7. **Permissions:** clipboard-read/write for one context (site [49]) (`Browser.grantPermissions`).

**What can run inside the page instead** (DOM APIs, no driver privilege):
- Every read: `querySelector(All)`, `textContent`, `.value`, `location.hash`, row counts. Every one of them is
  already written as an in-page function in places: `waitForFunction` bodies at query [110-111,184,292,404,413] and
  site [77,132,153,166-169,176,218-221,230,236].
- Every wait, as in-page polling or MutationObserver with a timeout.
- `fill` and `selectOption`: set `.value`, then dispatch `input`/`change`.
- `check`: set `.checked`, then dispatch `change`.
- Plain `click` on buttons: `element.click()` is the one synthetic event that triggers default actions.
- Reading the clipboard: `navigator.clipboard.readText()` is already called in the page (site [107,147]); it needs
  the permission grant from the driver.
- Playwright's selector extensions must be rewritten as DOM code: `:has-text` 43 uses, `:text-is` 15, `>> nth=` 9,
  `:visible` 6, `text=` 1, `:text()` 1, and `locator({has, hasText, hasNot})` 18. CSS `:has()` and `:scope` are
  standard CSS.

**What must stay in the driver** (privileged or outside the page):
- Trusted input wherever the app needs a real event:
  - Right-click and context menus: 9 sites.
  - Hover-opened submenus: 7.
  - dblclick on tree nodes: 13.
  - Keyboard into Monaco: 2.
  - Tab and Escape key handling: 10.
  - Whether a given handler accepts synthetic events is OPEN per site. Settle it by running each step once with
    synthetic DOM events in the pinned Chromium.
- Screenshots.
- Isolated contexts.
- Permission grants.
- The static HTTP servers: 3 in harnesses plus the 3 `serve` binaries, including query's config rewriting
  [69-80] and site's refusable remote file [22-32].
- Starting, stopping and health-checking servers: legend-lite JVM (query:verify, query-store lite_test),
  sdlc-server (server_helper), native warehouse (query:verify), Windows `taskkill` tree-kill (E4 [71]).
- Temp dirs and output dirs.
- Raw HTTP with forbidden headers (sdlc server_test `Origin`/`Host` [27-38]).
- File assertions (canary [61,69]).
- The exit code. Today that is `strict-reporter.mjs` + node:test.

**The 11 node_tests that start no server** (twins, query ×3, studio ×2, sdlc/depot wasm, query-store local/share,
plus the two guards, lock_matches and revision_test) use node:test/assert, node:fs reads of runfiles, and file-URL WASM
loading. In the page these become:
- an in-page runner and assert;
- inputs fetched over the driver's static server;
- the TeaVM runtime's browser branch (F7).

None of them needs trusted input.
