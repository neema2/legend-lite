# Area 5: DataCube (`//datacube`) — build inventory

Repo: `the bazel/exec checkout` (branch bazel/exec, HEAD `6f1d9aa9a`). Read-only; only
`bazel query` was run. 239 targets from `runs/inventory/area5_targets.tsv`, every one named below.

**Evidence shorthand.** `B:n` = `datacube/BUILD.bazel:n`. `inv` = `runs/inventory/inventory.json` (deps/users).
`q:` = a `bazel query` I ran (given inline). Lanes (`.github/workflows/gates-run.yml:50-66`; every lane runs on
linux, macos and windows per `.github/workflows/gate.yml:66-90`, except `browser` = Linux only (`gates-run.yml:69`)
and linux-arm runs `native` only (`gate.yml:97-101`)):

- **BUILD** = the `build` lane, `bazel build //...` (`gates-run.yml:63,163`). It builds test targets too (their
  runfiles), so every generator/diff-test/bundle in `//datacube` is executed by it; tests are not *run*.
- **APP** = `app` lane: `//datacube:tests //datacube:verify_app_test ...` (`gates-run.yml:61`).
- **BROWSER** = `browser` lane: `//datacube:live_snap_test //datacube:browser ...` (`gates-run.yml:65`), plus the
  step `bazel run //datacube:install_browser -- --with-deps` (`gates-run.yml:149-151`).
- **NATIVE** = `native` lane: `//datacube:app` (`gates-run.yml:64`).
- **CHECKS** = `checks` lane: `//:generated` (`gates-run.yml:51`), which holds `//datacube:update_generated_tests`
  (`BUILD.bazel:31`).
- **LOCAL** = `//gates:local` (`gates/BUILD.bazel:56-57`: `//datacube:tests`, `//datacube:verify_app_test`;
  `:15` `//:generated`). `//datacube:browser` is NOT in LOCAL.
- **RUN** = only a human's `bazel run`.

**Reach tags.** `core_libs`/`teavm`/`wasm_planner` on DataCube targets come from one of three paths:
(1) `//wasm:planner_dir` or `:vendor -> //wasm:planner -> //wasm:boundary -> //core:plan_side -> //core:compiler`
(q: `somepath(//wasm:planner_dir, //core:compiler)`); (2) the `//core` umbrella (`core/BUILD.bazel:207-236`) for
`offer_facts_main`/`catalog_facts_main` (q: `somepath(//datacube:offer_facts, //core:server_lib)` returns a path);
(3) `//core:drivers -> :duckdb_load -> :core` for `app_postgres` (q: `somepath(//datacube:app_postgres,
//core:compiler)`). The `chromium` tag on `typecheck_mjs_test` and on the js_binary harnesses is a false positive:
it is `//tools/browser:pinned_chromium`, a one-file `js_library` (`tools/browser/BUILD.bazel:38-41`);
q: `somepath(//datacube:typecheck_mjs_test, //tools/browser:chromium_headless_shell)` and
`somepath(//datacube:verify_charts, //tools/browser:chromium_headless_shell)` are both empty. Only the
`browser_test`s (`tools/browser/defs.bzl:28-31`) reach the 200 MB browser.

**Node column ("needs Node?").** (a) Node only launches a native binary; (b) Node runs our TS/JS, and could run in
the pinned Chromium; (c) Node-only API or package needed (named). "none" = no action runs Node. esbuild 0.28.2's
`bin/esbuild` is a `#!/usr/bin/env node` script that `execFileSync`s `@esbuild/<platform>` (seen in
`bazel-out/darwin_arm64-opt-exec/bin/datacube/node_modules/.aspect_rules_js/esbuild@0.28.2/node_modules/esbuild/bin/esbuild`:
`file` says "node script"; 2× `execFileSync`); the install script that would replace the shim never runs
(`datacube/package.json:28-30` `onlyBuiltDependencies: []`). typescript 7.0.2's `bin/tsc` is `import "../lib/tsc.js"`,
a 609-byte file that `execFileSync(getExePath())` (same bazel-out tree, `typescript/lib/tsc.js`, `lib/getExePath.js`);
per-platform packages `datacube/pnpm-lock.yaml:298-346`.

---

## Part A: every target

### A1. Sources and groupings

| target | kind | what it is | reads | produces | who uses it | SHOULD run when | runs TODAY | verdict | needs Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `:src` | js_library | the app's TS sources, `glob(src/**/*.ts)` (B:37-43) | datacube/src (87 files) + `:src_base` | nothing (provider only) | 64 users (inv): the 5 bundles, 6 scanners, 10 standalone tests, all harnesses, generators `emit_*`/`make_link_dictionary`, `//query:src`, `//query:typecheck_test` | n/a | n/a | WIRING | none (the code is browser code; shipped src imports no `node:*`: `grep "from 'node:" datacube/src` empty) | visible to //query (B:41) |
| `:src_base` | js_library | package.json + tsconfig.json + the libraries every source imports (B:47-69) | `//pure-protocol:pure_protocol`, `//query-store:query_store`, `//engine-client:engine_client`, npm duckdb-wasm, fflate, echarts | nothing | 94 users (inv): every TEST_IMPORTS-scoped node_test, `:src` | n/a | n/a | WIRING | none | P3-34 split (B:45-46) |
| `:styles` | filegroup | `src/**/*.css` (B:72-76) | css | — | `//query:cube_styles` | n/a | n/a | WIRING | none | still used (inv) |
| `:scanned_demo`, `:scanned_scripts`, `:scanned_tests` | filegroup | the declared inputs of the source scanners (B:102-124) | demo/**, bench/**, test/**, tools/** | — | `guardrails_test`; `portability_test`; `wasm_flag_test` (inv) | n/a | n/a | WIRING | none | one user each |
| `:import_scan` | filegroup | `src/**/*.ts` + `test/**/*.ts` (B:441-444) | ts | — | `:test_imports` (+2 internals) | n/a | n/a | WIRING | none | |
| `:all_files` | filegroup | `guards_package()` package glob (B:1101, `tools/guards/defs.bzl:109-115`) | all package files | — | `//tools/guards:repository_files` | n/a | n/a | WIRING | none | area 8's machinery |

### A2. The real "compile": esbuild bundles and the typecheck

The five bundles are one list comprehension (B:656-687), identical in shape: `esbuild.esbuild(name="bundle_"+out,
srcs=glob(demo/**/*.ts)+[":src"], ...)`; all but `planner-worker` are split with a chunk dir (B:667,673,680-685).
Each expands to 6 targets: `X` (`_run_binary`), `X__entry_point` (directory_path, manual), `X__js_binary`
(js_binary, manual: the Node launcher of esbuild), `X_copy_srcs_to_bin`, `X_js_info_files`, `X_runfiles` (all manual;
inv deps show the same pattern for all five).

| target | kind | entry (B:656-662) | who loads the output | users (inv) | SHOULD run when | runs TODAY | verdict | needs Node? | note |
|---|---|---|---|---|---|---|---|---|---|
| `:bundle_bundle` | esbuild | `demo/main.ts` -> `demo/bundle.js` + `demo/chunks-bundle/` | `demo/index.html:186` (the app page); copied into `:dist` (`demo/make-dist.mjs:30,34`) | `:site`, `bundle_budget_test` | datacube src/demo TS, engine-client/pure-protocol/query-store TS, package.json/tsconfig | BUILD; and every lane whose tests need `:site`/`:dist` (APP, LOCAL, BROWSER, NATIVE) | COMPILE | (a) esbuild via its Node shim | THE shipped app |
| `:bundle_planner_worker` | esbuild | `src/planner-worker.ts` -> `demo/planner-worker.js` (unsplit, B:667) | `demo/planners.ts:21` (`new URL('./planner-worker.js', ...)`); copied into `:dist` (`make-dist.mjs:30`) | `:site` | same | same | COMPILE | (a) | shipped |
| `:bundle_bundle_page` | esbuild | `demo/page.ts` -> `demo/bundle-page.js` | `demo/page.html:53` (several cubes on one page, "plan F6", `demo/page.ts:1-8`); NOT in `:dist` (`make-dist.mjs:30`), but in `//site:dist` (`site/BUILD.bazel:10-18` copies all of `:site`) | `:site` | same | same | COMPILE | (a) | only `verify_page(_test)` exercises it (`demo/verify-page.mjs:66`) |
| `:bundle_remote_bundle` | esbuild | `demo/remote-harness.ts` -> `demo/remote-bundle.js` | `demo/remote.html:5`, driven only by `demo/verify-remote.mjs:138` | `:site` | same | same | TOOL | (a) | a test fixture page bundled into the shipped `:site` and `//site:dist` |
| `:bundle_stress` | esbuild | `demo/stress.ts` -> `demo/stress.js` | `demo/stress.html:6`, driven only by `demo/run-stress.mjs:16` | `:site` | same | same | TOOL | (a) | a stress fixture shipped in `//site:dist` |
| 25 internals: `bundle_{bundle,bundle_page,planner_worker,remote_bundle,stress}{__entry_point,__js_binary,_copy_srcs_to_bin,_js_info_files,_runfiles}` | directory_path / js_binary / _copy_to_bin / js_info_files / filegroup | rules_js `esbuild` macro expansion | esbuild npm pkg; `:src` | launcher, copies | their own bundle (inv) | with their bundle | with their bundle (manual, built only as deps) | WIRING | `__js_binary` = the (a) launcher; others none | identical across the five (inv) |
| `:typecheck_test` | js_test (tsc_test) | `tsc -p datacube/tsconfig.json`, types only, over src/test/demo/tools (B:249-276) | src, test, demo, tools TS; engine-client, pure-protocol, query-store sources; @types/node, @types/jsdom, playwright types | test verdict | `:tests` | any TS edit in datacube or the three TS libraries it includes (`tsconfig.json:26`) | APP, LOCAL (via `:tests`); BUILD builds but does not run it | COMPILE | (a) tsc 7 via its Node shim | THE only TypeScript type check of product code: Node and esbuild both strip types (B:249-251). Being a test, `bazel build //...` never type-checks |
| `:typecheck_test__entry_point` | directory_path | tsc_test expansion | typescript pkg | — | `:typecheck_test` | with it | with it | WIRING | none | |
| `:typecheck_mjs_test` | js_test (tsc_test) | `tsc -p tsconfig.mjs.json` (allowJs/checkJs) over `demo/*.mjs`, `bench/*.mjs` (B:278-306; `tsconfig.mjs.json:13`) | harness .mjs, src/demo ts, playwright types, `pinned-chromium.mjs` | test verdict | `:tests` | an edit to a harness/bench .mjs or what it imports | APP, LOCAL | CHECK-GUARD | (a) | types the harnesses, not product |
| `:typecheck_mjs_test__entry_point` | directory_path | expansion | | | `:typecheck_mjs_test` | | | WIRING | none | |

### A3. JVM-generated facts and the other generators

`java_run` (`tools/java_run/defs.bzl:44-120`) runs a target-configuration Java program in a build action whose
inputs are the program's **transitive runtime jars** (`:46`, `:110`). Any edit to any library in that closure changes
a class jar and reruns the action.

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run when (TRUE trigger) | runs TODAY | verdict | needs Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `:emit_offer_queries` | js_binary | runs `tools/offer-facts/emit.ts` (B:315-323) | `:src` (all) | — | `:offer_queries` | when `:offer_queries` runs | BUILD (as a dep) | TOOL | (b): runs product TS (`src/query.ts`, `calc.ts`, `snapshot.ts`) + `node:fs.writeFileSync`, `process.argv` (`emit.ts:18,158-164`); could run in Chromium with the driver writing the two files | |
| `:offer_queries` | js_run_binary | DataCube's own queries, written by the product query builder (B:360-372) | emit.ts + `:src` | `offer_model.pure`, `offer_queries.tsv` | `:offer_facts` | change to DataCube's query builder / CALC_FUNCTIONS | BUILD, CHECKS, LOCAL (via `update_generated_0_test`) | GEN-BUILD | (b) as above | consumer: `:offer_facts` |
| `offer_queries_{copy_srcs_to_bin,js_info_files,runfiles}` | internals | js_run_binary expansion | | | `:offer_queries` | | | WIRING | none | |
| `:offer_facts_main` | java_library | `OfferFacts.java` (B:374-380); imports `Compiler`, `compiler.*`, `plan.UpstreamRelationType`, `protocol.*` (`tools/offer-facts/OfferFacts.java:6-17`) | `//core` umbrella (32 exports incl. exec, probe, driver, server_lib, ide, test, testdatagen: `core/BUILD.bazel:207-212`) | jar | `:offer_facts` | when `:offer_facts` runs | BUILD | TOOL | none | needs only plan-side classes (all are in `_PLAN_SIDE_TARGETS`, `core/BUILD.bazel:217-221`) |
| `:offer_facts` | java_run | what the COMPILER says each aggregate/filter/calc function gives a column of each type (B:382-396; `OfferFacts.java:31-40`) | `:offer_queries` + full `//core` runtime jars | `generated/offer-facts.gen.ts` -> committed `src/generated/offer-facts.ts` (B:462) | `update_generated_0`, `update_generated_0_test`, `guard_classpaths` | change to DataCube's query builder OR to the compiler's typing/function registry (compiler, builtin, parser, protocol) | BUILD, CHECKS, LOCAL; any `//core` edit reruns it | GEN-COMMITTED | none (JVM) | the committed copy is a `:src` file the bundles and tests read (B:39) |
| `:catalog_facts_main` | java_library | `CatalogFacts.java` (B:402-413); imports json, parser, protocol, `sql.dialect.{CatalogModel,CatalogRules,CatalogType,DuckDb,Postgres}` (`CatalogFacts.java:6-14`); runtime DuckDB 1.5.x from `@maven_warehouse` (B:412) | `//core`, `//json` | jar | `:catalog_rules`, `:catalog_corpus` | when they run | BUILD | TOOL | none | only plan-side classes needed |
| `:catalog_rules` | java_run | each database's catalog-type decisions + the DuckDB catalog SQL, as data (`CatalogFacts.java:28-33`, mode `rules`) | full `//core` runtime jars | `generated/catalog-facts.gen.ts` -> committed `src/generated/catalog-facts.ts` (B:463) | `update_generated_1`(+`_test`) | change to `CatalogRules`/`DuckDb.CATALOG_*`/`Postgres.CATALOG_RULES` (core `sql_dialect`) | BUILD, CHECKS, LOCAL; any `//core` edit reruns it | GEN-COMMITTED | none | |
| `:catalog_corpus` | java_run | `CatalogModel.database` answers for a fixed corpus of REAL DuckDB tables, made by running DuckDB in the action (`CatalogFacts.java:34-40`; `:239` `DriverManager.getConnection("jdbc:duckdb:")`) | full `//core` jars + `@maven_warehouse` duckdb_jdbc | `generated/catalog-corpus.gen.ts` -> committed `test/generated/catalog-corpus.ts` (B:464) | `update_generated_2`(+`_test`); the committed copy is data of `catalog_model_test` (B:158) | change to `CatalogModel` (core sql_dialect) or a DuckDB version bump (`maven_warehouse`) | BUILD, CHECKS, LOCAL; any `//core` edit reruns it | GEN-COMMITTED | none | executes a database inside a build action |
| `:test_imports_tool` | js_binary | `tools/test-imports.mts` (B:432-439) | — | — | `:test_imports` | with it | BUILD | TOOL | (c) `node:fs.readFileSync/writeFileSync`, `node:path.posix`, `process.argv` (`test-imports.mts:9-12,22,62`); replaceable by esbuild `--metafile` (native) | regex import scan |
| `:test_imports` | js_run_binary | each test's import closure under src/ (B:446-456) | `:import_scan` (all src/test TS) | `test_imports_generated.bzl` -> committed `datacube/test_imports.bzl` (B:465), which the BUILD file `load`s (B:21, B:216) | `update_generated_3`(+`_test`) | an import line change in src/ or test/ | BUILD, CHECKS, LOCAL; any src/test edit reruns it (cheap) | GEN-COMMITTED | (c) as above | a committed generated file that feeds analysis |
| `test_imports_{copy_srcs_to_bin,js_info_files,runfiles}` | internals | expansion | | | `:test_imports` | | | WIRING | none | |
| `:update_generated` | write_source_files | `bazel run` writer of the four (B:458-468) | the four generators | writes src/generated/*.ts, test/generated/*.ts, test_imports.bzl | `//:update_generated` (`BUILD.bazel:58`) | by hand after a diff test fails | RUN | WIRING | none | |
| `:update_generated_0` .. `_3` | _write_source_file | one writer per file (offer-facts, catalog-facts, catalog-corpus, test_imports) | one generator each (inv) | — | `:update_generated` | by hand | RUN (built by BUILD) | WIRING | none | |
| `:update_generated_0_test` .. `_3_test` | _diff_test | committed file vs generator output | one generator each | verdict | `:update_generated_tests` | when its generator's true trigger changes | CHECKS, LOCAL, BUILD (generators executed) | CHECK-DIFF | none | 0-2 reach core_libs |
| `:update_generated_tests` | test_suite | the four diff tests | | | `//:generated` | | CHECKS, LOCAL | WIRING | none | still used |
| `:make_link_dictionary` | js_binary | `tools/link-dictionary/make.ts` (B:329-340) | `:src` | — | `:link_dictionary_next` | | BUILD | TOOL | (b) product TS (`make.ts:14-24`), output on stdout; could run in Chromium | |
| `:link_dictionary_next` | js_run_binary | the share link's NEXT dictionary version from today's vocabulary (B:342-351), "built with //... so the tool cannot rot unseen" | `:src` | `link-p2.gen.ts` | `:cut_link_dictionary` | only when a version is cut (deliberate) | BUILD on every `:src` edit | GEN-COMMITTED | (b) | TRUE trigger "deliberate cut"; TODAY it runs on every src edit as a rot check hidden in the build |
| `link_dictionary_next_{copy_srcs_to_bin,js_info_files,runfiles}` | internals | expansion | | | | | | WIRING | none | |
| `:cut_link_dictionary` | write_source_files (no diff test, B:353-358) | writes `src/share/link-p2.ts` once | `:link_dictionary_next` | the frozen version file | users 0 (inv); named in `test/share-link.test.ts:98`, `docs/GATES.md:64`, `docs/DATACUBE_SAVE_SHARE_2026_09_28.md` | by hand when shipping p2 | RUN | WIRING | none | not DEAD: documented human entry point; the hash pin in `share-link.test.ts:96-112` is the freeze |
| `:emit_cube_queries` | js_binary | `test/wasm-differential/emit.ts` (B:473-481) | `:src` | — | `:cube_queries` | | BUILD | TOOL | (b) + `node:fs.writeFileSync`, `process.argv` (`emit.ts:7,12,20-21`) | |
| `:cube_queries` | js_run_binary | the cube's queries for the WASM differential (B:483-498) | `cases.ts`, emit.ts | `cube_model.pure`, `cube_queries.tsv` | `:cube_jvm_answers` | change to `test/wasm-differential/cases.ts` or the serialiser | BUILD, APP, LOCAL | GEN-BUILD | (b) | srcs list only cases.ts+emit.ts but the tool's data is all `:src` (B:475) |
| `cube_queries_{copy_srcs_to_bin,js_info_files,runfiles}` | internals | expansion | | | | | | WIRING | none | |
| `:cube_jvm_answers` | java_run | the JVM planner's answers to those queries, `planner.JvmMain` (B:500-516) | `:cube_queries` + `//wasm:jvm_main -> :boundary -> //core:plan_side` (q above) | `cube_jvm_answers.txt` | `wasm_differential_test` | a planner (plan_side) change or a case change | BUILD, APP, LOCAL; every plan-side edit reruns it | GEN-BUILD | none | the JVM half of a test, computed as a build output; consumer `wasm_differential_test` |

### A4. Site, dist, app

| target | kind | what it is | reads | produces | who uses it | SHOULD run when | runs TODAY | verdict | needs Node? | note |
|---|---|---|---|---|---|---|---|---|---|---|
| `:vendor` | copy_to_directory | DuckDB-WASM files, Roboto fonts, the planner `classes.wasm` + runtime into `demo/vendor` (B:689-718) | npm duckdb-wasm, @fontsource/roboto, `//wasm:planner` | dir | `:site` | duckdb/font bump, any planner (core plan_side) change | with `:site` | WIRING | none (bazel_lib copy binary) | the planner is why `:site` reaches core |
| `:projects` | copy_to_directory | Query's trading demo models into `demo/projects/trading` (B:720-731) | `//query:demo/models/*` | dir | `:site`, `saved_queries_test` | query demo model edit | with users | WIRING | none | |
| `:site` | filegroup | everything a browser fetches: html/css/json/pure + 5 bundles + vendor + projects (B:733-745) | | | 34 users (inv): `//site:dist`, `:dist`, `:serve`, all harnesses and browser tests, `torture_test`, `calc_vocabulary_test` | n/a | n/a | WIRING | none | includes test-only pages (`remote.html`, `stress.html`) and their bundles |
| `:make_dist` | js_binary | `demo/make-dist.mjs` (B:786-789) | — | — | `:dist` | | BUILD | TOOL | (c) `node:fs/promises.cp/mkdir/readFile/rm/writeFile` (`make-dist.mjs:11`), `process.argv`; it copies 5 files + 3 dirs and inlines CSS into index.html (`:30-49`): replaceable by `copy_to_directory` + a template step | |
| `:dist` | js_run_binary | the static-host folder of the app (B:791-800) | `:site` | `dist/` | `app_posix`, `app_windows`, `verify_app`, `verify_app_test`, `dist_complete_test` | site/bundle/planner change | BUILD, APP, LOCAL, NATIVE | COMPILE | (c) via make_dist | packaging of the shipped product |
| `dist_{copy_srcs_to_bin,js_info_files,runfiles}` | internals | expansion | | | `:dist` | | | WIRING | none | |
| `:app` | alias (warehouse_run) | `bazel run //datacube:app -- postgresql://...` launcher (B:769-782; `warehouse/defs.bzl:103-150`) | `app_posix`/`app_windows` | — | NATIVE lane builds it; docs (`docs/DATACUBE_ON_POSTGRES.md`, `docs/DATACUBE_APP_PLAN_2026_10_02.md`) | warehouse native image or dist change | NATIVE, BUILD | WIRING | none | users 0 in inv, but lane + docs |
| `:app_posix` | _posix_launcher | bash script exec'ing the native warehouse with `--site dist` (`warehouse/defs.bzl:56-101`) | `//warehouse:server_native`, `//warehouse:duckdb_library`, `:app_extensions`, `:dist` | script | `:app` | as above | NATIVE, BUILD | WIRING | none | builds the native image |
| `:app_windows` | launcher_binary | hermetic-launcher stub, Windows x64 | same | .exe | `:app` | | NATIVE (windows), BUILD | WIRING | none | |
| `:app_extensions` | copy_to_directory | DuckDB postgres extension dir (`warehouse/defs.bzl:126-133`) | `//warehouse:duckdb_extensions` | dir | `app_posix`, `app_windows` | extension pin bump | with app | WIRING | none | |

### A5. Helper binaries and launchers

| target | kind | what it is | reads | who uses it | runs TODAY | verdict | needs Node? | note |
|---|---|---|---|---|---|---|---|---|
| `:serve` | js_binary | `demo/serve.mjs`, the dev static server (B:751-767) | `:site` | `verify_picker_test` (via `:datacube_serve`); humans (`demo/README-realdata.md`, `make-sample.mjs`) | BUILD, BROWSER | TOOL | (c) `node:http.createServer`, `node:net`, `node:child_process.execFileSync` (open a browser), `node:fs/promises.stat`, `process.stdin` (`serve.mjs:19-22,41-43,104`) | |
| `:datacube_serve` | executable_of | `:serve`'s launcher file (B:989-994) | `:serve` | `verify_picker_test` | BROWSER | WIRING | none | |
| `:legend_server_for_torture` | executable_of | `//core:server` launcher (B:965-970) | `//core:server` | `torture_test` | APP, LOCAL | WIRING | none | |
| `:app_postgres` | java_binary (testonly) | embedded Postgres 16 loaded with a sample (B:1075-1091; `tools/apppostgres/AppPostgres.java:3-11`: imports only `com.legend.testing.EmbeddedPostgres` and `java.sql`) | `//testing`, `@embedded_postgres`, runtime `//core:drivers` | `verify_app_test`, `:app_postgres_executable` | APP, LOCAL, BUILD | TOOL | none | `//core:drivers` drags in all of core (q: `somepath(//datacube:app_postgres, //core:compiler)` = drivers -> duckdb_load -> core -> compiler) for one Postgres JDBC driver |
| `:app_postgres_executable` | executable_of | its launcher (B:1069-1073) | | `verify_app_test` | APP, LOCAL | WIRING | none | |

### A6. The js_binary harnesses (21)

All 19 in `_HARNESSES` are one comprehension (B:819-878): `js_binary(entry_point="demo/<h>.mjs", data=_HARNESS_HELPERS
+ [index.html, playwright, :site, :src, //tools/js:runfiles] (+pinned_chromium for some), env=_SITE_ENV)`; only
`make_sample` gets `[":src"]` alone (B:859). Each harness file names its own `bazel run //datacube:<name>` line
(git grep below), so none is DEAD by the brief's test. `git grep -l -E "datacube:<t>([^a-z_]|$)" -- ':!datacube/BUILD.bazel' ':!runs/'`
results are in the "doc" column.

| target | entry | is it a test's entry point? | needs beyond the site | doc telling a human | runs TODAY | verdict | needs Node? |
|---|---|---|---|---|---|---|---|
| `:verify_smoke` | demo/verify-smoke.mjs | yes: `verify_smoke_test` (B:883-898) | — | its header | BUILD (built), RUN | TOOL | (c) Playwright, node:fs/promises, process.env |
| `:verify_charts` | verify-charts.mjs | yes: `verify_charts_test` (B:900-912) | — | header | BUILD, RUN | TOOL | (c) Playwright |
| `:verify_cubes` | verify-cubes.mjs | yes: `verify_cubes_test` | — | header, `docs/DATACUBE_SAVE_SHARE_2026_09_28.md` | BUILD, RUN | TOOL | (c) Playwright, node:fs/promises |
| `:verify_page` | verify-page.mjs | yes: `verify_page_test` | — | header, `demo/page.ts:9` | BUILD, RUN | TOOL | (c) Playwright |
| `:verify_upload` | verify-upload.mjs | yes: `verify_upload_test` | — | header | BUILD, RUN | TOOL | (c) Playwright |
| `:verify_wasm_browser` | verify-wasm-browser.mjs | yes: `verify_wasm_browser_test` | — | header | BUILD, RUN | TOOL | (c) Playwright |
| `:run_stress` | run-stress.mjs | yes: `run_stress_test` | — | `src/samples.ts` | BUILD, RUN | TOOL | (c) Playwright, node:fs/promises |
| `:verify_features` | verify-features.mjs | yes: `verify_features_test` (B:933-948) | `WAREHOUSE=` variant needs a warehouse (header :16) | header, `datacube/docs/FEATURE_CENSUS.md` | BUILD, RUN | TOOL | (c) Playwright, node:fs/os/path |
| `:verify_picker` | verify-picker.mjs | yes: `verify_picker_test` (B:975-987) | a running `:serve` on :8000 under `bazel run` (header :9-10) | header | BUILD, RUN | TOOL | (c) Playwright |
| `:verify_remote` | verify-remote.mjs | shard 0 of `verify_remote_test` (B:914-931; `demo/verify-remote-test.mjs:7-10`) | — | header, `docs/DATACUBE_AUDIT_PASS2_FINDINGS_2026_09_26.md` | BUILD, RUN | TOOL | (c) Playwright, DuckDB-WASM NODE_RUNTIME (`verify-remote.mjs:34-48`) |
| `:verify_real_data` | verify-real-data.mjs | shard 1 of `verify_remote_test` | — | header, `demo/README-realdata.md` | BUILD, RUN | TOOL | (c) Playwright, DuckDB-WASM NODE_RUNTIME (`:46-71`) |
| `:verify_calc_vocabulary` | verify-calc-vocabulary.mjs | yes: `calc_vocabulary_test` with `CALC_PLANES=local` (B:996-1009) | engine half needs legend-engine at `ENGINE` (default :6300, `:42`) | header, `docs/DATACUBE_AUDIT_PASS2_FINDINGS_2026_09_26.md` | BUILD, RUN | TOOL | (b) the planner in Node; fetch to ENGINE |
| `:torture` | torture.mjs | yes: `torture_test` (B:950-963) | legend-lite server (`LEGEND_SERVER` / `ENGINE`, `torture.mjs:35-46`) | header | BUILD, RUN | TOOL | (c) DuckDB-WASM NODE_RUNTIME (`:109-114`), node:fs, child_process (via harness.startServer) |
| `:chaos` | chaos.mjs | no | a running engine at `ENGINE` (default `http://localhost:8080`, `chaos.mjs:35`) | header only | BUILD, RUN | TOOL | (c) Playwright |
| `:shots` | shots.mjs | no (looks, does not assert: header :3-6) | — | header only | BUILD, RUN | TOOL | (c) Playwright, node:fs |
| `:measure_startup` | measure-startup.mjs | no | — | header only | BUILD, RUN | TOOL | (c) Playwright |
| `:verify_engine` | verify-engine.mjs | no | legend-engine at `ENGINE` (:6300, `:13,24`) | header only | BUILD, RUN | TOOL | (b) fetch + node:fs/promises |
| `:verify_engine_differential` | verify-engine-differential.mjs | no | legend-engine at `ENGINE` (`:41,58`) | header only | BUILD, RUN | TOOL | (c) DuckDB-WASM NODE_RUNTIME (`:99-113`) |
| `:make_sample` | make-sample.mjs | no | — | header, `demo/README-realdata.md` | BUILD, RUN | TOOL | (c) node:fs/promises.writeFile (writes a CSV for a human, `:7-8`) |
| `:verify_app` | verify-app.mjs (manual, B:1022-1042) | same file as `verify_app_test` | a Postgres the user starts (`verify-app.mjs:6-10`) | `docs/DATACUBE_APP_PLAN_2026_10_02.md`, `docs/DATACUBE_ON_POSTGRES.md`, `docs/WINDOWS_APP_DESIGN_2026_10_02.md`, `LauncherTest.java` | RUN only (manual) | TOOL | (c) Playwright, child_process.spawn |
| `:install_browser` | install-browser.mjs (B:1093-1099) | no | network (Playwright CDN) | `docs/DATACUBE_ON_POSTGRES.md:237,246`, `query/README.md:45`, `query/demo/verify.mjs:9`, `site/verify.mjs:6`, `docs/GATES.md:35` | BROWSER lane step (`gates-run.yml:151`), RUN | TOOL | (c) `child_process.spawnSync` of Playwright's CLI (`:7-14`) |

Summary of the harnesses: **test entry points** (12 js_binaries duplicate a test's entry file): verify_smoke,
verify_charts, verify_cubes, verify_page, verify_upload, verify_wasm_browser, run_stress, verify_features,
verify_picker, verify_remote, verify_real_data, verify_calc_vocabulary, torture, plus verify_app (manual twin of
verify_app_test). **Human tools only**: chaos, shots, measure_startup, verify_engine, verify_engine_differential,
make_sample, install_browser, serve. **Dead**: none by the brief's three-part test (every one has a doc line), but
chaos, verify_engine and verify_engine_differential need an external engine no lane or test provides, and nothing
records their last run.

### A7. The js_tests (122), grouped

Every glob test is one comprehension (B:203-227, `node_test`, `tools/js/defs.bzl:29-63`): `size=medium` iff in
`_WASM_TESTS` (B:176-199), data = jsdom + fakes + `_EXTRA_DATA` + (`:src` if a scanner or not in TEST_IMPORTS, else
`:src_base` + its TEST_IMPORTS files) + duckdb-wasm if in `_DUCKDB_USERS` (B:149) + catalog-builder/lite-compiler if
wasm. `wasm=True` adds `//wasm:planner_dir` and `WASM_PLANNER` (`tools/js/defs.bzl:54-59`). Groups below were
computed from inv deps (presence of `//wasm:planner_dir`, `node_modules/@duckdb/duckdb-wasm`, `:src`, chromium,
`:site`, `:dist`, `//core:server`, `//warehouse:server_native`, `:cube_jvm_answers`).

| group | members (by target name) | needs | SHOULD run when | runs TODAY | verdict | needs Node? |
|---|---|---|---|---|---|---|
| **G1 pure unit (69)**: Node + fakes (+jsdom); no planner, no DuckDB | adhoc_mode, adhoc_session, adhoc_state, adhoc_transactions, app, apply_refusal, board, calc, cancel, catalog_model, chart_echarts, chart_tiles, columns_panel, columns_selector, cube_adhoc_shell, cube_document, cube_editors_app, cube_library, cube_lifecycle, cube_state, cube_store, cube_transactions, dimensions, duckdb_cancel, editor, editors_live, engine_remote, epoch, export_doc, export_model, export_rich, export, form, format_scale, format, grid_basics, grid_dom, grid_resize, grid, host, menu, menu_view, multi_cube, page_document, pivot_panel, pivot_values, plane, planner, relation_type, remote, runner, sample, save_dialog, scale, screen_colours, selection, share_link, shell, source_picker, style, tile_layout, tree, undo_coverage, values, warehouse_session, wasm_planner, window (67, all `_test`) + duckdb_test, snap_test (DuckDB-WASM in Node) | Node; duckdb/snap: `@duckdb/duckdb-wasm/blocking` NODE_RUNTIME | an edit to the files in its TEST_IMPORTS closure (`datacube/test_imports.bzl`), the test file, engine-client/pure-protocol/query-store TS; never a Java edit | APP (3 OS), LOCAL | TEST-UNIT | (b) node:test + node:assert (+jsdom); all could run in Chromium (Part D); duckdb/snap (c) NODE_RUNTIME; share_link uses `node:crypto.createHash` (WebCrypto exists); export/export_doc use `Buffer.from` |
| **G2 source scanners (6)** | config_readers, guardrails, menu_ids, portability, state_guardrail, wasm_flag (`_SCANNED`, B:83-100) | Node + the declared source files via `SOURCES` | an edit to any scanned file | APP, LOCAL | CHECK-GUARD | (c) read sources through `tools/js/runfiles.mts` `Sources` (`node:fs.readFileSync`, `process.env`, `:8,61-72`); portability also `node:http.createServer`, `node:fs`, `process.execPath` |
| **G3 artifact checks (2)** | bundle_budget_test (reads `:bundle_bundle`, B:156), dist_complete_test (reads `:dist`, B:154) | the built bundle / dist (dist pulls the planner) | bundle_budget: a bundle change; dist_complete: a `make-dist.mjs`/site layout change | APP, LOCAL | CHECK-GUARD | (c) `node:fs` + `node:zlib.gzipSync` (`bundle-budget.test.ts:7-10`); `node:fs` (`dist-complete.test.ts:6`) |
| **G4 WASM-planner integration (30)** | 17 with fakes for data: adhoc_query, calc_fix, child_groups, column_editor, column_kind, config, drill, escaping, filter_editor, fuzz, infer, json_shape, offer_facts, pivot_total, query, type_columns, window_columns; 4 + DuckDB: group_derived, sorting, treeview, upload; 6 real app + planner + DuckDB: cube_open, json_read, pivot_rows, typed_values, typed_values_tokyo, typed_values_new_york; saved_queries (+`//fixtures/saved-queries:records`, `:projects`); calc_vocabulary_test (+`:site`); wasm_differential_test (+`:cube_jvm_answers`) | Node + `//wasm:planner_dir` (TeaVM build of `//core:plan_side`) | a DataCube edit in its closure OR a planner (plan_side) change | APP, LOCAL; every `//core` plan-side edit reruns all 30 | TEST-INTEGRATION | (b) planner loaded by `runfileDirUrl('WASM_PLANNER')` (`pure-protocol/test/lite.ts:14-20`, `test/catalog-builder.ts`); DuckDB ones (c) NODE_RUNTIME |
| **G5 server-backed (2)** | torture_test (starts `//core:server` on port 0, B:950-963), live_snap_test (spawns `//warehouse:server_native`, B:627-647) | JVM legend-lite server; native warehouse image + DuckDB library | torture: planner/server change; live_snap: warehouse or snap code change | torture: APP, LOCAL. live_snap: APP (3 OS) + BROWSER (Linux) + LOCAL | TEST-INTEGRATION | (c) child_process.spawn (`live-snap.ts:25`), node:fs/os, NODE_RUNTIME |
| **G6 typecheck (2)** | typecheck_test, typecheck_mjs_test | tsc 7 | see A2 | APP, LOCAL | COMPILE / CHECK-GUARD (A2) | (a) |
| **G7 browser (10)** | verify_smoke_test (4 shards), verify_charts_test, verify_cubes_test, verify_page_test, verify_upload_test, verify_wasm_browser_test, verify_features_test (4 shards), verify_remote_test (2 shards), verify_picker_test (`:serve`, not `:site` directly) + verify_app_test (`:dist`, `:app_postgres`, `//warehouse:serve`) | pinned Chromium (`tools/browser/defs.bzl:28-31`); site (planner); verify_app_test also embedded Postgres + native warehouse launcher | a site/bundle change; verify_app_test also warehouse change | the 9: BROWSER only (Linux), not LOCAL; verify_app_test: APP (3 OS) + LOCAL | TEST-BROWSER | (c) Playwright (Part E) |
| **G8 stress (1)** | run_stress_test (`size="enormous"`, B:848) | pinned Chromium, site | a planner/serialiser change, nightly-class | BROWSER | TEST-STRESS | (c) Playwright |

Totals: 69 + 6 + 2 + 30 + 2 + 2 + 10 + 1 = **122** js_tests (G4 is 30: 17 + 4 + 6 + saved_queries +
calc_vocabulary + wasm_differential). Test suites:

| target | kind | groups | used by | verdict |
|---|---|---|---|---|
| `:tests` | test_suite (B:229-247) | the 99 glob tests + typecheck×2 + pivot_rows, json_read, cube_open, torture, calc_vocabulary, typed_values×3, wasm_differential, live_snap (111) | APP, LOCAL | WIRING (mixes unit, integration, native-image and typecheck) |
| `:browser` | test_suite (B:1011-1020) | the 10 browser/stress tests (not verify_app_test) | BROWSER | WIRING |

### Verdict counts (239)

| verdict | n | members |
|---|---|---|
| WIRING | 69 | A1 (8), bundle internals (25), typecheck entry points (2), js_run_binary internals of offer_queries/test_imports/link_dictionary_next/cube_queries/dist (15), update_generated + _0.._3 (5), update_generated_tests, cut_link_dictionary, vendor, projects, site, app, app_posix, app_windows, app_extensions, datacube_serve, legend_server_for_torture, app_postgres_executable, tests, browser (14) |
| TEST-UNIT | 69 | G1 |
| TOOL | 32 | bundle_remote_bundle, bundle_stress, emit_offer_queries, offer_facts_main, catalog_facts_main, test_imports_tool, make_link_dictionary, emit_cube_queries, make_dist, serve, app_postgres, and the 21 harnesses of A6 |
| TEST-INTEGRATION | 32 | G4 (30) + G5 (2) |
| TEST-BROWSER | 10 | G7 |
| CHECK-GUARD | 9 | G2 (6), G3 (2), typecheck_mjs_test |
| COMPILE | 5 | bundle_bundle, bundle_planner_worker, bundle_bundle_page, dist, typecheck_test |
| GEN-COMMITTED | 5 | offer_facts, catalog_rules, catalog_corpus, test_imports, link_dictionary_next |
| CHECK-DIFF | 4 | update_generated_0_test .. _3_test |
| GEN-BUILD | 3 | offer_queries, cube_queries, cube_jvm_answers |
| TEST-STRESS | 1 | run_stress_test |
| DEAD / OPEN | 0 / 0 | (one OPEN sub-question in Part B 10) |

---

## Part B: problems, with evidence

1. **Every engine edit reruns the three JVM fact generators, though none of them needs most of core.**
   `offer_facts_main` and `catalog_facts_main` depend on the `//core` umbrella (B:379, B:408), which exports 32 libraries
   (`//base`, `//json` and 30 core targets) including exec, probe, driver, server_lib, ide, test, testdatagen (`core/BUILD.bazel:207-212`); `java_run` feeds
   the action every transitive runtime jar (`tools/java_run/defs.bzl:46,110`). Their imports are plan-side only
   (`OfferFacts.java:6-17`, `CatalogFacts.java:6-14`; all in `_PLAN_SIDE_TARGETS`, `core/BUILD.bazel:217-221`). Their
   TRUE triggers are narrow: the compiler's typing/function registry and DataCube's query builder (offer_facts),
   `CatalogRules`/dialect constants (catalog_rules), `CatalogModel` + the DuckDB jar (catalog_corpus). Today they run
   in BUILD, CHECKS and LOCAL (`BUILD.bazel:31`, `gates/BUILD.bazel:15`) on every core edit, and catalog_corpus runs
   a real DuckDB inside a build action (`CatalogFacts.java:239`).
2. **`cube_jvm_answers` is a test's oracle computed as a build output**, rerun on every plan-side edit by BUILD as
   well as by the test lanes (B:500-516; q path to `//core:compiler`). It belongs to `wasm_differential_test` only.
3. **44 of DataCube's 122 tests rerun on every planner edit** (`awk` over area5_targets.tsv: 44 js_tests carry `wasm_planner`, the same 44 `core_libs`; 92 of 239 targets reach core_libs): the 30 G4 node tests and 9 browser tests depend on
   `//wasm:planner_dir` (`tools/js/defs.bzl:54-59`), and every `:site` user also on `:vendor -> //wasm:planner`
   (q: `somepath(//datacube:verify_picker_test, //wasm:planner)` = serve -> site -> vendor -> planner). For G4 that is
   the real trigger; for the browser tests' `wasm = True` it is **redundant**: no browser harness reads
   `WASM_PLANNER` (`grep -ln WASM_PLANNER datacube/demo/*.mjs` = only verify-calc-vocabulary.mjs), they load the
   planner from `:site`'s vendor dir.
4. **`torture_test` and `calc_vocabulary_test` depend on all of `:site` (5 bundles + vendor) just to locate
   `demo/torture.pure` / `demo/trades.pure`** through `siteRoot()` (`demo/harness.mjs:91-93`; `torture.mjs:34`;
   `verify-calc-vocabulary.mjs:38,68,89`), files already in `_HARNESS_HELPERS` (B:812-814). Every bundle rebuild
   reruns both.
5. **`verify_app_test` pulls all of core through `app_postgres`**: `runtime_deps = ["//core:drivers"]` (B:1089) ->
   `:duckdb_load` -> `:core` (`core/BUILD.bazel:244-267`) for the Postgres driver of a class that imports only
   `com.legend.testing.EmbeddedPostgres` and `java.sql` (`AppPostgres.java:3-11`).
6. **Test fixtures ship.** `bundle_remote_bundle` and `bundle_stress` (and `remote.html`, `stress.html`) are in
   `:site` (B:738-744), and `//site:dist` copies all of `:site` (`site/BUILD.bazel:10-18`), so the shipped
   two-app site carries the remote-harness and stress pages. `:dist` does not (`make-dist.mjs:30-37`).
7. **The only product type check is a test**, so `bazel build //...` (BUILD) compiles the bundles without checking
   types (esbuild strips them; B:249-251); a type error is caught only by APP/LOCAL running `typecheck_test`.
8. **`link_dictionary_next` is a rot check hidden in the build**: it runs the generator on every `:src` edit
   (B:342-351) though its true trigger is a deliberate version cut, and nothing checks its output (`diff_test =
   False`, B:356).
9. **`//datacube:tests` mixes four kinds**: 69 units, 30 planner integrations, 2 typechecks, a JVM-server test and
   the native-image `live_snap_test` (B:229-247). So the APP lane builds the warehouse native image on Linux, macOS
   and Windows for one test (B:244-245 says so), and live_snap_test runs again on Linux in BROWSER
   (`gates-run.yml:61,65`): a duplicate run. LOCAL runs it too (`gates/BUILD.bazel:56`), contradicting LOCAL's "no
   native image" premise (`gates/BUILD.bazel:3-5`).
10. **CI installs a second browser it no longer needs for DataCube.** BROWSER runs `bazel run //datacube:install_browser
    -- --with-deps` (`gates-run.yml:149-151`), but every DataCube browser test uses the pinned Chromium and points
    Playwright away from `$HOME` (`tools/browser/defs.bzl:33-37`). The step serves only the `browser-ci` harnesses of
    //query and //site (`gates-run.yml:178-195`; `query/demo/verify.mjs:9`, `site/verify.mjs:6`). Its
    `--with-deps` system libraries may still be needed by the pinned headless shell on Linux: OPEN, settled by running
    the BROWSER lane with the step removed.
11. **`docs/GATES.md:35` is stale**: it says DataCube's harnesses other than smoke "are still `bazel run` after
    `install_browser`, until P4"; CI runs `//datacube:browser` as tests since `36a470fa1` (P4-09).
12. **A fixed sleep survives in a converted test**: `demo/verify-picker.mjs:193` `page.waitForTimeout(150)` in
    `verify_picker_test`, against G-11 (workplan P4-02 proof: "`git grep -c waitForTimeout` prints 0 for the converted
    harnesses"). chaos (4) and shots (16) are not tests.
13. **`bazel build //...` builds 21 harness js_binaries no lane runs** (B:856-878, B:1095), each forcing `:site` and
    therefore the WASM planner; twelve of them duplicate a test's entry point. Cheap (symlink trees) but they keep
    the planner in the BUILD lane's critical path for no verdict.
14. **The `chromium` reach tag overstates**: see the header; only browser_tests fetch the browser.
15. **The BUILD file loads a committed generated file** (`test_imports.bzl`, B:21) to compute test data (B:216):
    an import edit makes the committed file stale until `bazel run //datacube:update_generated`; between those, the
    test falls back to all of `:src` only if it is missing from the dict (B:209-210), not if its closure grew.
    (Correct but drift-prone; a diff test guards it in CHECKS.)

---

## Part C: the right shape

1. **Compile = the shipped bundles + typecheck, and nothing else.** Keep `bundle_bundle`, `bundle_planner_worker`
   (and `bundle_bundle_page` if page.html is product) as the everyday compile. Move `remote-harness.ts`/`stress.ts`
   bundles and `remote.html`/`stress.html` into a `testonly` `:test_site` filegroup that the harnesses use, so
   `:site` / `//site:dist` ship only product (evidence: Part B 6). Make the typecheck a build action
   (`ts_project`-style `tsc --noEmit` writing a stamp, or a `build_test` over it) so BUILD catches type errors
   (Part B 7). Depends on area deciding what "everyday build" means for JS (coordinator).
2. **Generators keyed to their true triggers.** Point `offer_facts_main` and `catalog_facts_main` at
   `//core:plan_side` (extend its visibility, `core/BUILD.bazel:226`) instead of `//core`; this drops exec,
   server_lib, ide, test, testdatagen from their inputs (Part B 1). Better still, give them only the libraries they
   import (`:planner`/`:compiler`/`:protocol` for offer_facts; `:sql_dialect`/`:parser`/`:protocol`/`//json` for
   catalog) once area 1 decides core's library grain. Put the four diff tests in a `generated` lane that runs on core
   or DataCube changes, not in the everyday build; keep `bazel run //datacube:update_generated` as the writer.
   `link_dictionary_next`: tag `manual` (built only by `bazel run //datacube:cut_link_dictionary`) and, if rot
   matters, a small test that the tool runs (Part B 8).
3. **The differential's JVM half moves into its test's world**: `cube_jvm_answers` stays a build output but tagged
   so only `wasm_differential_test` pulls it (it already is the sole user, inv); the problem is BUILD building it,
   which goes away if BUILD stops building tests (coordinator decision).
4. **Split `:tests` by kind** (Part B 9): `:unit` (G1, 69; no planner), `:checks` (G2+G3+typecheck_mjs),
   `:planner` (G4, 30; runs on planner or DataCube change), `:server` (torture_test), and move `live_snap_test` to
   the native/browser lane only (it is already in BROWSER) and out of APP and LOCAL. LOCAL then takes `:unit`,
   `:checks`, typecheck, and `:planner` when the planner changed.
5. **Cut unneeded edges** (each with evidence above): drop `wasm = True` from the nine browser_tests (Part B 3);
   give `torture_test` and `calc_vocabulary_test` `demo/index.html` + the `.pure` files instead of `:site` (Part B 4);
   give `app_postgres` `@maven_core//:org_postgresql_postgresql` instead of `//core:drivers` (Part B 5; pool check by
   `tools/deps/pools.bzl` applies).
6. **Harnesses**: tag the 12 test-twin js_binaries and the 8 human tools `manual` so BUILD does not build them
   (Part B 13); delete or reconnect chaos / verify_engine / verify_engine_differential to a lane that provides an
   engine (decision for the DataCube owner). Remove `install_browser` from the BROWSER lane once //query and //site
   harnesses use the pinned browser (Part B 10; area that owns //query, //site).
7. **Node removal** (Parts D, E): the user has decided to remove Node; the shape above holds, with every node_test
   becoming a page test in the pinned Chromium and every harness a CDP-driven test.

---

## Part D: what DataCube needs Node for, and what each use would take to remove

| use | targets | kind | evidence | to remove |
|---|---|---|---|---|
| launch esbuild | 5 bundles (`*__js_binary`) | (a) | esbuild `bin/esbuild` is a node script doing `execFileSync` of `@esbuild/<platform>` (bazel-out path in header); `package.json:28-30` blocks the install script | call the platform binary directly: a small rule/toolchain over `@npm//datacube:@esbuild/<platform>` (or rules_esbuild's toolchain), selected per exec platform |
| launch tsc 7 | typecheck_test, typecheck_mjs_test | (a) | `typescript/lib/tsc.js` = `execFileSync(getExePath())` (609 bytes) | run `@typescript/typescript-<platform>`'s native `tsgo` binary from a `sh_test`-free test rule (e.g. a tiny native test runner or `build_test` over a `run_binary` stamp) |
| run unit tests (node:test, node:assert, jsdom) | G1 (67 + duckdb/snap), G4 fakes (17) | (b) | Part E test table: 41 files use only node:test/assert, 26 more add jsdom | a page runner in the pinned Chromium: a bundled test page (esbuild) + a minimal `describe/it/assert` shim; jsdom becomes the real DOM. Per-file one-off Node APIs: `node:crypto.createHash` -> `crypto.subtle.digest` (share-link.test.ts:6), `Buffer.from` -> `TextEncoder`/`Uint8Array` (export.test.ts:146-159, export-doc.test.ts:36); `process.env` config -> injected page globals |
| DuckDB-WASM Node runtime | duckdb, snap, group_derived, sorting, treeview, upload, saved_queries, cube_open, json_read, pivot_rows, typed_values×3, live_snap tests; torture, verify_remote, verify_real_data, verify_engine_differential harnesses | (c) `@duckdb/duckdb-wasm/blocking` + `NODE_RUNTIME` | Part E: `require('@duckdb/duckdb-wasm/blocking')` in 13 test files and 4 harnesses | use the browser build the product already ships (`:vendor` has `duckdb-browser-{mvp,eh}.worker.js`, B:706-709) inside the page; harness-side "DuckDB's own answer" checks move into the page too |
| load the WASM planner from runfiles | G4 (30) | (b) | `runfileDirUrl('WASM_PLANNER')` (`pure-protocol/test/lite.ts:14-20`, `test/catalog-builder.ts`) uses `process.env` + `node:fs` (`tools/js/runfiles.mts:8,13`) | serve `//wasm:planner_dir` from the test's static server and `fetch` it, as `demo/planners.ts` does in the product |
| read sources / build outputs as files | G2 scanners (6), bundle_budget, dist_complete | (c) node:fs, node:zlib | `runfiles.mts:61-72`; `bundle-budget.test.ts:7-10`; `dist-complete.test.ts:6` | these are file checks, not browser checks: do them in the driver (a small native/JVM checker) or as Bazel analysis (`dist_complete`'s file list can be a `build_test` / `diff_test` on a manifest; gzip budget via a native tool) |
| serve files over HTTP | `serve`, every harness via `harness.serve` (`demo/harness.mjs:12,68-90`), portability_test | (c) node:http, node:net | `serve.mjs:19`; `harness.mjs:12,82` | a static file server in the driver (or the warehouse's own `--site` server, which already serves `:dist`: `warehouse/defs.bzl:62`) |
| start servers / processes | torture_test (legend-lite JVM), live_snap_test (native warehouse), verify_app(_test) (Postgres + warehouse), verify_picker_test (`:serve`) | (c) child_process | `harness.mjs:153` (dynamic child_process), `live-snap.ts:25`, `verify-app.mjs:15,31` | the driver starts them (it is the privileged side), passes ports to the page |
| write generator outputs | emit_offer_queries, emit_cube_queries, make_link_dictionary, test_imports_tool, make_dist, make_sample | (b)/(c) node:fs | `offer-facts/emit.ts:18,162-164`; `wasm-differential/emit.ts:7,20-21`; `test-imports.mts:9,62`; `make-dist.mjs:11` | emit.ts/make.ts: run the product TS in headless Chromium, the driver writes stdout; test-imports: esbuild `--metafile` per test entry (native) gives the exact import closure; make_dist: `copy_to_directory` + one template expansion for the inlined CSS (`make-dist.mjs:39-49`) |
| drive a browser | 10 browser tests, 15 Playwright harnesses | (c) Playwright | Part E | CDP over `--remote-debugging-pipe` (the user's decision); the union of operations is in Part E |
| install a browser | install_browser | (c) Playwright CLI | `install-browser.mjs:7-14` | delete: the pinned Chromium (`//tools/browser`) replaces it once //query and //site use it |
| zone/locale/reporter plumbing | every node_test | (c) | `tools/js/zone.mjs:5-6` (process.env TZ), `strict-reporter.mjs:15` (process.exitCode) | the page runner sets its own reporting; TZ for the three typed_values zones becomes a Chromium launch env (`TZ=`) or `Emulation.setTimezoneOverride` |

No shipped code needs Node: `datacube/src` imports no `node:*` (grep empty); the only Node mention in engine-client
is a comment (`engine-client/src/warehouse.ts:199`).

---

## Part E: Playwright and Node APIs per harness and test, and what a CDP driver must provide

**Method.** A regex scan (script kept in my scratchpad, not the repo) over each file, comments skipped. Playwright
calls are counted only in files that import Playwright or are page helpers; `filter` only with an object argument
(`.filter({...})`, Playwright's), `count/first/last/all` only with empty parens, `keyboard.*`/`mouse.*` separately.
Caveat: `click`, `focus`, `getAttribute`, `dispatchEvent` could also be DOM calls inside a `page.evaluate`
callback; a grep for such lines found 1 (`verify-features.mjs:1246`, `document.querySelector(...).getAttribute`).
`event:data/exit/error` are child_process events, listed under Node. Lines are the first four; counts are exact
regex hits.

### E1. Harnesses (`datacube/demo/*.mjs`)

| file (datacube/demo/) | targets | Playwright API: count [lines] | Node API: count [lines] |
|---|---|---|---|
| chaos.mjs | //datacube:chaos (bazel run only) | locator×31 [116,157,160,169,...]; click×27 [117,175,176,177,...]; count×17 [117,157,170,181,...]; first×15 [117,160,198,203,...]; nth×9 [172,186,193,243,...]; keyboard.press×5 [200,227,237,396,...]; waitForTimeout×4 [122,271,413,449]; dispatchEvent×3 [346,354,356]; fill×3 [342,353,355]; waitForSelector×3 [121,298,309]; evaluate×2 [133,299]; close×1 [459]; dblclick×1 [243]; event:console×1 [65]; event:pageerror×1 [71]; focus×1 [304]; goto×1 [120]; hover×1 [219]; last×1 [214]; launch×1 [60]; mouse.wheel×1 [221]; newPage×1 [61]; textContent×1 [160] | process.env×3 [35,36,37]; process.exit×2 [54,462]; fetch (web API)×1 [44]; pkg:playwright×1 [30] |
| engine-cases.mjs | helper (data only) | — | — |
| grid-invariants.mjs | helper: a function passed to page.evaluate (in-page code already) | — | — |
| harness.mjs | helper of every harness: serve(), startServer(), frames(), outPath() | evaluate×1 [140] | process.env×3 [99,111,157]; child_process event:data×2 [167,168]; child_process event:exit×2 [169,181]; process.exit×2 [130,132]; server/child close()×2 [54,86]; Buffer.alloc×1 [46]; child_process event:error×1 [170]; node:child_process (dynamic)×1 [153]; node:fs.mkdirSync×1 [13]; node:fs.mkdtempSync×1 [13]; node:fs/promises.mkdtemp×1 [14]; node:fs/promises.open×1 [14]; node:fs/promises.readFile×1 [14]; node:fs/promises.stat×1 [14]; node:http.createServer×1 [12]; node:net (dynamic)×1 [82]; node:os.tmpdir×1 [15]; node:path.dirname×1 [16]; node:path.extname×1 [16]; node:path.join×1 [16] |
| install-browser.mjs | //datacube:install_browser (CI browser lane step, gates-run.yml:151) | — | createRequire×2 [8,11]; import.meta.url×1 [11]; node:child_process.spawnSync×1 [7]; node:module.createRequire×1 [8]; node:path.path×1 [9]; process.argv×1 [14]; process.execPath×1 [14]; process.exit×1 [15]; require:playwright×1 [13] |
| make-dist.mjs | //datacube:make_dist (tool of :dist) | — | process.argv×3 [17,21,22]; node:fs (dynamic)×1 [51]; node:fs/promises.cp×1 [11]; node:fs/promises.mkdir×1 [11]; node:fs/promises.readFile×1 [11]; node:fs/promises.rm×1 [11]; node:fs/promises.writeFile×1 [11]; node:path.join×1 [12]; node:path.resolve×1 [12]; process.exit×1 [19] |
| make-sample.mjs | //datacube:make_sample | — | Buffer.byteLength×1 [38]; node:fs/promises.writeFile×1 [10]; node:path.resolve×1 [11]; process.argv×1 [15]; process.cwd×1 [28]; process.env×1 [28]; process.exit×1 [24] |
| measure-startup.mjs | //datacube:measure_startup (bazel run only) | close×2 [43,45]; evaluate×1 [34]; goto×1 [25]; launch×1 [19]; newContext×1 [22]; newPage×1 [23]; waitForFunction×1 [26] | process.env×2 [14,15]; pkg:playwright×1 [10] |
| run-stress.mjs | //datacube:run_stress, //datacube:run_stress_test | evaluate×2 [20,23]; close×1 [24]; event:pageerror×1 [14]; goto×1 [16]; launch×1 [11]; newPage×1 [12]; waitForFunction×1 [18] | node:fs/promises.writeFile×1 [4]; pkg:playwright×1 [5]; process.exit×1 [147] |
| serve.mjs | //datacube:serve, via :datacube_serve in verify_picker_test | — | process.stdin×3 [41,42,43]; process.exit×2 [41,42]; process.platform×2 [126,127]; node:child_process.execFileSync×1 [20]; node:fs/promises.stat×1 [21]; node:http.createServer×1 [19]; node:net (dynamic)×1 [104]; node:path.basename×1 [22]; node:path.extname×1 [22]; node:path.resolve×1 [22]; process.argv×1 [29]; process.cwd×1 [36]; process.env×1 [36] |
| shots.mjs | //datacube:shots (bazel run only) | locator×47 [38,44,77,80,...]; click×19 [44,61,77,108,...]; waitForTimeout×16 [124,132,141,156,...]; nth×14 [108,137,137,138,...]; first×10 [38,44,77,82,...]; waitForSelector×7 [45,88,111,146,...]; evaluate×6 [54,117,126,240,...]; focus×4 [60,96,121,130]; keyboard.press×4 [97,98,134,188]; count×3 [230,252,253]; dispatchEvent×3 [198,206,221]; fill×3 [197,205,220]; selectOption×3 [204,216,219]; dblclick×2 [169,270]; last×2 [218,254]; waitForFunction×2 [99,255]; check×1 [176]; close×1 [288]; filter×1 [81]; goto×1 [85]; launch×1 [30]; newPage×1 [31]; screenshot×1 [39] | process.argv×2 [23,25]; node:fs.mkdirSync×1 [13]; node:path.resolve×1 [14]; pkg:playwright×1 [16]; process.cwd×1 [24]; process.env×1 [24]; process.exitCode×1 [286] |
| torture.mjs | //datacube:torture, //datacube:torture_test | — | process.hrtime×4 [355,368,388,390]; createRequire×2 [21,108]; process.env×2 [37,46]; require:@duckdb/duckdb-wasm/blocking×2 [109,110]; NODE_RUNTIME×1 [114]; fetch (web API)×1 [41]; import.meta.url×1 [108]; node:fs.readFileSync×1 [19]; node:module.createRequire×1 [21]; node:path.path×1 [22]; process.exit×1 [452] |
| typed-view.mjs | helper (readView/readColumn: page.evaluate) | evaluate×1 [20] | — |
| verify-app.mjs | //datacube:verify_app (manual), //datacube:verify_app_test | locator×18 [123,123,125,126,...]; count×8 [129,135,140,145,...]; click×3 [123,126,141]; first×3 [125,126,168]; goto×3 [111,167,179]; waitForFunction×3 [105,127,142]; event:pageerror×2 [96,161]; innerText×2 [131,147]; newPage×2 [94,160]; nth×2 [123,123]; $$eval×1 [119]; addInitScript×1 [98]; close×1 [185]; evaluate×1 [113]; exposeFunction×1 [97]; hover×1 [125]; launch×1 [93]; reload×1 [153]; textContent×1 [180]; waitFor×1 [168] | process.env×9 [23,24,25,30,...]; child_process event:exit×5 [37,45,85,193,...]; process.exit×3 [60,67,209]; child_process event:data×2 [40,80]; fetch (web API)×1 [195]; node:child_process.spawn×1 [15]; pkg:playwright×1 [16]; process.on×1 [37] |
| verify-calc-vocabulary.mjs | //datacube:verify_calc_vocabulary, //datacube:calc_vocabulary_test | — | process.env×3 [40,42,83]; process.exit×2 [77,161]; fetch (web API)×1 [84]; node:fs/promises.readFile×1 [23]; node:path.path×1 [34]; node:url.pathToFileURL×1 [35] |
| verify-charts.mjs | //datacube:verify_charts, //datacube:verify_charts_test | locator×24 [54,69,69,69,...]; first×12 [54,69,70,71,...]; click×10 [69,71,82,83,...]; count×4 [129,135,151,152]; waitForFunction×4 [36,57,143,153]; evaluate×3 [35,51,53]; waitFor×3 [115,124,138]; evaluateAll×2 [117,145]; nth×2 [69,69]; screenshot×2 [121,148]; close×1 [161]; event:pageerror×1 [19]; filter×1 [87]; getAttribute×1 [140]; goto×1 [62]; hover×1 [70]; launch×1 [16]; newContext×1 [17]; newPage×1 [17]; textContent×1 [94]; waitForSelector×1 [63] | process.env×2 [121,148]; pkg:playwright×1 [9]; process.exit×1 [167] |
| verify-cubes.mjs | //datacube:verify_cubes, //datacube:verify_cubes_test | locator×41 [80,90,102,107,...]; click×20 [93,96,104,116,...]; waitFor×13 [108,119,121,155,...]; evaluate×9 [63,82,146,211,...]; waitForFunction×9 [65,139,220,234,...]; textContent×7 [137,192,225,254,...]; setInputFiles×5 [127,194,243,371,...]; close×4 [355,377,388,446]; event:dialog×4 [269,340,397,430]; first×4 [80,90,180,434]; isVisible×4 [81,102,107,116]; waitForSelector×4 [73,191,242,370]; allTextContents×2 [181,292]; event:pageerror×2 [48,332]; fill×2 [134,156]; goto×2 [72,333]; newPage×2 [46,331]; hover×1 [90]; inputValue×1 [305]; isHidden×1 [280]; launch×1 [42]; newContext×1 [44]; waitForEvent×1 [430] | node:fs/promises.writeFile×1 [15]; node:path.join×1 [16]; pkg:playwright×1 [17]; process.exit×1 [452] |
| verify-engine-differential.mjs | //datacube:verify_engine_differential (bazel run only; needs ENGINE) | — | process.exit×4 [254,265,280,358]; createRequire×2 [44,98]; import.meta.url×2 [62,98]; process.env×2 [58,60]; require:@duckdb/duckdb-wasm/blocking×2 [99,100]; NODE_RUNTIME×1 [113]; fetch (web API)×1 [260]; node:fs/promises.readFile×1 [46]; node:module.createRequire×1 [44]; node:path.path×1 [45] |
| verify-engine.mjs | //datacube:verify_engine (bazel run only; needs ENGINE) | — | import.meta.url×3 [78,92,149]; process.env×2 [24,27]; process.exit×2 [85,237]; fetch (web API)×1 [36]; node:fs/promises.readFile×1 [18] |
| verify-features.mjs | //datacube:verify_features, //datacube:verify_features_test | locator×307 [169,175,405,421,...]; click×130 [171,406,427,473,...]; evaluate×124 [211,260,293,338,...]; first×98 [171,405,443,448,...]; count×59 [170,176,452,796,...]; selectOption×33 [964,997,1026,1081,...]; nth×25 [421,421,997,1005,...]; fill×19 [973,1005,1084,1118,...]; waitForSelector×17 [428,538,635,2004,...]; keyboard.press×15 [178,799,1054,1909,...]; check×14 [1103,2013,2041,3222,...]; waitFor×14 [594,600,602,5046,...]; textContent×13 [1075,1946,2743,2912,...]; dispatchEvent×11 [868,3526,3674,3676,...]; dragTo×11 [2147,3573,3725,3742,...]; getAttribute×11 [768,1206,1207,1246,...]; waitForFunction×10 [339,622,646,1011,...]; uncheck×9 [2014,2042,4796,4820,...]; allTextContents×8 [1934,1987,2877,2936,...]; hover×8 [470,531,598,1877,...]; boundingBox×7 [826,828,836,842,...]; inputValue×7 [926,1086,1087,2281,...]; mouse.move×7 [829,831,832,2546,...]; dblclick×5 [1754,4996,5004,5019,...]; innerText×5 [1248,1256,1260,1859,...]; isDisabled×5 [2741,2896,2918,3232,...]; press×5 [974,1006,1085,1119,...]; filter×4 [444,446,446,2125]; isChecked×4 [4795,4832,4875,4882]; evaluateAll×3 [1977,2871,5142]; event:download×3 [1801,1856,1878]; mouse.down×3 [830,2547,2561]; mouse.up×3 [833,2550,2563]; isHidden×2 [4975,5084]; isVisible×2 [522,592]; setViewportSize×2 [2623,2636]; waitForEvent×2 [1801,1878]; $$eval×1 [5126]; close×1 [5149]; evaluateHandle×1 [3673]; event:console×1 [140]; event:dialog×1 [618]; event:pageerror×1 [138]; focus×1 [2379]; goto×1 [634]; launch×1 [113]; mouse.click×1 [182]; newContext×1 [114]; newPage×1 [118]; route×1 [126]; screenshot×1 [285]; setInputFiles×1 [604] | process.env×18 [61,62,104,123,...]; Buffer.from×2 [1782,1892]; fetch (web API)×1 [5183]; import.meta.url×1 [5181]; node:fs/promises (dynamic)×1 [5180]; node:fs/promises.readFile×1 [21]; node:fs/promises.writeFile×1 [21]; node:os.tmpdir×1 [22]; node:path.join×1 [23]; pkg:playwright×1 [24]; process.cwd×1 [284]; process.exit×1 [5210]; process.platform×1 [5034] |
| verify-page.mjs | //datacube:verify_page, //datacube:verify_page_test | locator×14 [60,61,71,73,...]; evaluate×7 [36,42,116,117,...]; first×7 [60,61,97,101,...]; click×5 [97,101,111,134,...]; keyboard.press×4 [98,102,112,133]; boundingBox×2 [120,120]; count×2 [71,141]; waitForFunction×2 [43,67]; close×1 [153]; dragTo×1 [61]; evaluateAll×1 [73]; event:pageerror×1 [19]; goto×1 [66]; launch×1 [16]; newContext×1 [17]; newPage×1 [17]; screenshot×1 [132]; waitFor×1 [114] | process.env×2 [132,132]; process.platform×2 [96,110]; pkg:playwright×1 [9]; process.exit×1 [159] |
| verify-picker.mjs | //datacube:verify_picker, //datacube:verify_picker_test | click×9 [62,66,68,70,...]; locator×6 [65,65,66,66,...]; close×4 [50,247,254,259]; waitForFunction×4 [46,55,140,210]; evaluate×3 [101,157,215]; goto×3 [45,54,206]; newPage×3 [31,44,203]; waitForSelector×3 [63,69,156]; event:pageerror×2 [33,205]; textContent×2 [87,143]; $$eval×1 [75]; event:download×1 [93]; fill×1 [138]; hover×1 [65]; inputValue×1 [86]; keyboard.press×1 [192]; launch×1 [24]; newContext×1 [27]; waitFor×1 [67]; waitForEvent×1 [93]; waitForTimeout×1 [193] | process.env×3 [18,22,29]; pkg:playwright×1 [14]; process.exit×1 [260] |
| verify-real-data.mjs | //datacube:verify_real_data, //datacube:verify_remote_test (shard 1) | $$eval×2 [125,130]; close×1 [179]; event:pageerror×1 [116]; event:request×1 [114]; goto×1 [120]; launch×1 [111]; newPage×1 [112]; textContent×1 [134]; waitForFunction×1 [121] | process.env×5 [32,32,33,39,...]; createRequire×4 [43,45,63,65]; require:@duckdb/duckdb-wasm/blocking×4 [46,47,66,67]; NODE_RUNTIME×2 [51,71]; import.meta.url×2 [45,65]; node:module (dynamic)×2 [43,63]; node:path (dynamic)×2 [44,64]; pkg:playwright×1 [28]; process.exit×1 [187] |
| verify-remote-test.mjs | //datacube:verify_remote_test (entry; imports one of the two above) | — | process.env×4 [7,8,8,9]; node:fs/promises.writeFile×1 [5] |
| verify-remote.mjs | //datacube:verify_remote, //datacube:verify_remote_test (shard 0) | event:pageerror×2 [136,201]; goto×2 [138,205]; newPage×2 [134,199]; close×1 [225]; count×1 [210]; evaluate×1 [144]; launch×1 [133]; locator×1 [210]; textContent×1 [217]; waitForFunction×1 [139]; waitForSelector×1 [209] | createRequire×2 [17,24]; require:@duckdb/duckdb-wasm/blocking×2 [34,35]; NODE_RUNTIME×1 [48]; import.meta.url×1 [24]; node:fs/promises.rm×1 [18]; node:module.createRequire×1 [17]; node:path.path×1 [19]; pkg:playwright×1 [21]; process.exit×1 [228]; process.pid×1 [79] |
| verify-smoke.mjs | //datacube:verify_smoke, //datacube:verify_smoke_test | locator×8 [89,89,90,90,...]; evaluate×7 [106,134,147,173,...]; click×4 [88,90,92,94]; setViewportSize×2 [251,254]; waitFor×2 [91,93]; waitForFunction×2 [137,193]; close×1 [264]; count×1 [189]; dblclick×1 [191]; event:pageerror×1 [80]; first×1 [191]; goto×1 [129]; hover×1 [89]; launch×1 [72]; newContext×1 [75]; newPage×1 [78]; screenshot×1 [258]; setInputFiles×1 [95]; waitForSelector×1 [130] | process.env×8 [33,49,49,62,...]; process.exit×2 [69,284]; node:fs/promises.writeFile×1 [23]; node:path.join×1 [24]; pkg:playwright×1 [25] |
| verify-upload.mjs | //datacube:verify_upload, //datacube:verify_upload_test | locator×9 [71,71,72,72,...]; evaluate×7 [159,192,214,218,...]; click×4 [68,72,74,370]; waitForFunction×4 [61,95,113,278]; $$eval×3 [86,122,124]; textContent×3 [101,120,130]; count×2 [101,268]; setViewportSize×2 [189,243]; waitFor×2 [73,79]; close×1 [440]; dragTo×1 [277]; event:console×1 [55]; event:pageerror×1 [53]; first×1 [277]; goto×1 [59]; hover×1 [71]; launch×1 [50]; newPage×1 [51]; setInputFiles×1 [90]; waitForSelector×1 [69] | process.env×5 [32,33,33,34,...]; node:fs/promises (dynamic)×1 [38]; node:path (dynamic)×1 [39]; pkg:playwright×1 [18]; process.exit×1 [447] |
| verify-wasm-browser.mjs | //datacube:verify_wasm_browser, //datacube:verify_wasm_browser_test | locator×8 [68,69,70,96,...]; evaluate×3 [123,126,160]; first×3 [69,96,130]; waitForFunction×3 [55,64,136]; click×2 [130,135]; allTextContents×1 [70]; close×1 [198]; count×1 [68]; evaluateAll×1 [96]; event:console×1 [38]; event:pageerror×1 [46]; event:response×1 [35]; goto×1 [50]; hover×1 [133]; launch×1 [30]; newPage×1 [31]; textContent×1 [60]; waitForSelector×1 [131] | pkg:playwright×1 [18]; process.exit×1 [205] |
| warehouse-source.mjs | helper imported by verify-features.mjs (WAREHOUSE=... variant only) | locator×13 [33,35,38,38,...]; waitFor×8 [35,40,42,46,...]; click×7 [37,39,41,43,...]; fill×3 [48,49,50]; nth×3 [48,49,50]; waitForFunction×2 [59,61]; count×1 [47]; evaluate×1 [34]; first×1 [46]; hover×1 [38]; isVisible×1 [33] | process.env×6 [16,18,19,20,...] |

### E2. js_tests and their helpers (Node APIs; no test file imports Playwright except through the harnesses above)

Only `node:test` (describe/it/before/after) and `node:assert/strict`, nothing else Node-specific:

- with no DOM: adhoc-query, adhoc-session, adhoc-state, calc-fix, calc, cancel, catalog-model, chart-echarts, chart-tiles, child-groups, column-kind, config-readers, config, cube-adhoc-shell, cube-document, cube-editors-app, cube-lifecycle, cube-state, cube-store, cube-transactions, dimensions, drill, duckdb-cancel, engine-remote, epoch, escaping, export-model, format-scale, format, fuzz, grid, guardrails, host, infer, json-shape, menu-ids, menu, offer-facts, page-document, pivot-total, pivot-values, plane, planner, query, relation-type, remote, runner, sample, screen-colours, selection, state-guardrail, style, tile-layout, tree, type-columns, values, warehouse-session, wasm-flag, window-columns

- plus `jsdom` (`new JSDOM(...)` installed on globalThis): adhoc-mode, adhoc-transactions, app, apply-refusal, board, column-editor, columns-panel, columns-selector, cube-library, editor, editors-live, export-rich, filter-editor, form, grid-basics, grid-dom, grid-resize, menu-view, multi-cube, pivot-panel, save-dialog, scale, shell, source-picker, undo-coverage, window

- files with no Node API at all (helpers, data): test/adhoc-fixture.ts, test/catalog-builder.ts, test/fake-engine.ts, test/fake-planner.ts, test/gate.ts, test/generated/catalog-corpus.ts, test/lite-compiler.ts, test/wasm-differential/cases.ts

Files that use more than node:test/node:assert/jsdom:

| file | Node API: count [lines] |
|---|---|
| datacube/test/bundle-budget.test.ts | node:assert/strict.assert×1 [6]; node:fs.readFileSync×1 [7]; node:fs.readdirSync×1 [7]; node:path.dirname×1 [8]; node:path.join×1 [8]; node:test.describe×1 [9]; node:test.it×1 [9]; node:zlib.gzipSync×1 [10] |
| datacube/test/cube-fixture.ts | process.on×2 [91,111]; process.removeAllListeners×2 [90,110]; node:assert/strict.assert×1 [6]; pkg:jsdom×1 [7]; process.listeners×1 [109] |
| datacube/test/cube-open/cube-open.ts | createRequire×2 [8,68]; require:@duckdb/duckdb-wasm/blocking×2 [69,70]; NODE_RUNTIME×1 [74]; import.meta.url×1 [68]; node:assert/strict.assert×1 [7]; node:module.createRequire×1 [8]; node:path.path×1 [9]; node:test.before×1 [10]; node:test.describe×1 [10]; node:test.it×1 [10]; pkg:jsdom×1 [11] |
| datacube/test/dist-complete.test.ts | node:assert/strict.assert×1 [5]; node:fs.existsSync×1 [6]; node:fs.readFileSync×1 [6]; node:fs.statSync×1 [6]; node:path.dirname×1 [7]; node:path.join×1 [7]; node:test.describe×1 [8]; node:test.it×1 [8] |
| datacube/test/duckdb.test.ts | createRequire×2 [8,21]; require:@duckdb/duckdb-wasm/blocking×2 [28,29]; NODE_RUNTIME×1 [42]; import.meta.url×1 [21]; node:assert/strict.assert×1 [7]; node:module.createRequire×1 [8]; node:path.path×1 [9]; node:test.after×1 [10]; node:test.before×1 [10]; node:test.describe×1 [10]; node:test.it×1 [10] |
| datacube/test/export-doc.test.ts | Buffer.from×1 [36]; node:assert/strict.assert×1 [7]; node:test.describe×1 [8]; node:test.it×1 [8] |
| datacube/test/export.test.ts | Buffer.from×3 [146,151,159]; node:assert/strict.assert×1 [1]; node:test.describe×1 [2]; node:test.it×1 [2] |
| datacube/test/group-derived.test.ts | createRequire×2 [9,23]; require:@duckdb/duckdb-wasm/blocking×2 [24,25]; NODE_RUNTIME×1 [38]; import.meta.url×1 [23]; node:assert/strict.assert×1 [8]; node:module.createRequire×1 [9]; node:path.path×1 [10]; node:test.after×1 [11]; node:test.before×1 [11]; node:test.describe×1 [11]; node:test.it×1 [11] |
| datacube/test/json-read/json-read.ts | createRequire×2 [10,102]; require:@duckdb/duckdb-wasm/blocking×2 [103,104]; NODE_RUNTIME×1 [108]; import.meta.url×1 [102]; node:assert/strict.assert×1 [9]; node:module.createRequire×1 [10]; node:path.path×1 [11]; node:test.before×1 [12]; node:test.describe×1 [12]; node:test.it×1 [12]; pkg:jsdom×1 [13] |
| datacube/test/live-snap/live-snap.ts | createRequire×2 [27,107]; require:@duckdb/duckdb-wasm/blocking×2 [108,109]; NODE_RUNTIME×1 [113]; fetch (web API)×1 [74]; import.meta.url×1 [107]; node:assert/strict.assert×1 [24]; node:child_process.ChildProcess×1 [25]; node:child_process.spawn×1 [25]; node:fs.mkdtempSync×1 [26]; node:module.createRequire×1 [27]; node:os.tmpdir×1 [28]; node:path.path×1 [29]; node:test.after×1 [30]; node:test.before×1 [30]; node:test.it×1 [30]; process.env×1 [87]; process.stderr×1 [95] |
| datacube/test/pivot-rows/pivot-rows.ts | createRequire×2 [13,221]; require:@duckdb/duckdb-wasm/blocking×2 [222,223]; NODE_RUNTIME×1 [227]; import.meta.url×1 [221]; node:assert/strict.assert×1 [12]; node:module.createRequire×1 [13]; node:path.path×1 [14]; node:test.before×1 [15]; node:test.describe×1 [15]; node:test.it×1 [15]; pkg:jsdom×1 [16] |
| datacube/test/portability.test.ts | import.meta.url×2 [53,54]; process.cwd×2 [60,61]; process.env×2 [74,148]; node:assert/strict.assert×1 [14]; node:fs.mkdtempSync×1 [15]; node:fs.rmSync×1 [15]; node:fs.writeFileSync×1 [15]; node:fs.x×1 [126]; node:http.createServer×1 [126]; node:os.tmpdir×1 [16]; node:path.join×1 [17]; node:path.sep×1 [17]; node:test.after×1 [18]; node:test.describe×1 [18]; node:test.it×1 [18]; process.execPath×1 [79] |
| datacube/test/saved-queries.test.ts | createRequire×2 [8,35]; require:@duckdb/duckdb-wasm/blocking×2 [36,37]; NODE_RUNTIME×1 [44]; import.meta.url×1 [35]; node:assert/strict.assert×1 [6]; node:fs.readFileSync×1 [7]; node:module.createRequire×1 [8]; node:path.path×1 [9]; node:test.after×1 [10]; node:test.before×1 [10]; node:test.describe×1 [10]; node:test.it×1 [10] |
| datacube/test/share-link.test.ts | process.env×3 [104,105,110]; node:assert/strict.assert×1 [5]; node:crypto.createHash×1 [6]; node:test.describe×1 [7]; node:test.it×1 [7] |
| datacube/test/snap.test.ts | createRequire×2 [5,20]; require:@duckdb/duckdb-wasm/blocking×2 [24,25]; NODE_RUNTIME×1 [38]; import.meta.url×1 [20]; node:assert/strict.assert×1 [4]; node:module.createRequire×1 [5]; node:path.path×1 [6]; node:test.after×1 [7]; node:test.before×1 [7]; node:test.describe×1 [7]; node:test.it×1 [7] |
| datacube/test/sorting.test.ts | createRequire×2 [5,20]; require:@duckdb/duckdb-wasm/blocking×2 [21,22]; NODE_RUNTIME×1 [35]; import.meta.url×1 [20]; node:assert/strict.assert×1 [4]; node:module.createRequire×1 [5]; node:path.path×1 [6]; node:test.after×1 [7]; node:test.before×1 [7]; node:test.describe×1 [7]; node:test.it×1 [7] |
| datacube/test/treeview.test.ts | createRequire×2 [2,322]; require:@duckdb/duckdb-wasm/blocking×2 [323,324]; NODE_RUNTIME×1 [337]; import.meta.url×1 [322]; node:assert/strict.assert×1 [1]; node:module.createRequire×1 [2]; node:path.path×1 [3]; node:test.after×1 [4]; node:test.before×1 [4]; node:test.describe×1 [4]; node:test.it×1 [4] |
| datacube/test/typed-values/typed-values.ts | createRequire×2 [9,148]; process.env×2 [313,316]; require:@duckdb/duckdb-wasm/blocking×2 [149,150]; NODE_RUNTIME×1 [154]; import.meta.url×1 [148]; node:assert/strict.assert×1 [8]; node:module.createRequire×1 [9]; node:path.path×1 [10]; node:test.before×1 [11]; node:test.describe×1 [11]; node:test.it×1 [11]; pkg:jsdom×1 [12] |
| datacube/test/upload.test.ts | createRequire×2 [8,31]; require:@duckdb/duckdb-wasm/blocking×2 [32,33]; NODE_RUNTIME×1 [46]; import.meta.url×1 [31]; node:assert/strict.assert×1 [7]; node:module.createRequire×1 [8]; node:path.path×1 [9]; node:test.after×1 [10]; node:test.before×1 [10]; node:test.describe×1 [10]; node:test.it×1 [10]; process.env×1 [28] |
| datacube/test/wasm-differential/compare.ts | node:assert/strict.assert×1 [18]; node:fs.readFileSync×1 [19]; node:test.it×1 [20] |
| datacube/test/wasm-differential/emit.ts | node:fs.writeFileSync×1 [7]; process.argv×1 [12] |
| datacube/test/wasm-planner.test.ts | node:assert/strict.assert×1 [1]; node:test.describe×1 [2]; node:test.it×1 [2]; node:url.fileURLToPath×1 [3]; node:url.pathToFileURL×1 [3] |
| pure-protocol/test/lite.ts | node:url.fileURLToPath×1 [4] |
| tools/js/runfiles.mts | process.env×5 [13,13,30,37,...]; node:fs.readFileSync×1 [8]; node:path.basename×1 [9]; node:path.join×1 [9]; node:path.posix×1 [9]; node:url.pathToFileURL×1 [10] |
| tools/js/zone.mjs | process.env×3 [5,6,6] |
| tools/js/strict-reporter.mjs | process.exitCode×1 [15] |
| tools/browser/pinned-chromium.mjs | process.env×13 [14,19,20,20,...]; node:fs.existsSync×1 [11]; node:fs.realpathSync×1 [11]; node:path.dirname×1 [12]; node:path.join×1 [12]; node:path.resolve×1 [12]; process.cwd×1 [26]; process.platform×1 [30] |

Notes on E2: (1) the six scanners (config-readers, guardrails, menu-ids, portability, state-guardrail, wasm-flag)
and the planner tests' helpers (`test/catalog-builder.ts`, `test/lite-compiler.ts` -> `pure-protocol/test/lite.ts`)
reach Node through `tools/js/runfiles.mts` (`Sources`, `runfileDirUrl`: `node:fs.readFileSync`, `process.env`),
listed in the last rows; bundle-budget, dist-complete, compare.ts and the standalone tests import it directly
(`grep -ln runfiles.mts datacube/test`: 16 files). (2) Every node_test also runs `tools/js/zone.mjs` (preload) and
`tools/js/strict-reporter.mjs` (`tools/js/defs.bzl:45-51`). (3) `portability.test.ts` `node:fs.x` at :126 is the
scanner's reading of `const x = await import('node:http')`-style code; the file's real Node APIs are
`node:fs.mkdtempSync/rmSync/writeFileSync`, `node:http.createServer`, `node:os.tmpdir`, `process.cwd/env/execPath`.

### E3. The union: what a CDP driver must provide

Union over the 15 Playwright harness files + 2 page helpers (warehouse-source.mjs, typed-view.mjs), totals from the
E1 scan (calls, files):

| operation | Playwright calls (count, files) | CDP domain/method | can it run in the page instead? |
|---|---|---|---|
| start/stop a browser, contexts, tabs | `launch` 15/15, `newContext` 7/7, `newPage` 20/15, `close` 22/15 | `Target.createBrowserContext`, `Target.createTarget`, `Target.closeTarget`, `Browser.close` (pipe transport) | no: driver |
| navigate | `goto` 21/15, `reload` 1/1 (verify-app.mjs:153) | `Page.navigate`, `Page.reload`, `Page.loadEventFired` | no: driver (a page can reload itself, but the driver must survive it) |
| run code in the page | `evaluate` 178/16, `evaluateAll` 7/4, `$$eval` 8/5, `evaluateHandle` 1/1, `waitForFunction` 49/15 | `Runtime.evaluate` / `Runtime.callFunctionOn` with `awaitPromise`, `returnByValue` | already in-page; the driver only transports |
| query and read the DOM | `locator` 527/13, `first` 155/11, `count` 99/11, `nth` 55/6, `filter({..})` 6/3, `last` 3/2, `textContent` 31/10, `innerText` 7/2, `inputValue` 9/3, `getAttribute` 12/2, `allTextContents` 11/3, `isVisible` 7/3, `isHidden` 3/2, `isChecked` 4/1, `isDisabled` 5/1, `boundingBox` 9/2 | none needed beyond `Runtime.evaluate` | **yes**: `querySelectorAll`, `textContent`, `.value`, `getAttribute`, `checkVisibility()`/`getBoundingClientRect()` in a page-side helper library |
| wait for the DOM | `waitFor` 45/9, `waitForSelector` 39/10 | none beyond `Runtime.evaluate` (awaited promise) | **yes**: a `MutationObserver`/`requestAnimationFrame` poll in the page |
| fixed sleep | `waitForTimeout` 21/3 (chaos 4, shots 16, verify-picker 1) | none | yes (`setTimeout`), and G-11 says remove them |
| pointer input | `click` 240/12, `dblclick` 9/4, `hover` 17/10, `dragTo` 13/3, `mouse.move` 7/1, `mouse.down` 3/1, `mouse.up` 3/1, `mouse.click` 1/1, `mouse.wheel` 1/1 (chaos) | `Input.dispatchMouseEvent` (+ `DOM.getBoxModel`/page-side rect for coordinates) | partly: `datacube/src` never checks `isTrusted` (grep empty), so synthetic `MouseEvent`/`PointerEvent`/`DragEvent` would reach its listeners; but CSS `:hover`, native HTML5 drag start and real hit-testing need trusted input: keep `Input.dispatchMouseEvent` in the driver for hover/drag (verify-features drag/resize lines 826-842, 2545-2563) |
| keyboard and text | `keyboard.press` 29/5, `press` 5/1, `fill` 31/6, `selectOption` 36/2, `check` 15/2, `uncheck` 9/1, `focus` 6/3 | `Input.dispatchKeyEvent`, `Input.insertText` | mostly yes: `fill`/`selectOption`/`check` are value-set + `input`/`change` events in the page; keyboard shortcuts read `keydown` listeners (no isTrusted check), so synthetic `KeyboardEvent` works; keep `dispatchKeyEvent` for default actions the browser performs only on trusted keys (typing into inputs, Tab focus) |
| synthetic events | `dispatchEvent` 17/3 | `Runtime.evaluate` | yes: already synthetic |
| file input | `setInputFiles` 8/4 (verify-cubes ×5, verify-features, verify-smoke, verify-upload) | `DOM.setFileInputFiles` (driver reads the file from disk) | partly: a page can build `File` objects and assign `input.files` via `DataTransfer`, but the bytes must come from the driver (disk) |
| downloads | `event:download` 4/2 + `waitForEvent('download')` (verify-features :1801,1856,1878; verify-picker :93) | `Browser.setDownloadBehavior`, `Browser.downloadWillBegin/downloadProgress` | no: driver (or intercept the Blob/anchor in the page and return bytes by `Runtime.evaluate`) |
| dialogs | `event:dialog` 5/2 (verify-cubes ×4, verify-features ×1) | `Page.javascriptDialogOpening`, `Page.handleJavaScriptDialog` | yes: override `window.confirm/prompt/alert` by an init script (product calls `window.confirm` via `src/host.ts:30`) |
| page errors and console | `event:pageerror` 17/13, `event:console` 4/4 | `Runtime.exceptionThrown`, `Runtime.consoleAPICalled` | partly: `window.onerror`/`unhandledrejection` in an init script, but load-time errors before it runs need the CDP events: driver |
| network | `route` 1/1 (verify-features.mjs:126, abort the WASM files under `NO_WASM`), `event:request` 1/1 (verify-real-data.mjs:114), `event:response` 1/1 (verify-wasm-browser.mjs:35) | `Fetch.enable` + `Fetch.failRequest`; `Network.requestWillBeSent` / `responseReceived` | no for the abort (driver); request/response logging could be `PerformanceObserver('resource')` in the page |
| init scripts and bindings | `addInitScript` 1/1, `exposeFunction` 1/1 (verify-app.mjs:97-98) | `Page.addScriptToEvaluateOnNewDocument`, `Runtime.addBinding` | the driver must provide these two: they are what lets everything else move into the page |
| viewport | `setViewportSize` 6/3 (verify-features, verify-smoke, verify-upload); `colorScheme` option (verify-picker.mjs:29) | `Emulation.setDeviceMetricsOverride`, `Emulation.setEmulatedMedia` | no: driver |
| screenshots | `screenshot` 6/5 (shots, verify-charts ×2, verify-features, verify-page, verify-smoke) | `Page.captureScreenshot` | no: driver (all but shots' are optional `SHOTS=` artifacts) |

**Minimal CDP driver** (from the table): pipe launch of the pinned Chromium; `Target.createBrowserContext/createTarget/closeTarget`;
`Page.navigate/reload` + load events; `Runtime.evaluate/callFunctionOn` (awaitPromise); `Page.addScriptToEvaluateOnNewDocument`;
`Runtime.addBinding`; `Runtime.exceptionThrown/consoleAPICalled`; `Input.dispatchMouseEvent`, `Input.dispatchKeyEvent`,
`Input.insertText`; `DOM.setFileInputFiles`; `Browser.setDownloadBehavior` + download events; `Fetch.enable/failRequest`;
`Emulation.setDeviceMetricsOverride/setEmulatedMedia`; `Page.captureScreenshot`; `Page.handleJavaScriptDialog` (or the
init-script override). Everything in the "query/read/wait/evaluate/synthetic events/fill/select/check" rows (
1,405 of the 1,878 counted Playwright calls) becomes a page-side helper library, leaving the driver with trusted
pointer/keyboard input, files in and out, screenshots, network abort, viewport, errors, and process/server start.

**Node APIs the driver side must replace** (union of E1 + E2): `node:http.createServer` (static server: `harness.mjs:12`,
`serve.mjs:19`, `portability.test.ts:126`); `node:child_process.spawn/spawnSync/execFileSync` (servers and Postgres:
`harness.mjs:153`, `live-snap.ts:25`, `verify-app.mjs:15`, `serve.mjs:20`, `install-browser.mjs:7`); `node:net`
(free port: `harness.mjs:82`, `serve.mjs:104`); `node:fs` / `fs/promises` / `os.tmpdir` (fixtures, outputs, scanners,
artifact checks: E1/E2 rows); `node:zlib.gzipSync` (`bundle-budget.test.ts:10`); `process.env` (BUILD-provided
config, every runner); DuckDB-WASM `blocking` + `NODE_RUNTIME` (13 test files, 4 harnesses). In-page replacements exist
for `node:crypto.createHash` (`crypto.subtle.digest`), `Buffer` (`TextEncoder`/`Uint8Array`), `node:url`
(`URL`), `fetch` (already the web API), `node:test`/`node:assert` (a page runner), `jsdom` (the real DOM), and the
DuckDB Node runtime (the browser worker build already in `:vendor`, B:706-709).
