# Bazel audit: browser harnesses, Query, site and fixtures

Audited at origin/main 16c8120d5 (2026-10-02)

**Slice:** `datacube/demo`, the harness section of `datacube/BUILD.bazel`, `query/`, `site/` and `fixtures/`.

**Most important new findings:**
- `//site:verify` is tagged for CI but never runs. CI's query only looks in `//datacube:*` and `//query:*`.
- `//datacube:dist` is missing `fonts.css` and the font files. `//datacube:app` serves that directory, and no test covers it.
- `run-stress.mjs:157` uses a variable that doesn't exist. It's a CI harness, so this bug is live there.
- Three CI-path harnesses write files into the runfiles tree, or into the source tree via a path that isn't in the repo's `.gitignore`.
- Fixed ports go beyond the K13 list: 8741 (`verify-remote`, which CI runs), 8732 (`shots`) and 8734 (`chaos`). All three bind every network interface, not just loopback.
- Two separately locked Playwright copies rely on one non-Bazel browser install.

## Section 1: Per-target table

How each harness finds the site: every datacube harness uses `ROOT = fileURLToPath(new URL('..', import.meta.url))`. That only works because rules_js puts sources and outputs side by side under bazel-bin (`datacube/BUILD.bazel:599-602`). None of them uses a runfiles library. `verify-features` also reaches `ROOT/../fixtures/saved-queries` the same way (`verify-features.mjs:84`).

How Playwright finds Chromium: every harness calls `chromium.launch()` with no `executablePath`. Playwright then looks in its own per-user browser cache, which `//datacube:install_browser` fills.

| Target (file) | CI? | Needs that Bazel does not provide | Writes, and where | Asserts or only measures | Flakiness risks | Right Bazel shape |
|---|---|---|---|---|---|---|
| `//datacube:chaos` (chaos.mjs) | no | legend-lite server on :8080 (probed at :58; not in `data`, though it is buildable as `//core:server`); Playwright cache Chromium; binds fixed **:8734** on all interfaces (:38, :87); env ENGINE, CHAOS_ROUNDS, CHAOS_SEED | nothing | asserts (exit 1), but `tolerant()` swallows action errors by design (:127) | seeded random storm; sleeps of 40-100 ms (:300), 1200 ms (:151), 3500 ms (:442), 1800 ms (:478); selector races are expected | manual-tagged js_test: start `$(rootpath //core:server)` on port 0 and parse "started on port N" (`LegendHttpServer.java:347`); ephemeral 127.0.0.1 port; pinned browser |
| `//datacube:torture` (torture.mjs) | no | server on :8080 (:36); **no browser** (Node plus duckdb-wasm/blocking), yet it gets playwright and `:site` | nothing (DuckDB in memory) | asserts | wall-clock assertion `ms < 1000` (:385) | js_test, no browser, server from `$(rootpath //core:server)` on port 0. Could join `//datacube:tests` |
| `//datacube:shots` (shots.mjs) | no | browser; fixed **:8732** on all interfaces (:25, :52) | `$BUILD_WORKING_DIRECTORY/datacube-shots/*.png` (:24) | screenshots only (exit 1 only on an exception) | about 15 fixed sleeps (150-2500 ms) | plain `bazel run` dev tool; ephemeral port; in a test shape, write to TEST_UNDECLARED_OUTPUTS_DIR |
| `//datacube:run_stress` (run-stress.mjs, stress.ts, stress-corpus.ts) | **yes** | browser | `$BUILD_WORKING_DIRECTORY/stress-results.json` (:98-100). In CI that is the repo root, which the root `.gitignore` does not cover | asserts on unexplained breakage. Latent bug at :157 (N7) | waits up to 600 s (:35) | js_test with size=enormous; results to TEST_UNDECLARED_OUTPUTS_DIR |
| `//datacube:verify_charts` | **yes** | browser | `${SHOTS}/*.png`, relative to cwd, which is runfiles (:137, :164) | asserts | 400 ms "settle" window (:63), 500/1500 ms sleeps | js_test |
| `//datacube:verify_cubes` | **yes** | browser with clipboard permissions | four CSVs (one is 400k rows) in `os.tmpdir()/dc-cubes-*`, never removed (:41-54) | asserts | sleeps of 300, 1000 and 1500 ms (:83, :418, :434) | js_test (TMPDIR becomes TEST_TMPDIR) |
| `//datacube:verify_features` (5,179 lines) | **yes** | browser; optional warehouse (WAREHOUSE=…:8772 plus fixed PORT, `warehouse-source.mjs:6`); PLANNER=engine canary fetches legend-engine from config.json (:5148-5158); about 14 env knobs (DATA, ONLY, PORT, PLANNER, NO_WASM, CPU_THROTTLE, DEBUG, SHOTS, TIMINGS, WAREHOUSE*) | `tmpdir()/datacube-verify-features.csv`, a **fixed name** never removed (:54); SHOTS into BUILD_WORKING_DIRECTORY (:275) | asserts. 118 checks share one page and depend on order (:576-578; the last check turns the page into a board, :5060). `ONLY=` can run zero checks and still exit 0 | 74 `waitForTimeout` calls; 30 s per-check wall-clock deadline (:76); 20 s settle deadline | js_test split into shards; drop the env knobs; the warehouse/engine variants become separate manual tests |
| `//datacube:verify_page` | **yes** | browser | `${SHOTS}/page-window.png`, relative to cwd (:145) | asserts | busy-poll settle with a 300 ms quiet window and 30 s deadline (:55-68) | js_test |
| `//datacube:verify_real_data` | **yes** | browser. DATA/EXPECT/FORMAT knobs (the README recipe uses the host `duckdb` CLI) | duckdb COPY writes `dc-real-<pid>.parquet` into **cwd, i.e. the runfiles tree**, then deletes it (:69-72); `tmpdir()/dc-real-*` never removed (:73) | asserts (and fails when EXPECT is empty, :198) | 120 s waits | js_test with the fixture generated in TEST_TMPDIR, or by a Bazel action |
| `//datacube:verify_remote` | **yes** | browser; **fixed :8741 on all interfaces** (:27, :148); uses `localhost` | the same cwd write (`dc-fixture-*.parquet`, :78-83) | asserts | 60 s waits | js_test with an ephemeral 127.0.0.1 port; overlaps verify_real_data |
| `//datacube:verify_smoke` | **yes** | browser; ONLY, ROWS | samples in `tmpdir()/dc-smoke-*`, never removed (:75) | asserts | sleeps of 400, 900, 250 and 150 ms | js_test (sharded per sample) |
| `//datacube:verify_upload` | **yes** | browser; DATA, EXPECT_ROWS, EXPECT_COLS, DEBUG | a 50k-row CSV in `tmpdir()/dc-upload-*` (:43) | asserts | 400/150 ms sleeps | js_test |
| `//datacube:verify_wasm_browser` | **yes** | browser | nothing | asserts (pins 3 rows, AMER, 5 currency cells) | 200/500 ms sleeps | js_test |
| `//datacube:verify_picker` | no | **a separately started `bazel run //datacube:serve` on :8000** (:13, :34; bare-origin probes hard-code 8000 even when `URL=` is set); SCHEME | browser download only | asserts | 150 ms sleep | js_test that serves the site itself like the others (`datacube/BUILD.bazel:794` says that dependency is the only reason it is not in CI) |
| `//datacube:verify_calc_vocabulary` | no | **no browser**; WASM planner from `./vendor` (needs `:site`); optional legend-engine on :6300, and the result changes depending on whether one is up (:72-85) | nothing | asserts | none | local half: js_test. It largely duplicates the build-time offer-facts compile (`datacube/test/calc.test.ts:166-168`). Engine half: manual test |
| `//datacube:verify_engine` | no | **external legend-engine on :6300** (shaded jar, not fetched by Bazel); ONLY | nothing | asserts; exit 2 when no engine | 120 s fetch timeout | manual-tagged js_test with the engine jar from a pinned http_file, started in the test on a free port |
| `//datacube:verify_engine_differential` | no | engine on :6300; ONLY | nothing | asserts, **but "skipped" cases (local-plane failures) never fail** (:299-302, :358) | 3 s probe | manual test, as above; count skips as failures |
| `//datacube:measure_startup` | no | browser; PAGE, RUNS | nothing | **measures only** (always exits 0) | wall-clock by nature | `bazel run` dev tool (or a benchmark target) |
| `//datacube:make_sample` | no | nothing, yet it gets playwright and `:site` (builds the whole site and the WASM planner to write a CSV) | `$BUILD_WORKING_DIRECTORY/sample-trades.csv` by default (`make-sample.mjs:28-29`); `/tmp` only in the usage example | n/a | n/a | `bazel run` dev tool with minimal deps (`:src` only) |
| `//datacube:verify_app` (manual) | no | **Postgres 16+** (DATACUBE_APP_PG/TABLE/GROUP); spawns `warehouse/serve.sh`, a generated bash launcher (`warehouse/defs.bzl:69-80`), with RUNFILES_DIR worked out by path arithmetic (`verify-app.mjs:28-29, 37-39`) | `tmpdir()/verify-app-*`, removed (:147) | asserts | 60/120 s waits | manual-tagged js_test, Postgres from a pinned binary archive or testcontainer; use the runfiles library and the warehouse binary directly |
| `//datacube:install_browser` | CI step (`gates-run.yml:131`) | network; mutates Playwright's per-user browser cache; `--with-deps` runs apt via sudo | outside the build graph | n/a | network | **delete**, replaced by a Bazel-pinned browser repository |
| `//datacube:serve` | no | `--open` uses host `open`/`xdg-open` (`serve.mjs:161-164`); default :8000 | nothing | n/a | n/a | `bazel run` dev tool (fine). Note: prints the requested port, so `--port 0` prints `:0` (`serve.mjs:141`) |
| `//datacube:dist` / `make_dist` | built by `//datacube:app` and verify_app | nothing | Bazel output (`out_dirs`) | **untested** | n/a | a proper build action, but the content is wrong (N2) |
| `//query:verify` (`query/demo/verify.mjs`) | **yes** | browser; spawns `//core:server` (shell launcher plus JDK) and the native warehouse | **`$BUILD_WORKSPACE_DIRECTORY/.scratch/verify/run-*`**: server store, warehouse data, screenshots, never cleaned (:29-31, :159) | asserts | **random ports** 18090+rand(500) and +1000 with no bind check (:25-26); 30 s engine wait; no warehouse start timeout (:52-59) | js_test: port 0 for the server (it prints its port), TEST_TMPDIR, runfiles library |
| `//query:serve` | no | default :8100 | nothing | n/a | n/a | `bazel run` dev tool (fine) |
| `//query:*_test`, `typecheck_test` | `//query:tests` | none found (`query/test/lite.ts` reads `../../wasm/planner/` and `../demo/models` by relative URL) | nothing | asserts | none | already correct |
| `//site:verify` | **tagged browser-ci but never run** (N1) | browser; Playwright through `createRequire('../datacube/node_modules/')` (`site/verify.mjs:15`) | nothing | asserts | `Date.now()` in the query name (:52); 120 s waits | js_test |
| `//site:serve`, `//site:dist` | no | default :8200 | Bazel output | n/a | n/a | correct |
| `query/tools/icons.mjs` | no target | curl, tar, npm registry (K3) | redirect into `query/src/ui/icons.ts` | n/a | network | http_archive for react-icons@5.5.0, a js_run_binary, and a diff test |
| `fixtures/saved-queries/make.mjs` | no target | `java -jar bazel-bin/...deploy.jar 18777 &`; relative paths from the repo root (K2) | writes `fixtures/saved-queries/*.json` in the source tree | prints only | fixed port; output contains timestamps | js_run_binary that starts `//core:server` on port 0, normalises timestamps, plus a diff test |

**Harnesses that duplicate each other:**
- `verify_upload` overlaps `verify_features` (sidebar collapse, header follows scroll, grouping) and `verify_smoke` (grid invariants, GROUP BY).
- `verify_real_data` and `verify_remote` both build a Parquet file with node DuckDB, serve it with Range support and open `index.html?remote=`.
- `verify_engine` is mostly covered by `verify_engine_differential`. Both, plus `verify_calc_vocabulary`, share `engine-cases.mjs`.
- `verify_calc_vocabulary`'s local half overlaps the offer-facts build step (`calc.test.ts:166-168`) and `remote.test.ts` covers the Node side of `mountRemote`.
- The source-picker and sample flows of `verify_picker` repeat in `verify_cubes` (`pickSample`) and `verify_features` (:5061).
- The static server (`createServer` + `servedPath` + its own `TYPES` map) is copy-pasted into about 15 files with differing MIME maps.

## Section 2: New findings

- **N1 (P1).** `site/BUILD.bazel:34` tags `//site:verify` `browser-ci`, but the CI loop queries `attr(tags,"browser-ci", //datacube:* + //query:*)` (`.github/workflows/gates-run.yml:163`). The one-origin end-to-end check therefore never runs. Fix: make it a js_test in a test_suite the browser lane runs, rather than tag discovery.
- **N2 (P1).** `datacube/demo/make-dist.mjs:29-38` hand-lists `bundle.js`, `planner-worker.js`, `trades.pure`, `chunks-bundle` and six vendor files. It only inlines `../` stylesheet links (:43). The page still links `fonts.css` (`index.html:11`), which loads `vendor/fonts/*` (`fonts.css:10`). Neither is copied, nor are `config.json` or `projects/`.
  - Consequence: `//datacube:app` (`datacube/BUILD.bazel:722`, `site=":dist"`) serves a page with a missing stylesheet and missing fonts.
  - `README-realdata.md:124` claims `dist/` is self-contained, and the only consumer test is the manual verify_app.
  - Fix: derive `dist` from `:site` with copy_to_directory (keep a small action only for CSS inlining). Add a js_test that every URL referenced by `dist/index.html` and its CSS exists in `dist`, plus a browser test over `dist`.
- **N3 (P1).** There are two independently locked Playwright copies:
  - `datacube/package.json:27` says `^1.63.0` (lock `datacube/pnpm-lock.yaml:587` resolves 1.63.0); `query/package.json` pins `1.63.0` (`query/pnpm-lock.yaml:584`).
  - Only datacube's copy installs browsers; `//query:verify` launches with query's copy (`query/BUILD.bazel:126`).
  - A lock bump on either side silently mismatches the browser revision.
  - `site/verify.mjs:15` reaches into another package's `node_modules` by path.
  - Fix: one pinned Playwright (exact version, no caret) shared by all three, and the browser pinned in Bazel keyed to it.
- **N4 (P1).** Fixed ports beyond the K13 list, all on every interface: `verify-remote.mjs:27/148` **:8741** (CI-run), `shots.mjs:25/52` :8732, `chaos.mjs:38/87` :8734. `listen(PORT)` without a host binds every interface. Fix: `listen(0,'127.0.0.1')`.
- **N5 (P1).** `query/demo/verify.mjs:25-26` picks ports with `Math.random` and never binds-checks them. The server already supports an ephemeral port and prints it (`core/src/main/java/com/legend/server/LegendHttpServer.java:347`), and the warehouse already uses `--port 0` (:48).
  - verify.mjs also computes the runfiles root by path arithmetic: `RUNFILES = resolve(ROOT,'..','..')` (:23), `SERVER = resolve(ROOT,'..','core','server')` (:24).
  - `verify-app.mjs:28-29/37-39` does the same and forces `RUNFILES_DIR`.
  - Fix: port 0 plus stdout parsing; `@bazel/runfiles` (the official JS runfiles library) with `$(rlocationpath)`.
- **N6 (P1).** Two CI-run harnesses write into the runfiles/output tree. `verify-remote.mjs:78-83` and `verify-real-data.mjs:69-72` run duckdb-wasm's NODE_RUNTIME `COPY t TO '<relative name>'`, which writes to the real filesystem in cwd (the runfiles directory under `bazel run` and `bazel test`), then `rm`s it. The comment at `verify-remote.mjs:71-77` documents earlier stale-file breakage from this. Fix: absolute paths under TEST_TMPDIR, or produce the Parquet fixture in a build action passed as `data`.
- **N7 (P2, but live on CI).** `run-stress.mjs:157` calls `offered.has(...)`, but the variable is `offeredNames` (:38). Any non-ok ingest throws a ReferenceError, which exits 1 for the wrong reason, and the "an offered sample must ingest" invariant never actually evaluates. Fix the name (use a Set) and convert to a js_test.
- **N8 (P1).** Paths that silently pass:
  - verify-features `ONLY=` can run zero checks and exit 0 (:227, :5179).
  - verify-engine-differential treats local-plane failures as "skipped", not failed (:299-302, :358).
  - verify-calc-vocabulary checks "locally only" when nothing answers on :6300 (:82-85), so the verdict depends on host state.
  - Fix: no behaviour-changing env knobs in test targets; zero checks means failure; engine presence becomes a separate target.
- **N9 (P2).** verify-features is one sequential, order-dependent, single-page run of 118 checks (:576-578; the last check mutates the page, :5060). It cannot be sharded and one failure cascades. `freshCube()` already exists (:599), so it can be split into sharded js_tests by section.
- **N10 (P2).** Wall-clock assertions and sleeps: `torture.mjs:385` (`<1000 ms`); verify-features :76 (30 s deadline) with 74 `waitForTimeout` calls; chaos :442/:478; verify-page :65 (300 ms quiet window); verify-charts :63 (400 ms window). These are flake sources on loaded CI.
- **N11 (P2).** Temp handling:
  - `verify-features.mjs:54` uses a fixed filename (collides between concurrent runs and is never removed).
  - verify-cubes:41 (400k-row CSV), verify-smoke:75, verify-upload:43 and verify-real-data:73 are never cleaned.
  - Fix: TEST_TMPDIR with cleanup.
- **N12 (P2).** SHOTS is inconsistent. `verify-charts.mjs:137/164` and `verify-page.mjs:145` write `${SHOTS}/…` relative to cwd (runfiles); `verify-features.mjs:275` writes relative to BUILD_WORKING_DIRECTORY. Fix: TEST_UNDECLARED_OUTPUTS_DIR.
- **N13 (P2).** Outputs written to BUILD_WORKING_DIRECTORY are only ignored under `datacube/` (`datacube/.gitignore:9-11`). CI runs from the root, so `stress-results.json` lands untracked at the repo root. `.scratch/` (query verify) is ignored only by the author's local git exclude file, not by the repo `.gitignore`.
- **N14 (P2).** Data is too broad: all 19 generated harness js_binaries get `playwright`, `:site` and `:src` (`datacube/BUILD.bazel:802-816`), including the Node-only `torture`, `verify_engine`, `verify_engine_differential`, `verify_calc_vocabulary` and `make_sample`. That forces the WASM planner and bundle builds for a CSV writer. Give each target its own data.
- **N15 (P2).** `query/test/strict-reporter.mjs` is byte-identical to datacube's. `//datacube:strict_reporter` exists (`datacube/BUILD.bazel:18-25`), but its visibility excludes `//query`.
- **N16 (P2).** Non-Bazel recipes and parallel paths in this slice:
  - `README-realdata.md:9` `cd datacube && npm install` (and the lock is pnpm), :121 `npx serve`, :141 the host `duckdb -c` used to produce EXPECT.
  - `datacube/package.json` `scripts.test` / `typecheck` and `datacube/.gitignore:1-3` (a "pnpm editor loop").
- **N17 (P2).** `verify-picker.mjs:34` probes `http://localhost:8000` regardless of `URL`. It is the only harness not serving the site itself, and that is the sole stated reason it is excluded from CI (`datacube/BUILD.bazel:794`).
- **N18 (P2).** `datacube/BUILD.bazel:747-750` states as policy that "a browser binary is not something Bazel fetches here", which directly contradicts the goal. Harnesses are `js_binary`, so they get no `size`/`timeout`, no caching, and no test results.

**Pinned-browser plan:**
- **Version.** Playwright is 1.63.0 in both locks. The installed revision observed in the auditor's local Playwright browser cache is `chromium-1243` / `chromium_headless_shell-1243` (plus `ffmpeg-1011`). playwright-core@1.63.0's `browsers.json` is not in the Bazel output base, so confirm 1243 against it. Headless launch uses `chromium_headless_shell`.
- **Fix:**
  1. Per-platform `http_archive`s with sha256 for chromium-headless-shell build 1243 (Playwright CDN `builds/chromium/1243/chromium-headless-shell-{linux,mac-arm64,mac,win64}.zip`), or adopt rules_playwright, which derives the archives from the locked playwright-core.
  2. Lay them out as a `PLAYWRIGHT_BROWSERS_PATH` tree with copy_to_directory, or pass `executablePath` from `$(rlocationpath)`.
  3. Set the env on each js_test.
  4. Add a diff test that `browsers.json`'s chromium revision equals the pinned one.
  5. Delete `install_browser` and its CI step.
  6. Provide the Linux system libraries through a pinned CI image rather than `--with-deps` apt.

## Section 3: Known items re-confirmed

- **K2.** `fixtures/saved-queries/make.mjs:4-6` gives the recipe. :10 uses fixed port 18777. :11 and :39 use relative paths from cwd. :55 writes into the source tree. There is no BUILD target (`fixtures/saved-queries/BUILD.bazel` holds only `records`). Deepened: the records carry wall-clock `createdAt`/`lastUpdatedAt`/`lastOpenAt` (e.g. `graph-fetch.json`), so output is non-deterministic and can't be diff-tested without normalisation. make.mjs prints the row shapes but doesn't assert the README's counts (6/5/6/4; `README.md:26-29`).
- **K3.** `query/tools/icons.mjs:4-5` gives the `curl … | tar` and `node … > query/src/ui/icons.ts` recipe. There is no target, http_archive or diff test. `icons.ts:1` says GENERATED.
- **K9.** `make-dist.mjs` itself is a proper js_run_binary action (`datacube/BUILD.bazel:731-745`). Deepened by N2: it produces an incomplete, untested `dist`.
- **K11.** `.github/workflows/gates-run.yml:155-169`: `bazel query` plus a serial `bazel run` loop piped to `tee`, with only `set -u` (:158), so failure detection depends on GitHub's implicit `bash -eo pipefail`. It misses `//site:*` (N1). The browser is installed at :129-131.
- **K13.** Re-confirmed:
  - Harnesses are js_binary (`datacube/BUILD.bazel:802-829`). Chromium comes from `install-browser.mjs:13-14` (`cli.js install [--with-deps] chromium`).
  - External ports: :8080 for chaos/torture (`chaos.mjs:40` is the engine it probes; it binds 8734 itself), :6300 (verify-engine:24, -differential:58, calc-vocabulary:33), :8000 (verify-picker:13), :5432 (`verify-app.mjs:6` is Postgres), :8772 (`warehouse-source.mjs:6`).
  - About 33 env knobs across harnesses.
  - run-stress writes at :98 and shots at :24.
  - verify-features is 5,179 lines.
  - `query/demo/verify.mjs` writes `.scratch` (:29-31) and spawns `//core:server` (:34) and the native warehouse (:47).
  - **Correction:** make-sample writes to `$BUILD_WORKING_DIRECTORY/sample-trades.csv` by default (:28-29). `/tmp` appears only in the usage example (:8, `README-realdata.md:32`).

## Section 4: Coverage

**Read in full:**
- `datacube/demo`: 45 of 46 files, including all 5,179 lines of `verify-features.mjs`, every `verify-*.mjs`, chaos, torture, shots, run-stress, measure-startup, make-sample, make-dist, install-browser, serve, static-files, engine-cases, grid-invariants, stress-corpus, stress.ts, typed-view, warehouse-source, main.ts, page.ts, page-config, planners, remote-harness, index/page/remote/stress html, fonts.css, config.json, README-realdata, torture.pure and trades.pure.
- `datacube/BUILD.bazel` lines 1-140 and 560-837.
- `query/BUILD.bazel`, package.json, README, tsconfig, every demo file (verify.mjs, serve.mjs, main.ts, the configs, index.html, fonts.css), `test/lite.ts`, `saved-queries.test.ts`, `strict-reporter.mjs`, `tools/icons.mjs`.
- All of `site/` (BUILD, index, serve, verify).
- All of `fixtures/` (BUILD, make.mjs, README, a sample record).

**Skimmed or grepped, not read line by line:**
- `datacube/demo/boot.ts` (1,972 lines; grepped for fetch/URL/env/ports, read 296-420) and `trades-h2.pure` (header only).
- `query/src` (grepped for fetch, import.meta, hosts, GENERATED, vendor) and `query/test/build.test.ts` / `load.test.ts` (grepped).
- `query/docs/UPSTREAM_QUERY_CENSUS.md` (grepped for recipes, none found), and the pnpm locks (Playwright entries only).

**Also consulted:** `.github/workflows/gates-run.yml` (browser lane), `warehouse/defs.bzl` (the `warehouse_run` launcher verify_app spawns), `datacube/package.json`, the `.gitignore` files, `LegendHttpServer.java:347`, and the local Playwright browser cache.

**Not verified:** playwright-core@1.63.0 `browsers.json` (so revision 1243 is inferred from the local cache), and rules_js's exact cwd under `bazel run`.
