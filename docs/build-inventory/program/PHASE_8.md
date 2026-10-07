# Phase 8 brief: the rest of the build (for a fresh session)

**Applied on 2026-10-07, after this brief was written:** the plan's Phase 8 section now says two of the Windows PR's
four follow-ups are done and two remain, points at this brief's §2.8 for the 24 carried shortcuts, names the caching
decision as open (no "D14" exists), lists design D1, D4, D6 and D7, and points at this brief as the phase's plan in
detail. The memory note on PR #14 is corrected. Statement #28 in §5 is out of date: the program folder now holds one
brief per remaining phase. Everything else here is unchanged and its open decisions are still open.

**After the cold read (`COLD_READ_2026_10_07.md`):** Compile-3's "no product user exists" misses the probe's test and
tool users: `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:223` calls
`com.legend.probe.Shadow.CONTEXT.set(fqn)`; `core/src/test/resources/META-INF/services/com.legend.builtin.DecisionProbe`
names `com.legend.probe.Shadow`; `tools/untangle/probe_counts.py` and `bare_tiers.py` read its rows; and the parked
compiler plan's §0 relies on those probe rows (`LL_SHADOW=1`). Deleting `//core:probe` takes that probe away: a
question for the user before design D8 is applied. OD-9's list of PR texts also covers `gates/BUILD.bazel`'s header
("goes through a PR instead") and the root `progress.txt` (stale since April). The landing rules in START_HERE §4 now
say that `[skip ci]` stops main's push run and that the next nightly run is the check after a landing (CI-10: confirm
it fires).

**Agreed on 2026-10-07 (the planning session): the CI work is the program's first landing, L1** (the plan's §4), not
interleaved later. §8 below is its agreed shape: the lane set, what moves, what stays for later landings. The
measurements that decided it are `evidence/phase8/CI_LANES_2026_10_07.md`: the build lane was the slowest lane on
every platform (42 minutes on Windows) because it built `//...`; the browser lane's 23 minutes were a serial shell
loop of harnesses plus a cold fetch; GitHub's 10 GB cache cap had evicted every cache but macOS's (§0 item 4's "5
caches, 17.5 GB" was already 3 and 9.9 GB by the afternoon); GitHub runs at most five macOS jobs at once, so on macOS
the queue is the wall clock. CI-10 is settled: the nightly run fires (a `schedule` run started 13:38 UTC for the 06:23
cron; GitHub runs schedules late). Compile-1, Compile-2, Gen-5, Check-4, Test-6, Test-7, Test-8, CI-1, CI-2 (as the
downloads-only cache), CI-4, CI-5, Meas-1 (a), Exec-1 (the four harness commits) and the harness half of Node-4 are
L1's; §7 item 2's CI bullet is superseded by §8.


Written 2026-10-07 by a read-only research pass. Nothing in any repository was changed and no Bazel command was run.
Every claim below carries a path (with line or symbol), a commit, or a command; anything I could not check is marked
**UNVERIFIED**. Open designs are listed as decisions for the user, with options and evidence; none is decided here.

**Which tree a citation means.** "main" is `origin/main` at `f306bd698`. The worktree `runs/build-rebuild` (branch
`build/phase3`) has exactly main's build files (`git diff origin/main -- '*BUILD.bazel' '*.bzl' .bazelrc MODULE.bazel
.github warehouse/src datacube/demo` is empty), so its line numbers are main's. "plan" is the working copy of
`docs/REBUILD_PROGRAM_2026_10_06.md` in `runs/bazel-plan` (branch `docs/bazel-first-class-plan`); it is being edited
today, so plan citations name the section, not a line. "exec" is the parked branch `bazel/exec` (`6f1d9aa9a`,
worktree `runs/bazel-exec`) plus its uncommitted changes. The old Bazel plan's documents (`BAZEL_FIRST_CLASS_WORKPLAN`,
`BAZEL_EXECUTION_LOG`, `BAZEL_EXECUTION_RUNBOOK`, `GENERATORS.md`) exist only on the plan branch, not on main.

**Two decision series, never mixed.** "old D*n*" is the old workplan's series
(`docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` §2, D1 to D21). "design D*n*" is
`docs/BUILD_REBUILD_DESIGN_2026_10_05.md` §6 (D1 to D15). Old P-numbers (P0-01 … P8-01) are the old workplan's items.

---

## 0. In plain words

**What Phase 8 is.** The program has two halves. Phases 3 to 7 change the compiler and make the upstream bump
self-contained. Phase 8 is everything else the build still needs: "build" must mean compile only; every generator,
test and check must run only when something it really reads changes; Node must leave the build and the tests; CI must
be a list of Bazel suites with a cache that works; the old Bazel plan's parked work must land or be retired; the
shortcuts the build still carries must be fixed properly; and, last, the product folders get their final names.

**The ten things a new session must know first.**
1. **Phase 8 has no design document of its own.** Its plan entry is nine bullets (plan §3, Phase 8). The detail lives in
   the design doc (§4 and §5), the generator dossiers (`docs/build-inventory/generators/G1`–`G6`), the area reports
   (`docs/build-inventory/area1`–`8_report.md`, Part C of each) and the old workplan's unfinished items (Phases 3 to 8).
   This brief pulls them into one list (section 2).
2. **Two of the four "PR #14 follow-ups" in the plan are already done on main**: the gzip `run_shell` went in
   `4f7448989` (2026-10-04) and `LauncherTest` stopped pinning the JDK's text in `067973962` (2026-10-05). Two remain:
   the hard-coded Windows bash path (`.bazelrc:41-43`) and `warehouse_run`'s missing `visibility`/`tags`/
   `target_compatible_with` (`warehouse/defs.bzl:103`).
3. **The plan's "caching (D14)" points at nothing.** No D14 exists in the design doc in any of its 14 revisions
   (`git log -- docs/BUILD_REBUILD_DESIGN_2026_10_05.md`, each grepped). Old D14 is about saved-query fixtures. The
   caching design is an open decision (section 3.4, OD-5).
4. **CI's cache barely works today.** `gh cache list` (2026-10-07): 5 entries, 17.5 GB, all macOS, all from
   2026-10-06; no Linux or Windows lane has a cache. The key is per lane and changes only when a pin changes
   (`.github/workflows/gates-run.yml:121-124`), so lanes start cold. A full run takes 40 to 57 minutes
   (`gh run list --workflow gate.yml`, runs 37518604572 … 37544420432).
5. **CI's build lane runs the corpus.** It builds `//...` (`gates-run.yml:63,165-166`), and the four non-manual corpus
   passes are build actions with no `manual` tag (`spec/corpus.bzl:72-114`), so each full CI run computes each pass up
   to twice per platform (`docs/build-inventory/generators/G2_dossiers.md` §A5). It also builds the manual 2 GB
   `//pct:ratchets`, because `//:update_generated` is not manual and lists it (`BUILD.bazel:107-123`, line 120).
6. **Design D9 collides with old P3-32.** D9 (decided) hands the warehouse its files by `$(rootpath)`, which needs a
   runfiles *tree* on Windows (the design says so: "`.bazelrc` enables runfiles trees there"); old P3-32's goal is
   Windows *without* runfiles trees, and the short `C:/bzl` output root exists because paths inside a test's runfiles
   tree grew past Windows' limit (`.bazelrc:22-26,33`; old P3-32's note: drop it once Windows runs without trees).
   One of them has to give (OD-1).
7. **The parked `bazel/exec` work is mostly Node harness work that the no-Node goal will redo.** Of its 14 commits,
   the Node-independent parts are P4-14 (the JDBC service file), the servers' `--exit-with-parent` flag (P4-18's Java
   half) and the completeness-test idea of P4-11; the rest converts Playwright harnesses that the CDP client replaces.
   One commit adds a 7,824-line junk file that a later commit deletes (`055e79636`, `6f1d9aa9a`). Whether to land it
   as an interim is a decision (OD-3).
8. **The build inventory was taken on `bazel/exec`, not main** (`docs/build-inventory/BRIEF.md`, "Where": "the
   bazel/exec checkout"). Targets it counts, like `//datacube:browser`, `verify_*_test` and `//scripts/corpus:
   engine_stress`, do not exist on main (`git grep` on main finds `browser-ci` tags and `install_browser` instead,
   `datacube/BUILD.bazel:865,950`). Re-take counts on main before using them as baselines.
9. **The CDP transport the design names may not fit Java.** The design says "a small Java CDP client over
   `--remote-debugging-pipe`" (§4.6). Chromium's pipe mode uses file descriptors 3 and 4, and Java's `ProcessBuilder`
   can only connect stdin, stdout and stderr; the JDK does ship a WebSocket client (`java.net.http.WebSocket`) that
   the port transport (`--remote-debugging-port=0`) would use. **UNVERIFIED**: needs a spike before any port work
   (homework U-1).
10. **Several "decided" items from the old plan conflict with rules made since**: no PRs (2026-10-06) vs old P8-01
    "restore D17's guardrail 2" and AGENTS.md's "fix it in a PR"; the throwaway-CI rule (dispatch `-f gates=…`) vs
    old P5-03's "delete the `gates` dispatch input"; design D7 "delete the 16 probe scripts" vs old D18 "keep them as
    `bazel run` tools" (decided twice). Section 5 lists every one.

**Reading order.** This brief; the plan (§0 to §5; §0.1, §0.3 and §0.4 are this phase's goals); `docs/build-inventory/program/START_HERE.md` (process, exact
commands, traps); the design doc §3 to §7; the dossiers named per item below; then the old workplan only for the items
section 4 maps.

**Rules that bind every Phase 8 change** (the user's; START_HERE §4 has the commands):
- Plan the change with the user and get agreement before code; explain plainly; never invent a new mechanism when an
  existing one does the job (here: `java_run`, `write_source_files`, `test_suite`, the esbuild rule pattern, the
  runfiles libraries, bazel_lib's built-ins).
- No hacks. A shortcut that must stay becomes a row in `docs/PARKED_WORK_LEDGER.md` with an anchor test.
- Bazel's built-ins and bazel_lib first; prove a choice on Windows CI before relying on it (memory note
  "bazel-native-first": the `.gz` mtime bug of Bazel 9.2 broke Windows, run 37224856941).
- No hard-coded tool paths or host programs in tests or Bazel config, not even behind a Windows `select`, not via PATH.
- An audit agent reviews every Bazel change before a push. No PRs: audit, fix, local gate once, `[skip ci]` tip, push
  the branch, one full CI run on it (`gh workflow run gate.yml --ref <branch> -f gates= -f platforms=all`), then push
  that commit to main. During the work, throwaway CI only on affected lanes; a full run when `MODULE.bazel`, a
  toolchain, `.bazelrc` or a workflow changes.
- Core edits (anything under `core/`) are announced in `docs/IN_FLIGHT.md` on main first, listing every file.
- Files owned by another line (IN_FLIGHT main, lines 80-83: Studio owns `studio/`, `legend-art/`, `query/`,
  `datacube/` imports and labels, `site/`; the DataCube line touches `warehouse/`): note it once in IN_FLIGHT, then
  proceed (memory note "focus-own-program").

---

## 1. Phase 8 at a glance

| Plan bullet (plan §3, Phase 8) | State on main | Where in this brief |
|---|---|---|
| Tests and checks by their true trigger; narrow dependencies | Not started, except Phase 1's generator narrowing | 2.3, 2.4, Compile-9 |
| D8, D9, D10, D5, D13, D15, the TS typecheck cleanup | None done; `//:sites` exists (Phase 0) | 2.1 |
| Node out of the tests (CDP client) | `//:web` already runs without Node (Phase 0); tests all Node | 2.5 |
| CI lanes from `//gates`, caching ("D14") | Not started; "D14" does not exist | 2.6 |
| `bazel/exec`'s Phase 4 commits and fixes, P3-25 | Parked, unrebased (base `3faa7d291`) | 2.7 |
| The carried shortcuts (PR #14's four, Phase 1's six) | 2 of the 4 done; 6 open; at least 16 more not listed (Short-25 and Short-26 added 2026-10-07) | 2.8 |
| The measurement group | Untouched since set aside (2026-10-05) | 2.9 |
| The renames (D11, D12) | Last; not started | 2.10 |
| (implied) the old plan's Phase 6 guards, Phase 7 cleanup, P8-01 | Not started | 2.11 |

---

## 2. All remaining build work, by theme

Each item: its source, its status with evidence, what to do, how to check it, and what it depends on. IDs here are
only labels for this brief.

### 2.1 Compile targets and the compile-only guard

Already done and on main (Phase 0, PR #25, merge `ab4723ba2`): the five targets `//:java`, `//:web`, `//:wasm`,
`//:native`, `//:sites` (`BUILD.bazel:31-75`); the guard `//tools/guards:compile_only_test` (`tools/guards/
compile_only.bzl`, `CompileOnlyTest.java`), in `//gates:local` and the checks lane; product jars on `http_jar`
(clean `//:java` 12.4 s, design §5b); `//:web` through esbuild's own binary (`tools/js/esbuild.bzl`, design §5c);
stamping off (`.bazelrc:86`).

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Compile-1 | CI's build lane builds the compile targets, not `//...` | design §4.1, §5 step 2 | Not started: `gates-run.yml:63` (`"targets":"//..."`), `:165-166` | Build lane = `bazel build //:java //:web //:wasm //:native //:sites`, keep `--nobuild --config=bazel10 //...` and the A25 check (`gates-run.yml:167-179`) | CI log of the build lane shows only compile actions; `compile_only_test` green | First make sure every generator and measurement now exercised only by `//...` has a lane (2.2, 2.9), or decide it may go unbuilt. Conflicts with old P5-03 (section 5, #16). Workflow change: full CI |
| Compile-2 | `//:update_generated` tagged `manual` | design §4.2 "Mechanics", §5 step 2; GENERATORS §6 step 6; plan Phase 7 | Not started: `BUILD.bazel:107-123` (testonly, no `manual`; lists the manual `//pct:update_ratchets` at :120) | One tag; the plan puts it in Phase 7. Pulling it forward stops `bazel build //...` building `//pct:ratchets` and its 4 GB PAR (G6 §8 P1) | `bazel query 'attr(tags, manual, //:update_generated)'`; `bazel query 'somepath(//..., //pct:ratchets)'` minus manual targets is empty | Decision OD-10 |
| Compile-3 | design D8: delete `//core:ide` and `//core:probe` | design D8 (decided 2026-10-05, `c7fa8366c`) | Not started: `core/BUILD.bazel:166-170` (probe), `:208` (ide), both in `:core`'s exports `:223-224`, so `//core:server` (`:288-295`) ships them in `//:java` | Announce in IN_FLIGHT (core edit). Delete both packages, their tests (`core/src/test/java/com/legend/ide/ModelIndexerTest.java`, `ModelOrchestratorTest.java`), `"ide"` in `_CORE_TEST_PACKAGES` (`core/BUILD.bazel:408`), their rows in `ArchitectureTest.java:83,88,326,510,524,818,981-982` (a guard file: dated justification) and `tools/deps/core-layers.txt`; fix the javadoc mentions (`ElementParser.java:547,2804`). No product user exists (`git grep "com.legend.probe\|import com.legend.ide"` over `core/src/main` outside the two packages is empty) | `bazel build //:java` (two jars fewer); `//core:guardrails`, `//tools/deps:all`, `//core:core_tests` | Independent of the compiler phases (touches `core/BUILD.bazel` lines Phase 5 does not) |
| Compile-4 | design D9: the warehouse server knows nothing about Bazel | design D9 (decided 2026-10-05) | Not started: `warehouse/src/main/java/com/legend/warehouse/server/ServerRunfiles.java` exists; `WarehouseServer.java:912-960` (runfiles fallback) and `:820-830` (reads `BUILD_WORKING_DIRECTORY`); `@rules_java//java/runfiles` in `server_lib` (`warehouse/BUILD.bazel:63`); "DO NOT RENAME … ServerRunfiles" couplings (`warehouse/BUILD.bazel:278,385`); `//:java` carries "the 2 runfiles jars (gone with D9)" (design §5a) | Agree the design first (OD-1, OD-2). Then: every launcher and test passes `--duckdb-library`, `--duckdb-extensions`, `--site`; the server drops `ServerRunfiles` and the runfiles dependency; `warehouse_run` is reshaped, taking `visibility`, `tags`, `target_compatible_with` (Short-3); `LauncherTest` follows; decide `hermetic_launcher`'s fate (`MODULE.bazel:308`). Supersedes old P1-16's product half and old P4-12's design (section 4) | `bazel test //warehouse:tests //warehouse:tests_native //warehouse:launcher_test //datacube:verify_app_test`; `bazel cquery 'somepath(//warehouse:server_native, @rules_java//java/runfiles)'` empty; `git grep ServerRunfiles` empty; Windows CI's `native` and `app` lanes | OD-1, OD-2; Windows proof; announce to the DataCube line (it edits `warehouse/`) |
| Compile-5 | design D10 (one shape per app) and design D5 (DataCube's fixtures out of the site) | design D10, D5 (decided 2026-10-05, `c7fa8366c`, `9c9dd76cb`) | Partly. Done: `//:sites` (`BUILD.bazel:72-75`), each app's `:bundles` (`datacube/BUILD.bazel:679-686`). Not done: `//datacube:site` is a filegroup over all five bundles, `remote_bundle` and `stress` included (`datacube/BUILD.bazel:643-649,735-746`), plus `demo/*.pure` (`torture.pure`); the second packaging path `make_dist`/`dist` through `demo/make-dist.mjs` remains (`:779-793`) and `//datacube:app` serves it (`:766-777`); `//datacube:dist` is broken as a deployable (absolute symlinks, no fonts: G5 cross-cutting #4) | One shared rule for every `//<app>:site` folder, built from an existing rule (bazel_lib `copy_to_directory` is what `//site:dist` already uses); `//site:dist` composes them; delete `make-dist.mjs`; `//datacube:app` serves the site folder; the fixture bundles and `torture.pure` become test-only; carry over a completeness test (the idea of `dab833263` on exec, retargeted) | `bazel build //:sites`; completeness test: every URL in each page and stylesheet exists; `//datacube:verify_app_test`; `compile_only_test` | Studio owns `site/`, `query/`, `datacube/` (IN_FLIGHT main 80-83): note it. Replaces old P4-11 |
| Compile-6 | design D15: shipped bundles leak build paths | design §5c (the 225 comments in `datacube/demo/bundle.js`; a hermeticity hole, audit S8), D15 (open) | Not started: no setting in `tools/js/esbuild.bzl` addresses it | User picks among the design's options (minified whitespace, unsandboxed esbuild, another esbuild setting); implement in `tools/js/esbuild.bzl` | No `execroot` text in any bundle; bytes equal sandboxed vs unsandboxed and across platforms | OD (design D15) |
| Compile-7 | TypeScript type check as a build action of `//:web`, by TypeScript's own platform binary; close the coverage gaps | design §4.1 (`//:web` row), §4.6; "TypeScript type checking stays a test for now (decided)", cleanup recorded (design §6); area6 B2, B3, C2 | Not started: three `typescript.tsc_test` through Node's npm launcher (`datacube/BUILD.bazel:18,245`, `query/BUILD.bazel:195`, `studio/BUILD.bazel:167`); unchecked: `sdlc-client` and `depot-client` tests, the harness `.mjs`; `engine-client`, `pure-protocol`, `query-store` sources checked twice under different `lib` settings (area6 B3) | Reuse Phase 0's esbuild pattern (platform tarballs pinned by the registry's sha512, run directly: `MODULE.bazel:329-344`): the locks already resolve `@typescript/typescript-<os>-<arch>@7.0.2` (`datacube/pnpm-lock.yaml:109-175`). One check per library package and per app's own directories | A type error anywhere fails `bazel build //:web`; no `tsc_test` remains; `compile_only_test` allows the new action kind | OD-13 (the user said "for now a test"); easier after the tests stop using `node:*` (2.5) |
| Compile-8 | design D1: one Error Prone policy for all first-party code | design R9, §4.5, D1 (open); area8 C4 | Open: null-gated libraries switch every default check off (`tools/nullaway/defs.bzl:12` `-XepDisableAllChecks`) and add back only NullAway and two locale checks (`tools/java/defs.bzl:17-20,37`); `nullaway = False` libraries (tests, tools) run the full default set | User picks (a) the full defaults everywhere or (b) one explicit shared list; measure first (U-8) | `bazel aquery` javacopts per library; build green | Pairs with Check-7 (NullAway everywhere) |
| Compile-9 | Narrow the remaining "all of core" dependencies | design R1, R2, §5 step 3; area1 C6, area3 C1, area7 C4, area8 C1 | Partly. Done in Phase 1: `gen_dynafn`, `gen_imports`, `lite_facts`, the catalog and offer facts, `vocab`, `gen_fixtures`, `gen_manifest`, the reachability metadata, `zone_jvm` (`runs/homework/phase1/evidence/phase1/AUDIT_PHASE1.md` answer 1). Not done: `//sdlc-server:rules` on `//core` (`sdlc-server/BUILD.bazel:14-24`; it feeds the 2 GB Studio page TeaVM compile); the model projects' `compiles_test_lib` on `//core` (`tools/legend/BUILD.bazel:6-16`); `pe_tests_lib`; the corpus passes' `spec_tests_lib` (2.9); `//core:server` = the `:core` umbrella, so the product ships `:test` and `:testdatagen` (`core/BUILD.bazel:223-224`; area1 C6, OPEN) | Per consumer, depend on the smallest slice (`//core:planner`, `//core:plan_side`, parser, …), proved by a strict-deps compile | Before and after: design §5a's B2 query (`rdeps(<all packages>, //core:src/main/java/com/legend/Compiler.java)`, 495 targets at `1479486dd`) and the count of targets depending on `//core:exec` (design §5 step 3 exit) | OD-15 for `:test`/`:testdatagen` |

### 2.2 Generators outside the bump

"The bump's generators" are the upstream records (GENERATORS §2), which Phases 1, 2, 4, 5 and 7 own. Everything else
is here, from GENERATORS §4 ("ours"), §5 ("the rest") and design §4.2 groups C, D, E, F. Group E (engine behaviour
that should be tests) is the measurement group, section 2.9.

**State of each "ours" generator (GENERATORS §4).** Phase 1 narrowed all of them; what is left belongs elsewhere.

| Generator | Phase 1 result (evidence) | What is left, and where |
|---|---|---|
| `//scripts/corpus:gen_dense`, `gen_stress` | exact module closures, 14 and 27 (evidence/phase1/AUDIT_PHASE1.md answer 1) | none for the generators; the gates' 29-module library is Gen-7 |
| `//core:stress_layout` | right (analysis-time write) | none |
| `//datacube:catalog_rules` | 3 libraries (option B, the user, plan Phase 1 status) | none |
| `//datacube:offer_facts` | exact core deps | its tool reads its own output (Gen-3); its query emitter runs on Node (Gen-2) |
| `//datacube:test_imports` | no npm inputs to narrow (plan Phase 1 status) | runs on Node (Gen-2) |
| `//engine-client:lite_facts` | `//core:compiler_element_type` closure | none |
| `//legend-art:icons_gen` | unchanged (its pin is `@react_icons`) | runs on Node (Gen-2) |
| `//warehouse:reachability_metadata` | own library `:reachability_metadata_lib` (`warehouse/BUILD.bazel:406` and `:414-422`; excluded from the test library at `:123-124`) | none |

Also owned by Phase 7, not here (so nobody does them twice): `//:update_generated` becoming "ours only and manual" and
the guard that every writer and diff test belongs to a suite and a gate (GENERATORS §6 steps 6 and 7; the plan's
GENERATORS update note maps steps 5 to 7 to Phases 1 and 7). Phase 5 retires `native_membership_draft` and
`//core:draft_native_membership` with the membership list (plan Phase 5), and `gen_claims`/`core_next` (design D2).

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Gen-1 | Build outputs `testonly`; dead generators deleted | GENERATORS §5, §6 steps 1-2 | Done in Phase 1: `cube_queries`, `cube_jvm_answers`, `jvm_answers`, `zone_jvm`, `stress_index`, `gen_differential`, `ref_dump` testonly (each `testonly = True` in its BUILD); `migration_sizing`, `eager_corpus_compile_world2` deleted (no BUILD names them). `offer_queries` stays non-testonly on purpose (plan Phase 1 status) | nothing | — | — |
| Gen-2 | The JavaScript generators leave Node | design §4.6 "Servers and tools"; area5 Part D ("write generator outputs"); G5 cross-cutting #2 (they declare all of `:src` plus 122 npm files but import 33 to 48 files) | Not started: `//legend-art:icons_gen` (`legend-art/tools/icons.mjs`), `//datacube:test_imports` (`tools/test-imports.mts`), `//datacube:offer_queries` via `emit_offer_queries` (`tools/offer-facts/emit.ts`, `datacube/BUILD.bazel:278-286`), `//datacube:cube_queries` (`test/wasm-differential/emit.ts`), `//datacube:link_dictionary_next` (`make.ts`), `//fixtures/saved-queries:gen` (`make.mjs`, `fixtures/saved-queries/BUILD.bazel:33-62`), `//datacube:make_sample`, and `make-dist.mjs` (goes with Compile-5) | Per generator, one of the design's two existing routes: a Java `java_run`, or run the product code in the pinned Chromium through the CDP driver. `test_imports` may become unnecessary once each test is an esbuild bundle (esbuild's metafile lists exact inputs): **UNVERIFIED** | `bazel query 'kind("js_binary\|js_run_binary", //...)'` empty at the end; each diff test byte-identical | The CDP driver (2.5) for the in-page route; Compile-5 for `make-dist.mjs` |
| Gen-3 | The offer-facts tool reads its own committed output | Phase 1 deferral (evidence/phase1/AUDIT_PHASE1.md N9); plan Phase 1 status | Open: `emit_offer_queries` takes all of `:src` (`datacube/BUILD.bazel:280`), which includes the committed `src/generated/offer-facts.ts` | Give the tool exactly its import closure (G5 #2); folds into Gen-2 | `bazel aquery` of `//datacube:offer_queries` has no `offer-facts.ts` input | Gen-2 |
| Gen-4 | The link-dictionary tool lost its runtime check | Phase 1 deferral (evidence/phase1/AUDIT_PHASE1.md N12) | Open: `//datacube:link_dictionary_next` is manual, so nothing runs `makeDictionary` (`datacube/BUILD.bazel:306-322`) | A small test that runs it; on Node today, so do it with Gen-2 | the test fails when `makeDictionary` throws | Gen-2 |
| Gen-5 | Drafts, reports and hand tools `manual` | design §4.2 F; area2 C6, area7 C7, area8 C4 | Partly. Done in Phase 1: the drafts (`spec:native_membership_draft`, `core:draft_native_membership`, `parser-equivalence:gen_own_corpus_draft`, `docs:draft_own_corpus_ledger`). Not done: `//tools/census:render_census` and `lanes_diff` (`tools/census/BUILD.bazel:10-23`, no tag), `//tools/junit:compare_testcases`, `//wasm:startup`, and P3-25's `//scripts/corpus:run` (exec, uncommitted, no tag) | Tag `manual` | `bazel query 'attr(tags, manual, …)'` lists them | Small; anytime |
| Gen-6 | The 45 unused per-project file groups | design §4.3 ("The 45 dead file groups are deleted"); area8 A1, C1 | Not started: `projects/BUILD.bazel:139-147` makes one per `PROJECT_DEPS` entry; only the 11 `LINKED_PROJECTS` are used | Make them for `LINKED_PROJECTS` only (load `//core:stress.bzl`) | `bazel query '//projects:*_files'` lists 11 | Small |
| Gen-7 | The stress-corpus Python library, split for the gates | design R5; area4 C5 | Partly: the generators run on exact closures (Phase 1), but the five ratchet gates keep "every corpus module" (`scripts/corpus/BUILD.bazel:14-17`; evidence/phase1/AUDIT_PHASE1.md N5) | Split `:corpus` by each gate's import closure | Editing one module reruns only the gates that import it | Tests-by-trigger work |
| Gen-8 | `//tools/gunzip` back to Bazel's own unpacking | old P1-17 amendment; area7 C8 | Watch item: kept because Bazel 9.2 sets a bad mtime on `.gz` fetch-time unpacking, which Windows refuses (`warehouse/BUILD.bazel:374-378`; run 37224856941) | When a Bazel release fixes it, switch and prove on Windows CI | `bazel build //warehouse:duckdb_extensions` on 3 platforms | Bazel upgrade |
| Gen-9 | Engine-tree subset filegroups | Phase 1 deferral (`evidence/phase1/PHASE1_CHANGES.md` item 9) | Open: `gen_fixtures`' three tier-2 modules, `gen_dynafn`'s relationalStore and the spec roots still take the whole engine tree; the fix is new filegroups in `third_party/legend_engine_src.BUILD`; it only shrinks sandbox inputs (the tree changes only in a bump) | Measure sandbox setup time first (U-12); do it only if it shows | Action input counts and setup time before/after | These are bump generators: coordinate with Phase 7 |

### 2.3 Tests by their true trigger

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Test-1 | Core test sources split into per-package libraries (old P3-06); the stress corpus to its own library | old P3-06 (workplan §4); design §4.3; area1 C5, area4 C1 | Held: "P3-06 … moves to the end with the script review" (log, USER 2026-10-05). Today one `core_tests_lib` (`core/BUILD.bazel:313-345`) carries the stress corpus and linked projects as resources (`:330-337`) | One testonly library per test package with minimal core deps, plus a shared helpers library; stress corpus to `stress_lib` used only by the stress lanes and the differential | old P3-06's proof: touch one file in `core/src/main/java/com/legend/lexer/` and only the dependent `core_tests_*` targets rerun | After program Phases 3b, 4, 5 (they add core tests) |
| Test-2 | `spec_tests_lib` split into corpus, parity, reference and ratchets | design §4.3; area2 C7 | Not started (one `spec_tests_lib`; G2 §A3 lists what the corpus passes pull from it) | Split by trigger; spec tests per package with exact data | A core edit outside the corpus path leaves the corpus passes cached | After program Phase 6 (it rewrites the corpus runner) and 5 |
| Test-3 | `pe_tests_lib` on its six core libraries | design §4.3; area3 C1 (also old compiler plan W1.9) | Not started | Depend on `//core:parser`, `:diagnostics`, `:lexer`, `:model`, `:protocol` instead of `//core` | An engine edit outside the front end leaves `//parser-equivalence` cached | Core visibility for those libraries |
| Test-4 | Channel B stops building the PCT adapter's PAR | design §4.3; area3 C5 | Not started: `pct_channel_b_*` (`pct/BUILD.bazel:228-243`) run on `pct_tests_lib`, which runtime-depends on `:adapter` → `:adapter_par_jar` → `:adapter_par` (`:57-100`). (Phase 1 made the PAR reproducible, so it no longer invalidates tests between runs) | A `channelb_lib` without the adapter and upstream jars | `bazel aquery` of a Channel B test has no PAR | Small |
| Test-5 | The 56 project compiles in one JVM, on `//core:planner` | design §4.3; area8 C1 | Not started: 56 `<p>_test` JVMs on `//core` (`tools/legend/BUILD.bazel:6-16`) | One test compiling each project's closure separately, one dynamic test per project; `//core:planner` deps (strict-deps compile proves it: area8 C1, OPEN) | 2 JVMs instead of 57 on a compiler edit | Compile-9 |
| Test-6 | Every CI lane is a `//gates` suite; `//gates:local` a suite of suites | design §4.3, §5 step 5; old P5-01 | Not started: `gates/BUILD.bazel` has only `local` (hand list, :12-80); the lanes are a jq list in `gates-run.yml:49-66` | Suites per lane; workflows name only `//gates:<lane>` | `bazel query 'tests(//gates:all)'` equals all non-manual tests (old P5-01 proof) | First in CI work (2.6); full CI |
| Test-7 | A guard: every test is in some lane, or on a reviewed list with its reason | design §4.3; old P6-03 (G3); area8 C3 | Not started. It would have caught `postgres_live` (area8 D2) | `tests(//...)` minus every lane suite equals a reviewed allowlist | the guard fails on a new test in no lane | Test-6 |
| Test-8 | design D4: `//warehouse:postgres_live` and `postgres_live_native`: a lane, or delete | design D4 (open); area7 C3 | Open: both are ordinary non-manual tests since old P3-22 (`c08f72edc`; `warehouse/BUILD.bazel:250-275`), in no CI lane (`gates-run.yml:49-66`) and not in `//gates:local` | User decides; area7 C3 suggests `:postgres_live` in app/local and `_native` in the `native` lane | they run somewhere, or are gone | OD (design D4) |
| Test-9 | The GraalVM image leaves `//gates:local` | design §4.3 | Not started: `//gates:local` runs `//datacube:tests` (`gates/BUILD.bazel:57`), which includes `live_snap_test` built on `//warehouse:server_native` (`datacube/BUILD.bazel:227-239,618-632`), and `//datacube:verify_app_test` (`gates/BUILD.bazel:58`), which starts `//warehouse:serve` (`datacube/BUILD.bazel:905-920`) | Move both to the `native` and `browser` lanes | `bazel cquery 'somepath(//gates:local, //warehouse:server_native)'` empty | Test-6 |
| Test-10 | Heavy benchmarks excluded by tag, not by their class names | design §4.3 | Not done: only `ProfileBuildCost.java:13` and `StressTestChaotic.java:36` carry `@Tag("heavy")`; core's excluded tags lack it (`core/BUILD.bazel:433-438`); the scale classes are skipped only because JUnit's default name pattern does not match them (`tools/junit/JUnitMain.java:184-185`, `STANDARD_INCLUDE_PATTERN`) | Tag every scale class; exclude `heavy` in core's targets | renaming a scale class to `*Test` does not pull it into gate 1 | Small |
| Test-11 | The manual heavy tests get a runner (weekly) | old P5-08; old D18 (engine_stress in the weekly suite) | Not started. Manual tests no lane runs: `//spec:corpus_warehouse`, `//spec:reference_lane`, `//spec:update_reference_lane_test`, `//spec:manifest_world_census`, `//parser-equivalence:diagnostics` (only `diagnostics.yml` on path triggers), `//pct:update_ratchets_test`, and exec's `//scripts/corpus:engine_stress` (area8 D2) | `//gates:heavy` (manual) and a scheduled workflow, or a dated reason per test | `attr(tags, manual, tests(//...)) except tests(//gates:heavy)` empty or each has a row | Test-6; OD-12 |
| Test-12 | Test sizes from measured durations | old P7-13; log ("sizing follow-up owed": `//wasm:differential_test`, `//sdlc-client:wasm_test` timed out at 60 s under the full gate) | Not started | sizes from CI `test.xml` over several runs | CI green; fewer `large` | After CI is stable |
| Test-13 | Query's one library over the app plus all of DataCube | area6 B7, C4 | Open question: whether a query test's import closure reaches DataCube (U-9) | Split `query:src` (builder vs app/ui) if U-9 says so | a `datacube/src` edit leaves query's unit tests cached | U-9 |

### 2.4 Checks (structure guards)

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Check-1 | One whole-repo check instead of two reports per package | design §4.4; area8 C2 | Not started: `guards_package()` adds `guard_markdown` and `guard_classpaths` to every package (`tools/guards/defs.bzl:92-108`); design §4.4 counts 108, 57 always empty, 2 unused | An aspect (a Bazel mechanism that visits targets during analysis and can emit extra outputs) or validation actions in the build lane, as the design says; keep a per-package report only for manual targets if the aspect cannot see them (area8 C2) | `markdown_inputs_test` and `classpath_test` fail the same negative edits as today; prove on Windows CI | — |
| Check-2 | `classpath_test` and `markdown_inputs_test` leave the light gate | design §4.4 | Not done: `gates/BUILD.bazel:22,26` | Move to the build/checks lane | `//gates:local` no longer analyses the whole repo (design §5 step 6 exit, by query) | Check-1 |
| Check-3 | `inventory_test` compares file names, not contents | design §4.4; area8 C2 | Not done: its data is every file of every package (`tools/guards/BUILD.bazel:13-34`, `:repository_files`) | Compare per-package name lists (written at analysis time) with `@repo_inventory//:files.txt` | an edit inside a file leaves it cached; an added file reruns it | — |
| Check-4 | The 30 layer queries built only by the layering test | design §4.4 | Not done: `core/layers.bzl:20-30` (no `manual`) | Tag them `manual` (the test still builds them) | `bazel build //...` skips them | Small |
| Check-5 | The old plan's guards never built | old §5 (P6-01 … P6-20) | Done: P6-00 (`34a8cb7ff`), P6-10 (`86361427f`), P6-11 (`c0c229fd8`), P6-14 (`b71829132`), P6-16 (`88af28276`), P6-17 (`485997b1c`), P6-19 (`2216161ab`). Not started: G1 generated marker (P6-01), G2 scripts registry (P6-02), G3 lane coverage (P6-03 = Test-7), G4 test environment (P6-04), G5 process spawning (P6-05), G6 literal ports (P6-06), G7 writes into inputs (P6-07), G8 workflows (P6-08), G9 documents (P6-09), G12 data globs (P6-12), G13 test discipline (P6-13), G15 runfiles (P6-15), G18 linker id, Linux only (P6-18), G20 product environment (P6-20) | Decide which stay in scope (OD-11). G5's and G15's allowlists were written for a world with Node harnesses and `ServerRunfiles` (G15 allows `RUNFILES_DIR` "outside `ServerRunfiles.java` and `pinned-chromium.mjs`"): re-specify after D9 and 2.5. G19 (`//tools/browser:revision_test`) retires with Playwright | each guard's negative edit fails it | Late: G2, G5, G9 need the final tree (script review, docs) |
| Check-6 | NullAway in every first-party library | old P7-16 | Not started: 22 libraries say `nullaway = False` (old P1-20 amendment; e.g. `core/BUILD.bazel:315-316`, `warehouse/BUILD.bazel:113-114`, `pct/BUILD.bazel:73-74`) | A shrink-only count guard, then library by library (792 findings in the first wave) | the count only falls | With Compile-8; L-sized |
| Check-7 | Private by default; a public-target allowlist | old P7-11 (G-22) | Partly: core is private by default (old P1-20); `base`, `json`, `warehouse` and the others not; no `visibility_test` | As old P7-11 | `//tools/guards:visibility_test` | — |

### 2.5 No Node: the tests in the pinned Chromium, through a CDP client

**What is decided.** The user, 2026-10-05: "we should try to rip out node completely just use some fast CDP client"
(memory note `no-node-in-legend-lite.md`; design §1 principle 7, §4.6). `//:web` already runs no Node (Phase 0).
**What is open.** The client (design D6: a small Java client in the repo, recommended; or Go's chromedp) and its
transport (U-1). Note design §4.6 calls it "(decision D6)" while §6 lists D6 among the decisions for the user.

**The size of the job (all counts from the area reports, taken on exec; re-count on main):** DataCube's 15 Playwright
harnesses make 1,878 Playwright calls, of which 1,405 are DOM reads, waits and value-setting that can run inside the
page (area5 Part E, E3; design §4.6). Its tests: 41 files use only `node:test`/`node:assert`, 26 add `jsdom`, 13 use
DuckDB-WASM's Node build (area5 E2). On main there are 98 DataCube test files, plus 3 in `query`, 2 `studio`, 3
`query-store`, 1 `pure-protocol`, 2 `sdlc-client`, 2 `depot-client`, 1 `tools/js` (`find … -name '*.test.ts'`).
What the driver must provide is listed in area5 E3 ("Minimal CDP driver") and area6 E6.

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Node-1 | A spike: Java CDP client, transport, three OSes | design §5 step 7 ("proven on one harness on all three operating systems"); U-1 | Not started | Launch the pinned `chrome-headless-shell` (`//tools/browser`, `MODULE.bazel:379-401`) from a Java test, drive one harness, on Linux, macOS, Windows CI | the harness passes on 3 OSes | OD-4 decides client and transport from it |
| Node-2 | The driver and the page side: an in-page test runner (describe/it/assert), a page helper library for DOM reads and waits, trusted input only where needed | design §4.6; area5 E3 | Not started | As the design; trusted input only for hover, the browser's own drag-and-drop and real hit-testing (area5 E3: `datacube/src` never checks `isTrusted`; drag/resize at `verify-features.mjs` lines 826-842, 2545-2563) | the first ported test page runs under `bazel test` | Node-1 |
| Node-3 | Port the JS tests | design §4.6; area5 E2, area6 E5 | Not started | Each test file bundled into a test page (esbuild); `jsdom` becomes the real DOM; Node-only APIs replaced (`node:crypto` → `crypto.subtle`, `Buffer` → `TextEncoder`: area5 Part D); DuckDB-WASM's browser build (already in `:vendor`, `datacube/BUILD.bazel:690-718`) instead of `blocking`/`NODE_RUNTIME` | the same test names pass; `node:test` gone | Node-2; U-3 |
| Node-4 | Port the browser harnesses (DataCube's 15, `query/demo/verify.mjs`, `site/verify.mjs`, `studio/demo/verify.mjs`) | design §4.6; old P4-01…P4-09; area5 E1, area6 E1-E3 | Not started on main (main still runs `query:verify` and `site:verify` by a `bazel run` loop with Playwright's own Chromium: `gates-run.yml:151-153,180-197`). Exec holds the Playwright versions (2.7): their awaited conditions, sections and port-0 changes are the knowledge to carry over | Each harness a test on the driver; no fixed sleeps; port 0 | `bazel test` of each; CI's browser lane names suites only | Node-2; OD-3 |
| Node-5 | Node servers replaced by Java servers or a small Java static server | design §4.6 ("Dev servers are our Java servers") | Not started: `datacube/demo/serve.mjs`, `query/demo/serve.mjs`, `site/serve.mjs`, `harness.serve` | As the design | no `node:http` in the repo | Node-2 |
| Node-6 | File and system checks written in Node become JVM tests or Bazel checks | area5 Part D ("read sources"), area6 Part D ("guards") | Not started: `bundle-budget.test.ts`, `dist-complete.test.ts` (exec), the six source scanners, `portability.test.ts`, `//tools/js:lock_matches_package_json_test` | JUnit tests like `//tools/guards`'s, or analysis-time checks | same verdicts | — |
| Node-7 | The JS generators | Gen-2 | — | — | — | Node-2 |
| Node-8 | Remove the Node machinery | design §5 step 7 exit ("`bazel query` finds no Node toolchain dependency") | Not started: `rules_nodejs` (`MODULE.bazel:30`); `node_test`/`browser_test` (`tools/js/defs.bzl`, `tools/browser/defs.bzl`); `tools/js/runfiles.mts`, `zone.mjs`, `strict-reporter.mjs`; `//tools/browser:pinned_chromium` and `revision_test`; `//datacube:install_browser`; Playwright in the package files | Delete; decide whether rules_js stays only to fetch npm files (design §4.6 says decide in step 7) | the exit query | Node-3, -4, -5, -6, -7 |
| Node-9 | The Chromium pin's own policy | `MODULE.bazel:379-401` (pinned to "the locked playwright-core's" revision); old G19 | Open once Playwright goes: nothing ties the revision anymore | A pin rule for the browser alone (the user decides) | — | Node-8 |

### 2.6 CI and caching

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| CI-1 | Lanes from `//gates` | design §5 step 8; old P5-01 | = Test-6 | — | — | First |
| CI-2 | A cache that works | the plan's "caching (D14)" (no such decision exists); design §5 step 8 ("one cache per platform (then a remote cache …)"); old P5-04 (key by commit, restore by lock hash); old §6.3 (remote cache deferred) | Not started. Measured 2026-10-07: 5 caches, 17.5 GB, all macOS (`build` 6.78 GB, `7p` 3.70, `8` 3.22, `5` 2.18, `6` 1.60), all created 2026-10-06; key per platform, image, pins and lane (`gates-run.yml:115-124`), so a cache is saved once and never refreshed until a pin moves | OD-5 | the second run of an unchanged commit is mostly cache hits on every platform | Decide early: every landing pays a 40-57 min full run |
| CI-3 | Workflows are only checkout, cache and `bazel` lines | old P5-03 and its 2026-10-03 amendments | Not started: the jq matrix (`gates-run.yml:33-80`), the browser harness loop (`:180-197`), `install_browser` (`:151-153`), the `tzutil` step (`:125-134`), MSYS settings (`:87-97`), the `git config` step (`:103-107`), the libxml2 `apt-get` (`:135-140`), bazelisk by curl (`:141-150`) | As old P5-03, except: **keep** the `gates` dispatch input (it now names `//gates` suites), because the user's throwaway-CI rule depends on it (section 5, #13) | old G8 (P6-08) passes | Test-6; Node-4 (for the browser loop) |
| CI-4 | actionlint as a Bazel target | old P5-05 | Not started: `gate.yml:64-67` downloads it with `curl` and no checksum | `http_archive` per platform with `integrity`; a test over the workflows | no `curl` in any workflow | Small; anytime |
| CI-5 | Actions pinned by commit; the `paths-ignore` question | old P5-04 | Not started: `uses: actions/checkout@v4`, `actions/cache@v4` by tag (`gates-run.yml:108,116`); `paths-ignore` (`gate.yml:23-29`) | Pin by full SHA; decide `paths-ignore` with CI-2 | no tag pins | With CI-2 |
| CI-6 | A pinned Linux CI image | old P5-02; old D4 (b) and old D7 (b), decided 2026-10-03 | Not started; still owed: libxml2 is "a recorded exception until P1-09's final form … owed with P5-02's pinned CI image" (`MODULE.bazel:260-262`) | Re-confirm (OD-12): Chromium's Linux system libraries are still needed by the pinned browser after Playwright goes | Linux jobs need no `apt` step | OD-12 |
| CI-7 | What still needs bash, measured on every platform; README Prerequisites | old P5-07; old D20 (a) | Not started | `bazel aquery` per platform for actions starting a shell; record in `.bazelrc` | the list exists | Feeds Short-1; after Node-8 the list changes |
| CI-8 | A Turkish-locale CI entry | old P5-06 | Not started | As old P5-06 | same verdicts | OD-12 |
| CI-9 | A PowerShell "desk" lane on Windows, with `--self-test` | old P5-09 (C2) | Not started | As old P5-09 | the lane is green and catches a desk-only break | OD-12 |
| CI-10 | The nightly run on main | Phase 2 (`gate.yml:33-34`, cron `23 6 * * *`) | **UNVERIFIED** that it fires: `gh run list --workflow gate.yml --event schedule` returned none at 2026-10-07 06:50 UTC | Check after the next cron | a `schedule` run appears | — |
| CI-11 | `diagnostics.yml`'s path triggers | `.github/workflows/diagnostics.yml:7-25` | Works; a CI-level trigger filter beside Bazel's own caching | Decide: keep, or a `//gates` suite in a scheduled run | — | With Test-11 |

### 2.7 The parked `bazel/exec` work

**State.** 14 commits on `3faa7d291` (an old main), not rebased; 13 of the files they touch have changed on main
since (`git diff --name-only` both ways: `.github/workflows/gates-run.yml`, `core/BUILD.bazel`, `datacube/BUILD.bazel`,
three `datacube/bench/*.mjs`, five `datacube/demo` files, `datacube/test/upload.test.ts`, `warehouse/BUILD.bazel`).
Step 0 of the design: "They are re-checked against this design before anything lands."

| Commit | Item | Depends on Node? | What it is worth now |
|---|---|---|---|
| `d2d421ca3` | P4-14: the JDBC driver's service file ships only with the client | no | Keep (one BUILD line); independent of D13's answer |
| `8cfdc9c34` | P4-16: the upload test writes to its temp directory | yes (DuckDB-WASM's Node build) | Moot after Node-3 |
| `055e79636` | P4-01: one shared harness module (`datacube/demo/harness.mjs`) | yes | Interim; **adds the 7,824-line `stress-results.json` at the root**, removed by `6f1d9aa9a`: squash before landing anything |
| `812968133` | P4-02: the mechanical harnesses are browser tests, no fixed sleeps | yes | Interim; the awaited conditions carry over to Node-4 |
| `4a35e4573` | P4-03: verify_remote/real_data one test, two shards | yes | Interim |
| `d730b8381` | P4-04: verify_features sharded by section | yes | Interim; the section split carries over |
| `1bcb4f572` | P4-08: dev tools on port 0, explicit output | yes | Superseded by Node-5 (Java servers) |
| `14178ed8b` | P4-18 and part of P4-06: servers exit with their parent; torture is a test; no `taskkill` | Java half no; JS half yes | Keep the `--exit-with-parent` flag in `LegendHttpServer.java` and `WarehouseServer.java` (any driver needs it); its IN_FLIGHT announcement is on main, parked (`fcb03af52`; IN_FLIGHT main lines 19-21): re-announce |
| `e614ab885` | P4-06 rest: verify_picker serves the site; calc vocabulary local half | yes | Interim |
| `36a470fa1` | P4-09, DataCube's part: CI's browser lane runs `//datacube:browser` | yes | Interim; replaced by `//gates` lanes |
| `78cff70a4` | P4-17: DataCube's `.mjs` typechecked | yes | Moot once harnesses are gone |
| `dab833263` | P4-11, completeness half: `//datacube:dist` complete, and a test | edits `make-dist.mjs` | `make-dist.mjs` goes with D10; keep the test's idea (Compile-5) |
| `ec61c8177` | P4-15 part: the real-data guide needs no npm/npx/duckdb | partly | Keep the doc change |
| `6f1d9aa9a` | Phase 4 audit follow-ups | mixed | Squash with the above |

**Uncommitted on exec** (`git -C runs/bazel-exec status`): harness fixes in `datacube/demo/verify-features.mjs`,
`verify-real-data.mjs`, `verify-remote.mjs`, `datacube/test/upload.test.ts` (forward slashes for DuckDB on Windows; in
`verify-features.mjs` a `waitForFunction(...).catch(() => {})` followed by the real assertion), and **P3-25**:
`scripts/corpus/run.py` gets one `launch()` that starts `//tools/engine-runner:testable` from runfiles (old D18 (a));
`py_test(name = "engine_stress", size = "enormous", tags = ["manual"])` and `py_binary(name = "run", testonly =
True)` in `scripts/corpus/BUILD.bazel`; `//tools/wrongrows:compare` a `py_binary` (new `tools/wrongrows/BUILD.bazel`);
`tools/wrongrows/engine-rows.sh` reduced to `exec bazel run //scripts/corpus:run -- …`.

| ID | Item | Source | Status (evidence) | What to do | How to check | Depends / order |
|---|---|---|---|---|---|---|
| Exec-1 | Decide what of the 14 commits lands | design §5 step 0; plan Phase 0 ("parked … until Phase 8") | Parked | OD-3 | — | First, before Node work |
| Exec-2 | Finish P3-25 | old P3-25; old D18 (decided 2026-10-03 `151acdce6`, re-confirmed 2026-10-05, log) | Uncommitted, with the two issues the design names: `:run` is not `manual`, and `engine_stress` has no `shard_count` though `run.py` reads `TEST_TOTAL_SHARDS` (design §5 step 0). Arithmetic: 4,737 testables in batches of 200 at up to 1,200 s each (`run.py`, uncommitted) is up to 24 batches, beyond an `enormous` test's 3,600 s limit without shards | Tag `:run` manual; set `shard_count`; put `engine_stress` in the weekly heavy suite (old D18); its true trigger is a legend-engine bump or a corpus/quarantine change, never our engine (area4 C8), so it also belongs in the bump's checks (Phase 7) | `bazel test //scripts/corpus:engine_stress` alone (heavy, manual); `bazel build //...` does not build `//tools/engine-runner:testable` | Test-11; delete `engine-rows.sh` with the script review (old P7-01) |
| Exec-3 | The rest of old Phase 4 never started | old P4-05, P4-07, P4-10, P4-12, P4-13, P4-15 (rest), P4-90 | Not started | P4-05/P4-07/P4-10 become Node-4 work on the driver; P4-12 becomes Compile-4 (D9); P4-07's engine server target (`//tools/engine-runner:server` from `legend-engine-server-http-server:4.145.0`, old P1-18 investigation) lands with its first user; P4-13 (a packaged warehouse, `pkg_tar`/`pkg_zip`) and P4-15's `bench/model/*.py` as `py_binary` on `@pypi//duckdb` (old D21 (b)): re-confirm (OD-12) | per item | OD-3, OD-12 |

### 2.8 The carried shortcuts

The plan's list (2026-10-07, "fixed correctly, not worked around"), verified against main, then the ones it does not
list. Each found on main unless marked.

| ID | Shortcut | Status (evidence) | The proper fix, and where |
|---|---|---|---|
| Short-1 | Hard-coded Windows bash path: `common:windows --repo_env="BAZEL_SH=C:/Program Files/Git/usr/bin/bash.exe"` and `build:windows --shell_executable=…` | Open: `.bazelrc:41-43`; the comment names the users: "rules_jvm_external's Maven fetch (it fails without BAZEL_SH) and rules_js's launchers" (`.bazelrc:34-35`) | Measure first (CI-7). The rules_js launchers go with Node-8; which part of rules_jvm_external needs `BAZEL_SH` is **UNVERIFIED**. Options for the user (OD-14): remove the need for bash on Windows; or drop the lines and rely on Bazel's own search (`BAZEL_SH`, MSYS2's registry key, PATH), documented as a prerequisite (old D20 (a)). A hard-coded path is not an option (memory "no-hardcoded-paths-in-bazel") |
| Short-2 | The gzip `run_shell` in `warehouse/defs.bzl` | **Done** on main: `4f7448989` (2026-10-04, "the build's last shell action goes"); `warehouse/defs.bzl:122-133` uses bazel_lib's `copy_to_directory` | Remove from the plan's list |
| Short-3 | `warehouse_run` takes no `visibility`, `tags`, `target_compatible_with` | Open: `def warehouse_run(name, server, library, site = None, args_before = [], testonly = False)` (`warehouse/defs.bzl:103`) | Inside Compile-4 (D9 reshapes `warehouse_run`); doing it alone first would be done twice |
| Short-4 | `LauncherTest` pins the JDK's message text | **Done** on main: `067973962` (2026-10-05, P3-21): the server prints "--port takes a number, not 'x&y z'" and the test asserts that (`LauncherTest.java:51-52`) | Remove from the plan's list |
| Short-5 | Engine-tree subset filegroups | Open | Gen-9 |
| Short-6 | Parser-equivalence test data never read | Open: `_LEDGERS` includes `protocol-roster` and `_FILE_DATA` includes `corpus-manifest.tsv` (`parser-equivalence/BUILD.bazel:208-222`), unread by the tests (PHASE1_CHANGES item 10). The plan calls it "the parity tests' unread data" | Drop them from the tests' data; or, with area2 C9, move the ledgers next to their generators |
| Short-7 | The PAR's permanent entry-time test | Open, a user call (PHASE1_CHANGES item 11): an optional test in `//pct:pct_duckdb` that the PAR's entries keep constant times | OD-16 |
| Short-8 | The offer-facts tool reads its own output | Open | Gen-3 |
| Short-9 | `//spec:eager_corpus_compile` declares the pure tree it no longer reads | Open: `srcs = _INPUTS + UPSTREAM_TREES`, `program_jvm_flags("spec")` (`spec/BUILD.bazel:533-544`; evidence/phase1/AUDIT_PHASE1.md N10) | With the measurement group (Meas-8) |
| Short-10 | The link dictionary's runtime check | Open | Gen-4 |
| Short-11 | **Not listed:** a hard-coded machine path for Bazel's output root on Windows, `startup:windows --output_user_root=C:/bzl` | Open: `.bazelrc:22-26`; kept because runfiles trees push pyarrow's DLLs past Windows' path limit; "USER: keep it for now" (old P3-32 note) | Goes when Windows runs without runfiles trees (OD-1), re-measured |
| Short-12 | **Not listed:** `build:windows --enable_runfiles` | Open: `.bazelrc:33`; old P3-32 "blocked on rules_js" (workplan P3-32 amendment 2026-10-05; log "P3-32 held") | After Node-8 removes rules_js tests; but D9's `$(rootpath)` needs it (OD-1) |
| Short-13 | **Not listed:** the rules_graalvm patch | Open: `MODULE.bazel:221-230`, `third_party/rules_graalvm_sysroot.patch`; upstream sgammon/rules_graalvm#602 still OPEN (`gh pr view`, last update 2026-10-04; latest release v0.12.0, 2026-07-19); old P1-12 (b), per-platform GraalVM toolchains, never investigated; Bazel 10's `--incompatible_stop_exporting_language_modules` blocked by it (old P1-12 amendment, G-21) | Old P1-12; held to the end with the script review (log 2026-10-05) |
| Short-14 | **Not listed:** the rules_jvm_external testonly patch | Carried by decision: `MODULE.bazel:14-18`; "USER: no upstream PR" (log, batch 6) | Keep; re-check on each rules_jvm_external upgrade |
| Short-15 | **Not listed:** libxml2 installed by `apt-get` on Linux CI | Recorded exception (`MODULE.bazel:260-262`; `gates-run.yml:135-140`); old D7 (b) owed | CI-6 |
| Short-16 | **Not listed:** actionlint fetched by `curl` with no checksum | `gate.yml:64-67` | CI-4 |
| Short-17 | **Not listed:** `taskkill` in tests on main | `datacube/demo/verify-app.mjs:20-25,199-216` and `query-store/test/lite.test.ts:13-18,63-71`; removed on exec (`14178ed8b`) | Exec-1 / Node-4 |
| Short-18 | **Not listed:** CI installs Playwright's own Chromium and runs two harnesses by `bazel run` | `gates-run.yml:151-153,180-197` | Node-4, Node-8 |
| Short-19 | **Not listed:** 22 libraries without NullAway | old P1-20 amendment | Check-6 |
| Short-20 | **Not listed:** `postgres_live` tests never run | `warehouse/BUILD.bazel:250-275` | Test-8 |
| Short-21 | **Not listed:** `//spec:manifest_world_census` fails its ceilings and no one owns it | log (P3-17 audit): "FAILS its core_relational ceilings (load walls 37 > 32, failing bodies 1,476 > 1,447)", manual, "Owner: the compiler line"; **UNVERIFIED** today | Meas-9 |
| Short-22 | **Not listed:** scripts that read a host JDK or checkout | `scripts/corpus/run.py` on main (`~/jdk/jdk-21.0.11+10`), `coverage.py:48`, `mutate.py:43`, `scripts/census_gate.py:40-41`, the probes through `runner.JAVA_HOME` (`probe_aggregates.py:208`, …), `scripts/parser/*`; `tools/wrongrows/engine-rows.sh` (a macOS JDK path on main) | The script review, old P7-01 to P7-05, held to the end (log 2026-10-05); D7 vs old D18 first (OD-8) |
| Short-23 | **Not listed:** `//datacube:dist` broken as a deployable | G5 cross-cutting #4 | Compile-5 |
| Short-24 | **Not listed:** the bash launcher and `hermetic_launcher` behind `//warehouse:serve` and `//datacube:app` | `warehouse/defs.bzl:56-101` (`#!/usr/bin/env bash` script), `:155-169`; `MODULE.bazel:308` | Compile-4 (old P4-12's goal: "No launcher script and no `hermetic_launcher` remain") |
| Short-25 | **Not listed (found 2026-10-07, writing plan §0):** the SQL census's two sides are shell scripts that drive Bazel from outside it, at two commits checked out by hand (`git switch`, `tools/census/README.md`) | `tools/census/lanes.sh:12,16,20` (`bazel query`, `bazel test` with `--cache_test_results=no`, then copies each lane's `test.log` out of `bazel info bazel-testlogs`); `tools/census/render.sh:9` hard-codes the macOS JDK inside the output base (`remotejdk25_macos_aarch64`), then runs `javac` and `java` itself (`:16-19`). The comparing halves are already targets (`//tools/census:render_census`, `//tools/census:lanes_diff`, both still to tag `manual`: Gen-5) | With the comparison design (plan §0.4: "the Bazel way to compare a run with a previous run or a sibling run", not designed yet); the script review |
| Short-26 | **Not listed (found 2026-10-07):** `tools/ci-watch.sh` watches CI through `curl` and `python3` from the host (`:8-9`, unchanged since `8c2103d13`, 2026-09-13) | A hand tool, not build work; the `gh` CLI does the same (`gh run watch`) | Delete, or keep as a hand tool: the script review decides |

Handed off, not build work (recorded so nobody hunts for them): `Lexicon.H2` lacks H2 2.x reserved words (log, batch
11), for the dialect owner; F-L1 (a view inside a Schema lifted twice) is taken by plan Phases 3b/6; PARK-13 (a debug
trace switched by an environment variable in `Overloads`) overlaps old P7-14 and G20 (P6-20): fix it once, after the
program, as plan §5 says.

### 2.9 The measurement group

Set aside by the user on 2026-10-05 (GENERATORS §5); the plan asks: "Decide what each is and carve it by its true
trigger." The dossiers already hold a recommendation for each; none is agreed. Two things block a clean carve:
- **Phase 7 touches the same writers.** GENERATORS §6 step 6 (= Phase 7) removes "the upstream records and the
  measurements from `//:update_generated`"; so the user must say, before Phase 7, where each measurement's writer goes
  (OD-6).
- **Phase 6 rewrites what the corpus passes read** (plan Phase 6: the runner loads its manifest, replacing
  `LIBRARY_FILES`, `SHAPE_FILES`). Narrowing the passes' inputs before Phase 6 would be done twice.

| ID | Member | What it is (dossier) | True trigger (dossier) | How it runs today (evidence) | Recommendation in the dossier (not agreed) |
|---|---|---|---|---|---|
| Meas-1 | The six corpus passes `//spec:judge_{host,database}_{duckdb,h2,warehouse}` | Tests in disguise: a verdict on engine behaviour plus goldens a person re-blesses (G2 §A9) | Engine behaviour on that database: core main on the corpus path, the corpus harness, the policy files, the driver; an upstream bump (G2 Part C) | Build actions; duckdb and h2 not manual, so `bazel build //...` runs them (`spec/corpus.bzl:72-114`; `spec/BUILD.bazel:175-221`); declared inputs 17,180, of which 1,333 core sources and the whole pure tree are never read (G2 §A3); no test timeout applies (G2 §A5) | (a) tag them so `//...` skips them, lanes 4/5 name the suites; (b) narrow the inputs and give the pass a corpus-only library; (c) writers stay deliberate, per lane; (d) diff tests where the passes run. Open point: the verdict test becoming the database pass itself (G2 §A9). Do (b) after plan Phase 6 |
| Meas-2 | `//core:ladder_report` (the lean SQL ladder) | A test in disguise: the committed SQL is an expected result (G4 §4) | Engine behaviour on the verdict SQL path, or `LadderRender.java` | A `java_run` on the whole `core_tests_lib` (`core/BUILD.bazel:829-840`); its writer is inside `//:update_generated` (`BUILD.bazel:111`), so an update re-blesses it silently | Out of `//:update_generated` and the bump; explicit `bazel run //core:update_ladder` only; its own small library; diff test in gate 1 (G4 §4; area4 C2) |
| Meas-3 | The ratchets: `//spec:ratchets`, `//parser-equivalence:ratchets`, `//pct:ratchets`; plus core's missing ones (old P2-21) | Measured values beside hand-owned ceilings (old D9 (b)) | spec: our catalog, registrations, DynaFn, spec constants; an upstream bump (G1 table). pe: `//core:parser`, its own snippets and fixtures, upstream jars (G3). pct: G3 calls it "COMMITTED-UPSTREAM", bump-only plus explicit re-pin | spec: not manual, on `spec_tests_lib`, so every core edit reruns it (`spec/BUILD.bazel:408-418`). pe: not manual, reads all of `//core:srcs`, `//pct:srcs`, `//spec:srcs` (`parser-equivalence/BUILD.bazel:512-526`). pct: manual, but `//:update_generated` runs it (`BUILD.bazel:120`; `pct/BUILD.bazel:275-283`). Core: old P2-21 never started (no `core/ratchets.tsv`; `git log` has no P2-21) | Own small libraries; pe reads `:test_java` filegroups (area3 C1); pct with the bump (Phase 7); core per old P2-21, or a dated row per family. Carve spec's after plan Phase 5 (it changes what spec measures) |
| Meas-4 | `//datacube:catalog_corpus` | Disputed: G5 says test in disguise (it runs real DuckDB at build time); plan Phase 1 status says "DataCube's test expectations (real DuckDB's answers), not a measurement" | DuckDB 1.5.5.1's catalog (jar pin), CatalogModel and its 12 libraries (G5) | A non-testonly, non-manual `java_run` (`datacube/BUILD.bazel:409-414`) whose output is committed and diff-tested | OD-7 |
| Meas-5 | `//fixtures/saved-queries:gen` | Records of ours plus a row-count check that is engine behaviour (G4 §7) | the record literals, demo models, the store's record shape; row counts: the server | A `js_run_binary` around Node `make.mjs` that starts the server's deploy jar (`fixtures/saved-queries/BUILD.bazel:33-62`; old D14 (a)) | Row counts become a server test; the writer leaves `//:update_generated`; a narrower server dependency is OPEN (area4 C3). Also Node (Gen-2) |
| Meas-6 | `//spec:reference_lane_report`, its golden and test | A test in disguise for our front end (G1 table) | our parser, resolver, typer; an upstream bump | Manual (`spec/BUILD.bazel:255-298`); compiler phases run it by hand before landing (START_HERE §5) | Re-blessed only deliberately; a runner is OD-12 (weekly heavy) or by hand, documented |
| Meas-7 | Coverage: `//parser-equivalence:gen_roster`, `//scripts/parser:keyword_coverage` | Mixed (G3) | roster: upstream release and our test sources; keyword coverage: our `.pure` and engine grammars | `gen_roster` reads core, pct and spec sources whole (area3 C1); `keyword_coverage` no longer pulls `vocab` (Phase 1) | Narrow `gen_roster` to `:test_java` filegroups |
| Meas-8 | On request: `corpus_census`, `grammar_keyword_census`, `eager_corpus_compile`, `gen_own_corpus_draft` | Drafts and measurements, manual (G1, G3) | a person asking | manual (`parser-equivalence/BUILD.bazel:569-570`; `spec/BUILD.bazel:533-544`) | Keep manual; narrow `eager_corpus_compile` (Short-9) |
| Meas-9 | Unowned measurements: `//spec:manifest_world_census`, `//parser-equivalence:diagnostics` | census with ceilings; the diagnostics battery | the compiler; the parser and pins | manual; `diagnostics.yml` path triggers | Give each an owner and a runner (Test-11) |

### 2.10 The renames (last)

| ID | Item | Source | Status | What to do | Depends |
|---|---|---|---|---|---|
| Rename-1 | design D11: `core`, `db` (today `warehouse`), `sdlc` (`sdlc-server`), `depot` (`depot-server`): folders, Java packages, targets, docs, CI lane names | design D11 (decided 2026-10-05, `0bd6ebcfa`) | Not started | Its own announced change, after the build work | Everything else; touches CODEOWNERS paths (`.github/CODEOWNERS`), `tools/deps/pools.bzl` grants, visibility lists, guard allowlists |
| Rename-2 | design D12: Depot its own server | design D12 (decided 2026-10-05) | Not started | "Done with D11, or before it as a product change. It is not build work." | D11 |

### 2.11 The end: the old plan's remaining cleanup and the final audit

| ID | Item | Source | Status | Note |
|---|---|---|---|---|
| End-1 | Record the script-review decisions (187 rows) and apply them | old P7-01 … P7-05; old D3, D3b, D16, D18, D19 | Not started; "waits until everything else is done" (log, USER 2026-10-05) | D7 vs old D18 first (OD-8); old D19: `upstream-drift.py` reads `@legend_*_src`, `move_classes.py` keeps `git mv`, both still to do |
| End-2 | Documentation: FAQ, README, core/README, AGENTS.md lines 39-40; GATES, ENGINEERING_LOG, RUNNING_THE_CORPUS, UPSTREAM_*; tool READMEs; history moves | old P7-06 … P7-09 | Not started | AGENTS.md is shared: ask the user first |
| End-3 | Small dead files; product debug switches; test sizes | old P7-15, P7-14, P7-13 | Not started | P7-14 overlaps PARK-13 (2.8); P7-13 = Test-12 |
| End-4 | Phase audits not done | old P3-90, P4-90, P5-90, P6-90, P7-90 | Not done (workplan §6.5 has P0-90 to P2-90 only) | Fold into one Phase 8 audit per landing (the program's audit-agent rule) |
| End-5 | The final re-audit against every finding | old P8-01 | Not started | Its "restore D17's guardrail 2" conflicts with "no PRs" (OD-9); its "the plan and its evidence move to `docs/history/`" still applies |
| End-6 | Close old P0-14 | old P0-90 ("partial until `codeowners/errors` is `[]`") | Closable: `gh api repos/neema2/legend-lite/codeowners/errors` returns `{"errors":[]}` (2026-10-07) | Record it |

---

## 3. Every decision, both series

### 3.1 The old workplan's series ("old D1" … "old D21")

Source: `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` §2, each section's "Decided" or "Status" line. **Its summary
table (§2, top) still says D17 to D21 are OPEN; every one was decided** (section 5, #1).

| # | Question | Status, who, when (record) | Today |
|---|---|---|---|
| old D1 | macOS SDK for the C toolchain | Decided (b) 2026-10-03; **revised 2026-10-04**: "USER: 'Let's just do similar thing on Mac that we did for windows'": the Command Line Tools declared and checked | Done (P1-10, `@host_cc`) |
| old D2 | PCT sharding | Decided (b), 2026-10-03 | Done (P3-09, `729928b5b`) |
| old D3, D3b | The script review's 11 rows; files to delete | Decided 2026-10-03: every recommendation, except row 6 "keep and repair" and row 9 "wire the tool, no moves"; D3b "as listed" | Rows applied by P7-01…05, held to the end |
| old D4 | Linux system libraries for Chromium | Decided (b): a pinned Debian 12 image defined in Bazel, 2026-10-03 | Never built (P5-02); re-confirm (OD-12) |
| old D5 | MSVC on Windows | Decided (a), 2026-10-03 | Done (P1-11) |
| old D6 | Composite PCT lanes | Decided (b) delete, 2026-10-03 | Done (P3-09) |
| old D7 | `libxml2.so.2` on Linux hosts | Decided "(b), then (a) as a recorded exception", 2026-10-03 | (a) in force (`MODULE.bazel:260-262`); (b) owed with P5-02 |
| old D8 | Stress diff tests in the default `bazel test //...` | Decided (a) plus P2-05 | Done |
| old D9 | Ratchets: measurement generated, policy hand-owned | Decided (b) | Applied (P2-16, P3-30); core's half (P2-21) open |
| old D10 | A download mirror | Decided (c): no mirror | Done (P1-27) |
| old D11 | Windows browser CI | "Not now", **revised** 2026-10-03: yes for `verify_app` ("USER: 'we need that to make sure mads doesnt get mad at us'") | Done (P1-14b) |
| old D12 | Python version | 3.12 | Done |
| old D13 | Stress-generator details | As recommended | Done (P2-01) |
| old D14 | How the saved-query fixtures are made | Decided "(a) if P2-06 shows no exec-configuration double compile, else (b)"; (a) held (P2-06 amendment) | Done (`e8f02f9ce`), but (a) is a Node `js_run_binary`: in tension with no-Node and design §4.2 E (Meas-5). **Not the plan's "D14"** |
| old D15 | One Playwright or two locks | Two locks, exact versions, one revision test | Done (P1-26, G19); superseded by no-Node (Node-8, Node-9) |
| old D16 | Where history goes | `docs/history/<original path>` | P7-04/P7-09, not done |
| old D17 | Branch protection | Decided 2026-10-03 (the fast loop); amended 2026-10-03 (ruleset with owner bypass: "USER: 'can we add branch protection but me still be able to push directly to main?…'"); guardrail 2 suspended 2026-10-04 ("USER: 'if codeowners going to block us we should undo until the end'") | Overtaken by "no PRs" (2026-10-06); old P8-01's restore conflicts (OD-9) |
| old D18 | Python programs that start Java | Decided 2026-10-03 (`151acdce6`: probes become `bazel run` tools; files written become actions; verdicts become tests); re-confirmed 2026-10-05 (log: "USER … 'fine to keep python but as real first bazel'") | P3-25 uncommitted (Exec-2); probes in P7-03. Contradicted by design D7 (OD-8) |
| old D19 | `bazel run` tools that call host programs | Decided 2026-10-03: `git` stays in Bump and `move_classes.py`; `upstream-drift.py` reads `@legend_*_src`; Bump's nested Bazel through `$BAZEL_REAL` | Bump done (P2-10); the two scripts in P7-02/P7-03 |
| old D20 | bash as a declared host prerequisite | Decided (a): declare and measure | Measurement (P5-07) not done; feeds Short-1 |
| old D21 | `datacube/bench/model/*.py` | Decided (b): `py_binary` on `@pypi//duckdb` | P4-15 rest, not done; re-confirm (OD-12) |

### 3.2 The design's series ("design D1" … "design D15")

Source: `docs/BUILD_REBUILD_DESIGN_2026_10_05.md` §6. Decision commits: `c7fa8366c` (D8-D10), `0bd6ebcfa` (D11,
D12), `9c9dd76cb` (D5, the TypeScript decision, D13 open), `49b36bad8` (D15 open). No commit records D1, D3, D4, D6
or D7 as decided (`git log --all` grep).

| # | Question | Status | Today |
|---|---|---|---|
| design D1 | One Error Prone policy | **Open.** (a) full defaults for all first-party code; (b) one explicit list | Compile-8 |
| design D2 | `native-claims.tsv` | **Decided by the user 2026-10-06** through the program (plan §2, decision 5: retire it with `core_next` and `gen_claims`, Phase 5) | Phase 5, not Phase 8 |
| design D3 | Parse upstream with upstream's parser for `gen_natives` | **Superseded** by plan Phase 5 (Pure.java as rows keyed by function id; the signature generator retires) | — |
| design D4 | `postgres_live` tests: a lane or delete | **Open** | Test-8 |
| design D5 | DataCube's fixture bundles out of the site | **Decided** 2026-10-05 | Not done (Compile-5) |
| design D6 | The CDP client | The goal is **decided** (no Node, the user 2026-10-05); the client is **open**: a small Java client (recommended) or Go's chromedp; the transport is unproven (U-1) | Node-1 |
| design D7 | Delete the 16 broken `probe_*.py`; say how `docs/FUNCTIONS_EXECUTED.tsv` is regenerated | **Open**, and contradicts old D18 (keep them as tools) | OD-8. The functions gate reads that file (`scripts/corpus/BUILD.bazel:247-250`), written only by the unrunnable `probe_functions.py` (`functions.py:42-43`); old P2-18/P7-03's answer was `bazel run //scripts/corpus:probe_functions -- --record` |
| design D8 | Delete `ide` and `probe` | **Decided** 2026-10-05 | Not done (Compile-3) |
| design D9 | The shipped server knows nothing about Bazel | **Decided** 2026-10-05 | Not done (Compile-4); conflicts with old P3-32 (OD-1) |
| design D10 | One shape per app; `//:sites` | **Decided** 2026-10-05 | Partly (Compile-5) |
| design D11 | One naming scheme, at the end | **Decided** 2026-10-05 | Rename-1 |
| design D12 | Depot its own server | **Decided** 2026-10-05 | Rename-2 |
| design D13 | `//warehouse:client` (JDBC over the warehouse's HTTP API): product or delete | **Open** | Direct users today (grep `jdbc:warehouse\|WhDriver`): `WarehouseServerTest`, `WarehouseJdbcTest`, and `DuckWorkspaces.java:174`, the manual corpus warehouse leg (`spec/BUILD.bazel:206`); `postgres_live` only carries it through `tests_lib`. (The design's mention of the reachability generator is stale: section 5, #19) |
| design D14 | — | **Does not exist** | The plan's "caching (D14)" is OD-5 |
| design D15 | The bundles' build-path comments | **Open** | Compile-6 |
| (no number) | TypeScript type checking | **Decided** 2026-10-05: "stays a test for now", cleanup recorded | Compile-7 |
| (no number) | The DuckDB jar's manifest rewrite | **Decided**: stamping off (`669b39ad1`, Phase 0) | Done |

### 3.3 The program's own decisions and rules that bind Phase 8

- Plan §2 (the user, 2026-10-06): 4. "Bazel changes are reviewed by an audit agent before a push or PR"; 5. D2 retires.
- No PRs (2026-10-06; plan header): audit, local gate, one full CI run on the branch, push that commit to main.
- Throwaway CI only on affected lanes (memory note, 2026-10-05: "run only the jobs we care about in these throw away
  runs"); a full run for `MODULE.bazel`, toolchain, `.bazelrc` or workflow changes.
- Carried shortcuts are "fixed correctly, not worked around" (plan Phase 8, 2026-10-07); a new shortcut is a new
  `PARKED_WORK_LEDGER` row (plan §5).
- Old P3-06 and the script review (with P1-12 and P7-16) go last (log, USER 2026-10-05).
- Bazel's built-ins first; prove on Windows (memory note, 2026-10-04).

### 3.4 Open decisions this brief found (each needs the user)

| # | Decision | Options | Evidence |
|---|---|---|---|
| OD-1 | Windows runfiles: trees or manifest only | (a) keep runfiles trees on Windows for good (D9's `$(rootpath)` works; old P3-32's goal and the `C:/bzl` removal are dropped); (b) manifest only (old P3-32), so launchers and tests must resolve runfile locations through a runfiles library, not `$(rootpath)` | design D9 text; `.bazelrc:22-26,33`; old P3-32 and its 2026-10-05 amendment |
| OD-2 | D9's details | (1) the server's `BUILD_WORKING_DIRECTORY` read (`WarehouseServer.java:820-830`) stays (a Bazel fact inside the product), goes with a documented "use absolute paths" (old §6.3 row, S2 Q5), or moves to whatever starts the server; (2) `hermetic_launcher`: keep, or prove on Windows that `bazel run` of the plain executable keeps arguments with `&` and spaces intact (U-6) | PR #14 review (memory note): the `&`-splitting is why `hermetic_launcher` exists (`warehouse/defs.bzl:104-112`) |
| OD-3 | The 14 parked commits | (a) rebase and land them as an interim (DataCube's harnesses become real tests on the pinned Chromium now), port to the CDP driver later; (b) land only the Node-independent parts (P4-14, the Java half of P4-18, the completeness test idea, the doc) and go straight to the driver; (c) drop them | 2.7's table; 13 overlapping files; the junk-file commit pair |
| OD-4 | CDP client and transport | Java in the repo (design's recommendation) or Go's chromedp; pipe (fds 3/4) or port 0 with the JDK's WebSocket client | U-1 spike |
| OD-5 | Caching (the plan's "D14") | (a) per platform and lane, keyed by commit with fallback restores (old P5-04); (b) one cache per platform (design §5 step 8); (c) a remote cache (old §6.3: "not needed for correctness") | CI-2's measurements; 40-57 min full runs |
| OD-6 | Each measurement's class and its writer's home | per member, the dossier recommendation in 2.9, or another; and whether it leaves `//:update_generated` in Phase 7 (GENERATORS §6 step 6) or in Phase 8 | 2.9 |
| OD-7 | `catalog_corpus` and `pct:ratchets` | `catalog_corpus`: a test (G5) or test expectations kept as a generator (plan Phase 1 status). `pct:ratchets`: a bump record (G3) or a measurement (GENERATORS §5) | G5 #2, G3 #11 |
| OD-8 | The probe scripts | design D7 (delete; say how `FUNCTIONS_EXECUTED.tsv` is regenerated) or old D18 (keep as `bazel run` tools; `--record` refreshes the file) | 3.1, 3.2 |
| OD-9 | Process text that still says PRs | Update AGENTS.md "Pushing to main" (main lines 358-378: rule 2 "restored by P8-01", rule 3 "fix it in a PR") and drop old P8-01's restore of guardrail 2; or keep a PR path for some changes. Also the PR template (old P0-15) | plan header (no PRs); AGENTS.md is shared |
| OD-10 | Tag `//:update_generated` manual now, or in Phase 7 | now: `//...` stops building `//pct:ratchets`; Phase 7: as planned | Compile-2 |
| OD-11 | Which of the old Phase 6 guards stay in scope | per guard (Check-5) | old §5 |
| OD-12 | Old items never started: still wanted? | P5-02 CI image, P5-06 locale lane, P5-09 PowerShell lane, P4-10 flake soak, P4-13 packaged warehouse, P4-15 `bench/model` `py_binary`s, P5-08 weekly heavy suite | section 4 |
| OD-13 | TypeScript check: build action now, or keep the test for now | design §4.1 vs the 2026-10-05 "for now" | Compile-7 |
| OD-14 | How the Windows bash path goes | remove the need for bash; or rely on Bazel's own search, declared | Short-1, after CI-7 |
| OD-15 | `//core:server` ships `:test` and `:testdatagen` | keep, or move them out of the product (area1 C6, OPEN until reflective loads are ruled out) | `core/BUILD.bazel:223-224,288-295` |
| OD-16 | The PAR's entry-time test | add it, or rely on the PCT lanes | Short-7 |
| OD-17 | The `C:/bzl` output root | stays until OD-1 (b) lands, then re-measured | Short-11 |

---

## 4. The old workplan's items, mapped to Phase 8

So nothing is lost or done twice. "Done" items are on main (evidence: the commit named, or the log's batch entry).

| Old phase / item | Status (evidence) | Where it goes now |
|---|---|---|
| Phase 0, P0-01 … P0-15, P0-90 | Done (PR #19 `5a44c6554`, PR #21 `0662fed80`; workplan §6.5) | P0-14: closable (End-6) |
| Phase 1, P1-01 … P1-28, P1-90 | Done (batches 0-6, log; §6.5), except: | |
| — P1-02 (no `heavy` dispatch lane) | Partly (§6.5) | Test-11 |
| — P1-09 (gcc-less proof; libxml2) | Partly (§6.5) | CI-6 |
| — P1-12 (rules_graalvm) | Open (#602 OPEN) | Short-13, last |
| — P1-16 (`ServerRunfiles` in the server) | Done (`b0e7a86cd`) | **Reversed** by design D9 (Compile-4) |
| — P1-18 (engine server artifact) | Answered yes; no target yet | Exec-3 |
| Phase 2, P2-01 … P2-22, P2-90 | Done (batches 7-9; `9876ecf63` P2-22; `499338109` P2-04), except P2-21 | P2-21 → Meas-3 |
| Phase 3, P3-01 … P3-34 | Done (`git log` grep of each ID on main), except: | |
| — P3-06 | Held to the end (log) | Test-1 |
| — P3-25 | Uncommitted on exec | Exec-2 |
| — P3-32 | Held: blocked on rules_js (log; workplan amendment) | After Node-8; OD-1; Short-11, Short-12 |
| — P3-90 | Not done | End-4 |
| Phase 4: P4-01, -02, -03, -04, -06, -08, -09 (DataCube part), -11 (completeness half), -14, -15 (part), -16, -17, -18 | On exec only (2.7) | OD-3; Node-4; Compile-5 |
| — P4-05, P4-07, P4-10 | Not started | Node-4 (on the driver) |
| — P4-12 | Not started | Superseded by D9 (Compile-4) |
| — P4-13, P4-15 rest | Not started | OD-12 |
| — P4-90 | Not done | End-4 |
| Phase 5: P5-01 … P5-09, P5-90 | None started | P5-01 → Test-6; P5-02 → CI-6; P5-03 → CI-3 (keep the dispatch input); P5-04 → CI-2, CI-5; P5-05 → CI-4; P5-06 → CI-8; P5-07 → CI-7; P5-08 → Test-11; P5-09 → CI-9 |
| Phase 6 (§5): P6-00, -10, -11, -14, -16, -17, -19 | Done (commits in Check-5) | G19 retires with Node-8 |
| — P6-01 … P6-09, -12, -13, -15, -18, -20, -90 | Not started | Check-5; P6-03 → Test-7; P6-08 → CI-3; OD-11 |
| Phase 7: P7-10, P7-12 | Done (`a8a87b301`, `fb43cd0a8`) | — |
| — P7-01 … P7-05 | Not started; last (log) | End-1; OD-8 |
| — P7-06 … P7-09, P7-15 | Not started | End-2, End-3 |
| — P7-11 | Partly | Check-7 |
| — P7-13 | Not started | Test-12 |
| — P7-14 | Not started | End-3 (with PARK-13; partly gone with D8: `probe/Shadow` reads `TEST_UNDECLARED_OUTPUTS_DIR`) |
| — P7-16 | Not started | Check-6, with Compile-8 |
| Phase 8: P8-01 | Not started | End-5; OD-9 |
| Old §6.3 deferrals (remote cache; Linux manifest lane; FFM `chdir`; generated runfiles defaults; …) | Recorded | The runfiles-defaults and FFM rows depend on `ServerRunfiles`, which D9 removes: re-check them then |

And the reverse: each plan Phase 8 bullet's old items. Tests/checks by trigger: P3-06, P5-01, P6-*; D9: P1-16, P4-12,
P3-32, G15; D10: P4-11; the TypeScript cleanup: P4-17; Node out of the tests: P4-01 … P4-10, P4-17, P1-23, P1-24 (their
machinery retires), G19; CI: P5-*; parked work: Phase 4 and P3-25; carried shortcuts: section 2.8; the measurement
group: P2-21 plus design §4.2 E; renames: none (D11 and D12 are the design's).

---

## 5. Stale or contradictory statements (location → correction)

| # | Where | What it says | What is true |
|---|---|---|---|
| 1 | Old workplan §2 summary table (top of §2) | D17-D21 "OPEN" | All decided on 2026-10-03 (`151acdce6` for D17-D20; D21 (b) recorded the same day; D17 amended 2026-10-03 and 2026-10-04) |
| 2 | Old execution log, batch 12 ("Waiting on the user") | "P3-25 depends on D18 (still OPEN; …)" | D18 was decided 2026-10-03 (`151acdce6`), and re-confirmed by the log's own last entries |
| 3 | Plan §3 Phase 8 | "CI lanes from `//gates`, and caching (D14)" | No D14 in the design doc, ever; old D14 is the saved-query fixtures. Name the caching decision (OD-5) |
| 4 | Plan §3 Phase 8, carried shortcuts | Four PR #14 follow-ups open | Two done: `4f7448989` (gzip `run_shell`), `067973962` (LauncherTest text). Same in the memory note `legend-lite-pr14-windows-review.md` (written 2026-10-03) |
| 5 | Plan §3 Phase 8, carried shortcuts | The list is complete | It misses at least Short-11 to Short-24 (2.8) |
| 6 | Plan §3 Phase 8 | Lists D8, D9, D10, D5, D13, D15 | The design's other open decisions have no home: design D1, D4, D6 (the client), D7 |
| 7 | `docs/IN_FLIGHT.md` on main, line 10 | "The Bazel program (`docs/BAZEL_IMPLEMENTATION_PLAN.md`; …)" | That file is bannered "SUPERSEDED — 2026-08-06 … Do not act on it" (`git show origin/main:docs/BAZEL_IMPLEMENTATION_PLAN.md`); the program's plan is `docs/REBUILD_PROGRAM_2026_10_06.md` |
| 8 | AGENTS.md on main, "Pushing to main" (lines 358-378) | Rule 2 "restored by P8-01"; rule 3 "fix it in a PR" | No PRs since 2026-10-06 (plan header). Shared file: ask the user (OD-9) |
| 9 | Old workplan P8-01 | "restore D17's guardrail 2 (CODEOWNERS paths by PR)" | Conflicts with no PRs (OD-9) |
| 10 | Old runbook §A step 5 | "No PRs, no pre-push CI" | Since 2026-10-06: one full CI run on the branch before the push to main |
| 11 | Old workplan §8, row 6 | "the macOS CLT SDK files, copied and sha256-checked (D1)" | old D1 revised 2026-10-04: CLT declared and checked, nothing copied (P1-10 as built, `@host_cc`) |
| 12 | Old workplan §8 rows 1, 2, 5; G15 (P6-15) | Assume Node harnesses (`harness.mjs startServer`, `live-snap.ts`, `lite.test.ts`), `ServerRunfiles`, `make.mjs` | Superseded by no-Node and design D9; re-derive the end state and the G5/G15 allowlists |
| 13 | Old P5-03 amendment (L:P-5) | "Delete the `gates` dispatch input" | The user's throwaway-CI rule dispatches `gate.yml -f gates=<lanes>` (memory note, 2026-10-05); keep it, naming `//gates` suites |
| 14 | Old P4-12 | Serve and app as plain executables resolving defaults through `ServerRunfiles` | design D9 drops `ServerRunfiles` from the product |
| 15 | Old P2-06 / old D14 (a) | Saved-query records by a Node `js_run_binary` around `make.mjs` | design §4.2 E (records on a narrow server library plus a server test) and no-Node; settle with Meas-5 |
| 16 | Old P5-03 change list | "plus a final job `bazel test --config=ci //...` and `bazel build --config=ci //...`" | design §4.1: "CI's build lane builds the tiers, not `//...`" (Compile-1) |
| 17 | Design §2, §5a; area reports | Counts (1,162 targets, 13 browser tests, …) presented as the repo's | Taken on `bazel/exec` (`docs/build-inventory/BRIEF.md`, "Where"), which differs from main (e.g. `//datacube:browser` exists only there) |
| 18 | Design §5a, `//:java` row | 46 jars including checker-qual | §5b's step dropped checker-qual (Postgres 42.7.13 without it) |
| 19 | Design D13 | The client reaches "the reachability-metadata generator" through the warehouse test library | Phase 1 moved the generator to its own `:reachability_metadata_lib` (`warehouse/BUILD.bazel:406`), out of the test library (`:123-124`) |
| 20 | Design §4.6 vs §6 | "(decision D6)" vs D6 listed under "Decisions for the user" | The no-Node goal is decided; the client choice is not (3.2) |
| 21 | Design §5 step 8 | "(then a remote cache, Phase 5)" | Means the old workplan's Phase 5 (P5-04, §6.3), not the program's Phase 5 (Pure.java rows) |
| 22 | Design D7 vs old D18 | Delete the probes vs keep them as tools | Unresolved (OD-8) |
| 23 | Design §4.2 "Mechanics" | Diff tests grouped in `//gates:generated_upstream` and `//gates:generated_source` | GENERATORS §3: the seal's everyday test replaces the upstream records' diff tests (Phase 7); only a source group may remain |
| 24 | Design §4.3, last bullet | Describes heavy benchmarks excluded by `@Tag("heavy")` | Not yet so (Test-10): the exclusion is still by class name |
| 25 | Design §3 R2 (gen_dynafn "no core class at all"), R4 ("builds every generator a second time") | — | Both wrong per G6 §8 P10 (gen_dynafn read `EngineHandlers` and `Pure`; only vocab and an exec-configuration core were doubled); Phases 1-2 have since changed both |
| 26 | GENERATORS §5 | `catalog_corpus` and the PCT ratchets in the measurement group | Plan Phase 1 status reclassifies `catalog_corpus`; G3 classifies `pct:ratchets` as a bump record (OD-7) |
| 27 | GENERATORS §6 heading | "Windows in the end-of-rebuild PR" | No PRs; Windows proof by the branch's full CI run |
| 28 | `START_HERE.md` §3 | "Each has a brief here" (one per remaining phase) | Only `START_HERE.md`, `PHASE_3_LANDING.md` and `DEBTS_RESOLVE_AND_TYPE_ONCE.md` exist in `docs/build-inventory/program/` (2026-10-07); this file is the Phase 8 brief |
| 29 | `warehouse/BUILD.bazel:310-315` and `:92-93` comments | The server finds its files in runfiles; relative `--data` resolved by the server | True today; both change with D9 (Compile-4) |
| 30 | `MinimalCorpusTest.java:362-363` (per G2 §A5) | "a runaway is the lane's Bazel timeout" | Stale since P3-01: a pass is a build action, bounded only by the job's 90 minutes (G2 §A5) |
| 31 | `scripts/corpus/functions.py:42-43` | Its evidence file is written by `probe_functions.py` | That script cannot run under Bazel today (host JDK path; Short-22): the gate's remedy cannot be followed (design D7, second half) |

---

## 6. Homework before coding (what to measure, and how)

| # | Unknown | Why it matters | How to settle it |
|---|---|---|---|
| U-1 | Can a Java client use Chromium's pipe transport? | Design D6/§4.6 names `--remote-debugging-pipe`; Chromium reads fds 3 and 4 in that mode (Chromium's documented switch; **UNVERIFIED** here) and `ProcessBuilder` connects only stdin/stdout/stderr | Spike on 3 OSes: (a) `--remote-debugging-port=0`, read the `DevToolsActivePort` file in the profile directory, connect with `java.net.http.WebSocket` (in the JDK); (b) the pipe through FFM (`posix_spawn` with file actions; Windows handle inheritance). Measure start time and stability; pick (OD-4) |
| U-2 | Which UI handlers need trusted input | design §7; decides how much of the driver's input code is needed | Port one harness first (design §7); area5 E3 already shows `datacube/src` never checks `isTrusted`; hover, the browser's own drag-and-drop and hit-testing need trusted events |
| U-3 | DuckDB-WASM's browser build vs its Node build in tests | 13 test files and 4 harnesses use `blocking` + `NODE_RUNTIME` (area5 E2); `COPY TO` behaved differently in Node (old P4-16) | Port one DuckDB test (e.g. `upload.test.ts`) to a page first |
| U-4 | What still runs a shell on each platform | Short-1, old D20 | `bazel aquery` per platform (old P5-07), before and after Node-8 |
| U-5 | Windows path lengths without runfiles trees | Short-11, OD-1 | Run the Windows lanes manifest-only once rules_js tests are gone; record the longest path |
| U-6 | Does `bazel run` of a plain Windows executable keep `&` and spaces in arguments? | OD-2: whether `hermetic_launcher` is needed after D9 | A throwaway Windows run of `bazel run //warehouse:serve -- --port 'x&y z'` against a `native_binary`-style target (S2 E3's commands) |
| U-7 | Does analysing `//gates:local` on a cold machine download GraalVM, Chromium or the upstream archives? | design §7; the light gate's cost | `bazel cquery` on a fresh output base, then list `external/` (design §7) |
| U-8 | How much of the core compile is NullAway? | design D1 (Compile-8) | Clean `//core:server` with and without it, execution logs, quiet machine (design §7) |
| U-9 | Do worker bundles and query's tests import the whole app? | Test-13, design §7 | esbuild's metafile per entry (design §7; area6 B7) |
| U-10 | Do the corpus passes read `//core:srcs`? | Meas-1 (b) | From the source: no (G2 §A3). Confirm with one sandboxed pass without it (design §7) |
| U-11 | Does DuckDB autoload or install an extension during the corpus? | hermeticity of Meas-1 | One pass with network denied, grep the log (G2 §A3, OPEN) |
| U-12 | Sandbox setup time of the whole-tree generators | Gen-9 | Execution log of `gen_fixtures`, `gen_dynafn` |
| U-13 | Cache sizes and hit rates per lane and platform | OD-5 | `gh cache list` after a few runs; Bazel's cache-hit summary in each lane log |
| U-14 | Can `pct:ratchets` run without the properties only the tests pass? | design §7; Meas-3 | Read `PctRatchets` against the test configuration (design §7) |
| U-15 | Does `//core:server` load `:test`/`:testdatagen` classes at run time? | OD-15 | grep for reflective loads; run the server's tests without them on the class path |
| U-16 | Does `//spec:manifest_world_census` still fail? | Short-21, Meas-9 | `bazel test //spec:manifest_world_census` (manual, 4 GB), alone |
| U-17 | How hard is the `bazel/exec` rebase? | OD-3 | A trial rebase in a scratch worktree under `runs/`; count conflicts (13 overlapping files) |
| U-18 | Re-take the inventory counts on main | the design's numbers describe exec (section 5, #17) | `docs/build-inventory/build_inventory.py` on main |
| U-19 | Which esbuild setting removes the path comments | Compile-6 | Try each design option on the 19 outputs; compare bytes across strategies and platforms |
| U-20 | The B2 baseline today | Compile-9's exit measure | Rerun design §5a's `rdeps` query on main (495 at `1479486dd`) |

---

## 7. A sensible order (a proposal, written before the planning session; item 2's CI bullet became L1, §8; the rest stands as a proposal for L10 to L13)

The plan says Phase 8's items interleave with Phases 3b, 6, 4, 5 and 7 "where they do not touch the same files".
Those phases touch: core's compiler and builtins (3b, 4, 5), the corpus runner and parser (6), the prelude generator
and the system metamodel (4), `Pure.java` and its registries and generators (5), `tools/bump`, the root writers and
the seal (7). So:

1. **Decisions and homework first, no code:** OD-1 to OD-17; U-1 (the CDP spike plan), U-13, U-17, U-18. Announce
   Phase 8's file list in IN_FLIGHT on main.
2. **Independent landings, any time (none touches a compiler file):**
   - CI first, because every later landing pays for CI: lanes as `//gates` suites with the lane guard and the
     `postgres_live` answer (Test-6, -7, -8); the cache (CI-2, CI-5); actionlint (CI-4). Workflow changes: full CI.
   - The build lane builds the compile targets (Compile-1), with `manual` on hand tools and layer queries (Gen-5,
     Check-4) and on `//:update_generated` if OD-10 says now.
   - Checks: one whole-repo check, names-only inventory, the light gate stops analysing the repo (Check-1 … -3).
   - D8 (Compile-3, a small core edit). D9 with `warehouse_run`'s arguments, P4-14 and the servers' exit flag
     (Compile-4, Short-3, Exec-1): Windows proof. D10 and D5 with the completeness test (Compile-5).
   - Small items: Gen-6, Test-10, Test-4, Short-6, Short-7 (if wanted), Exec-2 (P3-25 finished, manual).
3. **No Node, in several landings:** the spike (Node-1) → the driver and the in-page runner on one harness, three
   OSes (Node-2) → the tests (Node-3) → the harnesses, carrying over exec's knowledge (Node-4) → servers and file
   checks (Node-5, -6) → the JS generators (Gen-2, -3, -4) → TypeScript as a build action (Compile-7) → remove Node
   (Node-8, -9) → then Short-1 (the bash path) and, per OD-1, Short-11/-12.
4. **After program Phase 6:** the corpus passes (Meas-1), `spec_tests_lib` (Test-2), `eager_corpus_compile` (Short-9).
5. **After program Phase 5, and with Phase 7:** the ratchets (Meas-3, with old P2-21), the ladder (Meas-2, or earlier
   if the user wants), the saved queries and `catalog_corpus` (Meas-4, -5), the writers' homes (OD-6); then the
   test-library splits and narrowing (Test-1, -3, -5, Compile-9).
6. **Last:** the script review and documents (End-1 … End-3), NullAway and the Error Prone policy (Check-6,
   Compile-8), the remaining guards (Check-5), the weekly heavy suite and test sizes (Test-11, -12), the CI image and
   the PowerShell lane if still wanted (CI-6, CI-9), rules_graalvm (Short-13), the renames (Rename-1, -2), and the
   final audit (End-5).

Sizes known from the old workplan (engineer-days): P3-06 5; P5-01 1; P5-02 2; P5-03 2.5; P5-07 1; P5-09 1; P4-12
1.5; P4-13 1.5; P7-01 … P7-05 about 6; P7-06 … P7-09 about 4.5; P7-14 3; P7-16 L. No estimate exists for the no-Node
port: make one after the spike (U-1, U-2).

---

## 8. The CI landing (L1), as agreed with the user on 2026-10-07

**What it is.** The build lane builds the product and nothing else; every lane is a `//gates` suite with one name, one
command and the same members on every platform; a guard holds that every test is in some suite; the browser harnesses
are Bazel tests; the downloads are cached per platform; the heavy and hand targets leave the wildcard. Two landings:
**L1a** (the structure and the cache: workflow and BUILD files only) and **L1b** (the harnesses as tests: the parked
`bazel/exec` commits P4-02, P4-03, P4-04 and P4-08 rebased, the Linux-only tests constrained; `datacube/`, `query/`,
`site/`, `studio/` files, which Studio's line owns: noted in IN_FLIGHT, then proceed). Each: an audit agent, one full
CI run, a push to main.

**The lanes.** Every lane runs `bazel test //gates:<name>` on Linux (`ubuntu-latest`), macOS (`macos-14`, one test JVM
at a time) and Windows (`windows-2022`); `linux-arm` runs `warehouse` only; `product` runs `bazel build`. Tests that
run only on Linux (the Chromium harnesses: "the same on every platform", and live-vs-snap deliberately checks
x86_64) declare `target_compatible_with = ["@platforms//os:linux"]` and are skipped by the suite elsewhere.
Minutes are estimates, Linux / macOS / Windows; L1a's own run replaces them.

| Lane | Members (exact) | min |
|---|---|---|
| `product` | `bazel build //:java //:web //:wasm //:native //:sites //datacube:app`; then `bazel build --nobuild --config=bazel10 //...`; the A25 cross-platform analysis of `//warehouse //pct //datacube`; the lane guard (`bazel query 'tests(//...) except tests(//gates:lanes) except tests(//gates:heavy)'` is empty: a query's `//...` includes manual targets, so every test is in a lane or in `heavy`) | 6–7 / 6 / 8–9 |
| `core` | `//core:core_tests` (24) `//core:duckdb_load_test` `//core:section_grammar_registry_test` `//core:corpus_differential_test` `//core:planner_on_java_base_test` `//core:postgres_arm_test` (kept with core, as the local gate had it) `//spec:spec_tests` `//json:tests` `//pure-protocol:twins_test` `//tools/engine-runner:smoke_test` | 4 / 3 / 4 |
| `checks` | `//:generated` `//projects:tests` `//tools/deps:all` `//tools/guards:classpath_test` `:compile_only_test` `:inventory_test` `:locks_test` `:markdown_inputs_test` `//tools/junit:pins_test` `:runner_test` `//tools/java_run:pins_test` `//tools/python:lock_matches_requirements` `//tools/browser:revision_test` `//tools/bump:bump_test` `//tools/js:lock_matches_package_json_test` `//scripts/corpus:density_gate` `:executed_gate` `:stacking_gate` `:scoreboard_gate` `:functions_gate` `//core:guardrails` `//core:census` `//pct:pct_discipline` (its home; `//pct:pct_duckdb`'s own suite also holds it) + new `//tools/guards:workflows_test` (actionlint and shellcheck from pinned archives, replacing `gate.yml`'s `curl` job); `tools_build_test` (every hand tool compiles) comes with L1b, whose packages it reaches | 10 / 8 / 13 |
| `corpus_duckdb` | `//spec:corpus_duckdb` (host and database pass, verdict, roster diff tests; gates 4 and 11) | 7 / 5 / 7 |
| `corpus_h2` | `//spec:corpus_h2` (gate 5) | 6 / 6 / 5 |
| `pct_duckdb` | `//pct:pct_duckdb` (five suites, and PCT's discipline guard inside it; gate 6) | 6 / 6 / 4 |
| `pct_h2` | `//pct:pct_h2` (gate 7) | 5 / 3 / 3 |
| `pct_postgres` | `//pct:pct_postgres` (five suites; gate 7P) | 6 / 6 / 6 |
| `pct_channel_b` | `//pct:pct_channel_b` (five suites; gate 9) | 5 / 4 / 4 |
| `parser_equivalence` | `//parser-equivalence:parser_parity` (gate 8) | 7 / 7 / 5 |
| `stress` | `//core:stress_suites` `//core:stress_suites_h2` (gate 10; the H2 suite's 292 serial seconds are the lane's floor until it is split per suite, L12) | 8 / 8 / 4 |
| `warehouse` (also linux-arm) | `//warehouse:tests` `:sqlapi_wasm_build_test` `:tests_native` `:launcher_test` (builds the image itself: the one duplicate left) + `:postgres_live` `:postgres_live_native` once a throwaway run shows they pass (Test-8) | 6 / 6 / 7 |
| `datacube` | `//datacube:tests` (the Node tests, typecheck, bundle budget, `live_snap_test`, `verify_app_test`) `//datacube:verify_smoke_test` + the nine harnesses as tests (Linux-only): `run_stress`, `verify_charts`, `verify_cubes`, `verify_features` (sharded by section), `verify_page`, `verify_real_data`, `verify_remote`, `verify_upload`, `verify_wasm_browser`; `//wasm:differential_test` `//wasm:zone_test` (the planner in WebAssembly, which DataCube runs in the tab; kept out of `core` because of the TeaVM build) | 10–11 / 7 / 8–10 |
| `ui` | `//studio:tests` `//query:tests` `//query-store:local_test` `:share_test` `:lite_test` + (Linux-only) `//studio:verify_test` `//query:verify_test` `//site:verify_test` | 8 / 3 / 5 |
| `sdlc` | `//sdlc-server:git_repository_test` `//sdlc-client:tests` `//depot-client:tests` (the model home: SDLC and Depot, server and page) | 4 / 3 / 4 |
| `heavy` (manual; a weekly scheduled run, Test-11) | `//spec:reference_lane` `//spec:update_reference_lane_test` `//spec:corpus_warehouse_verdict` `//spec:manifest_world_census` `//parser-equivalence:diagnostics` `//pct:update_ratchets_test` `//tools/python:requirements.test` | — |
| `local` (`//gates:local`, not a job) | `core` + `checks` + `warehouse` (its JVM tests) + `datacube` + `ui` + `sdlc`, without the Linux-only and the native-image tests: what a session runs before pushing, as today | — |

15 jobs per platform, the same names everywhere; every test target in the repository (337 non-manual on 2026-10-07,
7 manual) has exactly one home, so there is no "misc" and the guard is strict. Expected wall clock: Linux ~11
(`datacube`), Windows ~13 (`checks`), macOS ~15 (fifteen lanes through the five-at-a-time queue): **about 15
minutes**, against 40 to 57 today.

**What is gone from CI.** `bazel build //...` as a lane; the `lint workflows` job (`curl`); the Chromium install step;
the shell loop of `bazel run` harnesses and their fixed ports; the jq list of hand-typed targets (the keys stay for
`-f gates=`, now suite names); the output-cache tarballs and their 2–3 minute saves (only `~/.cache/bazel-repo` is
cached: one key per platform and pin hash, restored by every lane and saved by the product job once it has fetched
everything, since `actions/cache` never adds to an existing key). **Built by no wildcard any more (`manual`):**
`//:update_generated` (and through it `//pct:ratchets`) and the hand tools (`//tools/census:render_census`,
`:lanes_diff`, `//tools/junit:compare_testcases`, `//wasm:startup`); the product job analyses them by name. The
judge passes and the layer queries stay as they are: the lanes' tests depend on them, so a tag would change nothing.

**Not L1's, with its landing:** `checks`' generator builds (L6 deletes `gen_natives`, `gen_claims` and `core_next`;
L8's seal takes the upstream generators out of `//:generated`; L12 carves the measurements: if `//:generated` still
dominates after L1's run, `generated` becomes its own lane, one line once suites exist); the H2 stress suite's serial
five minutes (L12); the Node tests' 14 minutes on Windows in `datacube` (L11); the native image built twice (the
remote cache, OD-5, decided in L10 with L1's hit rates); `core_tests_integration` at 173 s on Windows (sharding; third
order).

---

## Appendix: commands used for this brief (all read-only)

- `git -C <the main checkout> log --oneline origin/main..bazel/exec`; `git -C runs/bazel-exec status
  --short`; `git -C runs/bazel-exec diff`.
- `git diff --name-only 3faa7d291 bazel/exec` and `git diff --name-only 3faa7d291 origin/main`, intersected.
- `git log --oneline -G"ctx.actions.run_shell" origin/main -- warehouse/defs.bzl` (→ `4f7448989`);
  `git log --oneline -S"takes a number, not" origin/main -- warehouse/` (→ `067973962`).
- For each revision of the design doc: `git show <c>:docs/BUILD_REBUILD_DESIGN_2026_10_05.md | grep -c D14` (all 0).
- `gh cache list --limit 100 --json key,sizeInBytes,lastAccessedAt,createdAt`;
  `gh run list --workflow gate.yml --limit 12`; `gh run list --workflow gate.yml --event schedule`;
  `gh api repos/neema2/legend-lite/codeowners/errors`; `gh pr view 602 --repo sgammon/rules_graalvm`.
- Per old item ID, a grep of `git log --format='%h %s' origin/main` and of `origin/main..bazel/exec`.
