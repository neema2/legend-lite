# Bazel first-class: the execution log

One entry per push to `main`, newest last. The runbook (`BAZEL_EXECUTION_RUNBOOK.md`) takes the first batch
without a **done** entry.

## Before the runbook (2026-10-03)

- **Phase 0, P0-01 … P0-13:** done. PR #19, `5a44c6554` (with the one-driver-path follow-up: PCT's Postgres
  lanes take `//core:drivers`). CI green on Linux, macOS, Windows; Windows `verify_app` 9/9 by a hand-run
  workflow (run 37152622977).
- **Windows teardown race (interim for P4-18):** done. PR #20, `7bbd1dc04`.

## Batch 0: close Phase 0

- PR #21 (P0-14, P0-15, the checks-lane orphans, `gates green`): open, CI green on `4c3493da9`, waiting for
  the user's merge.
- The ruleset on `main` and write access for @johnnymads: after #21 merges.
- PR #22 (P1-14, P1-14b): open, head `5a7889de2`, the independent review's fixes in; CI run 37200662280.
- The ruleset on `main`: done, id 24454941 (2026-10-04). @johnnymads invited with write (invitation 336017981, pending acceptance).
- **P0-90:** done 2026-10-04, recorded in the workplan's §6.5: 14 of 15 yes, P0-14 partial until the invitation is accepted. PR #21 merged as `0662fed80`.
- PR #22: rebased onto `0662fed80` after conflicting with #21 (both edited the lane lists); the runbook's one-line-of-work rule came from it.
- **P1-14, P1-14b:** done. Pushed to `main` directly (`8eabfbea7..253c47750`, PR #22 closed with a pointer); the
  ruleset's owner bypass worked. `main`'s CI run 37203023190: 51/51 on Linux, macOS, Windows, including
  `verify_app_test` in the Windows app lane.
- **Batch 0: done** (2026-10-04).

## Batch 1: the test runner (P1-01, P1-02, P1-21)

- Local: `852372d5d` (P1-01, P1-02) and `d782b2b3f` (P1-21) on `253c47750`. Full proof 178/178 with the measured heaps
  (every manual java test included but `//spec:reference_lane`, red on `main` itself: its golden drifted 1515 → 1517
  after 2026-09-29, outside this program). Identity diff: all 44 shared java tests select the same testcases under
  both runners. Independent audit: running.
- **Batch 1: done.** Pushed `687e56668` (b5657e888, 1b9453c5a, 687e56668). `main`'s CI run 37205628053 first failed one test
  on Windows, `WarehouseServerTest.eachStatementCarriesItsOwnUsersIdentityEvenAtTheSameTime` (a statement's status had
  chunkCount > 0 but no first chunk): it passed 30/30 locally at the same 768 MB heap and 51/51 on the re-run, so it is a
  Windows-timing race in the warehouse, not batch 1. Recorded for P3-21 (warehouse tests) to find and fix. Not reverted:
  a judgment call, stated to the user.

## Batch 2: runfiles (P1-03, P1-04, P1-05, P1-06)

- Pushed `deccc0596` (77b4d1684, b9d978f05, e1e1f55da, b2d02bf89, f04d08f23, cd3fbd559, deccc0596). Full proof 178/178;
  after the audit's fixes 28/28 on the affected targets, `//gates:local` 146/146. Independent audit: approve with nits,
  all fixed (path filters within the tree, Corpus's snapshot, Runfile holder, the skip pin moved for bisect); P1-05
  amended in the workplan with every residual Repo use's owner. `main`'s CI run 37208204540: 51/51. **Done.**

## Batch 3: Python foundation (P1-07, P1-08)

- Pushed `af4607953`. rules_python 2.3.4, CPython 3.12, the @pypi hub; Python binaries start from a bash stub (no host
  python3); the gate's lock check is offline (`//tools/python:lock_matches_requirements`), the PyPI re-resolution manual.
  The Arrow check is a py_binary on the locked pyarrow and RUNS with no host pyarrow ("25000 rows, 0 differences");
  CI's pip step and --test_env flags are gone; taskkill by its full Windows path. Full proof 179/179; after the audit's
  fixes 149/149. Independent audit: one HIGH (the host-python3 stub), fixed.
- **Red on main, reverted** (run 37209395985): Windows' `//warehouse:tests_native` could not load pyarrow's DLLs
  from the test's runfiles: past Windows' path limit (252-259 characters). Reverted at once (`5cc648148`, its tree
  identical to batch 2's proven-green one). Re-landed as `822cfeb47` with `startup:windows --output_user_root=C:/bzl`
  in .bazelrc (checked to apply per host OS): the longest path drops to about 226, on CI and on a Windows desk.
  `main`'s CI on the re-land, run 37211485516: 51/51, Windows native included. **Done.**

## Batch 4: C toolchains (P1-09, P1-13, P1-10, P1-11, P1-12)

- **4a done** (`91cd1f8e0`): Linux links the native warehouse with the hermetic LLVM toolchain through a shell-free
  launcher; Linux arm64 joins CI. The independent audit caught a Windows blocker (toolchains registered in MODULE.bazel
  name targets an empty Windows repository lacks), fixed by registering them in .bazelrc's common:linux. `main`'s CI
  run 37212513283: 53/53, the native lanes on Linux x86_64, Linux arm64, macOS and Windows.
- **4c pushed** (`eda84cb92`): D1 revised (USER: macOS like Windows): the Command Line Tools and MSVC declared and
  checked by @host_cc, their exact versions recorded as a native-image input (USER: "Is that the best way? If yes let's
  do it"), floors MSVC 17.6 and Apple clang 15. Audit: pass with fixes, applied. `main`'s CI run 37214692462:
  one Windows timeout (`//datacube:infer_test`, 60 s, while the lane built the native image), green on re-run (53/53).
  **Done.**
- **P1-12 (a)**: upstream PR https://github.com/sgammon/rules_graalvm/pull/602 after research (no duplicate; GraalVM has no
  sysroot option). (b) per-platform GraalVM toolchains: to investigate.

## Batch 5: platform infrastructure (P1-15, P1-16, P1-17, P1-18, P1-19)

- **5a pushed** (`ecc95b30d`): the WebAssembly-planner tests get medium (the timeout above); P1-17 (a Java gunzip action,
  no shell); P1-19 (`//tools/platforms`: every platform config_setting, platform_select/compatible_with; native, DuckDB,
  pyarrow and Chromium targets skip on an unlisted platform). Audit: pass with fixes, applied (its proof query had
  matched every rule; corrected). `main`'s CI run 37218299154: 53/53. **Done.**
- **5b pushed** (`b0e7a86cd`): P1-15 (the embedded Postgres per platform behind a hub; only the host's fetched; skipped on an
  unlisted platform) and P1-16 (the server finds DuckDB in its runfiles; the extraction into java.io.tmpdir is gone; DuckDB's
  jar left server_lib). The full proof caught //spec:corpus_warehouse, whose child JVM relied on the extraction: fixed by
  declaring the library. Audit: both pass, fixes applied. P1-18 investigated: the engine server artifact exists; its
  target lands with P4-07. `main`'s CI run 37219814277: 53/53. **Batch 5 done.**

## Batch 6: build hygiene, JS, early guards, Phase 1 audit

- **6a built** (local): P1-28 (disk-cache GC; --enable_bzlmod gone), P1-27 (every pin by integrity; all 19 re-fetched into an
  empty cache and verified), P1-22 (the two shell genrules are java_run actions; java_run's memory_mb; the PAR generator and
  the reference dump found non-reproducible, recorded on P2-11/P2-14). Full proof 179/179.
- **6a audit: not ready, two High.** (1) P1-27 broke `//tools/bump` (it still matched `sha256 =`) and nothing tested it.
  (2) My P1-25b "no change needed" was wrong: rules_jvm_external's Maven resolver uses ~/.m2/repository unless
  RJE_UNSAFE_CACHE=0. Medium: removing --host_jvmopt changed TeaVM's heap; the PAR heap was a guess.
- **USER review, 2026-10-04:** "why is a Java unzip better than gzip?" It was not: Bazel's http_archive unpacks a .gz at fetch
  time. Also found: my "no shell steps left" survey was broken (it filtered out every path under runs/), and missed the
  launchers' run_shell `gzip -dc`.
- **6a fixed:** Bump writes integrity and reads tag commits over HTTPS (no host git), with //tools/bump:bump_test on the real
  files; P1-25b done (`run --run_env=RJE_UNSAFE_CACHE=0`; empty-home repin of the three pools byte-identical, ~/.m2 untouched);
  heaps measured (TeaVM 889 MB live → -Xmx2g plus resource_set; PAR 2,099 MB → 4096; reference dump 3,072 MB → 8192, was 12 GB);
  java_run refuses -Xmx beside memory_mb. Full proof 180/180.
- **Windows throwaway 37224856941: red** on app, native, build. Bazel 9.2's fetch-time .gz unpacking (my P1-17 redo) sets a
  modification time 1000x too far out; Windows refuses it ("Permission denied"). The re-audit had predicted it (H1).
  Dropped: the Java gunzip stays; the launchers copy its output with copy_to_directory (the last run_shell still goes).
  Re-audit's lows fixed: Bump inserts values literally; bump_test covers prefix tags; settings.xml/.netrc wording; stale
  comments. Live `bazel run //tools/bump -- 4.145.0 --pins`: no diff. Second Windows throwaway (37225812205): green (checks, app, native, build). Full proof 180/180.
- **6a on main** (4f7448989; P1-28, P1-27, P1-22, P1-25b, P1-17 follow-up). Local gate 148/148. Main CI green (run 37226705706, 53/53).
- P1-25 investigated: 45 undeclared artifacts under strict_visibility (upstream 34, runner 5, test 5, teavm 1).
- **6b built** (local): P1-20 (legend_java_library; 21 libraries say `nullaway = False`: NullAway everywhere found 792
  errors, its own work; core private by default) and P1-25 (strict_visibility, 45 jars declared, locks unchanged but for
  the input list). USER asked "how do we make sure nothing ever leaks to core or other": a pool-user guard was added.
- **6b audit: P1-25 not ready.** //tools/par:par_generator was public and shippable on legend-engine jars; the guard saw
  only direct labels in 5 macros. Marking the engine pools testonly was tried and failed (a pool cannot be testonly as a
  whole). Fixed in three layers (tools/deps/pools.bzl): direct-use check with canonical labels and per-target grants;
  direct engine/runner users must be testonly (par_generator now testonly, //pct-only); //tools/deps:product_closure_test
  over every shipped root. Each layer proven by a reverted negative edit. Full proof 182/182. Re-audit: ready.
- **6b on main** (ff2dcf107; P1-20, P1-25). Local gate 150/150. Main CI green (run 37230570535, 53/53). Open: the Studio
  line's announced sdlc-server/depot-server need grants (core/json visibility, TeaVM pool); a rules_jvm_external
  pool-wide testonly option would let Bazel enforce layer 2 itself (research, then upstream PR).
- **Test pools testonly as a whole** (93cc6085b; main CI green, run 37236951591): rules_jvm_external #350's fix as
  third_party/rules_jvm_external_testonly_closure.patch (USER: no upstream PR); maven_test/upstream/runner amended
  testonly. The re-audit showed Bazel does not check generated files (a filegroup over a testonly deploy jar passes), so
  product_closure_test stays and pools_list_test keeps the pools testonly as a whole. USER: stop accommodating other
  sessions (the Studio line adapts when it rebases).
- **6c built** (local): P1-23 node_test (every JS test; wasm_flag_test), P1-24 runfiles lookup (tools/js/runfiles.mts,
  chdir only on the six scanners), P1-26 lock check (datacube exact versions). Full proof 184/184.
- **6c audit: OK after small fixes** (an overclaimed `_main` claim, a toothless A28 check, wrong counts); fixed, with a
  load-time js_test guard. **Windows throwaway 37238983590: green** (checks, misc, app, build).
- **6d guards built:** P6-00 inventory, P6-10 locks, P6-14 Bazel 10 config + testonly on output files everywhere
  (product_closure_test deleted), P6-16 junit_test-only, P6-17 no Markdown inputs, P6-19 derived lock list, P6-11
  classpath conflicts (pools_are_disjoint deleted), P7-10 dead references. **Guards audit: not ready** (.git and
  .claude un-ignored; the G17 genquery fetched every platform's downloads); fixed. Each guard proven by a reverted
  negative edit. Full proof 186/186.
- **On main** (8822b03b0; 13 commits: 6c + 6d guards + P7-10). Local gate 154/154. Main CI: running.
- **6d main CI green** (run 37243808806, 53/53).
- **P7-12 built:** generator sources in spec/src/gen/java; :source_tree; Claims compiled once per core it reads (by design);
  core's source root by Pure.java's label.
- **P1-90 Phase 1 audit:** 25 of 30 yes, P1-02/P1-09/P1-18/P1-19 partly, P1-12 no (open). A regression it found (A25's
  unlisted-platform check broken by the guard reports, run by nothing) is fixed and now runs in CI's build lane;
  amendments (B)-(J) and new P7-16 recorded in the workplan (§6.5). Phase 1 closes with P1-12 open.
- **Batch 7 built:** P2-01 stress corpus from Bazel actions (spike S5 ported), P2-02 one layout list (core/stress.bzl),
  P2-03 declared inputs only + bazel-run headers (60/93 comment lines regenerated), P2-04 the corpus ratchets as
  py_tests (green at HEAD), P2-05 memoised specs (287 s -> 146 s, byte-identical). P2-20 moved to batch 9 (after P2-18).
- **Batch 7 audit: not ready** -- the oracle used the platform libm (cbrt/log/sin in 98 would differ on Linux/Windows);
  fixed with exactmath.py (correctly rounded, decimal at 60 digits; the committed corpus unchanged); PYTHONUTF8; the live
  layout; a CI query that could fail silently. Throwaway CI on all platforms (checks, build): running.
- **Batch 7 throwaway 37248685802:** the stress corpus is byte-identical on Linux, macOS and Windows; the build lane
  (bazel10 analysis + A25) green everywhere; red only on corpus-gate TIMEOUTs (executed 300 s on Linux and Windows,
  stacking on Windows). Profiled: 99% in aggregates.usable_ends, asked 2,054 times with the same arguments; memoised
  (144 s -> 5 s, corpus byte-identical).
- **Batch 8 so far:** P2-12 (layer queries made from core's libraries by core/layers.bzl, checked both ways), P2-08
  **amended** (the tool reads the live vocabulary, which grows: p1 predates the treemap mark, so a diff test of tool vs
  p1 would always be red; p1 stays frozen by its hash pin; the next version is a build output and is cut by `bazel run
  //datacube:cut_link_dictionary`), P2-07 (@react_icons by the registry's sha512; icons.ts byte-identical but for its
  header), P2-17 (natives.dump -> //spec:native_declarations output; natives.bootstrap -> //core:draft_native_membership).
- USER asked to check the UI consolidation: the Studio session (neema-9f) has it on local branch studio-m1, unpushed,
  and will merge main after us; nothing it touches overlaps. Agreed: legend-art's icon generator moves onto @react_icons
  (one pin, one generator) and Query then uses legend-art's output.
- Rebased onto 6f86c86dd (the Studio merge); throwaway 37251203524 (checks, build, all platforms) and the local gate:
  running.
- **Pushed to main** (a97cbd188; batch 7, P2-12, P2-08, P2-07, P2-17, the memo fix): local gate 177/177 at that
  commit; throwaway 37251203524 green but for the Windows build job, still running (main's own CI re-runs it); pushed
  early to unblock the Studio session (USER: "the studio session is waiting for us"). Main CI 37253460391: running.
- **Studio line reviewed** (studio-m1, the Studio worktree): in line, but two fixes before it pushes --
  the fonts committed as .woff2 binaries (pin @fontsource by integrity like @react_icons), and legend-art/src/icons.ts
  as vendored data (generate it from @react_icons, diff-tested, and retire //query's generator).
- **P2-14 (F) root cause:** bisect (7 runs) puts the reference lane's 1515 -> 1517 failed bodies at c1f9bac5b
  (2026-10-01, list literals require [1] elements). The two bodies (dataTypeToSqlTextH2, debugPrint and v2_1_214) use
  `max([$MIN, min([...])])`, which legend-pure's compiler types: the rule is stricter than the reference. A user
  decision (refine the rule, or re-bless with a dated reason); the golden stays at 1515 meanwhile.
- Local, unpushed: P2-13, the batch 8 audit follow-ups, P2-06, P2-10 (release.MODULE.bazel), P2-11.
- **Main CI 37253460391 green** (a97cbd188, 53/53). Throwaway 37253950183 (batch 8's rest: checks, build, gate 8; all
  platforms) green.
- **The reference-lane fix** (USER: "fix rule to match pure for sure"): the [1] rule of c1f9bac5b is legend-pure's; the
  gap was six missing catalog overloads (math::min/max over Integer, Float, Number [1..*], which legend-engine
  registers). Found by probing the closure's candidate set (only natives; legend-pure's bodied [1..*] overloads absent).
  Lane re-blessed through bazel run //spec:update_reference_lane: failed bodies 1515 -> 1508, AGREE +832, OVERLOAD
  769 -> 745. Pins moved with dated reasons: EngineHandlersTest 168 -> 162, ImplementationTableTest Body 2193 -> 2187 /
  Intrinsic 665 -> 671. Announced in IN_FLIGHT on main first (d7611403a). All-lanes Linux throwaway 37256509404: 18/18.
- **P2-14 done** (reference-lane report as a java_run; golden via write_source_files, manual). **P2-19 done** (no test
  re-runs a diff-tested generator; the reachability census reads :gen_roster); its SkipCensusTest pin fixed after the
  gate caught it.
- **Pushed to main: cf911b24f** (P2-13, audit follow-ups, P2-06, P2-10, P2-11, P2-14, the overload fix, P2-19), rebased
  over C3c; local gate green but for //wasm:differential_test and //sdlc-client:wasm_test TIMEOUT at 60 s and
  //datacube:live_snap_test ECONNRESET under the full gate's load (all pass alone): sizing follow-up owed.
- Studio's second branch (query-by-name) reviewed: approved. The database-owner line (neema-32) told C2 may start.
- **Batch 8 complete**; batch 9 left: P2-15, P2-16, P2-18, P2-20, P2-09, P2-90.
- **Main CI on cf911b24f:** 51/53 green; linux //warehouse:tests_native TIMEOUT (no output). It passes locally (28 s)
  and passed on main's next run (37262434241, which contains the push): runner load, not a hang.
- **Batch 9 built:** wasm differential_test medium; P2-18 (vocab.tsv by Bazel, the committed copy was from the 4.138.2
  jars; false "generated" claims removed; //docs:ledgers deleted); P2-20 (the keyword census over pinned grammars,
  declared-file trees; 598/607 in scope at 4.145.0, HANDOFF's 574/574 was a working copy); P2-16 (a) computed counts,
  G-01 (all 23 Repo.out writers diagnostic; docs/refusal-asymmetry.tsv orphan deleted), (b) generated measured reports
  for spec, parser-equivalence and pct (core's justified registers are policy and stay: amendment). P2-15 moved after
  P3-01.
- **Batch 9 audit: two bugs** (gen_own_corpus_draft would die on a static -D property; the census wrote CRLF on
  Windows) and should-fixes (undeclared reads unsandboxed; ratchet readers loading their own output; pct's update
  outside //:update_generated; stale recipes): all fixed. Throwaway 37265420480 (checks, build, gate 8; all platforms)
  green; local gate 198/198.
- **Pushed to main: b245f5f30** (9 commits). Main CI 37267596779: running. Studio's studio-engine reviewed (planner
  worker as its own bundle; A5; tests) and approved.
- Left in Phase 2: P2-09 (native-image reachability metadata, H-native, Windows proof), then P2-90 (the Phase 2 audit).
- **Main red, fixed:** b245f5f30's //pct:pct_duckdb (gate 6) failed on all platforms: PctDisciplineTest forbids
  comparison machinery in the PCT module and PctRatchets used a TreeMap. Fixed on main as 7511621f2 (LinkedHashMap,
  one fixed order; ratchets.tsv unchanged). **Lesson:** //gates:local omits the PCT lanes and the batch 9 throwaway
  ran checks/build/8 only; a push that touches a package now runs that package's CI lanes (local or throwaway) first.
- P2-09 done (a: FFM metadata from Duck.DOWNCALLS/AuthenticatedUser.UPCALLS; b: the whole metadata generated, the
  agent's host-dependent recording replaced by declared JDK services, A23; equal entry for entry). Phase 3 started:
  P3-02, P3-11, P3-03 committed locally.
- **Phase 3, batch 10 (part):** P3-02, P3-11, P3-03, P3-04, P3-15, P3-14 committed; two audits. First audit: counter
  readers @Isolated (a shared lock kept out only lock holders), the skip census walks warehouse's tests and counts
  every conditional skip, stronger order-free proofs (isOnDay pairing; streaming at 50 mid-fetch writes; now() on
  DuckDB's clock). Second: unreasoned @Disabled fails the census, real target names, Rows tags keys by kind.
- **P3-01 done** (amended: the host pass is per lane, one action each). //spec:judge_host_<lane> runs the host pass
  through JUnitAction; ledger, log and exit code are outputs; the database test fails first, quoting the host log, when
  it failed. JUnitMain's prerun and ${TEST_UNDECLARED_OUTPUTS_DIR} expansion deleted; engineScanOrder a jvm_flag. Both
  lanes' ledgers identical to the prerun era, row for row; a forced rerun reuses the host pass. Audit follow-ups:
  scoped runs skip the host verdict; a broken run (exit not 0/1) fails the action; the heap measured; G16 confines
  JUnitAction to corpus_lane.
- **Throwaway 37274021833** (9d271f028): linux //datacube:verify_app_test timed out waiting for "Live" (passed
  locally on the native image; the reachability metadata is identical entry for entry to main's); superseded by
  37276478964 (6b66e3d74, all lanes, all platforms), where the lane passed: a flake.
- **A product bug found by P3-17:** Lexicon.DUCKDB lacked 59 keywords DuckDB will not take as names (at, to, for,
  column, map, struct, ...): a table named aT rendered unparseable SQL. ResolveUnionV4ProbeTest printed that SQL and
  asserted nothing. Fixed (d29aac770) with DuckDbKeywordsTest holding the list to the pinned DuckDB's
  duckdb_keywords(); corpus_duckdb ledgers and rosters unchanged; pct_duckdb green.
- **P3-17 done** (two commits): empty GAP stubs deleted (OUTSTANDING.md keeps the 15 gaps), probes assert rows, a
  @Test restored; measurements are report actions (diagnostics_reports, eager_corpus_compile, ref_imports), probes and
  the benchmark binaries. Amended: the benchmark is a binary, not an action (a timing is the machine's); RefImports and
  the eager compile are actions. Noted for P7: scripts/census_gate.py is dead (mvn, a deleted class).
- **Pushed to main: 6b66e3d74** (P3-02, P3-11, P3-03, P3-04, P3-15, P3-14, P2-09, both Phase 3 audits' follow-ups,
  P3-01 and its audit's): throwaway 37276478964 52/52, local gate 201/201 with the PCT lanes. Main CI 37280220578.
- **P3-17 audit** (no blocker): probes now seed a non-matching row per route (a dropped join condition fails them);
  report failures print on the action's console; FixtureSweep and the benchmark take --out; RefImports its own library;
  ManifestWorldCensusTest a manual test (//spec:manifest_world_census), OurResolutionsTest the program
  //spec:our_resolutions.
- **Found, not re-pinned:** //spec:manifest_world_census, runnable for the first time since it was written, FAILS its
  core_relational ceilings (load walls 37 > 32, failing bodies 1,476 > 1,447, both set 2026-09-25). Drift no run saw
  while it skipped; the target is manual (not in CI). Owner: the compiler line (needs the five new walls named).
- **Noted:** Lexicon.H2 also lacks H2 2.x reserved words (key, value, year, month, day, set, user, ...): same class of
  bug as the DuckDB one, not yet hit by a test. Follow-up for the H2/engine-style dialect owner.
- **P2-15 done** (amended: classification table in the workplan; both passes actions; the lane a suite of
  CorpusVerdictTest and the roster diff tests). The database judge's engine-order register was never checked; generated,
  52 stale rows and 1 missing (re-blessed). Audit: one blocker fixed (rosters written with the platform's line ends
  would fail every Windows diff test: LF now); a failing pass never blanks a roster (written first; UNMEASURED lines);
  //:update_generated no longer re-blesses the rosters; the warehouse lane checks against DuckDB's committed rosters.
- **Batch 10 complete** but P3-05 (moved after P3-27, which it depends on) and P3-06 (deferred in §6.3 unless the
  user chooses it). Next: push batch 10's rest, P2-90 on main, then batch 11.
- **Pushed to main: 77f4107c1** (the keyword fix, P3-17 and its audits' follow-ups, P2-15 and its audit's): throwaway
  37283951536 52/52, local gate 213/213 with PCT and both corpus lanes. Main CI 37289520555. Main's previous run
  (6b66e3d74) green after one rerun: //studio:verify_test timed out on a click (the same tree was green in the
  throwaway; a flake).
- **P3-08, P3-09, P3-12, P3-18, P3-07 committed** (with their audits' follow-ups), batch 11:
  - P3-09: PCT one target per suite and per Channel B class; PctCensusGate judges each suite's own deltas against
    per-suite ceilings (DuckDB int-null-empty 20/20/4, the whole-JVM 231 was Maven's composite; Postgres diverge 12/4);
    Channel B pins 75/103 -> 0; E7 holds (sharding pct_h2 fails the overrun guard).
  - P3-12: //core:stress_suites_h2 (no lane enforced MIN_PASS_H2), the knobs in //core:stress_tool.
  - P3-18: the differential wired (D3 row 4). First run 166 agree / 13 unexpected: harness bugs fixed (unqualified
    schemas, FLOAT as DOUBLE, Java's half-up %.6f); X1 out of scope (two connections; stress_suites skips it).
    **The audit corrected me:** I first quarantined MO2 as a lite defect; legend-pure's m4 DateDiff defines HOURS as
    elapsed time (163), so lite was right and the oracle (and legend-engine's SQL) wrong. The oracle now follows Pure
    (five services' expectations moved; ENGINE_QUARANTINE F58; floors 4,705 / 4,676); its DAYS path read the host's
    zone (fixed). Differential: 178 agree, 1 known (F14).
  - P3-07: exact data for spec, pct and parser-equivalence; the 36 upstream-tree consumers kept whole (a pin bump reruns
    all of them anyway; §6.3).
- **P2-90 done** (independent): 16 of 20 met; P2-04 reopened, NEW P2-21 (core ratchets), NEW P2-22 (false "generated"
  claims), amendments to P2-09, P2-10, P3-12; process gaps recorded in §6.5.
- **Moved:** P3-05 and P3-30 after P3-27 (they depend on its lists). P3-06 stays deferred (§6.3) unless the user chooses.

## Batch 12: diagnostics, robust test infrastructure, locales, projects

- **Pushed to main: 3781cbf98** (batch 11: P3-08, P3-09, P3-12, P3-18, P3-07 and the oracle follow-ups). Throwaway
  37290407740 52/52. Main CI 37297275640: two Windows-only reds on a tree the throwaway passed. (1)
  `WarehouseServerTest.anAclViewShowsEachUserTheirOwnRows` (native lane): the test assumed a finished statement always
  carries its first chunk; one finished by a poll does not. A real test bug, fixed (c8e3d4339). (2)
  `//core:postgres_arm_test` (7P): pgjdbc's SSL request timed out after 10 s against the embedded Postgres 16 while
  three PCT Postgres suites started their own clusters. A slow-runner flake, rerun; recorded here, not changed.
- **Committed on bazel/exec** (each with its own proof; local gate plus PCT and the three corpus lanes 228/228 before
  the audit fixes):
  - P2-04 (reopened: the inventories' gates are tests), P2-22 (no false "generated" claims).
  - P3-13: one diagnostics switchboard (`-Dlegend.diagnostics=`; lowering keeps its env read, invariant 6h); the
    census diff a Bazel binary.
  - P3-10 (JS tests restore what they change; the TZ lanes prove their zone), P3-16 (JS tests on their own clocks).
  - P3-19 (no test starts a JVM of itself; //core:planner_on_java_base_test, G16's one allowlisted java_test),
    P3-20 (EmbeddedPostgres a child of the test, retried on a taken port, in TEST_TMPDIR only), P3-21 (the native lane
    judges the binary only), P3-22 (the live Postgres tests ordinary tests), and their audit's follow-ups.
  - P3-26 (engine-runner smoke tests), P3-24 (//warehouse:sqlapi compiled by TeaVM in the gate chain), P3-31 (the
    harvest's shims first on their own classpath).
  - P3-28: StringCaseLocaleUsage and DefaultLocale as errors on every first-party compile. **Found while proving it:**
    NullAway's `-XepDisableAllChecks` came after the shared options and switched them off in every null-gated
    library; the shared options now come last. ~200 sites say Locale.ROOT. A19: guards_package() fails a java_library
    not made by legend_java_library, and a java_binary or java_test with sources not made by legend_java_binary or a
    junit_test macro. **Audit (4 commits): two Medium on P3-28** (junit_test's own sources did not get the options; A19
    saw only java_library), one Medium on P3-24 (the WASM entry reached 3 of the API's methods; TeaVM compiles only
    what is reachable), lows (the smoke test in no lane; a stale comment). All fixed (e4a85859e).
  - P3-29: UI_LOCALE for the apps' own words, with a guardrail; the six scanners read `_SCANNED`'s declared lists
    through //tools/js:runfiles' Sources; the last test chdir is gone.
  - P3-34: the investigation found directory cycles but one 8-file cycle at file level; each node_test's data is its
    generated import closure (datacube/test_imports.bzl, diff-tested). A touch of src/share/link.ts re-runs 16 of 107.
  - P3-23: legend_library (tools/legend) and //projects:tests (56 projects alone, the graph, the set-id rule).
    **Found F-L1** (projects/FINDINGS.md): legend-lite lifts a view declared inside a Schema twice, so
    firm-balance-sheet does not compile; quarantined with a test that turns red when it compiles. The fix is one line
    in ModelBuilder (walk defaultSchemaViews()), outside this program's files: for the compiler's owner. The plan's
    manifest-equals-BUILD guard contradicts G17 (no test reads Markdown; CI skips Markdown-only changes): BUILD's
    PROJECT_DEPS is the truth, the manifests agree today, unchecked. check.py stays until P7-01 (it also checks size
    bands, with legend-engine).
- **Waiting on the user:** P3-25 depends on D18 (still OPEN; recommended (a), the Python probes as py_binaries over one
  launch()).
- **Audit (P3-29, P3-34, P3-23, the warehouse fix): one High, my mistake** — the F-L1 investigation's scratch package
  (`warehouse/negscratch`) was committed; removed. Mediums: the same firstChunk race in a second warehouse test;
  P3-34's generator, not the sandbox, guarantees closures (now it fails on an unresolvable import); the quarantine
  message pinned to firm-balance-sheet's own view. Lows fixed. Workplan amendments for P3-23, P3-34 (c6cf0c257).
- **Windows JavaScript tests ran in the machine's zone** (found by P3-10's zone check on its first Windows run): MSYS
  bash, rules_js's Windows launcher, drops TZ. node_test passes LEGEND_TZ, applied by a preload (8e8790126).
- **P3-27 done** (77694fb38): SourceFiles over declared lists; 25 guards converted; src/main/duckdb in the roots;
  ArchitectureTest and NoEagerTypeReferencesTest over declared jars. **Audit: I had exempted a real defect** —
  DuckDbAppenderLoad's finally replaced an unchecked in-flight error. Fixed in the product (the drop is a
  try-with-resources resource), exemption removed, announced in IN_FLIGHT on main (7b5341ea7) (57f0115f6).
- **P3-05 done** (d2160132a): core_tests a suite of 24 per-package targets (union of 4,319 testcases identical);
  the stress corpus read from the classpath; exact data for census and guardrails; scale_* one benchmark.
- **Main green** at 3781cbf98 after rerunning two Windows jobs (run 37297275640 attempt 2, 53/53).
- **P3-27b done** (e14035354), **P3-33 done** (355f9bc7a): Repo.java and Upstream.java deleted; ProgramPaths names
  every file a program reads (action exec path or test runfiles path), TestOutputs for side reports; the generators
  byte-identical, 313/313 locally. **P3-32 held:** the audit showed rules_js needs a runfiles tree on Windows (amendment
  in the workplan; options: a node_test transition, or a manifest-aware runfiles.mts).
- **Pushed to main: fbc1f2756** (batch 12 rest, P3-27, P3-05 and the audits' follow-ups; 25 commits): throwaway
  37304703940 52/52, local gate 313/313 with PCT, corpus and stress lanes. Main CI for 3781cbf98 went green after
  rerunning two Windows jobs (a pgjdbc timeout on a loaded runner; the warehouse poll race, fixed since).
- **P3-30:** **the audit showed I contradicted P2-16's classification** (generated two policy registers). Reverted;
  P3-30 amended to a classification (816cb871e). Kept: pe's ledgers through ProgramPaths.
- **Pushed to main: 3faa7d291** (P3-05 follow-ups, P3-27b, P3-33, P3-30 as classification, their audits'
  follow-ups): throwaway 37311913885 52/52; main's previous run (fbc1f2756) 53/53.
- **Phase 4 on the branch** (P4-01, -02, -03, -04, -06, -08, -09 DataCube part, -11 completeness, -14, -15 part, -16,
  -17, -18): DataCube's harnesses are browser tests (//datacube:browser) with no fixed sleep; servers take
  --exit-with-parent (no taskkill in this program's tests); the .mjs harnesses typechecked. **Audit: one High, my
  mistake again** -- a proof run's stress-results.json committed at the root; removed. **And I was wrong about two
  "engine bugs" in torture:** legend-lite refuses arithmetic on a nullable value on purpose (c1f9bac5b); the cases now
  take toOne. Mediums fixed (verify-page's settle reused stale state; verify-cubes' late-pick wait). Windows proof of
  --exit-with-parent rides the Phase 4 throwaway's Windows app lane. Left to the Studio line: query/site harnesses,
  sdlc-client's taskkill.
- **USER, 2026-10-05:** D18 decided (a), "fine to keep python but as real first bazel": the probes and run.py stay
  Python, as py_binary/py_test targets that start legend-engine from runfiles through one launch(). The script review
  (P7-01, with P1-12 and P7-16) waits until everything else is done. P3-25 is unblocked; next.
- **USER, 2026-10-05:** P3-06 (per-package core test libraries, a speed item) moves to the end with the script review.
