# Coverage check: workplan vs audits 05, 06, 07, spikes S1–S5 and the script review

**Checked:** `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` (read in full), against `05-harnesses-query-site.md`, `06-scripts-docs-ci.md`, `07-bazel-files.md`, `spikes/S1`–`S5`, and `script-review.md` (the rows flagged "writes committed files consumed by tests", the 11 rows decided in D3, and the D3b list). Context: `docs/BAZEL_FIRST_CLASS_PLAN_2026_10_02.md`. This was a read-only check: nothing was built or run, and no repository file other than this one changed.

**What the verdicts mean**

| Verdict | Meaning |
|---|---|
| COVERED | A named item fixes the finding fully, and that item's Proof or Done-when would show it. |
| PARTIAL | An item touches the finding, but something is left. The table says what. |
| MISSING | No item fixes it. |
| DEFERRED-OK | The workplan defers it explicitly with a reason, or the user decided it (D1–D16). |

I was strict about the user's goal: "everything fully Bazel first class: no non-Bazel orchestration (no shell, no scripts, no hand recipes, no CI shell logic), no host dependence, no hacks". An item that names a finding but leaves the file, the shell or the host step in place counts as PARTIAL, as in the known `Repo.out` case.

---

## 1. Summary

### Counts per source

| Source | Items checked | COVERED | PARTIAL | MISSING | DEFERRED-OK |
|---|---|---|---|---|---|
| 05 harnesses, query, site (table rows, §1 notes, duplicates, N1–N18, browser plan, K items) | 68 | 58 | 9 | 0 | 1 |
| 06 scripts, docs, CI (§1 inventory rows, §2 CI, §3 every doc instruction, the missing-script table, §4) | 159 | 144 | 9 | 3 | 3 |
| 07 Bazel files (N1–N23, K items, the test-target table, the generated-file table) | 101 | 89 | 11 | 0 | 1 |
| S1 test runner | 23 | 21 | 1 | 1 | 0 |
| S2 runfiles and launchers | 49 | 39 | 4 | 0 | 6 |
| S3 hermetic C toolchain | 41 | 28 | 5 | 0 | 8 |
| S4 pinned Chromium | 41 | 31 | 3 | 0 | 7 |
| S5 stress generator | 35 | 27 | 1 | 0 | 7 |
| script-review (flagged rows, the 11 D3 rows, D3b) | 24 | 16 | 5 | 1 | 2 |
| **Total** | **541** | **453** | **48** | **5** | **35** |

Some gaps show up in more than one source and are counted once in each. For example, the `keywords.py` repair is missing in 06 §1c and again at script-review D3 row 6. After merging those, the PARTIAL and MISSING verdicts reduce to the **33 distinct gaps** in §2.

### The 10 most important gaps

1. **`testing/Repo.java` survives** (07 N18, S2 "Tests", 06 §1b on `differential.py`). P1-05 and P3-32 keep `Repo.out` ("stays, or moves into `Runfile`"), so the file survives. The ~25 test classes that write through `Repo.out` are never listed, and nothing sorts them into diagnostic output (fine as it is) or goldens refreshed by unzipping CI output by hand (each must become an action with a diff test).
2. **`PctDisciplineTest` drops out of every gate.** This is an AGENTS.md load-bearing guard. Today it runs only inside the composite `//pct:pct_duckdb` (`pct/BUILD.bazel:96-98`) and in the `manual` `//pct:pct_discipline`. P3-09 deletes the composite and lists only the suite classes, and P5-01's gates exclude manual targets. G3 (P6-03) would notice, but only months later.
3. **The rules_graalvm sysroot patch adds a shell.** It works through a `/bin/bash -c` `run_shell` (S3 E7 aquery). P1-09 ports that into `third_party/rules_graalvm_sysroot.patch`, and P1-12's Done-when lets the patch stay for good ("or carries a link to the upstream PR"). That leaves a new shell action plus a vendored patch on a third-party rule.
4. **D3 row 6, "keep and repair `keywords.py` (+ `tiers.py`)", has no item.** P2-18 handles only `vocab.tsv`. No item makes `keywords.py` read the `.g4` grammars from `@legend_engine_src` instead of `~/legend/legend-engine`, or makes the tracked metric a target. P7-03's list leaves it out.
5. **The G9 docs guard misses most of the recipes 06 §3 lists.** P6-09's regex matches `mvn|npx|npm install|java -jar|python3 x.py|node x.mjs|*.sh`. It does not match `java -cp … perf.TokenDump`, `javac`, `curl`, `duckdb -c`, `pip install`, `cd … &&`, `git switch`, `diff`, direct `bazel-bin/…` runs, `--sandbox_writable_path` or `gh release upload`. So docs can pass G9 with the very recipes 06 §3 found. `scripts/parser/HANDOFF.md:84` (the `vocab.tsv` recipe) is in no Phase 7 item.
6. **Harnesses still find the site through `import.meta.url` path arithmetic.** 05 §1: every harness uses `ROOT = fileURLToPath(new URL('..', import.meta.url))`, and `verify-features.mjs:84` reaches `ROOT/../fixtures/saved-queries` the same way. No Phase 4 item converts this. G15's pattern `new URL\('\.\./` does not match `new URL('..', …)`, so the guard misses it too.
7. **Manual targets have nothing that runs them.** `scale_*`, `corpus_warehouse`, `reference_lane` and P2-14's own manual diff test `update_reference_lane_test` stay in no lane (07 K18). Nothing ever checks the reference golden for drift. P1-02's identity diff covers only non-manual lanes, so the runner change is never verified on those (S1 Q0).
8. **`chaos` is filed as an engine harness, and the engine harnesses have a host-dependent fallback.** `chaos` needs only `//core:server` (05 table). P4-07 puts it in the bucket that depends on P1-18, so it stays a `js_binary` if P1-18 finds no engine artifact. P1-18's "no" branch also leaves `verify_engine*` and `verify_calc_vocabulary`'s engine half depending on a hand-started engine on :6300. The workplan defers this, but it never tries 05's suggested fix, the pinned shaded jar.
9. **D10 conflicts with the items that implement it.** D10 chose (c), no mirror. But P1-27's Change, Proof and Done-when still describe the D10 (a) release-asset mirror, P2-10 still has Bump print "the D10 mirror upload commands", and §6.2 still maps "Mirror the Linux sysroots" to P1-27. Separately, `--incompatible_stop_exporting_language_modules` (07 N7(c), N22) is neither guarded nor in the §6.3 upstream-deferral list.
10. **The pinned CI image (P5-02) is not specified as runnable.** It names no bazelisk and no non-root user, yet P3-20 makes `initdb` fail as root and GitHub container jobs run as root by default. D7's fallback "recorded exception" (`libxml2`) has no recording place (README or Done-when). P5-03's literal step `bazel test --config=ci //gates:<lane>` drops today's `--repository_cache` flag (`gates-run.yml:146`), and no item moves it into `.bazelrc`. Then every CI run fetches the ~120 MB Chromium archive again (S4 download risk).

---

## 2. PARTIAL and MISSING items, with proposed fixes

IDs `G-01`…`G-33` are this check's own. Each fix is either an amendment to an existing item or a new item, with Change / Proof / Done when.

| # | Source | Finding / row | Sub-point | Verdict | What is left | Proposed fix (Change / Proof / Done when) |
|---|---|---|---|---|---|---|
| G-01 | 07 N18 and gen-file table; S2 "Tests" (Java); 06 §1b `differential.py` | Test-written goldens; `Repo` resolver | `Repo.out` stays (P1-05, P3-32), so `testing/Repo.java` survives. The "~25 test classes write via `Repo.out`" are not listed or classified. | PARTIAL | Repo.java not deleted. No inventory of `Repo.out` writers. Any golden among them that is still refreshed by unzipping `outputs.zip` stays a hand recipe. | **Amend P3-32.** Change: move `out(String)` into `testing/.../TestOutputs.java` (reads `TEST_UNDECLARED_OUTPUTS_DIR`, fails if unset); rewrite every `Repo.out` caller; `git rm testing/src/main/java/com/legend/testing/Repo.java`; G15 (P6-15) bans `\bRepo\.` outright. **New sub-step in P2-16, first:** a table of every `Repo.out` writer (`git grep -l "Repo.out"`), classed as diagnostic or golden. Each golden gets a `java_run` plus `write_source_files`, as P2-13/P2-14 do. Proof: `git ls-files testing \| grep -c Repo.java` prints 0; `git grep -n "Repo\."` is empty; the classification table is in the PR. Done when: no `Repo` class exists, and no committed file is refreshed from test outputs by hand. |
| G-02 | 07 test table (`//pct:pct_discipline`, manual); AGENTS.md (load-bearing `PctDisciplineTest`) | Discipline guard placement | P3-09 deletes the composite `pct_duckdb`, which is the only non-manual target selecting `PctDisciplineTest`. | PARTIAL | After P3-09, a load-bearing guard runs in no gate until G3 lands (P6-03, step 13). | **Amend P3-09.** Change: drop `manual` from `//pct:pct_discipline` and add it to `test_suite(name = "pct_duckdb")`. Proof: `bazel query 'tests(//pct:pct_duckdb)'` lists `//pct:pct_discipline`; `bazel query 'attr(tags, manual, //pct:pct_discipline)'` is empty. Done when: every class `AGENTS.md` names as a guard is selected by a non-manual target in `//gates:all`. |
| G-03 | S3 "What changed" item 3 and E7; S3 rec 3 and Q4 | rules_graalvm sysroot passthrough | The sysroot branch runs native-image through `/bin/bash -c '…--sysroot=$PWD/…'` (a `run_shell`). P1-12 allows the patch to stay for good. | PARTIAL | A new shell action in our patch, against standing rule 1. A vendored third-party patch can persist with no end condition. | **Amend P1-09 and P1-12.** Change (P1-09): build the absolute sysroot without a shell, through rules_graalvm's own `ctx.actions.run`. Options: native-image's `@argfile` written by `ctx.actions.write` with the sysroot made absolute by a `-H:CCompilerOption=--sysroot=` path native-image resolves against `-H:Path`, or a ten-line `java_binary` launcher in the exec configuration; **investigate first** which native-image option accepts a relative path. Change (P1-12): the upstream PR carries that shell-free form. Add a removal deadline: if it is not released by the next rules_graalvm minor, record the patch in §6.3 with the upstream PR link. Proof: `bazel aquery 'mnemonic("NativeImage", //warehouse:server_native)' --output=text \| grep -c /bin/bash` prints 0. Done when: no native-image action runs a shell, and the patch is gone or listed in §6.3 with its PR. |
| G-04 | 06 §1c (`keywords.py`, `tiers.py`); script-review D3 row 6 (decided **keep and repair**) | Keyword-coverage metric | P2-18 makes only `vocab.tsv` a `java_run`. `keywords.py` still reads `~/legend/legend-engine/**.g4` (SR row) and is in no item. P7-03's list leaves it out. | MISSING | A tracked metric depends on a host checkout, with no target. | **New item P2-20.** Change: `scripts/parser/BUILD.bazel` with `py_library(tiers)` and `py_binary(keywords)`. Data: `@legend_engine_src//:grammars` (a new narrow filegroup of `**/*.g4` in `third_party/legend_engine_src.BUILD`), `//tools/engine-runner:vocab` (P2-18's output) and `//projects:srcs` plus `//core:stress_sources`. A `run_binary` writes `keyword-coverage.tsv`, a `write_source_files` golden (a measurement, D9), and a `py_test` asserts any dated floor. Proof: `bazel test //scripts/parser:all`; `git grep -n "legend/legend-engine" scripts/parser` is empty. Done when: the metric is produced by Bazel from pinned inputs and diff-tested. |
| G-05 | 06 §3 (every instruction); P6-09 (G9) | Docs guard coverage | G9's regex misses `java -cp`, `javac`, `curl`, `duckdb -c`, `pip install`, `cd … &&`, `git switch`, direct `bazel-bin/…` runs, `--sandbox_writable_path` and `gh`. | PARTIAL | G9 can pass while the recipes 06 §3 found remain. | **Amend P6-09.** Change: the pattern list becomes `\b(mvn\|npx\|npm (install\|run\|test)\|pnpm (install\|run)\|pip3? install\|java (-jar\|-cp)\|javac\|python3? \S+\.py\|node \S+\.m?js\|duckdb -c\|curl\|wget\|git switch\|gh (run\|release))\b`, plus `^\s*cd \S+ &&`, `bazel-bin/\S+` used as a command, and `--sandbox_writable_path`, all inside code spans or blocks. Add a fixture test: one sample line per 06 §3 bullet must fail. Proof: `bazel test //tools/guards:docs_test` (fixtures included). Done when: every recipe class in 06 §3 has a failing fixture. |
| G-06 | 06 §1c row `tools/engine-runner/vocab.tsv` (cites `scripts/parser/HANDOFF.md:84`) | Doc instruction | `HANDOFF.md:84`'s `java … perf.TokenDump > vocab.tsv` is in no P7 item. | MISSING | A hand recipe for a committed file stays in a current doc. | **Amend P7-08.** Change: add `scripts/parser/HANDOFF.md:84` → `bazel run //tools/engine-runner:update_vocab` (P2-18), or move the file to `docs/history/` if it is history. Proof: as P7-06. Done when: as P7-07. |
| G-07 | 05 §1 intro; 05 table row `verify_features` (`:84`) | How harnesses find the site | `ROOT = fileURLToPath(new URL('..', import.meta.url))` in every harness, and `ROOT/../fixtures/saved-queries`. No Phase 4 item converts this. G15's regex `new URL\('\.\./` misses `'..'`. | PARTIAL | Layout-dependent path arithmetic stays in every harness. The guard is blind to it. | **Amend P4-01 and P6-15.** Change (P4-01): `harness.mjs` exports `siteRoot()` and `fixture(name)` over `@bazel/runfiles`, from `env = {"SITE": "$(rlocationpath :site)", "SAVED_QUERIES": "$(rlocationpath //fixtures/saved-queries:records)"}` set by `browser_test`. Every harness drops its `ROOT`. Change (P6-15): the pattern becomes `new URL\(\s*['"]\.\.?(/\|['"])` plus `fileURLToPath\(new URL`. Proof: `git grep -n "new URL('\.\." -- datacube/demo query/demo site` is empty; `bazel test //tools/guards:runfiles_test`. Done when: no harness computes a path from its own location. |
| G-08 | 07 K18 (`corpus_warehouse`, `scale_*` in no lane); 07 test table (`reference_lane`, `diagnostics`); P2-14 (`update_reference_lane` tagged manual); S1 Q0 (heavy and manual lanes never identity-checked) | Manual targets | Manual targets and one manual diff test are run by nothing. P1-02 checks only non-manual lanes. | PARTIAL | `scale_*`, `corpus_warehouse` and `reference_lane` can rot silently. The reference golden's diff test never runs. The new runner is unverified on these lanes. | **New item P5-08.** Change: `gates/BUILD.bazel` `test_suite(name = "heavy", tests = [scale_*, corpus_warehouse, reference_lane, update_reference_lane_test, engine_stress, diagnostics], tags = ["manual"])`. A new `.github/workflows/heavy.yml` with `on: schedule` (weekly) plus `workflow_dispatch`, whose one step is `bazel test --config=ci //gates:heavy` (G8-shaped). **Amend P1-02:** run that workflow once on the PR branch and on main, and compare with `compare_testcases`. Proof: the scheduled run's log; `compare_testcases` output is empty for the heavy lanes. Done when: every manual target has a runner, or a dated reason why it has none. |
| G-09 | 05 table row `chaos`; 05 N4 | Shape of `chaos` | `chaos` needs `//core:server` on :8080 (it probes it at `:58`), not legend-engine. P4-07 makes it depend on P1-18 ("if no artifact, these stay `js_binary`"). Its env knobs `ENGINE`, `CHAOS_ROUNDS`, `CHAOS_SEED` and `tolerant()`'s swallowed errors are not addressed. | PARTIAL | `chaos` may stay a `js_binary` with a hand-started server. | **Amend P4-07.** Change: `chaos` becomes `browser_test(tags = ["manual"])` unconditionally, with `startServer(//core:server)` on port 0. `CHAOS_SEED` and `CHAOS_ROUNDS` become fixed per target (seeded variants are separate targets). `ENGINE` goes. Remove `chaos` from the P1-18 condition. Proof: `bazel test //datacube:chaos_test`. Done when: `chaos` needs no hand-started process. |
| G-10 | 05 table rows `verify_engine`, `verify_engine_differential`, `verify_calc_vocabulary` (engine half); P1-18 | Engine-backed harnesses | P1-18's "no" branch defers them with a recorded reason. It does not try 05's suggested fix: "engine jar from a pinned `http_file`, started in the test on a free port". | PARTIAL | A "no" from P1-18 leaves three harnesses on a hand-started engine on :6300 (host dependence). | **Amend P1-18.** Change: before answering "no", check for a published shaded server jar (for example `legend-engine-server-http-server` with the `shaded` classifier at the pinned 4.145.0) and pin it with `http_file` plus `sha256`. Start it through a `java_binary` wrapper over `@bazel_tools//tools/jdk:current_java_runtime`. Proof: as P1-18. Done when: either a pinned target starts the engine on port 0, or §6.3 records that no released artifact exists, with the URL checked. |
| G-11 | 05 N10 (sleeps and wall-clock windows); 05 rows `verify_features` (`:76` 30 s per-check deadline; 74 `waitForTimeout`), `verify_page` (300 ms window), `verify_charts` (400 ms window), `chaos` sleeps | Flake sources | P4-02's risk says "replace them … where they flake; P4-10 soaks". That is reactive. The 30 s per-check wall-clock deadline is not mentioned at all. | PARTIAL | Fixed sleeps and wall-clock deadlines stay in tests until they flake. | **Amend P4-02 and P4-04, and add a guard row to G13 (P6-13).** Change: replace every `waitForTimeout(n)` and fixed `setTimeout` sleep in `browser_test` harnesses with an awaited condition (`waitForFunction`, `locator.waitFor`). Remove `verify-features.mjs:76`'s per-check deadline; Bazel's `timeout` bounds the run. Guard: no `waitForTimeout(` in `datacube/demo/**`, `query/demo/**` or `site/**` outside a dated allowlist. Proof: `git grep -c waitForTimeout -- datacube/demo query/demo site` prints 0 or only allowlisted lines; `bazel test //tools/guards:test_discipline_test`. Done when: no harness asserts on, or waits for, wall-clock time. |
| G-12 | 05 "Harnesses that duplicate each other" | `verify_upload` overlaps `verify_features` and `verify_smoke`; `verify_picker` repeats `verify_cubes`/`verify_features` flows; `verify_calc_vocabulary`'s local half duplicates `calc.test.ts:166-168` | No item decides these three overlaps (the other three are merged by P4-01, P4-03 and P4-07). | PARTIAL | Duplicate browser time and duplicate assertions (low). | **Amend P4-06.** Change: an investigation table. For each overlap, keep the unique checks and delete the duplicates. `verify_calc_vocabulary`'s local half folds into `calc.test.ts` if fully duplicated. Proof: the PR's table; `bazel test //datacube:browser`. Done when: each overlap is merged or justified in a BUILD comment. |
| G-13 | 06 §1b row 16 `probe_*.py` (`probe_functions.py --record` rewrites `docs/FUNCTIONS_EXECUTED.tsv`); script-review D3 row 2; P2-18 | Writer of a committed TSV | P7-03 revives `probe_functions.py`, the writer of `FUNCTIONS_EXECUTED.tsv`. P2-18 calls that TSV a "frozen snapshot … no generator exists". | PARTIAL | The two items contradict each other. The `--record` path would write a committed file outside any action (a hand recipe). | **Amend P2-18 and P7-03.** Change: `FUNCTIONS_EXECUTED.tsv` is a *draft* refreshed by `bazel run //scripts/corpus:probe_functions -- --record`, written to `$BUILD_WORKSPACE_DIRECTORY` (the P2-17 draft pattern). Its header says "refreshed by …", not "generated", and G1's allowlist carries it with a date. P2-18 drops it from the "no generator" list. Proof: `bazel run //scripts/corpus:probe_functions -- --help`; G1 passes. Done when: the TSV's header names its refresh target, and no item calls it generator-less. |
| G-14 | 06 §1c `scripts/projects/check.py`; script-review D3 row 5 (decided: **interim `py_test`**) | Interim projects gate | P7-03 makes `check.py` a repaired `py_binary`. No item creates the decided interim `py_test`. | PARTIAL | The 45 never-compiled projects stay ungated until P3-23 (L, 5 d). | **Amend P7-03.** Change: `py_test(name = "projects_check", srcs = ["check.py"], data = ["//tools/engine-runner:testable", "//projects:srcs"])` in `//scripts/projects`, deleted by P3-23. Proof: `bazel test //scripts/projects:projects_check`. Done when: the test runs in `//...` until P3-23 replaces it. |
| G-15 | 06 §1c `tools/native-axes.py`; script-review D3 row 8 | Repair scope | P7-03 says "on `@legend_*_src`". The optional input `core/target/lowering-coverage-probe.txt` (SR option 2: "emit the probe as a `java_run`") is not addressed. | PARTIAL | One input still points at a Maven `target/` path. | **Amend P7-03.** Change: a `java_run` `//core:lowering_coverage_probe` emits the probe file, passed as `data`. Drop the `core/target` default. Proof: `git grep -n "core/target" tools/native-axes.py` is empty. Done when: the script reads only declared inputs. |
| G-16 | 06 §1c `tools/upstream-drift.py` | Host tools | P7-03 keeps "host checkouts optional through an argument". The script shells out to `curl` and `git ls-tree`. | PARTIAL | Host `curl` and `git` stay. | **Amend P7-03.** Change: replace `curl` with `urllib.request` and `git ls-tree` with reads of the `@legend_*_src` trees, for both the pinned side and the target side (pass the target tag as a second `http_archive` fetched by a `bazel run` repository-env flag, or read the GitHub tree API through `urllib`). Proof: `git grep -n "subprocess\|curl\|git " tools/upstream-drift.py` is empty. Done when: the tool spawns no host process. |
| G-17 | 06 §1c `tools/untangle/move_classes.py`; script-review D3 row 9 (decided: wire, **delete its dead `pom.xml` handling**) | Wire, minus dead code | P7-02 wires it, but its Change and Done-when do not delete the `pom.xml` handling. The tool also runs host `git mv`. | PARTIAL | Dead code stays. Host `git` is undeclared. | **Amend P7-02.** Change: delete the `pom.xml` branch (lines 35-56 region); keep `git mv` and say so in the tool's header (a repository-editing codemod run with `bazel run` from `BUILD_WORKSPACE_DIRECTORY`, like Bump). Proof: `git grep -n "pom.xml" tools/untangle` is empty. Done when: as stated. |
| G-18 | 06 §3 `datacube/bench/model/README.md:66-69` (`duckdb -c`, `python3 widepivot.py`); D3b (`bench/model/*.py`: "`py_binary` … or history") | Fate of 8 scripts | D3b was decided "as listed", but the listing itself offers two choices. P4-15 repeats the "or". | PARTIAL | Undecided. The host `duckdb` CLI recipe stays either way until someone picks. | **Ask the user (one line), then amend P4-15.** Recommended: history (P7-04) unless a benchmark is cited by a present-tense doc; otherwise `py_binary`s on `@pypi//duckdb`, with `bench.sql` run through the Python API, not the CLI. Proof: `bazel build //datacube/bench/model:all` or `git ls-files datacube/bench/model/*.py` is empty. Done when: one fate is recorded in `script-review.md`. |
| G-19 | 06 §1e `repro/` ("duplicates the concept of `scripts/corpus/repro/`") | Two repro trees | No item decides whether the two trees merge. | PARTIAL | Two places for one concept (low). | **Amend P7-08.** Change: move `scripts/corpus/repro/**` under `repro/` (or the reverse), with one README convention (`bazel run //scripts/corpus:probe_* -- …`). Proof: `ls scripts/corpus/repro` fails. Done when: one repro tree exists. |
| G-20 | 06 §4.5 (`docs/{OUTSTANDING.md, WALL_DEPTH.txt, SCOREBOARD.md}`); P2-18 ("history (P7-04)"); P7-06 (AGENTS.md: only `:39-40` may change) | Moving files AGENTS.md links | `AGENTS.md`'s standing-documents table links `docs/OUTSTANDING.md` as "History". Moving it under `docs/history/` breaks that link, and P7-06 forbids editing that line. | PARTIAL | A contradiction between P2-18/P7-04 and P7-06. | **Amend P2-18.** Change: files that `AGENTS.md` names stay in place. Only their "generated" claim is removed. P7-04 skips them. Proof: `git grep -n "docs/OUTSTANDING.md" AGENTS.md` still resolves (`test -f docs/OUTSTANDING.md`). Done when: no AGENTS.md link breaks. |
| G-21 | 07 N7(c), N22 (`--incompatible_stop_exporting_language_modules` fails on the patched `rules.bzl`) | Bazel 10 flag | P1-10 only investigates. If the flag still fails, it is in neither G14's `bazel10` config nor §6.3's list of upstream deferrals. | PARTIAL | A known Bazel 10 failure can be forgotten. | **Amend P1-10 and §6.3.** Change: P1-10's Done-when adds "the flag passes and joins `build:bazel10` (G14), or §6.3 lists it with the rules_graalvm issue". Proof: `bazel build --nobuild --incompatible_stop_exporting_language_modules //warehouse:all`, or the §6.3 row. Done when: the flag is either guarded or tracked. |
| G-22 | 07 N14 (public visibility in `tools/nullaway`, `third_party/*.BUILD`, `wasm`, `tools/teavm`, `parser-equivalence:252`, `docs`) | Visibility | P7-11 changes only `base`, `json`, `core` and `warehouse`, plus five `testonly` targets. Its Proof (`build --nobuild`) cannot show "nothing is public that no outside package uses". | PARTIAL | The other sites listed in N14 are untouched, and nothing checks them. | **Amend P7-11.** Change: include every N14 site. Add `//tools/guards:visibility_test` (`genquery` of `attr(visibility, "//visibility:public", //...)` against a dated allowlist). Proof: `bazel test //tools/guards:visibility_test`. Done when: every public target is allowlisted with a reason. |
| G-23 | 07 N21; D10 (c); P1-27, P2-10, §6.2 (S3 row "Mirror the Linux sysroots → P1-27") | Mirror decision vs item text | P1-27's Change, Proof and Done-when still implement D10 (a). P2-10's Bump "prints the D10 mirror upload commands". | PARTIAL | An executor following P1-27 would build a mirror the user rejected. P1-27's Proof cannot pass without one. | **Rewrite P1-27.** Change: `https` everywhere plus `integrity = "sha256-…"` on every `http_archive`/`http_file`; no release assets. Proof: `git grep -n 'sha256 = ' MODULE.bazel tools/**/*.bzl` is empty (all converted); `bazel fetch //...`. Done when: every pin uses `integrity`. **Amend P2-10:** delete "prints the D10 mirror upload commands". **Amend §6.2:** S3 mirror row → DEFERRED (D10 c). |
| G-24 | S4 "Linux system libraries" / D4 (b); S3 rec 4 (`libxml2`); P5-02; P3-20 (initdb refuses root) | A runnable CI image | P5-02 lists packages only. It names no bazelisk, no non-root user (GitHub container jobs run as root, and P3-20 then fails Postgres on purpose), and does not say where D7's fallback "recorded exception" is written. | PARTIAL | The image may not run the gates. A host dependence (`libxml2`) may land without a record. | **Amend P5-02.** Change: the image adds a pinned bazelisk (an `http_file` plus `sha256`, a `pkg_tar` layer) and a non-root user `ci` (uid 1001) set as the image `user`. If D7 lands on (a), add `libxml2` and a README "Prerequisites" line plus a `docs/history`-exempt note in `.bazelrc` naming D7. Proof: **H-docker:** `docker run --rm <image> id -u` is not 0; inside the image, `bazel test //pct:pct_postgres_essential //datacube:verify_smoke_test` passes. Done when: the Linux gate jobs run in the image with no setup step. |
| G-25 | S4 risk "Download size" (keep the repository cache); P5-03 literal step | The repository cache in CI | Today the CI flags carry `--repository_cache="$HOME/.cache/bazel-repo"` (`gates-run.yml:146`). P5-03's step `bazel test --config=ci //gates:…` drops it, and no item moves it into `.bazelrc` or caches that directory (P5-04 talks only of the cache key). | PARTIAL | Each CI run would download Chromium (~120 MB), LLVM (~1 GB) and the sysroots again. | **Amend P5-03 and P5-04.** Change: `.bazelrc` `common:ci --repository_cache=~/.cache/bazel-repo`; P5-04's `actions/cache` paths include it. Proof: a second CI run's log shows no `Downloading … chrome-headless-shell`. Done when: the repository cache is policy in `.bazelrc` and is restored in CI. |
| G-26 | S4 "The rule for all three: every Playwright copy … add each new lock to its `LOCKS` list" | Lock coverage of `revision_test` | The `LOCKS` list in `revision_test` is hand-kept. A new `pnpm-lock.yaml` would go unguarded. | PARTIAL | Silent drift is possible for a third lock (low). | **Amend P1-14 (or P6-19).** Change: `revision_test` takes every `pnpm-lock.yaml` from `@repo_inventory` (P6-00) and fails on any lock that resolves `playwright-core` but is not checked. Proof: add a scratch lock in a test fixture, and the test fails. Done when: the lock list is derived, not hand-kept. |
| G-27 | S2 Windows point 6 and risk 7 (ANSI argv); S2 risk 6 (`args` apply only under `bazel run`/`bazel test`) | Docs notes | P4-12's docs bullet covers only "absolute paths in DSN file parameters". | PARTIAL | Two known behaviours are not documented (low). | **Amend P4-12.** Change: `docs/WINDOWS_APP_DESIGN_2026_10_02.md` gains the ANSI-argv note. `warehouse/defs.bzl`'s `warehouse_run` docstring states that `args` reach only `bazel run`/`bazel test` callers, so fixed defaults belong in `ServerRunfiles`. Proof: review. Done when: both notes exist. |
| G-28 | S5 "Remaining gaps" (60's header: "the base model's 210 tables", "every join … single-column": stale facts) | Generated header text | P2-03 makes this "Optionally". | PARTIAL | Stale facts stay in a generated file's header (low). | **Amend P2-03.** Change: make it mandatory: the generator computes those counts (or drops the sentence). Regenerate by `bazel run //core:update_stress_corpus`. Proof: the diff shows only header lines in 60. Done when: no static, stale fact remains in the header. |
| G-29 | 06 §3 `docs/GATES.md:28` ("every `//datacube` target tagged `browser-ci`") | Doc line | COVERED through P7-07 ("the head states the Bazel chain"). Listed here only because P7-07's line list (`:1, 86-150`) does not name `:28` and P7-10's grep would not catch it. | PARTIAL (low) | The line could survive a literal reading of P7-07. | **Amend P7-07.** Change: name `docs/GATES.md:28` and `:47` (P0-13). Proof: `git grep -n "browser-ci" docs/GATES.md` is empty. Done when: as P7-07. |
| G-30 | S1 risk "Bumping JUnit to 6.x" | Future upgrade | No item and no deferral. | MISSING (low) | Not tracked. | **Amend P1-01's Risk field:** "JUnit 6: the Launcher APIs used are stable; `runner_test` is the upgrade check." Done when: noted. |
| G-31 | 05 N3 / S4 Q6; D15 | One Playwright vs two locks | — | (DEFERRED-OK) | Listed for completeness: D15 decided two locks plus `revision_test`. G-26 hardens it. | — |
| G-32 | 06 §1c `tools/census/lanes.sh` (copies results out of `bazel-testlogs` to compare commits) | Cross-commit comparison | P3-13 replaces the script with a `test_suite`, but the README workflow "`git switch`, run, copy, `lanes_diff`" needs a stated output location. | PARTIAL (low) | The comparison may fall back to a hand copy recipe. | **Amend P3-13.** Change: `//tools/census:lanes_diff` takes two `bazel-testlogs`-shaped directories, or one `--baseline <dir>` plus the current `bazel-testlogs`. P7-08's README says `bazel test //tools/census:lanes && bazel run //tools/census:lanes_diff -- <saved-dir> "$(bazel info bazel-testlogs)"`, which G9's revised regex allows. Proof: `bazel run //tools/census:lanes_diff -- --help`. Done when: no copy step is documented. |
| G-33 | 06 §3 missing-script table (`tools/classpath-convergence.sh`, `.github/actions/gate-env`) | P7-10's proof grep | P7-10's grep lacks `classpath-convergence.sh` and `gate-env`. | PARTIAL (low) | A leftover reference would pass P7-10's Proof. | **Amend P7-10.** Change: the grep adds `classpath-convergence.sh\|gate-env\|bump.sh\|cp.txt`. Proof: as stated. Done when: as P7-10. |

---

## 3. Appendix: COVERED findings → workplan items

### 05: harnesses, query, site

| Finding / row | Item(s) |
|---|---|
| Table: `torture` | P4-06 (node_test, `startServer`, `<1 s` assertion deleted) |
| `shots` | P4-08 (port 0, `--out`) |
| `run_stress` | P0-06, P4-02 (`size = "enormous"`, `outPath`) |
| `verify_charts`, `verify_page` (shape, SHOTS) | P4-02 (sleeps: G-11) |
| `verify_cubes` | P4-02 (`tmpPath`) |
| `verify_features` (sharding, env knobs, fixed tmp name, variants) | P4-04, P4-07 (deadline/sleeps: G-11; site lookup: G-07) |
| `verify_real_data`, `verify_remote` | P4-03; EXPECT from host `duckdb`: P4-15 |
| `verify_smoke` (sharded), `verify_upload`, `verify_wasm_browser` | P4-02; env knobs: Phase 4 rules + G4 (P6-04) |
| `verify_picker` | P4-06 |
| `measure_startup`, `make_sample` | P4-08 |
| `verify_app` | P4-07, P4-12, P1-15 |
| `install_browser` | P4-09 |
| `serve` (prints `:0`) | P4-08 |
| `dist` / `make_dist` | P4-11 |
| `//query:verify` | P4-05 |
| `//query:serve`, `//query:*_test`, `//site:serve`, `//site:dist` | already correct (no change needed); `//query:tests` joins CI via P0-03 |
| `//site:verify` | P0-03, P4-05, P4-09 |
| `query/tools/icons.mjs` | P2-07 |
| `fixtures/saved-queries/make.mjs` | P2-06 |
| §1: Playwright finds Chromium in the per-user cache | P1-14 |
| Duplicates: `verify_real_data`/`verify_remote` | P4-03 |
| Duplicates: `verify_engine`/`verify_engine_differential` | P4-07 |
| Duplicates: copy-pasted static server (~15) | P4-01 |
| N1 | P0-03, P4-05, P4-09 |
| N2 | P4-11; `README-realdata.md:124` claim: P4-15 |
| N4 | P4-03, P4-08, P4-07 (and G-09), G6 (P6-06) |
| N5 | P4-05, P4-07 |
| N6 | P4-03 |
| N7 | P0-06 |
| N8 | P4-04 (`ONLY`), P4-07 (skips fail), P4-06 (calc split), P4-01 `checks()` |
| N9 | P4-04 |
| N11 | P4-02, P4-03, P4-04 (`tmpPath`) |
| N12 | P4-02, P4-04 |
| N13 | P4-02 (`outPath`), P4-05 (`.scratch`), P4-08 (`.gitignore` litter) |
| N14 | P4-02, P4-08 |
| N15 | P1-23 |
| N16 | P4-15 |
| N17 | P4-06 |
| N18 | P4-09 (comment deleted), P4-02 (tests, not binaries) |
| Pinned-browser plan steps 1–6 | P1-14 (1–4), P4-09 (5), P5-02 (6) |
| K2 | P2-06 |
| K3 | P2-07 |
| K9 | P4-11 |
| K11 | P0-02 (`set -euo pipefail`), P4-09 (loop deleted) |
| K13 | P1-14, P4-01–P4-09, G4 (P6-04) |
| §4 "not verified: `browsers.json` revision" | S4 verified; P1-14 `revision_test` |

DEFERRED-OK: N3 (D15).

### 06: scripts, docs, CI

| Finding / row | Item(s) |
|---|---|
| §1a `build.py` + the 26-module closure; `model.py` (`__file__` roots); `query.py`; `oracle.py`; `seed.py`; library modules | P2-01 |
| §1a `density.py`, `executed.py`, `stacking.py` `--gate` | P2-04 |
| Blocker 1 (input = output; glob of generated files) | P2-01 (`queries.pure`, `stress_sources()`) |
| Blocker 2 (host tz data) | P1-07, P2-01 (`tzdata`, `PYTHONTZPATH=""`) |
| Blocker 3 (59, 60, 64 with no writer) | P2-01 |
| §1b codemods: `add_taxonomy_edges`, `brokerage`, `curves`, `curves2`, `largeexp`, `schedule`, `timeseries`, `refdata`, `taxa_*` | P7-01, P7-04 / P7-05 (per SR) |
| `dense_mapping.py`, `dense_store.py` | P2-01 |
| `coverage.py` (+ json) | P7-05 |
| `differential.py` | P3-18 |
| `functions.py` | P7-02 |
| `mutate.py` | P7-05 |
| `run.py` | P3-25, P7-03 |
| `scoreboard.py` (scripts/corpus) | P2-04 |
| `scripts/corpus/repro/**` README | P7-08 |
| `scripts/corpus/verified/**` | no action (kept as docs) |
| §1c `census_gate.py`, `generate_pure_constants.py` | P7-05 |
| `outstanding.py`, `walldepth.py` | P7-04 / P7-05 (per SR) |
| `parser/fixtures.py`, `mutants.py` (+ tsv), `parity.py` | P7-04, P2-18 |
| `projects/loadtime.py` | P7-03 |
| `projects/spec.py` | P7-02 |
| `tools/census/lanes.sh`, `render.sh`, `lanes_diff.py` | P3-13 (comparison workflow: G-32) |
| `tools/ci-watch.sh` | P7-05 |
| `tools/golden_shape_survey.py` (+ tsv out of ledgers) | P7-04, P2-18 |
| `tools/metamodel-census/*` (+ json) | P7-04, P2-18 |
| `tools/scoreboard.py` | P7-04 / P7-05 |
| `tools/spikes/*` | P7-04 |
| `tools/untangle/bare_tiers.py`, `probe_counts.py`, `groups.txt` | P7-02 |
| `tools/reference/join.py`, `source_drift.py` | P7-04 |
| `tools/reference/RefImports.java` | P3-17 |
| `tools/wrongrows/engine-rows.sh` | P3-25 |
| `tools/wrongrows/compare.py`, `damage.py` | P3-25, P7-02 |
| `tools/oracle-pins.env` | P2-10 |
| `tools/engine-runner/vocab.tsv` | P2-18 (`java_run` of TokenDump, since keywords is kept) |
| `tools/engine-runner/.gitignore` | P7-15 |
| `scripts/parser` fixture corpus duplicate | D3b → P7-04 |
| §1d all `docs/` scripts and Java probes | P7-04 (D16) |
| §1e `.bazelignore` comment | P0-01 |
| §1e `repro/*` 14 instructions | P7-03, P7-08 |
| §1e `projects/` (45 uncompiled) | P3-23 |
| §1e `projects/CONTRACT.md` | P7-08, P3-23 |
| §2 jq lane map, low-memory split, platform jq | P5-03 (D6) |
| §2 `pip install pyarrow`, `--test_env=PATH/PYTHONPATH/WAREHOUSE_ARROW_CHECK` | P1-08 |
| §2 GENERATED runtime query; silent failure | P0-02 |
| §2 browser bash loop; install step | P4-09 |
| §2 `7p` key missing | P0-03 |
| §2 GATES.md:28 browser-lane description | P7-07 (head rewrite; see G-29) |
| §2 actionlint by `curl` | P5-05 |
| §2 `paths-ignore` false claim | P0-09, P6-17, P5-04 |
| §2 actions pinned by tag | P5-04 |
| §2 `diagnostics.yml` triggers on `oracle-pins.env` | P2-10 |
| §2 no workflow runs any ratchet | P2-04 (mutants: history) |
| §2 `docs/gate-additions.patch` | P7-15 |
| §3 FAQ.md `:312-314`, `:318-324`, `:327-328`, `:333-349` + NLQ | P7-06 |
| §3 README.md `:43-44`, `:65-68`/`:242-245`, `:428-437` | P7-06 |
| §3 AGENTS.md `:39-40` | P7-06 |
| §3 core/README.md `:22`, `:322` | P7-06 |
| §3 GATES.md `:1`, `:86-103`, `:105-150` | P7-07 |
| §3 ENGINEERING_LOG.md `:25`, `:55-67`, `:222-226` | P7-07 |
| §3 RUNNING_THE_CORPUS.md `:14-25`, `:40-41`, `:147`, `:169-172` | P7-07 |
| §3 UPSTREAM_FINDINGS.md `:11-12`, `:23`, `:219` | P7-07 |
| §3 UPSTREAM_BOUNDARY_PROGRAM.md `:13`, `:185`, `:199…539`, `:209`, `:423` | P7-07 |
| §3 tools/engine-runner/README.md `:36-46` | P7-08 |
| §3 tools/census/README.md | P7-08 |
| §3 tools/wrongrows/README.md `:13-14`, `:27-28`, `:38` | P7-08 |
| §3 tools/reference/README.md `:29-42` | P7-08 |
| §3 scripts/parser/README.md `:28-33`, `:135-136` | P7-08 |
| §3 scripts/corpus/repro/README.md; `repro/*/README.md`; projects/CONTRACT.md `:72` | P7-08 |
| §3 datacube/bench/README.md `:104-107` | P4-15 (Done-when: no `npm`/`node` for DataCube) |
| §3 `make.mjs` header recipe; `icons.mjs` header recipe | P2-06; P2-07 |
| Missing-script table: `allgates.sh` (GATES, ENGINEERING_LOG, UPSTREAM_BOUNDARY, `parser-equivalence/BUILD.bazel:104`, `CorpusDifferentialTest.java:21`) | P7-07, P7-10, P3-18 |
| `diagnostics.sh` | P7-07, P7-10 |
| `oracle-roots.sh` (GATES, native-axes, upstream-drift) | P7-07, P7-03, P7-10 |
| `version-report.sh`, `classpath-convergence.sh` | P7-07, P7-03 (grep: G-33) |
| `bump.sh`, `corpus-both.sh`, `judge-lanes.sh` | dated / phrased as history: no action needed |
| `.github/actions/gate-env` | P7-07 |
| `pom.xml` references (MODULE comments, `tools/par`, core/README) | P7-10, P7-06 |
| BAZEL_* "SUPERSEDED" banner points at a history doc | P7-09 |
| §4.1 CorpusDifferentialTest | P3-18 |
| §4.2 engine-side stress run | P3-25 |
| §4.3 LINKED_PROJECTS ×3 | P2-02, P3-25 |
| §4.4 `//docs:ledgers` globs snapshots | P3-07, P2-18 |
| §4.5 generated files without coverage (mutants, vocab, metamodel json, corpus-coverage, census-baseline, 59/60/64, saved-queries, icons) | P2-18, P7-04, P7-05, P2-01, P2-06, P2-07 (AGENTS-linked files: G-20) |
| §4.6 hard-coded machine paths | P7-03 (Done-when), P7-04, P7-05, P3-13, P3-25 |
| §4.7 `.bazelignore` comment | P0-01 |
| Header: `nlq/` described in FAQ | P7-06 |

DEFERRED-OK: experiments/ READMEs and dead prototypes (user decision; G9 excludes `experiments/**`); `semantics_probe.sh` (D3 row 11).

### 07: Bazel files

| Finding | Item(s) |
|---|---|
| N1 | P0-03 (interim), P5-01, P5-03 (`bazel test //...` and `bazel build //...`) |
| N2 | P0-04, P0-05, P6-10 |
| N3 | P0-01 |
| N4 | P1-07, P1-08 |
| N5 (a)–(d) | P0-03, P4-02–P4-09, P1-14, P0-02 |
| N6 | P1-15 |
| N7 (a), (b) | P1-12, P1-09, P1-10 ((c): G-21) |
| N8 | P0-12, P1-19 (mirror: D10 c) |
| N9 | P1-25, P6-10 |
| N10 | P0-11, P0-10 |
| N11 | P4-14 |
| N12 | P1-19 |
| N13 | P1-20, P1-23 |
| N15 | P1-21, P1-22 |
| N16 | P1-28, P0-04, P5-04 |
| N17 | P0-02 |
| N19 | P1-26 |
| N20 | P3-16, P7-13 |
| N23 | P1-25, P6-11 |
| K8 | P1-17, P4-12 |
| K15 | P3-05, P3-07, P2-18, P6-12 |
| K16 | P1-22 |
| K17 | P6-11 |
| K18: `_GENERATOR_SRCS`, hard-coded `core/src/main/java` | P7-12 |
| K18: `corpus_lanes` = `judge_lanes` | P0-13 |
| K18: `postgres_live*` manual | P3-22 |
| K18: `use_testrunner = False` consequences | P1-01, P1-02 |
| K18: `"../" + repo_name`; `Repo` via `TEST_SRCDIR` | P1-04, P1-05, P3-32 |
| Test table: `core_tests` (child JVM) | P3-19, P3-05 |
| `guardrails`, `census`, `stress_suites`, diff tests, datacube js tests | P5-01 (gate suites); P3-12 (`stress_suites_h2`) |
| `live_snap_test` | P0-13; host CC: P1-09/P1-10 |
| `json:tests`, `twins_test`, `query:*_test`, `query-store:*` | P0-03; `lite_test` size: P3-16 |
| `parser_parity`; `diagnostics` | P5-01; P2-10, P3-17 |
| `pct_duckdb`/`pct_postgres` composite and per-suite manual | P3-09 (D2, D6) (discipline: G-02) |
| `pct_h2`, `pct_channel_b` | P5-01, P3-08 |
| `pct_postgres` host-keyed repository | P1-15 |
| `spec_tests`, `corpus_duckdb`, `corpus_h2` | P3-01, P2-15 |
| `tools/deps:*` | P2-10 (`one_release` deleted), P6-11 (`pools_are_disjoint` deleted) |
| `warehouse:tests`, `tests_native` (python on PATH) | P1-08, P3-21 |
| `postgres_live`, `postgres_live_native` | P3-22 |
| `wasm:*` (`zone_jvm` host TZ) | P0-10 |
| Browser-ci binaries via `bazel run`; 9 tests in no lane | P4-09; P0-03 |
| Green-ability: Linux gcc/zlib | P1-09 |
| Green-ability: Windows bash for `adapter_par`; symlinks | P1-22, P5-07; P3-32 |
| Green-ability: warehouse skips without pyarrow | P1-08 |
| Gen-file table: DynaFn, Pure, NameResolver, engine-handlers, native-claims, prelude | already covered (`//core:update_generated`) |
| `native-membership.tsv` (hand-owned) | P2-17 (draft tool) |
| datacube generated, catalog-corpus, protocol-roster, corpus-manifest, fixtures jsonl | already covered |
| `link-p1.ts` | P2-08 |
| `own-corpus-protocol-diffs.tsv` | G1 allowlist (P6-01) |
| `icons.ts`; stress 10; saved-queries | P2-07; P2-01; P2-06 |
| `mutants.tsv` | P2-18, P7-04 |
| `NATIVE_CLAIMS_CENSUS…tsv`, `type-audit/data/*.tsv` | P2-18 (explicit `//docs:ledgers`) |
| maven locks; `MODULE.bazel.lock`; pnpm locks | P0-04; P0-04; P1-26 |

DEFERRED-OK: green-ability "macOS needs CLT" (D1 b: the CLT stays installed for SDK files only, sha256-checked).

### S1: test runner

| Finding | Item(s) |
|---|---|
| Decision B | P1-01 |
| Q5 / reword the PCT criterion | P3-09 Done-when |
| Migration steps 1–4 | P1-01 |
| Step 5 (per-class PCT or `shard_by`; `core_tests` shards) | D2 → P3-09; P3-05 |
| Phase 6 guard | P6-16 |
| Risks: JUnit 3 suites; round-robin determinism; `@BeforeAll` in shards; owned runner code; filter semantics; `System.exit` | P1-01 (overrun guard, `runner_test`), P1-02, P3-05 ("measured first") |
| Q1 (E7 on a real PCT class) | P3-09 first proof |
| Q2 | D2 |
| Q3 | P1-01 (warn) |
| Q4 | P3-01 |
| Q5 | P1-01 |
| Q6 | P3-05 |
| E6 (memory tag clamping) | P1-21 |

PARTIAL: Q0 / step 6 for manual lanes (G-08). MISSING: JUnit 6 note (G-30).

### S2: runfiles and launchers

| Finding | Item(s) |
|---|---|
| Decision 1 (serve/app plain executables) | P4-12 |
| Decision 1a (gunzip) | P1-17 |
| Decision 2 (Java official runfiles) | P1-03, P1-04, P1-05, P1-06, P3-27 |
| Decision 3 (JS `@bazel/runfiles`) | P1-23, P1-24, P3-29 |
| Done criterion rewritten | P3-32, P6-15 |
| `_warehouse_binary`, `warehouse_run` | P4-12 |
| Runfiles defaults for library and extension | P1-16 |
| `ServerRunfiles` | P1-16 |
| `named()`, `--duckdb-extensions` as file, `--token-key-file` | P4-12 |
| `DuckLibrary.resourceName()` public | P1-16 |
| JS tests: drop `chdir`, reporter by label | P1-23, P1-24, P3-29 |
| "What it deletes" (launcher, `hermetic_launcher`, `run_shell`, `Repo` for tools/deps, `RUNFILES_DIR` hand-off, URL arithmetic in one test) | P4-12, P1-17, P1-03, P1-24 |
| Windows points 1–4 | P4-12 (CI Windows lane; desk run) |
| Windows point 5 (symlinked `.exe`) | P4-12 (desk run; copy remedy) |
| Windows point 7 | P4-12 |
| Risk 1 (rules_java stub) | P3-32 (issue filed) |
| Risk 3 (`chdir` on 18 targets) | P1-24, P3-29, then P3-32 |
| Risk 4 | P4-12 |
| Risk 5 (hard-coded runfiles paths) | P1-16 (BUILD comment), `ServeTest` |
| Recommendation Phase 4.3; criteria 1 and 2; "remove `--enable_runfiles` only after chdir"; "don't present `@bazel/runfiles` as the unblocker" | P4-12, P6-15, P3-32 |
| Effort rows | P4-12, P1-17, P1-03–P1-06, P1-23/P1-24 |
| Q1 Windows desk | P4-12 |
| Q3 rules_java issue | P3-32 |
| Q4 `planner_dir` | P1-24 |

DEFERRED-OK: criterion 3, Linux manifest lane, and Q2 (§6.3); risk 2 (rules_js Unix; §6.3); FFM `chdir` (Q5; §6.3, docs in P4-12); generated defaults resource (Q6; §6.3). PARTIAL: `Repo.out` (G-01); ANSI argv and `args` docs (G-27).

### S3: hermetic C toolchain

| Finding | Item(s) |
|---|---|
| macOS GO / SDK | P1-10 (D1) |
| Linux aarch64 GO | P1-09 |
| Linux x86_64 (addendum: verified) | P1-09 |
| Windows keep MSVC, explicit | P1-11 (D5) |
| zig NO-GO | §6.2 (rejected) |
| rules_graalvm uses the cc toolchain only partly | P1-09, P1-12 |
| MODULE changes; `warehouse/BUILD.bazel` (`static_zlib`, `-fuse-ld=lld`) | P1-09, P1-10 |
| Rec 1: explicit `register_toolchains`; `BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN` | P1-09, P1-10 |
| Rec 2 | P1-09 |
| Rec 3: rename the patch | P1-09 |
| Rec 4: CI drops gcc / zlib-dev / Xcode selection | P5-02, P5-03 |
| Rec 5: blocked-CLT guard | P1-10 (`--config=hermetic-cc`), P6-18 |
| Can the patch be dropped (CLT hunk) | P1-10 (investigate) |
| Host glibc runs build tools; macOS OS components; libstdc++ for `tests_native` | accepted as OS components; Debian base image (P5-02) |
| Risk 2 (lld as the Mach-O linker) | P1-10 (`tests_native` is the check) |
| Risk 3 (glibc 2.31 baseline) | D10 pin table |
| Risk 4 (upstream drift) | P1-12 |
| Risk 5 (download size) | repository cache (see G-25) |
| Risk 6 (x86_64) | P1-09 CI proof |
| Q1 (aarch64 crash) | P1-13 |
| Q5 (x86_64) | P1-09 |
| Addendum: two sysroot defects | P1-09 (per-architecture file lists) |

DEFERRED-OK: SDK source and SDK contents (D1 b); mirror the sysroots (D10 c); `cc-toolchain-x86_64-darwin` (§6.3); Windows clang-cl + xwin and Q6 (D5, §6.3); Q2 (D1). PARTIAL: the patch's shell and its permanence (G-03); recording the `libxml2` exception (G-24); upstream PR end condition (G-03).

### S4: pinned Chromium

| Finding | Item(s) |
|---|---|
| Decision B; A rejected | P1-14; §6.2 |
| Verified revision; CfT URL scheme; Google mirror URL; one `browsers.json` for both locks | P1-14 |
| `add_prefix` launch guard; `TMPDIR = TEST_TMPDIR` | P1-14 |
| 17 Debian packages | D4 → P5-02 |
| Module extension (Q1); `//tools/browser` aliases; `pinned-chromium.mjs`; `revision_test`; `browser_test` macro; `no_copy_to_bin` | P1-14, P6-19 |
| Import-first vs `node_options --import` (Q5) | P1-14 (investigate) |
| Shared `harness.mjs`; per-sample sharding | P4-01, P4-02, P4-04 |
| Query and site share the browser; site reach-around | P4-05, P1-26 (D15) |
| CI: delete install step and loop; browser tests in `//...`; Linux in the image; macOS needs nothing | P4-09, P5-01, P5-02, P5-03 |
| Risks: CDN path internal; Linux x86_64 not run; processwrapper vs linux-sandbox; test sizes | P1-14 (second URL), P4-10, P7-13 |
| Effort table | P1-14, P4-01–P4-10, P5-02 |
| Q3 base image | D4 (Debian 12) |

DEFERRED-OK: hermetic `.deb`s (§6.3); roll script (§6.2: "not built"); Windows browser CI and Q2 (D11); fonts and Q4 (D4); Q6 (D15). PARTIAL: `LOCKS` derived (G-26); repository cache (G-25); runnable image (G-24).

### S5: stress generator

| Finding | Item(s) |
|---|---|
| Q1 rules_python 2.3.4, CPython 3.12, `@pypi` | P1-07 (D12) |
| Q2 `build.py` action, 92 split, `stress_sources()` | P2-01 |
| Q3 59/60 seeds and view-root rule | P2-01 (D13) |
| Q4 64 writer | P2-01 |
| Q5 determinism (`PYTHONHASHSEED`, `PYTHONTZPATH`, `LC_ALL`) | P2-01 |
| File split; one data file for generated lists and `LINKED_PROJECTS` | P2-01, P2-02 |
| `CORPUS_ROOT`; `--out`/`--dense`; `dense_build.py` | P2-01 |
| BUILD targets; root `update_generated` wiring | P2-01 |
| pyarrow / duckdb join the lock later | P1-08, P4-15 |
| Retire `--check` and the in-place write | P2-01 |
| `py_test` gates (density, executed, stacking) | P2-04 |
| Risk 1 (cost of an edit) | D8, P2-05 |
| Risk 2 (unsandboxed reads) | P2-03 |
| Risk 3 (rules_python bump) | P1-07 |
| Risk 4 (interpreter version) | D12 |
| Risk 5 (`today`/`now`) | P2-01 |
| Risk 6 (`runs/` not bazelignored) | P0-01 |
| Effort: RUNNING_THE_CORPUS; "Regenerate with" headers | P7-07; P2-03 |

DEFERRED-OK (decided): filter name, seed location and `queries.pure` location (D13, Q2–Q4); default `//...` (D8, Q1); Python 3.12 (D12, Q5). PARTIAL: 60's stale header text (G-28).

### Script review

| Row | Item(s) |
|---|---|
| Flag: `build.py` (+ closure) | P2-01 |
| Flag: `fixtures/saved-queries/make.mjs` | P2-06 (D14) |
| Flag: `query/tools/icons.mjs` | P2-07 |
| Related: 59, 60, 64 with no writer | P2-01 |
| Related: one-shot codemods | P7-01, P7-04 / P7-05 |
| Related: unread ledger TSVs (`FUNCTIONS_EXECUTED`, `golden-shape-survey-4AD`) out of `//docs:ledgers` | P2-18 (writer conflict: G-13) |
| Related: `differential.py` writes where no test sees | P3-18 |
| D3 row 1 `run.py` | P3-25, P7-03 |
| D3 row 3 dense generators | P2-01 |
| D3 row 4 `differential.py` | P3-18 |
| D3 row 7 `scoreboard.py` | P2-04 |
| D3b `install-browser.mjs` | P4-09 |
| D3b `make-dist.mjs` | P4-11 |
| D3b `query/test/strict-reporter.mjs` | P1-23 |
| D3b `scripts/parser/{fixtures,negative}/` | P7-04 |
| D3b `docs/gate-additions.patch` | P7-15 |

DEFERRED-OK: D3 row 10 (`Probe.java` to history; a fresh probe is a separate request); D3 row 11 (`semantics_probe.sh` kept). PARTIAL: rows 2 (G-13), 5 (G-14), 8 (G-15), 9 (G-17); D3b `bench/model` (G-18). MISSING: row 6 (G-04).
