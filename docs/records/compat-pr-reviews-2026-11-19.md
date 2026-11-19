# Independent reviews of the compatibility program's stage-A PRs (2026-11-19)

The program's independent reviewer, who wrote none of this code. Each PR is checked against:
- the final plan's done-when (`compat-final-plan-2026-11-19.md`);
- the rules of record (`compat-rules-of-record-2026-11-19.md`).

For each PR: the diff read; every moved pin, ledger, golden and guard checked against its dated note; the done-when
shown, not just claimed; no new hook, switch or mode in the core; no rule broken.

Review only: no GitHub comment, no product code, no pin moved.

## PR #123: the edge net hardened (S0), head `7b7ee6ea3`

**Verdict: the code is APPROVED. Do not merge yet.** Its own CI is not green on every OS (A1's done-when):
- run 44545061580's `gates green` failed: Linux `product` hit Maven Central's 404 for `teavm-jso`;
- 5 macOS lanes were cancelled after 40 to 47 minutes without a runner: `core`, `pct_duckdb`, `pct_channel_b`, `sdlc`
  and `product`;
- A1 also requires the Maven download fix on `main` first.

Re-run those six jobs once the mirror lands. Every other lane on Linux, Windows, macOS and linux-arm is green.
`edge_net` is green on all three OSes, 38 of 38 each.

**My earlier HOLD items (at `5e3bbe6d1`), all closed:**

| Item | Now |
|---|---|
| 3 of 10 plants vacuous | **Fixed.** The Float-digit and message plants go through `PctCaseRecorder.flat` and `relationText`, where the mask bug was. The escaped-key plant reverses the rows' text order. Shown not vacuous: with the old 16-hex mask planted, 3 recorder plants fail; with a full sort planted, 3 normalizer plants fail |
| PCT's library depended on a sorting tool (`PctDisciplineTest`'s ban) | **Fixed.** The normalizer's plants are `//tools/guards:edge_normalize_test`. `pct_tests_lib` gains no dependency |
| Guarded moves not named | **Named** in the body: `EDGE_TEST_SRCS` +3 (dated 2026-11-18); the `gates/BUILD.bazel` moves; the two new overlay files |
| `json` visibility widened | **Reverted** (`b6e3bd013`): `EdgeNormalize` reads the recorder's JSON itself |
| CI red | Only external failures remain (above) |

**Checked myself:**
- **No product code moves.** No file under any `src/main` changes against the merge base `b75d6c73`.
- **No golden moved since my re-derivation of "no answer moved" at `5e3bbe6d1`.** The diff `5e3bbe6d1..7b7ee6ea3` touches
  no answers, outcomes, ledger or pure/v1 golden. It adds only `overlay_windows.tsv` (17 rows) and `overlay_macos.tsv`
  (16 rows), so my earlier back-mapping still holds.
- **`PlatformOverlay` (`tools/edgenet/PlatformOverlay.java`) is sound and keeps every digit pinned.**
  - On a row's own OS, its line must hold that OS's value, which is rewritten to Linux's. A missing line or another
    value fails the run, so there are no stale rows.
  - On another OS, the row's Linux value is checked against the recording, so the overlay cannot drift from the golden.
  - The current OS's rows apply first, which is right when two OSes move the same fact. This has a plant.
  - It touches `<db>_<suite>_answers.tsv` only.
  - One limit: a row finds the first line with its test and case. A case with several VALUE lines fails loudly rather
    than matching silently, which is acceptable.
- **No new hook, switch or mode** in the core. `ModeBranchCensusTest` and `OnePlanGuardTest` stay report-only.
- **The rules of record.** No product code is touched, so rules 1 to 3 are not engaged, and rule 4 is unaffected. The
  overlays' finding is the database computing (rule 1), measured.

**For the user's word at the merge:**
- `EDGE_TEST_SRCS` +3;
- the `gates/BUILD.bazel` moves (`edge_net` a lane of its own; the corpus ledgers' diff tests in `corpus_duckdb` and
  `corpus_h2`; `edge_contract_test` out of `heavy`);
- the two overlay files, 33 rows pinned;
- the S0 format re-records (ruled 2026-11-18, no answer moved);
- the normalizer's tests placed in `tools/guards` so that `@maven_test` needs no allowlist move. That is stated in the
  body, and the user should know it;
- the id mask by first appearance (12 golden lines, format only): offered, not taken.

**Nits (none blocks):**
1. `EdgeNetPlantedTest`'s javadoc (line 24) still says "tools/edgenet's EdgeNormalizeTest". It is
   `tools/guards/EdgeNormalizeTest` now.
2. **The overlays record a determinism fact.** Lite's answers on DuckDB and SQLite differ in the last place by OS:
   - Windows: `sinh`, `cosh`, `tanh`, `cbrt` and `pow`;
   - macOS: `acos`, `asin`, `cot`, `sinh`, `cosh` and `cbrt`.

   That bears on G10 (determinism), and should be a register row or a line on the differences page. Linux arm is
   unmeasured.

## PR #124: the ENGINE PCT lane, the batteries, slice R's oracle, head `a7a2a5aa9`

**Verdict: the code is APPROVED.** Merge after #123, once its own CI run 44557117163 completes green. It was in progress
at 16:10 UTC:
- Windows: every lane green;
- Linux: every lane green except `core` and `checks`, still running. `product` is green this time;
- macOS: `ui`, `pct_channel_b`, `edge_net`, `warehouse` and `datacube` queued; `pct_duckdb`, `checks` and `stress`
  running; `core`, `product`, `pct_h2`, `parser_equivalence`, the corpus lanes, `sdlc` and `pct_postgres` green;
- no failure so far.

This run carries #123's head, so on completion it also covers the macOS lanes #123's own run never got.

**My earlier HOLD items (at `f922740b`), all closed:**

| Item | Now |
|---|---|
| A Windows temp path in a Pure string, unescaped (`LiteBatteries.java:41-43`) | **Fixed** (`ff84dfa69`). The path is written with `/`, and the copy is made writable, with a check |
| The golden-format collision with #123 | **Fixed.** #123 is merged in (`d32495fc1`, `a7a2a5aa9`), and `GoldenCompareMain` reads the net's format, re-pinned (`fa8294cca`) |
| Refusals in other words classed as DEFECT | **Fixed** (`c9665ad7e`): `EngineLaneTable.REFUSAL_WORDS`, listed |
| No plant that the lane runs as legend-engine's audience | **Fixed.** `RouteAudience` counts how each case was planned, and `AudienceCheck` fails a lane with any case planned otherwise |
| `pct_tests_lib` gained a dependency | **Fixed.** `LiteBatteries` lives in `//pct:pct_batteries` |
| Guarded files not named | **Named** in the body |

**Checked myself:**
- **No product code moves.** No file under `core`, `pct` or `spec` `src/main` changes against #123's head. The only
  main-source tree is `tools/engine-runner`, and all 22 of its targets are `testonly`. The oracle libraries
  `planoracle` and `planoracle_compare` are checked by name.
- **The one allowlist entry has its dated note.** In `tools/deps/jars_table.bzl`, `duckdb_jdbc`'s users gain
  `tools/engine-runner`. The note cites CP-9, ruled ~10:45 UTC, and says that it is test tooling only.
- **The other moves are named:**
  - the runner pool (`release.MODULE.bazel` +10, `maven_runner_install.json` +10);
  - the lanes (`pct_duckdb` and `pct_h2` gain the ENGINE lanes and the batteries' per-push half; `heavy` gains the two
    oracle tests);
  - the visibility lines;
  - `JUnitMain`'s property-gated full-name outcomes;
  - the new pinned tables: `pct/engine_lane/*`, `pct/batteries/*` and the oracle's answers.
- **No new hook, switch or mode in the core.** The lane selects legend-engine's audience in PCT's test code
  (`PctPlanRoute`: `-Dpct.audience=engine` picks `PureText.PURE_V1` and no feature flag), through the existing switch.
- **The rules of record.**
  - Rule 4: the lane's verdict is PCT's own assert (channel A), not lite's database judge.
  - Rule 3: legend-engine's text comes from lite's typed rows, through the existing edge.
  - Rule 1: nothing computed outside a database. The table classifies words, and the batteries ask both engines.

**Notes for later steps (none blocks):**
1. **B5's count must include this lane's use of the switch.** `PctPlanRoute`'s `PureText.PURE_V1` is test code, and
   when `PlanStyle` goes the lane must move to the legend-engine door's formatter.
2. **A3 edits the same lines.** A3 removes the default audience's `CORRECT_SQL_SUBSTRING_INDEXING` from
   `PctPlanRoute`, so it lands on top of this PR. A3 will also move 12 rows of these pinned tables, all toward PCT's
   answer. Each is a pin move for the user's word, listed in A3's PR.
3. **"Report-only" means the lane never fails on lite's own failures.** It does fail when a row's class moves. That is
   right, and the body should say it in those words.

## PR #125: CI's Maven downloads fall through to Google's mirror, head `ca523848d`

**Verdict: the code is APPROVED. CI is pending.** Run 44573295984 started at 17:05 UTC; "every 404 gone" is answered
below once it completes.

**What I checked myself:**

1. **CI-only.** The diff is 3 files and 20 lines:
   - `.bazelrc`: two `common:ci` lines;
   - `tools/ci/downloader.cfg`: new;
   - `tools/ci/BUILD.bazel`: `exports_files`.

   The `ci` config is the gate jobs' own: `tools/ci/lane-flags.sh:16`, `flags=(--config=ci ...)`, used by every
   `bazel build` and `bazel test` in `gates-run.yml`. `diagnostics.yml` and `engine-verdict.yml` pass `--config=ci` too.
   A desk or cloud session without `--config=ci` is unchanged. No product code, pin, lock, `MODULE.bazel` entry or jar
   version moves.

2. **The sha256 checks are unchanged for every fetch.** The rewrite changes URLs only, and every fetch that names
   `repo1.maven.org` carries a pin. Counted on the tree:
   - `tools/deps/jars.bzl`'s `http_jar`s: all 16 entries of `jars_table.bzl` carry a `sha256`, passed as
     `sha256 = jar.sha256`;
   - the six rules_jvm_external pools (`maven_test`, `maven_tools`, `maven_teavm`, `maven_native`, `maven_upstream`,
     `maven_runner`): every one has `lock_file` and `fail_if_repin_required = True`. Their lock files hold a sha256 for
     every file: 22, 10, 29, 2, 402 and 619 files, 0 without;
   - **one more the PR body does not name:** `tools/postgres/postgres.bzl` downloads the embedded Postgres binaries from
     `repo1.maven.org` with an `integrity` pin per platform. The rule covers it too, safely.

   No pool resolves unpinned in CI, so no Coursier fetch bypasses Bazel's downloader.

3. **The rewrite touches only `repo1.maven.org/maven2`, with Central first.**
   - Rule 1 is the identity and rule 2 is the mirror (`maven-central.storage-download.googleapis.com/maven2`). Matching
     URLs try Central, then the mirror, in order.
   - There is no `allow` or `block` directive, so every other host passes unchanged: the BCR, GitHub archives,
     BuildBuddy.

4. **Nothing can change what a build fetches beyond the URL.**
   - A file from either host must match its pinned digest, or Bazel refuses it.
   - The repository cache is keyed by that digest, so a mirror fetch fills the same cache entry.
   - The proof in the body forced the first rule to a 404 path, and both an `http_jar` (h2 2.1.214, at its pinned
     `d623cdc0...`) and a pool jar came from the mirror.
   - A 429 is not forced. It takes the same next-URL path, and this PR's CI is the live check.

**Nits (none blocks):**
- `exports_files(["downloader.cfg"])` is public by default. `--downloader_config` reads the path directly and needs no
  target. If a guard (`inventory`) needs the file owned, give it the package's narrow visibility
  (`//tools/guards:__pkg__`).
- `--experimental_repository_downloader_retries=3` is an experimental flag. Bazel 9.2 parses it, and this run proves it
  on all three OSes.
- The body's list of fetches should name `postgres.bzl` as covered.

**The rules of record:** not engaged. This is CI configuration only, with no core hook, switch or mode.

**CI: every 404 gone?** Pending run 44573295984. To be filled in when it completes.

## PR #126: `markets_test`'s time, the headless host reads a model text's own answers once, head `699331ffd`

**Verdict: APPROVED. CI is pending,** and the Windows `warehouse` lane is the live check.

**What it is.** A product change in lite's own Python host (`python/legend_lite/_headless.py`, `projects.py`), not a
test change. It adds three bounded memos, plus new tests in `python/tests/test_headless.py`:
- the `modelElements` answers, by the whole request text, for the latest 4 texts;
- an extra engine-side hold of each model DataCube's code holds, by its model and library texts, for the latest 2,
  released when pushed out and at close;
- `ProjectDatabases`' text-to-database-name map, for the latest 8 texts.

**What I checked myself:**

1. **Every assertion the test made is kept.** `markets_test`'s test file and its BUILD target (timeout and size) are
   untouched. No case is dropped, skipped or weakened. The only test file in the diff is `test_headless.py`, which
   gains `ModelTexts`:
   - an edited text gets its own elements, and the old text keeps its own;
   - each text's hold is its own, and the holds are let go at close;
   - an edited project's rows are its own database;
   - the project memo's bound is kept, and a text pushed out is answered again from the same database.
2. **No pin, golden, ledger or allowlist moves.** The diff is 3 files, +168/−1.
3. **The memos are sound, because each answer is a function of its key alone:**
   - `model/elements` is a parse only (`LiteV1Planning.modelElements`: `Parse.documentJson(model, false)`). The held
     models play no part, so the same request text gets the same answer;
   - `compiler.project_data(model, type)` is a function of the text and the database type, and one
     `ProjectDatabases` has one type;
   - the hold memo is keyed by the model text and the library text (`datacube/src/lite-planner.ts:466-467`: `library`
     is a model text, not a handle);
   - a failed answer is never kept, the request's `id` is rewritten on a memo hit, and the holds are released at
     close;
   - the cached project path skips `self._guard()` (the Frames' connection lock) only for a lookup. The guard protects
     making a database, which the cached path never does, and the cursor was used outside the guard before as well.
4. **The rules of record hold.**
   - Rule 1: no value is computed in Python; the memos replay the compiler's own answers.
   - Rule 2: this is host orchestration, not presentation, and decides nothing.
   - No mode or switch. It relies on G10 (a text's answer is deterministic), which is ruled.

**Notes (none blocks):**
- **This is a product change, for the user's word as one.** Its title says "the Python host only", which is true, but
  that host is lite's own `legend_lite` package. The tab and the server get no such memos. That is a performance
  difference, not an answer drift, and it should be named.
- **The hold memo's eviction at `_HELD_KEPT = 2` is not tested.** The tests check the two holds and their release at
  close, not that a third model releases the oldest. Add a plant.
- **Memory:** up to 2 compiled copies of a large model stay alive per host beyond DataCube's own holds. That is
  bounded, and should be stated in the body.
