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

**CI: every 404 gone? Yes, on every lane that has finished** (run 44573295984, read at ~17:47 UTC):
- **No lane failed at fetch time.** That includes Linux `product`, `core` and `corpus_h2`, the lanes that hit Maven's 404
  for `teavm-jso`, `rhino` and `h2-2.1.214` on #123 and #124.
- **Linux:** all 21 lanes green.
- **Windows:** all green except `warehouse`. Its log says "1 test FAILED": `//python:markets_test`, the timeout #126
  fixes, not a download.
- **linux-arm:** `warehouse` green.
- **macOS:** `ui`, `pct_postgres`, `warehouse` and `corpus_h2` green; the rest still queued or running.
- **One limit:** the job tails do not show whether a fetch fell through to the mirror on this run. The proof that the
  mirror works is the PR's forced-404 run; this run shows the config parses and fetches everywhere with
  `--config=ci`.

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

## The combination: #124's branch merging #123, #125 and #126, head `f832a6671`

**Verdict: the combination is clean.** Its diff against `main` is exactly the union of the four PRs' diffs, and nothing
interacts. Its own CI, run 44580527071, started at 17:45 UTC.

**Checked myself, by script, file by file:**

| | |
|---|---|
| The pieces | #123 plus #124 at `a7a2a5aa9`; #125 at `ca523848d`; #126 at `684f923b`. #126's line gained `684f923b` after my review: the eviction plant I asked for, `test_a_third_model_held_lets_the_oldest_go`, test-only. All are based on `main` `b75d6c73a` |
| Files | each piece's changed files are disjoint from every other piece's (0 overlap in each of the 3 pairs). The combination changes 105 files, exactly the union: none extra, none missing |
| Content | every one of the 105 files' blob at `f832a6671` is byte-identical to its piece's. The two merges, `aa002b6a` (#125) and `f832a667` (#126), changed no line of any piece |

**No interaction:**
- **The mirror and #124's lanes.** #124's new fetches (the `@maven_runner` pool for `tools/engine-runner`'s oracle and
  batteries, and the referee's `h2_engine` jar) all name `repo1.maven.org`, so they now get the mirror too. That is the
  intended effect. Every runner-pool file at the combined head carries a sha256 (619 files, 0 without), and
  `--lockfile_mode=error` (`common:ci`) is unchanged.
- **#126 and the nets.** #126 touches only `python/`. Neither the edge net (#123) nor the ENGINE lane (#124) runs the
  Python host. `markets_test` runs in the `warehouse` lane alone.
- **No shared guard list.** No guard file is edited by two pieces: #125 adds `tools/ci` files, and #123 adds
  `tools/guards` and `tools/edgenet` files.

**What remains before the user's merge word:** the combined run's result on every OS. Windows `warehouse` is the lane
that shows #126's fix, and every Linux lane shows #125's.

## PR #127: the LITE-PCT lane (A4's gate) and judge parity, head `3cb787902`

**Verdict: APPROVED with changes.** One must-fix comes before it joins the batch, and five should-fixes are named below.
CI is pending.

**MUST-FIX: it conflicts with #124 in `PctPlanRoute.execute`.** A trial merge of #127 into the combined branch
(`f832a6671`) stops with `CONFLICT (content)` in `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java`.
Both PRs rewrite the same lines (the feature set, and `RaisedWords.run(PureText.UPSTREAM, ...)`):
- #124 adds `-Dpct.audience=engine` (`PureText.PURE_V1`, no flag);
- #127 adds `-Dpct.channela.rules=LITE` (`PlanStyle.PRODUCT`, no flag).

Resolve them into one selector with three values (legend-pure's, legend-engine's, lite's), and refuse a combination by
name. Two properties would allow a meaningless pair, such as `engine` plus `LITE`. #124's `RouteAudience` plant must
still see the engine audience after the merge.

**The five places A5 asked me to look hardest:**

1. **`Targets.forTest` and its three call sites: holds.**
   - The product door's `target(...)` is untouched. `forTest` is reached only from `PlanMaker`'s TEST door,
     `PlanMaker.testSessionTarget` and `JudgePlans.target`.
   - On DuckDB a test target's session list differs (`SET threads TO 1`), so its JSON, and so its `PlanSessions` key,
     differs from a product target's. An in-memory database is never shared.
   - On every other dialect `testSessionSetup` is empty, so test and product targets are equal and share sessions, as
     before.
   - **The caveat is a LocalFile DuckDB target** (`DuckDbConnector`: `jdbc:duckdb:<path>`). `threads` is the database's
     global setting, and a test target and a product target naming the same file in one JVM would meet in one database.
     That would happen, for example, if a server ran a project's tests and its queries on one file. Show that this
     cannot happen, or refuse `testSessionSetup` on a file database by name (see S1).
2. **`raisingRuns` and `MayRaise`: holds.**
   - A new run starts at each member that may raise, never at the first, so within a run only its first member may
     raise.
   - `withinBudget` halves a run in order. A rejected later half's lead cannot raise, except through the named gaps.
   - `MayRaise` reads the statement and changes nothing.
3. **`TestRun`, `RootLoops.Run.rejected` and `TestRunner.verdict`: holds.**
   - A rejection tells each verdict in the group that it has no row.
   - The segment is judged at once: the roots with rows are judged, and the first root without a row raises the
     database's own error.
   - Later groups are never sent.
   - The `IllegalStateException` is a loud invariant, never a fallback. It is reachable only if no root reads a
     rejected verdict's row, for example if every such root is `NO_VERDICT`. No test covers that path; add a plant, or
     show it unreachable.
4. **`VerdictSql`'s swapped cross join: holds.**
   - A CROSS join commutes, the verdict row reads each fact by its alias, and the ladder goldens move only in their
     `FROM` and `CROSS JOIN` lines.
   - Its purpose, raising the expected side's error first, rests on DuckDB's choice of build side. That is a physical
     plan, not a contract. `AssertBarTest#sidesTheDatabaseCannotHoldAreOneOutcomeUnderEveryJudge` (10 runs per judge)
     and the LITE lane are its guards, and they would catch a DuckDB upgrade that changed it.
5. **The `ChannelBLedger` guard and its plant: holds, with one coupling.**
   - Any `JudgesDisagree` outcome is refused by name, whatever row it would match.
   - **The coupling:** the guard matches the text `"JudgesDisagree: "`, the runner's exception's simple name. The plant
     builds that string itself, so a rename of `testexec.IsolatedRun.JudgesDisagree` would switch the guard off
     silently. Tie the constant to the class, for example a test checking `Class.forName(...)`'s simple name (see S3).

**The rules of record: hold.**
- **Rule 1:** no value is computed outside the database.
  - `LiteText.toTheMinute` is a SQL `CASE` (`STRPOS`, `cat`), computed by the database.
  - The AdjustBy literals stay in the database (the owner's option 2).
  - `SqlExpr.raisesItself` returns a Boolean about MIR, not SQL (invariant 3a).
- **Rule 4:** the LITE lane is the product's own, with both judges.

**What I checked myself:**
- **The H2 `firstDayOfWeek` fix** (`DATE_TRUNC` week as `iso_week`) agrees with Pure and with legend-engine's own H2
  spelling (`dateadd(DAY, -(mod(dayofweek(%s)+5, 7)), %s)`, `h2Extension2_1_214.pure:223`, pinned 4.145.0). So it
  removes a difference and needs no register row. Only `DateShifts`' first-day-of family builds a `DATE_TRUNC`
  (`DateShifts.java:131`), and only its week maps to `iso_week`. Nothing else on H2 moves.
- **The moves listed for the user's word are all in the body, each with its measurement:**
  - the judge-statement ratchets: essential +30%, grammar +33%, standard +8%;
  - the lean-SQL baseline, +0.121% of plan JSON, from the setting's one statement in test plans;
  - 462 `duckdb-lite` ledger rows in seven classes;
  - 26 channel-A LITE rows;
  - the `RootLoops.rejected` allowlist entry, with its dated reason;
  - the ladder and dialect-capability regenerations.

  No `JavaEvalLedgerTest` pin moves, and `TestPlanJson.VERSION` is unchanged.

**SHOULD-FIX:**
- **S1. `SET threads TO 1` is a planning difference keyed on the door.** Test sessions now run as product sessions do
  not. Item 1 asks the user's word on it. The body should also say two things:
  - the product's DuckDB sessions keep G10's gap, which of two failing expressions raises (for example two failing
    projected columns), and the test lanes can no longer see it;
  - the LocalFile case in item 1 above.
- **S2. Name casts in (b1)'s gap.** `raisesItself` leaves casts out, by measurement. So a text-to-number cast that
  raises in a later member (a `parseFloat` of a column, if it lowers to a `Cast`) is blamed on its group's first
  verdict, as a refused literal is. The body names only the literal.
- **S3. Tie the guard's prefix to the class** (item 5 above).
- **S4. `LiteText.toTheMinute` appends `:00` to any text without a colon.** An hour-precision text carrying a zone
  with no colon, such as `2015-04-16T14+0000`, would become `2015-04-16T14+0000:00`. Show that the carrier is always
  zone-free, or add a plant with an hour and a zone.
- **S5. The "compiler's Integer folds" finding should also list `FeatureRules.CORRECTED_SUBSTRING`.** It folds
  `start + 1` and `end - start` for `substr` today, and for every `substring` once A3 lands (my rules check, §7 above).

**CI:** pending. The PR was opened at 17:56 UTC.

### PR #127 re-check, head `6f21bed29` (~18:35 UTC)

**Verdict: APPROVED.** The must-fix and all five should-fixes are met. CI on `6f21bed29` is pending.

- **The MUST-FIX is met** (`b08ec103e`, an ordinary merge of #124's combined branch at `f832a6671`).
  - `PctPlanRoute.audience` is one selector of three: `LEGEND_PURE` (the default), `LEGEND_ENGINE`
    (`-Dpct.audience=engine`) and `LITE` (`-Dpct.channela.rules=LITE`).
  - Any other value, and `engine` together with `LITE`, is refused by name, and `PctPlanRouteAudienceTest` plants it.
  - `RouteAudience.planned(audience == LEGEND_ENGINE && features.isEmpty())` is kept, so `AudienceCheck` still sees
    every engine case.
  - Checked by script: the branch now changes 153 files against `main`. That is the union of #127's 49 and the combined
    branch's 105, plus 2 new test files. Every combined-branch file outside #127's own is byte-identical to
    `f832a6671`'s. Three files were touched by both sides:
    - `PctPlanRoute.java`, resolved as above;
    - `gates/BUILD.bazel`, which adds only `//pct:pct_duckdb_lite` to `heavy`;
    - `pct/BUILD.bazel`, auto-merged.
- **Item 1 / S1, LocalFile: met** (`032b8f3ef`). `Targets.forTest` writes `testSessionSetup` only where
  `Targets.madeInProcess(database)`. That is the predicate the same class already uses for a database's settings: a
  platform, the system catalog or a connection's in-memory database. A declared file keeps its own `threads`. It is
  planted in `PlanMakerDuckDbTest`.
- **S2: met.** `VerdictBatch.raisingRuns`' gap now names casts as well as literals.
- **S3: met.** `ChannelBLedgerTest` checks that `ChannelBLedger.DISAGREEMENT` equals
  `Class.forName("com.legend.testexec.IsolatedRun$JudgesDisagree").getSimpleName() + ": "`, so a rename fails the test.
- **S4: met by construction.** `AssertBarTest` plants it: a zoned minute is normalised to GMT at parse, and a zoned
  hour is refused at parse. legend-pure's `DateFormat` refuses `14+0100` too. So no zone ever reaches `toTheMinute`.
- **S5:** in the body, as words.
- **Item 3: the owner's ruling, accepted.**
  - `RootLoops.Run.raiseRejection` replaces the `IllegalStateException`. Where the judged segment raised nothing, the
    held rejection fails the body with the database's own error. That is a loud failure with the true cause, not a
    fallback.
  - The argument that it is unreachable holds: `judge` walks every root, `received` refuses a non-member, and a member
    read with no row raises first.
  - `RootLoopsRejectionTest` covers the reachable path and `raiseRejection` itself.
  - One cost, noted: if a runner defect ever reached it, it would read as an ordinary database failure rather than as
    a defect.

## A3 piece 4: the differences page, the A3 register rows, the rulebook amendment (docs only), reading copy `0eacaecdc`

**Verdict: APPROVED WITH 2 MUST-FIX, 6 SHOULD-FIX, 5 NITS.** The rulings as they stand now are stated right: the
detector refuses by default, `warn` is an operator's setting, and no load-time report is promised (~19:20). D1 and D2
match ~13:25. Every number I checked matches its record. The two must-fixes are each a few words or a few lines: one
attribution, and one classification that hides three answer differences.

**What was read.** `origin/claude/census-b2` at `0eacaecdc`, folder `docs/records/compat-homework/A3-piece4/`:
- the patch (`git diff 6551c2036 a0505ebe6`);
- the page whole;
- the README.

The patch applies cleanly to A2's head (`origin/claude/compat-a2-printers` = `6551c2036`). The page in the folder is
byte-identical to the patched file. The rules-of-record copy is byte-identical to `claude/compat-program`'s (blob
`1888321e9`).

**Checked against:**
- the owner's log §1, rows ~12:30 to ~19:20, `claude/compat-program`;
- the final plan §4, §4a and §5;
- `compat-batch-register-rows-2026-11-19.md`, the ids;
- H8 (a.3), (h), (j) to (j.5) and (k);
- `A2-roster.tsv` and `A2-shape-note.md`;
- `A3-shape-note.md` rev 3, `a3/corpus-a3-after-a2.txt` and `a3/{h2,duckdb}-literal-zero.tsv`, on
  `claude/compat-translator-spike`;
- H7, and B1-R's oracle (`engine_{h2,duckdb}_answers.tsv`), on `claude/compat-engine-lane`;
- #123's overlays;
- legend-pure 5.99.0 at the pin: `substring.pure:53`, `indexOf.pure:39`, `divide.pure`.

### What holds

**AGENTS.md:**
- There is one hunk (`@@ -91,18 +91,56 @@`), inside the tenet preamble, before the world-map tenet.
- Invariants 1 to 8 and 3a are unchanged, as are their numbers, the core/README note, the standing documents and the
  common mistakes.
- The amended tenet is the ~13:15 draft that the ~13:25 row names, plus the case-by-case clause, the engine runner's
  README and the page's pointer.
- The six tenets and the two readings are the rules of record's text.
- "Adopt tenets yes" is the user's words (~16:40).

**G8:** the first bullet is the ~13:15 draft's sentence, and it keeps the old wording as history. The other bullets
are unchanged.

**Register rows:**
- The S382 to S385 ids are the owner's allocation of ~19:21, each with its drafter's subject.
- S130 and S134 are rewritten in place, with their ids kept.
- All six are marked PROPOSED for the user's word.
- S134 quotes ~19:20 exactly: "I think it has to be refuse then for now".
- S130's literal-0 exemption matches the TSVs:
  - H2 2.1.214 reads `substring('abcdef', 0, 3)` as `'abc'`, in both modes;
  - DuckDB 1.5.6 reads it as `'ab'`, and its 2-argument form as `'abcdef'`.

**The detector on the page (item 1):**
- it refuses by default, in legend-engine's envelope, naming the call;
- `LEGEND_LITE_PURE_MEANING=warn` is an operator's choice, with the log line and the header;
- the corrected option passes `substring` only;
- no report is promised.

This matches ~19:20, and the final plan §4a's struck line.

**Numbers, all matching their records:**
- **Item 1:**
  - 7 corpus tests per database: 4 answers and 3 hidden text tests (`a3/corpus-a3-after-a2.txt`);
  - 63 PCT rows, 3 tests on 21 lanes;
  - 62 projects (the log, the ~11:00 measurement).
- **Item 2:**
  - 147 and 152 tests move: H2 64+20+52+3+2+6, DuckDB 64+22+53+4+3+6 (`A2-roster.tsv`);
  - 150 and 155 with R4's 3;
  - 2,761 declared tests (H8 (k.2));
  - the 15 requests (`A2-shape-note.md:128`).
- **Item 3:** every row matches H8 (j.1) and (j.3), and B1-R's oracle:
  - DuckDB divides to 200 `null`;
  - H2 divides to 500;
  - H2's `sqrt` is 200 `null`;
  - DuckDB's `sqrt` is 500.
- **Item 4:** 2 PCT answers per database, and the 2 H2 corpus gains (52 and 1 against `52.0` and `1.0`).
- **Item 5:** legend-engine answers 200 `null` on both databases (the oracle's `raise.toOneOfEmpty`).
- **Item 6:** the counts match the final plan's B4.
- **Item 7:** the 1 test, `testIsolationOfPropertyTargetFilter` (H8 (h)).
- **Item 8:** 7 tests set a flag. The one that needs the legacy flag, `testLegacyFlagProjectionEmitsPlainEquals`, is
  class a2 in A2's roster (SQL text only).
- **Item 10:** 99 of 110, 109, and 28 of 36 (H7:37-61).
- **Item 11:** Windows has 17 rows and 30 lines, macOS 16 rows and 31 lines. The functions are as listed.
- **The PCT table:**
  - DuckDB 1,212 and 1,005 (H7);
  - H2 Relation 411+28+9;
  - H2 Essential 125+26+101 and 125+6;
  - H2 Standard 107+6 and 107+27 (H8 (j.4)).

**Stages:**
- 3, 4, 6 and 7 are B4;
- 8's flags are B5 ("every legend-engine flag but the two it reads");
- 9's words are B1.

Each matches the final plan §5.

### MUST-FIX

**M1. Words quoted as the user's that the user did not say.**
- **The D1 label.** S130 and S134 read `the user's D1: "Pure's way, LOUD, no translator"`. That label is this
  program's (the log's ~13:25 row; the phrase "Pure's way, LOUD where it differs" first appears in the program's own
  analysis, log :794). The user's words at ~13:25 were "Okay agree with all", to the owner's message.
  - **Fix:** `D1, ruled by the user 2026-11-19 ~13:25 ("Okay agree with all"): Pure's way, loud, no translator`.
- **The goal's sentence.** It is the program's wording (log ~12:30, the ~13:15 draft), ruled by the same "Okay agree
  with all". AGENTS.md, G8 and the page each introduce it as `the user's ruling ...: "Lite follows Pure. ..."`. That
  reads as the user's own words in the rulebook.
  - **Fix:** the same pattern: `the goal the user ruled on 2026-11-19 ~13:25 ("Okay agree with all"): Lite follows
    Pure. ...`.
  - The sentence itself is right, and it is ruled.

**M2. Item 2 presents three answer differences as text-only.** The page says 147 tests on H2 and 152 on DuckDB "stop
counting, because they check only the text". `A2-roster.tsv` classes them differently:
- **class b, 2 on H2 and 3 on DuckDB:** "a DIVERGED appeal: the rows differ, an answer the text hid". The tests are:
  - `sqlstring::testHashFunctions`;
  - `sqlstring::testToSQLStringForTDSStringJoin`;
  - `sqlstring::testToSqlGenerationFirstDayOfWeek`, on DuckDB only.

  A2's shape note calls them "the answer differences F1 reported".
- **class c, 6 per database:** legend-engine's plan structure (`supportsStream`), not text.

The page exists to tell a migrating team where answers change, so these three cannot be hidden under "text".
- **Fix:**
  - split the count (H2: 139 text, 2 answers, 6 plan structure; DuckDB: 143, 3, 6);
  - list the class-b answers, as an entry, or under "Gaps" if they are lite's defects. `firstDayOfWeek` is lite's own
    lowering against legend-engine's;
  - cite `A2-roster.tsv`.
- S380, A2's row and not in this patch, says "text alone, or a plan structure" and has the same gap. Fix it with this.

### SHOULD-FIX

**S1. "What is the same" mixes stage B into today, and leaves out a ruled difference.**
- **(a) It is stage B, not today.** "Only the output's formatting happens in Java: legend-engine's printed tables,
  envelopes and error sentences" describes the state after B1 and B2. Today the tables are built in SQL (the 791-line
  `RelationTextLowering`), and an error reads lite's code (item 5's `Raised: SIZE_NOT_MULTIPLICITY`). Mark it "after
  stage B".
- **(b) Two separate claims are joined.** The 431 of 431 and 342 of 342 are H11's printed-table prototype for B2
  (legend-pure's relation text, where `toString` is the program's last step). They are not about "the legend-engine
  server routes Studio uses", whose envelopes the pure/v1 net measures. Make them two sentences.
- **(c) A ruled difference is missing.** By the ~18:12 ruling ("Yes record it, and count the nested cases"), a
  relation's text used further inside a query (`->toString()->length()`) answers lite's own text. The ~18:05 and
  ~18:36 rulings say what that text is: whole Floats as `2.0`, a date with a time as `...000000Z` (legend-pure prints
  `+0000`), an empty cell as `null`. It is a recorded difference that lands with B2, like items 3, 4, 6 and 7.
  - **Fix:** add it as a stage-B entry, "count pending (the nested cases are counted before B2)".
  - Qualify "byte for byte" to the case where the text is the program's last step.

**S2. The register's own header still states the old tenet.** `docs/SEMANTICS_REGISTER.md:6-9` at A2's head reads, in
bold: "Since 2026-10-29 (the user's upstream-fidelity tenet, AGENTS.md): lite does exactly what legend-pure and
legend-engine do wherever possible...". The patch edits this file and amends that tenet, but leaves the header. The
ledger the tenet cites would then contradict it.
- **Fix:** amend the sentence in the same patch, with a dated note pointing to the amended tenet.
- `docs/TOTAL_WALL_START_HERE_2026_11_11.md:187` ("Ask 'can we do exactly as pure/engine does?' first") sits in
  AGENTS.md's live table. Add a dated pointer there, or rule it history (the owner's call).
- `docs/RESULT_BOUNDARY_START_HERE.md:756` is history.

**S3. Date and complete the PCT table, and the gaps count.**
- **(a) Date the table.** Its numbers are B1-R's ENGINE lane before stage A (H7, H8 (j.4) and (j.5) at `10b311dee`).
  A3 itself moves 12 more to PCT's answer (7 on DuckDB, 5 on H2: the final plan's A3 row). Say "measured before
  stage A".
- **(b) Two H2 suites are left out.** H8 (j.4) measured Grammar and Unclassified once (on a widened, unpinned lane):
  - Grammar: lite 119, legend-engine 77;
  - Unclassified: lite 73, legend-engine 82.

  So lite is behind on Unclassified too, and "fewer on H2 standard" alone is incomplete. Add the two rows marked
  "measured once, not pinned", or say the table covers the pinned suites only.
- **(c) The same goes for the gaps section's "33 PCT tests on H2".** That is the pinned suites' LITE_BEHIND (Essential
  6, Standard 27); Unclassified adds 10.

**S4. Item 3's "PCT ... expects the error in every case" does not hold for `x / 0`.** legend-pure's `divide.pure` has
six PCT tests and none divides by zero. I did not read here whether legend-pure raises on it. Its interpreted
`Divide.java:53` hands the work to `NumericAccumulator.divide`, which I did not follow down.
- **Fix:** say that PCT expects the error for the first five rows, and cite the source of Pure's raise for divide
  (or ask the engine runner).

**S5. Item 1's recipe for legend-engine's answer holds in range only.** `substring($a - 1, $a - 1 + $b)` reproduces
legend-engine's `substring($a, $b)` only when `$a >= 1` and `$a - 1 + $b <= length`:
- at `$a = 0`, H2 reads 0 as position 1 (`h2-literal-zero.tsv`), while the rewrite starts at -1 and raises Pure's
  error;
- past the end, H2 clamps and Pure raises.

Say so in one clause, and give the 2-argument form, `substring($a - 1)`.

**S6. AGENTS.md's honesty marks.**
- **(a) "Each has a test, so it cannot quietly erode."** AGENTS.md marks enforcement honestly (`[ENFORCED]` and
  `[CONVENTION]`). The tenets' "tests" are questions a reviewer asks: nothing turns red until B5's guards. The words
  are the adopted text, so keep them, and add `[CONVENTION until B5's guards]` after the heading.
- **(b) The numbering.** The tenets add a third numbered list beside invariants 1 to 8 and core/README's 12, and the
  new rows cite "(tenet 1)". AGENTS.md already keeps the other two lists apart with a note. Add one line here too:
  cite them as "tenet N of the six", never as "invariant N".

### NITS

- **N1.** "Floats are doubles, as in legend-engine" sits beside item 4, where legend-engine computes a Float literal as
  a decimal. Qualify it: "a Float value read from the database".
- **N2.** Item 9's "Neither carries a source position" is not quite right. legend-engine's `raise.toOneOverMany` answer
  carries `Execution error at (resource:/core_relational/.../pureToSQLQuery.pure line:3136 column:188)` (the oracle).
  That is a position in legend-engine's own Pure, not in the user's code. Say "neither points into your code".
- **N3.** Item 10's "109 wrong answers fixed": H7's LITE_FIXES is "legend-engine fails by a defect", which counts
  planner and runtime faults too. "109 failures fixed" is accurate.
- **N4.** Item 1 says the door "refuses a query ... that calls either one". The detector flags the 2-argument String
  `indexOf` only. The 3-argument one is refused by legend-engine (the `testFromIndex` manifest row, so item 10's
  class). Add one clause.
- **N5.** Item 1's example, `'abcdef'->substring(1, 3)`, is a bare expression, but the difference is in code sent to
  the database: §4a's and S134's "store-routed". Write it as a column, for example `$p.name->substring(1, 3)` inside a
  `project`. I did not read how legend-engine answers a bare constant expression, so this is a wording point only.

### Outside this piece, for the owner

**O1. The ~19:20 ruling is struck in only one place.** The final plan's §4a still reads:
- "`indexOf` always warns";
- "What it does: a server log line ... under `=refuse`, a refusal";
- "Its tests: a planted request warned".

The A3 row in §4 still lists "the detector's default (ruled: warn, with the report)" and "6 to 6.5 days with the
detector's report". `compat-design-and-plan-plain-2026-11-19.md:77` still says "Warned at the legend-engine door ...
with a report of every such call when a project loads".

I will review the detector PR against refuse-by-default, with no report: a planted request refused, `warn` marked,
the option quiet for `substring` alone.

### Re-check 1: B2's fixes at `c957c2b49` (reading copy `2d16f6d54`) and the owner's O1 (`2330eb98`, `1bae0174`)

**Verdict: interim. Every item of mine is met at `c957c2b49`.** The verdict waits for B2's next commit, which carries
the owner's two additions. That commit is not in `c957c2b49`:
- **`firstDayOfWeek`, stated as on the merged batch.** The page's item 13 and S387 still say "lite on H2 still answers
  legend-engine's Sunday", with a Gaps entry.
- **legend-pure's `x / 0`, read to the end.** Item 3 still says "still to be read".

**How this was checked:**
- Both patches were applied to A2's head (`6551c2036`) and the two trees diffed.
- The page copy is byte-identical to the patched file.
- It now touches six files; the new sixth is `TOTAL_WALL_START_HERE`'s dated pointer.

**Met:**
- **M1:**
  - AGENTS.md's tenet heading, G8, the page and the register's header now say "the goal the user ruled ~13:25 ...
    'Okay agree with all'; the sentence is the program's wording".
  - S130 and S134 say "D1, ruled by the user 2026-11-19 ~13:25 ("Okay agree with all"): Pure's way, loud, no
    translator".
  - S383's "(D1: Pure's way)" carries no quotation marks.
- **M2:**
  - item 2 splits the count: H2 139/6/2, DuckDB 143/6/3, matching `A2-roster.tsv`'s classes a1-a4, c and b;
  - S380 says the same;
  - two new entries, 12 (`joinStrings`, S386) and 13 (`firstDayOfWeek`, S387). The ids are the owner's allocation
    (`09c58581`).
- **The new rows' citations, read at the pins:**
  - legend-engine `testToSQLString.pure:754-771`: the `concat(..., '|')` and `'[', ',', ']'` forms, as quoted;
  - `dateExtension.pure:202-205`: `mostRecentDayOfWeek(DayOfWeek.Monday)`;
  - legend-pure's `joinStrings` puts the separator between the strings;
  - the GATES.md batch-139 entry exists.
- **S1:**
  - "What is the same" now says "after stage B", and that today a table is built in SQL;
  - Studio's envelopes and H11's tables are separate sentences;
  - item 14 records the nested table text, stage B, count pending. Its three quotes are the user's words (log
    ~18:05, ~18:12, ~18:36), and its spellings match the rulings.
- **S2:** the register's header carries a dated amendment, and `TOTAL_WALL_START_HERE:187` a dated pointer.
- **S3:** the table is dated "before stage A", with the H2 Grammar (119/77) and Unclassified (73/82) rows marked
  "measured once, not pinned", the 33 split as 6 + 27 (+10 Unclassified), and A3's 12 more.
- **S4:** partly met. The first five rows are PCT's, and `x / 0` has no PCT test. legend-pure's answer is still "to be
  read", which is the owner's second addition.
- **S5:** "in range only", with the start-0 and past-the-end cases and the 2-argument form.
- **S6:**
  - `[CONVENTION until B5's guards]`;
  - "the tests are questions a reviewer asks";
  - "cite them as 'tenet N of the six', never as 'invariant N'". The new rows now say "tenet 1 of the six".
- **N1-N5:** all met.

**Nits:**
- S386 cites `joinStrings.pure:23`, but at the pin the rule is the doc block at `:18-21`, and line 23 is
  "**Parameters**".
- S380 is A2's row, fixed here because piece 4 is cut from A2's head. Fine, provided A2 lands first.

**For B2's next commit: `firstDayOfWeek` on the merged batch moves a count.** A5's `7cd59a57` makes H2 truncate to
`iso_week`, so H2 answers Monday, as DuckDB does. It moved no corpus roster on its own, because the engine-text printer
still judged the test by its text. Once A2 removes that printer, the H2 test `testToSqlGenerationFirstDayOfWeek` is
judged by its rows. Lite answers Monday and the golden says Sunday, so the row compare diverges on H2 too, as it does on
DuckDB.

If that holds, then on the merged batch:
- H2's class b is 3, not 2;
- the H2 counts are 148 and 151, not 147 and 150;
- the H2 fail roster carries that test;
- item 13 and S387 read "both databases".

This is my reading, not a run: the batch's H2 corpus lane settles it. Whichever way it comes out, the page's numbers
and the batch's roster must agree.

**O1:** mostly met.
- `2330eb98` and `1bae0174` state refuse-by-default with no report in §4a's "When it is quiet", the report bullet
  ("NOT BUILT ... history"), "Its tests", the A3 row's size cell and the plain plan's row 1. Two phrases remain:
  - the A3 row's "user's words it needs" cell still ends "the detector's default (ruled: warn, with the report)";
  - §4a's "What it does" still describes the warning as the default, and the refusal as `=refuse`'s.

### Re-check 2: `5b337b867` (reading copy `febd1adee`, with `A3-piece4-since-review.patch.txt` and `first-day-of-week-probe.md`)

**Verdict: APPROVED, with one should-fix outstanding (`x / 0`, below, with the reading supplied) and two conditions at
the batch.** Nothing blocks the PR.

**How this was checked:**
- The whole patch applies to A2's head (`6551c2036`): six files.
- The page copy is byte-identical to the patched file.
- Against `c957c2b49`'s tree, only the page and the register differ, as the README says.
- The stated delta applies to the previous tree.

**`firstDayOfWeek` (item 13, S387): the owner's correction holds.** I read each citation at legend-engine 4.145.0's pin:
- `relationalMappingExecution.pure:440-443`: `useDialectTranslation` is true for `DatabaseType.H2`.
- `sqlDialectTranslation/toPostgresModel.pure:356`: `firstDayOfWeek` is `dateTruncCall('week', true)`, and
  `dateTruncCall` is at `:612-619`.
- `testToSQLString.pure:650` pins `cast(cast(date_trunc('week', ...) as timestamp) as date)` for H2.
- `h2Extension2_1_214.pure:223` and `h2Extension1_4_200.pure:118` hold the `dateadd` text, which is not on the SELECT
  route.
- `duckdbExtension.pure:228` holds the `isodow` text.

The H2 extension's text is the wrong place to read legend-engine's H2 answer: its SELECTs go through the dialect
translation.

**The locale probe is sound.** The sources and verbatim outputs are in the record, on H2 2.1.214 and 2.5.252 and DuckDB
1.5.6.0, under three locales.

**Lite's tests are deterministic, so the referee's `2014-11-30` is stable.** Every test JVM pins `en_US`:
- `tools/junit/defs.bzl:53-54`;
- `spec/BUILD.bazel`'s four corpus targets;
- `tools/java_run/defs.bzl:87-88`.

**A5's `7cd59a57` (`iso_week`) is in #127's approved head `6f21bed29`**, so lite answers the Monday on every database
in this batch.

**S386 and S387 now cite the user's own rulings, read in GATES.md at A2's head:**
- batch 67b, the user 2026-09-05: joinStrings and firstDayOfWeek "are both just broken on H2 basically? …
  quarantine/bucket those as engine bugs";
- batch 144, the user 2026-09-08: "i agree with all 11", with `digest-joinStrings`, `joinStrings-literal-collection`
  and `firstDayOfWeek-h2-week-start` among them.

So these two rows are user-ruled meanings. The cited `7dfbdf601` is real: "Block-compiler rung 2a: text asserts are
verdict rows", 2026-09-21, where a text byte-equal to the golden became the verdict. That bears out the "passed on its
SQL text alone from 2026-09-21" sentence.

**Outstanding, should-fix: legend-pure's `x / 0`.** Item 3 still says "legend-pure's own answer is still to be read".
That was the owner's second addition, and it is not in `5b337b867`. Here is what the pin says:
- **Compiled legend-pure** raises for any zero divisor: `CompiledSupport.divide:1481-1486`,
  `PureExecutionException(..., "Cannot divide " + right + " by zero")`.
- **Interpreted legend-pure**, through `NumericAccumulator`:
  - with a Float or Decimal operand, BigDecimal's division raises an `ArithmeticException` (`BigDecimalWrapper.divide`,
    `:893-895`; a Float literal is a `BigDecimal` there, S120);
  - an Integer divided by an Integer 0 goes `LongWrapper.divide(long)` (`:1562-1565`) → `DoubleWrapper` `this.value /=
    number` (`:1170-1175`, "TODO check for underflow and overflow"), so it answers Infinity, with no raise.

So "Pure's error" in the table holds for compiled legend-pure, and for interpreted legend-pure with any Float or
Decimal. Interpreted legend-pure's Integer-by-Integer zero answers Infinity, a value Pure's Float has no spelling for.
- **Fix:** replace the clause with one sentence saying that, citing both. Lite's raise follows the compiled runtime,
  where legend-pure's tests run their asserts (S127).
- This does not move a verdict.

**Conditions at the batch:**
1. **The H2 count for `testToSqlGenerationFirstDayOfWeek`.** The merged batch's H2 corpus lane settles whether the test
   moves on H2 now that lite answers the Monday there. Item 13 rightly defers to that run. If it moves, item 2's H2
   figures ("2 on H2"; 147 and 150) and S380's must be updated in the same batch, so the page and the roster agree.
2. **The user's word on the PROPOSED rows** (S130, S134, S380, S382-S385) at the PR, as each row says. S386 and S387 are
   ruled meanings with drafted rows.

**Nits:**
- S386 and item 12 still cite `joinStrings.pure:23`. The rule is the doc block at `:18-21`.
- Item 13's "When: today" holds only with this batch. One clause would make it exact: "with this batch: A5's H2 fix,
  in #127; before it lite's H2 followed the locale too", as S387 already says.

**Observation, for the product track: lite's own H2 still follows the locale for week numbers.** The probe found this
on the way. On lite's H2, `extract(WEEK ...)` and `DATEDIFF(WEEK, ...)` follow the JVM locale: under `en_US` a Sunday
is week 49, under `en_GB` week 48. Tests cannot see it, because every test JVM pins `en_US`. A product server started
outside those launchers would answer by its machine's locale, and DuckDB's week is ISO. It is lite's defect, not a
difference by design. It needs an owner and a record (IN_FLIGHT or an issue) so it is not lost.

### Re-check 3: `9dc385235` (reading copy `f6dc36008`, with `divide-by-zero-probe.md`), sent by B2 directly

**Verdict: APPROVED. S4 is closed, and every item of my review is met.** Two conditions remain for the batch, and one
nit is left.

**How this was checked:**
- The whole patch applies to `6551c2036`: six files.
- The page copy is byte-identical to the patched file.
- Against `5b337b867`'s tree, only the page and the register differ:
  - item 2's H2 figures and S380 are hedged to the batch's lane;
  - item 3's `x / 0`;
  - the `joinStrings` citation in item 12 and S386.

**S4, legend-pure's `x / 0`: met.** Every citation was read at the pin, and the probe's outputs agree:
- **Compiled:** `CompiledSupport.java:1481-1486` raises "Cannot divide <divisor> by zero". legend-engine runs Pure in
  this mode: `PureModel.java:149` and `:206` build a `CompiledExecutionSupport`.
- **Interpreted, Integer by Integer 0:**
  - `Divide.java:56` makes the double result a Float by name;
  - `ModelRepository.java:281-287` → `newFloatCoreInstance` at `:547-550` → `new BigDecimal("Infinity")`, which
    throws `NumberFormatException`;
  - the path is `NumericAccumulator.java:1562-1565`, then `:1156-1161`.
- **Interpreted, with a Float or Decimal operand:** a `BigDecimal` division (`PrimitiveUtilities.java:80-83`), which
  raises "Division by zero" or "Division undefined" (`NumericAccumulator.java:895`).

So legend-pure raises in both modes, and item 3's "Pure's error" holds for every case.

**Correction to my own re-check 2.** I wrote that interpreted legend-pure answers Infinity for Integer by Integer 0,
"with no raise", citing `:1170-1175`. That was read short:
- the path is `divide(double)` at `:1156-1161`;
- the value then fails `new BigDecimal("Infinity")`.

B2's reading is the right one. The page states it correctly.

**The H2 count: the page and S380 now read "147 or 148" (and "150 or 151"), settled by the merged batch's H2 lane.**
That meets condition 1 in form. The condition stays until the judges' number replaces the "or", before the batch
lands.

**The `joinStrings` citation: met.** S386 and item 12 cite `essential/string/toString/joinStrings.pure:18-21`. B2 adds
that `:23` was the 4-argument native's declaration in the other `joinStrings.pure`, under `grammar/`.

**Still open:**
- **Condition 2:** the user's word on the PROPOSED rows at the PR (S130, S134, S380, S382-S385).
- **Nit:** item 13's "When: today". Lite's H2 answers the Monday only with A5's fix in this batch. One clause would make
  it exact.

**For the A2 review, from B2's message.** B2 reports that A2 put the user-accepted divergences (batch 144's rule: "a
fourth status beside PASS / FAIL / SKIPPED") on the fail rosters rather than the accepted rosters. I will check that in
A2's PR. If it holds, it is a must-fix there: the user ruled those rows a status of their own on 2026-09-08.

## A2 (piece): legend-engine's SQL text printers deleted, `origin/claude/compat-a2-printers` at `6551c2036` (7 commits on `main` `b75d6c73a`)

**Verdict: APPROVED WITH 1 MUST-FIX** (the user-accepted divergences), plus the listed moves for the user's word.
Checked against:
- the final plan §4 (A2);
- the rules of record;
- A2's section (`docs/records/compat-batch/A2-section.md` on `claude/compat-program`);
- `A2-roster.tsv` and `A2-shape-note.md` on `claude/census-b2`.

### What holds
- **The printers are deleted.** That covers `EngineStyleH2` (2,507 lines), `EngineStyleDB2`, `EngineStyleComposite`,
  `EngineTextDialect`, `PureText.engineText` and `enginePlanText`, `EngineShape` and `RelationalRootForm.apply`. The
  whole diff is 91 files, +2,177/-6,141.
- **The product changes are comments plus the dead `relationalRootForm` flag** on both `Compiler.lowerResolved`
  overloads. Every caller passed it false. `PlannerSurfaceTest` follows.
- **`edge/LiteSqlText` is package-private and computes no value.** It renders the plan's SQL through
  `PureText.dialect(type)`. Each legend-engine-only question gets its plainest answer, or is refused by name (the
  FreeMarker splice). No rule of record is touched: SQL text is a statement's presentation, not a value.
- **The fail rosters are exact.** H2 +147 and DuckDB +152, 0 removed, and the added names equal `A2-roster.tsv`'s lane
  columns. R4's three are left out, as they should be.
- **The corpus edge net's judge ledgers** move on 156 tests (H2) and 160 (DuckDB): 147 and 152 on the roster, plus 9
  and 8 already failing at `main`. None outside, as the section says.
- **Every guard move carries its dated note.** I read each diff:
  - ArchitectureTest's 4d rules went with their subjects, and the planner entries shrink;
  - CodeShapeGuardrailTest −2;
  - DialectBoundaryTest 17→12 and 20→13;
  - DialectOwnerExclusivityTest: the crossing left, and the chain retargeted to MariaDb;
  - DialectVocabularyTest's floor 15→12 (the three printers);
  - JavaEvalLedgerTest: EnginePlans 760→757;
  - ParkedWorkLedgerTest: PARK-18 closed;
  - CompatSpellingGuardTest: the plant moved;
  - PeRatchets 3310→3311, UP.
- **AGENTS.md changes only invariant 3's prose** about the printers. No number moves.
- **For the user's word, as the section lists:**
  - the closed edge-file list in JavaEvalLedgerTest gains `LiteSqlText.java` (FLAGGED there);
  - S380;
  - the 12-dialect widening;
  - the dead flag;
  - every guard row.

### MUST-FIX: the user-accepted divergences go to the FAIL rosters, not the accepted rosters
**The ruling.** On 2026-09-08 (batch 144, "i agree with all 11") the user ruled that an ACCEPTED divergence is "a fourth
status beside PASS / FAIL / SKIPPED — never a pass, never hidden". It is pinned in
`{duckdb,h2}-accepted-roster.txt` as `fqn ||| bucket ||| witness`. MinimalCorpusTest checks the witness: a failure
without it is an ordinary FAIL, "CHANGED".

**How these rows left that roster.** On 2026-09-21, rung 2a (`7dfbdf601`) took four of those rows off it, because their
SQL text then held and they passed: "accepted rosters −3 / −1 (text held → pass)". Read in that commit's patch:
- DuckDB:
  - `meta::relational::tests::functions::sqlstring::testHashFunctions ||| engine-golden-defect:joinStrings-literal-collection ||| aceae9411805b964e79dd85254a18a64`
  - `meta::relational::tests::functions::sqlstring::testToSQLStringForTDSStringJoin ||| engine-golden-defect:joinStrings-literal-collection ||| Anthony Allen[,]`
  - `meta::relational::tests::functions::sqlstring::testToSqlGenerationFirstDayOfWeek ||| engine-golden-defect:firstDayOfWeek-h2-week-start ||| 2014-11-30`
- H2:
  - `meta::relational::tests::functions::sqlstring::testToSQLStringForTDSStringJoin ||| engine-golden-defect:joinStrings-literal-collection ||| Anthony Allen[,]`

**What A2 does with them.** A2 stops comparing the text, so these tests are judged by their rows again and diverge (class
b of `A2-roster.tsv`). A2 adds them to the fail rosters, and the accepted rosters are untouched. The failures carry the
ruled witnesses (B2's `census-b2-sqltext-probe-2026-11-19.tsv`, the first 300 characters of each failure):
- `testToSQLStringForTDSStringJoin`, both lanes: golden-only `...Anthony Allen[,]|myValueAnthony Allen[,]...`. The
  witness is present.
- `testToSqlGenerationFirstDayOfWeek`, DuckDB: golden-only `[2014-11-30, ...]`. The witness is present.
- `testHashFunctions`, both lanes: golden-only `[20f1aeb7819d7858684c898d1e98c1bb|c3f15d27...|3090ad7f...`. The record's
  300 characters do not reach the ruled witness `aceae941...`. Running the lane settles it.

**Fix:**
- Restore the DuckDB rows and the H2 row above verbatim, and take those tests off the fail rosters: DuckDB −3, H2 −1.
- H2's `testHashFunctions` passed on H2 at batch 144, so it was never accepted there. It now fails on H2 with the same
  ruled divergence. An H2 accepted row for it is new and needs the user's word, with its witness read from the H2 lane.
  The same holds on the merged batch for H2's `testToSqlGenerationFirstDayOfWeek`: with A5 it diverges on H2 with
  `2014-11-30`.

**The counts that follow:**
- A2's fail rosters: H2 +145 (or +146 if H2's `testHashFunctions` stays a FAIL until the user rules); DuckDB +149.
- The accepted rosters: H2 41 → 42 (43 with `testHashFunctions`); DuckDB 46 → 49.
- `docs/GATES.md`'s A2 entry, S380, and A3 piece 4's item 2 ("answers the text had hidden") follow. Those are
  accepted divergences, ruled 2026-09-05 and 2026-09-08, not new FAILs.

The owner reports the user is being asked. If the user rules them FAILs instead, that ruling is the dated note those
rows need.

### Not shown here
- CI's other operating systems, and the lanes CI runs alone, as the section says.
- The merged batch, which the batch PR carries.

## A4 (piece): the database judge leaves compatibility, `origin/claude/compat-a4-host-only` at `e91005cc6` (6 commits on `main` `b75d6c73a`, before the batch merge)

**Verdict: APPROVED, with 3 should-fixes and a proof gap that the batch's merged run closes.** Checked against:
- the final plan §4 (A4);
- rule 4 of the rules of record ("Host judge is only judge in compatibility, database and host both in product");
- tenet 6 (no modes in the core).

### What holds
- **The guard.** `TestPlanner.refuseTheDatabaseJudgeInCompatibility` refuses a DATABASE or BOTH judge for any style but
  `PlanStyle.PRODUCT`, by name, before the style is asked anything. It is the first statement of the one public entry,
  `TestPlanner.plan`, and that is the only entry `TestExecution` (`:47`, `:71`) uses. The package-private overload and
  `planIsolated` are reached after it.
- **The plant.** `HostOnlyCompatibilityTest` plants a style that answers exactly as the product's but is not it:
  - under HOST it plans;
  - under DATABASE and BOTH it is refused, with the message checked;
  - the product plans every judge.

  The commit says it was shown to bite.
- **Every compatibility road is judged by the host:**
  - PCT channel B sets `JudgeMode.HOST`;
  - the `pct_channel_b_host_*` duplicate lanes are deleted;
  - `core_edge_tests_judge_{database,both}` are deleted;
  - MinimalCorpus's `both` mode is refused by name.

  No CI workflow names a deleted target.
- **The database judge's compatibility arms are deleted.** That covers the six two-form kinds (`CrossKindNumbers`,
  `VerdictNumbers`, `RenderedTextRule`, `DateLiteralTexts`, `GoldenNullWord`, JsonGrammar's planning read), the appeal
  path and `orElse`. The product's arms stay inline. Fewer forms in the core is tenet 6's direction. The host judge
  keeps legend-pure's rules until stage B (B3).
- **The rosters are byte-identical across A4.** No roster, register, edge-net ledger or PCT ledger file changes between
  `b75d6c73a` and `e91005cc6`. Only `lean-sql-baseline.tsv` moves.
- **Every guard move shrinks.** I checked ArchitectureTest, OpenClassMembersTest, HookLambdasTest, SqlFnSortedTest,
  EdgeSeparationTest, EdgeOnlyMembersTest and HookRulingsAgreeTest: the only added non-comment line re-terminates an
  existing entry. Each move is dated, and the JavaEvalLedger pins shrink with notes:
  - DatabaseJudge 852→808;
  - AssertVerdicts 1465→1363;
  - SqlTextVerdicts 1146→1086;
  - EngineTextPlanning 151→125;
  - SqlTextRuns 214→199;
  - BodyDriver 545→544.
- **The four ruled hooks that narrow** (EngineTestText's arms, BatchTexts deleted, `EdgeHearing.rowAppealed`,
  `EdgeRun.wordsAlike`) each say "NARROWS A HOOK THE USER RULED". They are for the user's word.
- **TestPlanJson 32 → 33** (`VERSION`): `RootLoop.Row` loses `orElse`, `appeal` and `decline`, and the edge's `Appeal`
  loses `batched` and `joins`. The javadoc says so, and the round-trip test is updated.

### SHOULD-FIX
**S1. The lean-SQL baseline's note leaves out one moved column.** The note says the baseline grows on the measure's road:
DuckDB statements 350 → 2,319, H2 349 → 2,313. Read across the whole file (626 families, keys equal), these columns rise
too:
- `plan` 10.8M → 49.9M chars;
- `subselects` 1,440 → 6,136;
- `refused` 108 → 115.

`refused` is the one the note does not name. Seven family rows (five bodies) newly refuse at planning under the host:
- XStore `inMemoryAndRelational` and modelJoin `advanced`, on both lanes;
- `objectReferenceIn`, `map` and `base64`, on H2.

Each family has a test already on that lane's fail roster, so no verdict hides. But a shrink-only pin's growth for the
user's word should name it. Add one line to the note.

**S2. `-Dpct.channelb.judge` is now ignored silently.** ChannelB hard-codes `JudgeMode.HOST`, while MinimalCorpus refuses
`both` by name. A session that follows the live start-here docs (`PCT_CORE_CLOSEOUT_START_HERE:596,631,692,703`,
`TOTAL_WALL_START_HERE:1361`) with `=DATABASE` gets a host run and is not told. Refuse a set value other than HOST by
name, as the corpus does, and add a dated pointer to those docs. That is invariant 4, no silent defaults.

**S3. A stale comment.** `TestPlanner.plan`'s BOTH branch still says "under the compatibility rules their verdicts (the
words are the host's alone...)". BOTH under a compatibility style is refused two lines above.

### Proof gap
- At `e91005cc6` the commits show //core:census, census_edge, guardrails and guardrails_edge green.
- At step 2 (`61653394`) they also show core_edge_tests and the 26 `core_tests_*_judge_{database,both}` targets green.
- No run is shown at the head after step 3's appeal-path deletion for the judge suites, PCT channel A/B or the corpus
  lanes.

The batch's merged run (the judges' measurement) must show them green with the unchanged rosters. That is A4's
done-when.

## A3 piece 3 (piece): the pure-meaning detector, `origin/claude/compat-a3-detector` at `64ead02b7` (on #124's branch, `f832a667`)

**Verdict: APPROVED WITH 2 MUST-FIX (tests), 2 should-fixes and a proof gap.**

The detector's code meets §4a as amended. Its tests do not yet show three of the reaches the owner listed, and one test
cannot fail on its own plant.

Checked against:
- the final plan §4a, as amended at `2330eb98`: refuse by default, `warn` an operator's choice, no load-time report,
  `engine` not accepted;
- the rules of record;
- the shape note's revision 2 on the branch;
- the user's ~20:10 ruling, which I read in the owner's log §1: "Yes", to the three guard moves.

### What holds
- **It never changes a query.** `PureMeaningCheck.of` reads the typed tree and computes no value. Each route plans and
  runs the `TypedQuery` it already held. The generatePlan test compares the warned plan's SQL with a fresh plan of the
  same query.
- **It refuses before anything runs.** `checkMeaning` is called right after `Compiler.query` types the request, and
  before `executionPlan`, `plan` or `runner.run`. That holds in generatePlan, in execute, in executeArrow, and in
  `TestDataRoutes.doored` for the three test-data routes. A refusal is a `NotImplementedException` inside `answer(500,
  "COMPILATION", ...)`, legend-engine's envelope, with the header. `aRefusedRequestRunsNoStatement_...` counts the
  runner's calls on every route: 0 when refused, 1 when clean.
- **It reads the setting in one place.** `System.getenv("LEGEND_LITE_PURE_MEANING")` appears once, at
  `EdgeServer.UPSTREAM`:
  - unset, empty or `refuse` gives REFUSE;
  - `warn` gives WARN;
  - any other value stops the start, and `engine` is tested and refused.

  The batteries pass `Mode.WARN` as an argument and do not read the variable. No load-time report is built.
- **It names no database outside PureText.** The literal-0 table is `PureText.ZERO_START_AS_PURE` (H2 for both
  arities, DuckDB for 2). The detector asks `zeroStartAnswersAsPure`. A runtime with more than one database type gets no
  exemption, which is conservative.
- **The reach in code matches §4a:**
  - host-side statements (`readsAStore` false) are not walked;
  - a let-bound lambda that store-bound code names is walked, to a fixpoint;
  - user functions and qualified properties are followed;
  - CONSTRAINT and CONSTRAINT_MSG lifted bodies are skipped. A `validate`'s constraints are walked, because
    `ValidateDesugar` (`Compiler.java:883`) inlines them as `filter({this | !body})` before typing;
  - an M2M binding is followed only when `chained` and not `graphFetch`;
  - a function value that is not a lambda, variable or reference is UNVERIFIED, and so is an untypeable body. Both are
    counted, never clean, and refused under REFUSE.
- **The guard moves are dated and ruled ~20:10:**
  - `DialectBoundaryTest` PureText 1 → 2;
  - `own_corpus.matched` 3310 → 3312;
  - native-claims names `PureMeaningCheck` for indexOf and both substrings.

  Also dated: `ObservabilityGuardrailTest.ENV_FLAGS` +1 and the JavaEvalLedger funnel register +1.
- **22 tests, all bite but one.** Read one by one, 21 bite on the plant their name implies:
  - each "flagged" or "quiet" test has its partner;
  - `theCheckBites_bothWays` catches "flag nothing" and "flag everything".

  The exception is the must-fix below.

### MUST-FIX
**M1. Three of §4a's reaches are untested.** The shape note's own §4 plans them:
- item 6: `unverified`, a lambda read from an instance's property reported, and refused under `refuse`;
- item 11: an M2M transform under a TDS query over a chain, flagged; the same under a graphFetch, not flagged; a
  constraint reached through `validate`, flagged.

None was built. MODEL has no M2M mapping, and no test asserts `unverified`. So any of these breaks passes all 22 tests:
- deleting `w.mappingReach(features)`;
- inverting the `chained`/`graphFetch` gate;
- dropping either UNVERIFIED site;
- skipping `validate`'s inlined filter.

None of these needs piece 1. Add the four tests (plus the unchained-M2M case), each shown to fail on its plant.

**M2. `aConstraintIsNotFollowed_legendEngineChecksItInJava` cannot fail on its plant.** Its constraint is
`$this.name->substring(0, 1)`. On t::RT's H2, a literal 0 start is exempt in both arities. So a check that wrongly
followed constraints would still answer clean.

Fix: give the constraint a non-zero start, for example `substring(1, 2)`. Then this test bites. The exact count in
`aQualifiedPropertyTheQueryReadsIsFollowed` (`substring/3=1`) bites on the same plant too.

### SHOULD-FIX
**S1. A user quote in the javadoc that is not the user's.** `PureMeaningCheck`'s javadoc quotes "the user's rulings of
2026-11-19, ~13:25 "Pure's way, LOUD, no translator"". The user's words at ~13:25 were "Okay agree with all", the same
attribution fixed in A3 piece 4's M1. Word it "D1, ruled ~13:25 ("Okay agree with all")".

**S2. A mapping it cannot resolve is skipped as if clean.** `mappingReach` does `ctx.findMapping(m).orElse(null)` →
`continue`, and an empty `findFunction` for a binding is skipped too. Such a request probably fails typing or planning
anyway. Either report it `unverified` or say in a comment why it cannot reach here.

### For the batch combination check
`own_corpus.matched` is 3312 here (the detector's two test models), and A2 takes it to 3311 (PureV1SqlTextTest's
model). If the three models are distinct, the merged tree's count is 3313. The note's "regenerated again on the merged
tree with A2's move" has to say which.

### Proof gap
The commit says "The local gate, the edge net and the batteries are running on this commit. Their result follows in
the report". No result is recorded on the branch. Show:
- `//core:core_edge_tests` with the 22, and later 25, tests green;
- `PureV1EdgeGoldenTest` with its goldens untouched (shape note §4 item 15);
- the batteries under `warn`.

## Design review: the `engine` rewrite (`docs/records/compat-homework/A3-engine-rewrite-design.md`, `origin/claude/compat-a3-detector` at `1dcc4259`, B1-R; nothing built)

**Verdict: APPROVED TO BUILD, WITH 1 MUST-FIX TO THE DESIGN (how the sites are matched) and 4 should-fixes.**

The raw SUBSTRING is the right thing to do, in the user's ~20:05 sense, and the design follows the door-only switch
rule. I see no shortcut the user would not want: every unmeasured or unreachable case is refused by name, not
approximated.

Checked against:
- the rules of record, including the switch rule ruled ~20:35 ("only door switches... nothing in the core reads a
  switch"), which I read in `compat-rules-of-record-2026-11-19.md`;
- the user's ~20:05 ruling, read in the owner's log §1. Its words include "let's not take shortcuts on the rewrite part
  just bc I asked, let's do the right design" and "raw substring only if it's the right thing to do";
- legend-engine 4.145.0 at the pin.

### What holds
- **The operation is the right one.** `processSubstring` (`pureToSQLQuery.pure:4660-4665`, read) is
  `processDynaFunction('substring', ...)` with Pure's arguments as given, unless `correctSubstringIndexing` is set.
  Each extension writes the plain call, and I read every cited line:
  - H2 `:262`;
  - DuckDB `:277`;
  - Postgres `:248`;
  - Snowflake `:379`.

  So legend-engine's answer is, by definition, the database's own SUBSTRING over those arguments. A native that hands
  the database exactly that call keeps rule 1: the database computes it, and nothing imitates it.
- **Both alternatives are rejected for the right reasons.** The Pure library imitates each database's SUBSTRING, which
  tenet 4 forbids. The swap to piece 1's `sqlSubstring` passes through `SubstringClamp` (`GREATEST(start, 1)`, the
  DuckDB execution clamp), and that clamp alters the answer: 16 misses on DuckDB, measured.
- **The switch rule holds.**
  - The setting is still read once, at `EdgeServer.UPSTREAM`.
  - Under `engine` the door rewrites the typed program, after the detector and before the planner sees it.
  - The core gains a function with its own meaning and reads no switch.
  - The plan is lowered from the rewritten body and carries nothing about the rewrite.
- **No shortcut:**
  - `engine` is enabled per database from a measured fact (`ENGINE_FACTS` in `PureText`, beside the literal-0 table).
    DuckDB is enabled only if its batteries reach 47 of 47 at build; otherwise it is refused by name.
  - Every other database is refused by name.
  - M2M chain transforms and `~func` mappings are refused by name. They are reached in phase H, after the body exists,
    so a body rewrite cannot honestly reach them.
  - Unverified sites, and a site left inside a call after inlining, are refused.
- **The helpers are handled without clones and without a second model.** `UserCallInliner` is public and is the
  product planner's own step. Calling it at the door and planning `withBody` was measured byte-identical on four
  probes, and the planner's G½ then finds nothing to inline.
- **No error can name the substitute.** The rewrite runs after typing, same arguments, same type. Under `engine`,
  Pure's out-of-range raise (S383) is gone by design: the database answers, as legend-engine's does.

### MUST-FIX (design)
**D-M1. "Matched by position among the detector's sites" cannot meet the design's own plant.** The plant (§6) is "a
user function called both host-side and store-side is rewritten at the store-side use only". Position matching cannot
do that, for two reasons:
- After `inlineBody`, every copy of a helper's call carries the helper's own source position. A host-side copy and a
  store-side copy are indistinguishable by position.
- The detector reports a helper's sites once: `Walk.followed` dedups by function and corrected state. So one site can
  stand for several store-side copies.

**Fix:** run the detector's walk on the inlined body, and rewrite exactly the nodes it flags, by identity. One walk
then decides both what is flagged and what is rewritten, so they agree by construction. Assert that the rewrite count
equals the flagged count, and refuse on a mismatch. The §6 plant then tests the real mechanism.

### SHOULD-FIX
**D-S1. The new native is the door's vocabulary, so name it that way.** No product frontend writes
`substringAsGiven`; only the edge's `engine` rewrite produces it. That is not "ordinary product vocabulary". It is core
vocabulary only the door reaches, which is what `EdgeOnlyDataTest` and `EdgeOnlyMembersTest` exist to name. Register it
there as the door's, by the user's word. That is the honest form of "the core still computes it".

Alternatively, legend-engine's `###Relational` dynaFunction `substring` on DuckDB is the same raw call. If piece 1's
islands and dynaFunctions should be raw too, one native serves both and is truly product vocabulary. If piece 1 keeps
the clamp (lite's uniform SQL meaning), two "SQL substrings" exist with different meanings, and each native's javadoc
must say which is which. Settle that before B1 builds.

**D-S2. Type the substitute as the typer would.** The replacement `TypedNativeCall`'s callee must be the registry's
function for the native's `FunctionId`, with its declared signature, never a hand-assembled signature or `ExprType`.
Invariant 1 says the frontend does all typing. Add this to the first-hour check beside the `UserCallInliner` one, and
plant a wrong callee to show it is caught.

**D-S3. H2 is cited on the wrong route.** legend-engine's H2 SELECTs go through its dialect translation
(`relationalMappingExecution.pure:440-443`). There, `'substring'` is `functionCall('substring')`
(`toPostgresModel.pure:418`) and `'indexOf'` is `functionCall('strpos')` (`:381`). The H2 extension's `:262`/`:231`
are not on that route. This is the same correction as `firstDayOfWeek`. The conclusion does not change: it is the raw
call with the arguments as given, and the batteries measured 46 of 46. Cite the route that runs.

**D-S4. The native's part lists "the edge's three engine-style printers".** A2 deletes them, so on the merged batch
there are none. Drop them.

### Notes for the build
- **The register and the page.** Under `engine` lite answers legend-engine's meaning, so no new difference row is
  needed. But S130, S134 and S383 should name the `engine` value and what it does. The differences page's item 1 ("To
  get legend-engine's answer") should name the operator's option beside the hand rewrite.
- **The guard moves the design expects** are each for the user's word, with a dated note:
  - `INTERNAL_DESUGAR` +1 (41 after piece 1);
  - native-claims;
  - the funnel register +1;
  - the two new pins.

  The `EdgeOnlyDataTest` census answer is D-S1's.

### Addendum: the owner's two questions on the native (B1's), and a correction to my D-S1
**1. Internal, not LITE_SURFACE: agreed.** `meta::legend::lite::substringAsGiven` stays lite-internal (not
user-resolvable). Lite follows Pure: a user-callable function whose answer is "whatever this database's SUBSTRING
does" would make lite's own programs answer differently per database. That is against G10 and the product's one
meaning. Its only producer is the door's `engine` rewrite.

**2. Where to record "only the door produces it" (this corrects my D-S1, which pointed at `EdgeOnlyDataTest`):**
- **`EdgeOnlyDataTest` cannot hold it.** The test is exact: `assertEquals(new TreeSet<>(OPEN.keySet()), found)`. And
  `found` holds only pieces with no real product use. `Scalars` (product) naming `SqlFn.SUBSTRING_AS_GIVEN` is a real
  use by that test's own definition ("a constant is NAMED by product code"). So an `OPEN` entry for it would fail the
  guard.
- **`SqlFnSortedTest`: PRODUCT +1, as B1 expects.** The sort is measured by writers. `Scalars` writes it, and EDGE_ONLY
  there means "the product's dialects refuse it by name", the opposite of this design, where every dialect renders it.
  It takes a dated note, for the user's word.
- **Not `ENGINE_VOCAB_SHIMS`.** That set is derived-verified equal to the dynaFunction SHIM rows (`DynaFnRegistryTest`),
  and `substringAsGiven` is not a dynaFunction name.
- **The honest record is where producers are counted:**
  - `native-claims.tsv`, whose row names its claimers: the edge's rewrite and `Scalars`;
  - `INTERNAL_DESUGAR` +1 on its ceiling (41 after piece 1), for the user's word. Its comment is amended from "emitted
    by lite's normalizer/lowering" to add "or, for `substringAsGiven` alone, by the legend-engine door's `engine`
    rewrite", with a dated note. Piece 1's `sqlSubstring` and `sqlPosition` set the precedent for the category.
- **To hold it mechanically to that one producer** (no product class ever constructing a call to it), a new pin is
  needed: for example, the native-claims row asserted to have no product claimer but `Scalars`. That is the user's
  word, and optional.

## parser_parity's referee memo, `origin/claude/compat-parser-parity-memo` at `37ef44ff0` (one commit on `main` `b75d6c73a`; the CI-health worker's)

**Verdict: APPROVED, with 1 should-fix.** Not a CI risk on the heap.

The change is five files, test sources only, in `parser-equivalence`: +206/−26. There is no change to
`parser-equivalence/BUILD.bazel`, `tools/` or `.bazelrc`.

### What holds
- **Every assertion is unchanged.** In each of the four classes the diff swaps only where the referee's JSON comes
  from:
  - `mapper.writeValueAsString(referee.parseModel(text))` becomes `RefereeParses.withSourceInformation(text).json()`;
  - the span-free `parseModel(run, "", 0, 0, false)` becomes `withoutSourceInformation(run)`;
  - `getElements().size()` and each element's `writeValueAsString(e)` become `elements()`.

  At `main`, all four made the same referee (`PureGrammarParser.newInstance()`) and the same mapper
  (`ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports()`). The memo uses exactly those.
  The comparisons, the cases and the failure texts are the classes' own, untouched.

  CorpusSweepTest's SPI parser and its own span-free parse (`:244`) are untouched. Lite's parser still parses every
  source in every class.
- **The proof is shown, against `main`:**
  - the same 45 testcases with the same outcomes;
  - 22 of 23 outputs byte-identical. The 23rd is a diagnostic never asserted: its messages go from "null" to the NPE's
    text, consistent with the JVM's fast-throw;
  - the logs equal once masked;
  - two planted differences fail the same testcases with the same messages on both: `Corpus.all()` without
    `corefunctions/` fails 3, and `sameBytes` refusing Enumeration JSON fails 5.
- **No class can hand another a changed answer.** What is held is immutable `String`s: the whole model's JSON and each
  element's JSON. `json()` is `before + join(",", elements) + after`, where `before` and `after` are cut from the
  mapper's own whole output around the joined elements. So it gives back exactly that output, whichever occurrence the
  cut found. If none is found, the whole text is kept. The exactness does not depend on Jackson writing the array the
  same way.

  The one shared mutable object is a refusal's `Throwable`. No class in `parser-equivalence` calls `addSuppressed`,
  `initCause` or `setStackTrace` (grep: none).
- **The soft hold is not a CI risk.** The JVM clears every soft reference before it throws `OutOfMemoryError`, so held
  answers can never be what runs the heap out. Under pressure they are let go, and a later caller parses again: the old
  cost, never a wrong answer.

  What stays strongly held is the keys (the texts) and the counters. The live peak, 616-665 → 827 MB, is 29% of the
  2,816 MB heap. The "peak before a collection" (2,429 MB) is occupancy including garbage, normal for a heap under
  allocation, and below what the engine-object variant reached (2,548 MB). The target's `memory_mb = 2816` is
  unchanged, and the same on macOS.

### SHOULD-FIX
**Do not memoize a `VirtualMachineError`.** `answer` stores any `Error` the referee throws and throws it again to
every later class that asks for that text. A `StackOverflowError` is deterministic per text, so storing it is harmless.
An `OutOfMemoryError` is transient: storing it spreads one moment's memory pressure to every class that later asks for
that text, where before each class parsed again.

Fix: throw a `VirtualMachineError` without storing it (or store only `RuntimeException` and `StackOverflowError`).

### Notes
- `answer` is `synchronized` around the parse itself, so parses of different texts wait on one another. That is right
  for a shared `PureGrammarParser`. The measured times show no cost here, since the classes run one at a time.
- The diagnostic's message change ("null" → the NPE text) shows that the catalog's message column depended on JIT
  state before. The new text is the better one, and it is still not asserted.

### Re-check: `7f36f2730` (37ef44ff0 plus one commit)
**Verdict: APPROVED. The should-fix is met.**

`parse()` now catches only `RuntimeException | StackOverflowError`. Any other `Error` (an `OutOfMemoryError` above
all) propagates out of `parse()` before `HELD.put`, so it is never stored, and a later class parses the text again.
`answer()`'s rethrow covers both kept kinds, and the javadoc and the record comment say so.

The worker reports `//parser-equivalence:parser_parity` passing in 167.3 s with the same counts (9,731 parsed, 28,176
reused, 0 kept whole). I did not rerun it here. The live peak moves 827-875 MB between runs, of 2,816 MB: still not a
CI risk, for the reason above.

### Re-check: `6c4509e0c` (one commit on `7f36f2730`: the planted test)
**Verdict: APPROVED.**
- **The refactor is behaviour-neutral for the four classes.** `RefereeParses` becomes an instance over a referee
  function. The run's one memo, `RUN`, calls `REFEREE.parseModel(text)` or `(text, "", 0, 0, false)` exactly as
  before. The static `withSourceInformation`, `withoutSourceInformation` and `counts` delegate to it, so the four
  classes are untouched. `Key` and `answer` widen to package-private, for the test alone.
- **`RefereeParsesTest` bites both ways:**
  - `anOutOfMemoryErrorIsThrownOnAndNeverKept`: with the old `RuntimeException | Error` catch, the OOM is kept and the
    second ask throws it again, so the test goes red;
  - `aRefusalAndAStackOverflowAreKeptAndThrownAgain`, with `assertSame`: a catch that dropped `StackOverflowError`
    parses that text twice, `asked` reaches 3, and the test goes red. A catch that kept every `Error` is caught by the
    first test.

  Both run in `parser_parity` through its package selection, so 47 = main's 45 + 2.
- **Nit, theoretical:** test 1's third ask relies on the soft reference surviving between two adjacent asks. Clearing
  it would need memory pressure in that instant, so the risk is negligible.
- The worker's run, taken as reported and not rerun here: `parser_parity` passes in 164.0 s with the same counts, and
  both plants fail as on `main`.

### The detector, re-check 1: `337e66ade` (one commit on `1dcc4259`)
**Verdict: APPROVED. M1, M2, S1, S2 and the proof gap are met.**
- **M1:**
  - Seven tests were added (29 in all):
    - a desugared `validate` constraint is flagged, and the door refuses a validate request;
    - an M2M transform in a TDS query over a chain (`withChainedMappings`) is flagged;
    - the same transform under a graphFetch is not, and neither is an unchained M2M;
    - a lambda read from an instance's property is `unverified=1`, refused 500 with the header;
    - an untypeable function body is `unverified`;
    - a mapping not in the model is `unverified`.
  - The commit's plant table (P1-P7) maps each sabotage of `PureMeaningCheck` to the tests it turns red. Each reach I
    named now has at least one:
    - `mappingReach` deleted (P1);
    - the gate inverted (P2);
    - each UNVERIFIED site dropped (P3, P4, P7);
    - a filter's predicate not walked (P5);
    - constraints followed (P6).

  I took the plant runs as reported and did not rerun them.
- **M2:** the constraint is now `substring(1, 2)`. The commit measures the old `(0, 1)` against the P6 plant: both
  constraint tests stay green there. That confirms the finding, and the fix.
- **S1:** the javadoc now reads "D1, ruled by the user 2026-11-19 ~13:25 ("Okay agree with all")".
- **S2:** a mapping the model does not declare, or a binding whose body it does not hold, adds an UNVERIFIED site. It is
  never clean.
- **The proof:**
  - 55 targets green, among them `core_edge_tests` (29), `//gates:edge_net`, `//pct:batteries_test`,
    `//core:edge_pure_v1` (the golden net) and `//docs:all`;
  - the golden net was regenerated and is identical to the goldens;
  - the batteries under `warn` give identical answers, with the agreement pins unchanged.
- **The guard move:** `own_corpus.matched` 3312 → 3315, under ~20:10, for the tests' new one-line models. 3316 is
  expected on the merged tree with A2's +1, which answers the batch-combination note above.
  - **Corrected 2026-11-19 ~21:15 (the compat owner's note, checked):** the merged tree is 3314, not 3316. A2's move is
    3310 → 3309 (DOWN, census B2's correction `c2269b7b4`), not +1, so 3310 − 1 + 2 + 3 = 3314. Read on
    `claude/compat-a4-host-only` at `5ab730cd`: `ratchets.tsv` line 44 carries the dated note, the value line reads
    3314, and lines 42-43 are the detector's two moves. My 3316 used A2's uncorrected line. The value is checked again
    in the combination review, against the gate on the batch head.
- **Two gaps found, both with the product track:**
  - pure/v1 types through `Compiler.query`, which does not run `ValidateDesugar`, so a validate request is refused at
    typing there. That is loud, never a wrong answer;
  - a packaged `ModelChainConnection` does not reach the typed context.

  Neither is the detector's.

**On the compat owner's ~21:12 rulings for B1's native** (to be checked when B1's commit comes): they are consistent
with the approved design and my addendum.
- `substringAsGiven` in `Pure.COMPILER_OWN`, refused to users by name, by signature id and under `eval`, is stronger
  than "internal". Producers are counted there instead of `INTERNAL_DESUGAR`, which is fine as long as native-claims
  names the door's class as the producer.
- MongoDB's arm refuses `SUBSTRING_AS_GIVEN` rather than writing its clamped `$substrCP` under the "as given" name.
  That is right (invariants 3 and 4: an arm that cannot express the variant throws).
- One edge class, `EngineSubstitutes`, holds both substitutes.

## A3, B1's native `substringAsGiven`, `origin/claude/compat-a3-raw-substring` at `64adcc05e`

The branch has three commits of its own (`f3553c63`, `897db4bb`, `64adcc05`). They sit on A2 PR 1 (`6551c203`) and
piece 1's patch (`ff069d8a`). B1's record is `docs/records/compat-homework/A3-B1-substring-as-given.md` on
`claude/compat-b1-homework` at `f9f0acbbd`.

**Verdict: the code is APPROVED. The branch is NOT LANDABLE until the user rules on three guard moves (S1).**

**S1. Three guard moves have no ruling.** B1's record names them honestly, and `//core:census`, `//core:guardrails`
and `//spec:spec_tests` stay red on them:
- `implementation.kinds.Intrinsic` 738 → 740;
- `CatalogUpstreamDiffTest.NOT_UPSTREAM_MAX` 76 → 78;
- `JavaEvalLedgerTest`'s edge funnel register + `EngineSubstitutes.java`.

The first two go up. The third is a new entry in a closed list. ~20:45 covers moves down only, and ~21:10 names native-claims,
`INTERNAL_DESUGAR`, `SqlFnSortedTest` and the pin, nothing else. So each needs the user's word.
The compat owner's message to me does not mention them. Until they are ruled, the done-when is not shown green. Piece
1's `FUNCTION_FQN_LITERALS_MAX` 346 → 347, the fourth red, is ruled (~20:50) and lands with piece 1.

**What I checked, each read on the branch:**
- **The two SUBSTRING functions stay apart:**
  - `SubstringClamp` matches `c.fn() == SqlFn.SUBSTRING` alone, and `StringCountsAsInt` keys `SqlFn.SUBSTRING` alone;
  - every other writer of `SqlFn.SUBSTRING` in main code is untouched (`VerdictSql`, `LiteText`, `AsorReaders`,
    `CellTexts`, `TestDataPlanner`, `RelationTextLowering`, `FeatureRules`, `Scalars`' own rules);
  - the one writer of `SUBSTRING_AS_GIVEN` is `Scalars`' rule for the native's two ids.
- **The render arms:** each relational dialect writes `case SUBSTRING, SUBSTRING_AS_GIVEN ->`, the same function name,
  and MySQL passes it through. Javac's exhaustiveness covers them all, and `SqlTyping` types it as SUBSTRING is typed.
  No `default ->` arm is added.
- **MongoDB refuses by `DialectCapability`.** That is an `IllegalStateException`, where AGENTS.md invariant 3 names
  `UnsupportedOperationException`, but it is the dialect's own refusal type for TRANSLATE and `CURRENT_USER_FN`
  already. The owner accepted it, and I agree: the arm throws, which is what invariant 3 requires.
- **`COMPILER_OWN`:**
  - `NameResolver.refuseCompilersOwn` reads `COMPILER_OWN` and `COMPILER_OWN_IDS`, so a call by full name, by either
    signature id, or as a function reference under `eval` is refused;
  - `InternalNativeCallTest` checks that the two sets agree, and both ids map to `SUBSTRING_AS_GIVEN`;
  - nothing else reads `COMPILER_OWN`;
  - the test pins all four refusals and the bare name's "unknown function".
- **`EngineSubstitutes` (D-S2):**
  - the callee comes from `ctx.findFunctionById` through `InferenceKernel.resolveOverload`, an existing public method
    (`InferenceKernel` is not in the diff), and the type is that resolution's output;
  - `checked` refuses any other callee or type, and the entry refuses a call that is not the Pure overload it replaces;
  - the five plants (the other arity, `sqlSubstring`, a hand-made signature, Pure's own `substring`, a hand-made
    `ExprType`) and the three mismatches are each refused;
  - the class is package-private, with no main caller until B1-R's rewrite.
- **The answers:** on H2 and DuckDB, lite's plan answers each of the 18 rows over the column and over the literal (72
  in all) as `SELECT substring(...)` sent directly on the same session.
- **`Lean.run(TypedQuery, ...)`** is a test helper that goes through `Execution.run`, the door. Nothing is bypassed.
- **The guard moves made**, each with a dated note citing ~21:10:
  - native-claims' two new rows read `EngineSubstitutes;Scalars`;
  - `sqlPosition`, `indexOf`/2 and `substring`/2 and /3 gain `EngineSubstitutes` in their `also` column (the calls it
    reads);
  - `INTERNAL_DESUGAR <= 41`, with piece 1's 38 → 40 carried in a line its own note replaces;
  - `SqlFnSortedTest` PRODUCT + `SUBSTRING_AS_GIVEN`;
  - the pin moved to spec's `ClaimRegistryTest`. It checks that the `also` column is exactly the two claimers, it bites
    on a planted `CypherToPure`, and it names its own limit (a use through `Pure.Lite`'s name constant alone).
- **No new core hook, switch or mode.** The native and its `SqlFn` are ruled (~21:10). Nothing reads a setting.

**Should, for B1-R's measurement rather than this branch.**
- **S2: show the answers against upstream's pinned answers, not the database's.** The test's reference is the
  database's own `substring`, which the design defines as legend-engine's answer (the sources read: `processSubstring`
  and the extensions' plain `substring`).
- **Why it matters:** the engine runner's answers are pinned on `main`
  (`tools/engine-runner/src/test/resources/batteries/engine_{h2,duckdb}_answers.tsv`: legend-engine 4.145.0, H2
  2.1.214, DuckDB v1.3.0). Lite runs its own versions (H2 2.5.252). Where the two versions read a negative start
  differently, "the database's own" and "legend-engine's" part ways.
- **What to show:** the agreement battery under `engine` (`pct/batteries/agreement_{h2,duckdb}.tsv`) should turn
  DuckDB's substring DIFFER rows to AGREE against those pins, for example `col.s01`, where legend-engine answers `C` and
  lite answers `Cr` today. H2's substring rows already AGREE.
- **The cited path:** the test's javadoc names `substring_indexof.tsv`, which exists on `main` but not on this
  branch's base. It resolves on rebase.

**Carried, not this branch's:**
- **C1, Postgres:** `StringCountsAsInt` does not take `SUBSTRING_AS_GIVEN`, so a BIGINT count would fail there. B1's
  record names this. It cannot be reached while `engine` refuses an unmeasured database (B1-R). It becomes due when
  Postgres is measured.
- **C2, the product track's ~21:10 (3) (`sqlSubstring` to follow the SQL standard):**
  - that change moves this test's partner half, where SUBSTRING is clamped on 10 dialects;
  - after it, `SUBSTRING` and `SUBSTRING_AS_GIVEN` may write the same text on DuckDB and Postgres. The two meanings
    stay separate (lite's uniform SQL meaning against each database's own reading), and H2 then needs the reverse
    correction for `sqlSubstring`.

**Nits:**
- **N1:** the test's "not a copy" message is checked by equality, and `checked` uses equality too. An exact copy is the
  same function, so equality is the right test, but the message overclaims.
- **N2:** a blank line is left at the end of `NativeCatalogGovernanceTest`, from moving the pin to spec.
- **Proof:** the worker's runs are taken as reported, not rerun here (no Bazel in this session). Green:
  `//core:core_edge_tests`, `:core_tests_builtin`, `:core_tests_sql`, `:guardrails_edge`, and `//docs:all` at the
  head. Red: census, guardrails and spec_tests, on S1's three moves and piece 1's count.

## A2 PR 2: the plan text's spelling-only code out, `origin/claude/compat-a2-plan-text` at `059cc71df`

The branch has two commits (`d9d1fdd0`, `059cc71d`) on A2 PR 1 (`6551c203`): 12 files, +95/−296.

**Verdict: APPROVED.** Every deletion answers exactly as PR 1's adapter did. Every guard moves down only, and nothing
recorded moves.

**Behaviour-neutral, each checked against PR 1's `LiteSqlText`:**
- **`params`:** PR 1's version was a null check only. Its call in `engineSql` goes, and `planParams.resolveDotted`
  still reads the table. In `EngineParams`, only the splice and temp-table sets and their four methods go; `copy()`
  and the imports are still used.
- **`selectList`:** PR 1's always answered null, so `PlanText.single`'s `item != null ? item : sql` was always `sql`.
  It is now `sql`.
- **`renderedAlias`:** PR 1's was the identity, so `scalarRelational` prints `c.table()` as before.
- **`renderAsAliased`:** PR 1's was `dialect.render`, so `TestDataPlanText` calls `render`.
- **`inTempTableSelect` and the FreeMarker conditional:** they were reached only through a conditional site's true
  block, after a refusal. They are deleted, with `PlanText.freeMarkerConditional` and the `isTest` and threshold
  arithmetic they used.
- **`collectionSplice`'s refusal:** it moves into `InProtocol.allNodeTexts` with the same exception type
  (`NotImplementedException`), the same message, and the same point: the first conditional site, before that site's
  texts.
  - **A non-conditional site's texts are unchanged:** the constants' allocation, when there are constants, then the
    create-and-populate.
  - **The one caller, `PlanEnvelope.emit`, still refuses first:** it calls `allNodeTexts` before it uses
    `InProtocol.apply`'s rewritten plan, which still carries the wrapper parameter.
- **The dialect:** `PureText.dialect(type)` makes a new dialect per call, as `LiteSqlText.of` did. The sites that
  reused one adapter for several renders (`EngineAllocations`, the cross-store spine's `planText`) reuse one dialect
  the same way.
- **No reference remains** to `LiteSqlText`, `isSplice`, `isTempTable`, `EngineParams.withProtocol`, `collectionSplice`,
  `selectList`, `renderedAlias`, `renderAsAliased` or `inTempTableSelect`. The exceptions are the dated history notes in
  `JavaEvalLedgerTest` and `SqlTextRatchetTest`, which are history and correct as such.

**Guards, all moving down under ~20:45 (read in the owner's log: "A standing approval for every guard move that only
goes DOWN"), each with a dated note:**
- `JavaEvalLedgerTest`: `PlanText.java` 762 → 748 and `EnginePlans.java` 757 → 755;
- the closed edge-file list loses `LiteSqlText.java`, the entry PR 1 added;
- `EngineParamsTest` loses its assertions on the deleted table;
- `ArchitectureTest`'s comment and `AGENTS.md`'s line name `PureText.dialect`.

The diff touches no roster, register, recording or golden.

**Shown, not claimed** (taken as reported, not rerun here), from `059cc71d`'s message:
- `//gates:checks`: 197 pass, 1 skipped;
- the corpus host lanes on H2 and DuckDB: their fail rosters and unordered and engine-order registers equal PR 1's
  files line for line;
- `//pct:edge_contract_test`'s 27 shards and `//core:edge_pure_v1` unchanged;
- `//spec:update_edge_corpus_{0..3}_test` and `//spec:lean_sql_corpus_{h2,duckdb}` pass.

**Should (small; the gap predates this PR, but this PR now owns the code):** no test reaches the conditional site's
refusal. Neither branch has a test, recording or golden carrying "FreeMarker collection splice", so the refusal now
written in `InProtocol` is unpinned. One planted test would pin it: a `String[*]` plan parameter in an `in(...)` on the
plan-text route (`planToString` or an execution plan over a connection with a threshold), refused by that message.
Without it, a later edit could drop the check and print a plan whose SQL names `${inFilterClause_X}` with nothing to
fill it.

**Dispositions (the compat owner, ~21:39):**
- **B1's S1:** the three moves went to the user at ~21:30. B1-R's `PureMeaningRewrite.java` funnel row went with them,
  and nothing moves until the user's word.
- **B1's S2:** sent to B1-R as the bar for enabling DuckDB under `engine`. That means agreement against the engine
  runner's pins, the substring rows going from DIFFER to AGREE.
- **A2 PR 2's should:** the judges add the planted conditional-refusal test on the batch branch. I'll check it in the
  combination review.
- **D-M1's refinement:** to be pushed to `claude/compat-a3-engine`, with design note rev 2.

## The engine runner runs storeless tests, `origin/claude/compat-engine-runner-storeless` at `b648c377b` (one commit on `main` `8eea8fa13`; the CI-health worker's)

**Verdict: APPROVED.** The change is confined to `tools/engine-runner`, the oracle tool, which is spec input. No
product code, guard, pool, `MODULE.bazel` or lock file moves.
- **The fix:** `perf.AbsoluteClassPath.install()` runs first in `TestableMain.main`. It rewrites each
  `java.class.path` entry as `Paths.get(entry).toAbsolutePath()`, against the JVM's starting directory.
  - It does not normalize, so `../x` becomes `/abs/_main/../x`. The commit measured ClassGraph keeping exactly that
    form.
  - An absolute entry and an empty entry are kept as they are.
  - The JVM's class loader is untouched, so only readers of the property see the change.
- **Why the corpus verdicts cannot move:**
  - **Off Windows:** `scripts/corpus/run.py` passes `--classpath_limit=0` (F94), so the class path is one absolute
    manifest jar and the call does nothing.
  - **On Windows:** the launcher always writes a manifest jar (`run.py:166-171`), so the by-hand engine-verdict
    workflow's Windows platform does not change either.
  - **The other runners:** the planoracle mains and the other mains do not call it.
- **`--dump`:** it now also writes a test error's whole text, `<testable>.<id>.error.txt`, with every character
  outside `[A-Za-z0-9_.-]` written as `_`. The README says so.
- **The test:** `RunnerSmokeTest.testableRunsFunctionsThatReadNoStore` runs two storeless testables, which give a pass,
  a fail and a raise. It asserts the three report lines (with the millis cut) and the dumped error's JDK text,
  `Range [7, 6) out of bounds for length 6`. The plant is taken as reported: without the call, it fails with the
  probe's "package ... store.shared does not exist".
- **Upstream's answer is asked, not recalled.** The probe's result (legend-engine computes a storeless `substring` in
  its plan's Java and raises "Failed in node: root" with the JDK's text as the cause) is the engine runner's own
  answer at the pins. It is an input to S383 (B2's docs, `393cb62ff`), not reviewed here.
- **Nit:** the dump name's character rewrite could make two testables collide (`a::b` and `a__b`). That is
  theoretical for real names.
- **The proof**, taken as reported: `//tools/engine-runner/...` (`smoke_test`, `update_vocab_test`),
  `//tools/guards:markdown_inputs_test`, `//scripts/corpus:tools_build_test` and `:quarantine_test` pass, with
  `--lockfile_mode=error`.

## A3's `engine` rewrite: the mechanism, the wiring and the batteries

**What was reviewed:**
- **Landing:** `origin/claude/compat-a3-engine` at `1e6d9c68c`: the detector's section fix `8a2af5770`, then the
  mechanism with design note rev 2.
- **Review only:** `origin/claude/compat-a3-engine-review` at `2dec438b0`: B1's native merged (`d60f6f929`), the
  wiring `a182f30a3`, and the batteries' `engine` half `2dec438b0`.

**Verdict: D-M1 is MET, and the mechanism is APPROVED. The wiring and the batteries are APPROVED, with one finding
that must be answered before `engine` is offered to an operator (E-M1) and three shoulds.**

**Nothing lands without the user's word on:**
- the funnel row for `PureMeaningRewrite.java`;
- the two new pin files `pct/batteries/agreement_engine_{h2,duckdb}.tsv`;
- B1's S1 (its three moves), which the wiring sits on.

### D-M1, checked in `PureMeaningRewrite`
- **One walk decides both what is flagged and what is rewritten:**
  - the first walk (`PureMeaningCheck.of`) names the roots: each store-bound statement, and each let-bound lambda's
    let that store-bound code names, in body order;
  - each root is inlined on its own (`UserCallInliner.inlineBody` over a list of one; a let's value with the let
    kept);
  - every other statement is left as written;
  - the second walk over that body records each flagged call by identity (`Site.node`). The call is recorded only when
    `following == 0`, that is, met in a root's own tree.
  - `follow` raises `following` for every body it enters: a user function, a function reference, and `mappingReach`'s
    bindings and association predicates. So a call met in any of those has no node and is refused by name ("cannot
    be rewritten at this door").
- **The rewrite:**
  - it rebuilds bottom-up through `mapChildren`, replacing a node when the original node's identity is in the map;
  - it walks the second walk's roots alone and leaves the other statements as they are;
  - the count check (`replaced`, an identity set, against `nodes`) refuses a half rewrite. A node shared by identity
    is counted once on both sides.
  - The model is immutable, so a function's or a class's body is never changed for its other callers.
- **The refusals run before anything is rewritten:** unverified, an unmeasured target or a runtime with no single
  database, and a call that cannot be reached.
- **The plants R1-R6 are each pinned to the tests they turn red.** I took the runs as reported. The tests cover:
  - the D-M1 plant (a function called both host-side and store-side, rewritten at the store-side use alone);
  - a host-side let kept as written beside a store-side rewrite (`assertSame` on the let);
  - a qualified property;
  - a let-bound lambda;
  - a store-bound let;
  - the corrected-indexing feature;
  - the refusals of an M2M chain, an unverified call and an unmeasured database.
- **`PureText.MEASURED`:** one line, a pair per database, so `DialectBoundaryTest`'s pin stays at 2. A literal-0
  start that is exempt on a database is not flagged, so it is not rewritten. Its answer was measured equal to Pure's
  there.

### The wiring (`a182f30a3`)
- **`Mode.ENGINE`:**
  - it is parsed by the enum's own lower-case names, so there is no new string switch;
  - an empty value is REFUSE;
  - any other value stops the server's start;
  - it is read once at the door (`EdgeServer.UPSTREAM`).
- **`checkMeaning` returns the query the route plans and runs:**
  - a clean query is returned as it is;
  - under refuse, a flagged query is refused;
  - under warn, it is returned as it is;
  - under engine, it is rewritten.
  - `generatePlan`, execute and the Arrow execute each take the returned query. These are the only callers: the
    detector and the rewrite have no other main-code callers.
  - The header reads `engine <codes>` on a 200.
- **The test-data routes:**
  - `doored` still runs the check, so an unmeasured database, an unverified call or an unreachable call is refused
    there;
  - the seed's query, typed from the planner-derived `q`, is rewritten before it is lowered;
  - `TestDataPlanner.plan` reads the request's query through `ScanRelations.relTree` alone (tables and columns, no
    computed value), so the seed is the one place the query's meaning chooses anything. That means the seed rewrite
    is the whole rewrite there, not half of one.
- **The switch rule (~20:35, door-only):** the core reads no switch. The seed's rewrite runs inside the edge's own
  lowering function, which the test-data planner calls back. That is within the rule's letter. I'm noting it so B5's
  row can name it (a callback the edge already supplied, not a core reader).

### The batteries (`2dec438b0`): my S2 is answered
- **The comparison:** the `engine` half is compared with the engine runner's pins (legend-engine 4.145.0, H2 2.1.214,
  DuckDB v1.3.0; lite ran H2 2.5.252 and DuckDB 1.5.6).
- **Read row by row against the `warn` pins:**
  - DuckDB moves exactly 16 rows from DIFFER to AGREE: `col.i07`, `col.s01`, `s06`, `s07`, `s11`, `s14`, `s15`,
    `s18`, `lit.s01`, `s06`, `s11`-`s15` and `s18`;
  - H2 moves none;
  - the `warn` pin files are unchanged.
- **What is left:** on both databases, `col.i04` and `lit.i04` (the 3-argument indexOf: legend-engine refuses, lite
  answers; E-R2, ruled). H2 also has one BOTH_FAIL.
- **I read the bar as you do:** every answer legend-engine gives is agreed, so DuckDB is enabled by the design's rule.
  The refusal tests now plant Postgres.

### E-M1 (must answer before `engine` is offered): a flagged call past the store's result
- **The gap:**
  - the detector treats a whole statement as store-bound once it reads a store (`readsAStore`), and the rewrite
    rewrites every flagged call in it;
  - the batteries measure one shape only: a call inside a relation's column function, where legend-engine sends it to
    the database;
  - a call applied to the store's result after a step legend-engine may run on the host (its router's platform
    cluster) is rewritten too. Example: a class query's terminal `->toOne().name->substring(0, 2)`.
  - If legend-engine computes that on the host with Pure's meaning, `engine` silently answers the database's instead.
    On DuckDB at a start of 0, that is `C` where legend-engine says `Cr`.
- **Why it only matters now:** under refuse and warn, an over-flag is a refusal or a warning. Under engine, it is a
  wrong answer.
- **What to do:** do not recall upstream. Ask the engine runner, one probe per shape (a class query's `toOne()` tail;
  any scalar tail lite plans past a relation), on DuckDB. Where legend-engine answers on the host, refuse that
  position by name under `engine`, or narrow the roots to the store-pushed part. Where it answers in the database,
  add the probe to the batteries as measured.

### Shoulds
- **E-S1: a let-bound lambda that stays a root is rewritten in place, for every use of the let.**
  - **When it happens:** in the tests the inliner β-reduces the store-side `$f->eval(...)`, so the let is no longer a
    root in the second walk. But if a store-side use is one the inliner does not reduce, the let stays a root and its
    lambda is rewritten.
  - **The harm:** a host-side statement that also evaluates `$f` then gets the database's meaning, where legend-engine
    computes Pure's on the host.
  - **The fix:** a cheap guard and a plant. Refuse by name when a rewritten let root is also named by a non-root
    statement.
- **E-S2: the seed rewrite (W2) has no test that bites on this tree.** DuckDB is now enabled, and there a 3-argument
  start of 0 differs between the two meanings today. A DuckDB seed test would pin W2 now rather than waiting for
  piece 2.
- **E-S3: design note rev 2 contradicts the build and the rulings in places:**
  - D-S1's paragraph (`EdgeOnlyDataTest`/`EdgeOnlyMembersTest`, "the build is as if registered door-only") is stale.
    It is superseded by my corrected D-S1 (`ca8ea4a5`) and the user's ~21:10: `COMPILER_OWN`, the `ClaimRegistryTest`
    pin, SqlFn PRODUCT, not `EdgeOnlyDataTest`.
  - `SubstringAsGivenCall.of` is now `EngineSubstitutes`.
  - "the three test-data routes ... refused by name, not rewritten in half" is contradicted by the wiring, which
    rewrites the seed.
  - The guard list's "the `EdgeOnlyDataTest` census answer" is stale.
  - The build table's "the replacement by position" is stale.
  - `1e6d9c68c`'s javadoc and message also name `SubstringAsGivenCall`.

### The detector's section fix (`8a2af5770`): APPROVED
- **The headers:** six model literals gain their `###` headers, and `parser_parity` is green as reported.
- **The replaced test model:** the function-typed class is lite's own dialect, which the reference grammar refuses. It
  is replaced by a lambda read out of a collection, which reaches the same `TypedEval` UNVERIFIED site, so P3 still
  bites.
- **The guard move:** `own_corpus.matched` 3315 → 3321 (+6) under ~20:10, with a dated note in both `PeRatchets` and
  `ratchets.tsv`.
- **For the combination review:** the merged tree's 3314, plus these 6, gives 3320 expected, to be regenerated there.

**Dispositions (the compat owner, ~22:18; the ruling read in `compat-program-log-2026-11-19.md` on
`claude/compat-program` at `663db991a`):**
- **The user's ~22:20, "Yes to all three", covers:**
  - B1's three moves: Intrinsic 738 → 740, `NOT_UPSTREAM_MAX` 76 → 78, and the funnel row for `EngineSubstitutes.java`;
  - the funnel row for `PureMeaningRewrite.java`;
  - the two new `agreement_engine_{h2,duckdb}.tsv` pin files.

  Each move carries a dated note citing it. B1's S1 is closed by that word, and I'll check the notes when the moves are
  made.
- **E-M1, E-S1, E-S2 and E-S3 went to B1-R.** E-M1 is a must before the wiring joins the batch: probes on DuckDB and H2
  per shape, then each position refused by name or added to the batteries.
- **My switch-rule note goes on B5's row.**

### E-M1, answered: CLOSED. The detector's graphFetch-tree fix, `origin/claude/compat-a3-detector` at `e4496b763`: APPROVED
- **The evidence is legend-engine's own** (`docs/records/compat-homework/a3-engine-probes/`; legend-engine 4.145.0,
  H2 2.1.214, DuckDB v1.3.0; 20 probes). I read it in the answer files:
  - **In the database:** `cls.toOne.sub3`, `sub2`, `sub0` answer through a `relationalDataTypeInstantiation` node
    whose SQL is `select substring("root".desk, ...)`. `graph.qualified.sub3` answers `"deskPart()":"Cre"`, SQL's
    meaning.
  - **On the host:** `let.then.sub3` answers `"re"`, Pure's meaning, from a Java plan.
  - **Refused by legend-engine:** `graph.serialize.sub` and `tds.value.sub3` (500).
- **What lite does today:** its execute route refuses every scalar and collection result before planning ("a
  SCALAR/COLLECTION result's serialization is unprobed", every setting). So the tail shapes cannot be answered wrong
  today. The TDS, filter and relation shapes agree under `engine`.
- **The one host-side shape is never store-bound:** `StatementEffects.reaches` follows user calls and children, never a
  variable to its let, so `$d->substring(...)` in its own statement is not flagged. I agree that no position needs
  refusing under `engine`, and I accept the owner's call: no battery move in stage A, and the class shapes guarded as
  `PureMeaningEngineTest` cases asserting the probed answers.
- **The fix:**
  - `TypedGraphFetch` now walks its tree. Each node's arguments are walked.
  - A derived property's body is followed (through `follow`, so its sites have no node, and under `engine` a qualified
    property in a tree is refused by name, not rewritten; legend-engine computes it in the database, so this is a
    conservative refusal, not a wrong answer).
  - Each class reached joins the mapping reach.
  - A property not found, or a body not in the model, is unverified.
  - M2M bindings stay skipped under a graphFetch, as before.
  - The test has its positive case and its partner. The plant P8, the edge net unchanged, census and guardrails are
    taken as reported.
- **Nits:**
  - **Pin the host-side shape:** add `{|let d = <store>->...->toOne().desk; $d->substring(1, 3)}` to
    `PureMeaningCheckTest` as clean. It is the one host-side shape, and today it is guarded only by `reaches` not
    following variables, which a later change could alter.
  - **A serialize tree that is not the fetch's:** only the `graphFetch` tree is walked. A `serialize` tree naming a
    qualified property the fetch tree does not is unprobed. Either probe it or refuse such a pair by name when it
    matters.
  - **Record the refusal:** the shape note should say that a graph-fetch tree's qualified property is refused under
    `engine`, since the tree's derived bodies are followed, not inlined.

### B1-R's round on 285f98e18 (`claude/compat-a3-detector` `9d86aa88f`, `claude/compat-a3-engine` `fcb57e406`, the review branch `984ee555e`)

**Verdict: APPROVED. E-S1, E-S2 and E-S3 are met. Two new shoulds (R2-S1, R2-S2) and one probe to ask.**

**`9d86aa88f`: the walk takes a let only when its value IS a lambda.**
- **Why:** legend-engine computes a store-free let made with a lambda (`['ax','bx']->map(...)`) on the host. Its SQL
  reads `qty in (1, 1)`, in the probes added to `a3-engine-probes/`. So the old `holdsALambda` rule over-refused under
  refuse, and under `engine` it could have rewritten that let against legend-engine.
- **Tests:** the new test sits beside its partner, the let-bound lambda that is store-bound. Plant P9 is taken as
  reported.
- **R2-S1 (should):** an `eval` of a let-bound variable whose let is no longer walked is still taken as followable.
  - **The cause:** the typer puts a literal lambda let into each of its uses, so a `TypedVariable` left under a
    `TypedEval` is a let whose value is not a literal lambda (`[{s|...}]->toOne()`, `my::makeFn()`, a cast).
  - **Before:** `holdsALambda` walked such a value.
  - **Now:** nothing walks it, and `node`'s `TypedEval` arm admits a `TypedVariable`, so the call is clean without its
    body ever being read.
  - **The fix:** an `eval` of a variable bound by a let that the walk did not take is UNVERIFIED, with a plant. A
    function's own parameter, whose lambda the caller's tree supplies, is not affected.

**`fcb57e406`: E-S1 and E-S3.**
- **The guard:** `refuseSharedLetRoots` refuses a let root that holds a flagged node when a statement that is not a
  root reads its name. That is tested on a built walk result, with its partner, plus the realistic shared-let query,
  where the typer's substitution leaves both lets as written and rewrites the store's use alone. Plant R7 is taken as
  reported.
- **E-S3:** the note's stale lines are fixed. I re-read it: D-S1 is settled as ruled, the class is named
  `EngineSubstitutes`, the test-data routes are rewritten, and the text says identity, not position.
- **R2-S2 (should): the guard is wider than its reason.**
  - **The reason holds only for a lambda let:** that is one piece of code evaluated in two places with two meanings.
  - **A store-bound VALUE let is computed once,** for example `let t = <relation>->extend(~a: ...substring...)->from(rt)`.
    A statement that only reads it (`$t->select(~a)`, with no store read of its own, so host-side to the detector)
    reads a value, and nothing in it changes meaning. The guard refuses it all the same, which is a false refusal of a
    shape a user can write.
  - **The fix:** narrow the guard to `let.value() instanceof TypedLambda`, or show by probe why a value let must be
    refused.
- **The probe to ask, on the same shape:**
  - **The question:** a statement that reads a store-bound let through a variable, with no store read of its own, is
    host-side to the detector. `StatementEffects.reaches` does not follow a variable, which is right for the probed
    scalar let (`let.then`, `"re"`). So that statement's own calls are not walked, for example
    `{|let t = #>{...}#->from(rt); $t->extend(~b: r|$r.desk->toOne()->substring(1, 3))}`.
  - **Why it matters:** whether legend-engine writes that statement into the let's SQL, or computes it over the
    let's result on the host, decides whether this is a silent miss under refuse and warn.
  - **The ask:** one probe on DuckDB, where `'Cre'` and `'red'` differ.
  - **If legend-engine writes it into the SQL:** a statement reading a relation-valued store-bound let is store-bound
    too (a variable followed to a let whose value is a relation that reads a store, not to a scalar let), and R2-S2's
    guard question goes away.

**`984ee555e`: E-S2.**
- **The seed test:** `theSeedIsRewrittenOnDuckDb_aThreeArgumentStartOfZero` plans the seed on DuckDB, without running
  it. Under engine its SQL holds `substr('abcdef', 0, 3)`; under warn it does not. Plant W2 now bites on this tree.
- **Nit:** the javadoc speaks of the answers (ID 4 against ID 3), but the test asserts the planned SQL. It should say
  so.
- **The graph fetch:** `aGraphFetchsQualifiedPropertyIsRefusedByNameUnderEngine` pins the refusal I asked the shape
  note to record (a9e827e2e). The runner is never called, the answer is 500 "cannot be rewritten at this door", and
  the header still names the call.

**The batch is at `f4525a31e`,** waiting on the translator's two pieces (the user's word in that session). The
combination review starts on the judges' merged measurement.

**The probe, answered (the compat owner, ~23:17, from B1-R's legend-engine 4.145.0 runs on H2 2.1.214 and DuckDB
v1.3.0). NOT a silent miss.**
- **What legend-engine answers:**
  - A relation let read by a later statement is refused by legend-engine in every form, with `from` inside or after
    the let, and in R2-S2's shape.
  - A class-collection let read by a later `map(p|$p.desk->substring(1, 3))` answers `["re"]`, Pure's meaning, on the
    host.
  - So `reaches` not following a variable is right. Following one to a class let would be wrong.
- **What I could read:** on `claude/compat-a3-detector` at `0490d059a`, `a3-engine-probes/` holds `let.then.sub3`
  (`"re"`) and `let.map.host` but not yet the relation-let or class-let rows. I take those as reported until they are
  pushed, and the conclusion stands on them.
- **To follow (B1-R):**
  - R2-S1;
  - R2-S2 narrowed to lambda lets;
  - the class-let shape pinned clean beside its flagged one-statement partner;
  - the seed test's javadoc.
- **`0490d059a` answers my notes on a9e827e2e: APPROVED.**
  - The host-side shape is pinned with its partner: one statement is flagged, the separate statement is clean.
  - The serialize-only property was probed. legend-engine serializes `deskPart()` as null and fails `deskAt()` with an
    NPE, so it never computes a substring or an indexOf, and the detector need not walk the serialize tree.
  - The shape note records the refusal under `engine`.
  - The model's new `firstC()` moves no parser-equivalence count, as reported.
- **The serialize-only difference is a register row, PROPOSED S388 (drafted by B2):** lite computes the property;
  legend-engine answers null or fails. The owner's call that it is a recorded difference, not a refusal, is consistent
  with "lite follows Pure". By the upstream-fidelity tenet, though, the row must be posted to the user and ruled before
  it lands: a row the user did not rule is a defect.

**S388 RULED:** the user said "Yes to A" at ~23:20, read in the owner's log on `claude/compat-program`. The row is
recorded as a difference (lite follows Pure), not a refusal. B2 marks the row ruled on `claude/compat-a3-docs`, and I
will check it in the combination review.

### B1-R's round on 7b70932b3 (`claude/compat-a3-detector` `e9b130d69`, `claude/compat-a3-engine` `e86b92689`, the review branch `18d5c7181`)

**Verdict: APPROVED. R2-S1, R2-S2 and the nit are met. The let shapes are pinned on legend-engine's own answers. Nothing
is open from my rounds on the engine rewrite.**
- **R2-S1:** the walk collects each variable that the query's own tree evaluates (`following == 0`). After the
  fixpoint, such a variable bound by a let is UNVERIFIED unless the walk took the let and its value is a lambda.
  - The condition is right even though `walkedLets.add` runs before the lambda test: the test requires both.
  - A followed function's parameter is untouched.
  - Plant P10 is taken as reported.
- **R2-S2:** `refuseSharedLetRoots` now skips any let whose value is not a `TypedLambda`. legend-engine's answer backs
  it: `let.value.sub3.then.upper` answers `"CRE"`, and `let.value.idx.then.plus` answers `13`, SQL's 1-based 3 plus
  10. So the let's own value is in SQL, and a later read of it is no reason to refuse. Plant R8 is taken as reported.
- **The let probes**, now pushed and read on `0490d059a`'s successor:
  - the `let.rel.*` rows are all 500 (`letFunction` has no SQL translation; "RelationStoreAccessor should be used with
    a from(...)");
  - `let.cls.then.map` answers `["re"]`, Pure's meaning on the host;
  - `let.cls.then.project` is 500.

  The detector's handling of lets read through a variable is therefore right in every probed shape.
- **The class shapes** (`1035b8f2e`) are guarded as `PureMeaningEngineTest` cases with the probed answers:
  - a project's substring `abc`;
  - a filter's rows;
  - indexOf `3`;
  - DuckDB's `substr(name, 0, 2)` in the plan under engine and not under warn;
  - the graph tree's refusal.
- **The merge** keeps one `following` field in `PureMeaningCheck`, both branches having added it.

## The window-in-a-list fix (Q5, the user's ~21:40), `origin/claude/window-list-lowering` at `1819a5446` (on `main` `8eea8fa13`)

**Verdict: APPROVED.** This is a product fix that rides the batch. The answer comes out right where it was silently
wrong (SQLite, Postgres), and the SQL is unchanged where it was right (DuckDB, H2, ClickHouse, Snowflake). No pin
moves.
- **The rule:** `SelectMerge` takes each dialect's rule of the expressions it writes as queries of their own.
  - `Reads.inQueries` collects each of the subselect's columns read inside such an expression.
  - A merge is refused when one of those columns is of the subselect's level (`levelled`: a window, an aggregate, an
    UNNEST). The subselect then stays the level the lowering made, so no statement is added and nothing is computed
    on the host.
  - The rule is wide by design (`queriedBy`: every non-leaf expression but a parenthesis and a CASE, plus the
    dialect's declared calls). It is an exhaustive switch over the MIR's expressions with no default, so a new
    expression must be decided there. Its cost is a level kept, never an answer: the merge is only ever refused.
- **Every dialect that runs `SelectMerge` declares a rule** (six, by grep): SQLite and Postgres
  `queriedBy(OPERANDS_IN_A_QUERY)`; DuckDB, H2, ClickHouse and Snowflake `IN_PLACE`. The constructor change makes the
  compiler hold every caller to it.
- **The census, `SelectMergeQueriedOperandsTest`:**
  - it holds each declared set to the renderer in both directions: the set must equal the measured set, less a named
    list of operations the census cannot build (lambdas, entry lists, maps, format);
  - it checks the in-place dialects write every operation and cast in place;
  - it shows the rule at work on SQLite, Postgres and DuckDB.
  - **Its limit:** "writes a query" is measured from the text (a second SELECT, WITH RECURSIVE, `json_each`,
    `jsonb_each`, `unnest(`, `generate_series(`). A future form using another table function would not be counted.
    Because the check runs both ways, a declared operation that stops nesting still fails it.
- **`WindowReadThroughAList`:** it is database-blind, with one test per database (the census's naming rule). DuckDB,
  SQLite and Postgres give PCT's rows. SQLite refuses the chunked form by name, as ruled, and H2 refuses as before.
  The local gate is green, as reported.
- **Notes:**
  - **N1, `SelectLayers`:** it runs after `SelectMerge` without the rule. It lays each value out at its lowest level,
    so it should never lift a window under a queried operand, and the PCT shapes pass. Nothing pins that, though. One
    planted chain (two windowed levels and a list read, on SQLite) would.
  - **N2, `Postgres.java`:** the new field's javadoc now sits between `passes()` and its old javadoc, which hangs over
    the field.
  - **The worker's open observation:** a value drawn afresh (`random`, `uuid`), merged under an operand a dialect
    writes several times, could draw once per copy. It applies to merges generally and predates this fix, so the
    product track is the right home.

**The window fix's notes, `bf2e9c4c3`: both met.**
- **N1:** `selectLayersNeverLiftsAWindowUnderAListRead` builds a three-level chain: a window of joined names, a second
  window of the same rows, and a list read of the first above them.
  - It asserts `SelectLayers` re-lays the chain from three levels to two, a precondition, so the pin cannot pass
    vacuously.
  - It asserts no window sits under a SQLite-queried operation, both after `SelectLayers` alone and after SQLite's
    merge then layout.
  - A planted lifted shape shows the check bites.
- **N2:** `Postgres.passes()`'s javadoc is back above its method and names the merge's rule. The field keeps its own
  javadoc.
- `core_tests_sql`, guardrails and `lean_sql_baseline_test` are green, as reported.

**The plan changed (the compat owner, ~01:50 on 2026-11-20):** one full measurement only, on the final head WITH A3
piece 2. There is no separate pre-piece-2 measurement and no delta review. Piece 2's lean-SQL re-pin, 59 entries, is
approved by the user (~01:35, "Okay let's raise the lean sql", read in the owner's log at `0b0ed6a17`). The user also
asked for a possible later "no check" lean mode, a follow-up for power users.

The combination review will check, on that one measurement:
- the channel-B MariaDB and MySQL failing sets against A2's base;
- each lean-SQL growth matched to its cause in piece 2's `lean-sql-growth.txt`, with none shrinking unexplained;
- `own_corpus.matched` regenerated on the merged tree;
- the accepted rosters restored, and S388 marked ruled.

## A3 piece 1: the SQL-family frontends say SQL's meaning by name, `origin/claude/compat-a3-frontends` at `a4c1f2b19` (3 commits on `main` `b75d6c73a`)

**Verdict: APPROVED.**
- **The change:** two lite-internal natives, `meta::legend::lite::sqlSubstring` (2 overloads) and `sqlPosition` (1).
  Their rules in `Scalars` write `SqlFn.SUBSTRING` and `SqlFn.STRPOS`, exactly the calls Pure's `substring` and
  String `indexOf` rules wrote before piece 2. So no SQL moves: `SubstringClamp` still matches the same function, and
  `StringCountsAsInt` the same counts.
- **Every frontend site is moved:**
  - Cypher, the SQL island's Postgres functions, IQ's functions and aggregates, and `RelOpTranslator`'s `substring`,
    `indexOf` and `position` dynaFunctions now name the new natives (`Calls.exact` / `SqlToPure.SQL_SUBSTRING`).
  - A grep of the protocol and normalizer trees finds no call of Pure's `substring` or `indexOf` left. What remains
    are source-language function names (Cypher's and SQL's own `substring`).
  - The one exception is `CypherToPure.labelsJson`'s `substr(text, 1, length - 1)`, which means Pure's offsets and
    says so in its comment.
- **The guard moves**, ruled ~20:50 ("Okay approved", read in the owner's log) and in the translator's session:
  - `INTERNAL_DESUGAR` 38 → 40;
  - `FUNCTION_FQN_LITERALS_MAX` 346 → 347;
  - `NOT_UPSTREAM_MAX` 73 → 76;
  - `Intrinsic` 735 → 738;
  - the 9 re-blessed text goldens (the call head prints as `sqlSubstring`/`sqlPosition`).

  Each Java ceiling carries a dated note. `spec`'s `ratchets.tsv` has no per-line notes by its own convention ("a move
  is a reviewed diff of this file, its reason in the commit", `SpecRatchets`), and `3a75adc4c`'s message gives the
  reason.

## A3 piece 2: the core switch to Pure's meaning, and S383's raise, `origin/claude/compat-a3-switch` at `1f4b108d1` (on A2 `6551c203`, piece 1 as `633a0561`)

**Verdict: APPROVED, with ONE MUST-FIX (P2-M1).** The fold claim is checked and correct.

**The fold claim, read in the rule (`Scalars`, the `AT_STRING_SUBSTRING` loop):**
- **The 3-argument form:**
  - `start < 0` is added unless the start is a literal of 0 or more;
  - `end > LENGTH(s)` is always added, since it depends on the length;
  - `end < start` is added unless both are literals with the end at or past the start.
- **The 1-argument form:** `start < 0` as above, and `start > LENGTH(s)` unless the start is a literal of 0 or less.
  So `substring(s, 0)` carries no guard.
- **Nothing is missed.** A start past the length in the 3-argument form is covered: either the end is past the length,
  or the end is before the start.
- **Nothing is computed in Java:** only literals are compared, and every length-dependent check is the database's.
- **The message:** operands start, end and length (for the 1-argument form the end is `LENGTH(s)`). `PureMessages`
  writes `Range [b, e) out of bounds for length n`, the engine runner's own probe text.
- **The folds not made** (a negative literal start, which always raises; a literal end below a literal start; a
  literal end of 0 or less) cost only SQL size, never an answer.

**`indexOf`:**
- **The 2-argument form:** `STRPOS - 1`, exact for a hit and for a miss (0 − 1 = −1), and for an empty needle (1 − 1 =
  0).
- **The 3-argument form:** `from0 = max(from, 0)`, the suffix searched from `from0 + 1`, a hit re-based to
  `k + from0 - 1`, and −1 on a miss.

**P2-M1 (must): S383's raise is missing on two roads that answer Pure's `substring`.**
- **Under `CORRECT_SQL_SUBSTRING_INDEXING`:**
  - `FeatureRules.select` returns `CORRECTED_SUBSTRING`, the unguarded rule, which wins over `Scalars`' plain guarded
    rule (`Scalars.lower`: "a flagged rule wins over the plain one").
  - So with the flag set, an out-of-range `substring` answers the database's clamp instead of raising.
  - `FeatureRules`' own javadoc says the flag is "a DOCUMENTED NO-OP ... the rule the flag selects here is the same".
    It is not, for out-of-range indexes.
- **On `substr`:** it is always `CORRECTED_SUBSTRING`, unguarded. By the code's own reading, legend-pure's `substr`
  is `substring`'s synonym and calls it (`SubstrIndexingTest`, the `Scalars` comment), so it raises in legend-pure too.
- **Why it matters:** S383 says lite raises "on all three" for `substring`. It names neither the flag nor `substr`.
  `SubstrIndexingTest` checks only in-range answers, with and without the flag, so nothing catches this.
- **The fix:**
  - one guarded rule for all three: plain `substring`, `substring` under the flag, and `substr`. The flag either
    registers the same rule or registers none, if an unregistered flag is still accepted;
  - a test that each of the three raises `SUBSTRING_OUT_OF_RANGE` out of range.
- **What may move:** the corpus or PCT rows that set the flag or call `substr` out of range. Measure them, for the
  user's word.
- **If instead** the flag is meant to keep legend-engine's corrected, unchecked answer, that is a divergence from Pure
  and needs its own row, ruled by the user.

**Shoulds:**
- **P2-S1:** `SubstringRangeTest`'s raises are all literal indexes over a one-row table. Add a computed start or end
  (from a column), in range on one row and out of range on another. That shows the guard is the database's, row by
  row, and covers the computed paths of the fold. Today only the lean-SQL pins' text covers them.
- **P2-S2:** for the 3-argument `indexOf` with an empty needle and `from` past the length, lite answers `from`. Java's
  `"abc".indexOf("", 5)` answers the length, 3. If legend-pure's `indexOf(s, t, from)` is Java's, as the comment and
  S134 say, read it at the pin. The fix is `from0 = least(max(from, 0), LENGTH(s))`, or a register row.

**Notes:**
- **The cross-kind fix:** the shape note (rev 2, §5.6) says "The cross-kind `!=` fix gates A3" (`63421e0a`). That
  fix is on neither `main` nor this branch, and `claude/compat-cross-kind-ne` does not exist on origin. But the gate
  on `09183550` ran the corpus verdicts green on this branch without it (`gate-summary.txt`), so the committed +7 rows
  per database are this branch's own. The note should say the fix no longer gates A3, and that it carries its own
  moves when it lands.
- **The JDK:** `PureMessages`' comment says "the JDK 21 text measured here ... read at the engine runner before this
  lands". S383 pins JDK 25.0.4's text, and the engine runner has since answered with the same form. Update the
  comment to cite those readings.
- **Lean SQL:** checked row by row on the diff of `core/src/test/resources/lean-sql/baseline.tsv`: 29 rows, 59 numeric
  pins grow, none shrink. The keys are those `lean-sql-growth.txt` names: `indexOf`'s two forms across channel A and
  core, and core `substring`, its sort key, extend and project. Ruled by the user (~01:35).
- **The other moves,** ruled in the translator's session ("approved: push both A3 branches and make piece 2's moves"):
  - PCT −63;
  - the 9 re-pins;
  - the corpus: +7 fail rows and −4 unordered-register rows per database. The 7 are the three `testFilterUsing...`
    tests, `testToSQLStringSubstringIsUncorrectedByDefault`, and H12's three plan-text tests, whose
    `indexOf(...) != -1` was vacuous under the old meaning;
  - the 5 edge-net re-records.
- **The PCT adapters** now set no flag (`c2cb56bf5`), as upstream's own adapters do. `DETERMINISTIC_AS_OF` stays.

## Stage-A combination audit: `origin/claude/compat-a4-host-only` at `71b7bf55e` over `main` `8eea8fa1` (2026-11-20)

**The question:** every piece was approved on its own; this audit checks the pieces together. Each finding was sent
to the compat owner as it was found.

**Verdict: no merge defect, and the interactions are clean, except M-a1. Before landing:**
- **M-a1:** an `engine` and S383 interaction, the user's call;
- **M-b1 to M-b4:** four guard moves without the user's word;
- **M-c1 to M-c3:** register rows (the code against S130/S383 via P2-M1, S383's stale status, six rows PROPOSED);
- **P2-M1:** piece 2's open must-fix.

### (d) The merges
- **28 merges** were re-merged with `--remerge-diff`: 19 are clean. The 9 hand resolutions each keep both sides:
  - `b08ec103e`: #124's audience enum beside A5's lanes.
  - `efc3a4e0e`:
    - VerdictSql: A5's join order, without A4's leniency;
    - RootLoops: A5's held rejection, without A4's `orElse`;
    - TestRunner: A5's rejection path, with A4's judge call (no `rowAppeals`);
    - pct/BUILD: A4's host lanes gone, A5's LITE lanes kept.
  - `5ab730cd0` and `9b3b6581e`: every dated ratchet line, plus the merged-tree lines.
  - `ca26a09d5`: A2 PR 2's ledger values.
  - `22ac877ab` and `caca297b4`: the detector's lambda-only rule, its roots and R2-S1.
  - `f7b72ed94` and `dfe2d189f`: the cumulative ceilings (`INTERNAL_DESUGAR` ≤ 41, NOT_UPSTREAM 78, Intrinsic 740),
    and piece 2's no-flag inside #124's audience switch.
- **Every piece's tip is an ancestor of the head.** The review-only branch is not.
- **The wiring re-applied at `087ee888e`** is identical to the reviewed copy, except PureText, where A4 deleted the
  database judge's words, and the engine pins' i04 rows, whose lite side piece 2 moved.

### (b) Guard moves without the user's word
- **M-b1:** `22d53782f` re-records the ENGINE lane's six pin files, a golden re-record.
- **M-b2:** `087ee888e` moves the warn battery pins: H2 AGREE 46 → 11, DuckDB 31 → 8. ~22:20 covers only the new
  engine files.
- **M-b3:** `39956d2b7` moves `own_corpus.matched` 3325 → 3327. The ~22:22 word list has only the +5.
- **M-b4:** A2's ord registers (+5/+4) and A2's fail rosters (+145/+149). No ruling is found in the log.

Notes:
- The EnginePlans 760 → 757 note cites "stage A ratified", not ~20:45. It is a down move, so it is covered.
- A5's new guard (`b0767cb7a`) is a tightening.
- The six accepted rows are verified at the head.

### (c) The register rows
- **RULED, and the code matches:**
  - S128 and S316 (amended ~20:05);
  - S386 and S387 (meanings ruled 2026-09-05/08);
  - S388 (~23:20).
- **M-c1:** S130 ("the flag a documented no-op") and S383 ("raises on all three") are contradicted at the head by
  P2-M1: the flag road and `substr` do not raise.
- **M-c2:** S383 is ruled ~01:35, but its row still says PROPOSED.
- **M-c3:** S130, S134, S380, S382, S384 and S385 are PROPOSED, on the ~22:22 word list. Their content matches the
  code.
- **Note:** S381 is cited by S382 and has no row.

### (a) The interactions
- **M-a1:** under `engine`, the literal-0 exemption leaves an H2 `substring(x, 0, n)` unrewritten.
  - With `n` past the length, piece 2's S383 guard raises where legend-engine answers the text
    (`h2-literal-zero.tsv`: `('abcdef', 0, 9)` → `'abcdef'`, `('', 0, 2)` → `''`).
  - The same happens under `CORRECT_SQL_SUBSTRING_INDEXING` once P2-M1 is fixed.
  - The options, for the user:
    - (1) under `engine`, ignore the exemption and rewrite every store-bound `substring`;
    - (2) record the narrowed promise in S130/S383, ruled.
  - Either way, add a battery row with an end past a value's length.
- **Clean:**
  - **A2 against the detector and the rewrite:** neither reads a printer, and SUBSTRING_AS_GIVEN has its arm on every
    edge dialect.
  - **A4's guard against A5's lane:** LITE_OPTIONS keep PRODUCT with BOTH, and COMPAT forces HOST.
  - **The window fix against piece 2:** a level is kept, never an answer changed. The merged-head lean-SQL and
    query-capture runs are the proof, and should be in the judges' measurement.
  - **The engine rewrite against piece 2's meaning:** a flagged call becomes the unguarded `substringAsGiven`, and an
    unflagged call keeps Pure's meaning (M-a1).

## A3 piece 2's review fixes, `origin/claude/compat-a3-switch` at `97d4c78b2` (3 commits on `1f4b108d1`)

**Verdict: APPROVED. P2-M1, P2-S1 and P2-S2 are met, and with them M-c1 is met at the code. The batch head
`71b7bf55e` does not yet carry these commits, so they must be merged into it before the judges' measurement.**
- **P2-M1:** one rule, `FeatureRules.PURE_SUBSTRING` (its body `Scalars.pureSubstring`, the guard unchanged), serves:
  - plain `substring`;
  - the flag's registration (`under(CORRECT_SQL_SUBSTRING_INDEXING, ..., PURE_SUBSTRING)`);
  - `substr`.

  So the flag is a true no-op, the raise included. FeatureRules' javadoc now says so, and `CORRECTED_SUBSTRING` is the
  in-range value alone. `SqlFnSortedTest`'s pin holds: no SQL function gains a writer class.
- **P2-S2:** `indexOf(s, t, from)` clamps `from` to `[0, LENGTH(s)]`, which is Java's `String.indexOf` (read at the
  pin: `IndexOfString.java:60`, `CompiledSupport.java:1536-1539`). The upper clamp is folded away only for a literal
  `from` of 0 or less, or a non-empty literal search, where it can never matter. For a non-empty search, a `from` past
  the end gives −1 either way.
- **P2-S1:** the new tests:
  - `substr` and the flagged `substring` raise;
  - a computed index (the row's `id`) is in range on one row and out of range on another (a second row `9,Al`);
  - the empty search past the end (`8, 3, 0, -1, 8`).
- **The moves** quote the user's word in `97d4c78b2`: "approved: push the review fixes and make the moves (corpus
  +2/-1 per database, edge net, lean SQL +172/-2, census +17)". I counted them on the diff:
  - lean SQL: 68 rows, 172 up and 2 down;
  - the cast-case census: 28 rows, 17 up and 11 down (the down moves are covered by ~20:45);
  - the corpus: +2 fail rows and −1 unordered row per database;
  - the edge net: 2 lines per database.
- **The JDK comment** in PureMessages now cites the JDK 25.0.4 measurement.
- **Note:** `97d4c78b2`'s message says "the compatibility edge's detector names store-routed substr as it does
  substring". It does not, at the head or on this branch: `PureMeaningCheck.call` matches `AT_STRING_SUBSTRING` alone.
  That is right as designed: legend-engine always corrects `substr` (processSubstr), so its only difference from Pure
  is S383's raise, a recorded difference that the detector does not flag. Correct the sentence where it is carried
  into a record.
- **M-a1 now applies under the flag too, as expected.** Under `engine`, with `CORRECT_SQL_SUBSTRING_INDEXING` set or at
  an exempt literal 0, an out-of-range call raises where legend-engine answers. This is still the user's call.

## The merged head with piece 2's fixes, `origin/claude/compat-a4-host-only` at `317204773` (3 commits on `71b7bf55e`)

**Verdict: APPROVED. The merge is clean, M-c2 is met, the S381 note is met, and the regeneration is consistent.**
- **`984e3b632` merges `97d4c78b2`.** `--remerge-diff` shows conflicts only in the two corpus edge ledgers, which
  `317204773` regenerates.
- **`317204773`** runs all eight generators on the merged tree, and only the two ledgers move. Against `71b7bf55e`, each
  database moves:
  - `testToSQLStringSubstrFoldsLiteralIndexes` and `testToSQLStringSubstrWithComputedIndex`, PASS → FAIL;
  - `testSubstringIndexingCorrectedByFeatureFlag`'s failure words, FAIL either way, because its plan text now carries
    the guard.

  That is the ruled roster +2. At the head, both tests are on both fail rosters, and `...WithComputedIndex` has left the
  unordered register. `...FoldsLiteralIndexes` keeps its unordered row: its assert 0 still compares, and assert 1 is the
  one that fails.
- **`9c4c9f231`:**
  - S383 now records the user's ~01:35 ruling (M-c2 met);
  - S381 (A5's single-threaded DuckDB test plans) is added as PROPOSED, so S382's reference resolves. It needs the
    user's word at the batch PR, like the other PROPOSED rows.
- **The judges report** `//core:lean_sql_baseline_test` and `//core:query_capture_test` green on this head. So piece 2's
  lean pins hold with the window fix in, which settles the proof I asked for in part (a).
- **Nit:** S383's lite column still says "raises ... on all three" without naming `substr` and the flagged `substring`,
  which raise since P2-M1. B1-R's pass rewrites S130, S134 and S383 for the deleted setting, so fold it in there.
- **Still open, for the batch PR or the user:**
  - M-b1 to M-b4 (the moves without a ruling);
  - M-c3, now with S381 (PROPOSED rows);
  - M-a1, superseded by the user's ~07:55 ruling: the door always rewrites, with no literal-0 exemption. It is checked
    in B1-R's pass.

## B1-R's pass: the setting deleted, the door always rewrites (`origin/claude/compat-a3-engine` at `aacb49b96`, on `317204773`)

The pass is `c35e90888` (the `substrAsGiven` native, the user's ~07:45 "Okay 1") and `aacb49b96`
(`LEGEND_LITE_PURE_MEANING` deleted, the user's ~07:55). Both rulings were read verbatim in the owner's log at
`25ede64f`, §1.

**Verdict: the code is APPROVED. The docs commit is still to come, and the up moves go on the PR's word list.**
- **The dead-path grep at the head** (code, tests, pct, spec, tools) finds none of these:
  - `Mode`, `mode()`, `SETTING`, `HEADER` or the sentence's refuse/warn branches;
  - the `getenv`, `withMeaning` or the WARNING line;
  - checkMeaning's switch, TestDataRoutes' `meaning` or `doored`'s discarded rewrite;
  - LiteBatteries' `--meaning` or the warn battery half;
  - `zeroStartAnswersAsPure` or `zeroStartExempt`.

  What remains is a test constant (`OLD_HEADER`), used to assert that the header is not sent, and
  ObservabilityGuardrailTest's dated note.
- **The detector:**
  - the four new codes (`CORRECTED_SUBSTRING_2/3`, `SUBSTR_2/3`) flag `substring` under the flag and `substr`;
  - no call is exempt, which covers both halves of M-a1, H2's 3-argument `substring(x, 0, n)` included;
  - the walk has no target any more;
  - a refusal names its calls (`calls()`) and counts.
- **The rewrite:**
  - `PureText.CHECKED = {H2, DuckDB}`. Any other database with a flagged call is refused by name ("legend-engine's
    answer for ... is not yet checked on Postgres");
  - unverified calls, unreachable calls and a shared let root are refused as before;
  - `checkMeaning` is the rewrite, always.
- **D-S2:**
  - `EngineSubstitutes.substr` is a callee swap through `substitute()` and `checked()`, replacing `AT_STRING_SUBSTR`
    and `AT_STRING_SUBSTRING`;
  - the arithmetic is the lowering's: `Scalars` writes `SUBSTRING_AS_GIVEN(s, start + 1[, end - start])` as SQL,
    which is processSubstr's correction, with no fold, clamp or check in Java and no typed arithmetic at the door;
  - `SubstringClamp` stays off it, as for `substringAsGiven`.
- **The batteries:** ten rows on a value shorter than its end, on both databases, covering both halves of M-a1:
  - `col`/`lit.s19`: the literal-0 `substring(0, 3)` on 'FX';
  - `r01`/`r02`: `substr(1, 3)` and `(0, 3)`;
  - `c01`/`c02`: the flagged `substring(1, 3)` and `(0, 3)`.

  All ten AGREE with the engine runner's pinned answers. H2 is now 56 AGREE, 1 BOTH_FAIL, 2 DIFFER; DuckDB 57 and 2.
  The DIFFERs are still the i04 pair (E-R2).
- **The plants**, taken as reported, each turn its tests red: s1, s2, s3, n1, n2, W1', W2' and R1.

**The test-data choice B1-R put to me: I agree, and do not overrule it.**
- **What changed:** `generateTestData_WithSeed` is not refused on an unchecked database, because its rows are the
  identifiers'. `TestDataPlanner.plan` reads the request's query through `ScanRelations.relTree` alone, for its tables
  and columns, so no call's value chooses a row. Refusing it would be a false refusal.
- **The other two routes:** `generateSeedData` and `generateTestData_WithDefaultSeed` still pass their seed through
  `checkMeaning`, so they are rewritten or refused by name.
- **Should:** a test pinning the choice. On Postgres, `WithSeed` with a flagged call answers, and gives the same CSV as
  the same request without the call.

**For the PR's word list (up moves; "Okay 1" chose the native, and its log row says "its guard moves posted before they
land"):**
- `INTERNAL_DESUGAR` 41 → 42;
- `NOT_UPSTREAM_MAX` 78 → 80;
- Intrinsic 740 → 742;
- `native-claims.tsv`'s two `substrAsGiven` rows;
- the engine battery pins' +10 AGREE rows per database, and the engine runner's pinned answers, +10 rows per database.
  `c35e90888` says "the pins did not move", but each `agreement_engine_*.tsv` gains ten rows and its header count moves
  46 → 56 and 47 → 57.

Down moves, covered by ~20:45:
- the warn pins `agreement_{h2,duckdb}.tsv` deleted;
- `ObservabilityGuardrailTest.ENV_FLAGS` −1.

The new `ClaimRegistryTest` pin is a tightening.

**The docs commit, to check when it lands.** These still name the setting at `aacb49b96`:
- `docs/SEMANTICS_REGISTER.md`: S130, S134 and S383, plus S385 and S388 per the ~07:55 row;
- `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md`;
- the design note (1 mention) and the shape note (3);
- the rules of record's switch registry, whose first entry was `LEGEND_LITE_PURE_MEANING`. AGENTS.md's tenet 6 still
  reads true.

S383 should also name `substr` and the flagged `substring`, which raise on lite's own doors.

**The docs commit, `67fa4df51` (on `aacb49b96`; the batch is fast-forwarded to it): APPROVED.**
- **The setting is gone everywhere outside the records,** except as history: AGENTS.md's tenet 6 ("The registry is
  empty: its first entry ... was deleted"), ObservabilityGuardrailTest's dated note, and the test constant `OLD_HEADER`.
- **The rules of record:** the switch registry stays, empty, with its ceiling and guards, and its first entry is marked
  deleted with the ~07:55 ruling.
- **The register:**
  - S130, S134, S385 and S388 describe the door with no setting: rewritten on H2 and DuckDB, refused by name
    elsewhere, a host-side statement Pure's, M-a1's no-exemption, the 56/57 battery counts;
  - S383's lite column names `substr` and the flagged `substring` (my nit) and the door's `substrAsGiven`, with
    `'FX'->substr(1, 3)` answering `'X'`.
- **The differences page's item 1:** the door's meaning, lite's own route, the raise out of range, and the in-range
  translation.
- **Nit:** S130's lite column still opens "... with or without it, on every road", while its next sentences describe
  the door's rewrite. "On lite's own roads", as S134 says, would be exact.
- **Still for the user's word at the PR:**
  - S130, S134, S385, and S380, S381, S382 and S384 (PROPOSED);
  - the up moves listed above;
  - M-b1 to M-b4.

**The two follow-ups, `origin/claude/compat-a3-engine-followup` at `2e24fb668` (off the batch, to land after #128): APPROVED.**
- **`d54c7818c`:** S130's lite column now reads "on lite's own roads", as S134 does. My nit is met.
- **`2e24fb668`:** `//core:pure_meaning_postgres_test` checks `generateTestData_WithSeed` on the embedded Postgres 16, a
  database not yet checked, run unprivileged.
  - It sends a flagged filter (`'xpublic'->substring(1, 7)`) and a clean one over `information_schema.schemata` with the
    identifier `'public'`. Both answer 200 with the same CSV, byte for byte: the identifiers' row alone, with no
    `pg_catalog` in it.
  - It asserts its own precondition (`!engineAnswers(Postgres)`) and writes nothing.
  - The plant, taken as reported: `doored` refusing a flagged call on an unchecked database turns the test red.
  - This pins B1-R's WithSeed choice, which I agreed with.
- **The moves:**
  - a new core target, and the local gate's Postgres list gaining it, which is a tightening;
  - `own_corpus.matched` 3327 → 3330 (the test's Database, Class and Mapping match the reference grammar), with its
    dated HISTORY line. It is an up move, for the user's word at the PR, beside M-b3's 3325 → 3327.

## The ClickHouse lambda-raise fix, `origin/claude/window-list-lowering` at `852a9737c` (`b87d3e789` and `852a9737c` on `67fa4df51`)

**Verdict: APPROVED. The lean-SQL re-pin, `852a9737c`, waits for the user's word.**
- **The guards are correct, NULL included:**
  - In a lambda, a WHEN branch's guard is the conjunction `NOT coalesce(c₁, false) AND … AND cᵢ`, and the ELSE's is
    every `NOT coalesce(cⱼ, false)`. That is exactly CASE's choice: a later branch is taken where an earlier condition
    is false or NULL.
  - Where the branch's own condition is NULL, the conjunction is NULL. ClickHouse's `if()` takes its else on NULL, so
    the cast reads `'0'` and nothing raises, as a CASE takes no branch on a NULL condition.
  - Nested CASEs join their guards outermost first, each parenthesized.
  - The date parameter's raises: the year's is guarded by `yearOutside`, where a NULL parameter makes it NULL, so there
    is no raise; the fraction's by `NOT coalesce(yearOutside, false) AND fractionPast`.
  - ADD_INTERVAL's guard is its range check.
- **Byte-identical outside a lambda:** unguarded, `error()` still writes `toInt64(` + `concat(…)` + `)`, the same text.
  `caseExpr` pushes nothing outside a lambda, and the date CASE's pieces are the same text. Only two lean rows move,
  both lambdas on ClickHouse. The render test asserts `toInt64(concat(` and no `toInt64(if(` outside a lambda in each
  case.
- **Invariant 3:** the diff adds no `default ->`.
- **The stated limit (an `if()`/`multiIf()` raise not through a CASE) is not reachable from the MIR today:**
  - the MIR has no IF function (its conditionals are `SqlExpr.Case`);
  - the dialect writes a raise at two sites alone, the `ERROR` arm and `guardedError`;
  - none of its own `if(`/`multiIf(` texts holds one.
- **State:** `lambdas` and `raiseGuards` are per instance, as `raises` and `correlatedExists` already are.
  `Databases.dialect` makes a new ClickHouse per call.
- **The tests:**
  - render: THEN, a later WHEN and ELSE, each with its exact guard; date arithmetic; the date parameter's two raises;
    and outside a lambda as before;
  - `//core:clickhouse_lambda_raise_test` on the embedded ClickHouse, manual and in `//gates:heavy`: valid text
    answers `'o,m,r'` and out-of-range text still raises; dates in range answer and past 2299 still raise. Each was red
    without the change, as reported.
- **Should (small):** a server case with a NULL element. A lambda over `['a', NULL, 'abcdef']->map(x|$x->substring(1,
  3))`, or a CASE whose first condition is NULL for one element, would pin at run time that a NULL condition neither
  raises falsely nor hides a later branch's raise. Today only the render test's guard text shows the `coalesce`.
- **The re-pin `852a9737c`, for the user's word:** two ClickHouse channel-A rows move up:
  - `removeDuplicatesBy`: +96 characters and +100 plan JSON;
  - `sort` with a substring key: +192 and +198.

  Statements and subselects are unchanged.

### Closed: the NULL-condition should is met (`4d5199577`, 2026-11-20)

**Verdict: met.** `4d5199577` sits on top of the held re-pin `852a9737c` on `claude/window-list-lowering`.
- **What it changes:** one test file (+44 lines). No product code, pin, ledger, golden or roster moves.
- **What it adds:** `ClickHouseLambdaRaiseServerTest.aNullConditionNeitherRaisesFalselyNorHidesALaterRaise`. It runs on
  the embedded ClickHouse, in the existing manual target in `//gates:heavy`.
- **How it is built:** the dialect's own SQL for `arrayMap(x -> CASE …)` over `['a', NULL, 'abcdef']`. A Pure list
  literal holds no NULL, so the CASE is built as the MIR and rendered by the backend's dialect.
- **Case 1:** `WHEN length(x) > 9 THEN error('boom') ELSE x`. Every element's condition is false or NULL, so the guard
  `coalesce(c, false)` is false and nothing raises. The answer `[a, NULL, abcdef]` keeps the NULL.
- **Case 2:** `WHEN length(x) > 9 THEN 'long' WHEN x IS NULL THEN error('boom') ELSE x`. For the NULL element the first
  condition is NULL and the second is true. The second raise's guard is `NOT coalesce(c₁, false) AND
  coalesce(c₂, false)`, which is true there, so it raises `boom`.
- **The plant:** a guard written as plain `NOT c₁` reads NULL there, so `if()` takes its `'0'` side and the raise is
  hidden. That would turn case 2 red, as reported; I did not run it (no Bazel here).

The re-pin `852a9737c` still waits for the user's word. Its two ClickHouse channel-A rows move up:
- `removeDuplicatesBy`: +96 characters and +100 plan JSON;
- `sort` with a substring key: +192 and +198.
