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
