# BUILD-GUARDS: verdict UNSOUND as it stands (one blocker, a one-line fix); SOUND-WITH-DEBT once it is fixed

## Summary (at most 10 lines)
- F1 is fixed. The eight tests CI's lane guard named at the PR head (job logs 134886148948, 134886167218, 134886140771) are each now in exactly one per-push lane, beside their siblings. None is `manual` or in `heavy`. Studio's suite is now derived from its own glob, so it picks up 7 test files plus typecheck, 8 targets.
- F2 is fixed. These are exactly the three timeouts in PR #118's checks lanes (linux: guardrails_edge 300.5 s; windows: that, lean_sql_baseline_test 300.6 s, shrink_only_test 61.8 s). The lean SQL fix removes a real cost: 2,451 queries share 43 models. Every query is still planned and checked as before.
- F3 is fixed on Linux and macOS. make-dist.mjs now copies the plane's bundle and its chunks. The new test does fail when either is missing. K2 still holds: the bundle is still reached only by `planners.ts`'s dynamic import, and the wheel's include list leaves it out.
- **BLOCKER.** The new `//datacube:dist_imports_test` checks chunk folders with `relative(DIST, f).startsWith('chunks-bundle/')`. On Windows, `relative` returns backslashes, so `windows / datacube` will go red. That lane is green at the PR head (run 44328106324).
- R2 and R3 are dated, name the user's ruling and change no value. One gap: TerminalRowsFrozenTest still records R2's two rows as "the orchestrator's ... not the user's".
- Pin moves: every value that moved is on the approved list, and each has a dated reason in the file or, for the generated ratchet files, in the commit. Two things are not on the list:
  - F8's ledger rewording: it is the fix itself, dated, with no verdict changed.
  - A line join in DatabaseJudge that holds the just-ratified 851 pin. Its honest count is 852 (FIX-AFTER).

## Per commit (sha, claim, verdict)
- 0e991443b, F1, the eight tests each placed in a lane: **fixes it.**
- 47c75dc9d, studio's suite derived from its glob, and the dist walk reads .mjs: **fixes it** (studio verified). The .mjs change carries the Windows bug below forward.
- 42dc96cc5, guardrails_edge sized large: **fixes it.** Linux CI measured 300.5 s; large is 900 s.
- 2da393cf4, lean_sql cause fixed, shrink_only_test sized medium: **fixes it.** It is a real cause, the same work and the same checks.
- a1e6a68a0, the dist ships legend-engine.js and chunks-legend-engine, and a test holds it: **partly.** Right on Linux and macOS; the test fails on Windows (blocker).
- 92fadfebd, R2 ratifying notes: **fixes it**, with one stale record in TerminalRowsFrozenTest (NOTE).
- 4d519abfa, R3 ratifying notes: **fixes it.** Four entry-line notes, no number moved.
- 788ef654e, F7's channel B ratchets: **approved values**, reason dated in the commit (the generated file's convention).
- 373e42960, INTERNAL_DESUGAR 37->38, NOT_UPSTREAM_MAX 72->73 and Intrinsic 734->735: **approved, dated.**
- 5da5a7e40 and eec3c95c7, own_corpus.matched 3309->3310: **approved and dated.** The tsv matches what PeRatchets writes (40/40 comment lines).
- f250b02d6 (N2), "pinned files keep their line counts": **true only by joining a line** in DatabaseJudge (finding 2).

## Blockers
1. **BUILD-GUARDS-1.** `//datacube:dist_imports_test` will fail on CI's Windows `datacube` lane.
   - **Fix.** In `datacube/test/dist/imports.ts:73`, compare a separator-neutral path. Either use `relative(DIST, f).split(sep).join('/').startsWith(\`${d}/\`)`, importing `sep` from `node:path`, or use `.replace(/\\/g, '/')` as `datacube/test/guardrails.test.ts:391` already does. About 2 lines.
   - **Proof.** The `windows / datacube` lane. Locally, the bite test could also run the check over `path.win32`.

## Findings

### BUILD-GUARDS-1 [BROKEN-RULE] [BLOCKER] The dist test's chunk check is POSIX-only, so `windows / datacube` will go red
- **Where.** `datacube/test/dist/imports.ts:72-74` (a1e6a68a0). It runs in `//datacube:tests` → `//gates:datacube`, on every OS. The target has no tag and no `target_compatible_with` (`datacube/BUILD.bazel:2536-2542`, `:1151`).
- **What.** The check `[...reached].some((f) => relative(DIST, f).startsWith(\`${d}/\`))` runs over `chunks-bundle`, `chunks-bundle-engine` and `chunks-legend-engine`. The walk's paths come from `resolve()` and `relative()` in `node:path`, which use backslashes on Windows.
- **Measured** with `node -e` and `path.win32`, for DIST `C:/b/.../datacube/dist`: `relative(DIST, chunk)` is `"chunks-bundle\\chart-AB12.js"` and `.startsWith('chunks-bundle/')` is `false`. The `reached.has(join(DIST, f))` checks agree on Windows, so only the chunk assertion fails. It fails with "the walk reached no file of chunks-bundle/".
- **Why it matters.** `windows / datacube` is green at the PR head (run 44328106324: success). The batch would turn it red, and "gates green" would fail.
- **Instead.** Normalize the separator, as the package's other tests do (`guardrails.test.ts:391`, `brand-casts.ts:51`).
- **Confidence.** High. Verified by reading and by simulating Windows paths in Node; not run on a Windows runner.

### BUILD-GUARDS-2 [BROKEN-RULE] [FIX-AFTER] DatabaseJudge's ratified 851 pin is held by joining two lines
- **Where.** `core/src/main/java/com/legend/testplan/DatabaseJudge.java:924` (f250b02d6, N2's judges' half).
- **What.** `boolean assertFamily = ... ASSERT || ... ASSERT_FALSE;` was two lines and is now one line of 150 characters. In the same hunk, the `assertMessage(root, words(env).conditionFailed(...))` call grew from one line to two.
  - JavaEvalLedgerTest's stripped count stays 851 (my count: 851 before, 851 after). Unjoined it would be 852.
  - The commit says "The pinned files keep their line counts (JavaEvalLedgerTest: DatabaseJudge 851 ...)".
  - In the same commit, `AssertVerdicts.java:1489` became a single 171-character line, the longest in the file (its previous longest was 162). That holds 1464 by not wrapping. It is a softer form of the same thing.
  - A scan of the whole batch diff for a `+` line equal to two adjacent `-` lines finds only the DatabaseJudge one.
- **Why it matters.**
  - The user ratified "DatabaseJudge 851" in R3 (4d519abfa). Minutes later the file grew by one logical line and the number did not move.
  - This is the "line count held by joining lines" pattern the brief names. The pin still bites on the next line, so the guard is not vacuous, but the record is no longer honest.
- **Instead.** Restore the two lines and re-pin 851 -> 852 with a dated note naming N2, which needs the owner's or the user's word (it is not on the approved list). Or find a genuine one-line shrink. About 3 lines.
- **Confidence.** Verified (`bg/ledger.py`, `bg/joins.py` in this folder's `bg/`).

### BUILD-GUARDS-3 [COSMETIC] [FIX-AFTER] R2's ratification is missing from TerminalRowsFrozenTest's record
- **Where.** `core/src/test/java/com/legend/TerminalRowsFrozenTest.java:59-64`.
- **What.** The TERMINAL map records the ruling that made each row terminal. For the two `testdatagen/TestDataPlanner.java` rows it still reads "the orchestrator's STAYS ruling 2026-11-17", and the comment above says "its own, not the user's -- not RULED, which is the user's". R2 updated only `CompatSpellingGuardTest` (`:89-91`, `:315-317`).
- **Also.** The register's own vocabulary (`CompatSpellingGuardTest:132`) says a user-ruled survivor is `RULED`. R2's rows now read "STAYS ... ruled by the user". Keeping STAYS avoids changing the rows' identity, which is defensible, but now the step and its note disagree.
- **Instead.** Update the TERMINAL ruling strings and the comment to "ruled by the user 2026-11-18 (PR #118, R2)". About 3 lines; no value moves.
- **Confidence.** Verified.

### BUILD-GUARDS-4 [NOTE] The pin, ratchet, ledger and register moves across the whole batch
| What | Old -> new | On the approved list | Dated reason where | Does it weaken the guard? |
|---|---|---|---|---|
| channel B `relation.judge.chars` | 1989680 -> 2006804 | yes | commit 788ef654e, with the user's "raise them" (a generated file with no history list: PctRatchets) | ceiling +17,124 (+0.9%) |
| channel B `relation.judge.plan` | 34485688 -> 34502872 | yes | same | +17,184 |
| channel B `standard.judge.chars` | 1547392 -> 1647537 | yes | same | +100,145 (+6.5%), FLOAT_SHORTEST_TEXT's decomposition |
| channel B `standard.judge.plan` | 24441771 -> 24542270 | yes | same | +100,499 |
| `INTERNAL_DESUGAR` <= (NativeCatalogGovernanceTest:174-177) | 37 -> 38 | yes | in place, with the user's "raise it" | admits assertFailedText only |
| `NOT_UPSTREAM_MAX` (CatalogUpstreamDiffTest:233-237) | 72 -> 73 | yes | in place, with the user's "Okay" | the same native |
| spec `implementation.kinds.Intrinsic` | 734 -> 735 | yes (owner, by delegation) | commit 373e42960 (a generated file; SpecRatchets puts the reason in the commit) | a measured count |
| `own_corpus.matched` | 3309 -> 3310 | yes | the tsv comment and `PeRatchets.HISTORY`, word for word | a floor rising: stronger |
| channel B `ledger.tsv`, 12 rows | text only, "DataError: the value is outside the range this database can hold: <reason>" | not a pin; it is fix F8 | dated group note above each of the 4 groups | no: same rows, same verdicts. Its lanes are `heavy`, not per push |
| CompatSpellingGuardTest REGISTER | `VerdictQueries.java \| Assert failed \| 1 \| STAYS` removed; R2 notes added | a shrink | dated (R1) | stronger: the spelling is gone from VerdictQueries (grep: 0) |
| TerminalRowsFrozenTest TERMINAL | the same VerdictQueries row removed | a shrink | dated | consistent: an exact set equal to the register's terminal rows |
| JavaEvalLedgerTest EVICT_SIZE | 4 entry-line notes (R3), no number moved | R3 | dated | none, apart from finding 2 |
| native-claims.tsv | +1 row, `assertFailedText` (SCALAR_RULE, Coercions) | generated (deee2d6ee) | commit | consistent with INTERNAL_DESUGAR |
| lite-api-cases.json | +1 case, `bindings: ["server"]`, the refusal's `raise` and `where` held | a fixture | dated | stronger on the server. Python is excluded and named as open in the file's own note |

Held unchanged by restructuring rather than moved (each checked; none is vacuous):
- **SqlFnSortedTest.** LiteText's REGEXP_REPLACE now goes through `LiteralSpelling.withoutDecimalMark`, an already-pinned writer, and `decimalCanon` shares it, which also removes duplication.
- **ErrorShapeGuardrailTest `CATCH_RETURNS_VALUE` = 32.** The two new catches in `SavedQueries.java:266` and `:282` rethrow a refusal; they return no value.
- **Equality's caller register.** `LiteHostWords.comparedKind` uses an if-chain where an enum switch would have made a synthetic class.
- **FixtureWriteCensus.** AssertBarTest's rows come from a declared-data load.

The `shrink-only` CI job watches only the Java pins. In the batch's watched files, entries were removed or comments added, and nothing was added or raised, so it passes by reading. The fixer ran it.

### BUILD-GUARDS-5 [NOTE] Lanes the batch's behaviour changes reach that no commit reports running
- F4 changes the host judge's assertEq to compare by the declared kind and strips the D from lite's Decimal text. F7 changes the edge's relation-text Float cells. N2 changes the in-body assert's default.
- The commits report the core shards and judges, the census and guardrails, channel B (all five suites at 373e42960), `edge_contract_test` (before F7), lean_sql_baseline (before N2) and the hosts' suites.
- None reports `pct_duckdb`, `pct_h2`, `pct_postgres`, `corpus_duckdb`, `corpus_h2`, `//spec:lean_sql_corpus_{duckdb,h2}` or `stress` after F4 and F7.
- By reading the risk is low:
  - V-EDGE found no lane sensitive to F7's form.
  - The lean SQL sets hold no assert (grep: 0 in `channel_a.jsonl` and `core.jsonl`).
  - The corpus lean SQL leaves out verdict SQL, where LiteText lives.
  - PCT runs the edge's words.
- These are exactly the lanes the owner's heavy round should include.
- First runs off Linux: the eight newly laned tests have only ever been measured on Linux. In CI they will now run on macOS and Windows too, and the warehouse one on linux-arm.
  - Their siblings in each lane already run there, and a scan found no path-separator assumption in them.
  - The first full CI run decides.

### BUILD-GUARDS-6 [NOTE] `//engine-client:tests` is still a hand-kept list
- **Where.** `engine-client/BUILD.bazel:658-675`.
- **What.** Studio's suite is now derived by construction (`studio/BUILD.bazel:199,225,259`), but engine-client's, where two of the eight were missing, is still hand-kept.
- **Why it matters.** Little: CI's lane guard catches any omission, as it did here. Deriving the list as studio's is would make the class impossible.

### BUILD-GUARDS-7 [COSMETIC] [NOTE] Pure.java: N2's new signature sits under TYPE_RAISE's javadoc
- **Where.** `core/src/main/java/com/legend/builtin/Pure.java:981-983`. N2's share, seen in passing.
- **What.**
  - The javadoc "A run-time type check's failure (see Lite#TYPE_RAISE) ..." now sits above `ASSERT_FAILED_TEXT`, and `TYPE_RAISE__STRING_1__STRING_1__STRING_1` has lost it.
  - The new field breaks the signatures' `NAME__ARGS` naming.
  - It shares the name `ASSERT_FAILED_TEXT` with the `Pure.Lite` String constant.

## Strengths
- **F1.**
  - Each test went where its siblings already run: the embedded-Postgres probes in `core`, the wasm module tests beside `differential_test`, `edge_test` beside `engine_test`. Each carries a dated comment and `//gates` visibility.
  - No test is in two lanes: grep finds one reference each. The two core classes are already excluded from `core_tests_lib`'s glob (`core/BUILD.bazel:621-626`), so they do not run twice.
  - `edge_test` adds no native image: `//native:engine` is already built in that lane for the `//datacube:python_*` tests.
- **F2.**
  - The lean SQL change memoizes per (database, model), inside the database loop (`LeanSqlBaseline.java:193,207`). This is the pattern `measureOwn` already uses, and the product's own held-model pattern.
  - A model that fails to compile is not cached, so it is refused again for each of its queries, as before.
  - Every query is still compiled, planned and measured, and the drift rule is untouched. The fixer measured 114.5 s -> 36.6 s.
- **F3.**
  - The fix is the minimum: 2 copies. The test reads the dist from the pages themselves, follows static and dynamic imports and `new URL(..., import.meta.url)`, has a bite case, and fails both when `legend-engine.js` is missing and when its chunk folder is.
  - The dist is main's set plus the plane's bundle and chunks. K2 is unchanged:
    - the import is dynamic and taken only on the `engine` plane (`planners.ts:33,94`);
    - `engine.ts` does not import `planners.ts`;
    - the wheel's `wheel_site` include list leaves the file out.
- **Pins.** All approved moves carry dated reasons. The fixer chose to restructure rather than grow three registers (Equality, the catch count, SqlFnSorted), and two of those are real design improvements, not just evasions.
- **Generated files.** These are consistent at the head:
  - //:update_generated was run at 5da5a7e40, after the last code commit;
  - the HISTORY/tsv pair was checked line by line.

## Measurements (numbers with the command that produced them)
- **PR #118's failed jobs** (`gh run view 44328106324 --json jobs`; `gh run view --job <id> --log-failed`):
  - `product` on all three OSes: exactly the 8 tests;
  - linux checks: `//core:guardrails_edge TIMEOUT in 300.5s`;
  - windows checks: guardrails_edge 302.6 s, lean_sql_baseline_test 300.6 s, shrink_only_test 61.8 s;
  - `windows / datacube`: success.
- **CI on the fix branch:** `gh run list --branch claude/pr118-fixes` returns no runs.
- **Channel A set:** 2,451 queries, 43 distinct models (`python3 -I` over `core/src/test/resources/lean-sql/channel_a.jsonl`).
- **Stripped code-line counts** (JavaEvalLedgerTest's own rule, `bg/ledger.py 68f20eb4d eec3c95c7`), all unchanged:

  | File | Before | After |
  |---|---|---|
  | HostJudgments | 679 | 679 |
  | PureAsserts | 107 | 107 |
  | AssertVerdicts | 1464 | 1464 |
  | DatabaseJudge | 851 | 851 |

- **Joined-line scan** (`bg/joins.py`): 1 hit, `DatabaseJudge.java` `@@ -923,3 +923,2 @@`.
- **Line lengths** (`awk 'length>120'`): `AssertVerdicts.java:1489` is 171 characters (the file's maximum; the previous maximum was 162). The joined DatabaseJudge line is 150.
- **Windows path simulation:** `node -e` with `path.win32` gives `relative(DIST, chunk)` = `"chunks-bundle\\chart-AB12.js"` and `startsWith('chunks-bundle/')` = false; `join` and `resolve` agree.
- **PeRatchets:** the HISTORY entries, rendered as the generator writes them, equal the tsv's comment lines (40 = 40, True).
- **Studio:** 7 files match `test/*.test.ts`, which with `typecheck_test` gives 8 suite members.
- **Assert sets:** `grep -c assert` over the two lean SQL sets gives 0 and 0.

(The `bg/` scripts and logs are under `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/fixreview/bg/`.)
