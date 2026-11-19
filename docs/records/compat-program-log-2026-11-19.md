# The compatibility program's log (from 2026-11-19)

The running record of the program: every ruling, every finding, every number, every session and branch, written as it
happens (the user, 2026-11-19 ~10:45 UTC: "write down all the learning/outcomes/details immediately").
- **The plan:** `docs/records/compat-program-plan-2026-11-19.md`, on this branch (`claude/compat-program`).
- **The owner:** this session (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **Order:** newest entries go at the end of each section.

## 1. Rulings (the user's words, verbatim)

| When (UTC) | Words | What it rules |
|---|---|---|
| 2026-11-19, before 03:50 (relayed to B2; the words as recorded in `docs/records/census-b2-inventory-2026-11-19.md` §6.2 on `claude/census-b2`) | "we're not going to change prelude or parser" | the prelude's and the parser's upstream bodies and words stay (the census's N-PRELUDE, N-PARSER and COMPLETENESS-7 are (b)); register row S376 |
| 2026-11-19 ~04:02 | "1-3 yes" | R4: lite's `substring`/`indexOf` follow Pure (CP-2); E-R1 (CP-3, later switched); upstream's SQL text never promised (CP-4) |
| ~04:02 | "Explain 4 clearly, what's the alternative? Is this a sneaky way to have two sql paths" | led to CP-5's revision: no special functions, pending the count (0.4 (j)) |
| ~04:02 | "Let's not worry or even support older database versions" | CP-8: legend-engine's H2 2.1.214 LEGACY and DuckDB 1.3.0 are neither emulated nor chased; one blanket register row |
| ~04:30 | "Switch e-r1" | CP-3 is E-R2: the legend-engine route answers everything lite can |
| ~04:30 | "We definitively should not delete the fallback that checks answers when SQL doesn't match right? That's one of the crown jewels of this project" | the SQL-text appeal and the referee are KEPT; under CP-4 they decide every SQL-text assert by answer |
| ~10:45 | "yes to 1-5 and the independent review, do as much homework, research, measurements right now up front as possible and write down all the learning/outcomes/details immediately" | 1, CP-1 (translator in, formatters out, no modes); 2, sessions may push their own `claude/compat-*` branches; 3, two new sessions now (judges, translator), hosts in week 2, xhigh; 4, CP-9 (per-push checks, the oracle's `jars_table` entry); 5, CP-10 (housekeeping); and an independent review |
| ~11:27 | "1 yes, 2 a1" (on the hosts worker's `currentUser` case) | `/api/server/v1/currentUser` on both hosts: GET answers 200 `"anonymous"`, any other method 405 with lite's upstream-405 body, plus one register row (the 405 body stands in for Dropwizard's, unread; HEAD and OPTIONS unread). Python's user is `"anonymous"` everywhere, with a one-time re-owning of records saved as 'this user' (a data migration, never a run-time fallback) |
| ~12:10 | "Yes to the six" | CP-13: defer phase 5 (export, import) and hand the LITE-host drift fixes of 1.5 and 1.6 to the product track, keeping HOSTS-9's store pilot (R11(a)), CP-7's part, the ENGINE host pieces and 1.7. CP-14 (iii): the compatibility judge is lite's verdict records worded by legend-pure's presenter, with four door rewrites of corpus test bodies (S137, S144, S131's typed half, S15). CP-6: the typed facts. CP-7: error words leave the plan, with one `EdgeOnlyDataTest` row. The 6 plan-structure tests are not promised. CP-11: SQLite Decimal as REAL for the legend-pure audience, kept as a translator rule |
| ~13:25 | "Okay agree with all" (to this session's message setting out the implications of the goal's sentence, D1 and D2, each with a recommendation) | **(1) THE GOAL:** "Lite follows Pure. legend-engine's formats are matched, and its answers where they agree with Pure. The rest is recorded." AGENTS.md's upstream-fidelity tenet and G8 are amended to say so (the draft of ~13:15, below), riding with the first stage-A code PR. **(2) D1 (was CP-12): Pure's way, LOUD:** no translator; a detector at the legend-engine server door warns (by default) or refuses (by a server setting) when a query sent to the database calls `substring`/`indexOf` with positions and the request has not set legend-engine's corrected option; built in A3. **(3) D2 (was CP-5): remove both shortcuts** (S20's exact-decimal literal and the unchecked-maths plain set), in B4. **(4) A "differences from legend-engine" page** for people bringing legend-engine projects, built from the register rows |
| ~14:30 | "Yes take over the paused pieces we need and/or fold them into existing sessions" (to this session's question 3: the paused product pieces stage A needs) | The compatibility program owns, as stage-A prerequisites: the Maven mirror fix (`f183b784a`), the PCT-under-both-judges lane C3 with the 5 judge-parity fixes and `firstDayOfWeek` (`claude/parity-a5`), and the cross-kind `!=` fix (`63421e0a`); the window-in-a-list lowering at stage B. Each still lands only on the user's word |
| ~14:55 | "Detector default warn is okay" (to this session's options: (a) warn plus a report when a project loads, recommended; (b) refuse by default) | D1's detector warns by default, with the load-time report as planned (the reviewer's condition for warn); refusal by the server setting `LEGEND_LITE_PURE_MEANING=refuse` |
| ~15:05 | "Wait what? Why are you going against our tenets - everything must be pushed to database" (to this session's call of option 1 on A5's AdjustBy fix) | Option 1 (neither judge sends a date literal to the database) REVERSED as against G6 and G3. Option 2: both judges send the literal to the database, ordered reads make both fail deterministically on the database's own range error, and the difference from upstream is a register row. A standing lesson: a call that takes evaluation out of the database is the user's, and is checked against G6 first |
| ~15:10 | "We have the completely wrong design and plan if you are allowing things to not be pushed to database - legend lite tenet is java orchestrates and database does the work, upstream is not our implementation guide" | The design and plan are to be redone from lite's execution tenet: the database computes every value, text and verdict; Java orchestrates; legend-pure and legend-engine define the answers, never the implementation. Every piece that moves work from the database to Java (stage B's Java formatters, error records worded in Java, verdict presenters, A4 as written) is withdrawn |
| ~15:20 | "Yes to your rule / The host judge is the only exception bc I want to keep that in java for the product and it gets reused in compatibility mode as the only judge / Isn't this all the handoff/getting started? Have we lost that context? I thought we had this decided as part of the plan already / I'm worried we keep relitigating / Maybe we need to write these down in a consolidated way/doc? / Database calculates everything (there is some compiler fold rules for constant string and boolean) including format and tostring / Presentation is thin layer - in product it does not get to do anything but just serialize values correctly / Presentation is compatibility does all the pure or engine fixes in java it's allowed to do those in java to match pure/engine upstream but only from output of typed values from lite / Host judge is only judge in compatibility, database and host both in product with carve out that host is allowed to compare / This really isn't in the handoff doc? Or written as the goal/plan?" | **The four rules of record**, written in `docs/records/compat-rules-of-record-2026-11-19.md`: the database computes everything (folds for constant String and Boolean only), format and toString included; product presentation only serializes; compatibility presentation fixes upstream's output in Java from lite's typed values only; the host judge is the only judge in compatibility, both judges in the product. Every brief and review checks against it first |
| ~16:40 | "Adopt tenets yes / Folds okay / Error records yes / Let's show steps of A again now that we are ready to go" | The six tenets adopted (written into the rules of record). Folds: the compiler may fold constants and facts fixed by the types, never row data (so `substring`'s index arithmetic stays a compile-time fold). Error records: a raised error's record is lite's typed output, so compatibility words upstream's sentences in Java from it (B1 stands) |
| ~16:45 | "Okay let's go / I'm very interested in why PCT only passes 787 so when we get there would love to learn what the problems are there" (to this session's restated stage A: A1 to A4 with their builders, proofs and sizes, "Say go and I'll brief the builders now") | **STAGE A RATIFIED** as `docs/records/compat-final-plan-2026-11-19.md` §4 at `0b62425a`, governed by `compat-rules-of-record-2026-11-19.md`. Builders briefed: A1 B1, B1-R, A5, CI-health; A2 B2; A3 the translator; A4 the judges session. Every step a PR, CI on every OS, the independent reviewer's review, merged only on the user's word. The user wants the PCT-under-lite's-rules failures explained when A1's lane lands |
| ~18:05 | "Agree with all four table printing recommendations" (to this session's four, from H14's measurement: V1 363, V2 390, V2 + D1 390, V2 + D2 423 of 423 on DuckDB) | **The table-text rulings.** (1) Lite's own route prints a relation's `toString` in the database, like every other `toString` (rule 1; it replaces the refusal of the B17 ruling, 2026-11-17). (2) A whole Float prints `2.0`, not `2` (one more REPLACE; changes the 2026-11-10 ruling, S311; 5 core tests and some ledger rows move with this word). (3) A date with a time inside a table prints lite's standard text, `2000-10-25T06:30:00.000000Z` (the 2026-11-10 ruling, S312); the 33 PCT tests expecting legend-pure's `+0000` are recorded spelling differences under lite's rules. (4) It lands with stage B's B2, so the compatibility routes print tables in Java from typed rows and no two-form switch is needed. Product-track work, not stage A |
| ~18:12 | "Yes record it, and count the nested cases" (to this session's recommendation: take over a relation's `toString` at the program's last step, lite's text with the difference recorded where the text is used further) | **Compatibility's table text (B2).** Where a relation's `toString`/`toCSV` is the program's last step (an assert side's root; every PCT test), the legend-pure route takes it over: the database returns the typed rows and compatibility prints legend-pure's text from them in Java (rule 3; what legend-pure and legend-engine themselves do: the adapter sends only the query, the `toString` runs in legend-pure's Java). Where the text is used further inside the query (`->toString()->length()`), the database computes lite's own text and the difference is recorded; no SQL is kept for legend-pure's spelling (the 791-line `edge/RelationTextLowering.java` goes with B2). Six-decimal Floats in compatibility's SQL were weighed and not taken: legend-pure prints every digit, and the lost digits could not be fixed afterwards. The nested cases to be counted before B2 is built |
| ~18:30 | "Keep 1 thread for duckdb tests and multiple for product is good" (to A5's S1 measurement) | **DuckDB threads:** a test plan's session on a database the platform makes runs one thread (`SET threads TO 1`, G10: one error every run; the DuckDB lanes 12% faster); product plans keep DuckDB's threads; a declared file keeps its own. The product's remaining gap is recorded as a register row: a multi-threaded product plan with two failing expressions in different pipelines (joined sides, CTEs) may raise either error from run to run; a single projection raises the first (30 of 30) |
| ~18:36 | "Null is good" (to this session's question: a printed table's empty cell, legend-pure's `null` or nothing) | **A printed table's empty cell reads `null`** in lite's own route (a cell is a position, not a joined list's element; S316(a)'s rule that `makeString` skips empty elements is unchanged). It matches legend-pure and PCT, and tells an empty cell from an empty string |
| ~19:20 | "I think it has to be refuse then for now" (to this session's three options for the detector's load-time report: (a) as designed, (b) only when the work is already done, (c) no report, with refuse as the default) | **The detector refuses by default, for now** (replacing the ~14:55 "warn" default): a legend-engine request whose answer would differ is refused in legend-engine's error envelope, naming the call; `warn` stays a value of the setting for an operator who chooses it. **The load-time report is not built now** (it was "warn"'s condition), so no body is typed ahead of time. The engine-answer opt-in, if built, is a third value of the same setting |
| ~20:05 | "Okay now I understand the detector and I like it - for the rewites let's not take shortcuts on the rewrite part just bc I asked, let's do the right design for the rewrite part / Decision 1 engine opt in yes / raw substring only if it's the right thing to do / Explain switch rule - is it right thing to do / Build in this batch yes / Keep judge yes and rebase / toCSV yes toString or another easy database sql that makes sense / Println yes / Move which 4 or 5 tests?" | **(1) The `engine` opt-in: YES** (the amendment: where an operator opts in, the legend-engine door may rewrite a program to legend-engine's meaning; the core still computes it; the default stays refuse), **built with the right design, no shortcuts** (a design note reviewed before the build). **(2) The raw database SUBSTRING: only if it is the right design** (this session's reasoning given; the design note decides it). **(3) The switch rule: explained, not yet ruled.** **(4) Built in this batch: YES.** **(5) The judge-SQL ratchet: KEPT, re-based on the LITE-PCT lane.** **(6) `toCSV`: the database prints it in lite's route, like `toString` or another simple SQL that makes sense** (S316 amended; with B2). **(7) `println`: S128 amended as drafted (H15 §10).** **(8) The accepted divergences: the tests listed for the user; not yet ruled** |
| ~20:10 | "Yes" (to this session's question: (a), plus the other two moves; asked after "Can we delegate the dialect or very bad idea", answered: the fact is a measured comparison of legend-engine's meaning with Pure's, compatibility knowledge; a dialect method only the edge reads is the creep EdgeOnlyMembersTest stops, and unmeasured dialects would have to guess) | **The detector's three guard moves:** the literal-0 exemption table in `PureText` (H2 both arities, DuckDB the 2-argument form), `DialectBoundaryTest`'s pin for `PureText` 1 → 2; the parser-equivalence ratchet `own_corpus.matched` regenerated (once, on the merged tree, with A2's move); `native-claims.tsv` regenerated. Each with a dated note citing this ruling |
| ~20:35 | "Agree with the decision 2 and 3 / I'm rerunning the failed 124 jobs then we merge / What sessions need me / Let's do fast dev loops to get A done" | **(2) The switch rule, door-only** (written into the rules of record; B5's row). **(3) The accepted divergences:** restore the four batch-144 rows to the accepted rosters verbatim (DuckDB `testHashFunctions`, `testToSQLStringForTDSStringJoin`, `testToSqlGenerationFirstDayOfWeek`; H2 `testToSQLStringForTDSStringJoin`) and add two H2 rows (`testHashFunctions`, `testToSqlGenerationFirstDayOfWeek` on the merged batch), witnesses read from the lanes. **#124 merges** once the user's re-run of its two cancelled macOS jobs is green. **Fast dev loops** for the rest of stage A |
| ~20:45 | "Approved to move guards down" | **A standing approval for every guard move that only goes DOWN** (tightens): a pin, ceiling, size ledger or allowlist that shrinks, or an entry removed with the code it guarded (A2's and A4's shrinking moves, and any like them in stage A), each still with its dated note citing this ruling. **Not covered** (still each the user's word): a move UP (A3 piece 1's four counters, the `engine` native's +1), a new entry in a closed list, a re-recorded golden, a moved roster |
| ~20:50 | "Okay approved" (to this session's explanation of why A3 piece 1's guards go up) | **A3 piece 1's guard moves approved:** INTERNAL_DESUGAR 38 → 40, FUNCTION_FQN_LITERALS_MAX 346 → 347, the spec ratchet's Intrinsic 735 → 738, NOT_UPSTREAM_MAX → 76, and the 9 re-recorded translator goldens, each with a dated note citing this ruling |
| ~21:10 | "Yes to all three for substring" (to this session's three points on doing `substring` right) | **(1) Three functions, kept separate:** Pure's `substring` (A3's core meaning, database-computed, raising like Pure, S383), `sqlSubstring` (lite's SQL-family frontends), `substringAsGiven` (lite-internal, the database's raw call, reached only by the door's `engine` rewrite). **(2) `substringAsGiven` recorded as the reviewer laid it out** (D-S1 corrected, `ca8ea4a5`): `native-claims.tsv` regenerated, `INTERNAL_DESUGAR` +1 (40 → 41) with its comment amended, `SqlFnSortedTest` PRODUCT +1, a pin that no product claimer exists but Scalars; not `EdgeOnlyDataTest`; each with a dated note. **(3) Product track:** make lite's `sqlSubstring` follow the SQL standard (`'C'` for `('Credit', 0, 2)`, as DuckDB and Postgres, not H2's clamp), measured first |
| ~21:40 | "Q5 take it over / Q12 every PR has crazy time and machine cost if easy to bundle with stage A PR that would be great, if not we can do it after / Do it in order and parallelism that makes the most sense - try to batch to move fast in fast dev loop / For all the rest we need to go through each one so understand the full context and implications" (to `compat-stage-b-questions-2026-11-19.md`) | **Q5:** the compat program takes over the window-in-a-list fix now (fixed in `SelectMerge`; SQLite's regexp refusal kept), to ride stage A's batch if green in time, never holding it. **Q12:** the engine runner's storeless gap goes in stage A's batch if easy (a timeboxed job), else after. **Q1/Q2 left to this session, batching first:** B1 and B3 side by side, then B2, B4, B5, all on one stage-B batch branch with fast loops per piece and one CI run at the end (a midway run only if a risk shows). **Q3, Q4, Q6-Q11, Q13:** walked through one by one before any is ruled |

**Superseded ~13:25:** **Still open:** CP-12 (copy legend-engine's departures from Pure, or answer Pure's way) and CP-5 (no special functions),
each waiting on its measurement; then the final plan and the ratification of phases 1 to 3. PCT under LITE's judges is
being built by the product's judges track (C3).

**Held by the previous orchestrator:** R-a (a fractional cell under an Integer property, PR #121).

## 2. Findings, with their evidence

| # | When | Finding | Evidence | Where it went |
|---|---|---|---|---|
| F1 | 04:20 | **CP-4's cost:** 143 distinct relational-corpus tests (DuckDB 143, H2 139) pass today only through legend-engine's SQL text and become "not promised". By cause: a check on a string with nothing to re-run (22 compared strings, 53 plan printouts lite cannot produce); legend-engine's expected SQL not runnable on the referee (23 written for DB2 or Composite, 22 missing setup, syntax errors or now-relative dates); lite's rows underivable on the referee (19); SQL read out of legend-engine's plan (4) | B2: `docs/records/census-b2-sqltext-probe-2026-11-19.md` on `claude/census-b2` at `32ef19118` | plan CP-4 |
| F2 | 04:20 | **6 tests assert legend-engine's plan structure** (`testSupportStreamFlag*`, a `FunctionParametersValidationNode`'s `supportsStream`), not SQL text | as F1 | plan CP-4; for the user at the checkpoint |
| F3 | 04:20 | **legend-engine's `joinStrings` SQL appends the separator** (`concat(a, ' ', b, '\|')`), against Pure's `joinStrings`; lite answers Pure (`testToSQLStringForTDSStringJoin`, `testHashFunctions`) | as F1 | an upstream-defect register row (0.5) |
| F4 | 04:20 | **A lite parity bug:** `firstDayOfWeek` answers Monday on DuckDB (Pure's rule) and Sunday on H2 (`testToSqlGenerationFirstDayOfWeek`; lite's `date_trunc('week')` lowering) | as F1 | handed to the product owner (`session_0195rQEoRb1AGCHxtZChjWsg`), 04:2x |
| F5 | 04:08 | **B1's review of r1:** the EngineShape family (`relations`, `keyedCounts`, `computedPruning`, `view`, `wireCast`, `unionFrame`) differs only in legend-engine's TEXT; `traceCell` and the graph keys are ANSWERS; the compatibility database judge writes through the text facets 1.3 deletes, so 2.2 must come first; executed generated DDL is not text; `RelationalRootForm.primaryKeyColumns` is the product's key rule | B1's message, against `00cd4295d` | plan r3 |
| F6 | 04:13 | **What "feeds" means:** the text's content is read (compare, concatenate, sort, group, join keys, filter, length), not merely carried (select, rename, limit, slice, a `let` to the root). Prints nested inside a returned structure are presentable by position | B1's review of r2 | plan r4, 1.3 |
| F7 | 04:13 | **"No special functions" has a measurable cost:** pure/v1's `raise.sqrtOfNegative` matches legend-engine today only through `LoweringForms.raise`'s plain set, and ENGINE answers on H2 and DuckDB rely on S20's bare-DECIMAL literal | B1's review of r2 | plan 0.4 (j) |
| F8 | 04:35 | **The appeal can move into the edge judge as one slice** (3 to 4 days), if the judge is one object with two narrow interfaces (planning, run) and the product keeps two call sites (`TestDoor`, `TestRunner`): the appeal needs planning-time artifacts the plan does not carry (the referee's seed copies from the CSV load's MIR) and the live session after the body runs | B1's message | plan 2.2, done-when 1 |
| F9 | 04:30 | **The goals of record already rule the appeal and the referee in, at the edge** (GOALS_OF_RECORD G6 and §2 E3, 2026-11-07; `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`, 2026-09-01, and its 2026-09-21 ruling); r3 deleting them was this session's error | the documents | plan r5 |
| F10 | 04:30 | DataCube's calculated-column help writes `substring(0, 2)` in Pure's meaning (`datacube/src/calc.ts:88`), so R4 makes the product's own example right; saved queries and cubes using `substring`/`indexOf` will change answers | `grep` at `main` | plan §12 I1 |

## 3. Sessions and branches

| Session | Role | Branch | State |
|---|---|---|---|
| this session | owner, orchestrator | `claude/compat-program` | |
| B1 `session_0195rQEnAa5CYXHygAxEFh3Y` | presentation lead, phase 1 | `claude/presentation-b1`; 0.2 on a new `claude/compat-*` branch | |
| B1-R `session_0195rQEkcevkNQ66fixkk4FF` | legend-engine oracle, phase 5 | `claude/presentation-r` | |
| B2 `session_0195rQEjuF2imym274M9Qvva` | census, guards, measurements | `claude/census-b2` | |
| previous orchestrator `session_0195rQEoRb1AGCHxtZChjWsg` | product correctness, landings | PR #121, others | coordinates by message |
| translator `session_0195rQEn5jsUCYS6QSWLrxgF` (new, xhigh) | the translator: H5 spike, 3.1 sizing, the 0.6 ledger draft | `claude/compat-translator-spike` | started 10:48 |
| judges `session_0195rQEm3vCjTiBifDsQTVB3` (new, xhigh) | the judges split's homework; PCT under LITE's judges (§12 I5) | `claude/compat-judges-homework` | started 10:49 |
| independent reviewer `session_0195rQEnCA5MRQEk6iGfgDnb` (new, xhigh) | an adversarial review of the design and the plan | `claude/compat-review` | started 10:49 |

## 4. Work in flight (started 2026-11-19 ~10:50 UTC)

| Workstream | Session | What | Record | Report |
|---|---|---|---|---|
| W1 | B1 | 0.2: the net lands as a PR (`claude/compat-s0`: S0 and the requests cherry-picked, the net per push); then phase-1 homework: 0.4 (f) prints by position and use, (g) G7 over the net, (i) Float round-trip through each dialect's JSON, the Operand shape's coverage | `docs/records/compat-homework/H9-phase1-measures.md` on `claude/compat-b1-homework` | the PR number; counts |
| W2 | B1-R | 0.3: the ENGINE PCT lane against the manifests (a classifying table under E-R2, sampled against the oracle); H3's batteries made permanent (pinned per push, re-asked in heavy); the per-push cost; the `jars_table` entry (CP-9, ruled) | `docs/records/compat-homework/H7-engine-lane.md` on `claude/compat-engine-lane` | counts per class and database |
| W3 | B2 | 0.4 (a) the strand map confirmed and the ratchet's start; (c) legend-engine's unchecked operations; (d) the strands' doors; (e) executed generated DDL and the lanes' time under the appeal; (h) the corpus's H2 lane without LEGACY, and the referee's settings; (j) CP-5's cost by case | `docs/records/compat-homework/H8-phase0-measures.md` | counts after each item |
| W4 | translator | H5: a model-level, target-aware translator spike through the corpus's 15 tests; 3.1's split sized; the 0.6 ledger's first draft | `docs/records/compat-homework/H5-translator-spike.md` | counts first |
| W5 | judges | the 36 combinations; the exact map of compatibility judging in the core; the two narrow interfaces sketched; PCT under LITE's judges with BOTH (§12 I5) | `docs/records/compat-homework/H6-judges.md` | counts first |
| W6 | reviewer | the independent review: its own view first, then the plan attacked; 12+ claims checked | `docs/records/compat-independent-review-2026-11-19.md` | verdict and top five |

**The phase-0 checkpoint** comes when W1-W6 have reported: the exact lists, the counts and the review go to the user,
who confirms or changes phases 1-3.

## 5. Learnings (as they arrive)

- **~10:55 UTC, the user's question:** "It would also be good to know what would happen/what would be lost if we don't
  do the model translation at all .. do we just lose substring/indexof or lots of other things break too? Maybe we have
  a flag/option that can do index/substr as 0 or 1 based instead of trying to build a whole translator for example. I
  wouldn't build a whole translation machine for substr/index but if turns out that we lose a lot more then might be
  worth it".
  - **Done:** the translator session's first task is now TASK 0. It lists every behaviour where today's edge makes lite
    compute legend-engine's MEANING (about 9 strands, first cut), runs the corpus, pure/v1 and PCT ENGINE with all of
    them switched off, and attributes each lost test to a strand. It then compares four ways forward: a full translator
    (A), a rename pass over a fixed short list (B), a flag in the core (C), or nothing (D).
  - **The orchestrator's first reading, before the number:** without a translator, only what today's edge does to
    imitate legend-engine's meaning is at stake, against today. Most of H1's other rewrite rows are differences lite
    already has. The full model-level translator is worth it only if task 0 shows much more than `substring`/`indexOf`
    and a few H2/DuckDB quirks.
- **~10:59 UTC, B2's 0.4 (a), the strand map confirmed** (`H8-phase0-measures.md` at `ece476172`):
  - every facet, two-form and `LoweringForms` member compared body to body;
  - the EngineShape family is S as written;
  - the ratchet's start: `PlanStyle` 10 methods, 49 parameters in 19 classes, 50 calls, 112 calls carrying a style
    in 22 classes, 36 classes naming it; `LoweringForms` 15 members, 25 calls;
  - corrections, applied to the plan: three PureText line numbers, `Appeals` is 401 lines (the appeal stack 3,411),
    `LoweringForms` 15 not 17, and a missing strand, `PlanStyle.siteWords` (P, 1.2).
- **~11:02 UTC, the translator session had not yet acted on task 0** (it was still building the H5 harness from its
  first brief); the message was re-sent at priority "next" (~11:05).
- **~11:02 UTC, the independent review's headline** (`session_0195rQEnCA5MRQEk6iGfgDnb`; plan r5 at `933ee1f1`; the
  full record follows on `claude/compat-review`). Verdict: **SOUND WITH CHANGES.** The architecture is the right shape.
  Five findings, most severe first, each folded into plan r6 (`af6d5a9a`):
  1. **The translator is unproven at scale and was tested late.** A parsed model has no types (overloads resolve at G),
     so a rewrite must work on resolved calls; target-aware means one compile per (audience, target), unmeasured.
     Checkpoint 2 moves to the end of the phase-0 spike.
  2. **2.2 would have undone the 2026-11-08 plan-as-data ruling.** The edge's effect and seed records ARE planned into
     the test plan's typed extension point (`TestDoor.java:141-149`, verified here). F8's "the plan does not carry
     them" was wrong. The planning interface stays data; only the run seams are replaced.
  3. **G2's guard would leave PCT.** A LITE-PCT lane (both judges, its own ledger) becomes a precondition of 2.2a.
  4. **The critical path and calendar were wrong:** the path is 2.1 -> 2.2 -> 1.4 -> 2.3 -> 4.1, about day 30 with one
     session per track; the realistic calendar is 7 to 9 weeks. 2.2 is split into 2.2a and 2.2b, and 1.3 and 1.4 go to
     two sessions.
  5. **CP-3 contradicts 3.3 and 3.4:** "answer correctly where legend-engine is wrong" against rewriting legend-engine's
     departures from Pure's definition. Now CP-12, for the user, decided with task 0's numbers. The strict reading
     shrinks the translator to almost nothing.
  - Also folded in: upstream's `Feature` flags in the core (a missed strand); R4 reaches the `#SQL` and `#IQ` islands;
    0.4 (j) covers the corpus; a new 0.4 (k), E-R2 against the corpus's expected refusals; 1.4's plan format moves to
    5.1's export; CP-8's differences stay attributable; the referee's settings are separated from the edge's dialects;
    `DatabaseJudge`'s pin is 852.
  - **Relayed:** to the translator (findings 1 and 5, resolved calls, compile cost, the islands); to the judges (2, 3,
    the split, the referee's settings); to B2 ((j) with the corpus, (k), the Feature flags); to B1 (2, 1.4 to export,
    the critical path).
- **~11:05 UTC, the independent review's full record** (`838e280c`; its "own view" section, written before it read the
  plan, at `511ef632`). Findings F1-F13; 35 claims checked against `main`, 24 holding and 11 not or only in part (each
  corrected in the plan: line numbers, `Appeals`' size, `DatabaseJudge` 852, `primaryKeyColumns`' two more users).
  - **Folded into plan r7:** 2.3 split (2.3a, the engine-text printers, early; 2.3b, the executed shapes, after 2.2);
    `toSQLString` for a database without a lite dialect refused by name; the appeal's "our rows" through the
    translator (2.2b with 3.2); guards on the two judge interfaces and on 1.3's print analysis; saved artifacts carry
    their meaning version (R4).
  - **CP-13 for the checkpoint:** a smaller plan, deferring phase 5 and the LITE-host drift fixes (about 20 to 25
    session-days).
  - **The review's order of work:** the translator spike, PCT under LITE, the rulings CP-12 and plan structure, 2.3a,
    2.2a, then the presenters. That is the order phase 0 and the checkpoint now follow.
- **~11:05 UTC, B2's 0.4 (c), legend-engine's unchecked operations** (`claude/census-b2` `9cd75578c`, H8 §(c)), read
  from legend-engine 4.145.0's 77 relational PCT manifests (11 databases) against the 26 PCT tests that assert an error:
  - **class A** (lite raises Pure's error; legend-engine answers the database's value): **16 test cells on 8
    databases**. DuckDB 4 (rem by zero, range step 0, slice inverted, bitShiftRight > 62); H2 3 (sqrt of a negative,
    both shifts); MemSQL 2; Oracle 2; Snowflake 2; ClickHouse, Databricks and Trino 1 each. Postgres, Spanner and
    SQL Server have none (their databases raise);
  - **the plain set reaches 7 raise kinds, not `ENGINE_UNCHECKED`'s 10:** `TEXT_NOT_INTEGER`, `TEXT_NOT_FLOAT` and
    `TEXT_NOT_DATE` are raised in the dialects, never through `LoweringForms.raise`, so they are dead entries;
  - **lite plants 27 raise kinds at 72 sites; PCT exercises 9.** The other 18 are unmeasured per database and need
    the oracle in (j);
  - `acos` and `asin` of 2.0: lite has no raise kind, so it answers a value, as legend-engine does on 6 databases.
    legend-pure raises (S138), so that is a gap in lite's PURE answer.
- **~11:05 UTC, the judges session's I5: PCT as a LITE suite under BOTH judges** (channel B, DuckDB, 1,249 tests, main
  plus a measurement-only switch on `claude/compat-judges-homework`):
  - today's COMPAT BOTH: 1,240 pass, 9 fail, 0 disagree;
  - **LITE BOTH: 786 pass, 463 fail, 5 judges disagree;** with R4's substring on, 790. HOST-only 787,
    DATABASE-only 786;
  - **the failures:** 423 relation tests assert legend-pure's print of a relation (refused by name under LITE: a
    presentation test by construction); 26 assert legend-pure's words or print forms; 7 are R4 (4 substring, 3
    indexOf); 2 are pre-existing (acos and asin);
  - **0 failures come from compatibility-only judging rules:** no 2-ULP, multiset-order or json-simple case occurs at
    all;
  - **5 REAL judge-parity bugs that COMPAT's verdicts-only comparison hides:** `testDateFromHour` (the database judge
    casts "2015-04-16T14" AS TIMESTAMP, and DuckDB refuses it); `testAdjustBy{Days,Hours,Months,Weeks}BigNumber`
    (the two judges fail differently);
  - **cost:** 33 to 77 s per run for the five suites, the same as today's channel-B lane; no extra build;
  - the parity bugs were sent to the previous orchestrator (A5's area) by the judges session.
  - **What it means:** the LITE-PCT lane is cheap and finds real bugs at once. Its known-failure ledger is mostly one
    class (PCT tests that assert legend-pure's print forms or words: 449), which the PURE presenter's lane covers.
- **~11:07 UTC, the judges session's H6 record** (`b39b3799` on `claude/compat-judges-homework`, §3: all 463 LITE
  failures classed).
  - **Its recommendation:** a LITE-PCT lane, about 75 s (`pct_channel_b_lite_<suite>`), with its own exact ledger, as
    an addition to 2.1.
  - **A correction to the plan's framing:** channel A has no lite judge. legend-pure's interpreter judges, and lite
    only answers each execute. So BOTH applies to channel B alone. The judges session is also running channel A under
    LITE's planning style, which judges lite's answers by value against legend-pure itself (the 423 relation tests
    included).
  - **The 36 combinations are defined** in `docs/records/e-java-shape-port-2026-11-17.md` on
    `claude/total-wall-e-shape-port` (`1bb0f3151`); their core coverage at `main` is being re-measured.
- **~11:10 UTC, B1:**
  - **F2 confirmed:** B1's F8 note was wrong. `TestDoor.java:141-155` plans the edge's records into
    `TestPlan.extensionsByStep`, `TestPlanJson.java:121-123` writes them, and the run reads them back
    (`TestRunner.java:431`, `:461`; `Appeals.java:112`).
  - **0.2:** S0 (`14f6d1c33`) and the requests (`68ab589f2`) are cherry-picked onto `main`, and the CP-9 lane is
    `e3bae6f3c`: `//gates:edge_net`, a lane in `//gates:lanes` and `gates-run.yml`. 41/41 green; no answer moved
    (11 PCT goldens 0 moves; the corpus ledgers' columns 1-4 identical; pure/v1 0 moves, 2 escape-only lines and the
    added `raise.toOneOfEmpty`). `//gates:local` was running.
    - **The per-push lane is heavier than CP-9's estimate:** 568 s wall locally (4 cores), with a 503 s critical path
      through the corpus's DuckDB host judge. It runs as its own parallel CI lane, so CI's wall time grows only if it
      is the longest lane. Asked of B1: its CI time, and its parts' times.
  - **0.4 (f):** of H2's 98 BARE print sites, **95 sit at the literal root**. The other 3 are in a relation column (an
    extend over an enum; a projected StrictDate; one inside `toSQLString`, never executed), and none is in a map, a
    Pair or `^C`, so 1.3 is the 4-day case. **0 of the 22 COMPOSED and FEEDS sites are over a Float, Decimal or
    DateTime.** One finding: PCT's `testPairCollectionToString` composes a Pair's print (R5: a Pair's text differs by
    audience), so 1.3 refuses it. It is the one known refusal.
- **~11:12 UTC, B1-R's H7 early counts, slice 0.3** (the ENGINE PCT lane, report-only). PCT channel A ran as
  legend-engine's audience (the pure/v1 route's style, no feature flag, no expected failures). Each outcome is keyed by
  the test's full name; the edge net's short names collide, 345 tests on 341 lines. Each was joined with legend-engine
  4.145.0's manifests and classified by status and class, never by words; every manifest row joined a test.
  - **DuckDB, 5 suites, 1,249 tests:**
    - legend-engine passes 1,005, REFUSES 59 ("No SQL translation", 23 signatures), and fails by its own defect 185
      (51 unsupported, 47 wrong answers, 44 planner faults, 21 database errors, 17 runtime faults, 5 error-text
      mismatches);
    - **lite as legend-engine's audience passes 1,212;**
    - the verdicts: SAME 1,004; LITE_ANSWERS 55 of the 59 refusals; LITE_FIXES 153; BOTH_FAIL 36; LITE_BEHIND 1
      (`testBigFloatAbs`, a 20-digit Float literal carried as a double, S120);
    - where legend-engine refuses, lite answers PCT's expected value 55 of 59. The 4 misses: sort-with-key twice
      (substring keys), `date(y, m)` once, 3-argument `indexOf` once.
  - **H2, relation, 469 tests:** legend-engine passes 411, refuses 0, fails by defect 58; **lite passes 448** (SAME
    411, LITE_FIXES 37, BOTH_FAIL 21, LITE_BEHIND 0). lite's 21 failures are 17 of its own refusals ("reached a
    dialect without a list encoding") and 4 database errors (H2 has no LATERAL).
  - **What it means:** on legend-engine's own PCT, lite's legend-engine route already answers more than legend-engine
    does (1,212 against 1,005 on DuckDB; 448 against 411 on H2), and is behind it once. That is strong evidence for
    E-R2.
  - Built: `//pct:engine_lane_<db>_<suite>`, the pinned tables in `pct/engine_lane/`, `//pct:update_engine_lane`,
    `//pct:engine_lane_test`. Commits on `claude/compat-engine-lane`, NOT yet pushed: `28f6d56c3` (the `jars_table`
    entry, dated), `4381eb8f6` (a merge of `claude/presentation-r`), `f5da0b965` (the lane).
- **~11:14 UTC, the judges session's tasks 1 and 2** (`69263372`, `452ebe09`):
  - **the 36 combinations:** none is covered by core at `main` (core has 13 BOTH targets, 922 planned asserts and 46
    combos, all green with the shape census on); 6 are judged by the LITE-PCT lane in passing tests, so 30 remain to
    port. All are small test bodies needing no new mechanism. 2.1 is 2.5 days;
  - **the map, three findings that change 2.2:**
    - (A) the appeal's planning half also lives at the test planner's per-assert entry (`AssertVerdicts`, 15 gate
      calls; `EngineTextPlanning`, 58 calls from `SqlTextVerdicts`), so the judge interfaces have three call sites,
      not two. The `EngineTestText` facet's 13 methods reshape into one claim; no new hook;
    - (B) more is deleted than moved: the database judge's appeal paths go with the compatibility database judge;
    - (C) a decision: is the compatibility host judge the edge's own copy (+3 days), or the product's judge with its
      rules as a typed parameter? Now CP-14, with a third option the architecture prefers: verdict records worded
      by a presenter, valid if the leniencies change no verdict (being measured);
  - **2.2:** 8.5 days under (ii), 11.5 under (i). Plan updated: done-when 1 says three call sites.
- **~11:19 UTC, the translator session's H5 first counts** (`claude/compat-translator-spike` at `4da71056`).
  Variant V3: Pure's `substring`/`indexOf` in the core, plus 3.1's dynaFunction split, plus a use-site translator.
  - **The corpus against `main`:**
    - **H2:** 2,285 pass / 280 fail (main 2,289/276). The +4 fails are all in B2's 143 "not promised" (plan or SQL text
      only);
    - **DuckDB:** 2,447 / 113 (main 2,452/108). The same 4, plus `testPlanForDateTimeVariableESTTimeZone`, a VACUOUS
      pass at `main` (its `indexOf(...) != -1` was always true under the old meaning). Under Pure's meaning it reaches
      legend-engine's own typo, `'...' != -1` (String against Integer), which DuckDB raises on: **a product defect**
      (a cross-kind `!=` raises instead of answering true), handed to the product owner;
    - **11 of H3's 15 pass on both lanes:** 3 query-lambda tests by the translator, 7 dynaFunction tests by the core's
      3.1 split, 1 by the appeal. 8 tests now pass BY ANSWER through the appeal.
  - **No new core hook was needed.** The translator is 494 lines, using the public API only (javac-checked); 26 s per
    corpus world uncached, of which `buildModule` is 3.5 s.
  - **The findings that change phase 3:**
    - **F1, two meanings by where code runs:** legend-engine's programs use `substring`/`indexOf` in the DATABASE's
      meaning only for code sent to the store (a routed query lambda, helpers forwarding one, an M2M mapping chained
      over relational), and in PURE's everywhere else (test bodies after the run, host-side helpers like `sortCsv`, M2M
      in memory). So the rewrite is per USE, not per definition, and **a whole-model rename (option B) would be
      wrong.** The corpus has **5 store-bound `substring` calls and 0 string `indexOf`**; 40 routed calls need data
      flow to follow;
    - **F2:** the `###Relational` dynaFunctions (7 of the 15) belong to the core's 3.1 split, not the translator. The
      spike did it in the normalizer, with no new native;
    - **F3:** 3.1 is bigger than planned: about 49 sites in 5 files (Cypher 20, `#SQL`/Postgres 12, `#IQ` 17);
    - errors keep their positions, but no lite error on these doors prints one, and the words leak the rewrite's name
      ("in call to 'meta::legend::compat::engine::substring'"), a presenter's job.
  - **Next:** V2 (the core only, no translator: the "nothing" option for substring/indexOf), V4 (a definition-site
    rename), V5 (an identity pass-through), then 3.1's sizing and the 0.6 ledger (127 rows drafted, 51 flagged
    unclear).
- **~11:19 UTC, the judges session's H6 complete** (`df5a1a37`, §0 the one-page answer):
  - channel A under LITE's planning style (legend-pure's interpreter judging lite's answers): COMPAT 1,222/27 (the
    switch inert on every lane), LITE 1,203/46. The 19 new failures are classes already seen in channel B (4 R4
    substring, 6 error words, 7 value prints, 2 format refusals). The relation and standard suites do not move, so
    the 423 channel-B relation failures concern legend-pure's asserted TEXT only: **lite's relation answers are right
    by value;**
  - **its recommendation:** yes to a per-push LITE channel-B lane (about 75 s, an exact ledger) in 2.1, plus channel A
    under LITE (129 s) as the value check for the relation tests.
- **~11:20 UTC, the product orchestrator:** the user asked that the whole product scope be fixed now, so five product
  tracks run:
  - judges `session_0195rQEn49s2BEcy7BVaoAL4`, `claude/parity-a5`: J3, Q3 A', H6's two parity findings,
    `firstDayOfWeek`, J7, JUDGES-8, and C3 (a real guard for "BOTH on every lane"). It lands before 2.1 and 2.2, and
    messages us before touching the PURE/ENGINE judging paths;
  - numbers `session_0195rQEjTP2YHCipAiqZmEma`, `claude/numbers-q1`: Q1, Q4, the Float print outside its windows,
    Decimal NaN;
  - hosts and planner `session_0195rQEjb2QyV1G7qABK31Wj`, `claude/hosts-followups`;
  - warehouse `session_0195rQEjKJc9QU59cK1esBJZ`;
  - CI health `session_0195rQEmJ9D6cn7yjYczb2bi`.
  - **Confirmed as ours:** C1 (0.2), substring's negative start (3.3), S138 and S63 (0.5), the census's REGEXP_REPLACE
    blind spot (B2), and F4 (Python's copy of Java's classifier, with the presenters).
  - **Proposed:** their C3 builds the LITE-PCT lane from H6's prototype, and our 2.1 then ports only the 30
    combinations.
- **~11:21 UTC, the product orchestrator agreed:** C3 is our 2.1's LITE-PCT lane, built by its judges track from H6's
  prototype, and its judges session will message ours before starting. The cross-kind `!=` defect is first in the
  hosts-and-planner track (`session_0195rQEjb2QyV1G7qABK31Wj`), to land before 3.3. The five parity bugs are in its
  judges track. Ownership as confirmed above.
- **~11:40 UTC, slice 0.5 done by this session: PR #122** (`claude/compat-register`, `a56fd0ca`, documentation only).
  - Corrected: S63 (the live refusal is `pureToSQLQuery.pure:10056`) and S138 (compiled legend-pure raises,
    `CoreHelper.java:141-148`).
  - Added: S371 (legend-engine's H2 `parseJson` UDF, a JSON object only, re-serialised; `LegendH2Extensions.java:116-135`),
    S372 (`joinStrings`' separator, upstream's defect), S373 (pending: a list aggregate's empty pivot cell on DuckDB,
    a suspected lite defect, handed to the product orchestrator), S374 (CP-8's blanket row), S375 (E-R2's blanket
    row), S376 (the prelude and the parser stay).
  - Each upstream citation was re-read at the pins, from blob-less clones of the release tags.
  - The independent reviewer checks the rows. The PR waits for the user's merge word.
- **~11:33 UTC:** S373 (a list aggregate's empty pivot cell on DuckDB) is scheduled in the product's hosts-and-planner
  track, after the cross-kind `!=` fix: confirm it by test, read Pure's answer, fix it to lite's meaning, and bring any
  difference from legend-engine back to the user.
- **~11:33 UTC, B2's H8 (a.3), (d), (k)** (`claude/census-b2` at `536e2e030`):
  - **(a.3) the Feature-flag strand:**
    - `LEGACY_SQL_NULL_UNSAFE_EQUALS` switches equality at 6 lowering sites;
    - `CORRECT_SQL_SUBSTRING_INDEXING` swaps in `CORRECTED_SUBSTRING`;
    - `PUSH_DOWN_ENUM_TRANSFORM` is a no-op in the product;
    - the flags reach the product with no style at all (the query's options, the body, the runner's defaults, the
      product's doors). Ratchet start: 20 classes name `Feature`;
    - **PCT A and PCT B set `CORRECT_SQL_SUBSTRING_INDEXING` on every query**, so PCT already runs Pure's `substring`.
      7 corpus tests set a flag;
  - **(d) the doors per strand** (a hook recorder, 21 JVM runs):
    - **pure/v1 is the only door that reaches the plain set and `siteWords`.** The plain set gave 6 answers there
      (DIVIDE_BY_ZERO 3, SQRT_OF_NEGATIVE 3);
    - only PCT B reaches `errorWords`, `VerdictNumbers`, `DateLiteralTexts`, `CrossKindNumbers` and most of
      `PureOnlyTexts`;
    - only the corpus reaches the EngineShape text forms, the engine-text printers, `UpstreamPlans`,
      `ResultEnvelopes`, `ActivityTexts`, `ExecutedValues`, `RoutedQueries`, `traceCell`, the graph keys and `raw`;
    - **reached by no door run:** two-forms `JsonGrammar`, `RenderedTextRule`, `GoldenNullWord`;
      `LoweringForms.assertFailed`; the Postgres, Databricks, Trino, SQLite and MongoDB dialect passes. These are
      deletion candidates, unless a lane not run here reaches them;
  - **(k) E-R2 against the corpus:** no runnable corpus test asserts an error (`assertError` = 0 over 2,613), so E-R2
    changes no runnable corpus verdict. Of the 148 tests legend-engine excludes (ExcludeAlloy 96, ToFix 52), lite on H2
    agrees with Pure's expected value in 44 and with the authors' value in 13, contradicts the test in 25, and refuses
    56. The 4 H2-only contradictions are a Float printed `100000` against `100000.0` (S20's class).
- **~11:37 UTC, B1-R's H7 part 2** (record pushed on `claude/compat-engine-lane`):
  - **the oracle sample: 21 of 21 rows agree with legend-engine's manifests**, asked of legend-engine 4.145.0 itself
    (DuckDB 11, including its own wrong answers reproduced: `indexOf 'quick'` = 5, `pow(3.0, 2)` = 27.0, `round(-16.5)`
    = -17; H2 relation 10, `asOfJoin` and `pivot` refused in the manifest's words). So the manifests are a trustworthy
    oracle. Method limit: each sampled operation was sent as a pure/v1 request over the oracle's seed;
  - **the batteries, permanent:** 49 requests (H3's 25 over the desk column, plus the same 24 over a literal).
    - heavy half: `//tools/engine-runner:batteries_oracle_test`, which re-asks legend-engine and pins its answers;
    - per-push half: `//pct:batteries_test` in `//gates:pct_duckdb`, with no legend-engine JVM; lite answers through
      pure/v1's shipped route;
    - today: H2 AGREE 46, BOTH_FAIL 1, DIFFER 2 (3-argument `indexOf`); DuckDB AGREE 31, DIFFER 18 (16 starts at or
      below 0, plus 2 3-argument `indexOf`): H3's 16 of 25, exactly;
  - **the per-push cost, measured:** the ENGINE lane is 6 actions, 272.7 s serial, about 1.5 to 2 minutes of wall time
    on a 16 GB Linux runner (3 at once) and 4.5 minutes on the 7 GB macOS runner; each action asks 4 GB but peaks at
    1.6-1.8 GB. The batteries' per-push half is 7.7 s; the heavy half 34 s;
  - a visibility change only: core's trades model export gains `//pct`. `EDGE_TEST_SRCS` is untouched.
- **~11:40 UTC, the independent review of PR #122: HOLD FOR CORRECTIONS.** Every cited upstream file and line holds at
  the pins, but:
  - S372 misread legend-engine's `joinStrings`: the separator is appended ONCE at the end, with the elements
    concatenated with nothing between them;
  - S373 is S110's residual, already recorded;
  - two quotations were not verbatim in this log (S371, S376);
  - S374 and S375 read plan text and CP-12 into their rulings;
  - S138 was inconsistent with H8 §(c).
  All corrected in `1faa54da`. S376's ruling is now in §1, citing the census record as the source of its words.
- **~11:42 UTC, the re-check of PR #122: GOOD TO GO.** Its two nits (S63's stray bracket; S138's wording elsewhere,
  the database's value or its own error) were fixed in `f2025fae`, and the PR body was updated with the review. PR #122
  waits for the user's merge word.
- **~11:48 UTC, the translator's H5 update** (`6fef76e0`; evidence `h5/corpus/`, `h5/parity/`; ledger draft
  `H5-translator-ledger-draft.tsv`):
  - **R4 cannot land before 3.1's frontend split.** Pure's meaning in the core alone breaks about 1,240 parity cases in
    lite's OWN frontends: `//core:cypher_parity_test` 808, `//core:iq_parity_test` 402, `//core:sql_parity_test` 27,
    and 2 in core's integration tests. The cause is 47 call sites in 4 files that write SQL-style
    `substring`/`indexOf`: `CypherToPure` 21, `PostgresFunctions` 10, `IqFunctions` 14, `IqAggregates` 2. A mechanical
    split (two shared helpers) is in the spike, its parity run going. 3.1 is 2 to 2.5 days, plus 0.5 day and one
    `NativeCatalogGovernanceTest` ceiling move when 3.3's "a negative start raises" lands (the dynaFunction
    `substring(col, 0, n)` then needs its own verbatim rule);
  - **V2, no translator (the core change and 3.1's split only): H2 2,281/284, DuckDB 2,443/117. So the translator
    wins exactly +4 tests per lane on the corpus, and nothing else moves** (for `substring`/`indexOf`);
  - **V4, H3's rename over every definition: worse** by 4 (H2) and 6 (DuckDB). 6 failures are a TYPING break:
    renaming legend-engine's LIST `indexOf` (`tdsExtension.pure:39`) to the compatibility overloads makes lite infer
    the wrong type. So a translator would need TYPED retargeting, which raises a first hook question for 3.2;
  - **V5, an identity pass-through** (all 1,029,638 records of the corpus world rebuilt, nothing renamed): rosters
    identical to V2, in 1.1 to 1.3 s.
- **~11:49 UTC, the product's numbers worker** (`session_0195rQEjTP2YHCipAiqZmEma`, `claude/numbers-q1`) gave notice
  before touching `FloatShortestText`'s whole-value arm (Q4 and "2^60"). Today it casts through an integer:
  - SQLite prints `9223372036854775807.0` for 1e19 and beyond;
  - ClickHouse is lossy past about 1e21;
  - DuckDB writes the exact binary integer, where the host writes the shortest digits, so the two judges' texts differ
    (G2);
  - H2 and Trino already match.
  The fix lays out the database's exponent text positionally (rearranging spelled tokens, nothing computed), giving
  the host's shortest digits everywhere. The edge's `RelationTextLowering` reads the same node, so its text moves too.
  - **This session's answer:** proceed, with no separate edge spelling. The old arm is wrong anyway, neither text
    equals legend-pure past 2^53 (legend-pure prints 2^60 as `1152921504606846980.0`, DecimalFormat over FloatingDecimal),
    and `RelationTextLowering` is deleted in 1.3, where the PURE presenter can write legend-pure's exact digits in
    Java. The worker lists the edge golden moves and orders its landing with B1's 0.2 PR.
- **~11:54 UTC, an edge fidelity question from the product orchestrator** (the numbers worker's S20 work): the edge
  keeps `floatLiteralKind` → DECIMAL on BigQuery, ClickHouse and Snowflake while the product moves to the double.
  - **This session's answer:** drop the BigQuery and ClickHouse overrides and keep Snowflake's. legend-engine writes
    the literal bare, and the database types it. BigQuery's bare `0.1` is FLOAT64 and ClickHouse's Float64, so
    legend-engine's literal there IS the double; only Snowflake's bare `0.1` is a NUMBER.
  - The pin (`EdgeSpellingHookTest.OVERRIDES`) shrinks by 2. The BigQuery and ClickHouse PCT ledgers are re-measured
    in their next lane round, and the dated note cites each database's own literal-typing rule.
  - This matches CP-5's direction: S20's edge literal goes entirely if the user rules "none".
- **~11:58 UTC, the judges session's H6 §6** (`9a8c4f6f`): the leniencies measured (logged, then switched off), every
  lane run twice.
  - **PCT:** 0 decisions and 0 verdicts change on channel B's DuckDB, H2 and SQLite lanes, or with S131 off under LITE.
    Channel A has no lite judge.
  - **The corpus** (the positive control: probe-on rosters byte-identical to the committed ones):
    - DuckDB: 77 decisions; the fail roster goes 108 → 177, i.e. 69 tests (S131 33, S137 21, S132 14, S15 2, S144 2);
    - H2: 30 decisions, 276 → 305, i.e. 29 tests;
    - S127 and S139: 0 everywhere.
  - **CP-14 (iii) as conditioned fails on the corpus.** It holds if four become door rewrites of test bodies (S137,
    S144, S131's typed half, S15), leaving only the rendered-text comparison. Recorded in the plan with sizes:
    2.2a 3.5 days; 2.2b 5.5 under (ii), 6 to 7 under (iii); plus about 1.5 days of rewrites.
  - **S131's multiset is LIVE in LITE today in both judges** (`HostJudgments.java:128-135`, `:819-821`, `:868-871`;
    `TdsCompare.java:76`; VerdictSql's multiset arguments). Its narrowing, ruled 2026-11-05, is "build pending": a
    product item, sent to the product orchestrator. With S131 off, 0 PCT verdicts move, so the LITE-PCT lane needs no
    leniency row.
  - **The referee has no session of its own:** it replays on the body's own target's session, H2 goldens only. On H2
    lanes that session carries the edge's `MODE=LEGACY`, so CP-8 would change the referee's replay. It needs its own
    declared LEGACY target first (plan 3.2, now a named step).
  - **Words are already separable** (verdict first, words only on failure; 29 kind-named sites). The one place words
    decide a verdict is `assertError`'s match (`BodyRun.java:39` → `HostWords.caught`).
  - The decoded-plan guard round-trips the edge's records (`TestPlanJson.java:121-124`, `:270-271`), so the planning
    interface stays data.
- **~11:58 UTC, the judges session's H6 §6** (`9a8c4f6f`): the leniencies measured (logged, then switched off), every
  lane run twice.
  - **PCT:** 0 decisions and 0 verdicts change on channel B's DuckDB, H2 and SQLite lanes, or with S131 off under LITE.
    Channel A has no lite judge.
  - **The corpus** (the positive control: probe-on rosters byte-identical to the committed ones):
    - DuckDB: 77 decisions; the fail roster goes 108 → 177, i.e. 69 tests (S131 33, S137 21, S132 14, S15 2, S144 2);
    - H2: 30 decisions, 276 → 305, i.e. 29 tests;
    - S127 and S139: 0 everywhere.
  - **CP-14 (iii) as conditioned fails on the corpus.** It holds if four become door rewrites of test bodies (S137,
    S144, S131's typed half, S15), leaving only the rendered-text comparison. Recorded in the plan with sizes:
    2.2a 3.5 days; 2.2b 5.5 under (ii), 6 to 7 under (iii); plus about 1.5 days of rewrites.
  - **S131's multiset is LIVE in LITE today in both judges** (`HostJudgments.java:128-135`, `:819-821`, `:868-871`;
    `TdsCompare.java:76`; VerdictSql's multiset arguments). Its narrowing, ruled 2026-11-05, is "build pending": a
    product item, sent to the product orchestrator. With S131 off, 0 PCT verdicts move, so the LITE-PCT lane needs no
    leniency row.
  - **The referee has no session of its own:** it replays on the body's own target's session, H2 goldens only. On H2
    lanes that session carries the edge's `MODE=LEGACY`, so CP-8 would change the referee's replay. It needs its own
    declared LEGACY target first (plan 3.2, now a named step).
  - **Words are already separable** (verdict first, words only on failure; 29 kind-named sites). The one place words
    decide a verdict is `assertError`'s match (`BodyRun.java:39` → `HostWords.caught`).
  - The decoded-plan guard round-trips the edge's records (`TestPlanJson.java:121-124`, `:270-271`), so the planning
    interface stays data.
- **~12:07 UTC, the translator's H5 done** (`c3befded`; the one-page answer at the top of `H5-translator-spike.md`;
  ledger draft `H5-translator-ledger-draft.tsv`; evidence `h5/corpus/` V0 and V2-V6, `h5/parity/`):
  - **3.1's frontend split works:** Pure's core plus the dynaFunction and frontend splits regress no parity answer.
    Cypher 4,267/0 (3,540/321/406 errors without the split), IQ 5,984/19 (its ledger test green), Postgres SQL 1,651/0.
    17 tests still fail, all listed: 9 pin the old meaning, and 8 are goldens of the frontends' generated text. 3.1 is
    about 2 days, plus 0.5 day and one ceiling move when 3.3's negative-start raise lands;
  - **typed retargeting from public classes only** (`ModelContext.findFunction` and `SpecCompiler.compile`; it renames
    only the calls that resolved to the String overloads): V6 is the best variant, H2 2,285/280 and DuckDB 2,448/112.
    Every one of V4's typing breaks is gone; it costs 7.1 s on the corpus world. The open check for 3.2 is whether
    `ArchitectureTest`'s entry rule admits an edge caller of `SpecCompiler`;
  - **the 0.6 ledger's first draft:** 130 rows, 52 flagged unclear. The main open groups are CP-5's cost rows, E-R2
    against H1's X rows, text read inside a query, the generator's in-body text, and upstream's vacuous or
    non-deterministic answers;
  - **still open:** the no-translator count for the other meaning strands (`isNumeric`, `parsedDecimal`, `parsedJson`,
    SQLite's Decimal arithmetic, `CrossKindNumbers`, `raw`, `LEGACY_SQL_NULL_UNSAFE_EQUALS`), asked of the translator
    session at ~12:08. B2's (j) covers S20 and the plain set.
- **~12:12 UTC, B1's 0.2 status:** the PR is about 1 hour away (`//gates:local` rerunning; its first run was broken by
  a disk-cache deletion). The first run found a real defect in S0, now fixed as `5e3bbe6d1`: `//core:census`
  (`DialectLeakRatchetTest`'s ban) refused S0's `EdgeCasesGoldenTest` for naming `DatabaseType.DuckDB` and `H2`; it
  now uses `Databases.PLATFORM` and `REPLAY_REFEREE`, the golden is unchanged, and the census is green.
  - **The landing order:** 0.2 first, and the numbers worker's `FloatShortestText` change re-records on top (this
    session's call, sent to both).
- **~12:12 UTC, CP-13's host drift taken by the product side:** the new host-parity track
  (`session_0195rQEmBA2Ek93uxggvDTQx`, `claude/host-parity`) takes P2, P3 (with the three stale Python readers), P4,
  P5, P6, P7, P9, P10, P12, H4 §8's byte-parity suite and per-export differentials, and PR #121's review F1-F3. The
  compatibility program keeps HOSTS-9's pilot, CP-7's part, the ENGINE host pieces, DC1 and F4. The edge file in its
  list (`edge/_pure_v1.py`'s stale ERR reader) is given to it as the same fix, reader only; any other edge file needs
  a message first.
- **~12:30 UTC, the owner's own review, asked by the user** ("does the goal make sense? is the design right? is the
  plan tractable? do we have a detailed plan in plain English? anything else to research? are you happy with it?").
  Re-read: the plan as it stands, the log, the checkpoint, H5's and H6's records, and two code checks. The honest
  findings:
  - **The plan document is not the plan.** Its §0 is stale (translator, 64 days, 5 to 6 weeks, phase 5), and it was
    amended about twenty times by patches. A banner now says so. A clean, short, plain-English final plan has to be
    written.
  - **The design's name is out of date.** After phase 0 the input side is nearly empty. The real design is: one
    meaning in the core (Pure's); formatters after the run; the legend-pure-style checker and the SQL-text appeal at
    the edge; and a handful of typed door rewrites (mostly of the corpus's test bodies).
  - **Three joints of the design are unproven** (no prototype has run them):
    1. **the appeal's move** (2.2b) onto three call sites with verdict records: mapped by H6, never built. It is now
       the riskiest slice;
    2. **legend-pure's relation text in Java:** no Java printer exists (`edge/Presenter.java` and `PurePrint.java`
       print values, not relations). The 791-line SQL builder would be replaced by new code that 423 PCT tests depend
       on. H3 (c) proved legend-engine's TDS envelope, a different thing;
    3. **error records through three hosts** (1.2, plan v59 and v60): scoped by B1, unbuilt.
  - **One feared unknown looks fine:** the edge already uses `SpecCompiler` at 55 places in several files, so typed
    door rewrites from the edge are within today's rules (the specific entry still to be confirmed).
  - **The goal has quietly changed** and should be said once, plainly: no longer "match legend-engine exactly", but
    "lite follows Pure; legend-engine's formats and its answers are matched where they agree with Pure, and the rest
    is recorded". AGENTS.md's upstream-fidelity tenet and the goals of record say otherwise today; amending them is
    the user's word.
  - **Sequencing risk:** five product tracks are changing the same judge, number and host files now. The product's
    judges track must land before 2.1 and 2.2a.
  - **Nothing has run on CI on every OS yet** (the first PR is not up).
- **~12:20 UTC, two more spikes started, under the user's standing ask for up-front homework:**
  - **H10, the judges session:** a prototype of 2.2b on a slice (verdict records worded by an edge presenter; one run
    interface; the appeal unchanged; `assertError`'s match; one door rewrite, S131's typed half). The check: both
    corpus rosters and channel B's outcomes and messages byte-identical. Record `H10-judge-records-spike.md`;
  - **H11, a new session** (`session_0195rQEjVtMXfZdcaQdweUzf`, `claude/compat-relation-text-spike`): legend-pure's
    relation `toString` printed in Java over typed rows, judged by PCT channel B's relation suite with the SQL builder
    off. Record `H11-relation-text-spike.md`.
- **~12:17 UTC, the judges session's shape note for 2.2a and 2.2b under CP-14 (iii)** (H6 §7, `491f6464`):
  - **the end state:** ONE `edge.CompatJudge` handed in at the door (`ExecuteOptions` carries the judge), with three
    product call sites: `TestDoor.java:141-155` through `testexecutionplan.Edge` (kept: records, effect, seed); the
    per-assert entry through one claim (`EngineTestText`'s 13 methods become `claim(TextAsk)`); `TestRunner` through
    `testexec.JudgeRun` and its `Hearing`;
  - **`VerdictRecord`:** one record per today's 28 `HostWords` kinds, worded by two presenters;
  - **the four door rewrites,** with exact inputs and outputs, run on the TYPED body through `TypedQuery.withBody`
    (public): R-S137, R-S144, R-S131 typed, R-S15;
  - **two cases the ruling did not cover:**
    1. the JSON root array multiset (1 test per corpus lane). **This session's call:** a second whole-kind rule of
       the edge judge (Json, as Rendered is). It is the same shape the ruling already allows, and loses no test;
    2. R-S131 is weaker for flat cells of TDSes with 2 or more columns (at least 3 of DuckDB's 7 cell cases). H10
       checks that no failing test turns into a pass; any case that would keeps the row-tuple compare in the edge
       judge's own rule;
  - `assertError` is the one place words decide a verdict: `Hearing.errorMatches` presents the raise record.
- **~12:17 UTC, a register id collision,** caught by the product's hosts worker: PR #121 adds S371 and PR #122 adds
  S371-S376. Proposed to the product orchestrator: #122 keeps its ids, #121's row becomes S377, and the `currentUser`
  405 row S378. Awaiting its answer.
- The hosts worker's re-owning plan for `currentUser` (a store-level `reown`, one native entry, run once under the
  folder's lock with a marker file) matches the user's ruling.
- **~12:21 UTC, the register ids settled:** PR #121 keeps S371 (it is mid-way through a full CI run), so **PR #122's
  new rows are S372-S377** (`81be6fa3`), and the `currentUser` 405 row is S378. The mapping from this log's earlier
  entries:
  - S371 → S372, H2 `parseJson`;
  - S372 → S373, `joinStrings`;
  - S373 → S374, the pivot's list aggregate (pending; the product track has it by its new number);
  - S374 → S375, the database versions' blanket row;
  - S375 → S376, E-R2's blanket row;
  - S376 → S377, the prelude and the parser.
- **~12:22 UTC, B2's H8 (h): the corpus's H2 lane without legend-engine's H2 settings** (`d37427ef4`): the product's
  `MODE=REGULAR` and `DEFAULT_NULL_ORDERING=LOW` in place of `MODE=LEGACY` and `HIGH`.
  - **Exactly 1 test moves, from pass to fail** (the fail roster 276 → 277; the skipped, unordered and engine-order
    registers are identical): `meta::relational::graphFetch::tests::isolation::testIsolationOfPropertyTargetFilter`. Its
    own model compares a `BIT` column with `1`, which H2's regular mode refuses and legacy mode allows. That is the
    test's own model text, not lite's lowering.
  - **Null ordering moves nothing:** lite's H2 writes the null placement on every sort key.
  - **The referee keeps its own database:** legend-engine's H2 2.1.214, isolated, opened with LEGACY, HIGH and
    NON_KEYWORDS (`core/src/test/java/com/legend/testbackend/EngineH2Referee.java`; the user's ruling of 2026-11-05,
    "Engine's 2.1.214, isolated"; "the mirror" beside every lane's session).
  - **So CP-8 costs 1 corpus test,** and `DatabaseSettingsForm` can simply be deleted (3.2). No "target settings as
    data" design is needed.
  - **To reconcile:** the judges session's H6 §6 (b) said the referee replays on the body's own target's session.
    Asked of it (~12:23); if B2 is right, plan 3.2's step "the referee first gets its own declared target" is
    unnecessary.
- **~12:23 UTC, the pivot row did not reproduce.** The product's hosts worker tested lite's probe-plan pivot on H2,
  DuckDB and SQLite with a group missing a key: sum, max, first, mode, joinStrings, maxBy and minBy answer null and
  count answers 0, as DuckDB's native PIVOT does; no Pure aggregate reaches a list in a pivot. So H1's "unrecorded
  difference" from S110 has no reachable case, and the pending row is **withdrawn from PR #122** (`6b65330b`; S374 is
  left unused). PR #122 now adds S372, S373, S375, S376 and S377.
  - **A new case found there, the product orchestrator's to put to the user:** legend-pure's pivot
    (`TestTDS.applyPivot`) leaves an absent (group, key) cell NULL for every aggregate, count included; lite and
    legend-engine answer 0 for count. Its recommendation: keep 0, with a register row. This session's note to it:
    that sides with legend-engine against legend-pure, the reverse of today's direction, so it should be put to the
    user as an exception. The deciding question is whether `applyPivot` is Pure's definition or only its in-memory
    test implementation.
  - **A lesson for the ledger:** H1's rows are readings, not measurements. Two of its "new" findings have now failed a
    test (this one, and its S138 claim was itself a correction). The 0.6 ledger marks each row "measured" or "read".
- **~12:26 UTC, B1-R's answers** (`claude/compat-engine-lane` pushed at `24c858542`):
  - **Where lite answers and legend-engine refuses or fails, lite gives PCT's expected value: no exception found.** Of
    the 245 such tests (DuckDB's 153 LITE_FIXES and 55 LITE_ANSWERS; H2's 37 LITE_FIXES), every body was scanned in the
    pinned sources:
    - 191 assert an exact value;
    - 44 assert a Boolean lite computed;
    - 4 assert emptiness;
    - 4 assert a type;
    - 2 check less than a value (`letFn::testLetChainedWithAnotherFunction`, `assertNotEmpty`;
      `parseDate::testParseDateTypes`, `assertInstanceOf`). Those 2 are the only places lite could differ from
      legend-pure's value and still pass.
  - **The per-push cost, by part:** the ENGINE lane 273 s serial when its inputs move, nothing when they don't
    (cached); the batteries 8 s; the heavy oracle half 34 s. The edge net is 568 s.
  - The census (`//gates:local`) refused the batteries' PCT runner for naming a database outside its owners and parsing
    by grammar level outside the parser. Fixed by moving the seeding into the oracle's package; no pin moved.
- **~12:27 UTC, slice 0.2 is PR #123** (`claude/compat-s0` at `5e3bbe6d1`, four commits on `main` `b75d6c73a`): the
  edge net hardened, plus `//gates:edge_net` as a per-push lane.
  - `//gates:local` is green: 557 of 558 as root in 2,539 s, and the 13 Postgres-backed tests as non-root.
  - No pin, ceiling, ledger or allowlist moves.
  - B1 is subscribed to its CI: the first run of this program's code on every OS.
  - The per-part cost and CI's wall time follow as the run reports.
  - Independent review requested of the reviewer session (~12:28).
- **~12:35 UTC, the user:** "I'm not merging 122 that was a mistake to even open that PR stop asking". **PR #122 is
  closed.** The lesson: no documentation-only PR is put up for the user's merge word; register corrections ride with
  the code change they belong to. The product orchestrator was told; ids S372-S377 are unused.
- **~12:35 UTC, the user asked** for "the actual design and plan clearly in detail in plain English", and "What do we
  lose ... Will still be able to take legend-engine projects and run them? What will break?". **Written:**
  `docs/records/compat-design-and-plan-plain-2026-11-19.md`: the design in four parts; what stays; what is lost, row
  by row with its measured size; the plan in two stages with a stop between them; the two open decisions and the two
  running prototypes. A new measurement for it: lite's own 62 projects use `substring` and `indexOf` 0 times.
- **~12:31 UTC, the translator session's task 0: the other meaning strands with NO translator** (measured on top of
  V2; one switch per strand; corpus H2 | corpus DuckDB | pure/v1's 28 lines; PCT follows in 30 to 40 minutes):
  - `substring`/`indexOf`: -4 | -4 | 0;
  - S20's bare-DECIMAL literal: **+2** | 0 | 0 answers. Turning it OFF wins 2 H2 tests
    (`testSubAggregationInQualifier`, `testSubAggregationWithIfOnRelationMapping`: the DECIMAL prints `52` where the
    golden says `52.0`);
  - the plain set: pure/v1 only, -1 answer (`raise.sqrtOfNegative`: legend-engine 200 with null; lite raises).
    `raise.divideByZero` moves in words only (both 500);
  - `isNumeric`: 0 | 0 | 0;
  - `parsedDecimal` (`DECIMAL(5,2)`): -2 | -2 | 0 (`testToSQLStringParseDecimal*inH2`);
  - `parsedJson` (`}{`): **+1** | -1 | 0. On H2 the edge's own form is broken (an invalid `REGEXP_REPLACE` flag), so
    OFF wins `testPushDownProjectWithParameter`; on DuckDB it loses `testSubTypeAtRootLevelWithInheritanceMapping`;
  - `LEGACY_SQL_NULL_UNSAFE_EQUALS`: -1 | -1 | 0, a plan-text assert (CP-4); no answer moved;
  - `DatabaseSettingsForm` (H2 LEGACY): -1 | n/a | 0 (B2's same test);
  - `CrossKindNumbers`: 0 by construction;
  - SQLite's `refusesDecimalArithmetic`: no corpus or pure/v1 lane on SQLite; PCT SQLite pending;
  - **`PureText.raw`: n/a | -2,452 | 0.** EVERY DuckDB corpus test: `createTablesAndFillDb`'s `executeInDb` text is
    written for H2, so setup fails without the conversion. That is input acceptance, not an answer's meaning, and it
    stays at the door.
  - **So beyond `substring`/`indexOf`, the real answer losses are at most 3 corpus tests per lane, offset by 3 wins on
    H2, and 1 pure/v1 answer.** This is the user's "do lots of other things break too?": no.
  - The switches are on `claude/compat-translator-spike` (`H5Off.java`) for B2's PCT ENGINE run.
- **~12:32 UTC, slice 0.3 and the batteries are PR #124** (`claude/compat-engine-lane` at `f922740bb`, 17 commits over
  `main`). It also carries slice R's oracle tooling, which never had a PR of its own. `//gates:local`: 558 pass, and
  the 12 Postgres-backed tests pass unprivileged. B1-R watches its CI. The independent reviewer takes it after #123.
  - **How PRs go to the user from now on:** quietly. A PR is brought to the user once, when CI is green on every OS
    and its review is in.
- **~12:34 UTC, B2's H8 (e): the rest of CP-4's cost** (`claude/census-b2` at `0b720d27d`):
  - **The appeal carries a great deal today:** 964 corpus tests on H2 and 990 on DuckDB pass THROUGH a successful
    appeal (the SQL text failed and the rows held); for 169 on each lane the appeal alone carries the pass.
  - **With every SQL-text assert sent to the appeal** (lite's own text), it decides 1,433 on H2 and 1,475 on DuckDB:
    469 and 485 more, and they stay passing, by answer. Tests passing on text alone fall from 22 to 0.
  - **Not decidable by the appeal: the 143** (F1), plus 3 diverged and 6 on plan structure.
  - **Programs that execute generated text:** `toDDL`, 3 aggregation-aware tests (57 statements), which pass at `main`
    and, on H2, under lite's own DDL too; `setUpDataSQLs`, 0 (S192's read-only reload). So 2.3b's "executed generated
    DDL" is 3 tests.
  - **Run time, with every SQL-text assert going to the appeal:** H2 187 s → 179 s; DuckDB 628 s → 630 s. It costs no
    corpus time.
  - (j) is running on B1-R's ENGINE lane.
- **~12:34 UTC, H11's interim report: legend-pure's relation text printed in Java WORKS**
  (`claude/compat-relation-text-spike`, record `a9df8c39`, code `bce10126`; scratch only). PCT channel B, HOST judge,
  with the SQL builder switched off for a relation's `toString`:
  - **DuckDB, all five suites: 1,240 of 1,249 pass, the same as `main`.** The 423 tests that assert a relation's text
    make 431 comparisons, and **431 of 431 equal PCT's expected text byte for byte**;
  - **every one of those prints is at an assert side's root** (0 composed, 0 nested);
  - **H2's relation suite: 432 → 436 passes.** 4 tests go from error to pass (on `main` the relation's SQL TEXT needs
    `LIST_GET`, which H2 lacks; its rows do not); 342 of 342 texts are byte-equal. Its exact ledger lists those 4, a
    ledger move for the real slice;
  - **no new hook in the core:** all six edited files are in `com.legend.edge`. It rides three existing hooks, which
    are exactly the ones 2.2b reshapes (claim an assert at planning; ask for its sides; be handed the sides' typed rows
    at the run);
  - **confirmed by measurement: 1.3 must follow 2.2a.** Under BOTH, the compatibility database judge refuses all 365
    relation bodies once the SQL builder is off;
  - **for CP-6:** the printer needed only the column's name, its Pure type and the typed cell. It needed NO DateTime
    written precision: legend-pure's TDS re-reads every date cell at exactly three subsecond digits
    (`TestTDS.java:209`). A finding: today's SQL builder prints a finer fraction in full where legend-pure prints `.123`;
    no PCT golden tests it;
  - **size so far:** the printer 137 lines, the arm 125, the record 16, against the SQL builder's 791;
  - a note for the real slice: `ArchitectureTest.PLANNER_ENTRIES` lists `EngineTextPlanning`'s callers by class, so a
    new edge class needs caller rows (the user's word) or lives inside an existing caller.
  - Next: the CSV printers, EDGE-1/2/3, more lanes and the 1.3 estimate.
- **~12:36 UTC, the independent review of PR #123: HOLD, for small fixes.**
  - **The central claim holds:** the reviewer re-derived "no answer moved" with its own back-mapping (pure/v1 28 of
    28; the PCT goldens 0 differences; the corpus ledgers' first four fields identical), and no `src/main` file moves.
  - **To fix (sent to B1):**
    - 3 of the 10 plants are vacuous, and 3 plants are missing;
    - `PctDisciplineTest`'s one-indirection rule is broken in spirit (`pct_tests_lib` now depends on the sorting
      normalizer);
    - two guarded entries moved without being named: `EDGE_TEST_SRCS` +3, and `json`'s visibility;
    - the PR text should say "new pins" for the order lines and the 171 unmasked values;
    - the net does not watch a sort on an unprojected key (2 tests on each of 3 databases);
    - PCT's own SQL is no longer diff-watched.
  - **CI is red for an infrastructure reason:** Maven Central returned 404 for `h2-2.1.214.jar` (the referee's H2).
    Told to the product orchestrator's CI-health track. The lane is not yet shown green on any OS.
  - **For the final plan:** `ModeBranchCensusTest` does not count `twoForm`, `facet`, `LoweringForms` or the Feature
    flags, so "no mode in the core" will rest on B2's bytecode ratchet (49 parameters, 50 calls → 0), not on that
    test.
- **~12:36 UTC, PR #124's scope, named by B1-R:** 50 files, +5,275/−34: slice R's oracle (19 files, +1,596); B1's
  requests commit (2 files); H7 (33 files, +3,642, of which 12 are code). Guarded files touched: `jars_table.bzl`
  (ruled); `gates/BUILD.bazel` (`pct_duckdb` gains `engine_lane_test` and `batteries_test`; `heavy` gains the two
  oracle tests); visibility lines (`//json` to `//tools/engine-runner`, core's two `exports_files` blocks, four
  engine-runner targets to `//pct`); the runner pool's 10 artifacts; JUnitMain's opt-in flag; no census inventory. It
  overlaps #123 in two files (the pure/v1 requests); whichever lands second merges. CI run `44524802258` is in
  progress on linux, macos and windows.
- **~12:38 UTC, the user asked** whether the real corpus ran on both H2 and DuckDB and all five PCT suites on DuckDB,
  and whether things look good across all. Answered with the runs' table (yes to both). The honest caveat: each change
  was measured ALONE. **H12, a combined stage-A dry run, was asked of the translator session:** V2, lite's SQL text,
  every strand but `raw` switched to the product's meaning, and H2's product settings, all at once, on both corpus
  lanes, PCT channels A and B on DuckDB's five suites, the ENGINE lane and pure/v1. Every moved test is attributed, or
  flagged as an interaction.
- **~12:40 UTC, task 0 COMPLETE** (`claude/compat-translator-spike` at `d5ba073e`; "Task 0" at the top of H5's
  record; evidence `h5/corpus/V7-V9`, `h5/purev1/`, `h5/pct/`). The PCT half, channel A as the PURE audience, strands
  on against all off:
  - PCT DuckDB (5 suites) and H2 (relation): every other strand off moves **0** tests; the core's `substring`/`indexOf`
    change itself gains **+3** (S134's known failures pass);
  - PCT SQLite (5 suites): +3 the same, and **-5** when SQLite's Decimal-as-REAL is off (`testRemWithDecimals`,
    `testDecimalTimes`/`Divide`/`Plus`/`Minus`): CP-11's strand, lite's own ruling (c).
  - **The whole table** (corpus H2 | corpus DuckDB | pure/v1 | PCT DuckDB/H2/SQLite | departs from Pure? | what it is):
    - `substring`/`indexOf`: -4 | -4 | 0 | +3/0/+3 | departs | legend-engine's acknowledged legacy;
    - S20's literal: +2 | 0 | 0 | 0 | departs | a defect (it costs legend-engine's own goldens);
    - the plain set: n/a | n/a | -1 | n/a | departs | a defect (no check);
    - `isNumeric`: 0 | 0 | 0 | 0 | Pure is silent | its only meaning;
    - `parseDecimal(5,2)`: -2 | -2 | 0 | 0 | departs | a defect (it rounds and overflows);
    - `parseJson` `}{`: +1 | -1 | 0 | 0 | Pure is silent | a defect (lenient); the edge's form is broken on H2;
    - SQLite Decimal as REAL: -5 on PCT SQLite only | departs | lite's own ruling (c);
    - `PureText.raw`: the whole DuckDB corpus lane | Pure is silent | input acceptance, kept;
    - `LEGACY_SQL_NULL_UNSAFE_EQUALS`: -1 | -1 | a plan-text assert (CP-4);
    - `DatabaseSettingsForm`: -1 on H2 (CP-8);
    - `CrossKindNumbers`: 0.
  - **The options for `substring`/`indexOf`** (corpus H2 | DuckDB): today's flag in the core 2,289 | 2,452; a typed
    use-site translator 2,285 | 2,447; a rename by name 2,281 | 2,442 (no better than nothing); nothing 2,281 | 2,443.
  - **A cost of any target-aware rewrite:** `HeldModels` keys by text hash only, so a holding host compiles once per
    (audience, target), 3.4 to 4.1 s per build of the corpus world.
  - **Reading for CP-12 (D1):** answering Pure's way costs 4 corpus tests per lane for `substring`, 2 for
    `parseDecimal`, 1 on DuckDB for `parseJson`, and gains 3 PCT tests and up to 3 H2 corpus tests. No model-level
    translator is worth building. Note: CP-14 (iii)'s four test-body rewrites and CP-11's SQLite rule are small typed
    rewrites at the door, so a short, fixed list of door rewrites exists either way.
- **~12:42 UTC, the independent review of PR #124: HOLD, for small fixes.** No answer and no product or edge main code
  moves; slice R holds the reference-checkout tenet (all targets testonly; the per-push half loads no legend-engine
  jar).
  - **A correction to numbers already given to the user:** the lane's classifier undercounts legend-engine's
    refusals. Planning refusals worded other than "No SQL translation" were classed as defects ("unsupported"):
    DuckDB 51, H2 relation 36. So legend-engine refuses at planning about **110** tests on DuckDB (not 59) and **36**
    on H2 (not 0), and "LITE_FIXES" shrinks by the same counts (DuckDB 153 → about 102). **Pass and fail totals are
    unchanged** (lite 1,212 against 1,005; 448 against 411).
  - **To fix (sent to B1-R):**
    - a Windows path bug in `//pct:batteries_test` (a backslash path inside a model literal);
    - the merge order with #123 (#123 first; #124 then learns its format and re-pins the oracle);
    - a plant that the lane really runs the ENGINE audience;
    - every guarded file named (`release.MODULE.bazel`, `maven_runner_install.json`, `json` and `core` BUILD
      visibility, `JUnitMain`, the gates' lists, `pct_tests_lib`'s deps);
    - the cause of `parser_equivalence` red on its CI run.
- **~12:42 UTC, the product's host-parity worker** (`claude/host-parity`): three things that touch this program.
  1. `edge/_pure_v1.py`'s stale ERR reader is fixed through the product's one reader (`_folded.py`); what the edge
     answers is unchanged, and no other edge file is touched.
  2. `PLAN_VERSION` will be generated from Java's `PlanJson.VERSION` (TypeFacts writes the TypeScript and Python
     constants; `PlanVersionsAgreeTest`'s regex goes). **So CP-7's later bump to v60 changes one constant and reruns
     two update targets.**
  3. Lite's Arrow metadata keys have one source (`com.legend.result.ArrowKeys`), product keys only. The edge's keys
     (`legend.builder`, `legend.activities`, `legend.columns`) are left to this program: an edge-side holder with its
     own generated output, a note for B1-R's ENGINE host pieces.
- **~12:46 UTC, two deep pieces started** (the user: "Anything we need to do deeply in next 15 min before token
  reset?"):
  - **the independent reviewer's checkpoint re-check:** the plain-English design and plan, the checkpoint, and every
    phase-0 record (H5 to H8, H11), judged against its own F1-F13: what is still open, what the "what we lose" table
    misses, each stage-A step's proof, and the strongest case against D1 and D2;
  - **H13, a new session** (`session_0195rQEoAgK9WB7AM5Cowp2f`, `claude/compat-error-records-spike`): the third
    unproven joint, error records (kind, place, literal operands) worded after the run, through the three hosts. The
    check: PCT's and pure/v1's pinned messages byte-identical or listed.
  - With H10 (the appeal's move), H11 (relation text, passed its interim) and H12 (the combined stage-A dry run),
    every unproven joint of the design now has a prototype or a measurement running.
- **~12:47 UTC, H11's FINAL report: legend-pure's relation text in Java works** (`claude/compat-relation-text-spike`
  at `5c62ee85`; §0 is the one page). PCT channel B, HOST judge, with the SQL builder off, each lane run twice:
  - **DuckDB, five suites:** 431 of 431 relation texts equal PCT's expected text byte for byte; 1,240 of 1,249 pass, as
    `main`;
  - **H2 relation:** 342 of 342 texts equal; 436 pass against `main`'s 432;
  - **SQLite** (relation and standard): 419 of 420 equal. **Postgres 16:** 427 of 430 equal;
  - **differences caused by the printer, on all four databases: 0.**
  - **A PRODUCT DEFECT it exposed** (sent to the product orchestrator): a window column read through a list operation
    lowers the window call inside the list encoding's subquery. SQLite and Postgres answer silently wrong rows
    (`testExtendJoinStringOnNull`: 'A' and 'E' where the answer is 'AE' and 'AE'), and Postgres refuses another. On
    `main` it is hidden because the SQL text builder wraps the relation as a subselect. It needs a product fix before
    1.3 lands.
  - **A counter-example to 0.4 (f) on the JUDGE side:** 41 corpus tests (`calendarAggregation/tests/testCalendarFunctions.pure`)
    are `toCSV($result.values)->replace('\n', ',')` over Float cells. **This session's call:** "CSV with rows joined
    by a separator" becomes a presenter form (the product's host judge already names the shape, `CSVJOIN:<sep>`), not
    a refusal. H11 now counts every judge-side composed print in the corpus and PCT, by shape.
  - **No new hook in the core.** Query roots use public doors only (24 of 24 edge-case answers equal upstream's text).
  - **For CP-6:** the column's name, its Pure type and the typed cell are enough. No written precision is needed, and
    no SQL slot.
  - **The edge cases:**
    - EDGE-1's Floats are the same in Java as in legend-pure;
    - EDGE-2 (`toCSV`'s DateTime) moves to upstream's `2024-01-02 10:30:00`;
    - EDGE-3 (`%r` of a newline) moves to upstream's `'a\nb'`.
    The last two are listed golden moves, toward upstream.
  - **Sizes:** the printer is 213 lines; with the arm, about 400, against the SQL builder's 791. The judge's SQL for
    the 365 relation tests shrinks 73%.
  - **Its estimate:** the relation-text half of 1.3 is 4 to 5 days; with the value prints (not prototyped), all of 1.3
    is 6 to 8 days, against the plan's 4 to 6.
  - Not done: the corpus run (asked for now), seven other lanes, any guard run.
- **~12:47 UTC, H10's first counts: the judge-as-records design and the appeal's move work on the spike**
  (`claude/compat-judges-spike` at `beacf4c9`; scratch).
  - **Built:** the product's host judge builds a `VerdictRecord` (25 kinds) and asks the run to present it;
    `LiteHostWords` and `EdgeHostWords` become presenters; ONE interface (`testexec.JudgeRun` with its `Hearing`)
    replaces `EdgeRun`, `EdgeHearing`, `HostWords` and `HostForms` (five files deleted); the SQL-text appeal runs
    through it unchanged; `testexecutionplan.Edge` is untouched; `assertError`'s words come from
    `hearing.caughtMessage(e)`. The diff is +410/-489.
  - **The check, outcomes AND messages:**
    - channel B DuckDB (5 suites, 1,249): 0 moved under COMPAT/BOTH, COMPAT/HOST and LITE/HOST (all 462 of lite's
      failure messages byte-identical). Under LITE/BOTH, 0 outcomes moved; 4 messages differ in DuckDB's own
      nondeterministic error text (the `testAdjustBy*BigNumber` tests);
    - corpus H2: identical in every file;
    - corpus DuckDB: 108 → 110. The +2 are exactly the S15 pair (`log::testFilter`, `testProject`), whose door rewrite
      R-S15 is not built yet; the other 106 messages are byte-identical.
  - **A stop-and-tell item:** the corpus has no door. `PureTestRunner` (`core/src/main/java/com/legend/test/`, the
    product's test runner) plans each typed body itself, so a door rewrite of corpus test bodies needs either a hook
    in it or the corpus run through an edge-owned door. **This session's steer:** no hook; measure an edge-owned door
    composed of public entries, and judge honestly whether a "body preparer" parameter would be a legitimate product
    extension point or a hook by another name.
- **~12:48 UTC:** the window-inside-a-list-encoding defect (H11 §8 P1) has its own product track
  (`session_0195rQEjkEZDfj2GjXCAhyJr`, `claude/window-list-lowering`). It gates slice 1.3, and it will report when the
  fix is on its branch.
- **~12:54 UTC, the independent reviewer's checkpoint re-check** (`docs/records/compat-checkpoint-recheck-2026-11-19.md`
  on `claude/compat-review` at `5d2ce214`; read from the records, not from this session's summaries). **VERDICT:
  proceed to stage A**, on four conditions:
  1. the user rules the change of goal first (R1): "lite follows Pure; legend-engine's formats are matched, its
     answers where they agree with Pure; the rest is recorded". AGENTS.md's upstream-fidelity tenet and G8 are amended.
     D1 and D2 then follow from it;
  2. the "what we lose" table is completed (R3): PCT SQLite's 5 Decimal tests; legend-engine's feature flags in
     requests; error words and positions; generatePlan after A2; lite's own users under R4; E-R2 being one-way;
     warehouse query cost; the mislabelled "fixes". **Done** in the plain plan (rows 8 to 12, and "not measured").
     Checked in the code for (d): `PureV1Routes.generatePlan` builds from lite's plan and does not use the printers
     A2 deletes;
  3. A1's nets are sound and green on every OS (R4): #123 and #124 are both on HOLD; the referee's H2 jar should be
     vendored or mirrored, sha-pinned;
  4. H6 §6.3 is corrected (R5): the referee replays on legend-engine's isolated H2 2.1.214; H8 is right. The plan's
     "declared LEGACY referee target" step is dropped.
  - **Its strongest case against D1:** the `substring` change is SILENT. Its fix: "Pure's way, LOUD where it differs",
    a detector at the ENGINE door (warn, or refuse by a switch), a subset of H5's use-site analysis. Its own count: 0
    calls of the affected functions in legend-engine's six pinned showcase projects (99 files).
  - **D2:** hold until the ENGINE-lane PCT count with both shortcuts off lands.
  - **Stage A:** the order A1, then A2 and A3, then A4 is right; A2's scope must name the plan-text family (about
    3,500 lines) and roster the moved tests; A3 and A4 have listed dependencies. All folded into the plain plan.
  - **Stage B:** the least proven step is B1, error records (H13 started at 12:46); B3 before B2's real build; H11's
    presentation instruction in the test plan needs done-when 3 amended, or kept out of the plan.
  - **B5's guard:** B2's 49 parameters and 50 calls is the ratchet, not `ModeBranchCensusTest`.
  - **Not yet looked at:** warehouse databases; Studio end to end; large results through Java presenters; how the
    pins are re-baselined when legend-engine moves past 4.145.0; the edge net's CI time.
  - **F1-F13:** answered F1, F2, F7, F11, F13; mostly F12; measured with a decision pending F5; measured but not yet
    planned F6 (now in B5); partly F3, F4, F8, F9; open F10 (who lists version-caused differences).
- **~12:55 UTC, the product orchestrator's register map** (a note, nothing asked):
  - The `currentUser` 405 row is **S377**, not S378. It is built in `4452313a` on `claude/hosts-followups`.
  - **S372-S376 and S378 are unused.** The pivot count case keeps **S379**, if the user rules a row.
  - A fraction under an Integer property answers the same on both roads: 422, data `castImpossible`
    ("Decimal to Integer"), converted in SQL by the planner. S371 is amended to say so.
  - **What this means here:** the register corrections that were in the closed PR #122 (S63, S138, and the new rows)
    have no ids reserved. Each takes the next free id when the code change it belongs to lands, and this log's earlier
    S372-S377 mapping (lines near "new rows are S372-S377") is void.
- **~13:05 UTC, H12, the combined stage-A dry run, is in** (the translator session;
  `docs/records/compat-homework/H12-combined-dry-run.md` and `h12/` on `claude/compat-translator-spike` at
  `1339c6fc`; read from the record). Applied at once: (1) Pure's `substring`/`indexOf` with 3.1's splits and no
  translator; (2) B2's lite-text probe (every engine-text and plan-text printer answers lite's own SQL); (3) every
  meaning strand to the product's, `PureText.raw` kept; (4) H2's product settings, the referee keeping its own.
  | lane | `main` | combined | moved |
  |---|---|---|---|
  | corpus H2 | 2,289 pass, 276 fail | 2,135 pass, 430 fail | +157 fail: (2) 142, (1)+(2) 4, (1) 4, (3/4) 3, (2)+(3/4) 1, INTERACTION 3; +3 pass (S20 2, `parsedJson` 1) |
  | corpus DuckDB | 2,452 pass, 108 fail | 2,290 pass, 270 fail | +162 fail: (2) 146, (1)+(2) 5, (1) 4, (3/4) 3, (2)+(3/4) 1, INTERACTION 3; no new pass |
  | PCT channel A, DuckDB, 5 suites | as ledgered | Essential: 3 ledger moves; the rest unchanged | S134's 3 `indexOf` known failures pass |
  | PCT channel B, DuckDB, 5 suites, HOST judge | as ledgered | the same 3 | (1) |
  | ENGINE lane (DuckDB 5 suites, H2 relation) | pinned | 13 rows, every one lite FAIL to PASS against PCT's expected value | (1) 7; (3) the plain set 6; H2 relation 0 |
  | pure/v1 net (28 lines) | the golden | 8 lines | `raise.sqrtOfNegative` 200 null to 500 (an answer); `raise.divideByZero` (words); S20's SQL text in 6 lines |
  - **Predicted 427 (H2) and 267 (DuckDB) failing; measured 430 and 270.** The difference is the one interaction:
    `testPlanForDateTimeConstantParameter{NoTimeZone,GMTTimeZone,ESTTimeZone}` assert
    `$planAsString->indexOf(...) != -1`, vacuous under the old `indexOf` (0 on a miss). Only with BOTH Pure's `indexOf`
    and lite's own text is the assert real and false. **So CP-4's count is 142 on H2 and 146 on DuckDB once R4 lands**
    (B2's probe could not see these three).
  - **Nothing predicted to fail passes; no interaction outside the corpus.**
  - **For D2:** with the plain set and S20 off, the ENGINE lane's PCT gains 6 rows (`testSliceError`, `testRemError`,
    `testSquareRootError`, `testRangeStepError`, `testBitShiftLeft_MoreThan62Bits`, `testBitShiftRight_MoreThan62Bits`),
    each BOTH_FAIL to lite passing; legend-engine's manifest calls every one a defect or a refusal. No row moves the
    other way. S20 off moves no PCT row and no pure/v1 answer (SQL text only), and gains 2 H2 corpus tests. The one
    legend-engine answer that stops matching is pure/v1's `sqrt` of a negative (null there, an error in lite). B2's
    (j) is the independent count.
  - **Words needed when A3 lands:** PCT's 3 known-failure ledger rows (S134) leave the ledger; they move only with
    the user's word.
  - Run time: about 15 minutes for everything on a 4-core container (DuckDB corpus 547 s, H2 156 s).
  - Folded into the plain plan: row 2, rows 4 and 5, "what gets better" (the corrected counts), A2's proof, and a
    dry-run table under stage A.
- **~13:15 UTC, two things prepared and HELD for the user's word** (this session's own homework; nothing applied):
  1. **The rulebook amendment, if the user rules the goal's sentence.** Draft wording, to replace the first two
     sentences of AGENTS.md's upstream-fidelity tenet and G8's first bullet
     (`docs/GOALS_OF_RECORD_2026_11_07.md:131-135`):
     > **Upstream-fidelity tenet (the user, 2026-10-29; amended 2026-11-19, the compatibility program):** lite follows
     > PURE: a Pure program means what legend-pure's definition of it says. legend-engine's and legend-pure's FORMATS
     > (result and error envelopes, printed values) are matched, at the edge. legend-engine's ANSWERS are matched
     > where they agree with Pure. Where legend-engine departs from Pure's own definition (a database's meaning for a
     > Pure function, an older database's behaviour, its SQL text), lite answers Pure's way, and the departure is
     > POSTED to the user before it lands and LEDGERED as a row of `docs/SEMANTICS_REGISTER.md`. Both answers, Pure's
     > and legend-engine's, are READ from the pinned sources or ASKED of the engine runner, never recalled or guessed.
     > A divergence that is not a row is a defect; a row the user did not rule is a defect too.
     The rest of the tenet (case by case; the engine runner's README) is unchanged. G8's "Lite does what upstream
     does" becomes "Lite follows Pure; legend-engine's formats are matched, and its answers where they agree with
     Pure". Both files change in the first stage-A code PR that relies on the sentence (A3), not in a PR of their own.
  2. **D1's "loud" option, sized from the code** (read at `main` `b75d6c73a` and H5; not built):
     - **Where:** the legend-engine server door only (`edge/PureV1Routes`, execute and generatePlan). A request there
       IS the code sent to the database, so none of H5's hard part (finding store-bound code inside a test body, the
       40 calls it could not follow) applies. The corpus needs no detector: its tests pass or fail.
     - **What it reads:** the typed query (`TypedQuery.expression`, public) and the user functions it reaches through
       `ModelContext.findFunction`: any call typed as the String overloads of `substring` (2 or 3 arguments) or
       `indexOf` (2 arguments). The `###Relational` dynaFunctions are not flagged (SQL's meaning in every audience,
       H5 F2).
     - **When it is quiet:** the query carries legend-engine's own corrected option. Lite already reads both of
       legend-engine's carriers (`ContextReading.contextFeatures` for `FeatureFlagOption.flags`,
       `ExecutionContext.treeFeatures` for `withFeatureFlags`). The pure/v1 routes read no feature from the request's
       JSON today (no hit in `edge/PureV1*.java`), so the query's own `withFeatureFlags` is the carrier there.
     - **What it does:** by default a warning that names the call (a server log line and a response header); under a
       server setting, a refusal by name before planning. Neither is a mode in the core: the detector reads the typed
       query and never changes it.
     - **Size (estimated):** about 150 lines in the edge plus its tests, 1 to 2 days, inside A3. One register row.
     - **To settle if the user picks it:** whether the default is "warn" or "refuse"; what legend-engine's clients do
       with an unknown response header (Studio must be shown to ignore it).
- **~13:00 UTC, the translator's H12 message** (the counts as logged above from the record). One more point, recorded
  in H5 at this session's request: `ArchitectureTest`'s `PLANNER_ENTRIES_ONLY` covers only `planner`, `testplan` and
  `lowering` (`ArchitectureTest.java:1638`), so it admits an edge caller of `SpecCompiler` and `ModelContext`; no
  other rule bars it, and the reflection ban holds outside the edge only. A typed walk at the door (D1's detector, or
  a typed retarget) needs no new core entry.
- **~13:00 UTC, B2's (j), D2's count** (`claude/census-b2` at `ed70d8b18`, H8 §(j)). With it B2's 0.4 items are all
  done: (a), (a.3), (c), (d), (e), (h), (j), (k). The probe: S20's bare DECIMAL off (the 5 `PureText`
  `floatLiteralKind` overrides return super) and the plain set emptied, on B1-R's ENGINE lane at `815690a57` in a
  local worktree; with PCT A's edge golden net, pure/v1's golden net and both corpus lanes.
  - **PCT ENGINE lane (verdicts):** 6 tests move, all BOTH_FAIL to LITE_FIXES; 0 SAME tests move. All 6 are DuckDB
    error tests: slice, rem, range, sqrt, bitShiftLeft, bitShiftRight.
  - **PCT A, default audience (answers):** 0 outcomes move. 2 values move, both S20: `testFloatPlus` 1.7 to
    1.7000000000000002 and `testFloatTimes` 2.1 to its double; PCT passes either way. 9 cases change only SQL text.
  - **pure/v1:** `raise.sqrtOfNegative` 200 `[null]` (legend-engine's answer) to 500; `raise.divideByZero` stays 500,
    words change; the two Float cells keep their values.
  - **Corpus:** 0 pass to fail; H2 2 fail to pass (S20 makes H2 answer 52 where the test expects 52.0); 4 per lane
    move from DIFFERENTIAL to LITERAL.
  - **Answers that match legend-engine today and would stop matching: 7.** 4 PCT DuckDB values where legend-engine
    answers a value and Pure raises (slice `[]`, rem null, range `[]`, `>> 63` 0; each checked against DuckDB 1.5.6
    itself; each reproduced by an ordinary guard, e.g. `if($d == 0, |[], |$x->rem($d))`; for `>>` a negative operand
    needs -1); 1 pure/v1 answer (`sqrt(-2.0)` null on H2; `if($x < 0, |[], |sqrt($x))`); 2 PCT Float answers from S20,
    reproducible only through Decimal (`toDecimal` on each literal, then `toFloat`). So 5 by a guard, 2 by Decimal.
  - **3 more keep their status and change only words:** PCT sqrt and bitShiftLeft on DuckDB, pure/v1's divide.
  - **Not measured:** H2's essential and standard suites (which hold (c)'s H2 value cells), the other databases.
  - **This session's call:** D2 goes to the user with "remove both" recommended. B2's next work, sent 13:10: (1) the
    same probe on H2's other four PCT suites through the legend-engine route; (2) A2's shape note (the exact deletion
    list with every caller and guard, what each caller answers afterwards, the moved tests by name reconciled with
    H12, a slice plan); (3) B5's ratchet as a prototype test (shrink-only pins on B2's bytecode counts, shown to fail
    on a plant; not turned on).
- **~13:00 UTC, B1-R: PR #124's review fixes are pushed** (head `c9665ad7e`); the merge with #123 waits on B1 (30 to
  60 minutes).
  - Windows path fixed (`ff84dfa69`). The audience plant is in (`AudienceCheck`: every case planned as
    legend-engine's audience, at least one; the table refuses a lane without it). The guarded files are named in the
    PR body. `pct_tests_lib` gains no dependency (`LiteBatteries` moved to `//pct:pct_batteries`).
  - **The classifier is fixed, and the counts that replace the ones given to the user earlier:**
    | | tests | legend-engine refuses | DEFECT | LITE_ANSWERS | LITE_FIXES | SAME | BOTH_FAIL | LITE_BEHIND |
    |---|---|---|---|---|---|---|---|---|
    | DuckDB, 5 suites | 1,249 | 110 (59 "No SQL translation", 51 in other words) | 134 | 99 | 109 | 1,004 | 36 | 1 |
    | H2 relation | 469 | 36 | 22 | 28 | 9 | 411 | 21 | 0 |
    Lite answers 99 of the 110 refusals on DuckDB. CP-12's 245 tests are the same. Every class is computed from
    words by a listed pattern set (`EngineLaneTable.REFUSAL_WORDS`), and the record says so.
  - **CI on the first run:** `linux / pct_duckdb` was the PR's own fault (a read-only seeded DuckDB file copied with
    its mode; invisible as root in the container; fixed). `parser_equivalence` and `core` were Maven Central's 404
    for `h2-2.1.214.jar` again (twice). Green: pct_h2, pct_channel_b, corpus_h2, linux-arm warehouse, diagnostics,
    guard pins.
  - **Per-push cost, measured on CI:** the ENGINE lane's six actions took about 3.5 minutes of an 8.4-minute
    `pct_duckdb` job on Linux (remote cache on); the batteries a few seconds.
- **~13:01 UTC, the host-parity worker's 404/405 note** (the product orchestrator's ruling: on every host an unknown
  `/api/lite/v1/*` path answers 404 `notFound`, a known path with a wrong method 405 `methodNotAllowed`). Answered
  13:10: it moves `no_such_route` from MALFORMED_REQUEST to MISSING in the four HOSTS-9 adapters now (the store pilot
  is not before stage B and inherits it); it updates `PureV1ApiTest`'s two expectations (lines 731-737) itself, checks
  against #123's head and tells B1; any edge-net golden line that moves is named to this session and B1 first; it
  confirms that `/api/pure/v1/nope` on the legend-engine route still gets legend-engine's envelope.
- **~13:02 UTC, B2's (j.3)** (`claude/census-b2` at `7082dd21f`): pure/v1's plain-set answers per database, from the
  golden net's two requests through execute and plan. legend-engine's answers are B1-R's oracle's; DuckDB's own were
  checked on DuckDB 1.5.6.
  | request | H2: legend-engine / lite without the plain set | DuckDB: legend-engine / lite without the plain set |
  |---|---|---|
  | `year / 0` | 500 (the database's error) / 500, words differ | 200 null / 500; the guard `if($d == 0, \|[], \|$x / $d)` gives null |
  | `sqrt(-2.0)` | 200 null / 500; the guard `if($x < 0, \|[], \|sqrt($x))` gives null | raises (Out of Range) / raises |
  - **Each guard is needed on one database and wrong on the other**, so a rewrite that copied legend-engine would have
    to be per target.
  - **DuckDB 1.5.6 answers the unchecked `/ 0` with Infinity**, so the plain set does not match legend-engine on
    DuckDB today either. The plain set matches legend-engine in fewer cases than its name suggests.
- **~13:03 UTC, B1-R's coverage question, and this session's call:** the ENGINE lane classifies DuckDB's five suites
  but only H2's relation suite. **Decided: add H2's essential and standard suites to the lane in PR #124** (two
  actions of about 45 s each). H2 is legend-engine's primary test database and A3's meaning change lands in those two
  suites. The three H2 actions go in the H2 PCT gate job if it has headroom, so `pct_duckdb` does not grow; the gate
  list's move is named in the PR body for the user's word. B2 runs its D2 probe on those lanes when they are on
  B1-R's branch (its task 1, amended 13:15: no private lane extension). Two limits recorded by B1-R, no code change:
  a tolerant float assert hides a moved answer (`testFloatPlus` stays SAME); the oracle's pure/v1 answers differ by
  database, and the batteries pin each database separately.
- **~13:05 UTC, B1: PR #123's review fixes are pushed** (head `ef9e9bfa3`; a fresh CI run of every job). The PR body
  is rewritten for all 7 items.
  - **Plants:** the three vacuous ones fixed; new plants for root order, `JudgeLedgerTest`'s message field and
    pure/v1's plan-text split; each shown not vacuous (the old 16-hex mask planted fails 3 recorder plants; a full sort
    planted fails 3 normalizer plants).
  - **Guards:** `pct_tests_lib` no longer depends on `//tools/edgenet` (the normalizer's plants are
    `//tools/guards:edge_normalize_test`); a `tools/deps/pools.bzl` allowlist edit was declined by the session's
    safety check and not worked around. `EDGE_TEST_SRCS` +3 named and dated for the user's word; json visibility
    avoided; no pool, pin or ledger move.
  - **Mask by first appearance: not taken.** It would move 12 golden lines, format only (`testGenerateGuid` and
    `testGenerateGuidWithRelation` on DuckDB and SQLite standard, which shows two distinct guids that `<id>` hides;
    pure/v1's 8 plan-text ids). Offered in the PR, reverted (`4b523368e`); a re-record is the user's word.
  - **CI at `5e3bbe6d1`:** the Maven 404 was transient. `windows/warehouse`'s `//python:markets_test` timed out at
    300 s, in files the PR does not touch. **`windows/edge_net` is RED: 4 PCT diffs, DuckDB and SQLite essential and
    standard**; everything else green. B1's unconfirmed hypothesis: the net now pins every digit, and DuckDB's and
    SQLite's maths take the platform's libm, whose last digit differs on Windows. `ef9e9bfa3` puts the normalized
    recordings in the lane's artifact so the lines show.
  - **Lane cost:** locally 690 s wall (PCT A 1,484 s machine time over 11 actions; channel B 250 s; the corpus
    ledgers 819 s, DuckDB's host pass 609 s the critical path). On CI: Linux `edge_net` 17.9 min green; Windows 19.4
    min red; macOS not yet run.
  - **This session's calls, 13:10:** take B1's split (PCT A, channel B, pure/v1 and the edge cases per push in
    `edge_net`; the corpus ledgers' two diff tests to the corpus lanes, which already build both judge hosts; each
    gates-list move named for the user's word). Windows: wait for the lines; if they are last-digit database maths,
    a small per-OS overlay of only the differing lines (each with test, field, both values; stop if over about 20),
    never fewer digits and never Linux-only; if they are anything else (order, locale, path, line ending), report at
    once. `markets_test` goes to the CI-health session, re-run once, not touched in this PR. The mask question goes
    to the user once, with the PR.
- **~13:06 UTC, the host-parity worker's `//core:lite_wire` proposal** (lite's own wire library, base and json only:
  the error kinds with `METHOD_NOT_ALLOWED`, `Refusal`, the closed-request readers, `UnknownRoute`, the user and
  not-posted answers; the classifier and everything of legend-engine's left out). **Agreed, with three shapes:**
  `Refusal.raise` carries the plan's raise object as JSON, not fixed scalar fields, so stage B's error records pass
  through unchanged (agreed with H13 before either lands); the store's refusals stay codes, worded per mount, and
  nothing at the edge reads a `Refusal`'s message back; `lite_wire` depends on nothing in `com.legend.edge`. Its
  guard-file rows are its track's to name.
- **~13:07 UTC, B1-R: H2's essential and standard ENGINE lanes run (local; pushing). 33 LITE_BEHIND rows.**
  | H2 suite | tests | legend-engine passes | refuses | DEFECT | lite passes | SAME | LITE_ANSWERS | LITE_FIXES | BOTH_FAIL | LITE_BEHIND |
  |---|---|---|---|---|---|---|---|---|---|---|
  | essential | 345 | 131 | 139 (44 "No SQL translation", 95 other words) | 75 | 252 | 125 | 85 | 42 | 87 | 6 |
  | standard | 205 | 134 | 38 (other words) | 33 | 113 | 107 | 0 | 6 | 65 | **27** |
  | relation | 469 | 411 | 36 | 22 | 448 | 411 | 28 | 9 | 21 | 0 |
  - **The 33:** standard 27 (`greatest` x7, `least` x7, `mode` x2, `testMax_Date`, `testMin_Date`: no list encoding
    on H2; `bitAnd`/`bitOr`/`bitXor` x2 each and `bitShiftRight_UpTo62Bits`: no bit-op support; `bitNot` x2: no
    `xor()`); essential 6 (`testSplit`, `testSplitWithNoSplit`, `testExistsInSelect`: no unnest placement;
    `testCubeRoot`, `testReverseString`: lite names CBRT and REVERSE, which H2 2.5.252 lacks; `testBigFloatAbs`:
    S120). None is a wrong answer: 29 refusals by name, 2 missing function names, 1 S120.
  - **This corrects Q2 and the plain plan's headline**: "lite is behind legend-engine once" held for DuckDB and H2
    relation only. On H2 standard lite passes fewer (113 against 134). Both documents corrected.
  - Two refusal phrases added to the listed words, dated (H2's "Ensure the target system understands Year or
    Year-month semantic."; the registry's "No matched function in function registry for - max(...)"); no DuckDB or
    battery row moved.
  - **Placement:** the three H2 lanes in `//gates:pct_h2`; `pct_duckdb` keeps DuckDB's five and the batteries. Pins
    split by database (`//pct:engine_lane_duckdb_test`, `//pct:engine_lane_h2_test`). CI's first run: `pct_h2` 5.6
    min, `pct_duckdb` 9.9 min.
  - **This session's calls, 13:12:** B1-R pushes now and cross-checks each of the 33 against lite's own H2 PCT
    ledger (a row not ledgered there is a route difference, sent at once). The 33 go to the product orchestrator as
    H2 dialect work. The translator re-runs H12's change (1) and the combined set on the two new lanes; B2 runs its
    S20/plain-set probe on them.
- **~13:09 UTC, the product orchestrator:** the 33 H2 gaps are scheduled for the numbers worker
  (`session_0195rQEjTP2YHCipAiqZmEma`) on `claude/h2-dialect-gaps`, after `claude/numbers-q1`; it messages B1-R first
  for the cross-check. A route difference (a row not a known failure in lite's own H2 ledger) goes to the product
  orchestrator separately.
- **~13:09 UTC, H13's interim report** (the error-records spike; `H13-error-records-spike.md` §1-7 on
  `claude/compat-error-records-spike` at `c8d74436`).
  - **PCT DuckDB, both channels, all five suites** (the net's recordings against `main`'s goldens): channel A answers
    2,710 lines, 8 differ; channel A outcomes 1,165, 6 differ; channel B outcomes 1,249, 6 differ. **Exactly 6 tests
    change, all Essential, the same on both channels** (`testAtError`, `testDayOfMonthError`, `testHourError`,
    `testMinuteError`, `testSecondError`, `testSquareRootError`): each prints an operand the plan holds as "computed",
    though the query wrote a literal; the lowering hands the raise a wrapped expression (`Cast(FloatLit -1.0,
    DOUBLE)`, `Cast(StringLit '2017', TEMPORAL_TEXT)`, `LIST_LENGTH(...)`). H13 is measuring three lowering sites that
    hand the raise the literal they were given: the true cost would then be 0. The other 2 answer lines differ only in
    the SQL text an answer quotes (the `concat(...)` sentence became its token). Every other pinned message is
    byte-identical, including 22 raises worded after the run from literal operands.
  - **The prototype:** `Site` gains `List<Operand>` (Text, Int, Flt, Dec, Bool, Day, Instant, Computed), built by
    `PlanMaker.sites` from `RaiseSite.operands()` with no style asked; `PlanJson` writes and reads it natively. One
    function words a raised site after the run (`PureMessages.worded`); both routes funnel through
    `RaisedWords.wordsOf`. **Deleted: `PlanStyle.siteWords`, `PlanStyle.errorWords`, S1's `SiteFacts`,
    `EdgeHearing.raisedWords`, `Edge.siteWords`. No new hook in the core.** Python's `_raised.py` and the tab's readers
    read 11 of 11 shared fixtures unchanged; a site view without operands beside a plan with them is refused by both.
  - **Static census:** 51 raise construction sites, 60 operand positions (30 always a String literal, 4 Integer, 1
    Float/Decimal, 8 always computed, 17 either); 12 of 27 kinds can lose a printed value; no Boolean, Null, Time or
    Array literal reaches an operand today.
  - **This session's calls (13:15):** (1) the plan JSON grows by the operands in a LITE plan too; accepted as the
    design (both sides word from records). **B1's done-when is amended** to "the product plan's JSON changes only by
    the sites' operands (and the SQL's raise text, which shrinks to its token), and its version"; measured against
    `lean-sql-baseline`. (2) The checked graph's defect cell (`RaiseText.asData`, CONSTRAINT_UNEVALUABLE, legend-engine's
    sentence written in SQL as an answer cell) becomes a typed defect cell worded by the presenter; counted and sized
    as a row for B2, not built. (3) `PureMessages.sentence`'s last caller, `EngineStyleH2.raiseMessage`, goes with A2
    (sent to B2 for the shape note). (4) `sizeWords`/`answerSizeWords` do not get a new `HostWords` member; they are an
    input to the judges' `Hearing` design (sent to the judges session, which is to check H13 §5 against `JudgeRun`).
    The three lowering sites: measured on two conditions (lite's own lowering choosing the typed node, never a run-time
    value or a hook; the operand printing exactly PCT's expected text); if a site needs type dispatch on a cast
    (invariant 2) it stops and reports.
- **~13:09 UTC, the translator:** B1-R's `10b311de6` (H2 essential and standard in the ENGINE lane) is merged into
  `claude/compat-translator-spike`; the two runs ((1) alone, the combined set) over the three H2 lanes are going.
- **~13:10 UTC, B1-R: a repo-wide CI blocker, not this program's.** Maven Central answers 404 to GitHub's runners for
  `h2-2.1.214.jar` (`h2_engine`, `tools/deps/jars_table.bzl`) on #124 (12:38, 13:06: parser_equivalence, core, stress)
  and #123 (12:28: checks), and for `httpcore-4.4.13.jar` on #121 (11:30). Every lane analysing a target that needs
  the jar fails before building; `main`'s last green run (03:26) would hit it now. The cause in the build:
  `tools/deps/jars.bzl`'s `_url` gives each `http_jar` one URL. **Sent to the product orchestrator and the CI-health
  session (13:20)** with a recommendation: a second Maven Central mirror in `_url`'s list
  (`maven-central.storage-download.googleapis.com/maven2/`), the sha256 unchanged and checked; vendoring the referee's
  jar can wait. Theirs to make and land; #123 and #124 re-run on it.
- **~13:11 UTC, the product orchestrator owns the Maven fix:** the CI-health session builds it on
  `claude/ci-maven-mirror` (from `main`): a CI-only Bazel downloader config that rewrites every repo1.maven.org fetch
  to try Central first and Google's mirror second, the same sha256 checks, no pin or lock change. It covers both
  `jars.bzl`'s `http_jar` and `rules_jvm_external` (better than a second URL in `_url`, which covers only the first),
  proven by forcing the first URL to fail. It lands on `main` on the user's word; #121, #123 and #124 then merge it or
  `main` to re-run.
- **~13:12 UTC, B2's task (1): D2's probe on H2** (B1-R's lane widened in B2's worktree to H2 essential, grammar,
  standard, unclassified and relation; baseline the same widened lane without the probe; H2 relation's baseline
  byte-identical to B1-R's pin).
  - **Verdicts:** 4 move, all BOTH_FAIL to LITE_FIXES (`rem`, `sqrt` in essential; `bitShiftLeft`/`bitShiftRight` by
    63 in standard). No SAME test moves; grammar, unclassified and relation do not move.
  - **Answers that match legend-engine on H2 and stop matching: 2, both S20** (`testFloatPlus`, `testFloatTimes`, as
    on DuckDB; only a Decimal round trip reproduces them). The plain set changes no H2 answer that matched
    legend-engine (`rem(5, 0)` raises on both; words only). **D2's total: 9 answers** (DuckDB: 4 plain set, 2 S20;
    pure/v1: 1; H2: 2 S20).
  - **3 legend-engine answers on H2 no guard reproduces, matched by lite neither today nor after:** `sqrt(-1.0)` NaN
    (Pure has no NaN; lite raises), `1->bitShiftLeft(63)` wraps to -9223372036854775808, `1->bitShiftRight(63)` 0.
    Lite's H2 dialect has no shift rendering (refused today). Outside D2; sent as evidence to the numbers worker's
    `claude/h2-dialect-gaps`.
  - **A correction in flight: `generatePlan` and the printers.** B2, by bytecode: under PURE_V1 every facet delegates
    to UPSTREAM, so a pure/v1 query containing a plan-text native (`toSQLString`, `planToString`, the executionPlan
    rows, the DDL natives) reaches `EnginePlans.ddlText` → `PureText.engineText` → `EngineStyleH2`
    (`BodyPlanner.prepare:311` → `facet(UpstreamPlans).ddlText`). Ordinary queries do not. This session's earlier
    "unchanged, checked" line (from reading `PureV1Routes.generatePlan`) is withdrawn to "being re-checked" in the
    plain plan. Asked of B2 first (13:25): a planted run, the count of recorded pure/v1 requests that take the path,
    whether an ordinary Studio generatePlan reaches it, and what such a request answers after A2. Expected: only a
    query that itself asks for SQL text, the same promise as CP-4.
- **~13:13 UTC, B1-R's cross-check of the 33, with a correction to this session's premise:** lite's own H2 PCT runs
  ONLY the relation suite (channel A `//pct:pct_h2` = `Test_LegendLite_H2_RelationFunctions_PCT`; channel B
  `_CHANNEL_B_LANES["h2"] = ["Relation"]`). H2's essential and standard suites had never run on lite before today's
  ENGINE lane, so there is no lite ledger to check against.
  - **Measured instead:** lite's own audience (the PCT default) on H2 essential and standard, a local scratch target:
    **all 33 fail there too**, with the same messages. None is a route difference; all are H2 dialect gaps.
  - **6 other tests differ by audience**, each BOTH_FAIL on the ENGINE route (lite reproduces legend-engine's defect
    there, as the audience intends) and passing on lite's own route: `testStart`, `testStartEnd` (substring's 1-based
    start), `testRemError`, `testSquareRootError` (the plain set), `testBitShiftLeft_MoreThan62Bits`,
    `testBitShiftRight_MoreThan62Bits`. These are exactly the rows A3 and D2 move.
  - **Lite's own route totals on H2:** essential 256 of 345, standard 115 of 205 (ENGINE route: 252, 113).
  - Into the record and #124's body with the #123 merge commit (one CI run).
  - **Sent to the product orchestrator (13:28):** a recommendation to add H2 essential and standard to lite's own PCT
    lanes (channels A and B), their known failures ledgered with the user's word; until then only #124's ENGINE lane
    runs those suites on H2.
- **~13:13 UTC, H12's addendum: stage A on H2's essential and standard ENGINE lanes** (`f9b010ac` on
  `claude/compat-translator-spike`, `h12/engine_lane_h2/`). **9 rows move, every one lite FAIL to PASS toward PCT's
  expected value; none the other way; no SAME row moves; no interaction.**
  - Essential, 7: change (1) alone 5 (`indexOf` `testSimple`, `testIndexOfOneElement`, `substring` `testStart`,
    `testStartEnd`: BOTH_FAIL to LITE_FIXES; `indexOf` `testFromIndex`: BOTH_FAIL to LITE_ANSWERS, legend-engine
    refuses the 3-argument form); combined only 2 (`testRemError`, `testSquareRootError`: the plain set).
  - Standard, 2: both 62-bit shift error tests (the plain set). Relation 0.
  - Every moved row is one legend-engine's manifest marks DEFECT or REFUSES. The two substring-keyed sorts that moved
    on DuckDB do not move on H2: lite refuses them there before and after (a pre-existing H2 refusal).
  - **So the whole dry run's ENGINE-route effect, all six DuckDB and H2 lanes: 22 rows, every one toward PCT's
    expected value** (DuckDB 13, H2 9). (Corrected ~13:55 by the reviewer's N1: stage A's own share is 12, 7 DuckDB
    and 5 H2; D2's removal, which lands in B4, is the other 10.)
- **~13:14 UTC, the product orchestrator:** H2 essential and standard join lite's own PCT lanes in both channels as
  step 0 of the H2 track (`claude/h2-dialect-gaps`), the known-failure list and the lane's minutes to the user first.
- **~13:16 UTC, the user: "Kill both now and restart on opus"** (the two spike sessions, H11 and H13). Done:
  - both interrupted and archived: H13 (`session_0195rQEoAgK9WB7AM5Cowp2f`, stopped mid-way through a guard-test run,
    "27 of 31"; its unpushed work after `dad98589` is lost) and H11 (`session_0195rQEjVtMXfZdcaQdweUzf`, stopped
    mid-way through the corpus half, "2 suites fail in measurement mode"; nothing pushed after `5c62ee85`);
  - restarted from their branch heads, xhigh, each with a continuation brief (the record so far, the remaining
    tasks, the same rules): **H11 continued `session_0195rQEnceykGKzTfRUo7XWp`** (the corpus half: the corpus on both
    databases with the Java printers, the 41 calendar tests as a `CSVJOIN` form, the judge-side composed prints by
    shape, the abstract-Date question); **H13 continued `session_0195rQEjNZoybxirC5uGba42`** (the six tests' three
    lowering sites, the database judge's 4 tests, the defect cell's count and size, the size words as a `Hearing`
    input, two pure/v1 golden requests drafted, the guarded files and the size estimate, the `lite_wire` shape check);
  - the host-parity worker and the judges session were given H13's new address.
- **~13:17 UTC, B1-R: #123 merged into #124 and the oracle re-pinned** (head `fa8294cca`; review item 2 closed).
  `d32495fc1` merges `claude/compat-s0` at `ef9e9bfa3`; `fa8294cca` teaches `GoldenCompareMain` #123's format (plan
  text compared from its own file; new pins `engine_<db>_plan_text.tsv`; core's export gains `pure_v1_plan_text.tsv`).
  - **The 15th recorded request, `raise.toOneOfEmpty`, asked of legend-engine:** lite answers 500 (`Raised:
    SIZE_NOT_MULTIPLICITY`), legend-engine 200 with a row. Another place legend-engine skips the check Pure makes
    (Q8's family). Lite already differs today, so D2 does not change it; it is a register row when the error-records
    slice words it.
  - H2's 38 golden lines against legend-engine: 5 the same, 33 differ, 2 extra (activities legend-engine reports where
    lite answered an error); recorded in slice R's record §8.
  - `//gates:local` plus `edge_net`, both lane pins, both battery halves and both oracle pins running; CI restarted on
    `fa8294cca` (the Maven 404 fix is not on it yet).
- **~13:25 UTC, THE USER RULED the goal, D1 and D2: "Okay agree with all"** (recorded in §1). The checkpoint's
  decisions table and the plain plan's §6 now say so.
- **~13:35 UTC, the final plan written fresh:** `docs/records/compat-final-plan-2026-11-19.md` (the goal; the design in
  four parts; the rulings it carries; stage A's four steps, each with what, done when, size, the user's words it needs
  and its builder; stage B's five with their prototypes and what is open; what is not in the program; the safeguards).
  The working plan is history. Sent to the independent reviewer to check before stage A goes to the user to ratify.
- **~13:26 UTC, B1's H9 (phase-1 homework)** (`claude/compat-b1-homework`, `H9-phase1-measures.md`; commits
  `277ab6792` to `867a20eb8`).
  - **0.4 (f):** of H2's 98 bare print sites, 95 at the literal root; 3 in a relation column (`testDayOfWeek_Relation`,
    an enum; the corpus's `testStrictDateProject`, a StrictDate; `testToSQLStringChar`, never executed); 0 in a map, a
    Pair or `^C`. Of the 22 composed or feeding sites, 0 over a Float, Decimal or DateTime. One Pair composed
    (`testPairCollectionToString`) is the one print the formatter's rule refuses.
  - **0.4 (g), G7 over the net:** 6 tests, all PCT essential, DuckDB and SQLite, channels A and B (24 outcome lines,
    all PASS today): the same six H13 found. Every value they print is a Pure literal; lite's lowering makes them look
    computed. Corpus, pure/v1, H2 relation, edge cases: 0.
  - **The operand census, all 5,017 raise renders in the net:** String 4,135, Integer 465, Float 36, Decimal 21 (no
    other literal kind). A literal under a cast in 100 renders (Cast(IntLit→DOUBLE) 36, Cast(StringLit→TEMPORAL_TEXT)
    32, Cast(FloatLit→DOUBLE) 32), plus INDEX_OUT_OF_BOUNDS's size. Truly computed: Call 217, Column 67, Cast over
    computed 32, ScalarSubquery 28. B1's proposal: a Cast over a literal is that literal (5 of 6 exact), and a literal
    list's size folded at planning (all 6). **Sent to H13 (13:40)** with two cautions: structural only (invariant 2),
    and the size as a type fact (a literal collection's multiplicity), never the compiler computing a value.
  - **0.4 (i), Floats in database-written JSON:** 0 databases round on DuckDB, H2 and SQLite (24 hard doubles each);
    SQLite writes 17 significant digits (5 of 24 not shortest); -0.0 lost by H2's and SQLite's storage, not their
    JSON. Postgres not measured: by reading, `Postgres.doubleJsonb` casts a whole double to numeric at 15 digits, a
    candidate to measure (sent to the product orchestrator).
  - **PR #123:** the lane split committed (`7d9929bf4`): `edge_net` 38 tests with no corpus; the corpus lanes +4 diff
    tests. Its run is going.
- **~13:27 UTC, H10's R-S131 counts (the judges session; `claude/compat-judges-spike` at `228a6a18`).** The edge
  rewrites `assertEquals` over an INCIDENTAL actual into `assertSameElements` at a door, the product judge's typed
  S131 off (the rendered text's kept).
  - **DuckDB corpus: 21 of 22 fixed** (values 14, cells 7, all three two-column cases). **H2: 3 of 4.** Left on both:
    `graphFetch::tests::union::propertyLevel::test6`, the JSON root a rewrite cannot reach (H6 §7.5).
  - Control (typed S131 off, no rewrite): exactly those 21 + 4 newly fail. With the rewrite: +1 fail per lane
    (test6), 0 new failures, 0 new passes; skipped rosters and both registers identical. The rewrite touches 448
    asserts in 374 tests, all verdicts kept (~440 per-assert ledger lines renamed).
  - **Ordering:** one DuckDB message moved (`sqlFunction::log::testFilter`, the S15 pair, worded as
    `assertSameElements`): **R-S15 must run before R-S131.**
  - `OrderView.INCIDENTAL` is reachable through public entries (`TypedQuery.body/context/withBody`,
    `ModelContext.findFunction`, `OrderView.of`, `TypedNativeCall`).
  - **STOP AND TELL: the corpus road has no door.** `PureTestRunner` types the body itself
    (`PureTestRunner.java:~349`, `TestExecution.planBody(Compiler.queryResolved(ctx, resolved), ...)`); the trial used
    a marked stand-in, `TestObserver.door`. S137's existing slot (`RelationCounting.counted`) cannot host it (no
    `ModelContext`). Options: (a) a door on the shared runner, one method and one call (the judges' recommendation);
    (b) widen `RelationCounting`'s signature (keeps a two-form alive); (c) an edge-owned copy of the run loop.
  - Correction: LITE/HOST's 462 byte-identical failures are 32 judge words and 426 lite typing refusals; the check
    exercises 9 of 25 record kinds, the other 16 checked statically (0 differences).
  - Revised sizes: 2.2b about 4.5 days (was 7), +1 to carry records to the listener; 2.2a 3.5; the door +0.25.
  - **This session (13:40):** the door is a new core hook, so it goes to the user. Asked the judges first: can all
    four rewrites run through it so S137's two-form is deleted in the same step (a net shrink); its exact place
    (`TestObserver`, which already carries the caller's options and `EdgeRun`; no default, as K6); other ordering
    constraints; and the cost of the no-hook alternative (the harness types each body with the public compiler and
    rewrites the PARSED body before the build, as H5's typed retarget did).
- **~13:29 UTC, H10's record committed** (`H10-judge-records-spike.md` at `b867f60b` on `claude/compat-judges-homework`;
  spike code `beacf4c9` and `228a6a18` on `claude/compat-judges-spike`, neither for merge). Additions beyond the
  counts: the stubs (§1: `AtIndex` carries its inner failure as text; `HostRules` kept only as the presenter's
  carrier; `Hearing.json` stays until R-S144; `EdgeHostWords.cellMultisetDiffers` still compares; core's test sources
  do not compile on the branch, 18 files, 12 of them guards, naming deleted types, no guard line touched); the door
  options (§5); sizes (§6: 2.2b about 4.5 days; removing S15 from the judge moves 2 DuckDB corpus tests until R-S15
  lands at a door, so 2.2b waits for the door or the two rows go to the user; 2.2b does not depend on 2.2a); lessons
  (§7: find the door first; thread the hearing, not a function; presenters as thin adapters; a failing fixture per
  record kind, since the lanes reach 9 of 25; measure the base's noise first). H6 corrected: `HostWords` has 27
  methods (25 kinds). The answers to this session's four door questions (13:28) crossed with this message; awaited.
- **~13:32 UTC, B2's A2 shape note** (`claude/census-b2` at `1e8e817ee`: `A2-shape-note.md`, `A2-roster.tsv`,
  `A2-probes/`).
  - **The deletion list, from the bytecode:** 4 whole files (`EngineStyleH2`, `EngineStyleDB2`,
    `EngineStyleComposite`, `EngineTextDialect`: 2,990 lines), a closed set instantiated only by the two `PureText`
    factories (`engineText` :298, `enginePlanText` :312). In-file: the two factories; `EdgeLoweringForms.engineSql`
    and `EngineShape` (:155-162, :186-272); `RelationalRootForm.apply` and `enumRawColumn` (:44-206;
    `primaryKeyColumns`, `stripQ`, `dedup` stay, the product's key rule at 9 sites). A correction: `apply` has a second
    caller, `Compiler.lowerResolved:962-964`, dead (every caller passes false), so the flag goes and two
    `PlannerSurfaceTest` signatures move. The plan-text family (`EnginePlans`, `PlanText`, `PlanEnumForm`,
    `InProtocol`, `PlanVariables`, `EngineParams`, `EngineSpans`, `EngineAllocations`, `PlanEnvelope`,
    `TestDataPlanText`) is referred to from outside the printers: only its printer seams go (17 call sites, each with
    its answer afterwards).
  - **Afterwards:** lite's own SQL where lite has a dialect, which widens `toSQLString` and plan text from H2, DB2 and
    Composite to all 12 lite dialects (a move toward E-R2); the 13 databases without a dialect refused by name at
    `Databases.dialect` (`Databases.java:95-97`). `toDDL`: `ddlText` makes both the printed text and the 3 executed
    setups, so PR 1 writes lite's H2 DDL (passed all 3 on both lanes) and leaves raw to PR 2.
  - **generatePlan, by bytecode** (rapid type analysis plus each style hook's key): it reaches the printers through 2
    hooks under PURE_V1 (the `UpstreamPlans` facet: plan-text natives, DDL natives, executionPlan reads,
    `BodyPlanner.prepare:311`; the `ActivityTexts` two-form: a query reading an execute's `activities[].sql`,
    `ResultEnvelopeSplice` → `PlanAllocations.activitySql`). With both cut, no path (26,945 nodes). So "generatePlan
    reaches none of the deleted code" holds for an ordinary query only. **This session's call:** that is CP-4's
    promise; PR 1 proves it with planted requests (an ordinary one byte-identical; one of each hook's shape answering
    lite's text). The plain plan's line is corrected.
  - **The roster, 155 tests:** H2 147 move with A2 today (64, 20, 52, 3, 2 and 6 by cause) plus the 3
    `testPlanForDateTimeConstantParameter*` once R4 lands, 150; DuckDB 152 (64, 22, 53, 4, 3, 6) plus 3, 155.
    Reconciled with H12: all 147 and 152 are text-attributed there, 0 mismatches.
  - **Guards:** 9 guard tests plus the rosters, the edge nets and the lean-SQL baseline, each with its lines and a
    dated justification ("slice 2.3a (A2) ... CP-4 ..."); all but the rosters and nets shrink; `DialectBoundaryTest`'s
    ratchet 17 to 14.
  - **Slices: two PRs, about 5 days** (PR 1, 2 to 3 days: the printers out and an adapter in, every ruled move; PR 2,
    about 2: the family's spelling-only code, nothing further moving). Accepted (13:45); the final plan's A2 row
    rewritten from it.
- **~13:33 UTC, B1-R on the Maven 404:** also from the `rules_jvm_external` pools (`rhino-1.7.15.jar` from
  @maven_teavm on `fa8294cca`'s linux/product; #121's `httpcore-4.4.13.jar` from @maven_runner), not only `jars.bzl`.
  #124's lanes hit: parser_equivalence, core, stress, corpus_h2, product. Already covered by the CI-health session's
  downloader config (both kinds of fetch); told B1-R.
- **~13:34 UTC, H13 continued (`session_0195rQEjNZoybxirC5uGba42`): tasks 1 and 2 done** (`538ab17d`, §11-12).
  - **The whole net now differs nowhere it should not:** channel A outcomes 0 on every lane (was 6 + 6); channel B 0
    of 1,249 (was 10); pure/v1 0 of 28; channel A answers only the 2 known Relation lines per database (the quoted SQL
    holds the site token; rows the same). Raises worded after the run: 26, all exact, 0 withheld.
  - **The six tests pass** on DuckDB and SQLite, both channels: 33 lines in `lowering/`, none in `sql/` or a dialect,
    no hook, no value read at run, no cast inspected. `sqrt`: the raise gets the argument as lowered, not the rule's
    own cast. Date parts: a date literal's text as written, from the same typed date node the rule already reads.
    `at`: the size as a literal where the typed multiplicity is exactly [k] (reading an annotation, invariant 2's
    MAY), else the length. The formatter prints the date as legend-pure's `DateFormat.append` does (read at the pin).
    One side effect toward legend-pure, outside PCT: `%2017-01-01T10:30->second()`'s message now prints
    '2017-01-01T10:30+0000' (main: the database's '2017-01-01 10:30:00').
  - **Not covered:** SIZE_NOT_MULTIPLICITY's size is built by the dialect (`AnsiSqlRenderer.checkedOne` from
    `CheckedOne`, no typed spec); stating it needs a MIR field (12 core edge tests, no PCT test). Open for the real
    slice: a typed `Operand.Date` (a MIR date literal, or typed operands on `RaiseText`).
  - **The database judge's 4 tests:** +15/-4 lines in two product files, no hook, no new member (`DatabaseAsserts`
    binds the planned failure's message instead of planting it; `TestRun.raisingVerdict` raises it at the verdict's
    turn, as `AssertErrorRuns` does). All 4 pass under both judges. Need not wait for the judges' 2.2a; checked against
    H10's `Hearing.caughtMessage`: the same shapes.
  - **So B1's prototype now holds every pinned message on every lane of the net.**
- **~13:34 UTC, the product orchestrator's register numbers, corrected:** S371 (#121, Integer over DECIMAL), S372 (the
  warehouse's file-load refusal, proposed, pending the user), S378 (`currentUser` 405, renumbered in `91f8505c`), S379
  (the pivot count, "Pure's null, by the user's 13:25 ruling"); S373-S377 unused. **Asked (13:50):** the user's 13:25
  ruling here named the goal, D1 and D2, not the pivot count; if S379 is derived from the goal, it still goes to the
  user case by case.
- **~13:35 UTC, the product orchestrator on S379:** the user had not ruled the pivot count case; it is now posted to
  the user case by case (the expression; Pure's `applyPivot` null; legend-engine's database 0; lite's), recommending
  (a). S379 reads "proposed, under the goal of ~13:25; the user's ruling of this case pending"; nothing lands before.
- **~13:36 UTC, the independent reviewer's check of the final plan** (`compat-final-plan-review-2026-11-19.md` on
  `claude/compat-review` at `fd88b9c8`; it read the final plan at `0a9303e5`, H12 and its TSVs, H6, `main`'s code and
  legend-engine 4.145.0's pinned source). **12 findings: 5 must change, 4 should change, 3 minor. Verdict: not yet,
  but close; every must-fix is a text change with no new measurement.**
  - **M1, the detector's quiet case is unsound for `indexOf`:** legend-engine's corrected option fixes `substring` only
    (`pureToSQLQuery.pure:4662` reads `state.correctSubstringIndexing`; `indexOf_String_1__String_1__Integer_1_` maps
    to `processDynaFunction` unconditionally, `:10644`; H2's `LOCATE`). **Verified here** at the pin. Fixed: `indexOf`
    always warns; a planted request with the option and an `indexOf` in A3's tests.
  - **M2, A3's "the corpus loses exactly its 4" fails in either order:** by H12's TSVs, A2 then A3 gives A3 7 per
    database (4 + the 3 hidden text tests), A2 147 H2 and 152 DuckDB; A3 then A2 gives 8 and 9, then 146 and 150.
    Fixed: the order is A1, A2, A3, A4, each roster by name.
  - **M3, lite's own PCT sets legend-engine's corrected-substring option on every query** (`PctPlanRoute.java:45-46`,
    `ChannelB.java:393`, `PctExecuteNative.java:140`; "the user's ruling of 2026-09-12, a written deviation from
    upstream's PCT adapter, which sets none"). **Verified here.** So lite's PCT never saw A3's `substring` half (why H12
    moved only the 3 `indexOf` rows). Fixed: A3 drops the option from lite's adapter after the switch; proof channels
    A and B unchanged without it; the reversal of the 2026-09-12 deviation and the 2026-11-07 "our tests should use the
    option" are on A3's user's-word list.
  - **M4, A2 restated the withdrawn claim and was unsized:** already rewritten from B2's shape note at `1ef9a9fc`
    (after the commit the reviewer read). Added: what a legend-engine request asking for SQL text answers after A2
    (lite's own SQL, CP-4), as a golden line and a register row.
  - **M5, A4 lost its gate:** restored (the product's 5 judge-parity fixes and the PCT-under-both-judges lane first);
    both corpus rosters byte-identical across A4; S131's narrowing does not ride in it.
  - **S1:** row 6's strands (`isNumeric`, `parseDecimal`, `parseJson`) land in B4 with their moved tests (D1 covers
    them). **S2, the detector:** its reach follows qualified properties and the runtime's mapping; a literal start of
    0 on H2 exempt if measured equal; the header a done-when with a bounded value; **warn is right as the default only
    with a one-time report at project load** (the bodies, the legend-engine query store's saved queries, qualified
    properties and mapping transforms; lite's own store too), else refuse by default. Folded in as §4a, with that
    recommendation put to the user. **S3:** the plain plan's row 1 was wrong ("a flag PCT always sets": legend-engine's
    adapter sets none, `pct_relational.pure:108`, and its own H2 manifest lists `testStart`, `testStartEnd`,
    `testSimple` as expected failures) and stale ("silent"). Corrected. **S4:** stage A is about 3 weeks (2 at best, 4
    if the product's gates slip): 11 to 15 working days for one builder plus about a day per PR. Corrected.
  - **N1:** the "22" is stage A's 12 (7 DuckDB, 5 H2) plus D2's 10 (B4); split in the log and both plans. **N2:** H6
    §6.3 at `1062df31` still says the referee has no session of its own; asked of the judges session to correct.
    **N3:** the rulebook amendment rides with A3's PR; B5 keeps refusing every other legend-engine flag by name; query
    cost on users' warehouses added to "not measured".
  - All folded into the final plan, the plain plan and this log (~13:55).
- **~13:36 UTC, B1: PR #123's Windows lines** (run 44529766966, the windows-lane-edge_net artifact). **Case (a): every
  moved line is the last digit or two of a Double the database's own maths computed;** no order, locale, path or line
  ending moved. 30 lines, over this session's ~20, so B1 stopped and built no overlay.
  - DuckDB essential 2 (`testCubeRoot` 002 3.0000000000000004 → 3.0; 003 0.1 → 0.09999999999999999); SQLite essential 2
    (`testComplexPow` 002 182.88729271224375 → ...372; `testCubeRoot` 003 0.10000000000000002 → 0.1); DuckDB standard
    13 and the SAME 13 with identical values on SQLite standard (sinh, cosh, tanh: `testCosH_Identities`,
    `testSinH_*`, `testTanH_*`).
  - **The finding:** DuckDB and SQLite take sinh, cosh, tanh, cbrt and pow from the platform's C maths library (glibc
    on Linux, the MSVC runtime on Windows); their hyperbolic results are identical to each other on each OS; cbrt
    differs in opposite directions on the two databases. H2 (Java maths), every relation and grammar suite, channel B,
    the corpus, pure/v1 and the edge cases matched on Windows. macOS still running; Linux arm runs no edge_net lane.
  - **Times:** Windows edge_net about 26 min this run (with the corpus pass; the split removes it); `main`'s longest
    lanes are core (Linux 33.5, Windows 23.4, macOS 24.1) and macOS datacube (26.7).
  - **This session's call (14:00):** a keyed overlay, one file per OS, 17 rows (suite, test, case, databases, function,
    the Linux value, that OS's value), every digit pinned; applied only on its OS; the net fails on an unused row or a
    row whose Linux value differs from the golden; any other difference still fails. Not per-OS copies of whole
    goldens, not fewer digits, not Linux-only. macOS the same rule (stop if over ~20 facts or anything but last-place
    maths). The overlay files are new pinned files, named in the PR body for the user's word; the finding goes in the PR
    body and the net's README.
- **~13:37 UTC, the judges' answers (H10 §9 at `c1aae56f` on `claude/compat-judges-homework`).**
  - **The referee:** H6 §6.3 corrected (`ce94cda6`). `ReplayReferee.verifyAuto` (:353-367) replays on the body's
    session only if `TestBackends.of(session) == TestBackends.referee()` (:362), which never holds, so
    `verifyOnSession` is dead; every golden replays into legend-engine's isolated 2.1.214 (`ReplayReferee:200-201`,
    `EngineH2Referee.openReferee` :63-72, MODE=LEGACY, DEFAULT_NULL_ORDERING=HIGH). A build may delete it.
  - **The 7 cell cases:** the same verdicts under row tuples and pooled `assertSameElements`; corpus-wide no assert
    goes FAIL to PASS, so no row-tuple compare is needed at the edge. The JSON root becomes the edge's Json whole-kind
    rule; test6 passes again.
  - **S131's narrowing** is its own step with the door rewrite, in neither 2.2a nor 2.2b's refactor.
  - **Size words:** `VerdictRecord.SizeNotMultiplicity(size, multiplicity)` presented through `Hearing.present` at the
    3 product raise sites; deletes `SideShape.sizeWords`, `TestPlan.answerSizeWords`, `EngineTestText`/
    `EngineTextGates.sizeWords`, `JudgePlans.sizeWords`, `TestDoor.answerSizeWords`; about 0.25 day in 2.2b.
  - **H13:** the shapes meet (`caughtMessage(RuntimeException)` is H13's `HostWords.caught`); whoever lands second
    drops `Appeals`' `siteWords` lookup; a typed parameter (Site | Planned | Message) suggested to both.
  - **H11:** make the relation text a PRODUCT judgment kind, `HostJudgment.Printed(side, PrintCall)`, planned from the
    program's own `toString(rel[, false])` call at a side's root (a fact of the typed tree, like `OrderView`), one
    `Hearing.print(Tabular, PrintCall)`; the edge's print is H11's printer, the product's refuses by name; no edge
    record, `EdgeCodec` tag or engine-text claim in the plan. Done-when 3 amended: "a plan may name the program's own
    presentation call at a side's root (toString, toCSV, as Rendered names its form today); how an audience spells it
    is the run's". Order 2.2a, 2.2b, 1.3. **Accepted as B2's design**, for the reviewer at stage B.
  - **The corpus door:** (a) an edge-owned door would copy about 230 of the runner's 288 code lines (sessions per
    package, setup inheritance, the private workspace, verdict bookkeeping), product surface for `<<test.Test>>` users
    too: two copies drift. (b) a body preparer is a hook by another name (its only implementation compatibility; a JDK
    `UnaryOperator` escapes `HookLambdasTest`; its identity answer fails `HookRulingTest`'s criterion (b)).
    **Recommended, a third shape with no callback:** `PureTestRunner.prepare(TestCase)` returns a public `Prepared`
    (the typed query and its facts), `run(Prepared)` runs it, `Prepared.withBody` re-states the facts; the spec harness
    composes prepare, the edge's rewrites, run, as ChannelB does. About 30 lines of public surface, one runner, no hook.
    **This session's call (14:00): taken**, on condition that all four rewrites run there (R-S15 first) and S137's
    `RelationCounting` two-form is deleted in the same step. It is not a hook or a switch, so it does not need the
    user's ruling; the user is told. The final plan's B3 row says so.
  - A5 (`session_0195rQEn49s2BEcy7BVaoAL4`) has its answers.
- **~13:38 UTC, the host-parity worker:** confirmed, after the 404/405 change (`86edc90e`, `3390f2e3`): the lite route's
  helper asked for `/api/pure/v1/nope` answers lite's 404 (`notFound`); a client on the legend-engine route still gets
  legend-engine's envelope (`PureV1Routes.route`, 404 `{"code":-1,...,"status":"error"}`, asserted by `PureV1ApiTest`,
  `//core:core_tests_server` green). No edge-net line moved; `//:generated` green. `no_such_route` moved to MISSING in
  the four adapters.
- **~13:38 UTC, B1:** the lane split is pushed (#123 head `7d9929bf4`; a new CI run). `//gates:edge_net` holds 38 tests
  and no corpus test; `corpus_duckdb` and `corpus_h2` each gained the 2 edge-ledger diff tests; the lane guard's query
  is empty; the PR body names each gates-list move, dated. The push cancelled macOS edge_net in the old run; this run's
  answers the same question.
- **~13:38 UTC, THE INDEPENDENT REVIEWER'S RE-CHECK of `51888e28`: M1-M5 met as meant; S1-S4 and N1-N3 folded in;
  stage A READY for the user to ratify** (`compat-final-plan-review-2026-11-19.md` §6 at `3736b2b7`). One decision is
  the user's at ratification: the detector's default (the reviewer's view: warn with the load-time report; refuse by
  default if the report is not built).
- **~13:39 UTC, the judges' answers to the four door questions** (H10 §9.8), crossing with this session's 13:40 call.
  (1) All four rewrites can run through one door and `RelationCounting` is deleted in the same step (today's slot runs
  on the typed body at `TestPlanner.lifted` :225-227, before G½; R-S137's walk recurses the whole tree, so nothing is
  lost; the product keeps lite's refusal; `assertEmpty($rel)`'s refusal moves to the door; every edge-style road applies
  it; deleted: `RelationCounting`, `UpstreamRelationCounting`, the `PlanStyleFacets` entry :40, `PureText`'s facet
  answer :159-161, `TestPlanner:227`'s two-form call, their guard rows): a net shrink. (2) Type once (today the runner
  types each body twice, :289 and :349) and feed the same `TypedQuery` to facts and planning; apply to setups too.
  (3) R-S15 before R-S131, and R-S15 order-free over an incidental actual. (4) The parse-level alternative reaches
  S131's shape by source positions, about 1 day more, a third typing, fragile, and cannot delete `RelationCounting`
  alone. **The call stands (13:45):** the third shape (prepare/run split, no callback), with all of §9.8's conditions
  carried over to it, written into §9.8 by the judges for the builder.
- **~13:40 UTC, the judges: R-S137 is a rewrite of the prepared body** (`UpstreamRelationCounting.countedAsOne` is
  already a typed-body walk), so the split's condition holds and `RelationCounting` (`PlanStyleFacets:40`,
  `PureText:159-161`, `TestPlanner.java:227`, `UpstreamRelationCounting`) is deleted in the same step;
  `RelationCountAsserts.REFUSED` stays as lite's own refusal. One case is a refusal, `assertEmpty($rel)`
  (`UpstreamRelationCounting.java:71`): the harness raises it before `run(Prepared)` and reports the test's FAIL with
  the runner's own reason text (`"ModelException: " + PureTestRunner.whole(message)`). The one difference: it is
  reported before the package's setups run; no corpus FAIL line on either lane carries that text. H10 done.
- **~13:42 UTC, the judges: H10 §9.8 rewritten as the builder's six conditions for the prepare/run split**
  (`claude/compat-judges-homework`, after `91b93c5c`): (1) type once, one `TypedQuery` for facts and planning;
  (2) setups typed once and carried in `Prepared` (`withSetup`), run through `runFixture` as today; (3) R-S15, then
  R-S131 (R-S15 pairs by sort over an incidental actual), then R-S137 and R-S144; (4) **a correction:** the
  `assertEmpty($rel)` refusal is carried as data on the prepared test (`Prepared.refusedBy(e)`) and raised by
  `run(Prepared)` after the setups, where `planBody` would be, inside the same try, so the runner's same catch gives the
  same Result and observer events (pinned by `RelationCountAssertsEdgeTest`; no lane meets it); (5) every edge-style
  road applies the four in order (`MinimalCorpus` and `LeanSqlCorpusTest` via the split; ChannelB on its
  `TypedQuery`; core's edge tests); (6) `RelationCounting` deleted in the same step, its guard rows
  (`HookLambdasTest` TWO_FORM_CALLERS, `HookRulingTest`, `JavaEvalLedgerTest`, any `PlanStyleSurfaceTest` row) moved
  with the user's word; `RelationCountAsserts.REFUSED` stays. H10 is done.
- **~13:43 UTC, the translator's A3 shape note** (`A3-shape-note.md` at `13212fb2` on `claude/compat-translator-spike`;
  no build, no PR, no pin moved).
  - **PRs: 2.** PR1 the frontends and the dynaFunction split (about 1.5 days); PR2 the core switch and the detector
    (about 3 days, plus 0.5 to 1 for the out-of-range raise). About 5 days.
  - **Moves:** 17 lite tests, all named (9 old-meaning pins in PR2; 8 text goldens in PR1, 7 under option (a)); **63
    PCT known-failure rows** leave (S134's 3 tests are ledgered per lane: 11 channel A classes, 33; 10 channel B lanes,
    30; measured on DuckDB, the rest predicted from identical texts); 9 edge-net golden lines (3 answer lines each in
    DuckDB's and SQLite's essential answers, 3 channel B outcomes ERROR to PASS; read from the pins); 12 ENGINE-lane
    rows if #124 lands first; **31 corpus guarded lines** (H5 V2: the H2 fail roster +8, DuckDB +9, each unordered
    register +6/-1); `NativeCatalogGovernanceTest` INTERNAL_DESUGAR 38 to 40 (option (a) only); register S130 and S134
    rewritten plus 1 new row. New core hooks 0; pure/v1 net lines 0.
  - **PR1, option (a), recommended and TAKEN:** two lite-internal natives keeping SQL's meaning verbatim for the
    frontends (`meta::legend::lite::sqlSubstring`, `sqlPosition`), lowering to the same SQL main emits: PR1 lands alone
    with byte-identical SQL, the frontends are immune to the raise, the detector never flags `#Cypher`/`#SQL`/`#IQ`
    islands. Option (b), the spike's shift, would force one PR, make SQL less lean and turn every island's substring
    into a false positive.
  - **FeatureRules:** javadoc only (the plain rule becomes `CORRECTED_SUBSTRING`, so the flag's registration is a
    no-op; removing it would make `requireConsumed` refuse a flag PCT and the corpus set). `LEGACY_SQL_NULL_UNSAFE_EQUALS`
    and `Lowerer.withFeatures` untouched; the rest is B5's.
  - **The detector:** `edge/PureMeaningCheck`, from `PureV1Routes` generatePlan and execute (Arrow included); reads
    `TypedQuery.body()`, user bodies via `specs().compile`, positions via `TypedNativeCall.pos()`, the flag via
    `TypedFrom.context().features()` and `ExecutionContext.treeFeatures`, all public; header
    `x-legend-lite-pure-meaning` (plus `Access-Control-Expose-Headers`); one `WARNING:` line on stderr with no query
    text; setting `LEGEND_LITE_PURE_MEANING=warn|refuse` in `EdgeServer.UPSTREAM` (any other value stops the start);
    refuse answers 500 COMPILATION in legend-engine's envelope, "unverified" refused too. H5's 40 untraceable calls are
    gone at this door; what remains is reported "unverified", never clean.
  - **For the user's word:** (a) quiet under the option for `substring` only (`indexOf` has no flag,
    `pureToSQLQuery.pure:10644`); (b) **the raise: legend-pure raises on start < 0, end > length and end < start**
    (`SubString.java:56,61`; `CompiledSupport.java:1546-1556`, read at the pin), not only a negative start; (c) the
    host-side rule assumes legend-engine plans a store-free `let` as a platform node: to be asked of the engine runner
    at the slice; (d) the test-data routes run queries without the detector; (e) a one-time census of a deployed store.
  - **This session's calls (14:05):** option (a) taken; the three raise conditions follow legend-pure, one register row;
    the detector covers the test-data routes too. **Asked to add:** the fixed order A1-A4 (A3's corpus roster 7 per
    database, with the 3 hidden text tests); the load-time report (design, cost on the corpus's 11,168 elements,
    tests, size); the reach through qualified properties and the runtime's mapping; the H2 literal-0 measurement; and a
    clarification of "DuckDB's 9th corpus move is the cross-kind != product defect (+8 if it is fixed first)". The final
    plan's A3 row and §4a are updated.
- **~13:44 UTC, B2's task (3): B5's ratchet, designed and prototyped, not turned on** (`claude/census-b2` at
  `c702d6df7`; `B5-ratchet-design.md`). `tools/meaning/StrandRatchet.java` and `//tools/guards:strand_ratchet_test`
  measure 27 things over `//core:product_jars`, the edge excluded: `PlanStyle` 49 parameters, 4 fields, 50 calls, 112
  carrying calls, 36 classes; `LoweringForms` 6/1/25/11/10; `PlanStyleFacets` 21 kinds, 0 product reads, 16 facet and
  16 two-form call sites; `Feature` 20 classes, 62 generic signatures, 4 member loads, 12 flag-field reads. Every
  measure equals H8 (a): 0 breaches. Shrink-only and exact, in `shrink_only.py`'s shape. **The bite:** a class
  synthesized in memory (`java.lang.classfile`, JDK 25) adding one use reports 7 breaches, each naming the plant. 2
  tests, 2.6-3.0 s. **The census ratchet:** `meaning_census_test` (in `//gates:checks`) gains report-only
  `RATCHET_PINS = {a: 142, p: 2}` and a planted-hit test; 3 tests in 11.3 s. Proposed gate `//gates:checks` for both;
  switching on is B5's, on the user's word. **The one limit** (it counts sites, not identities, so a strand moving
  sites passes): this session's call (14:10), commit each measure's site list beside its pin, landing with the ratchet.
  All of B2's homework is done; its next piece (14:10): a draft of the "differences from legend-engine" page.
- **~13:45 UTC, the product orchestrator: the user (~13:40, in its session) told it to stop everything that is not
  compatibility work; all seven product tracks are pausing**, each committing "WIP (paused 2026-11-19)" on its own
  branch. Four paused things this program depends on:
  1. **the Maven mirror fix** (`f183b784a`, parent `main` `b75d6c73a`; on `origin/claude/ci-health` as merged
     `05368270f`): a CI-only downloader config, proven with a forced failure; unblocks #123, #124 and `main`; needs a
     branch and the user's merge word. **Gates A1.**
  2. **the window-in-a-list lowering** (`session_0195rQEjkEZDfj2GjXCAhyJr`, `claude/window-list-lowering`). **Gates B2.**
  3. **the PCT-under-both-judges lane (C3)**, in the judges track (`session_0195rQEn49s2BEcy7BVaoAL4`,
     `claude/parity-a5`), with the 5 judge-parity fixes. **Gates A1's third net and A4.**
  4. **the cross-kind `!=` fix**, done at `63421e0a` on `claude/hosts-followups`, not landed. **Gates A3.**
  The product orchestrator stays reachable for questions. **Put to the user (14:15):** the program takes over 1, 3 and 4
  now as stage-A prerequisites (and 2 at stage B), each landing on the user's word.
- **~13:48 UTC, H11 continued: the corpus half's interim** (`claude/compat-relation-text-spike`, census at `ebb99c01`).
  Java printers on (toString, toCSV, toCSV→replace as the CSVJOIN form, a TDS's csv), the SQL builder off, against a
  baseline of the same head: the baseline is green with every roster equal (so the stopped session's "2 suites fail"
  was its own measurement run).
  - **DuckDB:** 282 comparisons, 253 byte-equal, 18 held by the ruled comparison (S131/S132), 11 differ, all 11 on
    `main` too (10 accepted divergences, 1 failing on both): **0 differences caused by the printer.** **H2:** 282, 270
    byte-equal, 1 ruled, the same 11.
  - **Moves:** both lanes 12 pass to fail; H2 also 3 fail to pass (calendar CSVJOIN `testMtdValue`,
    `testSubclassValue`; `testGroupByWithFilterFunction_noDatePath`: the golden spells a whole Float `3.0`, the SQL
    builder printed `3`, the Java printer prints upstream's). The 12 losses all reached the SQL builder, which is off:
    11 `assertSameElements(..., toCSV(...))` (testGroupBy x5, resultSourcing x6) and 1 `testEnumInRelation`'s
    `cast(@TDS<Any>).csv` (a missed cast receiver, fixed, re-running).
  - **Stop-and-tell, no hook added:** the edge's hook (`EngineTestText` via `AssertVerdicts.planArm`) is consulted only
    for `assertEquals`/`assertNotEquals`; `assertSameElements` over a print goes straight to the product's
    `renderedPlan`. 22 corpus sides (11 run). **This session's answer (14:20):** the judges' `HostJudgment.Printed`
    design (H10 §9.6, B2's) closes it if `Printed` is planned for every assert family over a printed side; sent to both
    sessions as a requirement. H11 runs a hybrid run to show nothing else moves, then writes the record.
  - **The census:** PCT's judge side 431 prints, all relation toString at a side's root, 0 composed. The corpus's 980
    assert sides in 828 tests: relation texts at the root 455 (toString 253, toCSV 160, csv 1), plus one composed
    shape, `toCSV->replace('\n', sep)`, 41 tests, covered by the CSVJOIN form; the other 525 are cell values the test
    composes over `.rows`, not relation texts. Only 1 corpus test reaches an edge value text in lowering.
  - **Calendar 41 per lane:** DuckDB 20 byte-equal + 14 ruled + 7 accepted divergences; H2 33 + 1 + 7.
  - **Abstract Date at midnight:** no corpus `toCSV` golden spells a time of day; upstream reads by the column's type
    (legend-pure `ResultSetValueHandlers`: TIMESTAMP → DateTime with nine sub-second digits, hasHour even at midnight;
    DATE → StrictDate), so the SQL builder's midnight rule is not upstream's; no cell where the two disagree.
- **~13:48 UTC, B2 catching up** (`claude/census-b2` at `707ddec22`): (1) D2's probe re-run on B1-R's own H2 lanes at
  `10b311dee` against its pins, (j.5): identical, 4 tests BOTH_FAIL to LITE_FIXES (rem, sqrt, both 63-bit shifts), no
  SAME test, relation 0; nothing pushed to B1-R's branch. (2) The numbers worker has the H2 shift and NaN evidence
  (H2's functions are LSHIFT and RSHIFT). (3) A2's note (`bbb100d1b`): **`PureMessages.sentence` has 13 callers at
  `main`** (`EngineStyleH2.raiseMessage:415` and 12 `PureText` dialect passes' interim wording), so it is not A2's: it
  goes in B1, after A2 deletes `raiseMessage` with its file. Corrected in the final plan (A2 and B1 rows). **0 of the
  golden net's 15 requests take either printer path**, so an ordinary Studio generate plan never reaches the printers;
  A2 unchanged; PR 1's planted requests are in the note. B2's next task (the differences page draft) was already sent.
- **~13:49 UTC, B1: the Windows overlay is pushed** (#123 head `e7764c9db`, on the split `7d9929bf4`; one CI run
  carries both). `pct/src/test/resources/edge/overlay_windows.tsv`, 17 rows (sinh/cosh/tanh 13 on DuckDB and SQLite,
  cbrt 3, pow 1); `tools/edgenet/PlatformOverlay` rewrites a row's line from its OS's value to Linux's before the diff,
  fails on a stale row or another value, and on every other OS checks each row's Linux value against the recording;
  any unnamed difference still fails. Plants in `//tools/guards:edge_normalize_test` (now 13 tests). `//gates:edge_net`,
  the plants and `//tools/guards:all` green on Linux. The finding is in the PR body and a new `tools/edgenet/README.md`;
  the overlay file and its BUILD wiring are named as a new pinned file for the user's word. macOS decides on this run.
- **~13:52 UTC, B2: the differences page drafted** (`differences-page-draft.md` on `claude/census-b2` at `d13d1d4ba`,
  about 1,800 words; register ids only in each entry's last line). **11 entries** (10 measured, 1 read: the feature
  flags); by when: today 4 (`toOne` of empty, error words, lite answering where legend-engine refuses, the Windows last
  digits), stage A 2 (`substring`/`indexOf` with the detector; lite's own SQL text), stage B 5 (unchecked maths as a
  6-row table with each guard, per database; Float literals with "write `1.1D` or `toDecimal()`"; `isNumeric`/
  `parseDecimal`/`parseJson`; H2's regular mode; feature flags). A gaps section (the 33 H2 functions; H2's NaN and shift
  wrap; what is not supported yet) and a "what is the same" section (6 points).
  - **New:** `toOne()` of an empty collection answers 200 with null on legend-engine (H2 and DuckDB, the oracle); lite
    raises; `$x->first()` reproduces legend-engine's. Pure's Decimal literal is `1.1D` (legend-pure's `M3CoreLexer`).
  - **Rows missing from the register:** `toOne` of empty, `parseJson`, CP-4, D2, the blanket "lite answers where
    legend-engine refuses". **This session's call (14:25):** each lands with the step that creates its difference (A2
    the CP-4 row; A3 the `substring`/`indexOf` rows plus the two that exist today, `toOne` and E-R2's blanket row, with
    the page; B4 D2's and `parseJson`'s). Written into the final plan. B2 waits for stage A's ratification (A2's
    proposed builder).
- **~14:30 UTC, THE USER: "Yes take over the paused pieces we need and/or fold them into existing sessions"**
  (recorded in §1). Done at once (14:35), each folded into the session that already holds its context:
  - **the Maven mirror fix** → the CI-health session (`session_0195rQEmJ9D6cn7yjYczb2bi`), resumed for that only: a
    fresh `claude/compat-ci-maven-mirror` from `main`, `f183b784a` cherry-picked (CI-only: `.bazelrc`,
    `tools/ci/BUILD.bazel`, `tools/ci/downloader.cfg`), a PR, CI on every OS;
  - **C3 and the 5 judge-parity fixes (and `firstDayOfWeek`, `d4f6b524`)** → the A5 session
    (`session_0195rQEn49s2BEcy7BVaoAL4`), resumed for those only, first a short landing plan (what remains, its base,
    PRs, the pins each moves);
  - **the cross-kind `!=` fix** → the translator session (A3's proposed builder): a fresh `claude/compat-cross-kind-ne`
    from `main`, `63421e0a` cherry-picked (+95 lines, 3 files), the local gate, the corpus and PCT moves, a PR;
  - **the window-in-a-list lowering** stays paused until stage B;
  - the product orchestrator told, and asked not to resume those sessions in parallel.
  The user also asked what the detector's default is and what ratifying stage A means concretely (answered in chat).
- **~14:06 UTC, B1-R: PR #124 at `fa8294cca`.** Local gate green (623 pass, 1 skipped; 12 fail only because initdb
  refuses root here, and pass as a non-root user). CI run 44531926654: **Linux** green on pct_duckdb, pct_h2, edge_net,
  pct_channel_b, checks, stress, parser_equivalence, corpus_duckdb, datacube, sdlc, warehouse, pct_postgres; red only on
  Maven 404s (ui `duckdb_jdbc-1.5.5.1.jar`, product `rhino-1.7.15.jar`, corpus_h2 `h2-2.1.214.jar`); core running.
  **Windows** green on 14 lanes (the batteries' path fix proven), red on edge_net (#123's float digits; the overlay at
  `e7764c9db` not yet proven); no 404s. **macOS** 8 green so far. Linux arm warehouse green. **Times:** Linux pct_duckdb
  8.3 min (the five DuckDB ENGINE lanes about 2.5 min beside PCT; batteries about 7 s); pct_h2 7.1 min (was 5.6; the
  three H2 lanes about 1.7 min); Windows pct_duckdb 9.9 min, pct_h2 5.2 min; macOS pct_h2 6.0 min. Next: one merge of
  B1's head (and the Maven mirror once on `main`), then the final per-OS results.
- **~14:07 UTC, the product orchestrator on the taken-over pieces:** (1) Maven: `f183b784a` is a single commit on `main`
  `b75d6c73a`, so land it from its own branch (as briefed). (3) `claude/parity-a5` at `f42f74444` holds much more than
  C3 (Q3b; J3 with `DatabaseJudge` 852→854; `testDateFromHour`; `firstDayOfWeek` on H2; F4's pairings; JUDGES-8 with
  `HostJudgments` 679→680; in the WIP head A6 and the Date rulings 2 and 3). **Of the five parity bugs only
  `testDateFromHour` is fixed; the four `AdjustBy*BigNumber` are designed, not started. C3 itself is not started** (its
  `JudgeMatrixTest` is in the WIP, not yet shown to fail on a plant). The WIP's two reds: `AssertVerdicts` 1466 > 1465
  (a +1 the product orchestrator approved at ~13:12 under the user's delegation) and `SqlFnSortedTest`. So A4's gate is
  further out than the plan assumed; the A5 session's landing plan will size it. (4) `63421e0a`: cherry-pick as is; it
  shrinks a lean-SQL key (13366 → 13258, no answer change), left unpinned. The paused state of every track:
  `docs/records/pr118-audit-2026-11-18/FOLLOW_UPS.md` §10 on `claude/refactor-code-quality-review-9fqhug` (`e16cdefec`).
- **~14:07 UTC, the translator:** it does the cross-kind fix's local work, but holds the push and PR until the user
  gives the word in its own session (its brief said "No PR"); it will ask the user directly. Its A3 measurement run X
  (A2 + A3) confirms the prediction: fail rosters 430 (H2) and 267 (DuckDB), exactly `main` + A2 + A3 by H12.
- **~14:08 UTC, the product orchestrator confirmed the resumes:** the A5 session resumes for C3's lane, the five parity
  bugs and `firstDayOfWeek` only; the CI-health session for the Maven mirror only (`claude/compat-ci-maven-mirror` and
  its PR). Both take direction from and report to this session; everything else on their branches stays paused.
- **~14:09 UTC, A5's landing plan** (builds nothing until answered). State from `claude/parity-a5` at `f42f74444` (main
  `b75d6c73a` plus 9 commits; PR #118 and its fix round are on `main`; nothing of the branch is): `testDateFromHour`
  DONE (`6932aa2c1`, 51 lines, all three judges give legend-pure's verdict); `firstDayOfWeek` on H2 DONE (`d4f6b5249`,
  `DATE_TRUNC('iso_week')`, 69 lines); the four `AdjustBy*BigNumber` NOT STARTED, designed, three causes: the host's side
  batch sends the EXPECTED literal (`%33803336-12-17`) to DuckDB as a DATE it cannot hold; **the database judge's fused
  statement fails with either of two casts by timing (a G10 determinism break)**; a rejected fused statement reports no
  verdict. The fix: a date literal side as a plan constant (as String, Integer and Boolean folds are), the fused statement
  read in order, a rejection reported as the group's first verdict's failure. The LITE-PCT lane NOT STARTED.
  - **Shortest path: ONE PR from `main`** (`claude/compat-judges-parity`): the two cherry-picks plus the ruling-3 cases
    (~140 lines); the AdjustBy fix (~200-350 lines); the lane (channel B `-Dpct.channelb.rules=LITE` judged by a ledger,
    ~80 lines; channel A `-Dpct.channela.rules=LITE` with a LITE expected-failure ledger, ~60 lines plus rows; 10
    targets: 5 channel B ~75 s, 5 channel A ~129 s with 4 GB JVMs). One PR because the lane proves the fixes.
  - **Moves for the user's word:** about 460 new LITE rows in channel B's ledger (DuckDB's five suites: 426
    ModelException, 32 AssertFailed, 4 DataError, mostly relation tests asserting legend-pure's text), about 46 in a new
    channel A LITE ledger, probably the AdjustBy COMPAT rows' texts on other databases, `ratchets.tsv` entries, maybe
    `TestPlanJson` VERSION 32 → 33; `JavaEvalLedgerTest` pins only if the judges' files grow.
  - Nothing from the paused track is needed (A6's approved +1, the `SqlFnSortedTest` question and the finished unlanded
    commits stay paused; whoever lands second re-blesses moved LITE rows).
  - **This session's calls (14:40):** build it, one PR, branch `claude/compat-judges-parity`; the 5 channel B LITE
    targets per push in `pct_channel_b`; the 5 channel A LITE targets in `//gates:heavy` unless they fit `pct_duckdb`
    under ~11 minutes on every OS; FIX the AdjustBy four (cause 2 is a G10 break, not to be ledgered); stop if the fix
    needs a hook or a mode or grows a pinned judge file; every move named in the PR body with a summary by class.
- **~14:10 UTC, the judges: the size words follow H13's shape** (H10 §9.4 at `6c0a5889`; H13 §16): the refusal is the
  1-operand placed raise site (SIZE_NOT_MULTIPLICITY, `[Text(bound)]`), raised as a `DataError` carrying the record and
  worded by the existing `Hearing.caughtMessage`; no new record kind, the same deletions, about 0.25 day in 2.2b. **One
  behaviour change for the user's word, at B3's PR:** today the refusal is an `IllegalStateException` that `assertError`
  does not catch; as a `DataError` it is caught, as upstream's `assertError` catches `PureExecutionException` (closer to
  upstream; reach 0 on the whole PCT net and both corpus lanes, H13 measured). Added to stage B's list.
- **~14:11 UTC, H11 continued: the corpus half FINAL** (`H11-relation-text-spike.md` §13 at `fe57c506`; scratch, no pin
  moved, no hook, all edits in `com.legend.edge`). **DuckDB: rosters, verdict ledger and every FAIL line, words
  included, BYTE-IDENTICAL to `main`** (283 comparisons: 254 byte-equal, 18 ruled, 11 differ on `main` too; 0 printer
  differences). **H2: identical except 3 gains toward upstream** (`3.0`/`10.0`/`100000.0`). These are the hybrid
  numbers (the SQL builder kept only for the 11 `assertSameElements(..., toCSV(...))` prints per lane the arm cannot
  claim); strict, those 11 fail by name and nothing else moves. PCT re-run: DuckDB relation 373/373, standard 58/58, H2
  relation 342/342 byte-equal. **Calendar 41 as `CSVJOIN:<sep>`:** 0 fail, no refusal. **Judge-side composed relation
  prints:** 41 in the corpus (one shape, covered by the form), 0 in PCT; the corpus half of 1.3's value prints is 1
  test (`specialUnion::testGroupBy`, a list's text). **Abstract Date at midnight settled:** upstream prints a TIMESTAMP
  cell at midnight as `yyyy-MM-dd 00:00:00` (`ResultSetValueHandlers`; `hasHour` is a precision fact); the SQL builder's
  midnight rule (`RelationTextLowering.timed`) is not upstream's; nothing measured depends on it. **For the plan:** the
  `Printed` judgment kind must cover `assertSameElements` (already written in); with it strict equals hybrid. Moves for
  the real slice: H2 corpus fail roster -3, unordered register +1, PCT H2 ledger -4 rows; DuckDB none. H11 is done.
- **~14:12 UTC, the translator's A3 note, revision 2** (`090dbff8`, first reported as `b0ffb80d`; evidence in `a3/`). **PRs 2; about 6 to 6.5 days**
  (PR1 1.5; PR2 4.5 to 5: the switch and guard moves 0.75, the raise 0.5 to 1, the PCT adapter's option off 0.25,
  the detector on 5 routes with the mapping reach 1.5, the report 1.5); the cross-kind fix gates PR2, unsized. Lite
  tests: 16 of the 17 move (the IQ showcase unchanged under option (a)). **Corpus, A3 after A2, MEASURED:** run Y (A2
  alone) reproduces B2's 423 and 260; run X (A2 + A3) gives 430 and 267: each fail roster +7, the same 7 on both
  (`testToSQLStringSubstringIsUncorrectedByDefault`, `testFilterUsing{Substring,ParseInteger,ParseDecimal}Function`,
  `testPlanForDateTimeConstantParameter{No,GMT,EST}TimeZone`), each unordered register -3, the skipped and engine-order
  registers unmoved. With the cross-kind fix the DuckDB roster is the same 7 (`testPlanForDateTimeVariableESTTimeZone`
  already fails under A2). **Detector:** 5 routes (generatePlan, execute with Arrow, the three test-data routes); the
  header at most 4 codes with counts; **H2's literal 0 measured** on 2.1.214 and 2.5.252, regular and legacy: start 0
  reads as 1, equal to legend-pure wherever legend-pure answers, so exempt both forms; DuckDB's 3-argument form differs,
  its 2-argument `substring(s, 0)` is equal (exempt too: this session's call, within §4a's rule). **Reach:** qualified
  properties already covered (`Typer.java:555-592`); added the runtime's mapping (M2M transforms, association and
  constraint bodies, via public entries); `###Relational` property mappings call PR1's natives, never flagged.
  **The load-time report:** at the edge; once per model hash (LRU 1,024), on a background thread; triggered by Studio's
  compile, the first query per model, or `POST /api/compat/v1/pure-meaning`; every lifted body pre-filtered by name,
  and the one store's saved queries; a bounded log block and `GET /api/compat/v1/pure-meaning`; about 4 s on the corpus
  world (8 s when compile triggers it), listing 48 calls (16 `indexOf/2`, 4 `substring/2`, 28 `substring/3`) and 83
  unverified bodies; 7 tests; about 1.5 days. **A tenet to post:** it is eager G, once per model, as compile already is;
  put to the user with the detector default (14:45). New core hooks 0.
- **~14:12 UTC, H11's last asks done** (§13.7 at `c0e6e55d`): relation-text sides by assert family. PCT:
  `assertEquals` 431 (423 tests, all toString), every other family 0. Corpus: `assertEquals` 433 (292 tests: toString
  253, toCSV 138, CSVJOIN 41, csv 1), `assertSameElements` 22 (all toCSV; 11 run in the lanes), every other family 0.
  So two families: `assertEquals` 864 sides, `assertSameElements` 22; under `assertSameElements` `main` judges the pair
  as a render form with `orderedForm=false` (lines as a multiset), so `Printed` must carry that order fact. The value
  half (composed cell values, the product's own text, no `Printed` needed): corpus `assertEquals` 361,
  `assertSameElements` 158, `assert` 5, `assertEq` 1; PCT 0. H11 stopped.
- **~14:15 UTC, A5's first counts** (`claude/compat-judges-parity`, local: `main` + `7cd59a571` firstDayOfWeek,
  `0fbae4318` testDateFromHour, `fd589b21b` the ruling-3 cases; the channel B LITE switch and its 5 targets built,
  uncommitted). **LITE channel B, DuckDB, BOTH judges: 787 PASS, 462 FAIL of 1,249** (the homework's 786/463 plus
  `testDateFromHour`). Per suite: essential 313/32 (39 s), grammar 136/1 (23 s), relation 100/369 (59 s), standard
  145/60 (34 s), unclassified 93/0 (9 s). **The 462 by class:** 423 legend-pure's relation text (refused by name by
  design); 16 legend-pure's error words; 7 legend-pure's print of a value (Float 3, DateTime 3, a date's format 1); 3
  refused by name (format's `%05d`, `%r`, one more); **7 R4** (substring `testStart`, `testStartEnd`, the 2 sort keys,
  indexOf `testSimple`, `testFromIndex`, `testIndexOfOneElement`: they leave in A3, told to the translator); 2
  pre-existing (`acos(2.0)`, `asin(2.0)` raise nothing; legend-engine's manifest ledgers them too); 4 judge parity
  (`AdjustBy*BigNumber`, being fixed: they become "both judges fail, the same database error"). Expected ledger: 462
  rows, none a judge disagreement. **Size: about 1.5 days** (the AdjustBy fix half a day; channel A LITE half a day;
  ledgers, ratchets and the PR a quarter; CI the rest).
- **~14:16 UTC, the translator's A3 note, revision 3** (`f4a2dd53`; first reported as `2cd744bd`): the literal-0 exemptions per target and arity (H2
  both arities; DuckDB `substring(s, 0)` only; any other database flagged, not measured); A5's lane's 7 R4 rows added to
  A3's moves, conditional on A5's PR landing first. The cross-kind fix's local run is going on local
  `claude/compat-cross-kind-ne` (`main` + `63421e0a`, cherry-picked clean as `c650d959`).
- **~14:19 UTC, H13 continued: tasks 1 to 7 done** (`H13-error-records-spike.md` §11-22 at `3bb9a2bd`).
  - **The whole net, run twice:** channel A outcomes 0 differ on every lane; channel B 0 of 1,249; pure/v1 0 of 28;
    channel A answers only the 2 known Relation lines per database. The 62 raises worded after the run: 62 exact, 0
    withheld. **The corpus, both lanes: rosters unchanged (10 of 10).** All 51 `core_tests_*` targets pass under the
    host judge, `judge_both` and `judge_database`; `core_edge_tests` 12 fail (was 20), all SIZE_NOT_MULTIPLICITY.
  - **Lite's plan:** JSON +48,052 (+0.033%), SQL -1,680 (on `main`, `at`'s count was read twice, so `LetBinding`
    hoisted it; now once). Channel B ratchets: 6 values in 3 suites (judge SQL shrinks; judge plan -9,130, +629,
    +1,716); statement counts unchanged.
  - **A pre-existing pure/v1 defect:** the envelope prints the `DataError`'s cause, so a raised site answers its bare
    kind (`main`: "Raised: DATE_COMPONENT_INVALID" for `date(2017, 13, 1)`). Fixed at the edge in 6 lines
    (`PureMessages.compile` words a raised site through `RaisedWords.wordsOf`): "Raised: Invalid month: 13". Golden
    28/28 unchanged. H13 §4's "Java's pure/v1 route is not affected" corrected. **Lands with B1.**
  - **Task 5, legend-engine ASKED** (the engine runner, 4.145.0; probe `H13/ErrorBodyProbe.java.txt`):
    `raise.datePartAbsent` (`%2017->hour()`): legend-engine 500 PureAssertFailException "Ensure the target system
    understands Year or Year-month semantic."; lite "Raised: Cannot get hour for 2017". `compile.windowFrameInverted`
    (`rows(4, 2)`): legend-engine 500, no errorType, an assert-failure sentence with its resource position; lite
    COMPILATION with legend-pure's sentence alone. Exact bodies in §17; not added to the golden.
  - **An adjacent divergence:** `#>{nope::DB...}#` answers lite's "Can't find database 'nope::DB'" (a comment claims
    legend-engine's words), where legend-engine answers "The store 'nope::DB' can't be found." (its MISSING_STORE
    sentence). **This session's call:** the legend-engine presenter words it, in B1.
  - **The defect cell:** 0 in PCT and pure/v1; 12 planned in the DuckDB corpus, 0 on H2; 0 answers carry one. 1 corpus
    test expects the sentence (`testCheckedWithCircularConstraints`), failing on both lanes for legend-engine's own
    `isDistinct` defect. The words are legend-engine's (`javaPlatformBinding` `constraints.pure:171`): the ENGINE
    presenter words it, about 120 lines, 1 to 1.5 days, in B2 (caution: `WriterCarriageGuardTest`, the wording in the
    presenter, not a writer). Also found: the checked graph's default violation message is legend-engine's sentence in
    the product (`resolver/GraphEmission.java:653`), for B2 and the census.
  - **Size words:** the one-operand placed site (kind plus bound); reach 0; adopted by the judges (B3).
  - **The three notes:** the date print moves legend-pure's audience only; `CheckedOne.statedSize` about 10 sites, 30
    lines, 0.5 day, fixing 3 of the 12 `toOne` tests (the other 9 are true run-time sizes A3 withholds, which need the
    withheld-spelling register row); `Operand.Date`: keep `Operand.Text` typed by the kind's contract, held by a unit
    test (a MIR date literal would add a variant for a value the product never prints).
  - **The host-parity worker's `lite_wire`:** agreed; `raise` stays the wire name, `operands` a sibling key.
  - **B1's H9 credited;** neither of its two forms was needed (no cast read; the size from the multiplicity).
  - **Guarded files** (§19): `PlanStyleSurfaceTest`, `EdgeOnlyDataTest` (CP-7's row, 8 pieces), `EdgeOnlyMembersTest`,
    `PlatformNamesGuardrailTest`, `CodeShapeGuardrailTest` (none), `baseline.tsv`, `ratchets.tsv`, `TestPlanJson` v34,
    `PlanJson` v59 or v60, the hosts' fixture, the register rows. Slice 1.6 (lite's sentence leaving the plan) breaks
    only §4's known host set.
  - **Size of the real slice (§20): about +1,450 / -880 lines including S1, 6.5 to 7 days.** No hook built; no pin,
    golden, ledger or allowlist moved.
  - **Folded into the final plan (14:50):** B1's row (the prototype complete, the two defects it lands); B2's row (the
    defect cell and the default violation message); stage B's size, honestly, about 4 to 5 weeks with B1 and B3 side by
    side (6 weeks of one builder's work).
- **~14:23 UTC, B1: Windows edge_net GREEN at `e7764c9db`** (run 44537117741): 38 of 38 tests; every overlay row
  applied and nothing else moved. The job took 11.0 min (was 19.4 before the split, about 26 on the second run). Times
  (`main`'s run in brackets): Windows corpus_duckdb 2.3 [2.5], corpus_h2 2.4 [2.3], datacube 16.9, core [23.4] still
  running; Linux core 23.3 [33.5], checks 22.4, datacube 20.2 [18.5], corpus_duckdb 3.5 [5.6], corpus_h2 4.8 [4.0];
  Linux arm warehouse 10.0. Linux edge_net stopped at analysis on Maven Central's 404s (h2 2.5.252 and 2.1.214) after a
  repository-cache miss; Linux product on the teavm 404. macOS lanes still queued. edge_net is not the longest lane on
  Windows. Next: macOS's verdict, Linux's re-run, Windows core, ~14:55.
- **~14:55 UTC, THE USER: "Detector default warn is okay"** (recorded in §1): warn by default, with the load-time
  report. The user also asked for stage A's steps "clearly in English" (written in chat).
- **~14:24 UTC, B1-R: PR #124 at `fa8294cca`, near-final CI** (run 44531926654). Linux: every lane the PR touches green,
  core green; red only ui, product and corpus_h2, all Maven 404s. Windows: all green but edge_net (#123's 4 tests).
  **macOS: pct_duckdb GREEN in 12.75 min** with the five DuckDB ENGINE lanes and the batteries; pct_h2 6.0 min;
  pct_postgres, parser_equivalence, product, checks, corpus, datacube, warehouse, sdlc, pct_channel_b green. **macOS
  edge_net fails the same 4 diff tests:** Float last digits from the platform's maths library, as on Windows, but macOS
  also moves `acos`, `asin` and `cot`, so it needs its own `overlay_macos.tsv` (rows sent to B1). None of #124's own
  code is red on any OS; left: #123's macOS overlay and the Maven mirror on `main`, then one batched merge.
  **Note:** macOS's pct_duckdb at 12.75 min is over the ~11 min this session set as a soft limit for A5's lanes; A5's
  channel A LITE targets stay out of `pct_duckdb` on that evidence.
- **~14:27 UTC, A5 stopped on the AdjustBy fix** (it reaches the database judge's side planning). Before any fix:
  DuckDB's HOST fails on the EXPECTED literal (`date field value out of range`), DATABASE with that or an INT64 cast by
  run (JudgesDisagree); SQLite JudgesDisagree; H2 both fail with the same class but H2 writes the whole SQL statement
  into its message, so the two judges' texts never match under BOTH (an H2-wide finding, not this fix's). Built,
  uncommitted: (1) the host plans a date-literal side as a plan constant (equality asks with fold=false); (2)
  `VerdictSql.batch` reads its asserts in order; (3) a rejected fused statement becomes its first verdict's failure.
  SQLite then agrees on every run; DuckDB does not, because the DATABASE judge's sides go through
  `BodyDriver.planValue`, which never folds, so one verdict row holds two failing expressions and DuckDB reports either.
  **Option 1 (upstream's answer):** neither judge sends a date literal to the database (`BodyDriver.planValue` ~6
  lines, its pin 545 moves; `VerdictSql.constantPlan` spells a date as its canonical text); risk: a DateTime literal no
  longer meets a computed TIMESTAMP inside the database, so a sweep is needed; +0.5 day. **Option 2 (narrow):** each
  row reads its expected side first, so both judges fail deterministically on the expected literal's range error (lite's
  own error, not upstream's; a register row). **This session's call (15:00): option 1, the sweep first** (core, PCT
  channels A and B, both corpus lanes): if only the 4 AdjustBy tests move, build it, the BodyDriver pin for the user's
  word in the PR; if anything else moves, stop and report. The H2 message finding counted separately.
- **~15:05 UTC, THE USER corrected this session** (recorded in §1): "Wait what? Why are you going against our tenets -
  everything must be pushed to database". **This session's 15:00 call (option 1: neither judge sends a date literal to
  the database, upstream's answer) was wrong**: it chose upstream's answer over G6 (the database does the work) and G3
  (values matched by type in the database), without checking them. Reversed at once (15:06) to **option 2**: both
  judges keep sending the literal; each verdict row reads its expected side first, so both fail deterministically with
  the database's own range error (G2, G10, G7 hold); A5's change (1) (the host's date-literal constant) is removed;
  (2) and (3) kept if neither moves a value out of the database; no BodyDriver change, no pin; the difference from
  upstream (legend-pure answers; a database refuses a date beyond its type's range: DuckDB DATE to year 5881580,
  TIMESTAMP to 294247) a register row for the user's word. **Lesson, standing for this program:** where upstream's
  answer and the tenets differ, the tenets win; a design that moves evaluation out of the database is checked against G6
  before it is chosen, and goes to the user if it does. The other Java-side pieces of the plan were re-checked against
  G6 the same hour (the next entry).
- **~15:10 UTC, THE USER: the design and plan are wrong** (recorded in §1): "We have the completely wrong design and
  plan if you are allowing things to not be pushed to database - legend lite tenet is java orchestrates and database
  does the work, upstream is not our implementation guide". **This session's error, stated plainly:** the design took
  legend-pure's and legend-engine's own structure (they print and word in Java, after the run) as the model, and
  stretched G6's "small Java formatting step at the edge" into a design pillar. **What in the plan moves work out of the
  database, and is withdrawn:** (1) stage B's formatters (printed tables and values in Java instead of SQL: B2, H11's
  printer, the judges' `HostJudgment.Printed`/`Hearing.print`); (2) error records worded in Java after the run (B1,
  H13's `PureMessages.worded`), where today the database builds the message, values included; (3) verdict records worded
  by Java presenters (B3's `Hearing`); (4) A4 as written (legend-pure's comparison rules kept only in the Java-side
  judge); (5) the date-literal call already reversed at 15:06. **What is sound (no work leaves the database):** A1's
  nets; A2 (legend-engine's SQL spelling deleted; lite's own SQL shown); A3 (Pure's meaning computed in SQL; the detector
  reads the query's types at planning); D2 (Pure's checks as SQL); the corpus door's rewrites (planning). Put to the
  user (15:15): the corrected principles, the one design question they hinge on, and whether A1-A3 go ahead while the
  rest is redesigned.
- **~14:35 UTC, B1: the macOS overlay pushed** (#123 head `7b7ee6ea3`). From #124's run 44531926654 (macOS edge_net
  finished there; the same 4 diff tests red): 31 value lines, each exactly 1 ulp, 16 distinct facts (acos, asin, cot 3
  on DuckDB and SQLite essential; cbrt 1 on DuckDB essential; sinh, cosh 12 on both standard suites); no order, locale,
  path or line ending moved; where both OSes move a fact, the values agree. `overlay_macos.tsv` 16 rows covers all 31
  lines. **A defect the second file exposed, fixed:** with one pass, Windows' row found macOS's value on macOS; the
  overlay now applies the current OS's rows first, then checks the others' (a new plant covers both orders on all three
  OSes and fails with the one-pass order). Local: 48 pass, 1 skipped. Both overlay files named in the PR body as new
  pinned files for the user's word; the README updated. Next: the new run on every OS.
- **~14:43 UTC, B1-R: PR #124's final CI at `fa8294cca`** (run 44531926654, complete). Green everywhere except 5 lanes,
  none #124's own code: Linux ui, product, corpus_h2 (Maven 404s; the mirror fixes them); Windows and macOS edge_net
  (#123's net; B1's overlays fix them: Windows green at `e7764c9db`, macOS in `7b7ee6ea3`, not yet run). Green on all
  three OSes: pct_duckdb (the five DuckDB ENGINE lanes and the batteries), pct_h2 (the three H2 ENGINE lanes), core,
  checks, stress, parser_equivalence, corpus_duckdb, datacube, sdlc, warehouse, pct_postgres, pct_channel_b (and
  product, ui, corpus_h2 on Windows and macOS); Linux arm warehouse green. Times: pct_duckdb Linux 8.3, Windows 9.9,
  macOS 12.75 min; pct_h2 7.1, 5.2, 6.0 min. Local gate green. Next: one batched merge of B1's head and the mirror.
- **~14:40 UTC, the user asked** whether everything in lite comes from the database as typed Arrow, with only the
  presentation layer (JSON, CSV) in Java. **This session's answer:** yes, with the line between the program and the
  response: the response's wire format (legend-engine's JSON envelope, CSV downloads, plan and error envelopes, a cell's
  JSON text) is presentation, Java; a text the program itself computes (`toString()`, `toCSV()`, `format()` inside a
  query, an error message a test checks) is a value, the database. So the H11 Java printer computed program values in
  Java (withdrawn); option (a) keeps legend-pure's text in SQL as lite's own, with no mode. Raised: lite's Java-side test
  judge compares rows in Java, which the rule questions (the user's call, now or later). Four decisions put.
- **~15:20 UTC, THE USER set the four rules of record** (recorded in §1) and asked whether they were already in the
  handoff. **This session's answer, checked in the documents:** mostly yes, in pieces: the host judge in Java (goals of
  record G2 and the 2026-11-07 ruling), typed values on the product path with text at the edge (G3), Java at the edges
  for compatibility (G6, the 2026-11-07 ruling), "fix up later in Java" (the presentation program's goal of 2026-11-18,
  and "Presentation at a root or a side is always Java"), Java orchestrates and the database executes (AGENTS.md). Not
  written before: the host judge as the ONLY judge in compatibility; folds for constant String and Boolean only;
  `format()` and `toString()` the database's in the product; compatibility's Java from lite's typed output only.
  **This session relitigated settled rules:** at ~15:10 it withdrew the design's Java formatters and A4 by misreading
  the correction; both are restored. The one real breach (the AdjustBy date literal as a constant) stays reversed.
  **Written now:** `compat-rules-of-record-2026-11-19.md` (one page: the rules verbatim, what they mean for the product
  and for compatibility, where each was already on record, how the plan stands against them). The final plan's banner
  now points to it. Every session is told to check against it first (~15:30).
- **~14:48 UTC, B1 checked PR #123 (`7b7ee6ea3`) against the rules of record:** nothing breaks them (no `src/main`
  file moves; the net records answers and judges no verdict). The per-OS overlays keep rule 1 on purpose: the digits
  are the database's own maths, and the overlay lets the database's answer stand, telling only the test which
  last-place values to expect on that OS; computing them in Java or rounding them would break rule 1.
- **~14:48 UTC, B1-R checked PR #124 against the rules of record:** nothing breaks them (no computation added; the
  ENGINE lane plans through the edge's existing style and the batteries ask lite's shipped route; the tooling only
  classifies and diffs recorded answers; channel A's verdict is PCT's own assert over lite's typed answer, the host
  judge; legend-engine's manifests and the oracle are spec input only). The H2 LITE_BEHIND rows are product gaps.
- **~14:48 UTC, the translator checked A3 against the rules** (holds: substring/indexOf and the raise in SQL; the
  natives lower to SQL; the detector and report read types at planning) **and flagged one reading:** the cross-kind fix
  lowers equality of two primitives of disjoint kinds to the SQL literal FALSE (or a nullness test where a side may be
  empty), decided at planning from the types. **This session's call (15:40): it holds under rule 1.** The outcome is
  the same for every value (legend-pure compares kinds first, `EqualityUtilities.java:67-71`), so planning reads a fact
  of the types, as overload resolution does; no value is computed or answered as a constant; the statement goes to the
  database, which evaluates the value-dependent part (emptiness). Unlike the AdjustBy call, which answered a value at
  planning. Not to be spelled as kind-tag comparisons in SQL. The PR body says so in two lines; the user is told.
- **~14:49 UTC, the judges' rules note (H10 §9.10):** 2.2a IS rule 4's deletion (A4), the same pieces, 3.5 days. Rule 4
  removes from 2.2b/§9: `Hearing.rowAppealed` and `JudgeRun.wordsAlike` (already 2.2a's); the edge presenter's
  Unjudged case (an UNJUDGED row is the database judge's); the database judge's word sites reached only with the
  product's hearing; COMPAT/BOTH as a check configuration (the checks become COMPAT/HOST, LITE/HOST, LITE/BOTH and the
  corpus); H13's database-judge binding. 2.2b about 4.25 days (was 4.5). Rules 1-3 hold against the designs; the one
  product gap the rules name (lite refusing a relation's `toString`) is the one its `Printed` design keeps.
- **~14:50 UTC, A5 (rules read):**
  1. **Where the compiler folds today** (no change made): `compiler/spec/NormalizeFolds.java` `fold`, reached only at the
     inliner (`UserCallInliner.java:1343`, G½): `size(x)` where x's multiplicity is exactly [n..n] to the Integer n (a
     type fact); `eq`/`equal` of two Integer literals to a Boolean; `and`/`or` of Boolean literals; an inlined `if` with a
     literal condition keeps its branch. `compiler/spec/StaticFold.java`, only at schema positions inside
     `<<functionType.NormalizeRequiredFunction>>` bodies (column names and lists the plan's shape needs): Integer
     `plus`/`minus` (exact, refused past a long), String `+`, `toString` of a String, Integer, Boolean or Float literal,
     `==`, `not`, `isEmpty`, `zip`, `pair`, `at`: schema, not row values, but beyond "constant String and Boolean".
     `LiteralUnroll.java` compares spelled literals and makes no value. **Put to the user** to make rule 1's fold list
     exact.
  2. **H2's error text carries the statement**, so on H2 under BOTH a raise from a judged side never matches by message:
     reach today 0 lanes (the LITE lane is DuckDB only); 4 channel-B COMPAT ledger rows carry the echo (the four
     `testLateralJoin*`); 0 channel-A rows; 0 corpus roster rows.
  3. **Stop: option 2 is built but not deterministic on DuckDB.** Built (WIP `81163ce10`, uncommitted parts): change
     (1) removed; each verdict row reads its expected side before its actual, the batch's verdicts in order (MATERIALIZED
     chains); a rejected fused statement is the first verdict's failure; no pin, no hook, no value leaves the database.
     Measured: HOST alone 9/9 and DATABASE alone 9/9 on assert 1's literal; BOTH 5/9 agree; the fused statement alone,
     30 runs: assert 1's literal 22, assert 2's 7, an INT64 overflow 1; with `SET threads TO 1` 30/30. A join's build
     side is not an order DuckDB keeps. Options: (a) DuckDB test plans run single-threaded via a target-declared setup
     step; (b) ledger as nondeterministic (breaks G10); (c) unfuse (still 1/30 within an assert).
  **This session's call (15:45):** (a), as narrow as is deterministic (first: only test plans with a fused verdict
  statement over a side that can raise; else every DuckDB test plan), as plan data the target declares; product plans
  untouched; stop if a PCT lane grows more than ~10%; the setting named in the PR body for the user's word, with G10 as
  its reason.
- **~14:50 UTC, H13's note against the rules** (§23 at `4da796b1`; nothing built). Rule 1: the product's raise message
  stays the database's (today a planner-known constant, kind plus where; if lite's message is ever to print a value,
  the SQL prints it). `at`'s size from the multiplicity is a type fact (in the fold list put to the user). **Rule 3, a
  design for stage B (B1 option "typed raise slots"):** the planner declares each withheld slot's position and Pure type
  on the site (no value); the SQL prints each value into the raise's envelope in a lossless per-dialect spelling; exec
  decodes the slots by their declared types; compatibility words upstream's sentence in Java from those typed values.
  Then the `<computed>` placeholder, its register row and both `statedSize` pieces go, and every operand legend-pure
  prints is printed. Per kind: INDEX_OUT_OF_BOUNDS offset and size; SIZE_NOT_MULTIPLICITY size (the 9 run-time `toOne`
  tests); SQRT_OF_NEGATIVE a Float; DIVIDE_BY_ZERO the dividend; DATE_PART_ABSENT a date at its precision;
  SLICE_BOUNDS_INVERTED both bounds; DATE_COMPONENT_INVALID at 1 of 4 sites; the three TEXT_NOT_* a String. To measure
  first: each dialect's error-text limits, Snowflake's and ClickHouse's quoting, the SQLite Float round trip. **This
  session's call:** recorded as a B1 option, for the user with stage B's ratification. Rule 4: the +15/-4 database-judge
  change is not needed in the product (both judges say lite's own message); dropped (-0.25 day).
- **~14:51 UTC, the independent reviewer: stages A and B against the rules of record** (§7 of
  `compat-final-plan-review-2026-11-19.md` at `293122a0`).
  - **Rule 1, A3 breaks it:** the A3 note makes `FeatureRules.CORRECTED_SUBSTRING` the plain rule for every String
    `substring`, and that rule folds Integer literals in Java (`IntLit(s+1)`, `IntLit(e-s)`, `lowering/FeatureRules.java:61-70`);
    `substr` already does this. Options: emit PLUS/MINUS over the literals and let the database fold them, or name
    Integer index arithmetic as an allowed fold. Nothing else in A or B computes a value.
  - **Rule 2:** B1's "each side's formatter words them" gives the product a formatter: restate lite's sentence as plan
    data the writer only serializes, compatibility's the only formatter. B2: for lite's own audience the database spells
    `toString`/`toCSV`, never a Java presenter.
  - **Rule 3 holds**, with one reading for the user: B1's compatibility sentences print the site's plan-literal operands
    in Java (Float, Decimal, DateTime, Boolean the database used to print); it holds if a raised error's record (kind,
    where, typed operands) counts as lite's typed output.
  - **Rule 4 holds by A4, but nothing enforces it:** add to A4 a guard, with a plant, that no compatibility audience
    plans a database-judge verdict.
  - **Folded in (15:50):** A3's row (the index arithmetic waits on the user's fold-list answer; the database folds it
    unless the user lists it); A4's guard; B1 and B2 restated. Put to the user: the fold list (now with `substring`'s
    index arithmetic) and the rule-3 reading.
- **~14:51 UTC, B2 checked its outputs against the rules:** none breaks one. The differences page gains the line "The
  database computes every answer. Only the output's formatting happens in Java ..." (`9512a9da4`). The SQL-text appeal
  runs legend-engine's expected SQL on the referee H2 and compares the rows in Java (`SqlReplayReferee.verify`), the
  host comparing, which fits rule 4. A2 proposes no value-computing Java. The B5 ratchet excludes `com/legend/edge/`
  and refuses any new compatibility hook in the core. Idle until A2's briefing.
- **~14:52 UTC, H13 done: "B1 option: typed raise slots"** (§24 at `ad7dede1`, not built). `Operand.Computed` carries
  its Pure type (`{"computed":"Integer"}`), so the plan holds no value; each dialect's raise writer prints the slots
  into the envelope after the position field in a lossless spelling per type (Integer digits, `DECIMAL_TO_TEXT`,
  `FLOAT_SHORTEST_TEXT`, a date's canonical text at its declared precision, a String escaped against chr(29)-chr(31));
  `RaisedErrors` decodes them by their declared types; compatibility words upstream's sentence from them. Removes the
  placeholder and its register row, `statedSize`, the unbuilt `CheckedOne.statedSize` and every withheld case (the 9
  `toOne` tests keep legend-pure's words). Measurements first, 0.5 to 1 day (the 12 raise writers' error-text limits;
  Snowflake's and ClickHouse's quoting; the Float round trip; MongoDB's raise; lite's SQL growth, since every plan
  carries the slots, estimated from H9's 344 renders). Size about +570 / -30, 3.5 to 4 days; B1 with it about 10 to 11
  days, or a later additive slice. For stage B's ratification.
- **~14:55 UTC, the user asked** whether this session agrees with the rules on the merits, and for tenets built on them.
  Answered: agreement with the reasons, four sharpenings (folds as constants and type facts, never row data;
  compatibility re-spells and never re-computes; the host judge's exception with its reason; the one-judge gap anchored
  by PCT's expected values and the engine check), and six proposed tenets, each with a test and a guard, for the user's
  word.
- **~15:09 UTC, A5: the DuckDB determinism fix built and measured.** Scope (ii), every DuckDB TEST plan: DuckDB's
  `threads` is global and a platform session is shared by every plan of an equal target (`PlanSessions.openShared`),
  so a node-level setting would leak into later plans, product ones included; a test plan's target differs from a
  product plan's, so they never share a database. Built as a dialect fact, `SqlDialect.testSessionSetup()` (DuckDB
  `SET threads TO 1`), written by `planner.Targets.forTest` at the three test-target sites; no exec change, no pinned
  file. **The costly ordered CTE chains are gone** (they grew the reference lanes' judge characters 5-45% and failed
  every ratchet): with the setting a zero-cost swap suffices (`FROM __sa a CROSS JOIN __se e`; DuckDB builds a cross
  join's right side first). Counts: setting plus swap 12/12 cells 30/30, every judge the same error; swap without the
  setting not deterministic; setting without the swap deterministic but BOTH disagree. **New fact** (legend-engine's
  pinned DuckDB manifest): legend-engine's DuckDB fails the four on the ACTUAL side's cast. **This session's call
  (16:00): keep the ruled outcome** (both judges on the expected literal's range error): neither failure is the answer
  (legend-pure passes; DuckDB cannot hold the value), both keep evaluation in the database, and matching legend-engine's
  failure text would reorder every host side batch. The register row states all three facts.
- **~15:15 UTC, the translator: the cross-kind fix (`63421e0a`) does NOT land as is** (measured locally on
  `claude/compat-cross-kind-ne` = `main` + `c650d959`; record `xk/` at `0b687302`; nothing pushed, nothing asked of the
  user). Corpus H2 fail roster 276 → 301 (+26, and -1: `testPlanForDateTimeVariableESTTimeZone` passes); DuckDB 108 →
  138 (+30); unordered registers -14 each; red: `//spec:corpus_duckdb_verdict` ("differential passes SHRANK: 975 < 990"),
  `//core:corpus_differential_test` (CV1_QuotedPillars rows lost), `//core:guardrails_edge` (`SqlFnSortedTest`); all
  three pass at `main`. Green: `CrossKindEqualityTest`, PCT, the rest of `//gates:local`. **Why:** every new failure is a
  cross-kind comparison now answering false (or true for `!=`) where legend-engine sends it to SQL and the database
  coerces: `###Relational` mapping filters (`"ordertable_0".prodFk = '2'`, SQL's `=` by definition, like its substring),
  and Pure query code legend-engine routes to SQL (`dayOfWeekNumber(...) != '2'`, `$t.id == '1'`). **A meaning change of
  D1's kind, not a defect fix. It does not gate A3** (A3's moves are 7 per database either way). **This session's calls
  (16:10):** out of stage A (the final plan corrected; no PR asked of the user); the scoped version (the mapping DSL's
  `=` keeps the database's comparison; Pure code keeps legend-pure's kinds-first rule) prototyped and measured as
  homework, then the meaning question goes to the user with numbers. The 15:40 "it holds under rule 1" call stands for
  where the types alone decide it; the meaning is the open question.
- **~15:27 UTC, B1: PR #123's edge_net GREEN on all three OSes at `7b7ee6ea3`** (run 44545061580, 38 of 38 each; both
  overlays verified on their own OS). edge_net against each OS's longest lane: Linux 11.0 min (core 34.6), Windows 14.2
  (core 24.8), macOS 17.7 (checks 21.3 among the lanes that ran): **not the longest lane on any OS** (before the split
  17.9 on Linux, 19-26 on Windows). Corpus lanes with the moved diff tests within noise (Linux 4.2/4.2, Windows 2.2/3.0,
  macOS 2.3/2.1). Not the PR's: Linux product's Maven 404 (teavm-jso); five macOS jobs cancelled after 40-47 min queued
  without a runner (capacity, cause unknown). B1 re-runs the failed and cancelled jobs once when the run completes;
  B1-R has `7b7ee6ea3` for #124's batched merge. #123 then waits for the user's word through this session.
- **~15:32 UTC, B1: run 44545061580 at `7b7ee6ea3` finished;** every lane #123 touches is green on Linux, Windows and
  macOS (correction: macOS's longest lane that ran is datacube, 25.8 min; edge_net 17.7 is not the longest on any OS).
  Six jobs are red and not #123's: Linux product (Maven 404, teavm-jso) and five macOS jobs (core, pct_duckdb,
  pct_channel_b, sdlc, product) cancelled after 40-47 min queued without a runner. Neither B1 nor this session can
  re-run them: the GitHub integration answers 403 on `rerun-failed-jobs` (this session tried at 15:35). B1 commented the
  six on #123 once. **The re-run needs the user's click:** "Re-run failed jobs" at
  https://github.com/neema2/legend-lite/actions/runs/44545061580. #123 then waits only for the user's word.
- **~15:35 UTC, A5: timing and ratchets for the single-thread step** (`a74435cb8` the fix, `39b7d81af` the pins). The 15
  DuckDB channel-B lanes, 2 runs each, off vs on: **single-threaded is FASTER on every lane, 3-19%, total 272.5 s →
  240.0 s (-11.9%)** (PCT's statements are tiny; DuckDB's thread scheduling was overhead). With the setting off,
  lite_essential also failed 1 of 2 runs: the non-determinism itself. Channel A (product plans) untouched. Ratchets: only
  the five judge-plan-JSON pins move (+0.016% to +0.043%, one more statement per verdict step); judge statements and
  characters do not move. Ledger: duckdb-lite 462 rows (the four AdjustBy as "a date past DuckDB's range"); the duckdb
  COMPAT AdjustBy rows' text unchanged. All 18 channel-B suite targets green. Next: the COMPAT channel-B lanes on SQLite,
  H2, MariaDB, MySQL, Postgres (Trino, ClickHouse and the cloud lanes named in the PR), the local gate, the PR.
- **~15:39 UTC, B1-R: PR #124 carries B1's head** (`7b7ee6ea3` merged as `a7a2a5aa9`, a normal merge; one comment
  conflict in `pct/BUILD.bazel`, both blocks kept). `main` has no Maven mirror yet, so not merged. Local: the 56 tests
  the merge touches pass; `//gates:local` running; the PR body updated. CI per OS to follow.
- **~15:46 UTC, A5 stopped on part (3)** (a rejected fused verdict statement counted as its group's first assert's
  failure): needed on DuckDB (30/30 agree with it), wrong on Postgres COMPAT (`testAdjustByMinutesBigNumber`: Postgres
  refuses the SECOND assert's expected literal while planning the fused statement; (3) blames assert 0). Root cause: a
  rejected fused statement does not say which verdict the database refused. Options: (beta) start a new fused group at
  each verdict whose sides may raise, by the planner's existing raise facts, so (3) is exact (gap: a literal past a
  database's range beside no raising function stays blamed on its group's first verdict); (gamma) weaken the per-assert
  comparison for a rejected group; (delta) leave the row red. **Also a guard hole:** a channel-B ledger row matches by
  "contains" and a JudgesDisagree outcome quotes the database's run, so the LITE lane passed while the judges disagreed
  (0 rows name a disagreement today). Other lanes: all 18 channel-B targets green; SQLite and H2 relation COMPAT green;
  MariaDB and MySQL red as on `main`, independent of the branch (MariaDB's list order moves run to run, MySQL's 13
  Decimal-scale rows). **This session's call (16:20): beta** (every evaluation stays in the database; no comparison
  weakened; G2), stop if a lane's judge statements grow more than ~25% or its time more than ~10%; **and the guard**
  (`ChannelBLedger.judge` refuses any JudgesDisagree outcome by name, with a plant). After A4, compatibility runs the
  host judge alone, so the COMPAT two-judge case goes; beta stays for the product's LITE lane.
- **~16:00 UTC, A5 stopped: beta crosses the 25% line** (`6c7b0dbf6`: beta backed out, the guard kept). Measured on the
  reference DuckDB lanes' pinned judge statements: beta as ruled (the full raising family, casts included) essential
  358 → 530 (+48%), grammar 135 → 187 (+39%), standard 207 → 335 (+62%), mostly the judge's own casts to DOUBLE; beta
  narrowed to raising functions, checked nodes and raise texts (no casts) +30%, +33%, +8%. A second defect beta needs
  fixed whatever the scope (a rejection in a group of its own did not complete its segment; the new guard caught it):
  the fix is in `RootLoops`, small, not pinned. Options: (b1) narrowed beta plus the fix, exact on every database;
  (b2) part (3) on the LITE lane only (DuckDB, single-threaded: 30/30, no extra statements), COMPAT Postgres red on one
  row until A4; (b3) hold for A4. **This session's call (16:35): (b1)**, the growth being in test plans' judge statements
  only, never a product plan; stop if any lane's time grows more than ~10%; the ratchet moves in the PR body for the
  user's word.
- **~16:40 UTC, THE USER adopted the six tenets, ruled folds ("constants and facts fixed by the types, never row data")
  and error records (yes)** (recorded in §1; the rules of record gain the tenets and both readings). A3's `substring`
  index arithmetic stays a compile-time fold; B1 stands. The user asked to see stage A's steps again, "now that we are
  ready to go".
- **~16:45 UTC, THE USER RATIFIED STAGE A: "Okay let's go"** (recorded in §1). Briefs sent at once (16:50) to every
  builder; the independent reviewer asked to review each stage-A PR before it goes to the user. The user asked to learn
  why PCT under lite's own rules passes 787 of 1,249 (a first answer given in chat; a full breakdown when A1's lane
  lands).
- **~16:11 UTC, B2: A2 brief received;** building PR 1 locally on `claude/compat-a2-printers` from `main` `b75d6c73a`.
  Its own rules need the user's word in its session to push outside `claude/census-b2` and to open a PR; it will ask
  the user there in two lines once PR 1 is green locally.
- **~16:13 UTC, the independent reviewer's PR reviews** (`compat-pr-reviews-2026-11-19.md` at `f7af4d8f` on
  `claude/compat-review`). **#123 (`7b7ee6ea3`): CODE APPROVED, not yet mergeable under A1's done-when:** its run's
  "gates green" failed on six jobs not its own (Linux product's Maven 404; five macOS lanes cancelled without a runner),
  and A1 wants the Maven fix on `main` first; re-run the six after the mirror lands. Every HOLD item closed; no
  `src/main` file moves; no golden moved since the "no answer moved" re-derivation, only the two overlays (17 + 16
  rows); `PlatformOverlay` sound. For the user's word: `EDGE_TEST_SRCS` +3, the gates moves, the 33 overlay rows, the
  S0 format re-records, the normalizer's tests in `tools/guards`. Nits: `EdgeNetPlantedTest`'s javadoc (line 24); the
  per-OS last-place fact as a register row or a differences-page line (sent to the translator for A3's page). **#124
  (`a7a2a5aa9`): CODE APPROVED, merge after #123** once run 44557117163 completes (at 16:10 no failure; it carries
  #123's head, covering the macOS lanes #123's run missed). All six items closed; no product `src/main` moves; all 22
  engine-runner targets testonly; the one allowlist entry dated (CP-9); no core switch; rules 3 and 4 hold. Notes: B5's
  count must include this lane's PURE_V1 use; A3 moves 12 of its pinned rows (each a pin move for the user's word);
  say in the body that "report-only" fails on class moves, not on lite's failures.
- **~16:35 UTC, the judges session: A4 step 1** (`claude/compat-a4-host-only` from `main`, pushed; no PR yet; it asked
  the user in its own session for the PR). `78dd1e2d`: **the guard** (`TestPlanner.plan` refuses, by name and before
  the style is asked anything, a DATABASE or BOTH judge for any style but the product's; `HostOnlyCompatibilityTest`
  plants a look-alike style; shown to bite). **Roads:** ChannelB's compatibility options HOST, the duplicate host
  channel-B pass deleted; `core_edge_tests_judge_{database,both}` deleted; 81 edge cases run under the host (their
  product halves keep every judge); MinimalCorpus's 'both' refused. **Results:** channel B passes its ledgers on DuckDB's
  5, H2 relation, SQLite's 5 and all 6 twice lanes (PCT unchanged; COMPAT/BOTH and COMPAT/HOST differ in 0 lines);
  `core_edge_tests` (958) and the H2/SQLite integration pass. **A surprise for the user's word:** `LeanSqlCorpusTest`
  (G3's corpus product-SQL baseline) planned the corpus under the DATABASE judge in the edge's style; under the host,
  the corpus's real road, the body's own statements count as their own (DuckDB 350 → 2,319, 1.40M → 2.36M chars; H2
  349 → 2,313, 1.34M → 2.41M; 246 families per lane); the product's SQL is unchanged, the measure's road changed. Pins
  regenerated in their own commit, marked for the user's word. **This session's call (17:05):** re-pin (no carve-out,
  not the product style), explained plainly in the PR body. Next: step 2 (legend-pure's comparison rules out of the
  database judge: `VerdictNumbers`' 2-ULP SQL, `RenderedTextRule`, `DateLiteralTexts`, `GoldenNullWord`,
  `CrossKindNumbers`, the JSON grammar's database read, `VerdictWords`' CANONS reads); step 3 (the database judge's
  SQL-text appeal path) sized before building.
- **~16:40 UTC, the user on a table's `toString()`:** "I think your original point was actually right, either we need to
  refuse or we need to print in database like every other function / Can we test with doing in db with lean sql of lite
  how many we can pass in the database? I guess it will break where 6 decimal float print or timestamp does not match
  what java does?" **Sent (17:10) to H11 continued (`session_0195rQEnceykGKzTfRUo7XWp`) as spike H14:** the product
  lowers a relation's `toString()` in the core to SQL in legend-pure's `#TDS` layout with the product's OWN lean value
  texts (`FLOAT_TO_TEXT` six decimals, `DATETIME_TO_TEXT`, the rest), no `FLOAT_SHORTEST_TEXT`, no edge builder, no
  facet, no hook; run PCT under LITE's rules (A5's channel-B LITE switch merged in), DuckDB's five suites first. Report:
  how many of the 423 pass byte for byte; failures by cause with examples; SQL size against `main`'s edge builder and
  the lean baseline; database limits; any hook (stop). Record `H14-table-text-in-db.md`. Half a day.
- **~16:45 UTC, the user: "Yes let's do it / Is there any cheap/lean sql way to get whole floats to be what people expect
  2.0? If not that's fine".** Added to H14 (17:15): two Float-cell variants measured side by side, (V1) the product's
  `FLOAT_TO_TEXT` as it is (`2`), (V2) a whole value keeping one fraction digit (`2.0`) by a lean spelling that reads the
  value once (trim the trailing zeros, append a marker, turn "point + marker" into ".0", drop the marker: two REPLACE
  calls); checked per dialect with negatives, NaN, the infinities, 1e20 and 1e-7; for each, the 423's pass count, the
  Float failures left and the SQL per cell; and, since V2 would change the product's scalar `toString()` of a whole
  Float from `2` to `2.0` (the user's 2026-11-10 ruling), the count of lite's own tests and pins that would move.
- **~16:50 UTC, the user: "Great let's do it"** (to H14 with both Float variants). Already running; no further change.
- **~17:05 UTC, the CI-health worker: the Maven mirror PR is open as #125** (on the user's word in its session: "If it
  only has CI change yes push PR and go"), branch `claude/compat-ci-maven-mirror`, head `ca523848d` = `main` +
  `f183b784a`: `.bazelrc`'s `common:ci` lines, `tools/ci/downloader.cfg` and its export. It watches CI per OS and reports
  any remaining 404 or 429; no merge. The independent reviewer reviews it before it goes to the user.
- **~17:07 UTC, the reviewer on PR #125 (`ca523848d`): CODE APPROVED;** CI pending (run 44573295984 from 17:05).
  CI-only (3 files, 20 lines, all under `common:ci`; the gate jobs pass `--config=ci`; desks and cloud sessions
  unchanged; no pin, lock, MODULE or version moves). The sha256 checks unchanged for every fetch (all 16 `jars_table`
  entries pinned; all six `rules_jvm_external` pools locked with a sha per file and `fail_if_repin_required`;
  `tools/postgres/postgres.bzl`'s fetch integrity-pinned too). It rewrites only repo1.maven.org/maven2, Central first;
  nothing can change what a build fetches beyond the URL. Nits: the export's visibility; the experimental retries flag
  (the run proves it parses on every OS); name `postgres.bzl` in the body. The 404 answer when CI completes.
- **~17:11 UTC, H14 DONE (V1)** (`H14-table-text-in-db.md` at `46354194` on `claude/compat-table-text-db-spike`, from
  `main` + A5's branch; scratch; no pin moved; NO hook, switch or mode). A relation's `toString` computed by the
  database in the product, legend-pure's `#TDS` layout, each cell the product's own scalar text, PCT channel B under
  LITE: **DuckDB 363 of 423 byte for byte** (relation 331/365, standard 32/58; the same under BOTH and HOST; nothing else
  moved); SQLite 352; Postgres 16 361; H2 relation 297/365. **Every DuckDB failure, two causes:** (1) a whole Float
  without its `.0`: 27 tests (`FLOAT_TO_TEXT` trims it; the six-decimal limit never bites: no PCT table has a Float
  beyond six decimals); (2) an abstract Date holding a time: 33 tests, 484 cells: **the product has no text for an
  abstract Date with a time**, so its scalar `toString` falls to the database's VARCHAR cast (DuckDB, H2, Postgres
  `2026-01-07 00:00:00`; SQLite `2026-01-07 00:00:00.000000`; PCT wants `2026-01-07T00:00:00.000+0000`): **a product
  parity gap** (lite answers differently by database). Decimal, StrictDate, null, Boolean, Integer, String, enums,
  Variant, order: 0 failures. Others: Postgres 2 Variant cells (jsonb key order, fail on `main` too); SQLite 11 dialect
  refusals in the tests' own queries; H2 35 list-encoding/LATERAL/fold refusals. **The SQL** (DuckDB, 365 relation
  tests): `main`'s edge builder 376 statements / 696,964 chars; lite's own text in the database 372 / 659,940 (5% less);
  rows presented in Java (H11) 376 / 187,864. **Database limits:** none reached (longest table 5,471 chars; 2 of 442
  exceed MySQL's 1,024 default, and lite's MariaDB/MySQL dialects already set `group_concat_max_len` to 1 GiB). **Side
  effect:** the window-in-list defect is hidden again (the text in SQL wraps the relation). **Built:** a 149-line
  `lowering/ProductRelationText` from product parts, called in place of the refusal. **Asked (17:15):** V2 (whole Float
  `2.0`), not yet run; and the abstract Date with one product text in two variants (lite's DateTime text; legend-pure's).
- **~17:17 UTC, B1-R: PR #124's CI at `a7a2a5aa9` complete** (run 44557117163). Linux all 16 lanes and the guard pins
  green (ui, product, corpus_h2 passed this time without the mirror); Windows all 16 green incl. edge_net; Linux arm
  warehouse green; macOS 14 green incl. edge_net, pct_duckdb, pct_h2, core; macOS ui and warehouse cancelled after
  54-57 min with no runner (capacity). The re-run needs the user (the integration answers 403). Times (Linux / Windows /
  macOS): pct_duckdb 10.9 / 8.7 / 12.1; pct_h2 6.1 / 5.5 / 4.2; edge_net 11.6 / 14.4 / 15.5. Local gate: 558 pass, 1
  skipped, 12 initdb-as-root (pass as ubuntu). `main` has no mirror yet; #124 holds.
- **~17:18 UTC, the judges session: A4 step 2 green** (`claude/compat-a4-host-only` at `61653394d`): legend-pure's
  comparison rules have left the database judge. 25 files, +103 / -1,202; 16 main files and 1 test file deleted; six
  two-form kinds deleted (CrossKindNumbers S127; VerdictNumbers S15/S132/S139 and the edge's 2-ULP tolerance;
  RenderedTextRule S131/S132 with the rows fallback and the golden parser; DateLiteralTexts; GoldenNullWord;
  JsonGrammar's planning read); `VerdictReads.CANONS` and the compatibility message branches deleted; the verdict row's
  LENIENT column planned false (removing the column later, a row-schema change). The host judge keeps every rule.
  Guard moves in 7 test files with dated notes (HookLambdasTest, HookRulingTest, SqlFnSortedTest, ArchitectureTest
  PLANNER_ENTRIES, UpstreamBranchesTest, JavaEvalLedgerTest AssertVerdicts 1465 → 1394 and DatabaseJudge 852 → 808).
  Green: the guardrails, the census, `core_edge_tests`, all 26 product database/both judge targets. PCT channel B, the
  edge integration and lean SQL running. **Step 3 sized:** the database judge's appeal path, about 350-450 lines deleted
  (the engine-text arms stay, the host's), three ruled hooks narrowed (EngineTestText 5 of 12 members,
  EdgeHearing.rowAppealed, EdgeRun.wordsAlike), the test plan JSON's `appeal` field leaving `RootLoop.Row`. **This
  session's call (17:25): in A4**, the hook narrowing marked plainly for the user's word in the PR body. The PR is
  asked of the user in the judges session.
- **~17:18 UTC, the CI-health worker on PR #125's run:** Windows warehouse red on `//python:markets_test` (TIMEOUT
  300.1 s), the pre-existing timeout (red on `main` and in both of #123's runs); no 404 or 429; the rest running. Its
  fix is `bb0fe2fc1` on the paused `claude/ci-health` (Python host only, `_headless.py` +63 and `projects.py` +13: the
  headless host keeps a model text's answers and the models DataCube held instead of re-reading a 5 MB project 48
  times; 210.6 s → 45.8 s). **This session's call (17:30):** a piece this program needs (it reds every compatibility
  PR's Windows lane), so its own small PR, `claude/compat-markets-test-time`, checked against the rules of record; the
  CI-health session asks the user in its session for the word to push it (not CI-only).
- **~17:20 UTC, the CI-health worker: the markets_test PR prepared locally, not pushed** (`claude/compat-markets-test-time`
  from `main`: `0c3c6da7f` = `bb0fe2fc1`, and `699331ffd` = `c733c4265`, the project-name memo bounded with the
  edited-text tests; Python host only, `_headless.py`, `projects.py`, `tests/test_headless.py`, +168 / -1). Checked
  against the rules of record: the caches hold the compiler's own answers keyed by the whole request text; no value
  computed or changed; no mode. `//python:all` running; it is asking the user in its session for the word to push.
- **~17:24 UTC, the judges session: A4 step 2's lane counts** (`61653394d`; all 23 targets pass, 328 s; each channel B
  lane against its pinned roster). DuckDB (and its twice lanes, agreeing exactly): essential PASS 336 / ERROR 9 of 345,
  grammar 137/137, relation 469/469, standard 205/205, unclassified 93/93. H2 relation 432 of 469 (21 agree-fail, 16
  wire-bug). SQLite essential 333/345, grammar 134/137, relation 458/469, standard 205/205, unclassified 92/93. The edge
  integration on H2 and SQLite passes; lean SQL DuckDB and H2 pass against the re-pinned baseline (`cfa6a634`). Step 3
  building: `RootLoop.Row` loses `decline` as well as `appeal`; the test plan JSON 32 → 33 with a note; both corpus
  rosters at A4's final head.
- **~17:26 UTC, B2: A2 PR 1's local counts** (`claude/compat-a2-printers`, 4 local commits; nothing pushed). **Corpus
  fail rosters exact:** H2 276 → 423 (+147), DuckDB 108 → 260 (+152), every added test on `A2-roster.tsv`, 0 extra, 0
  missing. **Registers moved more than the shape note said:** unordered H2 899 → 1,329 (+434 / −4), DuckDB 913 → 1,348
  (+440 / −5) (tests that passed by text, now judged by their rows through the appeal); DuckDB engine-order −2 / +1; ord
  +4 / +5 (`ordered-keys-unmappable`). **Edge nets:** the pure/v1 net and all 27 PCT edge-net files unchanged (the
  pure/v1 activities' SQL was already lite's); the corpus net's judge ledgers move only on the roster's tests plus 9 (H2)
  and 8 (DuckDB) already failing at `main`. Lean SQL passes. **Planted pure/v1 requests:** the net's `tds` request
  byte-identical; a DDL native's plan is the edge's H2 dialect's DDL (lite's own writer, "Create Table ... NULL"); an
  activity's SQL is lite's H2 text. **Guard moves:** 8 guard tests and the 192-row pre-quoted-names fixture; the
  dialect-construction ratchet 17 → 12 (not 14); the JavaEvalLedger edge list GAINS `LiteSqlText.java` (flagged). The
  CP-4 register row was drafted as S371, which is taken: **told to use S380**, provisional until landing. No rule
  broken. `//gates:local` running; then it asks the user in its session.
- **~17:30 UTC, status.** **H14 V2** (whole Float `2.0`): DuckDB's probe prints `2.0`, `-2.0`, `100.0`,
  `100000000000000000000.0`; `-0.0` and `±1e-7` print `0.0` where V1 printed `0`; NaN and the infinities unchanged.
  SQLite, H2 and Postgres probes being rerun; the V2 PCT lanes and lite's suites running (at V1's head 33 of 35 suites
  passed; `core_edge_tests` and `core_tests_exec` fail from the spike's own `toString` change, being listed). D1 and D2
  (the abstract Date's text) follow. **#125** (the Maven mirror): every Linux, arm and Windows job green but Windows
  `warehouse`, `//python:markets_test` TIMEOUT in 300 s (the known slow test the markets_test PR fixes, not #125's);
  Linux `core` and most macOS jobs still running or queued. **B2** agreed to S380; it edits the id after its
  `//gates:local` run.
- **~17:31 UTC, the CI-health worker: the markets_test fix is ready locally** (`claude/compat-markets-test-time`, head
  `699331ffd`, not pushed). `//python:all` 18 of 18 green. `markets_test` 47.7 s alone on the container (`main`
  210.6 s), 78.3 s inside the full run beside `engine_test` (247.9 s); Windows' 300 s limit is what failed #125's
  `warehouse` job. It waits for the user's word in its session to push and open the PR.
- **~17:32 UTC, PR #126 open** (the markets_test time, on the user's word in the CI-health session, "Push it"): head
  `699331ffd`, `main` `b75d6c73a` plus two commits, the Python host only. The CI-health worker watches its CI and
  #125's per OS; no merge. Sent to the independent reviewer (every assertion kept, no pin moved, the rules hold).
- **~17:34 UTC, the CI-health worker: #125 and #126.** #125 (run 44573295984): 35 jobs green, 1 red (windows /
  warehouse, the markets_test timeout #126 fixes), 15 running (mostly macOS); no lane hit a 404 or 429. Caveat: linux /
  stress restored a download cache, so it does not prove the mirror. #126 (run 44578192197): linux / stress red at
  analysis on Central's 404 for `h2-2.1.214.jar`, the failure #125 fixes (said once on #126). **Order: #125, then main
  merged into #126, then #126**, each on the user's word.
- **~17:33 UTC, the loop shortened** (the user: "Why are these pct tests taking long? Should it be fast loop"). The
  spike's run was lite's whole `//core:core_tests` (about 50 targets, uncached since the change is in lowering) with six
  PCT targets on 4 CPUs, about 8 min per variant; PCT alone is about 3 min. H14 told: V2 finishes as started; D1 and D2
  run only the PCT targets holding the 423, each count sent as it lands; lite's suites run once at the end, on the
  user's variant.
- **~17:34 UTC, the independent reviewer: #126 APPROVED** (record at `7e515130`). Every assertion kept (markets_test's
  file and BUILD target untouched; `test_headless.py` gains `ModelTexts` cases); no pin moved (3 files, +168 / −1); the
  memos are sound (each answer a function of its key alone; failures never kept; holds released at close); the rules
  hold (no value computed in Python; host orchestration, not presentation; no mode; relies on G10's ruled determinism).
  Notes, sent to the CI-health worker to fold in: a plant for the hold memo's eviction at `_HELD_KEPT=2`; the memory cost
  in the body (up to 2 extra compiled copies of a large model per host); named as a product change in lite's Python
  package (the tab and the server get no memos: a speed difference, never an answer difference).
- **~17:37 UTC, #126's notes folded** (head `684f923b4`, an ordinary commit): the eviction plant
  `ModelTexts.test_a_third_model_held_lets_the_oldest_go` (fails with the eviction broken, passes as written); the memory
  cost in the body, corrected: not 2 extra copies, since the native `HeldStore` keeps one compiled model per text shared
  by every holder (`HeldStoreTest.builtOncePerKeyWhileHeld`), so up to 2 large models per host stay compiled after
  DataCube's code lets them go; the body names it a product change. #126's new CI run will 404 on H2 until #125 lands.
- **~17:38 UTC, H14 V2 done** (record §6 at `487f3e40` on `claude/compat-table-text-db-spike`). **A whole Float as
  `2.0` costs one more REPLACE:** a six-decimal text ends in `.000000` exactly when the value is whole, so it is marked
  before the trim (`REPLACE(rtrim(REPLACE(REPLACE(t, '-0.000000', '0.000000'), '.000000', '.#'), '0'), '#', '0')`; the
  value read once, no concatenation). Per Float cell +33 characters (DuckDB 81 → 114; SQLite, H2, Postgres, BigQuery the
  same), +21 on the standard-trim dialects. **PCT's 423, V1 → V2:** DuckDB 363 → 390 (both judges), SQLite 352 → 379,
  Postgres 361 → 388, H2 relation 297 → 300. **Float-caused failures left: 0 on all four**; what remains is exactly the
  33 abstract-Date tests (and each database's own refusals). DuckDB essential gains 2 (`17.0`, `134210000.0`). Probed on
  eight databases, identical text: `2.0`, `-2.0`, `0.0` (and `-0.0`), `100.0`, `100000000000000000000.0`; `±1e-7` now
  `0.0` (V1 `0`); NaN and the infinities by name, except SQLite refuses NaN and MySQL/MariaDB all three, as today.
  **What V2 would move (counted, not moved):** it changes the product's scalar `toString` of a whole Float from `2` to
  `2.0`, a change to the user's 2026-11-10 ruling (S311): 5 core cases (`FloatTextTest`, `TypeInferenceIntegrationTest`,
  `AuditRound3Test`, two dialect SQL pins), 3 rows of channel A's lite ledger (2 now pass), channel B's duckdb-lite
  ledger (relation +4, standard +23 beyond V1's). V1 itself moves 4 core cases from the spike's `toString` change,
  including `FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure` (the edge style's table text now through the
  product lowering, `1e-05`): asked H14 to confirm the compatibility routes stay untouched in a real slice. **D1 and D2
  sent**, PCT only.
- **~17:40 UTC, the PR batch proposed** (the user: "Should we combine these CI PRs in single PR so we don't have so much
  CI waste and merge waste"). #124's branch already contains #123's (stacked); #125 (3 files) and #126 (3 files) touch
  none of #124's 99. A local test merge of all four is clean (105 files, +12,013 / −1,930). Proposed: merge #125 and #126
  into #124's branch (ordinary merges), close #123, #125, #126 pointing at #124, one CI run, the reviewer checks the
  combination, one merge word; later stage-A PRs stacked the same way. Waiting for the user's go. H14 started D1 (first
  count in about 10 minutes).
- **~17:42 UTC, H14 D1** (on V2: an abstract Date in a timestamp slot printed by lite's own `DATETIME_TO_TEXT`, in
  `Scalars.pureToString`, a table cell and a scalar alike). PCT's 423: DuckDB 390 (as V2), SQLite 379, Postgres 388, H2
  relation 300 of 365. The 33 Date tests still fail, now on spelling alone (PCT `2000-10-25T06:30:00.000+0000`, lite
  `2000-10-25T06:30:00.000000Z`), and **one text on every database** (each failing test's whole text identical to
  DuckDB's on H2 30/30, SQLite 33/33, Postgres 33/33; each database's own cast gone). DuckDB's five suites otherwise
  unchanged. D2 next.
- **~17:50 UTC, the four PRs combined** (the user: "Combine PRs and explain everything in them Clearly in plain
  English"). #125 and #126 merged into #124's branch `claude/compat-engine-lane` as ordinary merges (`aa002b6a`,
  `f832a667`; #123's head was already in it); pushed, no force. #124 retitled "Compatibility safety nets and two CI
  fixes, as one PR (was #123, #124, #125, #126)", its body rewritten in plain English (one section per part, the guarded
  files, the rules of record, the status; merge with a merge commit so each part reverts alone). #123, #125, #126 closed
  unmerged with a pointer each. #124's owner (B1-R) and the CI-health worker told not to push without word and to watch
  the one run; the reviewer asked to check the combination only.
- **~17:52 UTC, legend-pure's date text, READ** (the user: "Is pure date/time printing some iso or other standard?";
  `DateFormat.append`, legend-pure-m4 `DateFormat.java:237-267` at `7fbc7d6`): year (unpadded), `-`, month, `-`, day,
  `T`, hour, `:`, minute, `:`, second, then `.` and the subsecond digits exactly as the value holds them (no fixed
  width), then the literal `+0000` once the value has minutes. ISO 8601-shaped, but not strictly ISO or RFC 3339: the
  `+0000` offset is ISO's basic form glued to the extended date, and RFC 3339 requires `Z` or `+00:00`. Lite's
  `2000-10-25T06:30:00.000000Z` is valid ISO 8601 and RFC 3339.
- **~17:44 UTC, H14 D2** (on V2: an abstract Date in a timestamp slot printed as legend-pure's TDS text,
  `yyyy-MM-ddTHH:mm:ss.SSS+0000`, composed in SQL). PCT's 423: **DuckDB 423 of 423**; Postgres 421 (2 Variant keys in
  jsonb's order, failing on `main` too); SQLite 412 (11 of SQLite's own refusals); H2 relation 330 of 365 (35 H2
  refusals). The 33 Date tests pass on all four. DuckDB's other suites unchanged. **Summary on DuckDB:** V1 363, V2
  (Float `2.0`) 390, V2 + D1 (lite's DateTime text) 390, V2 + D2 (legend-pure's) 423. **Per-cell SQL of an abstract
  Date** (today's cast / D1 / D2): DuckDB 18 / 36 / 89; SQLite 15 / 166 / 206; Postgres 18 / 53 / 108; H2 18 / 76 / 134.
  Commits on `claude/compat-table-text-db-spike`: `46354194` V1, `487f3e40` V2, `9bcbd165` D1, `ca95a8be` D2,
  `b6a850d6` the sizes and the compatibility note.
- **H14's compatibility note:** the spike sends every style's table text through the product lowering (a shortcut).
  At V2 + D2 the compatibility rules' host-judge PCT relation 469/469 and standard 205/205 are as on `main`, but lite's
  edge test `FloatTextTest.anEdgeRelationsTextPrintsAFloatCellAsLegendPure` moves (`1e-05`, `1e+20` where legend-pure
  prints `0.00001`, `100000000000000000000.0`). A real slice keeps compatibility untouched only by (a) `RelationTexts`
  becoming a two-form kind (a `TWO_FORMS` row `HookRulingTest` pins, a guard row for the user's word, no new hook) or (b)
  landing with or after B2 (the edge prints tables in Java from typed rows, H11).
- **Read:** the product's DateTime text is ISO 8601 in UTC, six fraction digits and `Z`, the user's ruling of
  2026-11-10 (`SqlFn.DATETIME_TO_TEXT`, `dialect/DateTimeText.java`). D1 follows that ruling; D2 would print dates two
  ways in the product.
- **~17:47 UTC, the reviewer: the combination CLEAN** (record at `435777ca`): the diff against `main` is exactly the
  union of the four pieces (105 files, each byte-identical to its piece's), the pieces' files disjoint, no guard file
  edited by two pieces; #124's new fetches get the mirror too (619 runner files with a sha256). Remaining: the combined
  run 44580527071 green per OS. **B1-R** and the **CI-health worker** watch it, neither pushes without word. **B1**
  (#123's builder) idle; told #124's body names #123's guarded entries.
- **~17:53 UTC, the judges: A4 step 3 built** (head `e91005cc6` on `claude/compat-a4-host-only`, local). Step 3: 30
  files, +183 / −805. A4 whole in `core/src/main`: 52 files, +292 / −1,995, 18 files deleted, 1 added
  (`HostOnlyCompatibilityTest`); all paths 81 files, +1,119 / −2,960. Green: census, census_edge, guardrails,
  guardrails_edge; all 26 judge-matrix targets and `core_edge_tests` (at `7c53aefed`, same code). Running in one
  invocation: `//gates:local`, both corpora, PCT channel B, edge integration, lean SQL. **Two finds:** the ruled
  comparison (`VerdictBatch.orElse`, `VerdictSql.either`) lost its only caller in step 2 and is deleted; `BatchTexts` was
  a ruled hook of its own and is deleted, so the narrowed ruled hooks are four (`EngineTestText` loses five members,
  `BatchTexts`, `EdgeHearing.rowAppealed`, `EdgeRun.wordsAlike`), each with the dated note. TestPlanJson 32 → 33. Other
  guard moves with dated notes: `PLANNER_ENTRIES` 11 rows, `HOOKS_RULED`, `EdgeOnlyMembersTest`, `HookLambdasTest`,
  JavaEvalLedger shrinks (`SqlTextVerdicts` 1146 → 1086, `AssertVerdicts` 1394 → 1363, `EngineTextPlanning` 151 → 125,
  `SqlTextRuns` 214 → 199, `BodyDriver` 545 → 544, `BatchTexts`' row gone); one refusal by name added (unreachable in
  the product). **Landing changed:** stage A's remaining steps (A2, A3, A4, A5) land as ONE batch PR, each piece reviewed
  on its own branch, CI once; A4 asks the user for the push of its branch alone.
- **~17:56 UTC, the batch's merge order, measured.** The judges trial-merged A5 (`claude/compat-judges-parity` at
  `220c4453c`) into A4 (aborted): 7 files shared, 5 conflict with one hunk each (`VerdictSql`'s pair-facts join,
  `RootLoops.row()`, `TestRunner`'s condition, `pct/BUILD.bazel`'s suite list, the lean-SQL baseline, which is to be
  regenerated on the merged tree, never hand-merged); `VerdictBatch` and `ChannelB` auto-merge (A4 sets COMPAT's judge
  to HOST, A5 adds `-Dpct.channelb.rules=LITE`: for review). **Order adopted: A5 first, then A4**; A4 merges A5's final
  head with its five resolutions. Expected A2 and A4 meeting points (unmeasured, A2 not pushed): the edge's `PureText`,
  `UpstreamVerdictWords`, `SqlTextVerdicts`, `EdgeEngineTestText` and the guard maps; A2 and A3 asked to measure by trial
  merge once pushed. **Corrected:** the user's ~10:45 ruling 2 lets program sessions push their own `claude/compat-*`
  branches without an ask; this session had told A2, A3, A4 and A5 to ask. Corrected in each.
- **~17:55 UTC, B2: A2's meeting points with A4.** A2 touches none of A4's named files. Shared: `edge/PureText.java`
  (A2 deletes the engineText/enginePlanText factories), `ArchitectureTest` (A2 deletes invariant 4d's two quarantine
  rules and a clause of `upstreamSidesOutsideTheEdge`), `JavaEvalLedgerTest` (EnginePlans 760 → 758; the edge file list
  −4 printers +`LiteSqlText.java`), `core/edge_tests.bzl` (+`PureV1SqlTextTest`), the register (S380, S4 superseded),
  `docs/GATES.md`, `AGENTS.md` (invariant 3's printer paragraph and the dialect list), the corpus rosters and host-judge
  ledgers, and possibly `DialectBoundaryTest`, `CodeShapeGuardrailTest`, `DialectOwnerExclusivityTest`. **Order now: A2,
  then A5, then A4** (A4's rosters measured on A2's); A4 merges both and resolves. B2's own session rules keep a one-line
  ask to the user in its session before its push.
- **~17:55 UTC, the translator: the scoped cross-kind prototype measured** (homework, not a landing; record
  `compat-homework/xk/scoped/README.md` at `536f105d` on `claude/compat-translator-spike`). The scoped rule
  (`###Relational` `=` keeps the database's comparison; Pure code keeps kinds-first): corpus H2 276 → 286 (+11, −1
  fixed), DuckDB 108 → 123 (+15); each unordered register −5. By cause: `###Relational` `=` 15 per database (pass again
  under the scoped rule), lite-built comparisons 0, the program's own Pure code 11 (H2) and 15 (DuckDB). **Every
  Pure-code case is the program's own cross-kind comparison** in legend-engine's test code (e.g. `$t.id == '1'` with
  `Trade.id: Integer`, `testQualifier.pure:273`; the 8 `XStore::ordered` cross-store conditions, String against
  Integer, `testOrderedCrossStoreGraphFetch.pure:131,169,261,316`): legend-engine answers the database's coerced
  comparison, legend-pure "never equal". PCT and `//gates:local` green but the moved guards (rosters, registers, the
  verdict ledger's appeal passes 990 → 985, the fix's `SqlFnSortedTest` pin); `//core:corpus_differential_test` green.
  For the meaning decision after stage A, with these numbers.
- **~17:57 UTC, A5: PR #127 open** (`claude/compat-judges-parity` at `3cb787902`; it crossed this session's batch
  message). Kept open as A5's review and CI evidence; it folds into stage A's batch PR and closes then. Lane times: the
  DuckDB channel-B lanes 240.0 → 247.4 s with (b1) (+3.1%), the other 23 lanes 376.1 → 360.9 s; none past 10% beyond its
  spread; Postgres ×5 green including `testAdjustByMinutesBigNumber`. Local gate at `f13e73c28`: 12 of its own failures
  fixed and re-run, 14 the container's (green re-run). New moves for the user's word: the lean-SQL baseline (27 DuckDB
  families' plan JSON +19 characters per statement, `SET threads TO 1`, +0.121%, statements and SQL unchanged) and one
  allowlist entry (`CodeShapeGuardrailTest` gains `RootLoops.rejected`). **Why 462 of 1,249 fail under lite's rules**
  (#127's section): a relation's `toString` 423 (refused by name today; the open table-text decision); legend-pure's
  error words 16 (lite's own words; B1 words legend-pure's at the edge); the product's ruled Float and DateTime texts 7
  (S311, S312); 1-based `indexOf`/`substring` 7 (R4, the product not yet Pure's way; A3); a date past DuckDB's range 4;
  `format`'s `%r`/`%05d` and an instance's `toString` 3 (refused by name, the edge's); `acos`/`asin` out of domain 2
  (NaN, S119). Separate findings: H2 echoes the statement in its error text (4 COMPAT channel-B rows); the compiler's
  Integer folds (`NormalizeFolds.fold`, `StaticFold`) sit inside the user's fold ruling; MariaDB and MySQL channel-B
  lanes red on `main` already (Q3, Q3b).
- **~17:59 UTC, A5 final at `3cb787902`** (pushed; green locally: `//pct:pct_channel_b` 18 of 18, COMPAT channel B on
  SQLite, H2 relation and Postgres green, MariaDB and MySQL red as on `main`, channel A LITE ×5 green). #127 kept open
  for its one CI run (A5 goes first and changes every DuckDB test plan's session: an early per-OS signal on A5 alone);
  no more pushes; it closes pointing at the batch. The judges' session given A5's head and its files to watch; it waits
  for A2's head, then merges A2 then A5.
- **~18:01 UTC, the reviewer: #127 APPROVED WITH CHANGES** (record at `d58b7831`). MUST-FIX: a trial merge into
  #124's combined branch conflicts in `PctPlanRoute.execute` (#124's `-Dpct.audience=engine` and #127's
  `-Dpct.channela.rules=LITE` rewrite the same lines): one three-way selector, combinations refused, #124's
  `AudienceCheck` kept. A5's five risk points hold; rules 1 and 4 hold; H2's `firstDayOfWeek` fix matches Pure and
  legend-engine's H2 (`h2Extension2_1_214.pure:223`), no register row. Should-fix: S1 `threads = 1` is a door-keyed
  test/product session difference for the user's word (the product's DuckDB keeps the G10 gap; a LocalFile DuckDB shared
  by a test and a product plan in one JVM would share the global setting); S2 casts named in the (b1) gap; S3 the guard's
  string tied to the class; S4 `toTheMinute` and a colon-less zone; S5 `CORRECTED_SUBSTRING` among the Integer folds.
  Sent to A5: merge #124's branch and resolve with a plant, S2-S5 in the same push, and three lines for S1.
- **~18:05 UTC, the table-text rulings** (the user: "Agree with all four table printing recommendations"; §1's row).
  Recorded in the rules of record (the "product gap" line) and the final plan's B2 row. H14 asked to run lite's own
  suites once on V2 + D1 for the "tests that would move" count. A5 told: (x), merge #124's branch and resolve.
- **~18:12 UTC, compatibility's table text ruled** (§1's row): at the program's last step the legend-pure route
  takes a relation's `toString`/`toCSV` over; text used further in the query is lite's own, recorded. Written into the
  final plan's B2 row and the rules of record. The nested-case count assigned to B1 (idle since #123 folded into #124).
- **~18:23 UTC, A5: S1 measured and the review's fixes built** (local: `032b8f3ef` fixes 1, 3, 4; `b08ec103e` merges
  #124's branch `f832a6671` with ONE three-way selector, `PctPlanRoute.audience`: LEGEND_PURE default, LEGEND_ENGINE,
  LITE, any other value and engine + LITE refused by name, planted in `//pct:pct_route_audience_test`, RouteAudience
  still counting for AudienceCheck; `6f21bed29` the database's rejection raised in place of the ISE, with a test, and S2's
  casts named). The merge's checks 42 of 42 green. **S1, measured:** a product plan's DuckDB with two failing expressions
  in one projection raised the first expression's error 30 of 30 (both shapes); the error wanders only across
  pipelines (the test plan's fused statement of joined CTEs: three errors in 30). So a product plan that joins or reads
  CTEs keeps that G10 gap; test sessions no longer see it. **Shared file:** none left; `Targets.forTest` writes the test
  session statement only where the platform makes the database (its engine, the catalog, an in-memory connection), so a
  declared file's test target equals its product target; planted in
  `PlanMakerDuckDbTest#aDeclaredFileKeepsItsOwnThreads_anInMemoryOneTheTestsSetting`. #127's CI at `3cb787902`: linux
  pct_h2 and corpus_duckdb stopped on Central's 404 (no mirror on that head). Told: no re-run; push the three commits
  once (the merge brings the mirror), the new run is A5's evidence.
- **~18:26 UTC, B2: A2 against A4, trial-merged** (A2's local `0c043ae96` with A4's `e91005cc6`, aborted): **0
  conflicts**; 5 shared files all auto-merge (`core/BUILD.bazel`, `edge/PureText.java`, `ArchitectureTest`,
  `JavaEvalLedgerTest`, `BodyRouterParityTest`); no A2 pin moves under A4 (read diff by diff: the dialect-boundary pins
  stay 12 constructions and 13 arms; the ledgers re-pin different rows). The merged tree is built once by the judges'
  session at the batch. A2's `//gates:local` re-running after its last fixes.
- **~18:27 UTC, the reviewer: #127 re-checked at `6f21bed29`, APPROVED** (record at `13daaad8`): the must-fix (one
  three-way selector, combinations refused and planted) and the five should-fixes met; the merge of #124's branch is
  clean (153 files = #127's 49 plus the combined 105 plus 2 new tests; every combined file outside #127's byte-identical
  to `f832a6671`'s). Only CI on `6f21bed29` remains.
- **~18:30 UTC, DuckDB threads ruled** (§1's row). A5 asked to draft the register row for the product's gap (in its one push, or in the batch's merge) and to count lite's tests that run on a declared DuckDB file.
- **~18:33 UTC, H14 at V2 + D1, the ruled variant: lite's own suites, one run** (commits `c604cd72`, `206842ef` on
  `claude/compat-table-text-db-spike`; shape note `compat-homework/H14-slice-shape-note.md`). **PCT channel B under
  lite's rules: 1,179 of 1,249** (was 787; relation 435/469, standard 200/205, essential 315/345, grammar 136/137,
  unclassified 93/93); with A3's 7 `indexOf`/`substring` rows, about 1,186. Core: 9 cases move against the base (5 the
  Float `2.0`, 2 a relation's `toString` no longer refused, 2 under the edge style that B2's Java takes as last steps);
  channel A lite: 3 rows (2 pass and leave, 1 `0` → `0.0`); channel B duckdb-lite: 426 rows (392 leave, 34 change to the
  spelling difference: 33 dates, 1 Float), every one of the 423 refusal rows and no other. SQL of the table tests:
  relation 660,748 characters (the edge builder 696,964), standard 99,037 (194,885). **The slice:** a new
  `lowering/ProductRelationText` (~150 lines), V2's helpers, D1 in `Scalars`, about 240 of `RelationTextLowering`'s 791
  lines deleted; net about −80 lines. No guard row for the product half; register rows S311, S312 and S316(b) amended.
  Prerequisites: the whole value's `.0` on Snowflake, MySQL, MongoDB; B2's presenter claiming every last-step toString
  on the edge's routes. **Confirmed by this session:** text used further is the product's own (`Styled.PRODUCT`), per
  the ~18:12 ruling. **For the user:** a printed table's empty cell, legend-pure's `null` (the spike's layout) or
  nothing (S316(a)'s cell text).
- **~18:34 UTC, A5: the threads register row and its reach** (A5 had pushed `6f21bed29` at 18:24, so the row rides in
  the batch's merge). Drafted as "S371", which is taken: **renumbered S381 (provisional)** and kept in
  `compat-batch-register-rows-2026-11-19.md`. Reach: zero test plans run on a DuckDB the test declares as a file (seven
  core files declare one; one only parses it, six run product plans); none has two failing parts in different
  pipelines. #127's CI (run 44587411059): 6 of 55 checks done, all green.
- **~18:34 UTC, H14's shape note updated** (`31a919a5`): every cell of a relation's product text under
  `Styled.PRODUCT`, the Lowerer arm passing no style (no read of the plan style, tenet 6), pinned by a product test; the
  slice's prerequisites named (the whole value's `.0` on Snowflake, MySQL and MongoDB; B2's presenter claiming every
  last-step toString on the edge's routes, proven on a route; until then the edge's toString SQL stays). H14 stopped.
- **~18:36 UTC, the empty cell ruled** (§1's row): `null` in a printed table. Written into the final plan's B2 row.
- **~18:49 UTC, the judges: A4's counts at `e91005cc6`** (on `main`; one invocation, 3,114 s). `//gates:local` GREEN,
  597 pass and 1 skipped (12 failed on initdb refusing root and passed re-run unprivileged). **Both corpus rosters
  byte-identical across A4** (the roster diff tests and both verdict tests pass; only `lean-sql-baseline.tsv` moves
  under `rcorpus/`, for the user's word). PCT channel B: every lane against its pinned roster (DuckDB essential 336/345,
  grammar 137, relation 469, standard 205, unclassified 93; H2 relation 432/469; SQLite 333/134/458/205/92), unchanged
  from step 2. Edge integration H2 and SQLite pass; lean SQL both lanes against the re-pinned baseline; all 26 judge
  targets, the guards, the census and the shrink-only check green. Ready to merge A2 then A5 once A2's head is in; the
  rosters re-measured on top of A2 then.
- **~18:50 UTC, #124's run 44580527071 at `f832a6671`:** 40 of 54 jobs green, none red; the 14 left are macOS (13
  queued, warehouse running). Windows `warehouse` green, 23 of 23 (`markets_test` done by ~99 s; it timed out at 300 s
  before). Every H2 lane on Linux and Windows green, no 404 or 429 (the mirror). Linux and linux-arm `warehouse` green.
- **~18:51 UTC, #127's CI** (run 44587411059 at `6f21bed29`): 25 of 55 done, all green, none failed (Windows 13 of 16,
  Linux 3 of 16, linux-arm warehouse, the cross-OS checks); macOS mostly queued. A5 told the reviewer's approval.
- **~18:51 UTC, the user asked** whether `indexOf`/`substring` has a mode giving legend-engine's answer, and whether
  B2's takeover needs a planner flag. Answered: no mode today (D1: Pure's way, loud); legend-engine's answer cannot be
  fixed on the output (the value feeds the query), but could be an opt-in REWRITE AT THE LEGEND-ENGINE DOOR (the core
  keeps one meaning; tenet 6 allows the door), to be measured against #124's 49 pinned batteries per database first.
  B2's takeover needs no planner flag: the legend-pure door hands the planner the program without its last
  `toString()` (or rewrites the assert side), lite's route hands it whole. Offered the door-rewrite measurement.
- **~19:03 UTC, A3's honest status (the translator): none of its four pieces built yet**; today went to the spike, the
  notes and the cross-kind homework. Estimates: piece 1 (the frontends' `sqlSubstring`/`sqlPosition`, 47 + 3 sites,
  7 goldens) 1 to 1.5 days; piece 2 (the core switch, the raise, 9 pins, 63 PCT rows, 9 edge lines, the corpus lines,
  the PCT adapter's option off) about 1.5 days; piece 3 (the detector on 5 routes and the load-time report) about 3
  days; piece 4 (the differences page, the register rows, the rulebook amendment) 0.5 to 1 day. About 6 to 7 working
  days in one session: stage A's critical path. **Split across idle sessions:** the translator pieces 1 then 2 (piece
  2 on A2's pushed branch, not `main`); **B1-R piece 3** (the detector, branch `claude/compat-a3-detector` from A1's
  branch, a shape note first, no core hook); **B2 piece 4** after A2's push (branch `claude/compat-a3-docs`, docs only).
  With the split, A3 is about 3 days. The translator hands each its sections, spike commits and drafts. The user also
  asked whether lite should offer legend-engine's answer for `indexOf`/`substring` to production users who depend on
  it; answered with the measured options (the core flag 2,289 / 2,452; the typed door translator 2,285 / 2,447;
  nothing 2,281 / 2,443) and why a flag in the core is the wrong place; offered an opt-in at the door after A3, sized
  first.
- **~19:04 UTC, B2:** A3 piece 4 taken. A2's final commit `6551c2036` (one late fix: EnginePlans' drop and create share
  a lookup, re-pinned 760 → 757) under `//gates:local`; the previous run green but the 12 root-refusing targets, which
  pass 12 of 12 as `ubuntu`. Then it asks the user for the push.
- **~19:06 UTC, the user: "Yes size the engine-answer opt-in please."** Sizing given to B1-R (it built the batteries,
  and the opt-in sits on its detector): a third value of the detector's setting (`engine`), legend-engine door only,
  rewriting the request's program per use where the code runs in the database (F1), the core keeping one meaning. To
  measure: the translator's H5 prototype against the 49 batteries per database (and what each miss needs), the corpus
  with it on against the flag's and nothing's counts, the per-request cost and how to keep it off other requests' path,
  its size on the detector, any core hook (stop if so), the rules check. Note `H16-engine-answer-opt-in.md`, about half
  a day, before the detector's bulk build. The translator adds H5's commit to its handoff.
- **~19:06 UTC, B1-R took A3 piece 3.** #124's run: Linux, Windows, arm all green; macOS 4 of 17 done and green, 12
  queued. Before its shape note: two questions asked of legend-engine through slice R's oracle (does a store-free
  `'abc'->substring(1)` plan host-side; does a chained M2M transform or a constraint reach SQL) and a throwaway guard
  spike to name the guard moves exactly (candidates: the getenv allowlist, the System.err ceiling, EdgeOnlyMembersTest,
  PlannerSurfaceTest's routes). It flagged the report's eager typing for the user's word: **already ruled** at ~14:55
  with the report as designed (§4a: every body typed once, off the answer's path), so carried as ruled. Builds tested on
  flagging; lands in the batch after the core switch. Order: #124's CI, the opt-in sizing, the shape note, the build.
- **~19:14 UTC, the translator: A3 piece 1 built locally** (`claude/compat-a3-frontends`, `2a05b904` on `main`, not
  pushed): two natives, `sqlSubstring` and `sqlPosition`, each the database's function verbatim; 47 island call heads
  (CypherToPure 21, PostgresFunctions 10, IqFunctions 14, IqAggregates 2) and 3 dyna sites (RelOpTranslator);
  `native-claims.tsv` regenerated; it compiles; the local gate, both corpus lanes and the three parity lanes running.
  **Guard moves wait for the user's word in its session** (its own rules, and the standing constraint): the
  `NativeCatalogGovernanceTest` ceiling 38 → 40 (`INTERNAL_DESUGAR`, the two natives) and the 7 text goldens (the
  Cypher showcase, `CypherToPureTest`, `PostgresToPureTest`, `IqFunctionsTest`); later piece 2's 9 pins, 63 PCT rows,
  edge lines, rosters and registers. It asks the user there with diffs and dated notes once the runs finish.
- **~19:10 UTC, the user asked whether the detector compiles everything up front.** Answered: the per-request check
  reads the query's own compile; the load-time report types every body once per model in the background (about 4 s on
  legend-engine's corpus). Offered (b): produce the report only when the work is already done (Studio's compile) or on
  demand through its endpoint, never started on its own. Awaiting the user's answer.
- **~19:16 UTC, A2 PUSHED** (on the user's word in B2's session): `origin/claude/compat-a2-printers` at `6551c2036`, 7
  commits on `main`, no PR. `//gates:local` 558 pass, 1 skipped by design; the 12 root-refusing targets 12 of 12 as
  `ubuntu`; the corpus host passes, `//pct:edge_contract_test` and the lean-SQL lanes green. Fixes since the last
  counts: `toDDL`'s drop and the printed setup name a quoted table as its store declared it (lite's text had printed
  `Drop table if exists Items` beside `Create Table "Items"`); a seed's date cell is an escaped string (it had printed
  `DATE '2014'12'`); `EngineSeedText` leaves the temporal-literal makers list; `LegacyTextTest` deleted; the router
  parity programs respell lite's text; `EngineShape`'s six planner entries go; EnginePlans 760 → 757;
  parser-equivalence's corpus 3310 → 3311. Its batch section came garbled; asked to commit it as a file. **The batch
  merge started:** the judges' session merges A2 (`6551c2036`) then A5 (`6f21bed29`, which already carries #124's
  branch) into A4's branch, measures once on the merged tree, pushes; A3 merges last, then the batch PR.
- **~19:20 UTC, the detector's default ruled: refuse, for now** (§1's row); the load-time report not built. B1-R told; the final plan's §4a amended. The user also said: "I still like the FLAG concept for very narrow use cases - it like feature flags that we are going to want anyway for turning on/off features / We would have to very tightly guard how many flags so doesn't become a mess". A flag rule proposed to the user (below the reply), not yet ruled.
- **~19:21 UTC, B2: A3 piece 4's first pass** (local, `claude/compat-a3-docs` from `6551c2036`, docs only):
  `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md` (the ruled goal as its one rule; S380 for A2; the last-place maths on
  Windows and macOS from #123's overlays, H2 matching); AGENTS.md's upstream-fidelity tenet replaced by the ~13:15 draft
  with the six tenets and the two readings beside it; G8's first bullet amended; S382 (a Double's last digits by the
  operating system). **Answered:** the batch carries a copy of the rules of record (refreshed from
  `claude/compat-program` at the batch's assembly, after the pending switch rule); the provisional ids allocated by
  this session alone (S380 A2, S381 A5's threads, S382 the maths row, S383-S385 A3's new rows; S130 and S134 rewritten
  in place), in `compat-batch-register-rows-2026-11-19.md`. #127's CI: Linux 16/16 and Windows 16/16 green, macOS
  queued.
- **~19:20 UTC, B1: H15, the nested table texts counted** (`compat-homework/H15-table-text-nested.md` on
  `claude/compat-h15-nested`, `a8b5be0e55`; typed by lite's compiler, the relation-text calls being
  `TextRefusals.relation`'s three callers). **54 NESTED nodes, all in the corpus** (the same per lane); every other
  relation text is ROOT. PCT channel A 0/0; channel B DuckDB 431 ROOT (423 tests) and H2 relation 373 ROOT, 0 nested;
  core 20 ROOT sites in 16 tests, 0 nested; pure/v1 0. Corpus per lane: 398 ROOT in 253 tests (assertEquals 387,
  assertSameElements 11, every one a direct assert argument), **54 NESTED in 46 tests**: 41 are
  `toCSV($result.values)->replace('\n', ',')`, one in each calendarAggregations test, compared by `assertEquals` with a
  literal (the verdict depends on legend-pure's CSV spelling byte for byte, the Float text included); 13 are
  `println(toCSV(...))` in 5 `tds::tests::extensions` tests, read by no verdict. Cross-checked with H11 (PCT exact;
  corpus differences explained). Limits: 6,759 helper bodies would not type (every H11 side was still found). Asked:
  the 46 tests' outcomes on `main` and under the ruled B2, whether any lite text could pass the 41, and whether
  `println(<relation text>)` can count as a last step.
- **~19:24 UTC, A3 piece 4 for the reviewer** (B2: `claude/compat-a3-docs` at `a0505ebe6`, local; reading copy on
  `origin/claude/census-b2` at `0eacaecdc`): the differences page now says "nothing changes silently" (refuse by
  default, `warn` an operator's choice, no load-time report), S130 and S134 the same; the rules of record carried as a
  copy (refreshed at assembly); ids S380-S385 as allocated; `//gates:checks` 197 pass. Sent to the reviewer.
- **~19:23 UTC, the user asked what the closed edge-file list guards against.** Answered: `JavaEvalLedgerTest`'s
  closed registers keep Java from quietly doing the database's work (a new file fails until it is named with a reason,
  existing ones only shrink, an addition needs the user's word); `LiteSqlText.java` computes no value (it asks lite's
  dialect for the plan's SQL text) and the list shrinks net (four printers, about 3,000 lines, out; one 95-line file
  in). Recommended yes.
- **~19:24 UTC, B1-R: H16, the engine-answer opt-in sized** (`compat-homework/H16-engine-answer-opt-in.md` at
  `73a3331b8` on `claude/compat-a3-detector`; nothing built, no pin moved). **(1) The batteries:** with H5's typed
  use-site rewrite and two DuckDB rules added (a negative length counts backwards; a String literal's negative start
  through a literal twin), lite reproduces **every answer legend-engine gives: 46 of 46 on H2, 47 of 47 on DuckDB**
  (Pure's core alone 16 and 10; today's lite 46 and 31); what remains are legend-engine's own refusals (the
  3-argument `indexOf`; a list `indexOf` on H2 both sides fail). **(2) The corpus:** the rewrite equals the core flag on
  answers (the gap is 4 SQL-text-only tests that A2 removes anyway and 1 host-side DuckDB test only the old meaning made
  vacuous). **(3) Cost:** zero unless the setting is `engine` and the request is flagged; then about 1-2 ms for the
  rewrite plus 5-7 ms for one more typing; the model with the library built once per (model, target) and held, 3.4-4.1
  s on the corpus world (with no load-time report, the first such request waits for it; no fallback). **(4) Size on
  the detector:** about 2 to 2.5 days: 300-350 lines of edge code, about 85 Pure lines of library as edge resources,
  about 250 lines of tests, one funnel-register row, **no core hook**; refused by name under `engine` in its first
  form: qualified properties, mapping transforms, databases other than DuckDB and H2. **(5) Rules:** holds rules 1-4
  and tenets 1, 2, 5, 6, but it is a translator at the door that changes answers on purpose: it reverses D1's "Pure's
  way, loud, no translator" and the final plan's §2 part 2 for requests that opt in, and fails tenet 3's test, so it
  stops at the user (the note drafts the amendment). **Also settled by asking legend-engine:** a store-free
  `'abcdef'->substring(1, 3)` in a `let` plans host-side (SQL `where region = 'bc'`), so it is not flagged; a literal
  `substring` inside the routed lambda goes to SQL and is flagged. M2M transforms reach SQL only for a TDS query over a
  chain; constraints are checked in Java except in the validation helpers; a Relation class mapping's expressions go
  to SQL. The detector's guard moves named exactly: the getenv allowlist +1 and funnel-register rows.
- **~19:26 UTC, B1-R: the detector's shape note** (`compat-homework/A3-detector-shape-note.md` at `663f0e130` on
  `claude/compat-a3-detector`): two new edge classes, three changed (`PureV1Routes`, `TestDataRoutes`, `EdgeServer`),
  five routes (execute, Arrow, generatePlan, the three test-data routes); the setting read once at `EdgeServer.UPSTREAM`
  and passed down, an unknown value stopping the server's start; guard moves exactly 2 (`ENV_FLAGS` +1, funnel-register
  rows), pins 0, core hooks 0; the reach refined (host-side not flagged; user functions, qualified properties, a
  Relation `~func` mapping flagged; M2M only under a relation or TDS query; constraints only through validate or a
  DataQuality validation; dynaFunctions not; function values unverified); the literal-0 exemptions read from the
  request's runtime connection. **It still carried the load-time report** (crossed with the ~19:20 ruling): **GO given
  with the report dropped and refuse as the default** (`engine` not accepted until the user rules the opt-in);
  constraints and M2M confirmed; a new size asked for.
- **~19:27 UTC, B1-R building the detector** (shape note revision 2 on `claude/compat-a3-detector`): refuse the
  default, `warn` an operator's choice, `engine` not accepted; the load-time report dropped. About 1.5 to 2 days:
  about 380 main lines (`PureMeaningCheck` ~300, wiring ~80), about 400 test lines; guard moves `ENV_FLAGS` +1 and one
  funnel-register row, re-checked on the real code.
- **~19:29 UTC, #127's one red: macOS `parser_equivalence`**, `//parser-equivalence:parser_parity` timed out at 901.3 s
  against its 900 s limit on a slow runner (critical path 1220 s), no assertion failed; #127 touches nothing there; the
  lane passed on Linux and Windows and on macOS in #126's run. No re-run (#127 never merges alone; the batch's run is the
  evidence). The CI-health worker asked to measure parser_parity's times per OS on `main` and the smallest fix, read-only,
  so the batch does not trip on it.
- **~19:30 UTC, B1: H15's follow-up** (§8 at `bfa88b0189`; the Float check `H15-data/calendar-floats.tsv`). Of the 46
  tests with nested table texts, passing: **`main` today** DuckDB 39 (calendar 34 of 41, extensions 5 of 5), H2 32
  (calendar 32, extensions 0); **B2 as ruled, lite as it is** DuckDB 5, H2 0 (every calendar body refused by name:
  the product refuses `toCSV`, and the ~18:05 rulings gave only `toString` a database text); **B2 if lite had a CSV
  text** (legend-pure's layout, lite's V2 Floats) DuckDB 37, H2 30 (the two lost against today, testAnnualizedValue
  and testCmeValue, expect 15 and 16 decimals). 9 of the 41 expect more than six decimals, so no lite text passes them
  (2 pass today, 4 fail earlier at assertSameSQL, 3 fail at this text today). **println:** lite never evaluates it
  (no statement on either door), so the 13 nodes make no text and the takeover has nothing to do; but no register row
  names println (S128 names print's inertness, R18), an unledgered divergence; a one-line amendment to S128 closes it.
- **~19:35 UTC, B1: H15 §9 (toCSV's layout) and §10 (the S128 draft)** (`6b8c41345e`). `toCSV` is legend-engine's,
  not legend-pure's: `meta::relational::tests::csv::toCSV` over a legacy TabularDataSet in 4.145.0's
  `helperFunctions.pure`: each header name escaped like a cell, joined by `,`, then `\n`; rows joined with a `\n` after
  every row, the last included (no rows: `id\n\n`); an empty or null cell `''`; a text holding `,`, `"`, `\n` or `\r`
  wrapped in quotes with each `"` doubled; a Float cell its own `toString` (the same as a `#TDS` cell's); a Date
  `yyyy-MM-dd`; a DateTime `yyyy-MM-dd HH:mm:ss`, a space, whole seconds, no zone. A TDS's `csv` is a stored property of
  legend-pure's `TDS<T>` with no Pure printer. The S128 amendment drafted for `println` (legend-pure writes to standard
  output; lite plans no statement and writes nothing). **Asked next:** since `toCSV` is a legend-engine Pure PROGRAM,
  the world-map tenet says lite compiles its body (the database computing it with lite's natives) rather than
  intercepting it: is the body loaded as corpus input, why lite refuses it, and what the 46 tests and the corpus give
  with the interception removed (a scratch probe).
- **~19:37 UTC, the reviewer: A3 piece 4 APPROVED WITH 2 MUST-FIX, 6 SHOULD-FIX, 5 NITS** (record at `4d99d429`).
  Holds: the ~19:20 ruling stated right everywhere; AGENTS.md's invariants and numbering untouched; the six tenets and
  readings verbatim; ids as allocated; every number checked matches. M1: program wording quoted as the user's
  ("Pure's way, LOUD, no translator"; the goal's sentence introduced as "the user's ruling: ...") must be named as ruled
  by the user ~13:25 ("Okay agree with all"), not as their speech. M2: item 2's 147/152 split (H2 139/2/6, DuckDB
  143/3/6), naming the 2+3 answers the text hid. S1-S6 (what stage B adds; SEMANTICS_REGISTER's header and
  TOTAL_WALL_START_HERE's pointer; date the PCT table; divide-by-zero's source; the recipe's range; AGENTS.md's
  [CONVENTION] mark and tenet-vs-invariant numbering) and 5 nits. Sent to B2. **O1, this program's own plan:** §4a,
  the A3 row and the plain plan's row 1 still said "warn" and "with the report": corrected here to the ~19:20 ruling.
- **~19:43 UTC, B2: piece 4's review fixes in one commit** (`c957c2b49`, local; reading copy `2d16f6d54`): M1 (no
  program wording quoted as the user's; D1 named as ruled ~13:25, "Okay agree with all"), M2 (item 2 split H2
  139/2/6, DuckDB 143/3/6; new items 12, joinStrings with a separator, S386, and 13, firstDayOfWeek, S387), S1-S6 and the
  nits. Two more sent: firstDayOfWeek stated as on the merged batch (A5's H2 fix makes both databases answer Monday, so
  no H2 Gaps entry); legend-pure's `x / 0` read to the end. Then B2 sends it to the reviewer directly. The user also
  asked whether the door rewrite is really better than a compiler flag; answered with the trade (the flag is smaller
  and best on the corpus, but wrong for legend-engine's host-side constants and spread through every layer that
  touches `substring`); a simplification being measured by B1-R (swap each flagged call for A3's `sqlSubstring` /
  `sqlPosition`, the database's own functions, so no Pure library and no second model).
- **~19:45 UTC, B1-R: the swap measured** (H16's addendum at `f981e9d8e`). A3 piece 1's natives lower to exactly
  today's SQL (`sqlSubstring` is `SqlFn.SUBSTRING` with the arguments as given, `sqlPosition` is `SqlFn.STRPOS`), so
  the pinned batteries already give the swap's result: **H2 all 46 of legend-engine's answers; DuckDB 31 of 47.** The 16
  DuckDB misses are every start of 0 or below and every negative length: `sql/dialect/SubstringClamp` raises a start
  below 1 to 1 in the DuckDB, Postgres, Snowflake, Trino, MariaDB, SQLite, BigQuery, Databricks and ClickHouse
  dialects, and legend-engine sends DuckDB its SQL unclamped (e.g. `'Credit'->substring(0, 2)`: legend-engine `C`, the
  swap `Cr`). `indexOf` exact everywhere. Exact on every database needs a SUBSTRING the dialects leave unclamped: a new
  MIR variant (or a flag) only the edge's rewrite would produce, a core addition for the user (stopped there). The swap
  removes the 85-line library and the extended model (the rewritten lambda types against the request's own held
  model); user-function clones stay (one held rebuild per model and clone set) or such a request is refused by name in
  the first form. New size about 1 to 1.5 days on the detector with `engine` on H2 alone; clones about half a day more.
  Still a door rewrite, so still the user's amendment. The detector at `0896f7d39`, its gate, edge net and batteries
  running.
- **~19:47 UTC, the reviewer: piece 4 re-check 1** (record at `adcf56ee`): every reviewer item met at `c957c2b49`;
  this session's two additions (firstDayOfWeek on the merged batch, `x / 0` read to the end) not yet in. The reviewer's
  reading: once A2 removes the printer, H2's `testToSqlGenerationFirstDayOfWeek` is judged by its rows, lite (after A5's
  fix) answers Monday against the golden's Sunday, so on the merged batch H2's class b would be 3 and H2's counts 148 and
  151, the H2 fail roster carrying the test; the batch's H2 corpus lane settles it, and the page and the roster must
  agree. O1's two remaining phrases in the final plan (the A3 row's default; §4a's "What it does") corrected here. Nit:
  S386 cites `joinStrings.pure:18-21`, not `:23`.
- **~19:47 UTC, the CI-health worker: parser_parity measured** (read-only). Its lane on `main`: macOS 561-582 s,
  Linux 265-288 s, Windows 447-476 s; on #124: macOS 361-420 s. #127's macOS runner was about 2x slow on everything
  (critical path 1220 s against 530-768 s). macOS runs it alone (`--local_test_jobs=1`), so normal runs use 40-65% of
  the 900 s limit (junit_test's default size "large"; no guard reads a size). **The cause:** 71.6% of CPU is inside
  legend-engine's grammar parser (the referee); four classes (ModelComposerParityTest, CorpusSweepTest,
  ModelReaderParityTest, ComposerParityTest, 93% of the time) each parse every corpus source themselves, where
  CorpusSweepTest's own design is one referee parse per source. **GO given for the cause fix:** one per-JVM memo of the
  referee parse per source (keyed by source and options, held softly), no size, timeout or shard change; estimated 284
  s → about 130 s locally; branch `claude/compat-parser-parity-memo`, assertions proven unchanged, reviewed, riding in
  the batch.
- **~19:48 UTC, the judges: the batch merged** (head `b1dc377dc` on `claude/compat-a4-host-only`; the one measure
  running). A2 (`6551c2036`) merged with no conflicts, then A5 (`6f21bed29`, carrying A1): seven conflicts (the five
  named, plus the two edge-net corpus ledgers both re-recorded), resolved by the rules; one compile break outside them
  (A5's `RootLoopsRejectionTest` used the old six-field `RootLoop.Row`), adapted with what it proves unchanged;
  `PctPlanRoute` A5's. Regenerated: the lean-SQL baseline (statements unchanged, DuckDB 2,319, H2 2,313; every move
  A5's) and the edge-net corpus ledgers (DuckDB equal to A2's with the words stripped; H2 too, but
  `testToSqlGenerationFirstDayOfWeek` PASS → FAIL, the predicted move). **A guard A4 left silent, for the user's
  word:** channel B's judge-SQL ratchet measured only under COMPAT and BOTH on the reference lane; after A4 COMPAT runs
  the host judge, so the ratchet measured nothing. In the merge it moves to the LITE-PCT lane (lite's rules, both
  judges, DuckDB), the only place the database judge still runs, re-pinned at that lane's numbers (essential
  statements 466 → 463, characters 1.73M → 7.19M; relation 469 → 106 statements; a different lane's baseline, not
  growth). Taken to the user: keep it there (recommended) or drop the judge pins.
- **~19:53 UTC, B1: H15 §11, toCSV as the program it is** (`5ee5af2f09`; probe `4ffa125d1d`, scratch). The body
  IS corpus input (the corpus walks all of `core_relational/relational`), but lite intercepts the name: the three
  `toCSV` signatures are registry natives (`Pure.java:1871-1873`, "the RENDER phase's CSV text (F4.2): platform-owned
  lowerings"; moved to the edge by D1.3/S316, 2026-11-15), the Lowerer's TO_CSV arm asks the `RelationTexts` facet, and
  the judges read it by name. Removed, the body types and plans as a program; it needs two platform constants lite
  lacks (`SimpleDateTimeFormat()`, `ISO8601DateFormat()`). **Corpus with the program:** the 46 nested tests DuckDB 22
  (39 today), H2 33 (32); every toCSV test (170) DuckDB 112 (158), H2 94 (150, 5 gained); nothing else moved. Losses:
  14 DuckDB calendar tests compared EXACTLY now (today a rendered text within 2 ULP, S132; legend-pure compares
  exactly); 16 DuckDB a null cell vanishes (lite holds SQL NULL, no TDSNull); 14 per lane a column the TDS gains at run
  is missing (the typed schema at planning); 47 H2 no list encoding; 2 DuckDB binder errors. The relation's `toString`
  is a Pure program too, but PLATFORM (`meta::pure::functions::relation`, `PCT.platformOnly`): lite owns it. A TDS's
  `csv` is a stored property, no printer.
- **~19:54 UTC, B2: S387 corrected** (`claude/compat-a3-docs` at `5b337b867`, local; reading copy `febd1adee`; sent to
  the reviewer). **Upstream read and measured:** Pure's first day of the week is the most recent Monday
  (`dateExtension.pure:202`); legend-engine's H2 SELECTs all go through its dialect translation
  (`relationalMappingExecution.pure:440-443`), which writes `cast(date_trunc('week', d) as date)`
  (`toPostgresModel.pure:356`), and H2 starts that week on the JVM locale's first day: the Sunday under en_US (the
  corpus golden), the Monday under en_GB and fr_FR; the H2 extension the reviewer read (`:223`) is reached by no H2
  SELECT; legend-engine's DuckDB gives the Monday under every locale; lite after A5 the Monday everywhere. **Both
  meanings already ruled by the user:** 2026-09-05 (GATES.md batch 67b, joinStrings and firstDayOfWeek "are both just
  broken on H2 basically? … quarantine/bucket those as engine bugs") and 2026-09-08 (batch 144, the 11 accepted
  divergences, including firstDayOfWeek-h2-week-start, digest-joinStrings, joinStrings-literal-collection); S386 and
  S387 cite both. **A finding for the batch:** four of those user-accepted divergences (DuckDB testHashFunctions,
  testToSQLStringForTDSStringJoin, testToSqlGenerationFirstDayOfWeek; H2 testToSQLStringForTDSStringJoin) went to the
  FAIL rosters under A2 (class b), not back to the accepted rosters (MinimalCorpusTest's accepted register, matched by
  witness); with A5, H2's firstDayOfWeek likely joins them. Moving them is a guard-file move, for the user's word.
  **A product-track finding:** lite's H2 still writes `extract(WEEK ...)` and `DATEDIFF(WEEK, ...)`, which follow the
  JVM locale (week 49 under en_US, 48 under en_GB): `weekOfYear` and a WEEKS duration answer differently by machine,
  a G10 gap; A5 fixed only the truncation.
- **~19:58 UTC, the reviewer: A3 piece 4 APPROVED at `5b337b867`** (record `ce90f3df`). firstDayOfWeek's correction
  read at the pin and holds; the referee's Sunday is stable because every test JVM pins en_US
  (`tools/junit/defs.bzl:53-54` and the corpus targets); S386 and S387's rulings are the user's words in GATES.md (batch
  67b, 2026-09-05; batch 144, 2026-09-08, "i agree with all 11"). Outstanding: legend-pure's `x / 0`, read by the
  reviewer: compiled legend-pure raises "Cannot divide <right> by zero" (`CompiledSupport.divide:1481-1486`);
  interpreted, a Float or Decimal operand raises BigDecimal's ArithmeticException, an Integer over an Integer 0 answers
  Infinity. Conditions at the batch: the merged H2 lane settles item 2's H2 figures and S380; the user's word on the
  PROPOSED rows (S130, S134, S380, S382-S385). The H2 week-locale defect needs an owner and a record: added to the
  page's Gaps for the product track. Sent to B2 for one last commit, then its push on the user's word.
- **~20:00 UTC, B2: piece 4's last commit** (`9dc385235`, local; reading copy `f6dc36008`; sent to the reviewer).
  legend-pure's `x / 0` read to the end at 5.99.0 AND run (the pinned `NumericAccumulator` compiled and driven as
  `Divide.java` does; `divide-by-zero-probe.md`): **both modes raise for every zero divisor**: compiled "Cannot divide
  0 by zero" (`CompiledSupport.java:1481-1486`); interpreted Integer / Integer is a double division whose Infinity or
  NaN then fails as a Pure Float with a NumberFormatException, and a Float or Decimal operand raises BigDecimal's
  "Division by zero". (This corrects the reviewer's reading that the interpreter answers Infinity.) H2's count worded
  "147 or 148", pending the merged lane. **For B1's error-record backlog:** the edge's `PureMessages.java:26-29` words
  Integer / Integer's error "Division by zero" (wrong mode and words), and cites `CompiledSupport:1485` for rem's message
  (that line is divide's; rem's is `CoreHelper.java:544`). Sent to B1.
- **~20:02 UTC, the reviewer: A3 piece 4 APPROVED at `9dc385235`** (record `7fc86465`), every item met; the
  reviewer corrected its own re-check 2 (legend-pure raises for `x / 0` in both modes; the interpreter's Integer path
  throws a NumberFormatException at `new BigDecimal("Infinity")`). Open: the H2 count's "147 or 148" replaced by the
  judges' number before landing; the user's word on the PROPOSED rows at the PR. It will check A2's placing of the
  user-accepted divergences in the batch review (a must-fix there if it holds, by the user's 2026-09-08 ruling of a
  fourth status). B2 asks the user for the push of `claude/compat-a3-docs`.
- **~20:02 UTC, B1: the error-record backlog started** (`compat-homework/B1-error-records-backlog.md` at
  `49c51bfaf5` on `claude/compat-b1-homework`): divide's and rem's sites read at the pin (divide prints the divisor,
  `CompiledSupport.java:1485`; rem the dividend, `CoreHelper.java:544`); the typed record should name the operation.
- **~20:02 UTC, the translator: A3 piece 1 measured** (local `2a05b904` on `claude/compat-a3-frontends`; record
  `a3/piece1/` at `48801791` on the spike branch). The corpus on both lanes green with 0 moves; IQ and SQL parity green;
  Cypher parity 3 of 4 (the showcase pin `hard/pure.txt` moves, its SQL does not); the rest of `//gates:local` green
  but the guards. **Guard moves for the user's word, none made, more than predicted (9 goldens and 4 guards):**
  `NativeCatalogGovernanceTest` INTERNAL_DESUGAR 38 → 40; `PlatformNamesGuardrailTest` FUNCTION_FQN_LITERALS_MAX 346 →
  347; spec `ratchets.tsv` Intrinsic 735 → 738; `CatalogUpstreamDiffTest` NOT_UPSTREAM_MAX → 76; 9 text goldens
  (CypherToPureTest ×4, IqFunctionsTest, IqToPureTest, PostgresToPureTest ×2, the Cypher showcase). The translator asks
  the user in its session for these and its push.
- **~20:03 UTC, B2: A3 piece 4 final** (`claude/compat-a3-docs` at `3780307f1`, local; reading copy `38728c10a`;
  `//gates:checks` 197 pass): `x / 0` one statement; item 13 dated with this batch; the H2 week defect in the Gaps. Two
  wordings corrected at the source: legend-pure's interpreter fails on Integer / 0 with a Java NumberFormatException,
  never answering Infinity; **the product server `//core:server` pins no locale** (only the test launchers pin en_US),
  so even `bazel run //core:server` answers `weekOfYear` and WEEKS durations by its machine's locale. B2 asks the user
  in its session for the push (a new branch, no PR).
- **~20:05 UTC, the user ruled the batch's open decisions** (§1's row): the `engine` opt-in yes, with the right design and no shortcuts; the raw SUBSTRING only if it is the right design; build in this batch; the judge-SQL ratchet kept and re-based; `toCSV` printed by the database in lite's route; S128 amended for `println`. Asked: the switch rule explained, and which tests move to the accepted rosters.
- **~20:05 UTC, B1-R: the detector's full gate** (`0896f7d39`): 594 pass, 1 skipped, 16 fail (the 12 root-refusing
  Postgres targets and four of the detector's). Three need the user's word: **(a)** the literal-0 exemption table
  names H2 and DuckDB, which `DialectLeakRatchetTest`'s ban refuses outside a database's owners; proposed: the table in
  `PureText`, the edge's dialect registry (a listed owner), `PureMeaningCheck` asking it by the connection's type, so
  `DialectBoundaryTest`'s pin for `PureText` goes 1 → 2 with a dated note (rejected: (b) a dialect method only the edge
  reads, a product member; (c) no exemptions, refusing H2's equal `substring(s, 0, n)`); **the parser-equivalence
  ratchet** `own_corpus.matched` 3310 → 3312 (the test's two models; A2 moves it 3310 → 3311 too, so regenerated once on
  the merged tree); **`native-claims.tsv` regenerated** (the generated inventory now lists `PureMeaningCheck` among the
  two natives' claimants). Taken to the user, recommended yes.
- **~20:10 UTC, the detector's three guard moves ruled yes** (§1's row); B1-R told.
- **~20:13 UTC, the CI-health worker: the parser_parity memo built** (`5a6ce154a` on
  `claude/compat-parser-parity-memo`, not pushed; it asks the user in its session). `RefereeParses` (new) keeps the
  referee's answer per text and source-information option, held softly; the four classes switched (+16 / −11); no size,
  timeout or shard change. **284.7 s → 176.7 s locally (−38%).** 9,731 texts parsed, 28,176 answers reused, 0 let go.
  Heap: live peak 616-665 MB → 817 MB; peak before a collection 2,328-2,337 → 2,548 MB of 2,816 (watch on CI). Proof:
  the same 45 testcases and outcomes; 22 of 23 outputs byte-identical (the 23rd a never-asserted diagnostic whose crash
  messages read the JVM's NPE text, the fast-throw optimisation); two planted differences fail the same testcases with
  the same messages on `main` and the branch. For the reviewer once pushed.
- **~20:24 UTC, #124's run 44580527071 finished:** "gates green" SUCCESS; every Linux, Windows and arm job green;
  macOS green but two jobs cancelled for want of a runner (`corpus_duckdb`, `checks`). The same code on macOS (#127's
  run, which merges #124's branch): `corpus_duckdb` green at 20:21, `checks` running. **Reviews dispatched:** A2
  (`6551c2036`) and A4 (`e91005cc6`) as pieces, then the batch merge once measured. The memo design check sent to the
  CI-health worker (share the referee's serialized text, immutable, if every class uses only that form).
- **~20:32 UTC, the reviewer: A2 and A4 as pieces** (record `db6ee45e`). **A2 at `6551c2036`: APPROVED WITH 1
  MUST-FIX.** Holds: the printers deleted, `LiteSqlText` package-private and value-free, the rosters exact (+147, +152),
  the edge-net ledgers moving on the roster's tests plus those already failing, every guard move dated; for the user's
  word: `LiteSqlText`'s closed-list entry, S380, the 12-dialect widening. MUST-FIX: rung 2a (`7dfbdf601`, 2026-09-21) had
  taken four batch-144 rows off the accepted rosters; A2 sent them to the FAIL rosters, the accepted rosters untouched.
  Restore verbatim: DuckDB `testHashFunctions` (joinStrings-literal-collection, witness `aceae941...`),
  `testToSQLStringForTDSStringJoin` (`Anthony Allen[,]`), `testToSqlGenerationFirstDayOfWeek`
  (firstDayOfWeek-h2-week-start, `2014-11-30`); H2 `testToSQLStringForTDSStringJoin`. New, the user's word: H2
  `testHashFunctions` (never accepted on H2) and H2 `testToSqlGenerationFirstDayOfWeek` on the merged batch. Counts
  then: A2's fail rosters H2 +145 (or +146), DuckDB +149; accepted H2 41 → 42 or 43, DuckDB 46 → 49. **A4 at
  `e91005cc6`: APPROVED** with S1 (name refused 108 → 115 in the lean-baseline note), S2 (refuse a non-HOST
  `-Dpct.channelb.judge` by name; date-point the docs that say `=DATABASE`), S3 (a stale comment), sent to the judges
  for the batch branch; its proof gap closes with the merged run.
- **~20:35 UTC, the user ruled decisions 2 and 3** (§1's row): the switch rule in its door-only form (the rules of
  record and B5's row), and the six accepted divergences (four restored, two new on H2). The user re-runs #124's two
  cancelled macOS jobs; #124 merges once they are green. Fast dev loops for the rest of stage A: per change the fast
  loop (TOTAL_WALL_START_HERE §8.1, about 10 minutes, plus the touched packages); one `//gates:local` per piece before
  it joins the batch; one measurement on the batch; no uncached re-runs of unaffected suites.
- **~20:37 UTC, B1-R: the detector DONE and green** (`claude/compat-a3-detector` at `64ead02b7`, pushed).
  `//gates:local`, `//gates:edge_net`, `//pct:batteries_test` and `//core:guardrails_edge`: 598 pass, 1 skipped; the 12
  root-refusing targets pass as `ubuntu`. The three ruled moves in with dated notes (`PureText.ZERO_START_AS_PURE`, its
  pin 2; `own_corpus.matched` 3312; `native-claims.tsv`). `PureMeaningCheck` on the five routes; refuse the default,
  right after typing, so a refused request reaches no runner (a test proves it); `warn` an operator's choice;
  `engine` not yet accepted; the setting read once, at `EdgeServer.UPSTREAM`. 22 tests, each against a plant (the three
  needing piece 1 pass on its patch, 25 of 25, and join at the rebase). Next: the `engine` design note, fast loop.
- **~20:38 UTC, B2: piece 4's fold-ins** (`caca6a69b`, local; reading copy `31d3f11bb`; it asks the user for the
  push): S128 amended for `println`; S316 amended for `toCSV` (in `d-register-rows-2026-11-16.md`, where D's rows
  live); item 14 the toCSV line; item 1 the `engine` setting (read at the legend-engine door alone); AGENTS.md's tenet 6
  points to the switch rule; the rules-of-record copy refreshed. **Corrected:** this session's relayed H2 count was
  wrong; with six rows ruled, H2 accepted 41 → 44 and fail +145, DuckDB accepted 46 → 49 and fail +149 (B2 and the
  judges told). **The 2026-11-15 rule ("no PR edits a shared document"):** the AGENTS.md and G8 edits are ruled
  specifically (stage A's plan); the register rows enter the register once, in the batch PR, as D's rows were to "move
  into the register when D lands".
- **~20:40 UTC, the user: "Don't need approval to push to branch do you? / What guards are changing exactly in
  English / Yes on CI parser".** Relayed to the CI-health worker (push the memo), B2 (push the A3 docs) and the
  translator (push piece 1; its guard moves still need the user's word with dated notes): each pushes if its own
  session's rules accept a relayed word, else keeps its ask there. The user's 10:45 ruling already lets program sessions
  push their own `claude/compat-*` branches; three sessions' own start instructions or permission checks require the
  word in their session.
- **~20:40 UTC, the CI-health worker: the memo holds the referee's TEXT only** (`37ef44ff0`, amended locally, not
  pushed). All four classes use only the serialized form, so the engine's object is let go; each element's text held
  once and the whole model's JSON rebuilt as its exact bytes. **284.7 s → 168.8 s (−41%)**; live peak 616-665 → 827
  MB; peak before a collection 2,328-2,337 → 2,429 MB of 2,816 (lower than the object memo's 2,548). Proof against
  `main`: the same 45 testcases and outcomes; 22 of 23 outputs byte-identical (the 23rd an unasserted diagnostic); both
  plants fail the same testcases with identical messages. It pushes and sends the reviewer the proof once the push is
  allowed.
- **~20:40 UTC, A5: #127's CI finished:** 54 of 55 lane checks green (Linux 16/16, Windows 16/16, arm 1/1, macOS
  15/16); the one red macOS `parser_equivalence`, `parser_parity` timing out at 901.3 s, no assertion failing (the
  memo fixes it). A5 done until the batch closes #127.
- **~20:40 UTC, B1-R: the `engine` rewrite's design note** (`compat-homework/A3-engine-rewrite-design.md` on
  `claude/compat-a3-detector`). (1) **One new native, the database's own SUBSTRING as given**
  (`SqlFn.SUBSTRING_AS_GIVEN`, untouched by `SubstringClamp`): legend-engine's uncorrected `substring` is
  `processDynaFunction('substring', ...)` with the arguments as given (`pureToSQLQuery.pure:4658-4664`), and H2, DuckDB,
  Postgres and Snowflake each write the plain call; H5's library and the clamped swap rejected; `indexOf` reuses piece
  1's `sqlPosition`. (2) H2 and DuckDB first, one `ENGINE_FACTS` line in `PureText` (the literal-0 table folded in, the
  pin stays 2); others refused by name. (3) Helpers rewritten after the inliner through the public API
  (`UserCallInliner.inlineBody`, replace by position, `withBody`): no clones, no rebuilt model, byte-identical SQL on
  four probes. (4) M2M chain transforms and `~func` mappings refused by name (reached only in phase H). (5) No error can
  name the substitute. (6) The batteries pinned under `engine` per database. (7) About 2.5 days: B1 the core native (~1
  day) in parallel with B1-R's edge rewrite (~1.5). Sent to the reviewer; B1 briefed to read and wait.
- **~20:41 UTC, #124's re-run:** 53 of 54 green, none red; macOS `corpus_duckdb` re-run PASSED (20:33), macOS
  `checks` (attempt 2) running. The CI-health worker's permission check refuses a relayed push word ("Unsanctioned
  Communication"); it has asked the user in its own session.
- **~20:41 UTC, the reviewer: the detector APPROVED WITH 2 MUST-FIX** (record `88e3def8`). Holds: it never changes a
  query; it refuses before anything runs on every route (runner count 0); the setting read once at
  `EdgeServer.UPSTREAM`; no database named outside `PureText`; the reach in code matches §4a; the ~20:10 moves dated; 21
  of 22 tests bite. M1: three reaches untested (unverified; M2M under a TDS chain against graphFetch; a constraint via
  validate): add tests, each failing on its plant. M2: `aConstraintIsNotFollowed` cannot fail on H2 (a literal-0 start
  is exempt there): use `substring(1, 2)`. S1: the javadoc quotes program wording as the user's; S2: an unresolved
  mapping reported unverified, never clean. Proof gap: record `core_edge_tests`, the pure/v1 goldens and the batteries.
  The merged `own_corpus.matched` is regenerated on the merged tree. Sent to B1-R.
- **~20:41 UTC, the translator: A3 piece 2 built locally** (`claude/compat-a3-switch`: `2f01b45b` the core switch and
  S383's raise, `c2cb56bf` the PCT adapter's option off, on A2's `6551c203` plus piece 1); its fast loop, PCT DuckDB A
  and B and the corpus lanes running. It will not push on a relayed word (its start rule: "push only to
  claude/compat-translator-spike"); its three asks in its session were cut off by worker restarts; it asks again there.
- **~20:45 UTC, the user approved every guard move that goes down** (§1's row); moves up, new closed-list entries, goldens and rosters still need the word. Relayed to the builders.
- **~20:43 UTC, B1: the raw-SUBSTRING native's plan** (no build until the design is approved): two lite-internal
  signatures `meta::legend::lite::substringAsGiven` (2 and 3 arguments), one Scalars rule to `SqlFn.SUBSTRING_AS_GIVEN`
  (arguments as given), a label on each dialect's existing SUBSTRING case (SubstringClamp untouched), tests per dialect
  and on H2 and DuckDB against the database's own SUBSTRING; guard moves INTERNAL_DESUGAR 40 → 41, native-claims,
  SqlFnSortedTest's PRODUCT set +1, EdgeOnlyDataTest checked. **Corrected:** base on the translator's piece 2 branch (A2
  removes the engine-style printers, so no labels there). **Its question, to the reviewer:** the native stays
  lite-internal (edge-only vocabulary, one EdgeOnlyDataTest entry for the user's word; this session's leaning, since a
  user-facing per-database SUBSTRING would break G2/G10 for lite's own users) or goes on LITE_SURFACE.
- **~20:44 UTC, the reviewer: the `engine` rewrite's design APPROVED TO BUILD** (record `f4928599`). The raw
  SUBSTRING is "the right thing to do" in the ~20:05 sense (legend-engine's `processSubstring` is
  `processDynaFunction('substring')` over the arguments as given; H2, DuckDB, Postgres and Snowflake write the plain
  call, all read); the alternatives rightly rejected; the switch rule kept; no shortcut (every unmeasured or unreachable
  case refused by name). D-M1: rewrite by the detector's walk on the inlined body, by identity, not by position (a
  helper's inlined copies share its position); D-S1: the native is core vocabulary only the door reaches, to be
  registered as the door's by the user's word, or one native serves piece 1's islands and `###Relational`
  dynaFunctions too; D-S2: the substitute's callee from the registry; D-S3: cite H2's real route (the dialect
  translation); D-S4: no engine-style printers. B1-R and B1 told to build (B1 on the translator's piece 2 branch once
  pushed); D-S1 to the user.
- **~20:46 UTC, #124 MERGED to `main`** (`8eea8fa13`, a merge commit), on the user's word of ~20:35 ("I'm rerunning
  the failed 124 jobs then we merge"), after run 44580527071 went green on every OS on attempt 2 (54 of 54; the re-run
  macOS `corpus_duckdb` and `checks` passed; no 404 or 429 in any lane; Windows `warehouse` green with markets_test at
  64-99 s). **Stage A's A1 is on `main`** (#123, #124, #125, #126). The judges merge `main` into the batch branch once.
  B2 pushed `claude/compat-a3-docs` at `caca6a69b` on the user's word in its session; it classifies A2's guard moves
  (down ones noted by the ~20:45 ruling, applied by the judges on the batch branch) and builds A2 PR 2 (the plan-text
  family's spelling-only code) for the batch. The judges: A4's down moves noted; the batch branch at `90588a767` (S1-S3,
  A2's PureV1SqlTextTest merge fix, the six accepted rows), the rosters re-measuring.
- **~20:46 UTC, the reviewer corrected D-S1** (record `ca8ea4a5`): `substringAsGiven` stays lite-internal (agreed:
  a user-callable per-database SUBSTRING breaks G10); but `EdgeOnlyDataTest` cannot hold it (Scalars naming
  `SqlFn.SUBSTRING_AS_GIVEN` is a real product use, so the exact test would fail). The honest record: `native-claims.tsv`
  (claimers: the edge's rewrite and Scalars); INTERNAL_DESUGAR +1 (41) with its comment amended for this one door
  producer; SqlFnSortedTest's PRODUCT +1; optional, for the user: a new pin that the row has no product claimer but
  Scalars. B1 told; the user asked. B1-R confirmed #124 green 55 of 55 and is on the detector's review fixes. B2's docs
  branch at `46805b9ef` (the H2 counts corrected).
- **~20:47 UTC, the parser memo pushed** (on the user's word in the CI-health session, "Push parser test speed up"):
  `claude/compat-parser-parity-memo` at `37ef44ff0`, sent to the reviewer; it merges into the batch plainly (no file
  in common with #124).
- **~20:48 UTC, the reviewer: the parser memo APPROVED** (record `b029e84e`): assertions, cases and plants unchanged;
  only immutable Strings held; the heap no CI risk (soft references cleared before an OutOfMemoryError; live peak 827
  MB); no size, timeout, shard or guard change. Should-fix: never memoize a VirtualMachineError. Sent to the CI-health
  worker.
- **~20:50 UTC, A3 piece 1's up moves approved by the user** (§1's row); relayed to the translator, whose own rules need the word in its session too.
- **~20:52 UTC, the parser memo final** (`7f36f2730`: an OutOfMemoryError never kept; parser_parity green in 167.3 s,
  same counts); handed to the judges to merge into the batch branch. The translator confirmed it needs the user's word
  in its own session for piece 1's guard moves and its pushes.
- **~20:56 UTC, B1-R: the detector's review fixes** (29 tests green; seven plants each failing a named test; M2 proven
  on the old constraint). The parser-equivalence ratchet `own_corpus.matched` 3312 → 3315 (four new one-line test
  declarations, three parse alike on both parsers): **covered by the user's ~20:10 ruling** (the ratchet regenerated
  for the detector's test models; a matched-count floor rising strengthens the guard); expected 3316 on the merged
  tree. **Two product-track gaps found:** the pure/v1 door types through `Compiler.query`, which skips
  `ValidateDesugar`, so a `validate` request is refused at the door, never answered; a packaged runtime's
  `ModelChainConnection` does not reach the typed context (StressServiceSuitesTest's "has 0 mappings"); in both, no
  wrong answer escapes.
- **~20:55 UTC, the user asked how to do `substring` correctly and expertly.** Answered: three meanings, three named
  functions (Pure's `substring`, computed in the database and raising like Pure; lite's `sqlSubstring` for its SQL-family
  languages, one meaning everywhere; `substringAsGiven`, the database's raw call, door-only, opt-in). Flagged: lite's
  `sqlSubstring` today copies H2's quirk (a start below 1 counts as 1, `'Cr'`), where the SQL standard, DuckDB and
  Postgres give `'C'` for `SUBSTRING('Credit' FROM 0 FOR 2)`; proposed for the product track, measured first.
- **~20:57 UTC, the judges: the batch branch assembled** (`claude/compat-a4-host-only`, pushed): A2, A5 (with A1),
  `main` (#124) and the parser memo merged; regenerated on the merged tree: the lean-SQL baseline, the edge-net ledgers,
  the judge-SQL ratchet (LITE lane), parser-equivalence's ratchets (own_corpus.matched 3309 on the batch: A2's move was
  DOWN, 3310 → 3309); the reviewer's S1-S3; A2's PureV1SqlTextTest set against S0's net format; the six accepted rows.
  **The merged rosters:** DuckDB fail 257 (main 108, +149), accepted 46 → 49; H2 fail 421 (main 276, +145), accepted
  41 → 44; H2's `testToSqlGenerationFirstDayOfWeek` accepted (witness 2014-11-30); every new accepted row left its fail
  roster, no other roster or register moved. The ~20:45 ruling cited on A4's 32 down notes and A2's 16 down sites;
  `LiteSqlText.java`'s closed-list entry waits for the user's word. The last full measurement before these fixes:
  652 pass; its 16 failures were the 12 root-refusing Postgres targets, 3 killed by memory pressure (re-run alone), the
  PureV1SqlTextTest merge (fixed) and the H2 roster's firstDayOfWeek (now accepted). Now running: the fast loop and the
  touched lanes; then one full measurement on the head. B1-R corrected its ratchet note (elements, not models). B2 sent
  the merged numbers.
- **~21:02 UTC, the parser memo at `6c4509e0c`** (the planted OOM test; parser_parity 164.0 s, 47 testcases): sent to
  the reviewer for a quick re-check; the judges merge the new head into the batch before the full measurement.
- **~21:08 UTC, A3 piece 2 (the translator, `claude/compat-a3-switch`, local):** built (the core switch to Pure's
  meaning for `substring`/`indexOf`, the S383 raise, the PCT adapter, a raise test). Piece 1's moves made locally
  (`3a75adc4` counters, `a4c1f2b1` nine goldens) on the user's word in that session. Told the translator: the down moves
  (63 PCT rows, 4 unordered lines per database) are standing-approved with a dated note; rows on the 10 unmeasured
  databases stay until the batch's CI run measures them; the raise's legend-pure text is read from the engine runner,
  with its JDK recorded in S383, never a JDK's wording by recall. Still the user's word there: the push of both branches,
  +7 corpus fail-roster rows per database (up), 9 re-pinned tests, 9 edge-net re-records.
- **~21:10 UTC, the user ruled the three `substring` points yes** (§1's row). B1 (session_0195rQEnAa5CYXHygAxEFh3Y) told: the four recording moves approved, anything else (its "COMPILER_OWN") comes back first. The translator had already been given the push and piece 2 word by the user in its own session. The `sqlSubstring` item added to the final plan's product-track list.
- **~21:07 UTC, the reviewer approved the parser memo at `6c4509e0c`** (recorded at its `d9695125`): the refactor
  neutral; `RefereeParsesTest` red with the old catch either way. Ready for the batch (the judges merge it).
- **~21:08 UTC, the translator's piece 2 breakdown:** the 63 PCT rows are 3 `indexOf` tests × (11 channel-A classes +
  10 channel-B lanes), measured on DuckDB A and B, the rest predicted from identical ledgered texts. Refined the
  instruction: measure every lane runnable here; for the others, remove on prediction (dated "measured by the batch's CI
  run") only if a stale known failure turns its lane red, else keep and trim after CI. The unordered register's fourth
  line is `testToSQLStringSubstringIsUncorrectedByDefault` (in A2's real register). S383's legend-pure text: JDK 21
  says "Range [b, e) out of bounds for length n", JDK 17 "begin b, end e, length n"; the engine runner decides.
- **~21:10 UTC, B1-R: the detector's review fixes pushed** (`claude/compat-a3-detector` at `337e66ade`, one commit on
  `1dcc4259a`): M1, 7 tests (29 in all), each failing on its plant P1-P7; M2, the constraint `substring(1, 2)`, the old
  `(0, 1)` plant measured to leave both green; S1, S2 done; own_corpus.matched 3312 → 3315 under the ~20:10 ruling
  (3316 expected merged); 55 targets green (fast loop plus touched lanes); the golden net regenerated identical;
  LiteBatteries under `warn` identical. The two gaps (the validate door, the packaged chain) recorded for the product
  track. Sent to the reviewer for the re-check and to the judges to merge into the batch.
- **~21:12 UTC, B1's three questions, ruled here under the approved design (lite-internal, door-only):**
  (1) `substringAsGiven` joins `Pure.COMPILER_OWN` (a product set, not a guard; B1 measured that without it a user's
  `|meta::legend::lite::substringAsGiven('abc', 1, 2)` compiles), with a test that a user is refused by full name, by
  signature id and under eval; B1-R to prove the five routes run the rewritten node under `engine` (nothing re-resolves
  it by name). (2) MongoDB's arm refuses SUBSTRING_AS_GIVEN (invariants 3 and 4: its `$substrCP` clamps, so "as given"
  would be a fallback under a false name). (3) One edge class `EngineSubstitutes` (substring → substringAsGiven,
  indexOf → sqlPosition, one `checked` path). B1's measurement: 11 relational dialects write the plain call; the same
  arguments' `SUBSTRING` clamps on 10 of them (all but H2); H2 and DuckDB 72 of 72 equal to the database's own call.
- **~21:11 UTC, the reviewer approved the detector at `337e66ade`** (recorded at its `84d8d21f`): M1, M2, S1, S2 and
  the proof met; 3316 on the merged tree answers its batch note; the two gaps rightly loud refusals for the product
  track. The ~21:12 rulings consistent with the design, on one condition passed to B1: `native-claims.tsv` names the
  door's class `EngineSubstitutes` as `substringAsGiven`'s producer.
- **~21:15 UTC, the judges: the batch branch pushed** (`claude/compat-a4-host-only` at `5ab730cd`): before A3, 46
  targets pass (the core set, the edge set, spec, both corpus gates with the rosters and the six accepted rows,
  pct_channel_b, parser_equivalence); then merged the memo `6c4509e0c` (no conflicts) and the detector `337e66ade` (one
  conflict, the parser ratchet's history, both lines kept). **own_corpus.matched on the merged tree: 3314**, measured
  (3310 − 1 for A2's corrected DOWN + 2 + 3 for A3; B1-R's 3316 assumed A2's uncorrected +1); checked here against main
  and the detector's lines; the reviewer told. A3's fast loop running on the merged tree; the full measurement waits for
  the last pieces.
- **~21:16 UTC, the translator: S383's raise text, read and measured.** The engine runner cannot answer: every storeless
  function test (the in-range control included) errors with "package
  org.finos.legend.engine.plan.dependencies.store.shared does not exist" (legend-engine compiles the function to Java;
  the @maven_runner pool lacks the plan-dependencies jar). The pinned sources: no test pins the text; legend-pure's
  substring is the JDK's `String.substring` with no message of its own (`SubString.java:56,61` interpreted,
  `CompiledSupport.java:1546-1556` compiled). The JDKs measured: remotejdk_25 25.0.4 (lite's and the runner's) and the
  container's 21.0.13 both say "Range [-1, 6) out of bounds for length 6". **Ruled here:** no pool change in the batch
  (the source reading settles the text); the runner's gap recorded as a tooling finding for the user after stage A; the
  translator to read, file:line, whether legend-pure wraps the exception on either path before pinning, so S383 pins the
  text a legend-pure user sees and says which part is the JDK's. Piece 2's PCT essential lanes running on SQLite,
  MariaDB, MySQL, ClickHouse, Trino, MongoDB and Postgres for the rows by lane.
- **~21:17 UTC, the reviewer confirmed 3314** on the batch at `5ab730cd` (its record `9a557188` corrected); it checks the
  value again in the combination review, which waits for the judges' merged measurement, the restored batch-144 rows
  and the two new H2 rows.
- **~21:18 UTC, S383's wrap read at the pin (legend-pure 5.99.0):** interpreted, a native's non-Pure RuntimeException
  becomes `PureExecutionException(source, e.getMessage())`, printed `Execution error at (resource:… line:… column:…),
  "<JDK text>"` (`FunctionExecutionInterpreted.java:883-893`, `PureException.java:97-111, 257-259`); compiled,
  `RuntimeException("Error executing <signature>. " + e.getMessage())` (`FunctionExecutionCompiled.java:247-264`).
  Accepted: lite pins the JDK's part (as for every raise kind, e.g. DIVIDE_BY_ZERO's info alone); the envelopes are the
  presenter's (B1's error records). The row text goes to B2.
- **~21:30 UTC, the user: "Yes put the questions together but put them in context and with pros/cons and implications
  and alternatives"** (to this session's offer of stage B's open questions while A finishes). Written:
  `compat-stage-b-questions-2026-11-19.md`, 13 questions (how B runs and lands; B1's typed raise slots; B3's catchable
  size error; B2's window-in-list prerequisite, default violation message, defects cell and done-when 3; B5's legacy
  null-equality flag and the first switch's removal date; the cross-kind meaning; the engine runner's storeless gap;
  `engine` on more databases), each with context, choices, pros and cons, and a recommendation. Nothing ruled yet.
- **~21:25-21:29 UTC, four pieces moved:**
  - **B1's native pushed:** `claude/compat-a3-raw-substring` at `64adcc05e9` (on A2 `6551c2036` plus piece 1 squashed as
    `ff069d8a8d`; record `A3-B1-substring-as-given.md` on `claude/compat-b1-homework` `f9f0acbbd8`). Made per the ~21:10
    ruling: `native-claims.tsv` (`EngineSubstitutes;Scalars`, the reviewer's condition), INTERNAL_DESUGAR 40 → 41,
    SqlFnSortedTest PRODUCT +1, the pin in spec's `ClaimRegistryTest`; COMPILER_OWN refusal tested three ways;
    MongoDB refuses by the dialect's own `DialectCapability` (accepted here: its other refusal arms' convention);
    `EngineSubstitutes` one class; the dialect capability matrix regenerated. **For the user's word (up):** Intrinsic
    738 → 740, NOT_UPSTREAM_MAX 76 → 78 (the native's two overloads), the funnel register's `EngineSubstitutes.java`
    row. Recorded for later: Postgres's `StringCountsAsInt` must take SUBSTRING_AS_GIVEN when Postgres joins `engine`.
  - **B1-R's rewrite mechanism built** (local `b4a053a79` on `claude/compat-a3-detector`): D-M1 refined, each
    store-bound root inlined on its own so a host-side let keeps Pure's meaning (plants R1-R6; R1 reds 4 tests);
    12 tests. **For the user's word (up):** the funnel register's `PureMeaningRewrite.java` row. Branch ruled here: the
    wiring lands as one edge-only commit on `claude/compat-a3-engine`, rebased on the batch head after B1's merge;
    DuckDB enabled in it if its batteries match under `engine`. Sent to the reviewer.
  - **A2 PR 2 pushed** on the user's word in B2's session: `claude/compat-a2-plan-text` at `059cc71df`, +95/-296,
    LiteSqlText and the dead plan-text code deleted, every guard move down (the closed-list entry for LiteSqlText from
    PR 1 cancelled, so it leaves the user's batch-PR list); nothing else moved. `claude/compat-a3-docs` at `dd9ecd481`.
    Findings: nine AnsiSqlRenderer hooks only the deleted printers overrode (stage B's list); a DialectOwnerExclusivity
    down move for the judges.
  - **The judges' A3 fast loop on the batch:** 30 of 31; parser_parity red on PureMeaningCheckTest's text alone (six
    snippets without section headers, a mechanical fix by B1-R; what remains, expected PURE-DIALECT-function-types
    20 → 21 and one function-type leniency kind, is for the user's word). Merge order given: A2 PR 2 now; the
    translator's pieces; B1's native (the translator's side on piece-1 conflicts; the corpus rosters re-run, never by
    hand); B1-R's wiring; B2's docs.
- **~21:31 UTC, B2's docs at `db4b9eea6`** (a fast-forward: S383's wording, the product's words, legend-pure's two
  wrappers and the JDK text, each checked at 5.99.0 and JDK 21.0.13; the product side waits on the unpushed
  `claude/compat-a3-switch`); `//gates:checks` 197 pass. The judges told to merge this head.
- **~21:33 UTC, the translator: A3 piece 2's numbers final** (record `compat-homework/a3/piece2/` on
  `claude/compat-translator-spike` at `9dbf2056`). PCT known-failure rows −63 (the 3 `indexOf` rows on every lane): 45
  measured (channel A on 8 classes, channel B on 7 lanes), 18 predicted for BigQuery, Snowflake and Databricks (a stale
  row turns both channels red, so removed on prediction; the batch's CI measures them). Corpus fail rosters +7 per
  database (UP): `testPlanForDateTimeConstantParameter{No,GMT,EST}TimeZone`,
  `testToSQLStringSubstringIsUncorrectedByDefault`, `testFilterUsing{Substring,ParseInteger,ParseDecimal}Function`;
  unordered registers −4 per database; 9 re-pins; 5 edge-net files re-recorded (essential answers −3 +7 on DuckDB and
  SQLite; channel B DuckDB 3 lines ERROR → PASS; the corpus host-judge recordings −14 +4 each). SubstringRangeTest green
  on every in-process database. Heads, local: `claude/compat-a3-frontends` `a4c1f2b1`, `claude/compat-a3-switch`
  `bc356444`. The translator is putting the push and the up moves to the user in its session.
- **~21:42 UTC, the ~21:40 rulings applied:** the window-in-a-list session (session_0195rQEjkEZDfj2GjXCAhyJr) resumed under this session (its two asks answered; numbers before any pin; push to its own branch; the judges merge it into the batch); the product orchestrator told, for its records. The engine runner job to the CI-health worker (session_0195rQEmJ9D6cn7yjYczb2bi), about 2 hours: the artifact read from the pin, @maven_runner only, the translator's probe as its proof; a branch `claude/compat-engine-runner-storeless` on the user's word in that session if it works, a note and stage-after-A if not. Q3 put to the user first, in depth.
- **~21:40 UTC, the reviewer** (record `49b1d0a16`): **B1's native at `64adcc05e` APPROVED as code**, not landable
  until the user rules its three moves (with the user since ~21:30); S2: the bar for DuckDB under `engine` is the
  agreement battery against the engine runner's pins (legend-engine 4.145.0, H2 2.1.214, DuckDB v1.3.0), DuckDB's
  substring DIFFER rows turning AGREE, not only equality with DuckDB's own call (sent to B1-R). **A2 PR 2 at
  `059cc71df` APPROVED**; its should (nothing pins the plan-text conditional refusal) goes to the judges as one planted
  test on the batch branch. D-M1's refinement unseen: B1-R to push the section fix alone on `claude/compat-a3-detector`
  and the mechanism with its design note rev 2 on a new `claude/compat-a3-engine` (merging the batch head later, never
  rebasing a pushed branch).
- **~21:50 UTC, the user on Q3: "why not just a decent and cheap and lean message in lite that everyone uses?"**
  Lite's product message already is that (the user's ruling of 2026-11-10, DATA_DELIVERY_DESIGN §6.1: the kind plus
  where, a fixed text known at planning, never a value; `ErrorKind`'s sentences); the question is whether the
  legend-pure and legend-engine routes use it too, with legend-engine's envelope and status kept. Measurement sent to
  the error-records session (session_0195rQEjNZoybxirC5uGba42; its 14:52 question closed by the fold ruling): PCT
  every lane both channels, the corpus on H2 and DuckDB, the server route and the net's pins, B1's size under the
  option, and what cannot use lite's sentence.
- **~21:47 UTC, B1-R: the `engine` wiring built and tested** (local `d70b51089` on a merge with B1's `897db4bbe`): under
  `engine` a flagged request is answered on all six routes (execute and Arrow 200 with the header
  `engine substring/3=1, indexOf/2=1` and legend-engine's answers; generatePlan; the three seed routes lower the rewritten
  seed query); a recording runner shows the rewritten query reaches the runner (`substringAsGiven`, `sqlPosition` by
  FunctionId); an unmeasured database refused by name; 48 tests; plant W1 reds 3 route tests. Honest limit: without
  piece 2 on the tree, `warn` answers as `engine`, so the both-ways halves and plant W2 wait for piece 2's tree. The
  setting's `mode()` reads the enum's names (the bare-name-switch ceiling stays 188). **Product-track finding:** the
  test-data scan (`ScanRelations`) reads `$p.name->substring(...)` as a property hop, so it serves no function over a
  class's property in a filter, on any setting. Branches: section fix alone on `claude/compat-a3-detector`; the
  landing branch `claude/compat-a3-engine`; a review-only `claude/compat-a3-engine-review` with the wiring, so the
  reviewer reads it before the batch can take it.
- **~21:48 UTC, the window-in-a-list fix on `claude/window-list-lowering` at `e201769d`** (main `8eea8fa1` merged): in
  `SelectMerge`, each dialect declares which expressions it writes as a query of their own (SQLite and Postgres
  OPERANDS_IN_A_QUERY, exhaustive over the MIR; DuckDB, H2, ClickHouse, Snowflake IN_PLACE, their SQL unchanged); a
  window's, aggregate's or UNNEST's value is never merged under such an expression; no statement added.
  `SelectMergeQueriedOperandsTest` replaces the scratch census (both scratch files deleted). WindowReadThroughAListTest
  and its Postgres target pass (PCT's rows on DuckDB, SQLite, Postgres; the chunked shape refused by name on SQLite);
  the lean-SQL baseline green, no pin moves. Gates running.
- **~21:50 UTC, B1-R: the batteries under `engine` measured** (local `d61e524fb`): H2 AGREE 46 with every
  legend-engine answer (2 DIFFER, the 3-argument `indexOf`, E-R2; 1 BOTH_FAIL); DuckDB AGREE 47 (2 DIFFER, the same);
  so DuckDB is enabled under `engine` by the design's rule, every other database refused by name; the `warn` pins
  unmoved. **For the user's word:** two new pin files, `pct/batteries/agreement_engine_{h2,duckdb}.tsv` (named by the
  design; new pins need the word), with the funnel row.
- **~21:51 UTC, Q12 done within the timebox (the CI-health worker, local `b648c377b` on
  `claude/compat-engine-runner-storeless` from main):** the premise was wrong. The plan-dependencies jar was already
  in @maven_runner and on the JVM's class path; legend-engine's `EngineJavaCompiler` hands javac ClassGraph's class
  path, and ClassGraph 4.8.25 silently drops every entry starting with `../`, which is how `bazel run` names every
  external jar (47 of 666 kept). The corpus lanes already pass `--classpath_limit=0` (F94), so no verdict can move.
  The fix is runner code only (`perf.AbsoluteClassPath` first in TestableMain; `--dump` writes a test error's whole
  text; `RunnerSmokeTest.testableRunsFunctionsThatReadNoStore` with a plant; the smoke test 13.8 → 22.7 s); no pool,
  MODULE.bazel or lock change. **The probe answered:** `substring(1, 3)` 'bc'; each out-of-range case
  `java.lang.RuntimeException: Failed in node: root` caused by the JDK's `Range [b, e) out of bounds for length n`, word
  for word as JDK 25.0.4. S383 gains legend-engine's form (to B2). The push waits on the user's word in that session;
  the judges merge it into the batch.
- **~21:53 UTC, B2's docs at `393cb62ff`** (S383 names legend-engine's form, "Failed in node: root" with the JDK's text
  as its cause, cited as the storeless probe's, to be checked once that branch is pushed); the judges told. The user
  asked whether Java can rewrite the error to match compatibility: answered (it is B1's prototyped design, worded
  from the record, never by editing text; the only limit is a value from row data; options 1, 2, 3 with costs; the
  choice between 1 and 2 waits on the measurement).
- **~21:57 UTC, `claude/compat-engine-runner-storeless` pushed** on the user's word in the CI-health session (head
  `b648c377b`); its //gates:local running. Sent to the judges (merge, independent of the rest) and the reviewer.
- **~21:58 UTC, the judges: the batch at `ea8176a49`** (pushed): A2 PR 2 merged (`ca26a09d5`), 42 of 42 targets green
  (the core and edge sets, every pct_channel_b lane, both corpus verdicts, every freshness test). The reviewer's should,
  measured: no plan-text request reaches InProtocol's "FreeMarker collection splice" refusal, because the dialect
  refuses an untyped String[*] template first (4 corpus tests per lane; 0 FreeMarker refusals).
  `InCollectionSpliceRefusalTest` pins both levels: (a) the route refuses by the dialect's message, (b) the protocol's
  refusal directly, shown red with it removed. **Ruled here: keep both** (the dialect's refusal at planning is the
  right first stop; InProtocol's is the loud backstop no guard proves unreachable, invariant 4). The census advisory
  applied (DialectOwnerExclusivityTest's ExecuteInDbDuckDbTest H2 crossing 6 → 2, down, ~20:45; predates the batch).
- **~22:00 UTC, the user asked how many tests now fail in piece 2.** Answered: 14 newly failing (the 7 corpus tests on
  H2 and on DuckDB, as predicted); 63 PCT rows newly passing; 4 per database leave the unordered register into the
  fail roster (the same 7); 9 unit tests re-pinned. Corrected an earlier slip: the `engine` opt-in restores
  legend-engine's answer at the server door, not in the corpus lanes, which judge lite's own meaning. A discrepancy to
  reconcile, sent to B2: its docs expect +3 per database from A3 on the batch, the translator measured +7 on A2's
  base; the judges' full measurement on the final head is the figure of record.
- **~21:59 UTC, the engine runner fix APPROVED by the reviewer** (record `d7d2595ea`: tooling only; the corpus verdicts
  cannot move on any OS; nit: the dump file's name rewrite could in theory collide two testables) **and merged into the
  batch** (head `29ebd101b`, clean; the smoke test fresh, 27.9 s, the storeless case included). The batch waits on the
  detector's section fix and the translator's A3 heads.
- **~22:01 UTC, B2 reconciled the corpus counts** (docs `185e35e6f`): none of A3's 7 per database fails without A3 (read
  at main `b75d6c73a`, A2 `6551c2036` and the batch `29ebd101b`); the 4 answer tests were on the unordered registers
  from A2 on, passing by rows; the 3 text tests on no list. The earlier "+3" counted only the 3 hidden text tests and
  left out item 1's own 4 (B2's error, corrected). Expected with A3: H2 276 → 428 (+152), DuckDB 108 → 264 (+156); the
  judges' measurement on the final head is the figure of record.
- **~22:05 UTC, the user: "should we be running rcorpus in engine mode? Should PCT relation also be run in engine
  mode?"** Answered by the rule "each suite in the mode its authors run it in": the corpus yes in principle
  (legend-engine runs it uncorrected), in stage B with B3 (the corpus door's prepare/run halves give the rewrite a place
  without a hook), measured first, switching the lanes' mode rather than adding lanes; PCT no (Pure's expected answers;
  legend-engine runs PCT corrected; the relation suite's failures are table printing, B2's). Added as Q14 to
  `compat-stage-b-questions-2026-11-19.md`.
- **~22:11 UTC, B1-R: three branches pushed.** `claude/compat-a3-detector` at `8a2af5770` (the section fix alone;
  parser_parity green in full; the function-typed class replaced by a lambda read out of a collection, which
  legend-engine's grammar writes, plant P3 still biting; so the leniency ledger, the adjudication kind and an extension
  census row do NOT move, and the fourth guard move put to the user at ~21:50 falls away; own_corpus.matched 3315 → 3321
  under ~20:10). `claude/compat-a3-engine` at `1e6d9c68c` (the landing branch: the section fix plus the mechanism and
  design note rev 2; the funnel row PENDING). `claude/compat-a3-engine-review` at `2dec438b0` (review only: B1's
  native merged to compile, the wiring, the batteries). **The reviewer's S2 met:** against the runner's pins, DuckDB 16
  rows DIFFER → AGREE under `engine` (47 of 49), H2 46; the two left are the 3-argument `indexOf` (legend-engine refuses,
  lite answers, E-R2), untouched by `engine`. Sent to the judges (merge the detector fix now; the engine after B1's
  native) and the reviewer. **With the user now: three moves** (the two funnel rows, the counters +2/+2, the two new
  engine pin files).
