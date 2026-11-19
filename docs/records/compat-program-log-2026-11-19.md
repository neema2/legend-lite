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

**Still open:** CP-12 (copy legend-engine's departures from Pure, or answer Pure's way) and CP-5 (no special functions),
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
