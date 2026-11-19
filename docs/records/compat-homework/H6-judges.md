# H6: the judges' homework for slices 2.1 and 2.2, and the I5 measurement (2026-11-19)

**Status:** in progress, written as each finding lands (the user, 2026-11-19 ~10:45 UTC: "write down all the
learning/outcomes/details immediately"). Homework only: no product change for merge, no pin, ceiling, ledger, roster or
allowlist moved. The prototypes named below live on this branch alone and are marked MEASUREMENT ONLY in the code.

- **Branch:** `claude/compat-judges-homework`, from `main` `b75d6c73a`.
- **Session:** the judges session of the compatibility program; reports to the program's owner
  (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **Inputs read:** the plan (`docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program`: §1 done-when
  1, §3's J rows, slices 2.1 and 2.2, §12 I5); the log (F5, F8, F9); H1 §1.1; the census's N-JUDGE, JUDGES-6 and
  JUDGES-3 groups (`claude/census-b2`); the three-mode design §3.7 and §4 (c); FOLLOW_UPS §2 and §8; the charter
  `docs/SQLTEXT_ROW_VERDICT_CHARTER.md`; `docs/GOALS_OF_RECORD_2026_11_07.md`; AGENTS.md; and, found in the course of
  task 1, the plan of record of 2026-11-16 (`docs/records/total-wall-edge-program-2026-11-16.md` on
  `claude/total-wall-orch`, §12 at 01:30 and 02:30) and the shape-port record
  (`docs/records/e-java-shape-port-2026-11-17.md` on `claude/total-wall-e-shape-port`, `1bb0f3151`).

---

## 1. The 36 combinations (task 1)

### 1.1 Where they are defined

"C4, the 2026-11-17 02:30 decision" is the plan of record's §12 row of 2026-11-17 02:30 (`claude/total-wall-orch`,
`docs/records/total-wall-edge-program-2026-11-16.md:704`), following the user's judge model ruled at 01:30 ("Love it",
`:710`). Its two measurements, made on the measurement-only branch `claude/total-wall-e-measure` (`1fb5df6be`):

- **(a)** the database judge ALONE under lite's rules, verdicts only, against the verdict of record: **PCT channel B
  100%** on DuckDB's five suites, H2's relation suite and SQLite (0 disagree, 0 decline); the corpus about 61% (about
  95% of its disagreements compatibility noise: 955-965 engine-text asserts with no appeal under lite's rules).
- **(b)** a shape census: each planned assert's side shapes under the host judge (`HostSides.plan`,
  `-Dlegend.measure.shapes`), on core's 13 judged packages, PCT channel B DuckDB's five suites and both corpora. 67
  distinct assert x side-shape combinations in core, 21 in PCT, 69 in the corpus; **36 not in core**, covering 1,017
  planned asserts (with value types: 571 PCT- or corpus-only against core's 148).

DECIDED there: the corpus HOST-only at once (done the same night, `claude/total-wall-e-host-corpus` `b523c767d`); PCT
HOST-only after the 36 are ported into core; CORE the blocking check (isolated BOTH, lite's words). The three-mode
design's C4 (20:10-R8) re-ruled exactly that order. The 36 are listed in the shape-port record (`1bb0f3151`), every
row "to port"; no later branch closes a row (searched: no commit after `1bb0f3151` names the shape port).

A side shape is the side's `SideShape.Kind` (CONSTANT, GRAPH, TABULAR, KEYED, VALUES) and whether it is one value or
many, in the assert's side order; "quantified-assert" is the census's name for `forAll`/`exists` style asserts planned
as one quantified verdict.

### 1.2 The list (from `1bb0f3151`, verbatim), with today's coverage

The coverage columns are re-measured at `main` `b75d6c73a` (§1.3, method §5.1).

| # | Assert | Side shapes | PCT | Corpus | Core LITE test under BOTH at main | What porting it takes |
|---|---|---|---|---|---|---|
| 1 | `quantified-assert` | VALUES/one, VALUES/one | 0 | 274 | pending | |
| 2 | `assertFalse` | VALUES/one | 161 | 38 | pending | |
| 3 | `assertSameElements` | VALUES/one, VALUES/many | 0 | 196 | pending | |
| 4 | `assertEquals` | CONSTANT/many, VALUES/one | 0 | 92 | pending | |
| 5 | `assertEmpty` | GRAPH/many | 0 | 28 | pending | |
| 6 | `assertSameElements` | CONSTANT/many, VALUES/one | 0 | 26 | pending | |
| 7 | `assertJsonStringsEqual` | CONSTANT/one, VALUES/one | 0 | 25 | pending | |
| 8 | `assertSize` | CONSTANT/one, VALUES/one | 0 | 23 | pending | |
| 9 | `assert` | VALUES/one, CONSTANT/one | 0 | 18 | pending | |
| 10 | `assertEquals` | VALUES/many, VALUES/one | 0 | 16 | pending | |
| 11 | `equal` | CONSTANT/one x4 | 0 | 14 | pending | |
| 12 | `assertSameElements` | CONSTANT/one, VALUES/one | 0 | 12 | pending | |
| 13 | `assertSameElements` | VALUES/one, TABULAR/many | 0 | 12 | pending | |
| 14 | `assertIs` | VALUES/one | 0 | 8 | pending | |
| 15 | `assertSameElements` | VALUES/one, VALUES/one | 0 | 8 | pending | |
| 16 | `equal` | CONSTANT/one x6 | 0 | 6 | pending | |
| 17 | `assert` | CONSTANT/many, VALUES/many, VALUES/one | 0 | 6 | pending | |
| 18 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one, CONSTANT/one | 2 | 4 | pending | |
| 19 | `assertSameElements` | CONSTANT/many, VALUES/many | 0 | 4 | pending | |
| 20 | `assertConversion` | (none) | 0 | 4 | pending | |
| 21 | `quantified-assert` | VALUES/one x6 | 0 | 4 | pending | |
| 22 | `quantified-assert` | CONSTANT/one, CONSTANT/one | 0 | 4 | pending | |
| 23 | `assertEqualsH2Compatible` | (none) | 0 | 4 | pending | |
| 24 | `equal` | CONSTANT/one, CONSTANT/one | 0 | 4 | pending | |
| 25 | `assertNotEmpty` | VALUES/one | 3 | 0 | pending | |
| 26 | `assertError` | VALUES/many | 2 | 0 | pending | |
| 27 | `assertEqWithinTolerance` | VALUES/many, TABULAR/many | 0 | 2 | pending | |
| 28 | `assertSameElements` | VALUES/many, CONSTANT/many | 0 | 2 | pending | |
| 29 | `assertEmpty` | CONSTANT/many, CONSTANT/one | 0 | 2 | pending | |
| 30 | `quantified-assert` | CONSTANT/one x4 | 0 | 2 | pending | |
| 31 | `assertNotEmpty` | GRAPH/many, CONSTANT/one | 0 | 2 | pending | |
| 32 | `assertJsonStringsEqual` | VALUES/one, VALUES/one | 0 | 2 | pending | |
| 33 | `forAll` | CONSTANT/one x4 | 0 | 2 | pending | |
| 34 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one | 2 | 0 | pending | |
| 35 | `assertEmpty` | VALUES/one | 2 | 0 | pending | |
| 36 | `assertNotEmpty` | VALUES/many | 1 | 0 | pending | |

(Rows 11, 16, 21, 22, 30 and 33 are written "x N" for N equal sides; the source writes each side.)

### 1.3 Re-measured at `main`

Pending: the shape census ported to `main` (`HostSides.shapeCensus`, measurement only) over core's judged packages
under BOTH, split LITE (the product's style) from COMPAT (an edge style) per planned assert.

---

## 2. The map of compatibility judging in the core (task 2)

Pending.

---

## 3. PCT as LITE's own suite under BOTH judges (task 3, §12 I5)

### 3.1 Which channel can answer the question

- **Channel B** (`ChannelB.runOne`) plans each PCT test body whole through lite's test door
  (`TestExecution.runProgram`), so lite's judges, host and database, judge every assert. This is the channel I5 is
  about, and the one measured here.
- **Channel A** (`PctExecuteNative`) is legend-pure's interpreter running the test function; lite answers each
  `execute` and the interpreter's own asserts judge it. No lite judge runs there, so "BOTH" cannot apply. Channel A
  under LITE's planning style still says something different and useful: lite's answers under its own rules judged by
  legend-pure itself, the 423 relation tests of §3.3 included, by value. That run is §3.6.

### 3.2 The counts (channel B, DuckDB, the five suites, `main` `b75d6c73a` plus the switch of §5.1)

| Run | Pass | Fail | Of which judges-disagree | Wall (5 suites, 4 cores) | Per suite |
|---|---|---|---|---|---|
| COMPAT BOTH (today's lane, reproduced) | **1,240** | 9 | 0 | 64 s | 20-60 s |
| **LITE BOTH** | **786** | **463** | **5** | 75 s | 23-74 s |
| LITE BOTH + R4's substring half (`CORRECT_SQL_SUBSTRING_INDEXING`) | 790 | 459 | 5 | 77 s | 14-74 s |
| LITE HOST only | 787 | 462 | n/a | 33 s | 12-24 s |
| LITE DATABASE only | 786 | 463 | n/a | 55 s | 15-52 s |

- 1,249 tests: essential 345, grammar 137, relation 469, standard 205, unclassified 93 (the ratchets' discovery
  counts, unchanged).
- The COMPAT row is today's lane exactly: all five targets PASSED their own pins with the switch in place, so the
  switch leaves the compatibility run untouched. Its 9 failures are today's ledgered ones (4 big-number date
  adjustments, acos/asin domain, 3 indexOf).
- LITE per suite (pass/fail): essential 312/33, grammar 136/1, relation 100/369, standard 145/60, unclassified 93/0.
- Nothing passes under LITE that fails under COMPAT; 454 tests pass under COMPAT and fail under LITE.

### 3.3 Every LITE BOTH failure, classified (463)

| Class | Tests | What | Under COMPAT |
|---|---|---|---|
| **Compatibility presentation: a relation's text** | **423** (relation 365, standard 58) | the test asserts legend-pure's `toString` of a relation (`#TDS ... #`); the product refuses it by name ("toString of a relation is legend-pure's text, the compatibility edge's") | pass |
| Compatibility presentation: legend-pure's error words | 16 | an `assertError` expecting legend-pure's sentence, where lite raises its own: `at` (offset), `slice` bounds, `date` parts (4) and `newDate` month, `rem`/`sqrt`, `range` step, window frame bounds (4: over and reduce, rows and range), `bitShiftLeft`/`Right` past 62 bits | pass |
| Compatibility presentation: legend-pure's print of a value | 7 | Float `toString` (`17.0` vs lite's `17`; `134210000.0` vs `134210000`; `0.000000013421` vs `0`, lite's ruled six-decimal text: the user's ruling of 2026-11-10, `AnsiSqlRenderer.floatToText`), DateTime `toString` and `format` (`2014-01-01T00:00:00.000+0000` vs lite's `...000000Z`; 4) | pass |
| Compatibility presentation: refused by name | 3 | `format`'s `%05d` and `%r`; an instance's `toString` | pass |
| **R4** (lite's `substring`/`indexOf` differ from Pure's) | 7 | substring: `testStart`, `testStartEnd`, `sort` `testSimpleSortWithKey` and `...WithFunctionVariables` (a `substring(1, 2)` key): all 4 PASS once the corrected indexing is on; indexOf: `string::indexOf` `testSimple`, `testFromIndex`, `collection::indexof::testIndexOfOneElement` (`'c'->indexOf('c')`, a String receiver): no feature corrects indexOf today, and they fail under COMPAT too | substring pass, indexOf fail |
| Compatibility-only JUDGING rules (2 ULP, multiset order, json-simple, a relation counted as one value) | **0** | none of the 463 | |
| **Real judge-parity findings (G2)** | **5** | below, §3.4 | hidden (pass, or fail with the verdicts agreeing) |
| Pre-existing, not judging | 2 | `acos(2.0)`/`asin(2.0)` raise nothing (lite plants no domain check; legend-engine's own DuckDB manifest ledgers both) | fail |

So of the 463: **450 are compatibility presentation** (legend-pure's text or words asserted by the test), 7 are R4, 5
are judge-parity defects, 1 more pair is pre-existing, and none is a compatibility judging rule. The last agrees with
the 2026-11-17 02:30 measurement (the database judge alone, lite's rules, verdicts only: 100% on PCT channel B).

### 3.4 The five judge-parity findings (sent to the previous orchestrator, A5's owner, 2026-11-19 ~11:20 UTC)

1. **`date::tests::testDateFromHour`** (`assertEquals(%1973-11-13T23, date(1973, 11, 13, 23))`): HOST holds, DATABASE
   fails. The database judge's verdict statement, on lite's written-words path, writes the actual side's text as
   `strftime(CAST(__once.__ov AS TIMESTAMP), '%Y-%m-%dT%H:%M:%S.%fZ')` over the hour-precision DateTime's carried text
   `2015-04-16T14`, and DuckDB refuses the cast ("invalid timestamp field format"). Under COMPAT it passes both judges:
   the canon path does not cast. A LITE-only database-judge defect today's PCT cannot see.
2. **`date::tests::testAdjustBy{Days,Hours,Months,Weeks}BigNumber`** (4): both judges fail (DuckDB's dates are out of
   range; these 4 fail under COMPAT too), but they report differently. The host reports a failed `assertEquals`
   verdict carrying DuckDB's "date field value out of range"; the database judge's fused verdict statement fails the
   run with a different DuckDB error ("Type INT64 with value 12345678912 can't be cast ... INT32"). LITE's comparison
   (verdicts and messages) flags it; COMPAT's (verdicts only) skips an assert where one judge's run failed and the
   other's verdict failed (`IsolatedRun#difference`'s `!messages` branch, JUDGES-3's site, `IsolatedRun.java:154-163`).

FOLLOW_UPS J1 (an `assertEquals` over a scalar pair worded as a multiset, so BOTH disagrees) did not reproduce on PCT
DuckDB: no other disagreement among the 786 LITE passes, messages compared.

### 3.5 What a per-push LITE PCT lane would take

- **Run time:** the five suites under LITE BOTH take 75 s wall on this 4-core container (per suite 23-74 s; CPU about
  240 s), against 64 s for today's COMPAT lane. No new build: the same `//pct:pct_channel_b_<suite>` targets with
  `-Dpct.channelb.rules=LITE` as a jvm flag; five more `junit_test` rules of 768 MB each.
- **Its oracle:** an exact per-suite ledger, as channel B's lanes keep today (`channelb/ledger.tsv`, `ChannelBLedger`):
  every LITE failure with its text and its class from §3.3, PASS shrink-only. A failure the ledger does not hold, or
  a judges-disagree, fails the lane.
- **What it adds as evidence for G2:** 786 tests (790 with R4's substring half, 793 with R4 whole) judged by both
  judges, isolated, verdict AND message compared; today's core BOTH is the only lane comparing messages. It found 5
  defects on its first run.
- **What it cannot add without other work:** the 423 relation tests assert legend-pure's relation text, so they are
  never LITE tests as written. Three ways to win them, none free: (a) lite adopts a relation text of its own as a
  ruled meaning (a presentation decision, not this program's default); (b) a test-side rewrite of
  `assertEquals('<#TDS text>', $r->toString())` into a value comparison, which reads legend-pure's text, so it is
  compatibility work at the edge; (c) leave them to COMPAT HOST-only, where they pass, and to channel A (§3.6), which
  judges lite's relation answers by value.
- **Prerequisites:** the 5 parity findings fixed or ledgered (4 of them are already failures, so a ledger row each is
  honest); R4 built (moves 7 rows to PASS); the 26 word/print rows stay ledgered until a LITE presenter exists for
  them, or for good (they assert upstream's words).
- **Recommendation for the user's decision:** yes, as an ADDITION to 2.1: a per-push `pct_channel_b_lite_<suite>`
  lane (about 75 s, its own exact ledger) widens judge-parity evidence from core's suites to 786 PCT tests at the cost
  of one minute of CI, and found real defects at once. It does not replace the 36 combinations' port: only 7 of the 36
  have PCT asserts at all (§1).

### 3.6 Channel A under LITE's planning style

Pending (the switch `-Dpct.channela.rules=LITE|LITE_R4` in `PctPlanRoute`, §5.1).

---

## 4. Coordination (task 4)

Sent 2026-11-19 ~10:50 UTC to the previous orchestrator (`session_0195rQEoRb1AGCHxtZChjWsg`), one message: this
session measures the judges, homework only, on `claude/compat-judges-homework`; the items of its A5 the measurements
touch are J1 (an `assertEquals` over a scalar pair worded as a multiset, so BOTH raises `JudgesDisagree`) and J2/J3,
which the PCT-under-LITE run surfaces test by test; J4 (the edge's negated words differ by judge), in the map's
compatibility half; JUDGES-3 (`IsolatedRun#difference`), in the map. Q3 (MariaDB list order) and Q3b (MySQL Decimal
scale) are not touched: DuckDB only. Any judge-parity bug found goes to it before it is written up as a fix.

---

## 5. Method

### 5.1 The prototypes on this branch (MEASUREMENT ONLY)

- `pct/.../channelb/ChannelB.java`: `-Dpct.channelb.rules=COMPAT|LITE|LITE_R4`. COMPAT is today's run, unchanged.
  LITE runs every body with `ExecuteOptions.NONE` (the product's `NoEdge.NONE`, `PlanStyle.PRODUCT`), the product's
  default features (R4 not applied), `DETERMINISTIC_AS_OF`, the judge `-Dpct.channelb.judge` names (default BOTH),
  and `NoEdgeRun.NONE` as the run hook (no referee). LITE_R4 adds `CORRECT_SQL_SUBSTRING_INDEXING`, which the PCT
  lanes run with today, standing in for R4. Each outcome is printed as one `[chB-h6]` line whatever the lane's pins.
- `core/.../testplan/HostSides.java`: `-Dlegend.measure.shapes=stdout` prints one `[shape]` line per planned assert:
  LITE or COMPAT (the planning style), the running test, the assert's name and each side's kind and shape (the port
  of `claude/total-wall-e-measure`'s `d252748b`/`d8438c80` to today's `JudgePlans.shapeOf(PlannedSide, PlanStyle)`).
