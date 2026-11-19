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

Pending: the run is under way (channel B, DuckDB, five suites, `-Dpct.channelb.rules=LITE`).

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
