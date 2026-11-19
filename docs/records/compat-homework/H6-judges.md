# H6: the judges' homework for slices 2.1 and 2.2, and the I5 measurement (2026-11-19)

**Status:** complete for the four tasks and plan r6's follow-ups (§6), written as each finding landed (the user, 2026-11-19 ~10:45 UTC: "write
down all the learning/outcomes/details immediately"). Homework only: no product change for merge, no pin, ceiling,
ledger, roster or allowlist moved. The prototypes named below live on this branch alone and are marked MEASUREMENT
ONLY in the code.

## 0. The answers on one page

- **The 36 combinations (task 1, §1).** Defined at the 2026-11-17 02:30 row of the 2026-11-16 plan of record and listed
  in the shape-port record (`1bb0f3151`). Re-measured at `main`: **none is covered by core** (core's 13 BOTH targets
  plan 922 asserts in 46 combinations of their own). 7 have PCT asserts; 6 of those are judged in PCT tests that pass
  under LITE BOTH. All 36 are small `testing.Program` bodies; no new mechanism. **2.1: 2.5 days** (the plan's 2, plus
  half a day for the defects they will surface).
- **The map (task 2, §2).** Every two-form kind, gate, seam and hook of compatibility judging, with file:line, lines,
  callers and class. Three findings change 2.2:
  - **the appeal's planning half is not only at `TestDoor`:** the engine-text arm claims asserts inside the product's
    assert planner (15 gate calls in `AssertVerdicts`) through the planner's own environment, so the planning
    interface needs one call site in the per-assert entry as well: done-when 1's "exactly two call sites" is three as
    worded (a reshape of today's facet, not a new hook);
  - **more is deleted than moved:** the database judge's appeal paths go with the compatibility database judge;
  - **the user's call:** a "whole" compatibility host judge is either the edge's own copy of the 2,765-line host
    judge (done-when 1 literally; +3 days) or the product's host judge with its rules a typed parameter (no copy;
    done-when 1 reworded). **2.2: 8.5 days under the parameter, 11.5 under the copy** (the plan: 7.5).
- **PCT as LITE's own suite (task 3, §3; §12 I5).** Channel B, DuckDB, 1,249 tests: COMPAT BOTH 1,240 pass; **LITE
  BOTH 786 pass, 463 fail, 5 judges-disagree**; LITE with R4's substring half 790. Of the 463: 450 assert legend-pure's
  text or words (423 of them a relation's `toString`), 7 are R4, **0 are compatibility-only judging rules**, 2 are
  pre-existing, and **5 are real judge-parity defects** COMPAT's comparison hides (sent to A5's owner). Channel A under
  LITE: 1,203 of 1,249 pass, and the relation suite does not move, so lite's relation answers are right by value.
  **Recommendation: yes, add a LITE PCT lane to 2.1**: about 75 s per push, its own exact ledger, 786 tests of parity
  evidence with messages compared.
- **Coordination (task 4, §4).** Told the previous orchestrator at the start; sent it the 5 parity findings.
- **Plan r6's follow-ups (§6).** Where legend-pure's leniencies change a verdict, measured by switching each off:
  **0 tests on every PCT lane** (channel B DuckDB, H2 relation, SQLite; channel A runs no lite judge), but **69 tests
  of the DuckDB corpus and 29 of the H2 corpus** (S131 33/4, S137 21/21, S132 14/3, S15 2/0, S144 2/2); S127 and S139
  change nothing anywhere. So CP-14's (iii) holds for PCT and fails for the corpus unless the translator takes four
  input rewrites (S137, S144, S131's typed half, S15), leaving only the rendered-text comparison (26 DuckDB tests, 3 H2)
  as the edge judge's own. S131's multiset is live in LITE today (its narrowing, ruled 2026-11-05, is unbuilt), and
  turning it off moves no PCT verdict. 2.2a: 3.5 days; 2.2b: 5.5 under (ii), 7 under (iii). The referee replays on the
  body's own session (H2's, with the edge's LEGACY settings): it needs a session of its own before CP-8. The
  decoded-plan guard keeps seeing the `Edge` records.

**Context.**
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
row "to port"; the re-measure at `main` (§1.3) shows no row has closed since.

A side shape is the side's `SideShape.Kind` (CONSTANT, GRAPH, TABULAR, KEYED, VALUES) and whether it is one value or
many, in the assert's side order; "quantified-assert" is the census's name for a statement root that is a `map`
(`TypedMap`) asserting per element (`AssertVerdicts.listenerName`, `testplan/AssertVerdicts.java:320-325`).

### 1.2 The list (from `1bb0f3151`, verbatim), with today's coverage

The coverage columns are re-measured at `main` `b75d6c73a` (§1.3, method §5.1).

| # | Assert | Side shapes | PCT | Corpus | Covered at main (core LITE under BOTH; PCT under LITE BOTH, planned asserts) | What porting it takes |
|---|---|---|---|---|---|---|
| 1 | `quantified-assert` | VALUES/one, VALUES/one | 0 | 274 | none | a core body whose statement is a `map` asserting per element (`$xs->map(x \| assertEquals(...))`), both sides one computed value |
| 2 | `assertFalse` | VALUES/one | 161 | 38 | none; PCT LITE BOTH: 161 (all pass) | `assertFalse` over a computed Boolean (one value); PCT's 161 pass under LITE BOTH (§3) |
| 3 | `assertSameElements` | VALUES/one, VALUES/many | 0 | 196 | none | `assertSameElements` of a computed value against a computed collection |
| 4 | `assertEquals` | CONSTANT/many, VALUES/one | 0 | 92 | none | `assertEquals` of a literal collection against one computed value |
| 5 | `assertEmpty` | GRAPH/many | 0 | 28 | none | `assertEmpty` over a graph fetch (many) |
| 6 | `assertSameElements` | CONSTANT/many, VALUES/one | 0 | 26 | none | `assertSameElements`, a literal collection against one computed value |
| 7 | `assertJsonStringsEqual` | CONSTANT/one, VALUES/one | 0 | 25 | none | `assertJsonStringsEqual`, a literal text against a computed text (LITE's strict grammar) |
| 8 | `assertSize` | CONSTANT/one, VALUES/one | 0 | 23 | none | `assertSize`, a literal count against one computed value |
| 9 | `assert` | VALUES/one, CONSTANT/one | 0 | 18 | none | `assert` over a computed Boolean with a literal message |
| 10 | `assertEquals` | VALUES/many, VALUES/one | 0 | 16 | none | `assertEquals`, a computed collection against one computed value |
| 11 | `equal` | CONSTANT/one x4 | 0 | 14 | none | a body ending in `equal` over four literal sides (the corpus's boolean-returning bodies) |
| 12 | `assertSameElements` | CONSTANT/one, VALUES/one | 0 | 12 | none | `assertSameElements`, one literal against one computed value |
| 13 | `assertSameElements` | VALUES/one, TABULAR/many | 0 | 12 | none | `assertSameElements`, one computed value against a relation |
| 14 | `assertIs` | VALUES/one | 0 | 8 | none | `assertIs` over one computed value (one side planned) |
| 15 | `assertSameElements` | VALUES/one, VALUES/one | 0 | 8 | none | `assertSameElements`, two computed single values |
| 16 | `equal` | CONSTANT/one x6 | 0 | 6 | none | as 11, six literal sides |
| 17 | `assert` | CONSTANT/many, VALUES/many, VALUES/one | 0 | 6 | none | `assert` over a literal collection, a computed collection and one value (a helper's three asks) |
| 18 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one, CONSTANT/one | 2 | 4 | none; PCT LITE BOTH: 2 (pass) | `assertTdsEquivalent`, two relations, two literal arguments; PCT's 2 pass under LITE BOTH |
| 19 | `assertSameElements` | CONSTANT/many, VALUES/many | 0 | 4 | none | `assertSameElements`, a literal collection against a computed collection |
| 20 | `assertConversion` | (none) | 0 | 4 | none | a helper root with no side planned (the corpus's `assertConversion`): port as a core user helper wrapping an assert, not the upstream helper |
| 21 | `quantified-assert` | VALUES/one x6 | 0 | 4 | none | as 1, six one-value sides |
| 22 | `quantified-assert` | CONSTANT/one, CONSTANT/one | 0 | 4 | none | as 1, two literal sides |
| 23 | `assertEqualsH2Compatible` | (none) | 0 | 4 | none | as 20 (the corpus's `assertEqualsH2Compatible`) |
| 24 | `equal` | CONSTANT/one, CONSTANT/one | 0 | 4 | none | as 11, two literal sides |
| 25 | `assertNotEmpty` | VALUES/one | 3 | 0 | none; PCT LITE BOTH: 3 (pass) | `assertNotEmpty` over one computed value; PCT's 3 pass under LITE BOTH |
| 26 | `assertError` | VALUES/many | 2 | 0 | none; PCT LITE BOTH: 4 (all fail: words) | `assertError` whose body reads a collection; PCT's only tests of it assert legend-pure's words, so they fail under LITE: port with lite's own words |
| 27 | `assertEqWithinTolerance` | VALUES/many, TABULAR/many | 0 | 2 | none | `assertEqWithinTolerance`, a computed collection against a relation |
| 28 | `assertSameElements` | VALUES/many, CONSTANT/many | 0 | 2 | none | `assertSameElements`, a computed collection against a literal collection |
| 29 | `assertEmpty` | CONSTANT/many, CONSTANT/one | 0 | 2 | none | `assertEmpty`, a literal collection and a literal message |
| 30 | `quantified-assert` | CONSTANT/one x4 | 0 | 2 | none | as 1, four literal sides |
| 31 | `assertNotEmpty` | GRAPH/many, CONSTANT/one | 0 | 2 | none | `assertNotEmpty` over a graph fetch with a literal message |
| 32 | `assertJsonStringsEqual` | VALUES/one, VALUES/one | 0 | 2 | none | `assertJsonStringsEqual`, two computed texts |
| 33 | `forAll` | CONSTANT/one x4 | 0 | 2 | none | `forAll` over four literal sides |
| 34 | `assertTdsEquivalent` | TABULAR/many, TABULAR/many, CONSTANT/one | 2 | 0 | none; PCT LITE BOTH: 2 (pass) | `assertTdsEquivalent`, two relations and one literal; PCT's 2 pass under LITE BOTH |
| 35 | `assertEmpty` | VALUES/one | 2 | 0 | none; PCT LITE BOTH: 2 (pass) | `assertEmpty` over one computed value; PCT's 2 pass under LITE BOTH |
| 36 | `assertNotEmpty` | VALUES/many | 1 | 0 | none; PCT LITE BOTH: 1 (pass) | `assertNotEmpty` over a computed collection; PCT's 1 passes under LITE BOTH |

(Rows 11, 16, 21, 22, 30 and 33 are written "x N" for N equal sides; the source writes each side.)

### 1.3 Re-measured at `main`

The shape census ported to `main` (`HostSides.shapeCensus`, measurement only, §5.1), over core's 13 judged BOTH targets
(`//core:core_tests_<package>_judge_both`, the 12 packages and the root; all 13 PASSED with the census on, 174 s wall)
and PCT channel B's five DuckDB suites under COMPAT and LITE:

| Source | Planned asserts recorded | Distinct assert x side-shape combinations |
|---|---|---|
| core, the 13 BOTH targets (every line LITE: no core product test plans in an edge style) | 922 | **46** |
| PCT channel B, COMPAT BOTH | 5,466 (2,733 asserts: the reference lane plans each body twice under BOTH, once to sum its judge SQL, `ChannelB.measuresJudgeSql`) | 21 |
| PCT channel B, LITE BOTH | 4,594 | 21 (19 in tests that pass) |

**None of the 36 is covered by core at `main`**: no row closed since `1bb0f3151`, and core's 46 combinations hold none
of them (matched on the assert's short name: the census writes `forAll` and `assertError` by their full names). The
PCT counts, halved for the double planning, equal the shape-port record's (assertFalse 161, assertTdsEquivalent 2 and
2, assertNotEmpty 3 and 1, assertEmpty 2), except `assertError` VALUES/many: 4 at `main` against the record's 2.

Core's 46 at `main` (for the port's starting point): `assertEquals` over VALUES/one pairs is 549 of the 922; then
`assertEquals` VALUES/one+many (59), `assertEq` (36), `assertEquals` VALUES/many pairs (29), `assertSize` over a graph
(22), `assert` over a constant (17), and 40 smaller ones. The 2026-11-17 census counted 67 in core; the difference is
what it ran (the host judge over the same packages then, the 13 BOTH targets now), not a loss of tests.

**What porting takes.** Every row is a small core test in `testing.Program`'s style, so it runs under each judge
target already (`core_tests_<package>_judge_{database,both}`); none needs a new mechanism. They group as:
- **26 rows are an ordinary assert over a new side shape** (2-10, 12-15, 17-19, 25-29, 31-32, 34-36): one body each,
  its sides a query the database computes (VALUES), a relation (TABULAR), a graph fetch (GRAPH) or a literal of the
  plan (CONSTANT), as the row says;
- **4 rows are a `map` asserting per element** ("quantified-assert": the census's name for a statement root that is a
  `TypedMap`, `AssertVerdicts.listenerName`, `:320-325`; rows 1, 21, 22, 30), and row 33 is `forAll` over four literals;
- **3 rows end a body in `equal`** (11, 16, 24);
- **2 rows are the corpus's helpers with no side planned** (20 `assertConversion`, 23 `assertEqualsH2Compatible`):
  ported as a core user helper wrapping an assert, never the upstream helpers themselves.

**If the LITE PCT lane of §3.5 is adopted**, 6 rows (2, 18, 25, 34, 35, 36; 171 planned asserts) are judged under
LITE BOTH by PCT already, in tests that pass; 30 remain to port. Row 26's PCT tests assert legend-pure's words and so
fail under LITE: it must be ported with lite's own words.

**2.1 re-sized:** the plan's 2 days holds for the writing (36 bodies, or 30 with the LITE PCT lane). On the evidence of
§3 (5 parity defects in 786 PCT tests on a first LITE run), expect the new shapes to surface some; each goes to A5's
owner and is either fixed there or ledgered. Budget **2.5 days**, the half day for the defects' triage.

---

## 2. The map of compatibility judging in the core (task 2)

Read at `main` `b75d6c73a`. Paths are under `core/src/main/java/com/legend/`. "Lines" counts the file, or the method
with its javadoc. "Callers" are product (`core/src/main`) call sites outside `com.legend.edge` unless marked; test and
guard callers are in §2.8. The classes:
- **DEL**: deleted, because compatibility judging becomes host-only (C4) and the database judge then only ever runs
  under LITE's rules, so the edge's half of a database-judge decision is never asked;
- **PLAN**: moves into the edge judge's PLANNING interface (§2.9);
- **RUN**: moves into the edge judge's RUN interface (§2.9);
- **EDGE-HOST**: moves into the edge's compatibility HOST judge (legend-pure's rules and words over typed sides, after
  the run): the product's host judge keeps lite's rule with no seam;
- **1.2**: words out of the plan, slice 1.2's (not 2.2's), listed so the two slices meet;
- **KEEP**: stays in the product as it is.

### 2.1 The two-form judge kinds (`PlanStyle.twoForm`, `testplan/PlanStyleFacets.TWO_FORMS`)

| Kind | Where (interface; product form; edge form) | Lines | Asked at (product) | Class |
|---|---|---|---|---|
| `JsonGrammar` | `testexecutionplan/JsonGrammar.java:15`; `testexecutionplan/JudgeJson.java` (`STRICT`); `edge/JsonSimpleGrammar.java` | 20; 25; 341 | planning: `testplan/DatabaseJudge.java:1078` (the database judge's golden read); run: `testexec/HostJudgments.java:329` via `EdgeHearing.json()` (`testexec/EdgeHearing.java:44`, `NoEdgeRun.java:68`) | the kind DEL; the database judge reads strict JSON; json-simple 1.1.1 (S144, S366) is EDGE-HOST |
| `RelationCounting` | `compiler/spec/RelationCounting.java:17`; `compiler/spec/RelationCountAsserts.java` (`REFUSED`); `edge/UpstreamRelationCounting.java` | 21; 57; 75 | planning: `testplan/TestPlanner.java:227` (a rewrite of the body before it is planned) | the kind DEL; LITE's refusal KEEP; counting a relation as one value (S137) is a body rewrite at the door: PLAN (or the translator's; it is input-side, not a judgment) |
| `VerdictNumbers` | `lowering/VerdictNumbers.java:16`; `lowering/LiteVerdictNumbers.java` (`EXACT`); `edge/CompiledPureVerdictNumbers.java` | 27; 31; 54 | `testplan/DatabaseJudge.java:371` (`verdictNumbers(env)`), then `lowering/VerdictSql.java` at `:227/:234` (`equality`), `:342/:360-361` (`gridRows`), `:678/:698/:701` (`perElement`), `:1285-1296` (`gridPair`), `:1319`, `:1331`, `:1341`, `:1575` (`pairFacts`), `:3058/:3067` (`pairOk`) | DEL: the kind, `CompiledPureVerdictNumbers`, every `floatTolerance()` branch and the tolerance CTEs it guards (`gridCellsRowMajor`, `toleranceCells`, `floatsWithin`); the Decimal-by-value rule KEEPs as the database judge's own. The 2-ULP and scale rules (S15, S132, S139) are EDGE-HOST |
| `RenderedTextRule` | `testplan/RenderedTextRule.java:13`; `testplan/LiteRenderedTextRule.java`; `edge/UpstreamRenderedTextRule.java` | 17; 17; 18 | `testplan/AssertVerdicts.java:1872` (`textFirst`, `:1869-1890`, 25 lines; database judge only: both callers, `:1770`, `:1796`, plan `DatabaseJudge.runVerdict`) | DEL: the kind, the ruled-rows branch of `textFirst`; LITE's "the two strings are the verdict" KEEP. The ruled line-and-row comparison is EDGE-HOST (the host's `UpstreamRenderedTexts`, 241, already exists) |
| `DateLiteralTexts` | `testplan/DateLiteralTexts.java:12`; `testplan/LiteDateLiteralTexts.java`; `edge/UpstreamDateLiteralTexts.java` | 16; 18; 27 | `testplan/DatabaseJudge.java:304` (`written`, `:298-314`) | DEL |
| `GoldenNullWord` | `compiler/spec/GoldenNullWord.java:13`; `compiler/spec/LiteGoldenNullWord.java`; `edge/UpstreamGoldenNullWord.java` | 17; 17; 17 | `testplan/AssertVerdicts.java:1847`, into `compiler/spec/VerdictQueries.java:784` (`parseRendered`, `:782-926`) and `:976` (`tdsCellText`); database judge only | DEL (legend-engine's `TDSNull` is read by the edge's host judge alone) |
| `VerdictWords` | `lowering/VerdictWords.java:23`; `lowering/LiteVerdictWords.java`; `edge/UpstreamVerdictWords.java`; `lowering/VerdictReads.java` | 104; 180; 198; 14 | `testplan/DatabaseJudge.java:332-334` (`words(env)`) and `:336-338` (`lite(env)`), `testplan/VerdictBatch.java:209/:212`, `testplan/AssertVerdicts.java:1067/:1074`, `testplan/TestDoor.java:165` (the plan's `words` name); into `VerdictSql` at 20 sites (`:342-343`, `:374-375`, `:398/:410`, `:535-547`, `:573-608`, `:636-640`, `:678-741`, `:753-765`, `:836-908`, `:1285-1286`, `:1859-1885`, `:2082-2087`, `:2118`, `:2224`, `:2253-2283`, `:2332`) | the kind, `UpstreamVerdictWords` and `VerdictReads` DEL; every `reads() == WRITTEN` branch keeps its WRITTEN arm and loses its CANONS arm (`:343`, `:375`, `:574`, `:895`, `:1286`, and the canon-only helpers `gridRowCanons`/`peerRowCanons`/`gridCellCanons` where nothing else reads them); `LiteVerdictWords` KEEPs as the database judge's one sentence table (it can stop being an interface's implementation) |
| `CrossKindNumbers` | `testplan/CrossKindNumbers.java:16`; `testplan/LiteCrossKindNumbers.java` (`NEVER`); `edge/CompiledPureNumbers.java` | 21; 20; 22 | `testplan/DatabaseJudge.java:374-376` (`numbers(env)`), into `crossNumberKinds` `:363-368` | the judge half DEL (S127's compiled-legend-pure equality after results is EDGE-HOST); the body-fold half is the translator's (3.4) |

### 2.2 The engine-text gates

| Member | Where | Lines | Callers | Class |
|---|---|---|---|---|
| `PlanStyle.judgesEngineText` | `database/PlanStyle.java:85`; the product's answer `:115-118`; the edge's `edge/PureText.java:70`, `:227`; `edge/SiteWordsRecorder.java:116-117` | 1 + 4 | `testplan/EngineTextGates.java:26`, `:94`, `:101`, `:107` | DEL (JUDGES-6) |
| `EngineTextGates` | `testplan/EngineTextGates.java` | 110 | `testplan/AssertVerdicts.java` 15 sites (`:85` `batchTexts`, `:190` `gate`, `:314`/`:817` `isRoot`, `:401`/`:411` `facet`, `:496-497`/`:914` `reachesUpstreamText`, `:845`/`:918` `armFor`, `:852`/`:919` `notAConstant`, `:855`/`:923` `why`); `testplan/BodyDriver.java:752/:756` (`marking`); `testplan/DatabaseAsserts.java:629` (`isRoot`); `testplan/EngineTextPlanning.java:88/:92`; `testplan/JudgePlans.java:326` (`sizeWords`); `testplan/PlanningOptions.java:64` (`planningCall`); `testplan/TestDoor.java:154` (`siteWords`) | DEL as a class. What it routes splits: the host-side arm calls collapse into ONE claim per assert (PLAN, §2.9); `batchTexts` and `gate` (the database judge's engine-text side) DEL; `planningCall`/`siteWords`/`sizeWords` (legend-pure's raise-site and size words recorded at planning) 1.2 |
| `EngineTestText` (the facet) | `testplan/EngineTestText.java:20` (13 methods); the edge's `edge/EdgeEngineTestText.java` (229) over `edge/SqlTextVerdicts.java` (1,675) | 80 | through `EngineTextGates` only | `plan`, `tryArm`, `tryArmSameSql`, `tryArmDualGolden`, `isRoot`, `reachesUpstreamText`, `notAConstant`, `why`: PLAN (the claim); `batchTexts`, `gate`: DEL; `recording`, `siteWords`, `sizeWords`: 1.2 |
| `EngineTextPlanning` (the product planner's toolkit the arm calls back into) | `testplan/EngineTextPlanning.java` | 234 | the edge: `edge/SqlTextVerdicts.java` 58 calls, `edge/EdgeEngineTestText.java` 4 | KEEP, shrinking: it is the dependency the right way round (the edge calls the product). Its verdict-batch half (`active`, `defer`, the appeal numbering's database use) DEL with the database judge's engine-text side |

### 2.3 The test runner's seams (`testexec`)

| Member | Where | Lines | Product call sites | Class |
|---|---|---|---|---|
| `EdgeRun` | `testexec/EdgeRun.java:16`: `open`, `wordsAlike` | 26 | `TestExecution.java:130-174` (every door method takes one), `testexec/TestRunner.java:52-58`, `:82-98`; `test/PureTestRunner.java:228/:236/:303/:330`, `test/TestObserver.java:51/:80-81` | replaced by the RUN interface (one object). `open` RUN; `wordsAlike` DEL (§2.5) |
| `EdgeHearing` | `testexec/EdgeHearing.java:23`: 10 methods | 66 | `testexec/TestRunner.java:98` (`open`), `:99` (`hostForms().words()`), `:109` (`listener`), `:126` (`afterFailure`), `:132` (`decide`), `:207` (`rowAppealed`), `:260/:279` (`hostForms().words()`); `testexec/BodyRun.java:39` (`caught`), `:60` (`judged`), `:65` (`writing`); `testexec/HostJudgments.java:310` (`hostForms`), `:329` (`json`), `:330` (`renderedTexts`), `:332` (`judged` via BodyRun); `testexec/AssertErrorRuns.java:62` (`hostForms().words()`) | `listener`, `judged`, `writing`, `decide`, `afterFailure`: RUN. `rowAppealed`: DEL (the database judge's row appeal). `raisedWords`, `json`, `renderedTexts`, `hostForms`: EDGE-HOST |
| `NoEdgeRun` | `testexec/NoEdgeRun.java:24` | 103 | the product's default at every door caller; `edge/ServiceTestRunner.java:289`, `test/TestObserver.java:81`; 30 test sites | DEL (the product's door passes no compatibility judge) |
| `HostWords` | `testexec/HostWords.java:19`: 27 methods (25 failure kinds, `name`, `caught`; H10 recount) | 110 | `testexec/HostJudgments.java` 23 calls (through `words(rules)`, `:24`), `testexec/AssertErrorRuns.java` 4, `testexec/DatabaseVerdicts.java:20`, `testexec/TestRunner.java:260/:279`, `testexec/AssertRuns.java:110`; the product's form `testexec/LiteHostWords.java` (224), the edge's `edge/EdgeHostWords.java` (235) | the interface DEL; `LiteHostWords` KEEPs as the host judge's one sentence table; `EdgeHostWords` EDGE-HOST |
| `HostForms` | `testexec/HostForms.java:12`: `equalAcrossNumberKinds`, `equalDecimals`, `equalFloatsApart`, `words` | 27 | `testexec/Equality.java:168`, `:185`, `:206` (all in `same`, `:142-219`, 80 lines), `testexec/HostJudgments.java:24`; carried by `testexec/HostRules.java` (16); the product's `testexec/LiteHostForms.java` (36), the edge's `edge/CompiledPureHostForms.java` (68) | see §2.9's open question: DEL only if the edge writes a host judge of its own |
| `RenderedTexts` | `testexec/RenderedTexts.java`; `testexec/LiteRenderedTexts.java`; the edge's `edge/UpstreamRenderedTexts.java` | 17; 17; 241 | `testexec/HostJudgments.java:330`, into `renderedOf` (`:877-905`, 29 lines) | the interface DEL; LITE's two strings KEEP; the ruled rows EDGE-HOST |
| `BodyRun` | `testexec/BodyRun.java:18` | 73 | `testexec/TestRunner.java:108`; the host judge's runs | KEEP: the runner's own run record; it carries the RUN interface's hearing instead of an `EdgeHearing` |

### 2.4 The test plan's edge hooks (`testexecutionplan`) and the options

| Member | Where | Lines | Product call sites | Class |
|---|---|---|---|---|
| `Edge` | `testexecutionplan/Edge.java:16`: `records`, `effect`, `seed`, `seeded`, `effectRan`, `siteWords` | 47 | `testplan/TestDoor.java:142-154` (`records`, `effect` `:144`, `seed` `:148`, `siteWords` via `:154`/`:189`); `testexec/TestRunner.java:188` (`records`), `:428`/`:440` (`seeded`), `:460`/`:466` (`effectRan`); `testplan/PlanningOptions.java:20` (a field) | `effect`, `seed`: PLAN. `seeded`, `effectRan`: RUN. `siteWords`: 1.2. `records`: DEL (the door's choice of judge says it) |
| `NoEdge` | `testexecutionplan/NoEdge.java:16` | 53 | `ExecuteOptions.java:37`, `testplan/TestDoor.java:109` | DEL |
| `ExecuteOptions.recording(Edge, DataStores)` | `ExecuteOptions.java:82-85` | 4 | no product caller: `pct/.../ChannelB.java:391`, `spec/.../MinimalCorpus.java:603`, core tests `EdgeLiveSourceTest:50`, `ServiceTestProvisionKeyTest:217`, `testing/EdgeProgram.java:32`, `GeneratedTestDataPrintingTest:89`, `BodyRouterParityTest:2051` | DEL; with it `ExecuteOptions.edge()` (`:42-44`), the `(Edge, DataStores, Set)` constructor (`:28-35`) and `PlanningOptions.edge` (`testplan/PlanningOptions.java:20`). The door takes the judge instead |
| `TestPlan.words`, `TestPlan.answerSizeWords`, `SideShape.sizeWords` | `testexecutionplan/TestPlan.java:45-48`, `SideShape.java:37`; written at `testplan/TestDoor.java:163-166`; checked at `testexec/TestRunner.java:99-106` | | | 1.2 (the plan's `words` and the run's mixed-words refusal go when no words are in the plan) |

### 2.5 JUDGES-3 and the isolated comparison

| Member | Where | Lines | Class |
|---|---|---|---|
| `IsolatedRun#difference`'s `messages` flag | `testexec/IsolatedRun.java:147-182` (40); the flag's branches `:154-163` and `:176`; fed by `EdgeRun.wordsAlike()` at `testexec/TestRunner.java:58`, `:88` and `IsolatedRun.run`'s parameter (`:97-105`) | 40 | DEL: with compatibility judging host-only, an isolated BOTH run is LITE's alone, so messages are always compared. §3.4's four big-number tests are exactly what the `!messages` branch hides today |

### 2.6 The other N-JUDGE branch sites (the census's M3 BRANCH rows)

| Site | Lines | What branches | Class |
|---|---|---|---|
| `testexec/Equality.java:142-219` (`same`), reached by `ordered` (`:229-239`), `contains` (`:249-256`) | 80 | asks `rules.forms()` for a cross-kind pair (`:168`), two Decimals (`:185`), two Floats apart (`:206`) | the product keeps lite's rule inline; the compiled-legend-pure answers EDGE-HOST |
| `testexec/HostJudgments.java:564-577` (`eqOf`), `:877-905` (`renderedOf`) | 14; 29 | the forms; the rendered-text rule | as above |
| `testplan/AssertVerdicts.java:1869-1890` (`textFirst`) | 25 | `RenderedTextRule` | DEL (§2.1) |
| `testplan/DatabaseJudge.java:336-338` (`lite`), `:363-368` (`crossNumberKinds`), `:298-314` (`written`) | 3; 9; 19 | `VerdictWords.reads()`, `CrossKindNumbers`, `DateLiteralTexts` | DEL of the compatibility arms |
| `compiler/spec/VerdictQueries.java:782-926` (`parseRendered`), `:976-981` (`tdsCellText`) | 148; 9 | `GoldenNullWord` | DEL of the word parameter |
| `lowering/VerdictSql.java` `:234`, `:343`, `:375`, `:574`, `:698`, `:895`, `:1286`; REFUSED `:701`, `:3067` | (file 3,108) | `VerdictNumbers`, `VerdictWords.reads()` | DEL of the compatibility arms (§2.1) |

### 2.7 Scan order and the ledger

| Member | Where | Lines | Class |
|---|---|---|---|
| `edge.StableScanOrder` | `edge/StableScanOrder.java`, applied by the edge's dialects' `scanOrderPasses` (`edge/PureText.java:367-368`; the product's hook `sql/dialect/DuckDb.java:745`, `:774`) | 598 | DEL once the edge host judge compares an unordered actual as a multiset (S131, S14, D14) |
| `sql.ScanOrder` | `sql/ScanOrder.java` | 152 | KEEP: the product uses it itself (`testplan/JudgePlans.java:392`, `lowering/CanonicalRenderSql.java:417`); only the edge's use goes |
| `edge.CompatEdge` (the `Edge` the corpus and PCT pass), `edge.RawLedger` | `edge/CompatEdge.java` | 167 | becomes the edge judge's own state, both interfaces' implementation |
| `EffectPlanner`'s referee copies | `testplan/EffectPlanner.java:56` (`Statement.recorded`), `:60-63` (`refereeDdl`, `Databases.REPLAY_REFEREE`), `:163-164` (`PlannedLoad.referee`), `:222-233` (the CSV seed's copy), `:252-254` (`ddl`: every DDL rendered twice) | about 30 | PLAN: the product hands the edge the MIR (`SqlDdl`, the CSV load) and the edge renders it in the referee's dialect. Today every effect DDL is rendered in the referee's spelling even under LITE, where nothing reads it (a small lean-path win when it moves) |

### 2.8 Test and guard callers that move with them (counted, not listed line by line)

The guard lists that name today's seams (the plan's 2.2 names them for the user's word): `HookRulingTest`
(`JsonGrammar` `:364/:418/:421`, `RelationCounting` `:424/:427`, `VerdictNumbers` `:372`, `RenderedTextRule` `:378/:436/:439`,
`DateLiteralTexts` `:380/:443/:446`, `GoldenNullWord` `:382/:449/:451`, `VerdictWords` `:393/:480-498`,
`CrossKindNumbers` `:369/:430/:433`, `EdgeRun`/`EdgeHearing`/`NoEdgeRun`/`NoEdge` `:47-65`, `:235-246`, `:514-523`,
`:613-615`), `HookLambdasTest` (each kind's two lines), `ProductFormsUseTheirArgumentsTest` (`:74-76`, `:198`),
`JavaEvalLedgerTest` (`:592-595` `DatabaseJudge`'s pin, `:1028` `EngineTextPlanning`, `:1053-1056` `HostJudgments`,
`:1478-1490`, `:1716-1734`), `PlanStyleSurfaceTest` (`judgesEngineText`, 6 sites), `ArchitectureTest` (`:2350`,
`:2397`, `:2927`, `:2958`, `:4079-4085`, `:5146`, `:5224`), `HookRulingsAgreeTest`, `EdgeSeparationTest`,
`VerdictChannelRegisterTest`, `PlannerSurfaceTest` (`:1138-1161`, `:1246-1248`). And 30 test call sites of
`NoEdgeRun.NONE` (the door's argument), plus the 7 `ExecuteOptions.recording` callers of §2.4.

### 2.9 The two narrow interfaces, and what the appeal needs

**What the code says the appeal needs, beyond F8's list.** F8 named, at planning, the effect texts (`TestDoor.java:144`),
the seeds' referee copies (`:148`, from `EffectPlanner`) and the engine-text assert's planned comparison; at run,
the session and leases after the run (`TestRunner.java:126`, `:132`), the per-step events (`:440`, `:466`) and the
verdict callbacks. All confirmed, at those lines. The code shows four more things:

1. **The engine-text arm claims asserts INSIDE the product's assert planning, not at the door.** `AssertVerdicts`
   asks the gates at 15 sites in three places (the statement-root arms `assertSameSQL` and the dual golden,
   `:401`/`:411`; the equality arm when a side reaches upstream's text, `:496`/`:914`; the classification of a root
   call, `:314`/`:817`/`:845`), and `BodyDriver:752/:756`, `DatabaseAsserts:629` and `JudgePlans:326` ask too. The arm
   plans through the planner's own environment (`BodyDriver.ExecEnv`, its frames, lets and text collector) via
   `EngineTextPlanning`, 58 calls from `SqlTextVerdicts` alone. That environment exists only inside `TestPlanner`'s
   run, so the claim cannot be made at `TestDoor` ahead of planning. **The planning interface therefore needs one
   call site in the test planner's per-assert entry, beside `TestDoor`'s.** It is a reshape of today's facet
   (13 methods into 1 claim), not a new hook; but done-when 1's "exactly two call sites (`TestDoor`, `TestRunner`)"
   cannot hold as worded: it is three (`TestDoor`, the assert entry, `TestRunner`), or two if the claim is counted as
   the test planner's and `TestDoor` is read as "the test planner".
2. **The appeal also hears at the verdict step**: `HostJudgments.java:332` hands an `EdgeJudged` comparison to the
   hearing (through `BodyRun:60`), and the hearing's `listener()` (`TestRunner.java:109`) holds the caller's reports
   back while an appeal is pending. Both are run state the runner owns; they belong to the RUN interface, reached
   through `BodyRun`, which `TestRunner` makes.
3. **Some of what the appeal does today is the database judge's and is deleted, not moved:** the database root's row
   appeal (`EdgeHearing.rowAppealed`, `TestRunner.java:207`, `RootLoops`' `RowAppeal` at `:50`, `:135`, `:147`,
   `:159`, `:233`, `:265`; `edge/Appeals.java:150-151`), the engine-text side the verdict row computes
   (`EngineTestText.batchTexts`, `EngineTextGates.batchTexts`, `testplan/BatchTexts.java`), and the database gate
   for an edge-judged comparison (`EngineTestText.gate`, `AssertVerdicts:190`, `testplan/DatabaseGate.java`). PCT and
   the corpus run host-only, so the database judge never meets an appeal.
4. **The referee's copies are rendered by the product today** (`EffectPlanner.refereeDdl`, `Databases.REPLAY_REFEREE`).
   To move them into the planning interface, the product hands the edge the MIR (`SqlDdl`, the CSV load) and the edge
   renders in its referee's dialect.

**The sketch.** Both are handed in at the door as one object (the edge's compatibility judge, appeal included); the
product's door passes none. Each is typed, data in and out, no product decision asked of it.

```java
// planning: com.legend.testplan
public interface CompatPlanning {
    /** At the test planner's per-assert entry (ONE site): the edge's arm for this assert, or null (the product plans
     *  it). Replaces EngineTestText's plan/tryArm/tryArmSameSql/tryArmDualGolden/isRoot/reachesUpstreamText/
     *  notAConstant/why and EngineTextGates. The arm plans through EngineTextPlanning, as today. */
    @Nullable TextPlanned claim(TextAsk ask);
    /** At TestDoor, per effect step: the referee's ledger records, from the statements' MIR. */
    List<Extension> effect(List<EffectStatement> statements);
    /** At TestDoor, per seed step: the referee's copy, from the plan's load. */
    List<Extension> seed(ExecutionPlan.Load load);
}

// run: com.legend.testexec
public interface CompatRun {
    Hearing open(TestPlan plan, @Nullable AssertListener callers, UUID execution);
    interface Hearing {
        @Nullable AssertListener listener();                 // reports held while an appeal is pending
        TypedSide judged(HostJudgment.EdgeJudged j, List<TypedSide> sides, boolean messaged);  // the appeal's step
        TypedSide judge(HostJudgment j, List<TypedSide> sides);  // the compatibility host judge (see below)
        void seeded(int step, List<Boolean> answeredRows);
        void effectRan(int step);
        void writing();
        void decide(Sessions.Session s, SystemDatabase.Leases l, Function<Target, Sessions.Session> sessions);
        RuntimeException afterFailure(RuntimeException e, Sessions.Session s, SystemDatabase.Leases l,
                Function<Target, Sessions.Session> sessions);
    }
}
```

What it removes: `EngineTestText` (13 methods), `EngineTextGates`, `judgesEngineText`, the 8 judge two-forms,
`Edge`/`NoEdge` (6 methods), `EdgeRun`/`EdgeHearing`/`NoEdgeRun` (12 methods), `wordsAlike`, `rowAppealed`,
`ExecuteOptions.recording`, `PlanningOptions.edge`. Product call sites into compatibility judging go from 10 classes
at planning (`TestDoor`, `TestPlanner`, `PlanningOptions`, `AssertVerdicts`, `BodyDriver`, `DatabaseAsserts`,
`DatabaseJudge`, `JudgePlans`, `EngineTextPlanning`, `VerdictBatch`) and 7 at run (`TestRunner`, `BodyRun`,
`HostJudgments`, `AssertErrorRuns`, `AssertRuns`, `DatabaseVerdicts`, `Equality` through `HostRules`) to 2 at
planning (`TestDoor`, the assert entry) and 1 at run (`TestRunner`, with `BodyRun` its record).

**The open design question: what is "a whole compatibility host judge"?** `Hearing.judge` above is where the edge
judges a comparison by legend-pure's rules and words. The product's host judge is about 2,765 lines (`HostJudgments`
906, `Equality` 451, `TdsCompare` 179, `CanonicalForm` 117, `PureAsserts` 173, `AssertRuns` 207, `AssertErrorRuns`
102, `LiteWords` 317, `LiteHostWords` 224, and the small forms). legend-pure's rules differ from lite's in only
four decisions (cross-kind numbers, Decimal scale, Floats apart, the rendered-text rule), one grammar (json-simple),
one order rule (multiset) and the words. Two ways:
- **(i) the edge writes a host judge of its own**, legend-pure's rules inline: `HostForms`, `HostWords`,
  `RenderedTexts` and the JSON seam leave the product (done-when 1 as written), at the cost of a second copy of most
  of those 2,765 lines at the edge (the copied-judging-code debt FOLLOW_UPS §8 calls not P0) and about 3 more days;
- **(ii) the product's host judge stays the library both judges call, with its rules a typed parameter** (one
  record of the four decisions, the grammar, the order rule and the words, passed by the caller, never asked of a
  hearing): no copy, but the parameter is `HostForms` by another name, and done-when 1 has to say "a rules
  parameter, not a per-decision seam".
This is the user's to rule (it changes done-when 1's wording); (ii) is the smaller and keeps one judging code path
(G1's spirit), (i) is the literal reading.

### 2.10 Re-sizing 2.1 and 2.2

- **2.1 (2 days in the plan): 2.5 days** (§1.3): none of the 36 is covered at `main`; all are small core tests; the
  half day is the defects they will surface (PCT's first LITE run found 5). With the LITE PCT lane (§3.5) adopted, 30
  rows to port, not 36.
- **2.2 (7.5 days: the judges 4, the appeal's move 3-4):**
  - the deletions are larger than the plan lists, and cheaper than moves: add the database judge's appeal paths
    (§2.9 item 3: `rowAppealed`, `RootLoops`' row appeals, `batchTexts`, the database gate) and `VerdictSql`'s
    CANONS arms; about 0.5 day more of deletion, with the guard lists;
  - the appeal's move: 3-4 days holds if the claim is allowed its one site in the assert entry (§2.9 item 1); the
    MIR hand-over for the referee's copies adds about 0.5 day;
  - the judges half: 4 days holds under (ii); under (i) add about 3 days for the edge's own host judge;
  - **so: 8.5 days under (ii), 11.5 under (i)**, plus the five parity findings of §3.4 if 2.2 must leave LITE's
    BOTH green on PCT (they are A5's today; about 1 day if they land there first).

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
| **LITE BOTH** | **786** | **463** | **5** | about 75 s (the longest suite 74 s; the first run's wall included the build) | 23-74 s |
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

### 3.4 The five judge-parity findings (sent to the previous orchestrator, A5's owner, 2026-11-19 ~11:05 UTC)

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

### 3.6 Channel A under LITE's planning style (legend-pure's interpreter judging lite's answers)

`-Dpct.channela.rules=LITE` (§5.1) plans every `execute` in the product's style (`PlanStyle.PRODUCT`) with the
product's default features; a raise is lite's own words (no `RaisedWords` recorder). The relation answer still crosses
to the interpreter as legend-engine's JSON (`EngineAnswers.engine`, the harness's crossing after the run), so the
interpreter's asserts judge lite's relation VALUES.

| Run | essential | grammar | relation | standard | unclassified | All (1,249) | Wall (5 suites, 3 at a time) |
|---|---|---|---|---|---|---|---|
| COMPAT (today's lanes, reproduced: all five PASSED their ledgers) | 324/21 | 132/5 | 468/1 | 205/0 | 93/0 | **1,222 / 27** | 129 s |
| LITE | 308/37 | 131/6 | **468/1** | 203/2 | 93/0 | **1,203 / 46** | 129 s |

(pass/fail; the COMPAT failures are the ledgers' expected ones, `one("..._Function...")` in
`Test_LegendLite_<Suite>Functions_PCT.java`.) Under LITE, 26 tests deviate from the ledgers: 7 are ledgered failures
failing with another message (`testComplexClassToString`, `testAtError`, the 5 date-part and `newDate` errors), 19 are
new failures, and every one of the 19 is a class of §3.3: R4's substring (4: `testStart`, `testStartEnd`, the 2 sort
keys), legend-pure's error words (6: `rem`, `slice`, `sqrt`, `range`, both bit shifts), legend-pure's print of a value
(7: 3 Float, 3 DateTime `toString`, 1 `format` date), `format`'s `%05d` and `%r` refused (2).

**The relation and standard suites do not move** (relation 468/1 and standard 203/2 under LITE, the standard pair the
bit shifts' words). So the 423 channel B relation-text failures are about the asserted TEXT alone: lite's relation
answers under its own rules are the ones legend-pure expects. Channel A under LITE is therefore the value check for
exactly the tests channel B's LITE run cannot judge, and costs 129 s on this container (four 4 GB JVMs fit 15 GB three
at a time).


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
- `pct/.../extension/PctPlanRoute.java`: `-Dpct.channela.rules=COMPAT|LITE|LITE_R4`. COMPAT is today's route,
  unchanged (all five channel A lanes PASSED with the switch in place); LITE plans in `PlanStyle.PRODUCT` with the
  product's default features and no `RaisedWords` recorder; LITE_R4 keeps the corrected substring indexing.

### 5.2 The commands (cloud container, Bazel 9.2.0 via bazelisk, 4 cores, 15 GB)

- Setup: `tools/cloud-distdir.sh <targets>` twice (the second for the upstream source archives the channel B targets
  read: `legend-engine-4.145.0.tar.gz`, `legend-pure-5.99.0.tar.gz`, packed from git clones, pins matched).
- Channel B, a variant: `bazel test //pct:pct_channel_b_{essential,grammar,relation,standard,unclassified}
  --jvmopt=-Dpct.channelb.rules=LITE [--jvmopt=-Dpct.channelb.judge=HOST|DATABASE] --nocache_test_results`; each
  outcome is a `[chB-h6]` line of the suite's `test.log`.
- The shape census: the same, plus `--jvmopt=-Dlegend.measure.shapes=stdout`; core's:
  `bazel test //core:core_tests_<package>_judge_both` for the 12 judged packages and `root`, with the same flag; each
  planned assert a `[shape]` line.
- Channel A: `bazel test //pct:pct_duckdb_{essential,...} --jvmopt=-Dpct.channela.rules=LITE --local_test_jobs=3`; each
  deviation from the ledger is a JUnit failure in the suite's `test.xml`.
- The collectors and classifier (`collect.py`, `classify.py`, `combos.py`) were scratch scripts; their logic is what
  §3.3 and §1.3 state (classes by the failure's own text and the test's name; combinations keyed by the assert's short
  name and each side's `SideShape.Kind` and one-or-many).

### 5.3 What was not measured

- The corpus: the 29 corpus-only rows of the 36 were not re-measured (no corpus lane run here); their counts are the
  shape-port record's.
- Any database but DuckDB (Q3, Q3b untouched).
- LITE's run of the 36 themselves: porting them is 2.1's work, not homework.

---

## 6. Plan r6's follow-ups (the orchestrator's messages of 2026-11-19 ~11:05-11:15 UTC)

### 6.1 Where each leniency changes a verdict (CP-14's measurement)

**The probe** (`com.legend.diagnostics.LeniencyProbe`, MEASUREMENT ONLY, §5.1): each leniency's decision site logs a
`[lenient]` line, with the running test, wherever the leniency's answer differs from lite's exact rule, and
`-Dlegend.measure.leniency.off=<ids>|ALL` makes the site answer lite's rule. The sites, at `main`'s lines:

| Id | What | Site | Today in LITE too? |
|---|---|---|---|
| S15 (S132's scalar half) | two Floats within 2 ULP equal | `edge/CompiledPureHostForms.java` `equalFloatsApart` (host) | no (`LiteHostForms.equalFloatsApart` false) |
| S132 | a rendered text's Float cell within 2 ULP | `edge/UpstreamRenderedTexts.java` `cellEquals` (host) | no |
| S131 | an unsorted (incidental-order) actual compared as a multiset | the PRODUCT's host judge: `testexec/HostJudgments.java:128-135` (the row-multiset fallback), `:819-821` (`equalsOf`), `:868-871` (a JSON root array); `testexec/TdsCompare.java:76`; and the edge's rendered texts (`UpstreamRenderedTexts.renderedTextVerdict`, unsorted) | **yes**: the narrowing to compatibility alone (the register's S131, ruled 2026-11-05) is "build pending"; the database judge has its own (`VerdictSql`'s `multiset` arguments) |
| S144, S366 | json-simple 1.1.1's grammar | `edge/JsonSimpleGrammar.java` `read` (host) | no (`JudgeJson.STRICT`) |
| S137 | a relation counted as one value | `edge/UpstreamRelationCounting.java` `counted` (planning) | no (refused by name) |
| S127 | an Integer and a Decimal equal by their texts (a rule, not a leniency; measured with them) | `CompiledPureHostForms.equalAcrossNumberKinds` | no |
| S139 | a Decimal's scale part of its value (stricter than lite) | `CompiledPureHostForms.equalDecimals` | no |

The database judge's own copies (`CompiledPureVerdictNumbers`, its multiset and json-simple reads) are not probed:
2.2a deletes the compatibility database judge, so only the host's matter for CP-14.

**Channel A runs no lite judge** (legend-pure's interpreter judges, §3.1): no leniency of lite's can change a channel A
verdict, on any database, by construction.

**The counts, by lane** (the compatibility rules, the HOST judge, which is where compatibility judging is going; each
lane run twice, the probe logging, then every leniency off):

| Lane | Tests | Pass (leniencies on) | Decisions a leniency changed | Pass (all off) | Verdicts changed |
|---|---|---|---|---|---|
| channel B DuckDB, 5 suites | 1,249 | 1,240 | **0** | 1,240 | **0** |
| channel B H2, relation | 469 | 432 | **0** | 432 | **0** |
| channel B SQLite, 5 suites | 1,249 | 1,222 | **0** | 1,222 | **0** |
| channel B DuckDB under LITE (S131 is live there today), then S131 off | 1,249 | 787 | **0** | 787 | **0** |
| **the corpus, DuckDB** (host pass `//spec:judge_host_duckdb`) | 5,786 asserts | fail roster 108 | **77** (S131 38, S137 21, S132 14, S15 2, S144 2) | fail roster **177** | **69 tests** (159 asserts) |
| **the corpus, H2** (`//spec:judge_host_h2`) | 5,533 asserts | fail roster 276 | **30** (S137 21, S131 4, S132 3, S144 2) | fail roster **305** | **29 tests** (80 asserts) |

(Every logging run is the lane's normal answer: channel B's H2 and SQLite lanes PASSED their own ledgers, and both corpus
passes' measured rosters, fail, skipped, unordered and engine-order, equal the committed ones byte for byte. So the probe
is inert when only logging, and the corpus is its positive control: the same hooks that log nothing on PCT log 107
decisions there.)

**The corpus, by leniency** (each newly failing test attributed to the leniencies its logging run recorded for it):

| Leniency | Corpus DuckDB: tests it decides | of which it alone | Corpus H2 | PCT (every lane) |
|---|---|---|---|---|
| S131, an unsorted actual as a multiset | **33** (typed values 14, a grid's rows 7, a rendered text's lines 12, a JSON root array 1; one, `mapping::union::testUnionViewJoins`, logged nothing and failed with it off: an unordered UNION whose rows DuckDB returned in another order on the second run) | 30 | **4** (values 3, a JSON root 1) | 0 |
| S137, a relation counted as one value | **21** (`query::routing::*` 13, `groupBy::*` 4, and 4 more) | 18 | **21** (20 now fail; 1, `mapping::dataType::testSimpleTypeMappingProject`, is refused instead) | 0 |
| S132, a rendered Float cell within 2 ULP | **14** (all `functions::pureToSqlQuery::calendarAggregations::*`, a CSV join of a TDS) | 14 | **3** (the same family) | 0 |
| S15, a scalar Float within 2 ULP | **2** (`mapping::sqlFunction::log::testFilter`, `testProject`: `0.09531017980432492 ~ ...493`) | 2 | 0 | 0 |
| S144/S366, json-simple | **2** (`graphFetch::...::testMilestonedRootAndMilestonedProperty`, twice: the golden is not JSON, strict refuses it) | 2 | **2** (the same) | 0 |
| S127, cross-kind numbers by text | 0 | | 0 | 0 |
| S139, a Decimal's scale | 0 | | 0 | 0 |

3 DuckDB tests need both S131 and S137 (`query::routing::testCompositionIn{GroupBy,Join,Union}`). Nothing passes with
the leniencies off that failed with them on. The corpus pass takes 541 s on DuckDB and 167 s on H2 here (565 s and
174 s with them off).

**The minimum each lane needs:**
- **PCT, every lane** (channel B on DuckDB, H2 and SQLite; channel A by construction): **none**.
- **The corpus on DuckDB:** S131, S137, S132, S15, S144 (69 of the lane's 2,517 judged tests, 2.7%).
- **The corpus on H2:** S137, S131, S132, S144 (29 of 2,404, 1.2%); not S15.
- **S127 and S139:** no lane.

**What it means for CP-14.** Option (iii)'s condition ("the leniencies change no verdict on any lane") holds for PCT and
fails for the corpus, by 69 tests on DuckDB and 29 on H2. Not all of them need the host judge, though, because three
of the five are about the test's INPUT, which the translator (phase 3, CP-1) rewrites at the door with functions
every lite user has:
- **S137** (21 tests): a body rewrite already (`UpstreamRelationCounting` runs at planning, `TestPlanner.java:227`):
  the translator's by nature, not a judging rule;
- **S144** (2 tests): the golden literal is not JSON; reading it with json-simple's grammar at the door (re-writing the
  literal as the JSON json-simple reads) leaves the judge strict;
- **S131's typed half** (values 14, a grid's rows 7, a JSON root 1 on DuckDB; 4 on H2): `assertEquals` over an
  actual whose order is incidental (`OrderView.INCIDENTAL`, a planning fact) rewrites to `assertSameElements`, which
  is a multiset by its own meaning;
- **S15** (2 tests): `assertEquals` of two Floats rewrites to `assertEqWithinTolerance` with legend-pure's 2 ULP of
  the expected value, a lite function.
What is left needs a comparison rule after the results: **a rendered text's rows as a multiset (S131, 12 DuckDB tests)
and its Float cells within 2 ULP (S132, 14 DuckDB and 3 H2)**, both inside `UpstreamRenderedTexts`, the edge's ruled
comparison of a golden text against a rendered TDS. So (iii) holds for the host judge if (a) the translator takes S137,
S144, S131's typed half and S15, and (b) the rendered-text comparison stays the edge's own small presenter-side
judge (241 lines today), or its 26 DuckDB tests (12 + 14, 3 on H2) become "not promised", listed. Either way the
product's host judge needs no rules parameter. (iii) as first conditioned, with no translator help, fails.

### 6.2 The planning half stays data: `testexecutionplan.Edge`

Plan r6 keeps `testexecutionplan.Edge` (`records`, `effect`, `seed`; `testexecutionplan/Edge.java:16`) as the planning
interface: the edge plans its effect and seed records into the test plan's typed extension point
(`testplan/TestDoor.java:141-149`, `TestPlan.extensionsByStep`, `testexecutionplan/Extension.java`). The map's §2.4
row changes accordingly: `effect` and `seed` are PLAN and STAY, `records` stays, `seeded` and `effectRan` move into
the run interface, `siteWords` is 1.2's; §2.9's MIR hand-over for the referee's copies is not needed (the product's
`EffectPlanner` keeps writing the referee's spelling, `:60-63`, `:252-254`) and its 0.5 day leaves the size.

**The decoded-plan guard still sees the records.** The corpus's DuckDB lane runs with `-Dlegend.diagnostics=decoded-plan`
(`spec/BUILD.bazel:193`): `TestPlanner.plan` writes the plan to JSON and reads it back (`testplan/TestPlanner.java:195-199`,
after `TestPlanJson.requireRoundTrip`, `:194`), and `TestPlanJson` writes and reads both `extensionsByStep` and
`extensionsByVerdict` through the edge's `ExtensionCodec`, found by `ServiceLoader`
(`testexecutionplan/TestPlanJson.java:121-124`, `:270-271`, `:285-339`). The records `Edge.effect`/`seed` plan are in
that JSON, so the guard reads them whichever interface writes them. Nothing in r6's design changes that path.

The per-assert claim (§2.9 item 1) is the other planning seam; r6 keeps it as the reshaped `EngineTestText` (one
claim). It writes `extensionsByVerdict` records the same way (`TestPlanner.java:186-193`, `PlannedAppeals`).

### 6.3 Where the referee gets its session today

The referee has no session of its own. It replays a golden on the session of the target its appeal names
(`edge/SqlTextRuns.java:188-190`: `referee.verify(sessions.apply(r.on()), ...)`), and that target is the body's own,
planned with the assert (`edge/SqlTextVerdicts.java:1467-1472`: `EngineTextPlanning.target(appealOn(env) ?? env)`).
The run hands the appeals its sessions after the body (`testexec/TestRunner.java:132`, `EdgeHearing.decide`), mapped
by `sessionsByTarget` (`:143-151`): the graph's system database for a system target, else the body's session. Only a
golden whose database is H2 is replayed (`database/Databases.java:31`, `REPLAY_REFEREE = H2`;
`edge/SqlTextVerdicts.java:588-595`: any other is "foreign-dialect", text the contract).

So on the H2 lanes the replay runs on the body's H2 session, which the plan's target opens with the edge's settings,
legend-engine's `MODE=LEGACY` and `DEFAULT_NULL_ORDERING=HIGH` (`exec/H2Connector.java:15-19`; the two-form
`DatabaseSettingsForm`, J22, S368). Removing the edge's H2 settings (CP-8) would change the session the referee replays
legend-engine's goldens on. The independent review's point holds: the referee needs a session of its own, a
legend-engine H2 LEGACY session declared as the referee's target, before the edge's H2 settings go. That is a new
target the appeal plans (data), not a new hook.

### 6.4 Option (iii): how far the host judge's words are already separable

- **The verdict comes first, the words only after, by construction.** `HostJudgments.finish`
  (`testexec/HostJudgments.java:58-84` at `main`) decides `hostHeld == wantEqual` and asks the words' supplier only on a
  failure; "a method words a failure, it never decides one" (`testexec/HostWords.java:9-15`).
- **Each wording call is already a kind with operands**: the `HostWords` method is the kind, its arguments the facts
  the verdict decided. The 29 sites at `main`: `HostJudgments.java:142` `rowValuesDiffer`, `:144` `notEqual`, `:159`
  `cellMultisetDiffers`, `:390`/`:395` `notEquivalent`, `:413` `grids`, `:443` `size`, `:457` `notContained`, `:476`
  `notWithin`, `:521` `negated`, `:529` `condition`, `:560` `emptiness`, `:572` `notEq`, `:594`
  `notInstanceOfClass`, `:598` `notInstanceOf`, `:605` `notSameInstance`, `:669`/`:731` `atIndex`, `:729`
  `rowNotEqual`, `:831` `sidesNotEqual`, `:832` `notEqual`, `:839` `sameElements`, `:873` `jsonDiffers`;
  `AssertErrorRuns.java:64` `noError`, `:74` `errorDiffers`, `:93` `lineDiffers`, `:96` `columnDiffers`;
  `DatabaseVerdicts.java:36` `unjudged`; `BodyRun.java:39` `caught`.
- **What is missing for (iii):** a record type. Today each supplier returns a String at the verdict step
  (`Verdicts.fail(String)`), so `TypedSide`, the listener and `AssertFailed` carry text. (iii) makes `fail` carry a
  record (the method's name and arguments), and lite's `LiteHostWords` and legend-pure's `EdgeHostWords` become its two
  presenters, called by the door's audience after the verdict.
- **Two places where the words feed a decision**, which (iii) must handle:
  - `assertError` matches the test's expected text against the raise's words (`BodyRun.caughtMessage`, `:36-40`, into
    `HostWords.caught`; under compatibility the raise site's legend-pure words from the plan's records,
    `EdgeHearing.raisedWords`). Under (iii) the compatibility judge presents the raise record in legend-pure's words
    and then compares: a presenter called inside judging, at that one site;
  - the isolated BOTH comparison compares messages under LITE (`IsolatedRun.java:147-182`); with records it compares
    the records (stronger: no wording drift can hide a difference), and lite's presenter for the database judge's SQL
    words stays parity's question.
- `cellMultisetDiffers` takes `HostRules` (`:159`): a rules read inside wording, removed with the rules.

### 6.5 2.2a and 2.2b sized separately

| Part | Work | Days |
|---|---|---|
| **2.2a** delete the compatibility database judge's half; PCT host-only | the 8 two-forms' database halves and `VerdictSql`'s compatibility arms (§2.1, §2.6) | 1.5 |
| | the database judge's appeal paths (§2.9 item 3) | 0.5 |
| | PCT's compatibility runs HOST-only on every lane; `IsolatedRun`'s `!messages` branch (JUDGES-3) once no compatibility run is isolated; the channel B judge-SQL pins (`ratchets.tsv`'s `judge.*`, the reference lane under BOTH) re-pinned with the user's word | 0.5 |
| | the residue pins (`DatabaseJudge` 852 at `main`, `HostJudgments`, `BodyDriver`, `EngineTextPlanning`) and the guard lists, with the user's word | 0.5 |
| | checks: channel B HOST unchanged on every lane, core BOTH, the corpus (already host-only) unchanged | 0.5 |
| | **2.2a** (precondition: 2.1's LITE-PCT lane) | **3.5** |
| **2.2b** the appeal's run seams | one run interface for `EdgeRun`/`EdgeHearing`/`NoEdgeRun` (open, listener, judged, seeded, effectRan, writing, decide, afterFailure), routed through `TestRunner` and `BodyRun`; the door's choice of judge for `ExecuteOptions.recording` and its 7 callers | 2 |
| | the planning claim: `EngineTestText`'s 13 methods into one, `EngineTextGates` and `judgesEngineText` deleted | 1 |
| | the words: **(ii)** a typed rules-and-words parameter, 1; **(iii)** verdict records and two presenters, 2.5 (1.5 if 1.2's error records land first and are reused) | 1 / 2.5 |
| | the leniency code: **(ii)** into the edge's rules form, 0.5; **(iii)** S127 and S139 deleted (unused everywhere, §6.1); the rendered-text comparison (S131/S132 over a rendered text) kept as the edge judge's own decision for the `Rendered` judgment kind, a whole-kind override, not a per-decision seam, 0.5 | 0.5 |
| | checks and the guard lists | 1 |
| | **2.2b**: **(ii) 5.5, (iii) 7** (6 with 1.2's records) | |
| **2.2 in all** | **(ii) 9 days, (iii) 10.5 (9.5 with 1.2's records)** against my earlier 8.5 under (ii): the split's second landing and re-checks add about 0.5, the dropped MIR hand-over (§6.2) takes 0.5 away, and the claim's day, inside "the appeal's move" before, is now its own line | |

(iii) is not cheaper in days than (ii): it trades the rules parameter for the records. It is the smaller END STATE
(no rules in the product at all, words a presenter like every other upstream word), which is why the architecture
prefers it. After §6.1 it also needs the translator (phase 3) to take four input rewrites the corpus depends on: S137
(moved, it is a body rewrite today), S144 (the golden literal read by json-simple at the door), S131's typed half
(`assertEquals` over an incidental order to `assertSameElements`) and S15 (`assertEquals` of Floats to
`assertEqWithinTolerance`): about 1.5 days in phase 3, not 2.2's. Until they land, the corpus's 69 (DuckDB) and 29
(H2) tests need the leniencies where they are.

### 6.6 The LITE-PCT lane's numbers, as r6 asks them

- **Counts** (DuckDB, the five suites): 786 pass, 463 fail, 5 judges-disagree (§3.2); essential alone: 312 pass, 33
  fail, 5 disagree.
- **Failures from LITE's exact rules where S15, S131, S132 apply today: 0 on PCT.** Measured directly (§6.1): S15 and
  S132 are exact in LITE already and change no compatibility verdict on any channel B lane; S131, still live in LITE,
  was turned off under LITE and moved no verdict. The 463 are presentation (450), R4 (7), parity (5) and pre-existing
  (2). The lane's known-failure ledger therefore needs no leniency row. (The corpus is another matter, §6.1, but the
  LITE-PCT lane does not run the corpus.)
- **Run time:** about 75 s for the five suites (§3.5); essential 57 s.

### 6.7 How §6 was measured

- The probe: `core/.../diagnostics/LeniencyProbe.java` and the hooks named in §6.1's table (commit `5fa0464e`, MEASUREMENT
  ONLY). Channel B: a scratch script (`len.sh`, not committed) runs `--jvmopt=-Dpct.channelb.judge=HOST --jvmopt=-Dlegend.measure.leniency.log=1
  [--jvmopt=-Dlegend.measure.leniency.off=ALL]`, outcomes from the `[chB-h6]` lines, decisions from the `[lenient]`
  lines. LITE's S131: the same with `-Dpct.channelb.rules=LITE` and `off=S131`.
- The corpus: its host pass is a build action with a fixed environment, so the two flags were added to
  `spec/BUILD.bazel`'s `corpus_duckdb` and `corpus_h2` `jvm_flags` for the two runs and then restored to `main`'s lines;
  `bazel build //spec:judge_host_{duckdb,h2}`; outcomes from each pass's `judge-host.tsv` and measured rosters,
  decisions from `host.log`.
- Attribution: a newly failing test is credited to the leniencies its logging run recorded for it. One DuckDB test
  logged none and is S131's by its failure (rows in another order). A per-leniency OFF run for each id was not made;
  the attribution's only ambiguity is the 3 DuckDB tests that log both S131 and S137.

---

## 7. The shape note for 2.2a and 2.2b under CP-14 (iii) (the brief to build from)

The user ruled CP-14 (iii) on 2026-11-19 (~12:10 UTC, "Yes to the six", relayed by the orchestrator). The product's
host judge emits verdict RECORDS (kind and operands); legend-pure's words are a presenter's; no rules parameter. Four
door rewrites of corpus test bodies cover the corpus's leniencies; the rendered-text comparison stays the edge judge's
own rule for the `Rendered` judgment kind. No build until phases 1-3 are ratified. Paths under
`core/src/main/java/com/legend/` at `main` `b75d6c73a`.

### 7.1 The end state

- **One object at the door.** A compatibility caller (PCT channel B, the corpus, the core edge tests) hands the test door
  ONE judge: `edge.CompatJudge`. A LITE caller hands none: the door's default is the product's judge. `ExecuteOptions`
  carries the judge in place of `edge` (`ExecuteOptions.recording` and the `(Edge, DataStores, Set)` constructor go).
- **Three call sites in the product**, each through one narrow typed interface:
  1. `testplan/TestDoor.java:141-155`, the planning records: `testexecutionplan.Edge` (kept, r6);
  2. the test planner's per-assert entry, the claim: `testplan.EngineTextClaim` (the reshaped `EngineTestText`);
  3. `testexec/TestRunner.java:98-132` and the run sink's step events (`:425-468`): `testexec.JudgeRun`, whose hearing
     `BodyRun` carries to the host judge (`HostJudgments`' verdict step and `Rendered` kind).
- **The product's host judge has lite's rules only**, inline, and emits a `VerdictRecord` on a failure. Lite's words
  are `LiteHostWords` as a presenter of records; legend-pure's are `EdgeHostWords` as a presenter of the same records.
- **The database judge is LITE's alone** (2.2a): no compatibility arm anywhere in `VerdictSql`, `DatabaseJudge`,
  `AssertVerdicts`' database half, `VerdictQueries`.

### 7.2 The two interfaces' method sets

```java
// PLANNING (kept, r6): com.legend.testexecutionplan.Edge -- writes the edge's records into the test plan (data)
public interface Edge {
    boolean records();                                                   // whether the door plans records for it
    List<Extension> effect(List<String> texts, List<Boolean> queries);   // TestDoor.java:144
    List<Extension> seed(List<ExecutionPlan.LoadStep> steps, Map<String, String> content);  // TestDoor.java:148
}   // seeded/effectRan move to the run interface; siteWords goes with 1.2 (words out of the plan)

// PLANNING, the claim: com.legend.testplan.EngineTextClaim (EngineTestText's 13 methods reshaped to one)
public interface EngineTextClaim {
    /** The edge's arm for this assert root, or null: the product plans it. Asked once per assert, at the per-assert
     *  entry; the arm plans through EngineTextPlanning (the product's toolkit, dependency the right way round). */
    @Nullable TextPlanned claim(TextAsk ask);
}

// RUN: com.legend.testexec.JudgeRun (replaces EdgeRun, EdgeHearing, NoEdgeRun, HostForms, HostWords, RenderedTexts,
// the run's JsonGrammar)
public interface JudgeRun {
    Hearing open(TestPlan plan, @Nullable AssertListener callers, UUID execution);  // TestRunner.java:98
    interface Hearing {
        @Nullable AssertListener listener();                    // reports held while an appeal is pending (:109)
        TypedSide judged(HostJudgment.EdgeJudged j, List<TypedSide> sides, boolean messaged);  // the appeal's step
        TypedSide rendered(HostJudgment.Rendered j, List<TypedSide> sides);  // the edge's own rule for the kind
        boolean errorMatches(String expected, VerdictRecord.Raised raised);  // assertError's match (§7.6)
        void seeded(int step, List<Boolean> answeredRows);      // TestRunner.java:440
        void effectRan(int step);                               // TestRunner.java:466
        void writing();
        void decide(Sessions.Session s, SystemDatabase.Leases l, Function<ExecutionPlan.Target, Sessions.Session> by);
        RuntimeException afterFailure(RuntimeException failed, Sessions.Session s, SystemDatabase.Leases l,
                Function<ExecutionPlan.Target, Sessions.Session> by);
    }
}
```

The product's own judge (the door's default) answers `rendered` with lite's two-strings rule (`LiteRenderedTexts`'s
answer today), `errorMatches` with the raise's own message, `judged` and the appeal methods by refusing by name (no
appeal is planned under LITE: `NoEdgeRun`'s refusals, kept as the default's), and the step events with nothing. The
`VerdictRecord` family is the product's (`testexecutionplan`, data): one record per kind `HostWords` names today (28,
§6.4's list), its fields the method's arguments; `Verdicts.fail(VerdictRecord)` replaces `Verdicts.fail(String)`.

### 7.3 2.2a: what it deletes and what it touches (3.5 days; precondition: 2.1's LITE-PCT lane)

| Delete | Where (`main`) |
|---|---|
| the 7 judge two-forms' database halves and their edge forms: `VerdictNumbers` + `LiteVerdictNumbers` + `CompiledPureVerdictNumbers`; `RenderedTextRule` + `LiteRenderedTextRule` + `UpstreamRenderedTextRule`; `DateLiteralTexts` + `LiteDateLiteralTexts` + `UpstreamDateLiteralTexts`; `GoldenNullWord` + `LiteGoldenNullWord` + `UpstreamGoldenNullWord`; `VerdictWords`' edge form `UpstreamVerdictWords` and `VerdictReads`; `CrossKindNumbers` + `LiteCrossKindNumbers` + `CompiledPureNumbers`; `JsonGrammar`'s database read (`DatabaseJudge.java:1078` reads strict) | §2.1 |
| `VerdictSql`'s compatibility arms: every `floatTolerance()` branch and its CTEs; every `reads() == WRITTEN` branch's CANONS arm and the canon-only helpers | `lowering/VerdictSql.java` §2.1, §2.6 |
| `DatabaseJudge`'s compatibility arms (`lite`, `crossNumberKinds`, `written`'s date form, `numbers`, `verdictNumbers`) | `testplan/DatabaseJudge.java:298-376` |
| `AssertVerdicts.textFirst`'s ruled branch; `VerdictQueries`' null-word parameter | `:1869-1890`; `compiler/spec/VerdictQueries.java:782-981` |
| the database judge's appeal paths: `EdgeHearing.rowAppealed`, `TestRunner.java:207`, `RootLoops`' `RowAppeal` (`:50`, `:135`, `:147`, `:159`, `:233`, `:265`), `edge/Appeals.java:150-151`; `EngineTestText.batchTexts` and `gate`, `EngineTextGates.batchTexts`/`gate`, `testplan/BatchTexts.java`, `testplan/DatabaseGate.java`, `AssertVerdicts.java:85`, `:190` | §2.9 item 3 |
| `IsolatedRun#difference`'s `messages` flag and `!messages` branches (JUDGES-3), `EdgeRun.wordsAlike`: no compatibility run is isolated once PCT is host-only | `testexec/IsolatedRun.java:154-163`, `:176`; `TestRunner.java:58`, `:88` |

Touches: PCT channel B's compatibility runs go HOST (`ChannelB.OPTIONS`'s judge, every lane); the channel B judge-SQL
pins (`ratchets.tsv`'s `judge.*`, the reference lane under BOTH) and the residue pins (`DatabaseJudge` 852,
`HostJudgments`, `BodyDriver`, `EngineTextPlanning`) re-pinned with the user's word; `core_edge_tests_judge_{database,both}`
become host-only; the guard lists naming the deleted kinds (§2.8). Checks: channel B HOST unchanged on every lane (the
leniencies change nothing there, §6.1), core BOTH green, the LITE-PCT lane green against its ledger, the corpus
(already host-only) unchanged.

### 7.4 2.2b: what it moves and what it deletes (6-7 days, plus the rewrites' 1.5 in phase 3)

| Item | From | To |
|---|---|---|
| the run seams | `testexec/EdgeRun.java`, `EdgeHearing.java`, `NoEdgeRun.java` | `testexec.JudgeRun` (§7.2); the product's default judge; `edge.CompatJudge`'s run half (today `AppealRun` + `Appeals`' hearing) |
| the step events | `testexecutionplan.Edge.seeded`/`effectRan`, called at `TestRunner.java:440`, `:466` | `JudgeRun.Hearing.seeded`/`effectRan` |
| the claim | `EngineTestText` (13 methods), `EngineTextGates` (15 sites in `AssertVerdicts`, `BodyDriver:752/:756`, `DatabaseAsserts:629`, `JudgePlans:326`, `PlanningOptions:64`, `TestDoor:154`), `PlanStyle.judgesEngineText` | `EngineTextClaim.claim`, asked at the per-assert entry; `edge.CompatJudge`'s claim half over `SqlTextVerdicts` (unchanged) |
| the host judge's rules | `HostForms`, `HostRules`, `LiteHostForms`, `CompiledPureHostForms` (`equalAcrossNumberKinds` S127, `equalDecimals` S139, `equalFloatsApart` S15); `testexec/Equality.java:168`, `:185`, `:206` | lite's rule inline in `Equality.same`; S127 and S139 deleted (no lane uses them, §6.1); S15 becomes the door rewrite R-S15 |
| the words | `HostWords` (27 methods: 25 kinds, `name`, `caught`; H10 recount), `LiteHostWords`, `EdgeHostWords`, the 29 sites of §6.4 | `VerdictRecord` (the product's); `LiteHostWords` and `EdgeHostWords` as presenters of records; the door presents after the run (the listener and `AssertFailed` carry the record; H10 §6: presenting at the failure site through the hearing is enough, and smaller) |
| the rendered-text rule | `RenderedTexts`, `LiteRenderedTexts`, `HostJudgments.java:330`, `edge/UpstreamRenderedTexts.java` (241) | `JudgeRun.Hearing.rendered`: the product's default answers the two strings; `edge.CompatJudge` answers with `UpstreamRenderedTexts`' ruled comparison (S131 rendered lines, S132 cells), unchanged |
| the JSON grammar at run | `EdgeHearing.json()`, `HostJudgments.java:329`, `edge/JsonSimpleGrammar.java` (341) | the host judge reads strict JSON; json-simple moves to the door rewrite R-S144 |
| the relation counting | `compiler/spec/RelationCounting.java`, `RelationCountAsserts` (`REFUSED` stays the product's), `edge/UpstreamRelationCounting.java`, `TestPlanner.java:227` | the planner keeps lite's refusal (`RelationCountAsserts.refuse`); the rewrite is R-S137 at the door |
| the scan order | `edge/StableScanOrder.java` (598), `PureText.scanOrderPasses` | deleted once the corpus is measured without it under the rewrites (it fixes DuckDB's row order to H2's for tests that pass positionally and so log no leniency, §6.1: a check of its own, before the deletion) |
| the door's options | `ExecuteOptions.recording`, `ExecuteOptions.edge()`, `PlanningOptions.edge`, `NoEdge` | the door's judge parameter; `PlanningOptions` carries the planning half (`Edge`, `EngineTextClaim`), the run the `JudgeRun` |

### 7.5 The four door rewrites (the translator's, phase 3; ~1.5 days)

Where they run: an edge door for compatibility test bodies (`edge.CompatTestDoor`), on the TYPED body: after
`Compiler.queryResolved`, before `TestExecution.planBody`/`runProgram`, through `TypedQuery.withBody(List<TypedSpec>)`
(public, `TypedQuery.java:43`). They walk the whole body recursively (an assert in a value's position too: the lift
runs later, inside `TestPlanner`). Each is an input rewrite into ordinary lite functions; none adds a product hook.

| Rewrite | Input (matched on the typed body) | Output | Lanes it serves (§6.1) |
|---|---|---|---|
| **R-S137** | a call of `assertSize`, `assertNotEmpty` or `assertEmpty` (`NativeFn.Verdict.ASSERT_SIZE`, `ASSERT_NOT_EMPTY`, `ASSERT_EMPTY`) whose first argument's type is a relation (`Type.isRelation`) | `assertSize`/`assertNotEmpty` with that argument replaced by the Integer literal 1 (`TypedCInteger(1L, Integer[1])`): `assertSize($rel, n)` holds only for n = 1, `assertNotEmpty($rel)` always holds, as upstream counts; `assertEmpty($rel)` refused by name with today's text ("say assertEquals(0, $relation->size())"). Today's code, moved whole: `UpstreamRelationCounting.countedAsOne` | corpus DuckDB 21, H2 21 |
| **R-S144** | `assertJsonStringsEqual(e, a)` (and its message forms) whose `e` is a String constant of the plan (a literal or a constant concatenation, as in both corpus tests: `'[{...},' + ' {...}]"'`) that strict JSON refuses and json-simple 1.1.1 reads | the same call with `e` replaced by the literal of the prefix json-simple consumed (the root value; in both tests the golden without its stray trailing `"`), checked strict; where that prefix is not strict JSON, json-simple's value written as JSON (an object's key order is free: the assert compares parsed values). A constant json-simple refuses, or a non-constant `e`, is left as is (strict then refuses it, as lite does). S366's `}{` needs no rewrite: no lane meets it (§6.1) | corpus DuckDB 2, H2 2 (`graphFetch::milestoning::testMilestonedRootAndMilestonedProperty`, `graphFetch::embedded::otherwise::...`) |
| **R-S131** (typed half) | `assertEquals(e, a)` (and its message forms) where the actual's order is incidental, `OrderView.of(a, letPrefix) == OrderView.INCIDENTAL` (`compiler/spec/OrderView.java:97`, public; `letPrefix` the body's statements before it), and the pair is not two relations (that is the grid pair's own judgment) | `assertSameElements(e, a)` with the same message argument. Exact where `a` is a list of values (the host judge already compares an incidental `assertEquals` by `Equality.sameElements`, `HostJudgments.java:819-821`, the very function `assertSameElements` uses) and where `a` is the flat cells of a one-column TDS. **Weaker** where `a` is the flat cells of a TDS of two or more columns: `assertSameElements` pools cells, today's fallback matches row tuples (`Equality.rowMultiset(te, ta, w)`, `HostJudgments.java:128-135`); at least 3 of DuckDB's 7 cell cases have two columns (`testDynafunctionMergeWithProjectionQueryWithInnerJoinTableFilter`, `testConcatenateClassJoin`, `query::routing::testCompositionInJoin`) | corpus DuckDB 21 (values 14, cells 7), H2 3 |
| **R-S15** | `assertEquals(e, a)` where `e` is a Float literal or a literal list of Floats and `a` is Float-typed (both corpus tests: `assertEquals([0.09531017980432493, 0.5877866649021191], $result.values->at(0).rows.values)`) | for one value, `assertEqWithinTolerance(e, a, d)`; for a list, `assertEquals(<n>, a->size())` then, for each i, `assertEqWithinTolerance(e_i, a->at(i), d_i)`; each `d` = 2 × `Math.ulp(|e_i|)` written at the door as a Float literal (a build-time constant from the program's text). `assertEqWithinTolerance` takes one number a side (`assertEqWithinTolerance.pure:17`), hence the expansion; the body's assert count grows, so the per-assert ledger's lines move, the per-test rosters do not | corpus DuckDB 2 (`mapping::sqlFunction::log::testFilter`, `testProject`), H2 0 |

**Not covered by the four, for the user's list:**
- **the JSON root array compared as a multiset** (S131 in `HostJudgments.jsonOf`, `incidentalRoot`, `:868-871`): 1
  test on each corpus lane (`graphFetch::tests::union::propertyLevel::test6`). An `assertJsonStringsEqual` cannot become
  `assertSameElements`. Either the edge judge's own rule for the `Json` kind with an incidental root (a second
  whole-kind rule beside `Rendered`, one more `Hearing` method), or "not promised", listed;
- **R-S131's weaker cell pool** for TDSes of two or more columns: a check in phase 3 that no corpus test failing today
  passes under it; if one does, those tests take the `Json`-like kind rule instead.

### 7.6 The one place words decide a verdict

`assertError` matches the test's expected text against the raise. Today: `BodyRun.caughtMessage`
(`testexec/BodyRun.java:36-40`) asks the hearing's words (`HostWords.caught`), which under compatibility looks the
raise site's legend-pure words up in the plan's records (`EdgeHearing.raisedWords`; K10). Under (iii) the raise is a
record (its kind, place and operands: 1.2's error record), and the match is `JudgeRun.Hearing.errorMatches(expected,
raised)`: the product's default compares the raise's own message; `edge.CompatJudge` presents the record in
legend-pure's words and compares. That is the only presenter call inside judging; every other word is presented after
the verdict.

### 7.7 What 2.2 must show before it lands (the checks, by lane)

- **PCT** (channel B, every lane): HOST unchanged (the leniencies change nothing there); channel A unchanged (no lite
  judge); the LITE-PCT lane green against its ledger (786 pass on DuckDB at `main`).
- **The corpus**, DuckDB and H2: with R-S137, R-S144, R-S131, R-S15 at the door and no leniency in the host judge, the
  committed rosters unchanged except the listed rows: the JSON root test (1 per lane) unless its kind rule is built, and
  any move R-S131's cell pool causes (expected none); then the same with `StableScanOrder` off, its moves listed.
- **Core:** BOTH green (core is LITE: no compatibility judge reaches it), and the guards of done-when 1 (three call
  sites, no `HostForms`, `HostWords`, `EdgeRun`, `EdgeHearing`, `NoEdge` type left).
