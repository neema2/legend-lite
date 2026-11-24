# B3: test checking moves out of the planner -- progress record

The plan of record: `docs/records/compat-stage-b-plan-2026-11-20.md` on `claude/compat-program`, section B3 ("option
N"), every decision ruled there with its time; the 2026-11-24 additions (legend-engine's door rewriting `==`, `!=`,
`in` and `contains` to the database's comparison; the one equality, strict typing, as B3's last step):
`docs/records/compat-stage-b-plan-2026-11-24.md`. Branch `claude/compat-b3-nufdlk`, stacked on item 3 (PR #132).

## The one rulebook, measured (2026-11-24)

The judge has one rulebook, lite's (E1 withdrawn, 2026-11-20 ~21:00). The edge's run hook (`EdgeHearing`, answered by
`edge/Appeals`) still brings compatibility's own rules into the run, through five hooks: `judged` (the SQL-text
appeals), `json()` (S144, json-simple's grammar), `renderedTexts()` (S131, S132), `hostForms()` (compiled legend-pure's
host equality, `edge/CompiledPureHostForms`, and the error wording `caught`) and `writing()`. The product answers
lite's to each (`testexec/NoEdgeRun`).

**Run 1: `hostForms()`'s four equality rules switched to lite's** (`LiteHostForms.STRICT`; the wording `caught`
kept, it is presentation). The rules that differ:

| Rule | compiled legend-pure (the edge's) | lite's |
|---|---|---|
| an Integer against a Decimal | equal by their texts (`1D == 1`) | never equal (S127) |
| two dates | equal as written | by kind and instant (S399) |
| two Decimals | scale part of the value | by value (S139) |
| two Floats | within 2 ULP (S15/S132) | exact |

Lanes, on item 3's head 96e7c1fc with the switch applied (temporarily, never committed): PCT channel B on the
compatibility rules, DuckDB's and SQLite's five suites and H2's relation suite; the corpus on DuckDB and H2; the edge
net. **Result: no PCT verdict moves; the H2 corpus does not move; the DuckDB corpus moves 2 tests, both S15** --
`meta::relational::tests::mapping::sqlFunction::log::testFilter` and `::testProject`: DuckDB's `ln(1.1)` is
`0.09531017980432493`, the expected `0.09531017980432492` (one ULP apart); PASS before, FAIL with lite's exact Float.
The date, Decimal and number-kind rules move nothing on any lane. This matches the plan of record's H6 count for S15
(DuckDB 2).

Next, by the plan: ask the oracle (the engine runner, legend-engine 4.145.0) for legend-engine's own answer to the two
on DuckDB, class them (agrees with lite / float printing / a real difference), and bring the counts and names to the
user before the rule is deleted.

**Run 2: `renderedTexts()` switched to lite's as well (S131, S132),** on the same lanes and head. **No PCT verdict
moves on any lane. The corpus moves 28 DuckDB tests and 3 H2 tests**, every one from PASS to FAIL:

| Leniency | DuckDB corpus | H2 corpus |
|---|---|---|
| S131 (an unsorted actual compared as a multiset) | 12: the right rows in another order -- `advanced::structure::testQualifierWithForkAndOrWithInline`, `::testQualifierWithLineAndOrWithInLine`, `mapping::classMappingFilterWithInnerJoin::testMergeForClassMappingInnerJoinFilter`, `::testMergeForClassMappingInnerJoinFilter1`, `mapping::distinct::testDistinctMappingWithFullDenormSelfJoins`, `mapping::union::specialUnion::testGroupBy`, `::testProjectWithPostTdsOperations`, `postProcessor::testSqlRealiasFreeMarker`, `projection::function::concatenate::testConcatenateWithFilter`, `projection::view::testUnionOnViewsMapping`, `::testViewOnView`, `validation::showcase::standalone::validateAllConstraints` | 0 |
| S132 (Float tokens in a rendered text within 2 ULP) | 14: `functions::pureToSqlQuery::calendarAggregations::*` -- DuckDB's Float arithmetic printed with its last digits (`6.840000000000002` against the golden's `6.84`) | 3: `calendarAggregations::testAnnualizedValue`, `::testCmeValue`, `::testPmaValue` -- H2 2.5 prints the Decimal with more digits (`9.81285067873303158` against `9.812850678733032`) |
| S15 (two Floats within 2 ULP) | 2: `mapping::sqlFunction::log::testFilter`, `::testProject` | 0 |

(The plan of record's H6, 2026-11-19: DuckDB S131 38 decisions, S132 14, S15 2; H2 S131 4, S132 3 -- S131's count has
fallen since, the rest as measured then.)

**The oracle step, by the plan:** legend-engine's own answer to each moved test on that database. These are
legend-engine's relational tests, written for its H2 (2.1.214); legend-engine does not run them on DuckDB, so no answer
is on record and the engine runner (which runs testable suites) does not run them as they are: asking means a small
harness that runs the test functions through legend-engine 4.145.0 on DuckDB. The expected classes, to be confirmed by
that answer: S131's twelve compare an unordered result as ordered text, so any engine on DuckDB returns DuckDB's order
(agrees with lite); S15's two and S132's fourteen DuckDB rows are DuckDB's own Float results (the same double from
any engine on DuckDB: agrees with lite); S132's three H2 rows are a printing difference (H2 2.5's Decimal text), which
B2b's Java printer from typed rows may fix.
