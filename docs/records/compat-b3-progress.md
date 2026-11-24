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

**Run 2: `renderedTexts()` switched to lite's as well (S131, S132):** running.
