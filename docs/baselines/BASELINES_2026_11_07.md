# Baselines (2026-11-07, P1.1 of the goals of record)

These are the starting numbers G3 ("lean SQL, fast exec") is measured against. They were taken at the branch
`claude/corpus-walk-deletion`, after I3a-c and P1.0. Later phases report against them, and when a phase shrinks a
number, the new number becomes the floor.

## SQL sent, by statement origin

How it was measured: `-Dlegend.diagnostics=sql-sizes` prints every statement's characters with its origin
(`exec/StatementOrigin`) and its test.

**DuckDB relational corpus, host judge** (`bazel run //spec:corpus_one -- duckdb host "meta::"`; 2,412 pass, 108 fail,
7 skipped):

| Origin | Statements | Characters | Share |
|---|---:|---:|---:|
| `body` (the fused verdict statements) | 3,758 | 12,982,910 | 58.2% |
| `other` (no origin claimed: see below) | 6,018 | 7,517,178 | 33.7% |
| `referee_ours` (the compatibility appeal's rows) | 1,156 | 937,286 | 4.2% |
| `let` | 1,940 | 851,920 | 3.8% |
| `seed`, `system`, `session`, `statement` | 661 | 36,893 | 0.2% |
| **Total** | **13,533** | **22,326,187** | |

Per test (2,394 tests that sent SQL): median 1,955 characters, 90th percentile 31,568, maximum 195,855.

**DuckDB relational corpus, database judge** (2,429 pass, 108 fail, 7 skipped):

| Origin | Statements | Characters | Share |
|---|---:|---:|---:|
| `body` | 6,249 | 35,090,454 | 97.4% |
| `referee_ours` | 1,113 | 896,888 | 2.5% |
| `seed`, `system`, `session` | 641 | 32,439 | 0.1% |
| **Total** | **8,017** | **36,025,103** | |

**Stress service suites, DuckDB, shared sessions.** This is the closest to the product: services answering their
queries. 6,087 tests: 5,614 pass, 424 fail, 49 skipped.

| Origin | Statements | Characters | Share |
|---|---:|---:|---:|
| `statement` (the services' queries) | 5,795 | 3,847,645 | 95.7% |
| `seed` | 868 | 173,547 | 4.3% |
| `session` | 14 | 252 | 0.0% |
| **Total** | **6,677** | **4,021,444** | |

**A G9 finding:** a third of the corpus host judge's SQL (`other`, 6,018 statements) is sent under no origin. A
measurement that cannot say where SQL comes from cannot guard it, so the statements get their origins (P3).

## The plan as portable data (G11)

Every plan made in the three runs above was written to its JSON, read back and compared (`PlanJson.roundTripDifference`).
**None differs.** That is every test body's plan on the DuckDB corpus under both judges, and every service plan of the
stress lane.

The check is now a guard:
- The test-body door refuses a plan that does not survive its JSON (`BodyRouter`, `PlanJson.requireRoundTrip`).
- The product door checks every plan in the test lanes (`Execution.roundTrip`, `-Dlegend.plan.roundtrip=true` in
  `tools/junit/defs.bzl`), so a product query pays nothing.

## Time per lane

Seconds per Bazel test target, from the runs of 2026-11-07, the slowest first:

| Lane | Seconds |
|---|---:|
| `//core:stress_suites_h2` (fresh sessions per test) | 353 |
| `//core:cypher_parity_native_test` | 212 |
| `//core:stress_suites` (after P1.0) | 171 |
| `//core:cypher_parity_test` | 115 |
| `//core:iq_parity_test` | 99 |
| `//core:core_tests_integration_judge_database` | 88 |
| `//core:core_tests_integration_judge_both` | 85 |
| `//core:core_tests_integration` | 70-74 |
| `//pct:pct_h2` | 73 |
| `//pct:pct_duckdb_relation` | 67 |

Whole-chain wall time:
- The full lanes (`//gates:core`, `checks`, `corpus_duckdb`, `corpus_h2`, PCT on DuckDB, H2 and channel B, both
  stress suites) take about 23-27 minutes with two test jobs on a cloud container, builds included.
- The stress lane's peak memory went from 10.7 GB to 2.05 GB with P1.0.
