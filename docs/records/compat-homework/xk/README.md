# The cross-kind `!=` fix (`63421e0a`), measured before landing (2026-11-19)

The compat owner's ask: land the product track's `63421e0a` ("Two primitives of disjoint kinds are never equal") on a
fresh branch from `origin/main`, as the gate of A3. Local branch `claude/compat-cross-kind-ne` = `origin/main`
`b75d6c73` + the cherry-pick (`c650d959`, clean). **Not pushed, no PR**: this session pushes only to
`claude/compat-translator-spike` without the user's own word.

## The answer: it does not land as is

The fix moves far more than the one test it names. **The corpus loses 26 tests on H2 and 30 on DuckDB.** It also
breaks `//core:corpus_differential_test` and one guard. All of these pass at `main` (`main-check.txt`).

| target | at `main` | with the fix |
|---|---|---|
| `h2-fail-roster` | 276 | 301 (+26: newly failing; −1: `testPlanForDateTimeVariableESTTimeZone` now passes) |
| `duckdb-fail-roster` | 108 | 138 (+30) |
| `h2-unordered-register`, `duckdb-unordered-register` | 899, 913 | 885, 899 (−14 each) |
| `//spec:corpus_duckdb_verdict` | green | red: "differential passes ... SHRANK: 975 < 990" |
| `//core:corpus_differential_test` | green | red: `CV1_QuotedPillars` DISAGREE (rows lost) |
| `//core:guardrails_edge` | green | red: `SqlFnSortedTest.theSortIsMeasured`, a product writer not in the pin |
| `CrossKindEqualityTest` (in `//core:core_tests_exec`) | n/a | green |
| PCT: `//gates:pct_duckdb`, `//gates:pct_h2`, `//gates:pct_channel_b` | green | green |
| the rest of `//gates:local` (`--lockfile_mode=error`) | green | green; the 12 Postgres and DataCube tests that refuse root pass as `ubuntu` (`postgres-as-ubuntu.txt`) |

Per test: `corpus-diff.txt` and `new-failures.tsv`. Every new failure is rows lost, or kept, because a comparison now
answers `false`, or `true` for `!=`. Examples:
- `expected: ['ProductName2'] | actual: []`;
- `line-count 13 != 17`;
- `expected size: 1, actual size: 0`.

## Why: a cross-kind `==` in legend-engine's store-routed code is SQL's comparison, and the corpus relies on it

The failing tests compare across kinds in code legend-engine sends to the database. There the database coerces the
literal, and the corpus's expected answers are legend-engine's:

- **`###Relational` mapping filters and joins.** For example `testRootQueryWithInnerJoinClassMappingWithMilestoningTableFilter`,
  whose filter is `"ordertable_0".prodFk = '2'`, an integer column against a string literal
  (`testClassMappingFilterWithInnerJoin.pure`). The `###Relational` `=` is SQL's by definition, as its `substring` is
  (A3's dynaFunction split).
- **Pure query code legend-engine sends to SQL:**
  - `testDayOfWeekNumberFunction`: `$t.date->dayOfWeekNumber(DayOfWeek.Monday) != '2'`, an Integer against a String
    (`testWithFunction.pure`). Under legend-pure's rule it is always true, so 13 rows come back where 11 are expected.
  - `testFilterOnRootTypeWithOrWithFilterInQualifier`: `$t.id == '1'` (`testQualifier.pure`).
  - The qualifier, milestoning-propagation, cross-store graph-fetch and constraint-validation tests in
    `new-failures.tsv` are the same shape. They were sampled, not each read.

legend-pure's rule (kinds first: `EqualityUtilities.java:67-71`, `CompiledSupport.java:1086-1092`) answers these
differently from legend-engine's SQL. So the fix is a **meaning change of cross-kind `==` and `!=` in store-routed code**,
the same kind of question D1 answered for `substring` and `indexOf`. It is not a defect confined to one test.

## It does not gate A3

In the fixed order (A1, A2, A3), DuckDB's `testPlanForDateTimeVariableESTTimeZone` already fails under A2, on B2's
class a3 (an untyped plan parameter), before its `indexOf(...) != -1` assert can reach the cross-kind comparison. A3's
corpus moves are the same 7 per database with or without this fix (`../a3/corpus-a3-after-a2.txt`).

## What it would take

1. **Keep the database's comparison for `###Relational` equality in any case.** Its `=` is SQL's, as A3's PR1 keeps
   `substring` and `position` SQL's.
2. **For Pure code in store-routed position:** a ruling, posted to the user. legend-engine coerces (the database's
   comparison), and the corpus's 26/30 tests rely on that. legend-pure answers by kind.
3. **The guard:** `SqlFnSortedTest`'s pin moves with the fix, by the user's word, whatever the scope ends up.
