# Landing in progress: the DECIMAL Float literal deletion (B3, 2026-11-25, late night)

Ruled by the user 2026-11-25 ("yes delete the float decimal thing"; amendment 11). The edits are in `landing-decimal-literal-wip.patch.txt` beside this file. Apply it to `claude/compat-b3-nufdlk` at or after `025201a1` with `git apply`.

The edits:
- `edge/PureText.java`: the five `floatLiteralKind()` DECIMAL overrides removed (DuckDB, H2, Postgres, Databricks, Trino);
- `testexec/EqualityCompatEdgeTest`: `0.1 + 0.2` is `0.30000000000000004`;
- `core/src/test/resources/edge/pure_v1_plan_text.tsv`: `raise.sqrtOfNegative`'s SQL is `sqrt(CAST(-2.0 AS DOUBLE PRECISION))`;
- register S20 and DIFFERENCES item 4: the one recorded difference (legend-engine on DuckDB computes Float literal arithmetic as DECIMAL; on H2 it writes `CAST(x AS FLOAT)`, as lite does).

**The full round on it** (`round-template.sh.txt` targets plus the engine lanes): 284 of 293 passed. The 8 failures are the re-pins M1 predicted (`m1-float-literal.md`), plus one guard:
1. **`//core:core_edge_tests`:** `integration/EdgeSpellingHookTest`'s pinned list of the edge's overridden dialect arms still names `floatLiteralKind` (ROW_21) for Databricks, DuckDb, H2, Postgres and Trino. Drop `withRow21(...)` from those five `OVERRIDES` entries, and give `ROW_21`'s javadoc a dated note: "deleted 2026-11-25, the user's ruling; register S20". Guard edits need a dated justification (AGENTS.md).
2. **`//spec:update_rcorpus_h2_0_test`:** the H2 fail roster loses 2 lines (`testSubAggregationInQualifier`, `testSubAggregationWithIfOnRelationMapping` now pass). Regenerate with `bazel run //spec:update_rcorpus_h2_0`, and give the reason in `docs/GATES.md`.
3. **`//spec:lean_sql_corpus_duckdb`, `//spec:lean_sql_corpus_h2`:** the corpus's lean-SQL pins grow (DuckDB +2 to +4 characters per literal, 19 families; H2 +26 per literal, 23 families; numbers in M1 §3). This is the compatibility doors' SQL, not the product's. The product's `//core:lean_sql_baseline_test` passed.
4. **`//spec:update_edge_corpus_duckdb_1_test`, `//spec:update_edge_corpus_h2_1_test`, `//core:update_edge_pure_v1_1_test`, `//pct:update_edge_contract_3_test`:** the edge goldens. Regenerate each with its `bazel run //...:update_...` target and check that the diff is only the literal's spelling and the moves M1 listed.

Then rerun the failed targets plus `//gates:local` (`--lockfile_mode=error`), get an independent review of the diff, and commit and push.

**`//gates:local` (`--lockfile_mode=error`) on the same edits:** 19 failures, all expected:
- the 14 targets that fail only as root (the Postgres-backed tests, `is_numeric_probe_test`, `//datacube:verify_app_test`); rerun them unprivileged (`tools/postgres/as_unprivileged.sh`);
- `//core:core_edge_tests`, `//core:core_edge_tests_integration_h2` and `//core:core_edge_tests_integration_sqlite`: the same `EdgeSpellingHookTest` guard, item 1 above;
- `//spec:lean_sql_corpus_duckdb` and `//spec:lean_sql_corpus_h2`: item 3 above.

No other failure. After the re-pins, rerun those targets and the root-only ones unprivileged.
