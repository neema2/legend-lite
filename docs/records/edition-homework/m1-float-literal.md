# M1: the edge's DECIMAL Float literal, measured (B2, 2026-11-25)

R1's first measurement, the user's go of 2026-11-25 ("measure and then hopefully delete the float thing"), relayed by
B3. Documentation only: the probe was a temporary edit, reverted; no pin, ledger or roster moved.

**The probe.** The five edge dialects that override the Float literal's kind to DECIMAL (`edge/PureText.java:263, 310,
333, 370, 481`: DuckDB, H2, Postgres, Databricks, Trino; the edge program's row 21, ruling R2 of 2026-11-17) returned
the product's kind instead (`AnsiSqlRenderer.floatLiteralKind`, the double: register S20). So every door writes a Float
literal as the product does. Measured at `da231244b`.

**The lanes:** `//gates:local` (the corpus's lean-SQL pins among it), PCT channel A on DuckDB, SQLite and H2, channel B
(every lane), the corpus on DuckDB and H2, the edge's tests and their H2 and SQLite integration lanes, both engine lanes,
and `//pct:edge_net` (the edge contract's goldens, the pure/v1 golden net, the edge cases). The Postgres, `is_numeric`
and datacube targets failed only as root, as on every round, and passed on the unprivileged rerun (14 of 14).

## 1. The moves, per door and database

| Door | DuckDB | H2 | SQLite | Postgres, Databricks, Trino |
|---|---|---|---|---|
| PCT channel A (legend-pure's door) | 2 answers in tolerance, outcome unchanged | 0 (its lanes run the Relation suite alone) | 0 (no override: its literal was already REAL) | no edge lane |
| PCT channel B | 0 | 0 | 0 | no edge lane |
| the corpus (legend-engine's door) | 0 | **2 tests newly pass**, none newly fail | (no lane) | (no lane) |
| pure/v1's golden net (H2) | (H2 only) | answers 0; 1 plan text | | |
| the edge's own tests (DuckDB) | 1 assertion | | | |
| the engine lanes | 0 | 0 | | |

**PCT channel A, DuckDB** (`duckdb_grammar_answers.tsv`; both cases still pass, `assertEqWithinTolerance`):
- `plus::testFloatPlus` 001, `0.9 + 0.8`: was `1.7`, now `1.7000000000000002`. legend-pure's compiled double gives
  `1.7000000000000002`, so the value moves TOWARD compiled Pure.
- `times::testFloatTimes` 002, `3 * 0.7`: was `2.1`, now `2.0999999999999996`. Compiled Pure gives the same double, so
  this moves TOWARD Pure too.

**The corpus, H2** (`h2-fail-roster.txt`: two lines leave it, none join):
- `meta::relational::graphFetch::tests::qualifier::testSubAggregationInQualifier` now passes (run alone,
  `//spec:corpus_one -- h2 host <fqn>`: pass 1 of 1).
- `meta::relational::tests::mapping::relation::aggregation::testSubAggregationWithIfOnRelationMapping` now passes.
- Both move TOWARD legend-engine. Its H2 literal processor writes a Float literal as `CAST(%s AS FLOAT)`, a double
  (`h2Extension2_1_214.pure`'s `getLiteralProcessorsForH2`, cited at `edge/PurePrintForms.FloatLiteral.PRINT_AS_FLOAT`),
  not a DECIMAL. The edge's DECIMAL on H2 was not legend-engine's literal there.
- The corpus on DuckDB: no move.

**pure/v1's golden net (H2):** no answer moved. One plan text did: `raise.sqrtOfNegative`, `SELECT sqrt(-2.0) ...` became
`SELECT sqrt(CAST(-2.0 AS DOUBLE PRECISION)) ...` (the same raise answered).

**The edge's own tests** (`core_edge_tests`, DuckDB, the door's literal behaviour): one assertion moves.
`EqualityCompatEdgeTest.theEdgesDoorComparesFloatsExactlyAndDecimalsByValue` held `assertEquals(0.3, 0.1 + 0.2)`
because the edge added two DECIMAL literals exactly. Under the probe `0.1 + 0.2` is `0.30000000000000004` and the
assertion fails. Two readings, both measured or read:
- legend-engine's DuckDB literal processor is the default (`%s`, legend-pure's print, bare), which DuckDB types
  DECIMAL, so legend-engine on DuckDB answers `0.3`: this moves AWAY from legend-engine on DuckDB.
- compiled legend-pure answers `0.30000000000000004` (Java doubles), so it moves TOWARD Pure.

## 2. Toward Pure, or away from legend-engine

- PCT (legend-pure's door): both moves go toward compiled Pure's doubles; no outcome changes.
- The corpus on H2: both moves go toward legend-engine (H2's own literal processor writes a FLOAT).
- The corpus on DuckDB, pure/v1's answers, the engine lanes: no move.
- The edge's DuckDB door: one assertion away from legend-engine's DuckDB answer (`0.3`), toward Pure's.

## 3. SQL size: the corpus's lean pins (the edge's plans)

Families nest (a family's figure includes its sub-families'), so these figures are per family, not a sum.

- **DuckDB:** 19 families grew, 2 or 4 characters per literal (`1.0` written `1.0E0`). For example
  `query::function::cos` went 1,104 -> 1,108, `tds::tdsWindow` 2,490 -> 2,510, and `groupBy` 14,021 -> 14,053.
  2 shrank: `mapping::embedded::advanced` 5,389 -> 5,329, its `inline::targetid` 1,503 -> 1,479.
- **H2:** 23 families grew, 26 characters per literal (`CAST(<digits> AS DOUBLE PRECISION)`). For example
  `query::function::cos` went 1,300 -> 1,352, `groupBy` 14,755 -> 14,989, and `tds::tdsWindow` 1,354 -> 1,432.
  None shrank.
- The product's own lean pins (`//core:lean_sql_baseline_test`, in the gate) did not move.

## 4. New errors

None. No case newly errors on any lane: no DOUBLE overflow, no refused cast. The PCT ratchets, channel B's ledgers and
both engine lanes passed unchanged.
