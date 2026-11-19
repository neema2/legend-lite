# Register rows that ride in stage A's batch merge (2026-11-19)

Rows drafted by the builders whose branches were already pushed, for `docs/SEMANTICS_REGISTER.md` at the batch's
merge. Ids are provisional until landing, coordinated with the product track's rows (S371 is PR #121's, Integer over
DECIMAL; S372 proposed, the warehouse's file-load refusal; S378 `currentUser`'s 405; S379 the pivot count; S380 A2's
CP-4 row, SQL text never promised).

## The provisional ids, allocated by the compat owner (one allocator, 2026-11-19 ~19:21)

| id | row | drafted by |
|---|---|---|
| S380 | CP-4: upstream's SQL text never promised (supersedes S4) | B2 (A2) |
| S381 | which error a failing DuckDB product plan raises; the database's threads | A5 |
| S382 | a Double's last digits by the operating system (#123's overlays) | B2 (A3 piece 4) |
| S383 | A3: the raise on the three conditions | the translator (A3 piece 2) |
| S384 | A3: toOne of an empty collection | the translator |
| S385 | A3: answering where legend-engine refuses | the translator |
| S130, S134 | rewritten in place, ids kept | the translator |
| S386 | `joinStrings` with a separator: legend-engine's SQL concatenates it as one more element; Pure (and lite) join with it (testToSQLStringForTDSStringJoin, testHashFunctions) | B2 (A3 piece 4, from the review's M2) |
| S387 | `firstDayOfWeek`: Pure's most recent Monday against legend-engine's SQL's Sunday before (testToSqlGenerationFirstDayOfWeek); state lite's answer as on the MERGED batch (A5's H2 fix, `7cd59a571`) | B2 (A3 piece 4, from the review's M2) |

Any further row takes the next free id from this table, through the compat owner.

## S381 (provisional): which error a failing DuckDB product plan raises; the database's threads (A5)

Ruled by the user, 2026-11-19 ~18:30: "Keep 1 thread for duckdb tests and multiple for product is good". Drafted by
A5 (drafted as "S371", which is taken; renumbered here).

| S381 (provisional) | outcome (which error a failing DuckDB product plan raises; the database's threads) (RULED by the user, 2026-11-19, relayed by the compat owner: "Keep 1 thread for duckdb tests and multiple for product is good") | legend-pure has no database. Its interpreter evaluates one expression at a time, so the first failing expression in evaluation order raises. legend-engine's DuckDB connection is `jdbc:duckdb:` + path (`DuckDBManager`). Its PCT connection is in memory (`DuckDBTestConnectionIntegration`, `path = ""`). The DuckDB extension sets no `threads` anywhere (read in the pinned 4.145.0 sources), so engine's tests and products both run DuckDB's default thread count | a multi-threaded DuckDB product plan with two failing expressions in different pipelines (joined sides, or CTEs) may raise either error from run to run. Two failing expressions in one projection raise the first one's error (30 of 30 runs, measured 2026-11-19: `[adjust(+n), adjust(-n)]` and `adjust(n) == %33803336-12-17` both raise "Type INT64 with value 12345678912 can't be cast"). The test sessions of databases the platform makes run one thread (`SET threads TO 1`: `SqlDialect.testSessionSetup`, `DuckDb`, `Targets.forTest`; the TEST door, the test session target and the judges' plans), so a test's verdict and message do not vary between runs. A product plan's session does not get it (`[SET TimeZone='UTC']` alone). A DuckDB the test declares as a file keeps its own threads, because lite does not change a global setting of a database it did not make | the user's ruling: determinism for tests (G10), DuckDB's parallelism for the product (G6). The cross join's sides are swapped at no cost so one thread reads the actual side first (`VerdictSql`) | `PlanMakerDuckDbTest.aTestPlansSessionRunsSingleThreaded_aProductPlansDoesNot`, `.aDeclaredFileKeepsItsOwnThreads_anInMemoryOneTheTestsSetting`; `AssertBarTest.sidesTheDatabaseCannotHoldAreOneOutcomeUnderEveryJudge` (10 runs for each judge, one outcome); the 12 AdjustBy cells, 30 of 30 under HOST, DATABASE and BOTH | A5 (compat) |

**Reach, counted (A5, 2026-11-19 ~18:33):** zero test plans run on a DuckDB the test declares as a file. Seven core
test files declare `DuckDB { path: ... }`: `ElementParserTest` only parses it; the other six (`ConnectionLeaseTest`,
`LegendHttpServerIntegrationTest`, `PureV1HttpTest`, `ServerSessionsTest`, `TestDataRoutesTest`,
`UpstreamProtocolFieldsTest`) run product plans, multi-threaded before and after. PCT, the corpus inputs, spec and
legend-engine's models declare no DuckDB path. None has two failing parts in different pipelines, so no test reaches
the gap today.

- **S388** (allocated 2026-11-19 ~23:15; RULED by the user ~23:20, "Yes to A": recorded, not refused): a graphFetch serialize tree naming a qualified property the fetch tree lacks; legend-engine serializes `null` or fails with a NullPointerException; lite computes it as legend-pure does. Found during A3 (B1-R's probes `graph.serialize.extra.*`), pre-existing on main. Drafted by B2 on `claude/compat-a3-docs`.
