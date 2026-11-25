# M5: the SQL-text referee's coverage on the relational corpus (DuckDB and H2)

Measured 2026-11-25 in `/home/user/s2` at `da231244` (nothing committed; the probe reverted afterwards).

## How a SQL-text assert is decided (read from the code)

`core/src/main/java/com/legend/edge/SqlTextVerdicts.java` plans the SQL-text arms (`assertEquals(golden, toSQLString(...))`,
`assertSameSQL`, `assertSqlEquals`, `assertEqualsH2Compatible`, `sqlRemoveFormatting($result)`, an `executionPlan(...)`'s
text); `SqlTextRuns.judge` decides each at its verdict step:

1. a decline decided before the texts were compared (`foreign-dialect:<db>`, `plan-params-unbindable`) is reported;
2. texts byte-equal (or equal once legend-engine's chained-plan warning line is stripped) -> **pass by text**;
3. otherwise the comparison is **held** (`Appeals.hold`) and, after the body's run, decided by the appeal
   (`SqlTextRuns.decideAppeal`): a CONTRACT appeal fails with the texts; a ROWS appeal reads our rows and has the replay
   referee (`SqlReplayReferee`, implemented by the harness's `ReplayReferee` on legend-engine's own H2 2.1.214,
   `EngineH2Referee`) run the golden SQL: **MATCH** passes, **DIVERGED** fails, **FAULT** fails, **DECLINED** (the
   golden could not be replayed) leaves the text as the contract; our rows unreadable is **rows-underivable**, text the
   contract. The first appeal that fails is the body's failure and later held appeals of that body are dropped.

The existing census lines (`[corpus2] referee-outcome ...`, `referee-declined ...`, `strength ...`) count the referee's
replays and the passes' strength, but not the text-equal verdicts, so I added a logging-only probe.

## The probe (logging only; reverted)

`PROBE-REF\t<outcome>\t<test>\t<assert>` printed at every branch of `SqlTextRuns.judge`, `decideAppeal` and `rowsLegNow`
(and, for M3, `PROBE-RAISE` lines in `EdgeLoweringForms.raise` for the ten `ENGINE_UNCHECKED` kinds). Patch:
`/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m5-referee-census.patch.txt`
(applied by `probes/apply_m5.py`). It changes no verdict: both passes' measured rosters (fail, skipped, unordered,
engine-order) equal the committed `spec/src/test/resources/rcorpus/<lane>-*.txt` byte for byte.

```
cd /home/user/s2
python3 <scratchpad>/r1/probes/apply_m5.py /home/user/s2
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results //spec:judge_host_duckdb //spec:judge_host_h2
python3 <scratchpad>/r1/probes/m5.py bazel-bin/spec/judge_host_duckdb/host.log bazel-bin/spec/judge_host_duckdb/judge-host.tsv
python3 <scratchpad>/r1/probes/m5.py bazel-bin/spec/judge_host_h2/host.log bazel-bin/spec/judge_host_h2/judge-host.tsv
```

The two passes' logs and ledgers are kept in `<scratchpad>/r1/data/m5_duckdb/` and `.../m5_h2/`
(`<scratchpad>` = `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad`).

## Counts (SQL-text asserts, by how they were decided)

| outcome | DuckDB lane | H2 lane |
|---|---:|---:|
| SQL-text asserts reaching their verdict step | 1,658 (in 1,532 tests) | 1,613 (in 1,492 tests) |
| **text equal** (pass by text, no appeal) | **1** | **1** |
| text equal after the chained-plan warning is stripped | 0 | 0 |
| text differs -> appeal held | 1,657 | 1,612 |
| -- **rows match** (referee MATCH: pass) | **1,594** | **1,550** |
| -- **rows differ** (referee DIVERGED: fail) | **12** | **11** |
| -- **fault** (referee FAULT) | **0** | **0** |
| -- **referee declined** (golden not replayable; text is the contract, so fail) | **28** | **28** |
| -- our rows underivable (text is the contract, so fail) | 20 (19 DataError, 1 MappingResolutionException) | 20 (same) |
| -- held but never decided (an earlier appeal of the same body failed first) | 3 | 3 |
| declined before the texts were compared (`foreign-dialect:*`, `plan-params-unbindable`) | 0 | 0 |
| other (no text read at the step; a golden choosing an unplanned route; a CONTRACT appeal) | 0 | 0 |

The corpus's own census lines agree: DuckDB `referee-outcome verify8 MATCH=1590, verifyPlan MATCH=4, DIVERGED=12,
DECLINED=28`, `referee-faults=0`; H2 `MATCH=1546+4, DIVERGED=11, DECLINED=28`. Pass totals: DuckDB `pass=2275 fail=282
skipped=7 of 2613`; H2 `pass=2132 fail=430 skipped=7 of 2613`.

**The headline: essentially no SQL-text assert passes by its text.** One assert per lane is byte-equal
(`meta::pure::executionPlan::m2m2r::tests::executeProjectWithNestedDerivedProperty`, `assertEquals`); every other
SQL-text verdict is the referee's replay of legend-engine's golden on legend-engine's H2 (96% rows match), its decline, or
our rows. (Since A2 of the compatibility program the edge prints lite's own SQL, so a byte-equal text is the exception.)
No assert was declined for a foreign dialect: the decline branch is not reached on either lane.

The 28 referee declines are all a golden that legend-engine's H2 cannot run (`[corpus2] referee-declined` lines): 8
`datediff-to-now golden: referee replay and our execution are two instants`, 15 `golden execution/Schema "<x>" not found`
(SNDB 5, productSchema 6, SNDBDEFAULT 2, PRODUCTSCHEMANEWDBINC 2), 2 `Syntax error in SQL statement "select""r...`,
1 `Table "PRODUCTTABLENEWINC" not found`, 2 `Table "SELECTTOP1" not found`.

## Two examples of each non-trivial outcome (DuckDB lane; H2's are the same tests unless noted)

- **text equal**: only one -- `meta::pure::executionPlan::m2m2r::tests::executeProjectWithNestedDerivedProperty`.
- **rows match**: `meta::relational::tests::map::testFilterOnSimpleTypePropertyEq` (`assertEquals`),
  `meta::relational::tests::projection::drop::testSimpleDrop` (`assertEquals`).
- **rows differ**:
  - `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testP12WaValue` (`assertSameSQL`):
    "golden SQL on H2 gave 2 row(s), our pipeline gave 2 row(s); golden-only [Campus|0.383333333333,
    Lateral|0.391666666667], ours-only [Campus|0.38333333333333336, Lateral|0.39166666666666666]".
  - `meta::relational::tests::advanced::forced::structure::testQualifierWithOperation` (`assertEquals`): "golden SQL on H2
    gave 4 row(s), our pipeline gave 1 row(s); golden-only [Test, Test, Test], ours-only []".
  - (DuckDB only: `meta::relational::tests::groupBy::testAggToManyWithAverage`, "golden-only
    [CITY|7|25.71428571429|...], ours-only [CITY|7|25.714285714285715|...]".)
- **fault**: none on either lane.
- **referee declined**:
  - `meta::pure::alloy::connections::tests::relationalMapper::testRelationalDatabaseMapper`: "oracle declined: golden
    execution: Schema "SNDB" not found".
  - `meta::relational::tests::functions::sqlstring::testGenerateDateDiffExpressionForH2ForDifferenceInDays`: "oracle
    declined: datediff-to-now golden: referee replay and our execution are two instants -- row verdict
    non-reproducible".
- **rows underivable**:
  - `meta::relational::tests::m2m2r::testProp1`: "DataError: Catalog Error: Table with name "S.sourceEntitlement" does not
    exist because schema "S" does not exist".
  - `meta::relational::graphFetch::tests::milestoning::testMilestonedProperty`: "MappingResolutionException: runtime
    'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::milestoning::Order'".
- **held, never decided**: `meta::relational::tests::map::testSubAggregationMultiLevel` (three held; the second appeal
  DIVERGED, the third dropped), `meta::relational::tests::m2m2r::testProp1` (two held; the first's rows underivable).

## Limits

- Counted: the asserts the SQL-text arms recognize. A test that compares SQL text through its own plain `assert` /
  `assertEquals` on a String it computed (e.g. the ledger's "SQL not as expected for H2", "expected plain equals in
  plan") is judged as an ordinary value comparison and is not in these counts.
- One observation outside M5: at `da231244` the measured per-assert ledgers of both lanes differ from the committed edge
  goldens (`spec/src/test/resources/edge/corpus_<lane>_host_judge-host.tsv`) in one row --
  `meta::relational::tests::mapping::enumeration::testEnumInRelation` assert 0 is recorded as `assertSameElements`, the
  golden says `assertEquals` (PASS in both). The probe prints only; it does not touch assert names.
