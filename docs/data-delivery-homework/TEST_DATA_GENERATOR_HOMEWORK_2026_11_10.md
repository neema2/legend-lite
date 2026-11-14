# The test-data generator: can it be planned up front? (homework for B2d, 2026-11-10)

Read-only research (an agent of the orchestrator session; nothing run). Lite's code and upstream's pinned sources.

**Answer: yes. The whole generator plans up front as one fixed sequence of SQL statements, with no multi-round planner
and no keyed region.** No step branches on row values; each child level reads its parent's rows from a temporary
table inside the database.

## Today
- The product holds only the plan text: `TestDataGenerator.planText` (`testdatagen/TestDataGenerator.java:440-463`), via
  `planToString` (`planner/EnginePlans.java:244-248`, `testplan/BodyDriver.java:81`). The product door refuses a generator
  (`planner/ProductStatements.java:397-400`); a test body holding one is refused (`testplan/BodyClass.java:1035-1040, 1065`).
- The running generator moved to the tests on 2026-11-09 (`core/src/test/java/com/legend/testdatagen/TestDataGeneratorRun.java`),
  because it "plans and runs interleaved".

## Its steps (file references are to `TestDataGeneratorRun.java` unless named)
| # | step | depends on |
|---|---|---|
| 1 | arguments: query, mapping, row identifiers and dates as literals, the hash flag (`TestDataGenerationNatives.java:57-173`; non-literal refused, :290) | planning |
| 2 | relation tree from query + mapping (`ScanRelations.relTree`) | shape |
| 3 | columns per table: key, not-null, milestoning, scanned and join columns (`TestDataGenerator.java:75-169`) | shape |
| 4 | root: `CREATE TEMP TABLE … AS SELECT … WHERE <ids> LIMIT 20` (:168-171, :415-422) | shape (ids are literals) |
| 5 | each child: the same, joined to its parent's temp (:261-274) | shape |
| 6 | each temp read back for the transcript, after the database describes its columns (:172, :326-349) | shape: the declared types answer it at planning |
| 7 | the view fetch over the temps (`BodyPlanner.viewSql`, `BodyPlanner.java:489-505`) | shape |
| 8 | CSV per table: a LIMIT-0 probe finds the text columns (:520-529), Java picks hash/scrub/pass-through (:533-548); dedup, scrub, SHA-256, `---null---`, ORDER BY are SQL (:501-597); Java appends headers and `-----` (:510-516, :573-582) | shape: the probe and the branch belong at planning; the assembly can be `string_agg` or the edge writer |
| 9 | drop the temps (:674-695) | — |

- A self-join's depth is the query's navigation depth, not the data's (upstream's `testSelfJoin` produces 3 SQLs), so no
  recursive SQL is needed (the design's §B2d assumed it might be).
- An empty level is a zero-row `CREATE … AS`; upstream's "rows not empty" branch exists only because it builds INSERT text.

## Upstream
- Upstream's own plan form, `planTestDataGeneration`, is ONE plan whose children read `${res_cN}` placeholders; the Java
  service runs that one plan. Only the default-seed route takes two rounds (a seed plan, then its rows as identifiers);
  that is an implementation detail: `CREATE TEMP TABLE seed AS <seed query LIMIT 10>`, joined by the root fetch, keeps it
  one plan. Lite has no default seed (it throws at :163-167).

## What the skipped tests pin (S143)
- 44 `not-offered:test-data-generator(S143)` rows per accepted roster (37 `tests`, 7 `tests::alloy`); the "67" could not be
  reproduced (docs/GATES.md:20611 says 66 at first, 22 alloy tests moved later).
- The fetch SQL text and count exactly (46 `assertSqlEquals`; the goldens spell `testDataGen_Temp_<table>`, lite's
  `tdg_<n>_` names rely on the TDG appeal, `SqlTextVerdicts.java:1383-1408`); the CSV as a set; the slice must reload and
  answer the query (33 `loadAndTestExecution`); hashing; milestoning dates (7); a header-only empty slice; 6 plan-shape tests.

## Not determined
How upstream's executor fills `${res_cN}`; whether lite's 20-row limit and view-on-view order match every golden.
