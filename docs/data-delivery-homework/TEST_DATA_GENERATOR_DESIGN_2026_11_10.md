# The test-data generator as one plan: the design for B2d (APPROVED by the user, 2026-11-10)

Built on the homework (`TEST_DATA_GENERATOR_HOMEWORK_2026_11_10.md`, same folder).

> **The user's rulings, 2026-11-10:** no temporary tables ("Do we need temp table for our implementation? We don't have
> to copy the impl of there is a better way"): each table's rows are one read-only query whose `WITH` clauses carry the
> chain (§3 as revised); the default starting rows are supported ("Is it much harder to support the default in test
> gen?" ... "Great let's do it"). §3-§7 below are the approved design; the temporary-table form first drafted is kept in
> §8 only as the alternative not taken.

## 1. What it is (a product feature; the user, 2026-11-10: "We can't delete test data generator")

Given a query, its mapping, and which root rows to start from (row identifiers, and milestoning dates where the model is
milestoned), the generator extracts from a real database exactly the rows that query needs, down its join tree, and
returns them as test data: one CSV block per table, the form a test's data reloads. Optionally it hashes or scrubs text.

## 2. Today

- The product holds only the plan's text (`TestDataGenerator.planText`); the product door refuses a generator, and S143
  skips its tests (44 per roster).
- The running generator lives in the tests (`TestDataGeneratorRun`) because it plans and runs interleaved: a fetch run,
  its rows read in Java, the next fetch planned over them.
- The homework found the interleaving is not needed: every step depends only on the query's shape, never on row values.

## 3. The design: one plan of read-only queries, run by exec, written by the writer

The planner function takes the query, the mapping, the starting rows (row identifiers) if given, the milestoning dates
and the hash flag (all known at planning; a non-literal is refused by name, as today) and returns ONE ordinary product
plan: one `SELECT` per table in the query's join tree, all run in one read transaction.

Each table's `SELECT` carries its chain in `WITH` clauses:

1. **The starting rows.** `start AS (SELECT <keys and the columns the query needs> FROM <root table> WHERE <row
   identifiers> [AND <milestoning dates>] ORDER BY <key> LIMIT 20)`. Ordered by key, so every table's query picks the
   same rows (upstream's limit lets the database pick).
   - **The default, when no row identifiers are given:** the starting rows are a few rows the query itself reaches,
     ordered by key, by upstream's rule (how many, and from the root table or the query's result: read from the pinned
     sources when built, never guessed). It is one more `WITH` step, not a second round.
2. **Each join level.** `level_n AS (SELECT ... FROM <child table> JOIN level_(n-1) ON <the mapping's join>)`. A
   self-reference gets one level per navigation step in the query (`manager.manager` is two): no recursive SQL. A level
   with no rows is empty; nothing branches.
3. **The table's rows.** `SELECT DISTINCT ... FROM level_n`, text columns hashed (SHA-256 in SQL) or scrubbed where the
   flag says (which columns are text: their declared types, at planning), a null written as upstream's `---null---`,
   ordered.

Why `WITH` and not temporary tables: the generator is pointed at real, often production, databases, where the user may
only read. Nothing is created, so nothing is cleaned up after a failure, and no session state is kept. One read
transaction keeps a live database from changing between the queries. The cost: each level is evaluated again in each
table's query, trivial for 20 starting rows.

Exec runs the statements in order on one session, and nothing else. The writer turns the answers into the test-data
text: each table's header, its lines, and the `-----` separator. That is the generator's output format, which the
product itself reads as test data, so it is a product writer, not the edge.

No Java reads a row to decide anything, no second planning round, no keyed region, no temporary table.

## 4. Upstream fidelity

- Upstream's own plan form (`planTestDataGeneration`) is already one plan whose children read their parents' results;
  this design is that, with the parents' rows recomputed by `WITH` inside each query instead of passed through
  placeholders or kept in temporary tables.
- **The fetch SQL text differs by design.** Upstream's tests pin it exactly (46 `assertSqlEquals`, whose goldens name
  temporary tables `testDataGen_Temp_<table>`). Those 46 text comparisons become a register row (the SQL differs, the
  data does not); the TDG appeal that maps lite's temporary-table names is deleted. Every data check still runs.
- The 20-row limit (now ordered by key), the CSV as a set, hashing, milestoning and the default starting rows are
  upstream's, kept.

## 5. Databases

`WITH` exists on every SQL database lite runs. SHA-256 is a database function where it
exists. A database without one refuses the hash flag by name; the generator without hashing still runs there.

## 6. Proof

- S143's skipped tests run: the fetch SQL text and count (46), the CSV compared as a set, the slice reloaded and the query
  run again over it (33), hashing, milestoning (7), the header-only empty slice, and the plan's shape (6).
- The plan round-trips as JSON like every plan (it is data).
- The data-delivery matrix gains the generator's door; the census's M13 row reads zero.
- The default starting rows get lite's own tests (upstream has none: it is reached only through an Alloy server's stand-in).
- Deleted: the test-side runner `TestDataGeneratorRun` with its interleaving, and the TDG appeal.

## 7. Ruled (the user, 2026-11-10)

1. No temporary tables: `WITH` per table, read-only, one read transaction; upstream's 46 SQL-text comparisons a register row.
2. The default starting rows are built, by upstream's rule read from the pinned sources.

## 8. The alternative not taken: temporary tables (the first draft)

Drop first; `CREATE TEMPORARY TABLE` per level joined to its parent's; one `SELECT` per table; drop after. It matches
upstream's SQL text and computes each level once, but it needs the right to create tables on the source database and
leaves session state to clean up.
