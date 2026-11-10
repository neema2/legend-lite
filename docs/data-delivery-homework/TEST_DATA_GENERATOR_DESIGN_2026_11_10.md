# The test-data generator as one plan: the design for B2d (DRAFT for the user's review, 2026-11-10)

Built on the homework (`TEST_DATA_GENERATOR_HOMEWORK_2026_11_10.md`, same folder). Nothing is built until the user
approves this.

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

## 3. The design: one plan, planned whole, run by exec, written by the writer

The planner function takes the query, the mapping, the row identifiers, the dates and the hash flag (all known at
planning; a non-literal is refused by name, as today) and returns ONE ordinary product plan:

1. **Drop first.** For each temporary table below, a `DROP ... IF EXISTS`: a run that failed part-way leaves nothing
   that breaks the next (A4's rule).
2. **The root level.** `CREATE TEMPORARY TABLE <t> AS SELECT <the columns the query needs, plus keys and join columns>
   FROM <root table> WHERE <row identifiers> [AND <milestoning dates>] LIMIT 20`.
3. **Each child level.** The same over the child table, joined to its parent level's temporary table on the mapping's
   join. A self-reference gets one level per navigation step in the query (`manager.manager` is two), so no recursive
   SQL. A level with no rows is an empty table; nothing branches.
4. **The answer.** One `SELECT` per table over its temporary table: distinct rows, text columns hashed (SHA-256 in SQL)
   or scrubbed where the flag says (which columns are text is read from their declared types at planning), a null
   written as upstream's `---null---`, ordered. These are the plan's typed answers.
5. **Drop after.** The temporary tables dropped, on success and on failure.

Exec runs these statements in order on one session, and nothing else. The writer turns the answers into the test-data
text: each table's header, its lines, and the `-----` separator. That is the generator's output format, which the product
itself reads as test data, so it is a product writer, not the edge.

No Java reads a row to decide anything, no second planning round, no keyed region.

## 4. Upstream fidelity

- Upstream's own plan form (`planTestDataGeneration`) is already one plan whose children read their parents' results;
  this design is that, with the parent's rows kept in the database instead of passed through placeholders.
- **The fetch SQL is pinned exactly** by upstream's tests (46 `assertSqlEquals`), whose goldens name the temporary tables
  `testDataGen_Temp_<table>`. Recommended: use upstream's names exactly. They are safe because a temporary table belongs
  to its session and a session runs one plan at a time. Then the goldens match as written, and the TDG appeal
  (`SqlTextVerdicts`, the part that maps lite's `tdg_<n>_` names) is deleted. A self-reference's second level is named
  as upstream names it, read from the pinned sources when built.
- The 20-row root limit, the CSV as a set, hashing and milestoning are upstream's, kept.
- **The default seed** (no row identifiers given): lite does not offer it today, and no test reaches it (only through an
  Alloy server, whose stand-in passes without running). It stays refused by name, on the later list. If ever built, it is
  one more temporary table (`<seed query> LIMIT 10`) that the root level joins, still one plan.

## 5. Databases

`CREATE TEMPORARY TABLE ... AS SELECT` exists on every database lite runs. SHA-256 is a database function where it
exists. A database without one refuses the hash flag by name; the generator without hashing still runs there.

## 6. Proof

- S143's skipped tests run: the fetch SQL text and count (46), the CSV compared as a set, the slice reloaded and the query
  run again over it (33), hashing, milestoning (7), the header-only empty slice, and the plan's shape (6).
- The plan round-trips as JSON like every plan (it is data).
- The data-delivery matrix gains the generator's door; the census's M13 row reads zero.
- Deleted: the test-side runner `TestDataGeneratorRun` with its interleaving, and the TDG appeal if the names match.

## 7. For the user

1. Use upstream's temporary-table names (`testDataGen_Temp_<table>`), so the goldens match and the appeal is deleted?
   (Recommended.)
2. The default seed stays refused by name and goes on the later list? (Recommended.)
