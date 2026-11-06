# T8 -- the test-data generator on the program road: the shapes measured, the design, the question (2026-11-02)

The plan's T8 row: "the test-data generator: a Keyed step (its queries the keys step, its rows filling the value
regions of the statements after it; R14); if M0 finds a statement whose compiled SHAPE depends on the generator's
rows, that case is posted before any other form" (D38). This note is that posting: the bodies read as the walk sees
them (the probe `bazel run //spec:consumption_probe -- <fqn>`), what the walk does today, the design the measurement
supports, and the one place upstream fidelity (R21) needs the user's word. Nothing here is built.

## 1. The bodies (DuckDB host pass after item 7c; 184 bodies stay in all)

| Bodies | Shape | Tags in the kinds census |
|---|---|---|
| 28 | `generateTestData(...)->toOne()`, its `sqls` asserted, `assertTestData(csv, $testData.dataCsvString, $db)`, `loadAndTestExecution(...)` | `assert,effect,effect-map-computed,last-query,let-execute,system-read,tdg-generator` |
| 2 | the same with a `check` statement | `assert,check,effect,effect-map-computed,...,tdg-generator` |
| 3 + 1 | `generateTestData` with `assertTestData` alone, or with a seed string | `assert,effect,system-read,tdg-generator`, `assert,effect,tdg-generator` |
| 23 | the `_Alloy` twins: `createTablesAndFillDb()`, `createTableRowIdentifiers(...)`, `mayExecuteAlloyTest({...}, {\| true})` -- the server thunk never typed, the body IS the fallback (`MayExecuteChecker`): the generator is never called | `assert,effect,system-read` (PASS, vacuously, as the engine's own serverless run) |
| 7 | the `_Alloy` twins whose fixture is not seeded here | `effect,system-read` (SKIPPED) |
| 3 | `getRelationalCSVDataFromQuery` (the census carrier, `TypedCsvCensus`, folded from the MODEL) | `assert,csv-census` |

One body of the 28 as the walk sees it (`testSimpleSingleTable`, 48 statements after the resolver's statement inliner
unfolded every helper): the query, mapping, store and runtime lets; `createTablesAndFillDb()` as 26 effect statements;
two asserts and a hoisted let from `createRowIdentifier`'s own body; `let tableRowIdentifiers =
createTableRowIdentifiers($db, 'default', 'Person', ...)` (the one-row lookup over `relational_elements`, §6 of the
mixed-bodies note: a table named by three literals); `let testData = generateTestData($query, $mapping, $runtime,
$tableRowIdentifiers, ext)->toOne()` (the carrier, `TypedTestDataGen` flavour `generate`); `assertSize($testData.sqls,
1)`; `assertEquals('select top 20 "root".ID, "root".name from Person as "root" where "root".name = \'Atul Anand\'',
$testData.sqls->at(0))`; `assertTestData` unfolded -- `let setUpSQLs1 = setUpDataSQLs(<expected csv>, $db); let
setUpSQLs2 = setUpDataSQLs($testData.dataCsvString, $db); assertSameElements($setUpSQLs1, $setUpSQLs2)`;
`loadAndTestExecution` unfolded -- the connection, `let setUpSQLs = setUpDataSQLs($testData.dataCsvString, $db)`,
`$setUpSQLs->map(sql | executeInDb($sql, $testConnection))` (the seed form over COMPUTED data: item 7c's
`effect-map-computed`), `let plan = executionPlan(...)`, `let result = $plan->execute(...)`, and the boolean
`$result.values->isNotEmpty() && ...` as the last statement.

## 2. What the walk does today

`TestDataGenerationNatives.foldCensus` runs at the statement's turn: the generator (`TestDataGenerator.generate`)
walks the query's relation tree (`ScanRelations`), and for every reached table runs a FETCH into a session temp table
(`CREATE TEMP TABLE ... AS SELECT`, a child fetch joining its parent's temp), reads the temps back as typed rows
(`PlanRunner.rows`), and spells the CSV envelope (`csvEnvelope`: the dedup a DB-side `UNION`, the scrub and the
hashing SQL -- "Java only displays": each row's cells cross as display strings and are joined into the CSV text in
Java). Every statement is the plan runner's under the TDG origin; the temps drop at the end. The result -- the fetch
SQL texts (`sqls`) and the CSV text (`dataCsvString`) -- is spliced into the body as INSTANCE LITERALS, so every later
statement reads constants: the `sqls` compare is a constant compare, `assertTestData` folds two `setUpDataSQLs`
calls over two literal CSVs and compares the SQL lists, and `loadAndTestExecution` seeds the literal CSV. Measured:
each such body sends 1 to 10 statements over the system store (the `getTable` lookup) and the generator's own
(`tdg=890` statements on the host pass).

Two facts decide the design: (a) the fetch SQL texts are PLANNING-TIME: `ScanRelations.relTree` and the fetch
lowering read the model and the row identifiers (literals), never a row; (b) the CSV text is a DATABASE VALUE: the
envelope is already one SELECT per table (the union of the temps, scrubbed and hashed in SQL) whose rows Java only
joins with newlines.

## 3. The design the measurement supports

The generator becomes a plan FRAGMENT the test door writes, not a Java fold:

1. **The fetches as steps.** The generator's statements -- the temp tables' `CREATE TEMP TABLE ... AS SELECT`, in
   tree order -- are planning-time texts (fact (a)); they ride the program as an effect script (the walk's segment),
   on the body's session, and the temps drop at the program's end (a step the door adds). `$testData.sqls` is the
   list of those texts as a CONSTANT: `assertSize` and the `assertEquals` over `->at(0)` are planned as constants
   today (M10b's constant sides).
2. **The CSV as a value.** `$testData.dataCsvString` is the envelope SELECT per table, joined: one statement
   (`string_agg` over the union's rows in display form, the header spelled as a constant, the tables joined with the
   `-----` separator) -- the database computes the text (fact (b)); no row crosses to Java. Where it is read as a
   value, it is a side's region.
3. **`assertTestData`: the one R21 question (§4).**
4. **`loadAndTestExecution`.** `setUpDataSQLs($testData.dataCsvString, $db)` over the COMPUTED CSV -- the shape the
   plan's T8 row names ("a statement whose compiled SHAPE depends on the generator's rows": the INSERT texts are the
   rows) -- is not spelled from the CSV at all: the tables are dropped and created from the MODEL (the DDL half of
   `CsvSeed.sqls`, planning-time), and the rows come from the generator's temps -- `INSERT INTO t SELECT ... FROM
   <temp union>` -- the database executes, the compiled shape is static. Then `executionPlan(...)->execute(...)` is the
   frame M9 plans, and the last statement's boolean a planned value.
5. **The Alloy twins (23 + 7).** No generator runs: the fallback is `true`, and the only reads are the fixture's
   effects, two helper asserts and a DEAD let (`tableRowIdentifiers`, read by the dropped thunk alone). The class
   rules refuse the let for its system read (the `getTable` lookup: a system FUNCTION in `anotherWay`). A dead value
   let -- read by no later statement, no effect inside it -- is spliced nowhere on either road; admitting it is a
   one-line rule (the consumption analysis already says `consumed`). This is a side item, built with T8 or before.
6. **The census carrier (3).** `getRelationalCSVDataFromQuery` folds from the MODEL today (no execution): a
   planning-time constant on the program road too (the fold moves to planning, as `TypedCsvCensus`'s literal).

What is NOT proposed: a Keyed step whose rows fill later statements' value regions in Java (the plan's T8 wording).
The measurement says the only consumers of the rows are the CSV text (a SELECT) and the re-seeding (an `INSERT ...
SELECT`): both are the database's, so nothing needs to cross.

## 4. The question: `assertTestData`'s compare (upstream fidelity, R21)

Upstream: `assertTestData(s1, s2, db)` is `assertSameElements(setUpDataSQLs($s1, $db), setUpDataSQLs($s2, $db))` --
the two CSVs each spelled as their DDL and INSERT statements (strings), compared as multisets of strings. The spelling
is `meta::alloy::service::execution::setUpDataSQLs`, Pure in the engine, `CsvSeed.sqls` in lite (Java over a CSV
string and the model). On the program road the generated CSV is a database value (§3.2), so the three honest forms:

- **(a) Upstream-exact: spell both CSVs in Java at judge time and compare the string multisets.** The judge reads the
  generated CSV's rows (one statement) and runs `CsvSeed.sqls` over the text: a HOST computation over a database
  value -- the egress boundary's named residue (a judge instrument; it is what the walk does today through the
  fold). Verdict identical to upstream's in every case.
- **(b) Compare the two CSV texts as strings, each normalized by the same spelling.** The expected CSV is a literal:
  spelled at planning (a constant); the generated one is the database's value -- the compare is a constant side
  against a database side, the host judge's bread and butter. Diverges from upstream only where spelling two CSVs to
  SQL collapses a difference the texts keep (column order within a table -- upstream's spelling sorts? no: it keeps
  the CSV's order; null spelling `---null---` vs `NULL`; the `-----` separator). Measured divergence: none expected
  on the 32 bodies (the generator spells the CSV in the engine's own column order), to be measured by running both.
- **(c) Compare the ROWS per table: the expected CSV as a literal relation, the generated as the temp union.** The
  database judge's own grid compare (M7/M8), order-insensitive like upstream's `assertSameElements`. Diverges from
  upstream in the failure message's form and wherever the CSV's text form differs from the row form (a number's
  display `1` vs `1.0`, a date's); the verdict on the 32 bodies the same if the display forms agree -- measured.

My recommendation: **(b)**, with (c) as the database judge's form where the body runs under it; (a) kept out (it
would be the one new host computation over a database value in a program whose point is to retire them). The
divergence from upstream -- the compare over CSV texts, not over their SQL spellings -- posted here as R21 asks, and
a `docs/SEMANTICS_REGISTER.md` row if the user rules it in. If the user wants upstream-exact, (a) is a day's work
and a named residue row.

## 5. What is asked of the user

1. §3's shape -- the generator as a plan fragment (fetch steps, the CSV a database value, the re-seed an
   `INSERT ... SELECT`), no Keyed-step row crossing -- rather than the plan's T8 wording.
2. §4's compare form for `assertTestData`: (b) with a register row, or (a) upstream-exact as a named residue.
3. §3.5's dead-let rule for the 30 Alloy twins (built with T8 unless the user objects).

Until the word comes, the work continues with the next items in the agreed order (the refused assert shapes with M8c,
R24, in its slot).
