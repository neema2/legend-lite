# B2d.3: the generator's remaining refusals, upstream read and lite's plan (2026-11-11, design only)

Research and design only: nothing is built here. The generator's step 2 lands first, in another tree.

Read for this note:
- Upstream, the pinned sources (legend-engine 4.145.0):
  - `core_relational/relational/testDataGeneration/testDataGeneration.pure` ("TDG" below) and its tests,
    `tests/testDataGeneration.pure` ("TDGT");
  - `pureToSQLQuery.pure`'s `processColumnProjectionsFromRoot`;
  - the engine server's `TestDataGeneration.java`, `SeedDataGeneration.java` and `TestDataGenerationAPI.java`
    (`legend-engine-core-testable`);
  - legend-pure 5.99.0's `toRepresentation.pure`.
- Lite on `claude/total-wall-b2d` at d0eee7699: `TestDataPlanner`, `GeneratedTestData`, `GeneratorSplice`,
  `TestDataGenerator`, `ScanRelations`, `BodyClass`, `ProductStatements`.
- Measured: `//spec:corpus_one` and `//spec:consumption_probe` on the `_Alloy` default-seed tests.

## 0. The answers in short

1. **The default starting rows.** Upstream's `generateTestData` never defaults. Called with no identifiers for its root,
   it raises an error, and every corpus test calls it.
   - Upstream has two defaults:
     - the server's default-seed route, which uses the start-here's rule: `SELECT DISTINCT TOP 10` of the main table's
       keys, read from the query;
     - an untested Pure function, `generateTestDataWithDefaultPKs`, which takes the TOP 5 keys of the root TABLE.
   - No corpus test reaches either. The 7 `_Alloy` rows filed under S143 call the default only inside
     `mayExecuteAlloyTest`'s server leg, which never runs. Lite refuses those bodies for another reason (§4.2).
   - B2d.3's default moves no corpus test.
2. **Views.** 6 tests per lane reach a view. None checks a fetch text, so all 6 can pass. Upstream fetches a view's own
   tables, then the view's own query over them. It counts that query as one more fetch and writes the tables only.
3. **The seed string** (1 test per lane), **the census carrier** (3 per lane) and **the product door.** Upstream's
   generator product is three HTTP routes, plus a fourth for the census. §3 gives lite's plan for each.
4. **Rulings.** Five divergences need the user (§5).

## 1. The default starting rows

### 1.1 Upstream: four routes, two of them defaults

| Route | No identifiers for the root gives | Where |
|---|---|---|
| (a) `generateTestData`, the Pure function every corpus test calls (run interpreted) | **An error.** `getRowIdentifiersForTable` runs `select top 5 <pk> from <root table>` (with the dates' filter) only to build an example, then fails `assert($generateWithDefaultPKs, ...)`. The message is `Row Identifers should be provided for the root table[ of the view (<V>)]: <db>.<[schema.]table> [<k>:<type>,...]\nExample:\n<the 5 rows as createTableRowIdentifiers(...) code>`. A child view whose main table has no identifiers reads them from the parent's rows joined through the view (`top 20`), then fails the same assert. | TDG:482-518 (`top 5` :492, the assert :510); a child view TDG:520-565 (the assert :558) |
| (b) `planTestDataGeneration`, the plan form the server runs | **An error node.** An `ErrorExecutionNode` with the same message, its child the sample query (`top 5`, or `top 20` for a view root). Lite renders this text today (`TestDataGenerator.java:568`). | TDG:1203-1261; pinned by `TDGT::alloy::testErrorDueToNoSeedForRoot` |
| (c) **The server's default seed** (`POST pure/v1/execution/testDataGeneration/generateTestData_WithDefaultSeed`) | **The seed rule (§1.2)**, in two rounds: the seed plan runs, its rows become the root's identifiers, then plan (b) runs with them. | `TestDataGenerationAPI.java:151`; `TestDataGeneration.executeTestDataGenerateWithDefaultSeed` (:119-188) |
| (d) `generateTestDataWithDefaultPKs`, 6 Pure overloads | **The TOP 5 keys of the root TABLE**, not of the query: no DISTINCT, no ORDER BY. They are compiled back from generated Pure code (`compileValueSpecification`, `reactivate`). No upstream test or route calls it, and lite does not register it (`reference-lane/core_relational.txt:797-799`). | TDG:154-182, via (a) with the flag set |

### 1.2 The seed rule, every detail (route c, and `generateSeedDataString`)

- **Which query.** The user's own query, wrapped in `columnProjectionsFromRoot(query, mainTable, keys, true, 10)` (TDG:256-290):
  - every statement but the last is kept;
  - a trailing `serialize` is removed from the last;
  - parameters are bound by name.

  `processColumnProjectionsFromRoot` (`pureToSQLQuery.pure:7328`) lowers it.
- **Which root.** The relation tree must have exactly one root: "Seed data generation currently supports queries with
  a single root table/view (unions are not supported)" (TDG:805). A view root uses its main table.
- **Which keys.**
  - The main table's PRIMARY KEY columns, in declaration order.
  - With no primary key: every non-nullable column except a VARCHAR or VARBINARY longer than 1000 (TDG:266-271).
  - With neither: "Table : <T> must have atleast 1 primary key or non-null column to utilise this feature" (TDG:272).
- **How the keys are carried.** `recursiveAddColumnsToRoot` adds them to the query's ROOT select as
  `gen_col_projection_<k>`. It goes through each wrapping subselect down to the select that reads the table, and adds
  them to each GROUP BY on the way. A grouped query is therefore regrouped by the keys.
- **The outer select.** `select distinct top 10 "root".gen_col_projection_<k> as "<k>" from (<the query>) as "root"`.
  It is DISTINCT and TOP 10 with no ORDER BY, so the database picks which 10.
- **Nulls.** There are none by construction: the keys are a primary key or non-nullable columns. A null would in any
  case match no row through the root fetch's equality.
- **Duplicates.** DISTINCT removes them over the whole key tuple.
- **From rows to identifiers.** Each row becomes one identifier of (column name, value) pairs; a name holding a space
  is quoted (`SeedDataGeneration.getRowIdentifierFromNextRow`). The table is the main table.
- **The root fetch they start.** `select top 20 <cols> from <table> as "root" where (k = v and ...) or (...)`. An empty
  seed gives no filter at all (`orFilters([])`), so the table's first 20 rows. Lite's "an empty seed is no filter" is
  already ruled.

### 1.3 Lite's plan

- **Where lite offers it:** the server route of §3.3, which is upstream's one product default (R1). `generateTestData`
  without identifiers keeps upstream's ERROR (R3); lite's refusal today mislabels it "the default starting rows".
- **One read-only statement, as B2d.1 built.**
  - The seed is the first `WITH` clause: `seed AS (SELECT DISTINCT <keys> FROM (<the query, its root select carrying
    the keys>) ORDER BY <keys> LIMIT 10)`.
  - The root fetch reads `WHERE NOT EXISTS (SELECT 1 FROM seed) OR (<keys>) IN (SELECT <keys> FROM seed)`, by key,
    `LIMIT 20`. An empty seed is therefore no filter, and `IN` matches no NULL, as equality does.
  - Every later level is as B2d.1 built it. There is no second round.
- **Carrying the keys.**
  - The compiler plans the query as any other query (one road).
  - A planner pass over its MIR then does upstream's SQL-level rewrite: it adds the main table's key columns to the
    root select, through each wrapping subselect and into each GROUP BY. The rewrite is structural; nothing is inferred.
  - A root the pass cannot reach is refused, naming the shape: a set operation, a root under a window, or a root in a
    clause the lowering extracted.
- **Order.** By the keys (R2).
- **A child view under this route.** Its main table's keys come from the parent's clause joined through the view's
  query, by key, `LIMIT 20`, as upstream's plan path does it (§2.1).

## 2. Views

### 2.1 Upstream

The interpreted path (TDG:339-418; a child view TDG:425-438) fetches a view V in the query's tree as its own tree.

1. **V's main relation first.** If it is a table, its rows are fetched by identifiers (`top 20`, no order):
   - V is the root: the identifiers given for V's main table, or else the error of §1.1(a).
   - V is a child: the given identifiers of its main table if any (TDG:429), or else the parent's rows joined through V
     (TDG:520-565), and the error unless the default flag is set.

   If V's main relation is a view, that inner view is fetched first, recursively.
2. **Then V's own join web**, the view's column-mapping joins and its `~filter`'s joins (`nestedViewTree`). Each table
   is joined to its parent, `top 20`.
3. **Then V's own fetch.**
   - V's query (`processRelationalMappingSpecification`: its `~filter`, `~distinct`, `~groupBy` and joins) runs with
     each table replaced by a temporary table of that table's fetched rows (`testDataGen_Temp_<table>_<n>`, TDG:388-405,
     `fixTables`).
   - The interpreted path takes no `top`; the plan path takes `top 20`.
   - Its rows are the rows the query's own joins from V start from (TDG:298-303). The children join against V's columns
     through a temporary table of V's rows (TDG:440).
4. **Texts.** `sqls` is V's subtree's fetches, then V's fetch, then V's children's (TDG:417), so one more text per view
   LAYER. The test-data text holds TABLES only: V's rows are never written (TDG:193).
5. **The plan path differs for a CHILD view** (TDG:1023-1201): its main table's rows always come from the parent's
   joined through V (the `driver` select), whatever identifiers are given.

### 2.2 The 6 corpus tests (each lane; lite's refusal today: "in the query's relation tree")

| Test | Shape | Upstream checks |
|---|---|---|
| `testSimpleViewRoot` | `orderPnlView` root (`~filter`, `~distinct`, joins to `orderTable`, `accountTable`, `salesPersonTable`) | `assertSize(sqls, 5)`, `assertTestData`, `loadAndTestExecution` |
| `testSimpleViewRootToJoin` | the same, plus the query's `order` join from the view | 6, data, reload |
| `testSimpleTableToViewJoin` | `orderTable` root, a child `orderPnlView` whose main table's identifiers are given | 6, data, reload |
| `testUnionViewOnView` | a union of views on views | 14, data, reload |
| `testViewEmbeddedInChainedJoin` | a view inside a join chain (`dbInc`'s `personTable` identifiers given) | 5, data, reload |
| `TestDatGenForNestedViews` | a view on a view on a view, two mappings | data twice, reload twice (S192's "reloading again") |

None holds a fetch-text check (`assertSqlEquals`, `sqls->at(n)`), so with views planned all 6 should pass.

### 2.3 Lite's plan

- **The view as clauses, no temporary table (as ruled).**
  - V's subtree becomes clauses, as the walk builds them today. `TestDataGenerator.expandIfView` supplies the
    seed-table tree, and `refuseView` goes.
  - V's own clause is V's lifted relation, planned by the compiler. That is the node a user's read of V inlines to
    (`SpecCompiler.viewRelation`), as the deleted run's `BodyPlanner.viewSql` planned it.
  - Each table V reads is swapped, by S192's typed-identity swap (`lowering/TableSwaps`), for the FIRST clause that
    fetches it in V's subtree. That is upstream's `oldToNew`, first match.
  - V's clause takes no `LIMIT`, as the interpreted path, which the corpus calls, takes no `top`. Nothing limits it, so
    no order is needed.
- **The query's children of V join V's clause** on the mapping's join against V's columns, as upstream does. This
  replaces `substituteViewRefs`, which rewrites to base columns and refuses a column that is not a plain mapping.
- **`sqls`** gains V's clause's text, one per view layer, in upstream's order. The test-data text writes tables only.
- **A child view.**
  - When its main table's identifiers are given, they start its fetch, as the interpreted path does.
  - When none are given, `generateTestData` raises upstream's error (R3).
  - On the server's default route, the plan path's join applies (§1.3).
- **The reload (S192).** The 6 tests' `loadAndTestExecution` runs the query over the slice. The swap must reach the
  tables inside V's lifted relation; check that `TableSwaps` sees through a view node.
- **Milestoned tables in a view:** each table clause carries the dates' filter, as today.

## 3. The seed string, the census carrier, the product door

### 3.1 `generateSeedDataString` (S143's remainder; `GenerateTestDataChecker.checkSeed`'s `seedString` carrier)

**Upstream** (TDG:216-230, interpreted):
- The tree's one root must be a TABLE: "...a root table (unions/views are not supported, got: ...)" (TDG:222).
- The seed rule of §1.2 runs.
- The answer is Pure code:
  - `'\nmeta::relational::testDataGeneration::createTableRowIdentifiers(<db path>, '<schema>', '<table>', [\n'`;
  - then, joined by `,\n` and SORTED by Pure's string `sort()`, one line per row:
    `'       meta::relational::testDataGeneration::createRowIdentifier([<'k',...>], [<v,...>])'`;
  - then `'\n  ])\n'`.
- Each value is spelled by `toRepresentation` (legend-pure `toRepresentation.pure:40-51`):
  - a String in quotes, with `\`, `'` and LF escaped;
  - a date as `%` plus its text;
  - a Decimal with a `D` suffix;
  - a number or a Boolean as its text.

**Lite:**
- One read-only statement whose one cell is the text: the seed clause of §1.3, then `STRING_AGG` of the lines ordered by
  their text.
- The header and footer are planning constants. Each value is spelled per its key column's declared type, at planning:
  a String gets the three escapes in SQL, an Integer its text, other types the dialect's literal writer. A type a
  dialect cannot spell is refused by name.
- The order is codepoint order, through the dialect's binary collation (Postgres `COLLATE "C"`). A dialect with no
  binary collation refuses by name (R4).
- `GeneratedTestData` splices the `seedString` flavour as it does a generator's reads. The `TDG_GENERATOR` refusal
  keeps only "a generator read another way".

**Test:** `testGenerateSeedDataWithQuotedColumns`: three quoted key columns and 4 rows.

### 3.2 The census carrier, `getRelationalCSVDataFromQuery`

**Upstream** (TDG:753-777): the MODEL only, no database.
- The query's property tree (`scanProperties`, `buildPropertyTree`) goes through `scanColumns` over the mapping, giving
  each owning table's columns.
- Per table: its primary key sorted by name, then the scanned columns sorted, then its milestoning date columns
  sorted, without duplicates.
- The tables are sorted by schema + table.
- The answer is `RelationalCSVData(tables = RelationalCSVTable(schema, table, values = the names joined by ','))`.
- The server's `pure/v1/testData/generation/DONOTUSE_generateTestData` returns it as embedded data
  (`TestDataGenerationService.generateEmbeddedData`).

**Lite:**
- A planning constant: a model fact, under the charter's "build-time model text → typed constants". The checker's
  carrier (`TypedCsvCensus`) exists.
- The census is computed at planning from lite's one lineage: `ScanRelations`' tree holds each table's scanned columns.
- The carrier is replaced by the instance's literal, as `GeneratorSplice` replaces a generator's reads, so the body's
  navigation and asserts read constants and run under either judge.
- The Java that folded it went with the walk (2026-11-07). It returns as a planning step that executes nothing.
- Lite's relation tree follows joins; upstream's `scanColumns` follows property mappings. Any column on which they
  disagree is posted case by case, never patched.

**Tests:** the 3 `testGenerateNecessaryTableColumns*` tests: a single table, three tables through a qualifier, and a
milestoned table.

### 3.3 The product door (`ProductStatements.noGenerator`)

**Upstream:**
- There is no Pure-level product entry. The generator's product is the server's routes (`TestDataGenerationAPI.java`,
  under `pure/v1/execution`):
  - `testDataGeneration/generateSeedData`: the seed's rows as JSON
    `{"table":{"_type":"table","table":..,"schema":..,"database":..},"rowIdentifiers":[...]}`
    (`SeedDataGeneration.streamSeedDataResult`);
  - `testDataGeneration/generateTestData_WithSeed`: the generator's plan run, answering its test-data text;
  - `testDataGeneration/generateTestData_WithDefaultSeed` (§1.1c);
  - and, under another path, the census's `pure/v1/testData/generation/DONOTUSE_generateTestData` (§3.2).
- The routes' writer is the server's own (`TestDataGeneration.manageTestDataResult`, :226-330):
  - tables sorted by `schema.table`;
  - rows distinct across a table's fetches;
  - a NULL as `---null---`;
  - each cell by `toCSVString`, NOT scrubbed.

  This is a second upstream writer; S191 names only the Pure one's scrub.
- Reading the sources does not settle whether upstream's query execution plans a lambda holding `generateTestData`.
  Ask the engine runner (`docs/result-boundary-homework/README.md` §7) before keeping the refusal.

**Lite:**
- Keep `noGenerator` for a generator inside a product query, pending the runner's answer.
- Build the generator's product doors as server routes mirroring upstream's four. Each composes one planning call
  (`TestDataPlanner`, or the census) and one run: exec's product run, with the result library's writer (`text/plain`
  for the test data, JSON for the seed). Every route is read only.
- Whether lite's route writes the routes' layout (sorted tables, cells unscrubbed) or the Pure function's (S191's
  faithful cells) is R5.

## 4. The work in order, its tests, and the corpus

### 4.1 Slices

| Slice | Builds | Lite's tests | Corpus movers (each lane) |
|---|---|---|---|
| B2d.3a | views (§2.3) | `TestDataPlannerTest`: a view root; a view on a view; a child view with its main table's identifiers; a child view without them (upstream's error text); a join from a computed view column; `sqls`' count per layer. `GeneratedTestDataProgramTest`: the reload of a query over a view | the 6 of §2.2: S143 → pass |
| B2d.3b | the census as a planning constant (§3.2) | a planner test on the three shapes, plus an empty census | 3 `testGenerateNecessaryTableColumns*`: S143 → pass |
| B2d.3c | the seed clause and the seed string (§1.3, §3.1) | `TestDataPlannerTest` on DuckDB, H2 and SQLite: fewer and more than 10 keys; a table with no primary key (its non-null columns); a table with neither (upstream's text); a grouped query; a view root (its main table); a union (refused with upstream's text); a String key holding `'`, `\` and a line break | `testGenerateSeedDataWithQuotedColumns`: S143 → pass |
| B2d.3d | `generateTestData` without identifiers: upstream's error (R3) | the root, a view root and a child view, each with the message | none (no corpus test) |
| B2d.3e | the product doors (§3.3) | each route over HTTP on DuckDB, including the default seed's empty-seed case; the data-delivery matrix's generator door; the census's M13 row at zero | none |

### 4.2 The corpus

- **Expected to move:** 10 tests per lane leave S143 (6 + 3 + 1), all expected to pass. After B2d.3, S143 names no
  generator refusal except what a ruling keeps. S193's text changes from "a view's own fetch is not planned" to "a
  view's own fetch is planned, its text lite's".
- **Not moved: the 7 `_Alloy` rows** (`testAlloyTestDatGenWithQuotedColumns`, `testAlloyTestDatGenWithViewAsDriver`,
  `alloy::testTableToTDSSimple`, `alloy::testTableToTdsWithAppliedFunctions`, `testViewRootWithDefaultSeed_Alloy`,
  `testViewWithGroupByChild_Alloy`, `testViewWithGroupByRoot_Alloy`).
  - They are not the generator's. Each body is a set of lets, `createTablesAndFillDb()`, and
    `mayExecuteAlloyTest(server, {| true})`. The server leg holds the default-seed call and its `assertTestData`, and it
    never runs, neither in lite nor in upstream's CI without an Alloy server.
  - Measured on DuckDB with `corpus_one` and `consumption_probe`: lite refuses the body because its `let` of the query
    lambda is not a statement an effect program admits (`class(oneJudge)=null`). Upstream passes them vacuously.
  - Their S143 label is wrong, like the 15 of `docs/GATES.md`'s 2026-11-07 entry ("Their S143 rows were wrong: they
    never reached the generator"). Relabelling them, or admitting an unread lambda `let` in an effect program, is a
    roster or body-class question for the orchestrator, not B2d.3's.

## 5. Divergences for the user's ruling, case by case

- **R1: where lite offers the default starting rows.**
  - **Upstream:** the server's default-seed route alone, plus the untested `generateTestDataWithDefaultPKs` (the TOP 5
    keys of the root table). `generateTestData` with no identifiers is an error.
  - **Recommended:** exactly that. Offer the route with upstream's rule. `generateTestData` keeps the error, and
    `generateTestDataWithDefaultPKs` is not offered (unknown, as today).
  - **The alternative:** default inside `generateTestData` as well. That is a divergence, since upstream errors.
  - **Why it needs the user:** the approved design (§3 item 1) says "when no row identifiers are given", which reads as
    `generateTestData`'s default. Upstream has none there.
- **R2: the seed's 10, and a child view's 20 on the default route, by key.**
  - **Upstream:** `distinct top 10` with no ORDER BY, so the database picks.
  - **Lite:** ORDER BY the keys.
  - **Recommended:** count it under the ruled "every level ordered by its key" (§5.2), and widen that row's text to name
    the seed. If the user reads the ruling as levels only, it becomes a row of its own.
- **R3: `generateTestData` with no identifiers for a root, or for a child view's main table.**
  - **Upstream:** the error of §1.1(a), whose `Example:` lists the first 5 keys, read from the database before the error.
  - **Lite:** planning reads no row. Its options:
    - **Recommended:** upstream's message up to and including `Example:`, without the example's rows, plus a register
      row (only the example differs).
    - The example planned as a statement the run reads before raising. This needs a raise whose text is the database's,
      which the plan has no form for today.
  - No corpus test reads this message.
- **R4: the seed string's line order.**
  - **Upstream:** Pure's `sort()` (Java `String.compareTo`, UTF-16 code units).
  - **Lite:** the database's binary collation (UTF-8 bytes).
  - **Effect:** they differ only when a line holds a character above U+FFFF beside one from U+E000 to U+FFFF.
  - **Recommended:** a note on the seed string's row, not a fix.
- **R5: the product route's test-data layout.**
  - **Upstream's routes:** tables sorted by `schema.table`, and cells as `toCSVString` writes them, unscrubbed and
    unquoted, so a comma or a line break in a cell breaks the text.
  - **Upstream's Pure function:** the scrubbed writer.
  - **Lite (S191, ruled for the Pure function):** RFC 4180 quoting.
  - **Recommended:** the routes write S191's faithful cells, in the routes' table order. S191 is widened to name the
    routes' writer; the order is upstream's.
