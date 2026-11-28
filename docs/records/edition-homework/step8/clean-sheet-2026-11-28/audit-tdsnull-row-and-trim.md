*A research or audit subagent's report to the step 8 session, 2026-11-28, kept as it was written (read-only work; its scratch files were in the session's container and are not in the repository).*

## Audit of bd9d9132 (empty cell text as a table row) and e7096819 (trim's set)

Neither commit compiles yet as far as reading goes: I found no compile errors. Commit A reproduces the deleted pass's SQL in every case I traced, but at least three guard tests will fail. Commit B adds four answer changes and one baseline failure that are not recorded anywhere.

### A. bd9d9132

1. **BLOCKER: two guards fail on the new `"TDSNull"` literal.** It is at `core/src/main/java/com/legend/lowering/NamedRules.java:26` (`EMPTY_CELL_WORD`).
   - `CompatSpellingGuardTest.noCompatibilitySpellingLeaksIntoTheProduct`: `"TDSNull"` is in its `LITERALS` list ("legend-engine's text of an empty TDS cell", B10). The scan skips only `edge/`, so this is a new unregistered hit.
   - `CompatConstantPoolTest`: the pool hit `com.legend.lowering.NamedRules | TDSNull` is neither in `POOL_ONLY` nor backed by a register row of that file.
   - Fix (a): add `"lowering/NamedRules.java | TDSNull | 1 | RULED"` to `CompatSpellingGuardTest.REGISTER`, plus a dated entry in `TerminalRowsFrozenTest.TERMINAL` (its `theTerminalRowsAreExactlyTheRuledOnes` fails otherwise).
   - Fix (b): add it as an open D1.3 row, plus an `EdgeSeparationTest.CSG_OPEN` census line (`everyOpenCompatRowIsCounted` fails otherwise).
   - Either way it needs the user's word. The 2026-11-26 ruling made this a table row; it did not say legend-engine's word may live in product code. This is the first NamedRule whose body spells an upstream word, and tenet 3 puts that at the edge.
   - Fix (c), no ruling needed: keep the word in the edge (`Presenter.TDS_NULL`) and pass it to the rule through a `Styled` facet.

2. **BLOCKER: `SqlFnSortedTest.theSortIsMeasured` fails on `BOTH_WRITERS`.**
   - The `CELL_TEXT` entry (lines 234-236) must be deleted. Its only edge writer was the deleted `PurePrintForms`, which named it in a comparison.
   - The `COALESCE` entry (lines 240-252) must add `"com.legend.lowering.NamedRules"`, because `emptyAsWord` writes `SqlFn.COALESCE`. `COALESCE` stays in the map because `EngineResultEnvelopes`, `PlanEnumForm` and `PureListTexts` still write it.
   - `CONCAT`, `LIST_TRANSFORM` and `STRING_AGG` (`BOTH_AGG`) are unchanged: other edge writers remain and no new product writer was added.

3. **SHOULD-FIX: lowering now sees different nullability facts, so edge SQL text can change.**
   - `CELL_TEXT(x)` is nullable when `x` is (`SqlTyping` default arm). `COALESCE(x,'TDSNull')` is never null, and `nullStrict` treats `COALESCE` as null-tolerant. The old pass ran after these facts were stamped; the new code stamps them on `COALESCE`.
   - **Effect 1:** `Fold.resolveIntoExact` (`Fold.java:776/785/804`) reads `whereNullRejections`. A WHERE on a cell's text over a left join used to null-reject the column, which suppressed the pad flip. It no longer does.
     - That old fact was wrong at the edge: `toString()=='TDSNull'` accepts a padded row.
     - So answers stay the same or are corrected, but SQL text and Column facts change.
   - **Effect 2:** an `extend` of a cell's text gets `OutputCol.nullable=false`. That fact feeds `H2.listOfValues` (`H2.java:1231`, an ordered `JSON_ARRAYAGG` loses `NULL ON NULL`) and `AnsiSqlRenderer` starred slots (line 227).
   - So "nothing moves" holds for answers, not strictly for SQL text. The edge's SQL-text goldens and lanes should be run to confirm.

4. **NIT: comments still naming the deleted pass.**
   - `sql/dialect/CellTexts.java:16-17` ("run after the compatibility edge's spelling passes (which write legend-engine's TDSNull)").
   - `AnsiSqlRenderer.java:1567` and `:3007` (`edge/PurePrintForms`).
   - `integration/JoinedTextTest.java:22`.
   - `Scalars.java:916-918` ("legend-engine's 'TDSNull' cell the compatibility edge's").
   - `EngineFloatTextRowTest.java:24`.

**Verified for A:**
- **Equivalence with the old pass:**
  - A lone cell text becomes `COALESCE(arg,'TDSNull')`, identical to `tdsNullOf`.
  - In `makeString`, only the lambda body changes. The mixed-list branch (`pureToString`) and the `[0..1]` branch (`COALESCE(text,'')`) are untouched, as before. The 4-argument start/sep/end `CONCAT` wraps the join the same way.
  - For a row's cells, `rowCellsInterleaved` matches the pass case by case:
    - The separator is `rest[0]` with one extra argument, `rest[1]` with three, none with zero.
    - An empty-string literal separator is dropped, and one cell is itself.
    - The outer start/end `CONCAT` is kept.
    - Zero cells is left as `rowCellsJoin`, as the pass's `!isEmpty` guard left it.
  - The product path is bit-identical: every table answers null there and the code is the same.
- **Emitters of `CELL_TEXT`:** only `Coercions:167` (`cellText`), `Scalars` `makeString` and the `Lowerer` row-cells arm.
  - `Rule`s are reached only through `Scalars.lower`. A `StyledRule`'s plain `apply` throws. `string::plus` delegates to `joinStrings`, which has no `CELL_TEXT`.
  - Positions that skip `scalar()` (a window's column, a reducer's input) still pass `written` → `Scalars.lower` → the row.
- **Product-style lowering rendered by an edge dialect:**
  - `PlanMaker.count`: a single bound parameter, no cell text.
  - `SeedableLets`: a probe whose tree is discarded.
  - `Compiler.lowerResolved` (lineage): its SQL is scanned, never rendered.
  - `EnginePlans` and `EngineAllocations` use `env.styled()`.
- **Reach:** every `PlanStyle` (`PRODUCT`, `UPSTREAM`, `PURE_V1`, `ENGINE_TESTS`, `ENGINE_PCT`, `withEdition`) hands either `LiteEdition` or a `PureTable`/`EngineTable` table; none hands another table.
  - `EngineTable.lowersAs` asks `EmptyCellTexts` after `UpstreamTies` and before the stamp and the per-database rows. So it answers inside and outside a store query, and with no database (`table(null)`) too.
  - The `OF_TWO_NUMBERS` and `UpstreamTies` checks never match these calls.
- **Taking the row before the other checks changes nothing:**
  - `HeldKinds.READ` has none of these ids, and `FeatureRules.UNDER` holds only `substring`.
  - No `UnfoldableRef` is possible: each id has a rule.
  - `ownRule` exists for both `joinStrings` families, and `Rule`'s default three-argument `apply` covers the plain rule.
  - The `DecisionProbe` label is the same.
- **Guards already updated by the commit:** `ArchitectureTest` (the PLANNER_ENTRIES entry is present; `anEditionsTableAloneNamesARule` passes because `Lowerer` and `Scalars` only pass the value), `HookRulingTest`, `EdgeSpellingHookTest`, `JavaEvalLedgerTest`.

### B. e7096819

1. **BLOCKER (guard): `LeanSqlBaselineTest.theProductsSqlIsHeldToItsBaseline` fails.**
   - `lean-sql/baseline.tsv` pins `channel_a <db> meta::pure::functions::string::trim_String_1__String_1_` on every database. The `core.jsonl` families with `trim` are affected too (query lines 1854, 2921, 3665).
   - All of them grow in characters and plan JSON. MariaDB and MySQL grow most: `REGEXP_REPLACE` with a regex class.
   - It needs a re-pin with a dated note.

2. **SHOULD-FIX: the Postgres SQL island now departs from Postgres.**
   - `protocol/sql/PostgresFunctions.java:64-78` maps `trim`, `btrim`, `trim(x,' ')`, `ltrim` and `rtrim` to Pure's functions, on the ground that Postgres's default trim characters are "Pure's trims". Postgres strips spaces only.
   - So `SELECT btrim(E'a\n')` now answers `a` in lite and `a\n` in Postgres. This is unrecorded and the comment is now false. `sql-parity` does not catch it: its strings fixture has no tab or line-break ends.

3. **SHOULD-FIX: the IQ island's string-to-REAL changes.**
   - `IqFunctions.java:4509` computes `bare = trim(replace(v,'\t',''))` and reads `bare == ''` as 0.0.
   - A text made only of `\n`, `\r`, `\v` or `\f` used to raise (`FLOAT_TEXT` admits only space and tab). It now reads 0.0.

4. **SHOULD-FIX: legend-engine's door has no row for trim.**
   - Inside a store query, legend-engine writes the database's one-argument trim (spaces). `EngineTable` has no row, so its door now answers Pure's set.
   - Under "match engine for engine" this needs either a named rule in the table or the register row. S436 is reserved in fb0ffa6e but not written.
   - Also, `CypherToPure.java:635/11038` maps Cypher `trim` to Pure's, so Cypher answers change too (probably towards Neo4j, which strips whitespace). Worth a note in the record.

5. **SHOULD-FIX, unverified: Trino's two-argument form.**
   - The Trino dialect writes `trim(x, chars)` (`Trino.java:1186`).
   - Trino's own float-to-text code uses `trimmedSixStandard`, documented as "for a database whose RTRIM takes no character" (`Trino.java:2115`). That suggests Trino may refuse the comma form.
   - The standard `trim(BOTH chars FROM x)`, which Databricks and ClickHouse already write, is the safe spelling.

6. **NIT: misplaced doc comment.** `TRIMMED` is inserted at `Scalars.java:94-97`, between `family()`'s javadoc and the method. `family` loses its doc and the first comment dangles (a `-Xlint:dangling-doc-comments` warning on JDK 23+).

7. **NIT: MySQL edge case.** ICU's `$` also matches before a final U+0085, U+2028 or U+2029. So `'a \u2028'->trim()` on MySQL strips the space, where Java keeps it.

**Verified for B:**
- **Set semantics, from documentation or code:**
  - Postgres: `trim(x, chars)` is parsed as `btrim`.
  - Snowflake: `TRIM(x, chars)`.
  - BigQuery: `TRIM(value, set)`.
  - Databricks: `TRIM(BOTH s FROM x)`.
  - ClickHouse: `trim(BOTH s FROM x)`, which it implements as a regex class.
  - MariaDB and MySQL: `REGEXP_REPLACE` with `^[set]+|[set]+$`. Because `\n` is in the set, `$`-before-final-newline is harmless there.
  - MongoDB: `$trim` with `chars`.
- **Set semantics, run directly:** I ran a small JDBC check against the product's driver jars (H2 2.5.252, the SQLite JDBC jar, DuckDB 1.5.6), not the Bazel build. All three trim the six characters as a set, with the characters written raw in the literal.
  - H2 2.1.214 trims only by the set's first character. That jar is the referee's (`@h2_engine`), which runs legend-engine's text, not lite's TRIM.
- **Literal writers:**
  - The base writer and the Postgres, H2, SQLite, Snowflake, Databricks and ClickHouse dialects write the characters raw, which those databases accept.
  - BigQuery escapes them (`\t\n\r`, `\x0b`, `\x0c`).
  - `MongoJson.string` escapes them (`\u000b`, `\u000c`).
  - MariaDB's class needs no escaping for them, and its literal has no backslash.
  - The plan's JSON escapes control characters.
- **Other consumers:** `SqlTyping` does not depend on TRIM's arity. `TestDataPlanner:1343` builds its own one-argument TRIM, which is unaffected. No test pins one-argument `trim(` SQL text.
- **PrintTailParityTest change is sound:** the lexer reads `\t`, `\r` and `\n` (`Escapes.java:89-91`), and `PrintTail.Trim` is `String.trim`. For the new texts on the other steps, `indexOf(',',4)` past the end was already exercised by the text `"x"`.

**Not verified:**
- Whether Trino accepts `trim(x, chars)`.
- Whether Snowflake accepts a raw control character in a literal.
- Whether some plan cache shares lowered SQL between editions.
- Whether any edge SQL-text golden or PCT expected-outcome file pins the edge's old SQL text.
- Every lowering-time optimisation that pattern-matches `COALESCE` against a string literal.
