# Injection safety, Phase 0: the inventory and the design (stream S, 2026-11-14)

Stream S's brief, Phase 0: measure where a name or text becomes SQL or Pure text, design typed identifiers, give the
cost, order and risks, and report before building. Branch `claude/injection-safety`, based on
`claude/separation-foolproof-068` at `de5e87fb`. Read-only measurement; nothing here is built yet.

**The goal.** No input can change the meaning of SQL or Pure text that lite writes, on any dialect. The engine-text
printers (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`) are out of scope; their spelling rule is stream D's.

## 1. The finding in one paragraph

The model side is already half-typed. `RelationalIdentifier.bare` strips a declared name's quotes, and the model keeps a
`quoted` flag (`FromProtocol` :225-230). The DDL and DML records carry that flag (`SqlDdl.Column.declaredQuoted`,
`NameQuoting`, `SqlDml.quotedColumns`). The holes come from three things that remain.

1. **Many writers.** About 25 dialect methods write a name, and nine of them write it raw or escape it too little.
2. **A pre-quoted pass-through in ten writers.** It exists because some names still reach the IR with their quotes on:
   - names in Pure specs, such as `~'"FIRST NAME"'`;
   - view names and column-mapping names;
   - `TargetColumnRef`;
   - a schema written in single quotes;
   - the seed SQL that `RawSqlBoundary` rewrites.

   Nine separate ad-hoc strippers then reconcile these names downstream.
3. **Pure text with its own escaping.** Besides the one printer (`PureComposer`), two more escape tables write Pure, and
   one door splices its input raw.

## 2. Inventory: where a name becomes SQL text (`sql/dialect`, the engine-text printers excluded)

Each dialect's quote character comes from its Lexicon:
- BigQuery and Databricks use a backtick.
- Every other dialect uses `"`. MariaDB and MySQL need `ANSI_QUOTES` for that.

No in-scope dialect extends an engine-text printer. MySql extends MariaDb, and every other dialect extends
`AnsiSqlRenderer`.

### 2.1 The shared writers (`AnsiSqlRenderer`, inherited unless a dialect overrides)

| Site | Writes | How it quotes | Takes a pre-quoted name? |
|---|---|---|---|
| `ident` :2488 | the base identifier rule | bare when plain and not reserved; otherwise `delimited` | **yes**, through `quotedIdentifier` |
| `delimited` :2056 and `delimitedBody` :2081 | a delimited name | doubles the quote; ClickHouse :727 and BigQuery :709 backslash-escape `\` and the quote | no |
| `quotedIdentifier` :2068 | the one pre-quoted check (from C4's M1) | - | - |
| `columnName` :2048 | a column | `PHYSICAL_QUOTED` goes to `delimited`, anything else to `ident` | through `ident` |
| `aliasIdent` :2103 | every alias, and the qualifier of every column reference | `ident` | through `ident` |
| `tableName` :2032 | a table | splits `schema.table` at the **first dot**; a dotted name is always delimited and its `NameQuoting` is ignored | single part only |
| `cteName` :669 | a CTE's head (:340) and its references (:630) | **RAW** (only Snowflake :292 overrides it, with `ident`) | - |
| `perKeyName` :566 | a pivot key's column | escaped through `delimitedBody` | no |
| `structFields` :1734 | a struct field | `ident` | yes |
| `storedName` :2434 | a stored name | `delimited` when declared quoted, else `physicalName` | through `physicalName` |
| `physicalName` :2484 | a physical name | `ident` | yes |
| `ddlIdentifier` :2445 | a DDL column | `delimited` when declared quoted, else `ident` | yes, for an undeclared name |
| `insertColumnName` :2451, `ddlKeyIdentifier` :2456, `ddlColumns` :2462 | an INSERT's columns, a PRIMARY KEY list, a CREATE TABLE's columns | `ddlIdentifier` | as `ddlIdentifier` |
| `dmlColumns` :2357, `ddlQualified` :2423; UPDATE, DELETE and MERGE :2282-2356 | DML | `storedName` and `aliasIdent` | as those |
| `createTemporaryTableAs` :2389, `createTableAs` :2410 | a temp table, a CREATE TABLE AS | `tableName(BARE)` and `delimited` | - |

### 2.2 The dialects' own `ident` and how each treats a pre-quoted name

| Dialect | Site | Rule |
|---|---|---|
| MariaDb (and MySql) | :345 | a pre-quoted name passes through **as is**; anything else is `delimited` |
| Postgres | `ident` :1362, `physicalName` :1380 | a pre-quoted name passes through as is; a bare name is lower-cased, then left bare if plain and not reserved, else `delimited` |
| Snowflake | :281 | a pre-quoted name passes through as is; anything else is `delimited` |
| Trino | :357 | a pre-quoted name passes through as is; anything else is `delimited` |
| BigQuery :699, ClickHouse :715, Databricks :407 | `ident` | a `"…"` name is **unwrapped** (each `""` read as one `"`) and delimited again in the dialect's own quote; anything else is `delimited` |
| H2 | `ident` :216 | a FREED_BY_THE_ENGINE name becomes `delimited(UPPER)`; anything else goes to `super.ident` |
| H2 | `aliasIdent` :378 | a pre-quoted name passes through; anything else is `delimited` |
| H2 | `execPart` :435 (used by `physicalName` :431 and `tableName` :245) | a pre-quoted name passes through; a freed name becomes `delimited(UPPER)`; a name that is not plain or is reserved is `delimited`; anything else stays bare |
| MongoDb | `ident`, `delimited`, `ddlIdentifier` :2648-2658 | the **identity**, so the `insertColumnName` it inherits is raw. Its real output goes through `MongoJson` |

### 2.3 Raw or under-escaped (the places to fix)

1. **`DuckDb.ddlIdentifier` :754-756**
   - It writes `'"' + name + '"'`. The quote is not doubled, and nothing checks for a pre-quoted name.
   - It is reached by CREATE TABLE (`ddlColumns`), by an INSERT's columns (`insertColumnName`) and by PRIMARY KEY lists.
   - This is the breakout the audit reproduced (Phase 1).
2. **`AnsiSqlRenderer.cteName` :669** writes the name raw on every dialect but Snowflake. Every CTE name today is
   lite's own (frames, `__peer`, folds), so this is latent, not open.
3. **`InformationSchemaCatalog` :78-79** wraps an output name in quotes without doubling (DuckDB, H2). The names are
   fixed JDBC grid names, so this is latent.
4. **`DuckDb` :823** writes a lambda's parameter names raw at the declaration. The uses go through `ident`, so an odd
   or reserved name would be spelled differently at the declaration and at its uses.
5. **`MongoDb` :375 and `MongoDbPipeline` :1381** concatenate per-key field names raw into a JSON string. `key()` is
   called only as a validator, and it does not escape `"` or `\`.
6. **`MongoDb.ident` and `delimited`** are the identity (item 2.2).
7. **`RawSqlBoundary`**
   - :255: an INSERT's column list is split on `,` without regard to quotes. A name that starts with `"` passes
     through unchecked; any other name is wrapped without doubling.
   - :316: a pre-quoted head is cut at the first interior `"`, so `"a""b"` is mis-split.
   - :320: the name is wrapped without doubling.
8. **`SQLite.structInsert` :1078** builds a JSON-path label as `$."…"`, escaping `"` but not `\`.
9. **Raw but safe today:** `MariaDb` :147 and `SQLite` :1483 append an alias raw, but every caller passes a name already
   quoted or lite's own.

Smaller items:
- `DuckDbHostOperations.physicalName` :32 inherits the base pass-through, used at :72, :77, :259 and :260.
- `MySql.jsonTable` :375 strips quotes from an alias that is already quoted. It cannot break out, but it loses the
  name's exact identity.

### 2.4 Where names enter the IR (the model-to-MIR boundary)

**From the model: the quotes are stripped and the flag is set.** This part works.
- `FromProtocol` :168-230 calls `RelationalIdentifier.isQuoted` and `bare` (the lexer's backslash escape decoded).
- `TableReferenceChecker` :54-61 and :127-150 do the same for `#>{db."s"."t"}#`.
- `Lowerer.tableRead` :559-578 and `lowerStoreWrite` :428-465 carry the result on.
- `setup/Ddl.createTable` :83-91 builds the DDL records.
- `CsvSeed.blockSqls` :203-275, then `RowLoad` :92, builds the DML.
- The road from `Execution.runOnProjectDatabases` is: `TypedQuery.projectPlan`, then `PlanMaker` :1115, then
  `CsvSeed.planned(ProjectData.fillSteps)`, then `Ddl.createTable` and `RowLoad.insert`. This is the Phase 1 road.

**Names that still reach the IR with their quotes on** (the reason the pass-through exists):
- `FromProtocol.view()` :270-285 keeps a view's name and its column-mapping names raw.
- `RelOpFromProtocol` :74 builds `TargetColumnRef(c.column())` raw.
- `DatabaseProtocolParser` :248-256: a schema written in single quotes keeps its single quotes, and gets no flag.
- `SpecParser`, about :2380-2400: `~'"FIRST NAME"'` keeps the `"` in the column's name.
- Nine strippers reconcile these names downstream:
  - `InferenceKernel` :548
  - `Typer` :1284
  - `TdsDesugars` :258
  - `RelationalRootForm` :257
  - `CsvSeed` :574
  - `TestDataGenerator` :392
  - `TestDataPlanner` :1280
  - `PlanText` :771
  - `MappingFromProtocol` :660, a private `unquote` with no escape decoding

**Two more structural defects.**
- `SqlSource.Table.name` packs `schema.table` into one string, and two places split it back at the first dot
  (`Lowerer.schemaOf` and `tableOf` :581-590, `AnsiSqlRenderer.tableName` :2032). A quoted name containing a dot
  (`"a.b"`) breaks in both.
- `CsvSeed` writes its rows into the table name as the CSV spelled it (:271), but its DDL uses `def.name()` (:250).

## 3. Inventory: where lite writes Pure text

**The one printer** is `protocol.PureComposer` with `ModelComposer`, a port of upstream's grammar composer:
- `convertString` :791
- `convertIdentifier` :894
- `convertPath` :949
- `DatabaseComposer.convertIdentifierDoubleQuoted` :225

**Two other escape tables write Pure without the printer.**
- `server/ModelChanges` :587-614 has `isIdentifier`, `packagePath`, `quoted`, `quotedIfNeeded` and `literal`.
  `literal` escapes `\ ' \n \r \t` only.
- `sql/dialect/CatalogModel.ident` :244 escapes `\` and `"` with a backslash. Its `accessorName` refuses
  `. ( ) { } | ; = \n`.

**Where text is spliced:**

| Site | Spliced input | Read back before use? |
|---|---|---|
| `wasm/.../planner/Boundary.databaseFromCatalog` :648-660 | `path`, **raw**, through `CatalogModel` :175 (`"Database " + path`) and :221 (the accessor). No `packagePath` check | **no**: the `text` answer is never parsed or compiled. A newline and `###Pure` in `path` add elements to the host's model |
| `Boundary.tableModel` :684-710 and `tablesModel` :715-773 | `pkg` and `connection`, checked by `Boundary.packagePath` :888-896, which copies `ModelChanges.packagePath`'s rule | no |
| `CreateAssociation` :117-139 | `cls` and `on`, raw (protected only by the class lookup) | yes: `ModelChanges.made` compiles the result |
| `CreateDataSpace` :58-74 | `classes` and `runtime`, raw (lookup only) | yes |
| `protocol/*Composer` (jsonToGrammar, `PureV1Api` :235/249/262) | unescaped `'…'` splices in about 60 places (RelationalConnectionComposer :257-282; Authentication, FunctionActivator, Persistence, Database, Deephaven and others) | no |

On the `protocol/*Composer` splices: they appear to copy upstream's printing. That has to be read in the pinned sources
before lite changes any of them (upstream fidelity).

**`RawSqlBoundary`** (SQL, not Pure): see item 7 of §2.3.

## 4. Inventory: the fuzz tests today

| Test | Places it reaches | Coverage | Oracle | Gap |
|---|---|---|---|---|
| `InjectionFuzzTest` | 11 hand-listed places in queries; no store names | pinned by measurement | a lexer per database: the marker inside a string or a quoted identifier | `statements()` reads only `statement`, `statements`, `sql` and `content`; the SQL in `LoadStep.Stream`'s `prepare` and `count` (`PlanJson` :909-916) is never read, so snapStream's `$.schema` and `$.table` are counted but unread |
| `BoundaryRequestFieldsFuzzTest` | the Boundary doors (a hand list); fields walked from each request | pinned | a table-model answer compiled and read through; a statements answer lexed | `databaseFromCatalog` is filed as STATEMENTS, so its Pure `text` is never parsed; `PlannerSurfaceTest` :223 calls `path` a `DATA_REFERENCE` |
| `ModelTextInjectionFuzzTest` | createClass, createAssociation, createDataSpace: a hand list | pinned | parse; same element shape; the payload read back exactly | no payload in `class`, `on`, `classes`, `where.*` or the sample's schema; nothing on `protocol/*Composer` |
| `PreQuotedNamesTest` | SQL renderers, every dialect | a golden file (896 lines) | `InjectionFuzzTest.finding` | does not cover `RawSqlBoundary` |
| `UpstreamProtocolFieldsTest` | pure/v1 and testdatagen bodies | - | `refusedByName` accepts any `IllegalArgumentException: …` message, the JDK's included | weaker than `InjectionFuzzTest.namedRefusal` |

## 5. The design: typed identifiers

### 5.1 One record, and one quoting point per dialect

- **`com.legend.sql.Name(String text, boolean quoted)`**, a new IR record.
  - `text` is the bare name. It may hold any character; a quote in it is data.
  - `quoted` says whether the source declared it quoted.
  - Its factories:
    - `Name.declared(text, quoted)`, for the model's names;
    - `Name.bare(text)`, for names lite makes and for query aliases.

  The constructor refuses only a NUL, by name: no database in the list can hold one in a name.
- **One method writes a name**: `SqlWriter.name(Name)`, which asks the dialect's `spell(Name)`. Every dialect has
  exactly one `spell`:
  - **Quoted:** the dialect's quote, with the body escaped by its own lexer's rule. That rule is doubling for ANSI,
    DuckDB, H2, Postgres, Trino, SQLite, Snowflake and MariaDB (`ANSI_QUOTES`); a doubled backtick for Databricks; a
    backslash for BigQuery and ClickHouse. These are today's `delimitedBody` rules, unchanged.
  - **Unquoted:** today's per-dialect rule moves here unchanged:
    - bare when plain and not reserved, else delimited;
    - Postgres folds to lower case (D8);
    - DuckDB's DDL folds to upper case (2026-09-22);
    - H2 upper-cases its freed names.

    So the case rule is: **a quoted name keeps its case; an unquoted name folds as the dialect's own reader folds it**,
    each in one place.
- **Where a name stops being a String.** These fields become `Name`:
  - `SqlSource.Table`: its dotted `name` and `NameQuoting` become `schema: @Nullable Name, table: Name`. This deletes
    `NameQuoting` and both first-dot splits, and fixes `"a.b"`.
  - In DDL and DML: `SqlDdl.Column.name`, `CreateTable`, `DropTable`, `CreateSchema`, `DropSchema`, and the DML
    records' schema, table and columns. This deletes `declaredQuoted`, `quotedColumns` and `NameQuoting`.
  - `SqlExpr.Column.name`. Its `OutputCol.Origin` keeps saying what kind of column it is; `PHYSICAL_QUOTED` becomes
    `Name.quoted`.
  - Every `alias`, `Cte.name`, `SqlWith.Cte.name`, `Star` and `StarExcept` qualifiers, `StructLit.Field.name`,
    `StructGet.field`, `Lambda.params`, `Values.columns` and `Merge.keys`.
  - Exceptions:
    - `Catalog`'s patterns stay `String`, because they are values, written as literals.
    - `Pivot`'s key-derived names are already `perKeyName`'s escaped path.
- **`MirJson`** writes a `Name` as `{"name": …, "quoted": true}`. The plan is portable data (G11), so its JSON shape
  changes once, with the plan fixtures updated.

### 5.2 Names arrive bare: one strip, at the model-to-IR boundary

- `RelationalIdentifier.bare` and `isQuoted` become the only strip. They are already the rule for tables and columns.
- The remaining entries move onto it:
  - `FromProtocol.view` (names and column mappings);
  - `RelOpFromProtocol.TargetColumnRef`;
  - the schema written in single quotes;
  - `SpecParser`'s `~'"x"'` and the column-name spellings, which are decoded once where the spec is parsed, upstream's
    `removeQuotes` and `unescape` (the reason `RelationalIdentifier` gives).
- The nine ad-hoc strippers are deleted, each replaced by reading the name bare.
- Then the **pre-quoted pass-through is deleted from all ten writers** (and `quotedIdentifier` with it). A name that
  contains a quote is text, and is written delimited and escaped.

### 5.3 Mechanical guards (each with a planted violator)

1. **Only a dialect's `spell` reads a name's text.** ArchUnit: no class in `com.legend.sql.dialect` calls `Name.text()`
   outside a method named `spell`. The rule names its exceptions, the engine-text printers (stream D) and MongoDB's
   JSON writer, each with a dated note.
2. **No IR record holds a name as a String.** A reflection test over the sealed permits of `SqlQuery`, `SqlSource`,
   `SqlExpr`, `SqlDdl` and `SqlDml` fails if a field whose name matches a name pattern (`name`, `alias`, `table`,
   `schema`, `column`, `field`, `params`, `keys`, `columns`) is a `String`. An allowlist covers the literal-valued
   fields, each with its reason.
3. **The fuzz's coverage is computed from that same reflection walk**: every `Name`-typed field of every IR record,
   on every dialect. A test builds a record holding the payload in that field, renders it, and reads it with the oracle
   (§5.5). The coverage is the walk itself, not a hand list, so a new field is covered when it is added, or the build
   fails.

### 5.4 Pure text: one printer

- `PureComposer.convertIdentifier`, `convertString` and `convertPath` become the one escaping point.
- `CatalogModel.ident` and `ModelChanges.quoted`, `quotedIfNeeded` and `literal` delegate to it, and their tables are
  deleted.
- `packagePath` becomes one rule in `protocol`, used by `ModelChanges` and `Boundary`. Boundary's copy is deleted.
- `databaseFromCatalog` gets two fixes:
  - its `path` is checked by `packagePath`, so a bad path is refused by name;
  - its answer is parsed and compiled before it is returned, the same read-back `tableModel` gets in the fuzz.
- `CreateAssociation` and `CreateDataSpace` write their raw `cls`, `on`, `classes` and `runtime` through
  `convertPath` and `convertIdentifier`.
- The `protocol/*Composer` splices: I read upstream's composer for each one first.
  - Where upstream escapes, lite escapes.
  - Where upstream splices raw, lite's escaping only changes hostile input. That is a divergence, so it goes to you as
    a draft register row before it lands.
- **The Pure oracle:** print the text, parse it with lite's parser, check that the element shape matches the request's,
  and check that the payload reads back byte for byte. This is `ModelTextInjectionFuzzTest`'s oracle, applied to every
  composer and door that writes Pure. Coverage is computed from the composers' element kinds through reflection.

### 5.5 The SQL oracle

- `InjectionFuzzTest`'s per-database lexer becomes a stronger oracle. The rendered statement must:
  - be exactly one statement, with no `;` outside a string or a quoted name;
  - write the payload as one identifier token whose decoded text equals the payload exactly.
- Every SQL-bearing key a plan writes is read, `prepare` and `count` included. The oracle walks the plan's JSON for
  every planned statement, through the same walker exec's `PlannedSql` minting uses, not through a list of keys.
- `UpstreamProtocolFieldsTest.refusedByName` uses `InjectionFuzzTest.namedRefusal`: the class and the throwing frame
  must be lite's.

### 5.6 Defense in depth for DuckDB's multi-statement execution (a recommendation for you to rule)

DuckDB's driver runs `;`-separated text even through a prepared statement. Because exec may not edit the SQL, the only
defense in the right layer is **at planning**: when a statement is minted, a planner-side check (the oracle's lexer,
promoted to main for DuckDB, and for every dialect if cheap) refuses by name any statement that holds more than one
statement. It costs one lex per statement, against a plan that is made once. It catches a future escaping bug in any
writer, and not just the ones the fuzz reaches.

My recommendation: **yes**, as a planner check on every dialect. Its refusal is a planner error naming the dialect and
the statement's position.

## 6. Order, cost and risks

**Order:**

| Step | What | Size |
|---|---|---|
| S1 | **Phase 1, now**: the DuckDB DDL fix. `DuckDb.ddlIdentifier`'s quoted arm becomes `delimited(name)`, the same doubling every other dialect uses, so it is a class fix for DDL, INSERT and PK. The pinned exploit test goes through `Execution.runOnProjectDatabases` (no file written; the column made with its exact name). The structural route is not needed for this one: the names here are already typed (bare plus flag), only one writer was wrong | small |
| S2 | `Name` and `SqlWriter.name` and `spell`, first for DDL and DML (already flag-carrying, so the move is mechanical), then `SqlSource.Table` (schema and table split), then columns, aliases, CTEs, struct fields and lambda parameters. Guards 1 and 2 land at the end of S2 with planted violators | large: about 13 dialects, `Lowerer`, `PlanMaker`, `VerdictSql`, `MirJson`, setup, testdatagen. About 2-3 days |
| S3 | The entries of §5.2 made bare at the boundary, the nine strippers deleted, then the pre-quoted pass-through deleted | medium. **This is the riskiest step**, because spec-layer names feed the typer |
| S4 | `RawSqlBoundary`: a quote-aware tokenizer (the oracle's lexer) and writes through `spell` | small to medium |
| S5 | MongoDB's per-key names through `MongoJson.key` and `string`; SQLite's JSON-path label | small |
| S6 | Pure: one printer, `packagePath`, `databaseFromCatalog` validated and read back, the composers after reading upstream | medium |
| S7 | The fuzz with computed coverage, the stronger oracle, the plan-JSON walker, `refusedByName` | medium |
| S8 | The planner-side one-statement check (if you rule yes) | small |

**Risks:**
- **Byte identity.** Ordinary names must render byte for byte as today.
  - The guard is `PreQuotedNamesTest`'s golden file, widened to every `Name` field the reflection walk finds, and
    rendered BEFORE S2.
  - The rendered statements of the core test suites must not change on ordinary names. I would capture a corpus-wide
    SQL snapshot before S2 and diff it after.
- **The engine-text printers share `AnsiSqlRenderer`.** S2 changes their inputs (`Name` instead of `String`) but not
  their spelling. They keep their own `spell`, byte-identical, and stream D owns the rule.
- **Plan JSON (G11).** Every plan fixture changes shape once, in S2.
- **External lanes.** Every dialect is touched, so S2 and S3 each need a lane round (yours to run) before they're
  called done.
- **Upstream fidelity.** Only hostile names change spelling. An ordinary name does not. Upstream escapes the same
  hostile inputs in some places and not others. Every changed spelling or refusal is posted to you first.

## 7. Outside this brief (found on the way)

- `SqlSource.Table`'s first-dot split mis-reads a quoted name holding a dot. This is a correctness bug, not only an
  injection issue, and S2 fixes it.
- `CsvSeed` loads rows into the CSV's spelling of the table name but builds the DDL from `def.name()`. A case or quoting
  difference between them would load into a table it did not create.
- `DuckDb` :823: a lambda parameter is spelled one way at its declaration and another at its uses.
