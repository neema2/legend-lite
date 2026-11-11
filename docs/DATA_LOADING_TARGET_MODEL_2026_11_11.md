# Data loading: the target model (RULED by the user, 2026-11-11)

> **Status: the target, RULED.**
>
> **The user's question (2026-11-11):** "How should we generally align csv and json and any other data load in the
> platform so that they feel the same no matter what the format? And align them so that pasting the data in vs pointing
> to file also doesn't feel fragmented or different for users? What's the expert way to think about this problem
> correctly and expertly?"
>
> **The ruling.** The orchestrator answered with this model and asked whether to write it down as the target, with the
> work as a program after the total-wall PR. The user: "Yes write it".
>
> **What this binds now.** This program (the total-wall PR) changes no code for it. Two things are binding at once:
> - Work inside this program that touches data loading (B3's hosts above all) follows this model, so it does not add
>   another path.
> - The program in §5 runs after the PR.
>
> `docs/DATA_DELIVERY_DESIGN_2026_11_10.md` stays the design of what this PR builds. This file is where it is heading.

## 1. The model: four separate choices

Every data load is four choices. Fragmentation comes from mixing them up: when a combination of format, location and
database becomes its own feature, it gets its own rules and its own errors. Keeping the four separate is what makes data
feel the same everywhere.

| | the question | examples | who decides |
|---|---|---|---|
| **1. Schema** | what the rows mean | a table's declared columns, or a class's properties, with their types | the model |
| **2. Format** | how bytes encode rows | CSV; JSON as an array or one record per line; Parquet | the user |
| **3. Location** | where the bytes are | pasted in the model; a local file; a remote URL, with a named credential | the user |
| **4. Delivery** | how the database gets the rows | its own file reader; a bulk loader fed the bytes; INSERTs or rows in the query; a private session table | **the planner, per database, invisibly** |

Mature systems separate these the same way:
- Spark's reader is `read.format(...).options(...).schema(...).load(where)`.
- Snowflake has named file formats and stages.
- dbt has CSV seeds typed by declared column types.

## 2. The principles

1. **Pasting is a file whose bytes live in the model.** A `data:` URL is literally that.
   - Pasted and pointed-to data share one syntax shape, one schema binding, one set of errors and one result.
   - The only real differences: pasted bytes are fixed when the model is written and are meant to be small; a file is
     checked against its digest at run and may be large. Both are "bytes with a digest" by the time the plan holds them.
2. **One meaning per format, written down once, as a typed format spec in the plan.** Lite's defaults follow upstream.
   - **For CSV:** the header; the delimiter and quote; what is NULL and what is an empty string; a short or wide record
     (refused, or read as upstream reads it); how dates, numbers and booleans are written.
   - **For JSON:** an array or one record per line; a missing field against a null; nested values.
   - **Every reader is configured from that spec:** the planner's decoder, DuckDB's `read_csv`/`read_json`, H2's
     `CSVREAD`, Postgres `COPY`, MariaDB `LOAD DATA`, ClickHouse `FORMAT`, a cloud stage.
   - **Where a reader cannot express a rule,** a SQL check in the plan enforces it, or the load is refused by name.
3. **Types come from the model, never from the data.**
   - The same text becomes the same typed value by the same rule, whatever the format: `2024-01-02` in CSV and in JSON is
     the same StrictDate.
   - A value its column cannot take is refused with the same message everywhere, naming the data (the file or the pasted
     block), the row and the column.
4. **Nesting is the one real difference between formats.**
   - JSON records may nest, so they stay JSON values the SQL reads by path, the same whether pasted or a file.
   - CSV cannot nest.
5. **Delivery is the planner's choice per database, in this order, and invisible to the user except in speed:**
   1. the database reads the bytes itself;
   2. exec hands the bytes to the database's bulk loader, and the database parses them (exec never parses);
   3. the planner writes the rows as SQL values, only when the bytes are known at planning (pasted data);
   4. otherwise, a refusal by name.

   A per-dialect capability table makes the choice, as `SqlDialect.dataDelivery` already does for `data:` payloads.
6. **One proof: a conformance matrix.**
   - The matrix: a corpus of tricky inputs × each format × pasted or file × every database.
   - Every cell gives the identical typed rows or the identical named refusal.
   - It grows out of today's data-delivery matrix (`DataDeliveryMatrixTest`, `DataDeliveryMatrixPostgresTest`).
   - The tricky inputs: quoting, embedded line breaks, NULL against empty, a short or wide record, every type's edge
     values, Unicode, a large file, nested and missing JSON fields.
7. **One reader per database for both locations, where the database can read the format.** Pasted data and a file then
   cannot drift apart on the same database. The matrix holds the databases to each other.

## 3. Every load in the platform, mapped onto the model

| what the user writes (upstream's syntax kept as accepted syntax) | schema | format | location |
|---|---|---|---|
| a relational connection's test data (`Relational #{...}#`) | the store's tables | CSV | pasted |
| a project's `###Data` | the store's tables, or a class | CSV, JSON (ExternalFormat), instances (ModelStore) | pasted |
| a JSON connection, `url: 'data:...'` | the source class | JSON | pasted |
| a JSON connection, `url: 'file:...'` or a remote URL | the source class | JSON | file / remote |
| `loadCsvToDbTable(path, table)` | the table | CSV | file |
| a `file:` source read by a query | the declared columns | CSV, JSON, Parquet | file / remote |
| a `#TDS ... #` or relation literal | its own header | CSV-like text | pasted |
| the test-data generator's output | the store's tables | CSV (the test-data form) | produced, then pasted by the user |
| an uploaded file in the browser, DataCube or Python | the declared or inferred columns | CSV, JSON, Parquet | file |

Each row becomes the same internal reference: a schema, a format spec, and bytes by digest. Delivery follows from that
reference and the database.

## 4. Where lite stands (2026-11-11)

**Already in line with the model:**
- Typed model data on every database.
- JSON payloads on every database (`SqlDialect.dataDelivery`: SESSION_TABLE or INLINE).
- Files as pointers with digests.
- Delivery chosen per database at planning.
- No Java parses data at run.

**The gaps.** Each is a way loading still feels different:
1. **Two CSV readers.** Pasted CSV is decoded by the planner (`CsvSeed`), and a CSV file by the database. They already
   disagree on edge cases: S184 (DuckDB keeps a record wide by one empty cell) and S186 (H2 loads short records).
2. **Files work only on DuckDB and H2.** Pasted data works everywhere. A file elsewhere is refused by name: the user's
   "option 1", 2026-11-10.
3. **Pasted JSON and a JSON file take different paths.** Pasted JSON goes to a session table or inline rows; a JSON file
   goes to `read_json`, on DuckDB only.
4. **No format spec object.** The CSV rules are implicit in each reader.
5. **Upstream's syntax is fragmented** (§3's first column). Lite keeps it as accepted syntax, but routes it onto one
   model underneath.

## 5. The program, after the total-wall PR (on the later list)

1. **The format spec:** a typed `FormatSpec` in the plan (CSV options; JSON shape), with upstream's defaults. Every
   reader is configured from it.
2. **The conformance corpus,** added to the matrix (§2 item 6). Every existing divergence becomes a fixed cell or a
   ruled register row.
3. **Native bulk loaders,** so files work where pasted data does:
   - first Postgres `COPY … FROM STDIN`, MariaDB/MySQL `LOAD DATA LOCAL INFILE`, ClickHouse `INSERT … FORMAT CSV`;
   - then the cloud databases' load paths (a Snowflake stage, a BigQuery load job, a Databricks volume);
   - exec hands the bytes over, and the database parses them.
4. **One reader per database for both locations:** pasted CSV and JSON go through the database's own reader where it
   reads the format, so a pasted block and a file cannot differ on that database.
5. **The upstream forms routed onto the one reference** (§3). The syntax stays; the paths underneath become one.

**What a database with no file reader and no bulk loader keeps** (SQLite, Trino's memory connector, MongoDB for CSV):
- pasted data, as SQL values or inline rows;
- a refusal by name for a file.

That is the one honest limit, and it is the same for every format.
