# The plan and the result: one model each, rendered at the edge, streamed end to end

**Status: ratified with the user 2026-10-18.** This note governs the plan path program from step 7 on
(`docs/PLAN_PATH_PROGRAM_2026_10_17.md` §7). It is a design, not a status log: a change to any decision below is a
decision, recorded here with its date and reason, never an edit in passing. The program's rulings R1 to R11 stand;
where this note is more specific, it is the specific form of those rulings.

The user's framing, which this note writes down: *the in-memory model for the plan and for the result must each be
correct and have one implementation; every output format is a renderer of that model; the fast path renders no plan,
holds no data in memory, and streams rows back as one pipeline.*

## 0. The three things that were being called "text"

1. **The plan** is a structured record (`executionplan.ExecutionPlan`). Its JSON is one rendering of it.
2. **A plan's output format** (`CSV`, `JSON`, `JSON_PER_ROW`, `TDS_TEXT`, typed rows) says what the plan's answer
   is. Pure's TDS text is one such format, for one consumer (legend-pure's PCT harness, which reactivates values
   from it). It is a test-facing format in the same family as the served CSV and JSON.
3. **The result crossing the runner** is, today, text the database wrote: one JSON object per row for typed rows,
   or one whole text for the text formats. This is the part this note changes.

## 1. The plan: one record, three renderers

### 1.1 The model (as it is)

`ExecutionPlan(parameters, root)`: the parameters with their Pure types and multiplicities; a root node that is typed
rows (`TdsResult(columns, sql)`), a text (`TextResult(format, type, sql)`) or a sequence of nodes (`Sequence`); each
`Sql(statement, slots, target, tree)` the rendered statement, its parameter slots (each binding and, where the
database types a placeholder by its value, the type hole and the spelling table), the target (the database, the
servers it is written for, the session statements, the setup steps), and the typed SQL tree the statement was
rendered from. Every result column carries its three facts (`Column`: the Pure type and multiplicity, the SQL slot,
the store's declared type), decided at step 1 so that a reader needs no model.

### 1.2 Decisions

- **P1. One record.** `ExecutionPlan` is the plan. No second in-memory plan shape survives: the engine-protocol plan
  the `pure/v1` API serves (`plan.QueryPlan` today) becomes a rendering of this record (§1.3), and the legacy
  executor's own plan shapes go with it at step 11.
- **P2. The statement is rendered once, at planning.** The plan holds the dialect's text; the runner holds no
  dialect and renders nothing (the boundary program's rule, kept). "Zero plan rendering" on the fast path means no
  plan *JSON*; the SQL text is the plan's content, made exactly once.
- **P3. The tree travels only when asked.** The typed SQL tree exists for inspection, for the round-trip gate and
  for the engine-protocol projection. On the wire a plan carries the statement text; the tree is written only by
  the full rendering (§1.3). A serialized plan never carries its statement twice.
- **P4. JSON stays; no binary format.** Decided 2026-10-17 (the program's §2) and confirmed here against the "SQL
  can get very big" concern: a big plan is big because of SQL text, and a binary encoding of text is the text. Gzip
  at the transport layer handles the rare megabyte plan at no cost to the format. Before any other move, the largest
  plans we make are measured (§8); a decision to compress is a decision recorded here with the numbers.
- **P5. A plan is reviewed whole before it runs.** Streaming the result (§3) does not stream the plan: the runner
  checks every slot, every null type and the target before it opens a session, as it does today.
- **P6. The plan is model-free and dialect-free to its reader.** A reader of a plan (the runner, a renderer, a
  decode) consults the plan's own facts and nothing else. This is what makes the typed decode of §4 a carrier
  conversion, not an evaluation (docs/TENET_CHARTER.md Clause 2c).

### 1.3 The renderers

| Renderer | Reads | Writes | Owner |
|---|---|---|---|
| Lite JSON, full | the record with its tree | `legend-lite-plan` format 6, the tree under `tree` | `PlanJson` |
| Lite JSON, wire | the record without its tree | the same format, `tree` absent | `PlanJson` |
| Engine protocol | the record | legend-engine's plan JSON for `pure/v1` (`resultColumns`, the SQL node, the connection) | the compatibility projection (today split across `QueryPlan` and `PureV1Api`; made one function of the record at step 7) |
| Text for people | the record | the SQL dump (`LEGEND_LITE_DUMP_SQL=1`'s `[sql:*]` lines) | diagnostics |

A renderer reads the record and computes nothing. No rendering is an input to execution: the one rendering read back
is the lite JSON, by `PlanJson`, and the round-trip gate (`testcases/PlanRoundTrip`) proves it reads back to the same
record and the same statement on every dialect.

## 2. The result: one model, a stream

### 2.1 Today, honestly

Two result models exist: the legacy executor's `exec.ExecutionResult` (scalar, collection, tabular, TDS text, graph),
and the runner's `PlanRunner.Rows` (columns and a list of decoded rows). The runner's session seam (`exec.Statements`)
is text in, text out: the database writes one text per row and the runner hands each text to a callback. That seam
already streams. What accumulates is above it: `PlanRunner.rows` collects every row into a list, and the whole-text
formats ask the database for the entire result as one string.

### 2.2 The model

A **result is a stream of rows**, in the database's order, consumed once, by one sink. A row is one of three kinds:

| Row kind | What it carries | Who makes it |
|---|---|---|
| Typed cells | one value per plan column, decoded by the column's three facts (§4) | the session seam's typed method |
| Text row | one formatted row of a text format (a CSV line, a JSON object, a TDS line) | the database, through the format's per-row statement (§5) |
| Document | one document, as MongoDB returns it (a JSON object) | the MongoDB session |

The sink is the only consumer. The product's sinks write bytes to a response as rows arrive (the CSV writer, the JSON
writer, the typed-row protocol writer). The tests' and judges' sinks materialize: a collector that builds a table, a
scalar, a collection or a graph *at the edge*, where the old `ExecutionResult` shapes become views over what the
collector gathered.

### 2.3 Decisions

- **S1. One result model: the stream and its sinks.** `ExecutionResult` is retired at step 11; until then it is a
  collector's output, never something the runner builds.
- **S2. The runner accumulates nothing.** No list of rows, no string of the whole text. Memory is bounded by one row
  plus the sink's own buffer.
- **S3. Order is the database's.** The stream delivers rows in the order the statement produced them; a consumer that
  needs an order asks the statement for one (an `ORDER BY`), never the sink.
- **S4. Decode is a carrier conversion, never an evaluation.** A typed cell is the driver's value read by the plan's
  slot and Pure type (§4): a `long`, a `double`, an exact `BigDecimal`, a `PureDateLiteral`, a `String`, a document.
  Nothing is computed from a cell in Java (the execution tenet, `JavaEvalLedgerTest`).
- **S5. Materialization is an edge decision.** A collector is a sink like any other; it is used by tests, judges and
  the PCT adapter, and by nothing in the product's serving path.
- **S6. A document is a row.** MongoDB's results are documents; the stream admits them as they are, and a typed cell
  is read from a document by the same facts (§4) where a consumer asks for cells.

## 3. The session seam: text in, rows out

`exec.Statements` keeps its text-in half (the plan's statement, with its slots bound) and grows its rows-out half from
one method to two:

```
void query(statement, slots, values, Rows rows)                       // each row's one TEXT (as today)
void rows(statement, slots, values, List<Column> columns, Cells cells) // each row's typed CELLS, by the plan's columns
```

- **D12 amended** (docs/MONGODB_PLAN_2026_10_16.md D12 said text in, text out, so one seam served JDBC and MongoDB):
  text in, rows out, where a row is a text or typed cells. A JDBC session reads cells by the plan's slot (`getObject`
  and the typed getters; a date or timestamp as `java.time`, then the one carrier; a decimal exact; an integral slot's
  `BigDecimal` to its exact integral). A MongoDB session reads cells from the document's fields by the same facts.
- **The JSON rows wire retires for typed rows.** `TdsResult` today has the database concatenate each row into a JSON
  object and the runner parse it back (`Render.jsonWireRows`, `exec.RowDecode`). That was the right bridge to land
  the judges on plans with one seam (step 6); with the typed method it is a cost with no purpose: the database formats
  a row that Java immediately unformats. The judges move onto the typed method in step 7's first landing. `RowDecode`
  stays for the document row kind (a document's field read by the column's facts) and goes where that goes.
- **Arrow is a session capability, not a plan concern.** A database that offers Arrow (DuckDB) may implement the typed
  method over Arrow batches; the plan and the sink do not know. It is measured before it is built (§8).

## 4. The decode: by the plan's facts, slot first

The rule the step-6 corpus measured, kept for the typed method:

1. The **slot** decides the carrier: an integral slot a `long` (a `BigInteger` past 63 bits), a floating slot a
   `double`, a decimal slot an exact `BigDecimal`, a date or timestamp slot a `PureDateLiteral`, a time slot a
   `PureTimeLiteral`, a text slot a `String`, a JSON slot the document or text as it is, the labels `LITERAL`,
   `DECIMAL_TEXT` and `TEMPORAL_TEXT` their own parse.
1a. A **timestamp slot's cell carries nine subsecond digits** (`PureDateLiteral.fromDatabaseTimestamp`: the
   engine's relational read, `fromSQLTimestamp`'s `%09d`); a date slot's is a day; a text-carried temporal keeps
   its written digits. The judges' VALUE lane trims a timestamp's digits (`trimmed`), as the database judge's
   value-lane canon does and as step 6's carrier of a value root did; a grid's cell keeps the nine (step 7's first
   landing, 2026-10-18: measured on the corpus, six DuckDB grid comparisons against the engine's nine-digit
   literals).
2. A value the database holds as **text under a numeric slot** is the text, unless the Pure type is itself numeric: a
   store may declare a column INT over a VARCHAR, and legend-engine's own test store does (the step-6 finding).
3. A **Float the store holds as an integer** is the integer the database holds (the step-6 finding on the Float's
   text applies to the value the same way).
4. Where the slot is none (a late-bound grid, a pivot's generated column) the **Pure type** decides, and where both
   are silent the **carrier's own kind** does (a string, a number, a boolean, a structure).
5. Nothing else. A cell no rule reads is refused by name, never guessed (AGENTS.md invariant 4).

## 5. Formatting: per row, never whole

- **F1. The database formats a row; the runner writes the punctuation.** `JSON_PER_ROW` already works this way: the
  statement writes one object per row and the runner writes `[`, the commas and `]`. Every text format works this
  way from step 7: a CSV line per row with the runner writing the header line and the newlines; a TDS line per row
  with the runner writing the `name:Type[mult]` header; a JSON object per row with the runner's brackets. The bytes a
  client receives are the bytes it receives today (the output matrix, `OutputMatrixTest`, judges it byte for byte).
- **F2. The whole-text formats are retired.** `TextResult` with `CSV`, `JSON` or `TDS_TEXT` as one string, and the
  `string_agg` wires that build it (`Render.jsonWire`, the CSV wire, `TdsTextWrap`'s whole text) go when their
  per-row forms serve every caller. Two walls go with them: MongoDB's 16 MB document bound on a whole text (the
  named wall of the MongoDB plan's §4, the whole-text option's cost) and the SQL databases' one-value-in-memory bound, which has no number
  but is a ceiling all the same.
- **F3. One owner per value's text, unchanged.** The step-2 owners (`LiteralSpelling.floatPrint`, `dateTimeText`,
  the Decimal's scale, the text carrier's grammar) spell a cell the same in a row as they spelled it in a whole; the
  formatting moves from one statement shape to another, the spelling does not move at all.
- **F4. The PCT adapter's TDS text is assembled per row** by the runner from TDS lines, same bytes as the whole
  text today; the adapter reads what it read.

## 6. The fast path: one pipeline

```
text ──compile──▶ ExecutionPlan (in memory, no JSON)
                     │ review: slots, null types, target
                     ▼
              session for the target ──statement──▶ database
                     │
                     ▼ one row at a time: typed cells | text row | document
                   sink ──▶ response bytes (chunked, as rows arrive) | a collector (tests, judges)
```

- **Q1. No plan JSON on the in-process path.** The record is made and run; JSON is rendered only when a caller asks
  for the plan itself (the plan endpoints, a test's pin, the round-trip gate).
- **Q2. No accumulation anywhere between the database and the sink** (S2). The server's response is written as the
  sink receives rows; the response is chunked, and a client reading slowly slows the pipeline (back-pressure through
  the writer), it does not grow the heap.
- **Q3. Per-row formatting only** (F1): the database formats what the format needs per row, nothing more.
- **Q4. One thread of control.** The runner pulls from the session and pushes to the sink; no queue, no second
  thread, no buffer of rows. Parallelism, if ever wanted, is the database's.
- **Q5. What may be held whole:** the parameters, the plan, one row, and the sink's own output buffer. Nothing else.
- **Q6. The judges and the PCT adapter are consumers like any other:** collectors at the edge (S5), so a judge's
  verdict is a stream of one row, a side is a stream collected, and nothing in the product knows a judge exists.

## 7. MongoDB

MongoDB's session answers documents. It implements the text method by writing each document as JSON (today's text)
and the typed method by reading a column's cell from the document by the plan's facts (§4 rule 1, the JSON slot's
document arm included). The per-row formats (§5) are what free MongoDB from its 16 MB bound: a cursor streams
documents, a whole text was one document. Nothing from MongoDB's own documentation or code enters the repository
(the standing constraint); the session is ours.

## 8. The measurement before step 7's design is final

One benchmark, recorded in GATES with its numbers, decides the typed seam's implementation and whether any plan
compression is wanted:

| Measure | How |
|---|---|
| The JSON rows wire against typed JDBC columns | the same relation (10 million rows, mixed kinds: integer, text, decimal, date-time, a nullable column) on DuckDB and on H2; wall time to drain through a counting sink; peak heap; bytes crossing the seam |
| Per-row CSV by the database against the whole-text CSV wire | the same relation; the same measures; the heap at 256 MB to show the whole text failing and the per-row form draining |
| Arrow on DuckDB against typed JDBC columns | the same relation; wall time and heap; built only if the gap is worth its code |
| The largest plans we make | every plan the test suites and the corpus make, their JSON size with and without the tree, the top ten by size; the distribution decides P4's "measure first" |

Decision rule: the typed seam lands in step 7 whatever the first row shows, because it also removes a formatting
step the tenet never wanted; Arrow and compression land only with a measured gap, each as its own recorded decision.

### 8.1 Measured (2026-10-18; GATES "The plan and the result, the §8 measurement")

| Path, DuckDB at 10,000,000 rows | Wall | Drains under 256 MB |
|---|---|---|
| 1 typed columns, the driver's values by slot | 10 to 12 s | not run there (the small-heap run keeps to the text paths) |
| 2 the JSON rows wire, decoded by the plan's columns (step 6's seam) | 50 to 61 s | yes, 77 to 78 MB peak |
| 3 the same statement, text counted only | 32 to 35 s | yes, 75 to 76 MB |
| 4 a CSV line per row by the database | 14 to 18 s | yes, 75 to 76 MB |
| 5 the whole CSV text in one value | 24 to 32 s, 1.77 GB peak | no: the driver hands back no value for the 615 MB text |

H2 at 2,000,000 rows, with `LAZY_QUERY_EXECUTION` on: typed columns 2.9 to 3.3 s, the JSON rows wire 29 to 30 s, a
CSV line per row 7.4 to 7.8 s, the whole text 8.6 to 9.0 s at 1.1 to 1.2 GB peak; under 256 MB the streaming paths
drain at 77 to 78 MB and the whole text runs H2 out of memory. As our sessions open H2 today (no lazy execution), H2
buffers every result whole in the JVM's heap: the typed columns peak at 1.1 to 1.3 GB, the JSON rows wire at 2.1 GB, a
CSV line per row runs H2 itself out of memory, and under 256 MB no path drains at all. The plans: 452 judged by
the round-trip gate, their JSON a median of 27 KB with the tree and 4 KB without (the tree a median of 5.5 times
the rest, up to 40 times); the corpus's 16,810 statements a median of 662 characters, a p99 of 15 KB, a maximum of
74 KB.

**Decided from the numbers:**

- **The typed seam lands in step 7** (D1): typed columns drain in a fifth of the JSON rows wire's time on DuckDB
  (10 to 12 s against 50 to 61 s), and the wire's own formatting is well over half of its cost (32 to 35 s of text
  alone), Java's parse the rest. The seam's cost per row is pinned as a ceiling at the first typed-seam landing.
- **Per-row formatting replaces the whole text** (F1, F2): a CSV line per row drains under 256 MB where the whole
  text cannot, on DuckDB in half to three quarters of the whole text's time (14 to 18 s against 24 to 32 s) at a
  twentieth of its heap, on H2 lazy in 7.4 s against 8.6 s at an eighth.
- **H2's session takes lazy execution** (new, D2): the plan's target for an H2 database carries
  `SET LAZY_QUERY_EXECUTION 1` among its session statements, because H2 buffers a result whole otherwise and no
  streaming claim holds on it. Recorded here; landed with step 7's typed seam.
- **No compression, no binary format** (P4 stands): the corpus never makes a statement over 74 KB; the plans past
  100 KB are two MongoDB lowerings and the tree, and the tree stays off the wire (P3, measured as the bulk).
- **Arrow is not measured** and not built: the build has no Java Arrow dependency; the gap it could close is the
  10 to 12 s of typed columns, which is already the floor of this table. A later measurement is a decision of its own.

## 9. How this lands in the program

- **Step 7, the query service**, is built on this note: the typed method on `Statements` (JDBC and MongoDB), the
  stream and its sinks, per-row formatting for every text format, the engine-protocol plan as a rendering of the
  record; `Execution.executeWire`, `executeStreaming`, `wireOn`, `streamOn`, `PureV1Api.boundParameters` and
  `ConnectionResolver`'s reading of the compiled model deleted (the program's step 7 as written), the whole-text
  wires deleted with them (F2). Its first landing moves the judges onto the typed method, so the JSON rows wire has
  no caller left before the service is switched.
- **Step 8** gives the test runners sinks (collectors), not results.
- **Steps 9 and 10** switch the PCT lanes and the corpus onto the pipeline; the PCT adapter's TDS text is a sink (F4).
- **Step 11** retires `ExecutionResult` (S1) with the legacy executor.
- **Gates added:** the streaming gate (a generated relation larger than the test JVM's heap drained through every
  sink under a 256 MB heap: the proof of S2 and Q2); the output matrix byte-identical before and after each format
  moves to per-row (F1); the round-trip gate extended to the wire rendering (P3: a wire plan reads back to the same
  statement); the benchmark of §8 recorded once and pinned as a ceiling on the seam's cost per row.

## 10. What does not change

JSON for the plan (P4). The runner holds no dialect and no model (P2, P6). The plan is reviewed whole before it runs
(P5). *Java orchestrates, the database executes*: the database formats a row only where a text format wants one, and
Java converts carriers and computes nothing (S4). The three type facts per column (step 1) are the whole instruction
for every decode. Every deliberate difference from legend-engine is a `docs/SEMANTICS_REGISTER.md` row, and a
difference that is a defect is fixed, not registered.
