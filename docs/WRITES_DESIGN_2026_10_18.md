# Writes: one write layer, two kinds of graph (design sprint, 2026-10-18)

**Status:** design agreed in conversation with the user on 2026-10-18 (§7 records each call). Legs 1 (the judge) and
2 (L0 and L1, the store writes) landed on 2026-10-18; leg 3 (the spike) answered on 2026-10-19: the native store is
the class model over its own tables, contained work (§8).

**Asked for:** writes for the `#Cypher{...}#` island, designed so that the Postgres `#SQL{...}#` island's DML (and
GraphQL mutations later) land on the same layer, instead of each front end getting its own.

**Inputs:** three read-only surveys of 2026-10-18:
- lite's write machinery today;
- the TCK's remaining write cases;
- legend-pure 5.99.0 and legend-engine 4.145.0, read as spec.

§1 summarizes them. This builds on two earlier proposals:
- `docs/CYPHER_TCK_TO_ZERO_PLAN_2026_10_17.md`: W4, decisions 2, 5 and 6, questions Q5 and Q6.
- `docs/BI_AND_ETL_PLAN_2026_09_29.md`: F1, a `write` lowered to DML.

## 1. Where things stand

### Lite has no write path above raw SQL

- **No lowering emits DML.** The MIR's `SqlDml` has four records, all built by Java seed code:
  - `InsertValues`;
  - `InsertFromTable`, whose source is a staging table's name;
  - `DeleteAll`;
  - `CreateTempTableAs`, the one that already renders a `SqlQuery` inside a statement.

  `SqlDdl` has `CreateTable` (columns with a type, NOT NULL and PRIMARY KEY), `DropTable`, `CreateSchema` and
  `DropSchema`. Missing: UPDATE, DELETE … WHERE, INSERT … SELECT of a query, MERGE / ON CONFLICT, RETURNING, identity
  columns, defaults and foreign keys.

  `render(SqlDml)` returns text, and a bound parameter in a DML statement is refused (`AnsiSqlRenderer`:1629).
- **Pure's `write(rel, accessor): Integer[1]` type-checks but only counts rows** (`Render.writeCount`). A write to a
  store table throws "TypedWrite to a store destination is not yet implemented". No other write native exists
  (`mutateAdd` is walled).
- **The only write channel is raw SQL,** through the effect natives (`executeInDb`, …) and seeding.
  - Nothing reads an update count.
  - No production code calls `commit`, `rollback` or `setAutoCommit`.
  - A statement marked as writing makes the next statement reseed its test data.
- **The plan side only reads.** `ExecutionPlan.Node` has no DML node, and `PlanRunner` refuses `Sequence`.
- **Every island refuses writes:**
  - `#SQL`: "only queries are supported" (`SqlParser`:101).
  - `#IQ`: "only a SELECT is read".
  - `#Cypher`: "CREATE is not supported yet", for the first write clause, before anything else is translated.
- **No mapping is inverted.** A class mapping is a function over rows.
  - Keys exist as facts: `primaryKeyColumns`, and the `$pk:` and `$fk:` pseudo-bindings.
  - An association is a predicate; whether it is a foreign-key column or a junction table is implicit in its join
    chain.
- **GraphQL parses** (`#GQL{...}#`, mutations included), but the Typer calls it "parse-only surface".
- **The seam allows writes.** JDBC is allowed in `com.legend.exec`, the root package `com.legend`, server and
  testdatagen. The charter (C1.1) lets Java manage transactions and submit statements; C2.4 forbids
  Java-manufactured identities.

### legend-engine's writes are narrow

- **`relation::write(rel, accessor): Integer[1]` appends:** `INSERT INTO table SELECT …`, columns by position,
  returning the rows written.
- **`mutation::save(values, tree, mapping, runtime)` is the only write through a mapping.** It is a single-object
  upsert over plain column mappings of one root mapping's main table, with every primary key in the graph-fetch
  tree. Joins, DynaFunctions and enumeration mappings are silently dropped.
  - Upsert SQL exists for H2 and MemSQL only.
  - It returns a message string, not a count.
  - There is no delete through a mapping.
- **GraphQL mutations** are rewritten to `internalize → graphFetch → save`, so they inherit all of that.
- **The SQL layer parses INSERT, UPDATE and DELETE and refuses them** ("Unsupported Operation").
- **Persistence** (`###Persistence`) is an ingest pipeline: a service's rows are merged into a target table. Its ingest
  modes are useful vocabulary for later: append-only, snapshot, delta with a delete indicator, uni- and bitemporal
  milestoning, deduplication.

Nothing upstream is a design to copy. Its signatures (`write`, `save`) are verified spec that lite keeps where it
defines the same thing.

### The TCK's write work: 245 refused cases and 33 skipped

- **By first write clause:** CREATE 69, SET 53, MERGE 51, REMOVE 33, DELETE 30, DETACH DELETE 9.
- **Shape:**
  - 185 RETURN something after the write, 66 of them node, relationship or path values.
  - 84 mix writes with UNWIND, WITH or aggregation.
  - 164 MATCH before writing; only 2 read again after a write.
  - None uses FOREACH or a CALL subquery that writes.
- **What the TCK asserts:** 217 assert side-effect counts (+/- nodes, relationships, properties, labels), and 28 assert
  "no side effects". **The harness checks neither today.** It compares rows only, and the snapped answers record the
  graph before the query, never after. So the 60 cases that return nothing would pass if lite did nothing.
- **The 33 skipped cases** each write, then run a control query that reads the graph back. 18 of them are Temporal4,
  which stores temporal values and lists of them.
- **The harness's model can't hold most writes:**
  - The 80 empty-graph cases get no tables.
  - Many cases write a label, type or property the pre-graph lacks.
  - Label changes would move a node between classes.
  - A relationship's storage was chosen from the pre-graph's degrees.
  - 207 of the 245 share a pre-graph with other cases.
- **40 write scenarios expect an error** (33 at compile time, 7 at runtime). Lite agrees with Neo4j on them today only
  because it refuses the clause.

## 2. The design in one picture

```
  #Cypher over a domain model   #Cypher over a native graph   GraphQL mutation (later)   #SQL INSERT/UPDATE/DELETE
             |                            |                          |                           |
             v                            |                          v                           |
   L2  class writes over a direct (1-1) mapping: property -> column, key, FK column or junction row  |
             |                            |                                                      |
             |                            v                                                      |
             |                L2' the native store: labels, properties, relationships as rows    |
             v                            v                                                      v
   L1  store writes: write / update / delete / merge over a table accessor (#>{db.schema.table}#)
       lowering emits SqlDml (InsertSelect, Update, DeleteWhere, Merge); each dialect renders it
                                          |
                                          v
   L0  execution: one transaction per statement, update counts as values, rollback on error
```

- **Each front end translates to Pure,** as every island does today, and the one compiler lowers it. No island
  writes SQL.
- **`#SQL` DML lands on L1 directly,** because the SQL island already reads physical tables.
- **Cypher has two kinds of graph,** and a query is always on exactly one (§3).

## 3. Two kinds of graph

### A. A graph over a domain model (what the island reads today)

A label is a class, a property is a property, a relationship type is an association or an edge class. Writes keep
the model's shape:
- `CREATE` of a declared class (the database mints the key);
- `SET` and `REMOVE` of declared properties;
- links and unlinks of declared associations;
- `DELETE` and `DETACH DELETE`;
- `MERGE`.

Everything outside the model is refused by name:
- a label that names no class;
- a property the class doesn't declare;
- **a label added to or removed from a node: on a domain model a node's label is its class**, and an instance does
  not change class;
- a write to a class whose mapping is not direct (§4, L2).

This is the Legend value: Cypher over your model, with `#SQL` and GraphQL writing the same model under the same rules.

### B. A native graph store (new): lite as a graph database on any SQL database

There is no domain model. The platform owns one fixed schema, created once, that holds any graph:

```
nodes       (id PK)
node_labels (node_id, label)          -- a node's labels are rows
node_props  (node_id, key, value)     -- or one JSON column per node: leg 3 decides
rels        (id PK, type, src_id, dst_id)
rel_props   (rel_id, key, value)
```

Every Cypher write is an insert or delete of rows in these tables:
- `CREATE (:Robot {name: 'x'})` is a `nodes` row, a `node_labels` row and a `node_props` row.
- `SET n:Manager` inserts a `node_labels` row; `REMOVE n:Manager` deletes it.
- `SET n.nickname = 'A'` inserts or updates a `node_props` row.
- A relationship is a `rels` row, with its properties in `rel_props`.

Nothing needs declaring and nothing changes class. **This is how Neo4j itself works:**
- label names, relationship types and property keys are tokens created on first use;
- a node's record carries its label set and a chain of key/value property records, each value carrying its own type;
- a relationship's record carries its type and its two ends;
- constraints (uniqueness, existence, property types) are optional, restrict later writes, and never declare a
  schema.

So B emulates Neo4j by doing what Neo4j does. It needs no schema and no inference.

### How a query picks its graph

By the graph it names. `USE` already names a DataSpace (W1):
- `USE my::People`, a DataSpace over a model, is **A**;
- `USE my::Social`, declared as a native graph (a new store element), is **B**.

### How this relates to open and closed graphs

They are different questions. The backing (A or B) decides what a node is made of. Open or closed decides what a name
outside the schema means, so it only applies to A:

| | Reading an unknown name | Writing an unknown name |
|---|---|---|
| A, closed | compile error | refused |
| A, open (`<<graph.Open>>`, W1) | no rows, or null | refused: nothing to hold it |
| B | no rows, or null | a new row |

In GQL's own terms, an open graph has no graph type and takes any insert: that is B. A domain-model graph is GQL's
closed graph, typed by the class model. `<<graph.Open>>` on A means "lenient reads".

## 4. The layers

### L1: store writes

**Four verbs over a store accessor,** each returning `Integer[1]`, the rows affected (the database's update count, a
value it produced; charter C1.2):

| Verb | Pure | SQL | Notes |
|---|---|---|---|
| append | `rel->write(#>{db.t}#)` | `INSERT INTO t (cols) SELECT …` | upstream's native, lowered for real; columns by name |
| update | `#>{db.t}#->update(r | pred, ~[c: r | expr, …])` | `UPDATE t SET c = expr … WHERE pred` | lite native |
| delete | `#>{db.t}#->delete(r | pred)` | `DELETE FROM t WHERE pred` | lite native; `true` is `DeleteAll` |
| merge | `rel->merge(#>{db.t}#, ~[keys])` | `MERGE`, or `INSERT … ON CONFLICT (keys) DO UPDATE`, per dialect | lite native; an upsert by key |

- **Natives.** The lite natives are `meta::legend::lite::` functions, each moving the native governance ceiling
  with a dated justification. `write` stays upstream's (`meta::pure::functions::relation`), signature verified.
- **The MIR: extend what is there, nothing beside it.** New records follow `SqlDml`'s table naming (schema, table,
  `NameQuoting`, quoted columns):
  - `InsertSelect(table…, columns, SqlQuery)`, the general form of `InsertFromTable`;
  - `Update(table…, alias, assignments, where)`;
  - `DeleteWhere(table…, alias, where)`, of which `DeleteAll` is the `true` case;
  - `Merge(table…, SqlQuery source, keys, updates, inserts)`.

  Each record is pure data with a render arm per dialect. An arm that can't express its statement throws; there is
  no `default ->`.
- **Rendering with parameters.** `renderStatement(SqlDml)` returns a `RenderedStatement` (text plus its `?` binds),
  as queries have. The seed callers keep the text form.
- **DDL.** `SqlDdl.Column` gains an identity or sequence column where a store declares one (keys, D4).
- **The pipeline.** A write lowers to a `LoweredStatement`: a query, or a DML step with the query it reads.
- **Dialects.** DuckDB, Postgres and H2 first: the lanes that judge. The others refuse by name until each is measured.

### L0: execution

- **`Execution.executeWrite`** runs a statement's DML steps and its final query on one connection, in one
  transaction. It commits on success, returning the result and the counts, and rolls back on any error. The
  transaction calls live in `com.legend.exec`.
- **A caller-owned transaction is joined, not committed.** When the connection has autocommit off, the executor works
  inside the caller's transaction. The harness uses this to roll every case back.
- **The "a writing statement reseeds the next" rule** applies to test-data sessions only. A product write is never
  wiped.
- **Execution plans keep reading only for now.** A write plan node is a later leg.

### L2: class writes over direct (1-1) mappings (graph A, GraphQL)

**The verbs take a class or an association as the target.**
- `rows->write(m::Person)`, the rows' columns named by `Person`'s properties.
- `m::Person.all()->filter(p | …)->update(~[name: p | 'B'])`.
- `m::Person.all()->filter(p | …)->delete()`.
- `rows->merge(m::Person, ~[keys])`.

An association is written as its extent of pairs: a `write` links, a `delete` unlinks.

**Only direct mappings are writable.** Inverting mappings in general is the view-update problem, so a write requires
a direct mapping. Phase F checks this (it is never assumed). A mapping is **direct** when all of these hold:
- one main table, with no `~filter`, `~distinct` or `~groupBy`;
- not a union or operation, and not model-to-model;
- every property is a plain column of that table, or a value or embedded class made of its columns;
- no other class mapping uses the table;
- the key (`~primaryKey`, else the table's PRIMARY KEY) is known;
- every NOT NULL column is a property, the key, or minted.

A direct class write is a store write with property names renamed to columns.

| A direct mapping has | A write becomes |
|---|---|
| a plain column property | that column |
| an embedded or value class | its columns, in the same row |
| a key | the row's identity; required for update, delete, merge and links |
| an association over a foreign-key column (a to-one end) | an `UPDATE` of that column, or setting it to null |
| an association over a junction table of the two keys | an insert or delete of a junction row |
| an edge class over a bridge table | an insert or delete of a bridge row |

- **Everything else is refused, with the reason** ("`C` is not writable: its mapping filters its table").
- **Writability is Knowledge.** Phase F records each class's write shape; the DML is Work, built on demand.
- **Two later extensions:**
  - a user-written inverse (a Pure function over L1) for a mapping that isn't direct;
  - upstream's `save`, defined over `merge` and verified against its signature.

**Keys are minted by the database** (C2.4): an identity or sequence column where the store declares one; otherwise
`COALESCE(MAX(key), 0) + ROW_NUMBER() OVER ()` inside the statement. The latter is registered as unsafe under
concurrent writers.

**Staging gives read-your-own-writes.** A write clause materializes its rows, minted keys included, into a session
temp table (`CreateTempTableAs` exists). It applies its DML from that table, and the clauses after it read the temp
table. So `CREATE (n {name: 'foo'}) RETURN n.name` returns the written row, and a later `MATCH` sees it.

### L2': the native store (graph B)

- **A platform-owned schema** (§3 B), created by the platform's DDL. Every write is a `write` or a `delete` of its
  rows, and every read filters them.
- **Property values carry their type, as in Neo4j.** They are stored as Variants, and comparison and ordering use the
  Variant machinery already built (W7, W11).
- **The open question (leg 3):** can B be a platform-owned class model that the existing translator already reads?
  For example `Node` with `labels: String[*]` and Variant properties, over those tables, with a platform-defined
  inverse for its writes.
  - If yes, B is a new resolution rule for labels and properties: contained work.
  - If no, B is a second Cypher backend: much bigger.

## 5. The front ends

### Cypher

Each clause runs once per row of the rows in hand, as Neo4j's do.

- **`CREATE`:** a `write` of the node (A: to its class, with a minted key; B: `nodes`, `node_labels`, `node_props`
  rows). A relationship is a link or a bridge row (A), or a `rels` row (B).
- **`SET` and `REMOVE` of properties:** an `update` of the property (A: its column; B: its `node_props` row).
  - `SET n = map` replaces the properties: those the map names are written, the others nulled.
  - `SET n += map` updates only the properties the map names.
  - Writing `null` removes the property.
- **`SET n:L` and `REMOVE n:L`:** B inserts or deletes the `node_labels` row. A refuses by name.
- **`DELETE`:** deletes the row. Neo4j's DeleteConnectedNode is raised by the database, through a guarded statement,
  while relationships remain. **`DETACH DELETE`** unlinks first, then deletes.
- **`MERGE`:** a read of the pattern, then a create of what the read lacks, in the same statement. `ON CREATE SET` and
  `ON MATCH SET` are updates scoped to the created or the matched rows. A MERGE over a null property raises
  MergeReadOwnWrites.
- **The 33 compile-time write errors** (VariableAlreadyBound, UndefinedVariable and others) come from the static
  checks the island already runs, extended to the write clauses' variables.
- **A temporal written as a property** (2026-10-20): a date, a local time or a local date-time in its column; on B, a
  zoned value as two slot rows, its instant and its offset (a zoned time's spelled `time:instant` and
  `time:offsetSeconds`), and a duration as four (its months, days, seconds and nanoseconds), as the store's value
  classes hold them on A. A list of temporals is the Variant of each element's text, and the model's property carries
  the graph profile's list mark (`listOfDate` ... `listOfDuration`, `projects/graph/model.pure`): JSON holds no
  temporal, so the text says what an element is and the mark says what the list holds. A list of dates, local times
  or local date-times holds the island's own text of each and is read back element by element; a list of zoned
  values or durations holds each one's whole text, its offset written, and is returned whole (an element of one read
  alone is refused by name: the island carries one zone for a list's elements, not one each).

### `#SQL` (L1)

- `INSERT INTO t [(cols)] VALUES | SELECT` becomes `write`.
- `UPDATE … WHERE` becomes `update`; `DELETE … WHERE` becomes `delete`.
- `INSERT … ON CONFLICT … DO UPDATE` becomes `merge`, and `DO NOTHING` a `write` of the rows no row of the table holds
  the key of (as landed, leg 4).
- `RETURNING` reads the staged rows back.
- **The judge:** the table's state after the statement, compared with Postgres 16's (the SQL parity harness's oracle).
- **Refused by name:** data-modifying CTEs and multi-statement scripts.

### GraphQL (later, L2)

A mutation is a `merge` or a `delete` over a class by key, its input object the row. That covers legend-engine's
`save`, and more. It needs the `#GQL{...}#` read side compiled first.

## 6. The judge

**The judge lands first, so a write case can never pass by doing nothing.**

- **Neo4j is the verdict only, never the schema.**
  - `snap.py` runs each case's query in an explicit transaction and commits it: Neo4j checks some writes only at
    commit (a node deleted with its relationships left raises then), and the next case empties the graph. For a query
    Neo4j's summary marks as writing, it dumps the graph after (each element the graph before held keeping its
    ordinal), then runs the control query over it. (The first draft rolled back; the commit-time check made it
    commit, measured on the DELETE scenarios, 2026-10-18.)
  - The answers file gains Neo4j's post-graph (`after`) and the control query's answer (`control`). The side effects
    are the diff of the two graphs, computed the TCK's way, and `snap.sh --check` holds that diff to every TCK
    side-effect table (all 248 agree, and the 2935 "no side effects").
  - Nothing in lite's model is taken from them.
- **The schema on A is lite's own inference,** from two sources it owns:
  - the pre-graph's data, as today;
  - the query's write clauses, typed by lite's own typer: every label, relationship type and property key a CREATE,
    SET or MERGE names, each property typed by the expression written into it (`{num: 1}` is an Integer; `{num: x}`
    takes the UNWIND element's type).

  Two fallback rules cover what that leaves open:
  - a property whose type the writes don't fix (only ever null, or several types) becomes a Variant column, as W7's
    two-typed properties are;
  - a relationship type the pre-graph lacks becomes a junction table (any number of links), or an edge class when the
    query writes its properties.
- **B needs no schema.**
- **Each case runs in a transaction the harness owns:** the statement through the island, then the post-graph read
  back through the island (the oracle's own dump queries), then the side effects (lite's diff of its two reads),
  then the control query if any. Then it rolls back, so no case leaks into the next. The graph before is read back
  once per seeded graph and must be Neo4j's dump, or the judge is broken there (a JUDGE problem fails the lane).
- **The verdict has four parts:**
  - the rows, as today;
  - the side-effect counts, lite's diff against Neo4j's (which is the TCK's table);
  - the post-graph, against Neo4j's, compared structurally;
  - the control queries' rows.
- **Write errors** are judged as BOTH_ERROR is today, but against the error raised, not a refusal.
- **Every write case runs on both kinds of graph,** each over DuckDB and Postgres:
  - **On A,** the cases that change a node's labels are decided as refused (a register row: "on a domain model a
    node's label is its class").
  - **On B,** all 245 run, with the 33 control queries.

## 7. The decisions (2026-10-18)

1. **D1. Layering:** L1 store writes under L2 class writes and the native store, every front end translating to Pure.
   *Agreed.*
2. **D2. Vocabulary:** upstream's `write` lowered for real, plus lite's `update`, `delete` and `merge`. The same verbs
   take a table accessor (L1) or a class or association (L2). *Proposed, unchallenged.*
3. **D3. Writable mappings: direct (1-1) only,** checked in phase F and refused by name otherwise. *The user's call.*
4. **D4. Keys minted by the database only:** an identity or sequence, else max plus row number inside the statement
   (registered as unsafe under concurrent writers). *Proposed, unchallenged.*
5. **D5. Two kinds of graph:** A (domain model, schema-first) and B (native store, schemaless). A query names its
   graph. *Agreed.*
6. **D6. Labels:** on A a label is the class, and label changes are refused by name. On B labels are rows. The
   label-set row move of the first draft is dropped. *Agreed.*
7. **D7. One transaction per statement,** writes staged through session temp tables, rollback on error, a caller's
   transaction joined. *Proposed, unchallenged.*
8. **D8. The judge first.** Neo4j is the verdict only. A's schema is lite's own inference (§6), B needs none. Per-case
   rollback, the four checks. *Agreed (the user: "we can't use Neo4j, have to do our own inference").*
9. **D9. Order** as §8.
10. **D10. Ownership:** this line builds L1's MIR and render arms in the database owner's files, announcing each in
    `docs/IN_FLIGHT.md`. *Proposed, unchallenged.*

## 8. Legs

1. **The judge (harness only). Landed 2026-10-18** (`docs/CYPHER_STUDY_2026_10_15.md` §12.94).
   - `snap.py`: the post-graph and the control queries' answers, recorded in the answers file; the side effects the
     diff of the two graphs, checked against every TCK table. (Committed, not rolled back: Neo4j checks some writes
     at commit.)
   - `CypherParity`: per-case transaction and rollback, the side-effect counts, the post-graph compare, the control
     queries.
   - The measured size of each group, of the 276 cases Neo4j marks as writing: 111 inside the pre-graph's schema,
     127 that need the inferred schema, 26 label changes, 12 graphs the harness cannot model.
   - The 245 stay REFUSED (the 33 SKIPs now run: 31 REFUSED, 2 BOTH_ERROR), but none can pass vacuously.
   - **Moved to leg 5:** `CypherGraph`'s A schema from the query's typed writes. The parser refuses a write clause
     before it builds one, so the inference lands with the clauses.
   - Found on the way, fixed in the island: `elementId(x)` returned; an absent relationship's identity null; a
     Variant property no relationship end.
2. **L0 and L1. Landed 2026-10-18** (GATES' ninety-third landing).
   - The four `SqlDml` records (`InsertSelect`, `Update`, `DeleteWhere`, `Merge`), spelled by DuckDB, H2 and Postgres
     in the standard forms measured on each (H2 has no `ON CONFLICT`, so the upsert is `MERGE` on all three); every
     other dialect refuses each by name. `StoreWritesTest`.
   - `write` lowered for real; `delete`, `update` and `merge` lite natives (`LITE_SURFACE`), typed by `WriteChecker`
     against the table's columns by name (S97), lowered by `Lowerer.lowerStoreWrite`.
   - The execution: `StatementExecutor` runs a store write at the statement's root (never through the effect arms,
     which reseed), its value the database's update count (`Executor.executeUpdate`); a program that writes runs in
     one transaction (`Executor.inTransaction`), a caller's open transaction joined. It is `Execution.execute`'s own
     path, so no separate `executeWrite` entry was needed; `LoweredStatement` was not needed either (a write is the
     statement's root, lowered on its own).
   - Tests on DuckDB, H2 and Postgres 16: counts, column subsets, a failing write leaving the table as it was, a failing
     second write rolling the first back, a caller's transaction joined, a class query's rows, a source whose physical
     column order differs from its declaration, the checker's refusals. `StoreWritesExecutionTest`,
     `StoreWritesPostgresTest`.
   - **Deferred, then landed with leg 7 (2026-10-20):** `renderStatement(SqlDml)` (text plus `?` binds). A write
     runs as text at execution, as a query does; a bound parameter in a DML statement's text is refused, and its
     statement form binds it. The lite plan (`TypedQuery.executionPlan`) plans a program that writes (leg 7); the legacy
     plan (`TypedQuery.plan`) still refuses each write by name.
3. **The spike: the native store as a platform class model** (§4, L2'). It decides B's cost and its legs.
   **Answered 2026-10-19: yes, contained work.**
   - **How:** graph B is the domain realization's own class model, mapping and island, realized over the native
     store's fixed tables through views. The views are the platform's over its own system tables (the world map's
     tenet), and every one is a table the mapping already maps (`CypherGraph.Realization.NATIVE`,
     `CypherGraph.Native`).
   - **The layout, decided:** narrow rows, not one JSON column per node.
     - `nodes (id, labels)`: the label set as one canonical text (`:A:B:`), so a class's view is one equality.
     - `node_props (node_id, key, slot, v_int, v_float, v_str, v_bool, v_date, v_ts, v_time, v_json)`: one row
       per property slot (a scalar's slot `''`; a duration's, a zoned or a nanosecond value's, its parts). Each
       value sits in the column of its slot's type, with a scalar's JSON beside it for a property typed two ways.
     - `rels (id, type, src_id, dst_id)` and `rel_props`, alike.
     - Why: every Cypher write is then one of L1's four verbs over a row. A property set is a `merge` on (node, key,
       slot), a removal a `delete`, a label change an `update` of `labels`, a create an insert. A JSON column would
       need JSON-editing functions in every dialect.
   - **Every relationship type is a junction** (or an edge class's bridge) in the views. A foreign key holds one
     end, which writes would outgrow.
   - **Measured** (`//core:cypher_parity_native_test`, DuckDB, the whole corpus): PASS 3989 against the domain lane's
     3997 (a first run, 3981, found one view's mistake: a label among others is a marker class holding no node of
     its own). The rest is the same but for:
     - two refusals more (a deep pattern now fans out over a junction) and two fewer (a hop the domain graph held
       in a foreign key reads over a junction);
     - eight failures and one error from translator gaps over many-to-many associations, which the domain lane's
       graphs (mostly foreign keys) did not exercise: a pattern comprehension's list over a hop that finds nothing is
       `[null]`, not `[]`; two optional hops' columns.

     The native lane has a ledger of its own (`cypher-parity/native-ledger.tsv`).
   - **What it leaves for B's writes (leg 6):**
     - staging: the rows in hand materialized once into a session temp table before each write clause, with the
       ids the clause mints;
     - the write-schema: the labels, keys and types a query's writes introduce, typed by the translator itself, so
       the model has their classes and views;
     - the clauses, each over the staged rows and the narrow tables.
4. **`#SQL` DML** on L1, judged by Postgres 16. **Landed on the branch 2026-10-20** (GATES' ninety-sixth landing;
   register row S103).
   - **The parser:** `INSERT`, `UPDATE` and `DELETE` (`SqlAst.Write`: `Insert`, `Update`, `Delete`) as the statement
     itself; inside a query, Postgres's syntax error; as a WITH query, refused by name. An INSERT's VALUES is typed by
     the columns it writes (no text for a column of NULLs alone).
   - **The translator** (`SqlToPure`, the writes section): an INSERT is `write` of its source's rows, each column by
     its place (a literal of no type yet typed by its column, as Postgres types it); `ON CONFLICT (key) DO NOTHING` a
     `write` of the rows no row of the table holds the key of; `DO UPDATE SET c = EXCLUDED.c` L1's `merge` by the key,
     the key checked against the table's primary key as the store declares it (`Sources.primaryKey`). UPDATE is
     `update`, DELETE `delete`; `UPDATE … FROM` an EXISTS and a correlated scalar subquery per SET, `DELETE … USING` an
     EXISTS. A value written into a column of another type is Postgres's error unless both are numbers (the
     database's assignment cast).
   - **RETURNING** is a program of statements (`SqlToPure.program`, spliced in by `SpecCompiler` as a Cypher write's
     is): the rows the statement writes staged (`stage`), the write, then the RETURNING list over the staged rows, each
     column cast to its declared type (a `numeric(p, s)` cast, new in the translator: rounded to its places, its
     overflow the database's error). Staging named the session's temporary table's columns on Postgres (`CREATE
     TEMPORARY TABLE t ("ID", ...) AS ...`): a stored column a store declares bare is read folded, and the staged rows
     are read by their relation's names. A staged NULL constant is typed by a branch never taken (`if(false, |0,
     |[])`), so it is `[0..1]`: a NULL of `[0]` raises wherever it is read, and a column of it has no type.
   - **The judge:** a case of `sql-parity/lite/dml.yaml` carries a `check` query; Postgres's answer to the statement
     (its count, or its RETURNING rows) and to the check, run in one transaction rolled back after, are snapped
     (`postgres-answers.jsonl`, keys `lite/dml/<id>` and `lite/dml/<id>#check`); each lane runs both in a transaction
     it rolls back and judges each. 56 cases: 45 PASS on both lanes, 8 BOTH_ERROR (Postgres's own errors: unknown and
     repeated columns, too many expressions or columns, text into a number, ON CONFLICT on no unique index), 3 REFUSED
     (DEFAULT, DEFAULT VALUES, a data-modifying WITH).
   - **Keyed tables:** the parity schema has no primary key, so ON CONFLICT, RETURNING, `UPDATE … FROM` and
     `DELETE … USING` over a keyed table run in `SqlWritesExecutionTest` on DuckDB, H2 and Postgres 16 (H2 reaches no
     outer row from a derived table: a SET's correlated scalar subquery is its error there, so `UPDATE … FROM` is
     measured on DuckDB and Postgres).
5. **L2 and Cypher on A:** **Landed on the branch 2026-10-20** (GATES' ninety-fifth landing; register row S102).
   - **L2, the class writes:** `rows->meta::legend::lite::writeInstances(C, ~key)`, `mergeInstances(C, ~key)`,
     `deleteInstances(C, ~key)`, `pairs->link(C, ~end, ~key)` and `unlink(...)`, lite natives typed by their
     signatures. The store resolver rewrites each, in any space, into L1's statement over the class's mapping
     (`resolver/ClassWrites`): the rows' columns renamed from properties to columns, the key column into the primary
     key; a merge by the key; a delete where a row names the instance's key; a link or an unlink a merge of the
     foreign key's table (the class's own row, or the other end's). `ClassWritesExecutionTest` on DuckDB and H2.
   - **Direct, checked by name:** a mapping is writable when its pipeline is its main table alone (no filter, no
     distinct, no group-by, no union) and every property a plain column of it; the key the mapping's primary key.
     An embedded property is written by its sub-properties, the rows' columns named `p.sub` (2026-10-20), each the
     column the embedded mapping reads it from; the Cypher writer writes a zoned value's instant and offset, and a
     duration's four slots, so, and removes one by setting each sub-property to none. An
     association is writable over one foreign key (the association's condition the equality of it and the other
     table's key); a junction table is refused by name.
   - **Cypher's writer over a domain model** (`CypherToPure.Writer`, `domain`): a node's class is its label set's;
     the store's `nodes` and `rels` as the writer reads them are unions over the classes' and edge classes' extents,
     and a property's rows the classes declaring it (`domainTable`, `propRows`); every write a class write. Ids are
     the graph's, unique across the classes (minted as on B, S100); an element's identity its class's path and its id.
     A relationship's edge class is its type's between its ends' classes, the database choosing row by row where
     several may.
   - **The resolver, two gaps closed on the way:** a closed class query under a data lambda resolves where it stands
     (`SubQueryLift.resolveClosedQueries`: a delete's predicate over the instances a clause matched, a merge's rows
     no node matches), and a relation operator whose rows no class chain feeds resolves as the relation-space
     wrapper it is.
   - **The harness:** a write case's model is the schema of the graphs before and after (`CypherGraph.inferWrite`),
     every relationship type an edge class, plus each property of a literal value the query sets (a branch Neo4j's
     run never took: an ON MATCH where it created); its tables seeded with the graph before's rows alone.
   - **Refused by name** (S102): a label set or removed (a node's labels are its class), a property no class declares,
     a node deleted whose class has an association end, a mapping that is not direct, a junction association.
   - **Found on the way, fixed for both graphs:** a relation read whole as a value indexed its columns by a scan per
     column (`ValueCollections.rowMajorCellList`), quadratic in a staged row's columns: `Create4/[2]`'s few hundred
     CREATE clauses took 427s on the native lane, 21s after (`SqlExpr.Column.lookup`).
   - **Measured:** see GATES' ninety-fifth landing.
6. **B and Cypher on B:** the platform schema, then every write clause, label changes included; the 33 control queries
   run here. **Landed on the branch 2026-10-20** (GATES' ninety-fourth landing).
   - **The program.** A statement with a clause that updates the graph is a program, spliced in where the island
     stood (`SpecCompiler.typeQueryBody`, `CypherIslands.statements`): each clause in order, the rows in hand staged
     before each write clause (`let s = rows->meta::legend::lite::stage()`, a temporary table of the session, dropped
     when the program has run), the writes L1's `write`, `update` and `delete` over the staged rows and the narrow
     tables, the elements a later clause reads read back by their ids (`Writer.refreshed`).
   - **The native store:** a DataSpace marked `<<graph::graph.Native>>` holding the store's database
     (`CypherToPure.Model.nativeStore`); a write over a graph of a domain model is refused by name (leg 5).
   - **Ids** are minted by the database: above the store's greatest as the statement started and every id minted
     since (S100), the row's number and the element's place.
   - **The clauses.** CREATE (consecutive ones as one); SET and REMOVE of properties, maps and labels (the last row's
     value where several rows write one property); DELETE and DETACH DELETE of carried elements and of any value
     naming them (a path, a list's element, a map's entry, by its JSON's identities), Neo4j's DeleteConnectedNode and
     DeletedEntityAccess raised by the database; MERGE: the pattern matched against the narrow tables, the unmatched
     rows grouped by their merge key and each group's pattern created once (its first row creating it, ON CREATE over
     the creating rows, ON MATCH over the others), a named path bound, a null property Neo4j's error (S98, S99).
   - **The read back:** a node's identity as the views spell it (its label set's class's path and its id), so a
     merged, created or relabelled element re-anchors; a list written as a property read as its shape.
   - **The judge:** a case whose query updates the graph runs in a rolled-back transaction even where Neo4j raised;
     the store's TIMESTAMP and TIME columns read back; a statement ending in an update compared with no columns (S101).
   - **Refused by name:** a nanosecond value written, `startNode`/`endNode` of a relationship no write created or
     merged (one a pattern reads, or one carried through a projection), a write to an element carried without
     identity; on the read side over edge classes (every native relationship is one): a hop from a node an edge
     class reached past depth two, a type joining several class pairs in an OPTIONAL MATCH.
   - **Temporal properties** (2026-10-20, GATES' ninety-seventh landing; §5's Cypher): zoned values and durations
     written as their slots, a list of temporals as its elements' texts under the graph profile's list marks. All 18
     Temporal4 write cases pass on B, the 12 lists on A; on A a zoned value or a duration is an embedded value
     class, written since 2026-10-20 (GATES' ninety-ninth landing) through its sub-properties (leg 5's class
     writes), so all 18 pass there too. Refused by name still: a list of zoned values the database
     computes (one zone for its elements), of a named zone or a zone the database reads, a list that may hold a null
     or holds values of several kinds, a list of nanosecond values.
   - **`startNode` and `endNode`** (2026-10-20, GATES' ninety-eighth landing): of a relationship a write created or
     merged, the nodes the clause wrote it between, where the rows carry both (a relationship's shape names their
     identity columns, `RelEnds`). A CREATE's start is the pattern's; a MERGE's is the stored relationship's source,
     which the database reads row by row (an undirected MERGE matches either direction), so `startNode(r).name`
     is the column of whichever node that is. Over a domain model, a relationship over an association has no row
     of the store's relationships to read its source from: refused by name there. Merge5 [11] passes on both
     graphs (`CypherNativeEndsTest` pins the other directions).
   - **Found with it:** a MATCH of several patterns whose second hops from a node the first reached through an edge
     class (a cycle closed by the second, Match3 [18] on B) reads each pattern as rows, joined on the nodes they
     share, its relationships distinct across them, as patterns sharing no node already were. On A, the harness's
     model holds the class of a MERGE's free node's label set where Neo4j's run matched and created nothing (its
     creating branch still compiles; Match8 [2]), as it holds a property of a branch never taken.
7. **Write plans. Landed on the branch 2026-10-20** (GATES' hundredth landing), in the database owner's files under D10
   (`docs/IN_FLIGHT.md`). A program that writes plans as an `ExecutionPlan` the model-free runner executes, so a write
   runs where a query runs through a plan, every value bound. Before it, `TypedQuery.executionPlan` lowered a body as
   one query and refused a write in it by name, and the server's `pure/v1/execution/execute` ran the wire path, which
   refused one too: a Cypher write or `#SQL` DML ran in process (`Execution.execute`), not over HTTP.
   - **The dialect: DML as a statement.** `SqlDialect.renderStatement(SqlDml)`: the text and its `?` parameters in
     placeholder order, as `renderStatement(SqlQuery)` is. The store writes' and the staged table's render methods
     return the writer they wrote into (invariant 3); `render(SqlDml)` stays text, and a parameter in it is refused as
     a query's text refuses one. A dropped table's DDL is text (it holds no value). `StoreWritesTest` binds an UPDATE's
     value and predicate and a staged table's predicate on DuckDB and both H2 dialects (Postgres's placeholders
     listed).
   - **The plan records** (`ExecutionPlan`, `PlanJson` version 5):
     - `Update(sql, count)` is a statement run for the change it makes (a write into a store table, a staged table
       created or dropped). Its answer is the count the database reports. `count` is, where a later step reads that
       count, the name its slot binds it by (`#count`, which no declared parameter can be named).
     - `Check(sql)` is a query run for the error its database may raise (a Cypher write's guard: DeleteConnectedNode,
       DeletedEntityAccess). Its rows are read and dropped.
     - A `Sequence` holding an `Update` runs in one transaction of its session (D7): its changes all land or none do,
       and a caller's open transaction is joined. Its answer is its one result step's, passed on once the transaction
       has committed.
   - **The runner** (`PlanRunner`, `PlanSessions`):
     - A sequence's steps run on one session. Each statement's target must be the same, or the plan is refused by
       name. The session statements run once. Each change's count is kept for the slots after it, and the result's
       text is held until the commit. A raise the session holds surfaces as the execution's does (`RaisedErrors`).
     - Decision A's second rule: a plan that changes a database the runner shares runs on a database of its own,
       opened and set up from the target for that run (`Source.openToChange`), so the shared one is never changed. A
       caller's connection, a database the user names, or one a URL reaches keeps the change.
   - **The planner** (`PlanMaker.program`; `TypedQuery.resolve` and `lowerer`, the wire path's steps split out).
     A program that writes or stages is planned statement by statement, each with the lets before it, as the
     execution's body compiler takes them, every declared parameter a value its statement binds:
     - a staged let is an `Update` creating `legend_stage_s`; the let is bound to that table for the statements after
       (`StagedRows`, which now owns the table's name and its store for both paths), and an `Update` drops the table
       after the result;
     - any other let rides the lets of the statements after;
     - a store write (L1's, or L2's after the store resolver) is an `Update` of `Lowerer.lowerStoreWrite`;
     - any other statement before the last is a `Check`;
     - the last statement is the answer: a query's text in the form the caller asks for, or a write's count, bound
       after its `Update` into the one placeholder of a query of its own (`Lowerer.lowerPlanParam`). Either way the
       database writes the text.
   - **The server:** `pure/v1/execution/execute` answers a program that writes from its plan
     (`Execution.executeUpstream`, the query compiled once), on the runner's sessions. Step 4 of the plan program
     switches every query; writes go first, because the wire path cannot run them at all.
   - **The judge:**
     - `WritePlansTest` on DuckDB, H2 and Postgres 16 runs each of L1's writes with its parameters bound, planned and
       run, against the rows the table holds after. It also covers a failing write leaving the table as it was, a
       failing second write taking the first back, a caller's transaction joined, a staged program, and the plan read
       back from its JSON running alike.
     - `WritePlansH2Test` checks that a write plan never changes the database the runner shares.
     - `CypherWritePlansTest` runs nine Cypher writes on the native store (CREATE, SET of a property and a label,
       REMOVE, DETACH DELETE, MERGE that creates and that matches, UNWIND then CREATE, an undirected MERGE) and
       DeleteConnectedNode. Each is run by its plan and by its execution, and must give the same rows, the same graph
       after and the same error. A write with no RETURN also runs through upstream's execute path
       (`Execution.executeUpstream`), answering its empty relation and leaving the graph its plan leaves.
     - `PureV1ApiTest.e8_execute_aProgramThatWrites_runsFromItsPlan` runs a staged write program through `execute`.
   - **Not yet:** a write whose answer is a count, over `execute` (a scalar's TDS answer is unprobed for reads too).
     Since 2026-10-20 it is refused by name before its plan runs, as a read's scalar is
     (`Execution.executeUpstream`; `WritePlansH2Test.executeRefusesACountAnswerBeforeItsWriteRuns`): before, its write
     landed and the answer then failed to serialize.
8. **Later:**
   - GraphQL mutations, once `#GQL{...}#` compiles;
   - Persistence-style ingest modes over `merge`.

**The order since 2026-10-19 (the user: "keep going ... until we have all Cypher and TCK completely working end to
end"):** 3, then 6 (the TCK's write cases judged on B, which writes through L1 alone), then 5 (A, which needs L2's
inverse), then 4 (`#SQL` DML), then 7 (write plans). All seven landed on the branch by 2026-10-20.

**Expected end state:**
- the TCK at 3878 of 3880 (all but the 2 signed years) on B;
- on A, every write case except the label changes;
- every write judged on rows, side effects, the post-graph and the control queries;
- `#SQL` DML running on Postgres, DuckDB and H2.
