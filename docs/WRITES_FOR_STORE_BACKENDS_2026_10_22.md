# The write design, for a backend that takes writes into store tables (2026-10-22)

A handoff note for the Deephaven backend line (keyed tables: replace by key, delete by key), from the Cypher writes
line. The design is `docs/WRITES_DESIGN_2026_10_18.md`; its §8 records each leg as it landed. This note answers five
questions about it and points at the code. Related: F1 in `docs/BI_AND_ETL_PLAN_2026_09_29.md` ("write a relation into
a table").

**Where the code is:** branch `claude/keen-noether-v5mis0` (pull request neema2/legend-lite#67; `claude/cypher-writes`
holds the commits as of 2026-10-22, before main was merged in). It is NOT on `main` yet. Main was merged into the branch
on 2026-10-22: its conflicts were in `SqlToPure` (the `#SQL` island's write path), guard tests, ratchets, the register
and `tools/deps/core-layers.txt`.

## 1. The write operations

Three layers, every front end translating to Pure and the one compiler lowering it. No island writes SQL.

| Layer | Target | Operations |
|---|---|---|
| **L1, store writes** | a table accessor `#>{db.schema.table}#` | append, update, delete, merge (upsert) |
| **L2, class writes** | a class (or an association) through a direct mapping | create, merge, delete instances; link, unlink an association end |
| **L2', the native graph store** | the platform's fixed tables (`nodes`, `node_props`, `rels`, `rel_props`) | every Cypher write as L1 rows (no schema) |

**L1, the four verbs** (each returns `Integer[1]`, the rows the database affected):

| Verb | Pure | The database's statement (`SqlDml` record) |
|---|---|---|
| append | `rel->write(#>{db.t}#)` | `INSERT INTO t (cols) SELECT ...` (`InsertSelect`) |
| update | `#>{db.t}#->update(r \| pred, ~[c: r \| expr, ...])` | `UPDATE t SET c = expr ... WHERE pred` (`Update`) |
| delete | `#>{db.t}#->delete(r \| pred)` | `DELETE FROM t WHERE pred` (`DeleteWhere`; `r \| true` deletes every row) |
| merge | `rel->merge(#>{db.t}#, ~[keys])` | `MERGE INTO t USING (...) ON keys WHEN MATCHED UPDATE ... WHEN NOT MATCHED INSERT ...` (`Merge`) |

Beside them, `let s = rows->meta::legend::lite::stage()` computes rows once into a session temporary table
(`CreateTempTableAs.staged`), gone when the program ends: dropped by its last step, or, on H2 (whose `DROP TABLE`
commits), by the database as the transaction ends (`ON COMMIT DROP`). A program's later statements read it, so the
writes between read the rows as they were (read-your-own-writes for a multi-step write). Any other let is computed where
it is read: a let whose value writes, or one read after a write changes its rows, is refused by name (stage it).

**How each front end lands on them:**
- **Cypher:**
  - `CREATE` is an append;
  - `SET` and `REMOVE` of a property are an update (on the native store, a merge or a delete of a `node_props` row);
  - `SET` and `REMOVE` of a label update `nodes.labels` on the native store and are refused on a domain model (a
    node's label is its class);
  - `DELETE` is a delete, guarded by a check the database raises on (Neo4j's DeleteConnectedNode);
  - `DETACH DELETE` deletes the relationships, then the node;
  - `MERGE` is a match, then an append of what the match lacks, with `ON CREATE` and `ON MATCH` updates scoped to
    each.
- **`#SQL` DML** (a table named as the island names one, `tb('db::Store.TABLE')`, e.g. `#SQL{INSERT INTO
  tb('w::DB.ITEM') (ID, NAME) VALUES (1, 'a')}#`):
  - `INSERT` is `write`;
  - `UPDATE` is `update`; `DELETE` is `delete` (`DELETE ... USING` is an EXISTS);
  - `ON CONFLICT (k) DO UPDATE` is `merge`;
  - `DO NOTHING` is a `write` of the rows whose key no table row holds;
  - `RETURNING` reads the staged rows back.

**Not modelled yet** (F1 asks for them): a persistent `CREATE TABLE AS` from a relation's type, and truncate-then-insert
as one verb. Today that is two statements of one program, `delete(r | true)` then `write`, and they land in one
transaction (§5).

**For a keyed table:**
- *replace by key* is `merge` with the table's key;
- *delete by key* is `delete` with a predicate naming the keys (an EXISTS over a relation of keys, which is what
  `deleteInstances` and `DELETE ... USING` lower to).

## 2. Tables, or classes through mappings

**Both, layered.** L1 writes a table accessor only. L2 writes a class only when its mapping is *direct*, and the
store resolver rewrites each L2 call into L1 over the mapping's table (`core/src/main/java/com/legend/resolver/ClassWrites.java`):
the rows' columns are renamed from properties to columns, and the key goes into the primary key.

**A mapping is direct when:**
- its class's rows are one table's: no `~filter`, `~distinct`, `~groupBy` or union, and not model-to-model;
- every property written is a column of that table, or an embedded class's sub-property (written as `p.sub`);
- its key is one column.

An association is writable over one foreign key; a junction table is refused by name. Anything else is refused by
name, never guessed, because inverting a general mapping is the view-update problem (decision D3: the user's call).

## 3. The Pure functions

| Function | Layer | Typed by | Lowered or rewritten by |
|---|---|---|---|
| `meta::pure::functions::relation::write` (upstream's native, signature verified) | L1 | `compiler/spec/WriteChecker` | `lowering/Lowerer.lowerStoreWrite` |
| `meta::legend::lite::update`, `::delete`, `::merge` (user-facing) | L1 | `WriteChecker` | `Lowerer.lowerStoreWrite` |
| `meta::legend::lite::stage` | staging | its signature | `resolver/StagedRows` |
| `meta::legend::lite::writeInstances`, `::mergeInstances`, `::deleteInstances`, `::link`, `::unlink` | L2 | their signatures | `resolver/ClassWrites`, into L1 |

The FQNs are in `core/src/main/java/com/legend/builtin/Pure.java` (`Pure.Lite`). The L2 natives are emitted by the
Cypher translator only today: they are not bare-name resolvable from user Pure yet. Typing (register row S97) is by
NAME, where upstream's `write` binds by position:
- each column of the rows names a column of the table, of the same type or both numeric;
- the table's other columns take their defaults;
- `merge`'s keys are columns of both the rows and the table.

The MIR is `core/src/main/java/com/legend/sql/SqlDml.java`: pure data, one render arm per dialect, no `default ->`.
DuckDB, H2 and Postgres render all four (H2 has no `ON CONFLICT`, so the upsert is `MERGE` on all three). Every other
dialect refuses each by name. **A new backend is a render arm per record** (or a refusal by name), with the MIR
unchanged.

## 4. Keys and identity

- **L1 `merge`:**
  - The keys are the caller's `~[keys]`. The checker requires them to be columns of both sides, not a declared
    primary key (a `MERGE` needs no unique index).
  - `#SQL`'s `ON CONFLICT (k)` is checked against the table's PRIMARY KEY (`Sources.primaryKey`), because Postgres
    requires one there.
  - A matched row updates the columns the rows name, other than the keys. Columns the rows don't name are left as
    they are on an update, and take their defaults on an insert. When every column is a key, there is no
    `WHEN MATCHED` arm: it is insert-if-absent.
  - So "replace the whole row by key" means the rows name every column.
  - Two source rows with one key are not handled specially: whatever the database's `MERGE` does.
  - A NULL key matches no row (the database's `MERGE`): each merge of it inserts another row (measured on DuckDB, H2
    and Postgres, 2026-10-22).
- **L2:** an instance's key is its mapping's `~primaryKey`, else the main table's PRIMARY KEY (one column). It is what
  `elementId` spells.
- **Minted ids** (decision D4; register row S100) are the database's, never Java's. A created row's id is above the
  store's greatest as the statement started, plus its row number, so an id deleted within a statement is never given
  again in it. This is registered as unsafe under concurrent writers. An identity or sequence column, where a store
  declares one, is the design's preferred form and is not built yet.
- **Cypher's identity:** a stored element is its class's path and its id (`g::A:7`); on the native store, its
  `nodes.id`.

## 5. All-or-nothing

- **A program that writes, or stages rows, is one transaction.** On the execution path, `Executor.inTransaction` runs
  every statement on one connection and commits at the end; any error rolls all of them back. A failing second write
  takes the first back, a staging between them included (pinned by `StoreWritesExecutionTest` and `WritePlansTest`, on
  DuckDB, H2 and Postgres).
- **A caller's open transaction is joined, not committed.** The parity harness relies on this to roll every case back.
- **The plan path** (leg 7: `executionplan/ExecutionPlan`, `exec/PlanRunner`):
  - a write program plans as a `Sequence` of `Update` (a change, its count bindable as `#count`), `Check` (a query
    run for the error it raises) and one `TextResult`;
  - the sequence runs on one session in one transaction, every step on the same target (refused by name
    otherwise);
  - the answer is released only after the commit;
  - staged tables are dropped after the result (a failed transaction takes them back with its writes).
- **A database the runner shares is never changed** (decision A's second rule, `PlanSessions.Source.openToChange`):
  for example an H2 connection's in-memory test data. A write plan on one runs on a private copy, set up from the
  target. A caller's own connection, or a database a URL reaches, keeps the change.
- **Not covered:** a write across two databases (no two-phase commit; one target per sequence). Over upstream's
  `execute`, a program whose answer is a count (a value, not rows) is refused by name before it runs, because that
  answer's shape was never measured against legend-engine.

## Where to read more

- The design and its legs as landed: `docs/WRITES_DESIGN_2026_10_18.md` (§4 the layers, §8 legs 2, 4, 5, 6 and 7).
- The register rows: S97 (by-name typing), S100 (ids), S101 (a statement ending in an update), S102 (writes over a
  domain model), S103 (`#SQL` DML), in `docs/SEMANTICS_REGISTER.md`.
- The tests: `StoreWritesTest` (rendering), `StoreWritesExecutionTest` and `StoreWritesPostgresTest` (L1 on DuckDB, H2
  and Postgres 16), `ClassWritesExecutionTest` (L2), `WritePlansTest`, `WritePlansH2Test` and `WritePlansPostgresTest`
  (plans), `CypherWritePlansTest` (a Cypher write by plan and by execution), `SqlWritesExecutionTest` (`#SQL` over
  keyed tables).
