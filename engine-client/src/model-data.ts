// A model's own test data, in the tab's DuckDB (plan A2, the first source of rows): every relational Data element's
// tables (`relationalCSVData`: schema, table, CSV text), made and filled by the statements the server seeds a database
// with -- legend-lite's own (setup.CsvSeed, on the plan side), asked of the planner in the tab (a SeedSource) -- so a
// table here holds what the same table on the server holds, its column types the server's. This file says which tables
// a model's test data fills and which Database declares each, and runs the statements it is given.

/** The model JSON parts read here (grammarToJson/model). */
interface ModelElement {
  readonly _type: string;
  readonly name?: string;
  readonly package?: string;
  readonly schemas?: readonly {
    readonly name: string;
    readonly tables: readonly { readonly name: string; readonly columns: readonly { readonly name: string }[] }[];
  }[];
  readonly data?: { readonly _type: string; readonly tables?: readonly { readonly schema: string; readonly table: string; readonly values: string }[] };
}

/** A table a model's Database declares: where it is, and its columns' names. */
export interface DeclaredTable {
  /** The Database's path. */
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string }[];
}

/** One table of test data: the Database that declares it, where it goes, its CSV (header first), the Data element. */
export interface DataTable {
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly csv: string;
  readonly element: string;
}

/** A table asked for: where it is, and its CSV (header first; a header alone makes it empty). */
export interface SeedTable {
  readonly schema: string;
  readonly table: string;
  readonly csv: string;
}

/**
 * What the planner gives for one table: the server's statements that make and fill it, and the names that reach it
 * again as those statements spell them -- so this file spells no table or column name of the model's itself.
 */
export interface TableSeed {
  /** The schema, the table with the server's column types, its rows as one INSERT (none for a header alone). */
  readonly sql: readonly string[];
  /** The table's name in SQL (no schema for the `default` one, as the seed makes it). */
  readonly table: string;
  /** The statement that drops it. */
  readonly drop: string;
  /** Each declared column: its name as written (what a file's header says) and its name in SQL. */
  readonly columns: readonly { readonly name: string; readonly sql: string }[];
}

/** A table of test data with what the planner gave for it. */
export interface SeededTable extends DataTable {
  readonly seed: TableSeed;
}

/**
 * Where a model's test-data statements come from: legend-lite's planner in the tab (engine-client's
 * WasmGrammar.testDataSql, DataCube's WasmPlanner.testDataSql): for each table `database` declares, its TableSeed, in
 * order. A table the Database does not declare is refused.
 */
export interface SeedSource {
  testDataSql(model: string, database: string, tables: readonly SeedTable[]): Promise<TableSeed[]>;
}

/** Every table the model's Databases declare (its JSON's elements), in the order declared. */
export function databaseTables(elements: readonly ModelElement[]): DeclaredTable[] {
  const out: DeclaredTable[] = [];
  for (const e of elements) {
    if (e._type !== 'relational') continue;
    for (const s of e.schemas ?? []) {
      for (const t of s.tables) {
        out.push({ database: `${e.package}::${e.name}`, schema: s.name, table: t.name, columns: t.columns.map((c) => ({ name: c.name })) });
      }
    }
  }
  return out;
}

const tableKey = (schema: string, table: string): string => `${schema}.${table}`;

/**
 * The one Database of the model that declares `schema.table` (its JSON's elements). None, or more than one, is refused:
 * which one would be a guess (the server is told, by the test that names its store; the tab has no test to ask).
 * `what` names whose table it is, for the refusal.
 */
export function databaseOf(elements: readonly ModelElement[], schema: string, table: string, what: string): string {
  const dbs = databaseTables(elements).filter((t) => t.schema === schema && t.table === table).map((t) => t.database);
  if (dbs.length === 0) throw new Error(`${what} ${schema}.${table}, which no Database of the model declares`);
  if (dbs.length > 1) throw new Error(`${what} ${schema}.${table}, which more than one Database declares (${dbs.join(', ')})`);
  return dbs[0]!;
}

/** The test data of a model (its JSON's elements): one entry per table of every relational Data element (databaseOf). */
export function dataTables(elements: readonly ModelElement[]): DataTable[] {
  const out: DataTable[] = [];
  for (const e of elements) {
    if (e._type !== 'dataElement' || e.data?._type !== 'relationalCSVData') continue;
    const element = `${e.package}::${e.name}`;
    for (const t of e.data.tables ?? []) {
      const database = databaseOf(elements, t.schema, t.table, `the Data element ${element} fills`);
      out.push({ database, schema: t.schema, table: t.table, csv: t.values, element });
    }
  }
  return out;
}

/** What the planner gives for each table: one ask per Database (the model compiled once for each). */
export async function seeded(source: SeedSource, model: string, tables: readonly DataTable[]): Promise<SeededTable[]> {
  const byDatabase = new Map<string, DataTable[]>();
  for (const t of tables) byDatabase.set(t.database, [...(byDatabase.get(t.database) ?? []), t]);
  const seeds = new Map<DataTable, TableSeed>();
  for (const [database, ts] of byDatabase) {
    const answers = await source.testDataSql(model, database, ts.map((t) => ({ schema: t.schema, table: t.table, csv: t.csv })));
    if (answers.length !== ts.length) throw new Error(`the planner answered ${answers.length} tables of test data for ${database}'s ${ts.length}`);
    ts.forEach((t, i) => seeds.set(t, answers[i]!));
  }
  return tables.map((t) => ({ ...t, seed: seeds.get(t)! }));
}

/** What loading needs of the tab's DuckDB: SQL. */
export interface DataSink {
  run(sql: string): Promise<unknown>;
}

/** A sink that also takes a file's bytes: a person's own file (Parquet is binary). */
export interface FileSink extends DataSink {
  registerFileBuffer(name: string, bytes: Uint8Array): Promise<void>;
}

/** A name as DuckDB reads it: a person's file's own column names, read as the file has them. */
const ident = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const literal = (s: string): string => `'${s.replace(/'/g, "''")}'`;

/**
 * A table made from a person's own file (plan A2's second source), in one transaction: the table made empty by the
 * server's statements (a header alone: its column types the server's), then the file's rows put in -- each declared
 * column from the file's column of that name, a CSV's as text and a Parquet's as typed, DuckDB converting each value to
 * its column's type -- so a refused file (a column missing, a value its type cannot take) leaves the table as it was.
 * The file's name says which kind it is. It is registered as `<as>.csv` or `<as>.parquet`, never by the person's own
 * name: read_csv and read_parquet read a name as a glob (`sales[2024].csv` matches nothing). The table's names are the
 * planner's (`seed`); returned, to drop it by later.
 */
export async function loadFileTable(sink: FileSink, source: SeedSource, model: string,
  target: { readonly database: string; readonly schema: string; readonly table: string; readonly columns: readonly { readonly name: string }[] },
  file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<TableSeed> {
  const format = /\.csv$/i.test(file.name) ? 'csv' : /\.parquet$/i.test(file.name) ? 'parquet' : undefined;
  if (!format) throw new Error(`${file.name}: a table is filled from a .csv or a .parquet file`);
  const [made] = await seeded(source, model, [{
    database: target.database, schema: target.schema, table: target.table, element: file.name,
    csv: target.columns.map((c) => c.name).join(','),
  }]);
  const seed = made!.seed;
  const registered = `${as}.${format}`;
  await sink.registerFileBuffer(registered, file.bytes);
  const read = format === 'csv' ? `read_csv(${literal(registered)}, header = true, all_varchar = true)` : `read_parquet(${literal(registered)})`;
  await sink.run('BEGIN TRANSACTION');
  try {
    for (const sql of seed.sql) await sink.run(sql);
    await sink.run(`INSERT INTO ${seed.table} (${seed.columns.map((c) => c.sql).join(', ')}) `
      + `SELECT ${seed.columns.map((c) => ident(c.name)).join(', ')} FROM ${read}`);
    await sink.run('COMMIT');
  } catch (e) {
    try {
      await sink.run('ROLLBACK');
    } catch (r) {
      throw new Error(`${e instanceof Error ? e.message : String(e)} -- and putting the table back failed: ${r instanceof Error ? r.message : String(r)}`);
    }
    throw e;
  }
  return seed;
}

/** Each table made and filled: the planner's statements for it, run in order. */
export async function loadDataTables(sink: DataSink, tables: readonly SeededTable[]): Promise<void> {
  for (const t of tables) {
    for (const sql of t.seed.sql) await sink.run(sql);
  }
}

/**
 * The tab's test data, one model's at a time. Tables are named as a model's Databases declare them, so two versions of
 * a project fill the same names: `use` makes one model's test data the rows there -- its own tables made afresh, and
 * every table an earlier model's test data made that this one does not fill dropped -- and loads nothing when that
 * model's are there already. `key` names the model (a project version); none means its text may have changed since
 * (Studio's workspace), so it is loaded each time. `keep` are tables something else fills (a person's file): neither
 * made nor dropped here. Calls run one after another, in the order made; `with` runs a query inside its turn, so
 * another model's load cannot land between the rows made and the query that reads them (two of DataCube's cubes,
 * over two versions of one project, on one page).
 */
export class TestData {
  readonly #sink: DataSink;
  #current: string | undefined;
  /** The tables the last load made: the planner's statement that drops each, by `schema.table`. */
  #made = new Map<string, string>();
  #queue: Promise<unknown> = Promise.resolve();

  constructor(sink: DataSink) {
    this.#sink = sink;
  }

  use(key: string | undefined, tables: readonly SeededTable[], keep: ReadonlySet<string> = new Set()): Promise<void> {
    return this.with(key, tables, () => Promise.resolve(), keep);
  }

  /** `tables` made the rows there (as `use`), then `query` run, before any other call's turn. */
  with<T>(key: string | undefined, tables: readonly SeededTable[], query: () => Promise<T>, keep: ReadonlySet<string> = new Set()): Promise<T> {
    const next = this.#queue.then(async () => {
      await this.#make(key, tables, keep);
      return query();
    });
    this.#queue = next.catch(() => undefined);
    return next;
  }

  async #make(key: string | undefined, tables: readonly SeededTable[], keep: ReadonlySet<string>): Promise<void> {
    if (key !== undefined && key === this.#current) return;
    this.#current = undefined;
    const filled = tables.filter((t) => !keep.has(tableKey(t.schema, t.table)));
    const now = new Set(filled.map((t) => tableKey(t.schema, t.table)));
    for (const [k, drop] of this.#made) {
      if (!now.has(k) && !keep.has(k)) await this.#sink.run(drop);
    }
    this.#made = new Map(filled.map((t) => [tableKey(t.schema, t.table), t.seed.drop]));
    await loadDataTables(this.#sink, filled);
    this.#current = key;
  }
}
