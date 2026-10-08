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

/** A table of test data with the statements, the server's, that make and fill it. */
export interface SeededTable extends DataTable {
  readonly statements: readonly string[];
}

/**
 * Where a model's test-data statements come from: legend-lite's planner in the tab (engine-client's
 * WasmGrammar.testDataSql, DataCube's WasmPlanner.testDataSql). For each block -- one table `database` declares,
 * `schema\ntable\n` and its CSV -- the table made with the server's column types, then its rows as one INSERT (none
 * for a header alone); one list per block, in order.
 */
export interface SeedSource {
  testDataSql(model: string, database: string, blocks: readonly string[]): Promise<string[][]>;
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
 * The test data of a model (its JSON's elements): one entry per table of every relational Data element, with the one
 * Database that declares it. A table no Database declares, or two do, is refused: which one would be a guess.
 */
export function dataTables(elements: readonly ModelElement[]): DataTable[] {
  const declaring = new Map<string, string[]>();
  for (const t of databaseTables(elements)) {
    const k = tableKey(t.schema, t.table);
    declaring.set(k, [...(declaring.get(k) ?? []), t.database]);
  }
  const out: DataTable[] = [];
  for (const e of elements) {
    if (e._type !== 'dataElement' || e.data?._type !== 'relationalCSVData') continue;
    const element = `${e.package}::${e.name}`;
    for (const t of e.data.tables ?? []) {
      const dbs = declaring.get(tableKey(t.schema, t.table)) ?? [];
      if (dbs.length === 0) throw new Error(`the Data element ${element} fills ${t.schema}.${t.table}, which no Database of the model declares`);
      if (dbs.length > 1) throw new Error(`the Data element ${element} fills ${t.schema}.${t.table}, which more than one Database declares (${dbs.join(', ')})`);
      out.push({ database: dbs[0]!, schema: t.schema, table: t.table, csv: t.values, element });
    }
  }
  return out;
}

/** The block a table's statements are asked for: CsvSeed's form, `schema\ntable\n` and the CSV (header first). */
const block = (schema: string, table: string, csv: string): string => `${schema}\n${table}\n${csv}`;

/** Each table's statements, from `source`: one ask per Database, so the model is compiled once for each. */
export async function seeded(source: SeedSource, model: string, tables: readonly DataTable[]): Promise<SeededTable[]> {
  const byDatabase = new Map<string, DataTable[]>();
  for (const t of tables) byDatabase.set(t.database, [...(byDatabase.get(t.database) ?? []), t]);
  const statements = new Map<DataTable, readonly string[]>();
  for (const [database, ts] of byDatabase) {
    const answers = await source.testDataSql(model, database, ts.map((t) => block(t.schema, t.table, t.csv)));
    if (answers.length !== ts.length) throw new Error(`the planner answered ${answers.length} tables of test data for ${database}'s ${ts.length}`);
    ts.forEach((t, i) => statements.set(t, answers[i]!));
  }
  return tables.map((t) => ({ ...t, statements: statements.get(t)! }));
}

/** What loading needs of the tab's DuckDB: SQL. */
export interface DataSink {
  run(sql: string): Promise<unknown>;
}

/** A sink that also takes a file's bytes: a person's own file (Parquet is binary). */
export interface FileSink extends DataSink {
  registerFileBuffer(name: string, bytes: Uint8Array): Promise<void>;
}

const ident = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const literal = (s: string): string => `'${s.replace(/'/g, "''")}'`;

/**
 * A table made from a person's own file (plan A2's second source): the table made empty by the server's statements
 * (`source`, for a header alone: its column types the server's), then the file's rows put in, each declared column by
 * name -- a CSV's as text, a Parquet's as typed -- and DuckDB converting each value to its column's type. The file's
 * name says which kind it is; DuckDB refuses a file that lacks a declared column or holds a value its type cannot take.
 * The file is registered as `<as>.csv` or `<as>.parquet`, never by the person's own name: read_csv and read_parquet read
 * a name as a glob (`sales[2024].csv` matches nothing).
 */
export async function loadFileTable(sink: FileSink, source: SeedSource, model: string, target: DeclaredTable,
  file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<void> {
  const format = /\.csv$/i.test(file.name) ? 'csv' : /\.parquet$/i.test(file.name) ? 'parquet' : undefined;
  if (!format) throw new Error(`${file.name}: a table is filled from a .csv or a .parquet file`);
  const [made] = await seeded(source, model, [{
    database: target.database, schema: target.schema, table: target.table, element: file.name,
    csv: target.columns.map((c) => c.name).join(','),
  }]);
  const registered = `${as}.${format}`;
  await sink.registerFileBuffer(registered, file.bytes);
  const read = format === 'csv' ? `read_csv(${literal(registered)}, header = true, all_varchar = true)` : `read_parquet(${literal(registered)})`;
  for (const sql of made!.statements) await sink.run(sql);
  await sink.run(`INSERT INTO ${ident(target.schema)}.${ident(target.table)} SELECT ${target.columns.map((c) => ident(c.name)).join(', ')} FROM ${read}`);
}

/** A table dropped (a file's rows put back to the model's own). */
export async function dropTable(sink: DataSink, schema: string, table: string): Promise<void> {
  await sink.run(`DROP TABLE IF EXISTS ${ident(schema)}.${ident(table)}`);
}

/** Each table made and filled: its statements, the server's, run in order. */
export async function loadDataTables(sink: DataSink, tables: readonly SeededTable[]): Promise<void> {
  for (const t of tables) {
    for (const sql of t.statements) await sink.run(sql);
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
  /** The tables the last load made, by `schema.table`. */
  #made = new Map<string, { readonly schema: string; readonly table: string }>();
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
    for (const [k, t] of this.#made) {
      if (!now.has(k) && !keep.has(k)) await dropTable(this.#sink, t.schema, t.table);
    }
    this.#made = new Map(filled.map((t) => [tableKey(t.schema, t.table), { schema: t.schema, table: t.table }]));
    await loadDataTables(this.#sink, filled);
    this.#current = key;
  }
}
