// A model's own test data, in the tab's DuckDB (plan A2, the first source of rows): every relational Data element's
// tables (`relationalCSVData`: schema, table, CSV text) made tables named as the model's Database declares them, typed
// as its columns are, so a query planned on the model reads them. DuckDB parses the CSV (read_csv with the declared
// types); this file only says where each table goes and what its columns are.

/** The model JSON parts read here (grammarToJson/model). */
interface ModelElement {
  readonly _type: string;
  readonly name?: string;
  readonly package?: string;
  readonly schemas?: readonly {
    readonly name: string;
    readonly tables: readonly { readonly name: string; readonly columns: readonly { readonly name: string; readonly type: { readonly _type: string; readonly size?: number; readonly precision?: number; readonly scale?: number } }[] }[];
  }[];
  readonly data?: { readonly _type: string; readonly tables?: readonly { readonly schema: string; readonly table: string; readonly values: string }[] };
}

/** A table a model's Database declares: where it is, and its columns' SQL types. */
export interface DeclaredTable {
  /** The Database's path. */
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
}

/** One table of test data: where it goes, its columns' SQL types, its CSV (header first), and the Data element's path. */
export interface DataTable {
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly csv: string;
  readonly element: string;
}

/**
 * A relational column type as DuckDB's DDL spells it -- the server's spelling, so a table made here holds what the same
 * table on the server holds: `Float` is DOUBLE (DuckDB's FLOAT is single precision), `Bit` is BOOLEAN (DuckDB's BIT is a
 * bit string), `SemiStructured` and `Json` are JSON; a sized or scaled type keeps its size (`Varchar` 200 ->
 * VARCHAR(200), `Decimal` 10,2 -> DECIMAL(10, 2)); `Distinct` and `Other` have no DDL, and a kind the server's model does
 * not know is refused, in the server's words.
 *
 * A COPY, for now (docs/STUDIO_FULL_PLAN_2026_10_04.md, "The tab's table types"): the server's rule is
 * FromProtocol.dataType, StoreCompiler.declaredType, DuckDb.ddlType and DdlSpelling.h2Type, in core. It goes when the
 * planner hands the tab the server's own seed statements (the plan/exec split's step 2 moves them to the plan side);
 * test/model-data.test.ts pins every kind's spelling until then.
 */
export function sqlType(t: { readonly _type: string; readonly size?: number; readonly precision?: number; readonly scale?: number }): string {
  const plain = PLAIN_TYPES.get(t._type);
  if (plain !== undefined) return plain;
  if (SIZED_TYPES.has(t._type)) return t.size === undefined ? t._type.toUpperCase() : `${t._type.toUpperCase()}(${t.size})`;
  if (SCALED_TYPES.has(t._type)) return `${t._type.toUpperCase()}(${t.precision ?? 0}, ${t.scale ?? 0})`;
  if (t._type === 'Distinct' || t._type === 'Other') throw new Error(`no DDL spelling for declared column type ${t._type.toUpperCase()}`);
  throw new Error(`no model data type for protocol kind '${t._type}'`);
}

const PLAIN_TYPES: ReadonlyMap<string, string> = new Map(Object.entries({
  BigInt: 'BIGINT', SmallInt: 'SMALLINT', TinyInt: 'TINYINT', Integer: 'INTEGER', Float: 'DOUBLE', Double: 'DOUBLE',
  Real: 'REAL', Bit: 'BOOLEAN', Timestamp: 'TIMESTAMP', Date: 'DATE', SemiStructured: 'JSON', Json: 'JSON',
}));
const SIZED_TYPES: ReadonlySet<string> = new Set(['Varchar', 'Char', 'Binary', 'Varbinary']);
const SCALED_TYPES: ReadonlySet<string> = new Set(['Decimal', 'Numeric']);

/** Every table the model's Databases declare (its JSON's elements), in the order declared. */
export function databaseTables(elements: readonly ModelElement[]): DeclaredTable[] {
  const out: DeclaredTable[] = [];
  for (const e of elements) {
    if (e._type !== 'relational') continue;
    for (const s of e.schemas ?? []) {
      for (const t of s.tables) {
        out.push({ database: `${e.package}::${e.name}`, schema: s.name, table: t.name, columns: t.columns.map((c) => ({ name: c.name, type: sqlType(c.type) })) });
      }
    }
  }
  return out;
}

/** The test data of a model (its JSON's elements): one entry per table of every relational Data element. */
export function dataTables(elements: readonly ModelElement[]): DataTable[] {
  const columns = new Map(databaseTables(elements).map((t) => [`${t.schema}.${t.table}`, t.columns]));
  const out: DataTable[] = [];
  for (const e of elements) {
    if (e._type !== 'dataElement' || e.data?._type !== 'relationalCSVData') continue;
    for (const t of e.data.tables ?? []) {
      const cols = columns.get(`${t.schema}.${t.table}`);
      if (!cols) throw new Error(`the Data element ${e.package}::${e.name} fills ${t.schema}.${t.table}, which no Database of the model declares`);
      out.push({ schema: t.schema, table: t.table, columns: cols, csv: t.values, element: `${e.package}::${e.name}` });
    }
  }
  return out;
}

/** What loading needs of the tab's DuckDB: a file named by text, and SQL. */
export interface DataSink {
  registerFileText(name: string, text: string): Promise<void>;
  run(sql: string): Promise<unknown>;
}

/** A sink that also takes a file's bytes: a person's own file (Parquet is binary). */
export interface FileSink extends DataSink {
  registerFileBuffer(name: string, bytes: Uint8Array): Promise<void>;
}

const ident = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const literal = (s: string): string => `'${s.replace(/'/g, "''")}'`;

/**
 * A table made from a person's own file (plan A2's second source): CSV (a header row, read with the declared types) or
 * Parquet (each declared column, by name, cast to its declared type). The file's name says which; DuckDB refuses a file
 * that lacks a column or holds a value its type cannot take. The file is registered as `<as>.csv` or `<as>.parquet`,
 * never by the person's own name: read_csv and read_parquet read a name as a glob (`sales[2024].csv` matches nothing).
 */
export async function loadFileTable(sink: FileSink, target: DeclaredTable, file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<void> {
  const format = /\.csv$/i.test(file.name) ? 'csv' : /\.parquet$/i.test(file.name) ? 'parquet' : undefined;
  if (!format) throw new Error(`${file.name}: a table is filled from a .csv or a .parquet file`);
  const registered = `${as}.${format}`;
  await sink.registerFileBuffer(registered, file.bytes);
  const select = format === 'csv'
    ? `SELECT * FROM read_csv(${literal(registered)}, header = true, columns = {${target.columns.map((c) => `${literal(c.name)}: ${literal(c.type)}`).join(', ')}})`
    : `SELECT ${target.columns.map((c) => `CAST(${ident(c.name)} AS ${c.type}) AS ${ident(c.name)}`).join(', ')} FROM read_parquet(${literal(registered)})`;
  await sink.run(`CREATE SCHEMA IF NOT EXISTS ${ident(target.schema)}`);
  await sink.run(`CREATE OR REPLACE TABLE ${ident(target.schema)}.${ident(target.table)} AS ${select}`);
}

/** A table dropped (a file's rows put back to the model's own). */
export async function dropTable(sink: DataSink, schema: string, table: string): Promise<void> {
  await sink.run(`DROP TABLE IF EXISTS ${ident(schema)}.${ident(table)}`);
}

/** Each table created (or replaced) and filled from its CSV, which DuckDB reads with the declared column types. */
export async function loadDataTables(sink: DataSink, tables: readonly DataTable[]): Promise<void> {
  for (const [i, t] of tables.entries()) {
    const file = `model-data-${i}.csv`;
    await sink.registerFileText(file, t.csv);
    const types = t.columns.map((c) => `${literal(c.name)}: ${literal(c.type)}`).join(', ');
    await sink.run(`CREATE SCHEMA IF NOT EXISTS ${ident(t.schema)}`);
    await sink.run(`CREATE OR REPLACE TABLE ${ident(t.schema)}.${ident(t.table)} AS SELECT * FROM read_csv(${literal(file)}, header = true, columns = {${types}})`);
  }
}

const tableKey = (schema: string, table: string): string => `${schema}.${table}`;

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

  use(key: string | undefined, tables: readonly DataTable[], keep: ReadonlySet<string> = new Set()): Promise<void> {
    return this.with(key, tables, () => Promise.resolve(), keep);
  }

  /** `tables` made the rows there (as `use`), then `query` run, before any other call's turn. */
  with<T>(key: string | undefined, tables: readonly DataTable[], query: () => Promise<T>, keep: ReadonlySet<string> = new Set()): Promise<T> {
    const next = this.#queue.then(async () => {
      await this.#make(key, tables, keep);
      return query();
    });
    this.#queue = next.catch(() => undefined);
    return next;
  }

  async #make(key: string | undefined, tables: readonly DataTable[], keep: ReadonlySet<string>): Promise<void> {
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
