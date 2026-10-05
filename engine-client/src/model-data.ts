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
 * A relational column type as SQL: its name and size (`Varchar` 200 -> VARCHAR(200), `Decimal` 10,2 -> DECIMAL(10,2)).
 * No table of types here (DataCube's guardrail: one reader decides by a type's name): the store's types are SQL's own
 * names, and DuckDB accepts or refuses each one where the table is made -- never a guess.
 */
function sqlType(t: { readonly _type: string; readonly size?: number; readonly precision?: number; readonly scale?: number }): string {
  const name = t._type.toUpperCase();
  if (t.size !== undefined) return `${name}(${t.size})`;
  if (t.precision !== undefined) return `${name}(${t.precision},${t.scale ?? 0})`;
  return name;
}

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
 * that lacks a column or holds a value its type cannot take.
 */
export async function loadFileTable(sink: FileSink, target: DeclaredTable, file: { readonly name: string; readonly bytes: Uint8Array }, as: string): Promise<void> {
  const format = /\.csv$/i.test(file.name) ? 'csv' : /\.parquet$/i.test(file.name) ? 'parquet' : undefined;
  if (!format) throw new Error(`${file.name}: a table is filled from a .csv or a .parquet file`);
  await sink.registerFileBuffer(as, file.bytes);
  const select = format === 'csv'
    ? `SELECT * FROM read_csv(${literal(as)}, header = true, columns = {${target.columns.map((c) => `${literal(c.name)}: ${literal(c.type)}`).join(', ')}})`
    : `SELECT ${target.columns.map((c) => `CAST(${ident(c.name)} AS ${c.type}) AS ${ident(c.name)}`).join(', ')} FROM read_parquet(${literal(as)})`;
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
    const file = `model-data-${i}-${t.schema}-${t.table}.csv`;
    await sink.registerFileText(file, t.csv);
    const types = t.columns.map((c) => `${literal(c.name)}: ${literal(c.type)}`).join(', ');
    await sink.run(`CREATE SCHEMA IF NOT EXISTS ${ident(t.schema)}`);
    await sink.run(`CREATE OR REPLACE TABLE ${ident(t.schema)}.${ident(t.table)} AS SELECT * FROM read_csv(${literal(file)}, header = true, columns = {${types}})`);
  }
}
