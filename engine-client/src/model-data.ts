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

/** One table of test data: where it goes, its columns' SQL types, and its CSV (header first). */
export interface DataTable {
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly csv: string;
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

/** The test data of a model (its JSON's elements): one entry per table of every relational Data element. */
export function dataTables(elements: readonly ModelElement[]): DataTable[] {
  const columns = new Map<string, DataTable['columns']>();
  for (const e of elements) {
    if (e._type !== 'relational') continue;
    for (const s of e.schemas ?? []) {
      for (const t of s.tables) columns.set(`${s.name}.${t.name}`, t.columns.map((c) => ({ name: c.name, type: sqlType(c.type) })));
    }
  }
  const out: DataTable[] = [];
  for (const e of elements) {
    if (e._type !== 'dataElement' || e.data?._type !== 'relationalCSVData') continue;
    for (const t of e.data.tables ?? []) {
      const cols = columns.get(`${t.schema}.${t.table}`);
      if (!cols) throw new Error(`the Data element ${e.package}::${e.name} fills ${t.schema}.${t.table}, which no Database of the model declares`);
      out.push({ schema: t.schema, table: t.table, columns: cols, csv: t.values });
    }
  }
  return out;
}

/** What loading needs of the tab's DuckDB: a file named by text, and SQL. */
export interface DataSink {
  registerFileText(name: string, text: string): Promise<void>;
  run(sql: string): Promise<unknown>;
}

const ident = (s: string): string => `"${s.replace(/"/g, '""')}"`;
const literal = (s: string): string => `'${s.replace(/'/g, "''")}'`;

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
