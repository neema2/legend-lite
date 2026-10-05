// Where each table's rows come from, in this tab (plan A2): every table a model's Databases declare, filled from the
// model's own test data (its Data elements, model-data.ts) or from a file the person dropped in for it (CSV or
// Parquet) -- which stays, whatever the model's test data says, until it is reset -- or from nowhere yet.

import type { QueryEngine } from './engine.ts';
import { databaseTables, dataTables, dropTable, loadDataTables, loadFileTable, type FileSink } from './model-data.ts';

type Elements = Parameters<typeof dataTables>[0];

/** Where a table's rows are from: a Data element of the model, a person's file, or nothing yet. */
export type TableSource =
  | { readonly kind: 'test'; readonly element: string }
  | { readonly kind: 'file'; readonly name: string }
  | { readonly kind: 'none' };

export interface TableState {
  readonly database: string;
  readonly schema: string;
  readonly table: string;
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly source: TableSource;
  /** How many rows it has; undefined when there is no table yet. */
  readonly rows: number | undefined;
}

const key = (schema: string, table: string): string => `${schema}.${table}`;
const ident = (s: string): string => `"${s.replace(/"/g, '""')}"`;

export class TabTables {
  readonly #sink: FileSink;
  readonly #engine: QueryEngine;
  /** The tables a person's file fills: its file's name, by `schema.table`. */
  readonly #files = new Map<string, string>();
  #uploads = 0;

  constructor(sink: FileSink, engine: QueryEngine) {
    this.#sink = sink;
    this.#engine = engine;
  }

  /** The model's test data loaded, each table made afresh -- but a table a person's file fills keeps that file's rows. */
  async load(elements: Elements): Promise<void> {
    await loadDataTables(this.#sink, dataTables(elements).filter((t) => !this.#files.has(key(t.schema, t.table))));
  }

  /** `schema.table` filled from a person's file (a .csv or .parquet), until reset. */
  async put(elements: Elements, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void> {
    const target = databaseTables(elements).find((t) => t.schema === schema && t.table === table);
    if (!target) throw new Error(`no Database of the model declares ${schema}.${table}`);
    await loadFileTable(this.#sink, target, file, `upload-${++this.#uploads}-${file.name}`);
    this.#files.set(key(schema, table), file.name);
  }

  /** `schema.table` back to the model's own rows (none: no table). */
  async reset(elements: Elements, schema: string, table: string): Promise<void> {
    this.#files.delete(key(schema, table));
    await dropTable(this.#sink, schema, table);
    await this.load(elements);
  }

  /** Every table the model declares: where its rows are from, and how many there are. */
  async tables(elements: Elements): Promise<TableState[]> {
    const tests = new Map(dataTables(elements).map((t) => [key(t.schema, t.table), t.element]));
    const out: TableState[] = [];
    for (const t of databaseTables(elements)) {
      const file = this.#files.get(key(t.schema, t.table));
      const element = tests.get(key(t.schema, t.table));
      const source: TableSource = file !== undefined ? { kind: 'file', name: file } : element !== undefined ? { kind: 'test', element } : { kind: 'none' };
      let rows: number | undefined;
      try {
        rows = Number((await this.#engine.run(`SELECT COUNT(*) AS n FROM ${ident(t.schema)}.${ident(t.table)}`, 0)).columns[0]!.values[0]);
      } catch {
        rows = undefined;     // not made yet: nothing fills it
      }
      out.push({ ...t, source, rows });
    }
    return out;
  }
}
