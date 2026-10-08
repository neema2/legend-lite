// Where each table's rows come from, in this tab (plan A2): every table a model's Databases declare, filled from the
// model's own test data (its Data elements, model-data.ts) or from a file the person dropped in for it (CSV or
// Parquet) -- which stays, whatever the model's test data says, until it is reset -- or from nowhere yet.

import type { QueryEngine } from './engine.ts';
import { databaseTables, dataTables, dropTable, loadFileTable, TestData, type DataTable, type FileSink } from './model-data.ts';

type Elements = Parameters<typeof dataTables>[0];

/**
 * The tab's engine, for queries over one version of a project (`key`): each query it runs reads that version's test
 * data, made the tab's rows in the same turn (TestData.with) -- whatever version another query on the page opened
 * since. The engine itself is the tab's, shared: closing this closes it, as closing the engine would.
 */
export function withTestData(engine: QueryEngine, data: TestData, key: string, tables: readonly DataTable[]): QueryEngine {
  return {
    name: engine.name,
    execute: (plan, epoch, signal) => data.with(key, tables, () => engine.execute(plan, epoch, signal)),
    stream: (plan, epoch, onChunk, signal) => data.with(key, tables, () => engine.stream(plan, epoch, onChunk, signal)),
    run: (sql, epoch, signal) => data.with(key, tables, () => engine.run(sql, epoch, signal)),
    close: () => engine.close(),
  };
}

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
  readonly #tests: TestData;
  /** The tables a person's file fills: its file's name, by `schema.table`. */
  readonly #files = new Map<string, string>();
  #uploads = 0;

  constructor(sink: FileSink, engine: QueryEngine) {
    this.#sink = sink;
    this.#engine = engine;
    this.#tests = new TestData(sink);
  }

  /**
   * The model's test data loaded, each table made afresh, and a table the model's test data no longer fills (its Data
   * element deleted or renamed) dropped -- but a table a person's file fills keeps that file's rows. Loaded each time:
   * the workspace's model may have changed since.
   */
  async load(elements: Elements): Promise<void> {
    await this.#tests.use(undefined, dataTables(elements), new Set(this.#files.keys()));
  }

  /** `schema.table` filled from a person's file (a .csv or .parquet), until reset. */
  async put(elements: Elements, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void> {
    const target = databaseTables(elements).find((t) => t.schema === schema && t.table === table);
    if (!target) throw new Error(`no Database of the model declares ${schema}.${table}`);
    await loadFileTable(this.#sink, target, file, `upload-${++this.#uploads}`);
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
