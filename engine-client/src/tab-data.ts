// Where each table's rows come from, in this tab (plan A2): every table a model's Databases declare, filled from the
// model's own test data (its Data elements, model-data.ts) or from a file the person dropped in for it (CSV or
// Parquet) -- which stays, whatever the model's test data says, until it is reset -- or from nowhere yet.

import type { QueryEngine } from './engine.ts';
import { databaseOf, databaseTables, dataTables, loadFileTable, seeded, TestData, type FileSink, type SeededTable, type SeedSource } from './model-data.ts';

type Elements = Parameters<typeof dataTables>[0];

/** A model: its text (what the planner compiles) and its JSON's elements (what this file reads). */
export interface Model {
  readonly text: string;
  readonly elements: Elements;
}

/**
 * The tab's engine, for queries over one version of a project (`key`): each query it runs reads that version's test
 * data, made the tab's rows in the same turn (TestData.with) -- whatever version another query on the page opened
 * since. The engine itself is the tab's, shared: closing this closes it, as closing the engine would.
 */
export function withTestData(engine: QueryEngine, data: TestData, key: string, tables: readonly SeededTable[]): QueryEngine {
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
  readonly columns: readonly { readonly name: string }[];
  readonly source: TableSource;
  /** How many rows it has; undefined when there is no table yet. */
  readonly rows: number | undefined;
}

const key = (schema: string, table: string): string => `${schema}.${table}`;

export class TabTables {
  readonly #sink: FileSink;
  readonly #engine: QueryEngine;
  readonly #tests: TestData;
  /** The tables a person's file fills, by `schema.table`: its file's name, and the planner's statement that drops it. */
  readonly #files = new Map<string, { readonly name: string; readonly drop: string }>();
  #uploads = 0;

  readonly #source: SeedSource;

  /** `source`: where the server's statements for the model's test data come from (the planner in this tab). */
  constructor(sink: FileSink, engine: QueryEngine, source: SeedSource) {
    this.#sink = sink;
    this.#engine = engine;
    this.#source = source;
    this.#tests = new TestData(sink);
  }

  /**
   * The model's test data loaded, each table made afresh, and a table the model's test data no longer fills (its Data
   * element deleted or renamed) dropped -- but a table a person's file fills keeps that file's rows. Loaded each time:
   * the workspace's model may have changed since.
   */
  async load(model: Model): Promise<void> {
    const keep = new Set(this.#files.keys());
    const tables = dataTables(model.elements).filter((t) => !keep.has(key(t.schema, t.table)));
    await this.#tests.use(undefined, await seeded(this.#source, model.text, tables), keep);
  }

  /**
   * `schema.table` filled from a person's file (a .csv or .parquet), until reset -- the table the one Database that
   * declares it makes (databaseOf: none, or two, is refused). A refused file leaves the table as it was.
   */
  async put(model: Model, schema: string, table: string, file: { readonly name: string; readonly bytes: Uint8Array }): Promise<void> {
    const database = databaseOf(model.elements, schema, table, 'a file fills');
    const target = databaseTables(model.elements).find((t) => t.database === database && t.schema === schema && t.table === table)!;
    const seed = await loadFileTable(this.#sink, this.#source, model.text, target, file, `upload-${++this.#uploads}`);
    this.#files.set(key(schema, table), { name: file.name, drop: seed.drop });
  }

  /** `schema.table` back to the model's own rows (none: no table). */
  async reset(model: Model, schema: string, table: string): Promise<void> {
    const file = this.#files.get(key(schema, table));
    this.#files.delete(key(schema, table));
    if (file) await this.#sink.run(file.drop);
    await this.load(model);
  }

  /** Every table the model declares: where its rows are from, and how many there are (counted by the planner's name). */
  async tables(model: Model): Promise<TableState[]> {
    const declared = databaseTables(model.elements);
    const tests = new Map(dataTables(model.elements).map((t) => [key(t.schema, t.table), t.element]));
    const named = await seeded(this.#source, model.text, declared.map((t) => ({
      database: t.database, schema: t.schema, table: t.table, csv: t.columns.map((c) => c.name).join(','), element: '',
    })));
    const out: TableState[] = [];
    for (const [i, t] of declared.entries()) {
      const file = this.#files.get(key(t.schema, t.table));
      const element = tests.get(key(t.schema, t.table));
      const source: TableSource = file !== undefined ? { kind: 'file', name: file.name } : element !== undefined ? { kind: 'test', element } : { kind: 'none' };
      let rows: number | undefined;
      try {
        rows = Number((await this.#engine.run(`SELECT COUNT(*) AS n FROM ${named[i]!.seed.table}`, 0)).columns[0]!.values[0]);
      } catch {
        rows = undefined;     // not made yet: nothing fills it
      }
      out.push({ ...t, source, rows });
    }
    return out;
  }
}
