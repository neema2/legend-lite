// AN OUTPUT INTO A TABLE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, Output Data to a table; FL13): a box's rows written
// into a table of its host's own database, in one transaction -- the table made for them first where the Output makes
// it (`create`, `overwrite`: the database's own CREATE, as a Snap makes its copy -- the host's operation, D5), then a
// WRITE THE PLANNER PLANS over one model of the box's tables and the target (`write`, `delete` then `write`, `merge` by
// its keys: legend-lite's store writes, docs/WRITES_DESIGN_2026_10_18.md L1) and the host's plan runner runs
// (engine-client/src/plan-runner.ts) -- the target's model read off the database's own catalog, so its columns are the
// database's. Every value is the database's: the rows it writes, the count it answers. A failure anywhere takes back
// everything, the table made among it.
//
// One writer for the tab (app.ts writeOutput) and Python's flow.run (headless gridWriteTable). PURE: no DOM; a host gives
// the write's transaction, each of its steps the host's to plan and run (heldTableWriting, the tab's; headless/host.ts
// hostTableWriting, Python's: the #6 bridge, 2026-11-12).

import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import { hostSession, runPlan } from '../../engine-client/src/plan-runner.ts';
import { colSpecs, fn, lambda, lit, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { CatalogColumn, ModelTable, TableModels } from './infer.ts';
import type { Plan } from '../../engine-client/src/relation-type.ts';
import type { TableOutput } from './layout/flow.ts';
import { CubeRefusal } from './snapshot.ts';
import { catalogColumnsOf } from './upload.ts';
import type { PlannedSql } from '../../engine-client/src/planned-sql.ts';

/** The schema a host's own tables are in (a page's files, a snap's copies): DuckDB's own. */
const SCHEMA = 'main';

/** What writing a box's rows into a table asks of its host. */
export interface TableWriteHost {
  readonly databaseType: string;
  /** The box's tables of the host's database, each as its model declares it (join-open.ts oneModel's `convertible`; its
   *  package, where its model is its own: a Python frame's). */
  readonly sources: readonly { readonly table: string; readonly convertible: boolean; readonly pkg?: string }[];
  /** Tables of the host's database no Output may write: the page's own (its files, its copies). */
  readonly held: (table: string) => boolean;
  /** legend-lite's model writer: the one model of the box's tables and the target, from what the catalog says. */
  readonly models: Pick<TableModels, 'tablesModel'>;
  /** ONE TRANSACTION on the host's database, begun before `work` and committed after it -- rolled back, all of it, the
   *  table made among it, when anything in it fails: each of its steps the host's to plan and run (the #6 bridge,
   *  2026-11-12: DataCube's code decides the steps, never hands a host their statements). */
  writing<T>(work: (w: TableWriting) => Promise<T>): Promise<T>;
}

/** The steps of a write, each planned and run by the host in the write's transaction. */
export interface TableWriting {
  /** `table` made for the box's rows (`rows`: their query, with no row of them), its columns as the database types
   *  them -- replacing one of its name (`replace`), or not. */
  create(table: string, replace: boolean, rows: Lambda): Promise<void>;
  /** A table's columns as the catalog says them now, in this transaction (a table made in it among them). */
  catalog(schema: string, table: string): Promise<CatalogColumn[]>;
  /** `program` planned over `model` through `runtime` and run in this transaction, joining it: its answer, lite's JSON
   *  rows (the count the database wrote). */
  run(model: string, runtime: string, program: Lambda): Promise<string>;
}

/**
 * THE BOX'S ROWS WRITTEN INTO ITS TABLE (`rows`: its rows as it hands them on, flow-output.ts outputQuery's query): the
 * count of rows the database wrote. Refused in words: a table of the page's own, a box that reads none of the host's
 * tables; the database's own words for the rest (a table that exists already for a create, none for an append, a column
 * of the box's the table has not).
 */
export async function writeTable(rows: Lambda, output: TableOutput, host: TableWriteHost): Promise<number> {
  if (host.sources.length === 0) throw new CubeRefusal("its rows are read from none of this page's database's tables: a table output writes its own database");
  if (host.held(output.name) || host.sources.some((s) => s.table.toLowerCase() === output.name.toLowerCase())) {
    throw new CubeRefusal(`'${output.name}' is one of this page's own tables (a file's, a copy's): name the output's table another way`);
  }
  const body = rows.body[0];
  if (rows.parameters.length > 0 || rows.body.length !== 1 || body === undefined) throw new Error("an output's rows are one query of no parameter");
  return host.writing(async (w) => {
    // the table made first: its columns the database's, as the box's rows type them, none of its rows
    if (output.mode === 'create' || output.mode === 'overwrite') {
      await w.create(output.name, output.mode === 'overwrite', lambda([], fn('limit', body, lit.integer(0))));
    }
    // one model of its tables and the target, each read off the catalog as it is now (the target made just now, in
    // the same transaction)
    const tables: ModelTable[] = [];
    for (const s of [...new Set(host.sources.map((x) => x.table))]) {
      const given = host.sources.find((x) => x.table === s)!;
      tables.push({ table: s, convertible: given.convertible, ...(given.pkg !== undefined ? { pkg: given.pkg } : {}),
        columns: await w.catalog(SCHEMA, s) });
    }
    const target = await w.catalog(SCHEMA, output.name);
    if (target.length === 0) throw new CubeRefusal(`this page's database has no table '${output.name}': write it with Create a new table first`);
    tables.push({ table: output.name, convertible: false, columns: target });
    const model = await host.models.tablesModel(tables, host.databaseType);
    const into = model.tables.at(-1)!.source;
    return countOf(await w.run(model.model, model.runtime, lambda([], ...program(body, into, output))));
  });
}

/**
 * A WRITE ON A DATABASE RUN IN THIS PROCESS (the tab's): its connection held for the whole write (duckdb.ts exclusive),
 * the transaction's statements the planner's (hostSession); the table made from the box's rows' plan and the planner's
 * relation operation, the query's own values (a data: payload: M3) bound to the one statement that reads it; the program
 * planned (`executionPlan`) and run by the hosts' runner joining the transaction (runPlan).
 */
export function heldTableWriting(o: {
  readonly databaseType: string;
  readonly models: Pick<TableModels, 'catalogColumnsSql' | 'hostOperations'>;
  /** The host's database held for the write alone (duckdb.ts exclusive). */
  hold<T>(work: (held: HeldConnection) => Promise<T>): Promise<T>;
  /** A query over the box's own model planned: its SQL and the values it binds (the create's columns, its rows none). */
  planOf(query: Lambda): Promise<Plan>;
  /** A program planned over `model` as an execution plan (PlanJson's text). */
  executionPlan(model: string, runtime: string, program: Lambda): Promise<string>;
}): TableWriteHost['writing'] {
  return (work) => o.hold(async (held) => {
    const session = await hostSession(o.databaseType, held.rows, o.models);
    await session.transaction('begin');
    try {
      const engine = { run: (sql: PlannedSql) => held.table(sql) };
      const done = await work({
        create: async (table, replace, rows) => {
          const shape = await o.planOf(rows);
          const made = (await o.models.hostOperations(o.databaseType, [{
            op: 'relation', kind: 'table', target: { name: table }, replace, from: { query: shape.sql },
          }])).flat();
          const params = shape.params ?? [];
          // the values bind the one statement that reads the query; a create of several statements binds none of them
          if (params.length > 0 && made.length !== 1) {
            throw new CubeRefusal(`'${table}' is made by ${made.length} statements, and its rows' query binds a data: payload one of them reads: not written here`);
          }
          for (const statement of made) await held.rows(statement, params);
        },
        catalog: (schema, table) => catalogColumnsOf(engine, o.models, schema, table),
        run: async (model, runtime, program) => runPlan(await o.executionPlan(model, runtime, program), session, { joined: true }),
      });
      await session.transaction('commit');
      return done;
    } catch (e) {
      await session.transaction('rollback').catch(() => {});
      throw e;
    }
  });
}

/** The Pure program that writes the rows (`rows`) into its table (`into`): its statements in order. */
function program(rows: ValueSpecification, into: ValueSpecification, output: TableOutput): ValueSpecification[] {
  switch (output.mode) {
    case 'create':
    case 'overwrite':
    case 'append': return [fn('write', rows, into)];
    // every row of the table deleted, then the box's written: one change
    case 'deleteAppend': return [fn('delete', into, lambda(['r'], lit.boolean(true))), fn('write', rows, into)];
    case 'merge': return [fn('merge', rows, into, colSpecs(output.keys ?? []))];
  }
}

/** The count a write's plan answers: its relation's one row (`[{"value": n}]`, as the database wrote it). */
function countOf(answer: string): number {
  const read = JSON.parse(answer) as unknown;
  const n = Array.isArray(read) && read.length === 1 ? (read[0] as { value?: unknown }).value : undefined;
  if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new Error(`a write answered no count: ${answer}`);
  return n;
}
