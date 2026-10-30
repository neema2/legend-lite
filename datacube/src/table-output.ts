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
// its database, held for the write, and its planner.

import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import { hostSession, runPlan } from '../../engine-client/src/plan-runner.ts';
import { colSpecs, fn, lambda, lit, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
import type { ModelTable, TableModels } from './infer.ts';
import type { TableOutput } from './layout/flow.ts';
import { CubeRefusal } from './snapshot.ts';
import { catalogColumnsOf } from './upload.ts';

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
  readonly models: Pick<TableModels, 'tablesModel' | 'catalogColumnsSql' | 'hostOperations'>;
  /** The host's database held for the write alone (duckdb.ts exclusive). */
  hold<T>(work: (held: HeldConnection) => Promise<T>): Promise<T>;
  /** A query over the box's own model planned: its SQL (the create's columns, its rows none). */
  sqlOf(query: Lambda): Promise<string>;
  /** A program planned over `model` as an execution plan (PlanJson's text). */
  executionPlan(model: string, runtime: string, program: Lambda): Promise<string>;
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
  // the create's statement asked first: its columns the database's, as the box's rows type them, none of its rows
  const made = output.mode === 'create' || output.mode === 'overwrite'
    ? await host.models.hostOperations(host.databaseType, [{
      op: 'relation', kind: 'table', target: { name: output.name }, replace: output.mode === 'overwrite',
      from: { query: await host.sqlOf(lambda([], fn('limit', body, lit.integer(0)))) },
    }]) : [];
  return host.hold(async (held) => {
    const session = await hostSession(host.databaseType, held.rows, host.models);
    await session.transaction('begin');
    try {
      for (const statement of made.flat()) await held.rows(statement, []);
      // one model of its tables and the target, each read off the catalog as it is now (the target made just now, in
      // the same transaction)
      const engine = { run: (sql: string) => held.table(sql) };
      const tables: ModelTable[] = [];
      for (const s of [...new Set(host.sources.map((x) => x.table))]) {
        const given = host.sources.find((x) => x.table === s)!;
        tables.push({ table: s, convertible: given.convertible, ...(given.pkg !== undefined ? { pkg: given.pkg } : {}),
          columns: await catalogColumnsOf(engine, host.models, SCHEMA, s) });
      }
      const target = await catalogColumnsOf(engine, host.models, SCHEMA, output.name);
      if (target.length === 0) throw new CubeRefusal(`this page's database has no table '${output.name}': write it with Create a new table first`);
      tables.push({ table: output.name, convertible: false, columns: target });
      const model = await host.models.tablesModel(tables, host.databaseType);
      const into = model.tables.at(-1)!.source;
      const plan = await host.executionPlan(model.model, model.runtime, lambda([], ...program(body, into, output)));
      const answer = await runPlan(plan, session, { joined: true });
      await session.transaction('commit');
      return countOf(answer);
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
