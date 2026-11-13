// AN OUTPUT INTO A TABLE (docs/DATACUBE_FLOW_MODE_2026_10_24.md M6, Output Data to a table; FL13): a box's rows written
// into a table of its host's own database as ONE PLAN made before anything runs (the final audit's item 2, 2026-11-12,
// the user's ruling (b): Boundary.outputPlan) -- the table made for them first where the Output makes it (`create`,
// `overwrite`: the database's own create of the rows, planned), then the WRITE the planner plans over one model of the
// box's tables and the target (`write`, `delete` then `write`, `merge` by its keys: legend-lite's store writes,
// docs/WRITES_DESIGN_2026_10_18.md L1), in the plan's own transaction, which the host's plan runner runs
// (engine-client/src/plan-runner.ts) and asks the planner nothing. The tables' columns are read off the database's own
// catalog before the plan is asked for. Every value is the database's: the rows it writes, the count it answers. A
// failure anywhere takes back everything, the table made among it.
//
// One writer for the tab (app.ts writeOutput) and Python's flow.run (headless gridWriteTable). PURE: no DOM. A host is
// asked two things, each INTENT (the #6 bridge, 2026-11-12, met by the one plan in the DataCube stream's merge,
// 2026-11-13): a table's columns as its catalog says them, and the Output's request, which the host plans as one plan
// and runs (heldTableOutput, the tab's; headless/host.ts hostTableOutput, Python's) -- never a statement or a plan of
// DataCube's.

import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import { runPlan } from '../../engine-client/src/plan-runner.ts';
import { toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import type { CatalogColumn, ModelTable, OutputRequest, TableModels } from './infer.ts';
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
  /** A table's columns as the host's catalog says them now (none: no such table). */
  catalog(schema: string, table: string): Promise<CatalogColumn[]>;
  /** The Output's request planned by the host as ONE plan (Boundary.outputPlan) and run by its runner in the plan's own
   *  transaction: its answer, lite's JSON rows (the count the database wrote). */
  output(request: OutputRequest): Promise<string>;
}

/**
 * THE BOX'S ROWS WRITTEN INTO ITS TABLE (`rows`: its rows as it hands them on, flow-output.ts outputQuery's query): the
 * count of rows the database wrote. Refused in words: a table of the page's own; the database's own words for the rest
 * (a table that exists already for a create, none for an append, a column of the box's the table has not).
 */
export async function writeTable(rows: Lambda, output: TableOutput, host: TableWriteHost): Promise<number> {
  if (host.held(output.name)) {
    throw new CubeRefusal(`'${output.name}' is one of this page's own tables (a file's, a copy's): name the output's table another way`);
  }
  const makes = output.mode === 'create' || output.mode === 'overwrite';
  // the box's tables and, where it is there already, the target, as the catalog answers them now: read BEFORE the plan
  // is asked for, which then runs whole (the orchestrator's ruling, 2026-11-12: the sources' model is read before the run)
  const tables: ModelTable[] = [];
  for (const s of [...new Set(host.sources.map((x) => x.table))]) {
    const given = host.sources.find((x) => x.table === s)!;
    tables.push({ table: s, convertible: given.convertible, ...(given.pkg !== undefined ? { pkg: given.pkg } : {}),
      columns: await host.catalog(SCHEMA, s) });
  }
  const target = makes ? [] : await host.catalog(SCHEMA, output.name);
  const request: OutputRequest = {
    databaseType: host.databaseType,
    tables,
    output: { name: output.name, mode: output.mode, ...(output.mode === 'merge' ? { keys: output.keys ?? [] } : {}) },
    rows: toJson(rows),
    // a table there already is named by its columns; none, the planner refuses the write by name
    ...(target.length > 0 ? { target } : {}),
  };
  return countOf(await host.output(request));
}

/**
 * A TABLE OUTPUT ON A DATABASE RUN IN THIS PROCESS (the tab's): the catalog's question and the Output's plan its
 * planner's (`models`), each run on the database held for it alone (duckdb.ts exclusive) -- the plan whole, in its own
 * transaction (runPlan).
 */
export function heldTableOutput(
  database: { readonly databaseType: string; exclusive<T>(work: (held: HeldConnection) => Promise<T>): Promise<T> },
  models: Pick<TableModels, 'catalogColumnsSql' | 'outputPlan'>,
): Pick<TableWriteHost, 'catalog' | 'output'> {
  return {
    catalog: (schema, table) => database.exclusive((held) =>
      catalogColumnsOf({ run: (sql: PlannedSql) => held.table(sql) }, models, schema, table)),
    output: async (request) => {
      const plan = await models.outputPlan(request);
      return database.exclusive((held) => runPlan(plan, { databaseType: database.databaseType, run: held.rows }));
    },
  };
}

/** The count a write's plan answers: its relation's one row (`[{"value": n}]`, as the database wrote it). */
function countOf(answer: string): number {
  const read = JSON.parse(answer) as unknown;
  const n = Array.isArray(read) && read.length === 1 ? (read[0] as { value?: unknown }).value : undefined;
  if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new Error(`a write answered no count: ${answer}`);
  return n;
}
