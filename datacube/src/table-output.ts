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
// One writer for the tab (app.ts writeOutput) and Python's flow.run (headless gridWriteTable). PURE: no DOM; a host gives
// its database, held for the write, and its planner.

import type { HeldConnection } from '../../engine-client/src/duckdb.ts';
import { runPlan } from '../../engine-client/src/plan-runner.ts';
import { toJson, type Lambda } from '../../pure-protocol/src/index.ts';
import type { ModelTable, OutputRequest, TableModels } from './infer.ts';
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
  /** The catalog's question, and the Output's plan (Boundary.outputPlan). */
  readonly models: Pick<TableModels, 'catalogColumnsSql' | 'outputPlan'>;
  /** The host's database held for the write alone (duckdb.ts exclusive). */
  hold<T>(work: (held: HeldConnection) => Promise<T>): Promise<T>;
}

/**
 * THE BOX'S ROWS WRITTEN INTO ITS TABLE (`rows`: its rows as it hands them on, flow-output.ts outputQuery's query): the
 * count of rows the database wrote. Refused in words: a table of the page's own, a box that reads none of the host's
 * tables; the database's own words for the rest (a table that exists already for a create, none for an append, a column
 * of the box's the table has not).
 */
export async function writeTable(rows: Lambda, output: TableOutput, host: TableWriteHost): Promise<number> {
  if (host.held(output.name)) {
    throw new CubeRefusal(`'${output.name}' is one of this page's own tables (a file's, a copy's): name the output's table another way`);
  }
  const makes = output.mode === 'create' || output.mode === 'overwrite';
  // the box's tables and, where it is there already, the target, as the catalog answers them now: read BEFORE the plan
  // is asked for, which then runs whole (the orchestrator's ruling, 2026-11-12: the sources' model is read before the run)
  const { tables, target } = await host.hold(async (held) => {
    const engine = { run: (sql: PlannedSql) => held.table(sql) };
    const read: ModelTable[] = [];
    for (const s of [...new Set(host.sources.map((x) => x.table))]) {
      const given = host.sources.find((x) => x.table === s)!;
      read.push({ table: s, convertible: given.convertible, ...(given.pkg !== undefined ? { pkg: given.pkg } : {}),
        columns: await catalogColumnsOf(engine, host.models, SCHEMA, s) });
    }
    return { tables: read, target: makes ? [] : await catalogColumnsOf(engine, host.models, SCHEMA, output.name) };
  });
  const request: OutputRequest = {
    databaseType: host.databaseType,
    tables,
    output: { name: output.name, mode: output.mode, ...(output.mode === 'merge' ? { keys: output.keys ?? [] } : {}) },
    rows: toJson(rows),
    // a table there already is named by its columns; none, the planner refuses the write by name
    ...(target.length > 0 ? { target } : {}),
  };
  const plan = await host.models.outputPlan(request);
  return countOf(await host.hold((held) => runPlan(plan, { databaseType: host.databaseType, run: held.rows })));
}

/** The count a write's plan answers: its relation's one row (`[{"value": n}]`, as the database wrote it). */
function countOf(answer: string): number {
  const read = JSON.parse(answer) as unknown;
  const n = Array.isArray(read) && read.length === 1 ? (read[0] as { value?: unknown }).value : undefined;
  if (typeof n !== 'number' || !Number.isSafeInteger(n)) throw new Error(`a write answered no count: ${answer}`);
  return n;
}
