// A page that mounts a remote Parquet and queries it.
//
// Separate from the demo because the thing under test is the
// CONNECTION, not the grid: booting the whole app would prove the
// app works and say nothing about whether bytes moved. This boots
// duckdb-wasm, calls the real mountRemote, and runs the shape of
// query the planner emits.
//
// It has to run in a browser. The node build of duckdb-wasm answers
// a synthetic 404 for any http:// URL rather than making a request,
// so a node test here would be green and meaningless.


import type { DuckDbEngine } from '../../engine-client/src/duckdb.ts';
import { startDuckDbInTab } from '../../engine-client/src/duckdb-tab.ts';
import type { TableModels } from '../src/infer.ts';
import { mountRemote, type RemoteSource } from '../src/remote.ts';
import { heldDataHost } from '../src/data-host.ts';
import { loadModel } from './boot.ts';
import { plannerFor } from './planners.ts';
import type { Planner } from '../src/cube.ts';

declare global {
  interface Window {
    remoteTest?: (source: RemoteSource) => Promise<unknown>;
  }
}

async function boot(): Promise<{ readonly engine: DuckDbEngine; readonly tables: TableModels; readonly planner: Planner }> {
  // the tab's planner over the demo's model (trades.pure: the table TRADES the mount names): it writes the tab's
  // DuckDB's set-up, the mount's statements (D5) and the query below; the one start of a tab's DuckDB (engine-client's
  // startDuckDbInTab)
  const { tables, planner } = await plannerFor('local', await loadModel());
  return { engine: (await startDuckDbInTab('./vendor/', tables)).engine, tables, planner };
}

const ready = boot();

window.remoteTest = async (source: RemoteSource) => {
  const { engine, tables, planner } = await ready;
  if (source.name !== 'TRADES') throw new Error(`the harness plans over the model's table TRADES, not '${source.name}'`);
  await mountRemote(heldDataHost(engine, tables), { sources: [source] });

  // The planner's own query: a grouped aggregate over the table
  // NAME, with no idea the rows are coming over the network (a host
  // runs only what a plan says: C3, planned-sql.ts).
  const query = await planner.parse(
    '|#>{trades::DB.TRADES}#->groupBy(~region, ~m:x|$x.notional:y|$y->sum())->sort(~region->ascending())');
  const table = await engine.execute(await planner.plan(query), 1);
  return {
    columns: table.columns.map((c) => c.name),
    rowCount: table.rowCount,
    regions: table.columns[0]?.values ?? [],
    totals: table.columns[1]?.values ?? [],
  };
};

document.body.dataset['ready'] = 'yes';
