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

import * as duckdb from '@duckdb/duckdb-wasm';

import { DuckDbEngine, type ArrowishConnection } from '../src/duckdb.ts';
import { mountRemote, type RemoteSource } from '../src/remote.ts';

declare global {
  interface Window {
    remoteTest?: (source: RemoteSource) => Promise<unknown>;
  }
}

async function boot(): Promise<DuckDbEngine> {
  const asset = (f: string) => new URL(`./vendor/${f}`, location.href).href;
  const bundle = await duckdb.selectBundle({
    mvp: {
      mainModule: asset('duckdb-mvp.wasm'),
      mainWorker: asset('duckdb-browser-mvp.worker.js'),
    },
    eh: {
      mainModule: asset('duckdb-eh.wasm'),
      mainWorker: asset('duckdb-browser-eh.worker.js'),
    },
  });
  const worker = new Worker(bundle.mainWorker!);
  const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  const conn = await db.connect();
  return new DuckDbEngine(conn as unknown as ArrowishConnection);
}

const ready = boot();

window.remoteTest = async (source: RemoteSource) => {
  const engine = await ready;
  await mountRemote(engine, { sources: [source] });

  // The shape the planner emits: a grouped aggregate over the table
  // NAME, with no idea the rows are coming over the network.
  const table = await engine.execute(
    `SELECT region, sum(notional) AS m FROM ${JSON.stringify(source.name)}`
    + ' GROUP BY region ORDER BY region',
    1,
  );
  return {
    columns: table.columns.map((c) => c.name),
    rowCount: table.rowCount,
    regions: table.columns[0]?.values ?? [],
    totals: table.columns[1]?.values ?? [],
  };
};

document.body.dataset['ready'] = 'yes';
