// THE HOST'S DATABASE, as the bridge carries it (C5-T, 2026-11-15: out of host.ts, the run side's own module,
// //datacube:run): the one connection of DataCube's headless code that sends a statement -- a plan's, branded
// (PlannedSql) -- across the bridge to Python's DuckDB (calls.ts `query`, `statement`), its Arrow IPC answer read as
// DuckDB-WASM's are. It plans nothing and imports no planner (//datacube:run's closure is typechecked alone).

import { arrowIpcTable } from '../../../engine-client/src/arrow-ipc.ts';
import type { ArrowishConnection, ArrowishTable } from '../../../engine-client/src/duckdb.ts';
import type { PlannedSql } from '../../../engine-client/src/planned-sql.ts';
import type { HostCalls } from './calls.ts';
import { fromBase64 } from './web.ts';

/** The host's database: its connection (a test's files are its own host's: datacube/test/host/headless.ts). */
export interface HostDatabase {
  readonly connection: ArrowishConnection;
}

/**
 * The host's database as DataCube's engine takes one, over the host's two calls that send a statement alone (`query`,
 * `statement`: the C5-T audit's item 3, 2026-11-15 -- never the planner's call, which the run side is not handed): for what DataCube writes into it (a page's file read in, a remote
 * file's view, a snap's copy modelled) and its catalog -- never a grid's rows, which the host's plan-and-run reads
 * (hostEngine: D16). Each query is one whole call (no `send`: the host's DuckDB
 * answers a statement at once, so a superseded query is not cancelled between batches); its Arrow IPC read by
 * apache-arrow, as DuckDB-WASM's answers are.
 */
export function hostDatabase(calls: Pick<HostCalls, 'query' | 'statement'>): HostDatabase {
  const connection: ArrowishConnection = {
    async query(sql: PlannedSql): Promise<ArrowishTable> {
      return arrowIpcTable(fromBase64(await calls.query(sql)));
    },
    // a write's step, its values bound by the host's database (table-output.ts)
    prepare: (sql: PlannedSql) => ({
      query: async (...params: unknown[]): Promise<ArrowishTable> => arrowIpcTable(fromBase64(await calls.statement(sql, JSON.stringify(params)))),
    }),
  };
  return { connection };
}
