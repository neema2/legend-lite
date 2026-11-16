// THE HOST'S DATABASE, THROUGH ITS DOOR (the C5-T last round's T1, 2026-11-15): the bridge's one connection that sends a
// plan's statement (connection.ts, //datacube:run) as DataCube's code takes it -- an engine over it (what a page's data is
// read into and its catalog read from), or the database itself (what the headless entry hands its host). DataCube's
// headless code reaches the run side here alone.

import { DuckDbEngine } from '../../../engine-client/src/duckdb.ts';
import type { HostCalls } from './calls.ts';
import { hostDatabase, type HostDatabase } from './connection.ts';

/** The host's database as DataCube's engine takes one: DuckDB's engine over the bridge's connection. */
export function hostQueries(calls: Pick<HostCalls, 'query' | 'statement'>): DuckDbEngine {
  return new DuckDbEngine(hostDatabase(calls).connection);
}

/** The host's database itself, as the headless entry hands it to its host (entry.ts Headless.database). */
export function hostConnection(calls: Pick<HostCalls, 'query' | 'statement'>): HostDatabase {
  return hostDatabase(calls);
}
