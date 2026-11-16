// The executor seam.
//
// One interface, two implementations: SQL run against a warehouse on a
// server, or against DuckDB in the browser. Both receive the SAME SQL,
// because legend-lite is the single planner -- the snapshot compiles to
// Pure, Pure lowers to SQL once, and only the place it runs differs.
//
// This is also what makes the snap's LOCATION swappable. A snap in a
// browser tab, in OPFS, in a native DuckDB on the user's machine, or in
// a per-user cache on a server are all the same engine running the same
// SQL; moving it changes where the work happens, never the answer. That
// matters because the deciding factor is likely to be policy on data at
// rest rather than performance.

import type { PlannedSql } from './planned-sql.ts';

// the engine's shapes, records since the C5-T last round's T1 (query-engine.ts): read here by their importers
export type { QueryEngine, RawColumn, RawTable, ResultWindow, WindowedResult } from './query-engine.ts';
export { QueryError, typedByPlan } from './query-engine.ts';
import type { QueryEngine } from './query-engine.ts';

/**
 * THE SESSION THE PLANNER'S SQL ASSUMES (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): the dialect's own setup
 * (Boundary.sessionSetup: a DuckDB session in UTC), run on a database the tab opens before anything is read from or
 * written to it -- as legend-lite's server sets every session up as it opens it, and Python's Frames their own. Left
 * alone, DuckDB-WASM takes the browser's zone (a fixed offset of it at that), and `now()`, `today()` and a zoned value
 * answer otherwise than on the server. `setup` is the planner's answer for the database's type (WasmGrammar's
 * sessionSetup, DataCube's TableModels'). Every app's tab DuckDB is set up by it, in connectDuckDbInTab (the one start
 * of a tab's DuckDB).
 */
export async function setUpSession(engine: Pick<QueryEngine, 'run'>, setup: readonly PlannedSql[]): Promise<void> {
  // the planner's statements, asked by the door before the run (C5-T, 2026-11-15: until then this asked the planner
  // itself, inside the run): a run takes its statements, never their source
  for (const statement of setup) await engine.run(statement, 0);
}

