// THE DOOR TO THE TAB'S DUCKDB (C5-T, 2026-11-15: the TypeScript split of plan and run, as Java's doors only compose):
// DuckDB's download and instantiate begun at once -- they send no statement -- alongside the planner's asks for the
// setup every connection runs (the extension folder's host operation and the session's setup), outside any run; then,
// both in, DuckDB connected with that setup inside a run frame (run-frame.ts: the start's statements plan nothing). The
// overlap is the page's: its floor the slower of the planner's start and DuckDB's, never their sum (the C5-T audit's
// MUST 1). Every app's tab DuckDB opens here (DataCube, Query, Studio and the test pages standing in for them).

import { connectDuckDbInTab, extensionRepository, instantiateDuckDbInTab, type DuckDbInTab, type TabSetup } from '../duckdb-tab.ts';
import { DuckDbEngine } from '../duckdb.ts';
import type { TabDuckDbPlanner } from '../planner-sources.ts';
import { inRun } from '../run-frame.ts';

/** DuckDB in this tab from `vendor` (the folder of duckdb-eh.wasm and its worker), set up as `planner` answers. */
export function openDuckDbInTab(vendor: string, planner: TabDuckDbPlanner): Promise<DuckDbInTab> {
  return openedInTab(() => instantiateDuckDbInTab(vendor), extensionRepository(vendor), planner, connectDuckDbInTab);
}

/**
 * The door's order, apart from DuckDB's own calls (a test drives it with stand-ins): `instantiate` begun with the
 * planner's asks, neither waiting for the other; `connect`, with the setup answered, inside a run once both are in. A
 * failure of either is the start's (the first to fail: a browser DuckDB cannot run is refused before the download).
 */
export async function openedInTab<D, T>(
  instantiate: () => Promise<D>,
  repository: string,
  planner: TabDuckDbPlanner,
  connect: (database: D, setup: TabSetup) => Promise<T>,
): Promise<T> {
  const asked = (async (): Promise<TabSetup> => {
    const extensions = (await planner.hostOperations(DuckDbEngine.databaseType, [
      { op: 'extensionRepository', url: repository },
    ])).flat();
    return { extensions, session: await planner.sessionSetup(DuckDbEngine.databaseType) };
  })();
  const [database, setup] = await Promise.all([instantiate(), asked]);
  return inRun("the tab's DuckDB started", () => connect(database, setup));
}
