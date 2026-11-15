// THE DOOR TO THE TAB'S DUCKDB (C5-T, 2026-11-15: the TypeScript split of plan and run, as Java's doors only compose):
// the planner asked for the setup every connection runs -- the extension folder's host operation and the session's
// setup -- outside any run, then DuckDB started with that setup inside a run frame (run-frame.ts: the start's statements
// plan nothing). Every app's tab DuckDB opens here (DataCube, Query, Studio and the test pages standing in for them).

import { extensionRepository, startDuckDbInTab, TAB_DATABASE_TYPE, type DuckDbInTab } from '../duckdb-tab.ts';
import type { TabDuckDbPlanner } from '../planner-sources.ts';
import { inRun } from '../run-frame.ts';

/** DuckDB in this tab from `vendor` (the folder of duckdb-eh.wasm and its worker), set up as `planner` answers. */
export async function openDuckDbInTab(vendor: string, planner: TabDuckDbPlanner): Promise<DuckDbInTab> {
  const extensions = (await planner.hostOperations(TAB_DATABASE_TYPE, [
    { op: 'extensionRepository', url: extensionRepository(vendor) },
  ])).flat();
  const session = await planner.sessionSetup(TAB_DATABASE_TYPE);
  return inRun("the tab's DuckDB started", () => startDuckDbInTab(vendor, { extensions, session }));
}
