// THE DOOR TO THE TAB'S DUCKDB (C5-T, 2026-11-15: the TypeScript split of plan and run, as Java's doors only compose):
// DuckDB's download and instantiate begun at once -- they send no statement -- alongside the planner's asks for the
// setup every connection runs (the extension folder's host operation and the session's setup), outside any run; then,
// both in, DuckDB connected with that setup inside a run frame (run-frame.ts: the start's statements plan nothing). The
// overlap is the page's: its floor the slower of the planner's start and DuckDB's, never their sum (the C5-T audit's
// MUST 1). Every app's tab DuckDB opens here (DataCube, Query, Studio and the test pages standing in for them).

import {
  connectDuckDbInTab, duckDbRunsHere, extensionRepository, instantiateDuckDbInTab, vendorAsset, type DuckDbInTab, type TabSetup,
} from '../duckdb-tab.ts';
import { DuckDbEngine } from '../duckdb.ts';
import type { SynthesizeSource, TabDuckDbPlanner } from '../planner-sources.ts';
import type { SynthesizeRequest } from '../requests.ts';
import { inRun } from '../run-frame.ts';

/**
 * DuckDB in this tab from `vendor` (the folder of duckdb-eh.wasm and its worker), set up as `planner` answers. Its
 * worker is started here, from the deployment's vendor folder, once the browser is known to run it: a door's wiring,
 * the run side taking the started worker and loading no module (2026-11-15, the C5-T fold-in: until then
 * duckdb-tab.ts started it, one of the run layer's two named vendor loads; //tools/deps:layers_alone).
 */
export function openDuckDbInTab(vendor: string, planner: TabDuckDbPlanner): Promise<DuckDbInTab> {
  return openedInTab(async () => {
    await duckDbRunsHere();
    return instantiateDuckDbInTab(new Worker(vendorAsset(vendor, 'duckdb-browser-eh.worker.js')), vendorAsset(vendor, 'duckdb-eh.wasm'));
  }, extensionRepository(vendor), planner, connectDuckDbInTab);
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

/**
 * A table made and filled in this tab's DuckDB by the planner's SYNTHESIZE plan for `request` (DataCube's demo sample,
 * src/sample-trades.ts: the table made as declared, then one statement over range(N), every value the database's,
 * register S303): the plan asked outside any run, then run by the hosts' plan runner on the held connection inside a
 * run. The runner is fetched here, as a remote mount fetches it: a grid-only page carries none of it (2026-11-15, the
 * C5-T fold-in: DataCube's page ran this plan itself until then).
 */
export async function synthesizedInTab(engine: DuckDbEngine, planner: SynthesizeSource, request: SynthesizeRequest): Promise<void> {
  const [plan, { runLoad }] = await Promise.all([planner.synthesize(request), import('../plan-runner.ts')]);
  await inRun('a table synthesized', () => engine.exclusive((held) => runLoad(plan, (sql, params) => held.rows(sql, params))));
}
