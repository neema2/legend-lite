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
export function openDuckDbInTab(vendor: string, planner: TabDuckDbPlanner): Promise<OpenedDuckDb> {
  // THE WORKER IS THE DOOR'S TO END (2026-11-15: DataCube in another application's page, datacube/demo/library.ts, a
  // page that lives on after its DuckDB): by `close`, and by a start that fails -- the planner did not answer, DuckDB's
  // module was not there -- which ends the worker it started and starts none after
  let worker: Worker | undefined;
  let failed: { readonly why: unknown } | undefined;
  return openedInTab(async () => {
    await duckDbRunsHere();
    if (failed) throw failed.why;
    worker = new Worker(vendorAsset(vendor, 'duckdb-browser-eh.worker.js'));
    return instantiateDuckDbInTab(worker, vendorAsset(vendor, 'duckdb-eh.wasm'));
  }, extensionRepository(vendor), planner, connectDuckDbInTab).then((tab) => ({ ...tab, close: () => worker?.terminate() }), (why: unknown) => {
    failed = { why };
    worker?.terminate();
    throw why;
  });
}

/** The tab's DuckDB as its door hands it, with what ends it. */
export interface OpenedDuckDb extends DuckDbInTab {
  /**
   * The database ended: its worker stopped, and everything it held gone with it. For a host that takes its page down
   * while the tab lives on; a tab that closes needs no call. The worker is stopped as it stands -- not through
   * DuckDB-WASM's own terminate, after which a statement still on its way (a grid's, as its page is taken down) is
   * reported on the console as an error; sent to a stopped worker it goes nowhere, and says nothing.
   */
  close(): void;
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
