// THE PLANNER THE PAGE CHOSE, built (the user, 2026-09-30): one module for every page of the demo
// -- the cube page, the page of several cubes, the stress page -- so none of them is hard-wired to
// one planner. `chosenPlane` (boot.ts) says which; this builds it.
//
// The in-tab planner (legend-lite's compiler as WebAssembly) and legend-lite on a server answer lite API v1
// (docs/records/lite-api-v1-2026-11-17.md); legend-engine answers its own pure/v1, through the legend-engine client. A server
// planner is asked whether it is there first; not there, the page says so and stops
// (`refusePlanner`): there is no fallback.
//
// A TABLE OPENED ON THE PAGE -- a file in this tab, a remote file, a warehouse table: its model (a
// Pure Database from the table's own catalog, its connection and runtime) is written in the tab by
// legend-lite's one writer, in its WebAssembly module (src/infer.ts `TableModels`), whichever
// planner the page chose -- no server can read a database inside the browser, and legend-engine
// has no writer to ask -- and then compiled and typed by the chosen planner, whose answer decides
// whether it opens (the user, 2026-10-08: one writer; it replaced a TypeScript copy). In this tab
// the planner is the writer; beside a planner on a server, the module is loaded in a worker the
// first time a model is written -- so a page planning on a server now needs, to open a table, what
// the in-tab planner always needed: the module (vendor/classes.wasm) and a browser with WebAssembly
// GC. Without them the open is refused, saying so (PlannerUnavailableError).

import { refusePlanner, RUNTIME, SOURCE, type Engine, type PlaneWord } from './boot.ts';
import { pageConfig } from './page-config.ts';
import type { Planner } from '../src/planner-types.ts';
import type { ModelOptions } from '../src/planner.ts';
import { LitePlanner } from '../src/lite-planner.ts';
import { liteServerWorker } from '../src/services/lite-server.ts';

const WORKER = (): string => new URL('./planner-worker.js', import.meta.url).href;

/** DataCube's legend-engine plane, its compat entry (datacube/src/edge/engine-plane.ts, //datacube:edge_entry): its own
 *  bundle beside the page, loaded by its URL only when the `engine` plane is chosen (H11, 2026-11-17: no product module
 *  imports an edge module) -- the page names what it reads of it, a planner over a model it can repoint. */
const ENGINE_PLANE = (): string => new URL('./legend-engine.js', import.meta.url).href;

interface EnginePlane {
  legendEnginePlanner(baseUrl: string, model: string, runtime: string): Planner & {
    useModel(model: string, runtime: string, how?: ModelOptions): void;
    withModel(model: string, runtime: string, how?: ModelOptions): Planner;
    modelElements(text: string): Promise<unknown[]>;
  };
}

/** The planner the page chose, over `model`, ready to plan. */
export async function plannerFor(plane: PlaneWord, model: string): Promise<Engine> {
  return plane === 'local' ? inTab(model) : onServer(model, plane);
}

/** The planner in this tab: legend-lite's compiler as WebAssembly, off the main thread. */
async function inTab(model: string): Promise<Engine> {
  // the compiler's module from the page's vendor folder (page-config.ts): beside the page, or the site's one copy
  const { vendor } = await pageConfig();
  const planner = new LitePlanner({
    assetBaseUrl: vendor,
    model,
    runtime: RUNTIME,
    // Off the main thread: building the boot layer is ~600ms of synchronous WebAssembly, which on
    // the main thread froze the page and starved DuckDB's startup.
    workerUrl: WORKER(),
  });
  // Pay the cold cost (~1.3s, the boot layer) here rather than on the first interaction; `boot`
  // starts this concurrently with DuckDB, so most of it lands inside a wait the page was making.
  await planner.warmUp();
  return {
    planner,
    source: SOURCE,
    tables: planner,
    projects: planner,
    tsItem: (text) => planner.tsItem(text),
    label: 'local',
    models: {
      use: (next, runtime, how) => planner.useModel(next, runtime, how),
      another: (next, runtime, how) => planner.withModel(next, runtime, how),
      elements: (text) => planner.modelElements(text),
    },
  };
}

/** A planner on a server: legend-lite's, over lite API v1's own routes (lite-server.ts: the edge audit's H7, 2026-11-17),
 *  or legend-engine's, over its pure/v1 (UpstreamPlanner: the legend-engine client, the edge's). */
async function onServer(model: string, which: 'remote' | 'engine'): Promise<Engine> {
  const config = await pageConfig();
  const url = which === 'remote' ? config.legendLite : config.legendEngine;
  const name = which === 'remote' ? 'legend-lite' : 'legend-engine';
  const start = which === 'remote'
    ? 'start it with `bazel run //core:server`'
    : 'start it (the shaded jar needs no JDK, Maven or Docker install)';
  if (!url) refusePlanner(name, '', `set "${which === 'remote' ? 'legendLite' : 'legendEngine'}" in config.json, then ${start}`);
  const health = which === 'remote' ? `${url}/health` : `${url}/api/server/v1/info`;
  const answered = await fetch(health, { signal: AbortSignal.timeout(2500) }).then((r) => r.ok, () => false);
  if (!answered) refusePlanner(name, url, start);
  const planner = which === 'remote'
    ? new LitePlanner({ model, runtime: RUNTIME, worker: () => liteServerWorker(url) })
    : (await import(ENGINE_PLANE()) as EnginePlane).legendEnginePlanner(url, model, RUNTIME);
  // legend-lite's module beside the server's planner, nothing loaded until first asked: it writes a table's model, and a
  // saved query's project's test data as the server's statements
  const writer = new LitePlanner({ model: '', runtime: RUNTIME, workerUrl: WORKER(), assetBaseUrl: config.vendor });
  return {
    planner,
    source: SOURCE,
    tables: writer,
    projects: writer,
    tsItem: (text) => writer.tsItem(text),
    label: which,
    models: {
      use: (next, runtime, how) => planner.useModel(next, runtime, how),
      another: (next, runtime, how) => planner.withModel(next, runtime, how),
      elements: (text) => planner.modelElements(text),
    },
  };
}
