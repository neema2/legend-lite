// THE PLANNER THE PAGE CHOSE, built (the user, 2026-09-30): one module for every page of the demo
// -- the cube page, the page of several cubes, the stress page -- so none of them is hard-wired to
// one planner. `chosenPlane` (boot.ts) says which; this builds it.
//
// The in-tab planner (legend-lite's compiler as WebAssembly), legend-lite on a server and
// legend-engine are three addresses of the same service -- pure/v1's generatePlan. A server
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

import { refusePlanner, RUNTIME, SNAP_TARGET, SOURCE, type Engine, type PlaneWord } from './boot.ts';
import { pageConfig } from './page-config.ts';
import { UpstreamPlanner } from '../src/planner.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';

const WORKER = (): string => new URL('./planner-worker.js', import.meta.url).href;

/** The planner the page chose, over `model`, ready to plan. */
export async function plannerFor(plane: PlaneWord, model: string): Promise<Engine> {
  return plane === 'local' ? inTab(model) : onServer(model, plane);
}

/** The planner in this tab: legend-lite's compiler as WebAssembly, off the main thread. */
async function inTab(model: string): Promise<Engine> {
  const planner = new WasmPlanner({
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
    snapTarget: { ...SNAP_TARGET, planner },
    tables: planner,
    seeds: planner,
    label: 'local',
    models: {
      use: (next, runtime, how) => planner.useModel(next, runtime, how),
      another: (next, runtime, how) => planner.withModel(next, runtime, how),
      elements: (text) => planner.modelElements(text),
    },
  };
}

/** A planner on a server -- legend-lite or legend-engine, the same API at another address. */
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
  const planner = new UpstreamPlanner({ baseUrl: url, model, runtime: RUNTIME });
  // legend-lite's module beside the server's planner, nothing loaded until first asked: it writes a table's model, and a
  // saved query's project's test data as the server's statements
  const writer = new WasmPlanner({ model: '', runtime: RUNTIME, workerUrl: WORKER() });
  return {
    planner,
    source: SOURCE,
    snapTarget: { ...SNAP_TARGET, planner },
    tables: writer,
    seeds: writer,
    label: which,
    models: {
      use: (next, runtime, how) => planner.useModel(next, runtime, how),
      another: (next, runtime, how) => planner.withModel(next, runtime, how),
      elements: (text) => planner.modelElements(text),
    },
  };
}
