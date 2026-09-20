// The demo with NO SERVER: the planner runs in the tab.
//
// This is the DEFAULT entry point; main-server.ts is the one that
// needs legend-lite running.
//
// A separate entry point rather than a flag on the other one. The
// rule in test/guardrails.test.ts is that shipped code must not be
// able to CHOOSE a planner at runtime -- a URL parameter is exactly
// that choice, and a demo that can silently pick a different planner
// is how three real bugs stayed invisible last time. Here the
// decision is made by which bundle the page loads, which is static,
// visible in the build, and unreachable from the other bundle.
//
// `WasmPlanner` is not a second planner. It calls the same
// `Compiler.plan` that POST /engine/plan calls, compiled to
// WebAssembly; `npm run verify:wasm` checks 16 cube shapes -- this
// demo's own serialised grammar -- against the JVM and expects every
// answer, refusals included, to be identical.
//
//   npm run planner:vendor && npm run build:demo
//   then open demo/index.html

import { boot, loadModel, RUNTIME, SNAP_TARGET, SOURCE } from './boot.ts';
import type { Engine } from './boot.ts';
import { WasmPlanner } from '../src/wasm-planner.ts';

async function inBrowserPlanner(_status: HTMLElement): Promise<Engine> {
  const model = await loadModel();
  const planner = new WasmPlanner({
    model,
    runtime: RUNTIME,
    // Off the main thread: building the boot layer is ~600ms of
    // synchronous WebAssembly, which on the main thread froze the
    // page and starved DuckDB's startup.
    workerUrl: new URL('./planner-worker.js', import.meta.url).href,
  });

  // Pay the cold cost here rather than on the user's first
  // interaction. It is ~1.3s, almost all of it boot-layer
  // construction -- parsing the 300 KB Pure prelude, the system
  // metamodel, then resolving and normalizing both -- against ~10ms
  // warm. `boot` starts this concurrently with DuckDB, so most of it
  // lands inside a wait the page was making anyway.
  await planner.warmUp();

  return {
    planner,
    source: SOURCE,
    snapTarget: SNAP_TARGET,
    label: 'planner: legend-lite (wasm, no server)',
  };
}

void boot(inBrowserPlanner).catch((e) => {
  const s = document.getElementById('status');
  if (s) {
    s.textContent = `failed to start: ${e instanceof Error ? e.message : e}`;
  }
  throw e;
});
