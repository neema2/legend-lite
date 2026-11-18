// legend-engine's routes in this process, the edge's (the user's K2 ruling, 2026-11-18: "Product root, edge opt-in"): the
// engine module (//wasm:engine_planner), loaded as an app configured engine: 'legend-engine' loads it (engine-module.ts),
// beside the planner test/planner.ts loads. A test of the edge's imports it; a product test never does.

import { ENGINE, enginePort, loadEngineModule, type EnginePort } from '../src/legend/engine-module.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';
import { openInNode } from './planner.ts';

/** The engine module from WASM_ENGINE_PLANNER (node_test's `engine`), in-thread: what the edge's grammar asks beside the
 *  planner. */
export async function loadEnginePort(): Promise<EnginePort> {
  return enginePort(await loadEngineModule(ENGINE, runfileDirUrl('WASM_ENGINE_PLANNER'), openInNode));
}
