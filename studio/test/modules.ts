// The two WebAssembly modules Studio runs, loaded in node as the page loads them: legend-lite's
// planner (the compiler) and the SDLC (sdlc-server's rules) -- so tests drive Studio's model with the
// real things.

import { fileURLToPath } from 'node:url';

import { DepotClient } from '../../depot-client/src/client.ts';
import { MemoryRecords } from '../../sdlc-client/src/records.ts';
import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { WASM_API, WASM_DEPOT_API, wasmSdlcServer, type SdlcModule } from '../../sdlc-client/src/wasm-server.ts';
import { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import { WasmEdgeGrammar } from '../../engine-client/src/legend/wasm-edge.ts';
import { Compiler, type PlannerPort } from '../src/backend/planner.ts';
import { answer, type PlannerModule } from '../../engine-client/src/legend/planner-answer.ts';
import { ENGINE, answerEngine, loadEngineModule, type EngineModule, type EnginePort, type EngineRequest }
  from '../../engine-client/src/legend/engine-module.ts';
import type { RequestBody } from '../../engine-client/src/legend/planner-protocol.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

async function load<T>(dir: URL): Promise<T> {
  const runtime = await import(new URL('wasm-gc-module-runtime.js', dir).href) as {
    load(src: string, options: unknown): Promise<T>;
  };
  return runtime.load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(i: Record<string, unknown>) {
      i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}

let planner: Promise<PlannerModule> | undefined;

/** The planner in this process, answering as the tab's worker answers (planner-answer.ts). */
class DirectPort implements PlannerPort {
  async ask(r: RequestBody): Promise<string> {
    planner ??= load<PlannerModule>(new URL(runfileDirUrl('WASM_PLANNER')));
    return answer(await planner, r);
  }
}

let engine: Promise<EngineModule> | undefined;

/** The engine module in this process, loaded the first time legend-engine's routes are asked, as an app configured
 *  engine: 'legend-engine' loads it (engine-module.ts; the user's K2 ruling, 2026-11-18). */
class DirectEnginePort implements EnginePort {
  async ask(r: EngineRequest): Promise<string> {
    engine ??= loadEngineModule(ENGINE, runfileDirUrl('WASM_ENGINE_PLANNER'), (dir) => load<EngineModule>(new URL(dir)));
    return answerEngine(await engine, r);
  }
}

/** legend-lite's planner, in this process: the grammar Studio reads elements and queries with. */
export const grammar = new WasmGrammar(new DirectPort());
/** legend-engine's shapes over the same planner and the engine module, the edge's (wasm-edge.ts): what the edge's
 *  BrowserEngine asks. */
export const edge = new WasmEdgeGrammar(new DirectPort(), new DirectEnginePort());
export const compiler = new Compiler(grammar);

/** A fresh in-page SDLC (its own module, its own records) and a client over it. */
export async function pageSdlc(): Promise<SdlcClient> {
  return (await pageSdlcAndDepot()).client;
}

/** A fresh in-page SDLC and the Depot beside it (the same module), with clients over both. */
export async function pageSdlcAndDepot(): Promise<{ client: SdlcClient; depot: DepotClient }> {
  const module = await load<SdlcModule>(new URL(runfileDirUrl('SDLC_PAGE')));
  const server = await wasmSdlcServer(module, new MemoryRecords(), { userId: 'local', name: 'Local User' });
  return { client: new SdlcClient(WASM_API, server.fetch), depot: new DepotClient(WASM_DEPOT_API, server.depotFetch) };
}
