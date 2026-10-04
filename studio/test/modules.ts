// The two WebAssembly modules Studio runs, loaded in node as the page loads them: legend-lite's
// planner (the compiler) and the SDLC (sdlc-server's rules) -- so tests drive Studio's model with the
// real things.

import { fileURLToPath } from 'node:url';

import { DepotClient } from '../../depot-client/src/client.ts';
import { MemoryRecords } from '../../sdlc-client/src/records.ts';
import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { WASM_API, WASM_DEPOT_API, wasmSdlcServer, type SdlcModule } from '../../sdlc-client/src/wasm-server.ts';
import { Compiler, type PlannerPort } from '../src/backend/planner.ts';
import type { PlannerRequest } from '../src/backend/planner-worker.ts';

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

interface PlannerModule {
  readonly exports: Record<string, (...args: string[]) => string | number>;
}

let planner: Promise<PlannerModule> | undefined;

class DirectPort implements PlannerPort {
  async ask(r: PlannerRequest): Promise<string> {
    planner ??= load<PlannerModule>(new URL('../../wasm/planner/', import.meta.url));
    const e = (await planner).exports;
    switch (r.kind) {
      case 'modelJson': return e.modelJsonOrError!(r.text) as string;
      case 'compile': return e.compileOrError!(r.model) as string;
      case 'warm': e.warmModel!(r.model); return 'OK\n';
    }
  }
}

export const compiler = new Compiler(new DirectPort());

/** A fresh in-page SDLC (its own module, its own records) and a client over it. */
export async function pageSdlc(): Promise<SdlcClient> {
  return (await pageSdlcAndDepot()).client;
}

/** A fresh in-page SDLC and the Depot beside it (the same module), with clients over both. */
export async function pageSdlcAndDepot(): Promise<{ client: SdlcClient; depot: DepotClient }> {
  const module = await load<SdlcModule>(new URL('../../sdlc-server/page/', import.meta.url));
  const server = await wasmSdlcServer(module, new MemoryRecords(), { userId: 'local', name: 'Local User' });
  return { client: new SdlcClient(WASM_API, server.fetch), depot: new DepotClient(WASM_DEPOT_API, server.depotFetch) };
}
