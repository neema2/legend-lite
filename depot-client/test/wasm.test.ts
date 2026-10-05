// The page's Depot: depot-server's rules compiled to WebAssembly with sdlc-server's, over the page's own
// versions -- held to the one suite.

import { fileURLToPath } from 'node:url';

import { MemoryRecords } from '../../sdlc-client/src/records.ts';
import { WASM_API, WASM_DEPOT_API, wasmSdlcServer, type SdlcModule } from '../../sdlc-client/src/wasm-server.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';
import { conformance } from './conformance.ts';

const DIR = runfileDirUrl('SDLC_PAGE');
const runtime = await import(new URL('wasm-gc-module-runtime.js', DIR).href) as { load(src: string, options: unknown): Promise<SdlcModule> };
const module = await runtime.load(fileURLToPath(new URL('classes.wasm', DIR)), {
  stackDeobfuscator: { enabled: false },
  installImports(i: Record<string, unknown>) { i.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; },
});
const page = await wasmSdlcServer(module, new MemoryRecords(), { userId: 'local', name: 'Local User' });

conformance("the page's Depot, compiled from Java", () => ({
  sdlc: { api: WASM_API, fetch: page.fetch },
  depot: { api: WASM_DEPOT_API, fetch: page.depotFetch },
}));
