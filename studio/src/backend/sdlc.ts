// Where Studio's projects live (design S21): in this page (level 0 -- sdlc-server's rules compiled to
// WebAssembly, kept in IndexedDB) or on an SDLC server (sdlc-server, or a real legend-sdlc). The rest of
// Studio sees one SdlcClient either way.

import { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { BrowserRecords } from '../../../sdlc-client/src/records.ts';
import { WASM_API, wasmSdlcServer, type SdlcModule } from '../../../sdlc-client/src/wasm-server.ts';

export interface StudioConfig {
  /** `"page"` (no server), or an SDLC's API root, e.g. `http://127.0.0.1:6100/sdlc/api`. */
  readonly sdlc: string;
  /** Where the WebAssembly modules are served: `<vendor>/planner/` and `<vendor>/sdlc/`. */
  readonly vendor: string;
  /** The page's user when the SDLC is the page's own. */
  readonly user?: { readonly userId: string; readonly name: string };
}

export async function connect(config: StudioConfig): Promise<{ client: SdlcClient; where: string }> {
  if (config.sdlc !== 'page') return { client: new SdlcClient(config.sdlc), where: config.sdlc };
  const base = new URL(`${config.vendor}sdlc/`, globalThis.location.href).href;
  const runtime = await import(/* @vite-ignore */ `${base}wasm-gc-module-runtime.js`) as {
    load(src: string, options?: unknown): Promise<SdlcModule>;
  };
  const module = await runtime.load(`${base}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
  const server = await wasmSdlcServer(module, new BrowserRecords(), config.user ?? { userId: 'local', name: 'Local User' });
  return { client: new SdlcClient(WASM_API, server.fetch), where: 'this browser' };
}
