// Where an app's projects and published versions live (design S21), for every app here (Studio writes them, Query
// and DataCube read them by name): in this page (level 0 -- the SDLC's and Depot's rules compiled to WebAssembly,
// kept in this origin's IndexedDB, so every app served from the origin sees the same projects) or on servers (the
// model home's, or a real legend-sdlc and legend-depot). The app sees one SdlcClient and one DepotClient either way.

import { DepotClient } from './client.ts';
import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { BrowserRecords } from '../../sdlc-client/src/records.ts';
import { WASM_API, WASM_DEPOT_API, wasmSdlcServer, type SdlcModule } from '../../sdlc-client/src/wasm-server.ts';

export interface ModelHomeConfig {
  /** `"page"` (no server), or an SDLC's API root, e.g. `http://127.0.0.1:6100/sdlc/api`. */
  readonly sdlc: string;
  /** A Depot's API root; by default the model home's beside the SDLC (`…/depot/api`). Unused for `"page"`. */
  readonly depot?: string;
  /** Where the page's SDLC module is served (`<vendor>sdlc/`), for `"page"`. */
  readonly vendor: string;
  /** The page's user when the SDLC is the page's own. */
  readonly user?: { readonly userId: string; readonly name: string };
}

export interface ModelHome {
  readonly client: SdlcClient;
  readonly depot: DepotClient;
  /** Where the projects live, as a person reads it. */
  readonly where: string;
}

export async function connectModelHome(config: ModelHomeConfig): Promise<ModelHome> {
  if (config.sdlc !== 'page') {
    const depot = config.depot ?? config.sdlc.replace(/\/sdlc\/api\/?$/, '/depot/api');
    return { client: new SdlcClient(config.sdlc), depot: new DepotClient(depot), where: config.sdlc };
  }
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
  return {
    client: new SdlcClient(WASM_API, server.fetch),
    depot: new DepotClient(WASM_DEPOT_API, server.depotFetch),
    where: 'this browser',
  };
}
