// Where an app's projects and published versions live (design S21), for every app here (Studio writes them, Query
// and DataCube read them by name): in this page (level 0 -- the SDLC's and Depot's rules compiled to WebAssembly,
// kept in this origin's IndexedDB, so every app served from the origin sees the same projects) or on servers (the
// model home's, or a real legend-sdlc and legend-depot). The app sees one SdlcClient and one DepotClient either way.

import { DepotClient } from './client.ts';
import { SdlcClient } from '../../sdlc-client/src/client.ts';
import { BrowserRecords } from '../../sdlc-client/src/records.ts';
import { WASM_API, WASM_DEPOT_API, wasmSdlcServer, type ReleasedVersion, type SdlcModule } from '../../sdlc-client/src/wasm-server.ts';

export interface ModelHomeConfig {
  /** `"page"` (no server), or an SDLC's API root, e.g. `http://127.0.0.1:6100/sdlc/api`. */
  readonly sdlc: string;
  /** A Depot's API root; by default the model home's beside the SDLC (`…/depot/api`). Unused for `"page"`. */
  readonly depot?: string;
  /** Where the page's SDLC module is served (`<vendor>sdlc/`), for `"page"`. */
  readonly vendor: string;
  /** The page's user when the SDLC is the page's own. */
  readonly user?: { readonly userId: string; readonly name: string };
  /**
   * The folder of the versions the site releases, for `"page"` (docs/ONE_CONNECTION_DESK_2026_10_28.md D5): its
   * `index.json` -- `{"versions": [{"groupId", "artifactId", "versionId", "files": ["path/in/folder.pure", ...]}]}` --
   * and the files, fetched at every open and held by the page's model home, never in the browser's storage. A server's
   * model home reads its own (`--releases DIR`). Absent: none.
   */
  readonly releases?: string;
}

/** The versions a site releases, read from its folder (`ModelHomeConfig.releases`): refused, in words, when the folder
 *  or a file is not served. */
export async function releasedVersions(folder: string, fetcher: typeof fetch = fetch): Promise<ReleasedVersion[]> {
  const base = new URL(folder.endsWith('/') ? folder : `${folder}/`, globalThis.location?.href);
  const text = async (url: URL): Promise<string> => {
    const answered = await fetcher(url.href, { cache: 'no-cache' });
    if (!answered.ok) throw new Error(`the site's released versions: ${url.href} answered ${answered.status}`);
    return answered.text();
  };
  return Promise.all((await releasesIndex(base, text)).map(async (v) => ({
    groupId: v.groupId, artifactId: v.artifactId, versionId: v.versionId, ...(v.title ? { title: v.title } : {}),
    files: await Promise.all(v.files.map(async (path) => ({ path, pureCode: await text(new URL(path, base)) }))),
  })));
}

/** A releases folder's `index.json`: each version, its title and its files' paths in the folder. */
async function releasesIndex(base: URL, text: (url: URL) => Promise<string>): Promise<readonly {
  groupId: string; artifactId: string; versionId: string; title?: string; files: readonly string[];
}[]> {
  const index = JSON.parse(await text(new URL('index.json', base))) as {
    readonly versions?: readonly { groupId: string; artifactId: string; versionId: string; title?: string; files: readonly string[] }[];
  };
  return index.versions ?? [];
}

const refOf = (v: { groupId: string; artifactId: string; versionId: string; title?: string }): ReleasedRef =>
  ({ groupId: v.groupId, artifactId: v.artifactId, versionId: v.versionId, title: v.title ?? v.artifactId });

export interface ModelHome {
  readonly client: SdlcClient;
  readonly depot: DepotClient;
  /** Where the projects live, as a person reads it. */
  readonly where: string;
  /** The versions the site releases (`ModelHomeConfig.releases`), as its index names them: what an app opens at its
   *  start (the Query app's projects). Read through `depot` as any version is. */
  readonly released: readonly ReleasedRef[];
}

/** A version the site releases, by its coordinates and the title its index gives it. */
export interface ReleasedRef {
  readonly groupId: string;
  readonly artifactId: string;
  readonly versionId: string;
  readonly title: string;
}

export async function connectModelHome(config: ModelHomeConfig): Promise<ModelHome> {
  if (config.sdlc !== 'page') {
    const depot = config.depot ?? config.sdlc.replace(/\/sdlc\/api\/?$/, '/depot/api');
    // a server's model home reads its releases itself (`--releases DIR`): the page reads their index, to know them
    const base = config.releases === undefined ? undefined
      : new URL(config.releases.endsWith('/') ? config.releases : `${config.releases}/`, globalThis.location?.href);
    const released = base === undefined ? [] : (await releasesIndex(base, async (url) => {
      const answered = await fetch(url.href, { cache: 'no-cache' });
      if (!answered.ok) throw new Error(`the site's released versions: ${url.href} answered ${answered.status}`);
      return answered.text();
    })).map(refOf);
    return { client: new SdlcClient(config.sdlc), depot: new DepotClient(depot), where: config.sdlc, released };
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
  const released = config.releases === undefined ? [] : await releasedVersions(config.releases);
  const server = await wasmSdlcServer(module, new BrowserRecords(), config.user ?? { userId: 'local', name: 'Local User' }, released);
  return {
    client: new SdlcClient(WASM_API, server.fetch),
    depot: new DepotClient(WASM_DEPOT_API, server.depotFetch),
    where: 'this browser',
    released: released.map(refOf),
  };
}
