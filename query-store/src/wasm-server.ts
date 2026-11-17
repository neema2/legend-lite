// THE STORE IN THIS PAGE: lite's `/api/lite/v1/queries` (and `user`; lite's API v1 spec §2 and §7, H6, 2026-11-17: until
// then upstream's `/api/pure/v1/query`, which is the edge's) answered without a server, by legend-lite's own rules
// (core's `com.legend.queries.SavedQueries`, the ones its server and Python's native library answer with) compiled to
// WebAssembly (//query-store:page), over the page's own records (records.ts: IndexedDB, so every app on this origin
// shares them) -- one implementation, only the storage the page's (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5). Each
// call hands the module every record and writes back every query it wrote; the module keeps nothing between calls, so
// two tabs over the one IndexedDB see each other's queries at their next call; each call is one transaction of the
// records, so no tab writes between another's read and its writes. It is handed to the one client
// (client.ts) in place of the network, and the one wire-level suite (test/conformance.ts) runs against it and against
// legend-lite's server alike.
//
// What it cannot be is a server: its queries live in this browser only, and its user is the name the page was
// configured with, not someone signed in.

import type { Records } from './records.ts';

/** The API root the page's store answers at: never on the network (`fetch` below answers it). */
export const LOCAL_API = 'http://this-browser.invalid/api';

/**
 * What the module exports (`com.legend.queries.page.QueriesPage`, the product's: lite's words, H6, 2026-11-17): the saved
 * queries' calls, and DataCube's saved pages' (`cubes`, the same module: datacube/src/cube-store.ts `wasmCubeServer`;
 * the program's B6).
 */
export interface QueriesModule {
  readonly exports: {
    answer(method: string, rest: string, rawQuery: string, body: string, user: string, records: string): string;
    cubes(method: string, rest: string, rawQuery: string, body: string, user: string, rows: string): string;
  };
}

/** The module, from the directory holding its `classes.wasm` and TeaVM's runtime (`<vendor>queries/`). */
export async function loadQueriesModule(base: string, load?: (runtimeUrl: string) => Promise<unknown>): Promise<QueriesModule> {
  const dir = base.endsWith('/') ? base : `${base}/`;
  const runtimeUrl = `${dir}wasm-gc-module-runtime.js`;
  const runtime = (load ? await load(runtimeUrl) : await import(/* @vite-ignore */ runtimeUrl)) as {
    load(src: string, options?: unknown): Promise<QueriesModule>;
  };
  return runtime.load(`${dir}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}

export interface WasmServerOptions {
  /** The module, fetched at the store's first call (never at the page's start): a page that never asks never loads it. */
  readonly load: () => Promise<QueriesModule>;
  readonly records: Records;
  /** Who this page's caller is: the page's configured user (there is no sign-in here). */
  readonly user: string;
}

export interface LocalServer {
  /** One request to `LOCAL_API/lite/v1/queries...` or `LOCAL_API/lite/v1/user`, answered as the server answers it. */
  handle(request: Request): Promise<Response>;
  /** The same, shaped as `fetch`: what the client is handed. */
  readonly fetch: typeof fetch;
}

const answer = (status: number, body: string): Response =>
  new Response(status === 204 ? null : body, { status, headers: { 'Content-Type': 'application/json' } });

/** Lite's refusal (spec §3): `{kind, message}`, the store's status kept (LegendHttpServer.storeRoutes', on the server;
 *  a store's not-found stays 404, kind request, pending the user's word on a notFound kind). */
const refusal = (status: number, message: string): Response =>
  answer(status, JSON.stringify({ kind: status >= 500 ? 'internal' : 'request', message }));

/** The store's own answer: a refusal (400 and up) as lite's, its words the store's message; anything else as it is. */
function stored(status: number, body: string): Response {
  if (status < 400) return answer(status, body);
  let message = body;
  try {
    const said = JSON.parse(body) as { readonly message?: unknown };
    if (typeof said.message === 'string') message = said.message;
  } catch { /* not JSON: the text is the store's words */ }
  return refusal(status, message);
}

/** Where lite's store answers under the API root (spec §2), and its user. */
const QUERIES = '/lite/v1/queries';
const USER = '/lite/v1/user';

/** The records as the module reads them, `{"id": [versions...]}`, from each query's text as kept. */
const recordsJson = (held: ReadonlyMap<string, string>): string =>
  `{${[...held].map(([id, text]) => `${JSON.stringify(id)}:${text}`).join(',')}}`;

export function wasmQueryServer(options: WasmServerOptions): LocalServer {
  const { records, user } = options;
  // once, at the first call; a load that failed is tried again at the next, as a server not reached is
  let loaded: Promise<QueriesModule> | undefined;
  const module = (): Promise<QueriesModule> => (loaded ??= options.load().catch((e: unknown) => {
    loaded = undefined;
    throw e;
  }));

  // ONE CALL AT A TIME, as the server's `synchronized` methods: each call is one transaction of the records (records.ts),
  // which no other call, in this tab or another, interleaves with
  const handle = async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    // lite's user operation (spec §2): the page's configured user, the store's; a request holding a key refused, as
    // the server's
    if (url.pathname.endsWith(USER)) {
      if (request.method !== 'POST') return refusal(400, `lite's user operation is POST /api${USER}, not ${request.method} ${url.pathname}`);
      const asked = await request.text();
      let keys: string[];
      try {
        const o: unknown = JSON.parse(asked === '' ? '{}' : asked);
        if (o === null || typeof o !== 'object' || Array.isArray(o)) return refusal(400, `a user request is a JSON object: ${asked}`);
        keys = Object.keys(o);
      } catch (e) {
        return refusal(400, `a user request is a JSON object: ${(e as Error).message}`);
      }
      if (keys.length > 0) return refusal(400, `a user request has no key ${keys[0]} (it holds none)`);
      return answer(200, JSON.stringify({ user }));
    }
    const at = url.pathname.indexOf(QUERIES);
    if (at < 0) return refusal(404, `no such legend-lite API: ${url.pathname}`);
    const rest = url.pathname.slice(at + QUERIES.length);
    const body = request.method === 'GET' || request.method === 'DELETE' ? '' : await request.text();
    const queries = await module();
    return records.call((held) => {
      let said: { status: number; body: string; changed: Record<string, string> };
      try {
        said = JSON.parse(queries.exports.answer(request.method, rest, url.search.replace(/^\?/, ''), body, user,
          recordsJson(held))) as typeof said;
      } catch (e) {
        // what the rules answer of a body or records they cannot read is an exception out of the module (and a bug
        // in them a trap): the engine's 500, naming it, as the server answers its own -- and nothing written
        const error = e instanceof Error ? e : new Error(String(e));
        return { changed: new Map(), answer: refusal(500, `${error.name}: ${error.message}`) };
      }
      return { changed: new Map(Object.entries(said.changed)), answer: stored(said.status, said.body) };
    });
  };

  return {
    handle,
    fetch: ((input: RequestInfo | URL, init?: RequestInit) => handle(new Request(input, init))) as typeof fetch,
  };
}
