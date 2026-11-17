// THE STORE IN THIS PAGE: upstream's `/api/pure/v1/query` answered without a server, by legend-lite's own rules
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
 * What the module exports (`com.legend.edge.queries.QueriesPage`, the edge's since 2026-11-17): the saved queries' calls, and DataCube's saved
 * pages' (`cubes`, the same module: datacube/src/cube-store.ts `wasmCubeServer`; the program's B6).
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
  /** One request to `LOCAL_API/pure/v1/query...`, answered as the server answers it. */
  handle(request: Request): Promise<Response>;
  /** The same, shaped as `fetch`: what the client is handed. */
  readonly fetch: typeof fetch;
}

const answer = (status: number, body: string): Response =>
  new Response(status === 204 ? null : body, { status, headers: { 'Content-Type': 'application/json' } });

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
    const at = url.pathname.indexOf('/pure/v1/query');
    if (at < 0) {
      return answer(404, JSON.stringify({ code: -1, message: `no such API in this page's query store: ${url.pathname}`, status: 'error' }));
    }
    const rest = url.pathname.slice(at + '/pure/v1/query'.length);
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
        return { changed: new Map(), answer: answer(500, JSON.stringify({ code: -1, message: `${error.name}: ${error.message}`, status: 'error' })) };
      }
      return { changed: new Map(Object.entries(said.changed)), answer: answer(said.status, said.body) };
    });
  };

  return {
    handle,
    fetch: ((input: RequestInfo | URL, init?: RequestInit) => handle(new Request(input, init))) as typeof fetch,
  };
}
