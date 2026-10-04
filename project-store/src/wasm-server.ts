// THE SDLC IN THIS PAGE, compiled from Java (design S21; the spike of 2026-10-04): sdlc-server's rules
// (`com.legend.sdlc.Sdlc`) run in the page's WebAssembly module, so the page and the server answer from
// ONE implementation. This adapter is all the TypeScript there is: a `fetch` that becomes one `handle`
// call, and persistence -- the module keeps its records in memory, the page writes what each call
// changed to IndexedDB (records.ts) and loads them back when it opens.

import type { Records } from './records.ts';

/** The API root the page's SDLC answers at: never on the network. */
export const WASM_API = 'http://this-browser.invalid/sdlc/api';

/** What the module exports (`com.legend.sdlc.page.SdlcPage`). */
export interface SdlcModule {
  readonly exports: {
    start(userId: string, name: string): void;
    load(key: string, value: string): void;
    reset(): void;
    handle(method: string, target: string, body: string): string;
    changes(): string;
  };
}

/** One persisted record of the module's: its key and text. */
interface Kept {
  readonly key: string;
  readonly value: string;
}

export interface WasmSdlc {
  readonly fetch: typeof fetch;
}

/**
 * The page's SDLC over a loaded module: what `records` holds is loaded into it first, and every call's
 * changes are written back before the call answers (so a reload sees a save that was answered).
 */
export async function wasmSdlcServer(module: SdlcModule, records: Records, user: { userId: string; name: string }): Promise<WasmSdlc> {
  const e = module.exports;
  e.reset();
  for (const kept of await records.list<Kept>('')) e.load(kept.key, kept.value);
  e.start(user.userId, user.name);

  const root = new URL(WASM_API);
  let calls: Promise<unknown> = Promise.resolve();
  const one = async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    if (url.origin !== root.origin || !url.pathname.startsWith(`${root.pathname}/`)) {
      return new Response(JSON.stringify({ code: 404, message: 'HTTP 404 Not Found' }), { status: 404 });
    }
    const target = url.pathname.slice(root.pathname.length) + url.search;
    const answer = e.handle(request.method, target, await request.text());
    for (const [key, value] of JSON.parse(e.changes()) as [string, string | null][]) {
      if (value === null) await records.delete(key);
      else await records.put(key, { key, value } satisfies Kept);
    }
    const cut = answer.indexOf('\n');
    const status = Number(answer.slice(0, cut));
    const body = answer.slice(cut + 1);
    return new Response(status === 204 ? null : body, { status, headers: { 'Content-Type': 'application/json' } });
  };
  // one call at a time: the module is single-threaded and each call's changes are written before the next
  const serial = (request: Request): Promise<Response> => {
    const next = calls.then(() => one(request), () => one(request));
    calls = next.catch(() => undefined);
    return next;
  };
  return { fetch: (input, init) => serial(input instanceof Request ? input : new Request(input, init)) };
}
