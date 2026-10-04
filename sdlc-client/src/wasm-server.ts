// THE SDLC IN THIS PAGE, compiled from Java (design S21; the spike of 2026-10-04): sdlc-server's rules
// (`com.legend.sdlc.Sdlc`) run in the page's WebAssembly module, so the page and the server answer from
// ONE implementation. This adapter is all the TypeScript there is: a `fetch` that becomes one `handle`
// call, and persistence -- the module keeps its records in memory, the page writes what each call
// changed to IndexedDB (records.ts) and loads them back when it opens.

import type { Records } from './records.ts';

/** The API root the page's SDLC answers at: never on the network. */
export const WASM_API = 'http://this-browser.invalid/sdlc/api';
/** The API root the page's Depot answers at (the same module, over the page's own versions). */
export const WASM_DEPOT_API = 'http://this-browser.invalid/depot/api';

/** What the module exports (`com.legend.sdlc.page.SdlcPage`). */
export interface SdlcModule {
  readonly exports: {
    start(userId: string, name: string): void;
    load(key: string, value: string): void;
    reset(): void;
    handle(method: string, target: string, body: string): string;
    handleDepot(method: string, target: string, body: string): string;
    changes(): string;
  };
}

/** One persisted record of the module's: its key and text. */
interface Kept {
  readonly key: string;
  readonly value: string;
}

/** The page's own record beside the rules' (which never use `page/`): a counter every write moves. */
const STAMP = 'page/stamp';
/** How many times a call is run again after other tabs' writes before it gives up. */
const RACES = 5;
const same = (a: Kept | undefined, b: Kept | undefined): boolean => a?.value === b?.value;

export interface WasmSdlc {
  /** The SDLC, at {@link WASM_API}. */
  readonly fetch: typeof fetch;
  /** Depot, at {@link WASM_DEPOT_API}. */
  readonly depotFetch: typeof fetch;
}

/**
 * The page's SDLC over a loaded module: what `records` holds is loaded into it first, and every call's
 * changes are written back before the call answers (so a reload sees a save that was answered).
 */
export async function wasmSdlcServer(module: SdlcModule, records: Records, user: { userId: string; name: string }): Promise<WasmSdlc> {
  const e = module.exports;
  /** The stamp of the records the module holds: every write moves it (see `one`). */
  let held: Kept | undefined;
  /** The module's memory made what the records hold, exactly. */
  const restore = async (): Promise<void> => {
    // the stamp read first: a write landing between the two reads leaves an OLD stamp over new records, which
    // the next call sees and restores again -- never a new stamp over old records
    held = await records.get<Kept>(STAMP);
    e.reset();
    for (const kept of await records.list<Kept>('')) if (kept.key !== STAMP) e.load(kept.key, kept.value);
    e.start(user.userId, user.name);
  };
  await restore();
  /** Set when the module could not be put back to what is stored: its memory can no longer be trusted. */
  let broken: unknown;
  const restoreOrBreak = async (): Promise<void> => {
    try {
      await restore();
    } catch (cause) {
      broken = cause;
    }
  };

  let calls: Promise<unknown> = Promise.resolve();
  const one = async (request: Request, api: string, handle: (method: string, target: string, body: string) => string): Promise<Response> => {
    const root = new URL(api);
    const url = new URL(request.url);
    if (url.origin !== root.origin || !url.pathname.startsWith(`${root.pathname}/`)) {
      return new Response(JSON.stringify({ code: 404, message: 'HTTP 404 Not Found' }), { status: 404 });
    }
    const target = url.pathname.slice(root.pathname.length) + url.search;
    const sent = await request.text();
    let answer = '';
    // Another tab of this origin holds its own module over the same records (re-review C). Each write moves the
    // stamp, and is made only if the stamp is still the one this module was loaded at; so this module first
    // catches up with what another tab wrote, and a write that lost the race is run again over the newer
    // records -- tabs then share the projects as two clients share a server (the revision lock included).
    for (let attempt = 0; ; attempt++) {
      if (broken !== undefined) {
        throw new Error('the page lost track of its stored projects: reload this page', { cause: broken });
      }
      if (attempt === RACES) throw new Error('another tab of this page keeps changing the projects: try again');
      if (!same(await records.get<Kept>(STAMP), held)) await restoreOrBreak();
      if (broken !== undefined) continue;
      answer = handle(request.method, target, sent);
      // the call's changes are stored together or not at all; if not, the module goes back to what IS stored, so its
      // memory never runs ahead of the records (a later save must not reference a commit that was never written)
      const changes: (readonly [string, Kept | null])[] = (JSON.parse(e.changes()) as [string, string | null][])
        .map(([key, value]) => [key, value === null ? null : { key, value }] as const);
      if (changes.length === 0) break;
      const stamp: Kept = { key: STAMP, value: String(Number(held?.value ?? 0) + 1) };
      let stored: boolean;
      try {
        stored = await records.apply([...changes, [STAMP, stamp]], { key: STAMP, expect: held });
      } catch (cause) {
        await restoreOrBreak();
        throw new Error(`the page could not store this change, so it was not made: ${cause instanceof Error ? cause.message : String(cause)}`,
          broken === undefined ? { cause } : { cause: broken });
      }
      if (stored) {
        held = stamp;
        break;
      }
      await restoreOrBreak();
    }
    const cut = answer.indexOf('\n');
    const status = Number(answer.slice(0, cut));
    const body = answer.slice(cut + 1);
    return new Response(status === 204 ? null : body, { status, headers: { 'Content-Type': 'application/json' } });
  };
  // one call at a time: the module is single-threaded and each call's changes are written before the next
  const serial = (request: Request, api: string, handle: (method: string, target: string, body: string) => string): Promise<Response> => {
    const next = calls.then(() => one(request, api, handle), () => one(request, api, handle));
    calls = next.catch(() => undefined);
    return next;
  };
  const sdlc = (m: string, t: string, b: string): string => e.handle(m, t, b);
  const depot = (m: string, t: string, b: string): string => e.handleDepot(m, t, b);
  return {
    fetch: (input, init) => serial(input instanceof Request ? input : new Request(input, init), WASM_API, sdlc),
    depotFetch: (input, init) => serial(input instanceof Request ? input : new Request(input, init), WASM_DEPOT_API, depot),
  };
}
