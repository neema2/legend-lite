// DataCube's side of an embedding host: Python's V8, where DataCube's non-screen code runs with no browser and no
// WebAssembly (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). The host answers four calls, each asynchronous
// (a call out of the engine is: mini-racer's only kind), and this module turns them into what DataCube's code already
// takes: a planner worker (engine-client's planner-protocol.ts, answered by legend-lite's native library) and a DuckDB connection
// (engine-client's ArrowishConnection, answered by Python's DuckDB as Arrow IPC). Nothing here plans or queries.
//
// The host defines its calls as the global `legendLiteHost` BEFORE it loads a bundle, as a browser's globals are there
// before a page's scripts run: a module may ask the planner as it loads (a test's fixture, parsed at its top level).

import { arrowIpcTable } from '../../../engine-client/src/arrow-ipc.ts';
import type { ArrowishConnection, ArrowishTable } from '../../../engine-client/src/duckdb.ts';
import type { PlannerWorker, Request, Response } from '../../../engine-client/src/legend/planner-protocol.ts';
import type { PythonLink } from '../services/python-host.ts';
import { hostCalls, type HostCalls } from './calls.ts';
import { base64, fromBase64 } from './web.ts';

export { hostCalls, type HostCalls };

/**
 * A planner worker whose messages the host answers, off the engine's thread, as planner-worker.ts answers them in a
 * tab. A failure of the call itself (the host's, not the planner's refusal, which comes back as an answer) is the
 * worker's death, as a crashed tab worker's is: every request in flight fails, and the planner starts another.
 */
export function hostWorker(calls: HostCalls): PlannerWorker {
  let alive = true;
  const worker: PlannerWorker = {
    onmessage: null,
    onerror: null,
    postMessage(message: Request): void {
      calls.planner(JSON.stringify(message)).then(
        (answer) => {
          if (alive) worker.onmessage?.({ data: JSON.parse(answer) as Response });
        },
        (e: unknown) => {
          if (!alive) return;
          alive = false;
          worker.onerror?.({ message: e instanceof Error ? e.message : String(e) });
        },
      );
    },
    terminate(): void {
      alive = false;
    },
  };
  return worker;
}

/**
 * THE FETCH THE HOST ANSWERS (calls.ts `fetch`): what a client of a service takes -- the SDLC client, the model writer
 * (model-changes/) -- its calls carried by the host, each answer's status and text as a browser's Response gives them.
 */
export function hostFetch(calls: HostCalls): typeof fetch {
  const carried = async (input: unknown, init?: { readonly method?: string; readonly body?: unknown }): Promise<unknown> => {
    const body = init?.body;
    if (body !== undefined && body !== null && typeof body !== 'string') throw new Error("the host's fetch carries a body of text");
    if (typeof input !== 'string') throw new Error("the host's fetch is asked by an address, as text");
    const said = JSON.parse(await calls.fetch(init?.method ?? 'GET', input, body ?? null)) as { status: number; body: string };
    return {
      ok: said.status >= 200 && said.status < 300,
      status: said.status,
      statusText: '',
      text: async () => said.body,
      json: async () => JSON.parse(said.body) as unknown,
    };
  };
  return carried as unknown as typeof fetch;
}

/**
 * THE ENGINE THE HOST RUNS BESIDE DATACUBE, as a tab on it reaches it (services/python-host.ts PythonLink): its calls
 * carried by the host (calls.ts `engine`) -- a grid's query planned and run where its rows are, a warehouse table opened
 * under the session it holds -- each answer's status, media type, text and bytes as a browser's Response gives them. So
 * every grid of a page on the host reads its rows by the tab's own runner (pythonRunner), whatever its base: the one rows
 * path (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D16).
 */
export function hostEngine(calls: HostCalls): PythonLink {
  const carried = async (input: unknown, init?: { readonly method?: string; readonly body?: unknown }): Promise<unknown> => {
    const body = init?.body;
    if (typeof body !== 'string') throw new Error("the engine's calls carry a body of text");
    if (typeof input !== 'string' || !input.startsWith('/')) throw new Error(`the engine is asked at a path of its own, not ${String(input)}`);
    const said = JSON.parse(await calls.engine(input, body)) as { status: number; contentType: string; body: string };
    const bytes = fromBase64(said.body);
    return {
      ok: said.status >= 200 && said.status < 300,
      status: said.status,
      statusText: '',
      headers: { get: (name: string) => (name.toLowerCase() === 'content-type' ? said.contentType : null) },
      text: async () => new TextDecoder().decode(bytes),
      json: async () => JSON.parse(new TextDecoder().decode(bytes)) as unknown,
      arrayBuffer: async () => bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength),
    };
  };
  // its paths its own (`/plan-and-run.json`): no address before them
  return { baseUrl: '', fetch: carried as unknown as typeof fetch };
}

/** What a host's database gives beside its connection: DuckDB-WASM's file registration, the host's folder behind it. */
export interface HostDatabase {
  readonly connection: ArrowishConnection;
  registerFileBuffer(name: string, bytes: Uint8Array): Promise<void>;
  registerFileText(name: string, text: string): Promise<void>;
}

/**
 * The host's database as DataCube's engine takes one: for what DataCube writes into it (a page's file read in, a remote
 * file's view, a snap's copy modelled) and its catalog -- never a grid's rows, which the host's plan-and-run reads
 * (hostEngine: D16). Each query is one whole call (no `send`: the host's DuckDB
 * answers a statement at once, so a superseded query is not cancelled between batches); its Arrow IPC read by
 * apache-arrow, as DuckDB-WASM's answers are.
 */
export function hostDatabase(calls: HostCalls): HostDatabase {
  const connection: ArrowishConnection = {
    async query(sql: string): Promise<ArrowishTable> {
      return arrowIpcTable(fromBase64(await calls.query(sql)));
    },
    // a write's step, its values bound by the host's database (table-output.ts)
    prepare: (sql: string) => ({
      query: async (...params: unknown[]): Promise<ArrowishTable> => arrowIpcTable(fromBase64(await calls.statement(sql, JSON.stringify(params)))),
    }),
  };
  return {
    connection,
    registerFileBuffer: (name, bytes) => calls.registerFile(name, base64(bytes)),
    registerFileText: (name, text) => calls.registerFile(name, base64(new TextEncoder().encode(text))),
  };
}

/**
 * A host call's failure in its database's own words (calls.ts `query`, `statement`): the host's exception is carried as
 * its traceback's text (`Error running Python function: ... duckdb.CatalogException: Catalog Error: ...`), its last line
 * the database's -- that line's message; any other failure, its message as it is.
 */
export function hostWords(e: unknown): string {
  const message = e instanceof Error ? e.message : String(e);
  if (!message.includes('Error running Python function')) return message;
  const last = message.split('\n').map((l) => l.trim()).filter((l) => /^[\w.]+(Error|Exception): /.test(l)).at(-1);
  return last === undefined ? message : last.replace(/^[\w.]+(Error|Exception): /, '');
}
