// DataCube's side of an embedding host: Python's V8, where DataCube's non-screen code runs with no browser and no
// WebAssembly (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). The host answers its calls (calls.ts), each
// asynchronous (a call out of the engine is: mini-racer's only kind), and this module turns them into what DataCube's code
// already takes: a planner worker (engine-client's planner-protocol.ts, answered by legend-lite's native library) and
// the host's database as DataCube's data flows ask of it (data-host.ts DataHost; table-output.ts's write) -- intent,
// each ask planned and run by the host (the #6 bridge, 2026-11-12: until then a DuckDB connection, which handed Python
// each statement DataCube's code had planned). Nothing here plans or queries.
//
// The host defines its calls as the global `legendLiteHost` BEFORE it loads a bundle, as a browser's globals are there
// before a page's scripts run: a module may ask the planner as it loads (a test's fixture, parsed at its top level).

import type { PlannerWorker, Request, Response } from '../../../engine-client/src/legend/planner-protocol.ts';
import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PythonLink } from '../services/python-host.ts';
import type { DataHost, Rows } from '../data-host.ts';
import { catalogColumnsRead } from '../upload.ts';
import type { TableWriteHost } from '../table-output.ts';
import { hostCalls, type HostCalls } from './calls.ts';
import { fromBase64 } from './web.ts';

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

/** The database Python's host runs DataCube's work on. */
const DATABASE_TYPE = 'DuckDB';

/**
 * THE HOST'S DATABASE AS DATACUBE'S DATA FLOWS ASK OF IT (data-host.ts): each ask one call of the host's -- a load, host
 * operations, a catalog -- planned by Python's planner and run by its runner on Python's DuckDB, the answer handed back
 * (a grid's rows are never asked here: the host's plan-and-run reads them, hostEngine: D16). A mount's settings of the
 * connection are Python's to keep and replay (its engine's frames): none is kept here.
 */
export function hostData(calls: HostCalls): DataHost {
  return {
    databaseType: DATABASE_TYPE,
    load: async (request, holds = {}) => {
      await calls.load(JSON.stringify(request), JSON.stringify(holds));
    },
    operations: async (operations) => JSON.parse(await calls.operations(DATABASE_TYPE, JSON.stringify(operations))) as Rows[],
    catalog: async (schema, table) => {
      const said = JSON.parse(await calls.catalog(schema, table)) as { readonly columns: readonly string[]; readonly rows: Rows };
      return catalogColumnsRead((name) => {
        const at = said.columns.indexOf(name);
        if (at < 0) throw new Error(`the catalog did not return ${name} — got ${said.columns.join(', ')}`);
        return said.rows.map((r) => r[at]);
      });
    },
    // Python lets a mount's settings go with its view (its engine's own forget)
    forget: () => {},
  };
}

/** A query or a program as a plan request, as DataCube's planner writes one (lite-planner.ts planRequest): what the
 *  host plans over, its own planner's to plan. */
export interface PlanRequest {
  readonly lambda: string;
  readonly model: string;
  readonly runtime: string;
  readonly library?: string;
}

/**
 * A WRITE ON PYTHON'S DATABASE (table-output.ts TableWriting): its transaction the host's operations (begun, committed,
 * rolled back), the table made by the host from the relation the writer chose and the rows' query (`rows`: as a plan
 * request over the box's model), the catalog read in the transaction, the program planned and run by the host in it
 * (`program`: as a plan request over the model the writer wrote) -- each a call, none a statement.
 */
export function hostTableWriting(calls: HostCalls, requests: {
  rows(query: Lambda): Promise<PlanRequest>;
  program(model: string, runtime: string, program: Lambda): Promise<PlanRequest>;
}): TableWriteHost['writing'] {
  const data = hostData(calls);
  return async (work) => {
    await data.operations([{ op: 'transaction', step: 'begin' }]);
    try {
      const done = await work({
        create: async (table, replace, rows) => {
          await calls.create(JSON.stringify({ op: 'relation', kind: 'table', target: { name: table }, replace }),
            JSON.stringify(await requests.rows(rows)));
        },
        catalog: (schema, table) => data.catalog(schema, table),
        run: async (model, runtime, program) => calls.write(JSON.stringify(await requests.program(model, runtime, program))),
      });
      await data.operations([{ op: 'transaction', step: 'commit' }]);
      return done;
    } catch (e) {
      await data.operations([{ op: 'transaction', step: 'rollback' }]).catch(() => {});
      throw e;
    }
  };
}

/**
 * A host call's failure in its database's own words (calls.ts `load`, `operations`, `create`, `write`): the host's exception is carried as
 * its traceback's text (`Error running Python function: ... duckdb.CatalogException: Catalog Error: ...`), its last line
 * the database's -- that line's message; any other failure, its message as it is.
 */
export function hostWords(e: unknown): string {
  const message = e instanceof Error ? e.message : String(e);
  if (!message.includes('Error running Python function')) return message;
  const last = message.split('\n').map((l) => l.trim()).filter((l) => /^[\w.]+(Error|Exception): /.test(l)).at(-1);
  return last === undefined ? message : last.replace(/^[\w.]+(Error|Exception): /, '');
}
