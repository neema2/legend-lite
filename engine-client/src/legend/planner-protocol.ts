// THE PLANNER PROTOCOL (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C2): what an app sends the worker holding legend-lite's
// planner (WebAssembly), and what it gets back -- ONE protocol for every app that plans in a tab (DataCube's
// LitePlanner, Query's and Studio's WasmGrammar) and every host that answers it: the tab's worker (planner-worker.ts,
// the module's exports through planner-answer.ts) and Python (legend_lite/_planner.py, the native library's functions),
// each kind answered by the same `Boundary` function in both (docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md §3). A
// kind added here fails both hosts at once: planner-answer.ts's switch by the type checker, Python's by its start-up
// check (legend_lite._headless.check_interface) and its lane's tests.

/** Every request kind, in one list: the start-up check compares it with what a host answers (the design's §7). */
export const REQUEST_KINDS = [
  'hold',
  'release',
  'plan',
  'relationType',
  'tableModel',
  'catalogColumnsSql',
  'planJson',
  'executionPlan',
  'planModel',
  'pureV1',
  'executePlan',
  'executeAnswer',
  'testData',
  'sessionSetup',
  'projectData',
  'hostOperations',
  'warm',
] as const;

export type RequestKind = (typeof REQUEST_KINDS)[number];

/** A request, as the planner writes it. */
export type RequestBody =
  // a model held: compiled the first time its text is, the answer its handle
  | { readonly kind: 'hold'; readonly model: string }
  | { readonly kind: 'release'; readonly handle: number }
  // a query's Pure text planned against a model held
  | { readonly kind: 'plan'; readonly handle: number; readonly query: string; readonly runtime: string }
  | { readonly kind: 'relationType'; readonly handle: number; readonly query: string }
  | { readonly kind: 'tableModel'; readonly table: string }
  | { readonly kind: 'catalogColumnsSql'; readonly schema: string; readonly table: string }
  // a query's protocol JSON planned against a model held
  | { readonly kind: 'planJson'; readonly handle: number; readonly lambda: string; readonly runtime: string }
  // a query's protocol JSON planned against a model held as an execution plan (PlanJson's text): a program that writes
  | { readonly kind: 'executionPlan'; readonly handle: number; readonly lambda: string; readonly runtime: string }
  // a query's protocol JSON planned against a model's text, none held (Query's and Studio's: each request its model)
  | { readonly kind: 'planModel'; readonly model: string; readonly lambda: string; readonly runtime: string }
  // one pure/v1 call: its path (`/api/pure/v1/...`), its raw query string ('' for none) and its body
  | { readonly kind: 'pureV1'; readonly path: string; readonly query: string; readonly body: string }
  // an ExecuteInput's plan half (its body) over the only models the host runs (a JSON array of their texts); then that
  // plan's answer from the rows the host's database answered running it, a JSON array of objects (HostRows)
  | { readonly kind: 'executePlan'; readonly body: string; readonly models: string }
  | { readonly kind: 'executeAnswer'; readonly plan: string; readonly rows: string }
  // a model's test data as the statements the server seeds DuckDB with, and each table's names as they spell it
  | { readonly kind: 'testData'; readonly model: string; readonly database: string; readonly tables: string }
  // what a session of a database runs before it is queried, so that it answers as the planner's SQL expects
  | { readonly kind: 'sessionSetup'; readonly databaseType: string }
  // a project's own database on a host's database of that type (C5), named by what it holds
  | { readonly kind: 'projectData'; readonly model: string; readonly databaseType: string }
  // a host's named operations on its own database of that type (D5): a JSON array of them, each answered its statements
  | { readonly kind: 'hostOperations'; readonly databaseType: string; readonly operations: string }
  // the planner's boot layer built for a model now, before a person waits on it
  | { readonly kind: 'warm'; readonly model: string };

/** A request as a worker receives it: its id, and where the tab's module lives (a host with its own ignores it). */
export type Request = RequestBody & { readonly id: number; readonly base?: string };

/** What it gets back. `answer` is the function's raw tagged string (`OK\n...` or `ERR\n<kind>\n<message>`). */
export type Response =
  | { readonly id: number; readonly ok: true; readonly answer: string }
  | { readonly id: number; readonly ok: false; readonly error: string };

/**
 * What a planner needs of a worker. The browser's `Worker` is one; a host that answers the same messages off the
 * planner's thread is another (DataCube's `LitePlannerOptions.worker`).
 */
export interface PlannerWorker {
  postMessage(message: Request): void;
  onmessage: ((e: { readonly data: Response }) => void) | null;
  onerror: ((e: { readonly message: string }) => void) | null;
  terminate(): void;
}

// the list and the requests name the same kinds: a kind in one and not the other is a compile error here
type Same<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false;
const sameKinds: Same<RequestBody['kind'], RequestKind> = true;
void sameKinds;
