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
  'queryPlanHeld',
  'queryTypeHeld',
  'tableModel',
  'catalogColumnsSql',
  'pureV1',
  'modelCheck',
  'modelElements',
  'queryParse',
  'queryPrint',
  'queryType',
  'queryPlan',
  'modelCreateClass',
  'modelCreateAssociation',
  'modelCreateDataSpace',
  'tsItem',
  'tsStatement',
  'executePlan',
  'executeAnswer',
  'fileLoad',
  'remoteMount',
  'snapStream',
  'snapCopy',
  'outputPlan',
  'synthesize',
  'authored',
  'sessionSetup',
  'projectData',
  'hostOperations',
  'warm',
] as const;

export type RequestKind = (typeof REQUEST_KINDS)[number];

/** Lite API v1's operations, as planner kinds (operation `a/b` is kind `aB`: the spec's naming rule). */
export const LITE_OPERATIONS = ['modelCheck', 'modelElements', 'queryParse', 'queryPrint', 'queryType', 'queryPlan', 'modelCreateClass', 'modelCreateAssociation', 'modelCreateDataSpace', 'tsItem', 'tsStatement'] as const;

export type LiteOperation = (typeof LITE_OPERATIONS)[number];

/** A request, as the planner writes it. */
export type RequestBody =
  // a model held: compiled the first time its text is, the answer its handle
  // `library`: a library the model is compiled over, held by the module as a layer of its own, compiled once for every
  // model held over it (core's Compiler.compileModel(model, library); docs/TS_V1_PROGRAM_2026_11_01.md A5)
  | { readonly kind: 'hold'; readonly model: string; readonly library?: string }
  | { readonly kind: 'release'; readonly handle: number }
  // lite API v1's query/plan and query/type over a model held (§7a: a held model is the binding's cache): the
  // operation's request without its model, {query, runtime} and {query}
  | { readonly kind: 'queryPlanHeld' | 'queryTypeHeld'; readonly handle: number; readonly request: string }
  | { readonly kind: 'tableModel'; readonly table: string }
  | { readonly kind: 'catalogColumnsSql'; readonly schema: string; readonly table: string }
  // one pure/v1 call: its path (`/api/pure/v1/...`), its raw query string ('' for none) and its body
  | { readonly kind: 'pureV1'; readonly path: string; readonly query: string; readonly body: string }
  // lite API v1's operations (docs/records/lite-api-v1-2026-11-17.md §2, the user's go of 2026-11-17): each its
  // operation's request JSON, answered by the boundary's member of the same name with its answer JSON -- the same JSON the
  // server's /api/lite/v1 routes take and answer
  | { readonly kind: LiteOperation; readonly request: string }
  // an ExecuteInput's plan half (its body) over the only models the host runs (a JSON array of their texts); then that
  // plan's answer from the rows the host's database answered running it, a JSON array of objects (HostRows)
  | { readonly kind: 'executePlan'; readonly body: string; readonly models: string }
  | { readonly kind: 'executeAnswer'; readonly plan: string; readonly rows: string }
  // a host's own file read into its database, as a plan (B3): the request's JSON (Boundary.fileLoad)
  | { readonly kind: 'fileLoad'; readonly request: string }
  // a remote file mounted, as a plan (B3's Q3): the request's JSON, names and fields only (Boundary.remoteMount)
  | { readonly kind: 'remoteMount'; readonly request: string }
  // a snapshot from a remote plane, as a plan (the user's ruling of 2026-11-11): the request's JSON, the plane by name,
  // the pull the planner plans in the call -- never a statement (the user's ruling of 2026-11-12) -- and the table it
  // lands in (Boundary.snapStream)
  | { readonly kind: 'snapStream'; readonly request: string }
  // a snapshot's local copy, as a plan (the final audit's item 1(b), 2026-11-12): the request's JSON, the pull the
  // planner plans in the call and the table it is copied into -- never a statement (Boundary.snapCopy)
  | { readonly kind: 'snapCopy'; readonly request: string }
  // a table Output, as one plan (the final audit's item 2, 2026-11-12): the request's JSON, the box's tables, the
  // output, its rows as a lambda -- never a statement (Boundary.outputPlan)
  | { readonly kind: 'outputPlan'; readonly request: string }
  // rows the database generates into a declared table, as a plan (SYNTHESIZE, the user's ruling of 2026-11-12): the
  // request's JSON, the table and its knobs, never a row (Boundary.synthesize)
  | { readonly kind: 'synthesize'; readonly request: string }
  // a statement a person wrote, as a plan (Studio's SQL playground, the user's ruling of 2026-11-12): the request's JSON,
  // the database and the person's one text, carried as written (Boundary.authored)
  | { readonly kind: 'authored'; readonly request: string }
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
