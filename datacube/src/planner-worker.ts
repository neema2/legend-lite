// The planner module, on its own thread.
//
// Not an optimisation of last resort -- a correction. Building the
// boot layer (parsing the 300 KB Pure prelude, the system metamodel,
// then resolving and normalizing both) is ~600ms of SYNCHRONOUS work,
// and WebAssembly has no yield. On the main thread that is a 600ms
// freeze: DuckDB's own startup was measured slipping from 409ms to
// 1038ms purely because the planner was holding the thread, and
// starting the planner earlier made the page SLOWER, not faster.
// Two tasks do not overlap when one of them never yields.
//
// So the module lives here. The main thread stays free for DuckDB and
// for rendering, and the two startups genuinely run at once.
//
// This worker deliberately contains no planning logic of its own: it
// forwards to the module's `planOrError`, `relationTypeOrError`, `tableModelOrError`,
// `catalogColumnsSqlOrError`, `modelJsonOrError`, `testDataSqlOrError` and `warmModel` exports and
// returns what they say. A second planner is the one thing this whole
// design exists to avoid.

interface TeavmModule {
  readonly exports: {
    planOrError(model: string, query: string, runtime: string): string;
    relationTypeOrError(model: string, query: string): string;
    tableModelOrError(table: string): string;
    catalogColumnsSqlOrError(schema: string, table: string): string;
    planJsonOrError(model: string, lambdaJson: string, runtime: string): string;
    relationTypeJsonOrError(model: string, lambdaJson: string): string;
    composeLambdaOrError(lambdaJson: string, style: string): string;
    lambdaJsonOrError(text: string): string;
    modelJsonOrError(text: string): string;
    testDataSqlOrError(model: string, database: string, tablesJson: string): string;
    warmModel(model: string): number;
  };
}

/** What the main thread sends. */
export type Request =
  | { readonly id: number; readonly kind: 'warm'; readonly model: string }
  | {
    readonly id: number;
    readonly kind: 'plan';
    readonly model: string;
    readonly query: string;
    readonly runtime: string;
  }
  | { readonly id: number; readonly kind: 'relationType'; readonly model: string; readonly query: string }
  | { readonly id: number; readonly kind: 'tableModel'; readonly table: string }
  | { readonly id: number; readonly kind: 'catalogColumnsSql'; readonly schema: string; readonly table: string }
  | {
    readonly id: number;
    readonly kind: 'planJson';
    readonly model: string;
    readonly lambda: string;
    readonly runtime: string;
  }
  | { readonly id: number; readonly kind: 'relationTypeJson'; readonly model: string; readonly lambda: string }
  | { readonly id: number; readonly kind: 'compose'; readonly lambda: string; readonly style: string }
  | { readonly id: number; readonly kind: 'lambdaJson'; readonly text: string }
  | { readonly id: number; readonly kind: 'modelJson'; readonly text: string }
  | { readonly id: number; readonly kind: 'testData'; readonly model: string; readonly database: string; readonly tables: string };

/** What it gets back. `answer` is the export's raw tagged string. */
export type Response =
  | { readonly id: number; readonly ok: true; readonly answer: string }
  | { readonly id: number; readonly ok: false; readonly error: string };

let modulePromise: Promise<TeavmModule> | undefined;

async function load(base: string): Promise<TeavmModule> {
  const runtime = await import(
    /* @vite-ignore */ `${base}wasm-gc-module-runtime.js`
  ) as {
    load(src: string, options?: unknown): Promise<TeavmModule>;
  };
  return runtime.load(`${base}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      // Diagnostics arrive one character at a time; the messages that
      // matter come back through planOrError's return value.
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}

/** One request, answered by the module's export of the same name: no logic of its own. */
function answerOf(module: TeavmModule, msg: Request): string {
  switch (msg.kind) {
    case 'warm': module.exports.warmModel(msg.model); return 'OK\n';
    case 'relationType': return module.exports.relationTypeOrError(msg.model, msg.query);
    case 'tableModel': return module.exports.tableModelOrError(msg.table);
    case 'catalogColumnsSql': return module.exports.catalogColumnsSqlOrError(msg.schema, msg.table);
    case 'planJson': return module.exports.planJsonOrError(msg.model, msg.lambda, msg.runtime);
    case 'relationTypeJson': return module.exports.relationTypeJsonOrError(msg.model, msg.lambda);
    case 'compose': return module.exports.composeLambdaOrError(msg.lambda, msg.style);
    case 'lambdaJson': return module.exports.lambdaJsonOrError(msg.text);
    case 'modelJson': return module.exports.modelJsonOrError(msg.text);
    case 'testData': return module.exports.testDataSqlOrError(msg.model, msg.database, msg.tables);
    case 'plan': return module.exports.planOrError(msg.model, msg.query, msg.runtime);
  }
}

self.onmessage = async (e: MessageEvent<Request & { base?: string }>) => {
  const msg = e.data;
  try {
    if (!modulePromise) {
      const base = msg.base ?? './vendor/';
      modulePromise = load(base);
      modulePromise.catch(() => { modulePromise = undefined; });
    }
    const module = await modulePromise;
    const answer = answerOf(module, msg);
    const ok: Response = { id: msg.id, ok: true, answer };
    self.postMessage(ok);
  } catch (cause) {
    const bad: Response = {
      id: msg.id,
      ok: false,
      error: cause instanceof Error ? cause.message : String(cause),
    };
    self.postMessage(bad);
  }
};
