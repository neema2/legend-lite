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
// forwards to the module's `planOrError` and `warmModel` exports and
// returns what they say. A second planner is the one thing this whole
// design exists to avoid.

interface TeavmModule {
  readonly exports: {
    planOrError(model: string, query: string, runtime: string): string;
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
  };

/** What it gets back. `answer` is planOrError's raw tagged string. */
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

self.onmessage = async (e: MessageEvent<Request & { base?: string }>) => {
  const msg = e.data;
  try {
    if (!modulePromise) {
      const base = msg.base ?? './vendor/';
      modulePromise = load(base);
      modulePromise.catch(() => { modulePromise = undefined; });
    }
    const module = await modulePromise;
    const answer = msg.kind === 'warm'
      ? (module.exports.warmModel(msg.model), 'OK\n')
      : module.exports.planOrError(msg.model, msg.query, msg.runtime);
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
