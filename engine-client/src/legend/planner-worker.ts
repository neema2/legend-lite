// legend-lite's planner (WebAssembly) in a worker: each request is answered by the module's
// export of the same name, with no logic of its own. The main thread's side is wasm-grammar.ts.

interface TeavmModule {
  readonly exports: {
    modelJsonOrError(text: string): string;
    lambdaJsonOrError(text: string): string;
    composeLambdaOrError(lambdaJson: string, style: string): string;
    relationTypeJsonOrError(model: string, lambdaJson: string): string;
    planJsonOrError(model: string, lambdaJson: string, runtime: string): string;
    warmModel(model: string): number;
    compileOrError(model: string): string;
  };
}

export type PlannerRequest =
  | { readonly kind: 'modelJson'; readonly text: string }
  | { readonly kind: 'lambdaJson'; readonly text: string }
  | { readonly kind: 'compose'; readonly lambda: string; readonly style: string }
  | { readonly kind: 'relationType'; readonly model: string; readonly lambda: string }
  | { readonly kind: 'plan'; readonly model: string; readonly lambda: string; readonly runtime: string }
  | { readonly kind: 'warm'; readonly model: string }
  // the whole model compiled, its errors as a list (Studio's live problems)
  | { readonly kind: 'compile'; readonly model: string };

export type PlannerMessage = PlannerRequest & { readonly id: number; readonly base: string };

export type PlannerResponse =
  | { readonly id: number; readonly ok: true; readonly answer: string }
  | { readonly id: number; readonly ok: false; readonly error: string };

let modulePromise: Promise<TeavmModule> | undefined;

async function load(base: string): Promise<TeavmModule> {
  const runtime = await import(/* @vite-ignore */ `${base}wasm-gc-module-runtime.js`) as {
    load(src: string, options?: unknown): Promise<TeavmModule>;
  };
  return runtime.load(`${base}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
}

function answer(m: TeavmModule, r: PlannerRequest): string {
  switch (r.kind) {
    case 'modelJson': return m.exports.modelJsonOrError(r.text);
    case 'lambdaJson': return m.exports.lambdaJsonOrError(r.text);
    case 'compose': return m.exports.composeLambdaOrError(r.lambda, r.style);
    case 'relationType': return m.exports.relationTypeJsonOrError(r.model, r.lambda);
    case 'plan': return m.exports.planJsonOrError(r.model, r.lambda, r.runtime);
    case 'warm': m.exports.warmModel(r.model); return 'OK\n';
    case 'compile': return m.exports.compileOrError(r.model);
  }
}

const scope = globalThis as unknown as {
  onmessage: ((e: MessageEvent<PlannerMessage>) => void) | null;
  postMessage(m: PlannerResponse): void;
};

scope.onmessage = async (e: MessageEvent<PlannerMessage>) => {
  const msg = e.data;
  try {
    if (!modulePromise) {
      modulePromise = load(msg.base);
      modulePromise.catch(() => { modulePromise = undefined; });
    }
    scope.postMessage({ id: msg.id, ok: true, answer: answer(await modulePromise, msg) });
  } catch (cause) {
    scope.postMessage({ id: msg.id, ok: false, error: cause instanceof Error ? cause.message : String(cause) });
  }
};
