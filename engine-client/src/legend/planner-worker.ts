// legend-lite's planner (WebAssembly) in a worker: each request is answered by the module's
// export of the same name, with no logic of its own (planner-answer.ts). The main thread's side is wasm-grammar.ts.

import { answer, type PlannerModule, type PlannerRequest } from './planner-answer.ts';

export type { PlannerRequest } from './planner-answer.ts';

export type PlannerMessage = PlannerRequest & { readonly id: number; readonly base: string };

export type PlannerResponse =
  | { readonly id: number; readonly ok: true; readonly answer: string }
  | { readonly id: number; readonly ok: false; readonly error: string };

let modulePromise: Promise<PlannerModule> | undefined;

async function load(base: string): Promise<PlannerModule> {
  const runtime = await import(/* @vite-ignore */ `${base}wasm-gc-module-runtime.js`) as {
    load(src: string, options?: unknown): Promise<PlannerModule>;
  };
  return runtime.load(`${base}classes.wasm`, {
    stackDeobfuscator: { enabled: false },
    installImports(imports: Record<string, unknown>) {
      imports.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
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
