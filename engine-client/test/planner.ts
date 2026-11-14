// legend-lite's planner in this process, answering as the tab's worker does (WasmGrammar over the module's exports):
// what engine-client's tests ask for a project's database (C5) and a host's operations (D5).

import { fileURLToPath } from 'node:url';

import { WasmGrammar } from '../src/legend/wasm-grammar.ts';
import { answer, type PlannerModule } from '../src/legend/planner-answer.ts';
import type { RequestBody } from '../src/legend/planner-protocol.ts';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

/** The module from WASM_PLANNER (node_test's `wasm`), in-thread. */
export async function loadPlanner(): Promise<WasmGrammar> {
  const m = await loadPlannerModule();
  return new WasmGrammar({
    async ask(r: RequestBody): Promise<string> {
      return answer(m, r);
    },
  });
}

/** The module itself: a test that asks the planner protocol's kinds by hand (a model held, then planned). */
export async function loadPlannerModule(): Promise<PlannerModule> {
  const dir = new URL(runfileDirUrl('WASM_PLANNER'));
  const runtime = await import(new URL('wasm-gc-module-runtime.js', dir).href) as {
    load(src: string, options: unknown): Promise<PlannerModule>;
  };
  const m = await runtime.load(fileURLToPath(new URL('classes.wasm', dir)), {
    stackDeobfuscator: { enabled: false },
    installImports(i: Record<string, unknown>) {
      i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
    },
  });
  return m;
}
