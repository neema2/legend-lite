// legend-lite's planner (the //wasm:planner module) loaded in node: the same exports the page's
// worker calls, answered directly -- so tests exercise WasmGrammar exactly as the page does.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import type { PlannerPort } from '../../engine-client/src/legend/wasm-grammar.ts';
import { WasmGrammar } from '../../engine-client/src/legend/wasm-grammar.ts';
import type { PlannerRequest } from '../../engine-client/src/legend/planner-worker.ts';
import { ModelGraph } from '../src/model/graph.ts';
import type { PureModelContextText } from '../src/backend/wire.ts';
import { runfileDirUrl, runfileNamed } from '../../tools/js/runfiles.mts';

interface Module {
  readonly exports: Record<string, (...args: string[]) => string | number>;
}

const DIR = new URL(runfileDirUrl('WASM_PLANNER'));

let loaded: Promise<Module> | undefined;

function load(): Promise<Module> {
  loaded ??= (async () => {
    const runtime = await import(new URL('wasm-gc-module-runtime.js', DIR).href) as {
      load(src: string, options: unknown): Promise<Module>;
    };
    return runtime.load(fileURLToPath(new URL('classes.wasm', DIR)), {
      stackDeobfuscator: { enabled: false },
      installImports(i: Record<string, unknown>) {
        i.teavmConsole = { putcharStdout() {}, putcharStderr() {} };
      },
    });
  })();
  return loaded;
}

class DirectPort implements PlannerPort {
  async ask(r: PlannerRequest): Promise<string> {
    const e = (await load()).exports;
    switch (r.kind) {
      case 'modelJson': return e.modelJsonOrError!(r.text) as string;
      case 'lambdaJson': return e.lambdaJsonOrError!(r.text) as string;
      case 'compose': return e.composeLambdaOrError!(r.lambda, r.style) as string;
      case 'relationType': return e.relationTypeJsonOrError!(r.model, r.lambda) as string;
      case 'plan': return e.planJsonOrError!(r.model, r.lambda, r.runtime) as string;
      case 'warm': e.warmModel!(r.model); return 'OK\n';
      case 'compile': return e.compileOrError!(r.model) as string;
    }
  }
}

export const grammar = new WasmGrammar(new DirectPort());

/** The demo project's model (the domain, and its DuckDB runtime): its text, its model context, and its graph. */
export async function demoModel(): Promise<{ text: string; context: PureModelContextText; graph: ModelGraph }> {
  const text = ['trading.pure', 'runtime-duckdb.pure']
    .map((f) => readFileSync(runfileNamed('QUERY_MODELS', f), 'utf8'))
    .join('\n');
  return { text, context: { _type: 'text', code: text }, graph: new ModelGraph(await grammar.modelJson(text)) };
}
