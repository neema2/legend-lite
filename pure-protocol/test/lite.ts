// legend-lite's own parser and printer, for tests: the tab's WASM module (//wasm:planner), loaded
// directly -- the library under test depends on nothing but the wire.

import { fileURLToPath } from 'node:url';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

interface Module {
  readonly exports: {
    lambdaJsonOrError(text: string): string;
    composeLambdaOrError(json: string, style: string): string;
  };
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

function ok(answer: string): string {
  const nl = answer.indexOf('\n');
  if (answer.slice(0, nl) !== 'OK') throw new Error(answer);
  return answer.slice(nl + 1);
}

/** E1's twin: Pure text as lambda JSON, exactly as lite's parser writes it. */
export async function parse(text: string): Promise<string> {
  return ok((await load()).exports.lambdaJsonOrError(text));
}

/** E4's twin: lambda JSON printed as Pure text. */
export async function compose(json: string, style: 'STANDARD' | 'PRETTY' = 'STANDARD'): Promise<string> {
  return ok((await load()).exports.composeLambdaOrError(json, style));
}

/** The same two, synchronous once the module has loaded: for fixtures built at a module's top level. */
export async function ready(): Promise<{
  parse(text: string): string;
  compose(json: string, style?: 'STANDARD' | 'PRETTY'): string;
}> {
  const m = await load();
  return {
    parse: (text) => ok(m.exports.lambdaJsonOrError(text)),
    compose: (json, style = 'STANDARD') => ok(m.exports.composeLambdaOrError(json, style)),
  };
}
