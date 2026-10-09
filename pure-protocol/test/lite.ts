// legend-lite's own parser and printer, for tests: the tab's WASM module (//wasm:planner), loaded
// directly and asked legend-engine's pure/v1, as the tab asks it -- the library under test depends on nothing but the wire.

import { fileURLToPath } from 'node:url';
import { runfileDirUrl } from '../../tools/js/runfiles.mts';

interface Module {
  readonly exports: {
    pureV1OrError(path: string, rawQuery: string, body: string): string;
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

/** One pure/v1 call's body (`OK\n<status>\n<type>\n<body>`), refused unless it is a 200. */
function pureV1(m: Module, path: string, query: string, body: string): string {
  const answer = m.exports.pureV1OrError(path, query, body);
  const first = answer.indexOf('\n');
  const second = answer.indexOf('\n', first + 1);
  const third = answer.indexOf('\n', second + 1);
  if (first < 0 || second < 0 || third < 0
      || answer.slice(0, first) !== 'OK' || answer.slice(first + 1, second) !== '200') {
    throw new Error(answer);
  }
  return answer.slice(third + 1);
}

/** E1: Pure text as lambda JSON, exactly as lite's parser writes it (no source information). */
function parseWith(m: Module, text: string): string {
  return pureV1(m, '/api/pure/v1/grammar/grammarToJson/lambda', 'returnSourceInformation=false', text);
}

/** E4: lambda JSON printed as Pure text. */
function composeWith(m: Module, json: string, style: 'STANDARD' | 'PRETTY'): string {
  return pureV1(m, '/api/pure/v1/grammar/jsonToGrammar/lambda', `renderStyle=${style}`, json);
}

export async function parse(text: string): Promise<string> {
  return parseWith(await load(), text);
}

export async function compose(json: string, style: 'STANDARD' | 'PRETTY' = 'STANDARD'): Promise<string> {
  return composeWith(await load(), json, style);
}

/** The same two, synchronous once the module has loaded: for fixtures built at a module's top level. */
export async function ready(): Promise<{
  parse(text: string): string;
  compose(json: string, style?: 'STANDARD' | 'PRETTY'): string;
}> {
  const m = await load();
  return {
    parse: (text) => parseWith(m, text),
    compose: (json, style = 'STANDARD') => composeWith(m, json, style),
  };
}
