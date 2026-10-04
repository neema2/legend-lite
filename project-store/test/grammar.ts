// legend-lite's grammar as the page has it: the //wasm:planner module loaded in node, its
// `modelJsonOrError` export (the engine's grammarToJson) answered directly -- so the page's SDLC is
// tested with the very grammar it derives entities with in a browser.

import { fileURLToPath } from 'node:url';

import type { ModelReader } from '../src/local-server.ts';

interface Module {
  readonly exports: Record<string, (...args: string[]) => string | number>;
}

const DIR = new URL('../../wasm/planner/', import.meta.url);

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

/** `OK\n<json>` → the elements; `ERR\n<class>\n<message>` → an Error carrying the message. */
export const wasmGrammar: ModelReader = {
  async modelJson(text: string) {
    const answer = (await load()).exports.modelJsonOrError!(text) as string;
    if (answer.startsWith('OK\n')) return JSON.parse(answer.slice(3)) as { elements: Record<string, unknown>[] };
    const [, , ...message] = answer.split('\n');
    throw new Error(message.join('\n'));
  },
};
