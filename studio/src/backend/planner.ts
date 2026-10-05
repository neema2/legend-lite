// The compiler, in the tab: legend-lite's planner (WebAssembly) behind a port -- a worker in the page,
// the module called directly in tests. Two questions Studio asks it: what element a file's text is
// (its grammarToJson), and whether a whole model compiles (the server's compilation/compile).

import type { PlannerPort } from '../../../engine-client/src/legend/wasm-grammar.ts';

// the port and its worker are the in-tab engine's, shared with Query (engine-client/src/legend/)
export { WorkerPort, type PlannerPort } from '../../../engine-client/src/legend/wasm-grammar.ts';

/** A refusal from the compiler: its own words. */
export class CompilerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CompilerError';
  }
}

/** An export's folded answer (`OK\n<json>` / `ERR\n<class>\n<message>`) as its value or a refusal. */
function unfold(answer: string): string {
  if (answer.startsWith('OK\n')) return answer.slice(3);
  const [, kind = '', ...rest] = answer.split('\n');
  throw new CompilerError(rest.join('\n') || kind);
}

/** One element read from a file's text. */
export interface ElementOf {
  readonly path: string;
  readonly type: string;
}

export class Compiler {
  readonly #port: PlannerPort;

  constructor(port: PlannerPort) {
    this.#port = port;
  }

  /** The elements a text declares (its section index left out), or the parser's refusal. */
  async elements(text: string): Promise<ElementOf[]> {
    const pmcd = JSON.parse(unfold(await this.#port.ask({ kind: 'modelJson', text }))) as {
      elements: { _type: string; package: string; name: string }[];
    };
    return pmcd.elements.filter((e) => e._type !== 'sectionIndex').map((e) => ({ path: `${e.package}::${e.name}`, type: e._type }));
  }

  /**
   * The model's compile errors: [] when it compiles. The first element error stops the compile (as the
   * server's); body errors are all collected.
   */
  async compile(model: string): Promise<string[]> {
    const answer = await this.#port.ask({ kind: 'compile', model });
    if (answer.startsWith('OK\n')) return JSON.parse(answer.slice(3)) as string[];
    const [, kind = '', ...rest] = answer.split('\n');
    return [rest.join('\n') || kind];
  }

  /** Builds what a first compile needs while the page is still busy with other things. */
  async warm(): Promise<void> {
    await this.#port.ask({ kind: 'warm', model: '' });
  }
}
