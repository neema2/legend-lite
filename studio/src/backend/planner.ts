// The compiler Studio asks two questions (plan A1): what element a file's text is (its grammarToJson), and whether a
// whole model compiles (compilation/compile). Whatever answers is the session's engine: the in-tab one --
// legend-lite's planner in a worker (engine-client's WasmGrammar), the default -- or a legend server over pure/v1
// (lite's, or legend-engine: engine-client's HttpEngine). One interface, so Studio is the same over each.

import type { PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';

// the port and its worker are the in-tab engine's, shared with Query (engine-client/src/legend/)
export { WorkerPort, type PlannerPort } from '../../../engine-client/src/legend/wasm-grammar.ts';

/** What Studio needs of an engine: the in-tab WasmGrammar and a server's HttpEngine both answer it. */
export interface ModelCompiler {
  /** `grammarToJson/model`: the elements a text declares, or the parser's refusal (thrown). */
  modelJson(text: string): Promise<PureModelContextData>;
  /** [] when the model compiles, else its errors: every one from legend-lite (in the tab or a server), legend-engine's first. */
  compileErrors(code: string): Promise<string[]>;
}

/** One element read from a file's text. */
export interface ElementOf {
  readonly path: string;
  readonly type: string;
}

export class Compiler {
  readonly #engine: ModelCompiler;
  readonly #warm: (() => Promise<void>) | undefined;

  /** `warm`: what builds a first compile's state while the page is busy (the in-tab engine has one). */
  constructor(engine: ModelCompiler, warm?: () => Promise<void>) {
    this.#engine = engine;
    this.#warm = warm;
  }

  /** The elements a text declares (its section index left out), or the parser's refusal. */
  async elements(text: string): Promise<ElementOf[]> {
    const pmcd = await this.#engine.modelJson(text);
    return pmcd.elements.filter((e) => e._type !== 'sectionIndex').map((e) => ({ path: `${e.package}::${e.name}`, type: e._type }));
  }

  /** The model's compile errors: [] when it compiles. */
  compile(model: string): Promise<string[]> {
    return this.#engine.compileErrors(model);
  }

  /** Builds what a first compile needs while the page is still busy with other things. */
  async warm(): Promise<void> {
    await this.#warm?.();
  }
}
