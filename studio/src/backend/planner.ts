// The compiler Studio asks two questions (plan A1): what element a file's text is (its grammarToJson), and whether a
// whole model compiles (compilation/compile). Whatever answers is the session's engine: the in-tab one --
// legend-lite's planner in a worker (engine-client's WasmGrammar), the default -- or a legend-lite server over its
// /api/lite/v1 (engine-client's LiteServer; the edge program's H7, 2026-11-17). One interface, so Studio is the same
// over each.

import type { PureModelContextData } from '../../../engine-client/src/legend/pmcd.ts';
import type { LiteError } from '../../../engine-client/src/lite-error.ts';

// the port and its worker are the in-tab engine's, shared with Query (engine-client/src/legend/)
export { WorkerPort, type PlannerPort } from '../../../engine-client/src/legend/wasm-grammar.ts';

/** What Studio needs of an engine: the in-tab WasmGrammar and a server's LiteServer both answer it. */
export interface ModelCompiler {
  /** lite's `model/elements`: the elements a text declares, or the parser's refusal (thrown). */
  modelElements(model: string): Promise<PureModelContextData>;
  /** lite's `model/check`: none when the model compiles, else every error (lite's API v1 spec §2). */
  modelCheck(model: string): Promise<readonly LiteError[]>;
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
    const pmcd = await this.#engine.modelElements(text);
    return pmcd.elements.filter((e) => e._type !== 'sectionIndex').map((e) => ({ path: `${e.package}::${e.name}`, type: e._type }));
  }

  /** The model's compile errors, each its words (its position before them): [] when it compiles. */
  async compile(model: string): Promise<string[]> {
    return (await this.#engine.modelCheck(model)).map((e) => e.message);
  }

  /** Builds what a first compile needs while the page is still busy with other things. */
  async warm(): Promise<void> {
    await this.#warm?.();
  }
}
