// The engines a query app reaches a legend engine through -- legend-engine's own `/api` paths and shapes, served by
// legend-lite or legend-engine alike (docs/QUERY_APP_DESIGN_2026_09_30.md D1); pointing the app at legend-engine is a
// base URL. HttpEngine is the server's planning client (planning-client.ts) and its execute, the server's door, which
// plans and runs in one request on the server; RoutedEngine answers grammar and typing in the tab and the rest on a
// server. C5-T (2026-11-15): these are doors, the planning client and the API's types are not. H11 (2026-11-17): all of
// it is legend-engine's API, the edge's (//engine-client:edge): no product module reaches it.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { planning } from '../plan-guard.ts';
import type { Grammar } from './grammar.ts';
import { HttpPlanning, withContext } from './planning-client.ts';
import type { CompileResult, ExecuteInput, ExecutionResult, RelationTypeAnswer } from './engine-wire.ts';
import type { PureModelContext } from './wire.ts';

// the engine's refusal (engine-api.ts, records) and the planner's Grammar (grammar.ts), read here by their importers
export { EngineError } from './engine-api.ts';
export type { Grammar } from './grammar.ts';

// THE ENGINE, the server's interface (C5-T audit, item 3, 2026-11-15: out of the records, engine-api.ts, beside the
// engines that are one): a Grammar, and what needs a server -- its execute among it, which plans and runs on the server.
/** Everything else, which needs a server. */
export interface Engine extends Grammar {
  /** `compilation/compile`. */
  compile(model: PureModelContext): Promise<CompileResult>;
  /** `compilation/lambdaReturnType`: the result type's path. */
  returnType(model: PureModelContext, lambda: Lambda): Promise<string>;
  /** `execution/execute`. */
  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult>;
  /** `execution/generatePlan`. */
  generatePlan(input: ExecuteInput): Promise<unknown>;
  /** `server/v1/currentUser`. */
  currentUser(): Promise<string>;
}

/**
 * An engine at a base URL (`http://host:port/api`): legend-lite's server or legend-engine -- the server's planning
 * client (planning-client.ts) and its execute, the server's door (it plans and runs in one request, on the server).
 */
export class HttpEngine extends HttpPlanning implements Engine {
  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> {
    return this.json('POST', '/pure/v1/execution/execute', withContext(input), signal);
  }
}

/**
 * An engine whose grammar and typing are answered by `grammar` (the tab's planner) and the rest
 * by `server` -- fixed by configuration, never chosen on failure (AGENTS.md: no fallbacks).
 */
export class RoutedEngine implements Engine {
  readonly #grammar: Grammar;
  readonly #server: Engine;

  constructor(grammar: Grammar, server: Engine) {
    this.#grammar = grammar;
    this.#server = server;
  }

  modelJson(text: string): Promise<PureModelContextData> { return this.#grammar.modelJson(text); }
  lambdaJson(text: string): Promise<Lambda> { return this.#grammar.lambdaJson(text); }
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#grammar.lambdaText(lambda, style); }
  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#grammar.modelText(model, style); }
  // typing is planning, whatever engine it routes to (the last list, T2's neighbours): refused by a rejected promise
  // (each async, as HttpEngine's)
  async relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    planning('relationType');
    return this.#grammar.relationType(model, lambda);
  }
  async compile(model: PureModelContext): Promise<CompileResult> {
    planning('compile');
    return this.#server.compile(model);
  }
  async returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    planning('returnType');
    return this.#server.returnType(model, lambda);
  }
  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> { return this.#server.execute(input, signal); }
  async generatePlan(input: ExecuteInput): Promise<unknown> {
    // whatever engine it routes to (T2)
    planning('generatePlan');
    return this.#server.generatePlan(input);
  }
  currentUser(): Promise<string> { return this.#server.currentUser(); }
}
