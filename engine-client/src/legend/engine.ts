// The engines a query app reaches a legend engine through -- legend-engine's own `/api` paths and shapes, served by
// legend-lite or legend-engine alike (docs/QUERY_APP_DESIGN_2026_09_30.md D1); pointing the app at legend-engine is a
// base URL. HttpEngine is the server's planning client (planning-client.ts) and its execute, the server's door, which
// plans and runs in one request on the server; RoutedEngine answers grammar and typing in the tab and the rest on a
// server. C5-T (2026-11-15): these are doors (//engine-client:doors), the planning client and the API's types are not.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { planning } from '../run-frame.ts';
import type { Engine, Grammar } from './engine-api.ts';
import { HttpPlanning, withContext } from './planning-client.ts';
import type {
  CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer,
} from './wire.ts';

// the engine's API as types, and its refusal, in engine-api.ts (records: C5-T, 2026-11-15); their importers read them here
export { EngineError } from './engine-api.ts';
export type { Engine, Grammar } from './engine-api.ts';

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
