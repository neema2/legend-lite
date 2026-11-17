// THE SERVER'S PLANNING CLIENT (C5-T, 2026-11-15): legend-engine's `/api` paths a query app asks for grammar, typing,
// compile and plans -- legend-lite's server or legend-engine alike (docs/QUERY_APP_DESIGN_2026_09_30.md D1). One is
// legend-lite's own (a model's every compile error, `compileErrors`), asked of legend-lite's route alone (H8, 2026-11-17:
// no fallback to legend-engine's). Its execute, the server's door, is not here: engine.ts HttpEngine adds it.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { readLambda, toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { planning } from '../plan-guard.ts';
import { EngineError } from './engine-api.ts';
import type { Grammar } from './grammar.ts';
import type { CompileResult, ExecuteInput, PureModelContext, RelationTypeAnswer } from './wire.ts';

/** legend-engine refuses an ExecuteInput without its execution context (measured, 4.145.0: a 500). */
export function withContext(input: ExecuteInput): ExecuteInput {
  return { clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...input };
}

/**
 * THE SERVER'S PLANNING CLIENT at a base URL (`http://host:port/api`): legend-lite's server or legend-engine, asked its
 * grammar, its typing, its compile and its plans (generatePlan) -- every call of a query app but the server's execute,
 * which plans and runs in one request and is the doors' (engine.ts HttpEngine, which is this client and its execute).
 * The tab's planner (wasm-grammar.ts) asks its pure/v1 answers through this client, so the planner reaches no execute
 * (C5-T, 2026-11-15: the orchestrator's note 1, "the planner must reach nothing that can execute").
 */
export class HttpPlanning implements Grammar {
  readonly #base: string;
  readonly #fetch: typeof fetch;

  constructor(baseUrl: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)) {
    this.#base = baseUrl.replace(/\/+$/, '');
    this.#fetch = fetcher;
  }

  async #call(method: string, path: string, body?: string, contentType = 'application/json',
    signal?: AbortSignal): Promise<Response> {
    const init: RequestInit = { method, headers: { 'Content-Type': contentType } };
    if (body !== undefined) init.body = body;
    if (signal !== undefined) init.signal = signal;
    const res = await this.#fetch(`${this.#base}${path}`, init);
    if (!res.ok) {
      const text = await res.text();
      let message = text || `${res.status} ${res.statusText}`;
      let errorType: string | undefined;
      try {
        const e = JSON.parse(text) as { message?: string; errorType?: string };
        if (typeof e.message === 'string') message = e.message;
        errorType = e.errorType;
      } catch { /* not JSON: the text is the message */ }
      throw new EngineError(message, res.status, errorType);
    }
    return res;
  }

  /** A JSON request and its JSON answer: the server's execute is asked through it too (engine.ts HttpEngine). */
  protected async json<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    const res = await this.#call(method, path, body === undefined ? undefined : toJson(body), 'application/json', signal);
    return res.json() as Promise<T>;
  }

  async modelJson(text: string): Promise<PureModelContextData> {
    const res = await this.#call('POST', '/pure/v1/grammar/grammarToJson/model?returnSourceInformation=false', text, 'text/plain');
    return res.json() as Promise<PureModelContextData>;
  }

  async lambdaJson(text: string): Promise<Lambda> {
    return JSON.parse(await this.#lambdaJsonText(text)) as Lambda;
  }

  /** As `lambdaJson`, read by the protocol library: numbers exact (a query a person typed keeps its digits). */
  async lambda(text: string): Promise<Lambda> {
    return readLambda(await this.#lambdaJsonText(text));
  }

  async #lambdaJsonText(text: string): Promise<string> {
    const res = await this.#call('POST', '/pure/v1/grammar/grammarToJson/lambda?returnSourceInformation=false', text, 'text/plain');
    return res.text();
  }

  async lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    const res = await this.#call('POST', `/pure/v1/grammar/jsonToGrammar/lambda?renderStyle=${style}`, toJson(lambda));
    return res.text();
  }

  async modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    const res = await this.#call('POST', `/pure/v1/grammar/jsonToGrammar/model?renderStyle=${style}`, toJson(model));
    return res.text();
  }

  // a server's typing is planning: each refused inside a run of the caller's flow (run-frame.ts; the last list of
  // 2026-11-14, T2's neighbours) -- by a rejected promise, never a throw where a promise is promised (each method async;
  // the C4 final fix round's nit, 2026-11-14)
  async relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    planning('relationType');
    return this.json('POST', '/pure/v1/compilation/lambdaRelationType', { model, lambda });
  }

  async compile(model: PureModelContext): Promise<CompileResult> {
    planning('compile');
    return this.json('POST', '/pure/v1/compilation/compile', model);
  }

  /**
   * A whole model's compile errors, [] when it compiles: legend-lite's own route (`/lite/v1/compilation/compile`: the
   * first element error alone, else every body's). A server with no such route refuses by its own answer (a 404), never
   * answered by another route (G7; the edge audit's H8, 2026-11-17: legend-engine's compile is the edge's client's).
   */
  async compileErrors(code: string): Promise<string[]> {
    planning('compileErrors');
    const model: PureModelContext = { _type: 'text', code };
    const answer = await this.json<{ errors: { message: string }[] }>('POST', '/lite/v1/compilation/compile', model);
    return answer.errors.map((e) => e.message);
  }

  async returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    planning('returnType');
    return (await this.json<{ returnType: string }>('POST', '/pure/v1/compilation/lambdaReturnType', { model, lambda })).returnType;
  }

  async generatePlan(input: ExecuteInput): Promise<unknown> {
    // a server's planner: refused inside a run of the caller's flow (run-frame.ts; the final audit of 9af16fbe58's T2)
    planning('generatePlan');
    return this.json('POST', '/pure/v1/execution/generatePlan', withContext(input));
  }

  currentUser(): Promise<string> {
    return this.json('GET', '/server/v1/currentUser');
  }
}
