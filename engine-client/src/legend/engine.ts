// The calls a query app makes to a legend engine -- legend-engine's own `/api` paths and shapes,
// served by legend-lite or legend-engine alike (docs/QUERY_APP_DESIGN_2026_09_30.md D1). Nothing
// here is legend-lite's own; pointing the app at legend-engine is a base URL.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { readLambda, toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import type {
  CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer,
} from './wire.ts';

/** A refusal from the engine, in its error shape (`{code, errorType?, message, status}`). */
export class EngineError extends Error {
  readonly status: number;
  readonly errorType: string | undefined;

  constructor(message: string, status: number, errorType?: string) {
    super(message);
    this.name = 'EngineError';
    this.status = status;
    this.errorType = errorType;
  }
}

/** Grammar and typing: what the tab's WASM planner can answer as well as a server. */
export interface Grammar {
  /** `grammar/grammarToJson/model`, without source information. */
  modelJson(text: string): Promise<PureModelContextData>;
  /** `grammar/grammarToJson/lambda`, without source information. */
  lambdaJson(text: string): Promise<Lambda>;
  /** `grammar/jsonToGrammar/lambda`. */
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string>;
  /** `grammar/jsonToGrammar/model`: a model's protocol JSON as Pure text. */
  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string>;
  /** `compilation/lambdaRelationType`. */
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer>;
}

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


/** legend-engine refuses an ExecuteInput without its execution context (measured, 4.145.0: a 500). */
function withContext(input: ExecuteInput): ExecuteInput {
  return { clientVersion: 'vX_X_X', context: { _type: 'BaseExecutionContext' }, ...input };
}

/** An engine at a base URL (`http://host:port/api`): legend-lite's server or legend-engine. */
export class HttpEngine implements Engine {
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

  async #json<T>(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
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

  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    return this.#json('POST', '/pure/v1/compilation/lambdaRelationType', { model, lambda });
  }

  compile(model: PureModelContext): Promise<CompileResult> {
    return this.#json('POST', '/pure/v1/compilation/compile', model);
  }

  /** `compile` as a list of errors ([] when it compiles): a server answers its one refusal (the in-tab twin, all). */
  async compileErrors(code: string): Promise<string[]> {
    try {
      await this.compile({ _type: 'text', code });
      return [];
    } catch (e) {
      if (e instanceof EngineError && e.status < 500) return [e.message];
      throw e;
    }
  }

  async returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    return (await this.#json<{ returnType: string }>('POST', '/pure/v1/compilation/lambdaReturnType', { model, lambda })).returnType;
  }

  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> {
    return this.#json('POST', '/pure/v1/execution/execute', withContext(input), signal);
  }

  generatePlan(input: ExecuteInput): Promise<unknown> {
    return this.#json('POST', '/pure/v1/execution/generatePlan', withContext(input));
  }

  currentUser(): Promise<string> {
    return this.#json('GET', '/server/v1/currentUser');
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
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> { return this.#grammar.relationType(model, lambda); }
  compile(model: PureModelContext): Promise<CompileResult> { return this.#server.compile(model); }
  returnType(model: PureModelContext, lambda: Lambda): Promise<string> { return this.#server.returnType(model, lambda); }
  execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> { return this.#server.execute(input, signal); }
  generatePlan(input: ExecuteInput): Promise<unknown> { return this.#server.generatePlan(input); }
  currentUser(): Promise<string> { return this.#server.currentUser(); }
}
