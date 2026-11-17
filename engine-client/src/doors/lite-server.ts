// LITE'S ENGINE OVER A SERVER (2026-11-17, the edge program's H7): Studio's and Query's server paths on legend-lite's own
// API, /api/lite/v1 -- the grammar and typing routes (LiteV1Planning), a model's every compile error, and a query's
// plan and its run answered as typed rows (LiteAnswer) -- the same forms the tab answers (browser-engine.ts TabEngine).
// legend-engine's /api/pure/v1 client is the edge's (planning-client.ts and engine.ts HttpEngine), never asked here.

import { readLambda, toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import { EngineError } from '../legend/engine-api.ts';
import type { PureModelContextData } from '../legend/pmcd.ts';
import type { PureModelContext, RelationTypeAnswer } from '../legend/wire.ts';
import { stated, type LiteAnswer, type LiteEngine } from '../legend/browser-engine.ts';
import type { ParameterValue } from '../plan-runner.ts';

/** A route's answer, or its refusal in lite's error shape (`{code, errorType?, message, status}`) as an EngineError. */
async function answered(res: Response): Promise<string> {
  const text = await res.text();
  if (res.ok) return text;
  let message = text || `${res.status} ${res.statusText}`;
  let errorType: string | undefined;
  try {
    const e = JSON.parse(text) as { readonly message?: unknown; readonly errorType?: unknown };
    if (typeof e.message === 'string') message = e.message;
    if (typeof e.errorType === 'string') errorType = e.errorType;
  } catch {
    // not JSON: the text is the message
  }
  throw new EngineError(message, res.status, errorType);
}

/**
 * Legend-lite's server, asked at `baseUrl` (its `/api`): each call one of lite's own routes, its request a closed JSON
 * object, its answer lite's JSON.
 */
export class LiteServer implements LiteEngine {
  readonly name: string;
  readonly #base: string;
  readonly #fetch: typeof fetch;

  constructor(baseUrl: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis)) {
    this.#base = baseUrl.replace(/\/+$/, '');
    this.name = this.#base;
    this.#fetch = fetcher;
  }

  async #post(route: string, body: unknown, signal?: AbortSignal): Promise<string> {
    return answered(await this.#fetch(`${this.#base}/lite/v1/${route}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: toJson(body), ...(signal ? { signal } : {}),
    }));
  }

  async modelJson(text: string): Promise<PureModelContextData> {
    return JSON.parse(await this.#post('grammar/modelElements', { text, sourceInformation: false })) as PureModelContextData;
  }

  async lambdaJson(text: string): Promise<Lambda> {
    return JSON.parse(await this.#post('grammar/parseLambda', { text, sourceInformation: false })) as Lambda;
  }

  /** As `lambdaJson`, read by the protocol library: numbers exact. */
  async lambda(text: string): Promise<Lambda> {
    return readLambda(await this.#post('grammar/parseLambda', { text, sourceInformation: false }));
  }

  async lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    const answer = JSON.parse(await this.#post('grammar/printLambda', { lambda, style })) as { readonly text: string };
    return answer.text;
  }

  async relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    return JSON.parse(await this.#post('compilation/lambdaRelationType', { model, lambda })) as RelationTypeAnswer;
  }

  async compileErrors(code: string): Promise<string[]> {
    const answer = JSON.parse(await this.#post('compilation/compile', { _type: 'text', code })) as {
      readonly errors: readonly { readonly message: string }[];
    };
    return answer.errors.map((e) => e.message);
  }

  async statement(model: PureModelContext, lambda: Lambda): Promise<string> {
    return stated(await this.#post('execution/plan', { model, lambda })).sql;
  }

  async run(model: PureModelContext, lambda: Lambda, parameters: Readonly<Record<string, ParameterValue>> = {},
    signal?: AbortSignal): Promise<LiteAnswer> {
    return JSON.parse(await this.#post('execution/execute', { model, lambda, parameters }, signal)) as LiteAnswer;
  }
}
