// LITE'S API V1 OVER A SERVER (lite's API v1 spec §2, §5 and §7, the user's go of 2026-11-17): the server binding of the
// one TypeScript client (browser-engine.ts LiteEngine; its tab binding is TabEngine) -- one POST per operation to
// `/api/lite/v1/<operation>`, the request a closed JSON object, the answer the operation's JSON, a refusal lite's one
// shape (`{kind, message, where?, raise?}`, its status by its kind) thrown as a LiteRefusal. `query/run` asks the
// answer as Arrow IPC (typed columns; spec §4: our apps ask arrow, JSON is for small answers and third parties) or as
// JSON, by the caller's ask. legend-engine's /api/pure/v1 client is the edge's, never asked here.

import { fromJson, readLambda, toJson, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from '../legend/pmcd.ts';
import type { LiteAnswer, LiteEngine } from '../legend/browser-engine.ts';
import { liteError, LiteRefusal, type LiteError } from '../lite-error.ts';
import type { QueryColumn } from '../relation-type.ts';
import type { ParameterValue } from '../requests.ts';
import { parseExact } from '../values.ts';
import { arrowIpcTyped } from '../arrow-ipc.ts';
import { toRawTable } from '../duckdb.ts';

/** How `query/run` answers: Arrow IPC (the apps' ask) or JSON rows. */
export type RunFormat = 'arrow' | 'json';

/**
 * Legend-lite's server, asked at `baseUrl` (its `/api`): each method one of lite's operations (the spec's rule: `a/b` is
 * `aB`). `format`: how `query/run` answers, Arrow unless JSON is asked.
 */
export class LiteServer implements LiteEngine {
  readonly name: string;
  readonly #base: string;
  readonly #fetch: typeof fetch;
  readonly #format: RunFormat;

  constructor(baseUrl: string, fetcher: typeof fetch = globalThis.fetch.bind(globalThis), format: RunFormat = 'arrow') {
    this.#base = baseUrl.replace(/\/+$/, '');
    this.name = this.#base;
    this.#fetch = fetcher;
    this.#format = format;
  }

  /** One operation asked at its route (`a/b`): its answer, or its refusal thrown as lite's (a body in no refusal's shape
   *  refused by name). */
  async #ask(route: string, request: unknown, signal?: AbortSignal): Promise<Response> {
    const res = await this.#fetch(`${this.#base}/lite/v1/${route}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: toJson(request), ...(signal ? { signal } : {}),
    });
    if (res.ok) return res;
    const text = await res.text();
    let refusal: LiteError;
    try {
      refusal = liteError(JSON.parse(text));
    } catch (e) {
      throw new LiteRefusal({ kind: 'internal', message: `the server answered ${route} ${res.status} in no refusal's shape (${(e as Error).message})` }, res.status);
    }
    throw new LiteRefusal(refusal, res.status);
  }

  async #text(route: string, request: unknown, signal?: AbortSignal): Promise<string> {
    return (await this.#ask(route, request, signal)).text();
  }

  async modelCheck(model: string): Promise<readonly LiteError[]> {
    const { errors } = JSON.parse(await this.#text('model/check', { model })) as { readonly errors: readonly unknown[] };
    return errors.map(liteError);
  }

  async modelElements(model: string): Promise<PureModelContextData> {
    const { elements } = JSON.parse(await this.#text('model/elements', { model })) as Pick<PureModelContextData, 'elements'>;
    return { _type: 'data', elements };
  }

  async queryParse(text: string): Promise<Lambda> {
    // read by the protocol library: its numbers exact
    return readLambda((fromJson(await this.#text('query/parse', { text })) as { readonly query: unknown }).query);
  }

  async queryPrint(query: Lambda, style: 'pretty' | 'standard' = 'pretty'): Promise<string> {
    return (JSON.parse(await this.#text('query/print', { query, style })) as { readonly text: string }).text;
  }

  async queryType(model: string, query: Lambda): Promise<readonly QueryColumn[]> {
    return (JSON.parse(await this.#text('query/type', { model, query })) as { readonly columns: readonly QueryColumn[] }).columns;
  }

  async queryPlan(model: string, query: Lambda, runtime: string): Promise<string> {
    // the plan's JSON, its numbers exact (a constant's 2^63-1 stays its digits)
    return toJson((fromJson(await this.#text('query/plan', { model, query, runtime })) as { readonly plan: unknown }).plan);
  }

  async queryRun(model: string, query: Lambda, runtime: string, params: Readonly<Record<string, ParameterValue>> = {},
    signal?: AbortSignal): Promise<LiteAnswer> {
    const res = await this.#ask('query/run', { model, query, runtime, params, format: this.#format }, signal);
    if (this.#format === 'json') {
      // its cells exact: an Integer past 2^53 a bigint, a Decimal its text
      return parseExact(await res.text()) as LiteAnswer;
    }
    // Arrow: each column its Pure type from the plan's facts the field carries, each cell read as the tab's DuckDB
    // answer is (duckdb.ts toRawTable: the one narrowing), the answer's shape from the stream's metadata
    const { shape, columns, table } = arrowIpcTyped(new Uint8Array(await res.arrayBuffer()));
    const raw = toRawTable(table, 0, 0);
    const rows = Array.from({ length: raw.rowCount }, (_, r) => raw.columns.map((c) => c.values[r]));
    return { shape: shape as LiteAnswer['shape'], columns, rows };
  }
}
