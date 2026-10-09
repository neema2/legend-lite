// Grammar and typing answered in the tab by legend-lite's planner (WebAssembly, in a worker). The grammar is
// legend-engine's pure/v1 asked of the planner (plannerFetch): the same client as a server's (HttpEngine), the same
// code answering it (PureV1Api), so the tab and a server cannot answer differently (docs/PROTOCOL_PROGRAM_2026_10_05.md,
// invariant 5). The rest are the planner's own calls (docs/QUERY_APP_DESIGN_2026_09_30.md D3).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, HttpEngine, type Grammar } from './engine.ts';
import type { PlannerRequest, PlannerResponse } from './planner-worker.ts';
import type { PureModelContext, RelationTypeAnswer } from './wire.ts';
import type { SeedTable, TableSeed } from '../model-data.ts';

/** Where a request goes: a worker, or (in tests) the module called directly. */
export interface PlannerPort {
  ask(request: PlannerRequest): Promise<string>;
}

/** A worker running planner-worker.ts; `base` is where classes.wasm and its runtime live. */
export class WorkerPort implements PlannerPort {
  readonly #worker: Worker;
  readonly #base: string;
  readonly #pending = new Map<number, { resolve(a: string): void; reject(e: Error): void }>();
  #next = 1;

  constructor(workerUrl: string | URL, base: string) {
    this.#worker = new Worker(workerUrl, { type: 'module' });
    this.#base = new URL(base, globalThis.location?.href).href;
    this.#worker.onmessage = (e: MessageEvent<PlannerResponse>) => {
      const p = this.#pending.get(e.data.id);
      if (!p) return;
      this.#pending.delete(e.data.id);
      if (e.data.ok) p.resolve(e.data.answer);
      else p.reject(new Error(`the planner could not load: ${e.data.error}`));
    };
  }

  ask(request: PlannerRequest): Promise<string> {
    const id = this.#next++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ ...request, id, base: this.#base });
    });
  }
}

/**
 * legend-lite's refusals the server answers 400 (PureV1Api.answer): the parse error, and the
 * compile errors -- LegendCompileException's subclasses -- and an unimplemented construct.
 */
const PARSE_ERRORS = new Set(['com.legend.parser.ParseException']);
const COMPILE_ERRORS = new Set([
  'com.legend.error.LegendCompileException', 'com.legend.error.ModelException',
  'com.legend.error.ResolutionException', 'com.legend.error.MappingResolutionException',
  'com.legend.compiler.spec.TypeInferenceException', 'com.legend.error.NotImplementedException',
]);

/** An export's folded answer (`OK\n<json>` / `ERR\n<class>\n<message>`) as its value or a refusal. */
export function unfold(answer: string): string {
  if (answer.startsWith('OK\n')) return answer.slice(3);
  const [, kind = '', ...rest] = answer.split('\n');
  const message = rest.join('\n') || kind;
  if (PARSE_ERRORS.has(kind)) throw new EngineError(message, 400, 'PARSER');
  if (COMPILE_ERRORS.has(kind)) {
    throw new EngineError(message, 400, 'COMPILATION');
  }
  throw new EngineError(`${kind}: ${message}`, 500);
}

/**
 * A `fetch` that answers legend-engine's pure/v1 requests in the tab: each is routed by the planner's `pureV1OrError`,
 * the code legend-lite's server answers them with, its status, media type and body made a Response. An HttpEngine
 * over it asks and is answered exactly as over the network, a refusal included. The planner itself failing (its
 * folded `ERR` answer) rejects with the EngineError `unfold` makes of it; an aborted request rejects with its reason.
 */
export function plannerFetch(port: PlannerPort): typeof fetch {
  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    init?.signal?.throwIfAborted();
    const href = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    const url = new URL(href, 'http://planner.invalid');
    const body = init?.body;
    if (body !== undefined && body !== null && typeof body !== 'string') {
      throw new TypeError('the planner answers a pure/v1 request with a text body only');
    }
    const answer = unfold(await port.ask({ kind: 'pureV1', path: url.pathname, query: url.search.slice(1), body: body ?? '' }));
    init?.signal?.throwIfAborted();
    const first = answer.indexOf('\n');
    const second = answer.indexOf('\n', first + 1);
    if (first < 0 || second < 0) {
      throw new EngineError(`the planner answered a pure/v1 call in no form it has: ${JSON.stringify(answer.slice(0, 120))}`, 500);
    }
    return new Response(answer.slice(second + 1), {
      status: Number(answer.slice(0, first)),
      headers: { 'Content-Type': answer.slice(first + 1, second) },
    });
  };
}

export class WasmGrammar implements Grammar {
  readonly #port: PlannerPort;
  /** pure/v1 in the tab: the server's client over the planner. */
  readonly #pureV1: HttpEngine;

  constructor(port: PlannerPort) {
    this.#port = port;
    this.#pureV1 = new HttpEngine('/api', plannerFetch(port));
  }

  modelJson(text: string): Promise<PureModelContextData> {
    return this.#pureV1.modelJson(text);
  }

  lambdaJson(text: string): Promise<Lambda> {
    return this.#pureV1.lambdaJson(text);
  }

  /** As `lambdaJson`, read by the protocol library: numbers exact (DataCube's `Planner.parse`). */
  lambda(text: string): Promise<Lambda> {
    return this.#pureV1.lambda(text);
  }

  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.#pureV1.lambdaText(lambda, style);
  }

  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.#pureV1.modelText(model, style);
  }

  async relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'relationType', model: model.code, lambda: toJson(lambda) }))) as RelationTypeAnswer;
  }

  /** The SQL a query compiles to on its runtime, and the type of its result (upstream's relationType shape). */
  async plan(model: PureModelContext, lambda: Lambda, runtime: string): Promise<{ sql: string; type: unknown }> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'plan', model: model.code, lambda: toJson(lambda), runtime }))) as { sql: string; type: unknown };
  }

  /** The SQL a query compiles to on its runtime -- legend-lite's plan, shown to a person ("Show SQL"). */
  async sql(model: PureModelContext, lambda: Lambda, runtime: string): Promise<string> {
    return (await this.plan(model, lambda, runtime)).sql;
  }

  /** Build the planner's boot layer for this model now, before a person waits on it. */
  async warm(model: PureModelContext): Promise<void> {
    unfold(await this.#port.ask({ kind: 'warm', model: model.code }));
  }

  /**
   * A whole model compiled (`compilation/compile` in the tab): every error, [] when it compiles -- the first element
   * error stops the compile, as the server's does; every body error is collected (Studio's live problems). A parse or
   * compile refusal is a problem listed; anything else (the planner failing) is thrown, as HttpEngine's is.
   */
  async compileErrors(code: string): Promise<string[]> {
    try {
      return JSON.parse(unfold(await this.#port.ask({ kind: 'compile', model: code }))) as string[];
    } catch (e) {
      if (e instanceof EngineError && e.status < 500) return [e.message];
      throw e;
    }
  }

  /**
   * A model's test data as the statements the server seeds DuckDB with (setup.CsvSeed, on the plan side), and each
   * table's names as those statements spell them: for each table `database` declares, its SeedSource answer
   * (model-data.ts), in order.
   */
  async testDataSql(model: string, database: string, tables: readonly SeedTable[]): Promise<TableSeed[]> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'testData', model, database, tables: JSON.stringify(tables) }))) as TableSeed[];
  }
}
