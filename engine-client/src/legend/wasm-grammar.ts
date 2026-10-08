// Grammar and typing answered in the tab by legend-lite's planner (WebAssembly, in a worker):
// each call is the in-tab twin of a pure/v1 endpoint, the same core call behind it, so its
// answer is the server's (docs/QUERY_APP_DESIGN_2026_09_30.md D3).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { readLambda, toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, type Grammar } from './engine.ts';
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

export class WasmGrammar implements Grammar {
  readonly #port: PlannerPort;

  constructor(port: PlannerPort) {
    this.#port = port;
  }

  async modelJson(text: string): Promise<PureModelContextData> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'modelJson', text }))) as PureModelContextData;
  }

  async lambdaJson(text: string): Promise<Lambda> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'lambdaJson', text }))) as Lambda;
  }

  /** As `lambdaJson`, read by the protocol library: numbers exact (DataCube's `Planner.parse`). */
  async lambda(text: string): Promise<Lambda> {
    return readLambda(unfold(await this.#port.ask({ kind: 'lambdaJson', text })));
  }

  async lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return unfold(await this.#port.ask({ kind: 'compose', lambda: toJson(lambda), style }));
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
