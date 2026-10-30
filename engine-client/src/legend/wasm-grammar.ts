// Grammar and typing answered in the tab by legend-lite's planner (WebAssembly, in a worker). The grammar, a query's
// relation type and a model's compile are legend-engine's pure/v1 (and lite's own compile route) asked of the planner
// (plannerFetch): the same client as a server's (HttpEngine), the same code answering it (PureV1Api), so the tab and a
// server cannot answer differently (docs/PROTOCOL_PROGRAM_2026_10_05.md, invariant 5; leg 6). The rest are the
// planner's own calls (docs/QUERY_APP_DESIGN_2026_09_30.md D3).

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, HttpEngine, type Grammar } from './engine.ts';
import type { RequestBody, Response as PlannerResponse } from './planner-protocol.ts';
import type { CompileResult, ExecuteInput, PureModelContext, RelationTypeAnswer } from './wire.ts';
import type { SeedTable, TableSeed } from '../model-data.ts';
import type { ProjectDataStatements } from '../project-data.ts';
import type { HostOperation } from '../host-operations.ts';

/** Where a request goes: a worker, or (in tests) the module called directly. */
export interface PlannerPort {
  ask(request: RequestBody): Promise<string>;
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

  ask(request: RequestBody): Promise<string> {
    const id = this.#next++;
    return new Promise((resolve, reject) => {
      this.#pending.set(id, { resolve, reject });
      this.#worker.postMessage({ ...request, id, base: this.#base });
    });
  }
}

/**
 * An export's folded answer (`OK\n<json>` / `ERR\n<kind>\n<message>`) as its value or a refusal. The kind is the one
 * legend-lite's server gives the refusal (planner.Folded): legend-engine's errorType when the text is refused
 * (`PARSER`, `COMPILATION`, answered 400 as the server's grammar routes answer them), else the server's status.
 */
export function unfold(answer: string): string {
  if (answer.startsWith('OK\n')) return answer.slice(3);
  const [tag, kind = '', ...rest] = answer.split('\n');
  const message = rest.join('\n');
  if (tag === 'ERR' && (kind === 'PARSER' || kind === 'COMPILATION')) throw new EngineError(message, 400, kind);
  if (tag === 'ERR' && /^5\d\d$/.test(kind)) throw new EngineError(message, Number(kind));
  throw new EngineError(`the planner answered in no form it has: ${JSON.stringify(answer.slice(0, 120))}`, 500);
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
    const answer = httpAnswer(await port.ask({ kind: 'pureV1', path: url.pathname, query: url.search.slice(1), body: body ?? '' }));
    init?.signal?.throwIfAborted();
    return new Response(answer.body, { status: answer.status, headers: { 'Content-Type': answer.type } });
  };
}

/** A pure/v1 answer's body when it is a 200; else the engine's refusal, in its words and status, as a server's is. */
function answered(folded: string): string {
  const answer = httpAnswer(folded);
  if (answer.status === 200) return answer.body;
  let message = answer.body || `${answer.status}`;
  let errorType: string | undefined;
  try {
    const e = JSON.parse(answer.body) as { message?: string; errorType?: string };
    if (typeof e.message === 'string') message = e.message;
    errorType = e.errorType;
  } catch { /* not JSON: the text is the message */ }
  throw new EngineError(message, answer.status, errorType);
}

/** A pure/v1 answer the planner gives (`OK\n<status>\n<type>\n<body>`): its status, media type and body. */
function httpAnswer(folded: string): { status: number; type: string; body: string } {
  const answer = unfold(folded);
  const first = answer.indexOf('\n');
  const second = answer.indexOf('\n', first + 1);
  if (first < 0 || second < 0) {
    throw new EngineError(`the planner answered a pure/v1 call in no form it has: ${JSON.stringify(answer.slice(0, 120))}`, 500);
  }
  return { status: Number(answer.slice(0, first)), type: answer.slice(first + 1, second), body: answer.slice(second + 1) };
}

/**
 * Execute's plan half in the engine's JSON (PureV1Api.jsonPlan; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): `run`, the
 * typed statement the tab runs -- a relation's rows, or a graph fetch's objects, one per row -- its answer's shape and
 * result columns, and the answer's every part but its rows (its text, for the planner's executeAnswer).
 */
export interface ExecutePlan {
  /** The plan's statement: what the answer's activity reports. */
  readonly sql: string;
  /** What the host runs: the same statement, its rows handed back to executeAnswer. */
  readonly run: string;
  /** A relation's rows, or a graph fetch's objects. */
  readonly shape: 'RELATION' | 'OBJECTS';
  /** The statement's result columns with their type facts, by which the planner reads the rows back. */
  readonly resultColumns: readonly unknown[];
  readonly builder: { readonly _type: string };
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

  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> {
    return this.#pureV1.relationType(model, lambda);
  }

  /** `compilation/lambdaReturnType`, answered in the tab as the server answers it (PureV1Api.route). */
  returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    return this.#pureV1.returnType(model, lambda);
  }

  /** `execution/generatePlan`, answered in the tab as the server answers it: the plan legend-engine generates. */
  generatePlan(input: ExecuteInput): Promise<unknown> {
    return this.#pureV1.generatePlan(input);
  }

  /**
   * Execute's plan half (ExecutePlan) and its text, asked of the planner over `input`'s own model. A refusal -- a
   * parameter with no value, a query that does not compile -- is the engine's, in its words and status, as a server's
   * execute refuses it (500, its errorType COMPILATION).
   */
  async executePlan(input: ExecuteInput): Promise<{ readonly plan: ExecutePlan; readonly text: string }> {
    // the tab runs the model it is handed (its data the page's): that one, the planner's check then always met; a
    // model not given as text is the planner's to refuse, in the server's words
    const model = input.model as { readonly _type?: string; readonly code?: string };
    const models = model._type === 'text' && typeof model.code === 'string' ? [model.code] : [];
    const text = answered(await this.#port.ask({ kind: 'executePlan', body: toJson(input), models: JSON.stringify(models) }));
    return { plan: JSON.parse(text) as ExecutePlan, text };
  }

  /**
   * Execute's answer (PureV1Api.jsonAnswer): `plan` (executePlan's text) and the rows the tab's database answered its
   * `run` statement with (browser-engine's hostRows), decoded by the plan's columns and written by the writer
   * legend-lite's server answers with -- its text, the server's.
   */
  async executeAnswer(plan: string, rows: string): Promise<string> {
    return answered(await this.#port.ask({ kind: 'executeAnswer', plan, rows }));
  }

  /** The SQL a query compiles to on its runtime, and the type of its result (upstream's relationType shape). */
  async plan(model: PureModelContext, lambda: Lambda, runtime: string): Promise<{ sql: string; type: unknown }> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'planModel', model: model.code, lambda: toJson(lambda), runtime }))) as { sql: string; type: unknown };
  }

  /** The SQL a query compiles to on its runtime -- legend-lite's plan, shown to a person ("Show SQL"). */
  async sql(model: PureModelContext, lambda: Lambda, runtime: string): Promise<string> {
    return (await this.plan(model, lambda, runtime)).sql;
  }

  /**
   * What a session of the database runs before it is queried, so that it answers as the planner's SQL expects -- the
   * dialect's own setup (Boundary.sessionSetup: a DuckDB session in UTC), as legend-lite's server sets up its own.
   */
  async sessionSetup(databaseType: string): Promise<readonly string[]> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'sessionSetup', databaseType }))) as string[];
  }

  /**
   * A project's own database on a database of that type (C5, Boundary.projectData): its name and the statements that
   * make it, point a connection at it, fill it from the model's Data elements, and drop it.
   */
  async projectData(model: string, databaseType: string): Promise<ProjectDataStatements> {
    return JSON.parse(unfold(await this.#port.ask({ kind: 'projectData', model, databaseType }))) as ProjectDataStatements;
  }

  /** A host's named operations on a database of that type (D5, Boundary.hostOperations): each one's statements. */
  async hostOperations(databaseType: string, operations: readonly HostOperation[]): Promise<string[][]> {
    const asked = await this.#port.ask({ kind: 'hostOperations', databaseType, operations: JSON.stringify(operations) });
    return JSON.parse(unfold(asked)) as string[][];
  }

  /** Build the planner's boot layer for this model now, before a person waits on it. */
  async warm(model: PureModelContext): Promise<void> {
    unfold(await this.#port.ask({ kind: 'warm', model: model.code }));
  }

  /** `compilation/compile` in the tab, as the server answers it: OK, or the first failure, 400 COMPILATION. */
  compile(model: PureModelContext): Promise<CompileResult> {
    return this.#pureV1.compile(model);
  }

  /** A whole model's every error (Studio's live problems), [] when it compiles: HttpEngine's, asked of the planner. */
  compileErrors(code: string): Promise<string[]> {
    return this.#pureV1.compileErrors(code);
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
