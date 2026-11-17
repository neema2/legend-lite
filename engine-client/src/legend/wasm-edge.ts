// LEGEND-ENGINE'S SHAPES OVER THE TAB'S PLANNER, THE EDGE'S (H11, the orchestrator's ruling of 2026-11-17; split from
// wasm-grammar.ts, whose WasmGrammar is lite's own): legend-engine's Grammar (grammarToJson, jsonToGrammar,
// lambdaRelationType), pure/v1's compile, return type and generatePlan, each answered by the planner's pure/v1 route
// (pureV1, the edge's EdgeBoundary) as legend-lite's server's edge answers it, and execute's two halves for the edge's
// BrowserEngine. legend-engine's error (EngineError), its interface (Grammar) and its HTTP client (HttpPlanning) are
// imported here, never by the product's planner client.

import type { Lambda } from '../../../pure-protocol/src/index.ts';
import { toJson } from '../../../pure-protocol/src/index.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError } from './engine-api.ts';
import type { Grammar } from './grammar.ts';
import { HttpPlanning } from './planning-client.ts';
import { unfold, WasmGrammar, type PlannerPort } from './wasm-grammar.ts';
import type { CompileResult, ExecuteInput, RelationTypeAnswer } from './engine-wire.ts';
import type { PureModelContext } from './wire.ts';
import { fromPlan, type PlannedSql } from '../planned-sql.ts';
import { declareSites } from '../declared-raises.ts';

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
  /** What the host runs: the same statement, its rows handed back to executeAnswer (typed where the answer is read). */
  readonly run: PlannedSql;
  /** A relation's rows, or a graph fetch's objects. */
  readonly shape: 'RELATION' | 'OBJECTS';
  /** The statement's result columns with their type facts, by which the planner reads the rows back. */
  readonly resultColumns: readonly unknown[];
  readonly builder: { readonly _type: string };
  /** Where `run` binds values (a data: payload, the tenets audit's M3): the execution plan, which carries them by digest
   *  (plan-params.ts reads them). None for a statement that runs alone. */
  readonly plan?: unknown;
}

/** legend-engine's shapes over the tab's planner: the edge's grammar, over the same planner the product's WasmGrammar
 *  asks (the port), its lite operations under legend-engine's names where Grammar names them. */
export class WasmEdgeGrammar implements Grammar {
  readonly #port: PlannerPort;
  readonly #lite: WasmGrammar;
  /** pure/v1 in the tab: the server's client over the planner. */
  readonly #pureV1: HttpPlanning;

  constructor(port: PlannerPort) {
    this.#port = port;
    this.#lite = new WasmGrammar(port);
    this.#pureV1 = new HttpPlanning('/api', plannerFetch(port));
  }

  /** `grammar/grammarToJson/model`: lite's `model/elements`. */
  modelJson(text: string): Promise<PureModelContextData> {
    return this.#lite.modelElements(text);
  }

  /** `grammar/grammarToJson/lambda`: lite's `query/parse`. */
  lambdaJson(text: string): Promise<Lambda> {
    return this.#lite.queryParse(text);
  }

  /** `grammar/jsonToGrammar/lambda`: lite's `query/print`, legend-engine's style read as lite's word. */
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.#lite.queryPrint(lambda, style === 'PRETTY' ? 'pretty' : 'standard');
  }

  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> {
    return this.#pureV1.modelText(model, style);
  }

  /** `compilation/lambdaRelationType`, legend-engine's shape (Grammar is the edge's), answered in the tab by the edge's
   *  own route as `returnType` is (PureV1Routes, UpstreamRelationType). Lite's own typing is `queryType`. */
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
    const read = JSON.parse(text) as Omit<ExecutePlan, 'run'> & { readonly run: string; readonly raises?: unknown };
    // the raises `run` can send, worded by the planner (declared-raises.ts; phase D's option (D), 2026-11-15)
    declareSites(read, 'an execute plan');
    // the statement the host runs, as the planner's answer carries it (planned-sql.ts)
    return { plan: { ...read, run: fromPlan(read.run) }, text };
  }

  /**
   * Execute's answer (PureV1Api.jsonAnswer): `plan` (executePlan's text) and the rows the tab's database answered its
   * `run` statement with (edge-engine's hostRows), decoded by the plan's columns and written by the writer
   * legend-lite's server answers with -- its text, the server's.
   */
  async executeAnswer(plan: string, rows: string): Promise<string> {
    return answered(await this.#port.ask({ kind: 'executeAnswer', plan, rows }));
  }

  /** `compilation/compile` in the tab, as the server answers it: OK, or the first failure, 400 COMPILATION. */
  compile(model: PureModelContext): Promise<CompileResult> {
    return this.#pureV1.compile(model);
  }
}
