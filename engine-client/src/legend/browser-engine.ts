// The engine with no legend server: the tab's planner (legend-lite, WebAssembly) turns a query into
// SQL, and one of DataCube's engines runs it -- DuckDB-WASM in the tab, or the warehouse (DuckDB on
// a server, over its SQL API, as the signed-in user). The same seam DataCube plans and runs on;
// only where the SQL runs differs.
//
// It answers legend-engine's API with the Java the server and Python answer it with
// (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C1): an execute is planned by the planner (its parameters bound, the typed
// statement the server runs, PureV1Api.jsonPlan), run on the tab's database, and its rows -- handed back exactly as the
// engine's decoder read them -- decoded by the plan's columns and written by the server's own writer
// (PureV1Api.jsonAnswer), so its answer's text is the server's: the database computes every value, the planner's
// writer spells them, nothing here does either. A lambda's return type and an execution plan are the planner's
// pure/v1, as the server routes them.

import { findAll, isFunction, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { QueryEngine } from '../engine.ts';
import { planParams } from '../plan-params.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, type Engine } from './engine.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import type { CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer } from './wire.ts';
import type { RawTable } from '../engine.ts';
import type { Scalar } from '../result.ts';
import { inRun } from '../run-frame.ts';
import { runPlanRows, type ParameterValue } from '../plan-runner.ts';
import type { PlanText } from '../planned-sql.ts';

/** The runtime a query runs on: the last argument of its `->from(...)`. */
export function runtimeOf(lambda: Lambda): string {
  const from = findAll(lambda, isFunction).find((f) => f.function === 'from' || f.function.endsWith('::from'));
  const last = from?.parameters[from.parameters.length - 1];
  if (last?._type !== 'packageableElementPtr') throw new EngineError('the query names no runtime: its from(mapping, runtime) has none', 400);
  return last.fullPath;
}

/**
 * A QUERY'S ANSWER IN LITE'S OWN FORM (2026-11-17, the edge program's H3): the statement its plan answers it with, the
 * answer's shape (a relation's rows, a value's elements, a graph fetch's objects one per row), and its rows typed by
 * the plan's columns -- each cell the value the database computed, as the engine's decoder read it. Nothing here is
 * text a writer spelled: an app's grid reads the values by their types, and legend-engine's spellings are the edge's.
 */
export interface LiteAnswer {
  readonly sql: string;
  readonly shape: 'RELATION' | 'VALUE' | 'OBJECTS';
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly rows: readonly (readonly unknown[])[];
}

/**
 * WHAT STUDIO AND QUERY ASK OF LEGEND-LITE (2026-11-17, the edge program's H3), each in lite's own form: a model's and a
 * lambda's text read, a lambda printed, its relation type, a model's every compile error, and a query run -- planned by
 * lite's planner, run by the hosts' runner, answered as typed rows (LiteAnswer). In the tab (TabEngine) or of a
 * server's /api/lite/v1 (H7). legend-engine's API (Engine, below) is the edge's.
 */
export interface LiteEngine {
  readonly name: string;
  modelJson(text: string): Promise<PureModelContextData>;
  lambdaJson(text: string): Promise<Lambda>;
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string>;
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer>;
  compileErrors(code: string): Promise<string[]>;
  /** The statement `lambda`'s plan answers it with (Query's "show SQL"), planned and not run. */
  statement(model: PureModelContext, lambda: Lambda): Promise<string>;
  /** `lambda` run over `model`, its run's `parameters` bound at run (plan-runner.ts checkParameters). */
  run(model: PureModelContext, lambda: Lambda, parameters?: Readonly<Record<string, ParameterValue>>,
    signal?: AbortSignal): Promise<LiteAnswer>;
  currentUser(): Promise<string>;
}

/** The answer `plan` states: its shape and the statement that answers it (a dynamic pivot's: its body). */
function stated(plan: PlanText): { readonly shape: LiteAnswer['shape']; readonly sql: string } {
  type Node = { readonly _type?: string; readonly shape?: LiteAnswer['shape']; readonly sql?: { readonly statement?: string };
    readonly steps?: readonly Node[]; readonly body?: { readonly statement?: string } };
  const root = (JSON.parse(plan) as { readonly root?: Node }).root;
  if (root?._type === 'keyed') return { shape: 'RELATION', sql: root.body?.statement ?? '' };
  if (root?._type === 'constant') return { shape: 'VALUE', sql: '' };
  const result = root?._type === 'sequence' ? root.steps?.find((s) => s._type === 'tdsResult' || s._type === 'constant') : root;
  if (result?._type === 'constant') return { shape: 'VALUE', sql: '' };
  if (result?._type !== 'tdsResult' || result.shape === undefined) {
    throw new EngineError(`a query's plan answers with a typed result, not a ${String(result?._type)}`, 500);
  }
  return { shape: result.shape, sql: result.sql?.statement ?? '' };
}

/**
 * LITE'S ENGINE IN THE TAB (2026-11-17, the edge program's H3): Studio's and Query's in-tab grammar, typing and compile
 * through the planner's lite kinds (wasm-grammar.ts), and a query run as DataCube's tab runs one -- its execution plan
 * from the planner (the model held once per text), run by the hosts' runner (plan-runner.ts runPlanRows) on `engine`,
 * its rows typed by the plan's columns. No legend-engine route is asked.
 */
export class TabEngine implements LiteEngine {
  readonly #planner: WasmGrammar;
  readonly #engine: QueryEngine;
  readonly #databaseType: string;
  readonly #user: string;

  /** `engine`: where the plan's statements run (the tab's DuckDB, pointed at a project's database, or the warehouse);
   *  `databaseType`: its type, which the plan's target must name. */
  constructor(planner: WasmGrammar, engine: QueryEngine, databaseType: string, user: string) {
    this.#planner = planner;
    this.#engine = engine;
    this.#databaseType = databaseType;
    this.#user = user;
  }

  get name(): string {
    return this.#engine.name;
  }

  modelJson(text: string): Promise<PureModelContextData> { return this.#planner.modelJson(text); }
  lambdaJson(text: string): Promise<Lambda> { return this.#planner.lambdaJson(text); }
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#planner.lambdaText(lambda, style); }
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> { return this.#planner.relationType(model, lambda); }
  compileErrors(code: string): Promise<string[]> { return this.#planner.compileErrors(code); }

  async statement(model: PureModelContext, lambda: Lambda): Promise<string> {
    return stated(await this.#planner.executionPlan(model, lambda, runtimeOf(lambda))).sql;
  }

  async run(model: PureModelContext, lambda: Lambda, parameters: Readonly<Record<string, ParameterValue>> = {},
    signal?: AbortSignal): Promise<LiteAnswer> {
    // planned outside the run (a run plans nothing: run-frame.ts), then run inside one
    const plan = await this.#planner.executionPlan(model, lambda, runtimeOf(lambda));
    const { shape, sql } = stated(plan);
    const engine = this.#engine;
    const answered = await inRun('a query', () => runPlanRows(plan, {
      databaseType: this.#databaseType,
      // each statement on the engine, its values bound in order, its rows as its decoder read them
      run: async (statement, params) => rowsOf(await engine.run(statement, 0, signal, params)),
    }, { parameters }));
    return { sql, shape, columns: answered?.columns ?? [], rows: answered?.rows ?? [] };
  }

  currentUser(): Promise<string> {
    return Promise.resolve(this.#user);
  }
}

/** A raw result's rows, each its cells in its columns' order. */
function rowsOf(raw: RawTable): unknown[][] {
  return Array.from({ length: raw.rowCount }, (_, r) => raw.columns.map((c) => c.values[r]));
}

export class BrowserEngine implements Engine {
  readonly #planner: WasmGrammar;
  readonly #engine: QueryEngine;
  readonly #user: string;

  /**
   * `engine`: where the SQL runs -- the tab's DuckDB, its session set up as it was opened (engine.ts setUpSession), or
   * a server's (the warehouse), on that server's sessions. `user`: who saved queries belong to (the warehouse's
   * signed-in principal, or the name this browser was configured with).
   */
  constructor(planner: WasmGrammar, engine: QueryEngine, user: string) {
    this.#planner = planner;
    this.#engine = engine;
    this.#user = user;
  }

  get name(): string {
    return this.#engine.name;
  }

  modelJson(text: string): Promise<PureModelContextData> { return this.#planner.modelJson(text); }
  lambdaJson(text: string): Promise<Lambda> { return this.#planner.lambdaJson(text); }
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#planner.lambdaText(lambda, style); }
  modelText(model: PureModelContextData, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#planner.modelText(model, style); }
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> { return this.#planner.relationType(model, lambda); }

  async execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> {
    // the answer's text is the server's: read as a server's answer is
    return JSON.parse(await this.executeText(input, signal)) as ExecutionResult;
  }

  /** `execute`'s answer as its text: planned, run on the engine, and written by the server's own writer. */
  async executeText(input: ExecuteInput, signal?: AbortSignal): Promise<string> {
    const { plan, text } = await this.#planner.executePlan(input);
    // the plan's one statement, its values (a data: payload: plan-params.ts, M3) bound to its `?`s, its rows handed back
    // as the engine's decoder read them
    // run as a run (run-frame.ts: a run plans nothing): the planner asked before it, and after it for the answer's text
    const raw = await inRun('a query', () => this.#engine.run(plan.run, 0, signal, planParams(plan.plan)));
    return this.#planner.executeAnswer(text, hostRows(raw));
  }

  /** `compilation/compile` in the tab, answered by the planner as the server answers it: OK, or its first failure. */
  compile(model: PureModelContext): Promise<CompileResult> {
    return this.#planner.compile(model);
  }

  returnType(model: PureModelContext, lambda: Lambda): Promise<string> {
    return this.#planner.returnType(model, lambda);
  }

  generatePlan(input: ExecuteInput): Promise<unknown> {
    return this.#planner.generatePlan(input);
  }

  currentUser(): Promise<string> {
    return Promise.resolve(this.#user);
  }
}

/**
 * A raw result's rows as the planner reads a host's rows back (PureV1Api.jsonAnswer; result.HostRows on the Java side):
 * a JSON array of objects, each a column's name and its cell exactly as the engine's decoder read it -- an integer's
 * digits (a bigint's too), a double's shortest digits (a non-finite one as its name in a string), a text, a boolean,
 * null. A decimal, a date, a date-time and a structure arrive as their text; the plan's columns say how each reads.
 */
export function hostRows(raw: RawTable): string {
  const rows: string[] = [];
  for (let r = 0; r < raw.rowCount; r++) {
    const cells = raw.columns.map((c) => `${JSON.stringify(c.name)}:${cellToken(c.values[r] ?? null)}`);
    rows.push(`{${cells.join(',')}}`);
  }
  return `[${rows.join(',')}]`;
}

function cellToken(v: Scalar): string {
  if (v === null) return 'null';
  if (typeof v === 'bigint') return v.toString();
  // -0 as its own digits (String(-0) is "0"): the server's Double.toString and Python's repr keep the sign
  if (typeof v === 'number') return Object.is(v, -0) ? '-0.0' : Number.isFinite(v) ? String(v) : JSON.stringify(String(v));
  return JSON.stringify(v);
}
