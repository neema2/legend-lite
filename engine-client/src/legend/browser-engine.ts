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
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, type Engine } from './engine.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import type { CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer } from './wire.ts';
import type { RawTable } from '../engine.ts';
import type { Scalar } from '../result.ts';

/** The runtime a query runs on: the last argument of its `->from(...)`. */
export function runtimeOf(lambda: Lambda): string {
  const from = findAll(lambda, isFunction).find((f) => f.function === 'from' || f.function.endsWith('::from'));
  const last = from?.parameters[from.parameters.length - 1];
  if (last?._type !== 'packageableElementPtr') throw new EngineError('the query names no runtime: its from(mapping, runtime) has none', 400);
  return last.fullPath;
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
    // the plan's one statement, its rows handed back as the engine's decoder read them
    const raw = await this.#engine.run(plan.run, 0, signal);
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
