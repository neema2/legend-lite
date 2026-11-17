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
import { type Engine } from './engine.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import type { CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer } from './wire.ts';
import type { RawTable } from '../engine.ts';
import type { Scalar } from '../result.ts';
import { inRun } from '../run-frame.ts';
import { runPlanRows, type ParameterValue } from '../plan-runner.ts';
import { LiteRefusal, type LiteError } from '../lite-error.ts';
import type { QueryColumn } from '../relation-type.ts';
// a run's parameter value, as an app hands it to lite's engine (the apps reach the run side through the doors alone)
export type { ParameterValue } from '../plan-runner.ts';

/** The runtime a query runs on: the last argument of its `->from(...)`. */
export function runtimeOf(lambda: Lambda): string {
  const from = findAll(lambda, isFunction).find((f) => f.function === 'from' || f.function.endsWith('::from'));
  const last = from?.parameters[from.parameters.length - 1];
  if (last?._type !== 'packageableElementPtr') throw new LiteRefusal({ kind: 'request', message: 'the query names no runtime: its from(mapping, runtime) has none' });
  return last.fullPath;
}

/**
 * A QUERY'S ANSWER IN LITE'S OWN FORM (lite's API v1 spec §2 and §4, `query/run`): the answer's shape (a relation's rows,
 * a value's elements, a graph fetch's objects one per row), its columns' names and Pure types, and its rows -- each cell
 * the value the database computed, read exactly (an Integer a number while safe, a bigint beyond; a Decimal its text).
 * Its statement is `query/plan`'s (statementOf). Nothing here is text a writer spelled: legend-engine's spellings are
 * the edge's.
 */
export interface LiteAnswer {
  readonly shape: 'RELATION' | 'VALUE' | 'OBJECTS';
  readonly columns: readonly { readonly name: string; readonly type: string }[];
  readonly rows: readonly (readonly unknown[])[];
}

/**
 * LITE'S API V1, ONE TYPESCRIPT CLIENT (spec §2 and §7, the user's go of 2026-11-17): each method one operation, named
 * by the spec's rule (`a/b` is `aB`), its answer the operation's. Two bindings: in the tab (TabEngine, the planner's
 * kinds and the hosts' runner) and over a server's `/api/lite/v1` (doors/lite-server.ts LiteServer). A model travels as
 * its text, a query as its tree. A refusal is lite's (LiteRefusal). legend-engine's API (Engine, below) is the edge's.
 */
export interface LiteEngine {
  readonly name: string;
  /** `model/check`: every error of the model, none when it compiles. */
  modelCheck(model: string): Promise<readonly LiteError[]>;
  /** `model/elements`: the model's protocol elements. */
  modelElements(model: string): Promise<PureModelContextData>;
  /** `query/parse`: the query's tree, its numbers exact. */
  queryParse(text: string): Promise<Lambda>;
  /** `query/print`: the query's text, pretty unless standard is asked. */
  queryPrint(query: Lambda, style?: 'pretty' | 'standard'): Promise<string>;
  /** `query/type`: each column's name and Pure type. */
  queryType(model: string, query: Lambda): Promise<readonly QueryColumn[]>;
  /** `query/plan`: lite's plan of the query on `runtime`, its JSON text (data: its statements, slots and columns). */
  queryPlan(model: string, query: Lambda, runtime: string): Promise<string>;
  /** `query/run`: the query planned and run on `runtime`, its run's `params` bound at run (plan-runner.ts
   *  checkParameters). */
  queryRun(model: string, query: Lambda, runtime: string, params?: Readonly<Record<string, ParameterValue>>,
    signal?: AbortSignal): Promise<LiteAnswer>;
}

/** The answer `plan` (lite's execution plan, its JSON) states: its shape and the statement that answers it (a dynamic
 *  pivot's: its body). Read, never sent. */
export function stated(plan: string): { readonly shape: LiteAnswer['shape']; readonly sql: string } {
  type Node = { readonly _type?: string; readonly shape?: LiteAnswer['shape']; readonly sql?: { readonly statement?: string };
    readonly steps?: readonly Node[]; readonly body?: { readonly statement?: string } };
  const root = (JSON.parse(plan) as { readonly root?: Node }).root;
  if (root?._type === 'keyed') return { shape: 'RELATION', sql: root.body?.statement ?? '' };
  if (root?._type === 'constant') return { shape: 'VALUE', sql: '' };
  const result = root?._type === 'sequence' ? root.steps?.find((s) => s._type === 'tdsResult' || s._type === 'constant') : root;
  if (result?._type === 'constant') return { shape: 'VALUE', sql: '' };
  if (result?._type !== 'tdsResult' || result.shape === undefined) {
    throw new LiteRefusal({ kind: 'internal', message: `a query's plan answers with a typed result, not a ${String(result?._type)}` });
  }
  return { shape: result.shape, sql: result.sql?.statement ?? '' };
}

/** The statement `plan` (a `query/plan` answer's plan) answers with: what Query's and Studio's "SQL" show; none for a
 *  value the plan holds itself. */
export function statementOf(plan: string): string {
  return stated(plan).sql;
}

/**
 * LITE'S API V1 IN THE TAB (the browser binding of the client, spec §5): each operation the planner's kind of its name
 * (wasm-grammar.ts WasmGrammar), and `query/run` as DataCube's tab runs a query -- `query/plan` (the model held once per
 * text) run by the hosts' runner (plan-runner.ts runPlanRows) on `engine`, its rows typed by the plan's columns. No
 * legend-engine route is asked.
 */
export class TabEngine implements LiteEngine {
  readonly #planner: WasmGrammar;
  readonly #engine: QueryEngine;
  readonly #databaseType: string;

  /** `engine`: where the plan's statements run (the tab's DuckDB, pointed at a project's database, or the warehouse);
   *  `databaseType`: its type, which the plan's target must name. */
  constructor(planner: WasmGrammar, engine: QueryEngine, databaseType: string) {
    this.#planner = planner;
    this.#engine = engine;
    this.#databaseType = databaseType;
  }

  get name(): string {
    return this.#engine.name;
  }

  modelCheck(model: string): Promise<readonly LiteError[]> { return this.#planner.modelCheck(model); }
  modelElements(model: string): Promise<PureModelContextData> { return this.#planner.modelElements(model); }
  queryParse(text: string): Promise<Lambda> { return this.#planner.queryParse(text); }
  queryPrint(query: Lambda, style?: 'pretty' | 'standard'): Promise<string> { return this.#planner.queryPrint(query, style); }
  queryType(model: string, query: Lambda): Promise<readonly QueryColumn[]> { return this.#planner.queryType(model, query); }
  queryPlan(model: string, query: Lambda, runtime: string): Promise<string> { return this.#planner.queryPlan(model, query, runtime); }

  async queryRun(model: string, query: Lambda, runtime: string, params: Readonly<Record<string, ParameterValue>> = {},
    signal?: AbortSignal): Promise<LiteAnswer> {
    // planned outside the run (a run plans nothing: run-frame.ts), then run inside one
    const plan = await this.#planner.queryPlan(model, query, runtime);
    const { shape } = stated(plan);
    const engine = this.#engine;
    const answered = await inRun('a query', () => runPlanRows(plan, {
      databaseType: this.#databaseType,
      // each statement on the engine, its values bound in order, its rows as its decoder read them
      run: async (statement, values) => rowsOf(await engine.run(statement, 0, signal, values)),
    }, { parameters: params }));
    // each column its name and its Pure type, as the server's answer states it (LiteV1Planning.Answered)
    return { shape, columns: (answered?.columns ?? []).map((c) => ({ name: c.name, type: c.type })), rows: answered?.rows ?? [] };
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
