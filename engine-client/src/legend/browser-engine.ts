// LITE'S API V1 IN TYPESCRIPT (spec §2 and §7, the user's go of 2026-11-17): one client by operation (LiteEngine), its
// tab binding (TabEngine: the tab's planner -- legend-lite, WebAssembly -- plans, and one of DataCube's engines runs the
// plan: DuckDB-WASM in the tab, or the warehouse, DuckDB on a server over its SQL API, as the signed-in user), and what
// the apps read of an answer (runtimeOf, statementOf). legend-engine's API in the tab (BrowserEngine) is the edge's
// (edge-engine.ts, //engine-client:edge; H11, 2026-11-17).

import { findAll, isFunction, type Lambda } from '../../../pure-protocol/src/index.ts';
import type { QueryEngine, RawTable } from '../engine.ts';
import type { PureModelContextData } from './pmcd.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import { inRun } from '../run-frame.ts';
import { runPlanRows } from '../plan-runner.ts';
import type { ParameterValue } from '../requests.ts';
import { LiteRefusal, type LiteError } from '../lite-error.ts';
import type { QueryColumn } from '../relation-type.ts';
// a run's parameter value, as an app hands it to lite's engine (the apps reach the run side through the doors alone)
export type { ParameterValue } from '../requests.ts';

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
 * its text, a query as its tree. A refusal is lite's (LiteRefusal). legend-engine's API (Engine, BrowserEngine) is the
 * edge's (edge-engine.ts).
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
