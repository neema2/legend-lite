// The engine with no legend server: the tab's planner (legend-lite, WebAssembly) turns a query into
// SQL, and one of DataCube's engines runs it -- DuckDB-WASM in the tab, or the warehouse (DuckDB on
// a server, over its SQL API, as the signed-in user). The same seam DataCube plans and runs on;
// only where the SQL runs differs.
//
// It answers execute in the engine's own shapes (a TDS result, or a graph fetch's JSON), so the
// app above cannot tell which plane ran it. What only a legend server can answer (a lambda's
// return type, an execution plan) is refused, naming why -- never approximated.

import { findAll, isFunction, type Lambda, type ValueSpecification } from '../../../pure-protocol/src/index.ts';
import type { QueryEngine } from '../engine.ts';
import type { Plan } from '../relation-type.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, type Engine } from './engine.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import type {
  CompileResult, ExecuteInput, ExecutionResult, ParameterValue, PureModelContext, RelationTypeAnswer, TdsResult,
} from './wire.ts';

/** `{p1, p2 | body}` with values: `let p1 = v1; let p2 = v2; body` -- as legend-lite's server binds them. */
export function bindParameters(lambda: Lambda, values: readonly ParameterValue[]): Lambda {
  if (lambda.parameters.length === 0) return lambda;
  const byName = new Map(values.map((v) => [v.name, v.value]));
  const missing = lambda.parameters.filter((p) => !byName.has(p.name));
  if (missing.length > 0) throw new EngineError(`Missing external parameter(s): ${missing.map((p) => p.name).join(', ')}`, 500);
  const lets: ValueSpecification[] = lambda.parameters.map((p) => ({
    _type: 'func', function: 'letFunction', parameters: [{ _type: 'string', value: p.name }, byName.get(p.name)!],
  }) as ValueSpecification);
  return { _type: 'lambda', parameters: [], body: [...lets, ...lambda.body] };
}

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
  readonly #isEnumeration: (type: string) => boolean;
  readonly #user: string;

  /**
   * `isEnumeration`: the model's enumerations, whose columns the engines carry as their names'
   * strings. `user`: who saved queries belong to (the warehouse's signed-in principal, or the
   * name this browser was configured with).
   */
  constructor(planner: WasmGrammar, engine: QueryEngine, isEnumeration: (type: string) => boolean, user: string) {
    this.#planner = planner;
    this.#engine = engine;
    this.#isEnumeration = isEnumeration;
    this.#user = user;
  }

  get name(): string {
    return this.#engine.name;
  }

  modelJson(text: string): Promise<PureModelContextData> { return this.#planner.modelJson(text); }
  lambdaJson(text: string): Promise<Lambda> { return this.#planner.lambdaJson(text); }
  lambdaText(lambda: Lambda, style: 'PRETTY' | 'STANDARD'): Promise<string> { return this.#planner.lambdaText(lambda, style); }
  relationType(model: PureModelContext, lambda: Lambda): Promise<RelationTypeAnswer> { return this.#planner.relationType(model, lambda); }

  async execute(input: ExecuteInput, signal?: AbortSignal): Promise<ExecutionResult> {
    const lambda = bindParameters(input.function, input.parameterValues ?? []);
    const planned = await this.#planner.plan(input.model, lambda, runtimeOf(lambda));
    if (findAll(lambda, isFunction).some((f) => f.function === 'serialize' || f.function.endsWith('::serialize'))) {
      // a graph fetch: the database builds the objects' JSON array in one cell
      const raw = await this.#engine.run(planned.sql, 0, signal);
      const cell = raw.columns[0]?.values[0];
      if (typeof cell !== 'string') throw new EngineError('the graph fetch returned no JSON', 500);
      const values = JSON.parse(cell) as unknown[];
      // as legend-engine answers: exactly one object is written bare
      return { builder: { _type: 'json' }, values: values.length === 1 ? values[0] : values };
    }
    const plan: Plan = { sql: planned.sql, columns: this.#columns(planned.type) };
    const t = await this.#engine.execute(plan, 0, signal);
    const rows: { values: unknown[] }[] = [];
    for (let i = 0; i < t.rowCount; i++) rows.push({ values: t.columns.map((c) => c.values[i] ?? null) });
    const result: TdsResult = {
      builder: { _type: 'tdsBuilder', columns: t.columns.map((c) => ({ name: c.name, type: c.type })) },
      activities: [{ _type: 'relational', sql: planned.sql }],
      result: { columns: t.columns.map((c) => c.name), rows },
    };
    return result;
  }

  /** The plan's columns: the compiler's types, an enumeration's as String (as the engine's TDS builder names it). */
  #columns(type: unknown): Plan['columns'] {
    const rt = type as { _type?: string; columns?: { name: string; genericType: { rawType: { fullPath?: string } } }[] };
    if (rt._type !== 'relationType' || !rt.columns) throw new EngineError('the query does not answer a table', 400);
    return rt.columns.map((c) => {
      const t = c.genericType.rawType.fullPath ?? '';
      return { name: c.name, type: this.#isEnumeration(t) ? 'String' : t };
    });
  }

  /** `compilation/compile` in the tab, the planner's whole-model compile: the server's answer, OK or its first failure. */
  async compile(model: PureModelContext): Promise<CompileResult> {
    const [first] = await this.#planner.compileErrors(model.code);
    if (first !== undefined) throw new EngineError(first, 400, 'COMPILATION');
    return { message: 'OK', defects: [] };
  }

  returnType(): Promise<string> {
    return Promise.reject(new EngineError("a lambda's return type needs a legend server; this page runs without one", 501));
  }

  generatePlan(): Promise<unknown> {
    return Promise.reject(new EngineError('an execution plan needs a legend server; the SQL button shows the tab\'s plan', 501));
  }

  currentUser(): Promise<string> {
    return Promise.resolve(this.#user);
  }
}
