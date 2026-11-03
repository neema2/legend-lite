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
import { QueryError, type QueryEngine, type RawTable } from '../engine.ts';
import { answerStatement, keyedStatement } from '../plan-record.ts';
import { hostSession } from '../plan-runner.ts';
import type { HeldConnection } from '../duckdb.ts';
import type { PureModelContextData } from './pmcd.ts';
import { EngineError, type Engine } from './engine.ts';
import type { WasmGrammar } from './wasm-grammar.ts';
import type { CompileResult, ExecuteInput, ExecutionResult, PureModelContext, RelationTypeAnswer } from './wire.ts';
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
    const plan = await this.#planner.executePlan(input);
    const keyed = keyedStatement(plan);
    if (keyed !== undefined) return this.#keyed(plan, keyed, signal);
    // the plan's one statement, its rows handed back as the engine's decoder read them
    let raw: RawTable;
    try {
      raw = await this.#engine.run(answerStatement(plan), 0, signal);
    } catch (e) {
      // the database refusing the statement: answered as legend-engine answers it, by the planner (D18 B), from the
      // database's own words
      if (e instanceof QueryError) return this.#planner.executeRefusal(plan, e.message, e.message);
      throw e;
    }
    return this.#planner.executeAnswer(plan, hostRows(raw));
  }

  /**
   * A pivot whose columns its data names, run as the JVM's runner runs it (D18 D), on the tab's database held for it,
   * in one transaction (so the keys are the body's): its keys, then the body the planner writes for them, described
   * (the planner's `describe` operation: DuckDB's own type names, legend-engine's header's source -- its Arrow writes a
   * sum of integers as a decimal) and run with the keys bound; the answer the planner's, from the rows and the report.
   * The transaction's and the description's statements are the planner's (host operations, by the plan's database).
   * Once begun it runs to its end: DuckDB-WASM cannot interrupt a statement inside its call, so `signal` is checked
   * before anything runs. An engine that holds no connection (a server's) answers no such plan.
   */
  async #keyed(plan: string, keyed: { readonly statement: string; readonly databaseType: string }, signal?: AbortSignal): Promise<string> {
    const engine = this.#engine;
    if (engine.exclusive === undefined) {
      throw new EngineError(`a pivot whose columns its data names runs its keys first, on a database held for it, and ${engine.name} holds none`, 500);
    }
    if (signal?.aborted) throw signal.reason ?? new Error('aborted');
    return engine.exclusive(async (db: HeldConnection) => {
      const session = await hostSession(keyed.databaseType, (sql, params) => db.rows(sql, params), this.#planner);
      let keys: string;
      let rows: string;
      let reported: string;
      await session.transaction('begin');
      try {
        const read = await db.table(keyed.statement);
        keys = JSON.stringify(Array.from({ length: read.rowCount }, (_, r) => read.columns.map((c) => {
          const v = c.values[r];
          return v === null || v === undefined ? null : String(v);
        })));
        const body = await this.#planner.keyedBody(plan, keys);
        const [describe] = await this.#planner.hostOperations(keyed.databaseType, [{ op: 'describe', statement: body.statement }]);
        if (describe?.length !== 1) throw new EngineError(`the planner describes a statement in one statement, not ${describe?.length ?? 0}`, 500);
        const described = await db.table(describe[0]!, body.parameters);
        const types = described.columns.find((c) => c.name === 'column_type');
        if (types === undefined) throw new EngineError("the description answered no column_type: the tab's database reports no column types", 500);
        reported = JSON.stringify(types.values.map((v) => String(v)));
        rows = hostRows(await db.table(body.statement, body.parameters));
      } catch (e) {
        await session.transaction('rollback');
        // the database refusing a statement: answered as legend-engine answers it (D18 B)
        if (e instanceof QueryError) return this.#planner.executeRefusal(plan, e.message, e.message);
        throw e;
      }
      await session.transaction('commit');
      return this.#planner.keyedAnswer(plan, keys, rows, reported);
    });
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
