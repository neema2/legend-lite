// Who turns Pure into rows.
//
// There are two answers, and the difference is not a detail: it is
// where the data lives and who holds the connection to it.
//
// PLAN THEN RUN -- Pure to SQL through a planner, SQL to rows through
// a local engine. Both of our browser planes are this: the wasm
// planner or legend-lite compiles, DuckDB-WASM executes, the data is
// in the tab. It is also what upstream does for a CACHED source.
//
// REMOTE RUN -- Pure to rows in one call, because the engine runs the
// query itself against a store the browser cannot reach. That is
// upstream's uncached path (`_runQuery` posts to
// `execution/execute`), and the SQL it reports is a fact about what
// happened, not an instruction to anyone.
//
// The controller asks a RUNNER, so the hot path holds no branch and
// the choice is made once, where the app is constructed. That matters
// for the same reason `test/guardrails.test.ts` exists: shipped code
// must not be able to pick a planner at RUNTIME -- the one time it
// could, a health-check fallback hid three real bugs for the life of
// the project.

import type { Lambda } from '../../pure-protocol/src/index.ts';
import type { PrintStyle } from '../../engine-client/src/pure-v1.ts';
import type { QueryEngine } from '../../engine-client/src/engine.ts';
import type { Planner } from './cube.ts';
import type { RemoteExecutor } from '../../engine-client/src/engine-remote.ts';
import type { CubeSnapshot } from './snapshot.ts';
import type { LevelScope } from './query.ts';
import type { ResultTable } from '../../engine-client/src/result.ts';
import type { Plan, PlanColumn } from '../../engine-client/src/relation-type.ts';

export interface RunOutcome {
  readonly rows: ResultTable;
  /**
   * The SQL behind those rows.
   *
   * Generated here in the plan-then-run shape; reported by the engine
   * in the remote one. Either way it is what the query panes show,
   * and nothing re-runs it.
   */
  readonly sql: string;
}

/**
 * A query that failed, with what was sent: upstream's
 * DataCubeExecutionError (`queryCode`, `executeInput`). The message is
 * the cause's own, so every existing reader of `.message` sees what it
 * saw before; the alert's "Show debug info?" reads the rest.
 */
export class QueryFailure extends Error {
  /** The query this product built (a protocol tree; the compiler prints it for a person). */
  readonly query: Lambda;
  /** The SQL, when planning got that far. */
  readonly sql: string | undefined;

  constructor(cause: unknown, query: Lambda, sql?: string) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = 'QueryFailure';
    this.query = query;
    this.sql = sql;
  }
}

/** Whether a failure carries its query (duck-typed: errors cross realms). */
export function isQueryFailure(error: unknown): error is QueryFailure {
  return typeof error === 'object' && error !== null
    && (error as { name?: unknown }).name === 'QueryFailure'
    && typeof (error as { query?: { _type?: unknown } }).query?._type === 'string';
}

export interface QueryRunner {
  /** For diagnostics: which arrangement answered. */
  readonly name: string;
  run(query: Lambda, snapshot: CubeSnapshot, scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome>;
  /**
   * Run a query and hand its rows over a chunk at a time, none kept: reading every row of
   * a column in flat memory (`JsonColumnReader.all`). A plane that returns whole results
   * hands the one result over as one chunk. Resolves after the last chunk.
   */
  stream(query: Lambda, snapshot: CubeSnapshot, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void>;
  /**
   * Compile WITHOUT running: resolves when the query compiles, throws
   * the compiler's refusal when it does not. It never executes to find out.
   */
  compile(query: Lambda, snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void>;
  /**
   * The compiler's type of a query's result, compile-only (upstream
   * `lambdaRelationType` on every plane): how a cube's source and
   * calculated columns are typed before a level query runs.
   */
  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]>;
  /** What a person typed, as its lambda: the compiler's parse (E1 or its twin in the tab). */
  parse(text: string, signal?: AbortSignal): Promise<Lambda>;
  /** A query as Pure text for a person to read: the compiler's print (E4 or its twin). */
  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string>;
}

/** The planner makes the SQL, the tab's engine runs it. */
export class PlanThenRun implements QueryRunner {
  readonly name: string;
  readonly planner: Planner;
  readonly engine: QueryEngine;

  constructor(planner: Planner, engine: QueryEngine) {
    this.planner = planner;
    this.engine = engine;
    this.name = `plan+${engine.name}`;
  }

  async run(query: Lambda, snapshot: CubeSnapshot, _scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    let plan: Plan;
    try {
      plan = await this.planner.plan(query, signal);
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, query);
    }
    const sql = plan.sql;
    try {
      // the engine types every column by the plan (engine.ts typedByPlan)
      return { rows: await this.engine.execute(plan, snapshot.epoch, signal), sql };
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, query, sql);
    }
  }

  async stream(
    query: Lambda,
    snapshot: CubeSnapshot,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    let plan: Plan;
    try {
      plan = await this.planner.plan(query, signal);
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, query);
    }
    try {
      await this.engine.stream(plan, snapshot.epoch, onChunk, signal);
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, query, plan.sql);
    }
  }

  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    return this.planner.relationType(query, signal);
  }

  /** Planning IS compiling here: the planner compiles, nothing runs. */
  async compile(query: Lambda, _snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void> {
    await this.planner.plan(query, signal);
  }

  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.planner.parse(text, signal);
  }

  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string> {
    return this.planner.print(query, style, signal);
  }
}

/** One call: the engine runs it and sends rows back. */
export class RemoteRun implements QueryRunner {
  readonly name = 'engine';
  readonly executor: RemoteExecutor;

  constructor(executor: RemoteExecutor) {
    this.executor = executor;
  }

  async run(query: Lambda, snapshot: CubeSnapshot, _scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    try {
      const out = await this.executor.execute(query, snapshot.epoch, signal);
      return { rows: out.rows, sql: out.sql };
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, query);
    }
  }

  /** The engine's compile-only call: its `lambdaRelationType` answers or refuses. */
  /** The engine returns a whole result: handed over as one chunk. */
  async stream(
    query: Lambda,
    snapshot: CubeSnapshot,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    onChunk((await this.run(query, snapshot, undefined, signal)).rows);
  }

  async compile(query: Lambda, _snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void> {
    await this.executor.relationType(query, signal);
  }

  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    return this.executor.relationType(query, signal);
  }

  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.executor.parse(text, signal);
  }

  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string> {
    return this.executor.print(query, style, signal);
  }
}
