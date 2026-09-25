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

import type { QueryEngine } from './engine.ts';
import type { Planner } from './cube.ts';
import type { RemoteExecutor } from './engine-remote.ts';
import type { CubeSnapshot } from './snapshot.ts';
import type { LevelScope } from './serialize.ts';
import type { ResultTable } from './result.ts';

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
  /** The Pure this product emitted. */
  readonly pure: string;
  /** The SQL, when planning got that far. */
  readonly sql: string | undefined;

  constructor(cause: unknown, pure: string, sql?: string) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
    this.name = 'QueryFailure';
    this.pure = pure;
    this.sql = sql;
  }
}

/** Whether a failure carries its query (duck-typed: errors cross realms). */
export function isQueryFailure(error: unknown): error is QueryFailure {
  return typeof error === 'object' && error !== null
    && (error as { name?: unknown }).name === 'QueryFailure'
    && typeof (error as { pure?: unknown }).pure === 'string';
}

export interface QueryRunner {
  /** For diagnostics: which arrangement answered. */
  readonly name: string;
  run(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RunOutcome>;
  /**
   * Compile WITHOUT running: resolves when the query compiles, throws
   * the compiler's refusal when it does not. Absent where this plane
   * has no compile-only call -- a remote engine, until legend-lite
   * serves upstream's `lambdaRelationType` -- and then a caller says
   * so; it never executes to find out.
   */
  compile?(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    signal?: AbortSignal,
  ): Promise<void>;
}

/**
 * Plan, then execute locally. The shape both browser planes use.
 *
 * The two halves stay visible to the controller as well, because
 * SNAPPING needs them: freezing a cube means asking the planner for
 * the source SQL and the engine to materialise it, which a remote
 * engine cannot do for us.
 */
export class PlanThenRun implements QueryRunner {
  readonly name: string;
  readonly planner: Planner;
  readonly engine: QueryEngine;

  constructor(planner: Planner, engine: QueryEngine) {
    this.planner = planner;
    this.engine = engine;
    this.name = `plan+${engine.name}`;
  }

  async run(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RunOutcome> {
    let sql: string;
    try {
      sql = await this.planner.plan(pureGrammar, snapshot, scope, signal);
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, pureGrammar);
    }
    try {
      const rows = await this.engine.execute(sql, snapshot.epoch, signal);
      return { rows, sql };
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, pureGrammar, sql);
    }
  }

  /** Planning IS compiling here: the planner compiles, nothing runs. */
  async compile(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    signal?: AbortSignal,
  ): Promise<void> {
    await this.planner.plan(pureGrammar, snapshot, undefined, signal);
  }
}

/** One call: the engine runs it and sends rows back. */
export class RemoteRun implements QueryRunner {
  readonly name = 'engine';
  readonly executor: RemoteExecutor;

  constructor(executor: RemoteExecutor) {
    this.executor = executor;
  }

  async run(
    pureGrammar: string,
    snapshot: CubeSnapshot,
    scope?: LevelScope,
    signal?: AbortSignal,
  ): Promise<RunOutcome> {
    try {
      const out = await this.executor.execute(
        pureGrammar, snapshot, scope, signal,
      );
      return { rows: out.rows, sql: out.sql };
    } catch (error: unknown) {
      if (signal?.aborted) throw error;
      throw new QueryFailure(error, pureGrammar);
    }
  }
}
