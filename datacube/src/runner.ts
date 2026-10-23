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

import { ExactNumber, type Lambda, type ValueSpecification } from '../../pure-protocol/src/index.ts';
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

/**
 * A REWRITE OF EVERY QUERY A GRID RUNS, set by its host and never part of the grid's own state (a dashboard's page
 * filters on its class: docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.2, page-filters.ts).
 */
export type QueryLens = (query: Lambda) => Lambda;

/**
 * `runner`, each query it plans, runs, compiles or types first rewritten by `lens`: a grid's levels, its typing, its
 * pivot's values, its charts', its exports' and its drills' queries alike. A parse and a print are the person's own.
 */
export function lensed(runner: QueryRunner, lens: QueryLens): QueryRunner {
  return {
    name: runner.name,
    run: (query, snapshot, scope, signal) => runner.run(lens(query), snapshot, scope, signal),
    stream: (query, snapshot, onChunk, signal) => runner.stream(lens(query), snapshot, onChunk, signal),
    compile: (query, snapshot, signal) => runner.compile(lens(query), snapshot, signal),
    relationType: (query, signal) => runner.relationType(lens(query), signal),
    parse: (text, signal) => runner.parse(text, signal),
    print: (query, style, signal) => runner.print(query, style, signal),
  };
}

/** Whether two parts of a query are the same, part for part (the order a reader wrote an object's keys in aside). */
function sameSpec(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a instanceof ExactNumber || b instanceof ExactNumber) {
    return a instanceof ExactNumber && b instanceof ExactNumber && a.text === b.text;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    return Array.isArray(a) && Array.isArray(b) && a.length === b.length && a.every((x, i) => sameSpec(x, b[i]));
  }
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false;
  const ka = Object.keys(a).filter((k) => (a as Record<string, unknown>)[k] !== undefined);
  const kb = Object.keys(b).filter((k) => (b as Record<string, unknown>)[k] !== undefined);
  return ka.length === kb.length
    && ka.every((k) => sameSpec((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]));
}

/** `node` with every part the same as `live` (sameSpec) read as `copy` instead. */
function rebase<T>(node: T, live: ValueSpecification, copy: ValueSpecification): T {
  const walk = (n: unknown): unknown => {
    if (sameSpec(n, live)) return copy;
    if (Array.isArray(n)) return n.map(walk);
    if (n === null || typeof n !== 'object' || n instanceof ExactNumber) return n;
    return Object.fromEntries(Object.entries(n).map(([k, v]) => [k, walk(v)]));
  };
  return walk(node) as T;
}

/**
 * `runner`, reading `copy` wherever a query reads `live`: a snapped grid's runner (cube.ts, §18.3). Its copy has a
 * model of its own, declaring the copy alone, so a query a host built from the grid's live base -- a chart's, Ad Hoc
 * mode's, a drill-through's, a calculated column's preview -- reads the copy, as the grid's own levels do. A parse and
 * a print are the person's own.
 */
export function rebased(runner: QueryRunner, live: ValueSpecification, copy: ValueSpecification): QueryRunner {
  const onCopy = (snapshot: CubeSnapshot): CubeSnapshot => rebase(snapshot, live, copy);
  return {
    name: runner.name,
    run: (query, snapshot, scope, signal) => runner.run(rebase(query, live, copy), onCopy(snapshot), scope, signal),
    stream: (query, snapshot, onChunk, signal) =>
      runner.stream(rebase(query, live, copy), onCopy(snapshot), onChunk, signal),
    compile: (query, snapshot, signal) => runner.compile(rebase(query, live, copy), onCopy(snapshot), signal),
    relationType: (query, signal) => runner.relationType(rebase(query, live, copy), signal),
    parse: (text, signal) => runner.parse(text, signal),
    print: (query, style, signal) => runner.print(query, style, signal),
  };
}

/**
 * WHERE A QUERY IS PLANNED AND RUN, as one step (docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15): a host's two pieces --
 * its planner's plan, and its runner executing the plan's SQL on the connection, the rows typed by the plan by the
 * executor of the language they become (§15.3a) -- and the questions only the planner answers. The tab's host plans in
 * the page and runs on its own database (`localHost`); a Python host plans and runs in Python, in one call
 * (src/services/python-host.ts). Every grid on every host runs through `PlanAndRun` over one of these.
 */
export interface PlanAndRunHost {
  /** For diagnostics: which arrangement answered. */
  readonly name: string;
  /** Who answers the compiler's questions: a query's columns, a parse, a print, a plan alone (compile). */
  readonly planner: Planner;
  /**
   * The query planned and its plan's SQL run: the plan, and the rows typed by it. A failure is a QueryFailure: before
   * the plan, without SQL; after it, with the SQL that was run.
   */
  planAndRun(query: Lambda, epoch: number, signal?: AbortSignal): Promise<{ readonly plan: Plan; readonly rows: ResultTable }>;
  /** The same, its rows handed over a chunk at a time (`QueryRunner.stream`). */
  stream(query: Lambda, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void>;
}

/** Every grid's runner, on every host: the host plans and runs, and the planner answers the compiler's questions. */
export class PlanAndRun implements QueryRunner {
  readonly name: string;
  readonly host: PlanAndRunHost;

  constructor(host: PlanAndRunHost) {
    this.host = host;
    this.name = host.name;
  }

  async run(query: Lambda, snapshot: CubeSnapshot, _scope?: LevelScope, signal?: AbortSignal): Promise<RunOutcome> {
    const { plan, rows } = await this.host.planAndRun(query, snapshot.epoch, signal);
    return { rows, sql: plan.sql };
  }

  stream(
    query: Lambda,
    snapshot: CubeSnapshot,
    onChunk: (chunk: ResultTable) => void,
    signal?: AbortSignal,
  ): Promise<void> {
    return this.host.stream(query, snapshot.epoch, onChunk, signal);
  }

  relationType(query: Lambda, signal?: AbortSignal): Promise<PlanColumn[]> {
    return this.host.planner.relationType(query, signal);
  }

  /** Planning IS compiling: the planner compiles, nothing runs. */
  async compile(query: Lambda, _snapshot: CubeSnapshot, signal?: AbortSignal): Promise<void> {
    await this.host.planner.plan(query, signal);
  }

  parse(text: string, signal?: AbortSignal): Promise<Lambda> {
    return this.host.planner.parse(text, signal);
  }

  print(query: Lambda, style?: PrintStyle, signal?: AbortSignal): Promise<string> {
    return this.host.planner.print(query, style, signal);
  }
}

/** A step's failure as the runner says it: the plan's (no SQL yet), or the run's (with the SQL that ran). */
async function step<T>(query: Lambda, sql: string | undefined, signal: AbortSignal | undefined, work: () => Promise<T>): Promise<T> {
  try {
    return await work();
  } catch (error: unknown) {
    if (signal?.aborted) throw error;
    throw new QueryFailure(error, query, sql);
  }
}

/** THE TAB'S HOST: its planner plans in the page (its module, or a server it asks), its engine runs the SQL here. */
export function localHost(planner: Planner, engine: QueryEngine): PlanAndRunHost {
  return {
    name: `plan+${engine.name}`,
    planner,
    async planAndRun(query, epoch, signal) {
      const plan = await step(query, undefined, signal, () => planner.plan(query, signal));
      // the engine types every column by the plan (engine.ts typedByPlan)
      return { plan, rows: await step(query, plan.sql, signal, () => engine.execute(plan, epoch, signal)) };
    },
    async stream(query, epoch, onChunk, signal) {
      const plan = await step(query, undefined, signal, () => planner.plan(query, signal));
      await step(query, plan.sql, signal, () => engine.stream(plan, epoch, onChunk, signal));
    },
  };
}

/** The tab's PlanAndRun, its planner and engine named: the planner makes the SQL, the tab's engine runs it. */
export class PlanThenRun extends PlanAndRun {
  readonly planner: Planner;
  readonly engine: QueryEngine;

  constructor(planner: Planner, engine: QueryEngine) {
    super(localHost(planner, engine));
    this.planner = planner;
    this.engine = engine;
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
