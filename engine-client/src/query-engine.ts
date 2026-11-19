// THE QUERY ENGINE'S SHAPES (the C5-T last round's T1, 2026-11-15: out of engine.ts and duckdb.ts, both run-side, so an
// app reaches the run side through the doors alone and takes these shapes as records): what an engine is (QueryEngine),
// what raw SQL answers (RawTable), the connection a plan holds (HeldConnection), a rejected query (QueryError) and a
// planned query's rows typed by its plan (typedByPlan). No send, no plan (//engine-client:records); engine.ts and
// duckdb.ts re-export them for their importers.

import type { Plan } from './relation-type.ts';
import type { Receipt } from './receipt.ts';
import type { ResultTable, Scalar } from './result.ts';
import type { PlannedSql } from './planned-sql.ts';

/** A raw read's column: its name and values, and NO Pure type -- no compiler typed it. */
export interface RawColumn {
  readonly name: string;
  readonly values: readonly Scalar[];
}

/** What raw SQL returns: DDL, a DESCRIBE, a count -- SQL no planner wrote. */
export interface RawTable {
  readonly columns: readonly RawColumn[];
  readonly rowCount: number;
  readonly epoch: number;
  readonly elapsedMs: number;
  /** What ran it (receipt.ts); `typedByPlan` carries it onto the typed result. */
  readonly receipt?: Receipt;
}

export interface QueryEngine {
  /** For diagnostics and telemetry, e.g. 'duckdb-wasm'. */
  readonly name: string;
  /**
   * Run a PLANNED query and return it columnar, every column typed by the plan
   * (`typedByPlan`): the compiler's types, never the engine's wire types
   * (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T1). `epoch` is carried through
   * onto the result so a caller can tell which snapshot it answers; the engine itself
   * does no staleness checking -- that is EpochGuard's job.
   *
   * `signal` aborts work a newer interaction has replaced. How much an
   * engine can honour it is its own business and differs sharply: an
   * HTTP executor stops at once, while DuckDB-WASM cannot interrupt a
   * query already inside its C++ call and can only decline to start
   * one. An engine that cannot cancel must still not PRETEND to --
   * check the signal before starting, and say so in its own doc.
   */
  execute(plan: Plan, epoch: number, signal?: AbortSignal): Promise<ResultTable>;
  // (a plan's `params` -- the values its statement binds, M3 -- are bound by `execute` and `stream`, or refused by name by
  // an engine that binds none: never dropped)
  /**
   * Run a PLANNED query and hand its rows over a chunk at a time, as the engine produces
   * them, each typed by the plan like `execute`'s; none is kept, so a whole table can pass
   * through in flat memory (reading every row of a column, `JsonColumnReader.all`).
   * Resolves after the last chunk. An engine that cannot stream hands over its whole result
   * as one chunk, and says so in its own doc. Same `signal` rules as `execute`.
   */
  stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void>;
  /**
   * Run raw SQL no planner wrote -- or a plan's statement as its text (a host's pure/v1 execute) -- answering names and
   * values, no types. Same `signal` rules. `params`: the values its `?`s bind, in order (a plan's data: payloads,
   * plan-params.ts, the tenets audit's M3; a run's parameters, typed by their declarations, plan-runner.ts
   * checkParameters: 2026-11-17, the edge program's HOST stream); an engine that binds none refuses them by name, never
   * runs the text alone.
   */
  run(sql: PlannedSql, epoch: number, signal?: AbortSignal, params?: readonly (string | number | bigint | boolean | null)[]): Promise<RawTable>;
  close(): Promise<void>;
}

/** An engine pointed at another, switched with `use` (project-data.ts ProjectRunner, made by its door): what an app
 *  holds to run its screens on a project's database. */
export interface PointedEngine extends QueryEngine {
  /** Queries run on `engine` from now on; none, the host's own connection again. */
  use(engine: QueryEngine | undefined): void;
}

/** The connection as a plan holds it (DuckDbEngine.exclusive): no other query between its statements. */
export interface HeldConnection {
  /** A statement's rows, each its values by column; one with values to bind, prepared and run with them (refused by
   *  name where the connection prepares none). */
  rows(sql: PlannedSql, params: readonly (string | number | bigint | boolean | null)[]): Promise<unknown[][]>;
  /** A statement's answer decoded as the engine's queries are (a catalog read inside the hold). */
  table(sql: PlannedSql): Promise<RawTable>;
  /**
   * One Arrow IPC stream's rows into `target` by the connection's own Arrow import, no statement spelled: `create` makes
   * the table, else the rows are appended -- a planned stream step's chunks (plan-runner.ts runLoad: a snapshot from a
   * remote plane, in the step's transaction). A connection without the import refuses it by name.
   */
  insertArrow(bytes: Uint8Array, target: { readonly schema?: string; readonly table: string }, create: boolean): Promise<void>;
}

/**
 * A planned query's rows, typed by its plan. The engine decoded the values; the compiler says what they are. Read BY
 * POSITION, as Java's runner reads a typed-rows statement (JdbcStatements: "a typed-rows statement answers the plan's
 * columns in their order") and Python's typed_by_plan: each column named and typed as the plan names it -- a VALUE's
 * one column is `value` whatever its SQL alias (`u_map__n`). A statement answering another number of columns than the
 * plan declares is REFUSED: the two disagree about the query's shape, a bug to surface, never a gap to fill (F1 of PR
 * #121's review, 2026-11-19: until then read by name here, so the hosts differed).
 */
export function typedByPlan(raw: RawTable, plan: Plan): ResultTable {
  const declared = plan.columns;
  if (raw.columns.length !== declared.length) {
    const beyond = raw.columns.slice(declared.length).map((c) => c.name);
    const said = beyond.length > 0 ? `the plan does not type the result column '${beyond[0]}'`
      : `the statement answers ${raw.columns.length} columns, and the plan declares ${declared.length}`;
    throw new QueryError(`${said} [${declared.map((c) => c.name).join(', ')}]`, plan.sql);
  }
  return {
    ...raw,
    columns: raw.columns.map((c, i) => ({ name: declared[i]!.name, type: declared[i]!.type, values: c.values })),
  };
}

/** Thrown for a query the engine rejected, carrying the SQL for triage. */
export class QueryError extends Error {
  readonly sql: string;
  constructor(message: string, sql: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'QueryError';
    this.sql = sql;
  }
}
