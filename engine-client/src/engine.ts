// The executor seam.
//
// One interface, two implementations: SQL run against a warehouse on a
// server, or against DuckDB in the browser. Both receive the SAME SQL,
// because legend-lite is the single planner -- the snapshot compiles to
// Pure, Pure lowers to SQL once, and only the place it runs differs.
//
// This is also what makes the snap's LOCATION swappable. A snap in a
// browser tab, in OPFS, in a native DuckDB on the user's machine, or in
// a per-user cache on a server are all the same engine running the same
// SQL; moving it changes where the work happens, never the answer. That
// matters because the deciding factor is likely to be policy on data at
// rest rather than performance.

import type { Plan } from './relation-type.ts';
import type { Receipt } from './receipt.ts';
import type { ResultTable, Scalar } from './result.ts';

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
  /**
   * Run a PLANNED query and hand its rows over a chunk at a time, as the engine produces
   * them, each typed by the plan like `execute`'s; none is kept, so a whole table can pass
   * through in flat memory (reading every row of a column, `JsonColumnReader.all`).
   * Resolves after the last chunk. An engine that cannot stream hands over its whole result
   * as one chunk, and says so in its own doc. Same `signal` rules as `execute`.
   */
  stream(plan: Plan, epoch: number, onChunk: (chunk: ResultTable) => void, signal?: AbortSignal): Promise<void>;
  /** Run raw SQL no planner wrote: names and values, no types. Same `signal` rules. */
  run(sql: string, epoch: number, signal?: AbortSignal): Promise<RawTable>;
  close(): Promise<void>;
}

/**
 * A planned query's rows, typed by its plan. The engine decoded the values; the
 * compiler says what they are. A result column the plan does not type is REFUSED: the
 * two disagree about the query's shape, a bug to surface, never a gap to fill.
 */
export function typedByPlan(raw: RawTable, plan: Plan): ResultTable {
  const types = new Map(plan.columns.map((c) => [c.name, c.type]));
  return {
    ...raw,
    columns: raw.columns.map((c) => {
      const type = types.get(c.name);
      if (type === undefined) {
        throw new QueryError(`the plan does not type the result column '${c.name}'`, plan.sql);
      }
      return { name: c.name, type, values: c.values };
    }),
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
