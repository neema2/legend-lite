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

import type { ResultTable } from './result.ts';

export interface QueryEngine {
  /** For diagnostics and telemetry, e.g. 'duckdb-wasm'. */
  readonly name: string;
  /**
   * Run SQL and return it columnar. `epoch` is carried through onto the
   * result so a caller can tell which snapshot it answers; the engine
   * itself does no staleness checking -- that is EpochGuard's job.
   *
   * `signal` aborts work a newer interaction has replaced. How much an
   * engine can honour it is its own business and differs sharply: an
   * HTTP executor stops at once, while DuckDB-WASM cannot interrupt a
   * query already inside its C++ call and can only decline to start
   * one. An engine that cannot cancel must still not PRETEND to --
   * check the signal before starting, and say so in its own doc.
   */
  execute(sql: string, epoch: number, signal?: AbortSignal): Promise<ResultTable>;
  close(): Promise<void>;
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
